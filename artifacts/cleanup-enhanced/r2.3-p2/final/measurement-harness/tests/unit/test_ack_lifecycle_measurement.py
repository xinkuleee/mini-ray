"""Four controlled real lifecycle slices, three exact repetitions each.

Run one exact parameter selector per existing 30 s process-tree runner.
No process/socket/thread/sleep; two actual Core authorities and <=16 KiB store.
The Node completion callback is the existing local fixture boundary, not an
OS Worker or resource-ledger claim. Metadata callback cap: 256 per slice.
"""
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from enum import Enum
import hashlib
import json
import multiprocessing.process
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import uuid

import pytest

from miniray import core as core_module, enhanced_publication as ep, ids as ids_module
from miniray import output_protocol as wire, protocol, transport
from miniray.core import CoreWorker
from miniray.ids import LeaseID
from miniray.node import NodeServer
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState, OutputOwnerPublicationPlan
from miniray.publication_sources import BorrowedContainedSource, OwnedContainedSource
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.resources import ResourceVector
from tests.unit._ack_cost_capture import CostCapture
from tests.unit.test_enhanced_owner_client import _Runtime as _OwnerRuntime
from tests.unit.test_put_reference_runtime import _Runtime as _PutRuntime, _Mailbox

pytestmark = pytest.mark.unit


def _plain(value):
    if isinstance(value, ids_module._OpaqueID):
        return {"type": type(value).__name__, "hex": value.hex}
    if isinstance(value, Enum):
        return {"type": type(value).__name__, "value": value.value}
    if type(value) is bytes:
        return {"bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    if is_dataclass(value) and not isinstance(value, type):
        return {"type": type(value).__name__, **{field.name: _plain(getattr(value, field.name)) for field in fields(value)}}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_plain(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    if type(value) is dict:
        return [{"key": _plain(key), "value": _plain(item)} for key, item in value.items()]
    assert value is None or type(value) in (str, bool, int, float), type(value)
    return value


class _Probe:
    def __init__(self, capture, monkeypatch):
        self.capture, self.patch = capture, monkeypatch
        self.calls = 0
        self.stack = []
        self.observing = 0

    @contextmanager
    def operation(self, name):
        with self.capture.boundary(name):
            if sys.getprofile() is None:
                with self.capture.profile_current_thread():
                    yield
            else:
                yield

    def action(self, name, method, *args, **kwargs):
        with self.operation(name):
            return method(*args, **kwargs)

    def message(self, direction, handler, value, delivery, role):
        envelope = (lambda item: transport._WireRequest(handler, item)) if direction == "request" else (lambda item: transport._WireReply(ok=True, value=item))
        self.capture.message(direction=direction, handler=handler, value=value,
            serializer=transport._serialize, envelope=envelope, delivery=delivery, query_role=role)
        with self.capture.suspended():
            self.capture.events[-1]["value"] = _plain(value)

    def exchange(self, address, handler, request, invoke):
        self.calls += 1
        assert self.calls <= 256, "measurement exceeded fixed callback cap"
        role = "business" if type(request) is ep.GetPublication else "none"
        frame = {"request": request, "generated": None}
        with self.operation("rpc:" + handler):
            self.message("request", handler, request, "delivered", role)
            self.stack.append(frame)
            try:
                reply = invoke()
            except BaseException:
                if frame["generated"] is not None:
                    self.message("reply", handler, frame["generated"], "discarded", role)
                raise
            else:
                self.message("reply", handler, reply, "delivered", role)
                return reply
            finally:
                assert self.stack.pop() is frame

    def attach(self, runtime):
        runtime._cost_capture = self.capture
        authority = runtime.authority
        actual_apply, actual_query, actual_snapshots = authority.apply, authority.query, authority.snapshots

        def apply(request):
            reply = actual_apply(request)
            if self.stack and not self.observing and request is self.stack[-1]["request"]:
                self.stack[-1]["generated"] = reply
            return reply

        def query(request):
            # These are the fixture/harness's explicit history observations.
            # Production client queries cross runtime.rpc -> authority.apply.
            with self.capture.suspended(), self.capture.boundary("test_observation"):
                self.observing += 1
                try:
                    self.message("request", ep.PUBLICATION_HANDLER, request, "not_sent", "test_observation")
                    reply = actual_query(request)
                    self.message("reply", ep.PUBLICATION_HANDLER, reply, "generated", "test_observation")
                    return reply
                finally:
                    self.observing -= 1

        self.patch.setattr(authority, "apply", apply)
        self.patch.setattr(authority, "query", query)
        def snapshots():
            # Only fixture teardown/explicit test checks call this local scan.
            with self.capture.suspended():
                return actual_snapshots()
        self.patch.setattr(authority, "snapshots", snapshots)
        actual_rpc = runtime.rpc
        self.patch.setattr(runtime, "rpc", lambda address, handler, request: self.exchange(
            address, handler, request, lambda: actual_rpc(address, handler, request)))
        cores = runtime.cores if hasattr(runtime, "cores") else (runtime.owner, runtime.publisher)
        for core in cores:
            core._rpc = core._borrow_rpc = runtime.rpc
            core._borrow_rpc_with_deadline = runtime.owner_rpc
        if isinstance(runtime, _OwnerRuntime):
            # Direct adapter owner callbacks also carry real protocol messages.
            for name, handler in (("register_output_handoff", wire.REGISTER_OUTPUT_HANDOFF_HANDLER),
                                  ("report_output_handoff_complete", wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER),
                                  ("report_output_handoff_rollback", wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER)):
                actual = getattr(runtime.owner, name)
                self.patch.setattr(runtime.owner, name, lambda request, actual=actual, handler=handler: self.exchange(
                    runtime.owner.owner_address, handler, request, lambda: actual(request)))
            runtime.adapter._prepare_child = runtime.child
            runtime.adapter._promote_child = runtime.child
            runtime.adapter._release_child = lambda address, request: runtime.rpc(address, "release_contained_reference", request)

    def checkpoint(self, name, runtime, *, reference=None, outer=None):
        with self.capture.suspended():
            central = runtime.authority.query(ep.GetPublication(reference)).snapshot if reference is not None else None
            cores = runtime.cores if hasattr(runtime, "cores") else (runtime.owner, runtime.publisher)
            facts = {"authority": _plain(central), "store_used_bytes": runtime.store.used_bytes,
                     "cores": [{"worker_id": _plain(core.worker_id),
                         "accepted_tasks": core._accepted_task_count,
                         "finish_barriers": len(core._task_finish_barriers),
                         "gc_obligations": len(core._object_gc_obligations),
                         "borrowed_release_obligations": len(getattr(core, "_borrowed_release_obligations", {})),
                         "protocol_unresolved": len(core._protocol_unresolved)} for core in cores]}
            if outer is not None:
                collection = runtime.owner.owner_table.collection_state(outer.object_id)
                facts["collection"] = _plain(collection)
                facts["owner"] = (None if collection is ObjectCollectionState.COLLECTED
                                  else _plain(runtime.owner.owner_table.snapshot(outer.object_id)))
            if reference is not None and hasattr(runtime, "journal"):
                facts["journal"] = _plain(runtime.journal.snapshot(reference.key))
                facts["completion_callbacks"] = len(runtime.completions)
                facts["node_adoption_acks"] = len(runtime.node_acks)
            self.capture.checkpoint(name, facts)


class _TaskRuntime(_OwnerRuntime):
    """Address both existing real owners; no new authority or third Core."""
    def __init__(self):
        super().__init__()
        for core in (self.owner, self.publisher):
            core._reference_mailbox = _Mailbox(core)
            core._borrow_rpc_with_deadline = self.owner_rpc
        self.adapter._release_child = lambda address, request: self.rpc(address, "release_contained_reference", request)

    def owner_rpc(self, address, handler, request, timeout):
        assert timeout is None
        return self.rpc(address, handler, request)

    def child(self, address, request):
        handler = "prepare_stored_contained_pin" if type(request) is protocol.PrepareStoredContainedPin else "promote_stored_contained_pin"
        assert type(request) in (protocol.PrepareStoredContainedPin, protocol.PromoteStoredContainedPin)
        return self.rpc(address, handler, request)

    def rpc(self, address, handler, request):
        destinations = [core for core in (self.owner, self.publisher) if core.owner_address == address]
        if destinations:
            assert len(self.calls) < 160, "owner combination exceeded original callback budget"
            self.calls.append((handler, request))
            methods = {"prepare_stored_contained_pin": "prepare_stored_contained_pin",
                "promote_stored_contained_pin": "promote_stored_contained_pin",
                "release_contained_reference": "release_contained_reference",
                "acquire_borrowed_object": "acquire_exported_reference",
                "release_borrowed_object": "release_borrowed_reference",
                "get_owned_object": "get_owned_object"}
            assert handler in methods
            return getattr(destinations[0], methods[handler])(request)
        if address == self.owner.node_address and handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            assert len(self.calls) < 160
            self.calls.append((handler, request))
            assert type(request) is wire.AckOutputPublicationAdopted
            # Preserve the existing fixture's exact contract checks, but do not
            # charge their extra observation copies to production ACK cost.
            with self._cost_capture.suspended():
                identity = request.proof.complete.publication_id
                envelope = self.envelopes[identity]
                owner = self.owner.get_output_handoff(wire.GetOutputHandoff(identity)).snapshot
                assert owner.adoption == request.proof
                assert self.owner.owner_table.output_owner_publication_receipt(
                    OutputOwnerPublicationPlan(envelope.manifest.execution, envelope)).committed
                central = self.authority.query(ep.GetPublication(_publication(envelope, self).reference)).snapshot
                assert central.adoption == request.proof
                assert request.gcs_adoption == central.receipt(ep.PublicationStage.ADOPTED)
            self.journal.retire_completed(request.proof)
            self.node_acks.append(request)
            return wire.AckOutputPublicationAdoptedReply(request, True)
        return super().rpc(address, handler, request)

    def track(self, reference):
        self.references.append(reference)
        return reference

    def submit(self):
        # An importable, identical function avoids absolute fixture filename
        # differences in cloudpickle's local-lambda payload across snapshots.
        pending, reference = self.owner._register_submission(
            self.owner.define_remote_function(_measured_task), (), {}, ResourceVector({"CPU": 1}),
            max_retries=1, _enqueue=True)
        assert self.take() == (pending,)
        self.references.append(reference)
        return pending, reference

    def prepare_measured(self, probe, pending, value, *, stored=False):
        # Same real sequence as _OwnerRuntime.prepare, with an explicit 8 KiB
        # measurement sample cap for two refs; the original fixture is unchanged.
        identity = OutputPublicationID(LeaseID((len(self.envelopes) + 1).to_bytes(16, "big")), pending.execution)
        session = OutputDiscoverySession(OutputPublicationHeader(identity, self.owner.job_id,
            self.publisher.worker_id, self.owner.worker_id, self.incarnation), inline_threshold=0 if stored else 8192)
        outputs = probe.action("task_discovery", session.discover, (value, "payload"))
        assert outputs.manifest.value.size_bytes < 8192
        probe.action("node_prepare", self.adapter.prepare, outputs.manifest, outputs.payload)
        probe.action("source_handoff", session.release_sources_after_promotions)
        envelope = probe.action("node_complete", self.adapter.complete, identity, commit_lease=self.completions.append)
        self.envelopes[identity] = envelope
        assert probe.action("terminal_report", self.adapter.report_terminal, identity)
        return envelope

    def finish(self):
        super().finish()
        for core in (self.owner, self.publisher):
            assert not getattr(core, "_borrowed_release_obligations", {})
            assert not core._task_finish_barriers


def _measured_task():
    return None


def _publication(envelope, runtime):
    return ep.TaskPublication(envelope.manifest, runtime.owner.owner_address)


def _collect_task(probe, runtime, pending, outer, envelope):
    probe.action("finish_attempt", runtime.finish_attempt, pending)
    probe.action("outer_close", outer.close, timeout=0)
    probe.action("gc_drain", runtime.drain)
    assert runtime.owner.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
    assert not runtime.journal.snapshot(envelope.publication_id).result_retained
    final = runtime.authority.query(ep.GetPublication(_publication(envelope, runtime).reference)).snapshot
    assert final.receipt(ep.PublicationStage.RETIRED) is not None and not final.graph_active
    assert final.complete == envelope.complete and final.adoption is not None and final.closed_holds is not None
    probe.checkpoint("collected", runtime, reference=final.reference, outer=outer)


def _task_zero(probe, runtime):
    pending, outer = probe.action("submit", runtime.submit)
    envelope = runtime.prepare_measured(probe, pending, None)
    assert not envelope.manifest.value.transfers
    assert probe.action("adopt", runtime.adopt, pending, envelope)
    assert runtime.owner.owner_table.snapshot(outer.object_id).state is ObjectState.READY_INLINE
    assert len(runtime.completions) == len(runtime.node_acks) == 1
    probe.checkpoint("adopted", runtime, reference=_publication(envelope, runtime).reference, outer=outer)
    _collect_task(probe, runtime, pending, outer, envelope)


def _task_mixed(probe, runtime):
    owned = probe.action("owned_leaf_put", runtime.leaf, 11)
    foreign = runtime.track(probe.action("foreign_leaf_put", runtime.owner.put, 22))
    seed = runtime.track(probe.action("export_seed_put", runtime.owner.put, (foreign,)))
    seed_data = runtime.owner.owner_table.snapshot(seed.object_id).inline_data
    borrowed, = probe.action("actual_borrow_import", runtime.publisher._loads_owned_value, seed_data)
    runtime.track(borrowed)
    assert borrowed.borrower_token is not None and borrowed.borrow_source is not None
    expected_borrow = ((runtime.publisher.worker_id, borrowed.borrower_token), borrowed.borrow_source)
    assert expected_borrow in runtime.owner.owner_table.snapshot(foreign.object_id).borrowed_sources
    pending, outer = probe.action("submit", runtime.submit)
    envelope = runtime.prepare_measured(probe, pending, (owned, borrowed))
    transfers = envelope.manifest.value.transfers
    assert len(transfers) == 2
    assert {type(transfer.source) for transfer in transfers} == {OwnedContainedSource, BorrowedContainedSource}
    owners = {runtime.owner.worker_id: runtime.owner, runtime.publisher.worker_id: runtime.publisher}
    for transfer in transfers:
        snapshot = owners[transfer.contained_owner_worker_id].owner_table.snapshot(transfer.contained_object_id)
        assert transfer.final_hold in snapshot.contained_holds and transfer.provisional_hold not in snapshot.contained_holds
    runtime.lose = ep.CommitGraph
    assert not probe.action("adopt_lost_commit_ack", runtime.adopt, pending, envelope)
    publication = _publication(envelope, runtime)
    central = runtime.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert central.receipt(ep.PublicationStage.COMMITTED) is not None and central.adoption is None
    handoff = runtime.owner.get_output_handoff(wire.GetOutputHandoff(envelope.publication_id)).snapshot
    assert handoff.phase is OutputHandoffPhase.PENDING and handoff.adoption is None
    assert runtime.owner.owner_table.snapshot(outer.object_id).state is ObjectState.PENDING
    assert runtime.journal.snapshot(envelope.publication_id).result_retained and not runtime.node_acks
    probe.checkpoint("commit_ack_unknown", runtime, reference=publication.reference, outer=outer)
    probe.action("resume_exact_commit", runtime.resume, pending, envelope)
    assert runtime.owner.owner_table.snapshot(outer.object_id).state is ObjectState.READY_INLINE
    assert len(runtime.completions) == len(runtime.node_acks) == 1
    _collect_task(probe, runtime, pending, outer, envelope)
    for transfer in transfers:
        table = owners[transfer.contained_owner_worker_id].owner_table
        assert transfer.final_hold not in table.snapshot(transfer.contained_object_id).contained_holds
        assert table.contained_release_was_seen(transfer.contained_object_id, transfer.final_hold)
    probe.action("borrowed_source_close", borrowed.close, timeout=0)
    probe.action("borrow_release_drain", runtime.drain)
    assert expected_borrow not in runtime.owner.owner_table.snapshot(foreign.object_id).borrowed_sources


def _whole(probe, runtime):
    old_child, new_child = probe.action("old_leaf_put", runtime.leaf, 10), probe.action("new_leaf_put", runtime.leaf, 11)
    pending, outer = probe.action("submit", runtime.submit)
    first = runtime.prepare_measured(probe, pending, old_child, stored=True)
    assert probe.action("first_adopt", runtime.adopt, pending, first)
    probe.action("first_finish", runtime.finish_attempt, pending)
    old_publication = _publication(first, runtime)
    old_adoption = runtime.authority.query(ep.GetPublication(old_publication.reference)).snapshot.adoption
    old_transfer, = first.manifest.value.transfers
    assert runtime.store.contains(outer.object_id)
    assert probe.action("drop_first_bytes", runtime.owner.drop_object, outer)
    assert runtime.owner.owner_table.snapshot(outer.object_id).state is ObjectState.LOST
    outcome = probe.action("admit_whole_reconstruction", runtime.owner._admit_owned_object_reconstruction, outer.object_id)
    assert outcome.disposition is ReconstructionDisposition.START
    next_pending, = runtime.take()
    assert next_pending.spec.attempt_id == pending.spec.attempt_id.next()
    old = runtime.authority.query(ep.GetPublication(old_publication.reference)).snapshot
    assert not old.graph_active and old.receipt(ep.PublicationStage.RETIRED) is not None
    assert old_transfer.final_hold not in runtime.publisher.owner_table.snapshot(old_child.object_id).contained_holds
    probe.checkpoint("old_retired_before_replacement", runtime, reference=old_publication.reference, outer=outer)
    second = runtime.prepare_measured(probe, next_pending, new_child)
    assert probe.action("replacement_adopt", runtime.adopt, next_pending, second)
    current_publication = _publication(second, runtime)
    current = runtime.authority.query(ep.GetPublication(current_publication.reference)).snapshot
    late = (ep.CommitGraph(old_publication.reference), ep.RecordAdoption(old_adoption), ep.RetireGraph(old.closed_holds))
    for request in late:
        assert runtime.rpc(runtime.owner.gcs_address, ep.PUBLICATION_HANDLER, request).accepted
        assert runtime.authority.query(ep.GetPublication(current_publication.reference)).snapshot == current
    assert runtime.owner.owner_table.snapshot(outer.object_id).current_attempt == next_pending.spec.attempt_id
    _collect_task(probe, runtime, next_pending, outer, second)
    for request in late:
        assert runtime.rpc(runtime.owner.gcs_address, ep.PUBLICATION_HANDLER, request).accepted
    assert not runtime.authority.query(ep.GetPublication(current_publication.reference)).snapshot.graph_active
    assert len(runtime.completions) == len(runtime.node_acks) == 2


def _put_contained(probe, runtime):
    # Same borrowed/stored alias-and-child-lifetime path as the retained put
    # contract, with explicit business boundaries around actual Core calls.
    leaf = runtime.track(probe.action("source_leaf_put", runtime.owner.put, "leaf-value"))
    child = runtime.track(probe.action("source_child_put", runtime.owner.put, ("inner", leaf)))
    probe.action("source_leaf_close", runtime.close, leaf)
    seed = runtime.track(probe.action("source_seed_put", runtime.owner.put, (child,)))
    seed_data = runtime.owner.owner_table.snapshot(seed.object_id).inline_data
    source, = probe.action("source_actual_import", runtime.borrower._loads_owned_value, seed_data)
    runtime.track(source)
    assert source.borrower_token is not None
    probe.action("source_seed_close", runtime.close, seed)
    probe.action("source_original_child_close", runtime.close, child)
    core, leaf_id = runtime.borrower, leaf.object_id
    tag, imported_leaf = probe.action("source_get", core.get, source)
    runtime.track(imported_leaf)
    assert tag == "inner" and imported_leaf.borrower_token is not None
    assert probe.action("leaf_get", core.get, imported_leaf) == "leaf-value"
    probe.action("imported_leaf_close", runtime.close, imported_leaf)
    core.inline_threshold = 1
    outer = runtime.track(probe.action("contained_stored_put", core.put, ("whole", {"aliases": [source, source]}, 7)))
    snapshot = core.owner_table.snapshot(outer.object_id)
    assert snapshot.state is ObjectState.READY_STORED
    assert len(snapshot.outgoing_contained_edges) == 1 and snapshot.output_publication is None
    assert runtime.store.contains(outer.object_id)
    publication = core._publication_client().current(outer.object_id)
    central = runtime.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert type(publication) is ep.PutPublication and central.prepared is not None
    assert central.receipt(ep.PublicationStage.COMMITTED) is not None
    assert central.complete is None and central.adoption is None and central.receipt(ep.PublicationStage.ARMED) is None
    probe.checkpoint("put_ready_stored", runtime, reference=publication.reference)
    original_token = source.borrower_token
    probe.action("source_close", runtime.close, source)
    assert runtime.owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.ACTIVE
    value = probe.action("outer_get", core.get, outer)
    assert type(value) is tuple and len(value) == 3 and value[0] == "whole" and value[2] == 7
    restored = runtime.track(value[1]["aliases"][0])
    assert value[1]["aliases"][1] is restored
    assert restored.object_id == source.object_id and restored is not source
    assert restored.borrower_token is not None and restored.borrower_token != original_token
    probe.action("outer_close_gc", runtime.close, outer)
    assert core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
    assert not runtime.store.contains(outer.object_id, sealed_only=False)
    tag, last_leaf = probe.action("surviving_child_get", core.get, restored)
    runtime.track(last_leaf)
    assert tag == "inner" and probe.action("surviving_leaf_get", core.get, last_leaf) == "leaf-value"
    probe.action("restored_close", runtime.close, restored)
    assert runtime.owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED
    assert runtime.owner.owner_table.collection_state(leaf_id) is ObjectCollectionState.ACTIVE
    probe.action("last_leaf_close", runtime.close, last_leaf)
    assert runtime.owner.owner_table.collection_state(leaf_id) is ObjectCollectionState.COLLECTED
    assert not getattr(core, "_put_handoffs", {})
    runtime.assert_no_tasks()
    probe.checkpoint("put_collected", runtime, reference=publication.reference)


@pytest.fixture(autouse=True)
def _bounded_and_deterministic(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("ACK measurement attempted process/thread/socket/blocking work")
    def already_set(event, timeout=None):
        assert event.is_set()
        return True
    for kind, name in ((CoreWorker, "__init__"), (NodeServer, "__init__"),
            (threading.Thread, "start"), (threading.Timer, "__init__"),
            (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    counts = {}
    def identifier(cls):
        key = cls.__name__
        counts[key] = counts.get(key, 0) + 1
        assert counts[key] <= 32
        return cls(hashlib.sha256(("r23:" + key + ":" + str(counts[key])).encode()).digest()[:16])
    def token():
        counts["uuid"] = counts.get("uuid", 0) + 1
        assert counts["uuid"] <= 256
        return uuid.UUID(int=counts["uuid"])
    monkeypatch.setattr(ids_module._OpaqueID, "random", classmethod(identifier))
    monkeypatch.setattr(uuid, "uuid4", token)


@pytest.mark.parametrize("slice_name,repetition", [(name, repetition) for name in ("task0", "task_mixed", "put_contained", "whole") for repetition in (1, 2, 3)],
    ids=[name + "-r" + str(repetition) for name in ("task0", "task_mixed", "put_contained", "whole") for repetition in (1, 2, 3)])
def test_actual_lifecycle_cost(monkeypatch, capsys, slice_name, repetition):
    capture = CostCapture(slice_name=slice_name, repetition=repetition, max_events=2048)
    probe = _Probe(capture, monkeypatch)
    runtime = _PutRuntime() if slice_name == "put_contained" else _TaskRuntime()
    probe.attach(runtime)
    outcome = "FAILED_OR_INCOMPLETE"
    try:
        if slice_name == "put_contained":
            _put_contained(probe, runtime)
            runtime.assert_no_tasks()
            with capture.suspended():
                snapshots = runtime.authority.snapshots()
                assert snapshots and all(type(item.publication) is ep.PutPublication for item in snapshots)
                assert all(item.complete is None and item.adoption is None and item.receipt(ep.PublicationStage.ARMED) is None for item in snapshots)
                capture.checkpoint("put_collected", {"snapshots": _plain(snapshots), "store_used_bytes": runtime.store.used_bytes})
        else:
            {"task0": _task_zero, "task_mixed": _task_mixed, "whole": _whole}[slice_name](probe, runtime)
        probe.action("final_cleanup", runtime.finish)
        probe.checkpoint("all_cleanup_finished", runtime)
        outcome = "COMPLETED_ASSERTIONS"
    finally:
        # Never mask a failure by manufacturing a completed cleanup record.
        cleanup_error = None
        if outcome != "COMPLETED_ASSERTIONS":
            try:
                probe.action("failure_cleanup", runtime.finish)
            except BaseException as exc:
                cleanup_error = {"type": type(exc).__name__, "message": str(exc)}
        payload = capture.payload()
        payload["outcome"] = outcome
        payload["callback_count"] = probe.calls
        payload["failure_cleanup_error"] = cleanup_error
        payload["serializer"] = {"module": transport._pickler.__name__, "protocol": transport.pickle.HIGHEST_PROTOCOL}
        payload["scope"] = "controlled_local_Core_Node_authority_no_OS_Worker_or_resource_ledger"
        target = Path(os.environ.get("MINIRAY_ACK_CAPTURE_DIR", str(Path(__file__).resolve().parents[2] / "artifacts" / "ack-measurement")))
        target.mkdir(parents=True, exist_ok=True)
        output = target / (slice_name + "-r" + str(repetition) + ".json")
        output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with capsys.disabled():
            print("ACK_MEASUREMENT " + json.dumps({"slice": slice_name, "repetition": repetition, "outcome": outcome, "capture": str(output), "events": len(capture.events), "callbacks": probe.calls}, sort_keys=True))
