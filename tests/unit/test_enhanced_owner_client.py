"""Finite W3/W4 owner composition; no process, socket, thread or waiting.

Two real owner tables, the real Core submission/adoption/reconstruction/GC
methods, a real Node journal and a 16 KiB store share synchronous transport
callbacks. Each case publishes at most two attempts, two children and one
outer; callback faults lose one actual C5/C7 ACK. Node Complete's callback
counts this local fixture's execution fact, not a real scheduled OS Worker.
"""
from dataclasses import replace
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, enhanced_publication as ep, protocol, output_protocol as wire
from miniray.core import CoreWorker, _HomeRoute, _PendingTask, _DelayedReadyTask, _WAKE_COORDINATOR
from miniray.enhanced_publication_client import PublicationClient
from miniray.errors import SystemTaskError
from miniray.ids import LeaseID, TaskID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.ownership import ObjectState, ObjectCollectionState, OutputOwnerPublicationPlan
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.resources import ResourceVector
from tests.unit._pure_core import make_pure_core, close_pure_core

pytestmark = pytest.mark.unit


class _Runtime:
    def __init__(self):
        self.owner, self.publisher = make_pure_core(), make_pure_core()
        owner, publisher = self.owner, self.publisher
        publisher.job_id = owner.job_id
        publisher.driver_task_id = TaskID.for_driver(owner.job_id)
        publisher.node_id = owner.node_id
        owner.owner_address, publisher.owner_address = ("owner.invalid", 2001), ("publisher.invalid", 2002)
        owner.gcs_address = publisher.gcs_address = ("gcs.invalid", 2003)
        owner.node_address = publisher.node_address = ("node.invalid", 2004)
        for core in (owner, publisher):
            core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
        self.authority = ep.PublicationAuthority()
        for core in (owner, publisher):
            assert not core._test_publication_authority.snapshots()
            core._test_publication_authority = self.authority
        self.journal = OutputPublicationJournal()
        self.incarnation = OutputPublicationNodeIncarnation(owner.node_id, 21001, 3)
        self.node = node = object.__new__(NodeServer)
        node.node_id, node._node_pid, node._registration_epoch = owner.node_id, 21001, 3
        node._state_lock = threading.RLock()
        node._object_store = self.store = ObjectStore(16 * 1024)
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._local_replica_write_claims, node._object_localization_locks = {}, {}
        node._owner_death_fences = {}
        node._output_publication_journal = self.journal
        self.calls, self.node_acks, self.completions, self.references, self.envelopes = [], [], [], [], {}
        self.lose = None
        for core in (owner, publisher):
            core._rpc = core._borrow_rpc = self.rpc
            core._resolve_node_address = lambda node_id, *, home_route=None: owner.node_address
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register, report_complete=self.report_complete,
            report_rollback=self.rollback, publication_value=lambda manifest: ep.TaskPublication(manifest, owner.owner_address),
            publication_rpc=lambda request: self.rpc(owner.gcs_address, ep.PUBLICATION_HANDLER, request),
            abort_owner=self.abort, prepare_child=self.child, promote_child=self.child,
            release_child=lambda address, request: publisher.release_contained_reference(request),
            seal_replica=node._seal_output_publication_replica, drop_replica=node._drop_output_publication_replica,
        )

    def register(self, manifest):
        reply = self.owner.register_output_handoff(wire.RegisterOutputHandoff(manifest))
        assert reply.accepted, reply.error

    def report_complete(self, witness):
        assert self.journal.snapshot(witness.publication_id).complete == witness
        reply = self.owner.report_output_handoff_complete(wire.ReportOutputHandoffComplete(witness))
        assert type(reply) is wire.OutputHandoffCompleteAck and reply.witness == witness
        assert reply.accepted and reply.error is None

    def rollback(self, tombstone, *, manifest):
        reply = self.owner.report_output_handoff_rollback(wire.ReportOutputHandoffRollback(manifest, tombstone))
        assert reply.accepted, reply.error

    def abort(self, publication, scope):
        reply = self.owner.abort_owner_publication(ep.AbortOwnerPublication(publication, scope))
        assert reply.accepted, reply.error
        return reply.receipt

    def child(self, address, request):
        assert address == self.publisher.owner_address
        method = (self.publisher.prepare_stored_contained_pin if type(request) is protocol.PrepareStoredContainedPin
                  else self.publisher.promote_stored_contained_pin)
        return method(request)

    def rpc(self, address, handler, request):
        assert len(self.calls) < 160, "owner combination exceeded finite callback budget"
        self.calls.append((handler, request))
        if address == self.owner.gcs_address:
            assert handler == ep.PUBLICATION_HANDLER
            reply = self.authority.apply(request)
            if type(request) is self.lose:
                self.lose = None
                assert reply.accepted
                raise TimeoutError("actual GCS acknowledgement lost")
            return reply
        if address == self.publisher.owner_address:
            assert handler == "release_contained_reference"
            return self.publisher.release_contained_reference(request)
        assert address == self.owner.node_address
        if handler == "drop_object_replica":
            return self.node._handle_drop_object_replica(request)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert type(request) is wire.AckOutputPublicationAdopted
        identity = request.proof.complete.publication_id
        envelope = self.envelopes[identity]
        owner = self.owner.get_output_handoff(wire.GetOutputHandoff(identity)).snapshot
        assert owner.adoption == request.proof
        assert self.owner.owner_table.output_owner_publication_receipt(
            OutputOwnerPublicationPlan(envelope.manifest.execution, envelope)).committed
        gcs = self.authority.query(ep.GetPublication(ep.PublicationRef(identity, envelope.manifest.manifest_digest))).snapshot
        assert gcs.adoption == request.proof
        assert request.gcs_adoption == gcs.receipt(ep.PublicationStage.ADOPTED)
        self.journal.retire_completed(request.proof)
        self.node_acks.append(request)
        return wire.AckOutputPublicationAdoptedReply(request, True)

    def leaf(self, value):
        reference = self.publisher.put(value)
        self.references.append(reference)
        return reference

    def submit(self):
        pending, reference = self.owner._register_submission(
            self.owner.define_remote_function(lambda: None), (), {}, ResourceVector({"CPU": 1}),
            max_retries=1, _enqueue=True)
        assert self.take() == (pending,)
        self.references.append(reference)
        return pending, reference

    def prepare(self, pending, child, *, stored=False):
        identity = OutputPublicationID(LeaseID((len(self.envelopes) + 1).to_bytes(16, "big")), pending.execution)
        session = OutputDiscoverySession(OutputPublicationHeader(
            identity, self.owner.job_id, self.publisher.worker_id, self.owner.worker_id, self.incarnation),
            inline_threshold=0 if stored else 1024)
        outputs = session.discover((child, "payload"))
        assert outputs.manifest.value.size_bytes < 1024
        self.adapter.prepare(outputs.manifest, outputs.payload)
        session.release_sources_after_promotions()
        envelope = self.adapter.complete(identity, commit_lease=self.completions.append)
        self.envelopes[identity] = envelope
        assert self.adapter.report_terminal(identity)
        return envelope

    def adopt(self, pending, envelope):
        return self.owner._drive_output_publication_adoption(pending,
            core_module._OutputAdoptionObligation(envelope, self.owner.node_id))

    def take(self):
        output = []
        fifo = self.owner._submissions
        size = fifo.qsize()
        assert size <= 16
        for _ in range(size):
            item = fifo.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) in (_PendingTask, _DelayedReadyTask)
                    output.append(item)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0
        return tuple(output)

    def resume(self, pending, envelope):
        retries = self.take()
        assert len(retries) == 1 and type(retries[0]) is _DelayedReadyTask
        ready = retries[0].ready
        assert ready.pending == pending and ready.output_adoption.envelope == envelope
        assert self.owner._drive_output_publication_adoption(pending, ready.output_adoption)

    def finish_attempt(self, pending):
        assert self.owner._finish_pending_task(pending)
        assert self.take() == ()

    def drain(self):
        for _ in range(3):
            self.owner._reference_mailbox.drain()
            self.publisher._reference_mailbox.drain()

    def finish(self):
        self.lose = None
        for reference in reversed(self.references):
            if not reference.closed:
                reference.close(timeout=0)
        self.drain()
        assert self.store.used_bytes == 0
        assert not self.adapter.pending_terminal_reports()
        assert all(snapshot.receipt(ep.PublicationStage.RETIRED) is not None for snapshot in self.authority.snapshots())
        for core in (self.owner, self.publisher):
            assert not core._object_gc_obligations and not core._protocol_unresolved
            close_pure_core(core)


@pytest.fixture
def runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("pure owner combination attempted runtime or blocking work")
    def set_only(event, timeout=None):
        assert event.is_set()
        return True
    for kind, method in ((CoreWorker, "__init__"), (NodeServer, "__init__"),
                         (threading.Thread, "start"), (threading.Timer, "__init__"),
                         (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", set_only)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    value = _Runtime()
    try:
        yield value
    finally:
        value.finish()


def test_w3_graph_commit_ack_unknown_owner_still_pending_then_exact_replay_adopts(runtime):
    r = runtime
    child = r.leaf(7)
    pending, outer = r.submit()
    envelope = r.prepare(pending, child)
    publication = ep.TaskPublication(envelope.manifest, r.owner.owner_address)
    r.lose = ep.CommitGraph
    assert not r.adopt(pending, envelope)
    graph = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    owner = r.owner.get_output_handoff(wire.GetOutputHandoff(envelope.publication_id)).snapshot
    assert graph.receipt(ep.PublicationStage.COMMITTED) is not None and graph.adoption is None
    assert owner.phase is OutputHandoffPhase.PENDING and owner.adoption is None
    assert r.owner.owner_table.snapshot(outer.object_id).state is ObjectState.PENDING
    assert r.journal.snapshot(envelope.publication_id).result_retained
    assert not r.node_acks and len(r.completions) == 1
    r.resume(pending, envelope)
    assert r.owner.owner_table.snapshot(outer.object_id).state is ObjectState.READY_INLINE
    assert len(r.completions) == len(r.node_acks) == 1
    r.finish_attempt(pending)


def test_w3_owner_ready_and_actual_adoption_survive_unknown_c7_ack_and_early_close(runtime):
    r = runtime
    child = r.leaf(8)
    pending, outer = r.submit()
    envelope = r.prepare(pending, child)
    publication = ep.TaskPublication(envelope.manifest, r.owner.owner_address)
    r.lose = ep.RecordAdoption
    assert not r.adopt(pending, envelope)
    owner = r.owner.get_output_handoff(wire.GetOutputHandoff(envelope.publication_id)).snapshot
    graph = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert owner.adoption is not None and graph.adoption == owner.adoption
    assert r.owner.owner_table.snapshot(outer.object_id).state is ObjectState.READY_INLINE
    assert not r.node_acks and r.journal.snapshot(envelope.publication_id).result_retained
    outer.close(timeout=0)
    r.drain()
    assert r.owner.owner_table.collection_state(outer.object_id) is ObjectCollectionState.ACTIVE
    assert r.authority.query(ep.GetPublication(publication.reference)).snapshot.graph_active
    r.resume(pending, envelope)
    r.finish_attempt(pending)
    r.drain()
    assert len(r.completions) == len(r.node_acks) == 1
    assert r.owner.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
    final = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert not final.graph_active and final.adoption == owner.adoption


def test_w4_real_reconstruction_retires_old_edges_before_new_attempt_and_gc_preserves_history(runtime):
    r = runtime
    old_child, new_child = r.leaf(10), r.leaf(11)
    pending, outer = r.submit()
    first = r.prepare(pending, old_child, stored=True)
    assert r.adopt(pending, first)
    r.finish_attempt(pending)
    old_publication = ep.TaskPublication(first.manifest, r.owner.owner_address)
    old_adoption = r.authority.query(ep.GetPublication(old_publication.reference)).snapshot.adoption
    old_transfer = first.manifest.value.transfers[0]
    assert old_transfer.final_hold in r.publisher.owner_table.snapshot(old_child.object_id).contained_holds
    assert r.owner.drop_object(outer)
    assert r.owner.owner_table.snapshot(outer.object_id).state is ObjectState.LOST
    outcome = r.owner._admit_owned_object_reconstruction(outer.object_id)
    assert outcome.disposition is ReconstructionDisposition.START
    next_pending, = r.take()
    assert type(next_pending) is _PendingTask and next_pending.spec.attempt_id == pending.spec.attempt_id.next()
    old = r.authority.query(ep.GetPublication(old_publication.reference)).snapshot
    assert not old.graph_active and old.receipt(ep.PublicationStage.RETIRED) is not None
    assert old_transfer.final_hold not in r.publisher.owner_table.snapshot(old_child.object_id).contained_holds
    second = r.prepare(next_pending, new_child)
    assert r.adopt(next_pending, second)
    r.finish_attempt(next_pending)
    new_publication = ep.TaskPublication(second.manifest, r.owner.owner_address)
    current = r.authority.query(ep.GetPublication(new_publication.reference)).snapshot
    new_transfer = second.manifest.value.transfers[0]
    late_requests = (ep.CommitGraph(old_publication.reference), ep.RecordAdoption(old_adoption), ep.RetireGraph(old.closed_holds))
    for request in late_requests:
        assert r.authority.apply(request).accepted
        assert r.authority.query(ep.GetPublication(new_publication.reference)).snapshot == current
    assert new_transfer.final_hold in r.publisher.owner_table.snapshot(new_child.object_id).contained_holds
    assert r.owner.owner_table.snapshot(outer.object_id).current_attempt == next_pending.spec.attempt_id
    outer.close(timeout=0)
    r.drain()
    assert r.owner.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
    assert new_transfer.final_hold not in r.publisher.owner_table.snapshot(new_child.object_id).contained_holds
    for request in late_requests:
        assert r.authority.apply(request).accepted
    assert not r.authority.query(ep.GetPublication(new_publication.reference)).snapshot.graph_active
    assert len(r.completions) == len(r.node_acks) == 2


def test_client_rejects_wrong_stage_receipt_from_an_actual_history(runtime):
    r = runtime
    child = r.leaf(12)
    publication = r.publisher._publication_client().current(child.object_id)
    actual = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    request = ep.CommitGraph(publication.reference, actual.prepared)
    reply = r.authority.apply(request)
    changed = replace(reply)
    object.__setattr__(changed, 'receipt', actual.receipt(ep.PublicationStage.PREPARED))
    client = PublicationClient(lambda handler, sent: changed)
    with pytest.raises((SystemTaskError, ValueError), match="stage"):
        client.call(request, ep.PublicationStage.COMMITTED)
