"""Bounded common cleanup composition with real enhanced metadata authority.

One Core submission, one child table, one 1 KiB store and at most one retry
per case. A real journal/adapter prepares and completes one 20-byte result;
its local Complete stays unavailable to the Core until explicitly delivered.
Member death remains an explicit boundary input, not a real process exit.
Actual owner/graph/child reducers and Node Seal/Drop handlers supply receipts.
No runtime constructors, user execution, sockets, threads, timers or sleeps.
"""

from dataclasses import replace
import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, core as core_module, enhanced_publication as ep, output_protocol as wire, protocol, transport
from miniray.enhanced_publication_control import EnhancedPublicationControl
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import CoreWorker, _OutputNodeLossObligation
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.output_publication_journal import OutputPublicationAdoptionProof, OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.owner_death_fence_registry import OwnerDeathFenceRegistry, OwnerFenceNodeIncarnation
from miniray.ownership import ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations = []

    def forbidden(*args, **kwargs):
        violations.append(args[:2])
        pytest.fail("common cleanup composition attempted runtime work")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (NodeServer, "__init__"), (WorkerServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "__init__"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"), (queue.Queue, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield
    assert not violations


def _id(kind, value):
    return kind(bytes((value,)) * 16)


def _forbidden_rpc(*args, **kwargs):
    pytest.fail("unexpected cleanup RPC")


class _CorePublication:
    def __init__(self, *, stored=False):
        self.core = make_pure_core()
        definition = self.core.define_remote_function(lambda: None)
        self.pending, self.ref = self.core._register_submission(
            definition, (), {}, ResourceVector(), num_returns=1, max_retries=1,
            _enqueue=True,
        )
        # Use the real admission transaction, then let this synchronous fixture
        # own the accepted queue item without executing user code.
        assert self.core._submissions.get_nowait() == self.pending
        self.core._submissions.task_done()
        assert self.core._accepted_task_count == 1
        assert self.core._task_finish_barriers[self.ref.object_id] == self.pending
        self.child_owner = _id(WorkerID, 41)
        self.publisher = _id(NodeID, 42)
        self.child = ObjectID.for_task(_id(TaskID, 43))
        self.transfer = PreparedContainedTransfer(
            self.child, self.child_owner, ("child.invalid", 1234),
            OwnedContainedSource(self.child_owner),
            ContainedReferenceHold(self.ref.object_id, self.child_owner, "common-cleanup"),
            ContainedReferenceHold(self.ref.object_id, self.core.worker_id, "common-cleanup"),
        )
        payload = b"one published result"
        tier = protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE
        (self.value) = (OutputValue(tier, len(payload), hashlib.sha256(payload).hexdigest(), (self.transfer,)))
        self.identity = OutputPublicationID(_id(LeaseID, 44), self.pending.execution)
        self.manifest = OutputPublicationManifest.create(
            OutputPublicationHeader(
                self.identity, self.core.job_id, self.child_owner, self.core.worker_id,
                OutputPublicationNodeIncarnation(self.publisher, 4201, 1),
            ), (self.value),
        )
        self.core.gcs_address = ("publication.invalid", 1235)
        self.authority = self.core._test_publication_authority
        self.publication = ep.TaskPublication(self.manifest, self.core.owner_address)
        self.metadata_calls = []
        self.core._rpc = self.publication_rpc
        self.child_table = ObjectOwnerTable()
        self.child_table.register(self.child, local_token="child-source")
        self.child_table.publish_inline(self.child, None, b"child")
        self.node = node = object.__new__(NodeServer)
        node.node_id = self.publisher
        node._state_lock = threading.RLock()
        node._object_store = self.store = ObjectStore(1024)
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._object_localization_locks = {}
        self.journal = OutputPublicationJournal()
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register, report_complete=self.report_complete,
            report_rollback=_forbidden_rpc, publication_value=lambda manifest: ep.TaskPublication(manifest, self.core.owner_address),
            publication_rpc=lambda request: self.publication_rpc(self.core.gcs_address, ep.PUBLICATION_HANDLER, request),
            abort_owner=self.abort_owner, prepare_child=self.prepare_child, promote_child=self.promote_child,
            release_child=lambda address, request: self.release(address, "release_contained_reference", request),
            seal_replica=self.seal, drop_replica=lambda effect, request: node._handle_drop_object_replica(request),
        )
        self.local_completions = []
        self.adapter.prepare(self.manifest, payload)
        self.envelope = self.adapter.complete(self.identity, commit_lease=self.local_completions.append)
        self.complete = self.envelope.complete
        assert self.local_completions == [self.complete]
        assert self.authority.query(ep.GetPublication(self.publication.reference)).snapshot.complete is None
        assert self.core.get_output_handoff(wire.GetOutputHandoff(self.identity)).snapshot.complete is None
        self.node_death = protocol.NodeDeathRecord(
            "publisher-exited", self.publisher, 4201, 1, 1, 7,
            protocol.NodeDeathReason.PROCESS_EXIT, "explicit test member fact",
        )
        self.death = protocol.WorkerDeathRecord(
            "child-exited", protocol.WorkerIncarnation(
                _id(NodeID, 45), 4501, 1, self.child_owner, 4502,
            ), 1, 7, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        self.releases = []
        self.core._borrow_rpc = self.release

    def publication_rpc(self, address, handler, request):
        assert address == self.core.gcs_address and handler == ep.PUBLICATION_HANDLER
        assert not self.core._state_lock._is_owned()
        assert len(self.metadata_calls) < 48, "common cleanup exceeded finite metadata budget"
        self.metadata_calls.append(request)
        reply = self.authority.apply(request)
        assert type(reply) is ep.PublicationReply and reply.request == request
        assert reply.accepted, reply.error
        return reply

    def register(self, manifest):
        reply = self.core.register_output_handoff(wire.RegisterOutputHandoff(manifest))
        assert reply.accepted and reply.snapshot.manifest == manifest

    def report_complete(self, witness):
        reply = self.core.report_output_handoff_complete(wire.ReportOutputHandoffComplete(witness))
        assert type(reply) is wire.OutputHandoffCompleteAck and reply.accepted and reply.witness == witness

    def abort_owner(self, publication, scope):
        reply = self.core.abort_owner_publication(ep.AbortOwnerPublication(publication, scope))
        assert reply.accepted and reply.receipt is not None
        return reply.receipt

    def prepare_child(self, address, request):
        assert address == self.transfer.contained_owner_address
        disposition = self.child_table.prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        return protocol.StoredContainedPinReply(request, disposition)

    def promote_child(self, address, request):
        assert address == self.transfer.contained_owner_address
        disposition = self.child_table.promote_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        return protocol.StoredContainedPinReply(request, disposition)

    def seal(self, effect, descriptor, payload):
        request = protocol.SealObject.from_data(
            descriptor.object_id, effect.publication_id.attempt_id, descriptor.owner_worker_id, payload,
        )
        reply = self.node._handle_seal_object(request)
        assert reply.sealed and (reply.object_id, reply.node_id, reply.size_bytes, reply.checksum) == (
            descriptor.object_id, descriptor.node_id, descriptor.size_bytes, descriptor.checksum,
        )
        assert self.store.get(descriptor.object_id) == payload
        return descriptor

    def release(self, address, handler, request):
        assert len(self.releases) < 8
        assert address == self.transfer.contained_owner_address
        assert handler == "release_contained_reference"
        assert (request.object_id, request.owner_worker_id) == (self.child, self.child_owner)
        assert request.hold in (self.transfer.final_hold, self.transfer.provisional_hold)
        self.releases.append(request)
        released = self.child_table.release_contained_reference(request.object_id, request.hold)
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def install_death(self, death=None):
        supplied = self.death if death is None else death
        self.core.gcs_address = ("membership.invalid", 1234)

        def rpc(address, handler, request):
            if handler == ep.PUBLICATION_HANDLER:
                return self.publication_rpc(address, handler, request)
            assert address == self.core.gcs_address and handler == "get_worker_deaths"
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, supplied.death_epoch,
                (supplied,) if request.after_epoch == 0 else (),
            )

        self.core._rpc = rpc
        return self.core._sync_worker_deaths()

    def known_complete(self):
        assert self.adapter.report_terminal(self.identity)
        assert self.authority.query(ep.GetPublication(self.publication.reference)).snapshot.complete == self.complete

    def loss(self, *, envelope=None):
        return self.core._drive_output_node_loss(
            self.pending, _OutputNodeLossObligation(self.identity, self.node_death, envelope=envelope),
        )

    def close(self):
        self.ref.close()
        close_pure_core(self.core)


@pytest.fixture
def publication():
    value = _CorePublication()
    yield value
    value.close()


@pytest.mark.parametrize("reason", (
    protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT,
))
def test_membership_child_death_drains_core_node_loss_without_dead_rpc(publication, reason):
    f = publication
    f.known_complete()
    assert f.install_death(replace(f.death, reason=reason))
    f.core._borrow_rpc = _forbidden_rpc
    assert f.loss()
    state = f.core.owner_table.snapshot(f.ref.object_id)
    assert state.state is ObjectState.LOST and state.current_attempt == f.pending.spec.attempt_id
    assert state.local_tokens and state.producer_task_spec == f.pending.spec
    assert f.core._recovery.task_record(f.pending.task_id).state is TaskState.SUCCEEDED
    assert f.identity not in f.core._output_node_cleanup
    assert f.pending.task_key not in f.core._protocol_unresolved
    assert not f.releases
    central = f.authority.query(ep.GetPublication(f.publication.reference)).snapshot
    assert central.complete == f.complete and central.receipt(ep.PublicationStage.RETIRED) is not None


def test_membership_death_cache_detaches_input_and_rejects_conflicting_replay(publication, monkeypatch):
    f = publication
    f.known_complete()
    original = replace(f.death, incarnation=replace(f.death.incarnation))
    supplied = replace(original, incarnation=replace(original.incarnation))
    discharge = f.core._discharge_dead_owner_obligations
    failures = [True]

    def fail_once(death):
        if failures.pop() if failures else False:
            raise RuntimeError("interrupt after installing fence, before cursor commit")
        return discharge(death)

    monkeypatch.setattr(f.core, "_discharge_dead_owner_obligations", fail_once)
    assert not f.install_death(supplied)
    assert f.core._worker_death_cursor == 0
    retained = f.core._worker_death_records[f.child_owner]
    assert retained == original and retained.incarnation is not supplied.incarnation
    object.__setattr__(supplied.incarnation, "worker_pid", 9999)
    assert retained == original
    assert not f.install_death(supplied)
    assert f.core._worker_death_records[f.child_owner] == original
    assert f.core._worker_death_cursor == 0
    assert f.install_death(original)
    f.core._borrow_rpc = _forbidden_rpc
    assert f.loss()
    assert f.core.owner_table.snapshot(f.ref.object_id).state is ObjectState.LOST


@pytest.mark.parametrize("fact", ("missing", "timeout", "expected", "other-owner"))
def test_unproven_child_death_leaves_core_cleanup_pending(publication, fact):
    f = publication
    f.known_complete()
    if fact == "expected":
        assert f.install_death(replace(f.death, reason=protocol.WorkerDeathReason.EXPECTED))
    elif fact == "other-owner":
        assert f.install_death(replace(f.death, incarnation=replace(f.death.incarnation, worker_id=_id(WorkerID, 46))))
    elif fact == "timeout":
        f.core.gcs_address = ("membership.invalid", 1234)
        def unavailable_membership(address, handler, request):
            if handler == ep.PUBLICATION_HANDLER:
                return f.publication_rpc(address, handler, request)
            assert handler == "get_worker_deaths"
            raise TimeoutError("no member proof")
        f.core._rpc = unavailable_membership
        assert not f.core._sync_worker_deaths()
    attempts = []

    def timeout(address, handler, request):
        attempts.append(request)
        raise TimeoutError("live child release unavailable")

    f.core._borrow_rpc = timeout
    before = f.core.owner_table.snapshot(f.ref.object_id)
    assert not f.loss()
    assert len(attempts) == 1
    assert f.core.owner_table.snapshot(f.ref.object_id) == before
    assert f.identity in f.core._output_node_cleanup
    assert f.pending.task_key in f.core._protocol_unresolved
    assert f.child_owner not in getattr(f.core, "_worker_death_records", {})


def test_late_envelope_does_not_reverse_unknown_cleanup_or_consume_budget_twice(publication):
    f = publication
    first = [True]

    def release_then_lose_reply(address, handler, request):
        reply = f.release(address, handler, request)
        if first:
            first.pop()
            raise TimeoutError("first exact Release ACK lost")
        return reply

    f.core._borrow_rpc = release_then_lose_reply
    assert not f.loss()
    assert f.core._output_handoff_table().query(f.identity).phase is OutputHandoffPhase.ABORTED
    assert f.core._output_node_cleanup[f.identity].complete is None
    assert not f.loss(envelope=f.envelope)  # The real retry authority queues one new attempt.
    state = f.core.owner_table.snapshot(f.ref.object_id)
    record = f.core._recovery.task_record(f.pending.task_id)
    assert state.state is ObjectState.PENDING and state.current_attempt == f.pending.spec.attempt_id.next()
    assert record.retries_started == 1 and record.state is not TaskState.SUCCEEDED
    assert not f.child_table.snapshot(f.child).contained_holds
    assert f.core._output_handoff_table().query(f.identity).complete is None
    assert f.identity not in f.core._output_node_cleanup
    assert f.pending.task_key not in f.core._protocol_unresolved
    calls = len(f.releases)
    assert f.loss(envelope=f.envelope)
    assert len(f.releases) == calls and record.retries_started == 1
    assert f.core.owner_table.snapshot(f.ref.object_id) == state
    central = f.authority.query(ep.GetPublication(f.publication.reference)).snapshot
    assert central.complete is None and central.adoption is None and not central.graph_active
    assert central.receipt(ep.PublicationStage.RETIRED) is not None


def test_known_inline_envelope_keeps_actual_bytes_and_child_holds(publication):
    f = publication
    before = f.child_table.snapshot(f.child)
    f.core._borrow_rpc = _forbidden_rpc
    assert f.loss(envelope=f.envelope)
    state = f.core.owner_table.snapshot(f.ref.object_id)
    assert state.state is ObjectState.READY_INLINE and state.inline_data == (f.envelope.result).inline_data
    assert f.child_table.snapshot(f.child) == before
    assert f.core._recovery.task_record(f.pending.task_id).retries_started == 0
    central = f.authority.query(ep.GetPublication(f.publication.reference)).snapshot
    assert central.adoption.complete == f.complete and central.graph_active


def test_dead_child_retirement_blocks_reconstruction_until_replica_ack():
    f = _CorePublication(stored=True)
    try:
        f.known_complete()
        f.core._publication_client().commit_task(f.publication, f.complete)
        assert f.core.owner_table.commit_output_publication(
            OutputOwnerPublicationPlan(f.pending.execution, f.envelope),
        ).committed
        proof = OutputPublicationAdoptionProof(
            f.complete, f.core.worker_id,
            "output-owner:{}:{}".format(f.identity.transaction_id, f.manifest.manifest_digest),
        )
        f.core._output_handoff_table().adopt(proof)
        f.core._publication_client().adopt(proof)
        f.core._recovery.record_task_success(f.pending.task_id, f.pending.spec.attempt_id)
        assert f.core._finish_pending_task(f.pending)
        assert f.core.owner_table.mark_lost(f.ref.object_id, f.pending.spec.attempt_id)
        assert f.install_death()
        f.core._borrow_rpc = _forbidden_rpc
        first, drops = [True], []
        f.core._resolve_node_address = lambda node_id, *, home_route=None: ("replica.invalid", 1234)

        def replica_rpc(address, handler, request):
            if handler == ep.PUBLICATION_HANDLER:
                return f.publication_rpc(address, handler, request)
            assert address == ("replica.invalid", 1234) and handler == "drop_object_replica"
            assert request.object_id == f.ref.object_id and request.node_id == f.publisher
            drops.append(request)
            reply = f.node._handle_drop_object_replica(request)
            if first:
                first.pop()
                assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
                raise TimeoutError("replica effect acknowledgement unknown")
            assert reply.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
            return reply

        f.core._rpc = replica_rpc
        waiter = f.core._objects[f.ref.object_id]
        assert f.core._start_or_join_reconstruction(f.ref.object_id, waiter, return_requested_outcome=True) is None
        assert f.core.owner_table.snapshot(f.ref.object_id).current_attempt == f.pending.spec.attempt_id
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 0
        assert f.core.owner_table.has_active_output_retirements()
        outcome = f.core._start_or_join_reconstruction(f.ref.object_id, waiter, return_requested_outcome=True)
        assert outcome.disposition is ReconstructionDisposition.START
        assert f.core.owner_table.snapshot(f.ref.object_id).current_attempt == f.pending.spec.attempt_id.next()
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 1
        assert not f.core.owner_table.has_active_output_retirements()
        assert not f.core._output_retirement_work and not f.releases
        assert len(drops) == 2 and drops[0] == drops[1]
        assert f.store.used_bytes == 0
        central = f.authority.query(ep.GetPublication(f.publication.reference)).snapshot
        assert central.receipt(ep.PublicationStage.RETIRED) is not None and not central.graph_active
    finally:
        f.close()


@pytest.mark.parametrize("blocked", ("pinned", "timeout"))
def test_owner_fence_driver_rotates_past_blocked_target_and_survives_shrink(blocked):
    service = object.__new__(control.GCSLite)
    service._owner_death_control_lock = threading.RLock()
    service.owner_death_fences = OwnerDeathFenceRegistry()
    service._owner_fence_progress_cursor = service._owner_death_progress_cursor = 0
    service.nodes = control.NodeRegistry()
    service.workers = control.WorkerRegistry(service.nodes)
    service.publication_control = EnhancedPublicationControl(
        nodes=service.nodes, workers=service.workers, owner_fences=service.owner_death_fences,
        lock=service._owner_death_control_lock, rpc=_forbidden_rpc,
    )
    assert not service.publication_control.pending_deaths()
    assert service.publication_control.authority.snapshots() == ()
    targets = tuple(OwnerFenceNodeIncarnation(_id(NodeID, value), 6000 + value, 1) for value in (61, 62))
    for target in targets:
        service.owner_death_fences.register_node(target)
    death = protocol.WorkerDeathRecord(
        "fence-owner-exited", protocol.WorkerIncarnation(_id(NodeID, 63), 6301, 1, _id(WorkerID, 64), 6302),
        1, 7, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    effects = service.owner_death_fences.commit_owner_death(death)
    first, second = effects
    attempts, complete_replies = [], []
    mode = [blocked]

    def rpc(node_id, handler, request):
        assert not service._owner_death_control_lock._is_owned()
        assert handler == "install_owner_death_fence"
        attempts.append(request)
        assert len(attempts) <= 4
        if request == first.request:
            if mode[0] == "timeout":
                raise TimeoutError("first target unavailable")
            if mode[0] == "pinned":
                obj = ObjectID.for_task(_id(TaskID, 65))
                descriptor = protocol.ObjectStoreDescriptor(obj, death.worker_id, AttemptID(obj.task_id, 0), node_id, 1, hashlib.sha256(b"x").hexdigest())
                return protocol.InstallOwnerDeathFenceReply(request, protocol.OwnerDeathFenceDisposition.FENCED, (
                    protocol.OwnerDeathReplicaObservation(descriptor, protocol.OwnerDeathReplicaStatus.PINNED, 1),
                ))
            if mode[0] == "wrong-ack":
                return complete_replies[0]  # Second target's valid old ACK cannot retire first.
        reply = protocol.InstallOwnerDeathFenceReply(request, protocol.OwnerDeathFenceDisposition.FENCED)
        complete_replies.append(reply)
        return reply

    service._owner_fence_node_rpc = rpc
    assert not service._drive_owner_death_fence_once()
    assert service._drive_owner_death_fence_once()
    assert attempts == [first.request, second.request]
    assert service.owner_death_fences.pending() == (first,)
    mode[0] = "wrong-ack"
    assert not service._drive_owner_death_fence_once()
    assert service.owner_death_fences.pending() == (first,)
    mode[0] = "complete"
    assert service._drive_owner_death_fence_once()
    assert not service.owner_death_fences.pending()
    assert not service._drive_owner_death_fence_once()
    assert len(attempts) == 4
