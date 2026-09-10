"""Pure logical-finish barriers across ordinary task success and replay.

Real Core methods, owner/recovery tables, and FIFO admission are composed
synchronously.  No Core constructor, mailbox, thread, process, socket, or
wall-clock wait is started.  Interleaving hooks model the finalizer races.
Successful results use real OutputDiscovery, a Node publication adapter and
journal, one real GCS publication authority, actual Core owner handoffs, and
real Node prepare/Complete/retirement/storage handlers. Per case:
at most three logical tasks, six publications, one output per task, 128 bytes
per serialized output and one 4 KiB in-memory store. No output contains refs.
"""

from __future__ import annotations

import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import core as core_module, enhanced_publication as ep, output_protocol as wire, protocol
from miniray.core import _HomeRoute
from miniray.core import (
    CoreWorker, ObjectRef, RemoteFunctionDefinition, _ForeignDependencyGuard,
    _PendingTask,
)
from miniray.control import NodeRegistry
from miniray.errors import SystemTaskError
from miniray.foreign_lineage_runtime import (
    ForeignLineageRenewalDisposition, ForeignLineageRenewalResult,
)
from miniray.ids import JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseRecord, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.ownership import (
    ObjectCollectionState, ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan,
)
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector
from miniray.trace import MemoryEventSink
from miniray.transport import TransportTimeout
from tests.unit._pure_output_runtime import _metadata as _assert_metadata


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("finish-barrier test attempted actual runtime work")

    for owner, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)


class _Composition:
    """Reentrant synchronous composition/condition with no blocking wait."""

    def __init__(self) -> None:
        self.depth = 0
        self.notifications = 0
        self.waits = 0
        self.before_enter = None

    def __enter__(self):
        callback = self.before_enter
        self.before_enter = None
        if callback is not None:
            callback()
        self.depth += 1
        return self

    def __exit__(self, *_exc):
        self.depth -= 1
        assert self.depth >= 0

    def notify_all(self) -> None:
        assert self.depth > 0
        self.notifications += 1

    def wait(self, _timeout=None) -> None:
        self.waits += 1
        pytest.fail("the deterministic finish interleave must not wait")


class _OutputBackend:
    """Actual owner handoffs and bounded Node storage, with no transport."""

    def __init__(self, core, no_rpc):
        self.core, self.no_rpc = core, no_rpc
        self.journal = OutputPublicationJournal()
        self.authority = getattr(core, "_test_publication_authority", None)
        if self.authority is None:
            self.authority = core._test_publication_authority = ep.PublicationAuthority()
        self.nodes = getattr(core, "_test_publication_nodes", None)
        if self.nodes is None:
            self.nodes = core._test_publication_nodes = NodeRegistry()
            assert self.nodes.register(core.node_id, core.node_address, ResourceVector({"CPU": 1}), node_pid=3001)
        registered = self.nodes.get_state_reply(protocol.GetNodeState(core.node_id))
        assert registered.found and registered.node_pid is not None
        self.incarnation = OutputPublicationNodeIncarnation(
            core.node_id, registered.node_pid, registered.registration_epoch)
        self.store = ObjectStore(4096)
        self.ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        self.completed = {}
        self.envelopes = {}
        self.calls, self.gcs_calls = [], []
        self.executor = WorkerID(bytes.fromhex("ef" * 16))
        node = object.__new__(NodeServer)
        self.node = node
        node.node_id = core.node_id
        node._node_pid, node._registration_epoch = self.incarnation.node_pid, self.incarnation.registration_epoch
        node._state_lock = threading.RLock()
        node._object_store = self.store
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata = {}
        node._dropped_metadata = {}
        node._local_replica_write_claims = {}
        node._object_localization_locks = {}
        node._owner_death_fences = {}
        node._output_publication_journal = self.journal
        node._ledger = self.ledger
        node._cluster_nodes = (NodeSnapshot(node.node_id, ResourceVector({"CPU": 1}), ResourceVector()),)
        node._leases = {}
        node._workers = {self.executor: _WorkerSlot(self.executor)}
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self._register_owner,
            report_complete=self._report_complete,
            report_rollback=self._report_rollback,
            publication_value=lambda manifest: ep.TaskPublication(manifest, core.owner_address),
            publication_rpc=lambda request: self.rpc(core.gcs_address, ep.PUBLICATION_HANDLER, request),
            abort_owner=self._abort_owner,
            prepare_child=no_rpc, promote_child=no_rpc, release_child=no_rpc,
            seal_replica=node._seal_output_publication_replica,
            drop_replica=node._drop_output_publication_replica,
        )
        node._output_publications = self.adapter

    def address(self, node_id, *, home_route=None):
        if node_id != self.core.node_id:
            self.no_rpc("unexpected Node route", node_id)
        return self.core.node_address

    def _owner_call(self, request, method):
        _assert_metadata(request)
        reply = method(request)
        assert type(reply) is wire.OutputHandoffReply
        assert reply.request == request and reply.accepted, reply.error
        _assert_metadata(reply)
        return reply.snapshot

    def _register_owner(self, manifest):
        snapshot = self._owner_call(wire.RegisterOutputHandoff(manifest), self.core.register_output_handoff)
        assert snapshot.manifest == manifest and snapshot.complete is None

    def _report_complete(self, witness):
        assert self.journal.snapshot(witness.publication_id).complete == witness
        terminal = self.publication_snapshot(witness.publication_id)
        assert terminal.complete == witness
        assert terminal.receipt(ep.PublicationStage.TERMINAL) is not None
        assert terminal.receipt(ep.PublicationStage.COMMITTED) is None
        request = wire.ReportOutputHandoffComplete(witness)
        _assert_metadata(request)
        reply = self.core.report_output_handoff_complete(request)
        assert type(reply) is wire.OutputHandoffCompleteAck and reply.accepted, reply.error
        assert reply.witness == witness
        _assert_metadata(reply)
        assert self.core.owner_table.snapshot(witness.publication_id.object_id).state is ObjectState.PENDING

    def _report_rollback(self, tombstone, *, manifest):
        snapshot = self._owner_call(wire.ReportOutputHandoffRollback(manifest, tombstone),
                                   self.core.report_output_handoff_rollback)
        assert snapshot.manifest == manifest and snapshot.adoption is None

    def _abort_owner(self, publication, scope):
        request = ep.AbortOwnerPublication(publication, scope)
        _assert_metadata(request)
        reply = self.core.abort_owner_publication(request)
        assert type(reply) is ep.AbortOwnerPublicationReply
        assert reply.request == request and reply.accepted, reply.error
        assert reply.receipt is not None
        _assert_metadata(reply)
        return reply.receipt

    def handoff_snapshot(self, identity):
        snapshot = self._owner_call(wire.GetOutputHandoff(identity), self.core.get_output_handoff)
        assert snapshot.publication_id == identity
        return snapshot

    def publication_snapshot(self, identity):
        manifest = self.journal.snapshot(identity).manifest
        request = ep.GetPublication(ep.PublicationRef(identity, manifest.manifest_digest))
        reply = self.authority.query(request)
        assert reply.accepted and reply.snapshot is not None
        _assert_metadata(reply)
        return reply.snapshot

    def rpc(self, address, handler, request):
        lock = self.core._state_lock
        assert lock.depth == 0 if isinstance(lock, _Composition) else not lock._is_owned()
        if address == self.core.gcs_address:
            if handler == "get_node_state":
                assert type(request) is protocol.GetNodeState
                return self.nodes.get_state_reply(request)
            assert handler == ep.PUBLICATION_HANDLER
            assert len(self.gcs_calls) < 128
            _assert_metadata(request)
            reply = self.authority.apply(request)
            assert type(reply) is ep.PublicationReply and reply.request == request
            _assert_metadata(reply)
            self.gcs_calls.append((request, reply))
            return reply
        self.calls.append((handler, request))
        assert len(self.calls) <= 32
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            assert address == self.core.node_address
            assert type(request) is wire.AckOutputPublicationAdopted
            identity = request.proof.complete.publication_id
            assert self.completed[identity] == request.proof.complete
            assert self.handoff_snapshot(identity).adoption == request.proof
            envelope = self.envelopes[identity]
            owner = self.core.owner_table.output_owner_publication_receipt(
                OutputOwnerPublicationPlan(envelope.manifest.execution, envelope))
            assert owner is not None and owner.committed
            gcs = self.publication_snapshot(identity)
            assert gcs.adoption == request.proof
            assert gcs.receipt(ep.PublicationStage.COMMITTED) is not None
            assert request.gcs_adoption == gcs.receipt(ep.PublicationStage.ADOPTED)
            assert self.journal.snapshot(identity).result_retained
            reply = self.node._handle_ack_output_publication_adopted(request)
            assert type(reply) is wire.AckOutputPublicationAdoptedReply
            assert reply.request == request and reply.accepted
            assert not self.journal.snapshot(identity).result_retained
            return reply
        if handler == "drop_object_replica":
            assert address == self.core.node_address
            return self.node._handle_drop_object_replica(request)
        return self.no_rpc(handler, request)

    def succeed(self, pending, *, stored, value):
        assert len(self.completed) < 6 and len(pending.output_ids) == 1
        identity = OutputPublicationID(
            LeaseID((len(self.completed) + 1).to_bytes(16, "big")), pending.execution,
        )
        header = OutputPublicationHeader(
            identity, self.core.job_id, self.executor, self.core.worker_id,
            self.incarnation,
        )
        discovery = OutputDiscoverySession(header, inline_threshold=0 if stored else 128)
        outputs = discovery.discover(value)
        assert (len(outputs.payload) <= 128)
        assert (not outputs.manifest.value.transfers)
        _assert_metadata(outputs.manifest)
        assert discovery.source_references == ()
        token = AllocationToken("finish-lease-{}".format(len(self.completed)))
        self.ledger.allocate(ResourceVector({"CPU": 1}), token)
        lease = protocol.RequestWorkerLease(
            identity.lease_id, pending.task_id, pending.spec.attempt_id,
            ResourceVector({"CPU": 1}), self.core.node_id, self.core.worker_id,
            return_ids=pending.output_ids, requester_owner_address=self.core.owner_address,
        )
        grant = protocol.GrantWorkerLease(
            identity.lease_id, pending.task_id, pending.spec.attempt_id,
            self.core.node_id, self.executor, ("worker.invalid", 1), token,
        )
        record = _LeaseRecord(lease, token, grant, state=protocol.LeaseExecutionState.RUNNING)
        self.node._leases[identity.lease_id] = record
        slot = self.node._workers[self.executor]
        assert slot.active_lease_id is None
        slot.active_lease_id = identity.lease_id
        prepared = self.node._handle_prepare_output_publication(
            wire.PrepareOutputPublication(outputs.manifest, outputs.payload))
        assert prepared.accepted and record.output_publication_id == identity
        armed = self.publication_snapshot(identity)
        assert tuple(receipt.stage for receipt in armed.receipts) == (
            ep.PublicationStage.INTENT, ep.PublicationStage.PREPARED, ep.PublicationStage.ARMED,
        )
        assert armed.prepared == self.journal.preparation_receipt(identity)
        assert armed.complete is None and armed.adoption is None
        assert self.journal.snapshot(identity).complete is None
        discovery.release_sources_after_promotions()
        complete = protocol.CompleteWorkerLease(
            identity.lease_id, pending.task_id, pending.spec.attempt_id, self.executor,
            protocol.TaskReplyStatus.SUCCEEDED,
        )
        completed = self.node._handle_complete_output_worker_lease(complete, identity)
        assert completed.accepted and completed.released
        envelope = completed.output_publication
        assert envelope is not None and envelope.complete == self.journal.snapshot(identity).complete
        assert record.state is protocol.LeaseExecutionState.COMPLETED and record.completion == complete
        assert slot.active_lease_id is None
        assert identity not in self.completed
        self.completed[identity] = envelope.complete
        self.envelopes[identity] = envelope
        replay = self.node._handle_complete_output_worker_lease(complete, identity)
        assert replay.accepted and not replay.released and replay.output_publication == envelope
        assert self.ledger.available == ResourceVector({"CPU": 1})
        # C3 releases the physical lease while GCS still has only C1/ARM.
        assert self.publication_snapshot(identity) == armed
        assert self.handoff_snapshot(identity).complete is None
        assert self.core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert self.journal.snapshot(identity).result_retained
        assert self.adapter.report_terminal(identity)
        handoff = self.handoff_snapshot(identity)
        _assert_metadata(handoff)
        assert handoff.complete == envelope.complete and handoff.phase is OutputHandoffPhase.PENDING
        terminal = self.publication_snapshot(identity)
        assert terminal.complete == envelope.complete
        assert terminal.receipt(ep.PublicationStage.TERMINAL) is not None
        assert terminal.receipt(ep.PublicationStage.COMMITTED) is None
        assert terminal.adoption is None
        reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, self.executor,
            protocol.TaskReplyStatus.SUCCEEDED, ((envelope.result,)),
            output_publication=envelope,
        )
        assert self.core._publish_reply(
            pending, reply, expected_node_id=self.core.node_id,
            expected_lease_id=identity.lease_id,
        )
        assert not self.journal.snapshot(identity).result_retained
        assert self.handoff_snapshot(identity).adoption.complete == envelope.complete
        adopted = self.publication_snapshot(identity)
        assert adopted.adoption == self.handoff_snapshot(identity).adoption
        assert adopted.receipt(ep.PublicationStage.COMMITTED) is not None
        assert adopted.receipt(ep.PublicationStage.ADOPTED) is not None
        assert adopted.receipt(ep.PublicationStage.RETIRED) is None
        assert adopted.graph_active
        assert self.core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        return ((envelope.result,))

    def lose(self, pending):
        for object_id in pending.output_ids:
            owner = self.core.owner_table.snapshot(object_id)
            descriptor = owner.canonical_stored_result
            assert owner.state is ObjectState.READY_STORED and descriptor is not None
            assert owner.output_publication.publication_id.execution == pending.execution
            drop = protocol.DropObjectReplica(
                object_id, pending.spec.attempt_id, self.core.worker_id,
                self.core.node_id, descriptor.checksum,
            )
            reply = self.node._handle_drop_object_replica(drop)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert not self.store.contains(object_id, sealed_only=False)
            assert self.core.owner_table.mark_lost(object_id, pending.spec.attempt_id)
            self.core._stored_descriptors.pop(object_id, None)
        assert self.core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED


class _Fixture:
    def __init__(self) -> None:
        core = object.__new__(CoreWorker)
        self.core = core
        core.job_id = JobID.random()
        core.driver_task_id = TaskID.for_driver(core.job_id)
        core.worker_id = WorkerID.random()
        core.node_id = NodeID.random()
        core.node_address = ("127.0.0.1", 39931)
        core.owner_address = ("127.0.0.1", 39932)
        core.gcs_address = ("127.0.0.1", 39930)
        core.event_sink = MemoryEventSink()
        core._state_lock = _Composition()
        core._membership_epoch = 0
        core._installed_cluster_snapshot = None
        core._home_route = _HomeRoute(core.node_id, core.node_address, 0)
        core._completion = core._state_lock
        core._owner_table = ObjectOwnerTable()
        core._recovery = RecoveryManager()
        core._objects = {}
        core._stored_descriptors = {}
        core._submissions = queue.Queue()
        core._ready_tasks = queue.Queue()
        core._protocol_unresolved = {}
        core._object_gc_obligations = {}
        core._dead_nodes = {}
        core._task_finish_barriers = {}
        core._accepting = True
        core._owner_protocol_open = True
        core._owner_retain_admission_open = True
        core._inflight_submissions = 0
        core._inflight_puts = 0
        core._inflight_borrow_ops = 0
        core._accepted_task_count = 0
        core._submission_index = 0
        core._reference_index = 0
        core._finished_tasks = set()
        core._finishing_tasks = set()
        core._active_task_finishes = set()
        self.gc_checks = []
        core._enqueue_inline_gc_check = self.gc_checks.append
        core._loads_owned_value = cloudpickle.loads

        def no_rpc(*_args, **_kwargs):
            pytest.fail("finish-barrier fixture must never contact a runtime")

        self.backend = _OutputBackend(core, no_rpc)
        core._rpc = self.backend.rpc
        core._borrow_rpc = no_rpc
        core._resolve_node_address = self.backend.address

        def prepare_refs(object_ids):
            # Keep public handles inert: real reference finalizers would create
            # a mailbox thread.  Owner token installation stays in the actual
            # _register_submission admission transaction.
            prepared = []
            for object_id in object_ids:
                token = ("test-local", core._reference_index)
                core._reference_index += 1
                prepared.append((
                    ObjectRef(object_id, core.worker_id, core.owner_address), token
                ))
            return tuple(prepared)

        core._prepare_local_object_refs = prepare_refs
        core._bind_prepared_local_object_refs = (
            lambda prepared: tuple(ref for ref, _token in prepared)
        )
        key = protocol.FunctionKey(core.job_id, __name__, "producer", "v1")
        self.definition = RemoteFunctionDefinition(
            key, protocol.FunctionDefinition.from_payload(key, b"unused-function")
        )

    def submit(self, *refs: ObjectRef, num_returns: int = 1):
        pending, output = self.core._register_submission(
            self.definition, tuple(refs), {}, ResourceVector({"CPU": 1}),
            max_retries=3, num_returns=num_returns, _enqueue=True,
        )
        assert self.core._submissions.get_nowait() is pending
        self.core._submissions.task_done()
        assert all(
            self.core._task_finish_barriers[object_id] is pending
            for object_id in pending.output_ids
        )
        return pending, output

    def succeed(
        self, pending: _PendingTask, *, stored: bool = False, value: object = 7,
    ) -> tuple[protocol.ResultDescriptor, ...]:
        return self.backend.succeed(pending, stored=stored, value=value)

    def lose(self, pending: _PendingTask) -> None:
        self.backend.lose(pending)

    def close_refs(self, pending: _PendingTask) -> None:
        for object_id in pending.output_ids:
            for token in self.core.owner_table.snapshot(object_id).local_tokens:
                assert self.core.owner_table.release_local_reference(object_id, token)

    def queued(self) -> tuple[_PendingTask, ...]:
        values = []
        size = self.core._submissions.qsize()
        assert size <= 16
        for _ in range(size):
            item = self.core._submissions.get_nowait()
            self.core._submissions.task_done()
            if isinstance(item, _PendingTask):
                values.append(item)
        return tuple(values)

    def ready_dependency(self):
        pending, ref = self.submit()
        self.succeed(pending)
        assert self.core._finish_pending_task(pending)
        assert not self.queued()
        return pending, ref


def test_ordinary_stored_loss_waits_for_old_finalizer_before_get_reconstruction():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    pending, ref = fixture.submit(dependency_ref)
    old_hold = pending.dependency_hold
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({old_hold})
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    before = core.owner_table.snapshot(pending.object_id)
    recovery_before = replace(core._recovery.task_record(pending.task_id))

    with pytest.raises(TimeoutError, match="retirement did not finish"):
        core.get(ref, timeout=0)
    assert core._start_or_join_reconstruction(
        pending.object_id, core._objects[pending.object_id]
    ) is None
    assert core.owner_table.snapshot(pending.object_id) == before
    assert core._recovery.task_record(pending.task_id) == recovery_before
    assert core._accepted_task_count == 1
    assert not fixture.queued()

    assert core._finish_pending_task(pending)
    assert core._accepted_task_count == 0
    assert pending.object_id not in core._task_finish_barriers
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset()
    assert pending.object_id in fixture.gc_checks
    assert not fixture.queued()

    # A real get now admits one reconstruction, then timeout=0 exits at its
    # new PENDING state without blocking or executing the queued task.
    with pytest.raises(TimeoutError, match="was not ready before timeout"):
        core.get(ref, timeout=0)
    (reconstructed,) = fixture.queued()
    assert reconstructed.spec.attempt_id == pending.spec.attempt_id.next()
    assert reconstructed.dependency_hold != old_hold
    assert core._task_finish_barriers[pending.object_id] is reconstructed
    assert core._accepted_task_count == 1
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({
        reconstructed.dependency_hold
    })
    assert not core._finish_pending_task(pending)
    assert core._accepted_task_count == 1
    assert core._task_finish_barriers[pending.object_id] is reconstructed

    fixture.succeed(reconstructed)
    assert core.get(ref, timeout=0) == 7
    assert core._finish_pending_task(reconstructed)
    assert core._finish_pending_task(reconstructed)
    assert core._accepted_task_count == 0
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    assert not core._task_finish_barriers


def test_last_reference_gc_cannot_erase_output_before_its_finalizer():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    pending, _ref = fixture.submit(dependency_ref)
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    fixture.close_refs(pending)
    assert not core.owner_table.snapshot(pending.object_id).is_live
    before = core.owner_table.snapshot(pending.object_id)

    core._reference_released(pending.object_id)

    assert core.owner_table.snapshot(pending.object_id) == before
    assert not core._object_gc_obligations
    assert core._recovery.lineage_for_object(pending.object_id) is not None
    assert core._finish_pending_task(pending)
    core._reference_released(pending.object_id)
    assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
    assert pending.object_id not in core._objects
    assert core._recovery.lineage_for_object(pending.object_id) is None
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    assert not core.owner_table.snapshot(dependency.object_id).lineage_tokens
    assert core._accepted_task_count == 0
    assert not core._object_gc_obligations


def test_recursive_loss_preflights_every_finish_gate_before_any_reconstruction():
    fixture = _Fixture()
    core = fixture.core
    first, first_ref = fixture.submit()
    fixture.succeed(first, stored=True)
    assert core._finish_pending_task(first)
    assert not fixture.queued()
    second, second_ref = fixture.submit()
    fixture.succeed(second, stored=True)
    root, root_ref = fixture.submit(first_ref, second_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root)
    assert not fixture.queued()
    for item in (first, second, root):
        fixture.lose(item)
    before = {item.object_id: core.owner_table.snapshot(item.object_id) for item in (first, second, root)}
    records = {item.task_id: replace(core._recovery.task_record(item.task_id)) for item in (first, second, root)}

    # first precedes the blocked second in dependency order.  A per-node gate
    # inside the commit loop would incorrectly start first before noticing it.
    assert core._start_or_join_reconstruction(
        root.object_id, core._objects[root.object_id]
    ) is None
    assert not fixture.queued()
    assert core._accepted_task_count == 1
    for item in (first, second, root):
        assert core.owner_table.snapshot(item.object_id) == before[item.object_id]
        assert core._recovery.task_record(item.task_id) == records[item.task_id]
        assert core._recovery.active_recovery(item.task_id) is None
    assert not core._reconstruction._sessions

    assert core._finish_pending_task(second)
    assert not fixture.queued()
    outcome = core._start_or_join_reconstruction(
        root.object_id, core._objects[root.object_id], return_requested_outcome=True
    )
    assert outcome.disposition is ReconstructionDisposition.START
    reconstructed = fixture.queued()
    assert tuple(item.task_id for item in reconstructed) == (first.task_id, second.task_id, root.task_id)
    assert core._accepted_task_count == 3
    for item in reconstructed:
        assert core._task_finish_barriers[item.object_id] is item
        assert core._recovery.task_record(item.task_id).retries_started == 1
    for item in reconstructed:
        fixture.succeed(item)
        assert core._finish_pending_task(item)
    assert core.get(root_ref, timeout=0) == 7
    assert core._accepted_task_count == 0
    assert not core._task_finish_barriers


def test_pending_join_with_its_own_finish_barrier_does_not_block_parent():
    fixture = _Fixture()
    core = fixture.core
    child, child_ref = fixture.submit()
    fixture.succeed(child, stored=True)
    assert core._finish_pending_task(child)
    assert not fixture.queued()
    root, _root_ref = fixture.submit(child_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root)
    assert not fixture.queued()
    fixture.lose(child)
    fixture.lose(root)

    started = core._start_or_join_reconstruction(
        child.object_id, core._objects[child.object_id], return_requested_outcome=True
    )
    assert started.disposition is ReconstructionDisposition.START
    (child_retry,) = fixture.queued()
    assert core._task_finish_barriers[child.object_id] is child_retry
    joined = core._start_or_join_reconstruction(
        child.object_id, core._objects[child.object_id], return_requested_outcome=True
    )
    assert joined.disposition is ReconstructionDisposition.JOIN
    assert core._accepted_task_count == 1 and not fixture.queued()

    graph = core._reconstruction.preflight_graph(root.object_id)
    assert graph.node_for(child.object_id).action.value == "PENDING_JOIN"
    root_start = core._start_or_join_reconstruction(
        root.object_id, core._objects[root.object_id], return_requested_outcome=True
    )
    assert root_start.disposition is ReconstructionDisposition.START
    (root_retry,) = fixture.queued()
    assert root_retry.task_id == root.task_id
    assert core._accepted_task_count == 2
    assert core._task_finish_barriers[child.object_id] is child_retry
    assert core._task_finish_barriers[root.object_id] is root_retry
    assert core.owner_table.snapshot(child.object_id).submitted_tokens == frozenset({
        root_retry.dependency_hold
    })
    for item in (child_retry, root_retry):
        fixture.succeed(item)
        assert core._finish_pending_task(item)
    assert core._accepted_task_count == 0 and not core._task_finish_barriers


@pytest.mark.parametrize("deadline_already_elapsed", (False, True))
def test_recursive_finish_deferral_respects_get_timeout_without_busy_loop(
    monkeypatch: pytest.MonkeyPatch,
    deadline_already_elapsed: bool,
):
    fixture = _Fixture()
    core = fixture.core
    child, child_ref = fixture.submit()
    fixture.succeed(child, stored=True)
    root, root_ref = fixture.submit(child_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root)
    assert not fixture.queued()
    fixture.lose(child)
    fixture.lose(root)
    original = core._start_or_join_reconstruction
    attempts = []

    def bounded_admission(*args, **kwargs):
        attempts.append(args[0])
        if len(attempts) > 6:
            pytest.fail("get retried deferred lineage without checking its timeout")
        return original(*args, **kwargs)

    core._start_or_join_reconstruction = bounded_admission
    now = 100.0
    waits = []

    def monotonic():
        nonlocal now
        if deadline_already_elapsed:
            now += 1.0
        return now

    def wait(seconds):
        nonlocal now
        assert core._completion.depth > 0
        assert 0 < seconds <= 0.01
        waits.append(seconds)
        if len(waits) > 5:
            pytest.fail("get reset the deadline during finish deferral")
        now += seconds

    core._completion.wait = wait
    monkeypatch.setattr(core_module, "time", SimpleNamespace(monotonic=monotonic))
    with pytest.raises(TimeoutError, match="reconstruction admission did not finish"):
        core.get(root_ref, timeout=0.025)
    if deadline_already_elapsed:
        assert len(attempts) == 1 and not waits
    else:
        assert waits == pytest.approx([0.01, 0.01, 0.005])
        assert now == pytest.approx(100.025)
        assert len(attempts) == 4
    assert not fixture.queued()
    assert core._accepted_task_count == 1
    for pending in (child, root):
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.LOST
        assert core._recovery.task_record(pending.task_id).retries_started == 0


def test_finish_gate_arriving_during_foreign_renewal_prevents_local_commit():
    fixture = _Fixture()
    core = fixture.core
    pending, _ref = fixture.submit()
    fixture.succeed(pending, stored=True)
    assert core._finish_pending_task(pending)
    assert not fixture.queued()
    fixture.lose(pending)
    before = core.owner_table.snapshot(pending.object_id)
    record = replace(core._recovery.task_record(pending.task_id))
    calls = []

    def renewal(task_id, attempt_id):
        # The foreign hold exchange runs outside the Core composition lock.
        # Model a delayed admission exposing an old unfinished output before
        # it returns READY; commit must re-read the finish predicate.
        assert core._state_lock.depth == 0
        core._install_task_finish_barrier_locked(pending)
        calls.append((task_id, attempt_id))
        return ForeignLineageRenewalResult(
            task_id, attempt_id, ForeignLineageRenewalDisposition.READY, (), 0
        )

    def never_commit(*_args):
        pytest.fail("foreign renewal cannot commit across a new finish gate")

    core._foreign_lineage_registry = SimpleNamespace(
        snapshot=lambda task_id: object() if task_id == pending.task_id else None
    )
    core._foreign_lineage_runtime = SimpleNamespace(
        drive_renewal=renewal, validate_renewal_ready=never_commit,
        complete_renewal=never_commit,
    )
    assert core._start_or_join_reconstruction(
        pending.object_id, core._objects[pending.object_id]
    ) is None
    assert calls == [(pending.task_id, pending.spec.attempt_id.next())]
    assert core.owner_table.snapshot(pending.object_id) == before
    assert core._recovery.task_record(pending.task_id) == record
    assert core._recovery.active_recovery(pending.task_id) is None
    assert core._accepted_task_count == 0 and not fixture.queued()


def test_retirement_window_revalidates_all_foreign_renewals_before_any_start():
    fixture = _Fixture()
    core = fixture.core
    leaf, leaf_ref = fixture.submit()
    fixture.succeed(leaf, stored=True)
    assert core._finish_pending_task(leaf) and not fixture.queued()
    root, _root_ref = fixture.submit(leaf_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root) and not fixture.queued()
    fixture.lose(leaf)
    fixture.lose(root)
    denied = []
    retired = []
    checks = []
    retire = core._retire_lost_output_memberships
    def retire_then_revoke(output):
        assert core._state_lock.depth == 0
        result = retire(output)
        retired.append(output)
        if output == root.object_id:
            denied.append(root.task_id)
        return result
    def validate(task_id, attempt_id):
        checks.append(task_id)
        if task_id in denied:
            raise SystemTaskError("foreign owner died during output retirement")
    core._retire_lost_output_memberships = retire_then_revoke
    core._foreign_lineage_registry = SimpleNamespace(snapshot=lambda _task: object())
    core._foreign_lineage_runtime = SimpleNamespace(
        drive_renewal=lambda task, attempt: ForeignLineageRenewalResult(
            task, attempt, ForeignLineageRenewalDisposition.READY, (), 0),
        validate_renewal_ready=validate,
        complete_renewal=lambda *_: pytest.fail("no producer may START before all renewed holds validate"),
    )
    with pytest.raises(SystemTaskError, match="foreign owner died"):
        core._start_or_join_reconstruction(root.object_id, core._objects[root.object_id])
    assert retired == [leaf.object_id, root.object_id]
    assert checks == [leaf.task_id, root.task_id, leaf.task_id, root.task_id]
    assert core._accepted_task_count == 0 and not fixture.queued()
    for pending in (leaf, root):
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        assert record.state is TaskState.SUCCEEDED
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.LOST
        assert core.owner_table.snapshot(pending.object_id).current_attempt == pending.spec.attempt_id


def test_foreign_release_ack_loss_keeps_finish_gate_and_local_holds_until_retry():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    original, ref = fixture.submit(dependency_ref)
    foreign_owner = WorkerID.random()
    guard = _ForeignDependencyGuard(
        ObjectID.for_task(TaskID.random()), foreign_owner, ("127.0.0.1", 39933),
        core.worker_id, "foreign-source", protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
            original.task_id, original.spec.attempt_id,
        ),
    )
    accepted_guard = replace(
        guard, object_id=ObjectID.for_task(TaskID.random()),
        borrower_token="earlier-acknowledged-source",
    )
    pending = replace(
        original, foreign_dependency_guards=(accepted_guard, guard),
    )
    core._install_task_finish_barrier_locked(pending)
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    releases = []

    def release(_address, handler, request):
        assert handler == "release_owned_object_for_task"
        releases.append(request)
        if len(releases) == 2:
            raise TransportTimeout("Release applied; acknowledgement lost")
        return protocol.ReleaseOwnedObjectForTaskReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.hold, True, False,
        )

    core._borrow_rpc = release
    assert not core._finish_pending_task(pending)
    assert core._task_finish_barriers[pending.object_id] is pending
    assert core._accepted_task_count == 1
    assert pending.task_key in core._finishing_tasks
    assert pending.task_key not in core._active_task_finishes
    assert pending.task_key not in core._finished_tasks
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({
        pending.dependency_hold
    })
    assert pending.task_key in core._foreign_guard_release_retries
    retry = core._foreign_guard_release_retries[pending.task_key]
    assert retry.pending is pending
    assert retry.released_keys == (core._foreign_guard_key(accepted_guard),)
    with pytest.raises(TimeoutError, match="retirement did not finish"):
        core.get(ref, timeout=0)
    assert core._start_or_join_reconstruction(
        pending.object_id, core._objects[pending.object_id]
    ) is None
    assert not fixture.queued()

    assert core._finish_pending_task(pending)
    assert releases[0].object_id == accepted_guard.object_id
    assert releases[1] == releases[2]
    assert core._accepted_task_count == 0
    assert pending.object_id not in core._task_finish_barriers
    assert not core._foreign_guard_release_retries
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    assert core._finish_pending_task(pending)
    assert len(releases) == 3 and core._accepted_task_count == 0


def test_get_rechecks_finish_predicate_after_finalizer_notifies_before_condition(
    monkeypatch: pytest.MonkeyPatch,
):
    fixture = _Fixture()
    core = fixture.core
    pending, ref = fixture.submit()
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    completion = core._completion
    calls = []

    def finish_before_condition():
        calls.append("finish")
        assert core._finish_pending_task(pending)
        assert not core._task_finish_barriers

    def arm_interleave():
        # get computes the retirement wait deadline only after its old
        # snapshot saw the barrier, immediately before entering the condition.
        completion.before_enter = finish_before_condition
        return 100.0

    ticks = iter((lambda: 100.0, arm_interleave, lambda: 102.0))
    monkeypatch.setattr(
        core_module, "time", SimpleNamespace(monotonic=lambda: next(ticks)())
    )
    notifications = completion.notifications
    with pytest.raises(TimeoutError, match="was not ready before timeout"):
        core.get(ref, timeout=1.0)
    assert calls == ["finish"]
    assert completion.waits == 0
    assert completion.notifications > notifications
    (reconstructed,) = fixture.queued()
    assert reconstructed.spec.attempt_id == pending.spec.attempt_id.next()
    assert core._accepted_task_count == 1
    assert core._task_finish_barriers[pending.object_id] is reconstructed


def test_system_retry_moves_every_barrier_but_preserves_logical_hold():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    pending, _refs = fixture.submit(dependency_ref)
    assert not core._retry_system_failure(pending, SystemTaskError("worker exited"))
    (retried,) = fixture.queued()
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert retried.dependency_hold == pending.dependency_hold
    assert core._accepted_task_count == 1
    assert all(core._task_finish_barriers[object_id] is retried for object_id in pending.output_ids)
    assert not core._finish_pending_task(pending)
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({
        pending.dependency_hold
    })
    fixture.succeed(retried)
    assert core._finish_pending_task(retried)
    assert core._accepted_task_count == 0
    assert not core._task_finish_barriers
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
