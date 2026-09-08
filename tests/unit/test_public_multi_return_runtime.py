"""Mixed public multi-return contracts with explicit per-function modes.

Option validation, the guarded unified-publication case and two original
three-sibling terminal cases are unit. The latter use one pure Core, one
unstarted Node and a 1 KiB store. A bounded middle-value serializer still
decodes to 22, but exceeds the real 64-byte INLINE threshold; discovery thus
selects INLINE/STORED/INLINE without a fabricated descriptor-only success.
Every real wake observes all three owner results and stored routes committed.
Application error uses actual failed Node Complete and no successful-output
publication. Finish, exact local finalizers and sibling GC are explicit.
No user function, thread, process, socket, timer, wait or Queue.join runs in
these two cases.

Four original admission/retry cases also run on a threadless Core. They retain
the public remote surface or real three-sibling owner/recovery CAS, with at most
two Tasks, four local handles and one system retry. After their assertions an
explicit fixture-only local terminal error ends each still-unleased Task, then
real finish/finalizers/GC retire it. This is neither Worker execution nor a
successful result/shutdown claim. No Node or ObjectStore is created there.

The original five-field stored replay, three malformed manifests and three
terminal-preflight cases are also bounded. Stored replay retains both original
STORED siblings. Malformed replies retain a real completed output envelope;
wire revalidation and Core descriptor preflight are asserted separately. A
successful-output preflight exception now retains an adoption continuation and
returns False, rather than escaping from the obsolete descriptor-only backend.
Application/local terminal errors keep their original no-publication path.

The remaining lifetime cases now register the producer while PENDING, then its
three-output consumer, retaining the real submitted and producer-lineage holds
before either runs. Both publish through actual Node/owner reducers. Pure close
orders and a physically pinned STORED first sibling use explicit GC delivery.
The sibling-close concurrency case is L1, not unit: three real close threads
feed one real reference-event consumer. Its one-second barriers/close waits,
shared two-second work/cleanup join deadlines and exact thread ledger require
the outer 30-second runner; no unbounded Queue.join or process is used.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import math
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace

import cloudpickle
import pytest

import miniray as ray
from miniray import api, core as core_module, node as node_module, output_protocol as wire, protocol
from miniray.core import CoreWorker, ObjectRef, RemoteFunctionDefinition, _DelayedReadyTask, _PendingTask, _ReadyTask, _RetryInlineGc, _WAKE_COORDINATOR
from miniray.errors import ProtocolError, SystemTaskError, TaskError
from miniray.ids import JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan
from miniray.recovery import FailureKind, RecoveryManager, TaskState
from miniray.resources import NodeSnapshot, ResourceLedger, ResourceVector
from miniray.task_outputs import MAX_TASK_RETURNS
from miniray.trace import EventSink
from tests.unit._core_test_utils import add_core_thread_finalizer
from tests.unit._pure_core import close_pure_core, make_pure_core


def _core() -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.event_sink = EventSink()
    core._submission_index = 0
    core._accepted_task_count = 0
    core._owner_table = ObjectOwnerTable()
    core._recovery = RecoveryManager()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._submissions = queue.Queue()
    return core


def _descriptor(
    core: CoreWorker, object_id, payload: bytes, *, stored: bool = False
) -> protocol.ResultDescriptor:
    return protocol.ResultDescriptor(
        object_id,
        (
            protocol.ResultStorage.OBJECT_STORE
            if stored else protocol.ResultStorage.INLINE
        ),
        len(payload),
        core.worker_id,
        core.node_id,
        hashlib.sha256(payload).hexdigest(),
        None if stored else payload,
    )


def _multi_pending(
    core: CoreWorker, *, num_returns: int = 3, max_retries: int = 1
) -> tuple[_PendingTask, tuple[ObjectRef, ...]]:
    definition = RemoteFunctionDefinition.from_callable(lambda: None, core.job_id)
    pending, refs = core._register_submission(
        definition, (), {}, ResourceVector(),
        num_returns=num_returns, max_retries=max_retries,
    )
    assert isinstance(refs, tuple)
    return pending, refs


@pytest.mark.unit
def test_public_option_is_bounded_function_only_and_copied() -> None:
    fn = ray.remote(num_returns=3)(lambda: (1, 2, 3))
    overridden = fn.options(num_returns=2)
    assert fn._num_returns == 3
    assert overridden._num_returns == 2
    assert fn is not overridden

    for invalid in (True, 0, MAX_TASK_RETURNS + 1, 1.5, "2"):
        with pytest.raises((TypeError, ValueError), match="num_returns"):
            ray.remote(num_returns=invalid)(lambda: None)

    class Actor:
        pass

    with pytest.raises(TypeError, match="remote functions.*Actor"):
        ray.remote(num_returns=2)(Actor)
    with pytest.raises(TypeError, match="remote functions.*Actor"):
        ray.remote(Actor).options(num_returns=2)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_admission_runtime")
def test_public_remote_returns_single_ref_or_ordered_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, refs = make_pure_core(), []
    try:
        monkeypatch.setattr(api, "_active_core_worker", lambda: core)
        one = ray.remote(lambda: 1).remote()
        refs.append(one)
        many = ray.remote(num_returns=3)(lambda: (1, 2, 3)).remote()
        refs.extend(many if isinstance(many, tuple) else (many,))
        assert isinstance(one, ObjectRef)
        assert isinstance(many, tuple)
        assert len(many) == 3
        assert all(isinstance(ref, ObjectRef) for ref in many)
        assert tuple(ref.object_id.return_index for ref in many) == (0, 1, 2)
        single, multiple = _three_slot_take_submissions(core)
        assert single.output_ids == (one.object_id,)
        assert multiple.output_ids == tuple(ref.object_id for ref in many)
        assert (single.task_id, multiple.task_id) == tuple(
            TaskID.derive(core.job_id, core.driver_task_id, index) for index in (0, 1)
        )
        assert core._accepted_task_count == 2 and core._submission_index == 2
        assert core._task_finish_barriers == {
            **{output: single for output in single.output_ids},
            **{output: multiple for output in multiple.output_ids},
        }
        assert all(core.owner_table.snapshot(ref.object_id).state is ObjectState.PENDING for ref in refs)
        _finish_unleased_admission(core, single, (one,))
        assert core._accepted_task_count == 1
        _finish_unleased_admission(core, multiple, many)
        assert core._accepted_task_count == 0
    finally:
        _close_admission_handles(core, refs)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_admission_runtime")
def test_submission_registers_all_entries_waiters_refs_and_task_lineage() -> None:
    core, refs = make_pure_core(), ()
    try:
        pending, refs = _register_three_slot_admission(core)
        assert _three_slot_take_submissions(core) == (pending,)
        assert pending.output_ids == pending.spec.return_ids()
        assert pending.task_key == pending.spec.task_id
        assert tuple(ref.object_id for ref in refs) == pending.output_ids
        assert set(pending.output_ids) == set(core._objects)
        for ref in refs:
            snapshot = core.owner_table.snapshot(ref.object_id)
            assert snapshot.state is ObjectState.PENDING
            assert snapshot.producer_task_spec == pending.spec
            assert snapshot.current_attempt == pending.spec.attempt_id
            assert snapshot.local_tokens == frozenset({ref._local_token})
            assert ref._finalizer.alive and not ref._release_done.is_set()
            assert not core._objects[ref.object_id].event.is_set()
        lineage = core._recovery.lineage_for_object(pending.output_ids[-1])
        assert lineage is not None and lineage.output_ids == pending.output_ids
        assert all(core._recovery.lineage_for_object(output) == lineage for output in pending.output_ids)
        assert core._accepted_task_count == 1 and core._inflight_submissions == 0
        assert core._task_finish_barriers == {output: pending for output in pending.output_ids}
        assert not core._protocol_unresolved and not core._reference_mailbox.releases
        _finish_unleased_admission(core, pending, refs)
    finally:
        _close_admission_handles(core, refs)


@pytest.fixture
def _no_three_slot_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("three-slot terminal fixture attempted runtime work")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


def _never_execute_three_slots():
    pytest.fail("three-slot fixture executed the user function")


class _StoredTwentyTwo:
    """Same decoded value, deliberately longer serialized middle slot."""
    def __init__(self):
        self.reductions = 0

    def __reduce__(self):
        self.reductions += 1
        assert self.reductions == 1
        return int, ("22" + " " * 128,)


def _three_slot_take_submissions(core):
    size = core._submissions.qsize()
    assert size <= 16
    tasks = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            assert type(item) is _PendingTask
            tasks.append(item)
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    return tuple(tasks)


def _three_slot_release(ref):
    done, finalizer = ref._release_done, ref._finalizer
    assert done is not None and finalizer is not None
    ref._closed = True
    finalizer()
    assert done.is_set() and not finalizer.alive


@pytest.fixture
def _no_admission_runtime(monkeypatch, _no_three_slot_runtime):
    def forbidden(*_args, **_kwargs):
        pytest.fail("admission-only fixture attempted execution/storage work")

    monkeypatch.setattr(CoreWorker, "_execute", forbidden)
    monkeypatch.setattr(ObjectStore, "__init__", forbidden)


def _register_three_slot_admission(core):
    pending, refs = core._register_submission(
        RemoteFunctionDefinition.from_callable(_never_execute_three_slots, core.job_id),
        (), {}, ResourceVector(), num_returns=3, max_retries=1, _enqueue=True,
    )
    assert type(refs) is tuple and len(refs) == 3
    return pending, refs


def _admission_state(core, pending):
    """Immutable/snapshot authority facts around the original CAS fault."""
    return (
        tuple(core.owner_table.snapshot(output) for output in pending.output_ids),
        replace(core._recovery.task_record(pending.task_id)),
        tuple(core._recovery.reconstruction_snapshot(output) for output in pending.output_ids),
        tuple(core._submissions.queue), core._submissions.unfinished_tasks,
        core._accepted_task_count, core._inflight_submissions,
        dict(core._task_finish_barriers), dict(core._protocol_unresolved),
        dict(core._stored_descriptors), dict(core._object_gc_obligations),
        tuple(core._reference_mailbox.pending.queue), core._reference_mailbox.pending.unfinished_tasks,
        tuple(core._reference_mailbox.releases),
        tuple(core._objects[output].event.is_set() for output in pending.output_ids),
        frozenset(core._finished_tasks), frozenset(core._finishing_tasks),
        frozenset(core._active_task_finishes),
    )


def _finish_unleased_admission(core, pending, refs):
    """Explicit fixture terminal after assertions, not a Worker completion.

    No lease or output publication ever existed in these admission-only cases.
    The real local terminal path publishes one error, closes the current finish
    barrier, and later collects each sibling after its actual handle release.
    """
    assert 1 <= len(refs) <= 3 and tuple(ref.object_id for ref in refs) == pending.output_ids
    assert core._submissions.empty() and not core._protocol_unresolved
    assert not pending.protected_dependencies and not pending.foreign_dependency_guards
    before_count = core._accepted_task_count
    assert 1 <= before_count <= 2
    assert all(core._task_finish_barriers[output] is pending for output in pending.output_ids)
    assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
    error = SystemTaskError("pure admission fixture completion; user function was not run")
    assert core._publish_task_error(pending, error)
    assert all(
        core.owner_table.snapshot(output).state is ObjectState.ERROR
        and core.owner_table.snapshot(output).error is error
        and core.owner_table.snapshot(output).current_attempt == pending.spec.attempt_id
        for output in pending.output_ids
    )
    assert core._recovery.task_record(pending.task_id).state is TaskState.SYSTEM_FAILED
    assert core._finish_pending_task(pending) and core._finish_pending_task(pending)
    assert core._accepted_task_count == before_count - 1
    assert not set(pending.output_ids).intersection(core._task_finish_barriers)
    assert _three_slot_take_submissions(core) == ()
    for ref in refs:
        _three_slot_release(ref)
    fifo = core._reference_mailbox.pending
    size = fifo.qsize()
    assert size <= 16
    for _ in range(size):
        event = fifo.get_nowait()
        try:
            assert type(event) is _RetryInlineGc and event.object_id in pending.output_ids
            core._reference_released(event.object_id)
        finally:
            fifo.task_done()
    assert fifo.empty() and fifo.unfinished_tasks == 0
    assert _three_slot_take_submissions(core) == ()
    assert all(core.owner_table.collection_state(output) is ObjectCollectionState.COLLECTED for output in pending.output_ids)
    assert all(core._recovery.lineage_for_object(output) is None for output in pending.output_ids)
    assert not set(pending.output_ids).intersection(core._objects)
    assert not core._object_gc_obligations and not core._stored_descriptors


def _close_admission_handles(core, refs):
    # Failure cleanup only releases handles. It does not publish an artificial
    # terminal, clear a protocol marker, or reset accepted/owner/recovery state.
    for ref in refs:
        _three_slot_release(ref)
    close_pure_core(core)


class _ThreeSlotTerminal:
    """Canonical three-output registration, real Node terminal, owner GC."""
    def __init__(self, monkeypatch):
        self.core = core = make_pure_core()
        core.gcs_address = ("three-slot-control.invalid", 1)
        self.node = node = object.__new__(NodeServer)
        node.node_id, node.worker_id = core.node_id, WorkerID.random()
        node._node_pid, node._registration_epoch = 32301, 1
        node._gcs_address, node._registered_with_gcs = None, False
        node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
        node._cluster_addresses = {}
        node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
        node._stop_event = threading.Event()
        node._shutdown_request_id, node._active_lease_id = None, None
        node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
        node._lease_request_locks, node._inflight_lease_requests = {}, 0
        node._worker_process = type("PassiveWorker", (), {"is_alive": lambda _self: True})()
        node._worker_address = ("three-slot-worker.invalid", 1)
        node.event_sink = None
        node._object_store = ObjectStore(1024)
        node._object_manager = ObjectManager(node.node_id, node._object_store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._local_replica_write_claims, node._object_localization_locks = {}, {}
        self.journal = node._output_publication_journal = OutputPublicationJournal()
        self.recovery = OutputPublicationRecoveryAuthority()
        self.calls, self.drops, self.slot_reports, self.wakes = [], [], [], []
        self.outputs = self.envelope = self.error = None
        self.pending, self.refs = core._register_submission(
            core.define_remote_function(_never_execute_three_slots), (), {},
            ResourceVector({"CPU": 1}), num_returns=3, max_retries=1, _enqueue=True,
        )
        assert type(self.refs) is tuple and len(self.refs) == 3
        assert tuple(ref.object_id for ref in self.refs) == self.pending.output_ids
        assert _three_slot_take_submissions(core) == (self.pending,)
        self.request = protocol.RequestWorkerLease(
            LeaseID.random(), self.pending.task_id, self.pending.spec.attempt_id, self.pending.spec.resources,
            core.node_id, core.worker_id, target_node_id=node.node_id, return_ids=self.pending.output_ids,
        )
        self.grant = node._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease
        started = node._handle_start_worker_lease(protocol.StartWorkerLease(
            self.grant.lease_id, self.pending.task_id, self.pending.spec.attempt_id, self.grant.worker_id,
        ))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        assert node.resource_ledger.available.is_zero()
        self.identity = OutputPublicationID(self.grant.lease_id, self.pending.execution)

        def forbidden(*_args, **_kwargs):
            pytest.fail("ref-free terminal attempted child/graph/rollback work")

        self.adapter = node._output_publications = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent, arm_complete=self.recovery.arm_complete,
            report_terminal=self.recovery.report_terminal, report_rollback=self.recovery.report_rollback,
            prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
            prepare_graph=forbidden, abort_graph=forbidden, seal_replica=node._seal_output_publication_replica,
            drop_replica=forbidden,
        )
        monkeypatch.setattr(core, "_rpc", self.rpc)
        self.actual_wake = core._wake_object
        monkeypatch.setattr(core, "_wake_object", self.observe_wake)

    def complete(self, *, application_error=False):
        pending, node = self.pending, self.node
        if not application_error:
            middle = _StoredTwentyTwo()
            session = OutputDiscoverySession(OutputPublicationHeader(
                self.identity, pending.spec.job_id, self.grant.worker_id, self.core.worker_id,
                OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
            ), inline_threshold=64)
            self.outputs = session.discover((11, middle, 33))
            slots, payloads = self.outputs.manifest.slots, self.outputs.slot_payloads
            assert len(slots) == 3 and middle.reductions == 1
            assert tuple(slot.tier for slot in slots) == (
                protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE, protocol.ResultStorage.INLINE,
            )
            assert sum(map(len, payloads)) <= 512
            assert tuple(cloudpickle.loads(payload) for payload in payloads) == (11, 22, 33)
            assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(
                self.outputs.manifest, payloads,
            )).accepted
            assert self.recovery.snapshot(self.identity).armed
            assert node.resource_ledger.available.is_zero()
            session.release_sources_after_promotions()
        status = protocol.TaskReplyStatus.APPLICATION_ERROR if application_error else protocol.TaskReplyStatus.SUCCEEDED
        self.completion = protocol.CompleteWorkerLease(
            self.grant.lease_id, pending.task_id, pending.spec.attempt_id, self.grant.worker_id, status,
        )
        completed = node._handle_complete_worker_lease(self.completion)
        assert completed.accepted and completed.released
        assert completed.state is protocol.LeaseExecutionState.COMPLETED
        assert node.resource_ledger.available == node.resource_ledger.total
        assert all(self.core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
        assert not any(self.core._objects[output].event.is_set() for output in pending.output_ids)
        self.envelope = completed.output_publication
        if application_error:
            assert self.envelope is None and completed.output_completion is None
            assert not self.journal.publication_ids() and self.node.object_store.used_bytes == 0
            return protocol.TaskReply(
                pending.task_id, pending.spec.attempt_id, self.grant.worker_id, status,
                error=protocol.RemoteErrorInfo("ValueError", "boom"),
            )
        assert self.envelope.manifest == self.outputs.manifest
        assert self.envelope.results[1].inline_data is None
        assert node.object_store.get(pending.output_ids[1]) == self.outputs.slot_payloads[1]
        return protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, self.grant.worker_id, status,
            self.envelope.results, output_publication=self.envelope,
        )

    def observe_wake(self, output_id):
        core, pending = self.core, self.pending
        index = len(self.wakes)
        assert index < 3 and output_id == pending.output_ids[index]
        snapshots = tuple(core.owner_table.snapshot(value) for value in pending.output_ids)
        assert all(value.current_attempt == pending.spec.attempt_id for value in snapshots)
        assert tuple(core._objects[value].event.is_set() for value in pending.output_ids) == tuple(i < index for i in range(3))
        assert core._accepted_task_count == 1 and set(core._task_finish_barriers) == set(pending.output_ids)
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        if self.envelope is not None:
            assert tuple(value.state for value in snapshots) == (ObjectState.READY_INLINE, ObjectState.READY_STORED, ObjectState.READY_INLINE)
            assert core._stored_descriptors == {pending.output_ids[1]: self.envelope.results[1]}
            assert all(value.output_publication.manifest == self.envelope.manifest for value in snapshots)
            assert tuple(value.output_publication.slot_index for value in snapshots) == (0, 1, 2)
            assert tuple(core.owner_table.output_owner_result(value) for value in pending.output_ids) == self.envelope.results
            assert core.owner_table.output_owner_publication_receipt(OutputOwnerPublicationPlan(pending.execution, self.envelope)).committed
            assert self.node.object_store.get(pending.output_ids[1]) == self.outputs.slot_payloads[1]
            result = self.envelope.results[1]
            payload = self.node.object_store.get(result.object_id)
            assert len(payload) == result.size_bytes and hashlib.sha256(payload).hexdigest() == result.checksum
            assert self.node._sealed_metadata == {result.object_id: (
                pending.spec.attempt_id, core.worker_id, result.size_bytes, result.checksum,
            )}
            assert record.state is TaskState.SUCCEEDED
        else:
            assert all(value.state is ObjectState.ERROR for value in snapshots)
            if self.error is None:
                self.error = snapshots[0].error
            assert isinstance(self.error, TaskError)
            assert self.error.remote_type == "ValueError" and self.error.remote_message == "boom"
            assert all(value.error is self.error and value.output_publication is None for value in snapshots)
            assert not core._stored_descriptors and self.node.object_store.used_bytes == 0
            assert record.state is TaskState.APPLICATION_FAILED
        self.wakes.append(output_id)
        self.actual_wake(output_id)

    def rpc(self, address, handler, request):
        self.calls.append((handler, request))
        assert len(self.calls) <= 7 and self.envelope is not None
        if handler == "drop_object_replica":
            assert address == self.core.node_address and not self.drops
            result = self.envelope.results[1]
            assert request == protocol.DropObjectReplica(
                result.object_id, self.pending.spec.attempt_id, self.core.worker_id, self.node.node_id, result.checksum,
            )
            reply = self.node._handle_drop_object_replica(request)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            self.drops.append((request, reply))
            return reply
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.core.gcs_address
            if type(request) is wire.ReportOutputPublicationTerminal:
                assert request.witness == self.envelope.complete
                ack = self.recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                assert request.proof.complete == self.envelope.complete
                ack = self.recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                assert request.proof.complete == self.envelope.complete
                assert request.proof.slot_index == len(self.slot_reports) < 3
                assert self.core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                if request.proof.slot_index == 1:
                    assert len(self.drops) == 1 and self.node.object_store.used_bytes == 0
                self.slot_reports.append(request.proof)
                ack = self.recovery.report_slot_collected(request.proof)
            return wire.OutputRecoveryReply(request, ack)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert address == self.core.node_address
        assert self.recovery.snapshot(self.identity).adopted == request.proof
        return self.node._handle_ack_output_publication_adopted(request)

    def gc_notices(self):
        fifo = self.core._reference_mailbox.pending
        size = fifo.qsize()
        assert size <= 16
        for _ in range(size):
            event = fifo.get_nowait()
            try:
                assert type(event) is _RetryInlineGc and event.object_id in self.pending.output_ids
                self.core._reference_released(event.object_id)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0

    def finish_and_collect(self):
        core, pending = self.core, self.pending
        assert tuple(self.wakes) == pending.output_ids
        self.gc_notices()
        assert core._finish_pending_task(pending) and core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert _three_slot_take_submissions(core) == ()
        self.gc_notices()
        before = self.node.resource_ledger.snapshot()
        repeated = self.node._handle_complete_worker_lease(self.completion)
        assert repeated.accepted and not repeated.released
        assert self.node.resource_ledger.snapshot() == before
        for index, ref in enumerate(self.refs):
            _three_slot_release(ref)
            self.gc_notices()
            assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
            assert core._recovery.lineage_for_object(ref.object_id) is None
            if index < 2:
                assert core._recovery.task_record(pending.task_id).retries_started == 0
                assert all(core.owner_table.contains(value) for value in pending.output_ids[index + 1:])
                assert core._recovery.lineage_for_object(pending.output_ids[-1]).output_ids == pending.output_ids
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert _three_slot_take_submissions(core) == ()
        assert len(core._reference_mailbox.releases) == 3
        assert self.node.object_store.used_bytes == 0 and not self.node._sealed_metadata
        assert self.node.resource_ledger.available == self.node.resource_ledger.total
        assert not self.node._local_replica_write_claims
        if self.envelope is None:
            assert not self.calls and not self.journal.publication_ids()
        else:
            assert len(self.drops) == 1 and len(self.slot_reports) == 3
            assert self.recovery.snapshot(self.identity).slot_collections == tuple(self.slot_reports)
            assert self.adapter.report_terminal(self.identity)
            assert not self.journal.snapshot(self.identity).retained_result_slots
        assert not self.adapter.pending_terminal_reports() and not self.adapter.pending_lease_completions()
        with self.node._state_lock:
            assert self.node._output_publications_clean_locked()

    def close(self):
        for ref in self.refs:
            _three_slot_release(ref)
        close_pure_core(self.core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_ordered_mixed_success_publishes_and_wakes_all_siblings_atomically(monkeypatch) -> None:
    f = _ThreeSlotTerminal(monkeypatch)
    try:
        reply = f.complete()
        assert f.core._publish_reply(
            f.pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        assert tuple(f.core.owner_table.snapshot(value).state for value in f.pending.output_ids) == (
            ObjectState.READY_INLINE, ObjectState.READY_STORED, ObjectState.READY_INLINE,
        )
        assert tuple(f.wakes) == f.pending.output_ids
        assert all(f.core._objects[value].event.is_set() for value in f.pending.output_ids)
        assert f.core._stored_descriptors == {f.pending.output_ids[1]: reply.results[1]}
        payloads = (reply.results[0].inline_data, f.node.object_store.get(f.pending.output_ids[1]), reply.results[2].inline_data)
        assert tuple(cloudpickle.loads(payload) for payload in payloads) == (11, 22, 33)
        f.finish_and_collect()
    finally:
        f.close()


class _TwoStoredTerminal:
    """Keep the original replay test's two real STORED siblings."""
    def __init__(self, monkeypatch):
        self.core = core = make_pure_core()
        core.gcs_address = ("two-stored-control.invalid", 1)
        self.node = node = object.__new__(NodeServer)
        node.node_id, node.worker_id = core.node_id, WorkerID.random()
        node._node_pid, node._registration_epoch = 32501, 1
        node._gcs_address, node._registered_with_gcs = None, False
        node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
        node._cluster_addresses = {}
        node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
        node._stop_event = threading.Event()
        node._shutdown_request_id, node._active_lease_id = None, None
        node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
        node._lease_request_locks, node._inflight_lease_requests = {}, 0
        node._worker_process = type("PassiveWorker", (), {"is_alive": lambda _self: True})()
        node._worker_address = ("two-stored-worker.invalid", 1)
        node.event_sink = None
        node._object_store = ObjectStore(1024)
        node._object_manager = ObjectManager(node.node_id, node._object_store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._local_replica_write_claims, node._object_localization_locks = {}, {}
        self.journal = node._output_publication_journal = OutputPublicationJournal()
        self.recovery = OutputPublicationRecoveryAuthority()
        self.calls, self.drops, self.slot_reports, self.wakes = [], [], [], []
        self.pending, self.refs = core._register_submission(
            core.define_remote_function(_never_execute_three_slots), (), {},
            ResourceVector({"CPU": 1}), num_returns=2, max_retries=1, _enqueue=True,
        )
        assert len(self.refs) == 2 and _three_slot_take_submissions(core) == (self.pending,)
        request = protocol.RequestWorkerLease(
            LeaseID.random(), self.pending.task_id, self.pending.spec.attempt_id, self.pending.spec.resources,
            core.node_id, core.worker_id, target_node_id=node.node_id, return_ids=self.pending.output_ids,
        )
        self.grant = node._handle_request_lease(request)
        assert type(self.grant) is protocol.GrantWorkerLease
        assert node._handle_start_worker_lease(protocol.StartWorkerLease(
            self.grant.lease_id, self.pending.task_id, self.pending.spec.attempt_id, self.grant.worker_id,
        )).accepted
        assert node.resource_ledger.available.is_zero()
        self.identity = OutputPublicationID(self.grant.lease_id, self.pending.execution)

        def forbidden(*_args, **_kwargs):
            pytest.fail("two stored outputs attempted child/graph/rollback work")

        self.adapter = node._output_publications = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent, arm_complete=self.recovery.arm_complete,
            report_terminal=self.recovery.report_terminal, report_rollback=self.recovery.report_rollback,
            prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
            prepare_graph=forbidden, abort_graph=forbidden, seal_replica=node._seal_output_publication_replica,
            drop_replica=forbidden,
        )
        session = OutputDiscoverySession(OutputPublicationHeader(
            self.identity, self.pending.spec.job_id, self.grant.worker_id, core.worker_id,
            OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
        ), inline_threshold=0)
        self.outputs = session.discover((b"left", b"right"))
        assert tuple(slot.tier for slot in self.outputs.manifest.slots) == (protocol.ResultStorage.OBJECT_STORE,) * 2
        assert sum(map(len, self.outputs.slot_payloads)) <= 256
        assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            self.outputs.manifest, self.outputs.slot_payloads,
        )).accepted
        session.release_sources_after_promotions()
        self.completion = protocol.CompleteWorkerLease(
            self.grant.lease_id, self.pending.task_id, self.pending.spec.attempt_id, self.grant.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED,
        )
        completed = node._handle_complete_worker_lease(self.completion)
        assert completed.accepted and completed.released
        self.envelope = completed.output_publication
        assert self.envelope.manifest == self.outputs.manifest
        self.reply = protocol.TaskReply(
            self.pending.task_id, self.pending.spec.attempt_id, self.grant.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, self.envelope.results, output_publication=self.envelope,
        )
        monkeypatch.setattr(core, "_rpc", self.rpc)
        self.actual_wake = core._wake_object
        monkeypatch.setattr(core, "_wake_object", self.observe_wake)

    def observe_wake(self, output):
        index = len(self.wakes)
        assert index < 2 and output == self.pending.output_ids[index]
        assert all(self.core.owner_table.snapshot(value).state is ObjectState.READY_STORED for value in self.pending.output_ids)
        assert self.core._stored_descriptors == {result.object_id: result for result in self.envelope.results}
        assert tuple(self.core._objects[value].event.is_set() for value in self.pending.output_ids) == tuple(i < index for i in range(2))
        self.wakes.append(output)
        self.actual_wake(output)

    def rpc(self, address, handler, request):
        self.calls.append((handler, request))
        assert len(self.calls) <= 7
        if handler == "drop_object_replica":
            assert address == self.core.node_address and len(self.drops) < 2
            result = self.envelope.results[len(self.drops)]
            assert request == protocol.DropObjectReplica(
                result.object_id, self.pending.spec.attempt_id, self.core.worker_id, self.node.node_id, result.checksum,
            )
            reply = self.node._handle_drop_object_replica(request)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            self.drops.append(request)
            return reply
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.core.gcs_address
            if type(request) is wire.ReportOutputPublicationTerminal:
                ack = self.recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                ack = self.recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                assert request.proof.slot_index == len(self.slot_reports) < 2
                assert len(self.drops) == len(self.slot_reports) + 1
                assert self.core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                self.slot_reports.append(request.proof)
                ack = self.recovery.report_slot_collected(request.proof)
            return wire.OutputRecoveryReply(request, ack)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER and address == self.core.node_address
        assert self.recovery.snapshot(self.identity).adopted == request.proof
        return self.node._handle_ack_output_publication_adopted(request)

    def gc_notices(self):
        fifo = self.core._reference_mailbox.pending
        size = fifo.qsize()
        assert size <= 16
        for _ in range(size):
            event = fifo.get_nowait()
            try:
                assert type(event) is _RetryInlineGc and event.object_id in self.pending.output_ids
                self.core._reference_released(event.object_id)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0

    def finish_and_collect(self):
        assert tuple(self.wakes) == self.pending.output_ids
        self.gc_notices()
        assert self.core._finish_pending_task(self.pending)
        assert _three_slot_take_submissions(self.core) == ()
        self.gc_notices()
        for index, ref in enumerate(self.refs):
            _three_slot_release(ref)
            self.gc_notices()
            assert self.core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
            if index == 0:
                assert self.core.owner_table.contains(self.pending.output_ids[1])
                assert self.core._recovery.lineage_for_object(self.pending.output_ids[1]) is not None
        assert len(self.drops) == len(self.slot_reports) == 2
        assert self.adapter.report_terminal(self.identity)
        assert not self.adapter.pending_terminal_reports() and not self.adapter.pending_lease_completions()
        assert self.node.object_store.used_bytes == 0 and not self.node._sealed_metadata
        assert not self.node._local_replica_write_claims
        assert self.node.resource_ledger.available == self.node.resource_ledger.total
        assert not self.core._objects and not self.core._stored_descriptors and not self.core._object_gc_obligations
        assert self.core._accepted_task_count == 0 and not self.core._task_finish_barriers
        assert all(self.core._recovery.lineage_for_object(output) is None for output in self.pending.output_ids)
        assert _three_slot_take_submissions(self.core) == ()
        with self.node._state_lock:
            assert self.node._output_publications_clean_locked()

    def close(self):
        for ref in self.refs:
            _three_slot_release(ref)
        close_pure_core(self.core)


def _publication_physical_state(fixture):
    """Bounded read-only facts; keep canonical Node custody separate from wire."""
    record = fixture.node._leases[fixture.grant.lease_id]
    return (
        fixture.journal.snapshot(fixture.identity), fixture.recovery.snapshot(fixture.identity),
        fixture.node.resource_ledger.snapshot(), record.state, record.completion,
        tuple((output, fixture.node.object_store.get(output)) for output in fixture.pending.output_ids
              if fixture.node.object_store.contains(output)),
        dict(fixture.node._sealed_metadata), dict(fixture.node._dropped_metadata),
        dict(fixture.node._local_replica_write_claims),
    )


@pytest.mark.parametrize(
    "field", ["object_id", "owner_worker_id", "node_id", "size_bytes", "checksum"]
)
@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_core_stored_replay_drift_cannot_overwrite_any_descriptor(
    field: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _TwoStoredTerminal(monkeypatch)
    core, pending = f.core, f.pending
    try:
        assert core._publish_reply(
            pending, f.reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        results = f.reply.results
        assert len(results) == 2 and all(result.storage is protocol.ResultStorage.OBJECT_STORE for result in results)
        assert tuple(cloudpickle.loads(f.node.object_store.get(output)) for output in pending.output_ids) == (b"left", b"right")
        assert core._decode_reply(pending, f.reply) == results
        before = _admission_state(core, pending)
        physical = _publication_physical_state(f)
        calls, wakes = tuple(f.calls), tuple(f.wakes)
        changed = list(results)
        index = 1  # preserve the original healthy slot 0 / drifted slot 1
        if field == "object_id":
            changed[index] = replace(changed[index], object_id=ObjectID.for_task(TaskID.random(), 1))
        elif field == "owner_worker_id":
            changed[index] = replace(changed[index], owner_worker_id=WorkerID.random())
        elif field == "node_id":
            changed[index] = replace(changed[index], node_id=NodeID.random())
        elif field == "size_bytes":
            changed[index] = replace(changed[index], size_bytes=99)
        else:
            changed[index] = replace(changed[index], checksum=hashlib.sha256(b"changed").hexdigest())
        assert changed[0] == results[0]
        wire_error = "result IDs must belong" if field == "object_id" else "publish exactly its results"
        with pytest.raises(ProtocolError, match=wire_error):
            replace(f.reply, results=tuple(changed))
        damaged = deepcopy(f.reply)
        object.__setattr__(damaged, "results", tuple(changed))
        assert damaged.output_publication == f.envelope and damaged.output_publication is not f.envelope
        preflight_calls = []
        actual_preflight = core._preflight_stored_result_replays

        def observe_preflight(selected, descriptors):
            assert selected is pending and descriptors == tuple(changed)
            preflight_calls.append(descriptors)
            return actual_preflight(selected, descriptors)

        with monkeypatch.context() as patch:
            patch.setattr(core, "_preflight_stored_result_replays", observe_preflight)
            expected_error = (ProtocolError, SystemTaskError) if field == "object_id" else SystemTaskError
            detail = ("ordered task returns" if field == "object_id" else
                      "wrong object owner" if field == "owner_worker_id" else "canonical descriptor")
            # Independently exercise the original decoder preflight with a
            # post-construction damaged copy. No missing envelope substitutes
            # for checking its canonical stored replay descriptors.
            with pytest.raises(expected_error, match=detail):
                core._decode_reply(pending, damaged)
        assert len(preflight_calls) == int(field in ("node_id", "size_bytes", "checksum"))
        with pytest.raises(ProtocolError, match=wire_error):
            core._publish_reply(pending, damaged, expected_node_id=f.node.node_id)
        assert _admission_state(core, pending) == before
        assert _publication_physical_state(f) == physical
        assert (tuple(f.calls), tuple(f.wakes)) == (calls, wakes)
        assert core._stored_descriptors == {result.object_id: result for result in results}
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.parametrize("mode", ["missing", "reordered", "duplicate"])
@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_invalid_success_manifest_publishes_no_sibling(
    mode: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _ThreeSlotTerminal(monkeypatch)
    core, pending = f.core, f.pending
    try:
        reply = f.complete()
        values = reply.results
        assert len(values) == 3 and reply.output_publication == f.envelope
        if mode == "missing":
            invalid = values[:-1]
        elif mode == "reordered":
            invalid = (values[1], values[0], values[2])
        else:
            invalid = (values[0], values[1], values[1])
        before = _admission_state(core, pending)
        physical = _publication_physical_state(f)
        # This error is the current TaskReply/envelope ordered-result binding,
        # not the old raw success shape and not a claim that owner CAS ran.
        with pytest.raises(ProtocolError, match="changed its ordered selected outputs"):
            replace(reply, results=invalid)
        damaged = deepcopy(reply)
        object.__setattr__(damaged, "results", invalid)
        assert damaged.output_publication == f.envelope
        with pytest.raises(ProtocolError, match="changed its ordered selected outputs"):
            core._publish_reply(pending, damaged, expected_node_id=f.node.node_id)
        with pytest.raises(SystemTaskError, match="manifest must exactly match ordered task returns"):
            core._decode_reply(pending, damaged, expected_node_id=f.node.node_id)
        assert _admission_state(core, pending) == before
        assert _publication_physical_state(f) == physical
        assert not f.calls and not f.wakes
        assert all(core.owner_table.snapshot(value).state is ObjectState.PENDING for value in pending.output_ids)
        assert not any(core._objects[value].event.is_set() for value in pending.output_ids)
        assert core.owner_table.output_owner_publication_receipt(OutputOwnerPublicationPlan(pending.execution, f.envelope)) is None
        assert core._publish_reply(pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_application_error_publishes_same_terminal_error_to_all_siblings(monkeypatch) -> None:
    f = _ThreeSlotTerminal(monkeypatch)
    try:
        reply = f.complete(application_error=True)
        assert f.core._publish_reply(f.pending, reply)
        snapshots = tuple(f.core.owner_table.snapshot(value) for value in f.pending.output_ids)
        assert all(value.state is ObjectState.ERROR for value in snapshots)
        assert len({id(value.error) for value in snapshots}) == 1
        assert all(value.error is f.error for value in snapshots)
        assert tuple(f.wakes) == f.pending.output_ids
        assert all(f.core._objects[value].event.is_set() for value in f.pending.output_ids)
        assert f.core._recovery.task_record(f.pending.task_id).state is TaskState.APPLICATION_FAILED
        assert not f.calls and f.envelope is None and f.outputs is None
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_admission_runtime")
def test_system_retry_advances_all_siblings_once_and_preserves_task_key() -> None:
    core, refs = make_pure_core(), ()
    try:
        pending, refs = _register_three_slot_admission(core)
        assert _three_slot_take_submissions(core) == (pending,)
        old_attempt = pending.spec.attempt_id
        local_tokens = tuple(core.owner_table.snapshot(output).local_tokens for output in pending.output_ids)
        assert not core._retry_system_failure(pending, RuntimeError("retry"))
        (retried,) = _three_slot_take_submissions(core)
        assert isinstance(retried, _PendingTask)
        assert retried.task_key == pending.task_key == pending.task_id
        assert retried.output_ids == pending.output_ids == tuple(ref.object_id for ref in refs)
        assert retried.spec.attempt_id == old_attempt.next()
        assert retried.dependency_hold == pending.dependency_hold
        assert all(
            core.owner_table.snapshot(value).current_attempt == old_attempt.next()
            and core.owner_table.snapshot(value).state is ObjectState.PENDING
            for value in pending.output_ids
        )
        assert tuple(core.owner_table.snapshot(output).local_tokens for output in pending.output_ids) == local_tokens
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == old_attempt.next()
        assert record.state is TaskState.RETRY_PENDING and record.retries_started == 1
        assert isinstance(record.last_error, SystemTaskError) and str(record.last_error) == "retry"
        assert core._recovery.active_recovery(pending.task_id) is None
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {output: retried for output in pending.output_ids}
        assert not core._protocol_unresolved
        before_stale = _admission_state(core, retried)
        # A real successor is admitted; the old epoch cannot finish its count
        # or claim its barriers, nor consume another retry budget entry.
        assert not core._finish_pending_task(pending)
        assert _admission_state(core, retried) == before_stale
        assert core._retry_system_failure(pending, RuntimeError("retry"))
        assert _admission_state(core, retried) == before_stale
        _finish_unleased_admission(core, retried, refs)
    finally:
        _close_admission_handles(core, refs)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_admission_runtime")
def test_retry_owner_preflight_failure_consumes_no_recovery_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, refs = make_pure_core(), ()
    try:
        pending, refs = _register_three_slot_admission(core)
        assert _three_slot_take_submissions(core) == (pending,)
        before = _admission_state(core, pending)
        preflight = core.owner_table.validate_advance_task_outputs
        calls = []
        error = RuntimeError("owner preflight")

        def preflight_then_fail(expected, next_attempt):
            assert expected == pending.execution and next_attempt == pending.spec.attempt_id.next()
            plan = preflight(expected, next_attempt)
            calls.append(plan)
            assert len(calls) == 1 and _admission_state(core, pending) == before
            raise error

        monkeypatch.setattr(core.owner_table, "validate_advance_task_outputs", preflight_then_fail)
        with pytest.raises(RuntimeError, match="owner preflight") as caught:
            core._retry_system_failure(pending, SystemTaskError("retry"))
        assert caught.value is error and len(calls) == 1
        assert _admission_state(core, pending) == before
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert core._accepted_task_count == 1
        assert all(
            core.owner_table.snapshot(value).current_attempt == pending.spec.attempt_id
            and core.owner_table.snapshot(value).state is ObjectState.PENDING
            for value in pending.output_ids
        )
        assert all(ref._finalizer.alive and not ref._release_done.is_set() for ref in refs)
        # No retry was admitted. End this original, never-leased execution
        # through the separate real local-terminal path after the fault checks.
        _finish_unleased_admission(core, pending, refs)
        assert len(calls) == 1
    finally:
        _close_admission_handles(core, refs)


def _terminal_owner_facts(core, pending):
    """Local publication authority, excluding legitimate transport replay work."""
    return (
        tuple(core.owner_table.snapshot(output) for output in pending.output_ids),
        replace(core._recovery.task_record(pending.task_id)),
        tuple(core._recovery.reconstruction_snapshot(output) for output in pending.output_ids),
        dict(core._stored_descriptors), core._accepted_task_count, core._inflight_submissions,
        dict(core._task_finish_barriers), dict(core._object_gc_obligations),
        tuple(core._objects[output].event.is_set() for output in pending.output_ids),
    )


def _preflight_replay_transport(monkeypatch, fixture):
    """Exactly eight observable calls including both real terminal reports.

    The original seven-call fixture remains unchanged. This one continuation
    requires an additional terminal report, recorded in the same full call
    ledger rather than bypassed, dropped, or acknowledged without authority.
    """
    terminal_reports = []

    def rpc(address, handler, request):
        fixture.calls.append((handler, request))
        assert len(fixture.calls) <= 8
        if handler == "drop_object_replica":
            assert address == fixture.core.node_address and not fixture.drops
            result = fixture.envelope.results[1]
            assert request == protocol.DropObjectReplica(
                result.object_id, fixture.pending.spec.attempt_id, fixture.core.worker_id, fixture.node.node_id, result.checksum,
            )
            reply = fixture.node._handle_drop_object_replica(request)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            fixture.drops.append((request, reply))
            return reply
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == fixture.core.gcs_address
            if type(request) is wire.ReportOutputPublicationTerminal:
                assert request.witness == fixture.envelope.complete and len(terminal_reports) < 2
                ack = fixture.recovery.report_terminal(request.witness)
                terminal_reports.append((request, ack))
            elif type(request) is wire.ReportOutputPublicationAdopted:
                assert request.proof.complete == fixture.envelope.complete
                assert fixture.core.owner_table.output_owner_publication_receipt(
                    OutputOwnerPublicationPlan(fixture.pending.execution, fixture.envelope),
                ).committed
                ack = fixture.recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                assert request.proof.complete == fixture.envelope.complete
                assert request.proof.slot_index == len(fixture.slot_reports) < 3
                assert fixture.core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                if request.proof.slot_index == 1:
                    assert len(fixture.drops) == 1 and fixture.node.object_store.used_bytes == 0
                fixture.slot_reports.append(request.proof)
                ack = fixture.recovery.report_slot_collected(request.proof)
            return wire.OutputRecoveryReply(request, ack)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER and address == fixture.core.node_address
        assert fixture.recovery.snapshot(fixture.identity).adopted == request.proof
        return fixture.node._handle_ack_output_publication_adopted(request)

    monkeypatch.setattr(fixture.core, "_rpc", rpc)
    return terminal_reports


def _collect_local_terminal_batch(core, pending, refs):
    """Finish an already-published local error; never invent Worker success."""
    assert len(refs) == len(pending.output_ids) == 3
    assert all(core.owner_table.snapshot(output).state is ObjectState.ERROR for output in pending.output_ids)
    assert core._finish_pending_task(pending)
    assert core._accepted_task_count == 0 and not core._task_finish_barriers
    assert _three_slot_take_submissions(core) == ()
    for ref in refs:
        _three_slot_release(ref)
    fifo = core._reference_mailbox.pending
    count = fifo.qsize()
    assert count <= 16
    for _ in range(count):
        event = fifo.get_nowait()
        try:
            assert type(event) is _RetryInlineGc and event.object_id in pending.output_ids
            core._reference_released(event.object_id)
        finally:
            fifo.task_done()
    assert fifo.empty() and fifo.unfinished_tasks == 0
    assert not core._objects and not core._object_gc_obligations and not core._stored_descriptors
    assert all(core._recovery.lineage_for_object(output) is None for output in pending.output_ids)
    assert all(core.owner_table.collection_state(output) is ObjectCollectionState.COLLECTED for output in pending.output_ids)
    assert _three_slot_take_submissions(core) == ()


@pytest.mark.parametrize("status", ["success", "application", "terminal"])
@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_owner_terminal_preflight_failure_does_not_mutate_recovery_or_siblings(
    monkeypatch: pytest.MonkeyPatch, status: str,
) -> None:
    if status == "success":
        f = _ThreeSlotTerminal(monkeypatch)
        core, pending = f.core, f.pending
        try:
            # Preserve all three siblings; the current fixture additionally
            # includes a real STORED middle slot instead of the old all-INLINE
            # descriptor-only success. There is one actual Node Complete.
            reply = f.complete()
            before = _terminal_owner_facts(core, pending)
            physical_before = _publication_physical_state(f)
            terminal_reports = _preflight_replay_transport(monkeypatch, f)
            actual_preflight = core.owner_table.validate_output_publication
            preflights = []
            error = RuntimeError("owner preflight")

            def fail_preflight(plan):
                assert plan == OutputOwnerPublicationPlan(pending.execution, f.envelope)
                disposition = actual_preflight(plan)
                preflights.append((plan, disposition))
                assert len(preflights) == 1
                assert _terminal_owner_facts(core, pending) == before
                raise error

            with monkeypatch.context() as patch:
                patch.setattr(core.owner_table, "validate_output_publication", fail_preflight)
                # The unified adapter retains exact replay work on this local
                # exception. The historical escaping RuntimeError is obsolete.
                assert not core._publish_reply(pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
            assert len(preflights) == len(terminal_reports) == 1
            assert _terminal_owner_facts(core, pending) == before
            assert len(f.calls) == 1 and not f.wakes
            assert f.calls[0] == (wire.REPORT_OUTPUT_PUBLICATION_HANDLER, terminal_reports[0][0])
            registered = f.recovery.snapshot(f.identity)
            assert registered.complete == f.envelope.complete and registered.adopted is None
            physical_after = _publication_physical_state(f)
            # GCS legitimately learned terminal metadata; Node custody,
            # sealed bytes, ledger and completed lease did not change.
            assert (physical_after[0], physical_after[2:]) == (physical_before[0], physical_before[2:])
            assert core.owner_table.output_owner_publication_receipt(OutputOwnerPublicationPlan(pending.execution, f.envelope)) is None
            assert core._submissions.qsize() == 1
            delayed = core._submissions.get_nowait()
            core._submissions.task_done()
            assert type(delayed) is _DelayedReadyTask and type(delayed.ready) is _ReadyTask
            ready = delayed.ready
            assert ready.pending is pending and ready.spec == pending.spec and ready.dependencies == ()
            assert ready.output_adoption is not None and ready.output_adoption.envelope == f.envelope
            assert ready.output_adoption.round == 1
            assert ready.output_adoption.node_id == f.node.node_id
            marker = core._protocol_unresolved[pending.task_key]
            assert marker.phase == "output_adoption_wait" and marker.obligation == ready.output_adoption
            assert marker.output_candidate == f.identity
            assert core._output_result_custody[f.identity] == f.envelope
            assert not core._finish_pending_task(pending)
            assert _terminal_owner_facts(core, pending) == before
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
            assert core._execute(pending, ready.spec, ready.dependencies, output_adoption=ready.output_adoption)
            assert len(terminal_reports) == 2 and terminal_reports[0][0] == terminal_reports[1][0]
            assert len(preflights) == 1 and not core._protocol_unresolved
            assert f.journal.snapshot(f.identity).complete == f.envelope.complete
            f.finish_and_collect()
            assert len(f.calls) == 8 and len(f.slot_reports) == 3 and len(f.drops) == 1
        finally:
            f.close()
        return

    core, refs = make_pure_core(), ()
    try:
        pending, refs = _register_three_slot_admission(core)
        assert _three_slot_take_submissions(core) == (pending,)
        before = _admission_state(core, pending)
        error = TaskError("user") if status == "application" else SystemTaskError("terminal")
        kind = FailureKind.APPLICATION if status == "application" else None
        actual_preflight = core.owner_table.validate_publish_task_error
        preflights = []

        def fail_error_preflight(execution, candidate):
            assert execution == pending.execution and candidate is error
            plan = actual_preflight(execution, candidate)
            assert plan is not None
            preflights.append(plan)
            assert len(preflights) == 1 and _admission_state(core, pending) == before
            raise RuntimeError("owner preflight")

        with monkeypatch.context() as patch:
            patch.setattr(core.owner_table, "validate_publish_task_error", fail_error_preflight)
            assert not core._publish_task_error(pending, error, failure_kind=kind)
        assert len(preflights) == 1 and _admission_state(core, pending) == before
        assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
        assert not any(core._objects[output].event.is_set() for output in pending.output_ids)
        assert core._publish_task_error(pending, error, failure_kind=kind)
        assert all(core.owner_table.snapshot(output).error is error for output in pending.output_ids)
        assert core._recovery.task_record(pending.task_id).state is (
            TaskState.APPLICATION_FAILED if status == "application" else TaskState.SYSTEM_FAILED
        )
        _collect_local_terminal_batch(core, pending, refs)
    finally:
        _close_admission_handles(core, refs)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_task_lifecycle_tables_use_task_id_not_slot_zero(monkeypatch) -> None:
    f = _ThreeSlotTerminal(monkeypatch)
    core, pending = f.core, f.pending
    try:
        reply = f.complete()
        core._mark_protocol_unresolved(pending, "test", output_candidate=f.identity)
        assert set(core._protocol_unresolved) == {pending.task_id}
        assert pending.object_id not in core._protocol_unresolved
        assert core._clear_protocol_unresolved(pending)
        assert core._publish_reply(pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
        assert core._finish_pending_task(pending)
        assert pending.task_id in core._finished_tasks
        assert pending.object_id not in core._finished_tasks
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        f.finish_and_collect()
    finally:
        f.close()


class _StoredLifecycleZero:
    """Exercise slot-0 storage with a bounded stream that still decodes to 0."""
    def __reduce__(self):
        return int, ("0" + " " * 128,)


def _never_execute_lifetime(value=None):
    pytest.fail("lifetime fixture executed the user function")


class _SiblingLifetime:
    """Two canonical Tasks: pending dependency binding, publication and GC."""
    def __init__(self, monkeypatch, *, stored_first=False):
        self.core = core = make_pure_core()
        core.gcs_address = ("sibling-lifetime-control.invalid", 1)
        self.stored_first, self.live_reference = stored_first, False
        self.handles, self.leases, self.pushes, self.calls = [], [], [], []
        self.replies, self.slot_reports = {}, {}
        self.lineage_releases, self.drops, self.delayed = [], [], []
        self.producer = self.consumer = self.dependency = None
        self.refs = ()
        self.node = node = object.__new__(NodeServer)
        node.node_id, node.worker_id = core.node_id, WorkerID.random()
        node._node_pid, node._registration_epoch = 32701, 1
        node._gcs_address, node._registered_with_gcs = None, False
        node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
        node._cluster_addresses = {}
        node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
        node._stop_event = threading.Event()
        node._shutdown_request_id, node._active_lease_id = None, None
        node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
        node._lease_request_locks, node._inflight_lease_requests = {}, 0
        node._worker_process = type("PassiveWorker", (), {"is_alive": lambda _self: True})()
        node._worker_address = ("sibling-lifetime-worker.invalid", 1)
        node.event_sink = None
        node._object_store = ObjectStore(1024)
        node._object_manager = ObjectManager(node.node_id, node._object_store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._local_replica_write_claims, node._object_localization_locks = {}, {}
        self.journal = node._output_publication_journal = OutputPublicationJournal()
        self.recovery = OutputPublicationRecoveryAuthority()

        def forbidden(*_args, **_kwargs):
            pytest.fail("ref-free sibling lifetime attempted child/graph/rollback work")

        self.adapter = node._output_publications = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent, arm_complete=self.recovery.arm_complete,
            report_terminal=self.recovery.report_terminal, report_rollback=self.recovery.report_rollback,
            prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
            prepare_graph=forbidden, abort_graph=forbidden, seal_replica=node._seal_output_publication_replica,
            drop_replica=forbidden,
        )
        monkeypatch.setattr(core, "_rpc", self.rpc)
        monkeypatch.setattr(core, "_schedule_reference_event", self.schedule_gc)
        actual_release = core.owner_table.release_lineage_reference

        def release_lineage(output, token):
            assert output == self.producer.object_id and token == self.token
            assert not self.lineage_releases
            assert all(not core.owner_table.contains(value) for value in self.consumer.output_ids)
            released = actual_release(output, token)
            assert released
            self.lineage_releases.append((output, token))
            return released

        monkeypatch.setattr(core.owner_table, "release_lineage_reference", release_lineage)

    def register(self):
        core = self.core
        definition = core.define_remote_function(_never_execute_lifetime)
        self.producer, self.dependency = core._register_submission(
            definition, (), {}, ResourceVector({"CPU": 1}), _enqueue=True,
        )
        self.handles.append(self.dependency)
        self.consumer, self.refs = core._register_submission(
            definition, (self.dependency,), {}, ResourceVector({"CPU": 1}), num_returns=3, _enqueue=True,
        )
        self.handles.extend(self.refs)
        assert _three_slot_take_submissions(core) == (self.producer, self.consumer)
        assert len(self.refs) == 3 and core._accepted_task_count == 2
        assert tuple(ref.object_id for ref in self.refs) == self.consumer.output_ids
        assert core.owner_table.snapshot(self.producer.object_id).state is ObjectState.PENDING
        assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in self.consumer.output_ids)
        self.token = "lineage:{}:{}".format(self.consumer.task_id, self.producer.object_id)
        self.hold = self.consumer.dependency_hold
        parent = core.owner_table.snapshot(self.producer.object_id)
        assert parent.submitted_tokens == frozenset({self.hold})
        assert parent.lineage_tokens == frozenset({self.token})
        self.edges = core.owner_table.task_lineage_edges(self.consumer.task_id)
        assert len(self.edges) == 1 and next(iter(self.edges)).dependency_object_id == self.producer.object_id
        assert self.consumer.protected_dependencies == (self.producer.object_id,)
        assert type(self.consumer.spec.args[0]) is protocol.RefArg
        assert not core._dependencies_ready(self.consumer)

    def publish(self, pending, values):
        assert len(self.leases) < 2
        core, node = self.core, self.node
        prepared, dependencies, protected = core._prepare_task_dependencies(pending.spec)
        assert protected == pending.protected_dependencies and dependencies == ()
        if pending is self.consumer:
            assert type(pending.spec.args[0]) is protocol.RefArg
            assert type(prepared.args[0]) is protocol.InlineArg
            assert cloudpickle.loads(prepared.args[0].data) == 1
        request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, target_node_id=node.node_id, return_ids=pending.output_ids,
        )
        grant = node._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease
        self.leases.append((request, grant))
        push = protocol.PushTask(grant.lease_id, grant.worker_id, prepared)
        self.pushes.append(push)  # prepared wire args, canonical lineage remains RefArg
        assert node._handle_start_worker_lease(protocol.StartWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id, grant.worker_id,
        )).accepted
        identity = OutputPublicationID(grant.lease_id, pending.execution)
        session = OutputDiscoverySession(OutputPublicationHeader(
            identity, pending.spec.job_id, grant.worker_id, core.worker_id,
            OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
        ), inline_threshold=64)
        outputs = session.discover(tuple(values))
        expected = ((protocol.ResultStorage.OBJECT_STORE, protocol.ResultStorage.INLINE, protocol.ResultStorage.INLINE)
                    if self.stored_first and pending is self.consumer else
                    (protocol.ResultStorage.INLINE,) * len(pending.output_ids))
        assert tuple(slot.tier for slot in outputs.manifest.slots) == expected
        assert sum(map(len, outputs.slot_payloads)) <= 512
        assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            outputs.manifest, outputs.slot_payloads,
        )).accepted
        assert self.recovery.snapshot(identity).armed and node.resource_ledger.available.is_zero()
        session.release_sources_after_promotions()
        completion = protocol.CompleteWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id, grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
        )
        completed = node._handle_complete_worker_lease(completion)
        assert completed.accepted and completed.released
        envelope = completed.output_publication
        assert envelope.manifest == outputs.manifest
        reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
            envelope.results, output_publication=envelope,
        )
        self.replies[identity] = reply
        self.slot_reports[identity] = []
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=grant.lease_id)
        assert node.resource_ledger.available == node.resource_ledger.total
        assert core._finish_pending_task(pending)
        return reply

    def publish_both(self):
        self.publish(self.producer, (1,))
        assert self.core.owner_table.snapshot(self.producer.object_id).state is ObjectState.READY_INLINE
        assert self.core._accepted_task_count == 1
        assert self.hold in self.core.owner_table.snapshot(self.producer.object_id).submitted_tokens
        assert self.core._dependencies_ready(self.consumer)
        self.consumer_reply = self.publish(
            self.consumer, (_StoredLifecycleZero(), 1, 2) if self.stored_first else (0, 1, 2),
        )
        assert self.core._accepted_task_count == 0 and not self.core._task_finish_barriers
        parent = self.core.owner_table.snapshot(self.producer.object_id)
        assert not parent.submitted_tokens and parent.lineage_tokens == frozenset({self.token})
        assert _three_slot_take_submissions(self.core) == ()
        if not self.live_reference:
            self.gc_notices()

    def rpc(self, address, handler, request):
        self.calls.append((handler, request))
        assert len(self.calls) <= 12
        if handler == "drop_object_replica":
            assert self.stored_first and address == self.core.node_address and len(self.drops) < 2
            result = self.consumer_reply.results[0]
            assert request == protocol.DropObjectReplica(
                result.object_id, self.consumer.spec.attempt_id, self.core.worker_id, self.node.node_id, result.checksum,
            )
            reply = self.node._handle_drop_object_replica(request)
            self.drops.append((request, reply))
            return reply
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.core.gcs_address
            if type(request) is wire.ReportOutputPublicationTerminal:
                ack = self.recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                envelope = self.replies[request.proof.complete.publication_id].output_publication
                assert self.core.owner_table.output_owner_publication_receipt(
                    OutputOwnerPublicationPlan(envelope.manifest.execution, envelope),
                ).committed
                ack = self.recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                identity = request.proof.complete.publication_id
                reports = self.slot_reports[identity]
                assert request.proof not in reports and len(reports) < len(identity.output_ids)
                assert self.core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                reports.append(request.proof)
                ack = self.recovery.report_slot_collected(request.proof)
            return wire.OutputRecoveryReply(request, ack)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER and address == self.core.node_address
        assert self.recovery.snapshot(request.proof.complete.publication_id).adopted == request.proof
        return self.node._handle_ack_output_publication_adopted(request)

    def schedule_gc(self, mailbox, event, delay):
        assert self.stored_first and not self.live_reference and not self.delayed
        assert mailbox is self.core._reference_mailbox and type(event) is _RetryInlineGc
        assert event.object_id == self.consumer.output_ids[0] and 0 < delay <= 0.25
        self.delayed.append((mailbox, event, delay))

    def gc_notices(self):
        assert not self.live_reference
        fifo = self.core._reference_mailbox.pending
        assert fifo.qsize() <= 16
        for _ in range(16):
            try:
                event = fifo.get_nowait()
            except queue.Empty:
                return
            try:
                assert type(event) is _RetryInlineGc and event.object_id in tuple(ref.object_id for ref in self.handles)
                self.core._reference_released(event.object_id)
            finally:
                fifo.task_done()
        assert fifo.empty(), "sibling GC exceeded sixteen explicit events"

    def assert_consumer_collected(self):
        core = self.core
        assert all(core.owner_table.collection_state(output) is ObjectCollectionState.COLLECTED for output in self.consumer.output_ids)
        assert not core.owner_table.task_lineage_edges(self.consumer.task_id)
        assert self.lineage_releases == [(self.producer.object_id, self.token)]
        assert self.token not in core.owner_table.snapshot(self.producer.object_id).lineage_tokens
        assert all(core._recovery.lineage_for_object(output) is None for output in self.consumer.output_ids)
        assert core.owner_table.lineage_release_was_seen(self.producer.object_id, self.token)

    def assert_collected(self):
        core = self.core
        assert all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED for ref in self.handles)
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert not core._protocol_unresolved and core._accepted_task_count == 0
        assert all(core._recovery.lineage_for_object(ref.object_id) is None for ref in self.handles)
        assert self.lineage_releases == [(self.producer.object_id, self.token)]
        assert len(self.leases) == len(self.pushes) == len(self.replies) == 2
        for identity in self.replies:
            assert len(self.recovery.snapshot(identity).slot_collections) == len(identity.output_ids)
            assert self.adapter.report_terminal(identity)
            assert not self.journal.snapshot(identity).retained_result_slots
        assert not self.adapter.pending_terminal_reports() and not self.adapter.pending_lease_completions()
        assert self.node.resource_ledger.available == self.node.resource_ledger.total
        assert self.node.object_store.used_bytes == 0 and not self.node._sealed_metadata
        with self.node._state_lock:
            assert self.node._output_publications_clean_locked()
        assert _three_slot_take_submissions(core) == ()

    def close_pure(self):
        assert not self.live_reference
        for ref in self.handles:
            _three_slot_release(ref)
        close_pure_core(self.core)


@pytest.mark.parametrize(
    "close_order",
    ((0, 1, 2), (2, 0, 1), (1, 2, 0)),
)
@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_dependency_lineage_is_task_scoped_and_releases_with_last_sibling(
    close_order: tuple[int, int, int], monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _SiblingLifetime(monkeypatch)
    core = f.core
    try:
        f.register()  # both Tasks are still PENDING when the holds are bound
        producer, consumer, refs = f.producer, f.consumer, f.refs
        assert isinstance(refs, tuple) and len(refs) == 3
        assert f.token in core.owner_table.snapshot(producer.object_id).lineage_tokens
        assert len(f.edges) == 1 and next(iter(f.edges)).dependency_object_id == producer.object_id
        f.publish_both()
        for position, index in enumerate(close_order):
            _three_slot_release(refs[index])
            f.gc_notices()
            assert core.owner_table.collection_state(refs[index].object_id) is ObjectCollectionState.COLLECTED
            if position < len(close_order) - 1:
                assert f.token in core.owner_table.snapshot(producer.object_id).lineage_tokens
                assert core.owner_table.task_lineage_edges(consumer.task_id) == f.edges
                assert not f.lineage_releases
        f.assert_consumer_collected()
        assert not f.drops and not f.delayed
        # Unlike the obsolete PENDING-producer fixture, this producer really
        # succeeded. Observe its released lineage before closing its last ref,
        # and then require actual collection rather than snapshotting a corpse.
        _three_slot_release(f.dependency)
        f.gc_notices()
        f.assert_collected()
    finally:
        f.close_pure()


class _SiblingCloseProbe:
    """Own exactly three close threads and one actual reference consumer."""
    def __init__(self, monkeypatch):
        self.threads, self.starts, self.joins, self.errors, self.gc_observations = [], [], [], [], []
        self.error_overflow = False
        self.lineage_threads = []
        self.lock = threading.Lock()
        self.consumer_collected, self.producer_collected = threading.Event(), threading.Event()
        self.barrier = None
        self.fixture = None
        probe, real_thread = self, threading.Thread

        class _OwnedThread(real_thread):
            def __init__(thread, *args, **kwargs):
                super().__init__(*args, **kwargs)
                allowed = {"miniray-core-reference-events", *("miniray-sibling-close-{}".format(i) for i in range(3))}
                with probe.lock:
                    valid = (thread.name in allowed and len(probe.threads) < 4
                             and not any(item.name == thread.name for item in probe.threads))
                    if valid:
                        probe.threads.append(thread)
                if not valid:
                    probe.forbidden("unexpected sibling thread")

            def start(thread):
                with probe.lock:
                    valid = thread in probe.threads and thread not in probe.starts and len(probe.starts) < 4
                    if valid:
                        probe.starts.append(thread)
                if not valid:
                    probe.forbidden("unexpected sibling start")
                return super().start()

            def join(thread, timeout=None):
                if (thread not in probe.threads or type(timeout) not in (int, float)
                        or not math.isfinite(timeout) or not 0 <= timeout <= 2.0):
                    probe.forbidden("unbounded sibling join")
                with probe.lock:
                    valid = len(probe.joins) < 8
                    if valid:
                        probe.joins.append((thread, timeout))
                if not valid:
                    probe.forbidden("too many sibling joins")
                return super().join(timeout)

            def run(thread):
                try:
                    super().run()
                except BaseException as exc:
                    probe.record_error(exc)

        monkeypatch.setattr(threading, "Thread", _OwnedThread)
        for kind, method in ((CoreWorker, "__init__"), (NodeServer, "__init__"),
                             (CoreWorker, "_execute"), (threading.Timer, "start"),
                             (multiprocessing.process.BaseProcess, "start"),
                             (multiprocessing.process.BaseProcess, "join"), (queue.Queue, "join")):
            monkeypatch.setattr(kind, method, self.forbidden)
        for method in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, method, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        monkeypatch.setattr(core_module, "rpc_request", self.forbidden)
        monkeypatch.setattr(node_module, "rpc_request", self.forbidden)

    def forbidden(self, *_args, **_kwargs):
        error = AssertionError("sibling-close L1 attempted unbounded/unmodelled work")
        self.record_error(error)
        raise error

    def record_error(self, error):
        with self.lock:
            if len(self.errors) < 16:
                self.errors.append((threading.current_thread(), error))
            else:
                self.error_overflow = True

    def start_reference(self, monkeypatch, fixture):
        self.fixture = fixture
        core = fixture.core
        actual_gc = core._reference_released
        actual_release = core.owner_table.release_lineage_reference

        def observe_lineage_release(output, token):
            assert threading.current_thread() is core._reference_thread
            assert not self.lineage_threads
            self.lineage_threads.append(threading.current_thread())
            return actual_release(output, token)

        def observe_gc(output):
            try:
                actual_gc(output)
                with self.lock:
                    valid = len(self.gc_observations) < 32
                    if valid:
                        self.gc_observations.append(output)
                if not valid:
                    self.forbidden("too many reference-consumer events")
                if fixture.consumer is not None and all(
                    core.owner_table.collection_state(value) is ObjectCollectionState.COLLECTED
                    for value in fixture.consumer.output_ids
                ):
                    self.consumer_collected.set()
                if fixture.producer is not None and core.owner_table.collection_state(fixture.producer.object_id) is ObjectCollectionState.COLLECTED:
                    self.producer_collected.set()
            except BaseException as exc:
                # The real reference loop has finally/continue branches. A
                # callback failure must stay visible to the main test.
                self.record_error(exc)
                raise

        monkeypatch.setattr(core, "_reference_released", observe_gc)
        monkeypatch.setattr(core.owner_table, "release_lineage_reference", observe_lineage_release)
        assert not fixture.handles and core._reference_mailbox.pending.empty()
        fixture.live_reference = True
        # Call the actual lazy initializer before any handle/token is created.
        # Only its mailbox/thread replace the empty pure infrastructure.
        CoreWorker._initialize_reference_events(core)
        assert core._reference_thread in self.threads and core._reference_thread.is_alive()

    def close_siblings(self, refs):
        assert len(refs) == 3
        self.barrier = threading.Barrier(3, timeout=1.0)
        deadline = time.monotonic() + 2.0

        def close(ref):
            self.barrier.wait(timeout=min(1.0, max(0.0, deadline - time.monotonic())))
            ref.close(timeout=min(1.0, max(0.0, deadline - time.monotonic())))

        closers = tuple(threading.Thread(
            target=close, args=(ref,), name="miniray-sibling-close-{}".format(index), daemon=True,
        ) for index, ref in enumerate(refs))
        for thread in closers:
            thread.start()
        for thread in closers:
            thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in closers)
        assert not self.errors and not self.error_overflow
        assert self.consumer_collected.wait(timeout=1.0)
        assert all(ref.closed and ref._release_done.is_set() for ref in refs)
        assert len(self.threads) == len(self.starts) == 4
        assert self.lineage_threads == [self.fixture.core._reference_thread]

    def cleanup(self):
        """Stop only owned threads; never reenter owner state or close refs."""
        deadline = time.monotonic() + 2.0
        if self.barrier is not None:
            self.barrier.abort()
        core = None if self.fixture is None else self.fixture.core
        mailbox = None if core is None else getattr(core, "_reference_mailbox", None)
        if mailbox is not None:
            # These two real operations take only the short mailbox lock,
            # never Core's possibly contended owner/state lock. Queue puts
            # are nonblocking; the process runner still bounds lock failures.
            mailbox.close_admission()
            if hasattr(mailbox, "stop"):
                mailbox.stop()
        for thread in self.threads:
            if thread.ident is not None and thread.is_alive():
                thread.join(max(0.0, deadline - time.monotonic()))
        live = tuple(thread for thread in self.threads if thread.is_alive())
        if live and self.fixture is not None:
            # The outer runner owns a stuck thread's process. Do not allow
            # future Python GC finalizers to reenter the fenced mailbox/locks,
            # and do not pretend these owner tokens were successfully released.
            for ref in self.fixture.handles:
                if ref._finalizer is not None:
                    ref._finalizer.detach()
        finalizer = None if core is None else getattr(core, "_reference_runtime_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
        assert not live
        assert not self.errors and not self.error_overflow
        assert not set(self.threads).intersection(threading.enumerate())


@pytest.mark.loopback_smoke
def test_concurrent_sibling_closes_claim_task_lineage_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _SiblingCloseProbe(monkeypatch)
    f = _SiblingLifetime(monkeypatch)
    try:
        probe.start_reference(monkeypatch, f)
        f.register()
        f.publish_both()
        probe.close_siblings(f.refs)
        f.assert_consumer_collected()
        f.dependency.close(timeout=1.0)
        assert probe.producer_collected.wait(timeout=1.0)
        assert f.core._stop_reference_events(time.monotonic() + 1.0)
        assert not f.core._reference_thread.is_alive()
        assert f.core._reference_mailbox.stopped.is_set()
        assert not f.core._reference_mailbox.accepting
        assert f.core._reference_mailbox.events.empty()
        assert f.core._reference_mailbox.events.unfinished_tasks == 0
        assert not f.core._gc_retry_timers_open
        assert not f.core._reference_runtime_finalizer.alive
        assert not probe.errors and not probe.error_overflow and not f.core._gc_retry_timers
        f.assert_collected()
        assert not f.drops and not f.delayed
        assert all(ref.closed and ref._release_done.is_set() for ref in f.handles)
    finally:
        probe.cleanup()


@pytest.mark.unit
def test_static_multi_return_contained_refs_require_unified_publication(monkeypatch) -> None:
    """Pure: two selected outputs/four transfers, actual Node/owner reducers."""
    from tests.unit._pure_core import close_pure_core
    from tests.unit.test_core_output_publication import _fixture
    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    publication, node, core, pending, reply, _calls, _rpc = _fixture(refs=True)
    try:
        envelope = reply.output_publication
        assert envelope is not None and len(envelope.manifest.slots) == 2
        assert all(slot.transfers for slot in envelope.manifest.slots)
        assert not hasattr(reply, "contained_edges")
        before = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        with pytest.raises(TypeError, match="unexpected keyword argument.*contained_edges"):
            protocol.TaskReply(
                pending.task_id, pending.spec.attempt_id, envelope.manifest.header.executor_worker_id,
                protocol.TaskReplyStatus.SUCCEEDED, envelope.results,
                contained_edges=envelope.manifest.ordered_edges,
            )
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before
        assert core._publish_reply(
            pending, reply, expected_node_id=node.node_id,
            expected_lease_id=publication.id.lease_id,
        )
        for slot in envelope.manifest.slots:
            owner = core.owner_table.snapshot(slot.object_id)
            assert owner.output_publication.manifest == envelope.manifest
            assert owner.outgoing_contained_edges == frozenset(slot.edges)
        assert core._finish_pending_task(pending)
    finally:
        for output in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(output).local_tokens):
                assert core.owner_table.release_local_reference(output, token)
        close_pure_core(core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_three_slot_runtime")
def test_collecting_stored_sibling_does_not_own_or_move_task_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _SiblingLifetime(monkeypatch, stored_first=True)
    core = f.core
    pin = None
    try:
        f.register()
        producer, consumer, refs = f.producer, f.consumer, f.refs
        f.publish_both()
        stored_id = consumer.output_ids[0]
        assert tuple(result.storage for result in f.consumer_reply.results) == (
            protocol.ResultStorage.OBJECT_STORE, protocol.ResultStorage.INLINE, protocol.ResultStorage.INLINE,
        )
        pin = f.node.object_store.pin(stored_id, ("keep-collecting-sibling", stored_id))
        _three_slot_release(refs[0])
        f.gc_notices()
        assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTING
        assert len(f.drops) == 1 and f.drops[0][1].status is protocol.DropObjectReplicaStatus.PINNED
        assert f.node.object_store.snapshot(stored_id).pin_count == 1
        assert f.node.object_store.contains(stored_id)
        obligation = core._object_gc_obligations[stored_id]
        plan = obligation.plan
        assert plan.lineage_releases == ()  # not a slot-owned/moved Task hold
        assert core.owner_table.task_lineage_edges(consumer.task_id) == f.edges
        assert len(f.delayed) == 1 and not f.lineage_releases
        for index in (2, 1):
            _three_slot_release(refs[index])
            f.gc_notices()
        assert f.token in core.owner_table.snapshot(producer.object_id).lineage_tokens
        assert core.owner_table.task_lineage_edges(consumer.task_id) == f.edges
        assert core._object_gc_obligations[stored_id] is obligation and obligation.plan == plan
        assert not f.lineage_releases and len(f.drops) == 1
        assert f.node.object_store.unpin(stored_id, pin)
        pin = None
        core._reference_released(stored_id)
        assert len(f.drops) == 2 and f.drops[0][0] == f.drops[1][0]
        assert f.drops[1][1].status is protocol.DropObjectReplicaStatus.DROPPED
        f.assert_consumer_collected()
        mailbox, event, _delay = f.delayed[0]
        before = tuple(f.calls), tuple(f.lineage_releases), tuple(f.drops)
        assert mailbox.enqueue_internal(event)  # deliver, do not erase the old timer record
        f.gc_notices()
        assert (tuple(f.calls), tuple(f.lineage_releases), tuple(f.drops)) == before
        _three_slot_release(f.dependency)
        f.gc_notices()
        f.assert_collected()
    finally:
        if pin is not None:
            assert f.node.object_store.unpin(f.consumer.output_ids[0], pin)
        f.close_pure()
