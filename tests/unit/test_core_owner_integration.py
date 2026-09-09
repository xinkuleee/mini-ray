"""Threadless owner/Core composition with real publication boundaries.

At most two Tasks and two tiny INLINE completions use the real owner, recovery,
discovery and Node journal/adapter reducers. Reference work advances through
the existing synchronous mailbox and a bounded manual FIFO. No constructor,
thread, process, socket, timer, user callable or real receipt wait runs.
The ERROR-before-finish case deliberately retains that exact arbitration cut.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time
from dataclasses import replace

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker,
    ObjectRef,
    RemoteFunctionDefinition,
    _DelayedReadyTask,
    _WAKE_COORDINATOR,
)
from miniray.errors import SystemTaskError
from miniray.ids import JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.lease_dependencies import LeaseDependencyCustody
from miniray.node import NodeServer, _WorkerSlot
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import NodeSnapshot, ResourceLedger, ResourceVector
from miniray.transfer_pins import TransferPinOutbox
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure owner composition attempted runtime work")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure owner close attempted a blocking wait"
        return True

    for kind, name in ((CoreWorker, "__init__"), (threading.Thread, "start"),
                       (threading.Thread, "join"), (threading.Timer, "start"),
                       (threading.Condition, "wait"), (threading.Barrier, "wait"),
                       (multiprocessing.process.BaseProcess, "start"),
                       (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _core_without_runtime() -> CoreWorker:
    core = make_pure_core()
    core.job_id = JobID(bytes.fromhex("11" * 16))
    core.worker_id = WorkerID(bytes.fromhex("22" * 16))
    core.node_id = NodeID(bytes.fromhex("33" * 16))
    core.driver_task_id = TaskID.for_driver(core.job_id)
    return core


def _definition(core: CoreWorker) -> RemoteFunctionDefinition:
    return RemoteFunctionDefinition.from_callable(lambda value: value, core.job_id)


def _claim_accepted(core, pending):
    """Claim the real FIFO item without executing a coordinator or user code."""
    count = core._submissions.qsize()
    assert 0 < count <= 16
    claimed = []
    for _ in range(count):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            claimed.append(item)
    assert claimed == [pending]
    assert core._task_finish_barriers[pending.object_id] == pending
    assert not core._submissions.unfinished_tasks


def _submit_accepted(core, args=(), *, max_retries=0):
    accepted = core._accepted_task_count
    pending, reference = core._register_submission(
        _definition(core), args, {}, ResourceVector(),
        max_retries=max_retries, _enqueue=True,
    )
    _claim_accepted(core, pending)
    assert core._accepted_task_count == accepted + 1
    return pending, reference


class _AliveLeaseWorker:
    pid = 4242

    def is_alive(self):
        return True


def _cancellation_node(node_id, worker_id):
    """Empty-dependency lease authority with one passive, existing Worker."""
    node = object.__new__(NodeServer)
    node.node_id, node.worker_id = node_id, worker_id
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._gcs_address, node._registered_with_gcs = None, False
    total = ResourceVector({"CPU": 1})
    node._ledger = ResourceLedger(total)
    node._cluster_nodes = (NodeSnapshot(node_id, total, total),)
    node._cluster_addresses = {}
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(
        worker_id, process=_AliveLeaseWorker(), address=("worker.invalid", 31), pid=4242,
    )}
    node.num_workers_per_node = 1
    node._leases, node._lease_outcomes = {}, {}
    node._lease_cancellations, node._lease_request_locks = {}, {}
    node._inflight_lease_requests = 0
    node._lease_dependency_custody = LeaseDependencyCustody(node_id)
    node._source_pin_releases = TransferPinOutbox()
    node._dependency_pin_cleanups = {}
    node._object_store = ObjectStore(1024)
    node._sealed_metadata = {}
    node.event_sink = None
    return node


def _outputs(core):
    outputs = PureOutputRuntime(core)
    core.gcs_address, core._rpc = outputs.gcs_address, outputs.rpc
    return outputs


def _reply(core, outputs, pending, value):
    prepared, dependencies, _ = core._prepare_task_dependencies(pending.spec)
    push = protocol.PushTask(LeaseID.random(), WorkerID.random(), prepared, dependencies)
    reply = outputs.complete(push, (value,))
    assert reply.output_publication is not None
    assert reply.output_publication.manifest.execution == pending.execution
    assert outputs.journal.snapshot(reply.output_publication.publication_id).complete == (
        reply.output_publication.complete
    )
    return reply


def _drain(core):
    assert core._reference_mailbox.pending.qsize() <= 16
    core._reference_mailbox.drain()
    count = core._submissions.qsize()
    assert count <= 16
    for _ in range(count):
        assert core._submissions.get_nowait() is _WAKE_COORDINATOR
        core._submissions.task_done()
    assert not core._submissions.unfinished_tasks
    assert not core._reference_mailbox.pending.unfinished_tasks


@pytest.mark.unit
def test_submission_registers_attempt_lineage_and_local_handle() -> None:
    core = _core_without_runtime()
    pending, ref = core._register_submission(
        _definition(core), (7,), {}, ResourceVector()
    )

    try:
        snapshot = core.owner_table.snapshot(ref.object_id)
        assert snapshot.state is ObjectState.PENDING
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert snapshot.producer_task_spec is pending.spec
        assert snapshot.local_tokens == frozenset({ref._local_token})
    finally:
        ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_recovery_registration_preflight_fails_before_owner_or_hold_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core_without_runtime()
    dependency, dependency_ref = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )
    recovery = core._recovery_manager()
    monkeypatch.setattr(
        recovery, "validate_register_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("registration conflict")
        ),
    )
    before_objects = set(core._objects)
    before = core.owner_table.snapshot(dependency.object_id)

    try:
        with pytest.raises(RuntimeError, match="registration conflict"):
            core._register_submission(
                _definition(core), (dependency_ref,), {}, ResourceVector()
            )

        assert set(core._objects) == before_objects
        after = core.owner_table.snapshot(dependency.object_id)
        assert after.lineage_tokens == before.lineage_tokens
        assert after.submitted_tokens == before.submitted_tokens
        assert after == before
        assert core._inflight_submissions == 0
    finally:
        dependency_ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_object_ref_pickle_contains_only_logical_handle() -> None:
    core = _core_without_runtime()
    pending, ref = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )

    try:
        before = core.owner_table.snapshot(ref.object_id)
        restored = pickle.loads(pickle.dumps(ref))
        assert restored == ObjectRef(pending.object_id, core.worker_id)
        assert restored.object_id == ref.object_id
        assert restored.owner_worker_id == core.worker_id
        assert restored._finalizer is restored._release_done is restored._local_token is None
        assert core.owner_table.snapshot(ref.object_id) == before
        restored.close()
        assert not restored.closed
    finally:
        ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_dependency_is_protected_until_dispatch_releases_hold() -> None:
    core = _core_without_runtime()
    outputs = _outputs(core)
    dependency, dependency_ref = _submit_accepted(core)
    assert core._publish_reply(dependency, _reply(core, outputs, dependency, 9))
    assert core._finish_pending_task(dependency)
    consumer, consumer_ref = _submit_accepted(core, (dependency_ref,))

    try:
        assert consumer.dependency_hold is not None
        prepared, descriptors, protected = core._prepare_task_dependencies(consumer.spec)
        assert len(prepared.args) == 1 and type(prepared.args[0]) is protocol.InlineArg
        assert cloudpickle.loads(prepared.args[0].data) == 9
        assert prepared.kwargs == () and descriptors == ()
        assert prepared.task_id == consumer.spec.task_id
        assert prepared.attempt_id == consumer.spec.attempt_id
        assert protected == (dependency.object_id,)
        assert core.owner_table.snapshot(
            dependency.object_id
        ).submitted_tokens == frozenset({consumer.dependency_hold})
        assert consumer.dependency_hold == protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED,
            core.worker_id,
            consumer.spec.task_id,
            consumer.spec.attempt_id,
        )

        assert core._publish_reply(consumer, _reply(core, outputs, consumer, 9))
        assert core._finish_pending_task(consumer)
        assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
        consumer_ref.close()
        dependency_ref.close()
        _drain(core)
        assert core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(dependency.object_id) is ObjectCollectionState.COLLECTED
        outputs.assert_collected()
    finally:
        consumer_ref.close()
        dependency_ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_reply_publishes_owner_state_before_waking_and_fences_stale_attempt(monkeypatch) -> None:
    core = _core_without_runtime()
    outputs = _outputs(core)
    pending, ref = _submit_accepted(core, max_retries=1)
    # A genuinely prepared old delivery can arrive after a retry. Register it
    # while its accepted attempt is still current; no stale admission is faked.
    payload = cloudpickle.dumps(42)
    stale_reply = _reply(core, outputs, pending, 42)
    failure = SystemTaskError("advance to a current retry attempt")
    assert not core._retry_system_failure(pending, failure)
    current = core._task_finish_barriers[pending.object_id]
    _claim_accepted(core, current)
    next_attempt = current.spec.attempt_id
    assert next_attempt == pending.spec.attempt_id.next()
    try:
        before_calls = tuple(outputs.calls)
        assert not core._publish_reply(pending, stale_reply)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert not core._objects[pending.object_id].event.is_set()
        assert tuple(outputs.calls) == before_calls and not core._protocol_unresolved

        current_reply = _reply(core, outputs, current, 42)
        observed = []
        original_wake = core._wake_object

        def observe_wake(object_id):
            snapshot = core.owner_table.snapshot(object_id)
            assert snapshot.state is ObjectState.READY_INLINE
            assert snapshot.inline_data == payload and snapshot.current_attempt == next_attempt
            assert not core._objects[object_id].event.is_set()
            observed.append(object_id)
            original_wake(object_id)

        monkeypatch.setattr(core, "_wake_object", observe_wake)
        assert core._publish_reply(current, current_reply)
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.READY_INLINE
        assert snapshot.inline_data == payload
        assert core._objects[pending.object_id].event.is_set()
        assert observed == [pending.object_id]
        assert core._finish_pending_task(current)
        ref.close()
        _drain(core)
        assert core.owner_table.collection_state(current.object_id) is ObjectCollectionState.COLLECTED
        assert not core._protocol_unresolved
    finally:
        ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_shutdown_error_fences_a_late_successful_reply() -> None:
    """A cancelled lease cannot authorize a late success before local finish.

    The old descriptor-only success is now a malformed protocol delivery, not
    a legal Complete envelope. A live Push/adoption instead prevents shutdown
    from choosing ERROR; publication continuations have separate regressions.
    """
    core = _core_without_runtime()
    pending, ref = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )
    node = _cancellation_node(core.node_id, WorkerID.random())
    lease_request = protocol.RequestWorkerLease(
        LeaseID.random(), pending.task_id, pending.spec.attempt_id,
        pending.spec.resources, core.node_id, core.worker_id,
        target_node_id=core.node_id, return_ids=pending.output_ids,
    )
    grant = node._handle_request_lease(lease_request)
    assert type(grant) is protocol.GrantWorkerLease
    cancellations, receipts = [], []

    def rpc(address, handler, request):
        assert address == core.node_address
        if handler == "cancel_worker_lease":
            reply = node._handle_cancel_worker_lease(request)
            assert reply.accepted and reply.cancelled
            assert reply.retired_grant == grant
            assert reply.dependency_inventory.descriptors == ()
            cancellations.append(reply)
            return reply
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert request.inventory == cancellations[0].dependency_inventory
        reply = node._handle_ack_lease_dependency_custody(request)
        assert reply.accepted
        receipts.append(reply)
        return reply

    core._rpc = rpc
    shutdown_error = SystemTaskError(
        "CoreWorker shutdown timed out before task completion"
    )
    try:
        assert core._begin_known_grant_cancellation(
            pending, pending.spec, (), core.node_address, grant, lease_request, shutdown_error,
        )
        assert len(cancellations) == len(receipts) == 1
        assert not node._dependency_custody_registry_locked().has_pending()
        assert node._leases[grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert node.resource_ledger.available == node.resource_ledger.total
        late_start = node._handle_start_worker_lease(protocol.StartWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id, grant.worker_id,
        ))
        assert not late_start.accepted and late_start.state is protocol.LeaseExecutionState.ABANDONED
        late_complete = node._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id,
            grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
        ))
        assert not late_complete.accepted and late_complete.output_publication is None
        assert node.object_store.used_bytes == 0
        payload = cloudpickle.dumps(42)
        late_reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, grant.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, (protocol.ResultDescriptor(
                pending.object_id, protocol.ResultStorage.INLINE, len(payload),
                core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(), payload,
            ),),
        )
        # ERROR is authoritative, but its task has not crossed finish yet.
        # No successful Node Complete or GCS publication was manufactured.
        assert pending.task_key not in core._finished_tasks
        assert not core._protocol_unresolved
        before = core.owner_table.snapshot(pending.object_id)
        before_recovery = replace(core._recovery.task_record(pending.task_id))
        with pytest.raises(SystemTaskError, match="exact single-output envelope"):
            core._publish_reply(pending, late_reply)
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.ERROR
        assert snapshot.error is shutdown_error
        assert core._objects[pending.object_id].event.is_set()
        assert snapshot == before
        assert core._recovery.task_record(pending.task_id) == before_recovery
        assert not core._protocol_unresolved, "late success created an adoption obligation for terminal ERROR"
        assert not any(isinstance(item, _DelayedReadyTask) for item in tuple(core._submissions.queue))
        assert not getattr(core, "_output_result_custody", {})
        assert core._finish_pending_task(pending)
        ref.close()
        _drain(core)
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
    finally:
        # Do not clear a failing obligation or manufacture an adoption receipt
        # to make teardown look clean. The threadless fixture retains evidence.
        ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_successful_reply_fences_a_late_shutdown_error() -> None:
    core = _core_without_runtime()
    outputs = _outputs(core)
    pending, ref = _submit_accepted(core)
    payload = cloudpickle.dumps(42)
    reply = _reply(core, outputs, pending, 42)
    try:
        assert core._publish_reply(pending, reply)

        assert not core._publish_error(
            pending.object_id,
            pending.spec.attempt_id,
            SystemTaskError("CoreWorker shutdown timed out before task completion"),
        )
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.READY_INLINE
        assert snapshot.inline_data == payload
        assert core._finish_pending_task(pending)
        ref.close()
        _drain(core)
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        outputs.assert_collected()
    finally:
        ref.close()
        close_pure_core(core)
