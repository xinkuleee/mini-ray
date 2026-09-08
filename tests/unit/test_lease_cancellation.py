"""Pure original lease-cancellation contracts with real authority changes.

Each Core case registers one Task on a threadless Core and uses one unstarted
Node with an empty 1 KiB store. Real Grant, Release, Cancel, outcome and custody
reducers own their state; only delivery is controlled. A lost Cancel ACK follows
its real effect. Negative Release replies follow a real release and its exact
duplicate; only their advisory text is replaced by the original test parameter.

The direct Cancel cases freeze an actual empty request inventory and acknowledge
its custody. The known-Grant scalar path already completed dependency handoff
before Push failed, so its existing empty-inventory optimization adds no custody
RPC. Neither path invents a successful output or uses legacy descriptor-only
publication. At most one delayed reducer turn is driven manually.

Task finish, the actual ObjectRef finalizer and owner GC are also driven
explicitly. These cases do not claim public runtime shutdown or live ordering
coverage. No user code, thread, process, socket, timer, wait or sleep runs.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, node as node_module, protocol
from miniray.core import (
    CoreWorker,
    _DelayedReadyTask,
    _LeaseCancellationState,
    _LeaseRequestState,
    _LocationReportState,
    _ReadyTask,
    _RetryInlineGc,
    _WAKE_COORDINATOR,
)
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.resources import (
    NodeSnapshot,
    ResourceLedger,
    ResourceVector,
)
from miniray.transport import TransportConnectionError, TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure lease cancellation attempted runtime infrastructure")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"),
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


class _AliveWorker:
    def is_alive(self) -> bool:
        return True


def _identity():
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    return task, AttemptID(task, 0)


def _node(node_id=None):
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random() if node_id is None else node_id
    node.worker_id = WorkerID.random()
    total = ResourceVector({"CPU": 1})
    node._ledger = ResourceLedger(total)
    node._gcs_address = None
    node._cluster_nodes = (NodeSnapshot(node.node_id, total, total),)
    node._cluster_addresses = {}
    node._worker_process = _AliveWorker()
    node._worker_address = ("127.0.0.1", 19001)
    node._shutdown_request_id = None
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_cancellations = {}
    node._active_lease_id = None
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._stop_event = threading.Event()
    node._object_store = ObjectStore(1024)
    node._sealed_metadata = {}
    node.event_sink = None
    return node


def _request(node):
    task, attempt = _identity()
    return protocol.RequestWorkerLease(
        LeaseID.random(), task, attempt, ResourceVector({"CPU": 1}),
        NodeID.random(), WorkerID.random(), target_node_id=node.node_id,
    )


def _cancel(request):
    return protocol.CancelWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        request.requester_node_id, request.requester_worker_id,
    )


@pytest.mark.unit
def test_cancel_before_request_tombstone_prevents_late_grant() -> None:
    node = _node()
    request = _request(node)
    cancel = _cancel(request)

    cancelled = node._handle_cancel_worker_lease(cancel)
    replay = node._handle_cancel_worker_lease(cancel)
    late = node._handle_request_lease(request)

    assert cancelled.accepted and cancelled.cancelled and not cancelled.released
    assert replay.accepted and replay.cancelled and not replay.released
    assert isinstance(late, protocol.RejectWorkerLease)
    assert late.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
    assert node._leases == {}
    assert node._ledger.available == node._ledger.total


@pytest.mark.unit
def test_cancel_granted_lease_releases_once_and_replay_is_noop() -> None:
    node = _node()
    request = _request(node)
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)

    first = node._handle_cancel_worker_lease(_cancel(request))
    second = node._handle_cancel_worker_lease(_cancel(request))

    assert first.accepted and first.cancelled and first.released
    assert second.accepted and second.cancelled and not second.released
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
    assert node._ledger.available == node._ledger.total


@pytest.mark.unit
def test_running_lease_cannot_be_cancelled() -> None:
    node = _node()
    request = _request(node)
    grant = node._handle_request_lease(request)
    node._handle_start_worker_lease(
        protocol.StartWorkerLease(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id
        )
    )

    reply = node._handle_cancel_worker_lease(_cancel(request))

    assert not reply.accepted and not reply.cancelled and not reply.released
    assert reply.state is protocol.LeaseExecutionState.RUNNING
    assert node._ledger.available.is_zero()


def _take_submissions(core):
    """Consume one bounded snapshot, never wait for a coordinator."""
    size = core._submissions.qsize()
    assert size <= 8
    items = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            items.append(item)
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    return tuple(items)


def _release_local_ref(ref):
    """Use the actual close/GC finalizer without Event.wait."""
    finalizer, done = ref._finalizer, ref._release_done
    assert finalizer is not None and done is not None
    ref._closed = True
    finalizer()
    assert done.is_set() and not finalizer.alive


class _CancellationFixture:
    def __init__(self, monkeypatch, *, release_outcome=None, lose_cancel=False):
        self.core = core = make_pure_core()
        core._registered_functions = set()
        self.node = node = _node(core.node_id)
        self.pending, self.ref = core._register_submission(
            core.define_remote_function(lambda: None), (), {},
            ResourceVector({"CPU": 1}), max_retries=3, _enqueue=True,
        )
        pending = self.pending
        assert _take_submissions(core) == (pending,)
        self.request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id,
            pending.spec.resources, core.node_id, core.worker_id,
            preferred_node_id=core.node_id, return_ids=pending.output_ids,
        )
        self.lease_state = _LeaseRequestState(
            self.request, core.node_address, node.node_id, True,
        )
        self.grant = None
        self.calls, self.pushes, self.cancels, self.custody = [], [], [], []
        self.release_effects, self.selected_errors = [], []
        self.release_outcome = release_outcome
        self.lose_cancel = lose_cancel
        self.push_failure = TransportConnectionError("worker was never reached")
        self.error = None
        self.freed = None
        self.resource_versions = []
        monkeypatch.setattr(core, "_rpc", self.rpc)
        monkeypatch.setattr(core, "_push_task_rpc", self.reject_push)
        self.assert_pending()

    def assert_pending(self):
        core, pending = self.core, self.pending
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.PENDING and snapshot.error is None
        assert snapshot.current_attempt == self.request.attempt_id
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == self.request.attempt_id
        assert record.state is TaskState.PENDING
        assert record.max_retries == 3 and record.retries_started == 0
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {pending.object_id: pending}
        assert not core._finished_tasks
        assert not self.ref._release_done.is_set()

    def select_error(self):
        self.assert_pending()
        marker = self.core._protocol_unresolved[self.pending.task_key]
        state = marker.obligation
        assert isinstance(state, (_LeaseCancellationState, _LocationReportState))
        assert state.terminal_error is self.error
        assert marker.output_candidate.execution == self.pending.execution
        assert marker.output_candidate.lease_id == self.request.lease_id
        self.selected_errors.append(state.terminal_error)

    def assert_abandoned(self):
        node = self.node
        assert node.resource_ledger.available == node.resource_ledger.total
        assert node.object_store.used_bytes == 0 and not node._sealed_metadata
        if self.grant is None:
            assert not node._leases
            return
        record = node._leases[self.request.lease_id]
        assert record.request == self.request and record.grant == self.grant
        assert record.state is protocol.LeaseExecutionState.ABANDONED
        assert record.dependency_pins == () and record.completion is None
        assert node._lease_outcomes[self.request.lease_id].reply == self.grant
        assert node._workers[self.grant.worker_id].active_lease_id is None
        assert node._active_lease_id is None
        query = protocol.GetWorkerLeaseOutcome(
            self.grant.lease_id, self.grant.task_id, self.grant.attempt_id,
            self.grant.worker_id, self.core.worker_id, self.pending.output_ids,
        )
        outcome = node._handle_get_worker_lease_outcome(query)
        assert outcome.found and outcome.worker_alive
        assert outcome.state is protocol.LeaseExecutionState.ABANDONED
        assert outcome.completion_status is None and not outcome.cleanup_pending
        assert outcome.descriptors == outcome.orphan_descriptors == ()
        assert outcome.output_publication is None and outcome.output_completion is None
        if self.freed is None:
            self.freed = node.resource_ledger.snapshot()
        else:
            assert node.resource_ledger.snapshot() == self.freed

    def rpc(self, address, handler, message):
        assert address == self.core.node_address and len(self.calls) < 8
        self.calls.append((handler, message))
        self.assert_pending()
        if handler == "request_worker_lease":
            assert message == self.request and self.grant is None
            self.grant = grant = self.node._handle_request_lease(message)
            assert type(grant) is protocol.GrantWorkerLease
            assert grant.dependencies == () and grant.attempt_id == self.request.attempt_id
            assert self.node._leases[message.lease_id].state is protocol.LeaseExecutionState.GRANTED
            assert self.node._lease_outcomes[message.lease_id].reply == grant
            assert self.node.resource_ledger.available.is_zero()
            self.resource_versions.append(self.node._resource_report_version)
            self.assert_pending()
            return grant
        if handler == "release_worker_lease":
            assert self.grant is not None and not self.release_effects
            assert message == protocol.ReleaseWorkerLease(
                self.grant.lease_id, self.grant.worker_id, self.grant.allocation_token,
            )
            # A real release has happened, but this Core has no positive ACK.
            # In the negative-reply case the response is an exact duplicate
            # release's false result, with the original advisory-text input.
            first = self.node._handle_release_lease(message)
            assert first.released
            self.release_effects.append(first)
            self.resource_versions.append(self.node._resource_report_version)
            self.assert_abandoned()
            self.assert_pending()
            if isinstance(self.release_outcome, BaseException):
                raise self.release_outcome
            assert type(self.release_outcome) is protocol.ReleaseReply
            assert not self.release_outcome.released
            repeated = self.node._handle_release_lease(message)
            assert not repeated.released
            self.release_effects.append(repeated)
            self.assert_abandoned()
            return replace(repeated, detail=self.release_outcome.detail)
        if handler == "cancel_worker_lease":
            self.select_error()
            assert type(message) is protocol.CancelWorkerLease
            assert message.lease_request == self.request
            assert (message.lease_id, message.task_id, message.attempt_id) == (
                self.request.lease_id, self.pending.task_id, self.request.attempt_id,
            )
            assert (message.requester_node_id, message.requester_worker_id) == (
                self.core.node_id, self.core.worker_id,
            )
            reply = self.node._handle_cancel_worker_lease(message)
            assert reply.accepted and reply.cancelled and not reply.released
            assert reply.state is protocol.LeaseExecutionState.ABANDONED
            assert reply.retired_grant == self.grant
            assert reply.dependency_inventory.lease_request == self.request
            assert reply.dependency_inventory.descriptors == ()
            assert reply.dependency_inventory.node_id == self.node.node_id
            self.cancels.append((message, reply))
            self.assert_abandoned()
            self.assert_pending()  # remote cancellation is not a delivered ACK
            if self.lose_cancel:
                self.lose_cancel = False
                raise TransportTimeout("cancel acknowledgement was lost")
            return reply
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        self.select_error()
        assert self.grant is None and not self.custody
        assert type(message) is protocol.AckLeaseDependencyCustody
        assert message.requester_worker_id == self.core.worker_id
        assert message.inventory == self.cancels[-1][1].dependency_inventory
        state = self.core._protocol_unresolved[self.pending.task_key].obligation
        assert type(state) is _LocationReportState
        assert state.cancellation_reply == self.cancels[-1][1]
        assert state.inventory == message.inventory and not state.custody_acknowledged
        reply = self.node._handle_ack_lease_dependency_custody(message)
        assert reply.accepted and reply.request == message
        self.custody.append((message, reply))
        entry = self.node._lease_dependency_custody._entries[self.request.lease_id]
        assert entry.acknowledged == message.inventory
        self.assert_pending()  # publish only after the caller receives this ACK
        return reply

    def reject_push(self, address, handler, message):
        assert self.grant is not None and not self.pushes
        assert address == self.grant.worker_address and handler == "push_task"
        assert type(message) is protocol.PushTask
        assert message.lease_id == self.grant.lease_id and message.worker_id == self.grant.worker_id
        assert message.spec == self.pending.spec and message.dependencies == ()
        state = self.core._protocol_unresolved[self.pending.task_key].obligation
        assert type(state) is _LocationReportState and state.terminal_error is None
        assert state.inventory is None and state.reports == ()
        assert self.node._leases[self.request.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert self.node.resource_ledger.available.is_zero()
        self.pushes.append(message)
        self.assert_pending()
        # Definite pre-delivery failure: no Start, Complete or user execution.
        self.error = self.push_failure
        raise self.push_failure

    def cancellation(self, error):
        self.error = error
        request = protocol.CancelWorkerLease(
            self.request.lease_id, self.pending.task_id, self.request.attempt_id,
            self.core.node_id, self.core.worker_id, lease_request=self.request,
        )
        return _LeaseCancellationState(
            request, self.core.node_address, error, target_node_id=self.node.node_id,
            lease_request=self.request,
        )

    def execute(self):
        return self.core._execute(
            self.pending, self.pending.spec, (), lease_state=self.lease_state,
        )

    def next_ready(self):
        (delayed,) = _take_submissions(self.core)
        assert type(delayed) is _DelayedReadyTask
        ready = delayed.ready
        assert type(ready) is _ReadyTask and ready.pending is self.pending
        assert ready.spec == self.pending.spec and ready.dependencies == ()
        self.assert_pending()
        assert not self.core._finish_pending_task(self.pending)
        self.assert_pending()
        return ready

    def resume(self, ready):
        if ready.cancellation is not None:
            assert ready.location_state is None
            return self.core._resolve_lease_cancellation(
                self.pending, ready.spec, (), ready.cancellation,
            )
        assert ready.location_state is not None
        return self.core._execute(
            self.pending, ready.spec, (), location_state=ready.location_state,
        )

    def assert_terminal(self, error):
        core, pending = self.core, self.pending
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is error
        assert snapshot.current_attempt == self.request.attempt_id
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == self.request.attempt_id
        assert record.state is TaskState.SYSTEM_FAILED and record.retries_started == 0
        assert core._recovery.active_recovery(pending.task_id) is None
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {pending.object_id: pending}
        assert not core._protocol_unresolved and not core._finished_tasks
        assert self.selected_errors and all(item is error for item in self.selected_errors)
        self.assert_abandoned()
        before = self.node.resource_ledger.snapshot()
        version = getattr(self.node, "_resource_report_version", 0)
        cancellation, first = self.cancels[-1]
        replay = self.node._handle_cancel_worker_lease(cancellation)
        assert replay == replace(first, released=False)
        late = self.node._handle_request_lease(self.request)
        assert type(late) is protocol.RejectWorkerLease
        assert late.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
        if self.grant is not None:
            started = self.node._handle_start_worker_lease(protocol.StartWorkerLease(
                self.grant.lease_id, self.grant.task_id, self.grant.attempt_id,
                self.grant.worker_id,
            ))
            assert not started.accepted
            assert len(self.node._leases) == len(self.pushes) == 1
            assert len(self.release_effects) in (1, 2)
            assert sum(reply.released for reply in self.release_effects) == 1
            assert self.resource_versions[1] == self.resource_versions[0] + 1
            assert version == self.resource_versions[-1]
            assert not self.custody  # original scalar-handoff optimization
        else:
            assert not self.pushes and not self.release_effects
            assert len(self.custody) == 1
        assert self.node.resource_ledger.snapshot() == before
        assert getattr(self.node, "_resource_report_version", 0) == version
        assert not self.node._lease_dependency_custody.has_pending()

    def finish_and_collect(self):
        core, pending = self.core, self.pending
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.ERROR
        assert core._finish_pending_task(pending)
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert _take_submissions(core) == ()
        _release_local_ref(self.ref)
        _release_local_ref(self.ref)
        assert len(core._reference_mailbox.releases) == 1
        fifo = core._reference_mailbox.pending
        assert fifo.qsize() <= 8
        for _ in range(8):
            try:
                event = fifo.get_nowait()
            except queue.Empty:
                break
            try:
                assert type(event) is _RetryInlineGc and event.object_id == pending.object_id
                core._reference_released(event.object_id)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0
        assert _take_submissions(core) == ()
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert not core._objects and not core._stored_descriptors
        assert not core._object_gc_obligations and not core._protocol_unresolved

    def close(self):
        # A failing assertion must not be hidden by invented task completion.
        # Release only this handle; outstanding pure authorities stay intact.
        _release_local_ref(self.ref)
        close_pure_core(self.core)


@pytest.mark.unit
def test_core_cancel_transport_failure_keeps_object_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _CancellationFixture(monkeypatch, lose_cancel=True)
    cancellation = f.cancellation(RuntimeError("lease outcome unknown"))
    try:
        assert not f.core._resolve_lease_cancellation(
            f.pending, f.pending.spec, (), cancellation,
        )
        f.assert_pending()
        ready = f.next_ready()
        assert ready.cancellation.request == cancellation.request
        assert ready.cancellation.reply is None and ready.cancellation.round == 1
        assert ready.cancellation.terminal_error is cancellation.terminal_error
        assert len(f.cancels) == 1 and not f.custody
        assert f.node._lease_cancellations[cancellation.request.lease_id].reply == f.cancels[0][1]
        assert f.resume(ready)
        assert f.cancels[0] == f.cancels[1] and len(f.custody) == 1
        f.assert_terminal(cancellation.terminal_error)
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
def test_core_cancel_ack_is_required_before_terminal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _CancellationFixture(monkeypatch)
    error = RuntimeError("ambiguous lease safely cancelled")
    cancellation = f.cancellation(error)
    try:
        assert f.core._resolve_lease_cancellation(
            f.pending, f.pending.spec, (), cancellation,
        )
        # Both real handlers assert PENDING before returning their ACK.
        assert [handler for handler, _ in f.calls] == [
            "cancel_worker_lease", protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER,
        ]
        assert len(f.cancels) == len(f.custody) == 1
        f.assert_terminal(error)
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "release_outcome",
    (
        protocol.ReleaseReply(False, "lease already released or unknown"),
        TransportTimeout("release acknowledgement was lost"),
    ),
)
def test_known_grant_definite_push_failure_waits_for_cancel_ack(
    monkeypatch: pytest.MonkeyPatch, release_outcome: object,
) -> None:
    """Release false/loss proves nothing; typed Cancel ACK is terminal."""

    f = _CancellationFixture(monkeypatch, release_outcome=release_outcome)
    try:
        assert f.execute()
        assert [handler for handler, _ in f.calls] == [
            "request_worker_lease",
            "release_worker_lease",
            "cancel_worker_lease",
        ]
        cancel = f.calls[-1][1]
        assert isinstance(cancel, protocol.CancelWorkerLease)
        assert (cancel.lease_id, cancel.task_id, cancel.attempt_id) == (
            f.grant.lease_id, f.grant.task_id, f.grant.attempt_id,
        )
        assert (cancel.requester_node_id, cancel.requester_worker_id) == (
            f.core.node_id, f.core.worker_id,
        )
        assert len(f.cancels) == 1
        f.assert_terminal(f.push_failure)
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
def test_known_grant_cancel_transport_loss_keeps_attempt_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _CancellationFixture(
        monkeypatch, release_outcome=protocol.ReleaseReply(False, "not released"),
        lose_cancel=True,
    )
    try:
        assert not f.execute()
        f.assert_pending()
        ready = f.next_ready()
        # The existing scalar location handoff remains the canonical driver
        # across definite Push failure; do not replace it with a legacy Cancel
        # state and thereby discard its frozen grant/request identity.
        state = ready.location_state
        assert ready.cancellation is None and type(state) is _LocationReportState
        assert state.lease_request == f.request and state.grant == f.grant
        assert state.lease_id == f.grant.lease_id and state.round == 1
        assert state.terminal_error is f.push_failure and state.cancellation_reply is None
        assert [handler for handler, _ in f.calls] == [
            "request_worker_lease",
            "release_worker_lease",
            "cancel_worker_lease",
        ]
        assert len(f.cancels) == 1
        assert f.node._lease_cancellations[f.request.lease_id].reply == f.cancels[0][1]
        before = f.node.resource_ledger.snapshot()
        assert f.resume(ready)
        assert len(f.cancels) == 2 and f.cancels[0] == f.cancels[1]
        assert [handler for handler, _ in f.calls] == [
            "request_worker_lease", "release_worker_lease",
            "cancel_worker_lease", "cancel_worker_lease",
        ]
        assert f.node.resource_ledger.snapshot() == before and len(f.pushes) == 1
        f.assert_terminal(f.push_failure)
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "detail",
    (
        "lease already released or unknown",
        "safe: worker never started",
        "ABANDONED",
    ),
)
def test_release_string_detail_never_substitutes_for_cancel_ack(
    monkeypatch: pytest.MonkeyPatch, detail: str,
) -> None:
    f = _CancellationFixture(
        monkeypatch, release_outcome=protocol.ReleaseReply(False, detail),
    )
    try:
        assert f.execute()
        assert len(f.cancels) == 1
        assert [handler for handler, _ in f.calls] == [
            "request_worker_lease", "release_worker_lease", "cancel_worker_lease",
        ]
        f.assert_terminal(f.push_failure)
        f.finish_and_collect()
    finally:
        f.close()
