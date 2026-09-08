"""Pure Core admission, dependency and lease-retry composition contracts.

The two real lane/drain tests live in integration/test_core_dispatch_concurrency.py.
Here all queues are test-owned, delayed readiness uses a fake monotonic clock,
and the real Core methods run without starting infrastructure or waiting.
"""

from __future__ import annotations

import queue
import socket
import threading
import time

import pytest

from miniray import core as core_module, protocol
from miniray.core import (
    CoreWorker, _DelayedReadyTask, _LeaseRequestState, _PendingTask, _ReadyTask,
    _WAKE_COORDINATOR,
)
from miniray.errors import (
    InfeasibleTaskError, PendingCapacityError, RuntimeShuttingDownError,
)
from miniray.ids import LeaseID, WorkerID
from miniray.ownership import ObjectState
from miniray.resources import AllocationToken, ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _forbid_runtime_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure dispatch contract attempted runtime work")

    def immediate_event_wait(event, timeout=None):
        assert event.is_set(), "pure dispatch attempted to wait for background work"
        return True

    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Thread, "join", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", immediate_event_wait)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now


def _core(*, capacity_retry_rounds=None) -> CoreWorker:
    core = make_pure_core()
    # Only state needed by the real queue/gate/execute methods.  No shared
    # helper change or fake execution/owner/recovery implementation is needed.
    core._ready_tasks = queue.Queue()
    core._delayed_ready = queue.PriorityQueue()
    core._capacity_sequence = 0
    core._registered_functions = set()
    core._capacity_retry_rounds = capacity_retry_rounds
    return core


def _submit_ready(core, definition):
    ref = core.submit(definition, (), {}, ResourceVector())
    pending = core._submissions.get_nowait()
    assert isinstance(pending, _PendingTask)
    assert core._submissions.empty()
    core._admit_or_block(pending)
    ready = core._ready_tasks.get_nowait()
    assert isinstance(ready, _ReadyTask) and ready.pending is pending
    assert core._ready_tasks.empty()
    return pending, ref, ready


def _advance_one_capacity_retry(core, clock):
    delayed = core._submissions.get_nowait()
    assert isinstance(delayed, _DelayedReadyTask)
    assert core._submissions.empty()
    assert delayed.due_at > clock.now
    core._schedule_delayed_ready(delayed)
    core._promote_delayed_ready()
    assert core._ready_tasks.empty()
    assert core._next_coordinator_timeout() == pytest.approx(delayed.due_at - clock.now)
    clock.now = delayed.due_at
    core._promote_delayed_ready()
    ready = core._ready_tasks.get_nowait()
    assert ready is delayed.ready
    assert core._ready_tasks.empty() and core._delayed_ready.empty()
    assert core._delayed_ready.unfinished_tasks == 0
    return ready


def _execute_ready(core, ready):
    return core._execute(
        ready.pending, ready.spec, ready.dependencies,
        lease_state=ready.lease_state, ambiguity_round=ready.ambiguity_round,
    )


def _require_rpc_route(actual_address, handler, expected_address, expected_handler):
    if actual_address != expected_address or handler != expected_handler:
        pytest.fail("dispatch contract encountered an unexpected RPC route")


def test_unready_dependency_does_not_consume_a_dispatch_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core()
    definition = core.define_remote_function(lambda value=None: value)
    dependency_pending, dependency = core._register_submission(
        definition, (), {}, ResourceVector()
    )
    blocked = core.submit(definition, (dependency,), {}, ResourceVector())
    independent = core.submit(definition, (), {}, ResourceVector())
    try:
        blocked_pending = core._submissions.get_nowait()
        independent_pending = core._submissions.get_nowait()
        assert isinstance(blocked_pending, _PendingTask)
        assert isinstance(independent_pending, _PendingTask)
        core._admit_or_block(blocked_pending)
        assert core._ready_tasks.empty()
        assert core._blocked_tasks == {blocked_pending.task_key: blocked_pending}
        core._admit_or_block(independent_pending)
        ready = core._ready_tasks.get_nowait()
        assert isinstance(ready, _ReadyTask)
        assert ready.pending is independent_pending and ready.dependencies == ()
        assert core._ready_tasks.empty()
        # A lane can only consume readiness-queue entries.  Completing this
        # independent entry leaves the pending dependency outside that queue.
        assert core._publish_task_error(ready.pending, RuntimeError("executed"))
        assert core._finish_pending_task(ready.pending)
        with pytest.raises(RuntimeError, match="executed"):
            core.get(independent, timeout=0)
        assert not core.owner_table.snapshot(blocked.object_id).is_ready

        assert core._publish_error(
            dependency_pending.object_id,
            dependency_pending.spec.attempt_id,
            RuntimeError("dependency failed"),
        )
        core._promote_unblocked_tasks()
        with pytest.raises(RuntimeError, match="dependency failed"):
            core.get(blocked, timeout=0)
        assert core._blocked_tasks == {}
        assert core._ready_tasks.empty()
        assert core._accepted_task_count == 0
        assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    finally:
        for ref in (independent, blocked, dependency):
            ref.close()
        close_pure_core(core)


def _reject(request, reason):
    return protocol.RejectWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, reason, "test"
    )


def _grant(request, core):
    return protocol.GrantWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, core.node_id,
        WorkerID.random(), ("worker.invalid", 1),
        AllocationToken(str(request.lease_id)),
    )


def test_pending_capacity_requeues_exact_request_with_same_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core()
    clock = _Clock()
    monkeypatch.setattr(core_module, "time", clock)
    definition = core.define_remote_function(lambda: 7)
    leases = []
    output = PureOutputRuntime(core)
    core.gcs_address = output.gcs_address

    def lease_rpc(address, handler, request):
        if output.handles(handler):
            return output.rpc(address, handler, request)
        _require_rpc_route(address, handler, core.node_address, "request_worker_lease")
        assert isinstance(request, protocol.RequestWorkerLease)
        leases.append(request)
        if len(leases) == 1:
            return _reject(request, protocol.LeaseRejectReason.PENDING_CAPACITY)
        return _grant(request, core)

    def push_rpc(address, handler, push):
        _require_rpc_route(address, handler, ("worker.invalid", 1), "push_task")
        assert isinstance(push, protocol.PushTask)
        return output.complete(push, (7,))

    monkeypatch.setattr(core, "_rpc", lease_rpc)
    monkeypatch.setattr(core, "_push_task_rpc", push_rpc)
    try:
        pending, ref, ready = _submit_ready(core, definition)
        assert not _execute_ready(core, ready)
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.PENDING
        assert core._accepted_task_count == 1
        retried_ready = _advance_one_capacity_retry(core, clock)
        assert retried_ready.pending.spec.attempt_id == pending.spec.attempt_id
        assert retried_ready.pending.capacity_round == 1
        assert _execute_ready(core, retried_ready)
        assert core.get(ref, timeout=0) == 7
        membership = core.owner_table.snapshot(ref.object_id).output_publication
        assert membership is not None
        assert membership.publication_id.lease_id == leases[0].lease_id
        assert membership.publication_id.execution == pending.execution
        assert output.discoveries == len(output.completions) == 1
        assert len(leases) == 2
        assert leases[0] == leases[1]
        assert leases[0] is leases[1]
        assert core._finish_pending_task(retried_ready.pending)
        assert core._accepted_task_count == 0
        ref.close()
        core._reference_mailbox.drain()
        output.assert_collected()
    finally:
        if "ref" in locals():
            ref.close()
        close_pure_core(core)


def test_pending_capacity_exhaustion_is_typed_and_does_not_advance_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(capacity_retry_rounds=1)
    clock = _Clock()
    monkeypatch.setattr(core_module, "time", clock)
    leases = []

    def always_pending(address, handler, request):
        _require_rpc_route(address, handler, core.node_address, "request_worker_lease")
        leases.append(request)
        return _reject(request, protocol.LeaseRejectReason.PENDING_CAPACITY)

    monkeypatch.setattr(core, "_rpc", always_pending)
    try:
        pending, ref, ready = _submit_ready(core, core.define_remote_function(lambda: None))
        assert not _execute_ready(core, ready)
        retried_ready = _advance_one_capacity_retry(core, clock)
        assert _execute_ready(core, retried_ready)
        with pytest.raises(PendingCapacityError):
            core.get(ref, timeout=0)
        assert len(leases) == 2
        assert leases[0] == leases[1]
        assert leases[0].attempt_id == pending.spec.attempt_id
        assert core.owner_table.snapshot(ref.object_id).current_attempt == pending.spec.attempt_id
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._finish_pending_task(retried_ready.pending)
        assert core._accepted_task_count == 0
    finally:
        if "ref" in locals():
            ref.close()
        close_pure_core(core)


@pytest.mark.parametrize(
    ("reason", "error_type"),
    [
        (protocol.LeaseRejectReason.INFEASIBLE, InfeasibleTaskError),
        (protocol.LeaseRejectReason.SHUTTING_DOWN, RuntimeShuttingDownError),
    ],
)
def test_terminal_lease_reasons_publish_typed_errors(
    monkeypatch: pytest.MonkeyPatch, reason, error_type,
) -> None:
    core = _core()
    calls = 0

    def reject(address, handler, request):
        nonlocal calls
        _require_rpc_route(address, handler, core.node_address, "request_worker_lease")
        calls += 1
        return _reject(request, reason)

    monkeypatch.setattr(core, "_rpc", reject)
    try:
        pending, ref, ready = _submit_ready(core, core.define_remote_function(lambda: None))
        assert _execute_ready(core, ready)
        with pytest.raises(error_type):
            core.get(ref, timeout=0)
        assert calls == 1
        assert core.owner_table.snapshot(ref.object_id).current_attempt == pending.spec.attempt_id
        assert core._finish_pending_task(pending)
    finally:
        if "ref" in locals():
            ref.close()
        close_pure_core(core)


def test_ambiguous_lease_rpc_replays_exact_request_and_lease_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core()
    definition = core.define_remote_function(lambda: None)
    pending, ref = core._register_submission(
        definition, (), {}, ResourceVector()
    )
    request = protocol.RequestWorkerLease(
        LeaseID.random(), pending.spec.task_id, pending.spec.attempt_id,
        ResourceVector(), core.node_id, core.worker_id, preferred_node_id=core.node_id,
    )
    state = _LeaseRequestState(request, core.node_address, core.node_id, True)
    seen = []

    def ambiguous_then_grant(address, handler, message):
        _require_rpc_route(address, handler, core.node_address, "request_worker_lease")
        seen.append(message)
        if len(seen) < 3:
            raise TransportTimeout("lost lease reply")
        return _grant(message, core)

    monkeypatch.setattr(core, "_rpc", ambiguous_then_grant)
    try:
        assert isinstance(core._request_lease_hop(state), protocol.GrantWorkerLease)
        assert seen == [request, request, request]
        assert len({message.lease_id for message in seen}) == 1
    finally:
        assert core._publish_error(
            pending.object_id, pending.spec.attempt_id, RuntimeError("cleanup")
        )
        ref.close()
        close_pure_core(core)
