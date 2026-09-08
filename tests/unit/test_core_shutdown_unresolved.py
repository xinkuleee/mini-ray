"""Pure unresolved-protocol shutdown contracts; real races live in L1."""
from __future__ import annotations

import queue
import socket
import threading
import time
from contextlib import contextmanager

import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker, _DelayedReadyTask, _LeaseCancellationState, _LeaseRequestState,
    _PushRequestState, _STOP, _WAKE_COORDINATOR,
)
from miniray.ids import LeaseID, WorkerID
from miniray.ownership import ObjectState
from miniray.resources import AllocationToken, ResourceVector
from miniray.transport import RemoteCallError, TransportConnectionError, TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _forbid_runtime_effects(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure shutdown contract attempted runtime work")

    def immediate_wait(event, timeout=None):
        assert event.is_set(), "pure shutdown contract attempted to block"
        return True

    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", immediate_wait)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _close_ref_now(ref):
    if ref is None or ref.closed:
        return
    assert ref.borrower_token is None and ref._finalizer is not None
    ref._closed = True
    ref._finalizer()
    assert ref._release_done is not None and ref._release_done.is_set()


@contextmanager
def _core_pending(monkeypatch, *, dependency=True):
    core = None
    refs = []
    try:
        core = make_pure_core()
        core._registered_functions = set()
        output = PureOutputRuntime(core)
        core.gcs_address = output.gcs_address
        monkeypatch.setattr(core, "_rpc", output.rpc)
        dependency_ref = None
        if dependency:
            producer, dependency_ref = core._register_submission(
                core.define_remote_function(lambda: 1), (), {}, ResourceVector()
            )
            refs.append(dependency_ref)
            producer_push = protocol.PushTask(LeaseID.random(), WorkerID.random(), producer.spec)
            producer_reply = output.complete(producer_push, (1,))
            assert core._publish_reply(
                producer, producer_reply, expected_node_id=core.node_id,
                expected_lease_id=producer_push.lease_id,
            )
            assert core.owner_table.snapshot(producer.object_id).output_publication is not None
            assert _drain_protocol_queue(core) == (_WAKE_COORDINATOR,)
        pending, ref = core._register_submission(
            core.define_remote_function((lambda value: value) if dependency else (lambda: 7)),
            (dependency_ref,) if dependency else (), {}, ResourceVector(),
        )
        refs.append(ref)
        with core._state_lock:
            core._accepted_task_count = 1
            core._install_task_finish_barrier_locked(pending)
        prepared, dependencies, _protected = core._prepare_task_dependencies(pending.spec)
        assert dependencies == ()
        grant = protocol.GrantWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, core.node_id,
            WorkerID.random(), ("worker.invalid", 1), AllocationToken("pure-unresolved"),
        )
        state = _PushRequestState(
            protocol.PushTask(grant.lease_id, grant.worker_id, prepared),
            grant, core.node_address, grant.worker_address, 1, True,
        )
        yield core, dependency_ref, pending, ref, state, output
    finally:
        if core is not None:
            for ref in reversed(refs):
                _close_ref_now(ref)
            close_pure_core(core)


def _running_outcome(core, pending, state, request):
    assert request == protocol.GetWorkerLeaseOutcome(
        state.grant.lease_id, pending.task_id, pending.spec.attempt_id,
        state.grant.worker_id, core.worker_id, pending.output_ids,
    )
    return protocol.GetWorkerLeaseOutcomeReply(
        request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
        request.owner_worker_id, request.object_ids, core.node_id, True, True,
        protocol.LeaseExecutionState.RUNNING,
    )


def _drain_protocol_queue(core):
    values = []
    for _ in range(32):
        try:
            item = core._submissions.get_nowait()
        except queue.Empty:
            return tuple(values)
        try:
            assert item in (_STOP, _WAKE_COORDINATOR) or isinstance(item, _DelayedReadyTask)
            values.append(item)
        finally:
            core._submissions.task_done()
    pytest.fail("pure shutdown queue exceeded 32 messages")


def _finish_and_shutdown(core, pending, *refs):
    assert core._finish_pending_task(pending)
    assert core._accepted_task_count == 0 and not core._task_finish_barriers
    for ref in refs:
        _close_ref_now(ref)
    core._reference_mailbox.drain()
    assert not core._reference_mailbox.events.unfinished_tasks
    _drain_protocol_queue(core)
    assert core.shutdown(timeout=0.01)
    assert core._sink_closed


def test_short_shutdown_preserves_ambiguous_push_until_replay_recovers(monkeypatch):
    with _core_pending(monkeypatch) as (core, dependency, pending, ref, state, output):
        replies = [RemoteCallError("push_task", "RuntimeError", "late reply", ""),
                   (7,)]
        pushes, outcomes = [], []

        def push(address, handler, request):
            assert (address, handler) == (state.worker_address, "push_task")
            assert request is state.push and pending.task_key in core._protocol_unresolved
            assert replies, "more than two exact sends"
            pushes.append(request)
            reply = replies.pop(0)
            if isinstance(reply, BaseException):
                raise reply
            # Complete only on the second send; the earlier RUNNING outcome
            # must not contradict an already-crossed local Complete witness.
            return output.complete(request, reply)

        def outcome(address, handler, request):
            if output.handles(handler):
                return output.rpc(address, handler, request)
            assert (address, handler) == (state.granting_node_address, "get_worker_lease_outcome")
            assert type(request) is protocol.GetWorkerLeaseOutcome and not outcomes
            outcomes.append(request)
            return _running_outcome(core, pending, state, request)

        monkeypatch.setattr(core, "_push_task_rpc", push)
        monkeypatch.setattr(core, "_rpc", outcome)
        assert not core._replay_push(pending, state)
        delayed = [item for item in _drain_protocol_queue(core) if isinstance(item, _DelayedReadyTask)]
        assert len(delayed) == 1 and delayed[0].ready.push_state.push is state.push
        before = core.owner_table.snapshot(pending.object_id)
        assert not core.shutdown(timeout=0.01)
        assert core.owner_table.snapshot(pending.object_id) == before
        assert before.state is ObjectState.PENDING
        assert pending.dependency_hold in core.owner_table.snapshot(dependency.object_id).submitted_tokens
        assert core._accepted_task_count == 1
        assert not core._sink_closed and core._reference_mailbox.accepting
        assert not hasattr(core, "_reference_thread")
        assert output.discoveries == len(output.completions) == 1  # dependency only
        assert core._replay_push(pending, state)
        assert pending.task_key not in core._protocol_unresolved
        assert core.get(ref, timeout=0) == 7
        membership = core.owner_table.snapshot(ref.object_id).output_publication
        assert membership is not None
        assert membership.publication_id.lease_id == state.grant.lease_id
        assert membership.manifest.execution == pending.execution
        assert output.discoveries == len(output.completions) == 2
        _finish_and_shutdown(core, pending, ref, dependency)
        output.assert_collected()
        assert pushes == [state.push, state.push] and len(outcomes) == 1


def test_short_shutdown_preserves_unknown_cancel_until_ack(monkeypatch):
    with _core_pending(monkeypatch) as (core, dependency, pending, ref, state, output):
        request = protocol.CancelWorkerLease(
            state.grant.lease_id, pending.task_id, pending.spec.attempt_id, core.node_id, core.worker_id,
        )
        cancellation = _LeaseCancellationState(request, core.node_address, RuntimeError("push never reached worker"))
        replies = [TransportTimeout("lost cancel ack"), protocol.CancelWorkerLeaseReply(
            request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
            request.requester_worker_id, protocol.LeaseExecutionState.ABANDONED, True, True, True,
        )]
        seen = []

        def cancel(address, handler, message):
            if output.handles(handler):
                return output.rpc(address, handler, message)
            assert (address, handler) == (core.node_address, "cancel_worker_lease")
            assert message is request and type(message) is protocol.CancelWorkerLease
            assert pending.task_key in core._protocol_unresolved and replies
            seen.append(message)
            reply = replies.pop(0)
            if isinstance(reply, BaseException):
                raise reply
            return reply

        monkeypatch.setattr(core, "_rpc", cancel)
        assert not core._resolve_lease_cancellation(pending, state.push.spec, (), cancellation)
        delayed = [item for item in _drain_protocol_queue(core) if isinstance(item, _DelayedReadyTask)]
        assert len(delayed) == 1 and delayed[0].ready.cancellation.request is request
        assert not core.shutdown(timeout=0.01)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert pending.dependency_hold in core.owner_table.snapshot(dependency.object_id).submitted_tokens
        assert not core._finish_pending_task(pending) and core._accepted_task_count == 1
        assert core._resolve_lease_cancellation(pending, state.push.spec, (), cancellation)
        assert pending.task_key not in core._protocol_unresolved
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.ERROR
        _finish_and_shutdown(core, pending, ref, dependency)
        output.assert_collected()
        assert seen == [request, request]


def test_malformed_lease_reply_preserves_exact_lease_for_resolution(monkeypatch):
    with _core_pending(monkeypatch, dependency=False) as (core, _dependency, pending, ref, _state, output):
        request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, preferred_node_id=core.node_id, return_ids=pending.output_ids,
        )
        seen = []

        def malformed_then_unreachable(address, handler, message):
            if output.handles(handler):
                return output.rpc(address, handler, message)
            assert (address, handler) == (core.node_address, "request_worker_lease")
            assert message is request and type(message) is protocol.RequestWorkerLease
            assert len(seen) < 3
            seen.append(message)
            if len(seen) == 1:
                return object()
            raise TransportConnectionError("node temporarily unreachable")

        monkeypatch.setattr(core, "_rpc", malformed_then_unreachable)
        assert not core._execute(pending, pending.spec, lease_state=_LeaseRequestState(
            request, core.node_address, core.node_id, True
        ))
        assert seen == [request, request, request]
        delayed = [item for item in _drain_protocol_queue(core) if isinstance(item, _DelayedReadyTask)]
        assert len(delayed) == 1 and delayed[0].ready.lease_state.request is request
        assert pending.task_key in core._protocol_unresolved
        assert not core.shutdown(timeout=0.01)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        # Explicit fixture cleanup does not infer cancellation from a bad reply.
        core._clear_protocol_unresolved(pending)
        assert core._publish_task_error(pending, RuntimeError("test cleanup"))
        _finish_and_shutdown(core, pending, ref)
