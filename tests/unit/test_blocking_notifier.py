"""Pure contracts for exact Worker-to-Node blocking episodes."""

from __future__ import annotations

from dataclasses import replace
import socket
import threading
from types import SimpleNamespace

import pytest

from miniray import protocol
from miniray.blocking import (
    BLOCKED_HANDLER,
    UNBLOCKED_HANDLER,
    BlockingIdentity,
    BlockingNotificationError,
    BlockingNotifier,
)
from miniray.ids import AttemptID, JobID, LeaseID, TaskID, WorkerID
from miniray.transport import TransportTimeout


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure blocking-notifier contract attempted runtime work")

    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


def _notifier(identity, **options):
    notifier = BlockingNotifier(identity, **options)
    delays = []

    def wait(delay):
        assert 0 < delay <= 0.1
        delays.append(delay)
        assert len(delays) <= 9  # three Block plus eight Unblock attempts
        return False

    notifier._wait_event = SimpleNamespace(wait=wait, delays=delays)
    return notifier


def _identity() -> BlockingIdentity:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 3)
    return BlockingIdentity(
        LeaseID.random(), task_id, AttemptID(task_id, 0), WorkerID.random(),
        ("127.0.0.1", 19001),
    )


def _reply(message: object, *, accepted: bool = True) -> object:
    reply_type = (
        protocol.NotifyWorkerBlockedReply
        if isinstance(message, protocol.NotifyWorkerBlocked)
        else protocol.NotifyWorkerUnblockedReply
    )
    return reply_type(
        message.lease_id, message.task_id, message.attempt_id,
        message.worker_id, message.sequence,
        protocol.LeaseExecutionState.RUNNING, accepted, True if accepted else False,
        None if accepted else "rejected",
    )


def test_ambiguous_block_and_unblock_replay_exact_messages() -> None:
    calls: list[tuple[str, object]] = []
    attempts = {BLOCKED_HANDLER: 0, UNBLOCKED_HANDLER: 0}

    def rpc(address: object, handler: str, message: object) -> object:
        assert address == identity.node_address
        calls.append((handler, message))
        attempts[handler] += 1
        if attempts[handler] == 1:
            raise TransportTimeout("lost acknowledgement")
        return _reply(message)

    identity = _identity()
    notifier = _notifier(identity, rpc=rpc)
    with notifier.blocking_scope():
        pass

    assert [handler for handler, _message in calls] == [
        BLOCKED_HANDLER, BLOCKED_HANDLER,
        UNBLOCKED_HANDLER, UNBLOCKED_HANDLER,
    ]
    assert notifier._wait_event.delays == [0.005, 0.005]
    blocked = [message for handler, message in calls if handler == BLOCKED_HANDLER]
    unblocked = [
        message for handler, message in calls if handler == UNBLOCKED_HANDLER
    ]
    assert blocked[0] is blocked[1]
    assert unblocked[0] is unblocked[1]
    assert (
        blocked[0].lease_id, blocked[0].task_id, blocked[0].attempt_id,
        blocked[0].worker_id, blocked[0].sequence,
    ) == (
        unblocked[0].lease_id, unblocked[0].task_id,
        unblocked[0].attempt_id, unblocked[0].worker_id,
        unblocked[0].sequence,
    )


def test_lost_block_ack_still_sends_matching_unblock_tombstone() -> None:
    calls: list[tuple[str, object]] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        calls.append((handler, message))
        if handler == BLOCKED_HANDLER:
            raise TransportTimeout("all blocked acknowledgements were lost")
        return _reply(message)

    notifier = _notifier(_identity(), rpc=rpc)
    with pytest.raises(BlockingNotificationError, match="did not converge"):
        with notifier.blocking_scope():
            pytest.fail("an unresolved Blocked RPC must not enter user wait")

    blocked = [message for handler, message in calls if handler == BLOCKED_HANDLER]
    unblocked = [
        message for handler, message in calls if handler == UNBLOCKED_HANDLER
    ]
    assert len(blocked) == 3
    assert len(unblocked) == 1
    assert blocked[0] is blocked[1] is blocked[2]
    assert (
        blocked[0].lease_id, blocked[0].task_id, blocked[0].attempt_id,
        blocked[0].worker_id, blocked[0].sequence,
    ) == (
        unblocked[0].lease_id, unblocked[0].task_id,
        unblocked[0].attempt_id, unblocked[0].worker_id,
        unblocked[0].sequence,
    )


def test_reentrant_scope_is_one_episode_and_later_scope_advances_sequence() -> None:
    messages: list[tuple[str, object]] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        messages.append((handler, message))
        return _reply(message)

    notifier = _notifier(_identity(), rpc=rpc)
    with notifier.blocking_scope():
        with notifier.blocking_scope():
            pass
    with notifier.blocking_scope():
        pass

    assert [(handler, message.sequence) for handler, message in messages] == [
        (BLOCKED_HANDLER, 0),
        (UNBLOCKED_HANDLER, 0),
        (BLOCKED_HANDLER, 1),
        (UNBLOCKED_HANDLER, 1),
    ]


def test_lazy_group_without_actual_wait_sends_nothing() -> None:
    messages: list[object] = []
    notifier = _notifier(
        _identity(), rpc=lambda *_args: messages.append(_args)
    )

    with notifier.group_scope():
        pass

    assert messages == []


def test_lazy_group_coalesces_multiple_waits_into_one_episode() -> None:
    messages: list[tuple[str, object]] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        messages.append((handler, message))
        return _reply(message)

    notifier = _notifier(_identity(), rpc=rpc)
    with notifier.group_scope() as group:
        group.begin_blocking()
        group.begin_blocking()

    assert [(handler, message.sequence) for handler, message in messages] == [
        (BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)
    ]


def test_terminal_unblock_reply_safely_converges_after_completion_wins() -> None:
    messages: list[tuple[str, object]] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        messages.append((handler, message))
        if handler == BLOCKED_HANDLER:
            return _reply(message)
        normal = _reply(message)
        return replace(
            normal,
            state=protocol.LeaseExecutionState.COMPLETED,
            accepted=False,
            changed=False,
            error="completion already released the lease",
        )

    notifier = _notifier(_identity(), rpc=rpc)
    with notifier.blocking_scope():
        pass

    assert [handler for handler, _message in messages] == [
        BLOCKED_HANDLER, UNBLOCKED_HANDLER
    ]


def test_terminal_block_reply_never_enters_user_wait_body() -> None:
    entered = False
    calls: list[tuple[str, object]] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        calls.append((handler, message))
        normal = _reply(message)
        return replace(
            normal,
            state=protocol.LeaseExecutionState.WORKER_LOST,
            accepted=False,
            changed=False,
            error="worker was fenced",
        )

    notifier = _notifier(_identity(), rpc=rpc, stopping=lambda: True)
    with pytest.raises(BlockingNotificationError):
        with notifier.blocking_scope():
            entered = True

    assert not entered
    assert [(handler, message.sequence) for handler, message in calls] == [
        (BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)
    ]


def test_permanent_unblock_transport_failure_is_bounded() -> None:
    calls: list[str] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        calls.append(handler)
        if handler == BLOCKED_HANDLER:
            return _reply(message)
        raise TransportTimeout("unblock acknowledgement remains unavailable")

    notifier = _notifier(_identity(), rpc=rpc)
    with pytest.raises(BlockingNotificationError, match="exact replays"):
        with notifier.blocking_scope():
            pass

    assert calls.count(BLOCKED_HANDLER) == 1
    assert calls.count(UNBLOCKED_HANDLER) == 8


def test_deterministic_unblock_rejection_is_not_retried() -> None:
    calls: list[str] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        calls.append(handler)
        reply = _reply(message)
        if handler == UNBLOCKED_HANDLER:
            return replace(
                reply, accepted=False, changed=False,
                error="blocking episode sequence is stale",
            )
        return reply

    notifier = _notifier(_identity(), rpc=rpc)
    with pytest.raises(BlockingNotificationError, match="stale"):
        with notifier.blocking_scope():
            pass

    assert calls == [BLOCKED_HANDLER, UNBLOCKED_HANDLER]
