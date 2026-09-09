"""Pure failure-atomic entry for a real BlockingNotifier and its lazy groups.

A finite context-manager probe acquires the notifier's actual threading.Lock
nonblocking, or raises KeyboardInterrupt once before acquisition. This models
an interrupted entry, not a delivered signal or an OS/thread contention test.
A separate one-shot MemoryError models local message allocation failure with
a valid immutable BlockingIdentity; it is not recovery from forged identity.

The RPC callback returns genuine typed ACK/rejection values. It is an isolated
notification boundary, not a running Node or a proof of CPU resource effects.
Group retry deliberately catches an entry error inside the same helper group;
ordinary Core.get_many instead propagates that error and discards its group.
No user task, Core/Node/Worker runtime, thread, socket, timer, process, transport
or actual wait runs. At most two lock acquisitions and four typed RPC calls
occur. Guard violations survive any production exception/finalizer handling.

The final representative Core case uses one genuinely PENDING Task and two
threadless Cores with a really acquired typed contained borrower capability. Its
notifier is an explicit failed-entry CM, not a real Node notification: one
owner Get runs, no polling/storage work or Block RPC runs, and teardown applies
the real borrowed/contained/local releases without finishing or collecting the
Task. Only the two already-set local release receipts may be checked. This
tests the PENDING manual-scope branch, not the other two foreign branches.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import blocking as blocking_module, control, core as core_module, node, protocol, transport, worker
from miniray.blocking import BLOCKED_HANDLER, UNBLOCKED_HANDLER, BlockingNotificationError, BlockingNotifier
from miniray.core import CoreWorker, _LazyBlockingGroup
from tests.unit.test_blocking_notifier import _identity


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations = []

    def forbidden(*args, **kwargs):
        if len(violations) < 16:
            violations.append((args, kwargs))
        pytest.fail("pure notification-entry contract attempted unmodelled work")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (CoreWorker, "shutdown"), (node.NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (worker.WorkerServer, "__init__"),
        (transport.TCPServer, "__init__"), (threading.Thread, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "__init__"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "__init__"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (blocking_module, core_module, node, control, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield forbidden
    assert not violations, violations


class _EntryLock:
    """Only the entry boundary is simulated; successful custody is real."""

    def __init__(self, lock, forbidden, *, interrupt_once=False):
        self.lock, self.forbidden = lock, forbidden
        self.interrupt_once = interrupt_once
        self.entries = 0
        self.acquisitions = 0
        self.exits = 0
        self.interruption = KeyboardInterrupt("simulated episode-lock entry interruption")

    def __enter__(self):
        if self.entries >= 2:
            self.forbidden("episode-lock entry budget exhausted")
        self.entries += 1
        if self.interrupt_once:
            self.interrupt_once = False
            raise self.interruption
        if not self.lock.acquire(blocking=False):
            self.forbidden("unmodelled lock contention; never block")
        self.acquisitions += 1
        return self

    def __exit__(self, *_exc):
        if self.exits >= 2 or not self.lock.locked():
            self.forbidden("invalid episode-lock exit")
        self.exits += 1
        self.lock.release()


class _Boundary:
    def __init__(self, forbidden, *, reject_first=False, interrupt_once=False):
        self.forbidden, self.reject_first = forbidden, reject_first
        self.identity = _identity()
        # Save real classes before the constructor-failure case wraps the
        # exported Block constructor. ACK type checks remain genuine.
        self.block_type = protocol.NotifyWorkerBlocked
        self.unblock_type = protocol.NotifyWorkerUnblocked
        self.calls, self.acceptances = [], []
        self.notifier = BlockingNotifier(self.identity, rpc=self.rpc)
        self.lock = _EntryLock(self.notifier._episode_lock, forbidden, interrupt_once=interrupt_once)
        self.notifier._episode_lock = self.lock

    def rpc(self, address, handler, message):
        if len(self.calls) >= 4 or not self.lock.lock.locked():
            self.forbidden("unexpected notification RPC boundary", handler)
        if address != self.identity.node_address or handler not in (BLOCKED_HANDLER, UNBLOCKED_HANDLER):
            self.forbidden("notification route drift", address, handler)
        expected_type = self.block_type if handler == BLOCKED_HANDLER else self.unblock_type
        if type(message) is not expected_type:
            self.forbidden("notification message type drift", type(message))
        identity = self.identity
        if (message.lease_id, message.task_id, message.attempt_id, message.worker_id) != (
            identity.lease_id, identity.task_id, identity.attempt_id, identity.worker_id,
        ):
            self.forbidden("notification identity drift")
        accepted = not (self.reject_first and handler == BLOCKED_HANDLER and not self.calls)
        self.calls.append((handler, message))
        self.acceptances.append(accepted)
        reply_type = (protocol.NotifyWorkerBlockedReply if handler == BLOCKED_HANDLER
                      else protocol.NotifyWorkerUnblockedReply)
        return reply_type(
            message.lease_id, message.task_id, message.attempt_id, message.worker_id, message.sequence,
            protocol.LeaseExecutionState.RUNNING, accepted, accepted,
            None if accepted else "injected typed Block rejection",
        )

    def phases(self):
        return [(handler, message.sequence) for handler, message in self.calls]

    def assert_body_authorized(self, sequence):
        # This is a pure observation, never an executed user wait. Do not
        # accept a retry as active merely because begin_blocking returned.
        assert self.calls and self.calls[-1][0] == BLOCKED_HANDLER, self.phases()
        assert self.calls[-1][1].sequence == sequence
        assert self.acceptances[-1]
        assert getattr(self.notifier._local, "depth", 0) == 1
        assert self.lock.lock.locked()

    def assert_closed(self):
        assert getattr(self.notifier._local, "depth", 0) == 0
        assert not self.lock.lock.locked()
        assert self.lock.exits == self.lock.acquisitions
        assert self.lock.entries <= 2 and len(self.calls) <= 4


def _group(boundary, kind):
    if kind == "native":
        return boundary.notifier.group_scope()
    # A minimal notifier facade deliberately lacks group_scope so this tests
    # Core's supported fallback, not its delegation back to the native group.
    facade = SimpleNamespace(blocking_scope=boundary.notifier.blocking_scope)
    result = _LazyBlockingGroup(facade)
    assert result._group is None
    return result


def test_interrupted_episode_lock_entry_does_not_poison_same_thread_retry(_no_runtime):
    boundary = _Boundary(_no_runtime, interrupt_once=True)
    notifier = boundary.notifier
    with pytest.raises(KeyboardInterrupt) as failure:
        with notifier.blocking_scope():
            _no_runtime("interrupted lock entry reached scope body")
    assert failure.value is boundary.lock.interruption
    after_failure = (getattr(notifier._local, "depth", 0), notifier._sequence, tuple(boundary.calls))
    assert not boundary.lock.lock.locked()

    with notifier.blocking_scope():
        boundary.assert_body_authorized(0)
    assert after_failure == (0, -1, ())
    assert boundary.phases() == [(BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)]
    assert boundary.lock.entries == 2 and boundary.lock.acquisitions == 1
    boundary.assert_closed()


def test_pre_rpc_block_construction_failure_does_not_spend_episode_sequence(monkeypatch, _no_runtime):
    boundary = _Boundary(_no_runtime)
    notifier = boundary.notifier
    original = boundary.block_type
    constructions = []
    allocation_failure = MemoryError("simulated local Block construction failure")

    def construct_once_failing(*args, **kwargs):
        if len(constructions) >= 2:
            _no_runtime("Block construction budget exhausted")
        constructions.append((args, kwargs))
        if len(constructions) == 1:
            raise allocation_failure
        return original(*args, **kwargs)

    monkeypatch.setattr(protocol, "NotifyWorkerBlocked", construct_once_failing)
    with pytest.raises(MemoryError) as failure:
        with notifier.blocking_scope():
            _no_runtime("failed message construction reached scope body")
    assert failure.value is allocation_failure
    after_failure = (getattr(notifier._local, "depth", 0), notifier._sequence, tuple(boundary.calls))
    assert not boundary.lock.lock.locked()

    with notifier.blocking_scope():
        boundary.assert_body_authorized(0)
    assert after_failure == (0, -1, ())
    assert len(constructions) == 2
    assert boundary.phases() == [(BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)]
    assert boundary.lock.entries == boundary.lock.acquisitions == 2
    boundary.assert_closed()


@pytest.mark.parametrize("kind", ("native", "core-fallback"))
def test_failed_group_entry_can_be_retried_without_skipping_a_new_block(_no_runtime, kind):
    boundary = _Boundary(_no_runtime, reject_first=True)
    group = _group(boundary, kind)
    with group:
        with pytest.raises(BlockingNotificationError, match="injected typed Block rejection"):
            group.begin_blocking()
        assert boundary.phases() == [(BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)]
        boundary.assert_closed()
        failed_scope = group._scope
        group.begin_blocking()
        boundary.assert_body_authorized(1)
        assert failed_scope is None
        group.begin_blocking()  # Successful repeated entry still coalesces.
        boundary.assert_body_authorized(1)
    assert boundary.phases() == [
        (BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0),
        (BLOCKED_HANDLER, 1), (UNBLOCKED_HANDLER, 1),
    ]
    assert boundary.acceptances == [False, True, True, True]
    assert group._scope is None
    boundary.assert_closed()


@pytest.mark.parametrize("kind", ("native", "core-fallback"))
def test_group_unwind_preserves_entry_error_without_another_unblock(_no_runtime, kind):
    boundary = _Boundary(_no_runtime, reject_first=True)
    group = _group(boundary, kind)
    caught = []
    with pytest.raises(BlockingNotificationError, match="injected typed Block rejection") as failure:
        with group:
            try:
                group.begin_blocking()
            except BlockingNotificationError as exc:
                caught.append(exc)
                raise
            _no_runtime("rejected group entry reached wait body")
    assert caught == [failure.value]
    assert boundary.phases() == [(BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)]
    assert boundary.acceptances == [False, True]
    assert group._scope is None
    group.close()
    assert boundary.phases() == [(BLOCKED_HANDLER, 0), (UNBLOCKED_HANDLER, 0)]
    boundary.assert_closed()


def test_foreign_pending_get_does_not_exit_a_scope_that_failed_to_enter(monkeypatch, _no_runtime):
    from miniray.runtime_binding import bind_runtime
    from tests.unit.test_get_notification_deadline import (
        _BUDGET, _Borrower, _PendingFixture, _context, _install_clock,
    )

    f = _PendingFixture(_no_runtime)
    borrower = None
    receipts = []

    def allow_one_completed_receipt(ref):
        event = ref._release_done
        assert event is not None

        def check(timeout=None):
            if len(receipts) >= 2 or event in receipts or not event.is_set() or timeout != 0:
                _no_runtime("unexpected foreign-entry cleanup receipt", timeout)
            receipts.append(event)
            return True

        monkeypatch.setattr(event, "wait", check)

    allow_one_completed_receipt(f.ref)
    try:
        borrower = _Borrower(f.core, f.ref, _no_runtime)
        allow_one_completed_receipt(borrower.ref)
        before = f.core.owner_table.snapshot(f.ref.object_id)
        clock = _install_clock(monkeypatch, _no_runtime)
        monkeypatch.setattr(core_module, "_BORROW_POLL_EVENT", SimpleNamespace(wait=_no_runtime))
        monkeypatch.setattr(borrower.core, "_fetch_borrowed_stored_object", _no_runtime)
        entry_error = BlockingNotificationError("injected foreign notification entry failure")
        observations = []

        class FailedEntry:
            def __enter__(self):
                if observations:
                    _no_runtime("foreign notification entered more than once")
                observations.append("enter")
                assert not borrower.core._state_lock._is_owned()
                raise entry_error

            def __exit__(self, *_exc):
                if len(observations) >= 2:
                    _no_runtime("foreign notification exit observation budget exhausted")
                observations.append("exit")

        scope = FailedEntry()
        factories = []

        def blocking_scope():
            if factories:
                _no_runtime("foreign notification scope factory called twice")
            factories.append(scope)
            return scope

        notifier = SimpleNamespace(blocking_scope=blocking_scope)
        previous_deadline = core_module._RPC_CALL_DEADLINE.get()
        with bind_runtime(borrower.core, _context(borrower.core, notifier)):
            with pytest.raises(BlockingNotificationError) as failure:
                borrower.core.get(borrower.ref, timeout=_BUDGET)
        assert failure.value is entry_error
        assert observations == ["enter"] and factories == [scope]
        assert core_module._RPC_CALL_DEADLINE.get() == previous_deadline
        assert [handler for handler, _, _ in borrower.calls] == ["get_owned_object"]
        (reply,) = borrower.replies
        assert reply.accepted and reply.state is protocol.OwnedObjectState.PENDING
        assert reply.current_attempt == f.pending.spec.attempt_id
        assert clock.waits == [] and clock.now == 0.0
        borrower.assert_active()
        assert f.core.owner_table.snapshot(f.ref.object_id) == before
        assert f.core._task_finish_barriers[f.ref.object_id] is f.pending
        assert f.core._accepted_task_count == 1 and f.core._submissions.empty()
    finally:
        try:
            if borrower is not None:
                borrower.close()
        finally:
            f.close()
    assert len(receipts) == 2
    assert not borrower.core._borrowed_release_obligations
    assert [handler for handler, _, _ in borrower.calls] == [
        "get_owned_object", "release_borrowed_object",
    ]
    assert f.core._task_finish_barriers[f.ref.object_id] is f.pending
    assert f.core._accepted_task_count == 1
