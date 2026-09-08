"""Pure get deadlines include entry into the CPU-yield notification episode.

Real Core get/get_many and owner handlers run synchronously. Only the clock,
notification entry and selected Event.wait/poll boundaries are simulated.
The borrowed capability is really acquired from an explicitly installed
legacy export pin, normalized to ContainedTransferSource; this is not claimed
to exercise contained-result serialization. The LOST case uses two canonical
stored Tasks: the parent finished, but its child still owns its original
finish barrier, so real owner reconstruction defers with retryable NOT_LOST.

Per case: at most two Tasks/two slots, one 4-KiB store, 128 bytes per published
slot, <=32 observations per bounded channel. No user function, runtime
constructor, thread, socket, timer, process, real RPC or wall-clock wait runs.
Cleanup releases actual handles/capabilities but never fabricates task failure,
clears unfinished barriers, drains unresolved work or claims shutdown/GC.
These assertions bound the next wait, not notification or Unblock RPC latency.
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

from miniray import control, core as core_module, node as node_module, protocol, transport, worker
from miniray.core import CoreWorker, ObjectRef, _ReleaseBorrowedReference
from miniray.ids import AttemptID, TaskID
from miniray.node import NodeServer
from miniray.ownership import ObjectState
from miniray.resources import ResourceVector
from miniray.runtime_binding import ExecutionContext, bind_runtime
from tests.unit._pure_core import SynchronousReferenceMailbox, close_pure_core, make_pure_core
from tests.unit.test_core_lost_blocking_lock_order import _LostFixture, _ScopeProbe
from tests.unit.test_task_finish_barrier import _OutputBackend


pytestmark = pytest.mark.unit
_LIMIT = 32
_BUDGET = 0.005


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations, receipts = [], []

    def forbidden(*args, **kwargs):
        if len(violations) < _LIMIT:
            violations.append((args, kwargs))
        pytest.fail("pure get-deadline check attempted unmodelled runtime work")

    def already_set(event, timeout=None):
        if len(receipts) >= _LIMIT or not event.is_set():
            forbidden("unresolved or excessive Event receipt", timeout)
        receipts.append((event, timeout))
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (CoreWorker, "shutdown"), (NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (worker.WorkerServer, "__init__"),
        (transport.TCPServer, "__init__"), (threading.Thread, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "__init__"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "__init__"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"), (queue.Queue, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, control, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield forbidden
    # Core has best-effort catch paths; their handling cannot erase tripwires.
    assert not violations, violations


class _Clock:
    def __init__(self, forbidden):
        self.forbidden = forbidden
        self.now = 0.0
        self.reads = 0
        self.entries = []
        self.waits = []

    def monotonic(self):
        if self.reads >= _LIMIT:
            self.forbidden("clock read budget exhausted")
        self.reads += 1
        return self.now

    def enter(self, seconds):
        if self.entries or not 0 < seconds <= 0.01:
            self.forbidden("unexpected notification entry", seconds)
        self.entries.append(seconds)
        self.now += seconds

    def wait(self, seconds):
        if len(self.waits) >= _LIMIT or seconds is None or not 0 < seconds <= 0.01:
            self.forbidden("unexpected simulated wait", seconds)
        self.waits.append(seconds)
        self.now += seconds
        return False


def _install_clock(monkeypatch, forbidden):
    clock = _Clock(forbidden)
    monkeypatch.setattr(core_module, "time", SimpleNamespace(monotonic=clock.monotonic))
    return clock


def _context(core, notifier):
    parent = TaskID.derive(core.job_id, core.driver_task_id, 91)
    return ExecutionContext(core.job_id, parent, AttemptID(parent, 0), blocking_notifier=notifier)


class _PendingFixture:
    """One actually admitted but never executed canonical Task."""

    def __init__(self, forbidden):
        self.forbidden = forbidden
        self.core = core = make_pure_core()
        self.pending, self.ref = core._register_submission(
            core.define_remote_function(forbidden), (), {}, ResourceVector({"CPU": 1}),
            max_retries=1, _enqueue=True,
        )
        assert core._submissions.get_nowait() is self.pending
        core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        self.before = core.owner_table.snapshot(self.ref.object_id)
        assert self.before.state is ObjectState.PENDING
        assert core._task_finish_barriers[self.ref.object_id] is self.pending
        self.backend = None

    def assert_pending(self):
        assert self.core.owner_table.snapshot(self.ref.object_id) == self.before
        assert self.core._task_finish_barriers[self.ref.object_id] is self.pending
        assert self.core._accepted_task_count == 1
        assert self.core._submissions.empty()
        assert not self.core._objects[self.ref.object_id].event.is_set()

    def publish(self, *, error):
        if error:
            assert self.core._publish_task_error(self.pending, RuntimeError("entered-task-error"))
        else:
            self.core.gcs_address = ("deadline-output-control.invalid", 1)
            self.backend = _OutputBackend(self.core, self.forbidden)

            def rpc(address, handler, request):
                if len(self.backend.calls) >= _LIMIT:
                    self.forbidden("output route budget exhausted", handler)
                return self.backend.rpc(address, handler, request)

            self.core._rpc = rpc
            self.core._resolve_node_address = self.backend.address
            self.backend.succeed(self.pending, stored=False, value=42)
        assert self.core._finish_pending_task(self.pending)
        assert self.core._objects[self.ref.object_id].event.is_set()
        assert not self.core._task_finish_barriers and self.core._accepted_task_count == 0

    def close(self):
        self.ref.close(timeout=0)
        assert self.core._reference_mailbox.pending.qsize() <= _LIMIT
        assert self.core._submissions.qsize() <= _LIMIT
        close_pure_core(self.core)


def _local_wait(monkeypatch, fixture, clock, notifier):
    event = fixture.core._objects[fixture.ref.object_id].event

    def wait(seconds=None):
        if fixture.core._state_lock._is_owned() or event.is_set() or notifier.depth not in (1, 2):
            fixture.forbidden("unexpected local PENDING wait boundary")
        return clock.wait(seconds)

    monkeypatch.setattr(event, "wait", wait)


class _BorrowMailbox(SynchronousReferenceMailbox):
    """One real Release/ACK at close; owner GC remains under its own rules."""

    def __init__(self, core, forbidden):
        super().__init__(core)
        self.forbidden, self.borrowed_releases = forbidden, 0

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        if self.borrowed_releases or event.done is None or event.scheduled_round is not None:
            self.forbidden("unexpected borrowed release event")
        self.borrowed_releases += 1
        core = self.core_reference()
        assert core is not None
        try:
            if not core._drive_borrowed_reference_release(event.key):
                self.forbidden("exact borrowed release did not converge")
        finally:
            event.done.set()
        return True


class _Borrower:
    """A real active owner capability; transport alone is an exact callback."""

    def __init__(self, owner, local_ref, forbidden):
        self.owner, self.forbidden = owner, forbidden
        self.core = core = make_pure_core()
        core._reference_mailbox = _BorrowMailbox(core, forbidden)
        self.calls, self.replies = [], []
        self.source = protocol.ContainedTransferSource("deadline-fixture-export")
        self.acquire = protocol.AcquireBorrowedObject(
            local_ref.object_id, owner.worker_id, core.worker_id, self.source, "deadline-fixture-borrow",
        )
        self.release = protocol.ReleaseBorrowedObject(
            local_ref.object_id, owner.worker_id, core.worker_id, self.acquire.borrower_token,
        )
        # Supported explicit legacy export, not an invented live outer object.
        assert owner.owner_table.add_contained_reference(local_ref.object_id, self.source.hold)
        self.key, self.obligation, inserted = core._register_borrowed_release_obligation(
            owner.owner_address, self.acquire, self.release,
        )
        assert inserted
        acquired = owner.acquire_exported_reference(self.acquire)
        assert acquired.accepted and acquired.acquired and acquired.source == self.source
        self.ref = ObjectRef(local_ref.object_id, owner.worker_id, owner.owner_address)
        self.ref._bind_borrowed_reference(core, self.acquire.borrower_token, self.source)
        core._borrow_rpc = self.rpc
        core._borrow_rpc_with_deadline = self.rpc
        self.assert_active()

    def rpc(self, address, handler, request, remaining=None):
        if len(self.calls) >= _LIMIT or address != self.owner.owner_address:
            self.forbidden("unexpected owner route", address, handler)
        expected = {
            "get_owned_object": (protocol.GetOwnedObject, self.owner.get_owned_object),
            "request_owned_object_reconstruction": (
                protocol.RequestOwnedObjectReconstruction, self.owner.request_owned_object_reconstruction,
            ),
            "release_borrowed_object": (protocol.ReleaseBorrowedObject, self.owner.release_borrowed_reference),
        }
        if handler not in expected or type(request) is not expected[handler][0]:
            self.forbidden("unexpected owner request type", handler, type(request))
        requester = (request.requester_worker_id if type(request) is protocol.RequestOwnedObjectReconstruction
                     else request.borrower_worker_id)
        if (request.object_id != self.ref.object_id or request.owner_worker_id != self.owner.worker_id
                or requester != self.core.worker_id or request.borrower_token != self.ref.borrower_token):
            self.forbidden("owner route identity drift", request)
        if handler != "release_borrowed_object":
            if remaining is None or not 0 < remaining <= _BUDGET:
                self.forbidden("out-of-budget owner poll", remaining)
            assert core_module._RPC_CALL_DEADLINE.get() == pytest.approx(_BUDGET)
        self.calls.append((handler, request, remaining))
        reply = expected[handler][1](request)
        self.replies.append(reply)
        return reply

    def assert_active(self):
        assert self.core._active_borrower_capability(self.ref) == self.acquire
        snapshot = self.owner.owner_table.snapshot(self.ref.object_id)
        token = (self.core.worker_id, self.ref.borrower_token)
        assert token in snapshot.borrowed_tokens
        assert dict(snapshot.borrowed_sources)[token] == self.source
        assert not self.obligation.release_requested
        assert self.core._borrowed_release_obligations == {self.key: self.obligation}

    def close(self):
        # The owner's local ref remains alive throughout this release. In the
        # LOST case nothing here finishes or erases the descendant's Task.
        self.ref.close(timeout=0)
        assert not self.core._borrowed_release_obligations
        assert self.core._reference_mailbox.borrowed_releases == 1
        reply = self.owner.release_contained_reference(protocol.ReleaseContainedReference(
            self.ref.object_id, self.owner.worker_id, self.source.hold,
        ))
        assert reply.accepted and reply.released
        snapshot = self.owner.owner_table.snapshot(self.ref.object_id)
        assert not snapshot.borrowed_tokens and self.source.hold not in snapshot.contained_holds
        assert snapshot.local_tokens  # No normal owner collection can run yet.
        assert self.core._submissions.empty() and self.core._accepted_task_count == 0
        close_pure_core(self.core)


def test_local_pending_wait_uses_budget_remaining_after_notification(monkeypatch, _no_runtime):
    f = _PendingFixture(_no_runtime)
    try:
        clock = _install_clock(monkeypatch, _no_runtime)
        notifier = _ScopeProbe(f.core, _no_runtime, lambda: clock.enter(0.003))
        _local_wait(monkeypatch, f, clock, notifier)
        with bind_runtime(f.core, _context(f.core, notifier)):
            with pytest.raises(TimeoutError, match="not ready"):
                f.core.get(f.ref, timeout=_BUDGET)
        assert clock.waits == pytest.approx([0.002])
        assert clock.now == pytest.approx(_BUDGET)
        notifier.assert_unlocked((("enter", 1), ("exit", 1)))
        f.assert_pending()
    finally:
        f.close()


def test_local_pending_get_many_does_not_wait_after_group_exhausts_budget(monkeypatch, _no_runtime):
    f = _PendingFixture(_no_runtime)
    try:
        clock = _install_clock(monkeypatch, _no_runtime)
        notifier = _ScopeProbe(f.core, _no_runtime, lambda: clock.enter(0.01))
        _local_wait(monkeypatch, f, clock, notifier)
        with bind_runtime(f.core, _context(f.core, notifier)):
            with pytest.raises(TimeoutError, match="not ready"):
                f.core.get_many((f.ref,), timeout=_BUDGET)
        assert clock.waits == [] and clock.now == 0.01
        notifier.assert_unlocked((("enter", 1), ("enter", 2), ("exit", 2), ("exit", 1)))
        f.assert_pending()
    finally:
        f.close()


@pytest.mark.parametrize("error", (False, True), ids=("ready-inline", "task-error"))
def test_local_notification_completion_retains_terminal_precedence_after_deadline(monkeypatch, _no_runtime, error):
    f = _PendingFixture(_no_runtime)
    try:
        clock = _install_clock(monkeypatch, _no_runtime)

        def publish_on_enter():
            f.publish(error=error)
            clock.enter(0.01)

        notifier = _ScopeProbe(f.core, _no_runtime, publish_on_enter)
        with bind_runtime(f.core, _context(f.core, notifier)):
            if error:
                with pytest.raises(RuntimeError, match="entered-task-error"):
                    f.core.get(f.ref, timeout=_BUDGET)
            else:
                assert f.core.get(f.ref, timeout=_BUDGET) == 42
        snapshot = f.core.owner_table.snapshot(f.ref.object_id)
        assert snapshot.state is (ObjectState.ERROR if error else ObjectState.READY_INLINE)
        assert clock.waits == [] and clock.now == 0.01
        notifier.assert_unlocked((("enter", 1), ("exit", 1)))
    finally:
        f.close()


def _check_foreign_pending(monkeypatch, forbidden, *, elapsed, aggregate):
    f = _PendingFixture(forbidden)
    borrower = None
    try:
        borrower = _Borrower(f.core, f.ref, forbidden)
        before = f.core.owner_table.snapshot(f.ref.object_id)
        clock = _install_clock(monkeypatch, forbidden)
        monkeypatch.setattr(core_module, "_BORROW_POLL_EVENT", clock)
        notifier = _ScopeProbe(borrower.core, forbidden, lambda: clock.enter(elapsed))
        previous_deadline = core_module._RPC_CALL_DEADLINE.get()
        with bind_runtime(borrower.core, _context(borrower.core, notifier)):
            with pytest.raises(TimeoutError, match="not ready"):
                if aggregate:
                    borrower.core.get_many((borrower.ref,), timeout=_BUDGET)
                else:
                    borrower.core.get(borrower.ref, timeout=_BUDGET)
        assert core_module._RPC_CALL_DEADLINE.get() == previous_deadline
        assert [handler for handler, _, _ in borrower.calls] == ["get_owned_object"]
        assert len(borrower.replies) == 1 and borrower.replies[0].accepted
        assert borrower.replies[0].state is protocol.OwnedObjectState.PENDING
        assert borrower.replies[0].current_attempt == f.pending.spec.attempt_id
        assert clock.waits == pytest.approx([] if aggregate else [0.002])
        assert clock.now == pytest.approx(elapsed if aggregate else _BUDGET)
        expected = (("enter", 1), ("enter", 2), ("exit", 2), ("exit", 1)) if aggregate else (
            ("enter", 1), ("exit", 1),
        )
        notifier.assert_unlocked(expected)
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


def test_foreign_pending_poll_uses_budget_remaining_after_notification(monkeypatch, _no_runtime):
    _check_foreign_pending(monkeypatch, _no_runtime, elapsed=0.003, aggregate=False)


def test_foreign_pending_get_many_does_not_poll_after_group_exhausts_budget(monkeypatch, _no_runtime):
    _check_foreign_pending(monkeypatch, _no_runtime, elapsed=0.01, aggregate=True)


def test_foreign_lost_deferred_by_real_owner_does_not_poll_after_notification_exhausts_budget(monkeypatch, _no_runtime):
    f = _LostFixture(_no_runtime, descendant=True)
    borrower = None
    try:
        borrower = _Borrower(f.core, f.ref, _no_runtime)
        before = f.metadata()
        f.observe_admission(monkeypatch)
        clock = _install_clock(monkeypatch, _no_runtime)
        monkeypatch.setattr(core_module, "_BORROW_POLL_EVENT", clock)
        notifier = _ScopeProbe(borrower.core, _no_runtime, lambda: clock.enter(0.01))
        previous_deadline = core_module._RPC_CALL_DEADLINE.get()
        with bind_runtime(borrower.core, _context(borrower.core, notifier)):
            with pytest.raises(TimeoutError, match="not ready"):
                borrower.core.get(borrower.ref, timeout=_BUDGET)
        assert core_module._RPC_CALL_DEADLINE.get() == previous_deadline
        assert [handler for handler, _, _ in borrower.calls] == [
            "get_owned_object", "request_owned_object_reconstruction",
        ]
        observed, deferred = borrower.replies
        assert observed.accepted and observed.state is protocol.OwnedObjectState.LOST
        request = borrower.calls[1][1]
        assert request.credential == protocol.BorrowedCredential(borrower.source, borrower.ref.borrower_token)
        assert request.expected_owner_attempt == f.pending.spec.attempt_id
        assert deferred.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
        assert deferred.failure is protocol.OwnedObjectReconstructionFailure.NOT_LOST
        assert f.admissions == [(f.pending.object_id, None)]
        assert clock.waits == [] and clock.now == 0.01
        notifier.assert_unlocked((("enter", 1), ("exit", 1)))
        borrower.assert_active()
        assert f.metadata() == before
        assert f.core._task_finish_barriers[f.child.object_id] is f.child
        assert f.pending.object_id not in f.core._task_finish_barriers
        assert not f.core._reconstruction._sessions
        assert not f.core._targeted_reconstruction.active_task_ids()
        assert f.backend.store.used_bytes == 0 and f.take() == ()
    finally:
        try:
            if borrower is not None:
                borrower.close()
        finally:
            f.close()
