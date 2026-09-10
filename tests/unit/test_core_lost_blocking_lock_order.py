"""Pure lock-order checks at Core's two genuine LOST wait boundaries.

One threadless Core retains its real RLock and Condition. One existing bounded
output backend supplies actual discovery, Node store/journal, publication and
owner/recovery transitions: at most two logical Tasks, two publications, one
4-KiB store and <=128 bytes per selected slot. Core.drop_object removes actual
stored bytes; submission itself installed every finish barrier.

The notifier observes scope-entry/exit locks, not CPU or transport effects.
Only the selected Condition.wait boundary is replaced by a one-shot sentinel.
No thread, socket, timer, user callable or real wait runs. Interrupted cases
deliberately retain their admitted finalizer/lineage state; cleanup closes real
test handles without claiming distributed shutdown. No cross-thread runtime
binding or automatic ExecutionContext inheritance is asserted.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import core as core_module, node as node_module, transport, worker
from miniray.blocking import BlockingGroupScope
from miniray.control import GCSLite
from miniray.core import CoreWorker, _PendingTask, _WAKE_COORDINATOR
from miniray.ids import AttemptID, TaskID
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from miniray.runtime_binding import ExecutionContext, bind_runtime
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_task_finish_barrier import _OutputBackend


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations, receipts = [], []

    def forbidden(*args, **kwargs):
        if len(violations) < 16:
            violations.append((args, kwargs))
        pytest.fail("pure LOST lock-order check attempted unmodelled runtime work")

    def already_set(event, timeout=None):
        if len(receipts) >= 32 or not event.is_set():
            forbidden("unbounded or unresolved local-handle receipt", timeout)
        receipts.append((event, timeout))
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (NodeServer, "__init__"), (GCSLite, "__init__"),
        (worker.WorkerServer, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(queue.Queue, "join", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield forbidden
    assert not violations, violations


class _StopWait(BaseException):
    """A finite observation point, not a Task failure or fake wakeup."""


class _Clock:
    def __init__(self):
        self.now = 10.0

    def monotonic(self):
        return self.now


class _ScopeProbe:
    """Record both locks before assertions, preserving exception unwinding."""

    def __init__(self, core, forbidden, on_enter=None):
        self.core, self.forbidden, self.on_enter = core, forbidden, on_enter
        self.depth = 0
        self.events = []

    def group_scope(self):
        return BlockingGroupScope(self)

    @contextmanager
    def blocking_scope(self):
        if len(self.events) >= 16:
            self.forbidden("too many notifier scope observations")
        self.depth += 1
        depth = self.depth
        self.events.append(("enter", depth, self.core._state_lock._is_owned()))
        try:
            if depth == 1 and self.on_enter is not None:
                self.on_enter()
            yield
        finally:
            if len(self.events) >= 16:
                self.forbidden("too many notifier scope observations")
            self.events.append(("exit", depth, self.core._state_lock._is_owned()))
            self.depth -= 1

    def assert_unlocked(self, expected):
        assert tuple((phase, depth) for phase, depth, _ in self.events) == expected
        assert self.depth == 0
        assert not any(locked for _, _, locked in self.events), self.events


class _LostFixture:
    def __init__(self, forbidden, *, descendant=False):
        self.forbidden = forbidden
        self.core = core = make_pure_core()
        self.lock, self.condition = core._state_lock, core._completion
        core.gcs_address = ("lost-lock-control.invalid", 1)
        self.backend = _OutputBackend(core, forbidden)

        def rpc(address, handler, request):
            if len(self.backend.calls) >= 32:
                forbidden("output callback budget exhausted", handler)
            return self.backend.rpc(address, handler, request)

        core._rpc, core._resolve_node_address = rpc, self.backend.address
        self.refs, self.tasks = [], []
        self.admissions = []
        self.child = None
        if descendant:
            self.child, child_ref = self.submit()
            self.backend.succeed(self.child, stored=True, value=7)
            self.pending, self.ref = self.submit(child_ref)
            self.backend.succeed(self.pending, stored=True, value=11)
            assert core._finish_pending_task(self.pending)
            assert self.take() == ()
            # The parent can complete while its already-readable dependency
            # still owns its original finalizer barrier. Neither is fabricated.
            assert self.pending.object_id not in core._task_finish_barriers
            assert core._task_finish_barriers[self.child.object_id] == self.child
            assert core.drop_object(child_ref)
            assert core.drop_object(self.ref)
            graph = core._reconstruction_coordinator().preflight_graph(self.pending.object_id)
            assert tuple(step.task_id for step in graph.steps) == (self.child.task_id, self.pending.task_id)
            self.blocked_output = self.child.object_id
        else:
            self.pending, self.ref = self.submit()
            self.backend.succeed(self.pending, stored=True, value=7)
            assert core._task_finish_barriers[self.pending.object_id] == self.pending
            assert core.drop_object(self.ref)
            self.blocked_output = self.pending.object_id
        assert self.take() == ()
        assert self.backend.store.used_bytes == 0
        assert core._accepted_task_count == 1
        self.before = self.metadata()
        for pending in self.tasks:
            snapshot = core.owner_table.snapshot(pending.object_id)
            assert snapshot.state is ObjectState.LOST and not snapshot.locations
            assert snapshot.output_publication is not None
            assert snapshot.output_retirement_id is None
            assert snapshot.producer_task_spec == pending.spec
            assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
            assert core._recovery.task_record(pending.task_id).retries_started == 0

    def submit(self, *args):
        if len(self.tasks) >= 2:
            self.forbidden("logical Task fixture budget exhausted")
        pending, ref = self.core._register_submission(
            self.core.define_remote_function(self.forbidden),
            args, {}, ResourceVector({"CPU": 1}), max_retries=2, _enqueue=True,
        )
        self.tasks.append(pending)
        self.refs.append(ref)
        assert self.take() == (pending,)
        return pending, ref

    def take(self):
        size = self.core._submissions.qsize()
        assert size <= 16
        pending = []
        for _ in range(size):
            item = self.core._submissions.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) is _PendingTask
                    pending.append(item)
            finally:
                self.core._submissions.task_done()
        assert self.core._submissions.empty() and self.core._submissions.unfinished_tasks == 0
        return tuple(pending)

    def metadata(self):
        return (
            tuple(self.core.owner_table.snapshot(pending.object_id) for pending in self.tasks),
            tuple(replace(self.core._recovery.task_record(pending.task_id)) for pending in self.tasks),
            dict(self.core._task_finish_barriers), self.core._accepted_task_count,
        )

    def observe_admission(self, monkeypatch):
        original = self.core._start_or_join_reconstruction

        def observe(object_id, waiter, **options):
            if len(self.admissions) >= 2:
                self.forbidden("reconstruction admission observation budget exhausted")
            result = original(object_id, waiter, **options)
            self.admissions.append((object_id, result))
            return result

        monkeypatch.setattr(self.core, "_start_or_join_reconstruction", observe)

    def observe_wait(self, monkeypatch):
        waits = []

        def wait(timeout=None):
            assert self.core._completion is self.condition
            assert self.core._state_lock is self.lock and self.lock._is_owned()
            if waits or timeout is None or not 0 < timeout <= 1.0:
                self.forbidden("unexpected Condition wait observation", timeout)
            waits.append(timeout)
            raise _StopWait("observed one locked Condition wait boundary")

        monkeypatch.setattr(self.condition, "wait", wait)
        return waits

    def context(self, notifier):
        parent = TaskID.derive(self.core.job_id, self.core.driver_task_id, 91)
        return ExecutionContext(
            self.core.job_id, parent, AttemptID(parent, 0), blocking_notifier=notifier,
        )

    def assert_deferred(self):
        assert self.metadata() == self.before
        assert self.core._task_finish_barriers[self.blocked_output] in self.tasks
        assert not self.core._reconstruction._sessions
        assert all(self.core._recovery.active_recovery(task.task_id) is None for task in self.tasks)
        assert not getattr(self.core, "_output_retirement_work", {})
        assert not self.core.owner_table.has_active_output_retirements()
        assert not self.core._protocol_unresolved
        assert self.backend.store.used_bytes == 0 and self.take() == ()

    def collect_completed(self):
        assert self.core._accepted_task_count == 0 and not self.core._task_finish_barriers
        for ref in self.refs:
            ref.close(timeout=0)
        assert self.core._reference_mailbox.pending.qsize() <= 16
        self.core._reference_mailbox.drain()
        for pending in self.tasks:
            assert self.core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
            assert self.core._recovery.lineage_for_object(pending.object_id) is None
        for identity in self.backend.completed:
            snapshot = self.backend.handoff_snapshot(identity)
            assert snapshot.complete is not None and snapshot.adoption is not None
            assert self.core.owner_table.collection_state(((identity.object_id,))[0]) is ObjectCollectionState.COLLECTED
            assert not self.backend.journal.snapshot(identity).result_retained
        assert not self.core._objects and not self.core._stored_descriptors
        assert not self.core._object_gc_obligations and self.backend.store.used_bytes == 0
        assert self.take() == ()

    def close(self):
        # Exception-boundary tests retain valid unfinished Tasks. Do not
        # publish synthetic errors, finish them, clear barriers or drive GC.
        for ref in self.refs:
            ref.close(timeout=0)
        close_pure_core(self.core)


@pytest.mark.parametrize("descendant", (False, True), ids=("own-finish-barrier", "descendant-finish-barrier"))
def test_lost_wait_enters_and_exits_notifier_outside_real_core_lock(monkeypatch, _no_runtime, descendant):
    f = _LostFixture(_no_runtime, descendant=descendant)
    try:
        f.observe_admission(monkeypatch)
        waits = f.observe_wait(monkeypatch)
        notifier = _ScopeProbe(f.core, _no_runtime)
        with bind_runtime(f.core, f.context(notifier)):
            with pytest.raises(_StopWait):
                if descendant:
                    f.core.get_many((f.ref,), timeout=1.0)
                else:
                    f.core.get(f.ref, timeout=1.0)
        assert len(waits) == 1
        assert f.admissions == ([(f.pending.object_id, None)] if descendant else [])
        f.assert_deferred()
        expected = (("enter", 1), ("enter", 2), ("exit", 2), ("exit", 1)) if descendant else (
            ("enter", 1), ("exit", 1),
        )
        notifier.assert_unlocked(expected)
    finally:
        f.close()


def test_finishing_and_republishing_during_scope_entry_does_not_lose_wakeup(monkeypatch, _no_runtime):
    f = _LostFixture(_no_runtime)
    try:
        f.observe_admission(monkeypatch)
        waits = f.observe_wait(monkeypatch)
        reconstructed = []

        def finish_and_publish():
            assert not reconstructed
            assert f.core._finish_pending_task(f.pending) and f.take() == ()
            f.core._start_or_join_reconstruction(f.pending.object_id, f.core._objects[f.pending.object_id])
            (retried,) = f.take()
            reconstructed.append(retried)
            assert retried.spec.attempt_id == f.pending.spec.attempt_id.next()
            assert retried.output_ids == f.pending.output_ids
            f.backend.succeed(retried, stored=False, value=42)
            assert f.core._finish_pending_task(retried) and f.take() == ()

        notifier = _ScopeProbe(f.core, _no_runtime, finish_and_publish)
        with bind_runtime(f.core, f.context(notifier)):
            assert f.core.get(f.ref, timeout=1.0) == 42
        assert not waits and len(reconstructed) == len(f.admissions) == 1
        assert len(f.backend.completed) == 2
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 1
        notifier.assert_unlocked((("enter", 1), ("exit", 1)))
        f.collect_completed()
    finally:
        f.close()


def test_deferred_reconstruction_rechecks_deadline_after_notifier_entry(monkeypatch, _no_runtime):
    f = _LostFixture(_no_runtime, descendant=True)
    try:
        f.observe_admission(monkeypatch)
        waits = f.observe_wait(monkeypatch)
        clock = _Clock()
        monkeypatch.setattr(core_module, "time", SimpleNamespace(monotonic=clock.monotonic))

        def exhaust_deadline():
            clock.now += 0.05  # notification entry used the original 25-ms budget

        notifier = _ScopeProbe(f.core, _no_runtime, exhaust_deadline)
        with bind_runtime(f.core, f.context(notifier)):
            with pytest.raises(TimeoutError, match="reconstruction admission"):
                f.core.get(f.ref, timeout=0.025)
        assert waits == [] and f.admissions == [(f.pending.object_id, None)]
        f.assert_deferred()
        notifier.assert_unlocked((("enter", 1), ("exit", 1)))
    finally:
        f.close()
