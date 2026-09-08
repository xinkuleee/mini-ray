"""Pure Core contracts for Worker-side ``get`` blocking notifications.

These tests deliberately inject a tiny re-entrant notifier instead of a
NodeManager.  CoreWorker owns the readiness decision, so the useful contract
at this layer is whether it enters a blocking scope at all.  Typed RPC identity,
episode fencing, and CPU accounting are covered by the protocol/Node tests.
The threadless Core registers at most two Tasks without dispatch admission;
each successful single-slot result is at most 1 KiB and uses real
discovery, Node journal/adapter Complete and Core adoption/finish/GC reducers.
The supplied values are fixture outcomes, not executed user task functions.
Original ready ``put`` objects remain puts. Pending-only cases receive an
explicit fixture terminal error only after their notification assertions.
Waits are inert semantic callbacks or an already-set check; no original wait,
runtime constructor, thread, process, socket, timer or public shutdown runs.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import pytest

from miniray import control, core as core_module, node as node_module, protocol, transport, worker as worker_module
from miniray.core import CoreWorker, ObjectRef, _ObjectWaiter, _PendingTask, _WAKE_COORDINATOR
from miniray.ids import AttemptID, LeaseID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState, UnknownTaskError
from miniray.resources import ResourceVector
from miniray.runtime_binding import ExecutionContext, bind_runtime
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


pytestmark = pytest.mark.unit
_LIMIT = 32


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations = []
    receipt_checks = 0

    def forbidden(*_args, **_kwargs):
        if len(violations) < _LIMIT:
            violations.append(True)
        pytest.fail("pure blocking-get test attempted runtime or unbounded work")

    def already_set(event, timeout=None):
        nonlocal receipt_checks
        del timeout
        receipt_checks += 1
        if receipt_checks > _LIMIT or not event.is_set():
            forbidden()
        # The synchronous reference mailbox sets its release receipt before
        # close returns. Nested on-enter publication also sets its get event.
        # This is an inert state check, never the original Event.wait.
        return True

    for kind, name in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (CoreWorker, "shutdown"), (NodeServer, "__init__"),
        (WorkerServer, "__init__"), (control.GCSLite, "__init__"),
        (transport.TCPServer, "__init__"), (threading.Timer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "__init__"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, control, worker_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield forbidden
    # A best-effort production callback must not hide the tripwire failure.
    assert not violations


class _RecordingNotifier:
    """A semantic test double for the ExecutionContext notifier.

    Every ``blocking_scope`` invocation is visible through ``scope_calls``.
    Only depth transitions 0 -> 1 and 1 -> 0 create externally observable
    Blocked/Unblocked events.  This distinction catches an implementation that
    makes ``get_many`` yield and reacquire once per element.
    """

    def __init__(
        self,
        on_enter: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self.on_enter = on_enter
        self.depth = 0
        self.scope_calls = 0
        self.next_sequence = 0
        self.events: list[tuple[str, int]] = []

    def group_scope(self) -> "_RecordingGroupScope":
        return _RecordingGroupScope(self)

    @contextmanager
    def blocking_scope(self) -> Iterator[None]:
        assert self.scope_calls < _LIMIT
        self.scope_calls += 1
        call_number = self.scope_calls
        outermost = self.depth == 0
        if outermost:
            sequence = self.next_sequence
            self.next_sequence += 1
            self.events.append(("blocked", sequence))
        else:
            sequence = self.next_sequence - 1
        self.depth += 1
        try:
            if self.on_enter is not None:
                self.on_enter(call_number, self.depth)
            yield
        finally:
            self.depth -= 1
            if outermost:
                self.events.append(("unblocked", sequence))


class _RecordingGroupScope:
    """Match the production notifier's lazy aggregate-wait contract."""

    def __init__(self, notifier: _RecordingNotifier) -> None:
        self.notifier = notifier
        self.scope: object | None = None

    def begin_blocking(self) -> None:
        if self.scope is None:
            self.scope = self.notifier.blocking_scope()
            self.scope.__enter__()

    def close(self, exc_type=None, exc=None, tb=None) -> None:
        scope, self.scope = self.scope, None
        if scope is not None:
            scope.__exit__(exc_type, exc, tb)

    def __enter__(self) -> "_RecordingGroupScope":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(exc_type, exc, tb)


class _ImmediateWait:
    """Event-shaped wait point with no wall-clock delay."""

    def __init__(self, error: Optional[BaseException] = None) -> None:
        self.error = error
        self.wait_calls = 0
        self.was_set = False

    def wait(self, timeout: Optional[float] = None) -> bool:
        del timeout
        assert self.wait_calls == 0
        self.wait_calls += 1
        if self.error is not None:
            raise self.error
        return False

    def set(self) -> None:
        self.was_set = True


class _PublishingWait:
    """Publish one result exactly when Core reaches its actual wait."""

    def __init__(self, publish: Callable[[], None]) -> None:
        self.publish = publish
        self.wait_calls = 0
        self.was_set = False

    def wait(self, timeout: Optional[float] = None) -> bool:
        del timeout
        assert self.wait_calls == 0
        self.wait_calls += 1
        self.publish()
        return True

    def set(self) -> None:
        self.was_set = True


def _core(forbidden) -> tuple[CoreWorker, PureOutputRuntime]:
    core = make_pure_core()
    outputs = PureOutputRuntime(core)

    def rpc(address, handler, request):
        if len(outputs.calls) >= _LIMIT:
            forbidden()
        return outputs.rpc(address, handler, request)

    core.gcs_address, core._rpc = outputs.gcs_address, rpc
    return core, outputs


def _context(
    core: CoreWorker, notifier: _RecordingNotifier
) -> ExecutionContext:
    parent_task_id = TaskID.derive(
        core.job_id, TaskID.for_driver(core.job_id), 91
    )
    return ExecutionContext(
        job_id=core.job_id,
        parent_task_id=parent_task_id,
        parent_attempt_id=AttemptID(parent_task_id, 0),
        blocking_notifier=notifier,
    )


def _pending(core: CoreWorker) -> tuple[_PendingTask, ObjectRef]:
    assert core._submission_index < 2
    definition = core.define_remote_function(lambda: None)
    return core._register_submission(
        definition, (), {}, ResourceVector({"CPU": 1})
    )


def _publish(core: CoreWorker, outputs: PureOutputRuntime, pending: _PendingTask, value: object) -> None:
    assert outputs.discoveries < 2 and len(pending.output_ids) == 1
    assert core._dependencies_ready(pending)
    prepared, dependencies, protected = core._prepare_task_dependencies(pending.spec)
    assert prepared == pending.spec and dependencies == protected == ()
    push = protocol.PushTask(LeaseID.random(), WorkerID.random(), prepared, dependencies)
    reply = outputs.complete(push, (value,))
    assert reply.output_publication is not None
    assert reply.output_publication.manifest.execution == pending.execution
    assert core._publish_reply(
        pending, reply, expected_node_id=core.node_id, expected_lease_id=push.lease_id,
    )
    assert core.owner_table.snapshot(pending.object_id).output_publication is not None
    assert not outputs.journal.snapshot(reply.output_publication.publication_id).retained_result_slots
    assert core._finish_pending_task(pending)
    record = core._recovery.task_record(pending.task_id)
    assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
    assert record.current_attempt == pending.spec.attempt_id


def _terminate_pending_fixture(core: CoreWorker, pending: _PendingTask) -> None:
    # Test cleanup after the semantic assertion: this is not a Worker outcome,
    # task cancellation or an inference from the get timeout/wait exception.
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.PENDING and snapshot.output_publication is None
    cleanup = RuntimeError("explicit pending-only pure fixture cleanup")
    assert core._publish_task_error(pending, cleanup)
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.ERROR and snapshot.error is cleanup
    record = core._recovery.task_record(pending.task_id)
    assert record.state is TaskState.SYSTEM_FAILED and record.retries_started == 0
    assert core._finish_pending_task(pending)


def _collect(core, outputs, refs, pending_tasks, *, publications):
    assert outputs.discoveries == publications <= 2
    assert core._accepted_task_count == 0 and not core._task_finish_barriers
    assert not core._protocol_unresolved
    assert all(pending.task_key in core._finished_tasks for pending in pending_tasks)
    for ref in refs:
        ref.close(timeout=0)
    assert core._reference_mailbox.pending.qsize() <= _LIMIT
    core._reference_mailbox.drain()
    queued = core._submissions.qsize()
    assert queued <= _LIMIT
    for _ in range(queued):
        item = core._submissions.get_nowait()
        try:
            assert item is _WAKE_COORDINATOR
        finally:
            core._submissions.task_done()
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    assert core._reference_mailbox.pending.empty()
    assert core._reference_mailbox.pending.unfinished_tasks == 0
    assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
    for ref in refs:
        assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(ref.object_id) is None
    for pending in pending_tasks:
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(pending.task_id)
    assert len(outputs.calls) == 4 * publications  # three adoption ACKs + one slot GC
    outputs.assert_collected()


def _fence(core, refs):
    # Failure cleanup only releases actual handles. It neither fabricates a
    # terminal outcome nor erases an unfinished authority/GC obligation.
    try:
        for ref in refs:
            ref.close(timeout=0)
    finally:
        close_pure_core(core)


def test_ready_get_and_zero_timeout_pending_get_do_not_notify(_no_runtime) -> None:
    core, outputs = _core(_no_runtime)
    notifier = _RecordingNotifier()
    ready_ref = core.put("ready")
    pending_task, pending_ref = _pending(core)
    try:
        with bind_runtime(core, _context(core, notifier)):
            assert core.get(ready_ref, timeout=0) == "ready"
            with pytest.raises(TimeoutError, match="not ready"):
                core.get(pending_ref, timeout=0)

        assert notifier.scope_calls == 0
        assert notifier.events == []
        _terminate_pending_fixture(core, pending_task)
        _collect(core, outputs, (ready_ref, pending_ref), (pending_task,), publications=0)
    finally:
        _fence(core, (ready_ref, pending_ref))


def test_all_ready_get_many_and_ready_error_do_not_notify(_no_runtime) -> None:
    core, outputs = _core(_no_runtime)
    notifier = _RecordingNotifier()
    first = core.put("first")
    second = core.put("second")
    failed_pending, failed = _pending(core)
    failure = RuntimeError("already failed")
    assert core._publish_error(
        failed_pending.object_id, failed_pending.spec.attempt_id, failure
    )
    try:
        with bind_runtime(core, _context(core, notifier)):
            assert core.get_many((first, second), timeout=0) == [
                "first", "second"
            ]
            with pytest.raises(RuntimeError, match="already failed"):
                core.get(failed, timeout=0)

        assert notifier.scope_calls == 0
        assert notifier.events == []
        assert core._recovery.task_record(failed_pending.task_id).state is TaskState.SYSTEM_FAILED
        assert core._finish_pending_task(failed_pending)
        _collect(core, outputs, (first, second, failed), (failed_pending,), publications=0)
    finally:
        _fence(core, (first, second, failed))


@pytest.mark.parametrize(
    ("wait_error", "expected_error", "message"),
    [
        (None, TimeoutError, "not ready"),
        (RuntimeError("wait failed"), RuntimeError, "wait failed"),
    ],
)
def test_wait_timeout_or_error_always_closes_the_blocking_episode(
    wait_error: Optional[BaseException],
    expected_error: type[BaseException],
    message: str,
    _no_runtime,
) -> None:
    core, outputs = _core(_no_runtime)
    pending, ref = _pending(core)
    waiter = _ImmediateWait(wait_error)
    core._objects[pending.object_id] = _ObjectWaiter(waiter)
    notifier = _RecordingNotifier()
    try:
        with bind_runtime(core, _context(core, notifier)):
            with pytest.raises(expected_error, match=message):
                core.get(ref, timeout=1.0)

        assert waiter.wait_calls == 1
        assert notifier.scope_calls == 1
        assert notifier.depth == 0
        assert notifier.events == [("blocked", 0), ("unblocked", 0)]
        _terminate_pending_fixture(core, pending)
        _collect(core, outputs, (ref,), (pending,), publications=0)
    finally:
        _fence(core, (ref,))


def test_get_inside_an_existing_blocking_scope_is_one_episode(_no_runtime) -> None:
    core, outputs = _core(_no_runtime)
    pending, ref = _pending(core)
    notifier: _RecordingNotifier

    def publish_on_nested_enter(call_number: int, depth: int) -> None:
        if call_number == 2:
            assert depth == 2
            _publish(core, outputs, pending, 42)

    notifier = _RecordingNotifier(publish_on_nested_enter)
    try:
        with bind_runtime(core, _context(core, notifier)):
            with notifier.blocking_scope():
                assert core.get(ref, timeout=1.0) == 42

        assert notifier.scope_calls == 2
        assert notifier.depth == 0
        assert notifier.events == [("blocked", 0), ("unblocked", 0)]
        _collect(core, outputs, (ref,), (pending,), publications=1)
    finally:
        _fence(core, (ref,))


def test_get_many_uses_one_outer_episode_for_multiple_actual_waits(_no_runtime) -> None:
    core, outputs = _core(_no_runtime)
    first_pending, first_ref = _pending(core)
    second_pending, second_ref = _pending(core)
    first_published = False

    def publish_first_on_outer_enter(_call_number: int, depth: int) -> None:
        nonlocal first_published
        if not first_published:
            assert depth == 1
            first_published = True
            _publish(core, outputs, first_pending, "first")

    notifier = _RecordingNotifier(publish_first_on_outer_enter)
    second_waiter = _PublishingWait(
        lambda: _publish(core, outputs, second_pending, "second")
    )
    core._objects[second_pending.object_id] = _ObjectWaiter(second_waiter)
    try:
        with bind_runtime(core, _context(core, notifier)):
            assert core.get_many(
                (first_ref, second_ref), timeout=1.0
            ) == ["first", "second"]

        assert first_published
        assert second_waiter.wait_calls == 1
        assert notifier.scope_calls == 3
        assert notifier.depth == 0
        assert notifier.events == [("blocked", 0), ("unblocked", 0)]
        _collect(core, outputs, (first_ref, second_ref),
                 (first_pending, second_pending), publications=2)
    finally:
        _fence(core, (first_ref, second_ref))


def test_get_many_error_closes_its_single_outer_episode(_no_runtime) -> None:
    core, outputs = _core(_no_runtime)
    first_pending, first_ref = _pending(core)
    second_pending, second_ref = _pending(core)
    first_waiter = _PublishingWait(
        lambda: _publish(core, outputs, first_pending, "first")
    )
    second_waiter = _ImmediateWait(RuntimeError("second wait failed"))
    core._objects[first_pending.object_id] = _ObjectWaiter(first_waiter)
    core._objects[second_pending.object_id] = _ObjectWaiter(second_waiter)
    notifier = _RecordingNotifier()
    try:
        with bind_runtime(core, _context(core, notifier)):
            with pytest.raises(RuntimeError, match="second wait failed"):
                core.get_many((first_ref, second_ref), timeout=1.0)

        assert first_waiter.wait_calls == 1
        assert second_waiter.wait_calls == 1
        assert notifier.scope_calls == 3
        assert notifier.depth == 0
        assert notifier.events == [("blocked", 0), ("unblocked", 0)]
        _terminate_pending_fixture(core, second_pending)
        _collect(core, outputs, (first_ref, second_ref),
                 (first_pending, second_pending), publications=1)
    finally:
        _fence(core, (first_ref, second_ref))
