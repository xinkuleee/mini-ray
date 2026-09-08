"""Opt-in L1 for real dispatch-lane overlap and shutdown queue draining.

Each exact node ID constructs one Core with two dispatch lanes: four owned
daemon threads total (two lanes, coordinator, reference mailbox), no Node or
Worker processes, no sockets, no timers, and at most six tiny logical tasks.
The execution callback is a bounded in-memory probe, not user code or an RPC.

Core construction, submission and startup failure all remain inside finally
cleanup.  Gate waits and get calls have one-second bounds.  Finalizer events
share a one-second budget; normal shutdown gets one second and last-resort
structural joins get one further second.  Structural cleanup never changes
accepted counts or protocol authorities to make a failed drain look clean.
Run only one reviewed exact node ID through scripts/run_bounded_test.py.
"""

from __future__ import annotations

import queue
import socket
import threading
import time
from contextlib import contextmanager

import pytest

from miniray import core as core_module
from miniray.core import CoreWorker, ObjectRef, _STOP
from miniray.ids import NodeID
from miniray.resources import ResourceVector
from miniray.trace import MemoryEventSink


pytestmark = pytest.mark.loopback_smoke


def _runtime_threads(core):
    return tuple(dict.fromkeys(
        thread for thread in (
            *getattr(core, "_startup_thread_attempts", ()),
            getattr(core, "_coordinator", None),
            *getattr(core, "_dispatchers", ()),
            getattr(core, "_reference_thread", None),
        ) if isinstance(thread, threading.Thread)
    ))


def _close_refs_bounded(refs, deadline):
    """Use the real local finalizer, but never ObjectRef.close's bare wait."""

    for ref in refs:
        finalizer = ref._finalizer
        if finalizer is not None:
            ref._closed = True
            finalizer()
    unreleased = []
    for ref in refs:
        done = ref._release_done
        if done is not None and not done.wait(timeout=max(0.0, deadline - time.monotonic())):
            unreleased.append(ref.object_id)
    return tuple(unreleased)


def _stop_owned_threads(core, deadline):
    """Failure-only stop signals and joins; no fake semantic retirement."""

    core._accepting = False
    gate = getattr(core, "_startup_threads_gate", None)
    if gate is not None:
        gate.set()
    submissions = getattr(core, "_submissions", None)
    if submissions is not None:
        submissions.put_nowait(_STOP)
    ready = getattr(core, "_ready_tasks", None)
    if ready is not None:
        for _thread in getattr(core, "_dispatchers", ()):
            ready.put_nowait(_STOP)
    mailbox = getattr(core, "_reference_mailbox", None)
    if mailbox is not None:
        mailbox.stop()
    for timer in tuple(getattr(core, "_gc_retry_timers", ())):
        timer.cancel()
        if timer.ident is not None:
            timer.join(timeout=max(0.0, deadline - time.monotonic()))
    for thread in _runtime_threads(core):
        if thread.ident is not None:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))


@contextmanager
def _live_core(monkeypatch, execute, release):
    # Keep the partial object reachable before __init__ can start any threads.
    core = object.__new__(CoreWorker)
    refs: list[ObjectRef] = []
    errors: queue.Queue[BaseException] = queue.Queue()
    initialized = False

    def unexpected(*_args, **_kwargs):
        error = AssertionError("dispatch L1 attempted unmodelled RPC/socket/timer work")
        errors.put_nowait(error)
        raise error

    def guarded_execute(pending, prepared, dependencies=(), **options):
        try:
            return execute(core, pending, prepared, dependencies, **options)
        except BaseException as exc:
            errors.put_nowait(exc)
            # Avoid stranding a logical task after a failed test assertion in a
            # dispatcher.  The captured exception still fails the main test.
            core._publish_task_error(pending, RuntimeError("dispatch probe failed"))
            return True

    with monkeypatch.context() as patch:
        patch.setattr(socket, "socket", unexpected)
        patch.setattr(socket, "create_connection", unexpected)
        patch.setattr(core_module, "rpc_request", unexpected)
        for name in (
            "_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline",
            "_push_task_rpc", "_schedule_reference_event",
        ):
            patch.setattr(core, name, unexpected)
        patch.setattr(core, "_execute", guarded_execute)
        try:
            CoreWorker.__init__(
                core, ("node.invalid", 1), NodeID.random(),
                event_sink=MemoryEventSink(), dispatch_lanes=2,
            )
            initialized = True
            assert len(_runtime_threads(core)) == 4
            yield core, refs
        finally:
            release.set()
            unreleased = ()
            clean = False
            try:
                unreleased = _close_refs_bounded(refs, time.monotonic() + 1.0)
            finally:
                try:
                    if initialized:
                        clean = core.shutdown(timeout=1.0)
                    else:
                        clean = core._abort_unpublished_startup(timeout=1.0)
                finally:
                    _stop_owned_threads(core, time.monotonic() + 1.0)
            assert not unreleased, "reference finalizer exceeded its deadline"
            assert clean, "Core did not drain through its real shutdown"
            assert all(not thread.is_alive() for thread in _runtime_threads(core))
            assert not getattr(core, "_gc_retry_timers", ())
            if initialized:
                assert core._accepted_task_count == 0
                assert core._inflight_submissions == 0
                assert not core._protocol_unresolved
                assert core._sink_closed
            if not errors.empty():
                raise AssertionError("dispatch background probe failed") from errors.get_nowait()


def test_two_ready_tasks_execute_on_two_dispatch_lanes(monkeypatch):
    both_entered = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    active = 0
    maximum = 0

    def execute(core, pending, _prepared, _dependencies, **_options):
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
            if active == 2:
                both_entered.set()
        try:
            assert release.wait(timeout=1.0)
            assert core._publish_error(
                pending.object_id, pending.spec.attempt_id, RuntimeError("done")
            )
        finally:
            with lock:
                active -= 1
        return True

    with _live_core(monkeypatch, execute, release) as (core, refs):
        definition = core.define_remote_function(lambda: None)
        for _ in range(2):
            refs.append(core.submit(definition, (), {}, ResourceVector()))
        assert both_entered.wait(timeout=1.0)
        assert maximum == 2
        release.set()
        for ref in refs:
            with pytest.raises(RuntimeError, match="done"):
                core.get(ref, timeout=1.0)


def test_shutdown_drains_all_accepted_ready_tasks(monkeypatch):
    both_entered = threading.Event()
    release = threading.Event()
    stop_enqueued = threading.Event()
    lock = threading.Lock()
    executed: list[object] = []

    def execute(core, pending, _prepared, _dependencies, **_options):
        with lock:
            executed.append(pending.task_id)
            if len(executed) == 2:
                both_entered.set()
        assert release.wait(timeout=1.0)
        assert core._publish_error(
            pending.object_id, pending.spec.attempt_id, RuntimeError("drained")
        )
        return True

    with _live_core(monkeypatch, execute, release) as (core, refs):
        ordinary_put = core._submissions.put

        def observe_stop(item, *args, **kwargs):
            result = ordinary_put(item, *args, **kwargs)
            if item is _STOP:
                assert not core._accepting
                stop_enqueued.set()
                # This is only an observation hook after the real shutdown
                # admission fence.  Real lanes/coordinator still drain all six
                # queued tasks and perform every logical finish themselves.
                release.set()
            return result

        monkeypatch.setattr(core._submissions, "put", observe_stop)
        definition = core.define_remote_function(lambda: None)
        for _ in range(6):
            refs.append(core.submit(definition, (), {}, ResourceVector()))
        assert both_entered.wait(timeout=1.0)
        assert core._accepted_task_count == 6
        assert not release.is_set()

        assert core.shutdown(timeout=1.0)
        assert stop_enqueued.is_set()
        assert len(executed) == 6
        assert len(set(executed)) == 6
        assert all(core.owner_table.snapshot(ref.object_id).is_ready for ref in refs)
        assert core._accepted_task_count == 0
        assert all(not thread.is_alive() for thread in _runtime_threads(core))
