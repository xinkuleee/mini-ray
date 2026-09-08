"""Bounded process-hygiene helpers for synthetic CoreWorker fixtures."""

from __future__ import annotations

import threading
import time

from miniray.core import CoreWorker, _STOP


def _core_runtime_threads(core: CoreWorker) -> tuple[threading.Thread, ...]:
    candidates = [
        getattr(core, "_coordinator", None),
        *getattr(core, "_dispatchers", ()),
        getattr(core, "_reference_thread", None),
        *getattr(core, "_gc_retry_timers", ()),
    ]
    return tuple(
        dict.fromkeys(
            thread for thread in candidates
            if isinstance(thread, threading.Thread)
        )
    )


def assert_core_threads_stopped(core: CoreWorker) -> None:
    alive = tuple(
        thread.name for thread in _core_runtime_threads(core)
        if thread.is_alive()
    )
    assert alive == (), "Core fixture leaked threads: {!r}".format(alive)


def force_stop_core_threads(core: CoreWorker, timeout: float = 1.0) -> None:
    """Stop threads after a synthetic fixture rejects semantic shutdown.

    Focused reducer tests sometimes dequeue a pending task themselves.  Such a
    task can never cross the normal execution-lane retirement point, so this
    helper retires only that test-process drain count.  It deliberately leaves
    owner, recovery, lineage, and reference-authority tables unchanged.
    """

    deadline = time.monotonic() + timeout
    coordinator = getattr(core, "_coordinator", None)
    dispatchers = tuple(getattr(core, "_dispatchers", ()))
    submissions = getattr(core, "_submissions", None)
    ready = getattr(core, "_ready_tasks", None)

    with core._state_lock:
        core._accepting = False
        # Pending work removed by the test has no real dispatcher that could
        # perform _finish_pending_task's decrement.
        core._accepted_task_count = 0
        if submissions is not None:
            submissions.put(_STOP)

    core._stop_gc_retry_timers(deadline)
    core._stop_reference_events(deadline)

    current = threading.current_thread()
    if isinstance(coordinator, threading.Thread) and coordinator is not current:
        coordinator.join(max(0.0, deadline - time.monotonic()))

    # A normally exiting coordinator supplies one sentinel per dispatcher.  If
    # another synthetic state kept it alive, directly unblock only live lanes.
    live_dispatchers = tuple(
        thread for thread in dispatchers
        if isinstance(thread, threading.Thread) and thread.is_alive()
    )
    if ready is not None and live_dispatchers:
        for _thread in live_dispatchers:
            ready.put(_STOP)
    for dispatcher in live_dispatchers:
        if dispatcher is not current:
            dispatcher.join(max(0.0, deadline - time.monotonic()))
    if isinstance(coordinator, threading.Thread) and coordinator is not current:
        coordinator.join(max(0.0, deadline - time.monotonic()))

    assert_core_threads_stopped(core)


def add_core_thread_finalizer(request: object, core: CoreWorker) -> None:
    """Guarantee teardown for one narrow ``object.__new__`` Core fixture."""

    def stop() -> None:
        if any(thread.is_alive() for thread in _core_runtime_threads(core)):
            force_stop_core_threads(core)
        assert_core_threads_stopped(core)

    request.addfinalizer(stop)  # type: ignore[attr-defined]
