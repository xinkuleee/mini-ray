"""Opt-in L1 lifecycle contracts for real Core reference-retry Timers.

Per exact node ID: no Core constructor, socket, process, user task, or object
payload; at most two Timer objects and one started daemon Timer thread.  Event
waits are bounded to one second.  All starts occur inside a cleanup context
which releases blocked callbacks, explicitly cancels every constructed Timer,
and joins attempted starts against one shared one-second teardown deadline.

Run one reviewed exact node ID through the bounded runner after allowlisting.
The real Timer/teardown races are intentional L1 coverage, never default L0.
"""

from __future__ import annotations

import queue
import threading
import time
from contextlib import contextmanager

import pytest

from miniray import core as core_module
from miniray.core import CoreWorker


pytestmark = pytest.mark.loopback_smoke


class _MailboxProbe:
    def __init__(self) -> None:
        self.events: list[object] = []
        self.enqueued = threading.Event()

    def enqueue_internal(self, event: object) -> bool:
        self.events.append(event)
        self.enqueued.set()
        return True


class _BlockingMailboxProbe(_MailboxProbe):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def enqueue_internal(self, event: object) -> bool:
        self.entered.set()
        assert self.release.wait(1.0)
        return super().enqueue_internal(event)


def _narrow_core() -> CoreWorker:
    core = object.__new__(CoreWorker)
    core._state_lock = threading.RLock()
    return core


@contextmanager
def _tracked_timers(
    monkeypatch: pytest.MonkeyPatch, core: CoreWorker,
    *release_events: threading.Event,
):
    """Capture before start, including effect-then-error startup failures.

    The production Timer remains real.  The wrapper only records ownership and
    delivers callback failures back to the main test instead of losing them as
    unhandled background-thread warnings.
    """

    real_timer = threading.Timer
    created: list[threading.Timer] = []
    errors: queue.Queue[BaseException] = queue.Queue()

    def create_timer(interval, callback, args=None, kwargs=None):
        def guarded_callback(*args, **kwargs):
            try:
                callback(*args, **kwargs)
            except BaseException as exc:
                errors.put_nowait(exc)

        timer = real_timer(interval, guarded_callback, args=args, kwargs=kwargs)
        created.append(timer)
        assert len(created) <= 2, "Timer fixture exceeded its declared resource bound"
        return timer

    with monkeypatch.context() as patch:
        patch.setattr(core_module.threading, "Timer", create_timer)
        try:
            yield created
        finally:
            # Do not depend on either the Core's current timer set or the test
            # having reached its first assertion: callbacks may already have
            # retired from that set, or start may have raised after starting.
            for event in release_events:
                event.set()
            for timer in created:
                timer.cancel()
            deadline = time.monotonic() + 1.0
            for timer in created:
                if timer.ident is not None:
                    timer.join(timeout=max(0.0, deadline - time.monotonic()))
            alive = tuple(timer.name for timer in created if timer.is_alive())
            assert alive == (), "Timer cleanup exceeded its deadline: {!r}".format(alive)
            assert core._stop_gc_retry_timers(deadline)
            # Timer construction can fail before Core initializes the ledger.
            assert getattr(core, "_gc_retry_timers", set()) == set()
            assert not core._gc_retry_timers_open
            if not errors.empty():
                raise AssertionError("reference Timer callback failed") from errors.get_nowait()


def test_fired_reference_timer_removes_itself_after_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _narrow_core()
    mailbox = _MailboxProbe()
    event = object()

    with _tracked_timers(monkeypatch, core) as timers:
        core._schedule_reference_event(mailbox, event, 0.0)
        # The creation ledger is stable even if an immediate callback already
        # removed itself from the Core's active-timer set.
        (timer,) = timers
        assert mailbox.enqueued.wait(1.0)
        timer.join(timeout=1.0)
        assert mailbox.events == [event]
        assert not timer.is_alive()
        assert core._gc_retry_timers == set()


def test_timer_teardown_fences_cancelled_and_future_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _narrow_core()
    mailbox = _MailboxProbe()

    with _tracked_timers(monkeypatch, core) as timers:
        core._schedule_reference_event(mailbox, "cancelled", 10.0)
        (timer,) = timers
        assert core._stop_gc_retry_timers(time.monotonic() + 1.0)

        assert core._gc_retry_timers == set()
        assert not timer.is_alive()
        assert mailbox.events == []

        core._schedule_reference_event(mailbox, "after-fence", 0.0)
        assert len(timers) == 2
        assert timers[1].ident is None and not timers[1].is_alive()
        assert core._gc_retry_timers == set()
        # The synchronous schedule call returned without starting the Timer.
        # No negative wall-clock wait is needed to establish the fence.
        assert not mailbox.enqueued.is_set()
        assert mailbox.events == []


def test_timed_out_teardown_retains_running_callback_for_next_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _narrow_core()
    mailbox = _BlockingMailboxProbe()

    with _tracked_timers(monkeypatch, core, mailbox.release) as timers:
        core._schedule_reference_event(mailbox, "running", 0.0)
        assert mailbox.entered.wait(1.0)
        (timer,) = timers

        assert not core._stop_gc_retry_timers(time.monotonic() + 0.01)
        assert timer.is_alive()
        assert core._gc_retry_timers == {timer}

        mailbox.release.set()
        timer.join(timeout=1.0)
        assert not timer.is_alive()
        assert core._stop_gc_retry_timers(time.monotonic() + 1.0)
        assert core._gc_retry_timers == set()
        assert mailbox.events == ["running"]
