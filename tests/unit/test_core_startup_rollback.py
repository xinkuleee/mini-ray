"""Unpublished Core startup contracts with explicit runtime classification.

Only early validation and the fully fake API adapter are pure. The three L1
cases retain real constructors and real startup threads: at most four thread
objects, three started threads, zero Tasks, objects, processes or sockets.
Production abort joins share its one-second deadline; failure-finally signals
only the captured Core and joins its exact threads under a two-second deadline.
The bounded runner supplies the hard process limit, since internal startup
waits and lock acquisition are not cancelled by these join deadlines.

The successful-constructor case isolates only the coordinator's periodic
Worker-death polling, recording its scheduling and deferring the next poll.
GCS remains configured, and every sync/RPC is a pre-constructor tripwire whose
record is asserted on the main thread. Abort itself must never initiate one.
"""

from __future__ import annotations

import math
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module
from miniray.core import CoreWorker, _STOP
from miniray.ids import NodeID
from miniray.trace import EventSink


class _Sink(EventSink):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def _core_threads() -> tuple[threading.Thread, ...]:
    return tuple(
        thread for thread in threading.enumerate()
        if thread.name.startswith("miniray-core-")
    )


@pytest.mark.unit
def test_early_constructor_failure_closes_transferred_trace_sink() -> None:
    sink = _Sink()

    with pytest.raises(ValueError, match="inline_threshold"):
        CoreWorker(
            ("127.0.0.1", 19001), NodeID.random(),
            inline_threshold=-1, event_sink=sink,
        )

    assert sink.close_calls == 1


class _StartupProbe:
    """Observe real threads; cleanup never supplies a passing abort result."""

    def __init__(self, monkeypatch, *, fault=None):
        assert fault in (None, "before-second", "after-first")
        self.core = object.__new__(CoreWorker)  # retain even a partial constructor
        self.sink = _Sink()
        self.baseline = set(_core_threads())
        self.fault = fault
        self.created, self.attempts, self.started, self.joins = [], [], [], []
        self.violations, self.thread_errors, self.background_polls = [], [], []
        self.observation_lock = threading.Lock()
        self.coordinator_entered = threading.Event()
        self.cleaning = False
        self.injected = (None if fault is None else RuntimeError(
            "injected second lane start failure" if fault == "before-second"
            else "injected post-start failure",
        ))
        self.injections = 0
        real_thread = core_module.threading.Thread
        probe = self

        class _ObservedThread(real_thread):
            def __init__(thread, *args, **kwargs):
                super().__init__(*args, **kwargs)
                allowed = {
                    "miniray-core-reference-events", "miniray-core-worker-coordinator",
                    "miniray-core-worker-dispatch-0",
                }
                if probe.fault == "before-second":
                    allowed.add("miniray-core-worker-dispatch-1")
                with probe.observation_lock:
                    probe.created.append(thread)
                    valid = (thread.name in allowed
                             and len(probe.created) <= len(allowed)
                             and sum(item.name == thread.name for item in probe.created) == 1)
                if not valid:
                    probe.forbidden("unexpected startup thread construction")

            def start(thread):
                with probe.observation_lock:
                    probe.attempts.append(thread)
                    valid = thread in probe.created and probe.attempts.count(thread) == 1
                if not valid:
                    probe.forbidden("unexpected repeated startup thread start")
                if probe.fault == "before-second" and thread.name == "miniray-core-worker-dispatch-1":
                    probe.injections += 1
                    raise probe.injected
                super().start()
                with probe.observation_lock:
                    probe.started.append(thread)
                if probe.fault == "after-first" and thread.name == "miniray-core-worker-dispatch-0":
                    probe.injections += 1
                    raise probe.injected

            def join(thread, timeout=None):
                limit = 2.0 if probe.cleaning else 1.0
                if (type(timeout) not in (int, float) or not math.isfinite(timeout)
                        or not 0 <= timeout <= limit or thread not in probe.created):
                    probe.forbidden("startup join was unbounded or not owned")
                with probe.observation_lock:
                    probe.joins.append((thread, timeout, probe.cleaning))
                    valid = len(probe.joins) <= 8
                if not valid:
                    probe.forbidden("startup cleanup exceeded its fixed join count")
                return super().join(timeout)

            def run(thread):
                try:
                    return super().run()
                except BaseException as exc:
                    # A daemon exception must fail the main test, not merely
                    # disappear into best-effort runtime/error-warning code.
                    with probe.observation_lock:
                        probe.thread_errors.append((thread, exc))

        monkeypatch.setattr(core_module.threading, "Thread", _ObservedThread)
        for name in (
            "_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline",
            "_push_task_rpc", "_actor_call_rpc", "_sync_worker_deaths",
            "_sync_node_deaths", "_register_submission", "_execute",
            "_schedule_reference_event", "put", "shutdown",
            "stop_after_cluster_exit",
        ):
            monkeypatch.setattr(self.core, name, self.forbidden)
        monkeypatch.setattr(core_module, "rpc_request", self.forbidden)
        for name in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, name, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", self.forbidden)
        monkeypatch.setattr(threading.Timer, "start", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        if fault is None:
            monkeypatch.setattr(
                self.core, "_poll_worker_deaths_best_effort", self.defer_background_poll,
            )

    def forbidden(self, *args, **kwargs):
        error = AssertionError("startup L1 attempted forbidden runtime work")
        with self.observation_lock:
            self.violations.append((threading.current_thread(), args, kwargs))
        raise error

    def defer_background_poll(self):
        """Isolate background scheduling only, never fake a GCS sync result."""
        coordinator = getattr(self.core, "_coordinator", None)
        caller = threading.current_thread()
        if caller is not coordinator:
            self.forbidden("periodic poll invoked outside the coordinator")
        with self.observation_lock:
            self.background_polls.append(caller)
            valid = len(self.background_polls) <= 4
        if not valid:
            self.forbidden("unexpected coordinator polling loop")
        with self.core._state_lock:
            # A bare no-op leaves an expired timeout in the real loop and can
            # busy-spin. Defer that one scheduling field without clearing GCS
            # or replacing _sync_worker_deaths with an always-successful stub.
            self.core._worker_death_next_poll_at = time.monotonic() + 5.0
        self.coordinator_entered.set()

    def construct(self, *, gcs=False):
        CoreWorker.__init__(
            self.core, ("127.0.0.1", 19001), NodeID.random(),
            gcs_address=("127.0.0.1", 19002) if gcs else None,
            dispatch_lanes=2 if self.fault == "before-second" else 1,
            event_sink=self.sink,
        )

    def assert_aborted(self):
        core = self.core
        reference, coordinator = core._reference_thread, core._coordinator
        first = core._dispatchers[0]
        expected_created = [reference, coordinator, *core._dispatchers]
        expected_attempts = ([reference, first, core._dispatchers[1]]
                             if self.fault == "before-second" else
                             [reference, first] if self.fault == "after-first"
                             else [reference, first, coordinator])
        expected_started = ([reference, first] if self.fault is not None
                            else [reference, first, coordinator])
        assert self.created == expected_created and self.attempts == expected_attempts
        assert self.started == expected_started
        assert core._startup_thread_attempts == expected_attempts[1:]
        assert all(thread.ident is not None for thread in expected_started)
        assert all(thread.ident is None for thread in self.created if thread not in expected_started)
        expected_joined = [first, reference] if self.fault is not None else [first, coordinator, reference]
        assert [thread for thread, _timeout, cleanup in self.joins if not cleanup] == expected_joined
        assert all(not thread.is_alive() for thread in self.created)
        assert core._startup_abort_complete and not core._startup_threads_committed
        assert core._startup_threads_gate.is_set()
        assert not core._accepting and not core._owner_retain_admission_open
        assert not core._gc_retry_timers_open and not core._gc_retry_timers
        assert core._reference_mailbox.stop_enqueued and core._reference_mailbox.stopped.is_set()
        assert not core._reference_mailbox.accepting
        assert not core._reference_runtime_finalizer.alive
        assert core._accepted_task_count == core._inflight_submissions == 0
        assert not core._objects and not core._registered_functions and not core._protocol_unresolved
        assert self.sink.close_calls == 1 and core._sink_closed
        assert set(_core_threads()) == self.baseline
        assert not self.violations and not self.thread_errors

    def cleanup(self):
        """Failure-only signals/joins, after all normal-path assertions."""
        self.cleaning = True
        core = self.core
        if any(thread.is_alive() for thread in self.created):
            deadline = time.monotonic() + 2.0
            core._startup_threads_committed = False
            gate = getattr(core, "_startup_threads_gate", None)
            if gate is not None:
                gate.set()
            core._accepting = False
            submissions, ready = getattr(core, "_submissions", None), getattr(core, "_ready_tasks", None)
            if submissions is not None:
                submissions.put_nowait(_STOP)
            if ready is not None:
                for _thread in getattr(core, "_dispatchers", ()):
                    ready.put_nowait(_STOP)
            mailbox = getattr(core, "_reference_mailbox", None)
            if mailbox is not None:
                mailbox.stop()
            for thread in self.created:
                if thread.ident is not None:
                    thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in self.created)
        finalizer = getattr(core, "_reference_runtime_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
        if not getattr(core, "_sink_closed", False):
            core._sink_closed = True
            self.sink.close()
        # Never set _startup_abort_complete or rewrite task/owner/recovery
        # authorities here. A cleanup success cannot repair a failed assertion.
        assert set(_core_threads()) == self.baseline
        assert not self.violations and not self.thread_errors


@pytest.mark.loopback_smoke
def test_lane_start_failure_stops_all_started_local_threads_without_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _StartupProbe(monkeypatch, fault="before-second")
    try:
        with pytest.raises(RuntimeError, match="second lane start failure") as caught:
            probe.construct(gcs=True)
        assert caught.value is probe.injected and probe.injections == 1
        probe.assert_aborted()
        assert not probe.background_polls
    finally:
        probe.cleanup()


@pytest.mark.loopback_smoke
def test_lane_start_raise_after_start_is_still_joined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _StartupProbe(monkeypatch, fault="after-first")
    try:
        with pytest.raises(RuntimeError, match="post-start failure") as caught:
            probe.construct()
        assert caught.value is probe.injected and probe.injections == 1
        probe.assert_aborted()
        assert not probe.background_polls
    finally:
        probe.cleanup()


@pytest.mark.loopback_smoke
def test_unpublished_abort_is_idempotent_and_never_syncs_gcs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _StartupProbe(monkeypatch)
    core = probe.core
    try:
        probe.construct(gcs=True)
        assert probe.coordinator_entered.wait(timeout=1.0)
        assert core._startup_threads_committed and core._startup_threads_gate.is_set()
        assert len(probe.created) == len(probe.started) == 3
        assert all(thread.is_alive() for thread in probe.created)
        assert core.gcs_address == ("127.0.0.1", 19002)
        assert not probe.joins and probe.sink.close_calls == 0
        assert core._abort_unpublished_startup(timeout=1.0)
        probe.assert_aborted()
        first_joins = tuple(probe.joins)
        first_polls = tuple(probe.background_polls)
        assert first_polls and all(thread is core._coordinator for thread in first_polls)
        assert core._abort_unpublished_startup(timeout=1.0)
        assert tuple(probe.joins) == first_joins
        assert tuple(probe.background_polls) == first_polls
        probe.assert_aborted()
        assert not core.dispatcher_alive and not core._reference_thread.is_alive()
    finally:
        probe.cleanup()


@pytest.mark.unit
def test_api_startup_rollback_uses_local_core_abort_not_semantic_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from miniray import api

    core = object.__new__(CoreWorker)
    calls: list[tuple[str, float]] = []
    core._abort_unpublished_startup = (
        lambda timeout: calls.append(("abort", timeout)) or True
    )
    core.shutdown = lambda *_args, **_kwargs: pytest.fail(
        "startup rollback must not call semantic shutdown"
    )

    monkeypatch.setattr(
        api, "_attempt_startup_cleanup", lambda operation: operation()
    )
    api._attempt_startup_cleanup(
        lambda: core._abort_unpublished_startup(api._STOP_TIMEOUT_SECONDS)
    )

    assert calls == [("abort", api._STOP_TIMEOUT_SECONDS)]
