"""Managed-process monitoring contracts with explicit safety modes.

Unit cases validate before acquiring infrastructure or use fake pipes and
never start a real thread. The two original live-monitor cases use real
threads and pipes and remain heavy pending separate bounded-runtime review.
"""

from __future__ import annotations

import queue
import threading
import os

import pytest

from miniray.node_monitor import ManagedNodeMonitor


class _Process:
    def __init__(self, pid: int, sentinel: object) -> None:
        self.pid = pid
        self.sentinel = sentinel
        self.join_calls: list[int] = []

    def join(self, timeout: int) -> None:
        self.join_calls.append(timeout)


class _Waiter:
    def __init__(self) -> None:
        self.ready: queue.Queue[object] = queue.Queue()

    def __call__(self, readers: object) -> list[object]:
        values = tuple(readers)
        while True:
            candidate = self.ready.get(timeout=1)
            if candidate in values:
                return [candidate]


class _Connection:
    def __init__(self, *, close_error: Exception | None = None) -> None:
        self.close_error = close_error
        self.close_calls = 0
        self.send_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error

    def send_bytes(self, _payload: bytes) -> None:
        self.send_calls += 1


class _Context:
    def __init__(self, reader: object, writer: object) -> None:
        self.reader = reader
        self.writer = writer
        self.pipe_calls = 0

    def Pipe(self, *, duplex: bool):
        assert not duplex
        self.pipe_calls += 1
        return self.reader, self.writer


@pytest.mark.heavy
def test_reports_each_exact_process_once_and_joins_nonblocking() -> None:
    first = _Process(101, object())
    second = _Process(102, object())
    waiter = _Waiter()
    deaths: list[object] = []
    observed = threading.Event()

    def on_death(process: object) -> None:
        deaths.append(process)
        if len(deaths) == 2:
            observed.set()

    monitor = ManagedNodeMonitor(
        (first, second), on_death, wait_function=waiter
    )
    monitor.start()
    waiter.ready.put(second.sentinel)
    waiter.ready.put(first.sentinel)

    assert observed.wait(1)
    assert monitor.stop()
    assert deaths == [second, first]
    assert first.join_calls == [0]
    assert second.join_calls == [0]


@pytest.mark.heavy
def test_wakeup_stops_without_reporting_live_process() -> None:
    read_fd, write_fd = os.pipe()
    process = _Process(101, read_fd)
    try:
        monitor = ManagedNodeMonitor(
            (process,), lambda value: pytest.fail(str(value))
        )
        monitor.start()

        assert monitor.stop()
        assert not monitor.is_alive
        assert process.join_calls == []
    finally:
        os.close(read_fd)
        os.close(write_fd)


@pytest.mark.unit
def test_rejects_ambiguous_managed_identity() -> None:
    process = _Process(101, object())
    with pytest.raises(ValueError, match="objects must be unique"):
        ManagedNodeMonitor((process, process), lambda value: None)
    with pytest.raises(ValueError, match="sentinels must be unique"):
        ManagedNodeMonitor(
            (_Process(101, process.sentinel), _Process(102, process.sentinel)),
            lambda value: None,
        )


@pytest.mark.unit
def test_thread_start_failure_closes_pipe_without_join_and_cannot_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _Connection()
    writer = _Connection()

    class _Thread:
        join_calls = 0

        def __init__(self, **_kwargs: object) -> None:
            return None

        def start(self) -> None:
            raise RuntimeError("injected monitor start failure")

        def join(self, _timeout: float) -> None:
            self.join_calls += 1

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr("miniray.node_monitor.threading.Thread", _Thread)
    monitor = ManagedNodeMonitor(
        (_Process(101, object()),), lambda _process: None,
        mp_context=_Context(reader, writer),  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="monitor start failure"):
        monitor.start()

    assert monitor.stop()
    assert monitor._thread.join_calls == 0
    assert (reader.close_calls, writer.close_calls) == (1, 1)
    with pytest.raises(RuntimeError, match="stopped.*cannot be started"):
        monitor.start()


@pytest.mark.unit
def test_stop_before_start_is_idempotent_and_fences_later_start() -> None:
    reader = _Connection()
    writer = _Connection()
    monitor = ManagedNodeMonitor(
        (_Process(101, object()),), lambda _process: None,
        mp_context=_Context(reader, writer),  # type: ignore[arg-type]
    )

    assert monitor.stop()
    assert monitor.stop()
    assert (reader.close_calls, writer.close_calls) == (1, 1)
    with pytest.raises(RuntimeError, match="stopped.*cannot be started"):
        monitor.start()


@pytest.mark.unit
def test_pipe_cleanup_attempts_both_ends_when_one_close_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _Connection(close_error=RuntimeError("close failed"))
    writer = _Connection()

    monkeypatch.setattr(
        "miniray.node_monitor.threading.Thread",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("thread constructor failed")
        ),
    )
    context = _Context(reader, writer)

    with pytest.raises(RuntimeError, match="thread constructor failed"):
        ManagedNodeMonitor(
            (_Process(101, object()),), lambda _process: None,
            mp_context=context,  # type: ignore[arg-type]
        )

    # Thread construction deliberately precedes Pipe acquisition, so there is
    # no resource to release at this cut.
    assert context.pipe_calls == 0
    assert (reader.close_calls, writer.close_calls) == (0, 0)
