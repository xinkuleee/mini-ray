"""Exact managed-Node process death observation for the Driver.

The monitor deliberately knows nothing about GCS, snapshots, or recovery.  It
waits only on multiprocessing process sentinels plus a private wakeup pipe and
hands an exact managed process object to the API composition layer.
"""

from __future__ import annotations

import multiprocessing as mp
from multiprocessing.connection import Connection, wait as connection_wait
import threading
from typing import Callable, Iterable, Optional


NodeDeathCallback = Callable[[object], None]
WaitFunction = Callable[[Iterable[object]], list[object]]


class ManagedNodeMonitor:
    """Observe each configured Process sentinel exactly once."""

    def __init__(
        self,
        processes: Iterable[object],
        on_death: NodeDeathCallback,
        *,
        wait_function: WaitFunction = connection_wait,
        mp_context: Optional[mp.context.BaseContext] = None,
    ) -> None:
        process_tuple = tuple(processes)
        if not process_tuple:
            raise ValueError("ManagedNodeMonitor requires at least one process")
        if len({id(process) for process in process_tuple}) != len(process_tuple):
            raise ValueError("managed process objects must be unique")
        sentinels: dict[object, object] = {}
        for process in process_tuple:
            sentinel = getattr(process, "sentinel", None)
            pid = getattr(process, "pid", None)
            if sentinel is None:
                raise TypeError("managed process must expose a sentinel")
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                raise ValueError("managed process must have a positive PID")
            if sentinel in sentinels:
                raise ValueError("managed process sentinels must be unique")
            sentinels[sentinel] = process
        if not callable(on_death):
            raise TypeError("on_death must be callable")
        self._processes = process_tuple
        self._sentinels = sentinels
        self._on_death = on_death
        self._wait = wait_function
        self._lock = threading.Lock()
        self._stopping = False
        self._started = False
        self._closed = False
        self._reported: set[object] = set()
        self._thread = threading.Thread(
            target=self._run, name="miniray-managed-node-monitor", daemon=True
        )
        context = mp_context or mp.get_context("spawn")
        wake_reader = None
        wake_writer = None
        try:
            wake_reader, wake_writer = context.Pipe(duplex=False)
            self._wake_reader: Connection = wake_reader
            self._wake_writer: Connection = wake_writer
        except BaseException:
            for connection in (wake_reader, wake_writer):
                if connection is None:
                    continue
                try:
                    connection.close()
                except Exception:
                    pass
            raise

    def start(self) -> None:
        with self._lock:
            if self._started:
                raise RuntimeError("ManagedNodeMonitor already started")
            if self._stopping or self._closed:
                raise RuntimeError("a stopped ManagedNodeMonitor cannot be started")
            try:
                self._thread.start()
            except BaseException:
                # A failed Thread.start has uncertain implementation-level
                # side effects and is never retried.  There is no confirmed
                # running waiter to join, so close the private Pipe directly.
                self._stopping = True
                self._close_pipes_locked()
                raise
            self._started = True

    def stop(self, timeout: float = 5.0) -> bool:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        with self._lock:
            self._stopping = True
            started = self._started
        if not started:
            self._close_pipes()
            return True
        try:
            self._wake_writer.send_bytes(b"x")
        except (BrokenPipeError, EOFError, OSError):
            pass
        self._thread.join(timeout)
        stopped = not self._thread.is_alive()
        if stopped:
            self._close_pipes()
        return stopped

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        active = dict(self._sentinels)
        while active:
            ready = self._wait((*active, self._wake_reader))
            if self._wake_reader in ready:
                try:
                    self._wake_reader.recv_bytes()
                except (EOFError, OSError):
                    pass
                with self._lock:
                    if self._stopping:
                        return
            for sentinel in ready:
                process = active.pop(sentinel, None)
                if process is None:
                    continue
                try:
                    process.join(0)
                except (AssertionError, ValueError):
                    continue
                with self._lock:
                    if self._stopping or process in self._reported:
                        continue
                    self._reported.add(process)
                self._on_death(process)

    def _close_pipes(self) -> None:
        with self._lock:
            self._close_pipes_locked()

    def _close_pipes_locked(self) -> None:
        if self._closed:
            return
        self._closed = True
        for connection in (self._wake_reader, self._wake_writer):
            try:
                connection.close()
            except Exception:
                pass


__all__ = ["ManagedNodeMonitor"]
