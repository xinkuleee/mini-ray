"""Bounded proof that two Workers on one Node execute concurrently.

Both tiny tasks connect to a test-owned loopback barrier and remain blocked.
The Driver accepts and identifies both Worker connections before releasing
either, proving overlap without sleeps or elapsed-time assertions.

Run only its exact allowlisted ID through ``scripts/run_bounded_test.py`` after
review. One GCS, one Node, two Workers, one 1 MiB store, two logical CPUs and
two tiny Tasks fit the 30-second process-tree bound. The single barrier and
all gets share ten seconds after init. Both reference receipts and gate
release share three seconds in finally, before unconditional shutdown and
four-PID/six-endpoint checks. There is no failure injection or extra Task.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import struct
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime


pytestmark = pytest.mark.multiprocess_smoke

_BARRIER_TIMEOUT_SECONDS = 10.0
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_RELEASE = b"G"
_ARRIVAL = struct.Struct("!cQ")


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("two-Worker concurrency exceeded its work deadline")
    return remaining


def _close_reference(reference, deadline: float) -> None:
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _join_driver_barrier(
    barrier_address: tuple[str, int], marker: bytes, deadline: float
) -> tuple[bytes, int]:
    if not isinstance(marker, bytes) or len(marker) != 1:
        raise ValueError("barrier marker must be exactly one byte")
    worker_pid = os.getpid()
    with socket.create_connection(
        barrier_address, timeout=min(_BARRIER_TIMEOUT_SECONDS, _remaining(deadline))
    ) as connection:
        connection.settimeout(min(_BARRIER_TIMEOUT_SECONDS, _remaining(deadline)))
        connection.sendall(_ARRIVAL.pack(marker, worker_pid))
        connection.settimeout(min(_BARRIER_TIMEOUT_SECONDS, _remaining(deadline)))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the concurrency barrier")
    return marker, worker_pid


barrier_task = ray.remote(num_cpus=1)(_join_driver_barrier)


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert size == _ARRIVAL.size
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(min(_BARRIER_TIMEOUT_SECONDS, _remaining(deadline)))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed before announcing itself")
        payload.extend(chunk)
    return bytes(payload)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_one_node_two_workers_execute_two_tasks_concurrently() -> None:
    listener = None
    context = None
    report = None
    refs = []
    cleanup_errors = []
    connections: list[socket.socket] = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    arrivals: dict[bytes, int] = {}
    expected_worker_pids: tuple[int, ...] = ()
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        barrier_address = listener.getsockname()
        managed_addresses.add(barrier_address)
        context = ray.init(
            num_nodes=1,
            num_cpus=2,
            num_workers_per_node=2,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        node = context.nodes[0]
        expected_worker_pids = node.worker_pids
        assert context.trace_address is None
        assert len(node.worker_ids) == 2
        managed_pids.update(
            {context.gcs_pid, node.node_pid, *node.worker_pids}
        )
        managed_addresses.update(
            {context.gcs_address, node.node_address, *node.worker_addresses}
        )
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 4
        assert len(managed_addresses) == 6
        assert os.getpid() not in managed_pids

        refs.append(barrier_task.remote(barrier_address, b"A", deadline))
        refs.append(barrier_task.remote(barrier_address, b"B", deadline))

        # Neither task is released while accepting the other.  A one-Worker or
        # one-dispatch-lane runtime cannot reach the second accept.
        for _ in refs:
            listener.settimeout(min(_BARRIER_TIMEOUT_SECONDS, _remaining(deadline)))
            connection, _peer = listener.accept()
            connections.append(connection)
            marker, worker_pid = _ARRIVAL.unpack(
                _recv_exact(connection, _ARRIVAL.size, deadline)
            )
            assert marker not in arrivals
            arrivals[marker] = worker_pid

        assert set(arrivals) == {b"A", b"B"}
        assert set(arrivals.values()) == set(node.worker_pids)
        ready, remaining = ray.wait(refs, num_returns=1, timeout=0)
        assert ready == []
        assert remaining == list(refs)

        for connection in connections:
            connection.settimeout(min(_BARRIER_TIMEOUT_SECONDS, _remaining(deadline)))
            connection.sendall(_RELEASE)
        results = ray.get(refs, timeout=_remaining(deadline))
        assert {marker for marker, _pid in results} == {b"A", b"B"}
        assert {pid for _marker, pid in results} == set(node.worker_pids)
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for connection in connections:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if connection.fileno() != -1 and remaining > 0:
                        connection.settimeout(min(0.1, remaining))
                        connection.sendall(_RELEASE)
                except OSError:
                    pass
                finally:
                    try:
                        connection.close()
                    except OSError as exc:
                        cleanup_errors.append(exc)
        finally:
            try:
                if listener is not None:
                    listener.close()
            finally:
                try:
                    for ref in refs:
                        try:
                            _close_reference(ref, cleanup_deadline)
                        except Exception as exc:
                            cleanup_errors.append(exc)
                finally:
                    try:
                        report = ray.shutdown()
                    finally:
                        surviving_pids = tuple(pid for pid in managed_pids if _pid_exists(pid))
                        surviving_children = tuple(child.pid for child in mp.active_children() if child.pid in managed_pids)
                        open_addresses = []
                        for address in managed_addresses:
                            try:
                                with socket.create_connection(address, timeout=0.1):
                                    open_addresses.append(address)
                            except OSError:
                                pass
                        assert not surviving_pids, surviving_pids
                        assert not surviving_children, surviving_children
                        assert not open_addresses, open_addresses
                        assert not cleanup_errors, cleanup_errors
                        assert listener is None or listener.fileno() == -1
                        assert all(connection.fileno() == -1 for connection in connections)

    assert context is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert len(report.worker_pids) == 2
    assert tuple(report.worker_pids) == expected_worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
