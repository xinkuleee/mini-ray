"""Bounded proof that two dispatch lanes execute on two nodes concurrently.

Each task requires a custom resource present on exactly one logical node.  Both
Workers connect to a loopback barrier owned by this test process and wait for a
release byte.  The test accepts and identifies both connections before releasing
either one, which proves overlap without sleeps or elapsed-time thresholds.

Run this exact node ID only through ``scripts/run_bounded_test.py``.  The hard
bounds are one GCS, two NodeManagers, one Worker per node, two tiny tasks, one
test-owned loopback listener and two 1-MiB stores. All task, arrival, release
and result work shares fifteen seconds after init; accepted sockets and public
reference close share three seconds in finally before unconditional shutdown.
The listener's setup is inside that cleanup domain. Initial/returned/report
PIDs and all seven endpoints, including the Driver owner, are checked even on
failure. No test-owned thread, extra Task, injected failure or sleep is added;
startup/shutdown retain the runner's 30-second process-tree deadline.
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

_NODE_ZERO_RESOURCE = "parallel_node_zero"
_NODE_ONE_RESOURCE = "parallel_node_one"
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_RELEASE = b"G"
_ARRIVAL = struct.Struct("!cQ")


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("parallel-task barrier exceeded its work deadline")
    return remaining


def _join_driver_barrier(
    barrier_address: tuple[str, int], marker: bytes, deadline: float
) -> tuple[bytes, int]:
    """Announce this Worker and remain blocked until the Driver releases it."""

    if not isinstance(marker, bytes) or len(marker) != 1:
        raise ValueError("barrier marker must be exactly one byte")
    worker_pid = os.getpid()
    with socket.create_connection(
        barrier_address, timeout=_remaining(deadline)
    ) as connection:
        connection.settimeout(_remaining(deadline))
        connection.sendall(_ARRIVAL.pack(marker, worker_pid))
        connection.settimeout(_remaining(deadline))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the concurrency barrier")
    return marker, worker_pid


node_zero_task = ray.remote(
    num_cpus=1, resources={_NODE_ZERO_RESOURCE: 1}
)(_join_driver_barrier)
node_one_task = ray.remote(
    num_cpus=1, resources={_NODE_ONE_RESOURCE: 1}
)(_join_driver_barrier)


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert 0 < size <= _ARRIVAL.size
    payload = bytearray()
    # Each nonempty recv advances at least one byte: an arrival needs at most
    # nine reads, each charged to the same deadline rather than a fresh 15s.
    for _ in range(size):
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed the barrier before announcing itself")
        payload.extend(chunk)
        if len(payload) == size:
            return bytes(payload)
    raise RuntimeError("Worker barrier announcement exceeded its fixed frame bound")


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_two_nodes_execute_resource_pinned_tasks_concurrently() -> None:
    listener = context = report = core = None
    refs = []
    close_errors = []
    connections: list[socket.socket] = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    arrivals: dict[bytes, int] = {}
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        barrier_address = listener.getsockname()
        managed_addresses.add(barrier_address)
        listener.listen(2)
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _NODE_ZERO_RESOURCE: 1},
                {"CPU": 1, _NODE_ONE_RESOURCE: 1},
            ),
            num_workers_per_node=1, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        node_zero, node_one = context.nodes
        assert context.trace_address is None
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(node_zero.worker_pids) == len(node_one.worker_pids) == 1
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        refs.append(node_zero_task.remote(barrier_address, b"A", deadline))
        _remaining(deadline)
        refs.append(node_one_task.remote(barrier_address, b"B", deadline))

        # Neither connection is released while accepting the other.  A runtime
        # with only one blocking dispatch lane can never reach this point.
        for _ in refs:
            listener.settimeout(_remaining(deadline))
            connection, _peer = listener.accept()
            connections.append(connection)
            marker, worker_pid = _ARRIVAL.unpack(
                _recv_exact(connection, _ARRIVAL.size, deadline)
            )
            assert marker not in arrivals
            arrivals[marker] = worker_pid

        assert arrivals == {
            b"A": node_zero.worker_pid,
            b"B": node_one.worker_pid,
        }
        ready, remaining = ray.wait(refs, num_returns=1, timeout=0)
        assert ready == []
        assert remaining == list(refs)

        for connection in connections:
            connection.settimeout(_remaining(deadline))
            connection.sendall(_RELEASE)
        results = ray.get(refs, timeout=_remaining(deadline))
        assert results == [
            (b"A", node_zero.worker_pid),
            (b"B", node_one.worker_pid),
        ]
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        # Release every accepted Worker even when an assertion fails; closing the
        # listener makes a not-yet-connected task fail promptly and deterministically.
        try:
            for connection in connections:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if remaining > 0:
                        connection.settimeout(min(0.1, remaining))
                        connection.sendall(_RELEASE)
                except OSError:
                    # A Worker may have closed after the original release.
                    # A send failure is not an execution/death acknowledgement.
                    pass
                finally:
                    try:
                        connection.close()
                    except OSError as exc:
                        close_errors.append(exc)
        finally:
            try:
                if listener is not None:
                    listener.close()
            finally:
                try:
                    for reference in refs:
                        try:
                            finalizer, done = reference._finalizer, reference._release_done
                            assert finalizer is not None and done is not None
                            reference.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
                            assert reference.closed and done.is_set()
                        except Exception as exc:
                            close_errors.append(exc)
                finally:
                    try:
                        report = ray.shutdown()
                    finally:
                        if report is not None:
                            managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                        if core is not None and core.owner_address is not None:
                            managed_addresses.add(core.owner_address)
                        surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
                        surviving_children = tuple(
                            child.pid for child in mp.active_children() if child.pid in managed_pids
                        )
                        open_addresses = []
                        for address in sorted(managed_addresses):
                            try:
                                with socket.create_connection(address, timeout=0.1):
                                    open_addresses.append(address)
                            except OSError:
                                pass
                        assert not surviving_pids, surviving_pids
                        assert not surviving_children, surviving_children
                        assert not open_addresses, open_addresses
                        assert not close_errors, close_errors
                        assert not ray.is_initialized()

    assert context is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
