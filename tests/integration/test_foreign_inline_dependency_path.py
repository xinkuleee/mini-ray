"""Bounded foreign INLINE dependency lifetime acceptance test.

One parent task returns a still-pending, Worker-owned child ``ObjectRef``.  The
Driver submits that borrowed handle as a top-level dependency, then closes both
the borrower and its outer container before the child becomes ready.  The
logical consumer task's independently retained hold must keep the object alive;
after the child gate opens, the dependency is materialized as an ``InlineArg``
and the consumer executes exactly once.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Static
bounds are one GCS, one NodeManager, two ordinary Workers, exactly four tiny
tasks, one test-owned loopback gate, no Actor Worker, no trace collector, and
the runner's 30-second process-group deadline.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import struct

import pytest

import miniray as ray


pytestmark = pytest.mark.multiprocess_smoke

_GATE_TIMEOUT_SECONDS = 10.0
_CHILD_ARRIVAL_FORMAT = "!Q"
_CHILD_ARRIVAL_SIZE = struct.calcsize(_CHILD_ARRIVAL_FORMAT)
_RELEASE = b"G"


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed before completing gate message")
        payload.extend(chunk)
    return bytes(payload)


@ray.remote(num_cpus=1)
def _pending_inline_child(gate_address: tuple[str, int]) -> tuple[str, int]:
    child_pid = os.getpid()
    with socket.create_connection(
        gate_address, timeout=_GATE_TIMEOUT_SECONDS
    ) as connection:
        connection.settimeout(_GATE_TIMEOUT_SECONDS)
        connection.sendall(struct.pack(_CHILD_ARRIVAL_FORMAT, child_pid))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the child gate")
    return "foreign-inline", child_pid


@ray.remote(num_cpus=1)
def _return_pending_child_ref(gate_address: tuple[str, int]) -> object:
    return _pending_inline_child.remote(gate_address)


@ray.remote(num_cpus=1)
def _consume_foreign_inline(value: tuple[str, int]) -> tuple[str, int, int]:
    label, producer_pid = value
    return label, producer_pid, os.getpid()


@ray.remote(num_cpus=1)
def _independent_ready_probe() -> tuple[str, int]:
    return "ready-while-foreign-pending", os.getpid()


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_foreign_inline_dependency_survives_input_handle_close() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(_GATE_TIMEOUT_SECONDS)
    gate_address = listener.getsockname()

    context = None
    outer = None
    foreign = None
    consumer = None
    child_connection: socket.socket | None = None
    child_pid = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = {gate_address}
    report = None
    probe = None
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=2,
            num_workers_per_node=2,
            inline_threshold=1024,
            enable_tracing=False,
        )
        node = context.nodes[0]
        managed_pids.update(
            {context.gcs_pid, node.node_pid, *node.worker_pids}
        )
        managed_addresses.update(
            {context.gcs_address, node.node_address, *node.worker_addresses}
        )
        assert context.trace_address is None
        assert len(node.worker_ids) == len(node.worker_pids) == 2
        assert len(managed_pids) == 4
        assert len(managed_addresses) == 5
        assert os.getpid() not in managed_pids

        outer = _return_pending_child_ref.remote(gate_address)
        foreign = ray.get(outer, timeout=_GATE_TIMEOUT_SECONDS)
        assert isinstance(foreign, ray.ObjectRef)
        assert foreign.owner_worker_id in node.worker_ids
        assert foreign.owner_address in node.worker_addresses
        assert foreign.borrower_token is not None

        child_connection, _peer = listener.accept()
        child_connection.settimeout(_GATE_TIMEOUT_SECONDS)
        (child_pid,) = struct.unpack(
            _CHILD_ARRIVAL_FORMAT,
            _recv_exact(child_connection, _CHILD_ARRIVAL_SIZE),
        )
        assert child_pid in node.worker_pids

        # remote() may return only after the owner ACKs an independent logical
        # task hold.  Releasing both earlier lifetime reasons must therefore be
        # safe while the producer is still pending behind the gate.
        consumer = _consume_foreign_inline.remote(foreign)
        assert isinstance(consumer, ray.ObjectRef)
        foreign.close()
        outer.close()
        assert foreign.closed and outer.closed

        ready, remaining = ray.wait((consumer,), num_returns=1, timeout=0)
        assert ready == []
        assert remaining == [consumer]

        # A foreign PENDING dependency belongs to the coordinator gate, not a
        # dispatch lane, CPU allocation, or Worker lease.  The child still owns
        # one Worker/CPU while blocked on its socket, but the Worker/CPU released
        # by the parent remains available to this independent ready task.
        probe = _independent_ready_probe.remote()
        # A short bounded timeout is intentional here: the producer gate remains
        # closed, so this cannot accidentally pass after the dependency resolves.
        probe_label, probe_pid = ray.get(probe, timeout=2.0)
        assert probe_label == "ready-while-foreign-pending"
        assert probe_pid in node.worker_pids
        probe.close()
        probe = None

        child_connection.sendall(_RELEASE)
        label, producer_pid, consumer_pid = ray.get(
            consumer, timeout=_GATE_TIMEOUT_SECONDS
        )
        assert label == "foreign-inline"
        assert producer_pid == child_pid
        assert producer_pid in node.worker_pids
        assert consumer_pid in node.worker_pids
        consumer.close()
        assert consumer.closed
    finally:
        if child_connection is not None:
            try:
                child_connection.sendall(_RELEASE)
            except OSError:
                pass
            child_connection.close()
        listener.close()
        if probe is not None:
            probe.close()
        if consumer is not None:
            consumer.close()
        if foreign is not None:
            foreign.close()
        if outer is not None:
            outer.close()
        report = ray.shutdown()

    assert context is not None
    assert report is not None
    assert child_pid is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
    assert all(not _pid_exists(pid) for pid in managed_pids)
    assert all(child.pid not in managed_pids for child in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            socket.create_connection(address, timeout=0.1)
