"""Bounded proof that nested ``get`` yields its parent task's CPU.

The cluster has one Node, one CPU, and two ordinary Workers.  A one-CPU parent
submits a one-CPU child and blocks in ``ray.get``.  The child then reaches a
Driver-owned socket gate while the parent is still unfinished.  That state is
impossible unless the parent returned its CPU to the NodeManager; distinct PIDs
also prove the child used the second Worker rather than re-entering the parent.

Run only this allowlisted exact node ID through ``scripts/run_bounded_test.py``.
Bounds are one GCS, one Node, two Workers, one 1 MiB store, one socket gate,
and two tiny tasks with no retries. No Actors, tracing, failure injection, or
test-owned threads. All API/gate work shares one ten-second post-init deadline
across Driver and Workers; reference cleanup gets at most three seconds.
The runner's 30-second execution deadline covers startup through shutdown,
with its existing bounded process-tree cleanup grace on timeout. Runtime
Block/Unblock control RPCs retain their own finite retry bounds.
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
from miniray.ownership import ObjectCollectionState
from miniray.runtime_binding import current_core_worker
from tests.integration.test_task_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_CHILD_ARRIVAL_FORMAT = "!QQ"
_CHILD_ARRIVAL_SIZE = struct.calcsize(_CHILD_ARRIVAL_FORMAT)
_RELEASE = b"G"


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("CPU-yield acceptance exceeded its shared deadline")
    return remaining


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed before completing gate message")
        payload.extend(chunk)
    return bytes(payload)


@ray.remote(num_cpus=1, max_retries=0)
def _yield_child(
    gate_address: tuple[str, int], parent_pid: int, deadline: float
) -> tuple[int, int]:
    child_pid = os.getpid()
    with socket.create_connection(
        gate_address, timeout=_remaining(deadline)
    ) as connection:
        connection.settimeout(_remaining(deadline))
        connection.sendall(
            struct.pack(_CHILD_ARRIVAL_FORMAT, parent_pid, child_pid)
        )
        if _recv_exact(connection, 1, deadline) != _RELEASE:
            raise RuntimeError("Driver closed the child gate")
    return parent_pid, child_pid


@ray.remote(num_cpus=1, max_retries=0)
def _yield_parent(gate_address: tuple[str, int], deadline: float) -> tuple[int, int]:
    parent_pid = os.getpid()
    core = current_core_worker()
    assert core is not None
    child_ref = None
    try:
        _remaining(deadline)
        child_ref = _yield_child.remote(gate_address, parent_pid, deadline)
        assert child_ref.owner_worker_id == core.worker_id and child_ref.borrower_token is None
        result = ray.get(child_ref, timeout=_remaining(deadline))
        return result
    finally:
        # Closing only bounds the receipt wait; the same release obligation
        # remains with the runtime if this Worker-side wait times out.
        if child_ref is not None:
            child_ref.close(timeout=_CLEANUP_SECONDS)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _assert_runtime_exited(
    managed_pids: set[int], managed_addresses: set[tuple[str, int]]
) -> None:
    """Check recorded process/endpoint hygiene even when the main path fails."""

    live_pids = tuple(sorted(pid for pid in managed_pids if _pid_exists(pid)))
    active_pids = tuple(
        sorted(child.pid for child in mp.active_children() if child.pid in managed_pids)
    )
    open_addresses = []
    for address in sorted(managed_addresses):
        try:
            with socket.create_connection(address, timeout=0.1):
                open_addresses.append(address)
        except OSError:
            pass
    initialized = ray.is_initialized()
    assert not live_pids, live_pids
    assert not active_pids, active_pids
    assert not open_addresses, open_addresses
    assert not initialized


def test_nested_get_yields_cpu_to_child_on_second_worker() -> None:
    context = None
    report = None
    core = parent_ref = listener = None
    child_connection: socket.socket | None = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    expected_worker_pids: tuple[int, ...] = ()
    cleanup_errors: list[Exception] = []
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=2,
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        # Capture every known child and endpoint before structural assertions
        # can fail; an absent OwnerService must not hide the other endpoints.
        managed_pids.update(
            (context.gcs_pid, *context.node_pids, *context.worker_pids)
        )
        managed_addresses.update(
            (context.gcs_address, *context.node_addresses, *context.worker_addresses)
        )
        if context.trace_address is not None:
            managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        core = runtime.core_worker
        node = context.nodes[0]
        expected_worker_pids = node.worker_pids
        assert context.trace_address is None
        assert len(node.worker_ids) == 2
        assert len(expected_worker_pids) == 2
        assert runtime.owner_service is not None
        # Allocation and every partial gate-setup failure are now inside the
        # same finally that closes accepted sockets and shuts down the runtime.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        listener.listen(1)
        assert len(managed_pids) == 4
        assert len(managed_addresses) == 6
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        parent_ref = _yield_parent.remote(gate_address, deadline)

        # The only CPU starts assigned to the parent.  Reaching this accept
        # proves its blocking get yielded that CPU so the child could run.
        listener.settimeout(_remaining(deadline))
        child_connection, _peer = listener.accept()
        parent_pid, child_pid = struct.unpack(
            _CHILD_ARRIVAL_FORMAT,
            _recv_exact(child_connection, _CHILD_ARRIVAL_SIZE, deadline),
        )
        assert parent_pid != child_pid
        assert {parent_pid, child_pid} == set(expected_worker_pids)

        # The child remains behind the gate, hence the parent must still be in
        # its get and must not resume before the matching Unblocked transition.
        ready, remaining = ray.wait((parent_ref,), num_returns=1, timeout=0)
        assert ready == []
        assert remaining == [parent_ref]

        child_connection.settimeout(_remaining(deadline))
        child_connection.sendall(_RELEASE)
        child_connection.close()
        child_connection = None
        listener.close()
        listener = None
        assert ray.get(parent_ref, timeout=_remaining(deadline)) == (
            parent_pid,
            child_pid,
        )
        _close_reference(parent_ref, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        with core._completion:
            while (core.owner_table.collection_state(parent_ref.object_id) is not ObjectCollectionState.COLLECTED
                   or parent_ref.object_id in core._task_finish_barriers):
                core._completion.wait(_remaining(deadline))
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            try:
                if child_connection is not None:
                    try:
                        child_connection.settimeout(min(0.1, _remaining(cleanup_deadline)))
                        child_connection.sendall(_RELEASE)
                    except OSError:
                        pass  # Closing also releases a child that is still receiving.
                    finally:
                        child_connection.close()
            finally:
                if listener is not None:
                    listener.close()
        finally:
            try:
                _close_reference(parent_ref, cleanup_deadline)
            except Exception as exc:
                cleanup_errors.append(exc)
            finally:
                # Includes ray.init failure before it publishes a RuntimeContext.
                try:
                    report = ray.shutdown()
                finally:
                    _assert_runtime_exited(managed_pids, managed_addresses)

    assert context is not None
    assert report is not None
    assert not cleanup_errors, cleanup_errors
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    # This is the end-to-end accounting assertion: parent reacquire, child
    # completion, and parent completion must leave neither CPU debt nor a live
    # lease in the Node's authoritative ledger.
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced and not report.gcs_forced
    assert not any(report.worker_forced)
    assert report.node_pids == context.node_pids
    assert tuple(report.worker_pids) == expected_worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
