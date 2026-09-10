"""Bounded Driver-owned ObjectRef nested-argument acceptance path.

Two tiny blocker tasks occupy the only two ordinary Worker slots before the
consumer is submitted.  Consequently the Driver can close its source handle
after ``remote()`` has durably transferred the logical-task hold, while the
consumer is still unable to obtain a lease or receive ``PushTask``.  Once the
blockers are released, the Worker restores the repeated nested reference as a
public ObjectRef and resolves it through the Driver owner endpoint.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Static
bounds are one GCS, one NodeManager, two ordinary Workers, three tiny tasks,
one Driver-owned inline object, one test-owned loopback barrier, no Actor and
no trace collector. The Node has two CPUs and a 1-MiB store. This source is an
already-READY put, not a PENDING producer: the test proves nested manifest/
lifetime separation and sender-close-before-Push, not pending-input timing.

Driver and Worker barrier/get operations share fifteen seconds after init.
Each of two arrivals requires at most eight reads. Accepted sockets and all
four public handles share three seconds in finally before unconditional
shutdown. Listener setup is inside that cleanup domain; four PIDs and six
endpoints, including the Driver owner, are checked even after failure. No
additional Task, fault, observer thread or sleep is introduced. Startup and
shutdown retain the exact node ID's external 30-second process-tree bound.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import struct
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.ownership import ObjectState


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_RELEASE = b"G"
_ARRIVAL = struct.Struct("!Q")


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("nested-argument work exceeded its deadline")
    return remaining


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert 0 < size <= _ARRIVAL.size
    payload = bytearray()
    for _ in range(size):
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed before barrier arrival")
        payload.extend(chunk)
        if len(payload) == size:
            return bytes(payload)
    raise RuntimeError("Worker arrival exceeded its eight-byte bound")


@ray.remote(num_cpus=1)
def _occupy_worker(barrier_address: tuple[str, int], deadline: float) -> int:
    worker_pid = os.getpid()
    with socket.create_connection(
        barrier_address, timeout=_remaining(deadline)
    ) as connection:
        connection.settimeout(_remaining(deadline))
        # The remote function is serialized by value; keep its globals
        # pickleable instead of capturing a Driver-side struct.Struct.
        connection.sendall(struct.pack("!Q", worker_pid))
        connection.settimeout(_remaining(deadline))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the Worker barrier")
    return worker_pid


@ray.remote(num_cpus=1)
def _consume_nested_argument(container: object, deadline: float) -> tuple[bool, bool, object, object, int]:
    first = container["refs"][0]  # type: ignore[index]
    repeated = container["refs"][1]["again"]  # type: ignore[index]
    return (
        isinstance(first, ray.ObjectRef),
        first is repeated,
        first.object_id == repeated.object_id,
        ray.get(first, timeout=_remaining(deadline)),
        os.getpid(),
    )


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_nested_argument_survives_sender_close_before_worker_push() -> None:
    listener = context = core = None
    source = consumer = None
    blockers: list[ray.ObjectRef] = []
    connections: list[socket.socket] = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    close_errors = []
    report = None
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        barrier_address = listener.getsockname()
        managed_addresses.add(barrier_address)
        listener.listen(2)
        context = ray.init(
            num_nodes=1,
            num_cpus=2,
            num_workers_per_node=2,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        node = context.nodes[0]
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 4
        assert len(managed_addresses) == 6 and os.getpid() not in managed_pids
        assert len(node.worker_ids) == len(node.worker_pids) == 2
        assert context.trace_address is None

        _remaining(deadline)
        blockers.append(_occupy_worker.remote(barrier_address, deadline))
        _remaining(deadline)
        blockers.append(_occupy_worker.remote(barrier_address, deadline))
        arrived_pids = set()
        for _blocker in blockers:
            listener.settimeout(_remaining(deadline))
            connection, _peer = listener.accept()
            connections.append(connection)
            worker_pid, = _ARRIVAL.unpack(_recv_exact(connection, _ARRIVAL.size, deadline))
            arrived_pids.add(worker_pid)
        assert arrived_pids == set(node.worker_pids)

        _remaining(deadline)
        source = ray.put(("driver-owned-nested", 42))
        assert isinstance(source, ray.ObjectRef)
        source_object_id = source.object_id
        container = {"refs": [source, {"again": source}]}
        consumer = _consume_nested_argument.remote(container, deadline)
        assert isinstance(consumer, ray.ObjectRef)
        with core._state_lock:
            pending = core._task_finish_barriers[consumer.object_id]
            spec = pending.spec
            assert spec == core.owner_table.snapshot(consumer.object_id).producer_task_spec
            assert spec.attempt_id.attempt_number == 0 and spec.task_id == consumer.object_id.task_id
            assert pending.protected_dependencies == ()
            assert pending.nested_local_holds == (source_object_id,)
            assert len(spec.args) == 2 and all(type(argument) is protocol.InlineArg for argument in spec.args)
            transfer, = spec.args[0].nested_refs
            assert spec.args[1].nested_refs == ()
            assert (transfer.object_id, transfer.owner_worker_id, transfer.owner_address) == (
                source_object_id, core.worker_id, runtime.owner_service.address,
            )
            hold = pending.dependency_hold
            assert transfer.hold == hold and hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
            assert hold.submitting_worker_id == core.worker_id and hold.task_id == spec.task_id
            assert hold.origin_attempt_id == spec.attempt_id
            source_before = core.owner_table.snapshot(source_object_id)
            assert source_before.state is ObjectState.READY_INLINE and source_before.local_tokens
            assert source_before.submitted_tokens == frozenset((hold,))
            edge, = core.owner_table.snapshot(consumer.object_id).outgoing_lineage_edges
            assert edge.producer_object_id == consumer.object_id and edge.dependency_object_id == source_object_id
            assert source_before.lineage_tokens == frozenset((edge.token,))
            assert not source_before.borrowed_tokens

        # Both execution slots are still held at this point, so remote() has
        # completed its reference-transfer transaction but PushTask cannot yet
        # reach a consumer Worker.
        ready, remaining = ray.wait((consumer,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [consumer]
        done = source._release_done
        assert done is not None
        source.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        assert source.closed and source.object_id == source_object_id
        assert done.is_set()
        with core._state_lock:
            source_after = core.owner_table.snapshot(source_object_id)
            assert source_after.state is ObjectState.READY_INLINE and not source_after.local_tokens
            assert source_after.inline_data == source_before.inline_data
            assert source_after.current_attempt == source_before.current_attempt
            assert source_after.submitted_tokens == frozenset((hold,))
            assert source_after.lineage_tokens == frozenset((edge.token,))
            assert not source_after.borrowed_tokens

        for connection in connections:
            connection.settimeout(_remaining(deadline))
            connection.sendall(_RELEASE)
        assert set(ray.get(blockers, timeout=_remaining(deadline))) == arrived_pids

        is_public, same_instance, same_id, value, consumer_pid = ray.get(
            consumer, timeout=_remaining(deadline)
        )
        assert is_public
        assert same_instance
        assert same_id
        assert value == ("driver-owned-nested", 42)
        assert consumer_pid in node.worker_pids
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for connection in connections:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if remaining > 0:
                        connection.settimeout(min(0.1, remaining))
                        connection.sendall(_RELEASE)
                except OSError:
                    pass  # Already closed or late is not an execution ACK.
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
                    for reference in (consumer, *blockers, source):
                        if reference is None:
                            continue
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
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert tuple(report.worker_pids) == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
