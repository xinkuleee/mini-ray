"""Bounded proof of Driver-owned Task recovery after a Node dies.

Both logical nodes can run the recovery task.  A first gated task occupies the
Driver-local node, forcing attempt 0 onto the remote victim.  Once that attempt
has entered user code, the Driver kills the victim's exact managed process
group and waits for the GCS tombstone, survivor snapshot, and Core death fence.
Only then does the test release local capacity, so attempt 1 must run on the
surviving node with the original TaskID/ObjectID and fresh AttemptID/LeaseID.

Run only through this file's exact node ID in ``run_bounded_test.py``. One GCS,
two Nodes, one Worker per Node, two 1 MiB stores and three tiny logical Tasks
are managed by the 30-second process-tree runner. One loopback gate and all
get/membership observations share a 15-second post-init budget. At most 64
lease records are passive. Reference closes and gate release share three
seconds in failure finally, followed by unconditional cluster shutdown and
all five-PID/seven-endpoint checks. No additional fault or user Task is added.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import socket
import struct
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.node import REQUEST_LEASE_HANDLER


pytestmark = pytest.mark.multiprocess_smoke

_RECOVERABLE_RESOURCE = "recoverable"
_BOUND_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_INLINE_THRESHOLD = 1024
_MAX_OBSERVATIONS = 64
_POLL_SECONDS = 0.01
_RELEASE = b"G"
_ARRIVAL = struct.Struct("!cQ")


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("remote Node recovery exceeded its shared work deadline")
    return remaining


def _close_reference(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _join_driver_gate(
    gate_address: tuple[str, int], marker: bytes, deadline: float
) -> tuple[bytes, int]:
    """Announce the executing Worker and wait for one bounded release."""

    if not isinstance(marker, bytes) or len(marker) != 1:
        raise ValueError("gate marker must be exactly one byte")
    worker_pid = os.getpid()
    with socket.create_connection(
        gate_address, timeout=min(_BOUND_SECONDS, _remaining(deadline))
    ) as connection:
        connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        connection.sendall(_ARRIVAL.pack(marker, worker_pid))
        connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the task gate without release")
    return marker, worker_pid


def _execution_pid() -> int:
    return os.getpid()


occupy_survivor = ray.remote(
    num_cpus=1, resources={_RECOVERABLE_RESOURCE: 1}
)(_join_driver_gate)
recover_after_node_death = ray.remote(
    num_cpus=1,
    resources={_RECOVERABLE_RESOURCE: 1},
    max_retries=1,
)(_join_driver_gate)
survivor_probe = ray.remote(
    num_cpus=1, resources={_RECOVERABLE_RESOURCE: 1}
)(_execution_pid)


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert size == _ARRIVAL.size
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed the gate before announcing itself")
        payload.extend(chunk)
    return bytes(payload)


def _accept_arrival(
    listener: socket.socket, connections: list[socket.socket], deadline: float
) -> tuple[socket.socket, bytes, int]:
    listener.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
    connection, _peer = listener.accept()
    connections.append(connection)
    marker, worker_pid = _ARRIVAL.unpack(
        _recv_exact(connection, _ARRIVAL.size, deadline)
    )
    return connection, marker, worker_pid


def _release(connection: socket.socket, deadline: float) -> None:
    connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
    connection.sendall(_RELEASE)


def _pid_exists(pid: int) -> bool:
    """Return whether a POSIX process still exists, including a zombie."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_until_pid_gone(pid: int, deadline: float) -> None:
    wake = threading.Event()
    for _ in range(1024):
        if not _pid_exists(pid):
            return
        wake.wait(min(_POLL_SECONDS, _remaining(deadline)))
    assert not _pid_exists(pid), "PID {} remained alive".format(pid)


def _await_survivor_snapshot(runtime, death: protocol.NodeDeathRecord, deadline: float):
    """Read the snapshot publication immediately following the death barrier."""

    wake = threading.Event()
    for _ in range(1024):
        with runtime.node_death_lock:
            snapshot = runtime.latest_snapshot
        if (
            snapshot is not None
            and snapshot.membership_epoch >= death.death_epoch
            and all(info.node_id != death.node_id for info in snapshot.nodes)
        ):
            return snapshot
        wake.wait(min(_POLL_SECONDS, _remaining(deadline)))
    raise AssertionError("survivor membership snapshot was not published")


def test_remote_node_death_retries_task_on_survivor_and_reports_crash() -> None:
    listener = None
    context = None
    runtime = None
    core = None
    original_rpc = None
    report = None
    death = None
    blocker_ref = None
    recovery_ref = None
    probe_ref = None
    connections: list[socket.socket] = []
    observations: list[
        tuple[protocol.RequestWorkerLease, object]
    ] = []
    observation_lock = threading.Lock()
    observation_overflow = threading.Event()
    cleanup_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(3)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _RECOVERABLE_RESOURCE: 1},
                {"CPU": 1, _RECOVERABLE_RESOURCE: 1},
            ),
            num_workers_per_node=1,
            inline_threshold=_INLINE_THRESHOLD,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        survivor, victim = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
        original_rpc = core._rpc
        assert runtime.owner_service is not None and runtime.owner_service.is_running
        managed_addresses.add(runtime.owner_service.address)

        managed_pids.update(
            {
                context.gcs_pid,
                survivor.node_pid,
                survivor.worker_pid,
                victim.node_pid,
                victim.worker_pid,
            }
        )
        managed_addresses.update(
            {
                context.gcs_address,
                survivor.node_address,
                survivor.worker_address,
                victim.node_address,
                victim.worker_address,
            }
        )
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7 and context.trace_address is None
        assert os.getpid() not in managed_pids

        def inspect_rpc(address, handler, message):
            reply = original_rpc(address, handler, message)
            if (
                handler == REQUEST_LEASE_HANDLER
                and isinstance(message, protocol.RequestWorkerLease)
            ):
                with observation_lock:
                    if len(observations) < _MAX_OBSERVATIONS:
                        observations.append((message, reply))
                    else:
                        observation_overflow.set()
            return reply

        core._rpc = inspect_rpc

        # The preferred local Node wins the tie. Its own ledger and occupied
        # Worker slot constrain the next request; no timely GCS hint is assumed.
        blocker_ref = occupy_survivor.remote(gate_address, b"S", deadline)
        blocker_connection, marker, blocker_pid = _accept_arrival(
            listener, connections, deadline
        )
        assert (marker, blocker_pid) == (b"S", survivor.worker_pid)

        recovery_ref = recover_after_node_death.remote(gate_address, b"V", deadline)
        original_object_id = recovery_ref.object_id
        victim_connection, marker, attempt_zero_pid = _accept_arrival(
            listener, connections, deadline
        )
        assert (marker, attempt_zero_pid) == (b"V", victim.worker_pid)
        # This case tests a lost executor Node, not loss of unreconstructable
        # by-value argument puts. Verify the actual submitted control stream.
        submitted = core.owner_table.snapshot(original_object_id).producer_task_spec
        assert submitted is not None and len(submitted.args) == 3
        assert all(type(argument) is protocol.InlineArg for argument in submitted.args)
        assert sum(len(argument.data) for argument in submitted.args) <= _INLINE_THRESHOLD
        ready, remaining = ray.wait((recovery_ref,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [recovery_ref]

        # Entering user code proves the first PushTask reached the remote Worker.
        # The private failpoint returns only after the entire recovery barrier.
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert death.node_id == victim.node_id
        assert death.node_pid == victim.node_pid
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT

        snapshot = _await_survivor_snapshot(runtime, death, deadline)
        assert tuple(info.node_id for info in snapshot.nodes) == (
            survivor.node_id,
        )
        with core._state_lock:
            assert core._dead_nodes == {victim.node_id: death}
        with runtime.node_death_lock:
            assert runtime.node_deaths == {victim.node_id: death}
            assert runtime.latest_membership_epoch == snapshot.membership_epoch
            assert runtime.latest_live_nodes == snapshot.nodes
        ready, remaining = ray.wait((recovery_ref,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [recovery_ref]

        victim_connection.close()
        _wait_until_pid_gone(victim.node_pid, deadline)
        _wait_until_pid_gone(victim.worker_pid, deadline)

        # Recovery cannot obtain the only surviving slot until this release.
        _release(blocker_connection, deadline)
        assert ray.get(blocker_ref, timeout=_remaining(deadline)) == (
            b"S",
            survivor.worker_pid,
        )
        blocker_connection.close()

        retry_connection, marker, attempt_one_pid = _accept_arrival(
            listener, connections, deadline
        )
        assert (marker, attempt_one_pid) == (b"V", survivor.worker_pid)
        _release(retry_connection, deadline)
        assert ray.get(recovery_ref, timeout=_remaining(deadline)) == (
            b"V",
            survivor.worker_pid,
        )
        retry_connection.close()

        owner_snapshot = core.owner_table.snapshot(original_object_id)
        assert recovery_ref.object_id == original_object_id
        assert owner_snapshot.current_attempt.task_id == original_object_id.task_id
        assert owner_snapshot.current_attempt.attempt_number == 1

        with observation_lock:
            observed = tuple(observations)
        grants_by_attempt: dict[
            int, tuple[protocol.RequestWorkerLease, protocol.GrantWorkerLease]
        ] = {}
        for request, reply in observed:
            if (
                request.task_id != original_object_id.task_id
                or not isinstance(reply, protocol.GrantWorkerLease)
            ):
                continue
            attempt_number = request.attempt_id.attempt_number
            pair = (request, reply)
            previous = grants_by_attempt.setdefault(attempt_number, pair)
            # Exact lease RPC replay is allowed, but it cannot change outcome.
            assert previous == pair

        assert set(grants_by_attempt) == {0, 1}
        first_request, first_grant = grants_by_attempt[0]
        retry_request, retry_grant = grants_by_attempt[1]
        assert first_request.task_id == retry_request.task_id == original_object_id.task_id
        assert first_request.return_ids == retry_request.return_ids == (
            original_object_id,
        )
        assert first_request.attempt_id == first_grant.attempt_id
        assert retry_request.attempt_id == retry_grant.attempt_id
        assert first_request.attempt_id != retry_request.attempt_id
        assert first_request.lease_id == first_grant.lease_id
        assert retry_request.lease_id == retry_grant.lease_id
        assert first_request.lease_id != retry_request.lease_id
        assert first_request.target_node_id == victim.node_id
        assert retry_request.target_node_id is None
        assert (first_grant.node_id, retry_grant.node_id) == (
            victim.node_id,
            survivor.node_id,
        )
        assert (first_grant.worker_id, retry_grant.worker_id) == (
            victim.worker_id,
            survivor.worker_id,
        )

        probe_ref = survivor_probe.remote()
        assert ray.get(probe_ref, timeout=_remaining(deadline)) == survivor.worker_pid
        assert not observation_overflow.is_set()
        _remaining(deadline)
    finally:
        # Unblock every accepted live Worker before closing the listener.  A
        # not-yet-connected retry then gets a bounded connection failure rather
        # than leaving user code parked during cleanup.
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
                    for ref in (probe_ref, recovery_ref, blocker_ref):
                        try:
                            _close_reference(ref, cleanup_deadline)
                        except Exception as exc:
                            cleanup_errors.append(exc)
                finally:
                    try:
                        if core is not None and original_rpc is not None:
                            core._rpc = original_rpc
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
                        assert listener is None or listener.fileno() == -1
                        assert all(connection.fileno() == -1 for connection in connections)
                        assert not cleanup_errors, cleanup_errors
                        assert not observation_overflow.is_set()

    assert context is not None
    assert death is not None
    assert report is not None
    survivor, victim = context.nodes
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced

    assert report.node_pids == (survivor.node_pid, victim.node_pid)
    assert report.node_exitcodes == (0, death.exit_code)
    expected = report.node_deaths[0]
    assert expected is not None
    assert expected.reason is protocol.NodeDeathReason.EXPECTED
    assert expected.exit_code == 0
    assert report.node_deaths == (expected, death)
    assert report.node_cleans == (True, False)
    assert report.node_forced == (False, False)
    assert report.node_finalized == (True, False)
    assert report.node_shutdown_ack_clean == (True, False)
    assert report.node_resources_clean == (True, False)

    assert report.worker_pids == (survivor.worker_pid, victim.worker_pid)
    assert report.worker_exitcodes == (0, None)
    assert report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False)
    # Aggregate graceful properties deliberately remain false because the
    # report preserves the victim crash instead of rewriting it as clean exit.
    assert not report.node_clean
    assert not report.worker_clean
    assert not report.finalized
    assert not report.shutdown_ack_clean
    assert not report.resources_clean

    assert runtime is not None and runtime.owner_service is not None
    assert not runtime.owner_service.is_running
