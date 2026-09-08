"""Bounded proof that the Driver home route migrates after Node loss.

The test owns one GCS, two NodeManagers, one Worker per Node, and one loopback
gate.  Attempt zero enters user code on the initial home Node before that exact
process group is killed.  The same logical task then retries on the sole
survivor, after which fresh scheduling and Driver ``put`` use the migrated home.
Five managed children, two 1 MiB stores, one gated Task with two attempts, one
ordinary probe and one 2 KiB stored put. Control arguments stay INLINE while
each successful Task result has a 2 KiB payload generated inside its Worker.
Gate/get observations share fifteen seconds after init; at most 32 lease
observations remain passive. All reference
receipts share three seconds in finally, followed by unconditional shutdown and
the five-PID/seven-endpoint checks. Run one exact ID via the 30-second runner.
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
from miniray.ownership import ObjectState


pytestmark = pytest.mark.multiprocess_smoke

_BOUND_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 32
_POLL_SECONDS = 0.01
_INLINE_THRESHOLD = 1024
_PAYLOAD_BYTES = 2048
_RELEASE = b"G"
_ARRIVAL = struct.Struct("!cQ")


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("home recovery exceeded its shared work deadline")
    return remaining


def _close(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _gated_pid(address: tuple[str, int], marker: bytes, deadline: float) -> tuple[bytes, int, bytes]:
    with socket.create_connection(address, timeout=min(_BOUND_SECONDS, _remaining(deadline))) as connection:
        connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        connection.sendall(_ARRIVAL.pack(marker, os.getpid()))
        connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed task gate without release")
    return marker, os.getpid(), b"r" * _PAYLOAD_BYTES


def _pid() -> tuple[int, bytes]:
    return os.getpid(), b"p" * _PAYLOAD_BYTES


recover_local = ray.remote(num_cpus=1, max_retries=1)(_gated_pid)
survivor_probe = ray.remote(num_cpus=1)(_pid)


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert size == _ARRIVAL.size
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed gate before announcing itself")
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


def _wait_until_pid_gone(pid: int, deadline: float) -> None:
    wake = threading.Event()
    for _ in range(1024):
        if not _pid_exists(pid):
            return
        wake.wait(min(_POLL_SECONDS, _remaining(deadline)))
    assert not _pid_exists(pid)


def test_driver_local_node_death_migrates_home_and_retries_on_survivor() -> None:
    listener = None
    context = runtime = core = original_rpc = owner_service = None
    recovery_ref = probe_ref = put_ref = None
    gate_connections: list[socket.socket] = []
    observations: list[tuple[protocol.RequestWorkerLease, object]] = []
    observation_lock = threading.Lock()
    observation_overflow = threading.Event()
    cleanup_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    death = report = None
    owner_identity = owner_address = None
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=2, num_cpus=1, num_workers_per_node=1,
            inline_threshold=_INLINE_THRESHOLD, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        victim, survivor = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
        owner_service = runtime.owner_service
        assert owner_service is not None and owner_service.is_running
        owner_identity, owner_address = core.worker_id, owner_service.address
        original_rpc = core._rpc
        managed_pids.update((
            context.gcs_pid, victim.node_pid, victim.worker_pid,
            survivor.node_pid, survivor.worker_pid,
        ))
        managed_addresses.update((
            context.gcs_address, victim.node_address, victim.worker_address,
            survivor.node_address, survivor.worker_address, owner_address,
        ))
        assert len(managed_pids) == 5 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 7 and context.trace_address is None

        def inspect_rpc(address, handler, message):
            reply = original_rpc(address, handler, message)
            if handler == REQUEST_LEASE_HANDLER and isinstance(
                message, protocol.RequestWorkerLease
            ):
                with observation_lock:
                    if len(observations) < _MAX_OBSERVATIONS:
                        observations.append((message, reply))
                    else:
                        observation_overflow.set()
            return reply

        core._rpc = inspect_rpc
        recovery_ref = recover_local.remote(gate_address, b"L", deadline)
        object_id = recovery_ref.object_id
        listener.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        first, _ = listener.accept()
        gate_connections.append(first)
        marker, first_pid = _ARRIVAL.unpack(_recv_exact(first, _ARRIVAL.size, deadline))
        assert (marker, first_pid) == (b"L", victim.worker_pid)
        # The control tuple must survive the home Node's loss. A threshold of
        # one would lift these values into home-only puts without lineage,
        # testing unreconstructable input loss instead of home-route retry.
        original_spec = core.owner_table.snapshot(object_id).producer_task_spec
        assert original_spec is not None and len(original_spec.args) == 3
        assert all(type(argument) is protocol.InlineArg for argument in original_spec.args)
        assert sum(len(argument.data) for argument in original_spec.args) <= _INLINE_THRESHOLD

        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert death.exit_code == -signal.SIGKILL
        first.close()
        _wait_until_pid_gone(victim.node_pid, deadline)
        _wait_until_pid_gone(victim.worker_pid, deadline)

        listener.settimeout(min(_BOUND_SECONDS, _remaining(deadline)))
        second, _ = listener.accept()
        gate_connections.append(second)
        marker, second_pid = _ARRIVAL.unpack(_recv_exact(second, _ARRIVAL.size, deadline))
        assert (marker, second_pid) == (b"L", survivor.worker_pid)
        second.settimeout(_remaining(deadline))
        second.sendall(_RELEASE)
        assert ray.get(recovery_ref, timeout=_remaining(deadline)) == (
            b"L", survivor.worker_pid, b"r" * _PAYLOAD_BYTES
        )
        second.close()

        route = core._home_route_snapshot()
        assert route is not None
        assert route.node_id == survivor.node_id
        assert route.address == survivor.node_address
        assert route.membership_epoch >= death.death_epoch
        assert core.worker_id == owner_identity
        assert core.owner_address == owner_address
        assert owner_service.address == owner_address and owner_service.is_running

        owner_snapshot = core.owner_table.snapshot(object_id)
        assert owner_snapshot.state is ObjectState.READY_STORED
        assert owner_snapshot.locations == frozenset({survivor.node_id})
        assert owner_snapshot.current_attempt.attempt_number == 1
        with observation_lock:
            attempts = {
                request.attempt_id.attempt_number: (request, reply)
                for request, reply in observations
                if request.task_id == object_id.task_id
                and isinstance(reply, protocol.GrantWorkerLease)
            }
        assert set(attempts) == {0, 1}
        first_request, first_grant = attempts[0]
        retry_request, retry_grant = attempts[1]
        assert first_request.task_id == retry_request.task_id == object_id.task_id
        assert first_request.return_ids == retry_request.return_ids == (object_id,)
        assert first_request.lease_id != retry_request.lease_id
        assert (first_grant.node_id, retry_grant.node_id) == (
            victim.node_id, survivor.node_id
        )

        probe_ref = survivor_probe.remote()
        assert ray.get(probe_ref, timeout=_remaining(deadline)) == (survivor.worker_pid, b"p" * _PAYLOAD_BYTES)
        probe_snapshot = core.owner_table.snapshot(probe_ref.object_id)
        assert probe_snapshot.state is ObjectState.READY_STORED
        assert probe_snapshot.locations == frozenset({survivor.node_id})
        put_payload = b"s" * _PAYLOAD_BYTES
        put_ref = ray.put(put_payload)
        assert ray.get(put_ref, timeout=_remaining(deadline)) == put_payload
        put_snapshot = core.owner_table.snapshot(put_ref.object_id)
        assert put_snapshot.state is ObjectState.READY_STORED
        assert put_snapshot.locations == frozenset({survivor.node_id})
        assert not observation_overflow.is_set()
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for connection in gate_connections:
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
                    for ref in (put_ref, probe_ref, recovery_ref):
                        try:
                            _close(ref, cleanup_deadline)
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
                        assert all(connection.fileno() == -1 for connection in gate_connections)
                        assert not cleanup_errors, cleanup_errors
                        assert not observation_overflow.is_set()

    assert context is not None and death is not None and report is not None
    victim, survivor = context.nodes
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid and not report.gcs_forced and not report.forced
    assert report.node_pids == (victim.node_pid, survivor.node_pid)
    assert report.node_exitcodes == (death.exit_code, 0)
    expected = report.node_deaths[1]
    assert expected is not None and expected.reason is protocol.NodeDeathReason.EXPECTED
    assert report.node_deaths == (death, expected)
    assert report.node_cleans == (False, True)
    assert report.node_forced == (False, False)
    assert report.node_finalized == (False, True)
    assert report.node_shutdown_ack_clean == (False, True)
    assert report.node_resources_clean == (False, True)
    assert owner_service is not None and not owner_service.is_running
    assert report.worker_pids == (victim.worker_pid, survivor.worker_pid)
    assert report.worker_exitcodes == (None, 0)
    assert report.worker_cleans == (False, True)
    assert report.worker_forced == (False, False)
    assert not ray.is_initialized()
