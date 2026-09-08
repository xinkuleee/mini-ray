"""Bounded real-process acceptance test for one Actor restart.

The Actor itself exits only after a test-owned loopback gate proves that its
generation-0 method entered user code.  No raw PID is ever used as a signal
target.  Run this exact node ID only through ``scripts/run_bounded_test.py``.
The hard bounds are one GCS, one NodeManager, one ordinary Worker, one live
Actor Worker per generation, one injected exit, and one permitted restart.
Four children are live at once; five distinct managed PIDs are observed. Four
tiny method calls use one 1 MiB store, a shared 15-second post-init work budget
and one gate bounded by ten seconds. Reference cleanup shares three seconds;
shutdown is unconditional. Actor create has no per-call cancellation timeout:
only the outer 30-second runner bounds unresolved creation, not a fake ACK.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import struct
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.core import _RPC_CALL_DEADLINE
from miniray.errors import ActorDiedError


pytestmark = pytest.mark.multiprocess_smoke

_BOUND_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_POLL_SECONDS = 0.01
_CRASH_ACK = b"X"
_ARRIVAL_FORMAT = "!Q"
_ARRIVAL = struct.Struct(_ARRIVAL_FORMAT)
_INITIAL_VALUE = 17


@ray.remote(num_cpus=1, max_restarts=1)
class RestartableCounter:
    def __init__(self, initial_value: int) -> None:
        self.value = initial_value

    def identity(self) -> tuple[int, int]:
        return self.value, os.getpid()

    def increment(self) -> tuple[int, int]:
        self.value += 1
        return self.value, os.getpid()

    def crash_after_gate(self, gate_address: tuple[str, int], deadline: float) -> None:
        worker_pid = os.getpid()
        gate_deadline = min(deadline, time.monotonic() + _BOUND_SECONDS)
        with socket.create_connection(gate_address, timeout=_remaining(gate_deadline)) as gate:
            gate.settimeout(_remaining(gate_deadline))
            # Keep the Actor class cloudpickle-safe: a format string and the
            # imported module are serializable references, while a module-level
            # ``struct.Struct`` C object is not.
            gate.sendall(struct.pack(_ARRIVAL_FORMAT, worker_pid))
            gate.settimeout(_remaining(gate_deadline))
            if gate.recv(1) != _CRASH_ACK:
                raise RuntimeError("Driver closed the Actor crash gate without an ACK")
        os._exit(23)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Actor restart exceeded its work deadline")
    return remaining


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Actor Worker closed the gate before announcing itself")
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
    while _pid_exists(pid) and time.monotonic() < deadline:
        wake.wait(min(_POLL_SECONDS, _remaining(deadline)))
    assert not _pid_exists(pid), "PID {} remained alive".format(pid)


def _assert_tracked_hygiene(pids, addresses, listener, connections) -> None:
    """Check every recorded resource even when an earlier assertion failed."""
    deadline = time.monotonic() + 2.0
    wake = threading.Event()
    alive = tuple(pid for pid in pids if _pid_exists(pid))
    while alive:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wake.wait(min(_POLL_SECONDS, remaining))
        alive = tuple(pid for pid in pids if _pid_exists(pid))
    children = tuple(child.pid for child in mp.active_children() if child.pid in pids)
    open_endpoints = []
    for address in addresses:
        try:
            with socket.create_connection(address, timeout=0.1):
                open_endpoints.append(address)
        except OSError:
            pass
    open_handles = tuple(connection.fileno() for connection in (listener, *connections)
                         if connection is not None and connection.fileno() >= 0)
    assert not (alive or children or open_endpoints or open_handles), {
        "live_pids": alive, "active_children": children,
        "open_endpoints": open_endpoints, "open_gate_handles": open_handles,
    }
    assert not ray.is_initialized()


def _wait_for_restarted_actor(core, actor_id, old_snapshot, deadline):
    latest = None
    wake = threading.Event()
    while time.monotonic() < deadline:
        deadline_token = _RPC_CALL_DEADLINE.set(deadline)
        try:
            latest = core._query_actor_state(actor_id)
        finally:
            _RPC_CALL_DEADLINE.reset(deadline_token)
        if (
            latest.state is protocol.ActorState.ALIVE
            and latest.generation == old_snapshot.generation.next()
            and latest.route_epoch > old_snapshot.route_epoch
        ):
            with core._state_lock:
                installed = core._actor_clients.snapshot(actor_id)
            if installed == latest:
                return latest
        wake.wait(min(_POLL_SECONDS, _remaining(deadline)))
    raise AssertionError(
        "Actor restart did not converge; latest snapshot={!r}".format(latest)
    )


def test_actor_crash_restarts_once_fences_inflight_call_and_resets_state() -> None:
    listener = None
    context = None
    core = None
    actor = None
    old_snapshot = None
    new_snapshot = None
    report = None
    gate_connection = None
    connections: list[socket.socket] = []
    original_install = None
    close_errors = []
    actor_routes = {}
    route_lock = threading.Lock()
    route_failure = threading.Event()
    refs: list[ray.ObjectRef] = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=1,
            # One lifetime CPU makes a successful restart proof that the old
            # generation released its allocation before the replacement was
            # admitted.  With two CPUs an old-token leak could be hidden until
            # final shutdown.
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update(
            {context.gcs_pid, context.node_pid, context.worker_pid}
        )
        managed_addresses.update(
            {context.gcs_address, context.node_address, context.worker_address}
        )
        runtime = _get_runtime()
        core = runtime.core_worker
        assert context.trace_address is None and runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3 and len(managed_addresses) == 5
        original_install = core._install_actor_snapshot

        def observe_installed_route(snapshot):
            changed = original_install(snapshot)
            try:
                if snapshot.state is protocol.ActorState.ALIVE:
                    route = (snapshot.actor_id, snapshot.generation, snapshot.worker_pid, snapshot.worker_address)
                    with route_lock:
                        # Record every returned physical route before checking
                        # the two-generation bound; never hide an overflow PID.
                        if type(snapshot.worker_pid) is int and snapshot.worker_pid > 0:
                            managed_pids.add(snapshot.worker_pid)
                        else:
                            route_failure.set()
                        if (type(snapshot.worker_address) is tuple and len(snapshot.worker_address) == 2
                                and snapshot.worker_address[0] == "127.0.0.1"
                                and type(snapshot.worker_address[1]) is int
                                and 0 < snapshot.worker_address[1] < 65536):
                            managed_addresses.add(snapshot.worker_address)
                        else:
                            route_failure.set()
                        previous = actor_routes.get(snapshot.worker_id)
                        if previous is not None and previous != route:
                            route_failure.set()
                        if previous is None:
                            if len(actor_routes) < 2:
                                actor_routes[snapshot.worker_id] = route
                            else:
                                route_failure.set()
            except Exception:
                # Observer failures are test evidence, never business-RPC errors.
                route_failure.set()
            return changed

        core._install_actor_snapshot = observe_installed_route

        actor = RestartableCounter.remote(_INITIAL_VALUE)
        _remaining(deadline)
        actor_id = actor._actor_id
        with core._state_lock:
            old_snapshot = core._actor_clients.snapshot(actor_id)
        assert old_snapshot.state is protocol.ActorState.ALIVE
        assert old_snapshot.generation.generation == 0
        assert old_snapshot.route_epoch == 1
        assert old_snapshot.worker_id is not None
        assert old_snapshot.worker_pid is not None
        assert old_snapshot.worker_address is not None
        managed_pids.add(old_snapshot.worker_pid)
        managed_addresses.add(old_snapshot.worker_address)

        before_crash_ref = actor.increment.remote()
        refs.append(before_crash_ref)
        assert ray.get(before_crash_ref, timeout=_remaining(deadline)) == (
            _INITIAL_VALUE + 1, old_snapshot.worker_pid
        )
        before_crash_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        refs.remove(before_crash_ref)

        crash_ref = actor.crash_after_gate.remote(gate_address, deadline)
        refs.append(crash_ref)
        listener.settimeout(_remaining(deadline))
        gate_connection, _peer = listener.accept()
        connections.append(gate_connection)
        announced_pid = _ARRIVAL.unpack(
            _recv_exact(gate_connection, _ARRIVAL.size, deadline)
        )[0]
        assert announced_pid == old_snapshot.worker_pid
        gate_connection.settimeout(_remaining(deadline))
        gate_connection.sendall(_CRASH_ACK)
        gate_connection.close()
        gate_connection = None

        # A generation change fences this already-admitted call.  It must never
        # be replayed against the new constructor state.
        with pytest.raises(ActorDiedError):
            ray.get(crash_ref, timeout=_remaining(deadline))
        crash_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        refs.remove(crash_ref)

        new_snapshot = _wait_for_restarted_actor(core, actor_id, old_snapshot, deadline)
        assert new_snapshot.actor_id == old_snapshot.actor_id == actor_id
        assert new_snapshot.generation == old_snapshot.generation.next()
        assert new_snapshot.route_epoch == old_snapshot.route_epoch + 2
        assert new_snapshot.restarts_used == 1
        assert new_snapshot.max_restarts == 1
        assert new_snapshot.last_exit is not None
        assert new_snapshot.last_exit.actor_id == actor_id
        assert new_snapshot.last_exit.generation == old_snapshot.generation
        assert new_snapshot.last_exit.route_epoch == old_snapshot.route_epoch
        assert new_snapshot.last_exit.worker_id == old_snapshot.worker_id
        assert new_snapshot.last_exit.worker_pid == old_snapshot.worker_pid
        assert new_snapshot.last_exit.exit_code == 23
        assert new_snapshot.worker_id != old_snapshot.worker_id
        assert new_snapshot.worker_pid != old_snapshot.worker_pid
        assert new_snapshot.worker_pid is not None
        assert new_snapshot.worker_address is not None
        managed_pids.add(new_snapshot.worker_pid)
        managed_addresses.add(new_snapshot.worker_address)
        _wait_until_pid_gone(old_snapshot.worker_pid, deadline)

        reset_ref = actor.identity.remote()
        refs.append(reset_ref)
        assert ray.get(reset_ref, timeout=_remaining(deadline)) == (
            _INITIAL_VALUE, new_snapshot.worker_pid
        )
        reset_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        refs.remove(reset_ref)

        increment_ref = actor.increment.remote()
        refs.append(increment_ref)
        assert ray.get(increment_ref, timeout=_remaining(deadline)) == (
            _INITIAL_VALUE + 1, new_snapshot.worker_pid
        )
        increment_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        refs.remove(increment_ref)

        # A transparent replay of generation 0 would execute crash_after_gate a
        # second time and connect again.  The live generation-1 calls above and
        # this empty accept queue jointly prove that no such replay occurred.
        listener.settimeout(min(0.1, _remaining(deadline)))
        with pytest.raises(socket.timeout):
            unexpected, _peer = listener.accept()
            connections.append(unexpected)
            unexpected.close()
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            # EOF releases a failed pre-crash gate without injecting another
            # exit. Only the explicit successful-path ACK authorizes os._exit.
            for connection in connections:
                try:
                    connection.close()
                except OSError as exc:
                    close_errors.append(exc)
            if listener is not None:
                try:
                    listener.close()
                except OSError as exc:
                    close_errors.append(exc)
        finally:
            try:
                for ref in refs:
                    try:
                        ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
                    except Exception as exc:
                        close_errors.append(exc)
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    try:
                        if core is not None and original_install is not None:
                            core._install_actor_snapshot = original_install
                    finally:
                        with route_lock:
                            pids, addresses = set(managed_pids), set(managed_addresses)
                        _assert_tracked_hygiene(pids, addresses, listener, connections)
                        assert not route_failure.is_set(), "Actor route observer saw conflicting or excess routes"

    assert not close_errors and context is not None
    assert old_snapshot is not None and new_snapshot is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(not forced for forced in report.worker_forced)
    assert all(not forced for forced in report.node_forced)

    assert len(actor_routes) == 2 and len(managed_pids) == 5
    assert os.getpid() not in managed_pids
    assert {old_snapshot.worker_id, new_snapshot.worker_id} == set(actor_routes)
    assert managed_addresses == {
        gate_address, context.gcs_address, context.node_address, context.worker_address,
        runtime.owner_service.address, old_snapshot.worker_address, new_snapshot.worker_address,
    }
