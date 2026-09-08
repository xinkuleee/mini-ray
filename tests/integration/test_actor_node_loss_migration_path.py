"""Bounded proof that one restartable Actor migrates after Node loss.

A gated ordinary task first occupies the Driver-local Node, forcing the Actor
onto the remote victim without giving the Actor a victim-only resource.  The
blocker is then released so the surviving Node can admit generation 1.  Once a
generation-0 Actor call has entered user code, the Driver crashes the victim by
its exact managed NodeID and waits for the complete Node-death barrier.

Run only through this file's exact node ID in ``run_bounded_test.py``.  The
hard bounds are one GCS, two NodeManagers, one ordinary Worker per Node, one
live dedicated Actor Worker per generation, one Actor, and one injected fault.
Six children are live at peak; seven distinct managed PIDs include both Actor
generations. One tiny ordinary Task and two tiny Actor calls use 1 MiB per Node.
Work after init shares 15 seconds, each remote gate at most ten seconds, and
reference cleanup three seconds before unconditional shutdown. Synchronous
Actor creation retains its own exact-replay semantics; the outer 30-second
process-tree runner bounds that experiment, not a per-call cancellation ACK.
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
from miniray import debug as ray_debug
from miniray import protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.core import _RPC_CALL_DEADLINE
from miniray.errors import ActorDiedError


pytestmark = pytest.mark.multiprocess_smoke

_MIGRATION_RESOURCE = "actor_migration"
_BOUND_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_POLL_SECONDS = 0.01
_RELEASE = b"G"
_BLOCKER_MARKER = b"B"
_ACTOR_MARKER = b"A"
_ARRIVAL_FORMAT = "!cQq"
_ARRIVAL = struct.Struct(_ARRIVAL_FORMAT)
_INITIAL_VALUE = 41


def _join_driver_gate(
    gate_address: tuple[str, int], marker: bytes, value: int, deadline: float
) -> tuple[bytes, int, int]:
    """Announce one ordinary Worker and hold its Node allocation."""

    worker_pid = os.getpid()
    gate_deadline = min(deadline, time.monotonic() + _BOUND_SECONDS)
    with socket.create_connection(
        gate_address, timeout=_remaining(gate_deadline)
    ) as connection:
        connection.settimeout(_remaining(gate_deadline))
        connection.sendall(
            struct.pack(_ARRIVAL_FORMAT, marker, worker_pid, value)
        )
        connection.settimeout(_remaining(gate_deadline))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the placement gate without release")
    return marker, worker_pid, value


occupy_survivor = ray.remote(
    num_cpus=1, resources={_MIGRATION_RESOURCE: 1}, max_retries=0
)(_join_driver_gate)


@ray.remote(
    num_cpus=1, resources={_MIGRATION_RESOURCE: 1}, max_restarts=1
)
class MigratingCounter:
    def __init__(self, initial_value: int) -> None:
        self.value = initial_value

    def increment_after_gate(
        self, gate_address: tuple[str, int], deadline: float
    ) -> tuple[int, int]:
        """Mutate generation 0, prove entry, then remain in flight."""

        self.value += 1
        worker_pid = os.getpid()
        gate_deadline = min(deadline, time.monotonic() + _BOUND_SECONDS)
        with socket.create_connection(
            gate_address, timeout=_remaining(gate_deadline)
        ) as connection:
            connection.settimeout(_remaining(gate_deadline))
            # Do not close over the module-level Struct C object: Actor classes
            # are serialized by value for construction in a dedicated process.
            connection.sendall(
                struct.pack(
                    _ARRIVAL_FORMAT, _ACTOR_MARKER, worker_pid, self.value
                )
            )
            connection.settimeout(_remaining(gate_deadline))
            if connection.recv(1) != _RELEASE:
                raise RuntimeError("Driver closed the Actor gate without release")
        return self.value, worker_pid

    def increment(self) -> tuple[int, int]:
        self.value += 1
        return self.value, os.getpid()


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Actor migration exceeded its work deadline")
    return remaining


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("process closed the gate before announcing itself")
        payload.extend(chunk)
    return bytes(payload)


def _accept_arrival(
    listener: socket.socket, connections: list[socket.socket], deadline: float
) -> tuple[socket.socket, bytes, int, int]:
    listener.settimeout(_remaining(deadline))
    connection, _peer = listener.accept()
    connections.append(connection)
    assert len(connections) <= 2
    marker, worker_pid, value = _ARRIVAL.unpack(
        _recv_exact(connection, _ARRIVAL.size, deadline)
    )
    return connection, marker, worker_pid, value


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


def test_actor_migrates_after_remote_node_loss_and_resets_generation() -> None:
    listener = None
    context = None
    runtime = None
    core = None
    original_push = None
    original_install = None
    report = None
    death = None
    old_snapshot = None
    new_snapshot = None
    blocker_ref = None
    old_ref = None
    new_ref = None
    connections: list[socket.socket] = []
    actor_pushes: list[tuple[tuple[str, int], protocol.ActorCallRequest]] = []
    actor_push_lock = threading.Lock()
    actor_routes = {}
    route_lock = threading.Lock()
    route_failure = threading.Event()
    push_overflow = threading.Event()
    close_errors = []
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
                {"CPU": 1, _MIGRATION_RESOURCE: 1},
                {"CPU": 1, _MIGRATION_RESOURCE: 1},
            ),
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        survivor, victim = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
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
        assert context.trace_address is None and runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5 and len(managed_addresses) == 7
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

        # The local node wins the initial scheduling tie.  Holding its only
        # CPU and custom resource makes the Actor's initial remote placement
        # deterministic while preserving identical migration feasibility.
        blocker_ref = occupy_survivor.remote(
            gate_address, _BLOCKER_MARKER, 0, deadline
        )
        blocker_connection, marker, blocker_pid, blocker_value = (
            _accept_arrival(listener, connections, deadline)
        )
        assert (marker, blocker_pid, blocker_value) == (
            _BLOCKER_MARKER, survivor.worker_pid, 0
        )

        actor = MigratingCounter.remote(_INITIAL_VALUE)
        _remaining(deadline)
        actor_id = actor.actor_id
        deadline_token = _RPC_CALL_DEADLINE.set(deadline)
        try:
            old_snapshot = ray_debug.snapshot(actor)
        finally:
            _RPC_CALL_DEADLINE.reset(deadline_token)
        assert isinstance(old_snapshot, protocol.ActorSnapshot)
        assert old_snapshot.actor_id == actor_id
        assert old_snapshot.state is protocol.ActorState.ALIVE
        assert old_snapshot.generation.generation == 0
        assert old_snapshot.route_epoch == 1
        assert old_snapshot.node_id == victim.node_id
        assert old_snapshot.worker_id is not None
        assert old_snapshot.worker_pid is not None
        assert old_snapshot.worker_address is not None
        managed_pids.add(old_snapshot.worker_pid)
        managed_addresses.add(old_snapshot.worker_address)

        # Release survivor capacity before the fault, while the lifetime Actor
        # allocation keeps generation 0 pinned to the victim.
        blocker_connection.settimeout(_remaining(deadline))
        blocker_connection.sendall(_RELEASE)
        assert ray.get(blocker_ref, timeout=_remaining(deadline)) == (
            _BLOCKER_MARKER, survivor.worker_pid, 0
        )
        blocker_connection.close()
        blocker_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        blocker_ref = None

        original_push = core._push_task_rpc

        def inspect_actor_push(address, handler, message):
            if handler == "actor_call" and isinstance(
                message, protocol.ActorCallRequest
            ):
                with actor_push_lock:
                    if len(actor_pushes) < 32:
                        actor_pushes.append((address, message))
                    else:
                        push_overflow.set()
            return original_push(address, handler, message)

        core._push_task_rpc = inspect_actor_push

        old_ref = actor.increment_after_gate.remote(gate_address, deadline)
        old_task_id = old_ref.object_id.task_id
        actor_connection, marker, actor_pid, actor_value = _accept_arrival(
            listener, connections, deadline
        )
        assert (marker, actor_pid, actor_value) == (
            _ACTOR_MARKER, old_snapshot.worker_pid, _INITIAL_VALUE + 1
        )
        ready, remaining = ray.wait((old_ref,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [old_ref]

        # This exact-NodeID failpoint returns only after Actor migration and its
        # owner-side route installation have joined the Node-death barrier.
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert death.node_id == victim.node_id
        assert death.node_pid == victim.node_pid
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT

        deadline_token = _RPC_CALL_DEADLINE.set(deadline)
        try:
            new_snapshot = ray_debug.snapshot(actor)
        finally:
            _RPC_CALL_DEADLINE.reset(deadline_token)
        with core._state_lock:
            installed_snapshot = core._actor_clients.snapshot(actor_id)
        assert installed_snapshot == new_snapshot
        assert new_snapshot.actor_id == old_snapshot.actor_id == actor_id
        assert new_snapshot.state is protocol.ActorState.ALIVE
        assert new_snapshot.generation == old_snapshot.generation.next()
        assert new_snapshot.route_epoch == old_snapshot.route_epoch + 2
        assert new_snapshot.restarts_used == 1
        assert new_snapshot.max_restarts == 1
        assert new_snapshot.node_id == survivor.node_id
        assert new_snapshot.node_id != old_snapshot.node_id
        assert new_snapshot.worker_id != old_snapshot.worker_id
        assert new_snapshot.worker_pid != old_snapshot.worker_pid
        assert new_snapshot.worker_pid is not None
        assert new_snapshot.worker_address is not None
        assert isinstance(new_snapshot.last_exit, protocol.ActorNodeLossRecord)
        assert new_snapshot.last_exit.node_death == death
        assert new_snapshot.last_exit.actor_id == actor_id
        assert new_snapshot.last_exit.generation == old_snapshot.generation
        assert new_snapshot.last_exit.route_epoch == old_snapshot.route_epoch
        assert new_snapshot.last_exit.worker_id == old_snapshot.worker_id
        assert new_snapshot.last_exit.worker_pid == old_snapshot.worker_pid
        managed_pids.add(new_snapshot.worker_pid)
        managed_addresses.add(new_snapshot.worker_address)

        with runtime.node_death_lock:
            assert runtime.node_deaths == {victim.node_id: death}
            assert runtime.latest_live_nodes
            assert tuple(
                info.node_id for info in runtime.latest_live_nodes
            ) == (survivor.node_id,)
        with core._state_lock:
            assert core._dead_nodes == {victim.node_id: death}

        # The old invocation is fenced, not replayed against fresh constructor
        # state.  Its connection dies with the victim and its ObjectRef retains
        # the generation-specific ActorDiedError.
        actor_connection.close()
        with pytest.raises(ActorDiedError):
            ray.get(old_ref, timeout=_remaining(deadline))
        old_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        old_ref = None

        _wait_until_pid_gone(victim.node_pid, deadline)
        _wait_until_pid_gone(victim.worker_pid, deadline)
        _wait_until_pid_gone(old_snapshot.worker_pid, deadline)
        for address in (
            victim.node_address,
            victim.worker_address,
            old_snapshot.worker_address,
        ):
            probe_timeout = min(0.1, _remaining(deadline))
            with pytest.raises(OSError):
                with socket.create_connection(address, timeout=probe_timeout):
                    pass

        # Generation 1's first call must use sequence 0.  The returned value
        # proves constructor state was rebuilt: generation 0 had already changed
        # 41 -> 42, yet this increment also observes 41 -> 42.
        new_ref = actor.increment.remote()
        new_task_id = new_ref.object_id.task_id
        assert ray.get(new_ref, timeout=_remaining(deadline)) == (
            _INITIAL_VALUE + 1, new_snapshot.worker_pid
        )
        new_ref.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        new_ref = None

        with actor_push_lock:
            pushes = tuple(actor_pushes)
        old_pushes = tuple(
            (address, request)
            for address, request in pushes
            if request.task_id == old_task_id
        )
        new_pushes = tuple(
            (address, request)
            for address, request in pushes
            if request.task_id == new_task_id
        )
        assert old_pushes
        assert all(
            address == old_snapshot.worker_address
            and request.sequence == 0
            and request.generation == old_snapshot.generation
            and request.route_epoch == old_snapshot.route_epoch
            and request.target_worker_id == old_snapshot.worker_id
            for address, request in old_pushes
        )
        assert new_pushes
        # Exact physical RPC replay on one route is legal.  Every replay must
        # retain generation 1's first logical sequence and fresh route fence.
        assert all(
            address == new_snapshot.worker_address
            and request.sequence == 0
            and request.generation == new_snapshot.generation
            and request.route_epoch == new_snapshot.route_epoch
            and request.target_worker_id == new_snapshot.worker_id
            for address, request in new_pushes
        )
        assert {request.task_id for _address, request in pushes} == {
            old_task_id, new_task_id
        }
        assert not push_overflow.is_set()

        # A transparent user-code replay would open a second Actor gate.
        listener.settimeout(min(0.1, _remaining(deadline)))
        with pytest.raises(socket.timeout):
            unexpected, _peer = listener.accept()
            connections.append(unexpected)
            unexpected.close()
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for connection in connections:
                try:
                    if connection.fileno() >= 0:
                        connection.settimeout(max(0.0, cleanup_deadline - time.monotonic()))
                        connection.sendall(_RELEASE)
                except OSError:
                    pass
                finally:
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
                for ref in (new_ref, old_ref, blocker_ref):
                    if ref is not None:
                        try:
                            ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
                        except Exception as exc:
                            close_errors.append(exc)
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    try:
                        if core is not None and original_push is not None:
                            core._push_task_rpc = original_push
                        if core is not None and original_install is not None:
                            core._install_actor_snapshot = original_install
                    finally:
                        with route_lock:
                            pids, addresses = set(managed_pids), set(managed_addresses)
                        _assert_tracked_hygiene(pids, addresses, listener, connections)
                        assert not route_failure.is_set(), "Actor route observer saw conflicting or excess routes"

    assert not close_errors and not push_overflow.is_set() and context is not None
    assert death is not None
    assert old_snapshot is not None and new_snapshot is not None
    assert report is not None
    survivor, victim = context.nodes
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert not report.gcs_forced and not report.forced

    # The crash remains visible in aggregate diagnostics, while every survivor
    # component (including generation 1's Actor Worker) shuts down cleanly.
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
    assert not report.node_clean
    assert not report.worker_clean
    assert not report.resources_clean
    assert not report.finalized
    assert not report.shutdown_ack_clean

    assert len(actor_routes) == 2 and len(managed_pids) == 7
    assert os.getpid() not in managed_pids
    assert {old_snapshot.worker_id, new_snapshot.worker_id} == set(actor_routes)
    assert managed_addresses == {
        gate_address, context.gcs_address, *context.node_addresses, *context.worker_addresses,
        runtime.owner_service.address, old_snapshot.worker_address, new_snapshot.worker_address,
    }
