"""Bounded proof that one PG participant loss is terminal.

The test creates one two-bundle STRICT_SPREAD placement group, starts exactly
one gated task on the remote participant, and crashes that exact managed Node.
The existing crash failpoint returns only after the GCS death tombstone, the
survivor membership snapshot, and the Driver Core fence have converged. Task
entry is a socket event; later transitions use typed runtime observations.
The GCS resource hint is polled passively within the original work deadline,
not treated as a synchronous part of the survivor's successful Task reply.

Run only by this test's exact allowlisted node ID through the 30-second
``scripts/run_baseline.py --smoke EXACT`` process-tree runner. Bounds are five startup
children (one GCS, two one-Worker Nodes), 1 MiB per Node store, one gated PG
task, one ordinary survivor probe and one exact Node crash. Test-owned work
shares fifteen seconds after init; both real reference finalizers share three
seconds and always fall through to shutdown. Synchronous PG create/remove and
runtime startup/shutdown are still covered by the outer runner: a work deadline
is not transaction cancellation. Every tracked PID and endpoint, including the
Driver owner and gate, is checked even when the work or reference close fails.
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
from miniray.control import GET_NODES_HANDLER, GET_PLACEMENT_GROUP_HANDLER
from miniray.node import REQUEST_LEASE_HANDLER
from miniray.ownership import ObjectState
from miniray.transport import request as rpc_request
from tests.integration.test_task_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_LEASE_OBSERVATIONS = 32
_MAX_RESOURCE_POLLS = 1024
_RESOURCE_POLL_SECONDS = 0.01
_ARRIVAL_FORMAT = "!Q"
_ARRIVAL_SIZE = struct.calcsize(_ARRIVAL_FORMAT)
_RELEASE = b"G"


@ray.remote(num_cpus=1, max_retries=3)
def hold_victim_bundle(gate_address: tuple[str, int], deadline: float) -> int:
    """Prove attempt 0 entered the victim Worker, then await the crash."""

    worker_pid = os.getpid()
    with socket.create_connection(
        gate_address, timeout=_remaining(deadline)
    ) as connection:
        connection.settimeout(_remaining(deadline))
        connection.sendall(struct.pack(_ARRIVAL_FORMAT, worker_pid))
        connection.settimeout(_remaining(deadline))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the PG gate without release")
    return worker_pid


@ray.remote(num_cpus=1, max_retries=3)
def rejected_old_pg_handle(bundle_index: int) -> int:
    return bundle_index


@ray.remote(num_cpus=1)
def survivor_probe() -> int:
    return os.getpid()


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("placement-group loss work exceeded its deadline")
    return remaining


def _rpc_before(deadline: float, address, handler, message):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, message, connect_timeout=min(1.0, remaining),
        request_timeout=remaining, deadline=deadline,
    )


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert size == _ARRIVAL_SIZE
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("victim Worker closed before announcing entry")
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


def test_participant_node_loss_is_terminal_and_survivor_cleans_pg() -> None:
    listener = None
    context = None
    runtime = None
    core = None
    original_rpc = None
    group = None
    victim_ref = None
    probe_ref = None
    gate_connection = None
    death = None
    report = None
    lease_requests: list[protocol.RequestWorkerLease] = []
    lease_overflow = False
    cleanup_errors = []
    observation_lock = threading.Lock()
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        listener.listen(1)
        context = ray.init(
            num_nodes=2,
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        survivor, victim = context.nodes
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
        runtime = _get_runtime()
        core = runtime.core_worker
        original_rpc = core._rpc
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7
        assert os.getpid() not in managed_pids

        def inspect_rpc(address, handler, message):
            nonlocal lease_overflow
            if (
                handler == REQUEST_LEASE_HANDLER
                and isinstance(message, protocol.RequestWorkerLease)
            ):
                with observation_lock:
                    if len(lease_requests) < _MAX_LEASE_OBSERVATIONS:
                        lease_requests.append(message)
                    else:
                        lease_overflow = True
            # The recorder is passive even on overflow: never alter retries,
            # replies or runtime deadlines to satisfy a test observation.
            return original_rpc(address, handler, message)

        core._rpc = inspect_rpc
        _remaining(deadline)
        group = ray.placement_group(
            [{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD"
        )
        _remaining(deadline)
        assert group.bundle_count == 2
        assert tuple(key.bundle_index for key in group.placements) == (0, 1)
        assert {key.node_id for key in group.placements} == {
            survivor.node_id, victim.node_id
        }
        victim_key = next(
            key for key in group.placements if key.node_id == victim.node_id
        )

        victim_ref = hold_victim_bundle.options(
            placement_group=group, bundle_index=victim_key.bundle_index
        ).remote(gate_address, deadline)
        listener.settimeout(_remaining(deadline))
        gate_connection, _peer = listener.accept()
        gate_connection.settimeout(_remaining(deadline))
        announced_pid = struct.unpack(
            _ARRIVAL_FORMAT, _recv_exact(gate_connection, _ARRIVAL_SIZE, deadline)
        )[0]
        assert announced_pid == victim.worker_pid
        ready, remaining = ray.wait((victim_ref,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [victim_ref]

        # This is the only injected failure.  Return proves the complete
        # GCS -> survivor snapshot -> Core death transaction, not a timeout.
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert death.node_id == victim.node_id
        assert death.node_pid == victim.node_pid
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        victim_runtime = next(
            node for node in runtime.nodes if node.node_id == victim.node_id
        )
        assert victim_runtime.process.exitcode == death.exit_code
        assert not victim_runtime.process.is_alive()

        gcs_pg = _rpc_before(
            deadline,
            context.gcs_address,
            GET_PLACEMENT_GROUP_HANDLER,
            protocol.GetPlacementGroupRequest(group.placement_group_id),
        )
        assert isinstance(gcs_pg, protocol.GetPlacementGroupReply)
        assert gcs_pg.found and gcs_pg.attempt == group.attempt
        assert gcs_pg.phase is protocol.PlacementGroupPhaseStatus.LOST
        assert gcs_pg.placements == ()
        with core._state_lock:
            assert core._placement_group_states[(
                group.placement_group_id, group.attempt
            )] is protocol.PlacementGroupPhaseStatus.LOST
            installed = core._installed_cluster_snapshot
            assert tuple(info.node_id for info in installed.nodes) == (survivor.node_id,)
            expected_survivor = installed.nodes[0]

        # Attempt 0 entered user code on the dead participant.  Even with a
        # retry budget of three, terminal PG loss publishes the typed error
        # without minting another AttemptID or LeaseID.
        with pytest.raises(ray.PlacementGroupLostError):
            ray.get(victim_ref, timeout=_remaining(deadline))
        owner = core.owner_table.snapshot(victim_ref.object_id)
        assert owner.state is ObjectState.ERROR
        assert isinstance(owner.error, ray.PlacementGroupLostError)
        assert owner.current_attempt.attempt_number == 0
        with observation_lock:
            assert not lease_overflow
            victim_leases = tuple(
                request for request in lease_requests
                if request.task_id == victim_ref.object_id.task_id
            )
        assert victim_leases
        assert {request.attempt_id.attempt_number for request in victim_leases} == {0}
        assert len({request.lease_id for request in victim_leases}) == 1

        # The old capability is terminal as a handle and for either original
        # bundle.  Rejection happens before Core allocates a new TaskID.
        submission_index = core._submission_index
        _remaining(deadline)
        with pytest.raises(ray.PlacementGroupLostError):
            ray.remove_placement_group(group)
        for bundle_index in range(group.bundle_count):
            _remaining(deadline)
            with pytest.raises(ray.PlacementGroupLostError):
                rejected_old_pg_handle.options(
                    placement_group=group, bundle_index=bundle_index
                ).remote(bundle_index)
        assert core._submission_index == submission_index

        # Survivor abort released the committed root reservation and its child
        # ledger.  A normal full-CPU task can run, and GCS observes full root
        # availability again before shutdown starts.
        _remaining(deadline)
        probe_ref = survivor_probe.remote()
        assert ray.get(probe_ref, timeout=_remaining(deadline)) == survivor.worker_pid
        # Complete releases the Node's authoritative ledger locally. Its
        # versioned GCS report is an independent supervisor outbox, so get()
        # does not establish that the scheduling hint has arrived. Observe
        # convergence without flushing the Node or rewriting control metadata.
        resource_wake = threading.Event()
        for _ in range(_MAX_RESOURCE_POLLS):
            live_nodes = _rpc_before(
                deadline,
                context.gcs_address, GET_NODES_HANDLER, protocol.GetNodes(),
            )
            assert isinstance(live_nodes, protocol.GetNodesReply)
            assert live_nodes.membership_epoch >= death.death_epoch
            assert len(live_nodes.nodes) == 1
            survivor_info = live_nodes.nodes[0]
            assert (survivor_info.node_id, survivor_info.node_pid,
                    survivor_info.registration_epoch, survivor_info.address) == (
                survivor.node_id, survivor.node_pid,
                expected_survivor.registration_epoch, survivor.node_address,
            )
            assert survivor_info.total_resources == expected_survivor.total_resources
            assert survivor_info.state is protocol.NodeMembershipState.ALIVE
            assert survivor_info.available_resources.fits_in(survivor_info.total_resources)
            if survivor_info.available_resources == survivor_info.total_resources:
                break
            resource_wake.wait(min(_RESOURCE_POLL_SECONDS, _remaining(deadline)))
        assert survivor_info.available_resources == survivor_info.total_resources

        # The crash barrier joined the exact victim Node process; its two
        # public endpoints must already be gone while the survivor remains live.
        assert not _pid_exists(victim.node_pid)
        assert not _pid_exists(victim.worker_pid)
        for address in (victim.node_address, victim.worker_address):
            probe_timeout = min(0.1, _remaining(deadline))
            with pytest.raises(OSError):
                with socket.create_connection(
                    address, timeout=probe_timeout,
                ):
                    pass
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if gate_connection is not None:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if remaining > 0:
                        gate_connection.settimeout(min(0.1, remaining))
                        gate_connection.sendall(_RELEASE)
                except OSError:
                    # The sole victim crash normally closes this peer.
                    pass
                finally:
                    try:
                        gate_connection.close()
                    except Exception as exc:
                        cleanup_errors.append(exc)
            if listener is not None:
                try:
                    listener.close()
                except Exception as exc:
                    cleanup_errors.append(exc)
            for ref in (probe_ref, victim_ref):
                try:
                    _close_reference(ref, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                if core is not None and original_rpc is not None:
                    core._rpc = original_rpc
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    # These probes run even after an earlier work/close error.
                    # Each successful probe connection is itself always closed.
                    surviving_pids = tuple(
                        pid for pid in sorted(managed_pids) if _pid_exists(pid)
                    )
                    surviving_children = tuple(
                        child.pid for child in mp.active_children()
                        if child.pid in managed_pids
                    )
                    open_addresses = []
                    for address in sorted(managed_addresses):
                        try:
                            with socket.create_connection(address, timeout=0.1):
                                open_addresses.append(address)
                        except OSError:
                            pass
                    assert listener is None or listener.fileno() == -1
                    assert gate_connection is None or gate_connection.fileno() == -1
                    assert not surviving_pids, surviving_pids
                    assert not surviving_children, surviving_children
                    assert not open_addresses, open_addresses
                    assert not cleanup_errors, cleanup_errors

    assert context is not None and group is not None
    assert death is not None and report is not None
    assert not lease_overflow
    survivor, victim = context.nodes
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced

    # The single victim crash remains visible as unclean; independently, the
    # survivor drains its Worker and restored root ledger without force.
    assert report.node_pids == (survivor.node_pid, victim.node_pid)
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == (True, False)
    assert report.node_forced == (False, False)
    assert report.node_finalized == (True, False)
    assert report.node_shutdown_ack_clean == (True, False)
    assert report.node_resources_clean == (True, False)
    assert report.worker_pids == (survivor.worker_pid, victim.worker_pid)
    assert report.worker_exitcodes == (0, None)
    assert report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False)
    expected_exit, recorded_crash = report.node_deaths
    assert expected_exit is not None
    assert expected_exit.reason is protocol.NodeDeathReason.EXPECTED
    assert expected_exit.exit_code == 0
    assert recorded_crash == death
