"""Bounded proof that a foreign ObjectRef fails with its owner Worker.

Run only this exact node ID through ``scripts/run_baseline.py --smoke EXACT``.  Static
bounds are one GCS, two NodeManagers and one ordinary Worker per Node, with
two 1-MiB stores. One owner Worker is killed and reaped before one replacement
is spawned: five live children at peak, six child PIDs over the expected
lifetime. The original two logical user Tasks each execute one attempt and
return only tiny INLINE values. There is no tracing or test-owned listener.

All work shares fifteen seconds after init: two local finish/collection gates
with <=256 observations each, <=128 actual death-journal/state polls, and <=64 same-request
capacity polls for one additional lease-only probe. That probe has an
independent TaskID/LeaseID, empty inputs/returns and zero resources. It never
sends Start/Push, but obtains the replacement's real endpoint and GCS PID, then
is cancelled with an exact empty-inventory ACK. It is not a third user Task or
a read-only operation. Probe/refs cleanup share three seconds in finally and
always fall through to shutdown; startup/shutdown retain the outer 30s bound.

This is intentionally different from the consumer-death ownership smoke: the
Worker whose embedded Core owns the escaped ObjectRef is the exact SIGKILL
target.  Endpoint loss is not accepted as proof.  The Driver first consumes
the GCS Worker-death journal, then requires foreign ``get`` and ``wait`` to
raise the typed owner-death error from that installed authority.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing as mp
import os
import signal
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.control import GET_WORKER_STATE_HANDLER
from miniray.core import _RPC_CALL_DEADLINE, _worker_death_reference_id
from miniray.ids import AttemptID, LeaseID, TaskID
from miniray.node import CANCEL_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER, REQUEST_LEASE_HANDLER
from miniray.ownership import ObjectCollectionState
from miniray.resources import ResourceVector
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_OWNER_RESOURCE = "worker_owner_death_owner"
_PRODUCER_RESOURCE = "worker_owner_death_producer"
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_LOCAL_OBSERVATIONS = 256
_MAX_DEATH_POLLS = 128
_MAX_PROBE_POLLS = 64


@ray.remote(num_cpus=1, resources={_PRODUCER_RESOURCE: 1})
def _owned_child(value: int) -> tuple[str, int]:
    return "worker-owner-death", value + 1


@ray.remote(num_cpus=0, resources={_OWNER_RESOURCE: 1})
def _return_worker_owned_ref(value: int) -> object:
    return _owned_child.remote(value)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Worker-owner death work exceeded its deadline")
    return remaining


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.25, remaining),
        request_timeout=min(1.0, remaining), deadline=deadline,
    )


def _close_reference(reference, deadline):
    if reference is None:
        return
    # Public close observes the original local receipt, including a previous
    # timed-out close. It does not claim a remote Release/GC ACK occurred.
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _wait_local(core, ready, deadline):
    with core._completion:
        for _ in range(_MAX_LOCAL_OBSERVATIONS):
            if ready():
                return
            core._completion.wait(min(0.1, _remaining(deadline)))
    raise TimeoutError("Worker-owner local finish/collection did not converge")


def test_confirmed_worker_owner_death_fences_foreign_get_wait_and_release() -> None:
    context = None
    report = None
    outer = foreign = None
    core = None
    owner_node = producer_node = None
    death_state = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    cleanup_errors = []
    wake = threading.Event()
    replacement_pid = None
    probe_request = probe_cancel = probe_grant = None
    probe_attempted = probe_clean = False

    def worker_state(worker_id, deadline):
        reply = _query(
            context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(worker_id), deadline,
        )
        assert type(reply) is protocol.GetWorkerStateReply and reply.found
        reply = replace(reply)
        assert reply.worker_id == worker_id and reply.incarnation is not None
        assert reply.incarnation.node_id == owner_node.node_id
        assert reply.incarnation.node_pid == owner_node.node_pid
        managed_pids.add(reply.incarnation.worker_pid)
        return reply

    def remember_probe_grant(grant):
        nonlocal probe_grant
        grant = protocol.revalidate_worker_lease_grant(grant)
        assert (grant.lease_id, grant.task_id, grant.attempt_id, grant.node_id) == (
            probe_request.lease_id, probe_request.task_id, probe_request.attempt_id, owner_node.node_id,
        )
        assert grant.dependencies == () and grant.scheduling_key is None
        managed_addresses.add(grant.worker_address)
        assert grant.worker_id != owner_node.worker_id
        assert probe_grant is None or probe_grant == grant
        probe_grant = grant

    def cancel_probe(deadline):
        nonlocal probe_clean, replacement_pid
        if not probe_attempted or probe_clean:
            return
        # Saved before first send: Cancel fences even an unknown Grant ACK.
        reply = _query(owner_node.node_address, CANCEL_LEASE_HANDLER, probe_cancel, deadline)
        assert type(reply) is protocol.CancelWorkerLeaseReply
        reply = replace(reply)
        assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
                reply.requester_worker_id, reply.scheduling_key) == (
            probe_request.lease_id, probe_request.task_id, probe_request.attempt_id,
            probe_request.requester_node_id, core.worker_id, None,
        )
        assert reply.accepted and reply.cancelled and reply.state is protocol.LeaseExecutionState.ABANDONED
        if reply.retired_grant is not None:
            remember_probe_grant(reply.retired_grant)
        elif probe_grant is not None:
            raise AssertionError("cancel lost the known probe grant")
        inventory = reply.dependency_inventory
        assert inventory == protocol.LeaseDependencyInventory(probe_request, owner_node.node_id, ())
        request = protocol.AckLeaseDependencyCustody(core.worker_id, inventory)
        acknowledgement = _query(
            owner_node.node_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER, request, deadline,
        )
        assert type(acknowledgement) is protocol.AckLeaseDependencyCustodyReply
        assert acknowledgement.accepted and acknowledgement.request == request
        probe_clean = True
        if probe_grant is not None and replacement_pid is None:
            # Failure-finally may first learn the actual Worker via Cancel.
            replacement_pid = worker_state(probe_grant.worker_id, deadline).incarnation.worker_pid

    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _OWNER_RESOURCE: 1},
                {"CPU": 1, _PRODUCER_RESOURCE: 1},
            ),
            num_workers_per_node=1, inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        owner_node, producer_node = context.nodes
        assert len(owner_node.worker_ids) == len(producer_node.worker_ids) == 1
        assert context.trace_address is None
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5 and len(managed_addresses) == 6
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        outer = _return_worker_owned_ref.remote(41)
        foreign = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(foreign, ray.ObjectRef)
        assert foreign.owner_worker_id == owner_node.worker_id
        assert foreign.owner_address == owner_node.worker_address
        assert foreign.borrower_token is not None

        # Retire the container edge while its owner can still acknowledge it.
        # The independent borrower below is then the sole Driver-side lifetime
        # obligation and proves the child remains usable on its own.
        _wait_local(
            core, lambda: outer.object_id not in core._task_finish_barriers
            and not core._protocol_unresolved and core._accepted_task_count == 0, deadline,
        )
        assert outer.object_id.task_id in core._finished_tasks
        assert core.owner_table.snapshot(outer.object_id).current_attempt.attempt_number == 0
        _close_reference(outer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _wait_local(
            core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, deadline,
        )
        assert outer.closed and outer.object_id not in core._object_gc_obligations
        assert ray.get(foreign, timeout=_remaining(deadline)) == (
            "worker-owner-death",
            42,
        )

        obligation_key = (
            foreign.owner_worker_id,
            foreign.object_id,
            core.worker_id,
            foreign.borrower_token,
        )
        with core._state_lock:
            assert obligation_key in core._borrowed_release_obligations

        # The target is resolved from the immutable startup identity and
        # cross-checked against the escaped ref before this exact child PID is
        # signalled.  The NodeManager itself remains alive to reap, report, and
        # replace the Worker incarnation.
        before_death = worker_state(owner_node.worker_id, deadline)
        assert before_death.state is protocol.WorkerMembershipState.ALIVE and before_death.death is None
        assert before_death.incarnation.worker_pid == owner_node.worker_pid
        assert owner_node.worker_pid > 0
        assert _pid_exists(owner_node.worker_pid)
        os.kill(owner_node.worker_pid, signal.SIGKILL)

        for _ in range(_MAX_DEATH_POLLS):
            _remaining(deadline)
            token = _RPC_CALL_DEADLINE.set(min(deadline, time.monotonic() + 1.0))
            try:
                core._sync_worker_deaths()
            finally:
                _RPC_CALL_DEADLINE.reset(token)
            candidate = worker_state(owner_node.worker_id, deadline)
            installed = core.owner_table.dead_worker_record(
                owner_node.worker_id
            )
            if (
                isinstance(candidate, protocol.GetWorkerStateReply)
                and candidate.found
                and candidate.state is protocol.WorkerMembershipState.DEAD
                and candidate.death is not None
                and installed is not None
                and installed.death_id == _worker_death_reference_id(candidate.death)
                and not _pid_exists(owner_node.worker_pid)
            ):
                death_state = candidate
                break
            wake.wait(min(0.05, _remaining(deadline)))
        else:
            raise TimeoutError("Worker-owner death journal did not converge")

        assert isinstance(death_state, protocol.GetWorkerStateReply)
        assert death_state.worker_id == owner_node.worker_id
        assert death_state.state is protocol.WorkerMembershipState.DEAD
        death = death_state.death
        assert death is not None
        assert death.incarnation == before_death.incarnation
        assert death.worker_id == owner_node.worker_id
        assert death.worker_pid == owner_node.worker_pid
        assert death.node_id == owner_node.node_id
        assert death.node_pid == owner_node.node_pid
        assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT
        assert death.exit_code == -signal.SIGKILL
        installed = core.owner_table.dead_worker_record(owner_node.worker_id)
        assert installed is not None
        assert installed.death_id == _worker_death_reference_id(death)
        assert not _pid_exists(owner_node.worker_pid)

        # These public operations consult the locally installed GCS proof
        # before attempting the obsolete physical route.
        with pytest.raises(ray.OwnerDiedError, match="confirmed dead"):
            ray.get(foreign, timeout=_remaining(deadline))
        with pytest.raises(ray.OwnerDiedError, match="confirmed dead"):
            ray.wait([foreign], num_returns=1, timeout=_remaining(deadline))

        # Death authority discharges an unacknowledgeable remote Release; a
        # later explicit close is idempotent and cannot recreate the work.
        with core._state_lock:
            assert obligation_key not in core._borrowed_release_obligations
        _close_reference(foreign, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert foreign.closed
        with core._state_lock:
            assert obligation_key not in core._borrowed_release_obligations

        # There is no read-only ordinary-Worker endpoint directory. This one
        # independent lease observes the replacement without executing another
        # user Task, changing the foreign ref's owner or starting a new fault.
        probe_task = TaskID.random()
        assert probe_task not in (outer.object_id.task_id, foreign.object_id.task_id)
        probe_request = protocol.RequestWorkerLease(
            LeaseID.random(), probe_task, AttemptID(probe_task, 0), ResourceVector.empty(),
            core.node_id, core.worker_id, preferred_node_id=owner_node.node_id,
            target_node_id=owner_node.node_id, dependencies=(), return_ids=(),
        )
        probe_cancel = protocol.CancelWorkerLease(
            probe_request.lease_id, probe_task, probe_request.attempt_id,
            core.node_id, core.worker_id, lease_request=probe_request,
        )
        for _ in range(_MAX_PROBE_POLLS):
            probe_attempted = True
            reply = _query(owner_node.node_address, REQUEST_LEASE_HANDLER, probe_request, deadline)
            assert (reply.lease_id, reply.task_id, reply.attempt_id) == (
                probe_request.lease_id, probe_task, probe_request.attempt_id,
            )
            if type(reply) is protocol.GrantWorkerLease:
                remember_probe_grant(reply)
                break
            assert type(reply) is protocol.RejectWorkerLease
            assert reply.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
            assert reply.scheduling_key is None
            wake.wait(min(0.05, _remaining(deadline)))
        else:
            raise TimeoutError("replacement Worker did not grant the fixed probe lease")
        replacement = worker_state(probe_grant.worker_id, deadline)
        assert replacement.state is protocol.WorkerMembershipState.ALIVE and replacement.death is None
        assert replacement.incarnation.node_registration_epoch == death.node_registration_epoch
        replacement_pid = replacement.incarnation.worker_pid
        assert replacement_pid != owner_node.worker_pid and _pid_exists(replacement_pid)
        assert _pid_exists(producer_node.worker_pid) and not _pid_exists(owner_node.worker_pid)
        cancel_probe(deadline)
        assert probe_clean
        cancelled = _query(
            owner_node.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER,
            protocol.GetWorkerLeaseOutcome(
                probe_request.lease_id, probe_task, probe_request.attempt_id,
                probe_grant.worker_id, core.worker_id, (),
            ), deadline,
        )
        assert type(cancelled) is protocol.GetWorkerLeaseOutcomeReply and cancelled.found
        cancelled = replace(cancelled)
        assert (cancelled.lease_id, cancelled.task_id, cancelled.attempt_id, cancelled.executor_worker_id,
                cancelled.owner_worker_id, cancelled.object_ids, cancelled.node_id) == (
            probe_request.lease_id, probe_task, probe_request.attempt_id,
            probe_grant.worker_id, core.worker_id, (), owner_node.node_id,
        )
        assert cancelled.state is protocol.LeaseExecutionState.ABANDONED and cancelled.worker_alive
        assert cancelled.completion_status is None and not cancelled.cleanup_pending
        assert cancelled.descriptors == cancelled.orphan_descriptors == ()
        assert cancelled.output_publication is None and cancelled.output_completion is None
        assert foreign.owner_worker_id == owner_node.worker_id
        assert len(managed_pids) == 6 and len(managed_addresses) <= 7
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            try:
                cancel_probe(cleanup_deadline)
            except Exception as exc:
                cleanup_errors.append(exc)
            for reference in (foreign, outer):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                if context is not None and core is not None and core.owner_address is not None:
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
                assert not cleanup_errors, cleanup_errors
                assert not ray.is_initialized()

    assert context is not None
    assert owner_node is not None and producer_node is not None
    assert report is not None
    assert probe_attempted and probe_clean and replacement_pid is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
    assert len(report.worker_pids) == 2
    assert report.worker_pids[0] == replacement_pid
    assert replacement_pid != owner_node.worker_pid
    assert report.worker_pids[1] == producer_node.worker_pid
