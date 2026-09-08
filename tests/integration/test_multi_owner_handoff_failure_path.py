"""Bounded real multi-owner handoff when one owner Worker dies pre-Push.

Five startup children (GCS, two Nodes, one Worker each), one exact Worker kill
and one replacement. Three user tasks: two node-pinned factories each create
one 8 KiB Worker-owned put, then one never-executed consumer targets the second
Worker. Its real two-dependency grant is held at the Driver for at most eight
seconds, after source pin/pull/seal and before foreign location reporting.

The first owner alone dies; its Node and the healthy owner/executor stay alive.
Normal cancellation unpins the grant, the second report still reaches its owner,
and GCS owner-wide cleanup removes the dead owner's copies. No fake replies or
death facts, test threads/listeners, retries, Actor or trace. Two 1 MiB stores.
One additional empty-dependency, never-Pushed probe lease identifies the already
created replacement and is exactly cancelled, including in finally. All work
shares an 18-second deadline; the external runner caps the process tree at 30s.
"""

from __future__ import annotations

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
from miniray.control import (
    GET_NODES_HANDLER, GET_NODE_STATE_HANDLER, GET_WORKER_STATE_HANDLER,
    PROGRESS_PUBLICATION_OWNER_DEATH_HANDLER,
)
from miniray.core import _worker_death_reference_id
from miniray.errors import OwnerDiedError
from miniray.ids import AttemptID, LeaseID, TaskID
from miniray.node import (
    CANCEL_LEASE_HANDLER, GET_OBJECT_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
    REQUEST_LEASE_HANDLER, SHUTDOWN_STATUS_HANDLER,
)
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from miniray.transport import request as rpc_request
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_output_owner_death_path import _close_reference, _pid_exists


pytestmark = pytest.mark.multiprocess_smoke

_OWNER_A = "multi_owner_failure_a"
_OWNER_B = "multi_owner_failure_b"
_PAYLOAD_A = b"A" * (8 * 1024)
_PAYLOAD_B = b"B" * (8 * 1024)
_GATE_SECONDS = 8.0
_WORK_SECONDS = 18.0


@ray.remote(resources={_OWNER_A: 1}, max_retries=0)
def _create_owner_a_ref():
    return ray.put(_PAYLOAD_A)


@ray.remote(resources={_OWNER_B: 1}, max_retries=0)
def _create_owner_b_ref():
    return ray.put(_PAYLOAD_B)


@ray.remote(resources={_OWNER_B: 1}, max_retries=0)
def _consumer_must_not_run(first, second):
    raise AssertionError("owner-death handoff admitted consumer user code")


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "multi-owner acceptance exceeded its deadline"
    return remaining


def _poll(predicate, deadline, detail):
    wake = threading.Event()
    while True:
        _remaining(deadline)
        value = predicate()
        if value:
            return value
        assert time.monotonic() < deadline, detail
        wake.wait(min(0.01, _remaining(deadline)))


def _wait(core, predicate, deadline):
    with core._completion:
        while not predicate():
            core._completion.wait(_remaining(deadline))


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining / 2),
        request_timeout=min(2.0, remaining / 2), deadline=deadline,
    )


def _worker_state(context, worker, deadline):
    reply = _rpc(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(worker), deadline)
    assert type(reply) is protocol.GetWorkerStateReply and reply.found and reply.worker_id == worker
    return reply


def _node_state(context, node, deadline):
    reply = _rpc(context.gcs_address, GET_NODE_STATE_HANDLER, protocol.GetNodeState(node.node_id), deadline)
    assert type(reply) is protocol.GetNodeStateReply and reply.found and reply.node_id == node.node_id
    assert reply.state is protocol.NodeMembershipState.ALIVE and reply.node_pid == node.node_pid
    assert reply.death is None
    return reply


def _replica(node, object_id, deadline):
    reply = _rpc(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _owned_state(core, reference, deadline):
    reply = _rpc(reference.owner_address, "get_owned_object", protocol.GetOwnedObject(
        reference.object_id, reference.owner_worker_id, core.worker_id, reference.borrower_token,
    ), deadline)
    assert type(reply) is protocol.GetOwnedObjectReply and reply.accepted
    assert reply.object_id == reference.object_id and reply.owner_worker_id == reference.owner_worker_id
    return reply


def _assert_cancelled(reply, request):
    assert type(reply) is protocol.CancelWorkerLeaseReply
    assert reply.accepted and reply.cancelled and reply.state is protocol.LeaseExecutionState.ABANDONED
    assert (reply.lease_id, reply.task_id, reply.attempt_id,
            reply.requester_node_id, reply.requester_worker_id, reply.scheduling_key) == (
        request.lease_id, request.task_id, request.attempt_id,
        request.requester_node_id, request.requester_worker_id, request.scheduling_key,
    )


def test_dead_first_owner_cancels_grant_but_hands_off_second_foreign_replica():
    context = runtime = core = node_a = node_b = None
    outer_a = outer_b = ref_a = ref_b = consumer = None
    original_rpc = original_borrow = original_push = None
    probe = probe_grant = probe_cancel = probe_address = replacement_state = None
    probe_cancelled = False
    death = report = None
    pids, addresses = set(), set()
    entered, release, expired = threading.Event(), threading.Event(), threading.Event()
    overflow = threading.Event()
    observation_lock = threading.Lock()
    grant_gate, observations, pushes = [], [], []
    close_errors = []
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _OWNER_A: 1}, {"CPU": 1, _OWNER_B: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        node_a, node_b = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address,
                          node_a.node_address, node_a.worker_address, node_b.node_address, node_b.worker_address))
        assert len(pids) == 5 and len(addresses) == 6 and os.getpid() not in pids
        assert context.trace_address is None
        outer_a = _create_owner_a_ref.remote()
        ref_a = ray.get(outer_a, timeout=_remaining(deadline))
        outer_b = _create_owner_b_ref.remote()
        ref_b = ray.get(outer_b, timeout=_remaining(deadline))
        for reference, node in ((ref_a, node_a), (ref_b, node_b)):
            assert isinstance(reference, ray.ObjectRef) and reference.borrower_token is not None
            assert reference.owner_worker_id == node.worker_id and reference.owner_address == node.worker_address
            assert reference.owner_worker_id != core.worker_id
        _wait(core, lambda: all(item.object_id not in core._task_finish_barriers for item in (outer_a, outer_b)), deadline)
        # Driver's independent borrower handles survive these containers. No
        # Driver-owned graph edge must contact the dead owner during final GC.
        _close_local(outer_a, deadline)
        _close_local(outer_b, deadline)
        _wait(core, lambda: all(core.owner_table.collection_state(item.object_id) is ObjectCollectionState.COLLECTED
                               for item in (outer_a, outer_b)), deadline)
        before_a, before_b = _owned_state(core, ref_a, deadline), _owned_state(core, ref_b, deadline)
        assert before_a.state is before_b.state is protocol.OwnedObjectState.READY_STORED
        assert before_a.descriptor.node_id == node_a.node_id and before_b.descriptor.node_id == node_b.node_id
        assert before_a.current_attempt.attempt_number == before_b.current_attempt.attempt_number == 0
        node_a_before = _node_state(context, node_a, deadline)
        owner_a_before = _worker_state(context, node_a.worker_id, deadline)
        owner_b_before = _worker_state(context, node_b.worker_id, deadline)
        assert owner_a_before.state is owner_b_before.state is protocol.WorkerMembershipState.ALIVE
        assert owner_a_before.incarnation.worker_pid == node_a.worker_pid
        assert owner_a_before.incarnation.node_pid == node_a.node_pid
        assert owner_a_before.incarnation.node_registration_epoch == node_a_before.registration_epoch

        original_rpc, original_borrow, original_push = core._rpc, core._borrow_rpc, core._push_task_rpc

        def remember(kind, request, reply):
            with observation_lock:
                if len(observations) < 64:
                    observations.append((kind, request, reply))
                else:
                    overflow.set()

        def observe_rpc(address, handler, request):
            reply = original_rpc(address, handler, request)
            if handler in (REQUEST_LEASE_HANDLER, CANCEL_LEASE_HANDLER):
                remember(handler, request, reply)
            hold = False
            if handler == REQUEST_LEASE_HANDLER and type(reply) is protocol.GrantWorkerLease and len(reply.dependencies) == 2:
                with observation_lock:
                    if not grant_gate:
                        assert reply.node_id == node_b.node_id and reply.worker_id == node_b.worker_id
                        grant_gate.append((request, reply, time.monotonic() + _GATE_SECONDS))
                        hold = True
            if hold:
                entered.set()
                if not release.wait(_GATE_SECONDS):
                    expired.set()
            return reply

        def observe_borrow(address, handler, request):
            reply = original_borrow(address, handler, request)
            if handler in ("report_retained_object_location", "release_owned_object_for_task"):
                remember(handler, request, reply)
            return reply

        def observe_push(address, handler, request):
            with observation_lock:
                if len(pushes) < 2:
                    pushes.append(request)
                else:
                    overflow.set()
            return original_push(address, handler, request)

        core._rpc, core._borrow_rpc, core._push_task_rpc = observe_rpc, observe_borrow, observe_push
        consumer = _consumer_must_not_run.remote(ref_a, ref_b)
        assert entered.wait(min(_GATE_SECONDS, _remaining(deadline)))
        with observation_lock:
            (lease_request, grant, gate_deadline), = grant_gate
        gate_deadline = min(gate_deadline, deadline)
        assert grant.task_id == consumer.object_id.task_id and grant.attempt_id.attempt_number == 0
        assert tuple(item.object_id for item in grant.dependencies) == (ref_a.object_id, ref_b.object_id)
        assert tuple(item.owner_worker_id for item in grant.dependencies) == (node_a.worker_id, node_b.worker_id)
        assert all(item.node_id == node_b.node_id for item in grant.dependencies)
        for descriptor in grant.dependencies:
            physical = _replica(node_b, descriptor.object_id, gate_deadline)
            assert physical.found and physical.sealed and physical.producer_attempt_id == descriptor.producer_attempt_id
            assert physical.owner_worker_id == descriptor.owner_worker_id and physical.checksum == descriptor.checksum
        with observation_lock:
            assert not pushes and not any(kind == "report_retained_object_location" for kind, _, _ in observations)
        assert not expired.is_set()
        # Resolve the one allowed kill target by its immutable managed identity,
        # never by an RPC address or an inferred unreachable process.
        checked = _worker_state(context, ref_a.owner_worker_id, gate_deadline)
        assert checked.state is protocol.WorkerMembershipState.ALIVE and checked.death is None
        assert checked.incarnation == owner_a_before.incarnation and _pid_exists(node_a.worker_pid)
        assert checked.incarnation.worker_id != grant.worker_id
        os.kill(checked.incarnation.worker_pid, signal.SIGKILL)

        def owner_death_installed():
            candidate = _worker_state(context, ref_a.owner_worker_id, gate_deadline)
            core._sync_worker_deaths()
            installed = core.owner_table.dead_worker_record(ref_a.owner_worker_id)
            if candidate.state is protocol.WorkerMembershipState.DEAD and installed is not None:
                assert candidate.death.incarnation == checked.incarnation
                assert installed.death_id == _worker_death_reference_id(candidate.death)
                return candidate.death
            return None

        death = _poll(owner_death_installed, gate_deadline, "owner Worker death was not installed")
        assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT and death.exit_code == -signal.SIGKILL
        assert _pid_exists(node_a.node_pid) and _pid_exists(node_b.worker_pid)
        release.set()
        assert not expired.is_set()
        with pytest.raises(OwnerDiedError):
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, deadline)
        failed = core.owner_table.snapshot(consumer.object_id)
        assert failed.state is ObjectState.ERROR and isinstance(failed.error, OwnerDiedError)
        assert failed.current_attempt == grant.attempt_id
        task_record = core._recovery.task_record(consumer.object_id.task_id)
        assert task_record.retries_started == 0 and task_record.current_attempt == grant.attempt_id
        assert core._recovery.active_recovery(consumer.object_id.task_id) is None
        with observation_lock:
            observed = tuple(observations)
            assert not pushes
        cancels = tuple((request, reply) for kind, request, reply in observed if kind == CANCEL_LEASE_HANDLER)
        assert len(cancels) == 1
        cancel_request, cancel_reply = cancels[0]
        assert cancel_request.lease_id == grant.lease_id and cancel_request.task_id == grant.task_id
        assert cancel_request.attempt_id == grant.attempt_id
        assert cancel_request.requester_node_id == lease_request.requester_node_id
        assert cancel_request.requester_worker_id == lease_request.requester_worker_id
        assert cancel_request.scheduling_key == lease_request.scheduling_key
        _assert_cancelled(cancel_reply, cancel_request)
        assert cancel_reply.released
        reports = tuple((request, reply) for kind, request, reply in observed if kind == "report_retained_object_location")
        assert len(reports) == 1
        location_request, location_reply = reports[0]
        assert location_request.object_id == ref_b.object_id and location_request.owner_worker_id == node_b.worker_id
        assert location_reply.accepted and location_reply.custody_transferred
        assert location_reply.descriptor == grant.dependencies[1]
        assert observed.index((CANCEL_LEASE_HANDLER, cancel_request, cancel_reply)) < observed.index(("report_retained_object_location", location_request, location_reply))
        outcome_request = protocol.GetWorkerLeaseOutcome(
            grant.lease_id, grant.task_id, grant.attempt_id, grant.worker_id,
            core.worker_id, (consumer.object_id,),
        )
        outcome = _rpc(node_b.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline)
        assert type(outcome) is protocol.GetWorkerLeaseOutcomeReply and outcome.found and outcome.worker_alive
        assert outcome.state is protocol.LeaseExecutionState.ABANDONED and outcome.completion_status is None
        assert not core._node_is_dead(node_a.node_id) and not core._node_is_dead(node_b.node_id)
        node_a_after = _node_state(context, node_a, deadline)
        assert node_a_after.node_pid == node_a_before.node_pid
        assert node_a_after.registration_epoch == node_a_before.registration_epoch
        owner_b_after = _worker_state(context, node_b.worker_id, deadline)
        assert owner_b_after.state is protocol.WorkerMembershipState.ALIVE
        assert owner_b_after.incarnation == owner_b_before.incarnation and owner_b_after.death is None
        healthy = _owned_state(core, ref_b, deadline)
        assert healthy.state is protocol.OwnedObjectState.READY_STORED and healthy.descriptor == before_b.descriptor
        assert ray.get(ref_b, timeout=_remaining(deadline)) == _PAYLOAD_B
        with pytest.raises(OwnerDiedError):
            ray.get(ref_a, timeout=_remaining(deadline))

        def target_pins_and_resources_released():
            status = _rpc(node_b.node_address, SHUTDOWN_STATUS_HANDLER,
                          protocol.ShutdownStatusRequest("inspect-cancel-only"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
            return status.resources_clean

        _poll(target_pins_and_resources_released, deadline, "cancelled grant left target pins or resources")

        def dead_owner_copies_absent():
            return all(not _replica(node, ref_a.object_id, deadline).found for node in (node_a, node_b))

        _poll(dead_owner_copies_absent, deadline, "owner-wide sweep left an old owner replica")
        # This existing progress operation is owner-scoped, not BeginDrain: it
        # confirms the GCS outbox barrier without closing healthy admission.
        def owner_cleanup_complete():
            reply = _rpc(context.gcs_address, PROGRESS_PUBLICATION_OWNER_DEATH_HANDLER,
                         protocol.ProgressPublicationOwnerDeath(node_a.worker_id), deadline)
            assert type(reply) is protocol.ProgressPublicationOwnerDeathReply and reply.owner_worker_id == node_a.worker_id
            return reply.clean

        _poll(owner_cleanup_complete, deadline, "GCS owner cleanup did not converge")
        # Consumer output GC is the boundary for its normal foreign lineage.
        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: core._foreign_lineage_registry.snapshot(consumer.object_id.task_id) is None,
              deadline, "consumer foreign lineage did not release")
        _close_reference(ref_a, deadline)
        _close_reference(ref_b, deadline)
        _poll(lambda: not _replica(node_b, ref_b.object_id, deadline).found, deadline, "healthy put did not physically collect")

        def replacement_ready():
            reply = _rpc(node_a.node_address, SHUTDOWN_STATUS_HANDLER,
                         protocol.ShutdownStatusRequest("inspect-replacement-only"), deadline)
            assert type(reply) is protocol.ShutdownStatus and not reply.shutdown_requested and not reply.finalized
            return reply.child_pid if reply.child_pid not in (None, node_a.worker_pid) else None

        replacement_pid = _poll(replacement_ready, deadline, "owner Node did not replace its ordinary Worker")
        pids.add(replacement_pid)
        nodes = _rpc(context.gcs_address, GET_NODES_HANDLER, protocol.GetNodes(), deadline)
        assert type(nodes) is protocol.GetNodesReply
        current_a = next(item for item in nodes.nodes if item.node_id == node_a.node_id)
        assert current_a.node_pid == node_a.node_pid and current_a.registration_epoch == node_a_before.registration_epoch
        probe_address = current_a.address
        probe_task = TaskID.derive(core.job_id, core.driver_task_id, 9001)
        probe = protocol.RequestWorkerLease(
            LeaseID.random(), probe_task, AttemptID(probe_task, 0), ResourceVector({"CPU": 1}),
            core.node_id, core.worker_id, target_node_id=node_a.node_id, dependencies=(),
        )
        probe_cancel = protocol.CancelWorkerLease(
            probe.lease_id, probe.task_id, probe.attempt_id, probe.requester_node_id, probe.requester_worker_id,
        )

        def acquire_probe():
            candidate = _rpc(probe_address, REQUEST_LEASE_HANDLER, probe, deadline)
            assert type(candidate) in (protocol.RejectWorkerLease, protocol.GrantWorkerLease)
            assert (candidate.lease_id, candidate.task_id, candidate.attempt_id,
                    candidate.scheduling_key, candidate.target_execution) == (
                probe.lease_id, probe.task_id, probe.attempt_id, probe.scheduling_key, probe.target_execution,
            )
            if type(candidate) is protocol.RejectWorkerLease:
                assert candidate.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
                return None
            assert type(candidate) is protocol.GrantWorkerLease
            assert candidate.node_id == node_a.node_id and not candidate.dependencies
            return candidate

        probe_grant = _poll(acquire_probe, deadline, "replacement probe did not acquire the existing Worker")
        addresses.add(probe_grant.worker_address)
        replacement_state = _worker_state(context, probe_grant.worker_id, deadline)
        assert replacement_state.state is protocol.WorkerMembershipState.ALIVE
        assert replacement_state.incarnation.worker_pid == replacement_pid
        assert replacement_state.incarnation.node_id == node_a.node_id
        assert replacement_state.incarnation.node_registration_epoch == node_a_before.registration_epoch
        assert replacement_state.worker_id != ref_a.owner_worker_id
        assert _worker_state(context, ref_a.owner_worker_id, deadline).death == death
        probe_reply = _rpc(probe_address, CANCEL_LEASE_HANDLER, probe_cancel, deadline)
        _assert_cancelled(probe_reply, probe_cancel)
        assert probe_reply.released
        probe_cancelled = True
        assert len(pids) == 6 and not expired.is_set() and not overflow.is_set()
        assert not core._protocol_unresolved and not core._task_finish_barriers
    finally:
        release.set()
        if core is not None and original_rpc is not None:
            core._rpc, core._borrow_rpc, core._push_task_rpc = original_rpc, original_borrow, original_push
        cleanup_deadline = time.monotonic() + 3.0
        try:
            if probe_cancel is not None and not probe_cancelled:
                try:
                    result = _rpc(probe_address, CANCEL_LEASE_HANDLER, probe_cancel, cleanup_deadline)
                    _assert_cancelled(result, probe_cancel)
                except Exception as exc:
                    close_errors.append(exc)
            for reference in (consumer, ref_a, ref_b, outer_a, outer_b):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not close_errors and context is not None and report is not None and death is not None
    assert report.core_stopped and report.gcs_clean and report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean and not report.forced
    assert report.worker_pids == (replacement_state.incarnation.worker_pid, node_b.worker_pid)
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert not ray.is_initialized() and len(pids) == 6
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
