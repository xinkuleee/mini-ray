"""Bounded real custody handoff after a submitting Worker exits pre-Push.

Five startup children: GCS, two Nodes and one ordinary Worker per Node; one
replacement of A's Worker. A Driver-owned 8 KiB put remains live on A. A parent
on A receives that ObjectRef inside a list, submits one consumer pinned to B,
and exits itself with code 17 immediately after its embedded Core receives the
consumer's real Grant. It neither returns that Grant to Core nor Pushes user
code to B. No arbitrary PID kill, test listener/thread or fake protocol reply.

B's existing supervisor observes the GCS-confirmed submitter death, abandons
the GRANTED lease and offers the actual sealed input back to its live Driver
owner. The observer calls the real owner handler and records its custody-only
reply; exact Node outcome and physical GC establish the rest of the path.

Two logical tasks, one put, and one empty never-Pushed probe solely to identify
the existing replacement endpoint. Probe identity is cancelled in finally if
necessary. Two 1 MiB stores, no Actor/PG/tracing. Work after init has an 18 s
deadline; final reference/probe cleanup shares 3 s and always invokes shutdown.
Run only the exact test ID through the 30 s execution-deadline process-tree
runner, which adds bounded TERM/KILL/reap grace on timeout.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.control import GET_NODE_STATE_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.core import _worker_death_reference_id
from miniray.errors import WorkerDiedError
from miniray.ids import AttemptID, LeaseID, TaskID
from miniray.node import CANCEL_LEASE_HANDLER, GET_OBJECT_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER, REQUEST_LEASE_HANDLER, SHUTDOWN_STATUS_HANDLER
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from miniray.runtime_binding import current_core_worker
from miniray.transport import request as rpc_request
from tests.support._legacy_reference_cleanup import _close_local
from tests.integration.test_output_owner_death_path import _pid_exists


pytestmark = pytest.mark.multiprocess_smoke

_PARENT = "abandoned_submitter_node"
_CHILD = "abandoned_consumer_node"
_PAYLOAD = b"D" * (8 * 1024)
_EXIT_CODE = 17
_WORK_SECONDS = 18.0


@ray.remote(num_cpus=1, resources={_CHILD: 1}, max_retries=0)
def _consumer_must_not_run(value):
    raise AssertionError("abandoned child lease entered consumer user code")


@ray.remote(num_cpus=1, resources={_PARENT: 1}, max_retries=0)
def _exit_after_child_grant(container, target_node_id, target_worker_id, expected_submitter, expected_pid):
    core = current_core_worker()
    assert core is not None and core.worker_id == expected_submitter and os.getpid() == expected_pid
    assert len(container) == 1 and isinstance(container[0], ray.ObjectRef)
    reference = container[0]
    assert reference.owner_worker_id != core.worker_id and reference.borrower_token is not None
    original = core._rpc

    def exit_before_returning_grant(address, handler, request):
        reply = original(address, handler, request)
        if handler == REQUEST_LEASE_HANDLER and type(reply) is protocol.GrantWorkerLease:
            assert reply.node_id == target_node_id and reply.worker_id == target_worker_id
            assert request.requester_worker_id == expected_submitter
            assert len(reply.dependencies) == len(request.dependency_owner_routes) == 1
            assert reply.dependencies[0].object_id == reference.object_id
            assert reply.dependencies[0].owner_worker_id == reference.owner_worker_id
            assert reply.dependencies[0].node_id == target_node_id
            route = request.dependency_owner_routes[0]
            assert route.owner_address == reference.owner_address and route.owner_worker_id == reference.owner_worker_id
            assert route.hold.kind is protocol.TaskReferenceHoldKind.RETAINED
            assert route.hold.submitting_worker_id == expected_submitter and route.hold.task_id == request.task_id
            os._exit(_EXIT_CODE)
        return reply

    core._rpc = exit_before_returning_grant
    try:
        child = _consumer_must_not_run.remote(reference)
        ray.get(child, timeout=8.0)
        raise AssertionError("parent survived its granted-child exit boundary")
    finally:
        # os._exit intentionally skips finally. This restoration applies only
        # if an assertion/ordinary failure happens before that exact boundary.
        core._rpc = original


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "abandoned dependency acceptance exceeded its deadline"
    return remaining


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining / 2),
                       request_timeout=min(2.0, remaining / 2), deadline=deadline)


def _poll(predicate, deadline, detail):
    wake = threading.Event()
    while True:
        _remaining(deadline)
        result = predicate()
        if result:
            return result
        assert time.monotonic() < deadline, detail
        wake.wait(min(0.01, _remaining(deadline)))


def _wait(core, predicate, deadline):
    with core._completion:
        while not predicate():
            core._completion.wait(_remaining(deadline))


def _worker(context, worker_id, deadline):
    reply = _rpc(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(worker_id), deadline)
    assert type(reply) is protocol.GetWorkerStateReply and reply.found and reply.worker_id == worker_id
    return reply


def _node(context, node, deadline):
    reply = _rpc(context.gcs_address, GET_NODE_STATE_HANDLER, protocol.GetNodeState(node.node_id), deadline)
    assert type(reply) is protocol.GetNodeStateReply and reply.found and reply.node_id == node.node_id
    assert reply.node_pid == node.node_pid and reply.state is protocol.NodeMembershipState.ALIVE and reply.death is None
    return reply


def _physical(node, object_id, deadline):
    reply = _rpc(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _status(node, deadline):
    request = protocol.ShutdownStatusRequest("inspect-abandoned-dependency-only")
    reply = _rpc(node.node_address, SHUTDOWN_STATUS_HANDLER, request, deadline)
    assert type(reply) is protocol.ShutdownStatus and not reply.shutdown_requested and not reply.finalized
    return reply


def _assert_cancelled(reply, request):
    assert type(reply) is protocol.CancelWorkerLeaseReply and reply.accepted and reply.cancelled
    assert reply.state is protocol.LeaseExecutionState.ABANDONED
    assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
            reply.requester_worker_id, reply.scheduling_key) == (
        request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
        request.requester_worker_id, request.scheduling_key,
    )


def test_dead_submitter_hands_granted_input_back_to_live_owner_without_child_execution():
    context = runtime = core = report = node_a = node_b = None
    source = parent = None
    original_offer = original_rpc = None
    replacement = death = None
    probe = probe_cancel = probe_address = None
    probe_cancelled = False
    pids, addresses = set(), set()
    observation_lock = threading.Lock()
    offers, parent_grants = [], []
    overflow = threading.Event()
    cleanup_errors = []
    try:
        context = ray.init(num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _PARENT: 1}, {"CPU": 1, _CHILD: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False)
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        node_a, node_b = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address,
                          node_a.node_address, node_a.worker_address, node_b.node_address, node_b.worker_address))
        assert len(pids) == 5 and len(addresses) == 6 and os.getpid() not in pids
        assert context.trace_address is None and core.node_id == node_a.node_id
        a_before, b_before = _node(context, node_a, deadline), _node(context, node_b, deadline)
        submitter_before, executor_before = _worker(context, node_a.worker_id, deadline), _worker(context, node_b.worker_id, deadline)
        for state, node, info in ((submitter_before, node_a, a_before), (executor_before, node_b, b_before)):
            assert state.state is protocol.WorkerMembershipState.ALIVE and state.death is None
            assert state.incarnation.worker_pid == node.worker_pid and state.incarnation.node_pid == node.node_pid
            assert state.incarnation.node_id == node.node_id and state.incarnation.node_registration_epoch == info.registration_epoch
        assert len({core.worker_id, node_a.worker_id, node_b.worker_id}) == 3
        source = ray.put(_PAYLOAD)
        assert source.owner_worker_id == core.worker_id and source.borrower_token is None
        before = core.owner_table.snapshot(source.object_id)
        assert before.state is ObjectState.READY_STORED and before.locations == frozenset((node_a.node_id,))
        original_offer, original_rpc = core.report_abandoned_dependency_replica, core._rpc

        def observe_offer(request):
            reply = original_offer(request)
            if type(request) is protocol.ReportAbandonedDependencyReplica and request.descriptor.object_id == source.object_id:
                with observation_lock:
                    if len(offers) < 4:
                        offers.append((request, reply))
                    else:
                        overflow.set()
            return reply

        def observe_rpc(address, handler, request):
            reply = original_rpc(address, handler, request)
            if handler == REQUEST_LEASE_HANDLER and type(reply) is protocol.GrantWorkerLease:
                with observation_lock:
                    if len(parent_grants) < 2:
                        parent_grants.append((request, reply))
                    else:
                        overflow.set()
            return reply

        core.report_abandoned_dependency_replica, core._rpc = observe_offer, observe_rpc
        parent = _exit_after_child_grant.remote([source], node_b.node_id, node_b.worker_id, node_a.worker_id, node_a.worker_pid)
        with pytest.raises(WorkerDiedError):
            ray.get(parent, timeout=_remaining(deadline))
        _wait(core, lambda: parent.object_id not in core._task_finish_barriers, deadline)
        failed = core.owner_table.snapshot(parent.object_id)
        assert failed.state is ObjectState.ERROR and isinstance(failed.error, WorkerDiedError)
        assert failed.current_attempt.attempt_number == 0
        parent_record = core._recovery.task_record(parent.object_id.task_id)
        assert parent_record.max_retries == parent_record.retries_started == 0
        assert parent_record.current_attempt == failed.current_attempt
        assert core._recovery.active_recovery(parent.object_id.task_id) is None

        def handed_off():
            with observation_lock:
                successful = tuple((request, reply) for request, reply in offers if reply.custody_transferred)
            return successful[0] if successful else None

        abandoned, accepted = _poll(handed_off, deadline, "Node did not hand abandoned input to its live owner")
        assert type(accepted) is protocol.ReportAbandonedDependencyReplicaReply and accepted.request == abandoned
        assert accepted.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert accepted.custody_transferred
        inventory = abandoned.inventory
        child_request = inventory.lease_request
        assert inventory.node_id == node_b.node_id and inventory.descriptors == (abandoned.descriptor,)
        assert child_request.requester_worker_id == node_a.worker_id and child_request.requester_node_id == node_a.node_id
        assert child_request.target_node_id == node_b.node_id and child_request.attempt_id.attempt_number == 0
        assert child_request.task_id != parent.object_id.task_id
        assert len(child_request.return_ids) == len(child_request.dependencies) == len(child_request.dependency_owner_routes) == 1
        child_output, = child_request.return_ids
        expected_child = TaskID.derive(core.job_id, TaskID.derive(core.job_id, parent.object_id.task_id, 0), 0)
        assert child_request.task_id == expected_child and child_output.task_id == expected_child
        route, = child_request.dependency_owner_routes
        assert (route.object_id, route.owner_worker_id, route.owner_address) == (source.object_id, core.worker_id, runtime.owner_service.address)
        assert route.hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert route.hold.submitting_worker_id == node_a.worker_id and route.hold.task_id == expected_child
        assert route.hold.origin_attempt_id == child_request.attempt_id
        assert child_request.dependencies == (protocol.ObjectStoreDescriptor(
            source.object_id, core.worker_id, before.current_attempt, node_a.node_id,
            before.canonical_stored_result.size_bytes, before.canonical_stored_result.checksum,
        ),)
        descriptor = abandoned.descriptor
        assert (descriptor.object_id, descriptor.owner_worker_id, descriptor.producer_attempt_id, descriptor.node_id,
                descriptor.size_bytes, descriptor.checksum) == (
            source.object_id, core.worker_id, before.current_attempt, node_b.node_id,
            before.canonical_stored_result.size_bytes, before.canonical_stored_result.checksum,
        )
        dead_worker = _worker(context, node_a.worker_id, deadline)
        assert dead_worker.state is protocol.WorkerMembershipState.DEAD
        death = dead_worker.death
        assert death == abandoned.submitter_death and death.incarnation == submitter_before.incarnation
        assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT and death.exit_code == _EXIT_CODE
        installed = core.owner_table.dead_worker_record(node_a.worker_id)
        assert installed is not None and installed.death_id == _worker_death_reference_id(death)
        assert not core.owner_table.dead_worker_record(core.worker_id)

        outcome_request = protocol.GetWorkerLeaseOutcome(child_request.lease_id, child_request.task_id, child_request.attempt_id,
            node_b.worker_id, node_a.worker_id, child_request.return_ids, child_request.scheduling_key)
        outcome = _rpc(node_b.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline)
        assert type(outcome) is protocol.GetWorkerLeaseOutcomeReply and outcome.found and outcome.worker_alive
        assert (outcome.lease_id, outcome.task_id, outcome.attempt_id, outcome.executor_worker_id, outcome.owner_worker_id,
                outcome.object_ids, outcome.node_id, outcome.scheduling_key) == (
            child_request.lease_id, expected_child, child_request.attempt_id, node_b.worker_id, node_a.worker_id,
            child_request.return_ids, node_b.node_id, child_request.scheduling_key)
        assert outcome.state is protocol.LeaseExecutionState.ABANDONED and outcome.completion_status is None
        assert not outcome.descriptors and not outcome.orphan_descriptors and not outcome.cleanup_pending
        assert outcome.output_publication is None and outcome.output_completion is None
        assert all(not _physical(node, child_output, deadline).found for node in (node_a, node_b))
        with observation_lock:
            assert len(parent_grants) == 1 and not overflow.is_set()
            parent_request, parent_grant = parent_grants[0]
            assert parent_request.task_id == parent.object_id.task_id and parent_grant.worker_id == node_a.worker_id
            assert all(request == abandoned and reply.request == request for request, reply in offers)
        current = core.owner_table.snapshot(source.object_id)
        assert current.state is ObjectState.READY_STORED and current.locations == frozenset((node_a.node_id, node_b.node_id))
        assert current.canonical_stored_result == before.canonical_stored_result and current.current_attempt == before.current_attempt
        assert not current.retained_tokens and not current.borrowed_tokens and source._local_token in current.local_tokens
        assert ray.get(source, timeout=_remaining(deadline)) == _PAYLOAD
        for node in (node_a, node_b):
            physical = _physical(node, source.object_id, deadline)
            assert physical.found and physical.sealed and physical.owner_worker_id == core.worker_id
            assert physical.producer_attempt_id == before.current_attempt and physical.checksum == descriptor.checksum
        assert _node(context, node_a, deadline).registration_epoch == a_before.registration_epoch
        assert _node(context, node_b, deadline).registration_epoch == b_before.registration_epoch
        assert _worker(context, node_b.worker_id, deadline).incarnation == executor_before.incarnation
        _poll(lambda: _status(node_b, deadline).resources_clean, deadline, "abandoned target inventory/pins did not settle")

        def replacement_ready():
            status = _status(node_a, deadline)
            return status.child_pid if status.child_pid not in (None, node_a.worker_pid) else None

        replacement_pid = _poll(replacement_ready, deadline, "submitter Node did not replace its Worker")
        assert replacement_pid > 0 and replacement_pid not in pids and replacement_pid != os.getpid()
        pids.add(replacement_pid)
        probe_address = node_a.node_address
        probe_task = TaskID.derive(core.job_id, core.driver_task_id, 9001)
        probe = protocol.RequestWorkerLease(LeaseID.random(), probe_task, AttemptID(probe_task, 0), ResourceVector({"CPU": 1}),
            core.node_id, core.worker_id, target_node_id=node_a.node_id, dependencies=())
        probe_cancel = protocol.CancelWorkerLease(probe.lease_id, probe.task_id, probe.attempt_id,
            probe.requester_node_id, probe.requester_worker_id, lease_request=probe)

        def acquire_probe():
            candidate = _rpc(probe_address, REQUEST_LEASE_HANDLER, probe, deadline)
            assert type(candidate) in (protocol.RejectWorkerLease, protocol.GrantWorkerLease)
            assert (candidate.lease_id, candidate.task_id, candidate.attempt_id, candidate.scheduling_key) == (
                probe.lease_id, probe.task_id, probe.attempt_id, probe.scheduling_key)
            if type(candidate) is protocol.RejectWorkerLease:
                assert candidate.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
                return None
            assert candidate.node_id == node_a.node_id and not candidate.dependencies
            return candidate

        probe_grant = _poll(acquire_probe, deadline, "replacement did not grant the nonexecuting identity probe")
        addresses.add(probe_grant.worker_address)
        replacement = _worker(context, probe_grant.worker_id, deadline)
        assert replacement.state is protocol.WorkerMembershipState.ALIVE and replacement.death is None
        assert replacement.worker_id != node_a.worker_id and replacement.incarnation.worker_pid == replacement_pid
        assert replacement.incarnation.node_id == node_a.node_id and replacement.incarnation.node_pid == node_a.node_pid
        assert replacement.incarnation.node_registration_epoch == a_before.registration_epoch
        probe_reply = _rpc(probe_address, CANCEL_LEASE_HANDLER, probe_cancel, deadline)
        _assert_cancelled(probe_reply, probe_cancel)
        assert probe_reply.released and probe_reply.dependency_inventory.descriptors == ()
        probe_cancelled = True
        _close_local(parent, deadline)
        _wait(core, lambda: core.owner_table.collection_state(parent.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_local(source, deadline)
        _wait(core, lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: all(not _physical(node, source.object_id, deadline).found for node in (node_a, node_b)),
              deadline, "Driver input did not collect both physical replicas")
        for node, expected_pid in ((node_a, replacement_pid), (node_b, node_b.worker_pid)):
            status = _poll(lambda: candidate if (candidate := _status(node, deadline)).resources_clean else None,
                           deadline, "Node did not return to clean idle state")
            assert status.child_pids == (expected_pid,)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        assert not _pid_exists(node_a.worker_pid) and len(pids) == 6
        assert all(_pid_exists(pid) for pid in pids - {node_a.worker_pid}) and not overflow.is_set()
    finally:
        if core is not None and original_offer is not None:
            core.report_abandoned_dependency_replica, core._rpc = original_offer, original_rpc
        cleanup_deadline = time.monotonic() + 3.0
        try:
            if probe_cancel is not None and not probe_cancelled:
                try:
                    _assert_cancelled(_rpc(probe_address, CANCEL_LEASE_HANDLER, probe_cancel, cleanup_deadline), probe_cancel)
                except Exception as exc:
                    cleanup_errors.append(exc)
            for reference in (parent, source):
                try:
                    _close_local(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not cleanup_errors and context is not None and report is not None and replacement is not None and death is not None
    assert report.core_stopped and report.gcs_clean and report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean and not report.forced
    assert report.worker_pids == (replacement.incarnation.worker_pid, node_b.worker_pid)
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert not ray.is_initialized() and len(pids) == 6
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
