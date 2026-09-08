"""Bounded real local/foreign replica handoff after one local route failure.

Five children: GCS, two Nodes, and one ordinary Worker per Node. Two user
tasks: a factory on A returns a Worker-owned 8 KiB put; a never-executed
consumer on B depends on that foreign put and a Driver-owned 8 KiB put.
The consumer's actual grant is held for at most eight seconds. The Driver
publicly drops its source replica on A, then a one-shot dictionary write
failure interrupts local route installation without changing stored contents.

The complete grant must survive that exception. The baseline cancels the live
executor. A separate exact case kills only B's GCS-verified, never-Pushed
executor and waits for the real Node WORKER_LOST outcome before opening the
same gate. Cancel must then reject; the Core's own exact outcome query proves
execution fenced without inventing a cancellation ACK. Both input owners
remain alive, and foreign custody reporting and local exact replay are real.
That case permits one replacement and one empty, never-Pushed probe lease to
identify its endpoint, with exact cancellation in finally as well. Neither
case adds fake replies, test threads/listeners, Actor or tracing.
Two 1 MiB stores; an 18-second post-init work deadline and three-second
reference/probe finalizer budget. Shutdown has its own bounded drain budget;
the external runner begins process-tree termination at 30 seconds, followed
by its bounded TERM/KILL/reap grace. Internal deadlines are not a total bound.
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
from miniray.control import GET_NODE_STATE_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.core import _LocationReportState, _ReplicaLocationReceipt
from miniray.errors import SystemTaskError
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

_SOURCE = "local_handoff_failure_source"
_TARGET = "local_handoff_failure_target"
_LOCAL_PAYLOAD = b"L" * (8 * 1024)
_FOREIGN_PAYLOAD = b"F" * (8 * 1024)
_ROUTE_FAILURE = "one local target route write failed"
_GATE_SECONDS = 8.0
_WORK_SECONDS = 18.0


@ray.remote(resources={_SOURCE: 1}, max_retries=0)
def _create_foreign_ref():
    return ray.put(_FOREIGN_PAYLOAD)


@ray.remote(resources={_TARGET: 1}, max_retries=0)
def _consumer_must_not_run(local_value, foreign_value):
    raise AssertionError("local route failure admitted consumer user code")


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "local replica acceptance exceeded its deadline"
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


def _replica(node, object_id, deadline):
    reply = _rpc(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _foreign_state(core, reference, deadline):
    reply = _rpc(reference.owner_address, "get_owned_object", protocol.GetOwnedObject(
        reference.object_id, reference.owner_worker_id, core.worker_id, reference.borrower_token,
    ), deadline)
    assert type(reply) is protocol.GetOwnedObjectReply and reply.accepted
    assert reply.object_id == reference.object_id and reply.owner_worker_id == reference.owner_worker_id
    return reply


def _assert_cancelled(reply, request):
    assert type(reply) is protocol.CancelWorkerLeaseReply
    assert reply.accepted and reply.cancelled
    assert reply.state is protocol.LeaseExecutionState.ABANDONED
    assert (reply.lease_id, reply.task_id, reply.attempt_id,
            reply.requester_node_id, reply.requester_worker_id, reply.scheduling_key) == (
        request.lease_id, request.task_id, request.attempt_id,
        request.requester_node_id, request.requester_worker_id, request.scheduling_key,
    )


def _worker_state(context, worker_id, deadline):
    reply = _rpc(context.gcs_address, GET_WORKER_STATE_HANDLER,
                 protocol.GetWorkerState(worker_id), deadline)
    assert type(reply) is protocol.GetWorkerStateReply and reply.found and reply.worker_id == worker_id
    return reply


def _node_state(context, node, deadline):
    reply = _rpc(context.gcs_address, GET_NODE_STATE_HANDLER,
                 protocol.GetNodeState(node.node_id), deadline)
    assert type(reply) is protocol.GetNodeStateReply and reply.found and reply.node_id == node.node_id
    assert reply.state is protocol.NodeMembershipState.ALIVE and reply.node_pid == node.node_pid
    assert reply.death is None
    return reply


def _assert_empty_outcome(reply, request, node_id):
    assert type(reply) is protocol.GetWorkerLeaseOutcomeReply and reply.found
    assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.executor_worker_id,
            reply.owner_worker_id, reply.object_ids, reply.node_id, reply.scheduling_key, reply.target_execution) == (
        request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
        request.owner_worker_id, request.object_ids, node_id, request.scheduling_key, request.target_execution,
    )
    assert reply.completion_status is None and not reply.cleanup_pending
    assert not reply.descriptors and not reply.orphan_descriptors
    assert reply.output_publication is None and reply.output_completion is None


def _run_local_route_handoff(*, executor_exits):
    context = runtime = core = node_a = node_b = report = None
    local = outer = foreign = consumer = routes = None
    original_rpc = original_borrow = original_push = original_custody = None
    grant = lease_request = None
    target_before = owner_before = death = dead_outcome = replacement_state = None
    probe = probe_cancel = probe_address = None
    probe_cancelled = False
    pids, addresses = set(), set()
    entered, release, expired, overflow = (threading.Event() for _ in range(4))
    observation_lock = threading.Lock()
    gated, observations, pushes, failed_writes, repaired_writes, cancel_states = [], [], [], [], [], []
    close_errors = []
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _SOURCE: 1}, {"CPU": 1, _TARGET: 1}),
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
        assert context.trace_address is None and core.node_id == node_a.node_id
        local = ray.put(_LOCAL_PAYLOAD)
        assert local.owner_worker_id == core.worker_id and local.borrower_token is None
        local_before = core.owner_table.snapshot(local.object_id)
        assert local_before.state is ObjectState.READY_STORED and local_before.locations == frozenset((node_a.node_id,))
        outer = _create_foreign_ref.remote()
        foreign = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(foreign, ray.ObjectRef) and foreign.borrower_token is not None
        assert foreign.owner_worker_id == node_a.worker_id and foreign.owner_address == node_a.worker_address
        assert foreign.owner_worker_id != core.worker_id
        _wait(core, lambda: outer.object_id not in core._task_finish_barriers, deadline)
        foreign_before = _foreign_state(core, foreign, deadline)
        assert foreign_before.state is protocol.OwnedObjectState.READY_STORED
        assert foreign_before.descriptor.node_id == node_a.node_id

        original_rpc, original_borrow, original_push = core._rpc, core._borrow_rpc, core._push_task_rpc
        original_custody = core._record_replica_custody_locked

        def remember(kind, request, reply):
            with observation_lock:
                if len(observations) < 64:
                    observations.append((kind, request, reply))
                else:
                    overflow.set()

        def current_handoff():
            with core._state_lock:
                pending = core._task_finish_barriers[consumer.object_id]
                marker = core._protocol_unresolved[pending.task_key]
                assert type(marker.obligation) is _LocationReportState
                return pending, marker.obligation

        def observe_rpc(address, handler, request):
            if handler == CANCEL_LEASE_HANDLER:
                _pending, state = current_handoff()
                with observation_lock:
                    if len(cancel_states) < 2:
                        cancel_states.append(state)
                    else:
                        overflow.set()
            reply = original_rpc(address, handler, request)
            if handler in (REQUEST_LEASE_HANDLER, CANCEL_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER):
                remember(handler, request, reply)
            should_hold = False
            if handler == REQUEST_LEASE_HANDLER and type(reply) is protocol.GrantWorkerLease and len(reply.dependencies) == 2:
                with observation_lock:
                    if not gated:
                        assert reply.node_id == node_b.node_id and reply.worker_id == node_b.worker_id
                        gated.append((request, reply, time.monotonic() + _GATE_SECONDS))
                        should_hold = True
            if should_hold:
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

        def observe_custody(descriptor, *, active_hold):
            receipt = original_custody(descriptor, active_hold=active_hold)
            if descriptor.object_id == local.object_id:
                remember("local-custody", descriptor, receipt)
            return receipt

        core._rpc, core._borrow_rpc, core._push_task_rpc = observe_rpc, observe_borrow, observe_push
        core._record_replica_custody_locked = observe_custody
        consumer = _consumer_must_not_run.remote(local, foreign)
        assert entered.wait(min(_GATE_SECONDS, _remaining(deadline)))
        with observation_lock:
            (lease_request, grant, gate_deadline), = gated
        gate_deadline = min(gate_deadline, deadline)
        assert grant.task_id == consumer.object_id.task_id and grant.attempt_id.attempt_number == 0
        assert tuple(item.object_id for item in grant.dependencies) == (local.object_id, foreign.object_id)
        assert tuple(item.owner_worker_id for item in grant.dependencies) == (core.worker_id, node_a.worker_id)
        for descriptor in grant.dependencies:
            physical = _replica(node_b, descriptor.object_id, gate_deadline)
            assert physical.found and physical.sealed and physical.producer_attempt_id == descriptor.producer_attempt_id
            assert physical.owner_worker_id == descriptor.owner_worker_id and physical.checksum == descriptor.checksum
        with observation_lock:
            assert not pushes and not any(kind in ("local-custody", "report_retained_object_location") for kind, _, _ in observations)
        # This public failpoint really removes A's bytes and owner location. It
        # may keep A's stale descriptor, but there is no valid fetch route.
        assert ray.drop_object(local, node_id=node_a.node_id)
        assert not _replica(node_a, local.object_id, gate_deadline).found
        lost = core.owner_table.snapshot(local.object_id)
        assert lost.state is ObjectState.LOST and not lost.locations
        assert lost.current_attempt == local_before.current_attempt and lost.canonical_stored_result == local_before.canonical_stored_result

        class FailOneLocalRoute(dict):
            armed = True
            tracking = True

            def __setitem__(self, key, value):
                target_write = key == local.object_id and value.node_id == node_b.node_id
                if target_write and self.tracking:
                    pending, state = current_handoff()
                    assert state.grant == grant and state.granting_node_address == node_b.node_address
                    assert len(state.reports) == 1 and state.reports[0].request.descriptor == grant.dependencies[1]
                    assert state.lease_request.lease_id == grant.lease_id
                    assert state.lease_request.task_id == grant.task_id and state.lease_request.attempt_id == grant.attempt_id
                    before = core.owner_table.snapshot(local.object_id)
                    if self.armed:
                        self.armed = False
                        assert not failed_writes and not state.local_receipts and not state.receipts
                        assert state.terminal_error is None and state.cancellation_reply is None and state.round == 0
                        failed_writes.append((pending, state, before, value))
                        raise RuntimeError(_ROUTE_FAILURE)
                    dict.__setitem__(self, key, value)
                    if len(repaired_writes) < 2:
                        repaired_writes.append((pending, state, before, value))
                    else:
                        overflow.set()
                    return
                dict.__setitem__(self, key, value)

        with core._state_lock:
            existing = core._stored_descriptors
            route = existing.get(local.object_id)
            assert route is None or route.node_id not in lost.locations
            routes = FailOneLocalRoute(existing)
            assert dict(routes) == dict(existing)
            core._stored_descriptors = routes
        if executor_exits:
            # Both dependency owners are elsewhere: kill only the immutable
            # managed executor incarnation, after verifying its untouched grant.
            target_before = _node_state(context, node_b, gate_deadline)
            checked = _worker_state(context, grant.worker_id, gate_deadline)
            owner_before = _worker_state(context, foreign.owner_worker_id, gate_deadline)
            assert checked.state is owner_before.state is protocol.WorkerMembershipState.ALIVE
            assert checked.death is None and owner_before.death is None
            assert checked.incarnation.worker_id == grant.worker_id == node_b.worker_id
            assert checked.incarnation.worker_pid == node_b.worker_pid
            assert checked.incarnation.node_id == node_b.node_id and checked.incarnation.node_pid == node_b.node_pid
            assert checked.incarnation.node_registration_epoch == target_before.registration_epoch
            assert checked.incarnation.worker_pid > 0 and checked.incarnation.worker_pid in pids
            assert checked.worker_id not in (core.worker_id, foreign.owner_worker_id)
            assert checked.incarnation.worker_pid not in (os.getpid(), context.gcs_pid, *context.node_pids, node_a.worker_pid)
            query = protocol.GetWorkerLeaseOutcome(
                grant.lease_id, grant.task_id, grant.attempt_id, grant.worker_id, core.worker_id,
                (consumer.object_id,), grant.scheduling_key, grant.target_execution,
            )
            before = _rpc(node_b.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, query, gate_deadline)
            _assert_empty_outcome(before, query, node_b.node_id)
            assert before.worker_alive and before.state is protocol.LeaseExecutionState.GRANTED
            assert _pid_exists(checked.incarnation.worker_pid)
            os.kill(checked.incarnation.worker_pid, signal.SIGKILL)

            def executor_reclaimed():
                worker = _worker_state(context, grant.worker_id, gate_deadline)
                candidate = _rpc(node_b.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, query, gate_deadline)
                _assert_empty_outcome(candidate, query, node_b.node_id)
                if worker.state is not protocol.WorkerMembershipState.DEAD or candidate.state is not protocol.LeaseExecutionState.WORKER_LOST:
                    return None
                assert not candidate.worker_alive and worker.death.incarnation == checked.incarnation
                return worker.death, candidate

            # A dead PID alone does not mean Node reclamation won against
            # Cancel. Observe the actual outcome before opening the grant gate.
            death, dead_outcome = _poll(executor_reclaimed, gate_deadline, "Node did not reclaim exited granted executor")
            assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT and death.exit_code == -signal.SIGKILL
            assert _pid_exists(node_a.worker_pid) and all(_pid_exists(pid) for pid in context.node_pids)
        assert not expired.is_set()
        release.set()
        with pytest.raises(SystemTaskError, match=_ROUTE_FAILURE) as failure:
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, deadline)
        assert not expired.is_set() and not routes.armed and len(failed_writes) == len(repaired_writes) == 1
        pending, first_state, first_owner, first_route = failed_writes[0]
        assert first_owner.state is ObjectState.LOST and not first_owner.locations
        assert pending.protected_dependencies == (local.object_id,)
        assert pending.dependency_hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert pending.dependency_hold in first_owner.submitted_tokens
        assert first_state.reports[0].guard in pending.foreign_dependency_guards
        assert first_state.reports[0].guard.hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        repaired_pending, replay, replay_owner, repaired_route = repaired_writes[0]
        assert repaired_pending == pending and replay.round == 1
        assert replay.grant == first_state.grant and replay.reports == first_state.reports
        assert replay.lease_request == first_state.lease_request and replay.local_receipts == ()
        assert replay_owner.state is ObjectState.LOST and not replay_owner.locations
        assert repaired_route == first_route == replace(local_before.canonical_stored_result, node_id=node_b.node_id)
        failed = core.owner_table.snapshot(consumer.object_id)
        assert failed.state is ObjectState.ERROR and type(failed.error) is SystemTaskError
        assert failed.error is failure.value is replay.terminal_error
        assert failed.current_attempt == grant.attempt_id
        record = core._recovery.task_record(consumer.object_id.task_id)
        assert record.retries_started == 0 and record.current_attempt == grant.attempt_id
        assert core._recovery.active_recovery(consumer.object_id.task_id) is None
        with observation_lock:
            observed = tuple(observations)
            assert not pushes and len(cancel_states) == 1
        assert cancel_states[0].terminal_error is failure.value
        assert cancel_states[0].local_receipts == () and cancel_states[0].receipts == ()
        cancels = tuple((request, reply) for kind, request, reply in observed if kind == CANCEL_LEASE_HANDLER)
        assert len(cancels) == 1
        cancel_request, cancel_reply = cancels[0]
        assert (cancel_request.lease_id, cancel_request.task_id, cancel_request.attempt_id,
                cancel_request.requester_node_id, cancel_request.requester_worker_id) == (
            grant.lease_id, grant.task_id, grant.attempt_id, lease_request.requester_node_id, lease_request.requester_worker_id,
        )
        assert cancel_request.scheduling_key == lease_request.scheduling_key
        queried = tuple((request, reply) for kind, request, reply in observed if kind == GET_WORKER_LEASE_OUTCOME_HANDLER)
        if executor_exits:
            assert type(cancel_reply) is protocol.CancelWorkerLeaseReply
            assert (cancel_reply.lease_id, cancel_reply.task_id, cancel_reply.attempt_id,
                    cancel_reply.requester_node_id, cancel_reply.requester_worker_id, cancel_reply.scheduling_key) == (
                cancel_request.lease_id, cancel_request.task_id, cancel_request.attempt_id,
                cancel_request.requester_node_id, cancel_request.requester_worker_id, cancel_request.scheduling_key,
            )
            assert not cancel_reply.accepted and not cancel_reply.cancelled and not cancel_reply.released
            assert cancel_reply.state is protocol.LeaseExecutionState.WORKER_LOST
            assert replay.cancellation_reply is None and len(queried) == 1
            outcome_query, outcome_reply = queried[0]
            _assert_empty_outcome(outcome_reply, outcome_query, node_b.node_id)
            assert outcome_query == query and outcome_reply == dead_outcome == replay.execution_outcome
            assert observed.index((CANCEL_LEASE_HANDLER, cancel_request, cancel_reply)) < observed.index((GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_query, outcome_reply))
        else:
            _assert_cancelled(cancel_reply, cancel_request)
            assert cancel_reply.released and replay.cancellation_reply == cancel_reply
            assert replay.execution_outcome is None and not queried
        reports = tuple((request, reply) for kind, request, reply in observed if kind == "report_retained_object_location")
        assert len(reports) == 1
        location_request, location_reply = reports[0]
        assert location_request == first_state.reports[0].request
        assert type(location_reply) is protocol.ReportRetainedObjectLocationReply
        assert location_reply.accepted and location_reply.custody_transferred
        assert location_reply.descriptor == grant.dependencies[1] and replay.receipts == (location_reply,)
        local_receipts = tuple((request, reply) for kind, request, reply in observed if kind == "local-custody")
        assert len(local_receipts) == 1
        local_descriptor, local_receipt = local_receipts[0]
        assert type(local_receipt) is _ReplicaLocationReceipt and local_receipt.accepted and local_receipt.custody_transferred
        assert local_descriptor == local_receipt.descriptor == grant.dependencies[0]
        assert observed.index((CANCEL_LEASE_HANDLER, cancel_request, cancel_reply)) < observed.index(("report_retained_object_location", location_request, location_reply))
        if executor_exits:
            assert observed.index((GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_query, outcome_reply)) < observed.index(("report_retained_object_location", location_request, location_reply))
        assert observed.index(("report_retained_object_location", location_request, location_reply)) < observed.index(("local-custody", local_descriptor, local_receipt))

        outcome_request = protocol.GetWorkerLeaseOutcome(
            grant.lease_id, grant.task_id, grant.attempt_id, grant.worker_id, core.worker_id, (consumer.object_id,),
            grant.scheduling_key, grant.target_execution,
        )
        outcome = _rpc(node_b.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline)
        _assert_empty_outcome(outcome, outcome_request, node_b.node_id)
        if executor_exits:
            assert not outcome.worker_alive and outcome.state is protocol.LeaseExecutionState.WORKER_LOST
            assert outcome == dead_outcome and not core._node_is_dead(node_b.node_id)
            assert _node_state(context, node_b, deadline).registration_epoch == target_before.registration_epoch
            surviving_owner = _worker_state(context, foreign.owner_worker_id, deadline)
            assert surviving_owner.state is protocol.WorkerMembershipState.ALIVE and surviving_owner.death is None
            assert surviving_owner.incarnation == owner_before.incarnation
        else:
            assert outcome.worker_alive and outcome.state is protocol.LeaseExecutionState.ABANDONED
        restored = core.owner_table.snapshot(local.object_id)
        assert restored.state is ObjectState.READY_STORED and restored.locations == frozenset((node_b.node_id,))
        assert restored.current_attempt == local_before.current_attempt and restored.canonical_stored_result == local_before.canonical_stored_result
        assert core._stored_descriptors[local.object_id] == repaired_route
        assert ray.get(local, timeout=_remaining(deadline)) == _LOCAL_PAYLOAD
        assert ray.get(foreign, timeout=_remaining(deadline)) == _FOREIGN_PAYLOAD
        assert _foreign_state(core, foreign, deadline).current_attempt == foreign_before.current_attempt
        assert not _replica(node_a, local.object_id, deadline).found
        assert _replica(node_b, local.object_id, deadline).found and _replica(node_b, foreign.object_id, deadline).found
        if executor_exits:
            def replacement_ready():
                status = _rpc(node_b.node_address, SHUTDOWN_STATUS_HANDLER,
                              protocol.ShutdownStatusRequest("inspect-executor-replacement"), deadline)
                assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
                return status.child_pid if status.child_pid not in (None, node_b.worker_pid) else None

            replacement_pid = _poll(replacement_ready, deadline, "target did not replace its exited executor")
            assert replacement_pid > 0 and replacement_pid not in pids and replacement_pid != os.getpid()
            pids.add(replacement_pid)
            probe_address = node_b.node_address
            probe_task = TaskID.derive(core.job_id, core.driver_task_id, 9001)
            probe = protocol.RequestWorkerLease(
                LeaseID.random(), probe_task, AttemptID(probe_task, 0), ResourceVector({"CPU": 1}),
                core.node_id, core.worker_id, target_node_id=node_b.node_id, dependencies=(),
            )
            probe_cancel = protocol.CancelWorkerLease(
                probe.lease_id, probe.task_id, probe.attempt_id, probe.requester_node_id, probe.requester_worker_id,
            )

            def acquire_probe():
                candidate = _rpc(probe_address, REQUEST_LEASE_HANDLER, probe, deadline)
                assert type(candidate) in (protocol.RejectWorkerLease, protocol.GrantWorkerLease)
                assert (candidate.lease_id, candidate.task_id, candidate.attempt_id, candidate.scheduling_key, candidate.target_execution) == (
                    probe.lease_id, probe.task_id, probe.attempt_id, probe.scheduling_key, probe.target_execution,
                )
                if type(candidate) is protocol.RejectWorkerLease:
                    assert candidate.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
                    return None
                assert candidate.node_id == node_b.node_id and not candidate.dependencies
                return candidate

            probe_grant = _poll(acquire_probe, deadline, "existing replacement did not grant the nonexecuting probe")
            addresses.add(probe_grant.worker_address)
            replacement_state = _worker_state(context, probe_grant.worker_id, deadline)
            assert replacement_state.state is protocol.WorkerMembershipState.ALIVE and replacement_state.death is None
            assert replacement_state.incarnation.worker_pid == replacement_pid
            assert replacement_state.incarnation.node_id == node_b.node_id and replacement_state.incarnation.node_pid == node_b.node_pid
            assert replacement_state.incarnation.node_registration_epoch == target_before.registration_epoch
            assert replacement_state.worker_id != grant.worker_id
            assert _worker_state(context, grant.worker_id, deadline).death == death
            probe_reply = _rpc(probe_address, CANCEL_LEASE_HANDLER, probe_cancel, deadline)
            _assert_cancelled(probe_reply, probe_cancel)
            assert probe_reply.released
            probe_cancelled = True

        def resources_released(node):
            status = _rpc(node.node_address, SHUTDOWN_STATUS_HANDLER, protocol.ShutdownStatusRequest("inspect-local-handoff-only"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
            expected_pid = (replacement_state.incarnation.worker_pid
                            if executor_exits and node.node_id == node_b.node_id else node.worker_pid)
            assert status.child_pids == (expected_pid,)
            return status.resources_clean

        _poll(lambda: resources_released(node_b), deadline, "cancelled grant retained target resources or dependency pins")
        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: core._foreign_lineage_registry.snapshot(consumer.object_id.task_id) is None, deadline,
              "consumer foreign lineage did not release")
        _close_local(outer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_reference(foreign, deadline)
        _close_local(local, deadline)
        _wait(core, lambda: core.owner_table.collection_state(local.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: all(not _replica(node, ref.object_id, deadline).found
                          for node in (node_a, node_b) for ref in (local, foreign, outer, consumer)),
              deadline, "collected source or consumer left physical bytes")
        _poll(lambda: all(resources_released(node) for node in (node_a, node_b)), deadline, "cleanup did not return both Nodes to idle")
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        assert not overflow.is_set() and len(pids) == (6 if executor_exits else 5)
        surviving_pids = pids - ({node_b.worker_pid} if executor_exits else set())
        assert all(_pid_exists(pid) for pid in surviving_pids)
        if executor_exits:
            assert not _pid_exists(node_b.worker_pid)
    finally:
        # Keep all legitimate route entries added during the run. Disarming
        # this ordinary dict subclass must not restore a stale earlier cache.
        if routes is not None:
            routes.armed = False
            routes.tracking = False
        release.set()
        if core is not None and original_rpc is not None:
            core._rpc, core._borrow_rpc, core._push_task_rpc = original_rpc, original_borrow, original_push
            core._record_replica_custody_locked = original_custody
        cleanup_deadline = time.monotonic() + 3.0
        try:
            if probe_cancel is not None and not probe_cancelled:
                try:
                    result = _rpc(probe_address, CANCEL_LEASE_HANDLER, probe_cancel, cleanup_deadline)
                    _assert_cancelled(result, probe_cancel)
                except Exception as exc:
                    close_errors.append(exc)
            for reference in (consumer, outer, foreign, local):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not close_errors and context is not None and report is not None
    assert report.core_stopped and report.gcs_clean and report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean and not report.forced
    expected_workers = ((node_a.worker_pid, replacement_state.incarnation.worker_pid)
                        if executor_exits else context.worker_pids)
    assert report.worker_pids == expected_workers
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert not ray.is_initialized() and len(pids) == (6 if executor_exits else 5)
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass


def test_local_route_failure_replays_custody_without_executing_consumer():
    _run_local_route_handoff(executor_exits=False)


def test_exited_granted_executor_uses_outcome_fence_and_preserves_input_custody():
    _run_local_route_handoff(executor_exits=True)
