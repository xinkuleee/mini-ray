"""Bounded real custody recovery when every actual Grant reply is lost.

Five children: GCS, two Nodes, and one Worker per Node. A factory on A
returns a Worker-owned 8 KiB put; one never-executed consumer on B also
depends on a Driver-owned 8 KiB put. Hold its first actual Grant at most
eight seconds while the Driver publicly drops its source replica on A.
Discard all twelve actual B Grant replies across the existing four replay
rounds. Real Cancel must disclose the retired inventory, then both owners
must take custody before the original ambiguity becomes the task error.

No fake Node/reply, Push, kill, replacement, test thread/listener or tracing.
Two 1 MiB stores; an 18-second post-init work deadline and three-second
reference-finalizer budget. Shutdown has its own bounded drain; the outer
runner begins process-tree termination at 30 seconds, followed by bounded
TERM/KILL/reap grace. The internal deadlines are not a total runtime bound.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.core import (
    _LeaseCancellationState, _LeaseRequestAmbiguous, _LocationReportState,
    _ReplicaLocationReceipt,
)
from miniray.node import (
    CANCEL_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
    REQUEST_LEASE_HANDLER, SHUTDOWN_STATUS_HANDLER,
)
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import TransportTimeout
from tests.integration.test_local_replica_handoff_failure_path import (
    _FOREIGN_PAYLOAD, _LOCAL_PAYLOAD, _SOURCE, _TARGET, _assert_cancelled,
    _assert_empty_outcome, _consumer_must_not_run, _create_foreign_ref,
    _foreign_state, _poll, _remaining, _replica, _rpc, _wait,
)
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_output_owner_death_path import _close_reference, _pid_exists


pytestmark = pytest.mark.multiprocess_smoke


def test_lost_grant_replies_cancel_and_transfer_both_input_replicas():
    context = core = report = None
    local = outer = foreign = consumer = None
    original_rpc = original_borrow = original_push = original_custody = original_clear = None
    pids, addresses = set(), set()
    entered, release, expired, disarmed, overflow = (threading.Event() for _ in range(5))
    observation_lock = threading.Lock()
    gated, observations, timeouts, pushes, cancel_states, cleared_states = [], [], [], [], [], []
    close_errors = []
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _SOURCE: 1}, {"CPU": 1, _TARGET: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + 18.0
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
        original_custody, original_clear = core._record_replica_custody_locked, core._clear_protocol_unresolved

        def remember(kind, request, reply):
            with observation_lock:
                if len(observations) < 64:
                    observations.append((kind, request, reply))
                else:
                    overflow.set()

        def observe_rpc(address, handler, request):
            if handler == CANCEL_LEASE_HANDLER and not disarmed.is_set():
                with core._state_lock:
                    pending = core._task_finish_barriers[consumer.object_id]
                    state = core._protocol_unresolved[pending.task_key].obligation
                    assert type(state) is _LeaseCancellationState
                    before = core.owner_table.snapshot(local.object_id)
                with observation_lock:
                    if len(cancel_states) < 2:
                        cancel_states.append((pending, state, before))
                    else:
                        overflow.set()
            reply = original_rpc(address, handler, request)
            if handler in (REQUEST_LEASE_HANDLER, CANCEL_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER):
                remember(handler, request, reply)
            if (handler != REQUEST_LEASE_HANDLER or type(reply) is not protocol.GrantWorkerLease
                    or address != node_b.node_address or disarmed.is_set()):
                return reply
            assert reply.node_id == node_b.node_id and reply.worker_id == node_b.worker_id
            assert len(reply.dependencies) == 2
            with observation_lock:
                first = not gated
                if first:
                    gated.append((request, reply, time.monotonic() + 8.0))
            if first:
                entered.set()
                if not release.wait(8.0):
                    expired.set()
            if disarmed.is_set():
                return reply
            failure = TransportTimeout("actual complete Grant reply deliberately not delivered")
            with observation_lock:
                if len(timeouts) < 12:
                    timeouts.append(failure)
                else:
                    overflow.set()
            raise failure

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

        def observe_clear(pending):
            with core._state_lock:
                marker = core._protocol_unresolved.get(pending.task_key)
                if (consumer is not None and pending.spec.task_id == consumer.object_id.task_id
                        and marker is not None and type(marker.obligation) is _LocationReportState):
                    with observation_lock:
                        if len(cleared_states) < 2:
                            cleared_states.append(marker.obligation)
                        else:
                            overflow.set()
            return original_clear(pending)

        core._rpc, core._borrow_rpc, core._push_task_rpc = observe_rpc, observe_borrow, observe_push
        core._record_replica_custody_locked, core._clear_protocol_unresolved = observe_custody, observe_clear
        consumer = _consumer_must_not_run.remote(local, foreign)
        assert entered.wait(min(8.0, _remaining(deadline)))
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
            assert not pushes and not timeouts and not cancel_states
            assert not any(kind in ("local-custody", "report_retained_object_location") for kind, _, _ in observations)
        assert ray.drop_object(local, node_id=node_a.node_id)
        assert not _replica(node_a, local.object_id, gate_deadline).found
        lost = core.owner_table.snapshot(local.object_id)
        assert lost.state is ObjectState.LOST and not lost.locations
        assert lost.current_attempt == local_before.current_attempt and lost.canonical_stored_result == local_before.canonical_stored_result
        assert not expired.is_set()
        release.set()
        with pytest.raises(_LeaseRequestAmbiguous, match="ambiguous after replay") as failure:
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, deadline)
        with observation_lock:
            observed = tuple(observations)
            (pending, cancellation, cancellation_owner), = cancel_states
            (handoff,) = tuple(cleared_states)
            assert not pushes and len(timeouts) == 12
        grants = tuple((request, reply) for kind, request, reply in observed if type(reply) is protocol.GrantWorkerLease)
        assert len(grants) == 12 and all(request == lease_request and reply == grant for request, reply in grants)
        assert cancellation.known_grant is None and cancellation.reply is None and cancellation.round == 0
        assert cancellation.lease_request == lease_request and cancellation.target_node_id == node_b.node_id
        assert cancellation.address == node_b.node_address and cancellation.terminal_error is failure.value
        assert failure.value.cause is timeouts[-1] and failure.value.state.request == lease_request
        assert cancellation_owner.state is ObjectState.LOST and not cancellation_owner.locations
        assert pending.protected_dependencies == (local.object_id,)
        assert pending.dependency_hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert pending.dependency_hold in cancellation_owner.submitted_tokens
        cancels = tuple((request, reply) for kind, request, reply in observed if kind == CANCEL_LEASE_HANDLER)
        (cancel_request, cancel_reply), = cancels
        _assert_cancelled(cancel_reply, cancel_request)
        assert cancel_request == cancellation.request and cancel_reply.released and cancel_reply.retired_grant == grant
        assert not any(kind == GET_WORKER_LEASE_OUTCOME_HANDLER for kind, _, _ in observed)
        assert handoff.grant == grant and handoff.lease_request == lease_request
        assert handoff.granting_node_address == node_b.node_address and handoff.cancellation_reply == cancel_reply
        assert handoff.terminal_error is failure.value and handoff.execution_outcome is None
        reports = tuple((request, reply) for kind, request, reply in observed if kind == "report_retained_object_location")
        (location_request, location_reply), = reports
        assert len(handoff.reports) == 1 and location_request == handoff.reports[0].request
        assert handoff.reports[0].guard in pending.foreign_dependency_guards
        assert handoff.reports[0].guard.hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert type(location_reply) is protocol.ReportRetainedObjectLocationReply
        assert location_reply.accepted and location_reply.custody_transferred
        assert location_reply.descriptor == grant.dependencies[1] and handoff.receipts == (location_reply,)
        local_receipts = tuple((request, reply) for kind, request, reply in observed if kind == "local-custody")
        (local_descriptor, local_receipt), = local_receipts
        assert type(local_receipt) is _ReplicaLocationReceipt and local_receipt.accepted and local_receipt.custody_transferred
        assert local_descriptor == local_receipt.descriptor == grant.dependencies[0] and handoff.local_receipts == (local_receipt,)
        cancel_index = observed.index((CANCEL_LEASE_HANDLER, cancel_request, cancel_reply))
        assert cancel_index > max(index for index, (_, _, reply) in enumerate(observed) if type(reply) is protocol.GrantWorkerLease)
        assert cancel_index < observed.index(("local-custody", local_descriptor, local_receipt))
        assert cancel_index < observed.index(("report_retained_object_location", location_request, location_reply))
        failed = core.owner_table.snapshot(consumer.object_id)
        assert failed.state is ObjectState.ERROR and failed.error is failure.value and failed.current_attempt == grant.attempt_id
        record = core._recovery.task_record(consumer.object_id.task_id)
        assert record.retries_started == 0 and record.current_attempt == grant.attempt_id
        assert core._recovery.active_recovery(consumer.object_id.task_id) is None
        outcome_request = protocol.GetWorkerLeaseOutcome(
            grant.lease_id, grant.task_id, grant.attempt_id, grant.worker_id, core.worker_id, (consumer.object_id,),
            grant.scheduling_key, grant.target_execution,
        )
        outcome = _rpc(node_b.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline)
        _assert_empty_outcome(outcome, outcome_request, node_b.node_id)
        assert outcome.worker_alive and outcome.state is protocol.LeaseExecutionState.ABANDONED
        restored = core.owner_table.snapshot(local.object_id)
        assert restored.state is ObjectState.READY_STORED and restored.locations == frozenset((node_b.node_id,))
        assert restored.current_attempt == local_before.current_attempt and restored.canonical_stored_result == local_before.canonical_stored_result
        assert core._stored_descriptors[local.object_id] == replace(local_before.canonical_stored_result, node_id=node_b.node_id)
        assert ray.get(local, timeout=_remaining(deadline)) == _LOCAL_PAYLOAD
        assert ray.get(foreign, timeout=_remaining(deadline)) == _FOREIGN_PAYLOAD
        assert _foreign_state(core, foreign, deadline).current_attempt == foreign_before.current_attempt
        assert not _replica(node_a, local.object_id, deadline).found
        assert _replica(node_b, local.object_id, deadline).found and _replica(node_b, foreign.object_id, deadline).found

        def resources_released(node):
            status = _rpc(node.node_address, SHUTDOWN_STATUS_HANDLER, protocol.ShutdownStatusRequest("inspect-ambiguous-custody-only"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
            assert status.child_pids == (node.worker_pid,)
            return status.resources_clean

        _poll(lambda: resources_released(node_b), deadline, "Cancel retained target resources or pins")
        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: core._foreign_lineage_registry.snapshot(consumer.object_id.task_id) is None, deadline, "consumer lineage did not release")
        _close_local(outer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_reference(foreign, deadline)
        _close_local(local, deadline)
        _wait(core, lambda: core.owner_table.collection_state(local.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: all(not _replica(node, ref.object_id, deadline).found
                          for node in (node_a, node_b) for ref in (local, foreign, outer, consumer)),
              deadline, "collected reference left physical bytes")
        _poll(lambda: all(resources_released(node) for node in (node_a, node_b)), deadline, "Nodes did not return to idle")
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        assert not overflow.is_set() and not expired.is_set() and all(_pid_exists(pid) for pid in pids)
    finally:
        disarmed.set()
        release.set()
        if core is not None and original_rpc is not None:
            core._rpc, core._borrow_rpc, core._push_task_rpc = original_rpc, original_borrow, original_push
            core._record_replica_custody_locked, core._clear_protocol_unresolved = original_custody, original_clear
        cleanup_deadline = time.monotonic() + 3.0
        try:
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
    assert report.worker_pids == context.worker_pids
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert not ray.is_initialized() and len(pids) == 5
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
