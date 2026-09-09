"""Bounded real custody handoff after localization fails before any Grant.

Five children: GCS, two Nodes, and one Worker per Node. A factory on A
returns a Worker-owned 8 KiB put; a never-executed consumer on B also uses
a Driver-owned 8 KiB put. Hold its immutable targeted Request before the
real send for at most eight seconds. The Driver publicly drops the foreign
source on A after wire metadata is frozen, without changing that request.

B seals the first input, cannot pull the second, and actually rejects the
request. Real Cancel discloses only the first replica and no retired Grant.
The existing owner handoff and exact Node custody ACK must converge before
the original rejection terminates the consumer. No fake reply/Node, Push,
kill, replacement, new test thread/listener, failure envelope or tracing.

Two 1 MiB stores; 18-second post-init work and three-second reference-finally
budgets. Shutdown has its own bounded drain. The outer runner starts process
tree termination at 30 seconds plus bounded TERM/KILL/reap grace; these
internal deadlines alone are not a total runtime bound.
"""

from __future__ import annotations

from copy import deepcopy
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
from miniray.core import _LeaseCancellationState, _LocationReportState, _ReplicaLocationReceipt
from miniray.errors import LeaseRejectedError
from miniray.node import (
    CANCEL_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
    REQUEST_LEASE_HANDLER, SHUTDOWN_STATUS_HANDLER,
)
from miniray.ownership import ObjectCollectionState, ObjectState
from tests.integration.test_local_replica_handoff_failure_path import (
    _LOCAL_PAYLOAD, _SOURCE, _TARGET, _assert_cancelled,
    _consumer_must_not_run, _create_foreign_ref, _foreign_state, _poll,
    _remaining, _replica, _rpc, _wait,
)
from tests.support._legacy_reference_cleanup import _close_local
from tests.integration.test_output_owner_death_path import _close_reference, _pid_exists


pytestmark = pytest.mark.multiprocess_smoke


def test_second_source_loss_hands_off_first_replica_without_a_grant():
    context = core = report = None
    local = outer = foreign = consumer = None
    original_rpc = original_borrow = original_push = original_custody = original_clear = None
    pids, addresses = set(), set()
    entered, release, expired, disarmed, overflow = (threading.Event() for _ in range(5))
    observation_lock = threading.Lock()
    gated, observations, pushes, cancel_states, ack_states, cleared_states = [], [], [], [], [], []
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
        assert _replica(node_a, foreign.object_id, deadline).data is not None

        original_rpc, original_borrow, original_push = core._rpc, core._borrow_rpc, core._push_task_rpc
        original_custody, original_clear = core._record_replica_custody_locked, core._clear_protocol_unresolved

        def remember(kind, request, reply):
            with observation_lock:
                if len(observations) < 64:
                    observations.append((kind, request, reply))
                else:
                    overflow.set()

        def node_status(node):
            status = _rpc(node.node_address, SHUTDOWN_STATUS_HANDLER,
                          protocol.ShutdownStatusRequest("inspect-pregrant-custody-only"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
            assert status.child_pids == (node.worker_pid,)
            return status

        def observe_rpc(address, handler, request):
            target_request = (handler == REQUEST_LEASE_HANDLER and address == node_b.node_address
                              and request.target_node_id == node_b.node_id and len(request.dependencies) == 2)
            if target_request and not disarmed.is_set():
                with core._state_lock:
                    pending = core._task_finish_barriers[request.return_ids[0]]
                    frozen_spec = deepcopy(pending.spec)
                with observation_lock:
                    first = not gated
                    if first:
                        gated.append((request, deepcopy(request), frozen_spec, time.monotonic() + 8.0))
                if first:
                    entered.set()
                    if not release.wait(8.0):
                        expired.set()
                remember("target-lease-send", request, None)
            if handler == CANCEL_LEASE_HANDLER and not disarmed.is_set():
                with core._state_lock:
                    pending = core._task_finish_barriers[consumer.object_id]
                    state = core._protocol_unresolved[pending.task_key].obligation
                    assert type(state) is _LeaseCancellationState
                with observation_lock:
                    if len(cancel_states) < 2:
                        cancel_states.append((pending, state))
                    else:
                        overflow.set()
            if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER and not disarmed.is_set():
                with core._state_lock:
                    pending = core._task_finish_barriers[consumer.object_id]
                    state = core._protocol_unresolved[pending.task_key].obligation
                    assert type(state) is _LocationReportState and state.grant is None
                    owner = core.owner_table.snapshot(local.object_id)
                # Cancel releases lease pins, but is not the custody ACK.
                status = node_status(node_b)
                with observation_lock:
                    if len(ack_states) < 2:
                        ack_states.append((state, owner, status))
                    else:
                        overflow.set()
            reply = original_rpc(address, handler, request)
            if handler in (REQUEST_LEASE_HANDLER, CANCEL_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
                           protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER):
                remember(handler, request, reply)
            return reply

        def observe_borrow(address, handler, request):
            reply = original_borrow(address, handler, request)
            if handler in ("report_retained_object_location", "request_drop_owned_object", "release_owned_object_for_task"):
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
            (lease_request, frozen_request, frozen_spec, gate_deadline), = gated
            assert not pushes and not cancel_states and not ack_states
        gate_deadline = min(gate_deadline, deadline)
        assert lease_request.task_id == frozen_spec.task_id == consumer.object_id.task_id
        assert lease_request.attempt_id == frozen_spec.attempt_id and lease_request.attempt_id.attempt_number == 0
        assert lease_request.return_ids == frozen_spec.return_ids() == (consumer.object_id,)
        assert tuple(item.object_id for item in lease_request.dependencies) == (local.object_id, foreign.object_id)
        assert tuple(item.owner_worker_id for item in lease_request.dependencies) == (core.worker_id, node_a.worker_id)
        assert all(item.node_id == node_a.node_id for item in lease_request.dependencies)
        assert lease_request.dependencies[1] == foreign_before.descriptor
        assert all(not _replica(node_b, ref.object_id, gate_deadline).found for ref in (local, foreign))
        # The real borrowed-reference drop updates the foreign owner and A's
        # physical store. B still receives the original, unchanged wire DTO.
        assert ray.drop_object(foreign, node_id=node_a.node_id)
        assert not _replica(node_a, foreign.object_id, gate_deadline).found
        lost = _foreign_state(core, foreign, gate_deadline)
        assert lost.state is protocol.OwnedObjectState.LOST and lost.descriptor is None
        assert lost.current_attempt == foreign_before.current_attempt
        assert lease_request == frozen_request
        remember("source-drop-confirmed", frozen_request.dependencies[1], lost)
        assert not expired.is_set()
        release.set()
        with pytest.raises(LeaseRejectedError, match="DEPENDENCY_UNAVAILABLE") as failure:
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, deadline)
        with observation_lock:
            observed = tuple(observations)
            (pending, cancellation), = cancel_states
            (pre_ack, ack_owner, before_ack_status), = ack_states
            (handoff,) = tuple(cleared_states)
            assert not pushes
        assert lease_request == frozen_request and pending.spec == frozen_spec
        target_replies = tuple((request, reply) for kind, request, reply in observed
                               if kind == REQUEST_LEASE_HANDLER and request.target_node_id == node_b.node_id)
        ((rejected_request, rejection),) = target_replies
        assert rejected_request == frozen_request and type(rejection) is protocol.RejectWorkerLease
        assert rejection.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
        assert (rejection.lease_id, rejection.task_id, rejection.attempt_id, rejection.scheduling_key) == (
            frozen_request.lease_id, frozen_request.task_id, frozen_request.attempt_id, frozen_request.scheduling_key,
        )
        assert not any(type(reply) is protocol.GrantWorkerLease for _, _, reply in observed)
        assert not any(kind in (GET_WORKER_LEASE_OUTCOME_HANDLER, "report_retained_object_location") for kind, _, _ in observed)
        assert cancellation.known_grant is cancellation.reply is None and cancellation.round == 0
        assert cancellation.lease_request == frozen_request and cancellation.target_node_id == node_b.node_id
        assert cancellation.terminal_error is failure.value and type(failure.value) is LeaseRejectedError
        ((cancel_request, cancel_reply),) = tuple((request, reply) for kind, request, reply in observed if kind == CANCEL_LEASE_HANDLER)
        _assert_cancelled(cancel_reply, cancel_request)
        assert cancel_request == cancellation.request and cancel_request.lease_request == frozen_request
        assert not cancel_reply.released and cancel_reply.retired_grant is None
        inventory = cancel_reply.dependency_inventory
        expected_descriptor = replace(frozen_request.dependencies[0], node_id=node_b.node_id)
        assert inventory == protocol.LeaseDependencyInventory(frozen_request, node_b.node_id, (expected_descriptor,))
        ((local_descriptor, local_receipt),) = tuple((request, reply) for kind, request, reply in observed if kind == "local-custody")
        assert type(local_receipt) is _ReplicaLocationReceipt and local_receipt.accepted and local_receipt.custody_transferred
        assert local_descriptor == local_receipt.descriptor == expected_descriptor
        ((ack_request, ack_reply),) = tuple((request, reply) for kind, request, reply in observed
                                         if kind == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER)
        assert type(ack_request) is protocol.AckLeaseDependencyCustody
        assert ack_request.inventory == inventory and ack_request.requester_worker_id == core.worker_id
        assert type(ack_reply) is protocol.AckLeaseDependencyCustodyReply and ack_reply.accepted and ack_reply.request == ack_request
        assert not before_ack_status.resources_clean and not pre_ack.custody_acknowledged
        assert pre_ack.inventory == inventory and pre_ack.local_receipts == (local_receipt,)
        assert pending.dependency_hold in ack_owner.submitted_tokens
        assert pending.dependency_hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert handoff.grant is None and handoff.inventory == inventory and handoff.custody_acknowledged
        assert handoff.lease_request == frozen_request and handoff.cancellation_reply == cancel_reply
        assert handoff.terminal_error is pre_ack.terminal_error is failure.value and handoff.execution_outcome is None
        assert handoff.local_receipts == (local_receipt,) and handoff.reports == handoff.receipts == handoff.owner_deaths == ()
        remember_rejection = observed.index((REQUEST_LEASE_HANDLER, rejected_request, rejection))
        assert observed.index(("target-lease-send", frozen_request, None)) < remember_rejection
        assert remember_rejection < observed.index((CANCEL_LEASE_HANDLER, cancel_request, cancel_reply))
        ordered = ("source-drop-confirmed", "target-lease-send", CANCEL_LEASE_HANDLER,
                   "local-custody", protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER)
        indices = [next(index for index, (kind, _, _) in enumerate(observed) if kind == name) for name in ordered]
        assert indices == sorted(indices) and len(set(indices)) == len(indices)
        failed = core.owner_table.snapshot(consumer.object_id)
        assert failed.state is ObjectState.ERROR and failed.error is failure.value and failed.current_attempt == frozen_request.attempt_id
        record = core._recovery.task_record(consumer.object_id.task_id)
        assert record.retries_started == 0 and record.current_attempt == frozen_request.attempt_id
        assert core._recovery.active_recovery(consumer.object_id.task_id) is None
        local_after = core.owner_table.snapshot(local.object_id)
        assert local_after.state is ObjectState.READY_STORED and local_after.locations == frozenset((node_a.node_id, node_b.node_id))
        assert local_after.current_attempt == local_before.current_attempt and local_after.canonical_stored_result == local_before.canonical_stored_result
        assert ray.get(local, timeout=_remaining(deadline)) == _LOCAL_PAYLOAD
        assert _foreign_state(core, foreign, deadline) == lost
        physical = _replica(node_b, local.object_id, deadline)
        assert physical.found and physical.sealed and physical.checksum == expected_descriptor.checksum
        assert physical.producer_attempt_id == expected_descriptor.producer_attempt_id and physical.owner_worker_id == core.worker_id
        assert not _replica(node_b, foreign.object_id, deadline).found
        _poll(lambda: node_status(node_b).resources_clean, deadline, "pre-grant custody retained resources or pins")
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
        _poll(lambda: all(node_status(node).resources_clean for node in (node_a, node_b)), deadline, "Nodes did not return to idle")
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
