"""Pure NodeManager contracts for committed placement-group bundle ledgers."""

from __future__ import annotations

import threading
import os

import pytest

from miniray import protocol
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, ObjectID, PlacementGroupID, TaskID,
    WorkerID,
)
from miniray.node import NodeServer, _WorkerSlot
from miniray.object_store import ObjectStore
from miniray.placement import Bundle, BundleReservationLedger, ReservationState
from miniray.placement_group_runtime import (
    PlacementGroupAttempt, participant_digest,
)
from miniray.resources import (
    AllocationState, NodeSnapshot, ResourceLedger, ResourceVector,
)
from tests.unit._pure_node_output_current import prepare_ref_free_output


pytestmark = pytest.mark.unit


class _AliveProcess:
    pid = 7301
    exitcode = None

    def is_alive(self) -> bool:
        return True


def _rv(cpu: int) -> ResourceVector:
    return ResourceVector({"CPU": cpu})


def _node() -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    worker_id = WorkerID.random()
    node.num_workers_per_node = 1
    process = _AliveProcess()
    slot = _WorkerSlot(
        worker_id,
        process=process,
        address=("127.0.0.1", 27301),
        pid=process.pid,
    )
    node._worker_order = (worker_id,)
    node._workers = {worker_id: slot}

    node._ledger = ResourceLedger(_rv(2))
    node._bundle_reservations = BundleReservationLedger(node._ledger)
    node._placement_group_digests = {}
    node._cluster_nodes = (
        NodeSnapshot(node.node_id, node._ledger.total, node._ledger.available),
    )
    node._cluster_addresses = {}
    node._gcs_address = None
    node._registered_with_gcs = False
    node._node_pid = os.getpid()
    node._registration_epoch = 1
    node._membership_epoch = 1
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node._object_store = ObjectStore(1024)
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._dependency_pin_cleanups = {}
    node._pinned_transfers = {}
    node._object_localization_locks = {}
    node._actor_workers = {}
    node._actor_creation_locks = {}
    node._actor_finalize_results = {}
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_cancellations = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._worker_replacements_inflight = 0
    node._dead_worker_exitcodes = {}
    node._worker_drain_statuses = {}
    node._worker_finalize_results = {}
    node._shutdown_request_id = None
    node._stop_event = threading.Event()
    node._worker_supervisor_stop = threading.Event()
    node._worker_supervisor_thread = None
    node._worker_lifecycle_lock = threading.Lock()
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node.event_sink = None
    return node


def _enable_resource_reporting(
    node: NodeServer, monkeypatch: pytest.MonkeyPatch, *, fail: bool = False,
) -> list[protocol.UpdateNodeResources]:
    node._gcs_address = ("127.0.0.1", 27999)
    node._registered_with_gcs = True
    sent: list[protocol.UpdateNodeResources] = []

    def rpc(_address, handler, message):
        assert handler == "update_node_resources"
        assert not node._state_lock._is_owned()
        assert isinstance(message, protocol.UpdateNodeResources)
        sent.append(message)
        if fail:
            raise RuntimeError("GCS unavailable")
        return protocol.UpdateNodeResourcesReply(
            node.node_id, node._node_pid, node._registration_epoch,
            message.report_seq, True,
        )

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    return sent


def _participant(
    node: NodeServer,
    phase: protocol.PlacementGroupParticipantPhase,
    *,
    placement_group_id: PlacementGroupID | None = None,
    attempt: int = 0,
    bundles: tuple[Bundle, ...] = (Bundle(0, ResourceVector({"CPU": 1})),),
) -> protocol.PlacementGroupParticipantRequest:
    placement_group_id = placement_group_id or PlacementGroupID.random()
    digest = participant_digest(
        PlacementGroupAttempt(placement_group_id, attempt),
        node.node_id,
        bundles,
    )
    wire_bundles = tuple(
        protocol.PlacementGroupBundle(bundle.index, bundle.resources)
        for bundle in bundles
    )
    request_type = {
        protocol.PlacementGroupParticipantPhase.PREPARE:
            protocol.PreparePlacementGroupRequest,
        protocol.PlacementGroupParticipantPhase.COMMIT:
            protocol.CommitPlacementGroupRequest,
        protocol.PlacementGroupParticipantPhase.ABORT:
            protocol.AbortPlacementGroupRequest,
    }[phase]
    return request_type(
        placement_group_id, attempt, node.node_id, digest, phase, wire_bundles
    )


def _prepare_commit(
    node: NodeServer,
) -> tuple[
    protocol.PreparePlacementGroupRequest,
    protocol.CommitPlacementGroupRequest,
    protocol.PlacementGroupSchedulingKey,
]:
    prepare = _participant(
        node, protocol.PlacementGroupParticipantPhase.PREPARE
    )
    commit = _participant(
        node,
        protocol.PlacementGroupParticipantPhase.COMMIT,
        placement_group_id=prepare.placement_group_id,
        attempt=prepare.attempt,
    )
    assert node._handle_prepare_placement_group(prepare).applied
    assert node._handle_commit_placement_group(commit).applied
    key = protocol.PlacementGroupSchedulingKey(
        prepare.placement_group_id,
        prepare.attempt,
        prepare.bundles[0].bundle_index,
        node.node_id,
        prepare.plan_digest,
    )
    return prepare, commit, key


def _lease_request(
    node: NodeServer, key: protocol.PlacementGroupSchedulingKey,
) -> protocol.RequestWorkerLease:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    return protocol.RequestWorkerLease(
        LeaseID.random(), task, attempt, _rv(1), node.node_id,
        WorkerID.random(), target_node_id=node.node_id,
        return_ids=(ObjectID.for_task(task),), scheduling_key=key,
    )


def _abort_for(
    node: NodeServer, prepare: protocol.PreparePlacementGroupRequest,
) -> protocol.AbortPlacementGroupRequest:
    return protocol.AbortPlacementGroupRequest(
        prepare.placement_group_id,
        prepare.attempt,
        node.node_id,
        prepare.plan_digest,
        protocol.PlacementGroupParticipantPhase.ABORT,
        prepare.bundles,
    )


def test_participant_handlers_are_idempotent_and_fence_digest_drift() -> None:
    node = _node()
    prepare, commit, _key = _prepare_commit(node)

    replay_prepare = node._handle_prepare_placement_group(prepare)
    replay_commit = node._handle_commit_placement_group(commit)
    assert isinstance(replay_prepare, protocol.PreparePlacementGroupReply)
    assert isinstance(replay_commit, protocol.CommitPlacementGroupReply)
    assert replay_prepare.applied and replay_commit.applied
    root = node.resource_ledger
    assert root.available == _rv(1)
    assert len(root.snapshot().allocations) == 1

    wrong_first_node = _node()
    wrong_first = protocol.PreparePlacementGroupRequest(
        PlacementGroupID.random(), 0, wrong_first_node.node_id, "f" * 64,
        protocol.PlacementGroupParticipantPhase.PREPARE,
        (protocol.PlacementGroupBundle(0, _rv(1)),),
    )
    wrong_reply = wrong_first_node._handle_prepare_placement_group(wrong_first)
    assert not wrong_reply.accepted and "digest" in wrong_reply.error
    assert wrong_first_node.resource_ledger.available == _rv(2)

    changed = _participant(
        node,
        protocol.PlacementGroupParticipantPhase.PREPARE,
        placement_group_id=prepare.placement_group_id,
        attempt=prepare.attempt,
        bundles=(Bundle(0, _rv(2)),),
    )
    rejected = node._handle_prepare_placement_group(changed)
    assert not rejected.accepted and not rejected.applied
    assert "digest" in rejected.error
    assert root.available == _rv(1)
    assert len(root.snapshot().allocations) == 1

    abort = _abort_for(node, prepare)
    first_abort = node._handle_abort_placement_group(abort)
    replay_abort = node._handle_abort_placement_group(abort)
    assert isinstance(first_abort, protocol.AbortPlacementGroupReply)
    assert first_abort.applied and replay_abort.applied
    assert root.available == root.total
    assert node._bundle_reservations.snapshot(
        prepare.placement_group_id, prepare.attempt
    ).state is ReservationState.ABORTED


def test_pg_lease_uses_child_scope_for_yield_complete_and_removal() -> None:
    node = _node()
    prepare, _commit, key = _prepare_commit(node)
    root = node.resource_ledger
    child = node._bundle_reservations.ledger_for(
        key.placement_group_id, key.attempt, key.bundle_index
    )
    assert root.available == _rv(1)

    request = _lease_request(node, key)
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    assert grant.scheduling_key == key
    assert root.available == _rv(1)  # prepare charged root; lease did not debit twice.
    assert child.available.is_zero()
    record = node._leases[request.lease_id]
    assert record.allocation_ledger is child

    start = protocol.StartWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        grant.worker_id, key,
    )
    assert node._handle_start_worker_lease(start).scheduling_key == key
    blocked = protocol.NotifyWorkerBlocked(
        request.lease_id, request.task_id, request.attempt_id,
        grant.worker_id, 0,
    )
    unblocked = protocol.NotifyWorkerUnblocked(
        request.lease_id, request.task_id, request.attempt_id,
        grant.worker_id, 0,
    )
    assert node._handle_notify_worker_blocked(blocked).changed
    assert child.available == _rv(1) and root.available == _rv(1)
    assert node._handle_notify_worker_unblocked(unblocked).changed
    assert child.available.is_zero() and root.available == _rv(1)

    pending_abort = node._handle_abort_placement_group(_abort_for(node, prepare))
    assert pending_abort.accepted and not pending_abort.applied
    assert node._bundle_reservations.snapshot(
        key.placement_group_id, key.attempt
    ).state is ReservationState.REMOVING
    rejected = node._handle_request_lease(_lease_request(node, key))
    assert isinstance(rejected, protocol.RejectWorkerLease)
    assert rejected.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
    assert root.available == _rv(1)

    complete = protocol.CompleteWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED, key,
    )
    publication = prepare_ref_free_output(node, request, grant)
    completed = node._handle_complete_worker_lease(complete)
    assert completed.accepted and completed.released
    assert completed.output_publication.manifest == publication.manifest
    assert completed.scheduling_key == key
    assert child.available == child.total
    assert root.available == root.total
    assert node._bundle_reservations.snapshot(
        key.placement_group_id, key.attempt
    ).state is ReservationState.ABORTED


@pytest.mark.parametrize("terminal", ["cancel", "crash"])
def test_cancel_and_crash_reclaim_the_original_child_scope(terminal: str) -> None:
    node = _node()
    prepare, _commit, key = _prepare_commit(node)
    root = node.resource_ledger
    child = node._bundle_reservations.ledger_for(
        key.placement_group_id, key.attempt, key.bundle_index
    )
    request = _lease_request(node, key)
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    assert node._handle_abort_placement_group(
        _abort_for(node, prepare)
    ).applied is False

    if terminal == "cancel":
        cancelled = node._handle_cancel_worker_lease(
            protocol.CancelWorkerLease(
                request.lease_id, request.task_id, request.attempt_id,
                request.requester_node_id, request.requester_worker_id, key,
            )
        )
        assert cancelled.cancelled and cancelled.released
        assert cancelled.scheduling_key == key
    else:
        assert node._handle_start_worker_lease(
            protocol.StartWorkerLease(
                request.lease_id, request.task_id, request.attempt_id,
                grant.worker_id, key,
            )
        ).accepted
        assert node._reclaim_active_lease_after_worker_exit_locked(
            grant.worker_id
        )

    assert child.available == child.total
    assert root.available == root.total
    assert node._leases[request.lease_id].state in (
        protocol.LeaseExecutionState.ABANDONED,
        protocol.LeaseExecutionState.WORKER_LOST,
    )


def test_begin_drain_fences_pg_and_releases_root_only_after_child_terminal() -> None:
    node = _node()
    prepare, _commit, key = _prepare_commit(node)
    request = _lease_request(node, key)
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    root = node.resource_ledger

    node._install_worker_drain_fence("pg-drain")
    assert node._bundle_reservations.snapshot(
        key.placement_group_id, key.attempt
    ).state is ReservationState.REMOVING
    assert root.available == _rv(1)

    cancelled = node._handle_cancel_worker_lease(
        protocol.CancelWorkerLease(
            request.lease_id, request.task_id, request.attempt_id,
            request.requester_node_id, request.requester_worker_id, key,
        )
    )
    assert cancelled.released
    assert root.available == root.total
    assert node._bundle_reservations.snapshot(
        prepare.placement_group_id, prepare.attempt
    ).state is ReservationState.ABORTED
    assert all(
        record.state is AllocationState.RELEASED
        for record in root.snapshot().allocations
    )


@pytest.mark.parametrize("requested", [_rv(1), ResourceVector.empty()])
def test_removal_does_not_release_root_for_yielded_or_zero_resource_lease(
    requested: ResourceVector,
) -> None:
    node = _node()
    prepare, _commit, key = _prepare_commit(node)
    request = _lease_request(node, key)
    request = protocol.RequestWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, requested,
        request.requester_node_id, request.requester_worker_id,
        target_node_id=request.target_node_id, return_ids=request.return_ids,
        scheduling_key=key,
    )
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    assert node._handle_start_worker_lease(
        protocol.StartWorkerLease(
            request.lease_id, request.task_id, request.attempt_id,
            grant.worker_id, key,
        )
    ).accepted
    if requested.units("CPU"):
        assert node._handle_notify_worker_blocked(
            protocol.NotifyWorkerBlocked(
                request.lease_id, request.task_id, request.attempt_id,
                grant.worker_id, 0,
            )
        ).changed

    pending = node._handle_abort_placement_group(_abort_for(node, prepare))
    assert pending.accepted and not pending.applied
    assert node.resource_ledger.available == _rv(1)
    assert node._bundle_reservations.snapshot(
        key.placement_group_id, key.attempt
    ).state is ReservationState.REMOVING

    assert node._reclaim_active_lease_after_worker_exit_locked(grant.worker_id)
    assert node.resource_ledger.available == node.resource_ledger.total


def test_pg_prepare_and_terminal_root_release_report_latest_hint_outside_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    sent = _enable_resource_reporting(node, monkeypatch)
    prepare, _commit, key = _prepare_commit(node)
    assert [message.available_resources for message in sent] == [_rv(1)]

    request = _lease_request(node, key)
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    pending = node._handle_abort_placement_group(_abort_for(node, prepare))
    assert pending.accepted and not pending.applied
    assert [message.available_resources for message in sent] == [_rv(1)]

    assert node._handle_start_worker_lease(
        protocol.StartWorkerLease(
            request.lease_id, request.task_id, request.attempt_id,
            grant.worker_id, key,
        )
    ).accepted
    prepare_ref_free_output(node, request, grant)
    completed = node._handle_complete_worker_lease(
        protocol.CompleteWorkerLease(
            request.lease_id, request.task_id, request.attempt_id,
            grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED, key,
        )
    )
    assert completed.accepted
    # Unified Complete returns after the local root release. Its availability
    # hint is an outbox obligation, not a GCS round trip on that reply path.
    assert node.resource_ledger.available == _rv(2)
    assert [message.available_resources for message in sent] == [_rv(1)]
    assert node._resource_report_version > node._resource_reported_version
    assert node._flush_pending_resource_report()
    assert [message.available_resources for message in sent] == [_rv(1), _rv(2)]


def test_failed_pg_resource_report_never_rolls_back_local_ledger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    sent = _enable_resource_reporting(node, monkeypatch, fail=True)
    prepare = _participant(
        node, protocol.PlacementGroupParticipantPhase.PREPARE
    )

    reply = node._handle_prepare_placement_group(prepare)

    assert reply.accepted and reply.applied
    assert len(sent) == 1
    assert sent[0].available_resources == _rv(1)
    assert node.resource_ledger.available == _rv(1)
    assert node._bundle_reservations.snapshot(
        prepare.placement_group_id, prepare.attempt
    ).state is ReservationState.PREPARED
    assert node._resource_report_version > node._resource_reported_version


def test_pg_request_larger_than_child_is_infeasible_without_root_fallback() -> None:
    node = _node()
    _prepare, _commit, key = _prepare_commit(node)
    root_before = node.resource_ledger.snapshot()
    child = node._bundle_reservations.ledger_for(
        key.placement_group_id, key.attempt, key.bundle_index
    )
    child_before = child.snapshot()
    request = _lease_request(node, key)
    request = protocol.RequestWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, _rv(2),
        request.requester_node_id, request.requester_worker_id,
        target_node_id=node.node_id, return_ids=request.return_ids,
        scheduling_key=key,
    )

    reply = node._handle_request_lease(request)

    assert isinstance(reply, protocol.RejectWorkerLease)
    assert reply.reason is protocol.LeaseRejectReason.INFEASIBLE
    assert node.resource_ledger.snapshot() == root_before
    assert child.snapshot() == child_before
    assert request.lease_id not in node._leases


def test_ordinary_root_lease_reports_grant_release_and_retries_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    node._gcs_address = ("127.0.0.1", 27999)
    node._registered_with_gcs = True
    sent: list[ResourceVector] = []
    fail_once = True

    def rpc(_address, handler, message):
        nonlocal fail_once
        assert handler == "update_node_resources"
        assert not node._state_lock._is_owned()
        sent.append(message.available_resources)
        if fail_once:
            fail_once = False
            raise RuntimeError("transient GCS failure")
        return protocol.UpdateNodeResourcesReply(
            node.node_id, node._node_pid, node._registration_epoch,
            message.report_seq, True,
        )

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    request = protocol.RequestWorkerLease(
        LeaseID.random(), task, AttemptID(task, 0), _rv(1), node.node_id,
        WorkerID.random(), target_node_id=node.node_id,
    )

    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    assert sent == [_rv(1)]
    assert node._resource_report_version > node._resource_reported_version

    # Exact replay is later Node traffic: it retries the pending latest hint.
    assert node._handle_request_lease(request) == grant
    assert sent == [_rv(1), _rv(1)]
    assert node._resource_report_version == node._resource_reported_version

    released = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            grant.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert released.released
    assert sent == [_rv(1), _rv(1), _rv(2)]
    assert node._resource_report_version == node._resource_reported_version
