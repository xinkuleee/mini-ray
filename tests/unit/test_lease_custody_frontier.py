"""Pure Node frontiers for freezing and acknowledging dependency inventory.

Reuse two passive Nodes with two 1 KiB stores and two tiny source values.
At most six source pin/chunk/release calls execute synchronously. There are no
Cores, GCS, processes, threads, sockets, waits, user tasks or unbounded loops.
An ACK here is the submitter's explicit protocol operation, not a claim that
these Node-only tests ran owner handoff. Replica deletion is not faked.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import WorkerID
from tests.unit.test_cancelled_grant_inventory import _NodeFixture, _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


def _registry(f):
    with f.target._state_lock:
        return f.target._dependency_custody_registry_locked()


def _full_cancel(request):
    return protocol.CancelWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        request.requester_node_id, request.requester_worker_id, request.scheduling_key,
        lease_request=request,
    )


def _ack(f, inventory):
    request = protocol.AckLeaseDependencyCustody(inventory.lease_request.requester_worker_id, inventory)
    reply = f.target._handle_ack_lease_dependency_custody(request)
    assert type(reply) is protocol.AckLeaseDependencyCustodyReply and reply.request == request
    return reply


@pytest.mark.parametrize("partial", (False, True), ids=("empty-before-localization", "partial-before-grant"))
def test_ungranted_inventory_cannot_be_acknowledged_before_exact_cancellation(monkeypatch, partial):
    f = _NodeFixture(monkeypatch, commit=False)
    cancel = _full_cancel(f.request)
    cancelled = False
    try:
        # A correctly routed request may enter localization and fail before
        # grant. A wrong-target request binds no localized bytes at this Node.
        if partial:
            second = f.descriptors[1]
            dropped = f.source._handle_drop_object_replica(protocol.DropObjectReplica(
                second.object_id, second.producer_attempt_id, second.owner_worker_id,
                second.node_id, second.checksum,
            ))
            assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
            actual_request = f.request
        else:
            actual_request = replace(f.request, target_node_id=f.source.node_id)
            cancel = _full_cancel(actual_request)
        rejected = f.target._handle_request_lease(actual_request)
        assert type(rejected) is protocol.RejectWorkerLease
        assert rejected.reason is (protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
                                   if partial else protocol.LeaseRejectReason.WRONG_TARGET)
        assert actual_request.lease_id not in f.target._leases
        inventory = _registry(f).snapshot(actual_request.lease_id)
        expected = (replace(f.descriptors[0], node_id=f.target.node_id),) if partial else ()
        assert inventory == protocol.LeaseDependencyInventory(actual_request, f.target.node_id, expected)
        assert _registry(f).has_pending() is partial
        before = deepcopy(inventory)
        refused = _ack(f, inventory)
        assert not refused.accepted and "grant or cancellation" in refused.error
        assert _registry(f).snapshot(actual_request.lease_id) == before
        assert _registry(f).has_pending() is partial
        cancellation = f.target._handle_cancel_worker_lease(cancel)
        cancelled = True
        assert cancellation.accepted and cancellation.cancelled and not cancellation.released
        assert cancellation.retired_grant is None and cancellation.dependency_inventory == before
        accepted = _ack(f, cancellation.dependency_inventory)
        assert accepted.accepted and not _registry(f).has_pending()
        assert _ack(f, cancellation.dependency_inventory) == accepted
        assert _registry(f).snapshot(actual_request.lease_id) == before
        if partial:
            assert len(f.transfers) == 5  # includes close fence for the rejected second Pin
            assert f.target.object_store.get(f.descriptors[0].object_id) == f.payloads[0]
            assert f.target.object_store.snapshot(f.descriptors[0].object_id).pin_count == 0
            assert not f.target.object_store.contains(f.descriptors[1].object_id, sealed_only=False)
        else:
            assert not f.transfers and f.target.object_store.used_bytes == 0
    finally:
        if not cancelled:
            reply = f.target._handle_cancel_worker_lease(cancel)
            assert reply.accepted and reply.cancelled


def test_committed_grant_accepts_exact_inventory_ack_without_cancelling_execution(monkeypatch):
    f = _NodeFixture(monkeypatch)
    try:
        inventory = _registry(f).snapshot(f.request.lease_id)
        assert inventory == protocol.LeaseDependencyInventory(f.request, f.target.node_id, f.grant.dependencies)
        assert _registry(f).has_pending()
        assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.GRANTED
        result = _ack(f, inventory)
        assert result.accepted and not _registry(f).has_pending()
        assert _ack(f, inventory) == result
        assert f.request.lease_id not in f.target._lease_cancellations
        assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert f.target.resource_ledger.available.is_zero()
        f.assert_replicas(pin_count=1)
    finally:
        cancelled = f.target._handle_cancel_worker_lease(_full_cancel(f.request))
        assert cancelled.accepted and cancelled.cancelled and cancelled.released
        assert not _registry(f).has_pending()
        f.assert_replicas(pin_count=0)
        assert f.target.resource_ledger.available == f.target.resource_ledger.total


def test_identity_only_cancel_tombstone_rejects_later_requests_without_binding_registry(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    first = f.target._handle_cancel_worker_lease(f.cancel)
    assert first.accepted and first.cancelled and first.dependency_inventory is None
    assert _registry(f).request(f.request.lease_id) is None
    other_worker = WorkerID(bytes(value ^ 1 for value in f.request.requester_worker_id.value))
    conflicting = replace(f.request, requester_worker_id=other_worker)
    for request in (conflicting, f.request):
        rejected = f.target._handle_request_lease(request)
        assert type(rejected) is protocol.RejectWorkerLease and rejected.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
        assert _registry(f).request(f.request.lease_id) is None
        assert _registry(f).snapshot(f.request.lease_id) is None
        assert f.target._handle_cancel_worker_lease(f.cancel) == first
    assert not f.transfers and not _registry(f).has_pending()
    assert f.target._leases == {} and f.target._lease_outcomes == {}
    assert f.target.object_store.used_bytes == 0


def test_full_cancel_cannot_upgrade_or_poison_an_identity_only_cancel_tombstone(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    first = f.target._handle_cancel_worker_lease(f.cancel)
    assert first.accepted and first.cancelled and first.dependency_inventory is None
    original = f.target._lease_cancellations[f.cancel.lease_id]
    other_worker = WorkerID(bytes(value ^ 1 for value in f.request.requester_worker_id.value))
    wrong_request = replace(f.request, requester_worker_id=other_worker)
    # Both are distinct CancelWorkerLease values, even when the second one's
    # legacy identity matches. One current immutable request is authoritative;
    # this is not an implicit upgrade to a full-request cancellation protocol.
    for request in (wrong_request, f.request):
        rejected = f.target._handle_cancel_worker_lease(_full_cancel(request))
        assert not rejected.accepted and not rejected.cancelled and not rejected.released
        assert rejected.dependency_inventory is None and rejected.retired_grant is None
        assert f.target._lease_cancellations[f.cancel.lease_id] == original
        assert _registry(f).request(f.request.lease_id) is None
        assert _registry(f).snapshot(f.request.lease_id) is None
        assert f.target._handle_cancel_worker_lease(f.cancel) == first
    assert not f.transfers and not _registry(f).has_pending()
    assert f.target._leases == {} and f.target._lease_outcomes == {}
    assert f.target.object_store.used_bytes == 0
