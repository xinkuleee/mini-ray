"""Pure shared-replica safety across two independent lease inventories.

Two passive Nodes, two 1 KiB stores and two tiny source values. Request one
seals its first input, then fails because its second source was really dropped.
Request two reuses that first replica locally and receives an actual pinned
grant. Cancelling/acknowledging request one cannot release request two's pin
or delete their shared bytes. Five source RPCs and one actual target seal; no
Core, GCS, process, thread, socket, wait or user task runs. Node-only explicit
ACKs exercise the protocol boundary, not end-to-end owner handoff.
"""

from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import AttemptID, LeaseID, ObjectID, TaskID
from tests.unit.test_cancelled_grant_inventory import _NodeFixture, _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


def _cancel(request):
    return protocol.CancelWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
        request.requester_worker_id, request.scheduling_key, lease_request=request,
    )


def _ack(node, inventory):
    request = protocol.AckLeaseDependencyCustody(inventory.lease_request.requester_worker_id, inventory)
    reply = node._handle_ack_lease_dependency_custody(request)
    assert type(reply) is protocol.AckLeaseDependencyCustodyReply
    assert reply.accepted and reply.request == request
    return reply


def test_cancelled_partial_inventory_does_not_delete_or_unpin_a_second_lease_replica(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    first, unavailable = f.descriptors
    second_request = None
    first_cancelled = second_cancelled = False
    seals = []
    original_seal = f.target.object_store.seal

    def observe_seal(object_id):
        original_seal(object_id)
        seals.append(object_id)
        assert seals == [first.object_id]

    monkeypatch.setattr(f.target.object_store, "seal", observe_seal)
    try:
        dropped = f.source._handle_drop_object_replica(protocol.DropObjectReplica(
            unavailable.object_id, unavailable.producer_attempt_id, unavailable.owner_worker_id,
            unavailable.node_id, unavailable.checksum,
        ))
        assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
        rejected = f.target._handle_request_lease(f.request)
        assert type(rejected) is protocol.RejectWorkerLease
        assert rejected.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
        assert f.request.lease_id not in f.target._leases
        with f.target._state_lock:
            registry = f.target._dependency_custody_registry_locked()
            partial = registry.snapshot(f.request.lease_id)
        local = replace(first, node_id=f.target.node_id)
        assert partial == protocol.LeaseDependencyInventory(f.request, f.target.node_id, (local,))
        assert registry.has_pending() and len(f.transfers) == 5 and seals == [first.object_id]
        assert f.target.object_store.get(first.object_id) == f.payloads[0]
        assert f.target.object_store.snapshot(first.object_id).pin_count == 0

        second_task = TaskID(b"r" * 16)
        second_request = protocol.RequestWorkerLease(
            LeaseID(b"l" * 16), second_task, AttemptID(second_task, 0), f.request.resources,
            f.request.requester_node_id, f.request.requester_worker_id, target_node_id=f.target.node_id,
            dependencies=(first,), return_ids=(ObjectID.for_task(second_task),),
        )
        grant = f.target._handle_request_lease(second_request)
        assert type(grant) is protocol.GrantWorkerLease and grant.dependencies == (local,)
        assert grant.lease_id == second_request.lease_id and grant.task_id == second_task
        assert len(f.transfers) == 5 and seals == [first.object_id]
        shared_bytes = f.target.object_store.used_bytes
        assert shared_bytes == first.size_bytes
        second_inventory = registry.snapshot(second_request.lease_id)
        assert second_inventory == protocol.LeaseDependencyInventory(second_request, f.target.node_id, (local,))
        assert f.target.object_store.snapshot(first.object_id).pin_count == 1
        assert f.target.resource_ledger.available.is_zero()

        reply = f.target._handle_cancel_worker_lease(_cancel(f.request))
        first_cancelled = True
        assert reply.accepted and reply.cancelled and not reply.released and reply.retired_grant is None
        assert reply.dependency_inventory == partial
        first_ack = _ack(f.target, partial)
        assert registry.has_pending(), "acknowledging the failed request also acknowledged its independent borrower"
        assert registry.snapshot(second_request.lease_id) == second_inventory
        for _ in range(2):
            replay = f.target._handle_cancel_worker_lease(_cancel(f.request))
            assert replay == reply
            assert _ack(f.target, partial) == first_ack
            assert f.target._leases[second_request.lease_id].state is protocol.LeaseExecutionState.GRANTED
            assert f.target.object_store.snapshot(first.object_id).pin_count == 1
            assert f.target.resource_ledger.available.is_zero()
            assert f.target.object_store.used_bytes == shared_bytes
            assert f.target.object_store.get(first.object_id) == f.source.object_store.get(first.object_id) == f.payloads[0]
            assert len(f.transfers) == 5 and seals == [first.object_id]

        _ack(f.target, second_inventory)
        assert not registry.has_pending()
        assert f.target.object_store.snapshot(first.object_id).pin_count == 1
        second_reply = f.target._handle_cancel_worker_lease(_cancel(second_request))
        second_cancelled = True
        assert second_reply.accepted and second_reply.cancelled and second_reply.released
        assert second_reply.retired_grant == grant and second_reply.dependency_inventory == second_inventory
        assert f.target.object_store.snapshot(first.object_id).pin_count == 0
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        assert f.target.object_store.get(first.object_id) == f.payloads[0]
        assert f.target.object_store.used_bytes == shared_bytes and not registry.has_pending()
        assert len(f.transfers) == 5 and seals == [first.object_id]
    finally:
        if not first_cancelled:
            assert f.target._handle_cancel_worker_lease(_cancel(f.request)).cancelled
        if second_request is not None and not second_cancelled:
            assert f.target._handle_cancel_worker_lease(_cancel(second_request)).cancelled
