"""Pure exact physical-cleanup and retired-output metadata admission.

Two requests at most; one single-output owner fixture. No Core, Node, runtime,
thread, socket, process or wait. Typed replies are reducer inputs, not proof
of physical deletion; the real-store compositions verify that separately.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import NodeID, WorkerID
from miniray.errors import ProtocolError
from miniray.replica_cleanup import ReplicaCleanupQueue
from miniray.ownership import OutputOwnerPublicationConflictError
from tests.unit.test_output_owner_surviving_replica import _case, _resolution, _no_runtime


pytestmark = pytest.mark.unit


def _request():
    values, _owner, secondary = _case()
    slot = values.manifest.slots[0]
    return protocol.DropObjectReplica(slot.object_id, values.attempt, values.owner, secondary, slot.checksum)


def _reply(request, status=protocol.DropObjectReplicaStatus.DROPPED):
    return protocol.DropObjectReplicaReply(
        request.object_id, request.producer_attempt_id, request.owner_worker_id,
        request.node_id, request.checksum, status,
        None if status in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
        else "not a cleanup receipt",
    )


def test_claim_and_exact_receipt_are_distinct_from_pending_or_logical_retirement():
    request = _request()
    queue = ReplicaCleanupQueue()
    assert queue.enqueue(request) and not queue.enqueue(replace(request))
    assert queue.pending() == (request,) and queue.has_pending(request.object_id)
    assert queue.claim(request) and not queue.claim(request)
    assert queue.pending() == (request,)  # in-flight still fences shutdown
    queue.unclaim(request)
    assert queue.claim(request)
    assert queue.acknowledge(request, _reply(request))
    queue.unclaim(request)
    assert not queue.has_pending() and not queue.claim(request)
    assert not queue.enqueue(request)
    assert not queue.acknowledge(request, _reply(request, protocol.DropObjectReplicaStatus.ALREADY_DROPPED))
    assert queue.snapshot()[0].proof.status is protocol.DropObjectReplicaStatus.DROPPED


@pytest.mark.parametrize("status", (
    protocol.DropObjectReplicaStatus.PINNED, protocol.DropObjectReplicaStatus.STALE_EPOCH,
    protocol.DropObjectReplicaStatus.NODE_DRAINING, protocol.DropObjectReplicaStatus.INCONSISTENT,
    protocol.DropObjectReplicaStatus.REJECTED,
))
def test_non_receipts_never_release_exact_deletion_custody(status):
    request, queue = _request(), ReplicaCleanupQueue()
    queue.enqueue(request)
    before = queue.snapshot()
    assert not queue.acknowledge(request, _reply(request, status))
    assert queue.snapshot() == before and queue.has_pending()


@pytest.mark.parametrize("change", ("checksum", "node", "owner", "attempt", "truthy-status"))
def test_wrong_or_malformed_ack_leaves_pending_identity_unchanged(change):
    request, queue = _request(), ReplicaCleanupQueue()
    queue.enqueue(request)
    changed = request
    if change == "checksum":
        changed = replace(request, checksum="0" * 64)
    elif change == "node":
        changed = replace(request, node_id=NodeID.random())
    elif change == "owner":
        changed = replace(request, owner_worker_id=WorkerID.random())
    elif change == "attempt":
        changed = replace(request, producer_attempt_id=request.producer_attempt_id.next())
    reply = _reply(changed)
    if change == "truthy-status":
        object.__setattr__(reply, "status", True)
    before = queue.snapshot()
    with pytest.raises((TypeError, ValueError, ProtocolError)):
        queue.acknowledge(request, reply)
    assert queue.snapshot() == before


def test_request_and_snapshot_ids_cannot_mutate_queue_custody():
    request = _request()
    exact = deepcopy(request)
    queue = ReplicaCleanupQueue()
    queue.enqueue(request)
    object.__setattr__(request.node_id, "value", b"x" * 16)
    snapshot = queue.snapshot()[0]
    object.__setattr__(snapshot.request.object_id, "return_index", 99)
    pending = queue.pending()[0]
    object.__setattr__(pending.owner_worker_id, "value", b"y" * 16)
    assert queue.pending() == (exact,)
    assert queue.acknowledge(exact, _reply(exact))


def test_installed_node_death_discharges_only_that_node_without_fake_drop_ack():
    request, queue = _request(), ReplicaCleanupQueue()
    other = replace(request, node_id=NodeID.random())
    queue.enqueue(request)
    queue.enqueue(other)
    death = protocol.NodeDeathRecord(
        "queue-exit", request.node_id, 1901, 2, 4, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "already installed by Core",
    )
    completed = queue.acknowledge_node_death(death)
    assert completed == (request.object_id,)
    object.__setattr__(completed[0], "return_index", 99)
    assert queue.snapshot()[0].request == request
    assert queue.pending() == (other,)
    assert queue.snapshot()[0].proof == death
    assert queue.acknowledge_node_death(death) == ()
    with pytest.raises(ValueError):
        queue.acknowledge_node_death(replace(death, node_id=other.node_id, reason=protocol.NodeDeathReason.EXPECTED))
    assert queue.pending() == (other,)


def _descriptor(values, secondary):
    slot = values.manifest.slots[0]
    return protocol.ObjectStoreDescriptor(slot.object_id, values.owner, values.attempt, secondary,
                                          slot.size_bytes, slot.checksum)


def test_owner_late_replica_requires_latched_or_applied_retirement_not_current_route():
    values, owner, secondary = _case()
    descriptor = _descriptor(values, secondary)
    before = deepcopy(owner.snapshot(descriptor.object_id))
    assert owner.retired_output_replica(descriptor) is None
    drop = owner.retired_output_replica(descriptor, rejected_publications=(values.publication_id,))
    assert drop == protocol.DropObjectReplica(descriptor.object_id, values.attempt, values.owner, secondary, descriptor.checksum)
    assert owner.snapshot(descriptor.object_id) == before
    # Exact accepted release replies are explicit inputs to this owner-only
    # reduction; real child effects are tested by Core/Node compositions.
    cleanup = tuple(protocol.ReleaseContainedReferenceReply(
        transfer.contained_object_id, transfer.contained_owner_worker_id, hold, True, False,
    ) for transfer in values.manifest.slots[0].transfers
      for hold in (transfer.final_hold, transfer.provisional_hold))
    resolution = replace(_resolution(values), keep=False, cleanup=cleanup)
    assert owner.resolve_output_node_loss(values.manifest, resolution, values.envelope)
    assert owner.snapshot(descriptor.object_id).canonical_stored_result is None
    assert owner.retired_output_replica(descriptor) == drop
    # The original attempt stays deletable by exact history after reconstruction
    # changes current logical epoch; never derive drop from that new epoch.
    assert owner.advance_attempt(descriptor.object_id, expected_attempt=values.attempt,
                                 next_attempt=values.attempt.next())
    assert owner.retired_output_replica(descriptor) == drop
    assert owner.retired_output_replica(replace(descriptor, producer_attempt_id=values.attempt.next())) is None


@pytest.mark.parametrize("field", ("owner_worker_id", "size_bytes", "checksum"))
def test_mismatched_late_replica_metadata_never_authorizes_a_destructive_request(field):
    values, owner, secondary = _case()
    descriptor = _descriptor(values, secondary)
    changed = (WorkerID.random() if field == "owner_worker_id" else
               descriptor.size_bytes + 1 if field == "size_bytes" else "0" * 64)
    descriptor = replace(descriptor, **{field: changed})
    before = tuple(owner.snapshot(output) for output in values.publication_id.output_ids)
    with pytest.raises(OutputOwnerPublicationConflictError):
        owner.retired_output_replica(descriptor, rejected_publications=(values.publication_id,))
    assert tuple(owner.snapshot(output) for output in values.publication_id.output_ids) == before


@pytest.mark.parametrize("phase", ("collection", "retirement", "collected"))
def test_active_or_completed_collection_still_admits_only_exact_physical_cleanup(phase):
    values, owner, secondary = _case()
    descriptor = _descriptor(values, secondary)
    output = descriptor.object_id
    if phase in ("collection", "collected"):
        # Release all incoming reasons through their actual reducer APIs.
        before = owner.snapshot(output)
        for token in before.local_tokens:
            owner.release_local_reference(output, token)
        for hold in before.contained_holds:
            owner.release_contained_reference(output, hold)
        for token in before.lineage_tokens:
            owner.release_lineage_reference(output, token)
        plan = owner.begin_output_publication_collection(output, collection_id="late-report-collection")
        assert plan is not None
        if phase == "collected":
            # Current base collection binds the exact frozen owner plan.
            # Core owns real child/replica cleanup; no graph receipt is invented.
            owner.complete_output_publication_collection(plan)
            assert not owner.contains(output)
    else:
        owner.mark_lost(output, values.attempt)
        member = owner.output_owner_publication(output)
        owner.begin_output_publication_retirement((member,), retirement_id="late-report-retirement",
                                                  replica_locations={output: (values.node,)})
    before = deepcopy(owner._output_publication_receipts)
    request = owner.retired_output_replica(descriptor)
    assert request == protocol.DropObjectReplica(output, values.attempt, values.owner, secondary, descriptor.checksum)
    assert owner._output_publication_receipts == before
