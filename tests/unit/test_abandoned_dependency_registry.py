"""Pure authority and completion contracts for abandoned input custody.

Two byte-free input descriptors, one frozen lease request and typed reducer
receipts per case. No Node/Core/store, RPC, process, thread, wait or user code
is instantiated. Fence and owner receipts are explicit reducer inputs, not
claims that this metadata test independently observed physical cleanup.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import NodeID, WorkerID
from miniray.lease_dependencies import DependencyCustodyConflict, LeaseDependencyCustody
from tests.unit.test_abandoned_dependency_protocol import _fixture as _wire_fixture


pytestmark = pytest.mark.unit


def _case(*, foreign_only=False):
    request, full_inventory, death, _report = _wire_fixture()
    descriptors = full_inventory.descriptors[1:] if foreign_only else full_inventory.descriptors
    registry = LeaseDependencyCustody(full_inventory.node_id)
    registry.bind(request)
    for descriptor in descriptors:
        registry.begin(request, descriptor)
        registry.record(request, descriptor)
    inventory = registry.snapshot(request.lease_id)
    assert inventory == protocol.LeaseDependencyInventory(request, full_inventory.node_id, descriptors)
    return registry, inventory, death


def _owner_receipt(inventory, descriptor, death, status=protocol.RetainedLocationReportStatus.CUSTODY_ONLY):
    report = protocol.ReportAbandonedDependencyReplica(inventory, descriptor, death)
    error = "owner rejected this exact replica" if status in (
        protocol.RetainedLocationReportStatus.REJECTED, protocol.RetainedLocationReportStatus.STALE_PRODUCER,
    ) else None
    return protocol.ReportAbandonedDependencyReplicaReply(report, status, error)


def _fence(inventory, death, *, node_id=None, scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP, pinned=False):
    descriptor = inventory.descriptors[0]
    request = protocol.InstallOwnerDeathFence(
        "dead-input-owner-sweep", death, inventory.node_id if node_id is None else node_id,
        expected_replicas=(descriptor,) if scope is protocol.OwnerDeathFenceScope.PUBLICATION_EXACT else (),
        scope=scope,
    )
    observations = (() if not pinned and scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP else (
        protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.PINNED if pinned else protocol.OwnerDeathReplicaStatus.ABSENT,
            1 if pinned else 0,
        ),
    ))
    return protocol.InstallOwnerDeathFenceReply(request, protocol.OwnerDeathFenceDisposition.FENCED, observations)


def test_abandon_requires_the_exact_submitters_unexpected_death_and_freezes_its_proof():
    registry, inventory, death = _case()
    lease = inventory.lease_request.lease_id
    other = replace(death, incarnation=replace(death.incarnation, worker_id=WorkerID(b"x" * 16)))
    expected_exit = replace(death, reason=protocol.WorkerDeathReason.EXPECTED)
    for rejected in (other, expected_exit):
        with pytest.raises(DependencyCustodyConflict):
            registry.abandon(lease, rejected)
        assert registry._entries[lease].submitter_death is None and registry.has_pending()
    expected = deepcopy(death)
    registry.abandon(lease, death)
    object.__setattr__(death.incarnation.worker_id, "value", b"z" * 16)
    object.__setattr__(death.incarnation, "worker_pid", 9999)
    assert registry._entries[lease].submitter_death == expected
    registry.abandon(lease, deepcopy(expected))
    with pytest.raises(DependencyCustodyConflict, match="death changed"):
        registry.abandon(lease, replace(expected, detection_id="another-exit"))
    assert registry._entries[lease].submitter_death == expected
    assert registry._entries[lease].acknowledged is None


def test_cursor_visits_later_owner_and_retains_detached_receipts_without_overwriting_progress():
    registry, inventory, death = _case()
    lease = inventory.lease_request.lease_id
    first, second = inventory.descriptors
    registry.abandon(lease, death)
    assert registry.next_abandoned_descriptor(inventory) == first
    assert registry.next_abandoned_descriptor(inventory) == second
    receipt = _owner_receipt(inventory, second, death)
    expected = deepcopy(receipt)
    assert registry.record_abandoned_receipt(inventory, second, receipt)
    object.__setattr__(receipt.request.descriptor, "checksum", "b" * 64)
    object.__setattr__(receipt.request.submitter_death.incarnation, "worker_pid", 9999)
    assert registry.receipt(lease, second.object_id) == expected
    assert registry.next_abandoned_descriptor(inventory) == first
    changed_outcome = _owner_receipt(inventory, second, death, protocol.RetainedLocationReportStatus.RETIRED)
    assert not registry.record_abandoned_receipt(inventory, second, changed_outcome)
    assert registry.receipt(lease, second.object_id) == expected
    assert registry.record_abandoned_receipt(inventory, first, _fence(inventory, death))
    assert registry.next_abandoned_descriptor(inventory) is None
    registry.complete_abandoned(inventory)
    assert registry._entries[lease].abandoned_complete and registry._entries[lease].acknowledged is None
    assert not registry.has_pending()


def test_completion_waits_for_every_custody_or_dead_owner_receipt_without_faking_normal_ack():
    registry, inventory, death = _case()
    lease = inventory.lease_request.lease_id
    first, second = inventory.descriptors
    with pytest.raises(DependencyCustodyConflict, match="lacks owner custody"):
        registry.complete_abandoned(inventory)
    registry.abandon(lease, death)
    with pytest.raises(DependencyCustodyConflict, match="lacks owner custody"):
        registry.complete_abandoned(inventory)
    retired = _owner_receipt(inventory, second, death, protocol.RetainedLocationReportStatus.RETIRED)
    assert registry.record_abandoned_receipt(inventory, second, retired)
    with pytest.raises(DependencyCustodyConflict, match="lacks owner custody"):
        registry.complete_abandoned(inventory)
    assert not registry._entries[lease].abandoned_complete and registry.has_pending()
    assert registry.record_abandoned_receipt(inventory, first, _fence(inventory, death))
    registry.complete_abandoned(inventory)
    assert registry._entries[lease].abandoned_complete
    assert registry._entries[lease].acknowledged is None and not registry.has_pending()
    assert registry.abandoned_candidates() == ()


def test_wrong_inventory_death_or_non_custody_reply_cannot_discharge_abandoned_replica():
    registry, inventory, death = _case(foreign_only=True)
    lease = inventory.lease_request.lease_id
    descriptor, = inventory.descriptors
    registry.abandon(lease, death)
    changed_request = replace(inventory.lease_request, resources=inventory.lease_request.resources + inventory.lease_request.resources)
    wrong_inventory = protocol.LeaseDependencyInventory(changed_request, inventory.node_id, inventory.descriptors)
    wrong_death = replace(death, detection_id="different-death-proof")
    normal = protocol.ReportRetainedObjectLocation(
        descriptor.object_id, descriptor.owner_worker_id, inventory.lease_request.requester_worker_id,
        inventory.lease_request.dependency_owner_routes[1].hold, descriptor,
    )
    normal_reply = protocol.ReportRetainedObjectLocationReply(
        normal.object_id, normal.owner_worker_id, normal.borrower_worker_id, normal.hold, descriptor,
        protocol.RetainedLocationReportStatus.ADDED,
    )
    invalid = (
        _owner_receipt(wrong_inventory, descriptor, death),
        _owner_receipt(inventory, descriptor, wrong_death),
        _owner_receipt(inventory, descriptor, death, protocol.RetainedLocationReportStatus.REJECTED),
        _owner_receipt(inventory, descriptor, death, protocol.RetainedLocationReportStatus.STALE_PRODUCER),
        normal_reply,
    )
    for receipt in invalid:
        with pytest.raises(DependencyCustodyConflict):
            registry.record_abandoned_receipt(inventory, descriptor, receipt)
        assert registry.receipt(lease, descriptor.object_id) is None
        assert registry.has_pending() and not registry._entries[lease].abandoned_complete
    with pytest.raises(DependencyCustodyConflict, match="frozen inventory"):
        registry.record_abandoned_receipt(wrong_inventory, descriptor, _owner_receipt(wrong_inventory, descriptor, death))
    assert registry._entries[lease].owner_receipts == {}


def test_dead_owner_fence_receipt_must_match_owner_node_scope_and_complete_cleanup():
    registry, inventory, death = _case()
    lease = inventory.lease_request.lease_id
    descriptor = inventory.descriptors[0]
    registry.abandon(lease, death)
    other_death = replace(death, incarnation=replace(death.incarnation, worker_id=WorkerID(b"x" * 16)))
    invalid = (
        _fence(inventory, other_death),
        _fence(inventory, death, node_id=NodeID(b"z" * 16)),
        _fence(inventory, death, scope=protocol.OwnerDeathFenceScope.PUBLICATION_EXACT),
        _fence(inventory, death, pinned=True),
    )
    for receipt in invalid:
        with pytest.raises(DependencyCustodyConflict, match="dead owner cleanup"):
            registry.record_abandoned_receipt(inventory, descriptor, receipt)
        assert registry.receipt(lease, descriptor.object_id) is None
    complete = _fence(inventory, death)
    assert complete.complete and registry.record_abandoned_receipt(inventory, descriptor, complete)
    assert registry.receipt(lease, descriptor.object_id) == complete
    assert registry.has_pending()  # The live second owner still owes custody.


def test_completed_partial_inventory_cannot_grow_but_accepts_exact_normal_ack_replay():
    registry, inventory, death = _case(foreign_only=True)
    request = inventory.lease_request
    descriptor, = inventory.descriptors
    registry.abandon(request.lease_id, death)
    receipt = _owner_receipt(inventory, descriptor, death)
    assert registry.record_abandoned_receipt(inventory, descriptor, receipt)
    registry.complete_abandoned(inventory)
    previous_receipts = dict(registry._entries[request.lease_id].owner_receipts)
    new_descriptor = replace(request.dependencies[0], node_id=inventory.node_id)
    for operation in (registry.begin, registry.record):
        with pytest.raises(DependencyCustodyConflict, match="new replica"):
            operation(request, new_descriptor)
        assert registry.snapshot(request.lease_id) == inventory
        assert registry.candidates(request.lease_id) == () and not registry.has_pending()
    registry.begin(request, descriptor)
    registry.record(request, descriptor)
    assert registry._entries[request.lease_id].owner_receipts == previous_receipts
    assert registry._entries[request.lease_id].acknowledged is None
    assert registry.acknowledge(inventory)
    assert not registry.acknowledge(deepcopy(inventory))
    assert registry._entries[request.lease_id].abandoned_complete
    assert registry._entries[request.lease_id].acknowledged == inventory
    assert registry.abandoned_candidates() == () and not registry.has_pending()
