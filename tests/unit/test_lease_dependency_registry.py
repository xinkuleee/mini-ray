"""Pure Node-side registry transitions for dependency custody.

At most two request identities and two tiny descriptor identities per case.
No Core, Node, store, socket, process, thread, timer, wait or user function is
constructed. A record call supplies an assumed physical witness to this pure
reducer; actual Seal/metadata evidence is tested by pregrant composition cases.
The Node handler, not this registry, validates Grant/Cancel's finite frontier
before permitting an ACK, including an ACK of an explicitly empty inventory.
"""

from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.lease_dependencies import DependencyCustodyConflict, LeaseDependencyCustody
from tests.unit.test_lease_dependency_inventory import _inventory, _request


pytestmark = pytest.mark.unit


def _fixture():
    request = _request()
    registry = LeaseDependencyCustody(request.target_node_id)
    registry.bind(request)
    descriptors = tuple(replace(item, node_id=request.target_node_id) for item in request.dependencies)
    return registry, request, descriptors


def test_bind_and_request_detach_full_identity_and_preserve_exact_replay():
    registry, request, _ = _fixture()
    original = protocol.revalidate_worker_lease_request(request)
    frozen = registry.request(request.lease_id)
    assert frozen == original and frozen is not request
    assert frozen.resources is not request.resources
    assert frozen.dependencies[0] is not request.dependencies[0]
    assert frozen.target_execution is not request.target_execution
    assert frozen.scheduling_key is not request.scheduling_key
    registry.bind(frozen)
    assert registry.snapshot(original.lease_id).descriptors == ()
    assert not registry.has_pending()
    object.__setattr__(request.dependencies[0], "checksum", "d" * 64)
    object.__setattr__(request.resources, "_items", (("CPU", 999),))
    object.__setattr__(frozen.scheduling_key, "bundle_index", 8)
    assert registry.request(original.lease_id) == original
    with pytest.raises(DependencyCustodyConflict):
        registry.bind(request)
    with pytest.raises(DependencyCustodyConflict):
        registry.bind(frozen)
    assert registry.request(original.lease_id) == original
    assert len(registry._entries) == 1 and not registry.has_pending()


def test_bound_node_identity_does_not_alias_the_constructor_argument():
    node = NodeID(b"n" * 16)
    request = replace(_request(), scheduling_key=None, target_node_id=node)
    registry = LeaseDependencyCustody(node)
    registry.bind(request)
    expected = registry.snapshot(request.lease_id)
    assert expected.node_id == node
    object.__setattr__(node, "value", b"m" * 16)
    assert registry.snapshot(request.lease_id) == expected
    assert registry.node_id == expected.node_id and registry.node_id is not node


def test_record_accumulates_ordered_subset_across_exact_pending_request_replays():
    registry, request, (first, second) = _fixture()
    registry.begin(request, second)
    registry.record(request, second)
    partial = registry.snapshot(request.lease_id)
    assert partial.descriptors == (second,) and registry.has_pending()
    registry.bind(protocol.revalidate_worker_lease_request(request))
    assert registry.snapshot(request.lease_id) == partial
    registry.begin(request, first)
    registry.record(request, first)
    full = registry.snapshot(request.lease_id)
    assert full.descriptors == (first, second)
    registry.record(request, second)
    assert registry.snapshot(request.lease_id) == full
    assert partial.descriptors == (second,)
    object.__setattr__(first, "checksum", "d" * 64)
    object.__setattr__(full.descriptors[0], "checksum", "e" * 64)
    current = registry.snapshot(request.lease_id)
    assert current.descriptors[0].checksum == request.dependencies[0].checksum
    assert current.descriptors[1] == second and registry.has_pending()
    assert len(registry._entries) == 1


def test_begin_blocks_snapshot_and_ack_until_the_exact_candidate_is_reconciled():
    registry, request, (first, second) = _fixture()
    empty = registry.snapshot(request.lease_id)
    registry.begin(request, first)
    registry.begin(protocol.revalidate_worker_lease_request(request), replace(first))
    assert registry.has_pending()
    assert len(registry._entries[request.lease_id].pending) == 1
    with pytest.raises(DependencyCustodyConflict, match="reconciliation"):
        registry.snapshot(request.lease_id)
    with pytest.raises(DependencyCustodyConflict, match="reconciliation"):
        registry.acknowledge(empty)
    with pytest.raises(DependencyCustodyConflict):
        registry.discard_unsealed(request, replace(first, checksum="e" * 64))
    assert registry.has_pending()
    registry.record(request, first)
    settled = registry.snapshot(request.lease_id)
    assert settled.descriptors == (first,) and registry.has_pending()
    registry.begin(request, second)
    with pytest.raises(DependencyCustodyConflict):
        registry.snapshot(request.lease_id)
    # The caller's no-sealed-effect observation closes only this candidate; it
    # does not discard the first, already witnessed physical descriptor.
    registry.discard_unsealed(request, second)
    assert registry.snapshot(request.lease_id) == settled
    assert registry.acknowledge(settled)
    assert not registry.has_pending()


def test_empty_ack_requires_a_known_matching_request_and_is_idempotent():
    request = _request()
    inventory = _inventory((), request=request)
    registry = LeaseDependencyCustody(inventory.node_id)
    assert registry.request(request.lease_id) is None and registry.snapshot(request.lease_id) is None
    with pytest.raises(DependencyCustodyConflict):
        registry.acknowledge(inventory)
    assert not registry._entries
    registry.bind(request)
    assert not registry.has_pending()
    assert registry.acknowledge(inventory)
    assert not registry.acknowledge(protocol.revalidate_lease_dependency_inventory(inventory))
    assert registry.snapshot(request.lease_id) == inventory and not registry.has_pending()


@pytest.mark.parametrize("mismatch", ("node", "requester", "omission", "unrecorded"))
def test_mismatched_ack_does_not_relinquish_or_change_pending_custody(mismatch):
    registry, request, (first, _second) = _fixture()
    registry.record(request, first)
    actual = registry.snapshot(request.lease_id)
    if mismatch == "node":
        candidate = _inventory((0,), request=request, node_id=NodeID(b"x" * 16))
    elif mismatch == "requester":
        candidate = _inventory((0,), request=replace(request, requester_worker_id=WorkerID(b"w" * 16)))
    elif mismatch == "omission":
        candidate = _inventory((), request=request)
    else:
        candidate = _inventory(request=request)
    with pytest.raises(DependencyCustodyConflict):
        registry.acknowledge(candidate)
    assert registry.has_pending() and registry.snapshot(request.lease_id) == actual
    assert registry._entries[request.lease_id].acknowledged is None
    assert registry.acknowledge(actual) and not registry.has_pending()


def test_duplicate_ack_and_exact_record_do_not_make_settled_custody_dirty():
    registry, request, (first, _second) = _fixture()
    registry.record(request, first)
    inventory = registry.snapshot(request.lease_id)
    retained = protocol.revalidate_lease_dependency_inventory(inventory)
    assert registry.acknowledge(inventory)
    object.__setattr__(inventory.descriptors[0], "checksum", "d" * 64)
    assert registry.snapshot(request.lease_id) == retained and not registry.has_pending()
    assert not registry.acknowledge(retained)
    registry.bind(request)
    registry.record(request, first)
    assert registry.snapshot(request.lease_id) == retained and not registry.has_pending()
    assert not registry.acknowledge(retained)


@pytest.mark.parametrize("initial_slots", ((), (0,)), ids=("empty-frontier", "partial-frontier"))
def test_acknowledged_frontier_rejects_new_begin_and_record_without_pending_side_effect(initial_slots):
    registry, request, descriptors = _fixture()
    for index in initial_slots:
        registry.record(request, descriptors[index])
    inventory = registry.snapshot(request.lease_id)
    assert registry.acknowledge(inventory)
    new_descriptor = descriptors[1]
    with pytest.raises(DependencyCustodyConflict, match="acknowledged"):
        registry.begin(request, new_descriptor)
    assert registry.snapshot(request.lease_id) == inventory and not registry.has_pending()
    with pytest.raises(DependencyCustodyConflict, match="acknowledged"):
        registry.record(request, new_descriptor)
    assert registry.snapshot(request.lease_id) == inventory and not registry.has_pending()
    if initial_slots:
        registry.begin(request, descriptors[0])
        assert registry.snapshot(request.lease_id) == inventory and not registry.has_pending()
        registry.record(request, descriptors[0])
    assert not registry.acknowledge(inventory)


def test_two_leases_reusing_one_replica_require_independent_custody_acknowledgements():
    registry, first_request, (descriptor, _second) = _fixture()
    task = TaskID(b"q" * 16)
    second_request = replace(
        first_request, lease_id=LeaseID(b"l" * 16), task_id=task,
        attempt_id=type(first_request.attempt_id)(task, 0), target_execution=None,
        return_ids=(ObjectID.for_task(task),),
    )
    registry.bind(second_request)
    registry.begin(first_request, descriptor)
    registry.record(first_request, descriptor)
    registry.begin(second_request, descriptor)
    registry.record(second_request, descriptor)
    first = registry.snapshot(first_request.lease_id)
    second = registry.snapshot(second_request.lease_id)
    assert first.descriptors == second.descriptors == (descriptor,)
    assert first.lease_request != second.lease_request
    assert registry.acknowledge(first) and registry.has_pending()
    assert registry.snapshot(second_request.lease_id) == second
    assert not registry.acknowledge(first) and registry.has_pending()
    assert registry.acknowledge(second) and not registry.has_pending()
    assert not registry.acknowledge(second)
    assert len(registry._entries) == 2
