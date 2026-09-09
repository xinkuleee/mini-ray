"""Pure wire contracts for request-scoped dependency custody inventory.

Each fixture has two three-byte dependency identities, one output at attempt
two and no physical store. Constructors, deep validators and pickle
round-trips alone run: no Core/Node, listener, process, thread, wait or user
function. The fixed corruption tables never grow or retry at runtime.
"""

from dataclasses import fields, replace
import pickle

import pytest

from miniray import protocol
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, PlacementGroupID, TaskID, WorkerID
from miniray.resources import AllocationToken, ResourceVector


pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes((number,)) * 16)


def _request():
    task = _id(TaskID, 1)
    source, target, home = (_id(NodeID, value) for value in (3, 4, 5))
    attempt = AttemptID(task, 2)
    dependencies = tuple(
        protocol.ObjectStoreDescriptor(
            ObjectID.for_task(_id(TaskID, 11 + index)), _id(WorkerID, 7 + index),
            AttemptID(_id(TaskID, 11 + index), 1), source, 3, checksum * 64,
        )
        for index, checksum in enumerate(("a", "c"))
    )
    return protocol.RequestWorkerLease(
        _id(LeaseID, 2), task, attempt,
        ResourceVector({"CPU": "0.125", "custom": 2}), home, _id(WorkerID, 6),
        preferred_node_id=home, target_node_id=target, dependencies=dependencies,
        return_ids=(ObjectID.for_task(task),),
        scheduling_key=protocol.PlacementGroupSchedulingKey(
            _id(PlacementGroupID, 9), 1, 0, target, "b" * 64,
        ),
    )


def _inventory(slots=(0, 1), *, request=None, node_id=None):
    request = _request() if request is None else request
    node_id = request.target_node_id if node_id is None else node_id
    return protocol.LeaseDependencyInventory(
        request, node_id, tuple(replace(request.dependencies[index], node_id=node_id) for index in slots),
    )


def _grant(inventory):
    request = inventory.lease_request
    return protocol.GrantWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, inventory.node_id,
        _id(WorkerID, 20), ("worker.invalid", 21), AllocationToken("inventory-grant"),
        tuple(replace(item, node_id=inventory.node_id) for item in request.dependencies),
        request.scheduling_key,
    )


def _cancel(request):
    return protocol.CancelWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        request.requester_node_id, request.requester_worker_id, request.scheduling_key,
        lease_request=request,
    )


def _reply(inventory, *, grant=None, **changes):
    request = inventory.lease_request
    values = dict(
        lease_id=request.lease_id, task_id=request.task_id, attempt_id=request.attempt_id,
        requester_node_id=request.requester_node_id, requester_worker_id=request.requester_worker_id,
        state=protocol.LeaseExecutionState.ABANDONED, accepted=True, cancelled=True,
        released=grant is not None, scheduling_key=request.scheduling_key,
        retired_grant=grant, dependency_inventory=inventory,
    )
    values.update(changes)
    return protocol.CancelWorkerLeaseReply(**values)


def _corrupt(value, path, replacement):
    parts = path.split(".")
    for part in parts[:-1]:
        value = value[int(part)] if part.isdigit() else getattr(value, part)
    object.__setattr__(value, parts[-1], replacement)


@pytest.mark.parametrize("slots", ((), (0,), (1,), (0, 1)), ids=("empty", "prefix", "non-prefix", "full"))
def test_inventory_roundtrip_preserves_original_request_and_order_without_execution_capability(slots):
    request = _request()
    inventory = _inventory(slots, request=request)
    rebuilt = protocol.revalidate_lease_dependency_inventory(inventory)
    assert pickle.loads(pickle.dumps(inventory)) == rebuilt == inventory
    assert inventory.lease_request == request and inventory.lease_request is not request
    assert inventory.descriptors == tuple(
        replace(request.dependencies[index], node_id=inventory.node_id) for index in slots
    )
    assert tuple(field.name for field in fields(inventory)) == ("lease_request", "node_id", "descriptors")
    assert not hasattr(inventory, "worker_id") and not hasattr(inventory, "allocation_token")


def test_inventory_detaches_all_nested_request_resources_manifest_and_descriptors():
    original = _request()
    rebuilt = protocol.revalidate_worker_lease_request(original)
    inventory = _inventory(request=original)
    frozen = pickle.loads(pickle.dumps(inventory))
    assert rebuilt == original and rebuilt is not original
    assert rebuilt.resources is not original.resources and rebuilt.resources.units("CPU") == 125
    assert rebuilt.lease_id is not original.lease_id
    assert rebuilt.requester_worker_id is not original.requester_worker_id
    assert rebuilt.preferred_node_id is not original.preferred_node_id
    assert rebuilt.target_node_id is not original.target_node_id
    assert rebuilt.dependencies[0] is not original.dependencies[0]
    assert rebuilt.dependencies[0].object_id is not original.dependencies[0].object_id
    assert rebuilt.dependencies[0].producer_attempt_id is not original.dependencies[0].producer_attempt_id
    assert rebuilt.return_ids[0] is not original.return_ids[0]
    assert rebuilt.attempt_id is not original.attempt_id
    assert rebuilt.attempt_id.task_id is not original.attempt_id.task_id
    assert rebuilt.scheduling_key is not original.scheduling_key
    object.__setattr__(original.resources, "_items", (("CPU", 999),))
    object.__setattr__(original.dependencies[0], "checksum", "d" * 64)
    object.__setattr__(original.return_ids[0], "return_index", 1)
    object.__setattr__(original.scheduling_key, "bundle_index", 8)
    assert inventory == frozen and rebuilt == frozen.lease_request
    assert inventory.descriptors[0].checksum == "a" * 64


@pytest.mark.parametrize(("path", "replacement"), (
    ("lease_id.value", b"short"),
    ("attempt_id.attempt_number", True),
    ("requester_node_id.value", b"short"),
    ("requester_worker_id.value", b"short"),
    ("preferred_node_id.value", b"short"),
    ("target_node_id.value", b"short"),
    ("dependencies.0.owner_worker_id.value", b"short"),
    ("dependencies.0.producer_attempt_id.attempt_number", True),
    ("dependencies.0.size_bytes", True),
    ("dependencies.0.checksum", "z" * 64),
    ("return_ids.0.return_index", False),
    ("scheduling_key.bundle_index", True),
    ("attempt_id.task_id.value", b"short"),
    ("return_ids.0.task_id.value", b"short"),
))
def test_full_request_deep_revalidation_rejects_nested_corruption(path, replacement):
    request = _request()
    _corrupt(request, path, replacement)
    with pytest.raises(protocol.ProtocolError):
        protocol.revalidate_worker_lease_request(request)
    with pytest.raises(protocol.ProtocolError):
        _inventory((), request=request)


def test_resource_units_are_validated_before_mapping_conversion_can_hide_corruption():
    invalid_entries = (
        (("CPU", True),), (("CPU", 1.0),), (("CPU", -1),), (("CPU", 0),),
        ((" CPU", 1),), (("", 1),), (("CPU", 1), ("CPU", 2)),
        (("custom", 1), ("CPU", 2)), (("CPU",),), ((1, 1000),),
    )
    for entries in invalid_entries:
        request = _request()
        object.__setattr__(request.resources, "_items", entries)
        with pytest.raises(protocol.ProtocolError):
            protocol.revalidate_worker_lease_request(request)
    empty = replace(_request(), resources=ResourceVector())
    assert protocol.revalidate_worker_lease_request(empty).resources.is_zero()


@pytest.mark.parametrize("defect", ("duplicate", "reverse", "unrequested", "owner", "epoch", "node"))
def test_inventory_rejects_unordered_or_logically_different_subset(defect):
    inventory = _inventory()
    first, second = inventory.descriptors
    if defect == "duplicate":
        descriptors = (first, first)
    elif defect == "reverse":
        descriptors = (second, first)
    elif defect == "unrequested":
        task = _id(TaskID, 30)
        descriptors = (replace(first, object_id=ObjectID.for_task(task), producer_attempt_id=AttemptID(task, 1)),)
    elif defect == "owner":
        descriptors = (replace(first, owner_worker_id=_id(WorkerID, 31)),)
    elif defect == "epoch":
        descriptors = (replace(first, producer_attempt_id=first.producer_attempt_id.next()),)
    else:
        descriptors = (replace(first, node_id=_id(NodeID, 32)),)
    with pytest.raises(protocol.ProtocolError):
        replace(inventory, descriptors=descriptors)


def test_cancel_binds_and_detaches_full_original_request_even_before_node_admission():
    request = _request()
    cancellation = _cancel(request)
    saved = pickle.loads(pickle.dumps(cancellation))
    assert cancellation == saved and cancellation.lease_request == request
    assert cancellation.lease_request is not request
    assert cancellation.lease_request.resources is not request.resources
    object.__setattr__(request.dependencies[0], "checksum", "d" * 64)
    assert cancellation == saved
    assert fields(protocol.CancelWorkerLease)[-1].name == "lease_request"


@pytest.mark.parametrize("field", ("lease", "task", "requester-node", "requester-worker", "pg"))
def test_cancel_rejects_outer_identity_that_disagrees_with_full_request(field):
    cancellation = _cancel(_request())
    if field == "lease":
        changes = dict(lease_id=_id(LeaseID, 33))
    elif field == "task":
        task = _id(TaskID, 34)
        changes = dict(task_id=task, attempt_id=AttemptID(task, 2))
    elif field == "requester-node":
        changes = dict(requester_node_id=_id(NodeID, 35))
    elif field == "requester-worker":
        changes = dict(requester_worker_id=_id(WorkerID, 36))
    else:
        changes = dict(scheduling_key=replace(cancellation.scheduling_key, bundle_index=1))
    with pytest.raises(protocol.ProtocolError):
        replace(cancellation, **changes)


def test_cancel_reply_requires_complete_inventory_matching_retired_grant():
    inventory = _inventory()
    grant = _grant(inventory)
    reply = _reply(inventory, grant=grant)
    assert pickle.loads(pickle.dumps(reply)) == reply
    assert reply.dependency_inventory == inventory and reply.dependency_inventory is not inventory
    assert reply.retired_grant == grant and reply.retired_grant is not grant
    request = inventory.lease_request
    bad_requests = (
        replace(request, requester_worker_id=_id(WorkerID, 37)),
        replace(request, dependencies=(replace(request.dependencies[0], checksum="e" * 64), request.dependencies[1])),
        replace(request, task_id=_id(TaskID, 38), attempt_id=AttemptID(_id(TaskID, 38), 2),
                return_ids=(ObjectID.for_task(_id(TaskID, 38)),)),
    )
    for candidate in (replace(inventory, descriptors=inventory.descriptors[:1]),
                      *(_inventory(request=item) for item in bad_requests)):
        with pytest.raises(protocol.ProtocolError):
            replace(reply, dependency_inventory=candidate)
    changed_grant = replace(grant, dependencies=grant.dependencies[:1])
    with pytest.raises(protocol.ProtocolError):
        replace(reply, retired_grant=changed_grant)
    assert "dependency_inventory" in {field.name for field in fields(protocol.CancelWorkerLeaseReply)}


def test_no_grant_cancel_can_echo_empty_inventory_but_not_execution_permission():
    inventory = _inventory(())
    reply = _reply(inventory)
    assert reply.retired_grant is None and reply.dependency_inventory.descriptors == ()
    assert reply.dependency_inventory.lease_request.dependencies == _request().dependencies
    assert reply.accepted and reply.cancelled and not reply.released
    assert pickle.loads(pickle.dumps(reply)) == reply
    for state in (protocol.LeaseExecutionState.GRANTED, protocol.LeaseExecutionState.RUNNING,
                  protocol.LeaseExecutionState.COMPLETED):
        with pytest.raises(protocol.ProtocolError):
            replace(reply, state=state, accepted=False, cancelled=False, released=False, error="not cancelled")
    lost = _reply(inventory, state=protocol.LeaseExecutionState.WORKER_LOST,
                  accepted=False, cancelled=False, released=False, error="executor lost")
    assert pickle.loads(pickle.dumps(lost)) == lost and not lost.cancelled


def test_ack_uses_original_requester_and_reply_echoes_detached_exact_inventory():
    inventory = _inventory((1,))
    request = protocol.AckLeaseDependencyCustody(inventory.lease_request.requester_worker_id, inventory)
    reply = protocol.AckLeaseDependencyCustodyReply(request, True)
    assert protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER == "ack_lease_dependency_custody"
    assert pickle.loads(pickle.dumps(reply)) == reply
    assert reply.request == request and reply.request is not request
    assert reply.request.inventory is not request.inventory
    assert reply.request.inventory.lease_request.resources is not inventory.lease_request.resources
    with pytest.raises(protocol.ProtocolError):
        replace(request, requester_worker_id=_id(WorkerID, 38))
    with pytest.raises(protocol.ProtocolError):
        replace(reply, accepted=True, error="contradiction")
    with pytest.raises(protocol.ProtocolError):
        replace(reply, accepted=False)
    rejected = replace(reply, accepted=False, error="inventory still unconfirmed")
    assert rejected.request == request and not rejected.accepted


def test_nested_inventory_corruption_is_rejected_by_entry_and_pickle_revalidation():
    for build in (
        lambda inv: inv,
        lambda inv: _reply(inv),
        lambda inv: protocol.AckLeaseDependencyCustody(inv.lease_request.requester_worker_id, inv),
        lambda inv: protocol.AckLeaseDependencyCustodyReply(
            protocol.AckLeaseDependencyCustody(inv.lease_request.requester_worker_id, inv), True,
        ),
    ):
        message = build(_inventory())
        nested = (message if type(message) is protocol.LeaseDependencyInventory
                  else message.dependency_inventory if type(message) is protocol.CancelWorkerLeaseReply
                  else message.inventory if type(message) is protocol.AckLeaseDependencyCustody
                  else message.request.inventory)
        object.__setattr__(nested.descriptors[0], "checksum", "bad")
        with pytest.raises(protocol.ProtocolError):
            replace(message)
        with pytest.raises(protocol.ProtocolError):
            pickle.loads(pickle.dumps(message))
    cancellation = _cancel(_request())
    object.__delattr__(cancellation.lease_request, "requester_node_id")
    with pytest.raises(protocol.ProtocolError):
        pickle.loads(pickle.dumps(cancellation))


def test_identity_only_cancel_and_empty_probe_request_keep_their_existing_surface():
    original = _request()
    cancellation = protocol.CancelWorkerLease(
        original.lease_id, original.task_id, original.attempt_id,
        original.requester_node_id, original.requester_worker_id, original.scheduling_key,
    )
    assert cancellation.lease_request is None and pickle.loads(pickle.dumps(cancellation)) == cancellation
    assert cancellation.lease_id is not original.lease_id
    assert cancellation.attempt_id is not original.attempt_id
    assert cancellation.scheduling_key is not original.scheduling_key
    legacy_reply = protocol.CancelWorkerLeaseReply(
        cancellation.lease_id, cancellation.task_id, cancellation.attempt_id,
        cancellation.requester_node_id, cancellation.requester_worker_id,
        protocol.LeaseExecutionState.ABANDONED, True, True, False,
        scheduling_key=cancellation.scheduling_key,
    )
    assert legacy_reply.lease_id is not cancellation.lease_id
    assert legacy_reply.scheduling_key is not cancellation.scheduling_key
    object.__setattr__(cancellation.attempt_id, "attempt_number", True)
    with pytest.raises(protocol.ProtocolError):
        replace(cancellation)
    with pytest.raises(protocol.ProtocolError):
        pickle.loads(pickle.dumps(cancellation))
    assert legacy_reply.attempt_id.attempt_number == 2
    probe = replace(original, dependencies=(), return_ids=(), scheduling_key=None)
    assert protocol.revalidate_worker_lease_request(probe) == probe
    empty = _inventory((), request=probe)
    assert not empty.descriptors and not empty.lease_request.return_ids
    assert _reply(empty).dependency_inventory == empty


def test_inventory_reports_actual_node_not_placement_or_worker_permission():
    request = _request()
    wrong_node = _id(NodeID, 39)
    empty = _inventory((), request=request, node_id=wrong_node)
    assert empty.node_id != empty.lease_request.target_node_id
    assert _reply(empty).dependency_inventory == empty
    original = _inventory(request=request)
    with pytest.raises(protocol.ProtocolError):
        _reply(empty, grant=_grant(original))
    non_pg_request = replace(request, scheduling_key=None)
    misplaced = _inventory(request=non_pg_request, node_id=wrong_node)
    # An empty wrong-target observation is possible, but a full committed
    # Grant cannot claim it fulfilled an explicit target on another Node.
    with pytest.raises(protocol.ProtocolError):
        _reply(misplaced, grant=_grant(misplaced))


def test_concrete_wire_types_and_current_schema_are_required():
    class DisguisedRequest(protocol.RequestWorkerLease):
        pass

    original = _request()
    disguised = DisguisedRequest(*(getattr(original, field.name) for field in fields(original)))
    with pytest.raises(protocol.ProtocolError):
        protocol.revalidate_worker_lease_request(disguised)
    with pytest.raises(protocol.ProtocolError):
        protocol.revalidate_lease_dependency_inventory(original)
    inventory = _inventory()
    ack = protocol.AckLeaseDependencyCustody(inventory.lease_request.requester_worker_id, inventory)
    for message in (inventory, ack, protocol.AckLeaseDependencyCustodyReply(ack, True), _cancel(original), _reply(inventory)):
        values = tuple(getattr(message, field.name) for field in fields(message))
        for wrong in (values[:-1], values + (None,)):
            with pytest.raises(protocol.ProtocolError):
                protocol._rebuild_validated_wire_message(type(message), wrong)
