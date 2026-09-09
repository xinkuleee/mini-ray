"""Pure cancellation inventory wire and real Node reducer contracts.

Safety: two threadless Nodes, two 1 KiB stores and two tiny dependency values.
At most six synchronous source pin/chunk/release calls create one actual grant.
No Core, GCS, user function, socket, process, thread, timer, wait or sleep runs;
runtime tripwires make accidental infrastructure use fail immediately. Worker
loss uses the real local reclaim reducer with a passive process-state fake.
"""

from copy import deepcopy
from dataclasses import fields, replace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import node as node_module, protocol
from miniray.ids import (
    AttemptID, LeaseID, NodeID, ObjectID, PlacementGroupID, TaskID, WorkerID,
)
from miniray.node import NodeServer, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector


pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes((number,)) * 16)


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure cancelled-grant inventory attempted runtime infrastructure")

    for kind, method in (
        (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


def _wire_grant():
    task, child_task = _id(TaskID, 11), _id(TaskID, 12)
    node_id = _id(NodeID, 13)
    attempt = AttemptID(task, 2)
    dependency = protocol.ObjectStoreDescriptor(
        ObjectID.for_task(child_task), _id(WorkerID, 14),
        AttemptID(child_task, 1), node_id, 3, "a" * 64,
    )
    key = protocol.PlacementGroupSchedulingKey(
        _id(PlacementGroupID, 15), 1, 1, node_id, "b" * 64,
    )
    return protocol.GrantWorkerLease(
        _id(LeaseID, 16), task, attempt, node_id, _id(WorkerID, 17),
        ("worker.invalid", 18), AllocationToken("historical-allocation"),
        (dependency,), key,
    )


def _wire_reply(grant=None, **changes):
    grant = _wire_grant() if grant is None else grant
    values = dict(
        lease_id=grant.lease_id, task_id=grant.task_id, attempt_id=grant.attempt_id,
        requester_node_id=_id(NodeID, 19), requester_worker_id=_id(WorkerID, 20),
        state=protocol.LeaseExecutionState.ABANDONED, accepted=True,
        cancelled=True, released=True, scheduling_key=grant.scheduling_key,
        retired_grant=grant,
    )
    values.update(changes)
    return protocol.CancelWorkerLeaseReply(**values)


def test_inventory_roundtrip_is_detached_and_preserves_target_and_pg():
    grant = _wire_grant()
    validated = protocol.revalidate_worker_lease_grant(grant)
    reply = _wire_reply(grant)
    restored = pickle.loads(pickle.dumps(reply))
    assert restored == reply
    for detached in (validated, reply.retired_grant, restored.retired_grant):
        assert type(detached) is protocol.GrantWorkerLease and detached == grant
        assert detached is not grant
        assert detached.lease_id is not grant.lease_id
        assert detached.allocation_token is not grant.allocation_token
        assert detached.dependencies[0] is not grant.dependencies[0]
        assert detached.dependencies[0].object_id is not grant.dependencies[0].object_id
        assert detached.dependencies[0].producer_attempt_id is not grant.dependencies[0].producer_attempt_id
        assert detached.scheduling_key is not grant.scheduling_key
        assert detached.attempt_id is not grant.attempt_id
        assert detached.attempt_id.task_id is not grant.attempt_id.task_id
        assert detached.task_id is not grant.task_id
    object.__setattr__(grant.dependencies[0], "checksum", "c" * 64)
    object.__setattr__(grant.allocation_token, "value", "rebound")
    object.__setattr__(grant.scheduling_key, "bundle_index", 7)
    assert reply.retired_grant == restored.retired_grant == validated
    assert reply.retired_grant.dependencies[0].checksum == "a" * 64
    assert reply.retired_grant.allocation_token.value == "historical-allocation"
    assert reply.retired_grant.scheduling_key.bundle_index == 1


def _corrupt_path(root, path, value):
    current = root
    parts = path.split(".")
    for part in parts[:-1]:
        current = current[int(part)] if part.isdigit() else getattr(current, part)
    object.__setattr__(current, parts[-1], value)


@pytest.mark.parametrize(("path", "value"), (
    ("lease_id.value", b"short"),
    ("task_id.value", b"short"),
    ("attempt_id.attempt_number", True),
    ("node_id.value", b"short"),
    ("worker_id.value", b"short"),
    ("worker_address", ("worker.invalid", True)),
    ("allocation_token.value", ""),
    ("dependencies.0.size_bytes", True),
    ("dependencies.0.checksum", "z" * 64),
    ("dependencies.0.object_id.return_index", False),
    ("dependencies.0.producer_attempt_id.attempt_number", -1),
    ("dependencies.0.owner_worker_id.value", b"short"),
    ("dependencies.0.node_id", _id(NodeID, 21)),
    ("scheduling_key.attempt", True),
    ("scheduling_key.placement_group_id.value", b"short"),
    ("scheduling_key.plan_digest", "bad"),
    ("attempt_id.task_id.value", b"short"),
    ("dependencies.0.object_id.task_id.value", b"short"),
    ("scheduling_key.bundle_index", True),
))
def test_inventory_revalidates_nested_corruption_on_entry_and_wire(path, value):
    reply = _wire_reply()
    _corrupt_path(reply.retired_grant, path, value)
    with pytest.raises(protocol.ProtocolError):
        protocol.revalidate_worker_lease_grant(reply.retired_grant)
    with pytest.raises(protocol.ProtocolError):
        replace(reply)
    with pytest.raises(protocol.ProtocolError):
        pickle.loads(pickle.dumps(reply))


def test_inventory_rejects_nonexact_grant_and_duplicate_dependency():
    class ImpostorGrant(protocol.GrantWorkerLease):
        pass

    grant = _wire_grant()
    disguised = ImpostorGrant(*(getattr(grant, item.name) for item in fields(grant)))
    with pytest.raises(protocol.ProtocolError):
        protocol.revalidate_worker_lease_grant(disguised)
    object.__setattr__(grant, "dependencies", grant.dependencies * 2)
    with pytest.raises(protocol.ProtocolError):
        protocol.revalidate_worker_lease_grant(grant)


@pytest.mark.parametrize("changed", ("lease", "task", "attempt", "pg"))
def test_inventory_must_match_outer_cancellation_identity(changed):
    reply = _wire_reply()
    if changed == "lease":
        changes = dict(lease_id=_id(LeaseID, 22))
    elif changed == "task":
        task = _id(TaskID, 23)
        changes = dict(task_id=task, attempt_id=AttemptID(task, 2))
    elif changed == "attempt":
        changes = dict(attempt_id=AttemptID(reply.task_id, 3))
    else:
        changes = dict(scheduling_key=replace(reply.scheduling_key, bundle_index=0))
    with pytest.raises(protocol.ProtocolError):
        replace(reply, **changes)


@pytest.mark.parametrize(("state", "accepted", "cancelled"), (
    (protocol.LeaseExecutionState.GRANTED, False, False),
    (protocol.LeaseExecutionState.RUNNING, False, False),
    (protocol.LeaseExecutionState.COMPLETED, False, False),
    (protocol.LeaseExecutionState.ABANDONED, False, False),
    (protocol.LeaseExecutionState.ABANDONED, True, False),
    (protocol.LeaseExecutionState.WORKER_LOST, True, False),
))
def test_inventory_is_not_authority_in_other_reply_states(state, accepted, cancelled):
    with pytest.raises(protocol.ProtocolError):
        _wire_reply(
            state=state, accepted=accepted, cancelled=cancelled, released=False,
            error=None if accepted else "rejected",
        )


def test_worker_lost_inventory_and_legacy_empty_cancel_reply_roundtrip():
    lost = _wire_reply(
        state=protocol.LeaseExecutionState.WORKER_LOST, accepted=False,
        cancelled=False, released=False, error="worker lost",
    )
    assert pickle.loads(pickle.dumps(lost)) == lost
    absent = _wire_reply(retired_grant=None, released=False)
    assert pickle.loads(pickle.dumps(absent)) == absent
    values = tuple(getattr(lost, item.name) for item in fields(lost))
    for mismatched in (values[:-1], values + (None,)):
        with pytest.raises(protocol.ProtocolError):
            protocol._rebuild_validated_wire_message(protocol.CancelWorkerLeaseReply, mismatched)


def _node(node_id, worker_id):
    node = object.__new__(NodeServer)
    node.node_id, node.worker_id = node_id, worker_id
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._object_store = ObjectStore(1024)
    node._object_manager = ObjectManager(node_id, node._object_store)
    node._sealed_metadata = {}
    node._pinned_transfers = {}
    node._object_localization_locks = {}
    node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._active_lease_id = None
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._gcs_address = None
    node._cluster_nodes, node._cluster_addresses = (), {}
    node._worker_process = SimpleNamespace(is_alive=lambda: True)
    node._worker_address = ("worker.invalid", 31)
    node._workers = {worker_id: _WorkerSlot(worker_id, process=node._worker_process,
        address=node._worker_address, pid=3131)}
    node._worker_order = (worker_id,)
    node._gcs_lifecycle_lock = threading.Lock()
    node._registered_with_gcs = False
    node.event_sink = None
    return node


class _NodeFixture:
    def __init__(self, monkeypatch, *, commit=True):
        self.source = _node(_id(NodeID, 32), _id(WorkerID, 33))
        self.target = _node(_id(NodeID, 34), _id(WorkerID, 35))
        self.source_address = ("source.invalid", 36)
        self.target._cluster_addresses[self.source.node_id] = self.source_address
        self.payloads, descriptors = (b"local-child", b"foreign-child"), []
        for index, payload in enumerate(self.payloads):
            task = _id(TaskID, 40 + index)
            request = protocol.SealObject.from_data(
                ObjectID.for_task(task), AttemptID(task, 0), _id(WorkerID, 42 + index), payload,
            )
            assert self.source._handle_seal_object(request).sealed
            descriptors.append(protocol.ObjectStoreDescriptor(
                request.object_id, request.owner_worker_id, request.attempt_id,
                self.source.node_id, len(payload), request.checksum,
            ))
        self.descriptors = tuple(descriptors)
        self.transfers = []

        def transfer(address, handler, request, **options):
            assert address == self.source_address
            self.transfers.append((handler, request))
            assert len(self.transfers) <= 6
            handlers = {
                node_module.PIN_OBJECT_HANDLER: self.source._handle_pin_object_for_transfer,
                node_module.GET_OBJECT_CHUNK_HANDLER: self.source._handle_get_object_chunk,
                node_module.RELEASE_OBJECT_PIN_HANDLER: self.source._handle_release_object_pin,
            }
            assert handler in handlers
            return handlers[handler](request)

        monkeypatch.setattr(node_module, "rpc_request", transfer)
        task = _id(TaskID, 44)
        self.request = protocol.RequestWorkerLease(
            _id(LeaseID, 45), task, AttemptID(task, 0), ResourceVector({"CPU": 1}),
            self.source.node_id, _id(WorkerID, 42), target_node_id=self.target.node_id,
            dependencies=self.descriptors, return_ids=(ObjectID.for_task(task),),
        )
        self.cancel = protocol.CancelWorkerLease(
            self.request.lease_id, task, self.request.attempt_id,
            self.request.requester_node_id, self.request.requester_worker_id,
        )
        self.grant = None
        if commit:
            self.grant = self.target._handle_request_lease(self.request)
            assert type(self.grant) is protocol.GrantWorkerLease
            assert len(self.transfers) == 6
            assert self.grant.dependencies == tuple(
                replace(item, node_id=self.target.node_id) for item in self.descriptors
            )
            assert self.target.resource_ledger.available.is_zero()
            self.assert_replicas(pin_count=1)

    def assert_replicas(self, *, pin_count):
        for descriptor, payload in zip(self.descriptors, self.payloads):
            assert self.target.object_store.get(descriptor.object_id) == payload
            assert self.target.object_store.snapshot(descriptor.object_id).pin_count == pin_count
            assert self.source.object_store.snapshot(descriptor.object_id).pin_count == 0

    def start(self):
        return self.target._handle_start_worker_lease(protocol.StartWorkerLease(
            self.request.lease_id, self.request.task_id, self.request.attempt_id,
            self.grant.worker_id,
        ))


def test_real_cancel_returns_committed_inventory_on_first_and_replay(monkeypatch):
    fixture = _NodeFixture(monkeypatch)
    expected = deepcopy(fixture.grant)
    first = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    assert first.accepted and first.cancelled and first.released
    assert first.state is protocol.LeaseExecutionState.ABANDONED
    assert first.retired_grant == expected
    assert first.retired_grant is not fixture.target._leases[expected.lease_id].grant
    fixture.assert_replicas(pin_count=0)
    assert fixture.target.resource_ledger.available == fixture.target.resource_ledger.total
    # Mutating a received frozen object must not alter replay or Node metadata.
    object.__setattr__(first.retired_grant.dependencies[0], "checksum", "d" * 64)
    replay = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    assert replay.accepted and replay.cancelled and not replay.released
    assert replay.retired_grant == expected
    assert replay.retired_grant is not fixture.target._lease_cancellations[expected.lease_id].reply.retired_grant
    assert fixture.target._leases[expected.lease_id].grant == expected
    assert len(fixture.transfers) == 6
    assert not fixture.start().accepted
    assert isinstance(fixture.target._handle_request_lease(fixture.request), protocol.RejectWorkerLease)
    fixture.assert_replicas(pin_count=0)


def test_worker_lost_rejection_preserves_inventory_for_exact_outcome_query(monkeypatch):
    fixture = _NodeFixture(monkeypatch)
    slot = fixture.target._workers[fixture.grant.worker_id]
    slot.process = SimpleNamespace(is_alive=lambda: False)
    with fixture.target._state_lock:
        assert fixture.target._reclaim_active_lease_after_worker_exit_locked(slot.worker_id)
    first = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    replay = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    assert not first.accepted and not first.cancelled and not first.released
    assert first.state is protocol.LeaseExecutionState.WORKER_LOST
    assert first.retired_grant == fixture.grant == replay.retired_grant
    assert first.retired_grant is not replay.retired_grant
    inventory = first.retired_grant
    outcome = fixture.target._handle_get_worker_lease_outcome(protocol.GetWorkerLeaseOutcome(
        inventory.lease_id, inventory.task_id, inventory.attempt_id, inventory.worker_id,
        fixture.request.requester_worker_id, fixture.request.return_ids,
        inventory.scheduling_key,
    ))
    assert outcome.found and not outcome.worker_alive
    assert outcome.state is protocol.LeaseExecutionState.WORKER_LOST
    assert outcome.descriptors == () and outcome.orphan_descriptors == ()
    fixture.assert_replicas(pin_count=0)
    assert fixture.target.resource_ledger.available == fixture.target.resource_ledger.total


@pytest.mark.parametrize("completed", (False, True))
def test_running_or_completed_cancel_rejection_has_no_inventory(monkeypatch, completed):
    fixture = _NodeFixture(monkeypatch)
    assert fixture.start().accepted
    if completed:
        completion = fixture.target._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            fixture.grant.lease_id, fixture.grant.task_id, fixture.grant.attempt_id,
            fixture.grant.worker_id, protocol.TaskReplyStatus.SYSTEM_ERROR,
        ))
        assert completion.accepted and completion.released
    reply = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    assert not reply.accepted and not reply.cancelled and not reply.released
    assert reply.retired_grant is None
    assert reply.state is (
        protocol.LeaseExecutionState.COMPLETED if completed
        else protocol.LeaseExecutionState.RUNNING
    )
    fixture.assert_replicas(pin_count=0 if completed else 1)


@pytest.mark.parametrize("cached_cancel", (False, True))
def test_wrong_cancel_identity_never_discloses_committed_inventory(monkeypatch, cached_cancel):
    fixture = _NodeFixture(monkeypatch)
    if cached_cancel:
        assert fixture.target._handle_cancel_worker_lease(fixture.cancel).accepted
    key = protocol.PlacementGroupSchedulingKey(
        _id(PlacementGroupID, 46), 0, 0, fixture.target.node_id, "e" * 64,
    )
    other_task = _id(TaskID, 47)
    changes = (
        dict(requester_worker_id=_id(WorkerID, 48)),
        dict(requester_node_id=_id(NodeID, 49)),
        dict(attempt_id=AttemptID(fixture.cancel.task_id, 1)),
        dict(task_id=other_task, attempt_id=AttemptID(other_task, 0)),
        dict(scheduling_key=key),
    )
    for changed in changes:
        reply = fixture.target._handle_cancel_worker_lease(replace(fixture.cancel, **changed))
        assert not reply.accepted and not reply.cancelled and not reply.released
        assert reply.retired_grant is None
    fixture.assert_replicas(pin_count=0 if cached_cancel else 1)


def test_record_identity_still_fences_inventory_without_outcome_cache(monkeypatch):
    fixture = _NodeFixture(monkeypatch)
    fixture.target._lease_outcomes.pop(fixture.request.lease_id)
    reply = fixture.target._handle_cancel_worker_lease(replace(
        fixture.cancel, requester_worker_id=_id(WorkerID, 50),
    ))
    assert not reply.accepted and reply.retired_grant is None
    assert fixture.target._leases[fixture.request.lease_id].state is protocol.LeaseExecutionState.GRANTED
    fixture.assert_replicas(pin_count=1)


def test_none_inventory_does_not_claim_pregrant_partial_replica_absence(monkeypatch):
    fixture = _NodeFixture(monkeypatch, commit=False)
    partial = fixture.target._localize_one_dependency(fixture.descriptors[0])
    assert partial.node_id == fixture.target.node_id and len(fixture.transfers) == 3
    assert fixture.target.object_store.contains(partial.object_id)
    assert fixture.target._leases == {}
    reply = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    replay = fixture.target._handle_cancel_worker_lease(fixture.cancel)
    assert reply.accepted and reply.cancelled and not reply.released
    assert reply.retired_grant is None and replay.retired_grant is None
    assert fixture.target.object_store.get(partial.object_id) == fixture.payloads[0]
    assert fixture.target.object_store.snapshot(partial.object_id).pin_count == 0
    assert isinstance(fixture.target._handle_request_lease(fixture.request), protocol.RejectWorkerLease)
    assert len(fixture.transfers) == 3
