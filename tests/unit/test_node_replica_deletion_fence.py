"""Pure deletion-watermark contracts for Node dependency localization.

At most two bare Nodes, two 4 KiB in-memory stores, one logical object and one
consumer lease per case. Source pin/chunk/release, target seal, drop and grant
are actual handlers; only transport is synchronous in-memory dispatch. No
listener, process, thread, timer, sleep, blocking wait or user callable runs.
"""

from dataclasses import replace
import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import (
    GET_OBJECT_CHUNK_HANDLER, PIN_OBJECT_HANDLER, RELEASE_OBJECT_PIN_HANDLER,
    NodeServer,
    _WorkerSlot,
)
from miniray.object_manager import ObjectManager, PullState, PullStateError, UnknownPullError
from miniray.object_store import ObjectStore
from miniray.resources import AllocationState, ResourceLedger, ResourceVector
from miniray.transport import TransportTimeout


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("deletion fence pure test attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(NodeServer, "__init__", forbidden)


class _AliveWorker:
    def is_alive(self):
        return True


def _bare_node(byte):
    node = object.__new__(NodeServer)
    node.node_id = NodeID(bytes((byte,)) * 16)
    worker_id = WorkerID(bytes((byte + 2,)) * 16)
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._object_store = ObjectStore(4096)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._pinned_transfers = {}
    node._object_localization_locks = {}
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._gcs_address = None
    node._cluster_nodes = ()
    node._cluster_addresses = {}
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(
        worker_id, process=_AliveWorker(), address=("127.0.0.1", 25000 + byte),
    )}
    node.num_workers_per_node = 1
    return node


def _seal(node, descriptor, payload):
    request = protocol.SealObject.from_data(
        descriptor.object_id, descriptor.producer_attempt_id,
        descriptor.owner_worker_id, payload,
    )
    reply = node._handle_seal_object(request)
    assert type(reply) is protocol.SealObjectReply and reply.sealed
    assert reply.node_id == node.node_id and reply.checksum == descriptor.checksum


def _drop(node, descriptor):
    return protocol.DropObjectReplica(
        descriptor.object_id, descriptor.producer_attempt_id,
        descriptor.owner_worker_id, node.node_id, descriptor.checksum,
    )


def _pair(monkeypatch):
    source, target = _bare_node(1), _bare_node(2)
    object_id = ObjectID.for_task(TaskID(bytes((5,)) * 16))
    payload = b"old-sealed-copy"
    descriptor = protocol.ObjectStoreDescriptor(
        object_id, WorkerID(bytes((6,)) * 16), AttemptID(object_id.task_id, 0),
        source.node_id, len(payload), hashlib.sha256(payload).hexdigest(),
    )
    _seal(source, descriptor, payload)
    source_address = ("127.0.0.1", 24001)
    target._cluster_addresses[source.node_id] = source_address
    calls = []

    def rpc(address, handler, request, **options):
        assert address == source_address
        assert target._object_localization_locks[object_id].locked()
        assert not target._state_lock._is_owned()
        calls.append((handler, request))
        assert len(calls) <= 9
        if handler == PIN_OBJECT_HANDLER:
            return source._handle_pin_object_for_transfer(request)
        if handler == GET_OBJECT_CHUNK_HANDLER:
            return source._handle_get_object_chunk(request)
        assert handler == RELEASE_OBJECT_PIN_HANDLER
        return source._handle_release_object_pin(request)

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    return source, target, descriptor, payload, calls


def _lease(target, descriptor):
    task_id = TaskID(bytes((7,)) * 16)
    return protocol.RequestWorkerLease(
        LeaseID(bytes((8,)) * 16), task_id, AttemptID(task_id, 0),
        ResourceVector({"CPU": 1}), target.node_id,
        WorkerID(bytes((9,)) * 16), target_node_id=target.node_id,
        dependencies=(descriptor,),
    )


def _release(target, grant):
    reply = target._handle_release_lease(protocol.ReleaseWorkerLease(
        grant.lease_id, grant.worker_id, grant.allocation_token,
    ))
    assert reply.released
    assert target._ledger.available == target._ledger.total
    assert target._object_store.snapshot(grant.dependencies[0].object_id).pin_count == 0


def _assert_no_grant(target):
    assert not target._leases and target._workers[target.worker_id].active_lease_id is None
    assert target._inflight_lease_requests == 0
    snapshot = target._ledger.snapshot()
    assert snapshot.available == snapshot.total
    assert all(record.state is AllocationState.RELEASED for record in snapshot.allocations)


@pytest.mark.parametrize("deleted_epoch", (0, 1), ids=("same-epoch", "older-epoch"))
def test_committed_drop_blocks_old_pull_and_lease_despite_live_source(monkeypatch, deleted_epoch):
    source, target, descriptor, payload, calls = _pair(monkeypatch)
    deleted = replace(descriptor, producer_attempt_id=AttemptID(descriptor.object_id.task_id, deleted_epoch))
    _seal(target, deleted, payload)
    dropped = target._handle_drop_object_replica(_drop(target, deleted))
    assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
    tombstone = target._dropped_metadata[descriptor.object_id]

    with pytest.raises(RuntimeError, match="fenced by replica deletion"):
        target._localize_one_dependency(descriptor)
    request = _lease(target, descriptor)
    rejected = target._handle_request_lease(request)
    assert type(rejected) is protocol.RejectWorkerLease
    assert rejected.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
    assert "fenced by replica deletion" in rejected.detail
    assert target._handle_request_lease(request) == rejected
    assert calls == [] and target._object_store.used_bytes == 0
    assert descriptor.object_id not in target._sealed_metadata
    assert target._dropped_metadata[descriptor.object_id] == tombstone
    with pytest.raises(UnknownPullError):
        target._object_manager.snapshot(descriptor.object_id)
    assert source._object_store.get(descriptor.object_id) == payload
    assert source._object_store.snapshot(descriptor.object_id).pin_count == 0
    _assert_no_grant(target)


def test_newer_attempt_can_pull_seal_and_pin_without_erasing_old_tombstone(monkeypatch):
    source, target, descriptor, payload, calls = _pair(monkeypatch)
    target._localize_one_dependency(descriptor)
    drop = _drop(target, descriptor)
    assert target._handle_drop_object_replica(drop).status is protocol.DropObjectReplicaStatus.DROPPED
    tombstone = target._dropped_metadata[descriptor.object_id]
    assert source._handle_drop_object_replica(_drop(source, descriptor)).status is protocol.DropObjectReplicaStatus.DROPPED
    new_payload = b"new-attempt-copy"
    newer = replace(
        descriptor, producer_attempt_id=descriptor.producer_attempt_id.next(),
        size_bytes=len(new_payload), checksum=hashlib.sha256(new_payload).hexdigest(),
    )
    _seal(source, newer, new_payload)
    checks = []
    original_check = target._require_dependency_not_deleted_locked

    def observe_check(value):
        assert target._state_lock._is_owned()
        present = target._object_store.contains(value.object_id, sealed_only=False)
        checks.append("absent" if not present else
                      "sealed" if target._object_store.contains(value.object_id) else "staged")
        return original_check(value)

    monkeypatch.setattr(target, "_require_dependency_not_deleted_locked", observe_check)
    grant = target._handle_request_lease(_lease(target, newer))
    assert type(grant) is protocol.GrantWorkerLease
    assert grant.dependencies == (replace(newer, node_id=target.node_id),)
    # Admission, seal, custody inventory and final lease pin each consume
    # the deletion watermark; recording custody adds no physical write.
    assert checks == ["absent", "staged", "sealed", "sealed"]
    assert target._object_store.get(newer.object_id) == new_payload
    assert target._object_store.snapshot(newer.object_id).pin_count == 1
    assert target._sealed_metadata[newer.object_id][0] == newer.producer_attempt_id
    assert target._dropped_metadata[newer.object_id] == tombstone
    assert [handler for handler, _ in calls] == [
        PIN_OBJECT_HANDLER, GET_OBJECT_CHUNK_HANDLER, RELEASE_OBJECT_PIN_HANDLER,
    ] * 2
    replay = target._handle_drop_object_replica(drop)
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED and replay.accepted
    assert target._object_store.get(newer.object_id) == new_payload
    _release(target, grant)


def test_pinned_drop_rejection_does_not_create_a_deletion_watermark(monkeypatch):
    source, target, descriptor, payload, calls = _pair(monkeypatch)
    grant = target._handle_request_lease(_lease(target, descriptor))
    assert type(grant) is protocol.GrantWorkerLease
    drop = _drop(target, descriptor)
    pinned = target._handle_drop_object_replica(drop)
    assert pinned.status is protocol.DropObjectReplicaStatus.PINNED and not pinned.accepted
    assert not target._dropped_metadata
    assert not target._replica_drop_receipts
    assert target._localize_one_dependency(descriptor) == grant.dependencies[0]
    assert target._object_store.get(descriptor.object_id) == payload
    assert len(calls) == 3 and source._object_store.snapshot(descriptor.object_id).pin_count == 0
    _release(target, grant)
    assert target._handle_drop_object_replica(drop).status is protocol.DropObjectReplicaStatus.DROPPED
    with pytest.raises(RuntimeError, match="fenced by replica deletion"):
        target._localize_one_dependency(descriptor)
    assert target._object_store.used_bytes == 0 and len(calls) == 3


def test_lost_drop_ack_replays_exact_tombstone_without_repulling(monkeypatch):
    source, target, descriptor, payload, calls = _pair(monkeypatch)
    target._localize_one_dependency(descriptor)
    drop = _drop(target, descriptor)
    applied = []

    def drop_then_lose_ack():
        applied.append(target._handle_drop_object_replica(drop))
        raise TransportTimeout("drop ACK lost after actual deletion")

    with pytest.raises(TransportTimeout, match="after actual deletion"):
        drop_then_lose_ack()
    replay = target._handle_drop_object_replica(drop)
    assert applied[0].status is protocol.DropObjectReplicaStatus.DROPPED
    assert replay == replace(applied[0], status=protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
    assert target._handle_drop_object_replica(drop) == replay
    with pytest.raises(RuntimeError, match="fenced by replica deletion"):
        target._localize_one_dependency(descriptor)
    assert target._object_store.used_bytes == 0 and descriptor.object_id not in target._sealed_metadata
    assert source._object_store.get(descriptor.object_id) == payload and len(calls) == 3
    with pytest.raises(UnknownPullError):
        target._object_manager.snapshot(descriptor.object_id)


def test_exact_drop_receipt_survives_newer_replica_and_watermark_without_touching_them(monkeypatch):
    _source, target, descriptor, payload, _calls = _pair(monkeypatch)
    _seal(target, descriptor, payload)
    drop = _drop(target, descriptor)
    applied = target._handle_drop_object_replica(drop)
    assert applied.status is protocol.DropObjectReplicaStatus.DROPPED
    newer = replace(descriptor, producer_attempt_id=descriptor.producer_attempt_id.next())
    _seal(target, newer, payload)
    target._localize_one_dependency(replace(newer, node_id=target.node_id))
    before_pull = target._object_manager.snapshot(descriptor.object_id)
    before_store = target._object_store.snapshot(descriptor.object_id)
    before_metadata = dict(target._sealed_metadata)
    original_delete = target._object_store.delete
    original_forget = target._object_manager.forget_local_replica

    def forbidden(*_args, **_kwargs):
        pytest.fail("old successful drop receipt touched newer bytes or pull state")

    monkeypatch.setattr(target._object_store, "delete", forbidden)
    monkeypatch.setattr(target._object_manager, "forget_local_replica", forbidden)
    replay = target._handle_drop_object_replica(drop)
    assert replay == replace(applied, status=protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
    assert target._object_store.get(descriptor.object_id) == payload
    assert target._object_store.snapshot(descriptor.object_id) == before_store
    assert target._object_manager.snapshot(descriptor.object_id) == before_pull
    assert target._sealed_metadata == before_metadata
    assert len(target._replica_drop_receipts) == 1

    monkeypatch.setattr(target._object_store, "delete", original_delete)
    monkeypatch.setattr(target._object_manager, "forget_local_replica", original_forget)
    assert target._handle_drop_object_replica(_drop(target, newer)).status is protocol.DropObjectReplicaStatus.DROPPED
    assert target._dropped_metadata[descriptor.object_id][0] == newer.producer_attempt_id
    assert target._handle_drop_object_replica(drop) == replay
    assert len(target._replica_drop_receipts) == 2


@pytest.mark.parametrize("newer_deleted", (False, True), ids=("newer-sealed", "newer-deleted"))
def test_invented_old_drop_is_not_acknowledged_by_newer_epoch_alone(monkeypatch, newer_deleted):
    _source, target, descriptor, payload, _calls = _pair(monkeypatch)
    newer = replace(descriptor, producer_attempt_id=descriptor.producer_attempt_id.next())
    _seal(target, newer, payload)
    if newer_deleted:
        assert target._handle_drop_object_replica(_drop(target, newer)).status is protocol.DropObjectReplicaStatus.DROPPED
    receipts = set(getattr(target, "_replica_drop_receipts", set()))
    reply = target._handle_drop_object_replica(_drop(target, descriptor))
    assert reply.status is protocol.DropObjectReplicaStatus.STALE_EPOCH and not reply.accepted
    assert target._replica_drop_receipts == receipts
    assert target._object_store.contains(descriptor.object_id) is not newer_deleted
    if not newer_deleted:
        assert target._object_store.get(descriptor.object_id) == payload
        assert target._sealed_metadata[descriptor.object_id][0] == newer.producer_attempt_id


def test_drop_receipt_is_saved_only_after_object_manager_cleanup_succeeds(monkeypatch):
    _source, target, descriptor, _payload, _calls = _pair(monkeypatch)
    target._localize_one_dependency(descriptor)
    drop = _drop(target, descriptor)
    original_forget = target._object_manager.forget_local_replica
    attempts = []

    def fail_first(object_id, *, attempt_id):
        attempts.append((object_id, attempt_id))
        if len(attempts) == 1:
            raise PullStateError("manager cleanup has not acknowledged")
        return original_forget(object_id, attempt_id=attempt_id)

    monkeypatch.setattr(target._object_manager, "forget_local_replica", fail_first)
    first = target._handle_drop_object_replica(drop)
    assert first.status is protocol.DropObjectReplicaStatus.INCONSISTENT
    assert not target._replica_drop_receipts and target._object_store.used_bytes == 0
    assert target._object_manager.snapshot(descriptor.object_id).state is PullState.READY
    replay = target._handle_drop_object_replica(drop)
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert len(target._replica_drop_receipts) == 1
    assert attempts == [(descriptor.object_id, descriptor.producer_attempt_id)] * 2
    assert target._handle_drop_object_replica(drop) == replay and len(attempts) == 2
    with pytest.raises(UnknownPullError):
        target._object_manager.snapshot(descriptor.object_id)


def test_delete_intent_with_remaining_bytes_cannot_acquire_a_new_source_reader(monkeypatch):
    source, target, descriptor, payload, _calls = _pair(monkeypatch)
    request = _drop(source, descriptor)

    def interrupted_delete(_object_id):
        raise RuntimeError("delete did not execute")

    with monkeypatch.context() as fault:
        fault.setattr(source._object_store, "delete", interrupted_delete)
        pending = source._handle_drop_object_replica(request)
    assert pending.status is protocol.DropObjectReplicaStatus.INCONSISTENT
    assert source._object_store.get(descriptor.object_id) == payload
    assert not source._replica_drop_receipts
    pin = protocol.PinObjectForTransfer(
        "new-reader-after-deletion-intent", descriptor, target.node_id,
    )
    rejected = source._handle_pin_object_for_transfer(pin)
    assert not rejected.pinned and "fenced by replica deletion" in rejected.error
    assert pin.transfer_id not in source._pinned_transfers
    assert source._object_store.snapshot(descriptor.object_id).pin_count == 0
    assert source._handle_drop_object_replica(request).status is protocol.DropObjectReplicaStatus.DROPPED
    assert source._handle_drop_object_replica(request).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED


def test_drop_receipt_and_watermark_do_not_alias_request_or_reply_ids(monkeypatch):
    _source, target, descriptor, payload, _calls = _pair(monkeypatch)
    _seal(target, descriptor, payload)
    drop = _drop(target, descriptor)
    replay_request = protocol.DropObjectReplica(
        ObjectID(TaskID(bytes(descriptor.object_id.task_id)), descriptor.object_id.return_index),
        AttemptID(TaskID(bytes(descriptor.object_id.task_id)), 0),
        WorkerID(bytes(descriptor.owner_worker_id)), NodeID(bytes(target.node_id)), descriptor.checksum,
    )
    reply = target._handle_drop_object_replica(drop)
    assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
    receipts = set(target._replica_drop_receipts)
    object.__setattr__(drop.producer_attempt_id, "attempt_number", 9)
    object.__setattr__(reply.producer_attempt_id, "attempt_number", 8)
    object.__setattr__(reply.object_id, "return_index", 7)
    assert target._replica_drop_receipts == receipts
    assert target._dropped_metadata[replay_request.object_id][0].attempt_number == 0
    replay = target._handle_drop_object_replica(replay_request)
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert replay.producer_attempt_id == replay_request.producer_attempt_id
    assert replay.object_id == replay_request.object_id


def test_final_seal_rechecks_real_drop_watermark_after_stale_early_check(monkeypatch):
    source, target, descriptor, payload, calls = _pair(monkeypatch)
    _seal(target, descriptor, payload)
    assert target._handle_drop_object_replica(_drop(target, descriptor)).status is protocol.DropObjectReplicaStatus.DROPPED
    original_check = target._require_dependency_not_deleted_locked
    checks = []

    def miss_only_admission(value):
        # Fault-inject a stale early check, not a deletion ACK or a tombstone.
        # The real earlier drop remains committed; final seal must re-read it.
        checks.append(value)
        if len(checks) > 1:
            return original_check(value)

    monkeypatch.setattr(target, "_require_dependency_not_deleted_locked", miss_only_admission)
    with pytest.raises(RuntimeError, match="fenced by replica deletion"):
        target._localize_one_dependency(descriptor)
    assert checks == [descriptor, descriptor]
    assert len(calls) == 3 and calls[-1][0] == RELEASE_OBJECT_PIN_HANDLER
    assert target._object_store.used_bytes == 0 and descriptor.object_id not in target._sealed_metadata
    pull = target._object_manager.snapshot(descriptor.object_id)
    assert pull.state is PullState.FAILED
    assert source._object_store.get(descriptor.object_id) == payload
    assert source._object_store.snapshot(descriptor.object_id).pin_count == 0


def test_drop_between_localization_and_grant_prevents_pin_and_restores_resources(monkeypatch):
    source, target, descriptor, payload, calls = _pair(monkeypatch)
    original_localize = target._localize_dependencies
    dropped = []

    def localize_then_drop(values):
        localized = original_localize(values)
        # The per-object pull lock has been released, while no lease has a pin.
        dropped.append(target._handle_drop_object_replica(_drop(target, descriptor)))
        return localized

    monkeypatch.setattr(target, "_localize_dependencies", localize_then_drop)
    request = _lease(target, descriptor)
    rejected = target._handle_request_lease(request)
    assert type(rejected) is protocol.RejectWorkerLease
    assert rejected.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
    assert "fenced by replica deletion" in rejected.detail
    assert len(dropped) == 1 and dropped[0].status is protocol.DropObjectReplicaStatus.DROPPED
    assert target._handle_request_lease(request) == rejected
    assert target._object_store.used_bytes == 0 and len(calls) == 3
    assert source._object_store.get(descriptor.object_id) == payload
    _assert_no_grant(target)
