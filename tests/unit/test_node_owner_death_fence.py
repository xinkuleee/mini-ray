"""Owner-death fence reducers and separately classified pull races.

Direct in-memory scans/seals are pure. The two in-progress-pull cases start
real threads and waits without guaranteed failure teardown; they remain heavy
pending separate bounded-runtime review.
"""

from __future__ import annotations

import hashlib
import pickle
import threading

import pytest

from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import (
    GET_OBJECT_CHUNK_HANDLER,
    PIN_OBJECT_HANDLER,
    RELEASE_OBJECT_PIN_HANDLER,
    NodeServer,
)
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _death(
    owner: WorkerID,
    *,
    detection_id: str = "owner-death-1",
    death_epoch: int = 7,
    reason: protocol.WorkerDeathReason = (
        protocol.WorkerDeathReason.PROCESS_EXIT
    ),
) -> protocol.WorkerDeathRecord:
    return protocol.WorkerDeathRecord(
        detection_id,
        protocol.WorkerIncarnation(
            _id(NodeID, 91), 9001, 3, owner, 9002
        ),
        death_epoch,
        -9,
        reason,
    )


def _descriptor(
    node_id: NodeID, owner: WorkerID, byte: int, payload: bytes
) -> protocol.ObjectStoreDescriptor:
    task_id = _id(TaskID, byte)
    return protocol.ObjectStoreDescriptor(
        ObjectID.for_task(task_id),
        owner,
        AttemptID(task_id, 0),
        node_id,
        len(payload),
        hashlib.sha256(payload).hexdigest(),
    )


def _bare_node(node_id: NodeID | None = None) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = node_id or _id(NodeID, 1)
    node._state_lock = threading.RLock()
    node._object_store = ObjectStore(512 * 1024)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._object_localization_locks = {}
    node._pinned_transfers = {}
    node._owner_death_fences = {}
    node._owner_death_fence_outcomes = {}
    node._cluster_addresses = {}
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    return node


def _put_exact(
    node: NodeServer,
    descriptor: protocol.ObjectStoreDescriptor,
    payload: bytes,
) -> None:
    node._object_store.put(descriptor.object_id, payload)
    node._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )


@pytest.mark.unit
def test_fence_wire_binds_typed_worker_death_and_exact_replica_identity() -> None:
    node_id = _id(NodeID, 1)
    owner = _id(WorkerID, 2)
    death = _death(owner)
    payload = b"replica"
    descriptor = _descriptor(node_id, owner, 3, payload)

    empty = protocol.InstallOwnerDeathFence(
        "empty-publication", death, node_id
    )
    assert empty.expected_replicas == ()
    assert empty.scope is protocol.OwnerDeathFenceScope.PUBLICATION_EXACT
    request = protocol.InstallOwnerDeathFence(
        "publication-1", death, node_id, (descriptor,)
    )
    observation = protocol.OwnerDeathReplicaObservation(
        descriptor, protocol.OwnerDeathReplicaStatus.PRESENT
    )
    reply = protocol.InstallOwnerDeathFenceReply(
        request, protocol.OwnerDeathFenceDisposition.FENCED, (observation,)
    )

    assert pickle.loads(pickle.dumps(reply)) == reply
    assert observation.drop_request == protocol.DropObjectReplica(
        descriptor.object_id, descriptor.producer_attempt_id, owner, node_id,
        descriptor.checksum,
    )

    with pytest.raises(ProtocolError, match="PROCESS_EXIT or NODE_EXIT"):
        protocol.InstallOwnerDeathFence(
            "expected-exit",
            _death(owner, reason=protocol.WorkerDeathReason.EXPECTED),
            node_id,
        )
    with pytest.raises(ProtocolError, match="another owner"):
        protocol.InstallOwnerDeathFence(
            "wrong-owner", death, node_id,
            (_descriptor(node_id, _id(WorkerID, 4), 5, b"x"),),
        )
    with pytest.raises(ProtocolError, match="another node"):
        protocol.InstallOwnerDeathFence(
            "wrong-node", death, node_id,
            (_descriptor(_id(NodeID, 6), owner, 7, b"x"),),
        )
    with pytest.raises(ProtocolError, match="must be unique"):
        protocol.InstallOwnerDeathFence(
            "duplicate", death, node_id, (descriptor, descriptor)
        )
    with pytest.raises(ProtocolError, match="preserve request order"):
        protocol.InstallOwnerDeathFenceReply(
            request, protocol.OwnerDeathFenceDisposition.FENCED, ()
        )
    with pytest.raises(ProtocolError, match="cannot carry"):
        protocol.InstallOwnerDeathFence(
            "bad-owner-wide-manifest", death, node_id, (descriptor,),
            protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
        )
    with pytest.raises(ProtocolError, match="positive pin_count"):
        protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.PINNED
        )


@pytest.mark.unit
def test_atomic_scan_reports_ordered_typed_states_without_deleting_bytes() -> None:
    node = _bare_node()
    owner = _id(WorkerID, 10)
    death = _death(owner)
    present = _descriptor(node.node_id, owner, 11, b"present")
    absent = _descriptor(node.node_id, owner, 12, b"absent")
    conflict = _descriptor(node.node_id, owner, 13, b"expected")
    pinned = _descriptor(node.node_id, owner, 14, b"pinned")

    _put_exact(node, present, b"present")
    _put_exact(node, pinned, b"pinned")
    node._object_store.pin(pinned.object_id, "live-transfer")
    node._object_store.put(conflict.object_id, b"different")
    node._sealed_metadata[conflict.object_id] = (
        conflict.producer_attempt_id, owner, len(b"different"),
        hashlib.sha256(b"different").hexdigest(),
    )
    used_before = node._object_store.used_bytes
    request = protocol.InstallOwnerDeathFence(
        "scan-1", death, node.node_id,
        # Deliberately not canonical order: reply order is caller order even
        # though internal object-lock acquisition is canonical.
        (pinned, absent, present, conflict),
    )

    first = node._handle_install_owner_death_fence(request)

    assert first.request == request and first.accepted
    assert tuple(item.descriptor for item in first.observations) == (
        pinned, absent, present, conflict
    )
    assert tuple(item.status for item in first.observations) == (
        protocol.OwnerDeathReplicaStatus.PINNED,
        protocol.OwnerDeathReplicaStatus.ABSENT,
        protocol.OwnerDeathReplicaStatus.PRESENT,
        protocol.OwnerDeathReplicaStatus.CONFLICT,
    )
    assert first.observations[0].pin_count == 1
    assert node._object_store.used_bytes == used_before
    assert node._object_store.contains(present.object_id)
    assert node._object_store.contains(pinned.object_id)
    assert node._object_store.contains(conflict.object_id)

    # Exact replay returns the frozen witness rather than observing drift.
    node._object_store.put(absent.object_id, b"absent")
    node._sealed_metadata[absent.object_id] = (
        absent.producer_attempt_id, owner, absent.size_bytes, absent.checksum
    )
    replay = node._handle_install_owner_death_fence(request)
    assert replay is first
    assert replay.observations[1].status is (
        protocol.OwnerDeathReplicaStatus.ABSENT
    )


@pytest.mark.unit
def test_owner_wide_sweep_deletes_every_unpinned_local_replica() -> None:
    node = _bare_node()
    owner = _id(WorkerID, 15)
    other_owner = _id(WorkerID, 16)
    first = _descriptor(node.node_id, owner, 17, b"primary")
    second = _descriptor(node.node_id, owner, 18, b"secondary")
    unrelated = _descriptor(node.node_id, other_owner, 19, b"keep")
    _put_exact(node, first, b"primary")
    _put_exact(node, second, b"secondary")
    _put_exact(node, unrelated, b"keep")
    request = protocol.InstallOwnerDeathFence(
        "ordinary-owner-sweep", _death(owner), node.node_id, (),
        protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )

    reply = node._handle_install_owner_death_fence(request)

    assert reply.accepted and reply.complete
    assert tuple(item.descriptor for item in reply.observations) == (
        first, second,
    )
    assert all(
        item.status is protocol.OwnerDeathReplicaStatus.ABSENT
        for item in reply.observations
    )
    assert not node._object_store.contains(first.object_id, sealed_only=False)
    assert not node._object_store.contains(second.object_id, sealed_only=False)
    assert first.object_id not in node._sealed_metadata
    assert second.object_id not in node._sealed_metadata
    assert node._object_store.get(unrelated.object_id) == b"keep"
    # A terminal exact replay returns the frozen final acknowledgement.
    assert node._handle_install_owner_death_fence(request) is reply


@pytest.mark.unit
def test_owner_wide_sweep_cleans_same_object_on_primary_and_secondary() -> None:
    primary = _bare_node(_id(NodeID, 61))
    secondary = _bare_node(_id(NodeID, 62))
    owner = _id(WorkerID, 63)
    task = _id(TaskID, 64)
    attempt = AttemptID(task, 0)
    object_id = ObjectID.for_task(task)
    payload = b"replicated ordinary object"
    checksum = hashlib.sha256(payload).hexdigest()

    for node in (primary, secondary):
        descriptor = protocol.ObjectStoreDescriptor(
            object_id, owner, attempt, node.node_id, len(payload), checksum
        )
        _put_exact(node, descriptor, payload)
        request = protocol.InstallOwnerDeathFence(
            "ordinary-replica-sweep:{}".format(node.node_id),
            _death(owner), node.node_id, (),
            protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
        )
        reply = node._handle_install_owner_death_fence(request)
        assert reply.complete
        assert reply.observations == (
            protocol.OwnerDeathReplicaObservation(
                descriptor, protocol.OwnerDeathReplicaStatus.ABSENT
            ),
        )
        assert not node._object_store.contains(object_id, sealed_only=False)
        assert object_id not in node._sealed_metadata


@pytest.mark.unit
def test_owner_wide_sweep_retries_pinned_replica_then_caches_final_ack() -> None:
    node = _bare_node()
    owner = _id(WorkerID, 20)
    descriptor = _descriptor(node.node_id, owner, 21, b"pinned")
    _put_exact(node, descriptor, b"pinned")
    pin = node._object_store.pin(descriptor.object_id, "live-reader")
    request = protocol.InstallOwnerDeathFence(
        "ordinary-owner-pinned", _death(owner), node.node_id, (),
        protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )

    pending = node._handle_install_owner_death_fence(request)
    assert pending.accepted and not pending.complete
    assert pending.observations == (
        protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.PINNED, 1
        ),
    )
    assert request.request_id not in node._owner_death_fence_outcomes
    assert node._object_store.unpin(descriptor.object_id, pin)

    completed = node._handle_install_owner_death_fence(request)
    assert completed.accepted and completed.complete
    assert completed.observations == (
        protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.ABSENT
        ),
    )
    assert node._handle_install_owner_death_fence(request) is completed
    assert not node._object_store.contains(
        descriptor.object_id, sealed_only=False
    )


@pytest.mark.unit
def test_owner_wide_fence_rejects_late_seal_and_new_source_pin() -> None:
    node = _bare_node()
    owner = _id(WorkerID, 22)
    existing = _descriptor(node.node_id, owner, 23, b"existing")
    _put_exact(node, existing, b"existing")
    request = protocol.InstallOwnerDeathFence(
        "ordinary-owner-late-write", _death(owner), node.node_id, (),
        protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )
    assert node._handle_install_owner_death_fence(request).complete

    late = _descriptor(node.node_id, owner, 24, b"late")
    sealed = node._handle_seal_object(protocol.SealObject.from_data(
        late.object_id, late.producer_attempt_id, owner, b"late"
    ))
    assert not sealed.sealed and "fenced by Worker death" in (sealed.error or "")
    pin_reply = node._handle_pin_object_for_transfer(
        protocol.PinObjectForTransfer("late-pin", existing, _id(NodeID, 25))
    )
    assert not pin_reply.pinned
    assert "fenced by Worker death" in (pin_reply.error or "")


@pytest.mark.unit
def test_owner_wide_sweep_does_not_cache_conflicting_local_metadata() -> None:
    node = _bare_node()
    owner = _id(WorkerID, 26)
    descriptor = _descriptor(node.node_id, owner, 27, b"expected")
    # The owner index says this is a sealed candidate, but the physical bytes
    # disagree.  A sweep must preserve it for diagnosis/retry, not manufacture
    # successful deletion or cache a clean acknowledgement.
    node._object_store.put(descriptor.object_id, b"different")
    node._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id, owner, descriptor.size_bytes,
        descriptor.checksum,
    )
    request = protocol.InstallOwnerDeathFence(
        "ordinary-owner-conflict", _death(owner), node.node_id, (),
        protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )

    first = node._handle_install_owner_death_fence(request)
    second = node._handle_install_owner_death_fence(request)

    assert first.accepted and not first.complete
    assert second.accepted and not second.complete
    assert first.observations[0].status is (
        protocol.OwnerDeathReplicaStatus.CONFLICT
    )
    assert request.request_id not in node._owner_death_fence_outcomes
    assert node._object_store.contains(descriptor.object_id)


@pytest.mark.unit
def test_same_owner_allows_two_publication_scans_but_fences_conflicts() -> None:
    node = _bare_node()
    owner = _id(WorkerID, 20)
    death = _death(owner)
    first_replica = _descriptor(node.node_id, owner, 21, b"one")
    second_replica = _descriptor(node.node_id, owner, 22, b"two")
    _put_exact(node, first_replica, b"one")
    _put_exact(node, second_replica, b"two")
    first_request = protocol.InstallOwnerDeathFence(
        "publication-one", death, node.node_id, (first_replica,)
    )
    second_request = protocol.InstallOwnerDeathFence(
        "publication-two", death, node.node_id, (second_replica,)
    )

    first = node._handle_install_owner_death_fence(first_request)
    second = node._handle_install_owner_death_fence(second_request)

    assert first.accepted and second.accepted
    assert first.observations[0].descriptor == first_replica
    assert second.observations[0].descriptor == second_replica
    assert len(node._owner_death_fences) == 1
    assert len(node._owner_death_fence_outcomes) == 2

    rebound = node._handle_install_owner_death_fence(
        protocol.InstallOwnerDeathFence(
            first_request.request_id, death, node.node_id, (second_replica,)
        )
    )
    assert rebound.disposition is protocol.OwnerDeathFenceDisposition.CONFLICT
    assert "request_id" in (rebound.error or "")
    conflicting_proof = node._handle_install_owner_death_fence(
        protocol.InstallOwnerDeathFence(
            "publication-three",
            _death(owner, detection_id="different-proof", death_epoch=8),
            node.node_id,
        )
    )
    assert conflicting_proof.disposition is (
        protocol.OwnerDeathFenceDisposition.CONFLICT
    )
    assert len(node._owner_death_fence_outcomes) == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "reason",
    [
        protocol.WorkerDeathReason.PROCESS_EXIT,
        protocol.WorkerDeathReason.NODE_EXIT,
    ],
)
def test_installed_fence_permanently_rejects_owner_seal_and_localization(
    reason: protocol.WorkerDeathReason,
) -> None:
    node = _bare_node()
    owner = _id(WorkerID, 30)
    payload = b"late-result"
    descriptor = _descriptor(node.node_id, owner, 31, payload)
    request = protocol.InstallOwnerDeathFence(
        "seal-fence-{}".format(reason.value),
        _death(owner, reason=reason),
        node.node_id,
    )
    assert node._handle_install_owner_death_fence(request).accepted

    seal = node._handle_seal_object(
        protocol.SealObject.from_data(
            descriptor.object_id, descriptor.producer_attempt_id, owner, payload
        )
    )
    assert not seal.sealed and "fenced by Worker death" in (seal.error or "")
    assert not node._object_store.contains(
        descriptor.object_id, sealed_only=False
    )
    with pytest.raises(RuntimeError, match="fenced by Worker death"):
        node._localize_one_dependency(descriptor)
    assert not node._object_store.contains(
        descriptor.object_id, sealed_only=False
    )


@pytest.mark.heavy
def test_pull_in_progress_linearizes_before_fence_and_is_witnessed_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _bare_node(_id(NodeID, 40))
    source_id = _id(NodeID, 41)
    owner = _id(WorkerID, 42)
    payload = b"x" * 37
    source = _descriptor(source_id, owner, 43, payload)
    local = protocol.ObjectStoreDescriptor(
        source.object_id, source.owner_worker_id, source.producer_attempt_id,
        target.node_id, source.size_bytes, source.checksum,
    )
    target._cluster_addresses[source_id] = ("127.0.0.1", 29041)
    pin_entered = threading.Event()
    permit_pin = threading.Event()

    def rpc(_address, handler, message):
        if handler == PIN_OBJECT_HANDLER:
            assert isinstance(message, protocol.PinObjectForTransfer)
            pin_entered.set()
            assert permit_pin.wait(2.0)
            return protocol.PinObjectForTransferReply(
                message.transfer_id, message.descriptor, True
            )
        if handler == GET_OBJECT_CHUNK_HANDLER:
            assert isinstance(message, protocol.GetObjectChunk)
            data = payload[message.offset:message.offset + message.size_bytes]
            return protocol.GetObjectChunkReply(
                message.transfer_id, message.object_id, source_id,
                message.offset, data=data, ok=True,
            )
        if handler == RELEASE_OBJECT_PIN_HANDLER:
            assert isinstance(message, protocol.ReleaseObjectPin)
            return protocol.ReleaseObjectPinReply(
                message.transfer_id, message.object_id, source_id,
                accepted=True, released=True,
            )
        raise AssertionError(handler)

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    localized: list[object] = []
    localization_errors: list[BaseException] = []
    fence_replies: list[protocol.InstallOwnerDeathFenceReply] = []

    def localize() -> None:
        try:
            localized.append(target._localize_one_dependency(source))
        except BaseException as exc:  # surface thread failures in the test
            localization_errors.append(exc)

    def install_fence() -> None:
        fence_replies.append(
            target._handle_install_owner_death_fence(
                protocol.InstallOwnerDeathFence(
                    "during-pull", _death(owner), target.node_id, (local,)
                )
            )
        )

    pull_thread = threading.Thread(target=localize)
    pull_thread.start()
    assert pin_entered.wait(2.0)
    fence_thread = threading.Thread(target=install_fence)
    fence_thread.start()
    # The fence shares the target object lock and cannot witness the incomplete
    # target created by ObjectManager while the network pull is in progress.
    assert not fence_thread.join(0.05)
    assert fence_replies == []

    permit_pin.set()
    pull_thread.join(2.0)
    fence_thread.join(2.0)
    assert not pull_thread.is_alive() and not fence_thread.is_alive()
    assert localization_errors == [] and localized == [local]
    assert len(fence_replies) == 1
    assert fence_replies[0].observations == (
        protocol.OwnerDeathReplicaObservation(
            local, protocol.OwnerDeathReplicaStatus.PRESENT
        ),
    )
    assert target._object_store.get(local.object_id) == payload

    # Once the fence linearizes, even the local-ready dependency fast path is
    # closed permanently for this owner.
    with pytest.raises(RuntimeError, match="fenced by Worker death"):
        target._localize_one_dependency(source)


@pytest.mark.heavy
def test_owner_wide_sweep_fences_inflight_pull_before_late_seal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _bare_node(_id(NodeID, 70))
    source_id = _id(NodeID, 71)
    owner = _id(WorkerID, 72)
    payload = b"owner-wide pull race"
    source = _descriptor(source_id, owner, 73, payload)
    target._cluster_addresses[source_id] = ("127.0.0.1", 29071)
    pin_entered = threading.Event()
    permit_pin = threading.Event()

    def rpc(_address, handler, message):
        if handler == PIN_OBJECT_HANDLER:
            pin_entered.set()
            assert permit_pin.wait(2.0)
            return protocol.PinObjectForTransferReply(
                message.transfer_id, message.descriptor, True
            )
        if handler == GET_OBJECT_CHUNK_HANDLER:
            data = payload[message.offset:message.offset + message.size_bytes]
            return protocol.GetObjectChunkReply(
                message.transfer_id, message.object_id, source_id,
                message.offset, data=data, ok=True,
            )
        if handler == RELEASE_OBJECT_PIN_HANDLER:
            return protocol.ReleaseObjectPinReply(
                message.transfer_id, message.object_id, source_id,
                accepted=True, released=True,
            )
        raise AssertionError(handler)

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    localized = []
    errors = []

    def localize() -> None:
        try:
            localized.append(target._localize_one_dependency(source))
        except BaseException as exc:
            errors.append(exc)

    pull = threading.Thread(target=localize)
    pull.start()
    assert pin_entered.wait(2.0)
    request = protocol.InstallOwnerDeathFence(
        "owner-wide-during-pull", _death(owner), target.node_id, (),
        protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )
    # The pull has no sealed metadata yet.  The owner-wide fence wins now,
    # observes no committed replica, and makes the pull's later seal illegal.
    sweep_reply = target._handle_install_owner_death_fence(request)
    assert sweep_reply.complete and sweep_reply.observations == ()

    permit_pin.set()
    pull.join(2.0)
    assert localized == []
    assert len(errors) == 1
    assert "fenced by Worker death" in str(errors[0])
    assert not target._object_store.contains(
        source.object_id, sealed_only=False
    )
    assert source.object_id not in target._sealed_metadata
