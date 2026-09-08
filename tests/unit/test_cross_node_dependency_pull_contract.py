"""Pure contracts for cross-node, store-backed task dependencies.

These tests deliberately perform no socket I/O and start no processes.  They
pin down the byte-free protocol boundary and the local pull-before-ready state
transition used by the bounded end-to-end acceptance test.
"""

from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import (
    AttemptID,
    JobID,
    LeaseID,
    NodeID,
    ObjectID,
    TaskID,
    WorkerID,
)
from miniray.object_manager import ObjectManager, PullAction, PullState
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectOwnerTable
from miniray.resources import AllocationToken, ResourceVector


pytestmark = pytest.mark.unit


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _identity(
    byte: int = 1,
) -> tuple[JobID, TaskID, AttemptID, ObjectID, WorkerID]:
    job_id = _id(JobID, byte)
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    attempt_id = AttemptID(task_id, 0)
    return (
        job_id,
        task_id,
        attempt_id,
        ObjectID.for_task(task_id, 0),
        _id(WorkerID, byte + 1),
    )


def _descriptor(
    *, byte: int = 1, node_id: NodeID | None = None, payload: bytes = b"payload"
) -> protocol.ObjectStoreDescriptor:
    _job_id, _task_id, attempt_id, object_id, owner_worker_id = _identity(byte)
    return protocol.ObjectStoreDescriptor(
        object_id=object_id,
        owner_worker_id=owner_worker_id,
        producer_attempt_id=attempt_id,
        node_id=node_id or _id(NodeID, byte + 2),
        size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _task_spec(
    descriptor: protocol.ObjectStoreDescriptor, *, consumer_byte: int = 20
) -> protocol.TaskSpec:
    job_id, task_id, attempt_id, _object_id, owner_worker_id = _identity(
        consumer_byte
    )
    return protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=attempt_id,
        function=protocol.FunctionKey(job_id, __name__, "consume", "v1"),
        args=(protocol.RefArg(descriptor.object_id, descriptor.owner_worker_id),),
        num_returns=1,
        resources=ResourceVector({"CPU": 1, "target_only": 1}),
        owner_worker_id=owner_worker_id,
    )


def test_dependency_descriptor_is_typed_immutable_metadata_without_bytes() -> None:
    source_node_id = _id(NodeID, 3)
    payload = b"D" * (64 * 1024)
    descriptor = _descriptor(node_id=source_node_id, payload=payload)

    assert descriptor.node_id == source_node_id
    assert descriptor.size_bytes == len(payload)
    assert descriptor.checksum == hashlib.sha256(payload).hexdigest()
    assert descriptor.producer_attempt_id.task_id == descriptor.object_id.task_id
    assert not hasattr(descriptor, "data")
    assert not hasattr(descriptor, "inline_data")
    assert not hasattr(descriptor, "payload")

    with pytest.raises(ProtocolError):
        protocol.ObjectStoreDescriptor(
            object_id=descriptor.object_id,
            owner_worker_id=descriptor.owner_worker_id,
            producer_attempt_id=AttemptID(
                TaskID.derive(
                    _id(JobID, 9), TaskID.for_driver(_id(JobID, 9)), 0
                ),
                0,
            ),
            node_id=source_node_id,
            size_bytes=descriptor.size_bytes,
            checksum=descriptor.checksum,
        )
    with pytest.raises(ProtocolError):
        protocol.ObjectStoreDescriptor(
            object_id=descriptor.object_id,
            owner_worker_id=descriptor.owner_worker_id,
            producer_attempt_id=descriptor.producer_attempt_id,
            node_id=source_node_id,
            size_bytes=descriptor.size_bytes,
            checksum=descriptor.checksum.upper(),
        )


def test_lease_grant_proves_dependencies_are_sealed_on_granting_node() -> None:
    source_node_id = _id(NodeID, 4)
    target_node_id = _id(NodeID, 5)
    descriptor = _descriptor(node_id=source_node_id)
    local_descriptor = protocol.ObjectStoreDescriptor(
        object_id=descriptor.object_id,
        owner_worker_id=descriptor.owner_worker_id,
        producer_attempt_id=descriptor.producer_attempt_id,
        node_id=target_node_id,
        size_bytes=descriptor.size_bytes,
        checksum=descriptor.checksum,
    )
    spec = _task_spec(descriptor)
    lease_id = _id(LeaseID, 6)
    requester_node_id = _id(NodeID, 7)
    requester_worker_id = _id(WorkerID, 8)
    target_worker_id = _id(WorkerID, 9)

    request = protocol.RequestWorkerLease(
        lease_id=lease_id,
        task_id=spec.task_id,
        attempt_id=spec.attempt_id,
        resources=spec.resources,
        requester_node_id=requester_node_id,
        requester_worker_id=requester_worker_id,
        preferred_node_id=requester_node_id,
        dependencies=[descriptor],
    )
    grant = protocol.GrantWorkerLease(
        lease_id=lease_id,
        task_id=spec.task_id,
        attempt_id=spec.attempt_id,
        node_id=target_node_id,
        worker_id=target_worker_id,
        worker_address=("127.0.0.1", 12002),
        allocation_token=AllocationToken("target-allocation"),
        dependencies=[local_descriptor],
    )
    push = protocol.PushTask(
        lease_id, target_worker_id, spec, dependencies=grant.dependencies
    )

    assert request.dependencies == (descriptor,)
    assert request.dependencies[0].node_id == source_node_id
    assert grant.dependencies == (local_descriptor,)
    assert all(item.node_id == grant.node_id for item in grant.dependencies)
    assert push.dependencies == grant.dependencies
    assert push.spec.args == (
        protocol.RefArg(descriptor.object_id, descriptor.owner_worker_id),
    )

    wrong_location = _descriptor(byte=11, node_id=source_node_id)
    with pytest.raises(ProtocolError):
        protocol.GrantWorkerLease(
            lease_id=_id(LeaseID, 12),
            task_id=spec.task_id,
            attempt_id=spec.attempt_id,
            node_id=target_node_id,
            worker_id=target_worker_id,
            worker_address=("127.0.0.1", 12002),
            allocation_token=AllocationToken("wrong-location"),
            dependencies=(wrong_location,),
        )


def test_dependency_lists_reject_duplicate_object_ids() -> None:
    descriptor = _descriptor()
    duplicate = protocol.ObjectStoreDescriptor(
        object_id=descriptor.object_id,
        owner_worker_id=descriptor.owner_worker_id,
        producer_attempt_id=descriptor.producer_attempt_id,
        node_id=_id(NodeID, 15),
        size_bytes=descriptor.size_bytes,
        checksum=descriptor.checksum,
    )
    spec = _task_spec(descriptor)

    with pytest.raises(ProtocolError):
        protocol.RequestWorkerLease(
            lease_id=_id(LeaseID, 16),
            task_id=spec.task_id,
            attempt_id=spec.attempt_id,
            resources=spec.resources,
            requester_node_id=_id(NodeID, 17),
            requester_worker_id=_id(WorkerID, 18),
            dependencies=(descriptor, duplicate),
        )
    with pytest.raises(ProtocolError):
        protocol.PushTask(
            _id(LeaseID, 19),
            _id(WorkerID, 20),
            spec,
            dependencies=(descriptor, duplicate),
        )


def test_transfer_messages_keep_pin_identity_and_bound_chunk_ranges() -> None:
    descriptor = _descriptor(payload=b"abcdefgh")
    requester_node_id = _id(NodeID, 21)
    transfer_id = "transfer-1"

    pin = protocol.PinObjectForTransfer(
        transfer_id, descriptor, requester_node_id
    )
    pinned = protocol.PinObjectForTransferReply(
        transfer_id, descriptor, pinned=True
    )
    chunk_request = protocol.GetObjectChunk(
        transfer_id, descriptor.object_id, requester_node_id, offset=2, size_bytes=4
    )
    chunk_reply = protocol.GetObjectChunkReply(
        transfer_id,
        descriptor.object_id,
        descriptor.node_id,
        offset=2,
        data=b"cdef",
    )
    release = protocol.ReleaseObjectPin(
        transfer_id, descriptor.object_id, requester_node_id
    )
    released = protocol.ReleaseObjectPinReply(
        transfer_id,
        descriptor.object_id,
        descriptor.node_id,
        accepted=True,
        released=True,
    )

    assert pin.transfer_id == pinned.transfer_id == chunk_request.transfer_id
    assert chunk_reply.transfer_id == release.transfer_id == released.transfer_id
    assert chunk_reply.offset == chunk_request.offset
    assert len(chunk_reply.data) <= chunk_request.size_bytes
    assert released.accepted and released.released

    with pytest.raises(ProtocolError):
        protocol.GetObjectChunk(
            transfer_id,
            descriptor.object_id,
            requester_node_id,
            offset=-1,
            size_bytes=4,
        )
    with pytest.raises(ProtocolError):
        protocol.GetObjectChunkReply(
            transfer_id,
            descriptor.object_id,
            descriptor.node_id,
            offset=2,
            data=b"bytes",
            ok=False,
            error="source failed",
        )


def test_source_pin_uses_transfer_identity_and_is_released_idempotently() -> None:
    payload = b"source replica"
    descriptor = _descriptor(payload=payload)
    requester_node_id = _id(NodeID, 24)
    transfer_id = "transfer-pin-lifetime"
    store = ObjectStore(capacity_bytes=64)
    store.put(descriptor.object_id, payload)
    pin_request = protocol.PinObjectForTransfer(
        transfer_id, descriptor, requester_node_id
    )
    release_request = protocol.ReleaseObjectPin(
        transfer_id, descriptor.object_id, requester_node_id
    )

    pin_token = (pin_request.transfer_id, pin_request.requester_node_id)
    assert store.pin(pin_request.descriptor.object_id, pin_token) == pin_token
    assert store.pin(pin_request.descriptor.object_id, pin_token) == pin_token
    assert store.snapshot(descriptor.object_id).pin_count == 1
    assert not store.delete(descriptor.object_id)

    release_token = (release_request.transfer_id, release_request.requester_node_id)
    assert store.unpin(release_request.object_id, release_token)
    assert not store.unpin(release_request.object_id, release_token)
    assert store.snapshot(descriptor.object_id).pin_count == 0
    assert store.delete(descriptor.object_id)


def test_target_pull_is_not_ready_until_checksum_verified_and_store_sealed() -> None:
    payload = b"abcdefgh"
    descriptor = _descriptor(payload=payload)
    target_node_id = _id(NodeID, 30)
    owner = ObjectOwnerTable()
    owner.register(
        descriptor.object_id, current_attempt=descriptor.producer_attempt_id
    )
    assert owner.publish_stored(
        descriptor.object_id,
        descriptor.producer_attempt_id,
        descriptor.node_id,
    )
    target_store = ObjectStore(capacity_bytes=64)
    manager = ObjectManager(target_node_id, target_store, owner)

    decision = manager.request_pull(
        descriptor.object_id,
        waiter_token="consumer-attempt",
        expected_size=descriptor.size_bytes,
        expected_checksum=descriptor.checksum,
    )
    assert decision.action is PullAction.START_PULL
    manager.receive_chunk(
        descriptor.object_id,
        payload[:4],
        offset=0,
        transfer_id=decision.transfer_id,
        source_location=descriptor.node_id,
    )
    assert manager.snapshot(descriptor.object_id).state is PullState.PULLING
    assert not manager.is_ready(descriptor.object_id)

    manager.receive_chunk(
        descriptor.object_id,
        payload[4:],
        offset=4,
        transfer_id=decision.transfer_id,
        source_location=descriptor.node_id,
    )
    completion = manager.finish_pull(
        descriptor.object_id,
        transfer_id=decision.transfer_id,
        source_location=descriptor.node_id,
    )

    assert completion.local_location == target_node_id
    assert manager.snapshot(descriptor.object_id).state is PullState.READY
    assert manager.get_local(descriptor.object_id) == payload
    assert owner.snapshot(descriptor.object_id).locations == frozenset(
        {descriptor.node_id, target_node_id}
    )
