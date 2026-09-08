"""Pure validation tests for the node-to-node object-pull protocol.

No test in this module opens a socket or starts a process.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
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
from miniray.resources import AllocationToken, ResourceVector


pytestmark = pytest.mark.unit


def _ids() -> tuple[TaskID, AttemptID, ObjectID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return task_id, AttemptID(task_id, 0), ObjectID.for_task(task_id)


def _descriptor(*, node_id: NodeID | None = None) -> protocol.ObjectStoreDescriptor:
    _task_id, attempt_id, object_id = _ids()
    payload = b"sealed-object"
    return protocol.ObjectStoreDescriptor(
        object_id=object_id,
        owner_worker_id=WorkerID.random(),
        producer_attempt_id=attempt_id,
        node_id=node_id or NodeID.random(),
        size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _spec(descriptor: protocol.ObjectStoreDescriptor) -> protocol.TaskSpec:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=AttemptID(task_id, 0),
        function=protocol.FunctionKey(job_id, __name__, "consume", "v1"),
        args=(protocol.RefArg(descriptor.object_id, descriptor.owner_worker_id),),
        num_returns=1,
        resources=ResourceVector({"CPU": 1}),
        owner_worker_id=WorkerID.random(),
    )


def test_descriptor_is_frozen_and_rejects_noncanonical_metadata() -> None:
    descriptor = _descriptor()
    with pytest.raises(FrozenInstanceError):
        descriptor.size_bytes = 0  # type: ignore[misc]

    values = dict(
        object_id=descriptor.object_id,
        owner_worker_id=descriptor.owner_worker_id,
        producer_attempt_id=descriptor.producer_attempt_id,
        node_id=descriptor.node_id,
        size_bytes=descriptor.size_bytes,
        checksum=descriptor.checksum,
    )
    for name, invalid in (
        ("owner_worker_id", object()),
        ("node_id", object()),
        ("size_bytes", True),
        ("size_bytes", -1),
        ("checksum", descriptor.checksum.upper()),
        ("checksum", "0" * 63),
    ):
        changed = dict(values)
        changed[name] = invalid
        with pytest.raises(ProtocolError):
            protocol.ObjectStoreDescriptor(**changed)


def test_dependency_containers_normalize_once_and_fence_identity() -> None:
    source = _descriptor()
    spec = _spec(source)
    request = protocol.RequestWorkerLease(
        LeaseID.random(),
        spec.task_id,
        spec.attempt_id,
        spec.resources,
        NodeID.random(),
        WorkerID.random(),
        dependencies=[source],
    )
    assert request.dependencies == (source,)

    target_node_id = NodeID.random()
    local = protocol.ObjectStoreDescriptor(
        source.object_id,
        source.owner_worker_id,
        source.producer_attempt_id,
        target_node_id,
        source.size_bytes,
        source.checksum,
    )
    grant = protocol.GrantWorkerLease(
        request.lease_id,
        request.task_id,
        request.attempt_id,
        target_node_id,
        WorkerID.random(),
        ("127.0.0.1", 19000),
        AllocationToken.random(),
        dependencies=[local],
    )
    assert grant.dependencies == (local,)
    assert protocol.PushTask(
        request.lease_id, grant.worker_id, spec, grant.dependencies
    ).dependencies == (local,)

    with pytest.raises(ProtocolError):
        protocol.PushTask(request.lease_id, grant.worker_id, spec)
    with pytest.raises(ProtocolError):
        protocol.PushTask(
            request.lease_id, grant.worker_id, spec, (local, local)
        )


def test_transfer_message_error_contracts_are_unambiguous() -> None:
    descriptor = _descriptor()
    requester_node_id = NodeID.random()

    with pytest.raises(ProtocolError):
        protocol.PinObjectForTransfer("", descriptor, requester_node_id)
    with pytest.raises(ProtocolError):
        protocol.PinObjectForTransferReply(
            "transfer", descriptor, pinned=True, error="impossible"
        )
    with pytest.raises(ProtocolError):
        protocol.GetObjectChunk(
            "transfer", descriptor.object_id, requester_node_id, 0, 0
        )
    with pytest.raises(ProtocolError):
        protocol.GetObjectChunkReply(
            "transfer",
            descriptor.object_id,
            descriptor.node_id,
            0,
            data=b"must-not-leak",
            ok=False,
            error="read failed",
        )
    with pytest.raises(ProtocolError):
        protocol.ReleaseObjectPinReply(
            "transfer",
            descriptor.object_id,
            descriptor.node_id,
            accepted=False,
            released=True,
            error="unknown transfer",
        )

    replay = protocol.ReleaseObjectPinReply(
        "transfer",
        descriptor.object_id,
        descriptor.node_id,
        accepted=True,
        released=False,
    )
    assert replay.accepted and not replay.released


def test_get_object_supports_legacy_and_fully_fenced_reads() -> None:
    descriptor = _descriptor()
    requester_node_id = NodeID.random()

    legacy = protocol.GetObject(descriptor.object_id, requester_node_id)
    assert legacy.expected_attempt_id is None
    assert legacy.expected_owner_worker_id is None
    assert legacy.expected_size_bytes is None
    assert legacy.expected_checksum is None

    fenced = protocol.GetObject(
        descriptor.object_id,
        requester_node_id,
        expected_attempt_id=descriptor.producer_attempt_id,
        expected_owner_worker_id=descriptor.owner_worker_id,
        expected_size_bytes=descriptor.size_bytes,
        expected_checksum=descriptor.checksum,
    )
    assert fenced.expected_attempt_id == descriptor.producer_attempt_id
    assert fenced.expected_owner_worker_id == descriptor.owner_worker_id

    with pytest.raises(ProtocolError):
        protocol.GetObject(
            descriptor.object_id,
            requester_node_id,
            expected_attempt_id=descriptor.producer_attempt_id,
        )


def test_get_object_reply_can_fence_sealed_replica_metadata() -> None:
    descriptor = _descriptor()
    payload = b"sealed-object"
    reply = protocol.GetObjectReply(
        descriptor.object_id,
        descriptor.node_id,
        True,
        True,
        payload,
        descriptor.checksum,
        producer_attempt_id=descriptor.producer_attempt_id,
        owner_worker_id=descriptor.owner_worker_id,
        size_bytes=descriptor.size_bytes,
    )
    assert reply.producer_attempt_id == descriptor.producer_attempt_id
    assert reply.owner_worker_id == descriptor.owner_worker_id
    assert reply.size_bytes == len(payload)

    # Existing Driver reads and old fixtures remain source-compatible.
    legacy_reply = protocol.GetObjectReply(
        descriptor.object_id,
        descriptor.node_id,
        True,
        True,
        payload,
        descriptor.checksum,
    )
    assert legacy_reply.producer_attempt_id is None

    with pytest.raises(ProtocolError):
        protocol.GetObjectReply(
            descriptor.object_id,
            descriptor.node_id,
            True,
            True,
            payload,
            descriptor.checksum,
            producer_attempt_id=descriptor.producer_attempt_id,
        )
    with pytest.raises(ProtocolError):
        protocol.GetObjectReply(
            descriptor.object_id,
            descriptor.node_id,
            True,
            True,
            payload,
            descriptor.checksum,
            producer_attempt_id=descriptor.producer_attempt_id,
            owner_worker_id=descriptor.owner_worker_id,
            size_bytes=descriptor.size_bytes + 1,
        )
