"""Pure replica-drop contracts with a canonical public Core put/get path.

Unit cases use small in-memory stores and direct metadata handlers. The public
Core put/get case uses a threadless Core, one 1-KiB Node, exact Seal/Get/Drop
replies and explicit local close/GC. No runtime constructor or consumer runs.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import core as core_module, node as node_module, protocol, transport
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, _WAKE_COORDINATOR
from miniray.errors import ProtocolError, SystemTaskError, UnreconstructableObjectError
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import (
    ObjectManager,
    PullAction,
    PullStateError,
    UnknownPullError,
)
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState
from miniray.trace import EventSink
from tests.unit._pure_core import close_pure_core, make_pure_core


def _identity() -> tuple[ObjectID, AttemptID, WorkerID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return ObjectID.for_task(task_id), AttemptID(task_id, 0), WorkerID.random()


def _bare_node(capacity: int = 4096) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._object_store = ObjectStore(capacity)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._object_localization_locks = {}
    node._state_lock = threading.RLock()
    node._shutdown_request_id = None
    node._stop_event = threading.Event()
    return node


def _drop_request(
    node: NodeServer,
    object_id: ObjectID,
    attempt_id: AttemptID,
    owner_worker_id: WorkerID,
    payload: bytes,
) -> protocol.DropObjectReplica:
    return protocol.DropObjectReplica(
        object_id=object_id,
        producer_attempt_id=attempt_id,
        owner_worker_id=owner_worker_id,
        node_id=node.node_id,
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _assert_complete_echo(
    reply: protocol.DropObjectReplicaReply,
    request: protocol.DropObjectReplica,
) -> None:
    assert reply.object_id == request.object_id
    assert reply.producer_attempt_id == request.producer_attempt_id
    assert reply.owner_worker_id == request.owner_worker_id
    assert reply.node_id == request.node_id
    assert reply.checksum == request.checksum


@pytest.mark.unit
def test_drop_protocol_requires_one_matching_replica_epoch() -> None:
    object_id, attempt_id, owner = _identity()

    with pytest.raises(ProtocolError, match="belong to object_id"):
        protocol.DropObjectReplica(
            object_id,
            AttemptID(TaskID.random(), 0),
            owner,
            NodeID.random(),
            "0" * 64,
        )

    with pytest.raises(ProtocolError, match="SHA-256"):
        protocol.DropObjectReplica(
            object_id, attempt_id, owner, NodeID.random(), "not-a-digest"
        )

    with pytest.raises(ProtocolError, match="acknowledged replica drop"):
        protocol.DropObjectReplicaReply(
            object_id,
            attempt_id,
            owner,
            NodeID.random(),
            "0" * 64,
            protocol.DropObjectReplicaStatus.DROPPED,
            error="successful ACKs cannot carry errors",
        )

    with pytest.raises(ProtocolError, match="status is invalid"):
        protocol.DropObjectReplicaReply(
            object_id,
            attempt_id,
            owner,
            NodeID.random(),
            "0" * 64,
            "DROPPED",  # type: ignore[arg-type]
        )

    with pytest.raises(ProtocolError, match="must contain an error"):
        protocol.DropObjectReplicaReply(
            object_id,
            attempt_id,
            owner,
            NodeID.random(),
            "0" * 64,
            protocol.DropObjectReplicaStatus.PINNED,
        )


@pytest.mark.parametrize(
    ("status", "accepted", "dropped"),
    (
        (protocol.DropObjectReplicaStatus.DROPPED, True, True),
        (protocol.DropObjectReplicaStatus.ALREADY_DROPPED, True, False),
        (protocol.DropObjectReplicaStatus.PINNED, False, False),
        (protocol.DropObjectReplicaStatus.STALE_EPOCH, False, False),
        (protocol.DropObjectReplicaStatus.NODE_DRAINING, False, False),
        (protocol.DropObjectReplicaStatus.INCONSISTENT, False, False),
        (protocol.DropObjectReplicaStatus.REJECTED, False, False),
    ),
)
@pytest.mark.unit
def test_drop_reply_compatibility_views_follow_typed_status(
    status: protocol.DropObjectReplicaStatus,
    accepted: bool,
    dropped: bool,
) -> None:
    object_id, attempt_id, owner = _identity()
    reply = protocol.DropObjectReplicaReply(
        object_id,
        attempt_id,
        owner,
        NodeID.random(),
        "0" * 64,
        status,
        error=None if accepted else "not acknowledged",
    )

    assert reply.accepted is accepted
    assert reply.dropped is dropped
    assert reply.deleted is dropped


@pytest.mark.unit
def test_node_drop_is_idempotent_and_fences_reconstructed_attempt() -> None:
    node = _bare_node()
    object_id, attempt_0, owner = _identity()
    old_payload = b"attempt-zero"
    old_seal = protocol.SealObject.from_data(
        object_id, attempt_0, owner, old_payload
    )
    assert node._handle_seal_object(old_seal).sealed

    request = _drop_request(node, object_id, attempt_0, owner, old_payload)
    first = node._handle_drop_object_replica(request)
    repeated = node._handle_drop_object_replica(request)

    _assert_complete_echo(first, request)
    _assert_complete_echo(repeated, request)
    assert first.status is protocol.DropObjectReplicaStatus.DROPPED
    assert repeated.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert first.accepted and first.dropped
    assert repeated.accepted and not repeated.dropped
    assert not node.object_store.contains(object_id, sealed_only=False)
    assert object_id not in node._sealed_metadata

    stale_seal = node._handle_seal_object(old_seal)
    assert not stale_seal.sealed
    assert "fenced" in stale_seal.error

    attempt_1 = attempt_0.next()
    new_payload = b"attempt-one"
    assert node._handle_seal_object(
        protocol.SealObject.from_data(object_id, attempt_1, owner, new_payload)
    ).sealed

    stale = node._handle_drop_object_replica(request)

    # This exact old deletion really completed; its receipt survives new
    # bytes without granting permission to delete the reconstructed replica.
    assert stale.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert stale.accepted and not stale.dropped
    assert node.object_store.get(object_id) == new_payload
    assert node._sealed_metadata[object_id][0] == attempt_1


@pytest.mark.parametrize(
    "change",
    (
        "attempt",
        "owner",
        "checksum",
        "node",
    ),
)
@pytest.mark.unit
def test_node_drop_rejects_mismatched_replica_capability(change: str) -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    payload = b"immutable replica"
    assert node._handle_seal_object(
        protocol.SealObject.from_data(object_id, attempt_id, owner, payload)
    ).sealed
    request = _drop_request(node, object_id, attempt_id, owner, payload)
    values = {
        "object_id": request.object_id,
        "producer_attempt_id": request.producer_attempt_id,
        "owner_worker_id": request.owner_worker_id,
        "node_id": request.node_id,
        "checksum": request.checksum,
    }
    if change == "attempt":
        values["producer_attempt_id"] = attempt_id.next()
    elif change == "owner":
        values["owner_worker_id"] = WorkerID.random()
    elif change == "checksum":
        values["checksum"] = "f" * 64
    else:
        values["node_id"] = NodeID.random()

    reply = node._handle_drop_object_replica(
        protocol.DropObjectReplica(**values)
    )

    assert reply.status is (
        protocol.DropObjectReplicaStatus.REJECTED
        if change == "node"
        else protocol.DropObjectReplicaStatus.STALE_EPOCH
    )
    assert not reply.accepted and not reply.dropped
    assert node.object_store.get(object_id) == payload


@pytest.mark.unit
def test_node_cannot_drop_a_pinned_transfer_source() -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    payload = b"pinned replica"
    assert node._handle_seal_object(
        protocol.SealObject.from_data(object_id, attempt_id, owner, payload)
    ).sealed
    pin = node.object_store.pin(object_id, "teaching-transfer")
    request = _drop_request(node, object_id, attempt_id, owner, payload)

    rejected = node._handle_drop_object_replica(request)
    assert rejected.status is protocol.DropObjectReplicaStatus.PINNED
    assert not rejected.accepted and not rejected.dropped
    assert node.object_store.get(object_id) == payload

    assert node.object_store.unpin(object_id, pin)
    accepted = node._handle_drop_object_replica(request)
    assert accepted.status is protocol.DropObjectReplicaStatus.DROPPED
    assert accepted.accepted and accepted.dropped


@pytest.mark.unit
def test_missing_replica_without_matching_tombstone_is_not_acknowledged() -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    request = _drop_request(node, object_id, attempt_id, owner, b"never sealed")

    reply = node._handle_drop_object_replica(request)

    assert reply.status is protocol.DropObjectReplicaStatus.REJECTED
    assert not reply.accepted and not reply.dropped
    assert "tombstone" in reply.error


@pytest.mark.unit
def test_higher_deleted_epoch_fences_stale_drop() -> None:
    node = _bare_node()
    object_id, attempt_0, owner = _identity()
    request = _drop_request(node, object_id, attempt_0, owner, b"old")
    node._dropped_metadata[object_id] = (
        attempt_0.next(),
        owner,
        hashlib.sha256(b"new").hexdigest(),
    )

    reply = node._handle_drop_object_replica(request)

    _assert_complete_echo(reply, request)
    assert reply.status is protocol.DropObjectReplicaStatus.STALE_EPOCH
    assert not reply.accepted


@pytest.mark.unit
def test_present_replica_with_higher_tombstone_is_stale_not_deleted() -> None:
    node = _bare_node()
    object_id, attempt_0, owner = _identity()
    payload = b"impossible old replica"
    checksum = hashlib.sha256(payload).hexdigest()
    node.object_store.put(object_id, payload)
    node._sealed_metadata[object_id] = (
        attempt_0, owner, len(payload), checksum
    )
    node._dropped_metadata[object_id] = (
        attempt_0.next(), owner, hashlib.sha256(b"new").hexdigest()
    )
    request = _drop_request(node, object_id, attempt_0, owner, payload)

    reply = node._handle_drop_object_replica(request)

    assert reply.status is protocol.DropObjectReplicaStatus.STALE_EPOCH
    assert node.object_store.get(object_id) == payload


@pytest.mark.parametrize("inconsistency", ("bytes_only", "metadata_only"))
@pytest.mark.unit
def test_node_reports_store_metadata_inconsistency(
    inconsistency: str,
) -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    payload = b"split brain"
    checksum = hashlib.sha256(payload).hexdigest()
    if inconsistency == "bytes_only":
        node.object_store.put(object_id, payload)
    else:
        node._sealed_metadata[object_id] = (
            attempt_id, owner, len(payload), checksum
        )
    request = _drop_request(node, object_id, attempt_id, owner, payload)

    reply = node._handle_drop_object_replica(request)

    _assert_complete_echo(reply, request)
    assert reply.status is protocol.DropObjectReplicaStatus.INCONSISTENT
    assert not reply.accepted


@pytest.mark.unit
def test_forget_failure_is_inconsistent_then_exact_replay_finishes_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    payload = b"commit bytes before coordinator cleanup"
    assert node._handle_seal_object(
        protocol.SealObject.from_data(object_id, attempt_id, owner, payload)
    ).sealed
    request = _drop_request(node, object_id, attempt_id, owner, payload)

    class FailOnceManager:
        calls = 0

        def forget_local_replica(
            self,
            forgotten_object_id: ObjectID,
            *,
            attempt_id: AttemptID,
        ) -> bool:
            assert forgotten_object_id == object_id
            assert attempt_id == request.producer_attempt_id
            self.calls += 1
            if self.calls == 1:
                raise PullStateError("injected forget failure")
            return True

    manager = FailOnceManager()
    node._object_manager = manager
    original_delete = node.object_store.delete
    delete_calls = 0

    def counted_delete(deleted_object_id: ObjectID) -> bool:
        nonlocal delete_calls
        delete_calls += 1
        return original_delete(deleted_object_id)

    monkeypatch.setattr(node.object_store, "delete", counted_delete)

    first = node._handle_drop_object_replica(request)
    replay = node._handle_drop_object_replica(request)

    assert first.status is protocol.DropObjectReplicaStatus.INCONSISTENT
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert manager.calls == 2
    assert delete_calls == 1
    assert not node.object_store.contains(object_id, sealed_only=False)


@pytest.mark.unit
def test_finalizing_node_preserves_replica_and_returns_typed_status() -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    payload = b"keep through drain"
    assert node._handle_seal_object(
        protocol.SealObject.from_data(object_id, attempt_id, owner, payload)
    ).sealed
    request = _drop_request(node, object_id, attempt_id, owner, payload)
    node._stop_event.set()

    reply = node._handle_drop_object_replica(request)

    _assert_complete_echo(reply, request)
    assert reply.status is protocol.DropObjectReplicaStatus.NODE_DRAINING
    assert node.object_store.get(object_id) == payload


@pytest.mark.unit
def test_shutdown_drain_still_serves_embedded_owner_gc_drops() -> None:
    node = _bare_node()
    object_id, attempt_id, owner = _identity()
    payload = b"drop while ordinary workers drain"
    assert node._handle_seal_object(
        protocol.SealObject.from_data(object_id, attempt_id, owner, payload)
    ).sealed
    request = _drop_request(node, object_id, attempt_id, owner, payload)
    node._shutdown_request_id = "shutdown-1"

    first = node._handle_drop_object_replica(request)
    replay = node._handle_drop_object_replica(request)

    assert first.status is protocol.DropObjectReplicaStatus.DROPPED
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED


@pytest.mark.unit
def test_wrong_handler_message_type_is_a_transport_contract_error() -> None:
    node = _bare_node()

    with pytest.raises(TypeError, match="expects DropObjectReplica"):
        node._handle_drop_object_replica(object())


@pytest.mark.unit
def test_object_manager_forgets_ready_pull_only_after_bytes_are_deleted() -> None:
    local_node = NodeID.random()
    remote_node = NodeID.random()
    object_id, attempt_id, _owner = _identity()
    payload = b"pulled replica"
    checksum = hashlib.sha256(payload).hexdigest()
    store = ObjectStore(4096)
    manager = ObjectManager(local_node, store)
    decision = manager.request_pull(
        object_id,
        locations=(remote_node,),
        attempt_id=attempt_id,
        expected_size=len(payload),
        expected_checksum=checksum,
    )
    assert decision.action is PullAction.START_PULL
    manager.receive_object(
        object_id,
        payload,
        checksum=checksum,
        transfer_id=decision.transfer_id,
        source_location=remote_node,
    )

    with pytest.raises(PullStateError, match="while a local replica exists"):
        manager.forget_local_replica(object_id, attempt_id=attempt_id)

    assert store.delete(object_id)
    assert manager.forget_local_replica(object_id, attempt_id=attempt_id)
    with pytest.raises(UnknownPullError):
        manager.snapshot(object_id)


@pytest.fixture
def _no_public_put_runtime(monkeypatch):
    """Only the canonical public put/get case uses these runtime tripwires."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure put/drop contract attempted runtime infrastructure")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure reference close attempted a blocking wait"
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_public_put_runtime")
def test_core_drop_marks_put_lost_and_get_reports_unreconstructable() -> None:
    node = _bare_node(1024)
    core = make_pure_core()
    core.node_id = node.node_id
    core.node_address = ("put-drop.invalid", 1)
    core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
    core.inline_threshold = 1
    value = {"large": "enough"}
    payload = cloudpickle.dumps(value)
    assert core.inline_threshold < len(payload) <= 128
    task_id = TaskID.for_put(core.job_id, core.worker_id, 0)
    object_id, attempt = ObjectID.for_task(task_id), AttemptID(task_id, 0)
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
        core.worker_id, node.node_id, hashlib.sha256(payload).hexdigest(),
    )
    calls = []
    ref = None

    def rpc(address: object, handler: str, message: object) -> object:
        assert address == core.node_address
        assert len(calls) < 3 and handler == ("seal_object", "get_object", "drop_object_replica")[len(calls)]
        calls.append((handler, message))
        if handler == "seal_object":
            assert message == protocol.SealObject.from_data(object_id, attempt, core.worker_id, payload)
            assert core.owner_table.snapshot(object_id).state is ObjectState.PENDING
            assert core._recovery.reconstruction_snapshot(object_id).is_put
            reply = node._handle_seal_object(message)
            assert reply.sealed and node.object_store.get(object_id) == payload
            assert core.owner_table.snapshot(object_id).state is ObjectState.PENDING
            return reply
        if handler == "get_object":
            assert message == protocol.GetObject(
                object_id, core.node_id, expected_attempt_id=attempt,
                expected_owner_worker_id=core.worker_id, expected_size_bytes=len(payload),
                expected_checksum=descriptor.checksum,
            )
            return node._handle_get_object(message)
        assert message == _drop_request(node, object_id, attempt, core.worker_id, payload)
        before = core.owner_table.snapshot(object_id)
        assert before.state is ObjectState.READY_STORED
        reply = node._handle_drop_object_replica(message)
        _assert_complete_echo(reply, message)
        assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
        assert node.object_store.used_bytes == 0 and object_id not in node._sealed_metadata
        # Only this actual, identity-complete Node ACK authorizes Core to
        # remove its location. Physical deletion did not mutate the owner.
        assert core.owner_table.snapshot(object_id) == before
        return reply

    core._rpc = rpc
    try:
        ref = core.put(value)
        assert ref.object_id == object_id and ref.owner_worker_id == core.worker_id
        assert core._put_index == 1 and core._inflight_puts == 0
        ready = core.owner_table.snapshot(object_id)
        assert ready.state is ObjectState.READY_STORED and ready.current_attempt == attempt
        assert ready.locations == frozenset({node.node_id}) and len(ready.local_tokens) == 1
        assert ready.canonical_stored_result == core._stored_descriptors[object_id] == descriptor
        assert ready.producer_task_spec is None and ready.output_publication is None
        recovery = core._recovery.reconstruction_snapshot(object_id)
        assert recovery.is_put and recovery.lineage is None and recovery.task_state is None
        assert core.get(ref) == value

        assert core.drop_object(ref)
        assert not core.drop_object(ref)
        snapshot = core.owner_table.snapshot(ref.object_id)
        assert snapshot.state is ObjectState.LOST
        assert snapshot.locations == frozenset()
        assert snapshot.producer_task_spec is None
        assert snapshot.current_attempt == attempt and snapshot.local_tokens == ready.local_tokens
        assert snapshot.canonical_stored_result == descriptor and snapshot.error is None
        assert node._dropped_metadata[object_id] == (attempt, core.worker_id, descriptor.checksum)
        assert len(node._replica_drop_receipts) == 1

        with pytest.raises(UnreconstructableObjectError, match="no replayable"):
            core.get(ref)
        assert core.owner_table.snapshot(object_id) == snapshot
        assert core._recovery.reconstruction_snapshot(object_id) == recovery
        assert [handler for handler, _ in calls] == ["seal_object", "get_object", "drop_object_replica"]
        assert not core._task_finish_barriers and not core._protocol_unresolved
        assert core._accepted_task_count == 0 and not core._finished_tasks

        # Put has no executable Task to finish. Its actual final local token
        # instead authorizes normal owner GC and forgetting the put identity.
        ref.close(timeout=0)
        assert len(core._reference_mailbox.releases) == 1
        assert not core.owner_table.snapshot(object_id).local_tokens
        assert core._reference_mailbox.pending.qsize() <= 4
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert not core._recovery.reconstruction_snapshot(object_id).is_put
        assert core._recovery.lineage_for_object(object_id) is None
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
        # _wake_object admitted a wake for the absent coordinator. Consume its
        # finite actual suffix, never reset the queue or its work counter.
        count = core._submissions.qsize()
        assert count <= 4
        for _ in range(count):
            item = core._submissions.get_nowait()
            try:
                assert item is _WAKE_COORDINATOR
            finally:
                core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert node.object_store.used_bytes == 0 and not node._sealed_metadata
        assert len(calls) == 3
    finally:
        if ref is not None:
            ref.close(timeout=0)
        close_pure_core(core)


@pytest.mark.unit
def test_core_does_not_remove_owner_location_after_rejected_drop() -> None:
    core = object.__new__(CoreWorker)
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 12345)
    core.gcs_address = None
    core._owner_table = ObjectOwnerTable()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._membership_epoch = 0
    core._installed_cluster_snapshot = None
    core._home_route = _HomeRoute(core.node_id, core.node_address, 0)
    core._dead_nodes = {}
    core._completion = threading.Condition(core._state_lock)
    core.event_sink = EventSink()
    object_id, attempt_id, _ = _identity()
    payload = b"stored"
    core._owner_table.register(object_id, current_attempt=attempt_id)
    core._owner_table.publish_stored(object_id, attempt_id, core.node_id)
    core._objects[object_id] = type("Waiter", (), {"event": threading.Event()})()
    core._stored_descriptors[object_id] = protocol.ResultDescriptor(
        object_id,
        protocol.ResultStorage.OBJECT_STORE,
        len(payload),
        core.worker_id,
        core.node_id,
        hashlib.sha256(payload).hexdigest(),
    )
    from miniray.core import ObjectRef

    ref = ObjectRef(object_id, core.worker_id)
    core._rpc = lambda *_args: protocol.DropObjectReplicaReply(
        object_id,
        attempt_id,
        core.worker_id,
        core.node_id,
        hashlib.sha256(payload).hexdigest(),
        protocol.DropObjectReplicaStatus.PINNED,
        error="pinned",
    )

    with pytest.raises(SystemTaskError, match="pinned"):
        core.drop_object(ref)

    assert core.owner_table.snapshot(object_id).locations == frozenset(
        {core.node_id}
    )
