"""Bounded owner/Node contracts for complete debug-drop request identity.

One threadless Core, one 1-KiB Node store, one stored ObjectID and one small
source-container owner record per case. Real canonical publication, Acquire/Release and Node
Seal/Drop handlers establish the state; no reply or deletion success is faked.
The epoch case exercises the owner CAS boundary directly, not Task execution
or an end-to-end reconstruction. At most two small epochs and two actual
deletions are permitted, including final reference collection in teardown.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace

import pytest

from miniray import control, core as core_module, node as node_module, protocol
from miniray import transport, worker
from miniray.contained_edges import ContainedReferenceEdge, ContainedReferenceHold
from miniray.core import CoreWorker
from miniray.ids import AttemptID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from tests.unit._pure_core import close_pure_core, make_pure_core


def _forbid_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("owned-drop identity contract attempted runtime or blocking work")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (NodeServer, "__init__"), (worker.WorkerServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "__init__"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (control, core_module, node_module, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)


class _DropBoundary:
    def __init__(self, core, monkeypatch):
        self.core = core
        self.node = node = object.__new__(NodeServer)
        node.node_id = core.node_id
        node._state_lock = threading.RLock()
        node._object_store = self.store = ObjectStore(1024)
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata = {}
        node._dropped_metadata = {}
        node._object_localization_locks = {}
        self.calls, self.deletions, self.seals = [], [], []
        self.borrower = WorkerID.random()
        self.token, self.local_token = "drop-borrower", "owner-live"
        task_id = TaskID.derive(core.job_id, core.driver_task_id, 0)
        self.object_id = ObjectID.for_task(task_id)
        self.attempt = AttemptID(task_id, 0)
        container_task = TaskID.derive(core.job_id, core.driver_task_id, 1)
        self.source_container = ObjectID.for_task(container_task)
        self.source = protocol.ContainedTransferSource(ContainedReferenceHold(
            self.source_container, core.worker_id, "drop-source",
        ))
        core.owner_table.register(
            self.source_container, current_attempt=AttemptID(container_task, 0),
            local_token="source-container-live",
        )
        assert core.owner_table.add_outgoing_contained_edge(
            self.source_container, ContainedReferenceEdge(
                self.source_container, self.object_id, core.worker_id,
                core.owner_address, self.source.hold.transfer_token,
            ),
        )
        assert core.owner_table.publish_inline(
            self.source_container, AttemptID(container_task, 0), b"source-container",
        )
        core.owner_table.register(
            self.object_id, current_attempt=self.attempt,
            local_token=self.local_token,
        )
        real_delete = self.store.delete

        def delete(object_id):
            assert object_id == self.object_id and len(self.deletions) < 2
            assert self.store.contains(object_id)
            removed = real_delete(object_id)
            assert removed
            self.deletions.append(object_id)
            return removed

        monkeypatch.setattr(self.store, "delete", delete)
        core._rpc = self.rpc
        self.publish(self.attempt, b"original-result")
        assert core.owner_table.add_contained_reference(
            self.object_id, self.source.hold
        )
        self.acquire = protocol.AcquireBorrowedObject(
            self.object_id, core.worker_id, self.borrower, self.source, self.token
        )
        self.release = protocol.ReleaseBorrowedObject(
            self.object_id, core.worker_id, self.borrower, self.token
        )
        admitted = core.acquire_exported_reference(self.acquire)
        assert admitted.accepted and admitted.acquired
        ready = core.owner_table.snapshot(self.object_id)
        capability = (self.borrower, self.token)
        assert capability in ready.borrowed_tokens
        assert dict(ready.borrowed_sources)[capability] == self.source
        self.request = protocol.RequestDropOwnedObject(
            "drop-operation", self.object_id, core.worker_id, self.borrower,
            self.source, self.token, self.attempt, None,
        )

    def publish(self, attempt, payload):
        assert len(self.seals) < 2 and len(payload) <= 128
        seal = protocol.SealObject.from_data(
            self.object_id, attempt, self.core.worker_id, payload
        )
        reply = self.node._handle_seal_object(seal)
        assert reply.sealed and reply.object_id == self.object_id
        assert self.store.get(self.object_id) == payload
        descriptor = protocol.ResultDescriptor(
            self.object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
            self.core.worker_id, self.node.node_id, hashlib.sha256(payload).hexdigest(),
        )
        assert self.core.owner_table.publish_stored(
            self.object_id, attempt, self.node.node_id, descriptor=descriptor
        )
        self.core._stored_descriptors[self.object_id] = descriptor
        ready = self.core.owner_table.snapshot(self.object_id)
        assert ready.state is ObjectState.READY_STORED
        assert ready.current_attempt == attempt
        assert ready.canonical_stored_result == descriptor
        assert ready.locations == frozenset({self.node.node_id})
        self.seals.append(seal)
        return descriptor

    def rpc(self, address, handler, request):
        assert address == self.core.node_address
        assert handler == "drop_object_replica" and len(self.calls) < 2
        before = self.core.owner_table.snapshot(self.object_id)
        descriptor = before.canonical_stored_result
        assert request == protocol.DropObjectReplica(
            self.object_id, before.current_attempt, self.core.worker_id,
            self.node.node_id, descriptor.checksum,
        )
        reply = self.node._handle_drop_object_replica(request)
        assert reply == protocol.DropObjectReplicaReply(
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum, protocol.DropObjectReplicaStatus.DROPPED,
        )
        assert not self.store.contains(self.object_id, sealed_only=False)
        # The physical ACK precedes the caller's owner-location CAS.
        assert self.core.owner_table.snapshot(self.object_id) == before
        self.calls.append((request, reply))
        return reply

    def drop_once(self):
        assert self.store.get(self.object_id) == self.seals[0].data
        first = self.core.request_drop_owned_object(self.request)
        assert first == protocol.RequestDropOwnedObjectReply(
            self.request.operation_id, self.object_id, self.core.worker_id,
            self.borrower, self.source, self.token, self.attempt, None,
            protocol.DropOwnedObjectDisposition.DROPPED, self.node.node_id,
        )
        lost = self.core.owner_table.snapshot(self.object_id)
        assert lost.state is ObjectState.LOST and not lost.locations
        assert lost.current_attempt == self.attempt
        assert lost.canonical_stored_result == self.core._stored_descriptors[self.object_id]
        assert len(self.calls) == len(self.deletions) == 1
        assert len(self.node._replica_drop_receipts) == 1
        assert self.store.used_bytes == 0 and not self.node._sealed_metadata
        assert self.core._owned_drop_claims == {self.request.operation_id: self.request}
        return first

    def assert_exact_replay(self, first):
        before = self.core.owner_table.snapshot(self.object_id)
        byte_count = self.store.used_bytes
        assert self.core.request_drop_owned_object(replace(self.request)) is first
        assert self.core.owner_table.snapshot(self.object_id) == before
        assert self.store.used_bytes == byte_count
        assert len(self.calls) == len(self.deletions) == 1
        assert len(self.node._replica_drop_receipts) == 1
        assert self.core._owned_drop_replies == {self.request.operation_id: first}

    def close(self):
        # Keep the local hold until both real remote-lifetime releases finish.
        released = self.core.release_borrowed_reference(self.release)
        assert released.accepted
        pin = protocol.ReleaseContainedReference(
            self.object_id, self.core.worker_id, self.source.hold
        )
        releases = []

        def release_rpc(address, handler, request):
            assert address == self.core.owner_address
            assert handler == "release_contained_reference" and request == pin
            assert not releases
            reply = self.core.release_contained_reference(request)
            assert reply.accepted and reply.released
            releases.append(reply)
            return reply

        self.core._borrow_rpc = release_rpc
        assert self.core.owner_table.release_local_reference(
            self.source_container, "source-container-live",
        )
        self.core._reference_released(self.source_container)
        assert len(releases) == 1
        assert self.core.owner_table.collection_state(self.source_container) is ObjectCollectionState.COLLECTED
        assert self.core.owner_table.release_local_reference(
            self.object_id, self.local_token
        )
        self.core._reference_released(self.object_id)
        assert self.core.owner_table.collection_state(self.object_id) is ObjectCollectionState.COLLECTED
        assert self.store.used_bytes == 0 and not self.node._sealed_metadata
        assert not self.core._stored_descriptors and not self.core._object_gc_obligations
        assert not self.core._objects and self.core._accepted_task_count == 0
        assert self.core._reference_mailbox.pending.empty()
        assert self.core._reference_mailbox.pending.unfinished_tasks == 0
        assert self.core._submissions.empty()
        assert self.core._submissions.unfinished_tasks == 0
        close_pure_core(self.core)


@pytest.fixture
def boundary(monkeypatch):
    _forbid_runtime(monkeypatch)
    fixture = _DropBoundary(make_pure_core(), monkeypatch)
    try:
        yield fixture
    finally:
        fixture.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "field",
    ("node_id", "requester_worker_id", "borrower_token", "source",
     "expected_owner_attempt", "owner_worker_id"),
)
def test_terminal_drop_reply_rejects_changed_complete_request(boundary, field):
    first = boundary.drop_once()
    boundary.assert_exact_replay(first)
    # Even resolving None to the previously selected Node is a changed claim.
    replacements = {
        "node_id": boundary.node.node_id,
        "requester_worker_id": WorkerID.random(),
        "borrower_token": "another-borrower-token",
        "source": protocol.ContainedTransferSource(replace(
            boundary.source.hold, transfer_token="another-source",
        )),
        "expected_owner_attempt": boundary.attempt.next(),
        "owner_worker_id": WorkerID.random(),
    }
    changed = replace(boundary.request, **{field: replacements[field]})
    before = boundary.core.owner_table.snapshot(boundary.object_id)
    conflict = boundary.core.request_drop_owned_object(changed)
    assert conflict.disposition is protocol.DropOwnedObjectDisposition.FAILED
    assert conflict.failure is protocol.DropOwnedObjectFailure.REQUEST_CONFLICT
    assert getattr(conflict, "requested_node_id" if field == "node_id" else field) == replacements[field]
    assert conflict.operation_id == boundary.request.operation_id
    assert boundary.core.owner_table.snapshot(boundary.object_id) == before
    boundary.assert_exact_replay(first)


@pytest.mark.unit
def test_terminal_drop_exact_replay_survives_released_borrower(boundary):
    first = boundary.drop_once()
    released = boundary.core.release_borrowed_reference(boundary.release)
    assert released.accepted and released.released
    snapshot = boundary.core.owner_table.snapshot(boundary.object_id)
    capability = (boundary.borrower, boundary.token)
    assert capability not in snapshot.borrowed_tokens
    assert capability in snapshot.released_borrowed_tokens
    boundary.assert_exact_replay(first)
    # A fresh operation still requires a live capability; replay is narrower.
    fresh = replace(boundary.request, operation_id="after-release")
    rejected = boundary.core.request_drop_owned_object(fresh)
    assert rejected.failure is protocol.DropOwnedObjectFailure.RELEASED_CREDENTIAL
    boundary.assert_exact_replay(first)


@pytest.mark.unit
def test_terminal_drop_exact_replay_preserves_new_owner_epoch_bytes(boundary):
    first = boundary.drop_once()
    successor = boundary.attempt.next()
    assert boundary.core.owner_table.advance_attempt(
        boundary.object_id, expected_attempt=boundary.attempt, next_attempt=successor
    )
    assert boundary.core.owner_table.snapshot(boundary.object_id).state is ObjectState.PENDING
    payload = b"successor-result"
    descriptor = boundary.publish(successor, payload)
    boundary.assert_exact_replay(first)
    assert boundary.store.get(boundary.object_id) == payload
    assert boundary.core.owner_table.snapshot(boundary.object_id).canonical_stored_result == descriptor
    # An uncached old attempt is fenced, while the exact old reply stays valid.
    fresh = replace(boundary.request, operation_id="after-advance")
    rejected = boundary.core.request_drop_owned_object(fresh)
    assert rejected.failure is protocol.DropOwnedObjectFailure.EXPECTED_ATTEMPT_MISMATCH
    boundary.assert_exact_replay(first)
