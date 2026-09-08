"""Pure foreign wait/drop contracts with actual ownership and small storage.

Per case: two threadless Cores, at most three canonical Tasks or one real put,
one 4-KiB ObjectStore, at most one successful Task publication, and <=128-byte
stored payloads. Borrowing uses actual Acquire/Release identities. wait(timeout=0)
performs its original metadata scan without a real wait or fetching bytes.
Owner-drop and Worker rejection use real owner/Node handlers; public-drop keeps
its original typed reply double to prove the caller never updates owner state.
That caller-only case does not prove foreground physical deletion.

Cleanup runs actual releases and GC for terminal objects. The ordering case's
PENDING Task remains admitted and uncollected, with its lineage intact: no fake
completion, thread shutdown, live GCS, network, timer or user function is used.
"""

from __future__ import annotations

import hashlib
import queue
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker, ObjectRef, _PendingTask, _ReleaseBorrowedReference,
    _RetryInlineGc, _WAKE_COORDINATOR,
)
from miniray.errors import BorrowedObjectUnavailableError
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import FailureKind, TaskState
from miniray.resources import ResourceVector
from miniray.worker import WorkerServer
from tests.unit._pure_core import (
    SynchronousReferenceMailbox, close_pure_core, make_pure_core,
)
from tests.unit.test_task_finish_barrier import _OutputBackend


def _no_runtime(monkeypatch):
    import multiprocessing.process
    import socket
    import subprocess
    import time

    from miniray import control, core as core_module, node, transport, worker

    failed, receipts = [False], [0]

    def forbidden(*_args, **_kwargs):
        failed[0] = True
        pytest.fail("pure foreign wait/drop attempted runtime or value fetch")

    def already_set(event, timeout=None):
        if receipts[0] >= 32 or not event.is_set():
            forbidden()
        receipts[0] += 1
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (CoreWorker, "_execute"), (WorkerServer, "__init__"),
        (WorkerServer, "_embedded_core_for"), (node.NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "__init__"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    monkeypatch.setattr(core_module._BORROW_POLL_EVENT, "wait", forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node, control, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    return failed, forbidden


def _drain_wakes(core):
    for _ in range(32):
        try:
            item = core._submissions.get_nowait()
        except queue.Empty:
            assert core._submissions.unfinished_tasks == 0
            return
        try:
            assert item is _WAKE_COORDINATOR
        finally:
            core._submissions.task_done()
    pytest.fail("foreign wait/drop coordinator FIFO exceeded 32 messages")


def _drain_gc(core):
    fifo = core._reference_mailbox.pending
    for _ in range(32):
        try:
            event = fifo.get_nowait()
        except queue.Empty:
            assert fifo.unfinished_tasks == 0
            return
        try:
            assert type(event) is _RetryInlineGc
            core._reference_released(event.object_id)
        finally:
            fifo.task_done()
    pytest.fail("foreign wait/drop reference FIFO exceeded 32 messages")


class _BorrowMailbox(SynchronousReferenceMailbox):
    """Use the actual release driver/ACK; only event delivery is synchronous."""

    def __init__(self, core, failed):
        super().__init__(core)
        self.failed, self.borrowed_events = failed, []

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        try:
            assert len(self.borrowed_events) < 3
            assert event.done is not None and event.scheduled_round is None
            assert event.key not in {previous.key for previous in self.borrowed_events}
            self.borrowed_events.append(event)
            core = self.core_reference()
            assert core is not None
            assert core._drive_borrowed_reference_release(event.key)
            assert event.key not in core._borrowed_release_obligations
        except BaseException:
            self.failed[0] = True
            raise
        finally:
            if event.done is not None:
                event.done.set()
        return True


class _WaitDrop:
    """Bounded protocol composition, not a live Node or Worker execution."""

    def __init__(self, monkeypatch):
        self.failed, self.forbidden = _no_runtime(monkeypatch)
        self.cores, self.refs, self.entries, self.pins = [], [], [], []
        self.borrowings = []
        self.calls, self.drops, self.release_replies = [], [], []
        self.payload = cloudpickle.dumps(b"stored")
        assert len(self.payload) <= 128

    def start(self):
        for _ in range(2):
            core = make_pure_core()
            self.cores.append(core)
            for name in (
                "_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline",
                "_push_task_rpc", "_actor_call_rpc", "_initialize_reference_events",
                "_schedule_reference_event", "_loads_owned_value",
                "_fetch_borrowed_stored_object",
            ):
                setattr(core, name, self.forbidden)
        self.owner, self.borrower = self.cores
        self.owner.node_address = ("127.0.0.1", 29201)
        self.owner.owner_address = ("127.0.0.1", 29202)
        self.owner.gcs_address = ("wait-drop-control.invalid", 1)
        self.backend = _OutputBackend(self.owner, self.forbidden)
        self.owner._rpc = self.node_rpc
        self.borrower._reference_mailbox = _BorrowMailbox(self.borrower, self.failed)
        self.borrower._borrow_rpc = self.owner_rpc
        self.borrower._borrow_rpc_with_deadline = self.owner_rpc

    def record(self, handler, request):
        if len(self.calls) >= 32:
            self.forbidden()
        self.calls.append((handler, request))

    def node_rpc(self, address, handler, request):
        try:
            self.record(handler, request)
            if handler == "seal_object":
                assert address == self.owner.node_address
                assert type(request) is protocol.SealObject and request.data == self.payload
                assert self.owner._inflight_puts == 1
                return self.backend.node._handle_seal_object(request)
            reply = self.backend.rpc(address, handler, request)
            if handler == "drop_object_replica":
                assert len(self.drops) < 3 and type(request) is protocol.DropObjectReplica
                self.drops.append((request, reply))
            return reply
        except BaseException:
            self.failed[0] = True
            raise

    def owner_rpc(self, address, handler, request, timeout=None):
        try:
            assert address == self.owner.owner_address
            assert request.owner_worker_id == self.owner.worker_id
            self.record(handler, request)
            if timeout is not None:
                # Public wait(timeout=0) grants its initial metadata scan this
                # fixed RPC slice; it never enters the outer polling wait.
                assert timeout == 0.001
            if handler == "acquire_borrowed_object":
                assert type(request) is protocol.AcquireBorrowedObject
                return self.owner.acquire_exported_reference(request)
            if handler == "get_owned_object":
                assert type(request) is protocol.GetOwnedObject
                return self.owner.get_owned_object(request)
            if handler == "request_owned_object_reconstruction":
                assert type(request) is protocol.RequestOwnedObjectReconstruction
                return self.owner.request_owned_object_reconstruction(request)
            assert handler == "release_borrowed_object"
            assert type(request) is protocol.ReleaseBorrowedObject
            assert len(self.release_replies) < 3
            reply = self.owner.release_borrowed_reference(request)
            assert reply.accepted
            self.release_replies.append(reply)
            return reply
        except BaseException:
            self.failed[0] = True
            raise

    def foreign(self, *, index, state):
        assert len(self.entries) < 3
        pending, local = self.owner._register_submission(
            self.owner.define_remote_function(self.forbidden), (), {},
            ResourceVector(), max_retries=1, _enqueue=True,
        )
        self.refs.append(local)
        assert self.owner._submissions.get_nowait() is pending
        self.owner._submissions.task_done()
        assert type(pending) is _PendingTask
        if state is protocol.OwnedObjectState.READY_STORED:
            assert not self.backend.completed
            results = self.backend.succeed(pending, stored=True, value=b"stored")
            assert len(results) == 1 and results[0].size_bytes == len(self.payload)
            assert self.owner._finish_pending_task(pending)
        elif state is protocol.OwnedObjectState.ERROR:
            assert self.owner._publish_task_error(
                pending, RuntimeError("bad"), failure_kind=FailureKind.APPLICATION
            )
            assert self.owner._finish_pending_task(pending)
        else:
            assert state is protocol.OwnedObjectState.PENDING
        _drain_wakes(self.owner)
        _drain_gc(self.owner)
        entry = self.borrow(local, index=index, pending=pending)
        assert self.owner.owner_table.snapshot(local.object_id).state.value == state.value
        return entry

    def stored_put(self, *, index):
        assert not self.entries
        self.owner.inline_threshold = 0
        local = self.owner.put(b"stored")
        self.refs.append(local)
        entry = self.borrow(local, index=index, pending=None)
        snapshot = self.owner.owner_table.snapshot(local.object_id)
        assert snapshot.state is ObjectState.READY_STORED and snapshot.producer_task_spec is None
        assert snapshot.output_publication is None
        assert self.owner._recovery.reconstruction_snapshot(local.object_id).is_put
        assert self.owner._accepted_task_count == 0 and self.owner._submissions.empty()
        return entry

    def borrow(self, local, *, index, pending):
        assert len(self.borrowings) < 3 and len(self.pins) < 3
        source = protocol.ContainedTransferSource("transfer-{}".format(index))
        token = "borrow-{}".format(index)
        pin = protocol.ReleaseContainedReference(local.object_id, self.owner.worker_id, source.hold)
        assert self.owner.owner_table.add_contained_reference(local.object_id, source.hold)
        self.pins.append([pin, False])
        acquire = protocol.AcquireBorrowedObject(
            local.object_id, self.owner.worker_id, self.borrower.worker_id, source, token
        )
        release = protocol.ReleaseBorrowedObject(
            local.object_id, self.owner.worker_id, self.borrower.worker_id, token
        )
        key, obligation, created = self.borrower._register_borrowed_release_obligation(
            self.owner.owner_address, acquire, release
        )
        # Record the slot before Acquire can have an effect. Its entry is
        # upgraded once a bound Python ref owns the release, without forgetting
        # how to compensate a failure between acquisition and handle binding.
        borrowing = [key, release, None]
        self.borrowings.append(borrowing)
        assert created and obligation.acquire == acquire
        reply = self.owner_rpc(self.owner.owner_address, "acquire_borrowed_object", acquire)
        assert reply.accepted and reply.acquired and reply.source == source
        ref = ObjectRef(local.object_id, self.owner.worker_id, self.owner.owner_address)
        self.refs.append(ref)
        ref._bind_borrowed_reference(self.borrower, token, source)
        borrowing[2] = ref
        assert self.borrower._active_borrower_capability(ref) == acquire
        entry = SimpleNamespace(
            local=local, ref=ref, source=source, pending=pending, release=release, key=key,
            attempt=self.owner.owner_table.snapshot(local.object_id).current_attempt,
        )
        self.entries.append(entry)
        return entry

    def release_all(self):
        # Each setup object joins this ledger immediately. Cleanup attempts all
        # exact releases even if an earlier callback fails; it never completes a
        # PENDING Task, discards a lineage record or overwrites an owner table.
        errors = []
        for ref in reversed(self.refs):
            try:
                if ref._finalizer is not None and not ref.closed:
                    ref.close(timeout=0)
                if ref._release_done is not None:
                    assert ref._release_done.is_set()
            except BaseException as exc:
                errors.append(exc)
        for key, release, ref in self.borrowings:
            if key not in self.borrower._borrowed_release_obligations:
                continue
            if ref is not None or key in {
                event.key for event in self.borrower._reference_mailbox.borrowed_events
            }:
                # A failed actual delivery stays visible; do not invent its
                # receipt or keep retrying it in fixture teardown.
                continue
            try:
                # Registration precedes Acquire. If setup failed after owner
                # acquisition but before a Python handle was bound, issue the
                # same real release intent rather than leak that credential.
                done = threading.Event()
                assert self.borrower._enqueue_borrowed_reference_release(
                    release.object_id, release.owner_worker_id, self.owner.owner_address,
                    release.borrower_token, done,
                )
                assert done.is_set() and key not in self.borrower._borrowed_release_obligations
            except BaseException as exc:
                errors.append(exc)
        for item in self.pins:
            if item[1]:
                continue
            try:
                reply = self.owner.release_contained_reference(item[0])
                assert reply.accepted and reply.released
                item[1] = True
            except BaseException as exc:
                errors.append(exc)
        for core in self.cores:
            try:
                _drain_gc(core)
            except BaseException as exc:
                errors.append(exc)
        assert len(errors) <= 16
        if errors:
            raise errors[0]

    def assert_collected(self, *, pending=None):
        self.release_all()
        assert len(self.borrower._reference_mailbox.borrowed_events) == len(self.entries)
        assert len(self.release_replies) == len(self.entries)
        assert not self.borrower._borrowed_release_obligations
        for entry in self.entries:
            object_id = entry.ref.object_id
            if entry is pending:
                snapshot = self.owner.owner_table.snapshot(object_id)
                assert snapshot.state is ObjectState.PENDING
                assert not snapshot.local_tokens and not snapshot.borrowed_tokens and not snapshot.contained_holds
                assert self.owner._recovery.lineage_for_object(object_id).task_spec == entry.pending.spec
                assert self.owner._task_finish_barriers[object_id] is entry.pending
                assert self.owner._recovery.task_record(entry.pending.task_id).state is TaskState.PENDING
            else:
                assert self.owner.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
                assert self.owner._recovery.lineage_for_object(object_id) is None
            assert not self.borrower.owner_table.contains(object_id)
        assert self.owner._accepted_task_count == (1 if pending else 0)
        assert set(self.owner._objects) == ({pending.ref.object_id} if pending else set())
        assert not self.owner._stored_descriptors and not self.owner._object_gc_obligations
        assert not self.owner._protocol_unresolved
        assert not self.borrower._objects and self.borrower._submissions.empty()
        assert self.backend.store.capacity_bytes == 4096 and self.backend.store.used_bytes == 0
        assert not self.backend.node._sealed_metadata
        for identity in self.backend.completed:
            snapshot = self.backend.recovery.snapshot(identity)
            assert len(snapshot.slot_collections) == 1
            assert not self.backend.journal.snapshot(identity).retained_result_slots
        assert not self.backend.adapter.pending_terminal_reports()
        for core in self.cores:
            assert core._reference_mailbox.pending.empty()
            assert core._reference_mailbox.pending.unfinished_tasks == 0
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0

    def close(self):
        try:
            self.release_all()
        finally:
            for core in self.cores:
                close_pure_core(core)
            assert self.failed == [False]


@contextmanager
def _fixture(monkeypatch):
    fixture = _WaitDrop(monkeypatch)
    try:
        fixture.start()
        yield fixture
    finally:
        fixture.close()


@pytest.mark.unit
def test_foreign_wait_is_metadata_only_and_preserves_input_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _fixture(monkeypatch) as fixture:
        pending = fixture.foreign(index=0, state=protocol.OwnedObjectState.PENDING)
        stored = fixture.foreign(index=1, state=protocol.OwnedObjectState.READY_STORED)
        error = fixture.foreign(index=2, state=protocol.OwnedObjectState.ERROR)
        owner, borrower = fixture.owner, fixture.borrower
        before = tuple(owner.owner_table.snapshot(entry.ref.object_id)
                       for entry in (pending, stored, error))
        call_start = len(fixture.calls)
        assert fixture.backend.store.used_bytes == len(fixture.payload)
        with monkeypatch.context() as patch:
            # Metadata wait may not materialize even locally available bytes.
            patch.setattr(fixture.backend.store, "get", fixture.forbidden)
            ready, remaining = borrower.wait(
                [error.ref, pending.ref, stored.ref], num_returns=2, timeout=0
            )
        assert ready == [error.ref, stored.ref]
        assert remaining == [pending.ref]
        foreground = fixture.calls[call_start:]
        assert [handler for handler, _request in foreground] == ["get_owned_object"] * 3
        assert [request.object_id for _handler, request in foreground] == [
            error.ref.object_id, pending.ref.object_id, stored.ref.object_id,
        ]
        assert tuple(owner.owner_table.snapshot(entry.ref.object_id)
                     for entry in (pending, stored, error)) == before
        assert fixture.drops == [] and borrower._accepted_task_count == 0
        fixture.assert_collected(pending=pending)


@pytest.mark.unit
def test_foreign_wait_terminal_lost_is_ready_without_fetching_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _fixture(monkeypatch) as fixture:
        entry = fixture.stored_put(index=3)
        owner, borrower, ref = fixture.owner, fixture.borrower, entry.ref
        assert owner.drop_object(entry.local)
        before = owner.owner_table.snapshot(ref.object_id)
        assert before.state is ObjectState.LOST and not before.locations
        assert before.current_attempt == entry.attempt
        assert fixture.backend.store.used_bytes == 0
        call_start = len(fixture.calls)
        replies = []

        def metadata_rpc(address, handler, request, timeout):
            try:
                assert len(replies) < 2
                reply = fixture.owner_rpc(address, handler, request, timeout)
                replies.append(reply)
                return reply
            except BaseException:
                fixture.failed[0] = True
                raise

        with monkeypatch.context() as patch:
            patch.setattr(borrower, "_borrow_rpc_with_deadline", metadata_rpc)
            patch.setattr(fixture.backend.store, "get", fixture.forbidden)
            assert borrower.wait([ref], timeout=0) == ([ref], [])
        assert [handler for handler, _request in fixture.calls[call_start:]] == [
            "get_owned_object", "request_owned_object_reconstruction",
        ]
        assert replies[0].state is protocol.OwnedObjectState.LOST
        assert replies[1].disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
        assert replies[1].failure is protocol.OwnedObjectReconstructionFailure.PUT_OBJECT
        assert replies[1].credential == protocol.BorrowedCredential(entry.source, ref.borrower_token)
        assert replies[1].expected_owner_attempt == entry.attempt
        assert owner.owner_table.snapshot(ref.object_id) == before
        assert owner._submissions.empty() and borrower._submissions.empty()
        assert not fixture.backend.completed
        fixture.assert_collected()


@pytest.mark.unit
def test_owner_drop_validates_capability_updates_only_owner_and_exact_replays() -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        with _fixture(monkeypatch) as fixture:
            entry = fixture.foreign(index=4, state=protocol.OwnedObjectState.READY_STORED)
            owner, borrower, ref = fixture.owner, fixture.borrower, entry.ref
            checksum = hashlib.sha256(fixture.payload).hexdigest()
            assert fixture.backend.store.get(ref.object_id) == fixture.payload
            request = protocol.RequestDropOwnedObject(
                "operation", ref.object_id, owner.worker_id, borrower.worker_id,
                entry.source, ref.borrower_token, entry.attempt, None,
            )
            first = owner.request_drop_owned_object(request)
            replay = owner.request_drop_owned_object(request)
            assert replay is first
            assert first.disposition is protocol.DropOwnedObjectDisposition.DROPPED
            assert [drop for drop, _reply in fixture.drops] == [protocol.DropObjectReplica(
                ref.object_id, entry.attempt, owner.worker_id, owner.node_id, checksum
            )]
            assert fixture.drops[0][1].status is protocol.DropObjectReplicaStatus.DROPPED
            assert owner.owner_table.snapshot(ref.object_id).state is ObjectState.LOST
            assert borrower.owner_table.contains(ref.object_id) is False
            assert not owner.owner_table.snapshot(ref.object_id).locations
            assert not fixture.backend.store.contains(ref.object_id, sealed_only=False)
            assert fixture.backend.store.used_bytes == 0
            assert not borrower._objects and borrower._accepted_task_count == 0
            fixture.assert_collected()


@pytest.mark.unit
def test_public_foreign_drop_routes_owner_handler_and_returns_node_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _fixture(monkeypatch) as fixture:
        entry = fixture.foreign(index=5, state=protocol.OwnedObjectState.READY_STORED)
        owner, borrower, ref = fixture.owner, fixture.borrower, entry.ref
        before = owner.owner_table.snapshot(ref.object_id)
        seen = []

        def rpc(address, handler, request):
            try:
                assert address == owner.owner_address and len(seen) < 2
                fixture.record(handler, request)
                seen.append(handler)
                if handler == "get_owned_object":
                    return owner.get_owned_object(request)
                assert handler == "request_drop_owned_object"
                assert request.source == entry.source and request.borrower_token == ref.borrower_token
                assert request.expected_owner_attempt == entry.attempt and request.node_id is None
                # Preserve the original caller-only contract: this typed reply
                # is a delivery double, not an owner mutation or Node receipt.
                return protocol.RequestDropOwnedObjectReply(
                    request.operation_id, request.object_id, request.owner_worker_id,
                    request.requester_worker_id, request.source, request.borrower_token,
                    request.expected_owner_attempt, request.node_id,
                    protocol.DropOwnedObjectDisposition.DROPPED, dropped_node_id=owner.node_id,
                )
            except BaseException:
                fixture.failed[0] = True
                raise

        with monkeypatch.context() as patch:
            patch.setattr(borrower, "_borrow_rpc", rpc)
            assert borrower.drop_object(ref)
        assert seen == ["get_owned_object", "request_drop_owned_object"]
        assert owner.owner_table.snapshot(ref.object_id).state is ObjectState.READY_STORED
        assert owner.owner_table.snapshot(ref.object_id) == before
        assert fixture.backend.store.get(ref.object_id) == fixture.payload
        assert fixture.drops == [] and not borrower._objects
        # Physical GC is only teardown evidence, after the caller-isolation
        # assertions and after restoring the real release route.
        fixture.assert_collected()


@pytest.mark.unit
def test_worker_drop_proxy_preserves_typed_credential_rejection() -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        with _fixture(monkeypatch) as fixture:
            entry = fixture.foreign(index=6, state=protocol.OwnedObjectState.READY_STORED)
            owner, borrower, ref = fixture.owner, fixture.borrower, entry.ref
            released = owner.release_borrowed_reference(entry.release)
            assert released.accepted and released.released
            request = protocol.RequestDropOwnedObject(
                "released", ref.object_id, owner.worker_id, borrower.worker_id,
                entry.source, ref.borrower_token, entry.attempt, owner.node_id,
            )
            worker = object.__new__(WorkerServer)
            worker.worker_id = owner.worker_id
            worker._embedded_core = owner
            worker._embedded_core_lock = threading.Lock()
            before = owner.owner_table.snapshot(ref.object_id)
            reply = worker._handle_request_drop_owned_object(request)
            assert reply.failure is protocol.DropOwnedObjectFailure.RELEASED_CREDENTIAL
            assert reply.disposition is protocol.DropOwnedObjectDisposition.FAILED
            assert reply.source == entry.source and reply.borrower_token == ref.borrower_token
            call_start = len(fixture.calls)
            with pytest.raises(BorrowedObjectUnavailableError):
                borrower._drop_borrowed_object(ref, None)
            assert [handler for handler, _request in fixture.calls[call_start:]] == [
                "get_owned_object",
            ]
            assert owner.owner_table.snapshot(ref.object_id) == before
            assert fixture.backend.store.get(ref.object_id) == fixture.payload
            assert fixture.drops == []
            assert worker._borrow_owner_core() is owner and not hasattr(worker, "_server")
            fixture.assert_collected()
            assert len(fixture.release_replies) == 1
            assert fixture.release_replies[0].accepted and not fixture.release_replies[0].released
