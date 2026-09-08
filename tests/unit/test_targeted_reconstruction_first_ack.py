"""Pure first-ACK interleavings for a real two-slot targeted reconstruction.

One canonical Task publishes two STORED values through real discovery, the
publication adapter/journal, ObjectStore and Core adoption/finish. Actual
debug drop loses only slot 1. Slot 0 retains its original bytes and epoch.
The borrower acquires a real capability from an explicit legacy export pin;
no output-reference serialization or remote execution is claimed.

Two success-path tests finish the admitted target before the first ACK: one
after Core constructs its START outcome, one before Core can construct that
outcome. Both use the real selected publication/error and finish reducers.
Negative controls return an uncommitted preview or request a newly lost slot
while the first target is running. They retain genuine OPEN/STARTED work at
teardown instead of fabricating a terminal result to obtain clean metadata.

Per case: two threadless Cores, one Task, two output slots, at most two
Attempts/publications, two borrower capabilities, one 1-KiB store and <=128
bytes per serialized slot. Each FIFO/callback observation channel is capped
at 32. No user function, physical Worker, StartLease RPC, live GCS, runtime
constructor, thread, process, socket, timer or real blocking wait executes.
Happy-path collection uses actual Release/GC callbacks. Failure teardown
only releases handles and fences the fixtures; it never clears authority.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import control, core as core_module, node as node_module, protocol, transport, worker
from miniray.core import (
    CoreWorker, ObjectRef, _PendingTask, _ReleaseBorrowedReference,
    _RetryInlineGc, _StartTargetedReconstruction, _WAKE_COORDINATOR,
)
from miniray.errors import TaskError
from miniray.ids import LeaseID, WorkerID
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import FailureKind, TaskState, UnknownTaskError
from miniray.resources import ResourceVector
from miniray.targeted_reconstruction import TargetedSessionPhase
from tests.unit._pure_core import SynchronousReferenceMailbox, close_pure_core, make_pure_core
from tests.unit._pure_reference_output_runtime import PureReferenceOutputRuntime


pytestmark = pytest.mark.unit
_LIMIT = 32


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations, receipts = [], []

    def forbidden(*args, **kwargs):
        if len(violations) < _LIMIT:
            violations.append((args, kwargs))
        pytest.fail("pure targeted first-ACK test attempted unmodelled runtime work")

    def already_set(event, timeout=None):
        if len(receipts) >= _LIMIT or not event.is_set():
            forbidden("unresolved or excessive Event receipt", timeout)
        receipts.append((event, timeout))
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (CoreWorker, "shutdown"), (NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (worker.WorkerServer, "__init__"),
        (transport.TCPServer, "__init__"), (threading.Thread, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "__init__"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "__init__"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"), (queue.Queue, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    monkeypatch.setattr(core_module._BORROW_POLL_EVENT, "wait", forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, control, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield forbidden
    assert not violations, violations


class _BorrowMailbox(SynchronousReferenceMailbox):
    def __init__(self, core, fixture):
        super().__init__(core)
        self.fixture = fixture
        self.borrowed_releases = []

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        if (len(self.borrowed_releases) >= 2 or event.done is None
                or event.scheduled_round is not None
                or event.key in self.borrowed_releases):
            self.fixture.forbidden("unexpected borrowed release event")
        self.borrowed_releases.append(event.key)
        core = self.core_reference()
        assert core is not None
        try:
            if not core._drive_borrowed_reference_release(event.key):
                self.fixture.forbidden("borrowed release did not converge")
        except BaseException:
            self.fixture.callback_failed = True
            raise
        finally:
            event.done.set()
        return True


class _Fixture:
    def __init__(self, forbidden):
        self.forbidden = forbidden
        self.callback_failed = False
        self.owner, self.borrower = make_pure_core(), make_pure_core()
        self.output = PureReferenceOutputRuntime(self.owner)
        self.locals, self.capabilities, self.publications = (), [], []
        self.owner_calls, self.queue_events, self.gc_events = [], [], []
        self.start_wakes = []
        self.original = self.retry = None
        # A saved real method consumes obsolete OPEN-closure events even when
        # the test wraps the synchronous first-ACK closure boundary.
        self.start_open = self.owner._start_open_targeted_reconstruction
        self.owner._rpc = self.checked(self.output_rpc)
        self.owner._resolve_node_address = self.checked(self.node_address)
        self.borrower._reference_mailbox = _BorrowMailbox(self.borrower, self)
        self.borrower._borrow_rpc = self.checked(self.owner_rpc)
        self.borrower._borrow_rpc_with_deadline = self.checked(self.owner_rpc)

    def checked(self, callback):
        def invoke(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except BaseException:
                self.callback_failed = True
                raise
        return invoke

    def node_address(self, node_id):
        assert node_id == self.owner.node_id
        return self.output.node_address

    def output_rpc(self, address, handler, request):
        if len(self.output.calls) >= _LIMIT:
            self.forbidden("publication callback budget exhausted", handler)
        assert not self.owner._state_lock._is_owned()
        return self.output.rpc(address, handler, request)

    def owner_rpc(self, address, handler, request, remaining=None):
        if len(self.owner_calls) >= _LIMIT or address != self.owner.owner_address:
            self.forbidden("unexpected owner callback route", address, handler)
        if handler == "get_owned_object":
            assert type(request) is protocol.GetOwnedObject
            assert remaining is not None and 0 < remaining <= 1.0
            route = self.owner.get_owned_object
        elif handler == "release_borrowed_object":
            assert type(request) is protocol.ReleaseBorrowedObject
            route = self.owner.release_borrowed_reference
        else:
            self.forbidden("unexpected borrower callback", handler)
        matches = tuple(
            cap for cap in self.capabilities
            if cap.acquire.object_id == request.object_id
            and cap.acquire.borrower_token == request.borrower_token
        )
        assert len(matches) == 1
        assert request.owner_worker_id == self.owner.worker_id
        assert request.borrower_worker_id == self.borrower.worker_id
        self.owner_calls.append((handler, request))
        return route(request)

    def prepare(self):
        self.original, self.locals = self.owner._register_submission(
            self.owner.define_remote_function(self.forbidden), (), {},
            ResourceVector({"CPU": 1}), num_returns=2, max_retries=1, _enqueue=True,
        )
        assert self.consume() == (self.original,)
        self.publish(self.original, (10, 20), stored=True)
        assert self.owner._finish_pending_task(self.original)
        assert self.consume() == ()
        self.drain_gc()
        assert self.owner._accepted_task_count == 0 and not self.owner._task_finish_barriers
        self.healthy, self.lost = self.original.output_ids
        assert tuple(ref.object_id for ref in self.locals) == self.original.output_ids
        self.healthy_before = self.owner.owner_table.snapshot(self.healthy)
        self.healthy_descriptor = self.owner._stored_descriptors[self.healthy]
        self.healthy_bytes = self.output.store.get(self.healthy)
        assert cloudpickle.loads(self.healthy_bytes) == 10
        self.target_capability = self.borrow(1)
        assert self.owner.drop_object(self.locals[1])
        assert not self.output.store.contains(self.lost, sealed_only=False)
        assert self.output.store.used_bytes == len(self.healthy_bytes)
        assert len(self.output.dropped) == 1 and self.consume() == ()
        snapshot = self.owner.owner_table.snapshot(self.lost)
        assert snapshot.state is ObjectState.LOST and snapshot.current_attempt == self.original.spec.attempt_id
        assert snapshot.output_publication is not None and snapshot.output_retirement_id is None
        assert self.owner._recovery.task_record(self.original.task_id).state is TaskState.SUCCEEDED
        self.assert_healthy()

    def borrow(self, index):
        assert index in (0, 1) and all(cap.index != index for cap in self.capabilities)
        ref = self.locals[index]
        source = protocol.ContainedTransferSource("target-first-ack-export-{}".format(index))
        assert self.owner.owner_table.add_contained_reference(ref.object_id, source.hold)
        acquire = protocol.AcquireBorrowedObject(
            ref.object_id, self.owner.worker_id, self.borrower.worker_id, source,
            "target-first-ack-borrow-{}".format(index),
        )
        release = protocol.ReleaseBorrowedObject(
            ref.object_id, self.owner.worker_id, self.borrower.worker_id, acquire.borrower_token,
        )
        cap = SimpleNamespace(
            index=index, source=source, acquire=acquire, release=release, ref=None,
            export_released=False,
        )
        self.capabilities.append(cap)
        cap.key, cap.obligation, inserted = self.borrower._register_borrowed_release_obligation(
            self.owner.owner_address, acquire, release,
        )
        assert inserted
        acquired = self.owner.acquire_exported_reference(acquire)
        assert acquired.accepted and acquired.acquired and acquired.source == source
        cap.ref = ObjectRef(ref.object_id, self.owner.worker_id, self.owner.owner_address)
        cap.ref._bind_borrowed_reference(self.borrower, acquire.borrower_token, source)
        assert self.borrower._active_borrower_capability(cap.ref) == acquire
        return cap

    def request(self, capability=None):
        cap = self.target_capability if capability is None else capability
        return protocol.RequestOwnedObjectReconstruction(
            cap.ref.object_id, self.owner.worker_id, self.borrower.worker_id,
            protocol.BorrowedCredential(cap.source, cap.ref.borrower_token),
            cap.ref.borrower_token, self.original.spec.attempt_id,
        )

    def publish(self, pending, values, *, stored):
        assert len(self.publications) < 2
        assert len(values) == len(pending.output_ids) <= 2
        assert all(len(cloudpickle.dumps(value)) <= 128 for value in values)
        push = protocol.PushTask(
            LeaseID.random(), WorkerID.random(), pending.spec,
            target_execution=pending.target_execution,
        )
        reply = self.output.complete(push, values, inline_threshold=0 if stored else 1024)
        self.publications.append(reply)
        assert reply.output_publication.manifest.execution == pending.execution
        assert self.owner._publish_reply(
            pending, reply, expected_node_id=self.owner.node_id, expected_lease_id=push.lease_id,
        )
        assert self.owner._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED

    def consume(self):
        fifo = self.owner._submissions
        pendings = []
        for _ in range(_LIMIT):
            try:
                event = fifo.get_nowait()
            except queue.Empty:
                assert fifo.unfinished_tasks == 0
                return tuple(pendings)
            if len(self.queue_events) >= _LIMIT:
                self.forbidden("coordinator event budget exhausted")
            self.queue_events.append(event)
            try:
                if event is _WAKE_COORDINATOR:
                    continue
                if type(event) is _StartTargetedReconstruction:
                    assert self.original is not None and not self.start_wakes
                    assert event.task_id == self.original.task_id and event.round == 0
                    self.start_wakes.append(event)
                    before = self.owner._targeted_reconstruction.current_session(event.task_id)
                    assert before is None or before.phase is TargetedSessionPhase.STARTED
                    # This is the production closure, not a discarded event.
                    assert self.start_open(event.task_id, round=event.round) is None
                    assert self.owner._targeted_reconstruction.current_session(event.task_id) is before
                    continue
                assert type(event) is _PendingTask
                pendings.append(event)
                assert len(pendings) == 1
            finally:
                fifo.task_done()
        self.forbidden("coordinator FIFO did not converge")

    def take_target(self):
        assert self.retry is None
        pending, = self.consume()
        self.retry = pending
        assert pending.task_id == self.original.task_id
        assert pending.output_ids == (self.lost,) and pending.full_output_ids == self.original.output_ids
        assert pending.spec.attempt_id == self.original.spec.attempt_id.next()
        assert pending.spec.owner_worker_id == self.owner.worker_id
        session = self.owner._targeted_reconstruction.current_session(pending.task_id)
        assert session is not None and session.phase is TargetedSessionPhase.STARTED
        assert pending.target_execution == session.execution
        assert session.target_output_ids == (self.lost,)
        assert self.owner._task_finish_barriers == {self.lost: pending}
        assert self.owner._accepted_task_count == 1
        assert self.owner._recovery.active_recovery(pending.task_id) == pending.spec.attempt_id
        self.assert_healthy()
        return pending

    def finish_target(self, pending, *, application_error):
        assert pending is self.retry and pending.output_ids == (self.lost,)
        if application_error:
            self.error = TaskError("targeted reconstruction failed before its first ACK")
            assert self.owner._publish_task_error(pending, self.error, failure_kind=FailureKind.APPLICATION)
        else:
            self.publish(pending, (42,), stored=False)
        assert self.owner._finish_pending_task(pending)
        assert self.consume() == ()
        self.drain_gc()
        owner = self.owner.owner_table.snapshot(self.lost)
        recovery = self.owner._recovery.reconstruction_snapshot(self.lost)
        assert owner.current_attempt == recovery.current_attempt == pending.spec.attempt_id
        assert owner.state is (ObjectState.ERROR if application_error else ObjectState.READY_INLINE)
        if application_error:
            assert owner.error is self.error and owner.output_publication is None
        assert recovery.task_state is TaskState.SUCCEEDED and recovery.active_recovery is None
        assert self.owner._recovery.task_record(pending.task_id).retries_started == 1
        assert self.owner._targeted_reconstruction.current_session(pending.task_id) is None
        assert not self.owner._targeted_reconstruction.queued_losses(pending.task_id)
        assert self.owner._accepted_task_count == 0 and not self.owner._task_finish_barriers
        assert not self.owner._protocol_unresolved
        self.assert_healthy()

    def assert_healthy(self):
        assert self.owner.owner_table.snapshot(self.healthy) == self.healthy_before
        assert self.owner._stored_descriptors[self.healthy] == self.healthy_descriptor
        assert self.output.store.get(self.healthy) == self.healthy_bytes

    def assert_started_replay(self, request, reply):
        assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
        assert reply.reconstruction_attempt == self.original.spec.attempt_id.next()
        assert reply.object_id == self.lost and reply.owner_worker_id == self.owner.worker_id
        assert reply.requester_worker_id == self.borrower.worker_id
        assert reply.credential == request.credential and reply.expected_owner_attempt == request.expected_owner_attempt
        before = tuple(self.output.calls), tuple(self.queue_events)
        assert self.owner.request_owned_object_reconstruction(request) is reply
        assert (tuple(self.output.calls), tuple(self.queue_events)) == before
        assert self.owner._submissions.empty()
        assert self.borrower._accepted_task_count == 0 and self.borrower._submissions.empty()
        assert self.borrower._recovery.lineage_for_object(self.lost) is None

    def drain_gc(self):
        for core in (self.owner, self.borrower):
            fifo = core._reference_mailbox.pending
            for _ in range(_LIMIT):
                try:
                    event = fifo.get_nowait()
                except queue.Empty:
                    assert fifo.unfinished_tasks == 0
                    break
                if len(self.gc_events) >= _LIMIT:
                    self.forbidden("reference GC event budget exhausted")
                self.gc_events.append(event)
                try:
                    assert type(event) is _RetryInlineGc
                    core._reference_released(event.object_id)
                finally:
                    fifo.task_done()
            else:
                self.forbidden("reference GC did not converge")

    def release_capabilities(self):
        for cap in self.capabilities:
            if cap.ref is not None and not cap.ref.closed:
                cap.ref.close(timeout=0)
                assert cap.key not in self.borrower._borrowed_release_obligations
            if not cap.export_released:
                reply = self.owner.release_contained_reference(protocol.ReleaseContainedReference(
                    cap.acquire.object_id, self.owner.worker_id, cap.source.hold,
                ))
                assert reply.accepted and reply.released
                cap.export_released = True

    def collect(self):
        assert self.owner._accepted_task_count == 0 and not self.owner._task_finish_barriers
        assert self.owner._targeted_reconstruction.current_session(self.original.task_id) is None
        self.release_capabilities()
        assert not self.borrower._borrowed_release_obligations
        self.locals[1].close(timeout=0)
        self.drain_gc()
        assert self.owner.owner_table.collection_state(self.lost) is ObjectCollectionState.COLLECTED
        assert self.owner._recovery.lineage_for_object(self.healthy) is not None
        assert self.owner.owner_table.collection_state(self.healthy) is ObjectCollectionState.ACTIVE
        self.locals[0].close(timeout=0)
        self.drain_gc()
        assert self.owner.owner_table.collection_state(self.healthy) is ObjectCollectionState.COLLECTED
        assert self.owner._recovery.lineage_for_object(self.healthy) is None
        with pytest.raises(UnknownTaskError):
            self.owner._recovery.task_record(self.original.task_id)
        assert self.output.store.capacity_bytes == 1024
        assert self.output.store.used_bytes == 0 and not self.output.replicas
        assert len(self.output.dropped) == 2
        self.output.assert_collected()
        assert self.consume() == ()
        for core in (self.owner, self.borrower):
            assert not core._objects and not core._stored_descriptors
            assert not core._protocol_unresolved and not core._object_gc_obligations
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
            assert core._reference_mailbox.pending.empty()
            assert core._reference_mailbox.pending.unfinished_tasks == 0

    def close(self):
        try:
            self.release_capabilities()
        finally:
            try:
                for ref in self.locals:
                    if not ref.closed:
                        ref.close(timeout=0)
            finally:
                # Do not consume OPEN/STARTED work or manufacture a completion
                # when a negative control or failed assertion leaves it live.
                for core in (self.borrower, self.owner):
                    assert core._submissions.qsize() <= _LIMIT
                    assert core._reference_mailbox.pending.qsize() <= _LIMIT
                    close_pure_core(core)
                assert not self.callback_failed


@pytest.fixture
def targeted_ack(_no_runtime):
    fixture = _Fixture(_no_runtime)
    try:
        fixture.prepare()
        yield fixture
    finally:
        fixture.close()


def test_targeted_terminal_error_before_first_ack_preserves_started_and_healthy_slot(monkeypatch, targeted_ack):
    f = targeted_ack
    original_admit = f.owner._admit_owned_object_reconstruction
    outcomes = []

    def complete_before_ack(object_id):
        assert not outcomes
        outcome = original_admit(object_id)
        outcomes.append(outcome)
        assert outcome.disposition is ReconstructionDisposition.START
        pending = f.take_target()
        assert outcome.decision.attempt_id == pending.spec.attempt_id
        f.finish_target(pending, application_error=True)
        return outcome

    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", f.checked(complete_before_ack))
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    f.assert_started_replay(request, reply)
    assert len(outcomes) == 1 and len(f.publications) == 1
    with pytest.raises(TaskError, match="targeted reconstruction failed before its first ACK") as caught:
        f.borrower.get(f.target_capability.ref, timeout=1.0)
    assert caught.value.remote_type == "TaskError"
    f.assert_healthy()
    f.collect()


def test_targeted_success_before_outcome_construction_keeps_real_started_receipt(monkeypatch, targeted_ack):
    f = targeted_ack
    started_receipts = []

    def complete_before_return(task_id, *, round=0):
        assert not started_receipts
        started = f.start_open(task_id, round=round)
        assert started is not None and started.phase is TargetedSessionPhase.STARTED
        started_receipts.append(started)
        pending = f.take_target()
        assert pending.target_execution == started.execution
        f.finish_target(pending, application_error=False)
        assert f.owner._targeted_reconstruction.current_session(task_id) is None
        return started

    monkeypatch.setattr(f.owner, "_start_open_targeted_reconstruction", f.checked(complete_before_return))
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    f.assert_started_replay(request, reply)
    assert len(started_receipts) == 1 and len(f.publications) == 2
    assert reply.reconstruction_attempt == started_receipts[0].execution.attempt_id
    assert f.borrower.get(f.target_capability.ref, timeout=1.0) == 42
    f.assert_healthy()
    f.collect()


def test_uncommitted_targeted_preview_is_not_a_started_ack(monkeypatch, targeted_ack):
    f = targeted_ack
    owner_before = tuple(f.owner.owner_table.snapshot(value) for value in f.original.output_ids)
    recovery_before = replace(f.owner._recovery.task_record(f.original.task_id))
    previews = []

    def preview_only(task_id, *, round=0):
        assert not previews and task_id == f.original.task_id and round == 0
        with f.owner._state_lock:
            preview = f.owner._targeted_reconstruction.preview_start(task_id)
        assert preview.execution.attempt_id == f.original.spec.attempt_id.next()
        previews.append(preview)
        return preview

    monkeypatch.setattr(f.owner, "_start_open_targeted_reconstruction", f.checked(preview_only))
    reply = f.owner.request_owned_object_reconstruction(f.request())
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
    assert reply.failure in (
        protocol.OwnedObjectReconstructionFailure.NOT_LOST,
        protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
    )
    assert reply.reconstruction_attempt is None and len(previews) == 1
    assert tuple(f.owner.owner_table.snapshot(value) for value in f.original.output_ids) == owner_before
    assert f.owner._recovery.task_record(f.original.task_id) == recovery_before
    session = f.owner._targeted_reconstruction.current_session(f.original.task_id)
    assert session.phase is TargetedSessionPhase.OPEN and session.execution is None
    assert session.target_output_ids == (f.lost,)
    assert f.owner._recovery.active_recovery(f.original.task_id) is None
    assert not f.owner._task_finish_barriers and f.owner._accepted_task_count == 0
    assert not f.owner._owned_reconstruction._replies
    assert tuple(f.owner._submissions.queue) == (_StartTargetedReconstruction(f.original.task_id),)
    f.assert_healthy()
    # The genuine OPEN event is retained. Consuming it now with the real
    # closure would perform a START and invalidate this no-commit observation.


def test_new_lost_slot_is_queued_not_joined_to_another_started_target(targeted_ack):
    f = targeted_ack
    first = f.owner.request_owned_object_reconstruction(f.request())
    assert first.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
    pending = f.take_target()
    session = f.owner._targeted_reconstruction.current_session(f.original.task_id)
    second_capability = f.borrow(0)
    assert f.owner.drop_object(f.locals[0])
    assert f.output.store.used_bytes == 0 and not f.output.replicas
    second_request = f.request(second_capability)
    owner_before = tuple(f.owner.owner_table.snapshot(value) for value in f.original.output_ids)
    recovery_before = replace(f.owner._recovery.task_record(f.original.task_id))
    calls_before = tuple(f.output.calls)
    for _ in range(2):
        reply = f.owner.request_owned_object_reconstruction(second_request)
        assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
        assert reply.failure is protocol.OwnedObjectReconstructionFailure.NOT_LOST
        assert reply.reconstruction_attempt is None and reply.object_id == f.healthy
        assert f.owner._targeted_reconstruction.current_session(f.original.task_id) is session
        queued = f.owner._targeted_reconstruction.queued_losses(f.original.task_id)
        assert tuple((loss.object_id, loss.expected_attempt) for loss in queued) == (
            (f.healthy, f.original.spec.attempt_id),
        )
        assert tuple(f.owner.owner_table.snapshot(value) for value in f.original.output_ids) == owner_before
        assert f.owner._recovery.task_record(f.original.task_id) == recovery_before
        assert tuple(f.output.calls) == calls_before and f.owner._submissions.empty()
        assert f.owner._task_finish_barriers == {f.lost: pending}
        assert f.owner._accepted_task_count == 1
    assert recovery_before.retries_started == 1 and recovery_before.retries_remaining == 0
    assert owner_before[0].output_publication is not None
    assert owner_before[0].output_retirement_id is None
    assert owner_before[1].state is ObjectState.PENDING
    # A running target and a real queued loss remain; no synthetic failure,
    # session deletion or third Attempt is used as fixture cleanup.
