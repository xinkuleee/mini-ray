"""Pure owner-reconstruction ACK races after a real local START.

One canonical Task publishes a stored result through discovery, the Node
publication adapter/journal and Core owner-handoff/adoption, finishes, and
really drops its replica. The real reconstruction admission then queues the next Attempt. A
synchronous callback completes that exact Attempt before returning its START
outcome to the owner reducer. Success uses the same publication path; an
application failure uses Core's actual atomic terminal-error reducer. No user
function, scheduler, physical Worker, StartLease RPC or GCS service is
executed. Publication coordination runs the real synchronous GCS reducer.

The borrower has an actually acquired capability from an explicit typed
container hold. Its outer identity is a pure protocol fixture, not a public
serialized outer result; it never receives or queues producer lineage. A separate probe
observes the Core lock at the pre-admission owner/recovery metadata pair. It
does not simulate concurrent execution by publishing under a re-entrant lock.

Per case: one Task/one slot, at most two Attempts/publications, one 4-KiB
store, <=128 bytes per result, <=32 owner/output observations and <=64 exact
publication exchanges through the real authority. All
transport is exact synchronous routing. Runtime constructors, threads, sockets,
processes, timers and real waits are forbidden. Cleanup releases real handles
and the export capability; it does not finish unresolved Tasks, drain queued
GC or claim a runtime/cluster shutdown.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace

import pytest

from miniray import control, core as core_module, node as node_module, protocol, transport, worker
from miniray.core import CoreWorker, ObjectRef, _PendingTask, _ReleaseBorrowedReference, _WAKE_COORDINATOR
from miniray.contained_edges import ContainedReferenceHold
from miniray.errors import SystemTaskError, TaskError
from miniray.ids import LeaseID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectState, UnknownObjectError
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import FailureKind, RecoveryAction, TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import SynchronousReferenceMailbox, close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


pytestmark = pytest.mark.unit
_LIMIT = 32


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations, receipts = [], []

    def forbidden(*args, **kwargs):
        if len(violations) < _LIMIT:
            violations.append((args, kwargs))
        pytest.fail("pure reconstruction ACK race attempted unmodelled runtime work")

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
    """Drive one real borrowed Release/ACK without a reference consumer."""

    def __init__(self, core, forbidden):
        super().__init__(core)
        self.forbidden, self.borrowed_releases = forbidden, 0

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        if self.borrowed_releases or event.done is None or event.scheduled_round is not None:
            self.forbidden("unexpected borrowed release event")
        self.borrowed_releases += 1
        core = self.core_reference()
        assert core is not None
        try:
            if not core._drive_borrowed_reference_release(event.key):
                self.forbidden("exact borrowed release did not converge")
        finally:
            event.done.set()
        return True


class _OutputBackend(PureOutputRuntime):
    """Bounded single-output reducer composition, never a live Node/lease.

    Store sealing and deletion call actual Node methods. Complete accounting
    is the PureOutputRuntime callback, not a Worker execution or physical lease.
    """

    def __init__(self, core, forbidden):
        super().__init__(core)
        self.forbidden = forbidden
        self.completed = self.completions
        self.executor = WorkerID(bytes.fromhex("ef" * 16))
        self.store = ObjectStore(4096)
        self.node = object.__new__(NodeServer)
        node = self.node
        node.node_id = core.node_id
        node._node_pid = self.incarnation.node_pid
        node._registration_epoch = self.incarnation.registration_epoch
        node._state_lock = threading.RLock()
        node._object_store = self.store
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata = {}
        node._dropped_metadata = {}
        node._local_replica_write_claims = {}
        node._object_localization_locks = {}
        node._owner_death_fences = {}
        node._output_publication_journal = self.journal
        self.adapter._seal_replica = node._seal_output_publication_replica
        self.adapter._drop_replica = node._drop_output_publication_replica

    def address(self, node_id):
        assert node_id == self.core.node_id
        return self.node_address

    def rpc(self, address, handler, request):
        if handler == "drop_object_replica":
            assert address == self.node_address
            self.calls.append((handler, request))
            return self.node._handle_drop_object_replica(request)
        return super().rpc(address, handler, request)

    def succeed(self, pending, *, stored, value):
        assert len(self.completed) < 2 and len(pending.output_ids) == 1
        lease_id = LeaseID((len(self.completed) + 1).to_bytes(16, "big"))
        push = protocol.PushTask(lease_id, self.executor, pending.spec)
        reply = self.complete(push, (value,), inline_threshold=0 if stored else 1024)
        identity = reply.output_publication.publication_id
        assert reply.results[0].size_bytes <= 128
        assert self.adapter.report_terminal(identity)
        assert self.core._publish_reply(
            pending, reply, expected_node_id=self.core.node_id, expected_lease_id=lease_id,
        )
        assert not self.journal.snapshot(identity).retained_result_slots
        assert self.handoff_snapshot(identity).adoption.complete == reply.output_publication.complete
        return reply.results


class _Fixture:
    def __init__(self, forbidden):
        self.forbidden = forbidden
        self.owner, self.borrower = make_pure_core(), make_pure_core()
        self.local_ref = self.foreign_ref = None
        self.export_installed = False
        self.original = self.retried = None
        outer = ObjectID.for_task(TaskID.derive(
            self.owner.job_id, self.owner.driver_task_id, 999,
        ))
        self.source = protocol.ContainedTransferSource(ContainedReferenceHold(
            outer, self.owner.worker_id, "reconstruction-ack-race-export",
        ))
        self.backend = _OutputBackend(self.owner, forbidden)
        self.owner_calls, self.admissions = [], []

        def output_rpc(address, handler, request):
            if len(self.backend.calls) >= _LIMIT:
                forbidden("output callback budget exhausted", handler)
            return self.backend.rpc(address, handler, request)

        self.owner._rpc = output_rpc
        self.owner._resolve_node_address = self.backend.address
        self.borrower._reference_mailbox = _BorrowMailbox(self.borrower, forbidden)
        self.borrower._borrow_rpc = self.owner_rpc
        self.borrower._borrow_rpc_with_deadline = self.owner_rpc

    def prepare(self):
        owner = self.owner
        self.original, self.local_ref = owner._register_submission(
            owner.define_remote_function(self.forbidden), (), {},
            ResourceVector({"CPU": 1}), max_retries=1, _enqueue=True,
        )
        assert self.take() == (self.original,)
        self.backend.succeed(self.original, stored=True, value=41)
        assert owner._finish_pending_task(self.original)
        assert self.take() == ()
        assert owner._accepted_task_count == 0 and not owner._task_finish_barriers
        assert owner.owner_table.add_contained_reference(self.local_ref.object_id, self.source.hold)
        self.export_installed = True
        self.acquire = protocol.AcquireBorrowedObject(
            self.local_ref.object_id, owner.worker_id, self.borrower.worker_id,
            self.source, "reconstruction-ack-race-borrow",
        )
        release = protocol.ReleaseBorrowedObject(
            self.local_ref.object_id, owner.worker_id, self.borrower.worker_id,
            self.acquire.borrower_token,
        )
        self.key, self.obligation, inserted = self.borrower._register_borrowed_release_obligation(
            owner.owner_address, self.acquire, release,
        )
        assert inserted
        acquired = owner.acquire_exported_reference(self.acquire)
        assert acquired.accepted and acquired.acquired and acquired.source == self.source
        self.foreign_ref = ObjectRef(self.local_ref.object_id, owner.worker_id, owner.owner_address)
        self.foreign_ref._bind_borrowed_reference(
            self.borrower, self.acquire.borrower_token, self.source,
        )
        assert owner.drop_object(self.local_ref)
        assert self.backend.store.used_bytes == 0 and self.take() == ()
        state = owner.owner_table.snapshot(self.local_ref.object_id)
        assert state.state is ObjectState.LOST and not state.locations
        assert state.current_attempt == self.original.spec.attempt_id
        assert state.producer_task_spec == self.original.spec
        assert state.output_publication is not None and state.output_retirement_id is None
        assert owner._recovery.task_record(self.original.task_id).state is TaskState.SUCCEEDED
        assert not owner._task_finish_barriers
        self.assert_borrower_active()

    def request(self):
        ref = self.foreign_ref
        return protocol.RequestOwnedObjectReconstruction(
            ref.object_id, ref.owner_worker_id, self.borrower.worker_id,
            protocol.BorrowedCredential(self.source, ref.borrower_token),
            ref.borrower_token, self.original.spec.attempt_id,
        )

    def owner_rpc(self, address, handler, request, remaining=None):
        if len(self.owner_calls) >= _LIMIT or address != self.owner.owner_address:
            self.forbidden("unexpected owner callback route", address, handler)
        if handler == "get_owned_object":
            if type(request) is not protocol.GetOwnedObject or remaining is None or not 0 < remaining <= 1.0:
                self.forbidden("unexpected owner query", request, remaining)
            route = self.owner.get_owned_object
        elif handler == "release_borrowed_object":
            if type(request) is not protocol.ReleaseBorrowedObject:
                self.forbidden("unexpected owner release", request)
            route = self.owner.release_borrowed_reference
        else:
            self.forbidden("borrower must not re-admit or fetch after terminal ACK", handler)
        if (request.object_id != self.local_ref.object_id
                or request.owner_worker_id != self.owner.worker_id
                or request.borrower_worker_id != self.borrower.worker_id
                or request.borrower_token != self.acquire.borrower_token):
            self.forbidden("owner callback changed borrower identity", request)
        self.owner_calls.append((handler, request))
        return route(request)

    def assert_borrower_active(self):
        ref = self.foreign_ref
        assert self.borrower._active_borrower_capability(ref) == self.acquire
        snapshot = self.owner.owner_table.snapshot(ref.object_id)
        token = (self.borrower.worker_id, ref.borrower_token)
        assert token in snapshot.borrowed_tokens
        assert dict(snapshot.borrowed_sources)[token] == self.source
        assert self.borrower._borrowed_release_obligations == {self.key: self.obligation}
        assert not self.obligation.release_requested
        assert self.borrower._accepted_task_count == 0 and self.borrower._submissions.empty()
        assert self.borrower._recovery.lineage_for_object(ref.object_id) is None

    def take(self):
        fifo = self.owner._submissions
        size = fifo.qsize()
        if size > _LIMIT:
            self.forbidden("submission observation budget exhausted", size)
        tasks = []
        for _ in range(size):
            item = fifo.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) is _PendingTask
                    tasks.append(item)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0
        return tuple(tasks)

    def finish_admitted(self, outcome, *, application_error=False):
        assert not self.admissions and outcome.disposition is ReconstructionDisposition.START
        self.admissions.append(outcome)
        pending, = self.take()
        self.retried = pending
        attempt = self.original.spec.attempt_id.next()
        assert pending.object_id == self.original.object_id == self.foreign_ref.object_id
        assert pending.spec.owner_worker_id == self.owner.worker_id
        assert pending.spec.attempt_id == outcome.decision.attempt_id == attempt
        assert self.owner.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert self.owner._recovery.active_recovery(pending.task_id) == attempt
        assert self.owner._task_finish_barriers[pending.object_id] is pending
        if application_error:
            self.application_error = TaskError("reconstruction finished with application error")
            assert self.owner._publish_task_error(
                pending, self.application_error, failure_kind=FailureKind.APPLICATION,
            )
        else:
            self.backend.succeed(pending, stored=False, value=42)
        assert self.owner._finish_pending_task(pending)
        assert self.take() == ()
        snapshot = self.owner.owner_table.snapshot(pending.object_id)
        recovery = self.owner._recovery.reconstruction_snapshot(pending.object_id)
        assert snapshot.current_attempt == recovery.current_attempt == attempt
        assert recovery.active_recovery is None
        assert snapshot.state is (ObjectState.ERROR if application_error else ObjectState.READY_INLINE)
        assert recovery.task_state is (TaskState.APPLICATION_FAILED if application_error else TaskState.SUCCEEDED)
        assert not self.owner._reconstruction._sessions
        assert self.owner._accepted_task_count == 0 and not self.owner._task_finish_barriers
        assert self.backend.store.used_bytes == 0

    def close(self):
        try:
            if self.foreign_ref is not None:
                self.foreign_ref.close(timeout=0)
                assert not self.borrower._borrowed_release_obligations
                assert self.borrower._reference_mailbox.borrowed_releases == 1
            if self.export_installed:
                reply = self.owner.release_contained_reference(protocol.ReleaseContainedReference(
                    self.local_ref.object_id, self.owner.worker_id, self.source.hold,
                ))
                assert reply.accepted and reply.released
                self.export_installed = False
        finally:
            try:
                if self.local_ref is not None:
                    self.local_ref.close(timeout=0)
            finally:
                for core in (self.borrower, self.owner):
                    assert core._reference_mailbox.pending.qsize() <= _LIMIT
                    assert core._submissions.qsize() <= _LIMIT
                    close_pure_core(core)


@pytest.fixture
def owner_race(_no_runtime):
    fixture = _Fixture(_no_runtime)
    try:
        fixture.prepare()
        yield fixture
    finally:
        fixture.close()


def _complete_before_ack(monkeypatch, fixture, *, application_error=False):
    original_admit = fixture.owner._admit_owned_object_reconstruction

    def admit(object_id):
        outcome = original_admit(object_id)
        fixture.finish_admitted(outcome, application_error=application_error)
        return outcome

    monkeypatch.setattr(fixture.owner, "_admit_owned_object_reconstruction", admit)


def _assert_started_and_exact_replay(fixture, request, reply):
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
    assert reply.reconstruction_attempt == fixture.original.spec.attempt_id.next()
    assert reply.object_id == fixture.foreign_ref.object_id
    assert reply.owner_worker_id == fixture.owner.worker_id
    assert reply.credential == request.credential
    assert fixture.owner.request_owned_object_reconstruction(request) is reply
    assert len(fixture.admissions) == 1 and fixture.take() == ()
    fixture.assert_borrower_active()


def test_success_before_first_reconstruction_ack_still_returns_cached_started(monkeypatch, owner_race):
    f = owner_race
    _complete_before_ack(monkeypatch, f)
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    _assert_started_and_exact_replay(f, request, reply)
    assert f.borrower.get(f.foreign_ref, timeout=1.0) == 42
    assert [handler for handler, _ in f.owner_calls] == ["get_owned_object"]
    assert len(f.backend.completed) == 2


def test_application_error_before_first_ack_remains_task_error_not_authority_rejection(monkeypatch, owner_race):
    f = owner_race
    _complete_before_ack(monkeypatch, f, application_error=True)
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    _assert_started_and_exact_replay(f, request, reply)
    with pytest.raises(TaskError, match="reconstruction finished with application error") as caught:
        f.borrower.get(f.foreign_ref, timeout=1.0)
    assert caught.value.remote_type == "TaskError"
    assert caught.value.remote_message == str(f.application_error)
    assert [handler for handler, _ in f.owner_calls] == ["get_owned_object"]
    assert len(f.backend.completed) == 1


def test_uncommitted_preview_cannot_use_old_terminal_recovery_as_start_receipt(monkeypatch, owner_race):
    f = owner_race
    before = f.owner.owner_table.snapshot(f.local_ref.object_id)
    recovery_before = f.owner._recovery.reconstruction_snapshot(f.local_ref.object_id)
    previews = []

    def preview_only(object_id):
        assert not previews
        outcome = f.owner._reconstruction_coordinator().preview(object_id)
        previews.append(outcome)
        assert outcome.disposition is ReconstructionDisposition.START
        return outcome

    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", preview_only)
    reply = f.owner.request_owned_object_reconstruction(f.request())
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
    assert reply.failure is protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED
    assert reply.reconstruction_attempt is None and len(previews) == 1
    assert f.owner.owner_table.snapshot(f.local_ref.object_id) == before
    assert f.owner._recovery.reconstruction_snapshot(f.local_ref.object_id) == recovery_before
    assert f.take() == () and not f.owner._reconstruction._sessions


def test_terminal_owner_does_not_ack_an_outcome_for_a_different_epoch(monkeypatch, owner_race):
    f = owner_race
    original_admit = f.owner._admit_owned_object_reconstruction

    def wrong_epoch_outcome(object_id):
        outcome = original_admit(object_id)
        f.finish_admitted(outcome)
        # Fault only the callback's proposed receipt, never owner/recovery truth.
        return replace(outcome, decision=replace(
            outcome.decision, attempt_id=outcome.decision.attempt_id.next(),
        ))

    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", wrong_epoch_outcome)
    reply = f.owner.request_owned_object_reconstruction(f.request())
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
    assert reply.failure is protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED
    assert reply.reconstruction_attempt is None
    assert f.owner.owner_table.snapshot(f.local_ref.object_id).current_attempt == f.original.spec.attempt_id.next()
    assert f.borrower.get(f.foreign_ref, timeout=1.0) == 42
    assert len(f.admissions) == 1 and f.take() == ()


def test_core_reconstruction_ack_reads_owner_and_recovery_under_one_composition_lock(monkeypatch, owner_race):
    f = owner_race
    owner_snapshot = f.owner.owner_table.snapshot
    recovery_snapshot = f.owner._recovery.reconstruction_snapshot
    original_admit = f.owner._admit_owned_object_reconstruction
    reads, calls = [], []
    probing = True
    admitting = False

    def observe(kind, object_id):
        if not probing or admitting:
            return
        if len(reads) >= 4:
            f.forbidden("unexpected reconstruction metadata observation", kind)
        assert object_id == f.local_ref.object_id
        reads.append((kind, f.owner._state_lock._is_owned()))

    def read_owner(object_id):
        observe("owner", object_id)
        return owner_snapshot(object_id)

    def read_recovery(object_id):
        observe("recovery", object_id)
        return recovery_snapshot(object_id)

    def admit(object_id):
        nonlocal admitting
        assert not calls
        calls.append(object_id)
        admitting = True
        try:
            return original_admit(object_id)
        finally:
            admitting = False

    monkeypatch.setattr(f.owner.owner_table, "snapshot", read_owner)
    monkeypatch.setattr(f.owner._recovery, "reconstruction_snapshot", read_recovery)
    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", admit)
    try:
        request = f.request()
        reply = f.owner.request_owned_object_reconstruction(request)
        assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
        assert f.owner.request_owned_object_reconstruction(request) is reply
    finally:
        probing = False
    assert reads == [("owner", True), ("recovery", True)]
    assert calls == [f.local_ref.object_id]
    pending, = f.take()
    assert pending.spec.attempt_id == reply.reconstruction_attempt == f.original.spec.attempt_id.next()
    assert f.owner._task_finish_barriers[pending.object_id] is pending
    assert f.owner._accepted_task_count == 1
    # This admitted reconstruction remains PENDING; teardown does not invent a
    # completion merely to collect the last local/borrowed handles.


@pytest.mark.parametrize("complete_before_return", (False, True))
def test_admission_receipt_never_depends_on_a_post_admission_snapshot(
    monkeypatch, owner_race, complete_before_return,
):
    f = owner_race
    original_admit = f.owner._admit_owned_object_reconstruction
    original_snapshot = f.owner._owned_reconstruction_snapshot
    admissions, reads = [], []

    def admit(object_id):
        outcome = original_admit(object_id)
        admissions.append(outcome)
        assert outcome.disposition is ReconstructionDisposition.START
        if complete_before_return:
            f.finish_admitted(outcome)
        return outcome

    def only_pre_admission(object_id):
        if admissions or reads:
            f.forbidden("current metadata used as historical admission proof")
        pair = original_snapshot(object_id)
        reads.append(pair)
        return pair

    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", admit)
    monkeypatch.setattr(f.owner, "_owned_reconstruction_snapshot", only_pre_admission)
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
    assert reply.reconstruction_attempt == admissions[0].decision.attempt_id
    assert f.owner.request_owned_object_reconstruction(request) is reply
    assert len(reads) == len(admissions) == 1
    if not complete_before_return:
        pending, = f.take()
        assert pending.spec.attempt_id == reply.reconstruction_attempt
        assert f.owner._task_finish_barriers[pending.object_id] is pending
        assert f.owner._accepted_task_count == 1
    else:
        assert f.borrower.get(f.foreign_ref, timeout=1.0) == 42
        assert f.take() == ()
    f.assert_borrower_active()


@pytest.mark.parametrize(
    ("callback_fault", "expected_failure"),
    (
        ("failed-outcome", protocol.OwnedObjectReconstructionFailure.RETRY_EXHAUSTED),
        ("exception", protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED),
    ),
)
def test_committed_terminal_state_does_not_upgrade_failed_admission_callback(monkeypatch, owner_race, callback_fault, expected_failure):
    f = owner_race
    original_admit = f.owner._admit_owned_object_reconstruction
    detail = "injected callback failure after actual reconstruction completion"

    def failed_callback(object_id):
        outcome = original_admit(object_id)
        f.finish_admitted(outcome)
        if callback_fault == "exception":
            raise RuntimeError(detail)
        # Only the callback's returned outcome is faulty. No retry exhaustion
        # or failure is inserted into the successfully completed authorities.
        return replace(
            outcome, disposition=ReconstructionDisposition.FAILED, plan=None,
            decision=replace(
                outcome.decision, action=RecoveryAction.FAIL_RETRY_EXHAUSTED,
                reason=detail,
            ),
        )

    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", failed_callback)
    reply = f.owner.request_owned_object_reconstruction(f.request())
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
    assert reply.failure is expected_failure and detail in reply.detail
    assert reply.reconstruction_attempt is None
    assert not f.owner._owned_reconstruction._replies
    assert len(f.admissions) == 1 and len(f.backend.completed) == 2
    assert f.borrower.get(f.foreign_ref, timeout=1.0) == 42
    assert f.take() == () and not f.owner._task_finish_barriers
    assert f.owner._recovery.reconstruction_snapshot(f.local_ref.object_id).task_state is TaskState.SUCCEEDED


def test_terminal_system_failure_before_first_ack_preserves_started_attempt(monkeypatch, owner_race):
    f = owner_race
    original_admit = f.owner._admit_owned_object_reconstruction
    error = SystemTaskError("reconstruction ended in a terminal system failure")

    def terminal_before_ack(object_id):
        outcome = original_admit(object_id)
        assert not f.admissions and outcome.disposition is ReconstructionDisposition.START
        f.admissions.append(outcome)
        pending, = f.take()
        f.retried = pending
        attempt = f.original.spec.attempt_id.next()
        assert pending.object_id == f.original.object_id == f.foreign_ref.object_id
        assert pending.spec.owner_worker_id == f.owner.worker_id
        assert pending.spec.attempt_id == outcome.decision.attempt_id == attempt
        assert f.owner.owner_table.snapshot(object_id).state is ObjectState.PENDING
        assert f.owner._recovery.active_recovery(pending.task_id) == attempt
        assert f.owner._task_finish_barriers[object_id] is pending
        # This is Core's no-retry terminal system-error transaction, not an
        # execution retry or a fabricated RecoveryManager terminal snapshot.
        assert f.owner._publish_task_error(pending, error, failure_kind=FailureKind.SYSTEM)
        assert f.owner._finish_pending_task(pending)
        owner = f.owner.owner_table.snapshot(object_id)
        recovery = f.owner._recovery.reconstruction_snapshot(object_id)
        assert owner.state is ObjectState.ERROR and owner.error is error
        assert owner.current_attempt == recovery.current_attempt == attempt
        assert recovery.task_state is TaskState.SYSTEM_FAILED
        assert recovery.active_recovery is None and not f.owner._reconstruction._sessions
        assert f.owner._accepted_task_count == 0 and not f.owner._task_finish_barriers
        assert f.take() == () and f.backend.store.used_bytes == 0
        return outcome

    monkeypatch.setattr(f.owner, "_admit_owned_object_reconstruction", terminal_before_ack)
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    _assert_started_and_exact_replay(f, request, reply)
    with pytest.raises(TaskError, match="reconstruction ended in a terminal system failure") as caught:
        f.borrower.get(f.foreign_ref, timeout=1.0)
    assert caught.value.remote_type == "SystemTaskError"
    assert caught.value.remote_message == str(error)
    assert [handler for handler, _ in f.owner_calls] == ["get_owned_object"]
    assert len(f.backend.completed) == 1


@pytest.mark.parametrize(
    ("lookup_fault", "expected_failure"),
    (
        ("unknown-object", protocol.OwnedObjectReconstructionFailure.UNKNOWN_OBJECT),
        ("known-owner-recovery-error", protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED),
    ),
)
def test_pre_admission_snapshot_distinguishes_unknown_object_from_recovery_failure(monkeypatch, owner_race, lookup_fault, expected_failure):
    f = owner_race
    owner_snapshot = f.owner.owner_table.snapshot
    recovery_snapshot = f.owner._recovery.reconstruction_snapshot
    owner_before = owner_snapshot(f.local_ref.object_id)
    recovery_before = recovery_snapshot(f.local_ref.object_id)
    record_before = replace(f.owner._recovery.task_record(f.original.task_id))
    request = f.request()
    if lookup_fault == "unknown-object":
        # An unregistered sibling ID does not create another Task or object.
        unknown_id = ObjectID.for_task(f.original.task_id, return_index=1)
        assert not f.owner.owner_table.contains(unknown_id)
        request = replace(request, object_id=unknown_id)
    reads, lookup_errors, admissions = [], [], []

    def read_owner(object_id):
        assert not reads and object_id == request.object_id
        reads.append(("owner", object_id, f.owner._state_lock._is_owned()))
        try:
            snapshot = owner_snapshot(object_id)
        except UnknownObjectError as exc:
            lookup_errors.append(exc)
            raise
        assert snapshot == owner_before
        return snapshot

    def broken_recovery(object_id):
        assert lookup_fault == "known-owner-recovery-error"
        assert reads == [("owner", object_id, True)] and not lookup_errors
        assert object_id == f.local_ref.object_id
        reads.append(("recovery", object_id, f.owner._state_lock._is_owned()))
        # The real owner read succeeded. Only the second lookup boundary fails;
        # no missing-object state or altered recovery snapshot is fabricated.
        raise RuntimeError("injected reconstruction metadata lookup failure")

    def forbidden_admit(object_id):
        admissions.append(object_id)
        f.forbidden("failed pre-admission snapshot reached reconstruction")

    with monkeypatch.context() as patch:
        patch.setattr(f.owner.owner_table, "snapshot", read_owner)
        patch.setattr(f.owner._recovery, "reconstruction_snapshot", broken_recovery)
        patch.setattr(f.owner, "_admit_owned_object_reconstruction", forbidden_admit)
        reply = f.owner.request_owned_object_reconstruction(request)

    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
    assert reply.failure is expected_failure and reply.reconstruction_attempt is None
    assert reply.object_id == request.object_id and reply.credential == request.credential
    if lookup_fault == "unknown-object":
        assert reads == [("owner", request.object_id, True)]
        assert len(lookup_errors) == 1 and type(lookup_errors[0]) is UnknownObjectError
    else:
        assert reads == [("owner", request.object_id, True), ("recovery", request.object_id, True)]
        assert lookup_errors == []
    assert admissions == [] and not f.owner._owned_reconstruction._replies
    assert not f.owner._owned_reconstruction._claims
    assert owner_snapshot(f.local_ref.object_id) == owner_before
    assert recovery_snapshot(f.local_ref.object_id) == recovery_before
    assert f.owner._recovery.task_record(f.original.task_id) == record_before
    assert f.owner._accepted_task_count == 0 and not f.owner._task_finish_barriers
    assert not f.owner._reconstruction._sessions and f.take() == ()
    f.assert_borrower_active()


def test_post_handoff_trace_failure_cannot_hide_the_accepted_attempt(monkeypatch, owner_race):
    f = owner_race
    original_emit = f.owner._emit
    failed_events = []

    def emit(name, **attributes):
        if name == "object_reconstruction_started":
            failed_events.append(attributes)
            raise RuntimeError("observation failed after actual queue handoff")
        return original_emit(name, **attributes)

    monkeypatch.setattr(f.owner, "_emit", emit)
    request = f.request()
    reply = f.owner.request_owned_object_reconstruction(request)
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
    assert f.owner.request_owned_object_reconstruction(request) is reply
    pending, = f.take()
    assert pending.spec.attempt_id == reply.reconstruction_attempt
    assert f.owner._task_finish_barriers[pending.object_id] is pending
    assert f.owner._accepted_task_count == 1 and len(failed_events) == 1
