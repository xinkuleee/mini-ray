"""Pure owner-routed reconstruction with real publication and reference state.

The original successful cases use two threadless Cores, one logical Task with
two physical attempts, two publication envelopes and one 1-KiB ObjectStore.
Discovery, publication/adoption, debug replica loss, owner START/JOIN, retry
budget, borrower Release and final metadata GC are real reducers. The in-memory
Node/GCS/owner delivery is explicit: no Worker function, lease scheduler,
runtime constructor, thread, socket, timer or blocking wait runs here.
The transport-loss case injects an unavailable route; only an explicitly
installed owner-table death fact can turn unavailability into OwnerDiedError.
"""

from __future__ import annotations

import queue
import threading
from contextlib import contextmanager

import cloudpickle
import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import (
    CoreWorker, ObjectRef, _PendingTask, _ReleaseBorrowedReference,
    _RetryInlineGc, _WAKE_COORDINATOR,
)
from miniray.errors import (
    BorrowedObjectUnavailableError, OwnerDiedError, OwnerUnavailableError,
    SystemTaskError, UnreconstructableObjectError,
)
from miniray.ids import AttemptID, JobID, LeaseID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from miniray.worker import WorkerServer
from tests.unit._pure_core import (
    SynchronousReferenceMailbox, close_pure_core, make_pure_core,
)
from tests.unit._pure_reference_output_runtime import PureReferenceOutputRuntime


def _no_runtime(monkeypatch):
    import multiprocessing.process
    import socket
    import subprocess
    import time

    from miniray import control, core as core_module, node, transport

    failed = [False]
    receipt_checks = [0]

    def forbidden(*_args, **_kwargs):
        failed[0] = True
        pytest.fail("pure foreign reconstruction attempted runtime work")

    def already_set(event, timeout=None):
        if receipt_checks[0] >= 32 or not event.is_set():
            forbidden()
        receipt_checks[0] += 1
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (WorkerServer, "__init__"), (WorkerServer, "_embedded_core_for"),
        (node.NodeServer, "__init__"), (control.GCSLite, "__init__"),
        (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    for module in (core_module, node, control):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr("miniray.worker.rpc_request", forbidden)
    return failed, forbidden


def _new_core(forbidden):
    core = make_pure_core()
    for name in (
        "_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline",
        "_push_task_rpc", "_actor_call_rpc", "_execute",
        "_initialize_reference_events", "_schedule_reference_event",
    ):
        setattr(core, name, forbidden)
    return core


def _next_pending(core: CoreWorker) -> _PendingTask:
    for _ in range(32):
        item = core._submissions.get_nowait()
        try:
            if type(item) is _PendingTask:
                return item
            assert item is _WAKE_COORDINATOR
        finally:
            core._submissions.task_done()
    pytest.fail("no reconstruction within 32 explicit FIFO messages")


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
    pytest.fail("more than 32 non-task coordinator wakes")


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
    pytest.fail("foreign reconstruction GC exceeded 32 explicit events")


class _BorrowerMailbox(SynchronousReferenceMailbox):
    """Real borrowed release driver, synchronously consuming one close event."""

    def __init__(self, core, failed):
        super().__init__(core)
        self.failed = failed
        self.borrowed_events = []

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        try:
            assert not self.borrowed_events
            assert event.done is not None and event.scheduled_round is None
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


class _ForeignReconstruction:
    """One canonical stored producer, its borrower and an INLINE retry.

    The supported legacy explicit export pin does not pretend to be an output
    graph: its own release remains necessary alongside the borrower's ACK.
    PushTask supplies publication identity, not proof of actual Worker execution.
    """

    def __init__(self, monkeypatch):
        self.failed, self.forbidden = _no_runtime(monkeypatch)
        self.cores, self.refs, self.calls, self.publications = [], [], [], []
        self.pin_release = None
        self.pin_released = False
        self.retry = None
        self.acquire_reply = self.release_reply = None

    def record(self, handler, request):
        if len(self.calls) >= 32:
            self.forbidden()
        self.calls.append((handler, request))

    def start(self):
        owner = self.owner = _new_core(self.forbidden)
        self.cores.append(owner)
        borrower = self.borrower = _new_core(self.forbidden)
        self.cores.append(borrower)
        owner.owner_address = ("127.0.0.1", 29103)
        borrower._reference_mailbox = _BorrowerMailbox(borrower, self.failed)
        self.output = PureReferenceOutputRuntime(owner)
        owner._rpc = self.output_rpc
        borrower._borrow_rpc = self.owner_rpc
        pending, local = owner._register_submission(
            owner.define_remote_function(lambda: {"reconstructed": True}),
            (), {}, ResourceVector(), max_retries=1, _enqueue=True,
        )
        self.original, self.local = pending, local
        self.refs.append(local)
        assert _next_pending(owner) is pending
        assert owner._accepted_task_count == 1
        self.publish(pending, stored=True)
        assert owner._accepted_task_count == 0 and not owner._task_finish_barriers
        assert owner._recovery.task_record(pending.task_id).retries_started == 0
        assert self.output.store.used_bytes == len(cloudpickle.dumps({"reconstructed": True}))

        self.source = protocol.ContainedTransferSource(ContainedReferenceHold(
            ObjectID.for_task(TaskID(b'h' * 16)), owner.worker_id, "foreign-runtime-transfer"))
        assert owner.owner_table.add_contained_reference(
            pending.object_id, self.source.hold
        )
        self.pin_release = protocol.ReleaseContainedReference(
            pending.object_id, owner.worker_id, self.source.hold
        )
        self.token = "foreign-runtime-borrow"
        self.acquire = protocol.AcquireBorrowedObject(
            pending.object_id, owner.worker_id, borrower.worker_id, self.source, self.token
        )
        self.release = protocol.ReleaseBorrowedObject(
            pending.object_id, owner.worker_id, borrower.worker_id, self.token
        )
        self.acquire_reply = self.owner_rpc(
            owner.owner_address, "acquire_borrowed_object", self.acquire
        )
        assert self.acquire_reply.accepted and self.acquire_reply.acquired
        key, obligation, created = borrower._register_borrowed_release_obligation(
            owner.owner_address, self.acquire, self.release
        )
        assert created and obligation.acquire == self.acquire
        self.borrow_key = key
        self.ref = ObjectRef(pending.object_id, owner.worker_id, owner.owner_address)
        self.refs.append(self.ref)
        self.ref._bind_borrowed_reference(borrower, self.token, self.source)
        assert borrower._active_borrower_capability(self.ref) == self.acquire
        assert owner.drop_object(local)
        snapshot = owner.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.LOST and not snapshot.locations
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert snapshot.output_publication.manifest == self.publications[0].output_publication.manifest
        assert snapshot.output_publication.slot_index == 0
        assert snapshot.canonical_stored_result == self.publications[0].results[0]
        assert self.output.store.used_bytes == 0 and not self.output.replicas
        assert not owner.drop_object(local)  # Already LOST, no extra physical send.
        assert owner._submissions.empty() and borrower._submissions.empty()

    def output_rpc(self, address, handler, request):
        try:
            self.record(handler, request)
            return self.output.rpc(address, handler, request)
        except BaseException:
            self.failed[0] = True
            raise

    def owner_rpc(self, address, handler, request):
        try:
            assert address == self.owner.owner_address
            self.record(handler, request)
            if handler == "acquire_borrowed_object":
                assert type(request) is protocol.AcquireBorrowedObject
                return self.owner.acquire_exported_reference(request)
            assert handler == "release_borrowed_object"
            assert type(request) is protocol.ReleaseBorrowedObject and request == self.release
            assert self.release_reply is None
            self.release_reply = self.owner.release_borrowed_reference(request)
            assert self.release_reply.accepted and self.release_reply.released
            return self.release_reply
        except BaseException:
            self.failed[0] = True
            raise

    def publish(self, pending, *, stored):
        assert len(self.publications) < 2
        assert len(cloudpickle.dumps({"reconstructed": True})) <= 128
        push = protocol.PushTask(LeaseID.random(), self.owner.worker_id, pending.spec)
        reply = self.output.complete(
            push, ({"reconstructed": True},), inline_threshold=0 if stored else 1024,
        )
        self.publications.append(reply)
        assert self.owner._publish_reply(
            pending, reply, expected_node_id=self.owner.node_id, expected_lease_id=push.lease_id
        )
        snapshot = self.owner.owner_table.snapshot(pending.object_id)
        assert snapshot.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert snapshot.output_publication.publication_id == reply.output_publication.publication_id
        assert self.owner._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert self.owner._finish_pending_task(pending)
        _drain_wakes(self.owner)
        _drain_gc(self.owner)

    def request(self):
        acquire = self.borrower._active_borrower_capability(self.ref)
        return protocol.RequestOwnedObjectReconstruction(
            self.ref.object_id, self.ref.owner_worker_id, self.borrower.worker_id,
            protocol.BorrowedCredential(acquire.source, self.ref.borrower_token),
            self.ref.borrower_token, self.original.spec.attempt_id,
        )

    def assert_started(self, reply):
        assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
        assert reply.reconstruction_attempt == self.original.spec.attempt_id.next()
        snapshot = self.owner.owner_table.snapshot(self.original.object_id)
        assert snapshot.state is ObjectState.PENDING
        assert snapshot.current_attempt == reply.reconstruction_attempt
        assert snapshot.output_publication is None
        assert self.owner._recovery.task_record(self.original.task_id).retries_started == 1
        assert self.owner._recovery.active_recovery(self.original.task_id) == reply.reconstruction_attempt
        assert self.owner._accepted_task_count == 1

    def finish_retry(self, pending=None):
        assert self.retry is None
        pending = _next_pending(self.owner) if pending is None else pending
        self.retry = pending
        assert pending.object_id == self.original.object_id
        assert pending.task_id == self.original.task_id
        assert pending.output_ids == self.original.output_ids
        assert pending.spec.attempt_id == self.original.spec.attempt_id.next()
        assert pending.spec.max_retries == self.original.spec.max_retries == 1
        self.publish(pending, stored=False)
        assert self.owner._accepted_task_count == 0 and not self.owner._task_finish_barriers
        assert self.owner._recovery.active_recovery(pending.task_id) is None
        assert pending.task_id not in self.owner._reconstruction._sessions
        assert self.owner._recovery.task_record(pending.task_id).retries_started == 1
        assert not self.borrower._objects and self.borrower._accepted_task_count == 0

    def release_references(self):
        for ref in reversed(self.refs):
            if not ref.closed:
                ref.close(timeout=0)
            assert ref._release_done is not None and ref._release_done.is_set()
        if self.pin_release is not None and not self.pin_released:
            result = self.owner.release_contained_reference(self.pin_release)
            assert result.accepted and result.released
            self.pin_released = True
        for core in self.cores:
            _drain_gc(core)

    def assert_collected(self):
        self.release_references()
        assert self.release_reply is not None and self.release_reply.released
        assert not self.borrower._borrowed_release_obligations
        assert len(self.borrower._reference_mailbox.borrowed_events) == 1
        assert len(self.owner._reference_mailbox.releases) == 1
        assert self.owner.owner_table.collection_state(self.original.object_id) is ObjectCollectionState.COLLECTED
        assert self.owner._recovery.lineage_for_object(self.original.object_id) is None
        assert self.output.store.used_bytes == 0 and not self.output.replicas
        assert self.output.store.capacity_bytes == 1024
        assert len(self.output.dropped) == 1 and len(self.publications) == 2
        self.output.assert_collected()
        for core in self.cores:
            assert not core._objects and not core._stored_descriptors
            assert not core._protocol_unresolved and not core._object_gc_obligations
            assert core._accepted_task_count == 0 and not core._task_finish_barriers
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0

    def close(self):
        try:
            self.release_references()
        finally:
            for core in self.cores:
                close_pure_core(core)
            assert self.failed == [False]


@contextmanager
def _owner_and_borrower(monkeypatch):
    fixture = _ForeignReconstruction(monkeypatch)
    try:
        fixture.start()
        yield fixture
    finally:
        # A failed assertion leaves canonical Tasks/metadata intact. This
        # threadless fixture fence is not a claim of distributed shutdown.
        fixture.close()


@pytest.mark.unit
def test_foreign_lost_get_routes_start_to_owner_then_polls_same_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from miniray.core import _RPC_CALL_DEADLINE

    with _owner_and_borrower(monkeypatch) as fixture:
        owner, borrower, ref, original = fixture.owner, fixture.borrower, fixture.ref, fixture.original
        requests, deadlines, replies = [], [], []
        previous_deadline = _RPC_CALL_DEADLINE.get()

        def rpc(address, handler, request, timeout):
            try:
                assert address == owner.owner_address
                assert len(requests) < 3 and timeout is not None and 0 < timeout <= 1.0
                fixture.record(handler, request)
                requests.append(request)
                deadlines.append(_RPC_CALL_DEADLINE.get())
                if handler == "get_owned_object":
                    reply = owner.get_owned_object(request)
                    replies.append(reply)
                    return reply
                assert handler == "request_owned_object_reconstruction"
                reply = owner.request_owned_object_reconstruction(request)
                fixture.assert_started(reply)
                # Execute publication only after START's authoritative ACK was
                # formed; progress is explicit, not a racing Worker or a wait.
                fixture.finish_retry()
                return reply
            except BaseException:
                fixture.failed[0] = True
                raise

        monkeypatch.setattr(borrower, "_borrow_rpc_with_deadline", rpc)
        assert borrower.get(ref, timeout=1.0) == {"reconstructed": True}
        reconstruct = [request for request in requests
                       if isinstance(request, protocol.RequestOwnedObjectReconstruction)]
        assert len(reconstruct) == 1
        request = reconstruct[0]
        assert request.source == fixture.source
        assert request.credential == protocol.BorrowedCredential(fixture.source, fixture.token)
        assert request.expected_owner_attempt == original.spec.attempt_id
        assert owner.owner_table.snapshot(original.object_id).current_attempt == original.spec.attempt_id.next()
        assert len(requests) == 3 and requests[0] is requests[2]
        assert [reply.state for reply in replies] == [
            protocol.OwnedObjectState.LOST, protocol.OwnedObjectState.READY_INLINE,
        ]
        assert deadlines[0] is not None and deadlines == [deadlines[0]] * 3
        assert _RPC_CALL_DEADLINE.get() == previous_deadline
        assert borrower._submissions.empty() and not borrower._objects
        fixture.assert_collected()


@pytest.mark.unit
def test_lost_ack_replay_returns_exact_cached_start_without_second_enqueue() -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        with _owner_and_borrower(monkeypatch) as fixture:
            owner = fixture.owner
            request = fixture.request()
            first = owner.request_owned_object_reconstruction(request)
            replay = owner.request_owned_object_reconstruction(request)
            # Accepted reducer replies remain the same cached instance in the
            # current implementation, not merely equal new START responses.
            assert replay is first
            fixture.assert_started(first)
            # A fresh local admission joins the actual active attempt, while
            # retransmitting the exact remote transaction returns cached START.
            joined = owner._admit_owned_object_reconstruction(request.object_id)
            assert joined.disposition is ReconstructionDisposition.JOIN
            assert joined.plan is None and joined.decision.attempt_id == first.reconstruction_attempt
            assert owner._accepted_task_count == 1
            assert owner._recovery.task_record(fixture.original.task_id).retries_started == 1
            pending = _next_pending(owner)
            assert isinstance(pending, _PendingTask)
            with pytest.raises(queue.Empty):
                owner._submissions.get_nowait()
            fixture.finish_retry(pending)
            assert owner.request_owned_object_reconstruction(request) is first
            assert owner._submissions.empty()
            fixture.assert_collected()


@pytest.mark.unit
def test_worker_proxy_routes_owned_reconstruction_to_embedded_core() -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        with _owner_and_borrower(monkeypatch) as fixture:
            owner = fixture.owner
            worker = object.__new__(WorkerServer)
            worker.worker_id = owner.worker_id
            worker._embedded_core = owner
            worker._embedded_core_lock = threading.Lock()
            request = fixture.request()
            reply = worker._handle_request_owned_object_reconstruction(request)
            fixture.assert_started(reply)
            assert reply.reconstruction_attempt == fixture.original.spec.attempt_id.next()
            assert worker._borrow_owner_core() is owner
            assert worker._handle_request_owned_object_reconstruction(request) is reply
            fixture.finish_retry()
            assert not hasattr(worker, "_server") and worker._embedded_core is owner
            fixture.assert_collected()


@pytest.mark.parametrize(
    ("failure", "error_type"),
    (
        (protocol.OwnedObjectReconstructionFailure.PUT_OBJECT,
         UnreconstructableObjectError),
        (protocol.OwnedObjectReconstructionFailure.RETRY_EXHAUSTED,
         UnreconstructableObjectError),
        (protocol.OwnedObjectReconstructionFailure.RELEASED_CREDENTIAL,
         BorrowedObjectUnavailableError),
        (protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
         SystemTaskError),
    ),
)
@pytest.mark.unit
def test_foreign_reconstruction_failures_have_nondeath_typed_mapping(
    failure: protocol.OwnedObjectReconstructionFailure,
    error_type: type[BaseException],
) -> None:
    core = object.__new__(CoreWorker)
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    owner = WorkerID.random()
    source = protocol.ContainedTransferSource(ContainedReferenceHold(
        ObjectID.for_task(TaskID.derive(job, task, 1)), owner, "transfer"))
    request = protocol.RequestOwnedObjectReconstruction(
        ObjectID.for_task(task), owner, WorkerID.random(),
        source, "borrow",
        AttemptID(task, 0),
    )
    reply = protocol.RequestOwnedObjectReconstructionReply(
        request.object_id, request.owner_worker_id, request.requester_worker_id,
        request.source, request.borrower_token, request.expected_owner_attempt,
        protocol.OwnedObjectReconstructionDisposition.FAILED,
        failure=failure, detail=failure.value,
    )
    with pytest.raises(error_type, match=failure.value):
        core._raise_owned_reconstruction_failure(reply)


@pytest.mark.unit
def test_deadline_transport_loss_is_unavailable_until_death_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from miniray.core import _RPC_CALL_DEADLINE

    failed, forbidden = _no_runtime(monkeypatch)
    core = None
    try:
        core = _new_core(forbidden)
        owner = WorkerID.random()
        job = JobID.random()
        task = TaskID.derive(job, TaskID.for_driver(job), 1)
        request = protocol.GetOwnedObject(
            ObjectID.for_task(task), owner, core.worker_id, "borrow"
        )
        address = ("127.0.0.1", 29105)
        attempts, observations = [], []
        inherited_deadline = _RPC_CALL_DEADLINE.get()
        assert inherited_deadline is None

        def unavailable(target, handler, message, **options):
            try:
                assert not attempts
                assert (target, handler, message) == (address, "get_owned_object", request)
                assert 0 < options["connect_timeout"] <= 0.5
                assert 0 < options["request_timeout"] <= 1.0
                assert options["connect_timeout"] + options["request_timeout"] <= 1.0
                assert type(options["deadline"]) is float
                attempts.append((message, options["deadline"]))
            except BaseException:
                failed[0] = True
                raise
            raise TransportTimeout("ambiguous timeout")

        def authority_unavailable():
            try:
                assert not observations
                assert core.owner_table.dead_worker_record(owner) is None
                observations.append("unavailable")
                return False
            except BaseException:
                failed[0] = True
                raise

        # Invoke the real deadline method rather than the pure fixture's RPC
        # tripwire. Only its outbound transport and unavailable journal read
        # are injected; neither becomes an owner-death fact.
        monkeypatch.setattr("miniray.core.rpc_request", unavailable)
        monkeypatch.setattr(core, "_sync_worker_deaths", authority_unavailable)
        with pytest.raises(OwnerUnavailableError, match="unreachable"):
            CoreWorker._borrow_rpc_with_deadline(
                core, address, "get_owned_object", request, 1.0
            )
        assert len(attempts) == 1 and observations == ["unavailable"]
        assert core.owner_table.dead_worker_record(owner) is None
        # The original explicit authority boundary remains a pure reducer:
        # the fixture supplies a committed fact; no timeout proves process death.
        death = core.owner_table.install_dead_worker(owner, "committed-death")
        assert death.record == core.owner_table.dead_worker_record(owner)
        assert death.record.worker_id == owner and death.record.death_id == "committed-death"
        with pytest.raises(OwnerDiedError, match="confirmed dead"):
            CoreWorker._borrow_rpc_with_deadline(
                core, address, "get_owned_object", request, 1.0
            )
        assert len(attempts) == 1 and observations == ["unavailable"]
        assert _RPC_CALL_DEADLINE.get() is inherited_deadline
        assert not core._objects and core._submissions.empty()
        assert not core._object_gc_obligations and core._accepted_task_count == 0
    finally:
        if core is not None:
            close_pure_core(core)
        assert failed == [False]
