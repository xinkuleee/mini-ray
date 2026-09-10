"""Owner/borrower contracts split by actual infrastructure use.

Unit cases use protocol/owner tables, synchronous export callbacks or guarded
Worker publication. Seven borrower functions use real put/selected-output
ownership and explicit reference progress, including before-effect outages
and malformed/lost ACKs. Two separately marked L1 cases keep real Core
construction and shutdown with exactly three threads; transport and retry
timer delivery are isolated, and an external runner supplies the hard bound.
"""

from __future__ import annotations

import math
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace

import cloudpickle
import pytest

from miniray import (
    core as core_module, node as node_module, output_protocol as output_wire,
    protocol, transport as transport_module, worker as worker_module,
)
from miniray.contained_edges import ContainedReferenceHold
from miniray.control import NodeRegistry
from miniray.core import (
    CoreWorker, ObjectRef, _ObjectWaiter, _PendingTask, _ReleaseBorrowedReference,
    _STOP, _WAKE_COORDINATOR,
)
from miniray.errors import (
    BorrowedObjectUnavailableError, OwnerDiedError, OwnerUnavailableError,
)
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.ownership import (
    ConflictingBorrowerTokenError,
    ObjectCollectionState,
    ObjectOwnerTable,
    ObjectState,
    OutputOwnerPublicationPlan,
    ReleasedBorrowerTokenError,
)
from miniray.recovery import TaskState
from miniray.ref_transfer import discover_contained_reference
from miniray.resources import ResourceVector
from miniray.trace import EventSink
from miniray.transport import TransportTimeout
from tests.unit._pure_core import SynchronousReferenceMailbox, close_pure_core, make_pure_core
from tests.support._contained_output import ContainedOutput


@pytest.mark.unit
def test_acquire_wire_normalizes_full_contained_hold_source() -> None:
    object_id, _attempt_id = _object_id()
    owner = WorkerID.random()
    borrower = WorkerID.random()
    container = ObjectID.for_task(TaskID.random())
    hold = ContainedReferenceHold(container, borrower, "typed-transfer")

    request = protocol.AcquireBorrowedObject(
        object_id, owner, borrower, protocol.ContainedTransferSource(hold), "borrower-token"
    )
    reply = protocol.AcquireBorrowedObjectReply(
        object_id, owner, borrower, protocol.ContainedTransferSource(hold), "borrower-token", True, True
    )

    expected = protocol.ContainedTransferSource(hold)
    assert request.source == expected
    assert reply.source == expected


def _object_id(index: int = 0) -> tuple[ObjectID, AttemptID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return ObjectID.for_task(task_id), AttemptID(task_id, 0)


def _hold(token):
    return ContainedReferenceHold(ObjectID.for_task(TaskID(b'h' * 16)), WorkerID(b'h' * 16), token)


@pytest.mark.unit
def test_exporting_ref_without_bound_owner_endpoint_creates_no_custody():
    object_id, _ = _object_id()
    owner = WorkerID.random()
    ref = ObjectRef(object_id, owner)
    try:
        with pytest.raises(RuntimeError, match='bound owner endpoint'):
            discover_contained_reference(ref, owner, None)
        assert ref._local_token is None and ref.borrower_token is None and not ref.closed
    finally:
        ref.close(timeout=0)


@pytest.mark.unit
def test_owner_acquire_release_is_idempotent_and_fences_resurrection() -> None:
    table = ObjectOwnerTable()
    object_id, attempt_id = _object_id()
    table.register(object_id, current_attempt=attempt_id)
    table.add_contained_reference(object_id, _hold("transfer-1"))
    token = (WorkerID.random(), "borrow-1")

    assert table.acquire_exported_reference(object_id, protocol.ContainedTransferSource(_hold("transfer-1")), token)
    assert not table.acquire_exported_reference(object_id, protocol.ContainedTransferSource(_hold("transfer-1")), token)
    assert table.release_borrowed_reference(object_id, token)
    assert not table.release_borrowed_reference(object_id, token)
    with pytest.raises(ReleasedBorrowerTokenError):
        table.acquire_exported_reference(object_id, protocol.ContainedTransferSource(_hold("transfer-1")), token)

    snapshot = table.snapshot(object_id)
    assert snapshot.contained_tokens == frozenset({"transfer-1"})
    assert snapshot.borrowed_tokens == frozenset()
    assert token in snapshot.released_borrowed_tokens


@pytest.mark.unit
def test_borrower_token_is_bound_to_transfer_and_borrower_identity() -> None:
    table = ObjectOwnerTable()
    object_id, attempt_id = _object_id()
    table.register(object_id, current_attempt=attempt_id)
    table.add_contained_reference(object_id, _hold("transfer-a"))
    table.add_contained_reference(object_id, _hold("transfer-b"))
    first_borrower = WorkerID.random()
    second_borrower = WorkerID.random()

    assert table.acquire_exported_reference(
        object_id, protocol.ContainedTransferSource(_hold("transfer-a")), (first_borrower, "same-token")
    )
    with pytest.raises(ConflictingBorrowerTokenError):
        table.acquire_exported_reference(
            object_id, protocol.ContainedTransferSource(_hold("transfer-b")), (first_borrower, "same-token")
        )
    assert table.acquire_exported_reference(
        object_id, protocol.ContainedTransferSource(_hold("transfer-a")), (second_borrower, "same-token")
    )


@pytest.mark.unit
def test_release_before_acquire_tombstones_reordered_delivery() -> None:
    table = ObjectOwnerTable()
    object_id, attempt_id = _object_id()
    table.register(object_id, current_attempt=attempt_id)
    table.add_contained_reference(object_id, _hold("transfer"))
    token = (WorkerID.random(), "borrow")

    assert not table.release_borrowed_reference(object_id, token)
    with pytest.raises(ReleasedBorrowerTokenError):
        table.acquire_exported_reference(object_id, protocol.ContainedTransferSource(_hold("transfer")), token)


@pytest.mark.unit
def test_worker_plain_result_does_not_require_server_or_embedded_core(monkeypatch) -> None:
    """Pure: full Worker handoff with no listener or embedded Core creation."""
    from tests.unit.test_worker_unified_output import (
        _ActualNodePublication, _Fixture, _install_no_runtime,
    )

    _install_no_runtime(monkeypatch)
    fixture = _Fixture(monkeypatch, lambda: "plain")
    worker = fixture.worker
    del worker._server
    worker._embedded_core = None
    monkeypatch.setattr(worker, "_owner_address", lambda: pytest.fail("plain discovery requested an owner endpoint"))
    backend = _ActualNodePublication(fixture)
    reply = worker._handle_push_task(fixture.push)
    (result,) = reply.results
    assert cloudpickle.loads(result.inline_data) == "plain"
    assert worker._embedded_core is None
    assert not hasattr(worker, "_server")
    assert result.storage is protocol.ResultStorage.INLINE
    assert reply.output_publication == fixture.complete_envelope
    assert (reply.output_publication.manifest.value).transfers == ()
    assert backend.store.used_bytes == 0
    assert backend.completions == [reply.output_publication.complete]
    assert worker._handle_push_task(fixture.push) is reply
    assert fixture.executions == [True] and len(fixture.calls) == 3


@pytest.fixture
def _no_borrower_load_runtime(monkeypatch):
    """Only explicit borrower-load/release cases use these tripwires."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure borrower load attempted runtime infrastructure")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure ObjectRef close attempted a blocking wait"
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
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport_module, "request", forbidden)


class _BorrowerLoadMailbox(SynchronousReferenceMailbox):
    """Drive exact borrowed releases synchronously; ordinary GC stays FIFO."""

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        assert event.done is not None and event.scheduled_round is None
        core = self.core_reference()
        assert core is not None
        try:
            assert core._drive_borrowed_reference_release(event.key)
        finally:
            event.done.set()
        return True


class _BorrowerLoadFixture:
    """Actual same-owner Task publication followed by independent borrowers."""
    def __init__(self):
        self.output = output = ContainedOutput(same_owner=True)
        self.owner = output.core
        self.borrower = make_pure_core()
        self.borrower._reference_mailbox = _BorrowerLoadMailbox(self.borrower)
        self.borrower._borrow_rpc = self.borrow_rpc
        self.borrower._borrow_rpc_with_deadline = self.borrow_rpc_with_deadline
        self.calls, self.borrowed_refs = [], []
        self.lose_acquire_ack = False
        output.register()
        self.child, self.outer, self.pending = output.child_ref, output.ref, output.pending
        self.node, self.journal, self.adapter = output.node, output.journal, output.adapter
        assert self.take(self.owner) == (self.pending,)
        self.lineage = self.owner.owner_table.snapshot(self.child.object_id).lineage_tokens
        reply = output.complete()
        self.transfer, self.envelope = output.transfer, output.envelope
        assert self.owner._publish_reply(self.pending, reply,
            expected_node_id=self.node.node_id, expected_lease_id=output.grant.lease_id)
        assert self.owner._finish_pending_task(self.pending)
        assert self.take(self.owner) == ()
        self.payload = self.owner.owner_table.snapshot(self.outer.object_id).inline_data
        assert self.payload == (output.outputs.payload)
        assert not self.journal.snapshot(self.envelope.publication_id).result_retained

    def borrow_rpc(self, address, handler, message):
        assert address == self.owner.owner_address and len(self.calls) < 10
        assert message.object_id == self.child.object_id and message.owner_worker_id == self.owner.worker_id
        assert message.borrower_worker_id == self.borrower.worker_id
        self.calls.append((handler, message))
        if handler == "acquire_borrowed_object":
            reply = self.owner.acquire_exported_reference(message)
            assert reply.accepted and reply.acquired
            if self.lose_acquire_ack:
                # Preserve the original injected exception type. This is ACK
                # ambiguity, not a Worker death fact or permission to take over.
                raise OwnerDiedError("lost acquire ACK")
            return reply
        if handler == "release_borrowed_object":
            reply = self.owner.release_borrowed_reference(message)
            assert reply.accepted and reply.released
            return reply
        assert handler == "get_owned_object"
        return self.owner.get_owned_object(message)

    def borrow_rpc_with_deadline(self, address, handler, message, remaining):
        assert remaining is None
        return self.borrow_rpc(address, handler, message)

    def load(self):
        ref = self.borrower._loads_owned_value(self.payload)["child"]
        self.borrowed_refs.append(ref)
        assert len(self.borrowed_refs) <= 2
        return ref

    @staticmethod
    def take(core):
        size = core._submissions.qsize()
        assert size <= 8
        work = []
        for _ in range(size):
            item = core._submissions.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) is _PendingTask
                    work.append(item)
            finally:
                core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        return tuple(work)

    def assert_collected(self):
        for core in (self.owner, self.borrower):
            assert core._reference_mailbox.pending.qsize() <= 8
            core._reference_mailbox.drain()
            assert self.take(core) == ()
            assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
            assert not core._task_finish_barriers and not core._protocol_unresolved
            assert core._accepted_task_count == 0
            assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
        assert not self.borrower._borrowed_release_obligations
        assert all(self.owner.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                   for ref in (self.child, self.outer))
        assert not self.owner._recovery.reconstruction_snapshot(self.child.object_id).is_put
        assert self.owner._recovery.lineage_for_object(self.outer.object_id) is None
        assert self.node.object_store.used_bytes == 0
        snapshot = self.owner._output_handoff_table().query(self.envelope.publication_id)
        assert snapshot.adoption is not None
        assert self.adapter.report_terminal(self.envelope.publication_id)
        assert not self.adapter.pending_terminal_reports()

    def close(self):
        for ref in (*self.borrowed_refs, self.outer, self.child):
            if ref is not None:
                ref.close(timeout=0)
        close_pure_core(self.borrower)
        close_pure_core(self.owner)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_repeated_loads_acquire_distinct_tokens_and_remote_get_inline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _BorrowerLoadFixture()
    owner, borrower, child_id = f.owner, f.borrower, f.child.object_id
    try:
        first, second = f.load(), f.load()
        assert first.object_id == second.object_id == child_id
        assert first.owner_address == second.owner_address == owner.owner_address
        assert first.borrower_token != second.borrower_token
        assert first.borrow_source == second.borrow_source == (
            protocol.ContainedTransferSource(f.transfer.final_hold)
        )
        assert borrower.get(first) == {"answer": 42}
        assert borrower.get(second) == {"answer": 42}
        snapshot = owner.owner_table.snapshot(child_id)
        assert len(snapshot.borrowed_tokens) == 2
        assert len(snapshot.contained_tokens) == 1
        assert snapshot.contained_holds == frozenset({f.transfer.final_hold})
        assert snapshot.lineage_tokens == f.lineage
        f.child.close(timeout=0)
        owner._reference_mailbox.drain()
        assert not owner.owner_table.snapshot(child_id).local_tokens
        first.close(timeout=0)
        snapshot = owner.owner_table.snapshot(child_id)
        assert len(snapshot.borrowed_tokens) == 1
        assert borrower.get(second) == {"answer": 42}
        assert len(borrower._borrowed_release_obligations) == 1
        # Outer collection releases its contained edge and canonical lineage;
        # the second already-acquired handle remains an independent lifetime.
        f.outer.close(timeout=0)
        owner._reference_mailbox.drain()
        snapshot = owner.owner_table.snapshot(child_id)
        assert not snapshot.contained_holds and not snapshot.lineage_tokens and not snapshot.local_tokens
        assert snapshot.borrowed_tokens == frozenset({(borrower.worker_id, second.borrower_token)})
        assert snapshot.state is ObjectState.READY_INLINE
        assert borrower.get(second) == {"answer": 42}
        second.close(timeout=0)
        assert [handler for handler, _ in f.calls].count("acquire_borrowed_object") == 2
        assert [handler for handler, _ in f.calls].count("release_borrowed_object") == 2
        assert not borrower._borrowed_release_obligations
        f.assert_collected()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_ambiguous_acquire_attempts_release_tombstone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _BorrowerLoadFixture()
    borrower, owner, object_id = f.borrower, f.owner, f.child.object_id
    try:
        f.lose_acquire_ack = True
        with pytest.raises(OwnerDiedError, match="lost acquire ACK"):
            borrower._loads_owned_value(f.payload)
        assert [handler for handler, _ in f.calls] == [
            "acquire_borrowed_object", "release_borrowed_object"
        ]
        acquire, release = (message for _, message in f.calls)
        assert acquire.borrower_token == release.borrower_token
        assert acquire.source == protocol.ContainedTransferSource(f.transfer.final_hold)
        snapshot = owner.owner_table.snapshot(object_id)
        assert not snapshot.borrowed_tokens
        assert (borrower.worker_id, acquire.borrower_token) in snapshot.released_borrowed_tokens
        assert not borrower._borrowed_release_obligations and not f.borrowed_refs
        assert not borrower._owner_is_dead(owner.worker_id)
        rejected = owner.acquire_exported_reference(acquire)
        assert not rejected.accepted and not rejected.acquired and "released" in rejected.error
        repeated = owner.release_borrowed_reference(release)
        assert repeated.accepted and not repeated.released
        assert owner.owner_table.snapshot(object_id).contained_holds == frozenset({f.transfer.final_hold})
        f.child.close(timeout=0)
        f.outer.close(timeout=0)
        f.assert_collected()
    finally:
        f.close()


class _BorrowerReleaseMailbox(SynchronousReferenceMailbox):
    """Close receipts acknowledge real release intent, not an invented ACK."""

    def __init__(self, core):
        super().__init__(core)
        self.borrowed_drives = []

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        assert event.done is not None and event.scheduled_round is None
        core = self.core_reference()
        assert core is not None and not self.borrowed_drives
        try:
            completed = core._drive_borrowed_reference_release(event.key)
            self.borrowed_drives.append((event, completed))
            if not completed:
                obligation = core._borrowed_release_obligations[event.key]
                assert obligation.release_requested and obligation.scheduled_round is not None
        finally:
            event.done.set()
        return True


class _BorrowerReleaseDelivery:
    """One bad Release delivery and one exact scheduled replay, no timer.

    By default the owner applies both calls, and only the first response is
    lost or mutated; its tombstone returns released=False on replay. A separate
    before-effect option leaves the original owner token live during outage.
    Core chooses the event and round; the test explicitly delivers that event.
    """

    def __init__(self, fixture, monkeypatch, *, wrong_field=None, before_effect=False):
        self.fixture, self.wrong_field = fixture, wrong_field
        self.before_effect = before_effect
        assert not before_effect or wrong_field is None
        self.available = False
        self.events = queue.Queue(maxsize=2)
        self.scheduled, self.actual_replies, self.releases = [], [], []
        core = fixture.borrower
        assert not core._reference_mailbox.releases and core._reference_mailbox.pending.empty()
        self.mailbox = _BorrowerReleaseMailbox(core)
        original_rpc = core._borrow_rpc
        monkeypatch.setattr(core, "_reference_mailbox", self.mailbox)

        def schedule(mailbox, event, delay):
            assert mailbox is self.mailbox and type(event) is _ReleaseBorrowedReference
            assert event.done is None and self.events.empty() and not self.scheduled
            obligation = core._borrowed_release_obligations[event.key]
            assert event.scheduled_round == obligation.scheduled_round == obligation.retry_round == 1
            assert delay == min(core_module._PUSH_RETRY_MAX_SECONDS, core_module._PUSH_RETRY_BASE_SECONDS)
            self.scheduled.append((event, delay))
            self.events.put_nowait(event)

        def rpc(address, handler, message):
            if handler != "release_borrowed_object":
                return original_rpc(address, handler, message)
            assert address == fixture.owner.owner_address and type(message) is protocol.ReleaseBorrowedObject
            assert len(self.releases) < 2
            assert message.object_id == fixture.child.object_id
            assert message.owner_worker_id == fixture.owner.worker_id and message.borrower_worker_id == core.worker_id
            if self.releases:
                assert message == self.releases[0]
            fixture.calls.append((handler, message))
            self.releases.append(message)
            if self.before_effect and not self.available:
                assert fixture.owner.owner_table.has_borrowed_reference(
                    message.object_id, (core.worker_id, message.borrower_token),
                )
                raise OwnerDiedError("owner temporarily unavailable")
            reply = fixture.owner.release_borrowed_reference(message)
            assert reply.accepted and reply.released is (len(self.actual_replies) == 0)
            self.actual_replies.append(reply)
            token = (core.worker_id, message.borrower_token)
            snapshot = fixture.owner.owner_table.snapshot(message.object_id)
            assert token not in snapshot.borrowed_tokens and token in snapshot.released_borrowed_tokens
            if self.available:
                return reply
            if self.wrong_field is None:
                raise OwnerDiedError("lost release ACK")
            if self.wrong_field == "type":
                return object()
            changes = {
                "object": {"object_id": _object_id()[0]},
                "owner": {"owner_worker_id": WorkerID.random()},
                "borrower": {"borrower_worker_id": WorkerID.random()},
                "token": {"borrower_token": "other"},
                "accepted": {"accepted": False, "released": False, "error": "rejected"},
            }
            assert self.wrong_field in changes
            return replace(reply, **changes[self.wrong_field])

        monkeypatch.setattr(core, "_borrow_rpc", rpc)
        monkeypatch.setattr(core, "_schedule_reference_event", schedule)

    def replay(self, obligation):
        assert self.available and len(self.scheduled) == 1
        assert self.events.qsize() == self.events.unfinished_tasks == 1
        event = self.events.get_nowait()
        try:
            assert event is self.scheduled[0][0] and event.key == obligation.key
            assert event.scheduled_round == obligation.scheduled_round == 1
            assert self.fixture.borrower._drive_borrowed_reference_release(
                event.key, scheduled_round=event.scheduled_round,
            )
        finally:
            self.events.task_done()
        assert self.events.empty() and self.events.unfinished_tasks == 0
        if self.before_effect:
            assert len(self.actual_replies) == 1 and self.actual_replies[0].released
        else:
            assert self.actual_replies[0].released and not self.actual_replies[1].released
        assert self.releases == [obligation.release, obligation.release]


@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_failed_restore_compensation_persists_and_replays_exact_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _BorrowerLoadFixture()
    delivery = _BorrowerReleaseDelivery(f, monkeypatch)
    borrower, owner = f.borrower, f.owner
    try:
        f.lose_acquire_ack = True
        with pytest.raises(OwnerDiedError, match="lost acquire ACK"):
            borrower._restore_borrowed_reference(
                f.child.object_id, owner.worker_id, owner.owner_address, f.transfer.final_hold,
            )
        assert len(borrower._borrowed_release_obligations) == 1
        obligation = next(iter(borrower._borrowed_release_obligations.values()))
        assert obligation.acquire.source == protocol.ContainedTransferSource(
            f.transfer.final_hold
        )
        assert obligation.release_requested
        assert delivery.releases == [obligation.release]
        assert [handler for handler, _ in f.calls] == [
            "acquire_borrowed_object", "release_borrowed_object",
        ]
        assert f.calls[0][1] == obligation.acquire
        assert obligation.acquire.borrower_token == obligation.release.borrower_token
        assert obligation.retry_round == obligation.scheduled_round == 1
        assert len(delivery.scheduled) == 1 and delivery.events.qsize() == 1
        assert delivery.mailbox.borrowed_drives == []  # restore itself owns compensation
        assert not borrower._owner_is_dead(owner.worker_id)
        before = owner.owner_table.snapshot(f.child.object_id)
        assert not before.borrowed_tokens and before.contained_holds == frozenset({f.transfer.final_hold})
        assert before.lineage_tokens == f.lineage
        # Close only this inert Core's admission for the real drain precheck.
        # There are no runtime lanes to shut down or unobserved timers to wait.
        with borrower._state_lock:
            borrower._accepting = False
        assert not borrower.can_finalize_shutdown(
            require_distributed_clean=False
        )

        delivery.available = True
        delivery.replay(obligation)
        assert not borrower._borrowed_release_obligations
        assert all(item == obligation.release for item in delivery.releases)
        assert owner.owner_table.snapshot(f.child.object_id) == before
        assert borrower.can_finalize_shutdown(require_distributed_clean=False)
        rejected = owner.acquire_exported_reference(obligation.acquire)
        assert not rejected.accepted and not rejected.acquired and "released" in rejected.error
        assert not borrower._owner_is_dead(owner.worker_id)
        f.child.close(timeout=0)
        f.outer.close(timeout=0)
        f.assert_collected()
    finally:
        f.close()


@pytest.mark.parametrize("wrong_field", [
    "type", "object", "owner", "borrower", "token", "accepted",
])
@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_borrowed_release_requires_exact_accepted_ack(
    monkeypatch: pytest.MonkeyPatch, wrong_field: str,
) -> None:
    f = _BorrowerLoadFixture()
    delivery = _BorrowerReleaseDelivery(f, monkeypatch, wrong_field=wrong_field)
    borrower, owner = f.borrower, f.owner
    try:
        ref = f.load()
        ((key, obligation),) = tuple(borrower._borrowed_release_obligations.items())
        assert not obligation.release_requested and obligation.retry_round == 0
        assert obligation.acquire == f.calls[0][1]
        assert obligation.release.borrower_token == ref.borrower_token
        assert owner.owner_table.has_borrowed_reference(f.child.object_id, (borrower.worker_id, ref.borrower_token))
        ref.close(timeout=0)
        assert ref.closed and ref._release_done.is_set()
        assert delivery.mailbox.borrowed_drives[0][1] is False
        assert borrower._borrowed_release_obligations[key] is obligation
        assert obligation.release_requested and obligation.retry_round == obligation.scheduled_round == 1
        assert len(delivery.scheduled) == 1 and delivery.events.qsize() == 1
        assert delivery.actual_replies[0].accepted and delivery.actual_replies[0].released
        assert not owner.owner_table.snapshot(f.child.object_id).borrowed_tokens
        assert not borrower._owner_is_dead(owner.worker_id)
        with borrower._state_lock:
            borrower._accepting = False
        assert not borrower.can_finalize_shutdown(require_distributed_clean=False)
        delivery.available = True
        delivery.replay(obligation)
        assert key not in borrower._borrowed_release_obligations
        assert borrower.can_finalize_shutdown(require_distributed_clean=False)
        assert len(delivery.mailbox.borrowed_drives) == 1
        assert delivery.releases == [obligation.release, obligation.release]
        assert delivery.actual_replies[1].accepted and not delivery.actual_replies[1].released
        rejected = owner.acquire_exported_reference(obligation.acquire)
        assert not rejected.accepted and not rejected.acquired
        ref.close(timeout=0)
        assert len(delivery.releases) == 2  # repeated close cannot mint another release
        f.child.close(timeout=0)
        f.outer.close(timeout=0)
        f.assert_collected()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_successful_borrowed_close_failure_stays_shutdown_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _BorrowerLoadFixture()
    owner, borrower, child_id = f.owner, f.borrower, f.child.object_id
    delivery = _BorrowerReleaseDelivery(f, monkeypatch, before_effect=True)
    try:
        ref = f.load()
        before = owner.owner_table.snapshot(child_id)
        assert before.borrowed_tokens == frozenset({(borrower.worker_id, ref.borrower_token)})
        ref.close(timeout=0)
        assert len(borrower._borrowed_release_obligations) == 1
        obligation = next(iter(borrower._borrowed_release_obligations.values()))
        assert obligation.release_requested
        assert obligation.acquire == f.calls[0][1] and obligation.retry_round == obligation.scheduled_round == 1
        assert delivery.releases == [obligation.release] and delivery.actual_replies == []
        assert delivery.mailbox.borrowed_drives[0][1] is False
        assert owner.owner_table.snapshot(child_id) == before
        assert not borrower._owner_is_dead(owner.worker_id)
        with borrower._state_lock:
            borrower._accepting = False
        assert not borrower.can_finalize_shutdown(
            require_distributed_clean=False
        )
        delivery.available = True
        # This is the real in-memory shutdown GC precheck, not a public
        # process shutdown. Its synchronous drive wins before the old timer
        # event is manually delivered; that stale event must then be harmless.
        assert borrower._retry_gc_obligations_for_shutdown()
        assert not borrower._borrowed_release_obligations
        assert delivery.releases == [obligation.release, obligation.release]
        assert len(delivery.actual_replies) == 1 and delivery.actual_replies[0].released
        assert delivery.events.qsize() == 1
        delivery.replay(obligation)
        assert len(delivery.releases) == 2
        assert borrower.can_finalize_shutdown(require_distributed_clean=False)
        assert not owner.owner_table.snapshot(child_id).borrowed_tokens
        assert owner.owner_table.snapshot(child_id).contained_holds == before.contained_holds
        assert owner.owner_table.snapshot(child_id).lineage_tokens == f.lineage
        f.child.close(timeout=0)
        f.outer.close(timeout=0)
        f.assert_collected()
    finally:
        delivery.available = True
        f.close()


class _BorrowerShutdownSink(EventSink):
    def __init__(self):
        super().__init__()
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class _LiveBorrowerShutdownProbe:
    """Three real Core threads, one acquired handle, zero live Tasks or I/O.

    The published owner/outer fixture stays threadless. The borrower below
    uses the actual constructor, coordinator, dispatcher, reference consumer
    and Core.shutdown. One scheduling callback retains the exact retry event
    instead of constructing a Timer; transport calls invoke real owner
    handlers in-process. This L1 covers neither network nor timer races.

    Normal joins use at most one second; failure cleanup shares two seconds.
    Internal startup/Queue/lock waits still need the runner's 30-second bound.
    Every unexpected effect or daemon exception is also checked by the main
    test. Failure-only signals and joins never count as successful shutdown.
    """

    def __init__(self, monkeypatch):
        self.core = object.__new__(CoreWorker)
        self.owner_fixture, self.ref = None, None
        self.sink = _BorrowerShutdownSink()
        self.main_thread = threading.current_thread()
        self.baseline = self.core_threads()
        self.observation_lock = threading.Lock()
        self.created, self.started, self.joins, self.thread_errors, self.violations = [], [], [], [], []
        self.acquires, self.gets, self.releases, self.scheduled = [], [], [], []
        self.retry_events = queue.Queue(maxsize=1)
        self.owner_available = threading.Event()
        self.owner_available.set()
        self.constructing = self.constructed = self.cleaning = False
        real_thread, real_core_init = threading.Thread, CoreWorker.__init__
        probe = self

        class ObservedThread(real_thread):
            def __init__(thread, *args, **kwargs):
                super().__init__(*args, **kwargs)
                names = {
                    "miniray-core-reference-events", "miniray-core-worker-coordinator",
                    "miniray-core-worker-dispatch-0",
                }
                with probe.observation_lock:
                    probe.created.append(thread)
                    valid = (probe.constructing and thread.name in names and len(probe.created) <= 3
                             and sum(item.name == thread.name for item in probe.created) == 1)
                if not valid:
                    probe.forbidden("unexpected borrower shutdown thread")

            def start(thread):
                if thread not in probe.created or thread in probe.started:
                    probe.forbidden("unowned or repeated thread start")
                super().start()
                with probe.observation_lock:
                    probe.started.append(thread)

            def join(thread, timeout=None):
                limit = 2.0 if probe.cleaning else 1.0
                if (thread not in probe.created or type(timeout) not in (int, float)
                        or not math.isfinite(timeout) or not 0 <= timeout <= limit):
                    probe.forbidden("unowned or unbounded borrower shutdown join")
                with probe.observation_lock:
                    probe.joins.append((thread, timeout, probe.cleaning))
                    valid = len(probe.joins) <= 16
                if not valid:
                    probe.forbidden("borrower shutdown exceeded its join budget")
                return super().join(timeout)

            def run(thread):
                try:
                    return super().run()
                except BaseException as exc:
                    with probe.observation_lock:
                        probe.thread_errors.append((thread, exc))

        def owned_constructor(core, *args, **kwargs):
            if core is not probe.core or not probe.constructing:
                probe.forbidden("unexpected Core constructor")
            return real_core_init(core, *args, **kwargs)

        monkeypatch.setattr(threading, "Thread", ObservedThread)
        monkeypatch.setattr(CoreWorker, "__init__", owned_constructor)
        for kind in (NodeServer, worker_module.WorkerServer, transport_module.TCPServer):
            monkeypatch.setattr(kind, "__init__", self.forbidden)
        for name in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, name, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", self.forbidden)
        monkeypatch.setattr(threading.Timer, "__init__", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        for module in (core_module, node_module, worker_module):
            monkeypatch.setattr(module, "rpc_request", self.forbidden)
        monkeypatch.setattr(transport_module, "request", self.forbidden)
        for name in (
            "_rpc", "_push_task_rpc", "_actor_call_rpc", "_register_submission",
            "_execute", "put", "create_actor", "create_placement_group",
        ):
            monkeypatch.setattr(self.core, name, self.forbidden)
        monkeypatch.setattr(self.core, "_borrow_rpc", self.owner_rpc)
        monkeypatch.setattr(self.core, "_schedule_reference_event", self.schedule_retry)

    @staticmethod
    def core_threads():
        return {thread for thread in threading.enumerate() if thread.name.startswith("miniray-core-")}

    def forbidden(self, *args, **kwargs):
        with self.observation_lock:
            self.violations.append((threading.current_thread(), args, kwargs))
        raise AssertionError("borrower shutdown L1 attempted unmodelled runtime work")

    def construct(self):
        # Reuse only the published owner side; its unused pure borrower never
        # acquires a token or replaces the real Core being exercised.
        self.owner_fixture = _BorrowerLoadFixture()
        self.constructing = True
        try:
            CoreWorker.__init__(
                self.core, ("borrower-shutdown.invalid", 1), NodeID.random(),
                event_sink=self.sink, dispatch_lanes=1, gcs_address=None, poll_node_deaths=False,
            )
            self.constructed = True
        finally:
            self.constructing = False
        core = self.core
        assert set(self.created) == {core._reference_thread, core._coordinator, core._dispatcher}
        assert self.started == [core._reference_thread, core._dispatcher, core._coordinator]
        assert all(thread.is_alive() for thread in self.started)
        assert core._reference_mailbox is not self.owner_fixture.borrower._reference_mailbox
        assert not core._objects and core._accepted_task_count == 0 and not core._gc_retry_timers

    def owner_rpc(self, address, handler, message):
        owner = self.owner_fixture.owner
        if (address != owner.owner_address or message.object_id != self.owner_fixture.child.object_id
                or message.owner_worker_id != owner.worker_id or message.borrower_worker_id != self.core.worker_id):
            self.forbidden("owner route/credential drift")
        caller = threading.current_thread()
        if caller not in (self.main_thread, getattr(self.core, "_reference_thread", None)):
            self.forbidden("borrow operation on an unexpected thread")
        if handler == "acquire_borrowed_object":
            if type(message) is not protocol.AcquireBorrowedObject or self.acquires:
                self.forbidden("unexpected additional borrower acquisition")
            assert message.source == protocol.ContainedTransferSource(self.owner_fixture.transfer.final_hold)
            reply = owner.acquire_exported_reference(message)
            self.acquires.append((message, reply))
            assert reply.accepted and reply.acquired
            return reply
        if handler == "get_owned_object":
            assert type(message) is protocol.GetOwnedObject and len(self.gets) < 2
            reply = owner.get_owned_object(message)
            self.gets.append((message, reply))
            return reply
        if handler != "release_borrowed_object" or type(message) is not protocol.ReleaseBorrowedObject:
            self.forbidden("unexpected owner operation")
        acquire = self.acquires[0][0]
        assert message == protocol.ReleaseBorrowedObject(
            acquire.object_id, acquire.owner_worker_id, acquire.borrower_worker_id, acquire.borrower_token,
        )
        assert len(self.releases) < (5 if self.cleaning else 3)
        if not self.owner_available.is_set():
            assert owner.owner_table.has_borrowed_reference(message.object_id, (message.borrower_worker_id, message.borrower_token))
            self.releases.append((caller, message, None))
            raise OwnerDiedError("owner unavailable")
        reply = owner.release_borrowed_reference(message)
        self.releases.append((caller, message, reply))
        assert reply.accepted
        return reply

    def schedule_retry(self, mailbox, event, delay):
        core = self.core
        assert mailbox is core._reference_mailbox and type(event) is _ReleaseBorrowedReference
        assert event.done is None and event.scheduled_round == 1
        assert delay == min(core_module._PUSH_RETRY_BASE_SECONDS, core_module._PUSH_RETRY_MAX_SECONDS)
        assert not self.scheduled and self.retry_events.empty()
        with core._state_lock:
            obligation = core._borrowed_release_obligations[event.key]
            assert obligation.release_requested and obligation.retry_round == obligation.scheduled_round == 1
        self.scheduled.append(event)
        self.retry_events.put_nowait(event)

    def acquire(self):
        self.ref = self.core._loads_owned_value(self.owner_fixture.payload)["child"]
        ((key, obligation),) = tuple(self.core._borrowed_release_obligations.items())
        assert obligation.acquire == self.acquires[0][0]
        assert not obligation.release_requested and obligation.retry_round == 0
        assert self.core.get(self.ref) == {"answer": 42}
        return key, obligation

    def retire_saved_retry(self):
        assert len(self.scheduled) == 1 and self.retry_events.qsize() == 1
        event = self.retry_events.get_nowait()
        before = tuple(self.releases)
        try:
            assert event is self.scheduled[0] and event.key not in self.core._borrowed_release_obligations
            assert self.core._drive_borrowed_reference_release(event.key, scheduled_round=event.scheduled_round)
        finally:
            self.retry_events.task_done()
        assert tuple(self.releases) == before
        assert self.retry_events.empty() and self.retry_events.unfinished_tasks == 0

    def assert_stopped(self):
        core = self.core
        assert len(self.created) == len(self.started) == 3
        assert all(thread.ident is not None and not thread.is_alive() for thread in self.created)
        assert self.core_threads() == self.baseline
        assert not self.thread_errors and not self.violations
        assert not core._accepting and not core._owner_protocol_open and core._sink_closed
        assert self.sink.close_calls == 1
        assert core._reference_mailbox.stop_enqueued and core._reference_mailbox.stopped.is_set()
        assert not core._reference_runtime_finalizer.alive
        assert not core._gc_retry_timers_open and not core._gc_retry_timers
        assert not core._borrowed_release_obligations and not core._protocol_unresolved
        assert not core._objects and not core._task_finish_barriers and core._accepted_task_count == 0
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0

    def collect_owner(self):
        fixture = self.owner_fixture
        owner = fixture.owner
        snapshot = owner.owner_table.snapshot(fixture.child.object_id)
        assert not snapshot.borrowed_tokens
        assert snapshot.contained_holds == frozenset({fixture.transfer.final_hold})
        assert snapshot.lineage_tokens == fixture.lineage and snapshot.local_tokens
        fixture.child.close(timeout=0)
        fixture.outer.close(timeout=0)
        assert owner._reference_mailbox.pending.qsize() <= 8
        owner._reference_mailbox.drain()
        assert fixture.take(owner) == ()
        assert not owner._objects and not owner._object_gc_obligations
        assert all(owner.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                   for ref in (fixture.child, fixture.outer))
        assert owner._recovery.lineage_for_object(fixture.outer.object_id) is None
        assert not owner._recovery.reconstruction_snapshot(fixture.child.object_id).is_put
        assert fixture.node.object_store.used_bytes == 0
        assert owner._output_handoff_table().query(fixture.envelope.publication_id).adoption is not None
        assert fixture.adapter.report_terminal(fixture.envelope.publication_id)
        assert not fixture.adapter.pending_terminal_reports()

    def cleanup(self):
        self.cleaning = True
        self.owner_available.set()
        deadline = time.monotonic() + 2.0
        if self.ref is not None:
            try:
                self.ref.close(timeout=min(0.5, max(0.0, deadline - time.monotonic())))
            except TimeoutError:
                pass
        if self.constructed and any(thread.is_alive() for thread in self.created):
            remaining = min(1.0, max(0.0, deadline - time.monotonic()))
            if remaining > 0:
                try:
                    self.core.shutdown(timeout=remaining)
                except Exception:
                    pass
        if any(thread.is_alive() for thread in self.created):
            # Failure-only unblocking never clears owner/recovery obligations
            # or supplies a successful Core.shutdown result to the test.
            core = self.core
            core._accepting = False
            gate = getattr(core, "_startup_threads_gate", None)
            if gate is not None:
                gate.set()
            submissions, ready = getattr(core, "_submissions", None), getattr(core, "_ready_tasks", None)
            if submissions is not None:
                submissions.put_nowait(_STOP)
            if ready is not None:
                for _thread in getattr(core, "_dispatchers", ()):
                    ready.put_nowait(_STOP)
            mailbox = getattr(core, "_reference_mailbox", None)
            if mailbox is not None:
                mailbox.stop()
            for thread in self.created:
                if thread.ident is not None:
                    thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in self.created)
        finalizer = getattr(self.core, "_reference_runtime_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
        if not self.sink.close_calls:
            self.sink.close()
        if self.owner_fixture is not None:
            self.owner_fixture.close()
        assert self.core_threads() == self.baseline
        assert not self.violations and not self.thread_errors


@pytest.mark.loopback_smoke
def test_shutdown_retries_unresolved_borrowed_release_before_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _LiveBorrowerShutdownProbe(monkeypatch)
    try:
        probe.construct()
        core = probe.core
        key, obligation = probe.acquire()
        owner, object_id = probe.owner_fixture.owner, probe.ref.object_id
        before = owner.owner_table.snapshot(object_id)
        probe.owner_available.clear()
        probe.ref.close(timeout=0.5)
        assert obligation.release_requested and obligation.scheduled_round == 1
        assert len(probe.releases) == 1 and probe.releases[0][0] is core._reference_thread
        assert probe.releases[0][2] is None and owner.owner_table.snapshot(object_id) == before
        assert not core.shutdown(timeout=1.0)
        assert core._borrowed_release_obligations[key] is obligation
        assert owner.owner_table.snapshot(object_id) == before
        assert len(probe.releases) == 2 and all(reply is None for _, _, reply in probe.releases)
        assert not core._coordinator.is_alive() and not core._dispatcher.is_alive()
        assert core._reference_thread.is_alive() and core._owner_protocol_open and not core._sink_closed
        assert not core._reference_mailbox.stop_enqueued and not core.can_finalize_shutdown()
        assert len(probe.scheduled) == 1 and probe.retry_events.qsize() == 1
        assert not core._owner_is_dead(owner.worker_id)
        probe.owner_available.set()
        assert core.shutdown(timeout=1.0)
        assert not core._borrowed_release_obligations
        assert len(probe.releases) == 3 and probe.releases[-1][2].released
        assert all(message == obligation.release for _, message, _ in probe.releases)
        probe.retire_saved_retry()
        probe.assert_stopped()
        probe.collect_owner()
    finally:
        probe.cleanup()


@pytest.mark.loopback_smoke
def test_shutdown_releases_live_borrowed_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _LiveBorrowerShutdownProbe(monkeypatch)
    try:
        probe.construct()
        core = probe.core
        key, obligation = probe.acquire()
        assert not probe.ref.closed and not obligation.release_requested
        assert core.shutdown(timeout=1.0)
        assert key not in core._borrowed_release_obligations
        assert len(probe.releases) == 1 and probe.releases[0][0] is probe.main_thread
        assert probe.releases[0][1] == obligation.release and probe.releases[0][2].released
        assert not probe.scheduled and probe.retry_events.empty()
        probe.assert_stopped()
        # Shutdown already discharged this exact token; a later Python close
        # obtains its local receipt without making a second owner RPC.
        probe.ref.close(timeout=0)
        assert probe.ref.closed and probe.ref._release_done.is_set()
        assert len(probe.releases) == 1
        probe.collect_owner()
    finally:
        probe.cleanup()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_owner_unreachable_and_foreign_operations_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _BorrowerLoadFixture()
    core, owner = f.borrower, f.owner
    calls, effects = [], []
    available = False
    # The public methods must retain their actual transport error conversion
    # and finite idempotent retries, not a test helper raising the final error.
    monkeypatch.setattr(core, "_borrow_rpc", CoreWorker._borrow_rpc.__get__(core))
    monkeypatch.setattr(core, "_borrow_rpc_with_deadline", CoreWorker._borrow_rpc_with_deadline.__get__(core))

    def rpc(address, handler, message, **_options):
        assert address == owner.owner_address and message.owner_worker_id == owner.worker_id
        assert message.borrower_worker_id == core.worker_id and message.object_id == f.child.object_id
        assert len(calls) < 20
        calls.append((handler, message))
        if not available:
            raise TransportTimeout("owner gone")  # before any owner reducer effect
        handlers = {
            "get_owned_object": owner.get_owned_object,
            "acquire_borrowed_object": owner.acquire_exported_reference,
            "release_borrowed_object": owner.release_borrowed_reference,
            "release_owned_object_for_task": owner.release_owned_object_for_task,
        }
        assert handler in handlers
        reply = handlers[handler](message)
        effects.append((handler, message, reply))
        return reply

    monkeypatch.setattr(core_module, "rpc_request", rpc)
    try:
        available = True
        foreign = f.load()
        assert len(effects) == 1 and effects[0][0] == "acquire_borrowed_object"
        available = False
        before = owner.owner_table.snapshot(f.child.object_id)
        acquire = core._active_borrower_capability(foreign)
        key = (owner.worker_id, foreign.object_id, core.worker_id, foreign.borrower_token)
        obligation = core._borrowed_release_obligations[key]
        with pytest.raises(OwnerUnavailableError, match="unreachable"):
            core.get(foreign)
        with pytest.raises(OwnerUnavailableError, match="unreachable"):
            core.wait([foreign])
        with pytest.raises(
            (OwnerUnavailableError, BorrowedObjectUnavailableError)
        ):
            core.drop_object(foreign)
        with pytest.raises(OwnerUnavailableError, match="unreachable"):
            core._register_submission(
                core.define_remote_function(lambda value: value),
                (foreign,), {}, ResourceVector(),
            )
        assert owner.owner_table.snapshot(f.child.object_id) == before
        assert len(effects) == 1 and len(calls) == 16
        assert [handler for handler, _ in calls[1:]] == (
            ["get_owned_object"] * 9 + ["retain_owned_object_for_task"] * 3
            + ["release_owned_object_for_task"] * 3
        )
        assert core._active_borrower_capability(foreign) == acquire
        assert core._borrowed_release_obligations[key] is obligation and not obligation.release_requested
        assert not core._owner_is_dead(owner.worker_id)
        assert not core._objects and not core._task_finish_barriers and core._accepted_task_count == 0
        assert core._inflight_submissions == 0 and core._submission_index == 1
        (guard,) = tuple(core._orphan_foreign_guard_releases.values())
        assert guard.hold == calls[10][1].hold
        assert calls[10][1] == calls[11][1] == calls[12][1]
        assert calls[13][1] == calls[14][1] == calls[15][1]
        assert guard.hold == calls[13][1].hold
        assert core._foreign_lineage_registry.snapshot(guard.hold.task_id) is None
        assert core._recovery.lineage_for_object(ObjectID.for_task(guard.hold.task_id)) is None
        # The Retain never reached the owner, but a delayed delivery is still
        # possible to the caller. A real Release-before-Retain ACK is required
        # before forgetting the original rollback obligation.
        available = True
        core._retry_orphan_foreign_guard_releases()
        assert not core._orphan_foreign_guard_releases
        assert effects[-1][0] == "release_owned_object_for_task"
        assert effects[-1][2].accepted and not effects[-1][2].released
        assert owner.owner_table.retained_release_was_seen(f.child.object_id, guard.hold)
        assert core.get(foreign) == {"answer": 42}
        assert core.wait([foreign]) == ([foreign], [])
        foreign.close(timeout=0)
        assert effects[-1][0] == "release_borrowed_object" and effects[-1][2].released
        assert not core._borrowed_release_obligations
        f.child.close(timeout=0)
        f.outer.close(timeout=0)
        f.assert_collected()
    finally:
        available = True
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_borrower_load_runtime")
def test_owner_rejects_new_acquire_but_serves_existing_token_while_closing() -> None:
    f = _BorrowerLoadFixture()
    owner, borrower, object_id = f.owner, f.borrower, f.child.object_id
    try:
        ref = f.load()
        acquire = f.calls[0][1]
        assert acquire.source == protocol.ContainedTransferSource(f.transfer.final_hold)
        token = (borrower.worker_id, ref.borrower_token)
        assert owner.owner_table.snapshot(object_id).borrowed_tokens == frozenset({token})
        with owner._state_lock:
            owner._accepting = False
        owner.close_owner_retain_admission()
        assert owner._owner_protocol_open and not owner.can_finalize_shutdown()
        before = owner.owner_table.snapshot(object_id)
        rejected = owner.acquire_exported_reference(
            protocol.AcquireBorrowedObject(
                object_id, owner.worker_id, borrower.worker_id, acquire.source, "second",
            )
        )
        assert not rejected.accepted and not rejected.acquired
        assert "shutting down" in rejected.error
        assert owner.owner_table.snapshot(object_id) == before
        existing = owner.get_owned_object(
            protocol.GetOwnedObject(
                object_id, owner.worker_id, borrower.worker_id, ref.borrower_token,
            )
        )
        assert existing.accepted
        assert existing.state is protocol.OwnedObjectState.READY_INLINE
        assert cloudpickle.loads(existing.data) == {"answer": 42}
        assert borrower.get(ref) == {"answer": 42}
        ref.close(timeout=0)  # actual owner Release must remain legal while closing
        assert [handler for handler, _ in f.calls].count("release_borrowed_object") == 1
        assert not owner.owner_table.snapshot(object_id).borrowed_tokens
        assert token in owner.owner_table.snapshot(object_id).released_borrowed_tokens
        assert not borrower._borrowed_release_obligations
        assert owner._inflight_borrow_ops == 0 and owner._owner_protocol_open
        f.child.close(timeout=0)
        f.outer.close(timeout=0)
        f.assert_collected()
        assert owner.can_finalize_shutdown()  # metadata drain, not actual process shutdown
    finally:
        f.close()
