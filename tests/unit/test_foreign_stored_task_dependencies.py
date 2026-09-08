"""Foreign dependency contracts with explicit infrastructure classification.

Five canonical submission cases use real foreign-lineage, Node custody and
final GC authorities with finite in-memory progress. Three also cover selected
output or pre-Push failure paths. The two death-fenced cases preserve actual
membership/replica cleanup; the formal STALE replacement is an explicit typed
protocol fault, quarantine, then separately supplied owner-death authority.
The remaining bare-state unit contracts are separately scoped.
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

import cloudpickle
import pytest

from miniray import (
    control as control_module, core as core_module, node as node_module, output_protocol as output_wire,
    protocol, transport as transport_module, worker as worker_module,
)
from miniray.control import GCSLite, NodeRegistry, WorkerRegistry
from miniray.core import (
    CoreWorker, _DelayedReadyTask, _ForeignDependencyGuard,
    _LeaseRequestState, _LocationReportState, _ObjectWaiter, _PendingTask,
    _ReleaseBorrowedReference, _STOP, _WAKE_COORDINATOR,
)
from miniray.errors import OwnerDiedError, ProtocolError, SystemTaskError
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.ownership import (
    ObjectCollectionState, ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan,
)
from miniray.recovery import TaskState
from miniray.ref_transfer import ReferenceExportSession
from miniray.resources import AllocationToken, ResourceVector
from miniray.trace import EventSink
from miniray.transport import TransportConnectionError, TransportTimeout
from tests.unit._pure_core import (
    SynchronousReferenceMailbox, close_pure_core, make_pure_core,
)
from tests.unit._pure_node_output import prepare_ref_free_output
from tests.unit.test_node_placement_group_runtime import _node as _pure_report_node


def _identity(index: int = 0) -> tuple[ObjectID, AttemptID]:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), index)
    return ObjectID.for_task(task), AttemptID(task, 0)


def _retained_hold(
    borrower: WorkerID, origin_attempt: AttemptID,
) -> protocol.TaskReferenceHold:
    return protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, borrower,
        origin_attempt.task_id, origin_attempt,
    )


def _owner_core(
    borrower: WorkerID, *, node_id: NodeID | None = None, payload: bytes | None = None,
) -> tuple[CoreWorker, ObjectID, AttemptID, protocol.ObjectStoreDescriptor]:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = node_id or NodeID.random()
    core.node_address = ("127.0.0.1", 27101)
    core.event_sink = EventSink()
    core._owner_table = ObjectOwnerTable()
    core._stored_descriptors = {}
    core._objects = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._owner_protocol_open = True
    core._owner_retain_admission_open = True
    core._inflight_borrow_ops = 0
    core._dead_nodes = {}
    core._membership_epoch = 0
    core._placement_group_states = {}
    core._placement_group_manifests = {}
    core._submissions = queue.Queue()
    core._object_gc_obligations = {}
    object_id, attempt = _identity()
    data = payload or cloudpickle.dumps({"blob": b"x" * 4096})
    checksum = hashlib.sha256(data).hexdigest()
    result = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(data),
        core.worker_id, core.node_id, checksum,
    )
    core.owner_table.register(object_id, current_attempt=attempt)
    assert core.owner_table.publish_stored(
        object_id, attempt, core.node_id, descriptor=result
    )
    core.owner_table.add_borrowed_reference(object_id, (borrower, "borrow"))
    core._stored_descriptors[object_id] = result
    descriptor = protocol.ObjectStoreDescriptor(
        object_id, core.worker_id, attempt, core.node_id, len(data), checksum,
    )
    return core, object_id, attempt, descriptor


def _consumer_core() -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 27102)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.event_sink = EventSink()
    core._owner_table = ObjectOwnerTable()
    core._stored_descriptors = {}
    core._objects = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._submissions = queue.Queue()
    core._protocol_unresolved = {}
    core._foreign_guard_release_retries = {}
    core._active_task_finishes = set()
    core._finishing_tasks = set()
    core._finished_tasks = set()
    core._accepted_task_count = 0
    return core


def _pending(
    core: CoreWorker, descriptors: tuple[protocol.ObjectStoreDescriptor, ...]
) -> tuple[_PendingTask, tuple[_ForeignDependencyGuard, ...]]:
    task = TaskID.derive(core.job_id, core.driver_task_id, 0)
    attempt = AttemptID(task, 0)
    output = ObjectID.for_task(task)
    spec = protocol.TaskSpec(
        core.job_id, task, attempt,
        protocol.FunctionKey(core.job_id, __name__, "consume", "v1"),
        tuple(
            protocol.RefArg(item.object_id, item.owner_worker_id)
            for item in descriptors
        ),
        1, ResourceVector({"CPU": 1}), core.worker_id,
    )
    hold = _retained_hold(core.worker_id, attempt)
    guards = tuple(
        _ForeignDependencyGuard(
            item.object_id, item.owner_worker_id,
            ("127.0.0.1", 27200 + index), core.worker_id,
            "borrow{}".format(index), hold,
        )
        for index, item in enumerate(descriptors)
    )
    core.owner_table.register(output, current_attempt=attempt)
    core._objects[output] = _ObjectWaiter(threading.Event())
    return _PendingTask(
        output, spec, foreign_dependency_guards=guards
    ), guards


def _grant(
    pending: _PendingTask, dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
    *, target: NodeID | None = None,
) -> protocol.GrantWorkerLease:
    node = target or NodeID.random()
    return protocol.GrantWorkerLease(
        LeaseID.random(), pending.spec.task_id, pending.spec.attempt_id, node,
        WorkerID.random(), ("127.0.0.1", 27301),
        AllocationToken("target-allocation"),
        tuple(replace(item, node_id=node) for item in dependencies),
    )


def _install_hold(
    owner: CoreWorker, guard: _ForeignDependencyGuard, borrower_token: str = "borrow"
) -> None:
    # Helpers use distinct default tokens only when exercising Core-side report
    # machinery without a real owner.  Real owner tests bind the exact token.
    if not owner.owner_table.has_borrowed_reference(
        guard.object_id, (guard.borrower_worker_id, borrower_token)
    ):
        owner.owner_table.add_borrowed_reference(
            guard.object_id, (guard.borrower_worker_id, borrower_token)
        )
    owner.owner_table.retain_borrowed_reference_for_task(
        guard.object_id, (guard.borrower_worker_id, borrower_token),
        guard.hold,
    )


@pytest.mark.unit
def test_report_protocol_binds_full_credential_and_byte_free_descriptor() -> None:
    borrower = WorkerID.random()
    owner, object_id, _, descriptor = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    request = protocol.ReportRetainedObjectLocation(
        object_id, owner.worker_id, borrower, hold, descriptor
    )
    reply = protocol.ReportRetainedObjectLocationReply(
        object_id, owner.worker_id, borrower, hold, descriptor,
        protocol.RetainedLocationReportStatus.ADDED,
    )
    assert request.hold == reply.hold == hold
    assert reply.accepted and reply.added and not reply.stale
    assert not hasattr(request.descriptor, "data")

    with pytest.raises(ProtocolError, match="match object and owner"):
        protocol.ReportRetainedObjectLocation(
            object_id, WorkerID.random(), borrower, hold, descriptor
        )
    with pytest.raises(ProtocolError, match="must contain an error"):
        protocol.ReportRetainedObjectLocationReply(
            object_id, owner.worker_id, borrower, hold, descriptor,
            protocol.RetainedLocationReportStatus.STALE_PRODUCER,
        )


@pytest.mark.unit
def test_owner_reports_target_location_idempotently_and_fences_metadata() -> None:
    borrower = WorkerID.random()
    owner, object_id, attempt, source = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    owner.owner_table.retain_borrowed_reference_for_task(
        object_id, (borrower, "borrow"), hold
    )
    target = replace(source, node_id=NodeID.random())
    request = protocol.ReportRetainedObjectLocation(
        object_id, owner.worker_id, borrower, hold, target
    )
    first = owner.report_retained_object_location(request)
    replay = owner.report_retained_object_location(request)
    assert first.status is protocol.RetainedLocationReportStatus.ADDED
    assert replay.status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
    assert owner.owner_table.snapshot(object_id).locations == frozenset(
        {source.node_id, target.node_id}
    )

    wrong_checksum = replace(target, checksum="0" * 64)
    mismatch = owner.report_retained_object_location(replace(
        request, descriptor=wrong_checksum
    ))
    assert mismatch.status is protocol.RetainedLocationReportStatus.REJECTED
    stale = owner.report_retained_object_location(replace(
        request, descriptor=replace(target, producer_attempt_id=attempt.next())
    ))
    assert stale.status is protocol.RetainedLocationReportStatus.STALE_PRODUCER
    owner.close_owner_retain_admission()
    assert owner.report_retained_object_location(request).accepted


@pytest.mark.unit
def test_foreign_stored_prepare_keeps_ref_and_never_fetches_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = _consumer_core()
    owner, _, _, source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (source,))
    guard = replace(
        guards[0], borrower_token="borrow"
    )
    _install_hold(owner, guard)
    consumer._borrow_rpc = lambda _address, handler, request: (
        owner.release_owned_object_for_task(request)
        if handler == "release_owned_object_for_task"
        else owner.get_retained_owned_object(request)
        if handler == "get_retained_owned_object"
        else owner.retain_owned_object_for_task(request)
        if handler == "retain_owned_object_for_task"
        else owner.report_retained_object_location(request)
        if handler == "report_retained_object_location"
        else (_ for _ in ()).throw(AssertionError(handler))
    )
    pending = replace(pending, foreign_dependency_guards=(guard,))
    monkeypatch.setattr(
        consumer, "_query_foreign_dependency_guard",
        lambda item: owner.get_retained_owned_object(
            protocol.GetRetainedOwnedObject(
                item.object_id, item.owner_worker_id,
                item.borrower_worker_id, item.hold,
            )
        ),
    )
    monkeypatch.setattr(
        consumer, "_fetch_borrowed_stored_object",
        lambda *_args, **_kwargs: pytest.fail("Core must not fetch stored bytes"),
    )
    prepared, dependencies, _ = consumer._prepare_task_dependencies(
        pending.spec, pending.foreign_dependency_guards
    )
    assert prepared.args == pending.spec.args
    assert dependencies == (source,)


@pytest.mark.unit
def test_location_reports_preserve_partial_ack_and_block_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = _consumer_core()
    first_owner, _, _, first_source = _owner_core(consumer.worker_id)
    second_owner, _, _, second_source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (first_source, second_source))
    guards = tuple(
        replace(guard, borrower_token="borrow") for guard in guards
    )
    for owner, guard in zip((first_owner, second_owner), guards):
        _install_hold(owner, guard)
    pending = replace(pending, foreign_dependency_guards=guards)
    grant = _grant(pending, (first_source, second_source))
    reports = consumer._build_location_reports(
        (first_source, second_source), grant, guards
    )
    assert tuple(report.request.hold for report in reports) == tuple(
        guard.hold for guard in guards
    )
    state = _LocationReportState(
        grant, ("127.0.0.1", 27302), reports
    )
    calls: list[tuple[ObjectID, protocol.TaskReferenceHold]] = []
    failed = False

    def report(item):
        nonlocal failed
        # Report construction revalidates/copies immutable credentials. Match
        # the complete semantic guard, not the identity of its Python object.
        matches = tuple(
            (owner, guard)
            for owner, guard in zip((first_owner, second_owner), guards)
            if item.guard == guard
        )
        assert len(matches) == 1, "unexpected location-report owner credential"
        owner, guard = matches[0]
        expected_descriptor = next(
            descriptor for descriptor in grant.dependencies
            if descriptor.object_id == guard.object_id
        )
        assert owner.worker_id == guard.owner_worker_id
        assert item.request == protocol.ReportRetainedObjectLocation(
            guard.object_id, guard.owner_worker_id, guard.borrower_worker_id,
            guard.hold, expected_descriptor,
        )
        calls.append((item.guard.object_id, item.guard.hold))
        if guard == guards[1] and not failed:
            failed = True
            raise RuntimeError("ACK lost")
        return owner.report_retained_object_location(item.request)

    monkeypatch.setattr(consumer, "_report_foreign_dependency_location", report)
    assert not consumer._report_granted_dependency_locations(
        pending, pending.spec, (first_source, second_source), state
    )
    delayed = consumer._submissions.get_nowait()
    next_state = delayed.ready.location_state
    assert next_state is not None
    assert next_state.grant == grant
    assert next_state.reports == reports
    assert next_state.acknowledged_keys == (
        consumer._foreign_guard_key(guards[0]),
    )
    assert consumer._is_protocol_unresolved(pending)

    assert consumer._report_granted_dependency_locations(
        pending, pending.spec, (first_source, second_source), next_state
    )
    assert calls == [
        (guards[0].object_id, guards[0].hold),
        (guards[1].object_id, guards[1].hold),
        (guards[1].object_id, guards[1].hold),
    ]


@pytest.mark.unit
def test_location_report_ack_reenters_wire_validator() -> None:
    consumer = _consumer_core()
    owner, _, _, source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (source,))
    target = replace(source, node_id=NodeID.random())
    report = consumer._build_location_reports(
        (source,), _grant(pending, (source,), target=target.node_id), guards
    )[0]
    valid = protocol.ReportRetainedObjectLocationReply(
        source.object_id, source.owner_worker_id, consumer.worker_id,
        guards[0].hold, target,
        protocol.RetainedLocationReportStatus.ADDED,
    )
    malformed = object.__new__(protocol.ReportRetainedObjectLocationReply)
    for name in (
        "object_id", "owner_worker_id", "borrower_worker_id",
        "hold", "descriptor", "status", "error",
    ):
        object.__setattr__(malformed, name, getattr(valid, name))
    object.__setattr__(malformed, "status", "ADDED")
    consumer._borrow_rpc = lambda *_args: malformed
    cancellations: list[object] = []
    consumer._begin_known_grant_cancellation = (
        lambda *args: cancellations.append(args) or True
    )

    state = _LocationReportState(
        _grant(pending, (source,), target=target.node_id),
        consumer.node_address,
        (report,),
    )
    assert not consumer._report_granted_dependency_locations(
        pending, pending.spec, (source,), state
    )
    delayed = consumer._submissions.get_nowait()
    assert delayed.ready.location_state is not None
    assert delayed.ready.location_state.reports == (report,)
    assert consumer._is_protocol_unresolved(pending)
    assert cancellations == []
    assert consumer.owner_table.snapshot(
        pending.object_id
    ).state.name == "PENDING"


@pytest.mark.unit
def test_confirmed_owner_death_cancels_grant_without_report_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = _consumer_core()
    live_owner, _, _, live_source = _owner_core(consumer.worker_id)
    dead_owner, _, _, dead_source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (live_source, dead_source))
    for owner, guard in zip((live_owner, dead_owner), guards):
        _install_hold(owner, guard)
    consumer._accepted_task_count = 1
    grant = _grant(
        pending, (live_source, dead_source), target=consumer.node_id
    )
    request = protocol.RequestWorkerLease(
        grant.lease_id, pending.spec.task_id, pending.spec.attempt_id,
        pending.spec.resources, consumer.node_id, consumer.worker_id,
        preferred_node_id=consumer.node_id,
        dependencies=(live_source, dead_source),
    )
    reports = consumer._build_location_reports(
        (live_source, dead_source), grant, guards
    )
    state = _LocationReportState(
        grant, consumer.node_address, reports, lease_request=request
    )
    consumer._mark_protocol_unresolved(
        pending, "location_report_wait", target_node_id=grant.node_id
    )
    consumer.owner_table.install_dead_worker(
        dead_owner.worker_id, "confirmed-owner-death"
    )
    report_calls: list[WorkerID] = []
    cancellation_calls: list[protocol.CancelWorkerLease] = []
    release_calls: list[WorkerID] = []

    def report_location(item):
        report_calls.append(item.guard.owner_worker_id)
        assert item.guard.owner_worker_id == live_owner.worker_id
        return live_owner.report_retained_object_location(item.request)

    def cancel_grant(_address, handler, cancellation):
        assert handler == "cancel_worker_lease"
        cancellation_calls.append(cancellation)
        return protocol.CancelWorkerLeaseReply(
            cancellation.lease_id, cancellation.task_id,
            cancellation.attempt_id, cancellation.requester_node_id,
            cancellation.requester_worker_id,
            protocol.LeaseExecutionState.ABANDONED,
            accepted=True, cancelled=True, released=True,
            scheduling_key=cancellation.scheduling_key,
        )

    def release_guard(guard):
        release_calls.append(guard.owner_worker_id)
        assert guard.owner_worker_id == live_owner.worker_id
        reply = live_owner.release_owned_object_for_task(
            protocol.ReleaseOwnedObjectForTask(
                guard.object_id, guard.owner_worker_id,
                guard.borrower_worker_id, guard.hold,
            )
        )
        return reply.released

    monkeypatch.setattr(
        consumer, "_report_foreign_dependency_location", report_location
    )
    monkeypatch.setattr(consumer, "_rpc", cancel_grant)
    monkeypatch.setattr(
        consumer, "_release_foreign_dependency_guard", release_guard
    )

    assert consumer._execute(
        pending, pending.spec, (live_source, dead_source), location_state=state
    )
    assert report_calls == [live_owner.worker_id]
    assert len(cancellation_calls) == 1
    assert not consumer._is_protocol_unresolved(pending)
    assert consumer._submissions.empty()
    assert consumer.owner_table.snapshot(
        pending.object_id
    ).state.name == "ERROR"

    assert consumer._finish_pending_task(pending)
    assert release_calls == [live_owner.worker_id]
    assert not live_owner.owner_table.snapshot(
        live_source.object_id
    ).retained_tokens
    assert dead_owner.owner_table.snapshot(dead_source.object_id).retained_tokens
    assert consumer._accepted_task_count == 0


@pytest.mark.unit
def test_transport_failure_still_replays_without_owner_death(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = _consumer_core()
    owner, _, _, source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (source,))
    grant = _grant(pending, (source,), target=consumer.node_id)
    reports = consumer._build_location_reports(
        (source,), grant, guards
    )
    state = _LocationReportState(grant, consumer.node_address, reports)
    monkeypatch.setattr(
        consumer, "_report_foreign_dependency_location",
        lambda _report: (_ for _ in ()).throw(
            TransportTimeout("location ACK lost")
        ),
    )

    assert not consumer._report_granted_dependency_locations(
        pending, pending.spec, (source,), state
    )
    delayed = consumer._submissions.get_nowait()
    assert delayed.ready.location_state is not None
    assert delayed.ready.location_state.reports == reports
    assert consumer._is_protocol_unresolved(pending)
    assert not consumer._owner_is_dead(owner.worker_id)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_canonical_report_runtime")
def test_confirmed_death_of_already_acknowledged_owner_cancels_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _DeathFencedReportFixture(monkeypatch, owner_count=2)
    consumer, pending = f.consumer, f.pending
    try:
        # Both owners really record the localized replica. Only the second
        # ACK is lost, leaving the first real receipt in Core's retained state.
        f.lose_report_index = 1
        assert not f.execute()
        state = f.delayed()
        assert [index for index, _, _ in f.reports] == [0, 1]
        assert all(reply.status is protocol.RetainedLocationReportStatus.ADDED
                   for _, _, reply in f.reports)
        assert state.receipts == (f.reports[0][2],)
        assert state.acknowledged_keys == (consumer._foreign_guard_key(f.guards[0]),)
        assert state.terminal_error is None and state.cancellation_reply is None
        assert not state.owner_deaths and not f.cancels and not f.custody_acks
        f.assert_pending()

        death, record = f.install_owner_death(0)
        assert f.take() == ()  # Ordinary replay is already retained above.
        assert consumer._foreign_lineage_registry.owner_death_record(death.worker_id) == record
        assert len(f.service.owner_death_fences.pending_for_owner(death.worker_id)) == 2
        assert consumer.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        seen = []

        def healthy_after_cancel(index):
            assert index == 1
            f.assert_cancelled()
            saved = f.marker().obligation
            assert saved.receipts == state.receipts
            assert saved.owner_deaths == (record,)
            assert isinstance(saved.terminal_error, OwnerDiedError)
            assert saved.cancellation_reply == f.cancels[0][1]
            assert not f.releases and not f.fences
            seen.append(saved.terminal_error)

        f.before_report = healthy_after_cancel
        assert f.execute(state)
        assert len(seen) == len(f.cancels) == len(f.custody_acks) == 1
        assert [index for index, _, _ in f.reports] == [0, 1, 1]
        assert f.reports[1][1] == f.reports[2][1]
        assert f.reports[2][2].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert consumer.owner_table.snapshot(pending.object_id).error is seen[0]
        f.assert_terminal()
        assert f.owners[0].owner_table.snapshot(f.sources[0].object_id) == f.dead_owner_snapshot
        # Cancel revokes execution but is not physical deletion authority.
        assert f.target.object_store.get(f.sources[0].object_id) == f.payloads[0]
        assert f.target.object_store.get(f.sources[1].object_id) == f.payloads[1]
        f.collect()
        assert len(f.transfers) == 6 and len(f.fences) == len(f.drops) == 2
    finally:
        f.close()


@pytest.mark.unit
def test_report_stale_after_epoch_race_never_records_target() -> None:
    borrower = WorkerID.random()
    owner, object_id, attempt, source = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    owner.owner_table.retain_borrowed_reference_for_task(
        object_id, (borrower, "borrow"), hold
    )
    target = replace(source, node_id=NodeID.random())
    original_add = owner.owner_table.add_location

    def lose_epoch(*args, **kwargs):
        # Models reconstruction winning between the snapshot and final CAS.
        owner.owner_table.mark_lost(object_id, attempt)
        assert owner.owner_table.advance_attempt(
            object_id, expected_attempt=attempt, next_attempt=attempt.next()
        )
        return original_add(*args, **kwargs)

    owner.owner_table.add_location = lose_epoch  # type: ignore[method-assign]
    reply = owner.report_retained_object_location(
        protocol.ReportRetainedObjectLocation(
            object_id, owner.worker_id, borrower, hold, target
        )
    )
    assert reply.status is protocol.RetainedLocationReportStatus.STALE_PRODUCER
    assert target.node_id not in owner.owner_table.snapshot(object_id).locations


@pytest.mark.unit
def test_foreign_grant_never_writes_borrower_owner_table() -> None:
    consumer = _consumer_core()
    owner, _, _, source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (source,))
    grant = _grant(pending, (source,), target=consumer.node_id)
    reports = consumer._build_location_reports(
        (source,), grant, guards
    )
    assert len(reports) == 1
    with pytest.raises(Exception):
        consumer.owner_table.snapshot(source.object_id)


@pytest.fixture
def _no_canonical_report_runtime(monkeypatch):
    """Only the five canonical submission cases use this runtime tripwire."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure foreign report attempted runtime or unmodelled transport")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure reference close attempted to wait"
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
    for module in (core_module, node_module, worker_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport_module, "request", forbidden)


class _ReportReferenceMailbox(SynchronousReferenceMailbox):
    """Extend the pure local mailbox with one actual borrowed-release drive."""

    def enqueue_internal(self, event):
        if type(event) is not _ReleaseBorrowedReference:
            return super().enqueue_internal(event)
        assert event.scheduled_round is None and event.done is not None
        core = self.core_reference()
        assert core is not None
        try:
            assert core._drive_borrowed_reference_release(event.key)
        finally:
            event.done.set()
        return True


class _CanonicalReportFixture:
    """One normal consumer Task, two 1-KiB Nodes, one tiny stored put input.

    A real export pin/Acquire bootstraps a borrowed Python handle, then normal
    submission installs its TaskID-scoped foreign lineage. Closing that input
    handle cannot release the retained task credential. Node pull has exactly
    three in-process pin/chunk/release calls and one actual grant. One replay
    may complete selected INLINE output through real Prepare/Complete/adoption.
    No function execution, thread, socket, timer or live coordinator runs.
    """

    def __init__(self, monkeypatch):
        self.consumer, self.owner = consumer, owner = make_pure_core(), make_pure_core()
        consumer._reference_mailbox = _ReportReferenceMailbox(consumer)
        self.source_node, self.target = _pure_report_node(), _pure_report_node()
        self.source_address, self.target_address = ("report-source.invalid", 1), ("report-target.invalid", 2)
        self.gcs_address = ("report-control.invalid", 3)
        owner.node_id, owner.node_address = self.source_node.node_id, self.source_address
        owner.owner_address = ("report-owner.invalid", 4)
        consumer.node_id, consumer.node_address = self.target.node_id, self.target_address
        consumer.gcs_address = self.gcs_address
        consumer._registered_functions = set()
        self.registry = NodeRegistry()
        for node, address in ((self.source_node, self.source_address), (self.target, self.target_address)):
            node._object_manager = ObjectManager(node.node_id, node.object_store)
            assert self.registry.register(node.node_id, address, node.resource_ledger.total, node_pid=node._node_pid)
            node._registration_epoch = self.registry.get(node.node_id).registration_epoch
        self.target._registered_with_gcs = True
        self.workers = WorkerRegistry(self.registry)
        self.target._cluster_addresses = {self.source_node.node_id: self.source_address}
        self.transfers, self.calls, self.reports, self.releases, self.pushes = [], [], [], [], []
        self.custody_acks, self.cancels, self.abandons, self.drops = [], [], [], []
        self.borrow_calls = []
        self.report_losses = 0
        self.fail_push = False
        self.publication = self.envelope = None
        self.owner_ref = self.borrowed = self.output = None
        owner._rpc = self.owner_rpc
        owner._resolve_node_address = self.resolve
        consumer._rpc, consumer._borrow_rpc, consumer._push_task_rpc = self.rpc, self.borrow_rpc, self.push_rpc
        self.payload = cloudpickle.dumps({"blob": b"tiny-input"})
        assert len(self.payload) <= 128
        self.owner_ref = owner._put_serialized(self.payload, force_object_store=True)
        # The temporary handoff is an explicit input fixture, not a result
        # publication backend. Its rollback occurs only after real Acquire.
        session = ReferenceExportSession(
            owner.worker_id, owner.owner_address, pin=owner.owner_table.add_contained_reference,
            unpin=owner.request_export_pin_release,
        )
        exported = session.export(self.owner_ref)
        try:
            self.borrowed = consumer._restore_borrowed_reference(*exported)
        finally:
            session.rollback()
        assert not owner.owner_table.snapshot(self.owner_ref.object_id).contained_holds
        self.pending, self.output = consumer._register_submission(
            consumer.define_remote_function(lambda value: "done"),
            (self.borrowed,), {}, ResourceVector({"CPU": 1}), _enqueue=True,
        )
        assert self.take() == (self.pending,)
        (self.guard,) = self.pending.foreign_dependency_guards
        record = consumer._foreign_lineage_registry.snapshot(self.pending.task_id)
        assert record is not None and record.output_ids == self.pending.output_ids
        assert len(record.edges) == 1 and record.edges[0].hold == self.guard.hold
        assert record.edges[0].dependency_object_id == self.owner_ref.object_id
        assert self.guard.hold == _retained_hold(consumer.worker_id, self.pending.spec.attempt_id)
        self.borrowed.close(timeout=0)
        assert not owner.owner_table.snapshot(self.owner_ref.object_id).borrowed_tokens
        assert not consumer._borrowed_release_obligations
        self.assert_held()
        prepared, dependencies, _ = consumer._prepare_task_dependencies(
            self.pending.spec, self.pending.foreign_dependency_guards,
        )
        assert prepared == self.pending.spec and len(dependencies) == 1
        self.source = dependencies[0]
        assert self.source.object_id == self.owner_ref.object_id
        monkeypatch.setattr(node_module, "rpc_request", self.transfer)
        self.request = protocol.RequestWorkerLease(
            LeaseID.random(), self.pending.task_id, self.pending.spec.attempt_id,
            self.pending.spec.resources, consumer.node_id, consumer.worker_id,
            preferred_node_id=consumer.node_id, target_node_id=consumer.node_id,
            dependencies=dependencies, return_ids=self.pending.output_ids,
            dependency_owner_routes=consumer._dependency_owner_routes(self.pending, dependencies),
        )
        self.grant = self.target._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease
        self.inventory = self.target._lease_dependency_custody.snapshot(self.request.lease_id)
        assert self.inventory == protocol.LeaseDependencyInventory(self.request, self.target.node_id, self.grant.dependencies)
        assert len(self.transfers) == 3 and self.target._lease_dependency_custody.has_pending()
        assert self.source_node.object_store.snapshot(self.source.object_id).pin_count == 0
        assert self.target.object_store.snapshot(self.source.object_id).pin_count == 1
        assert self.target.object_store.get(self.source.object_id) == self.payload
        self.lease_state = _LeaseRequestState(self.request, self.target_address, self.target.node_id, False)

    def resolve(self, node_id):
        assert node_id in (self.source_node.node_id, self.target.node_id)
        return self.source_address if node_id == self.source_node.node_id else self.target_address

    def owner_rpc(self, address, handler, request):
        node = self.source_node if address == self.source_address else self.target
        assert address == self.resolve(node.node_id)
        if handler == "seal_object":
            assert node is self.source_node and self.owner_ref is None
            return node._handle_seal_object(request)
        assert handler == "drop_object_replica" and len(self.drops) < 2
        reply = node._handle_drop_object_replica(request)
        self.drops.append((request, reply))
        return reply

    def transfer(self, address, handler, request, **options):
        assert address == self.source_address and len(self.transfers) < 3
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        methods = {
            node_module.PIN_OBJECT_HANDLER: self.source_node._handle_pin_object_for_transfer,
            node_module.GET_OBJECT_CHUNK_HANDLER: self.source_node._handle_get_object_chunk,
            node_module.RELEASE_OBJECT_PIN_HANDLER: self.source_node._handle_release_object_pin,
        }
        assert handler in methods
        self.transfers.append((handler, request))
        return methods[handler](request)

    def borrow_rpc(self, address, handler, request):
        owner, consumer = self.owner, self.consumer
        assert address == owner.owner_address
        assert request.owner_worker_id == owner.worker_id and request.borrower_worker_id == consumer.worker_id
        assert request.object_id == self.owner_ref.object_id
        self.borrow_calls.append((handler, request))
        assert len(self.borrow_calls) <= 9
        if handler == "report_retained_object_location":
            assert request.hold == self.guard.hold and request.descriptor == self.grant.dependencies[0]
            reply = owner.report_retained_object_location(request)
            self.reports.append((request, reply))
            assert len(self.reports) <= 3
            if self.report_losses:
                self.report_losses -= 1
                raise TransportTimeout("actual owner report committed before ACK loss")
            return reply
        if handler == "release_owned_object_for_task":
            # Normal task lineage owns this hold until final output collection.
            assert consumer.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTED
            reply = owner.release_owned_object_for_task(request)
            self.releases.append((request, reply))
            assert len(self.releases) == 1 and request.hold == self.guard.hold
            return reply
        handlers = {
            "acquire_borrowed_object": owner.acquire_exported_reference,
            "release_borrowed_object": owner.release_borrowed_reference,
            "retain_owned_object_for_task": owner.retain_owned_object_for_task,
            "get_retained_owned_object": owner.get_retained_owned_object,
        }
        assert handler in handlers
        return handlers[handler](request)

    def rpc(self, address, handler, request):
        consumer, node = self.consumer, self.target
        self.calls.append((handler, request))
        assert len(self.calls) <= 12
        if handler == "get_worker_deaths":
            assert address == self.gcs_address
            return self.workers.deaths_after(request)
        if handler == output_wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.gcs_address and self.publication is not None
            if type(request) is output_wire.ReportOutputPublicationTerminal:
                assert request.witness == self.envelope.complete
                ack = self.publication.recovery.report_terminal(request.witness)
            elif type(request) is output_wire.ReportOutputPublicationAdopted:
                assert consumer.owner_table.output_owner_publication_receipt(
                    OutputOwnerPublicationPlan(self.pending.execution, self.envelope),
                ).committed
                ack = self.publication.recovery.report_adopted(request.proof)
            else:
                assert type(request) is output_wire.ReportOutputPublicationSlotCollected
                assert consumer.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTING
                ack = self.publication.recovery.report_slot_collected(request.proof)
            return output_wire.OutputRecoveryReply(request, ack)
        assert address == self.target_address
        if handler == "request_worker_lease":
            assert request == self.request and len(self.transfers) == 3
            return node._handle_request_lease(request)
        if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
            assert request.inventory == self.inventory and request.requester_worker_id == consumer.worker_id
            state = consumer._protocol_unresolved[self.pending.task_key].obligation
            assert isinstance(state, _LocationReportState) and state.inventory == self.inventory
            assert len(state.receipts) == 1 and state.receipts[0].custody_transferred
            reply = node._handle_ack_lease_dependency_custody(request)
            self.custody_acks.append((request, reply))
            assert len(self.custody_acks) <= 2 and reply.accepted
            return reply
        if handler == "cancel_worker_lease":
            assert request.lease_request == self.request
            reply = node._handle_cancel_worker_lease(request)
            self.cancels.append((request, reply))
            assert len(self.cancels) == 1 and reply.accepted and reply.cancelled and reply.released
            assert reply.retired_grant == self.grant and reply.dependency_inventory == self.inventory
            return reply
        if handler == "release_worker_lease":
            reply = node._handle_release_lease(request)
            self.abandons.append((request, reply))
            assert len(self.abandons) == 1 and reply.released
            return reply
        assert handler == output_wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert self.publication.recovery.snapshot(self.envelope.publication_id).adopted == request.proof
        return node._handle_ack_output_publication_adopted(request)

    def push_rpc(self, address, handler, push):
        assert address == self.grant.worker_address and handler == "push_task"
        assert push.lease_id == self.grant.lease_id and push.spec == self.pending.spec
        assert push.dependencies == self.grant.dependencies and not self.pushes
        assert not self.target._lease_dependency_custody.has_pending()
        self.pushes.append(push)
        if self.fail_push:
            raise TransportConnectionError("worker never reached")
        started = self.target._handle_start_worker_lease(protocol.StartWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
        ))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        self.publication = prepare_ref_free_output(
            self.target, self.request, self.grant, job_id=self.consumer.job_id, values=("done",),
        )
        complete = self.target._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
        ))
        assert complete.accepted and complete.released and complete.state is protocol.LeaseExecutionState.COMPLETED
        self.envelope = complete.output_publication
        assert self.envelope is not None and self.envelope.manifest == self.publication.manifest
        return protocol.TaskReply(push.spec.task_id, push.spec.attempt_id, push.worker_id,
                                  protocol.TaskReplyStatus.SUCCEEDED, self.envelope.results,
                                  output_publication=self.envelope)

    def take(self):
        size = self.consumer._submissions.qsize()
        assert size <= 8
        work = []
        for _ in range(size):
            item = self.consumer._submissions.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) in (_PendingTask, _DelayedReadyTask) or item is _STOP
                    work.append(item)
            finally:
                self.consumer._submissions.task_done()
        assert self.consumer._submissions.empty() and self.consumer._submissions.unfinished_tasks == 0
        return tuple(work)

    def execute(self, state=None):
        options = {"lease_state": self.lease_state} if state is None else {"location_state": state}
        return self.consumer._execute(self.pending, self.pending.spec, (self.source,), **options)

    def assert_held(self):
        assert self.owner.owner_table.has_retained_reference_for_task(self.owner_ref.object_id, self.guard.hold)
        assert not self.releases

    def collect(self):
        consumer, node = self.consumer, self.target
        assert not consumer._protocol_unresolved
        assert node._leases[self.grant.lease_id].state in (
            protocol.LeaseExecutionState.COMPLETED, protocol.LeaseExecutionState.ABANDONED,
        )
        assert not node._lease_dependency_custody.has_pending()
        assert consumer._finish_pending_task(self.pending) and consumer._finish_pending_task(self.pending)
        assert consumer._accepted_task_count == 0 and not consumer._task_finish_barriers
        self.assert_held()
        self.output.close(timeout=0)
        consumer._reference_mailbox.drain()
        assert len(self.releases) == 1 and self.releases[0][1].released
        assert not self.owner.owner_table.snapshot(self.source.object_id).retained_tokens
        assert consumer.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTED
        assert consumer._recovery.lineage_for_object(self.pending.object_id) is None
        assert not consumer._foreign_lineage_runtime.has_pending_obligations()
        assert not consumer._foreign_lineage_collection_receipts and not consumer._object_gc_obligations
        assert not consumer._objects and not consumer._stored_descriptors
        if self.publication is not None:
            identity = self.envelope.publication_id
            snapshot = self.publication.recovery.snapshot(identity)
            assert snapshot.adopted is not None and len(snapshot.slot_collections) == 1
            assert not self.publication.journal.snapshot(identity).retained_result_slots
            assert self.publication.adapter.report_terminal(identity)
        # The live owner's own input handle keeps both reported replicas alive
        # until normal owner GC; consumer failure is not deletion authority.
        assert node.object_store.get(self.source.object_id) == self.payload
        self.owner_ref.close(timeout=0)
        self.owner._reference_mailbox.drain()
        assert len(self.drops) == 2 and all(reply.dropped for _, reply in self.drops)
        assert node.object_store.used_bytes == self.source_node.object_store.used_bytes == 0
        assert not self.owner._objects and not self.owner._object_gc_obligations
        assert node.resource_ledger.available == node.resource_ledger.total
        assert self.take() == ()

    def close(self):
        # Failure cleanup releases only actual handles. It does not forge a
        # terminal task, clear custody, or retire an outstanding remote grant.
        for ref in (self.borrowed, self.output, self.owner_ref):
            if ref is not None:
                ref.close(timeout=0)
        close_pure_core(self.consumer)
        close_pure_core(self.owner)


class _UnboundReportControlEndpoint:
    """One inert TCP-constructor boundary; no GCS authority is replaced."""

    def __init__(self, handlers, **options):
        assert options["host"] == "127.0.0.1" and options["port"] == 0
        self.handlers = dict(handlers)
        assert all(callable(handler) for handler in self.handlers.values())
        self.address = ("death-report-control.invalid", 3)
        self.is_running = False

    def start(self):
        pytest.fail("pure report GCS cannot start a TCP endpoint")

    def stop(self):
        pytest.fail("pure report GCS has no TCP runtime to stop")


class _DeathFencedReportFixture:
    """Canonical foreign lineage with one explicitly modelled owner death.

    One consumer, one or two owners, two 1-KiB Nodes and real GCS authorities.
    GCS normally binds TCP during construction; only that endpoint constructor
    is inert here. No server, thread or socket is created, started or stopped.
    Each <=128-byte put uses real export/Acquire/Retain, three source-transfer
    callbacks and one shared actual grant. No Worker executes the function.
    Exactly two real GCS fence effects delete the dead owner's source/target
    replicas after Cancel unpins them. Assertions retain the dead Core's
    pre-death image; final fixture fencing only releases test-local handles,
    leaving its retained token and queued GC unprocessed. It is not a live
    endpoint or a claim of successful normal owner shutdown.

    Fault controls are local to these two cases. STALE is deliberately a typed
    protocol fault, not fabricated producer history; every cancellation and
    custody ACK still comes from its actual Node handler. All work is finite.
    The existing single-owner canonical fixture and its defaults are separate.
    """

    def __init__(self, monkeypatch, *, owner_count):
        assert owner_count in (1, 2)
        self.consumer = consumer = make_pure_core()
        consumer._reference_mailbox = _ReportReferenceMailbox(consumer)
        self.owners = tuple(make_pure_core() for _ in range(owner_count))
        self.source_node, self.target = _pure_report_node(), _pure_report_node()
        self.source_address = ("death-report-source.invalid", 1)
        self.target_address = ("death-report-target.invalid", 2)
        self.gcs_address = ("death-report-control.invalid", 3)
        with monkeypatch.context() as endpoint_boundary:
            endpoint_boundary.setattr(control_module, "TCPServer", _UnboundReportControlEndpoint)
            self.service = GCSLite(stored_hold_rpc=self.fence_rpc)
        assert type(self.service._server) is _UnboundReportControlEndpoint
        for node, address in ((self.source_node, self.source_address),
                              (self.target, self.target_address)):
            node._object_manager = ObjectManager(node.node_id, node.object_store)
            reply = self.service.register_node(protocol.RegisterNode(
                node.node_id, node._node_pid, address, node.resource_ledger.total,
            ))
            assert reply.accepted
            node._registration_epoch = reply.registration_epoch
        self.target._registered_with_gcs = True
        self.target._cluster_addresses = {self.source_node.node_id: self.source_address}
        consumer.node_id, consumer.node_address = self.target.node_id, self.target_address
        consumer.gcs_address = self.gcs_address
        consumer._registered_functions = set()
        self.transfers, self.calls, self.borrow_calls = [], [], []
        self.transfer_closes = []
        self.reports, self.protocol_faults, self.releases = [], [], []
        self.cancels, self.custody_acks, self.fences, self.drops, self.seals = [], [], [], [], []
        self.lease_queries = self.death_queries = 0
        self.lose_report_index = None
        self.report_ack_lost = False
        self.inject_stale = False
        self.lose_cancel_ack = False
        self.cancel_ack_lost = False
        self.before_report = None
        self.death = self.dead_index = self.dead_owner_snapshot = None
        self.output = None
        self.owner_refs, self.borrowed, self.incarnations, self.payloads = [], [], [], []
        consumer._rpc, consumer._borrow_rpc = self.rpc, self.borrow_rpc
        consumer._push_task_rpc = self.no_push
        source_info = self.service.nodes.get(self.source_node.node_id)
        for index, owner in enumerate(self.owners):
            owner.node_id, owner.node_address = self.source_node.node_id, self.source_address
            owner.owner_address = ("death-report-owner-{}.invalid".format(index), 10 + index)
            owner._rpc, owner._resolve_node_address = self.owner_rpc, self.resolve
            incarnation = protocol.WorkerIncarnation(
                self.source_node.node_id, source_info.node_pid, source_info.registration_epoch,
                owner.worker_id, 1811 + index,
            )
            assert self.service.register_worker_incarnation(
                protocol.RegisterWorkerIncarnation(incarnation),
            ).accepted
            self.incarnations.append(incarnation)
            payload = cloudpickle.dumps({"blob": b"tiny-owner-input", "owner": index})
            assert len(payload) <= 128
            self.payloads.append(payload)
            ref = owner._put_serialized(payload, force_object_store=True)
            self.owner_refs.append(ref)
            assert owner._recovery.lineage_for_object(ref.object_id) is None
            session = ReferenceExportSession(
                owner.worker_id, owner.owner_address, pin=owner.owner_table.add_contained_reference,
                unpin=owner.request_export_pin_release,
            )
            exported = session.export(ref)
            try:
                self.borrowed.append(consumer._restore_borrowed_reference(*exported))
            finally:
                session.rollback()
            assert not owner.owner_table.snapshot(ref.object_id).contained_holds
        self.pending, self.output = consumer._register_submission(
            consumer.define_remote_function(lambda *values: "not executed"),
            tuple(self.borrowed), {}, ResourceVector({"CPU": 1}), _enqueue=True,
        )
        assert self.take() == (self.pending,)
        by_id = {guard.object_id: guard for guard in self.pending.foreign_dependency_guards}
        self.guards = tuple(by_id[ref.object_id] for ref in self.owner_refs)
        assert len(by_id) == owner_count
        self.lineage = consumer._foreign_lineage_registry.snapshot(self.pending.task_id)
        assert self.lineage is not None and self.lineage.output_ids == self.pending.output_ids
        assert len(self.lineage.edges) == owner_count
        for guard in self.guards:
            assert guard.hold == _retained_hold(consumer.worker_id, self.pending.spec.attempt_id)
            edge = next(edge for edge in self.lineage.edges if edge.dependency_object_id == guard.object_id)
            assert (edge.owner_worker_id, edge.borrower_worker_id, edge.hold) == (
                guard.owner_worker_id, consumer.worker_id, guard.hold,
            )
            assert edge.owner_address == guard.owner_address and edge.task_id == self.pending.task_id
            assert edge.roles.name == "TOP_LEVEL"
        assert self.pending.spec.num_returns == 1 and self.pending.protected_dependencies == ()
        assert not self.pending.nested_foreign_guards and not self.pending.nested_local_holds
        for ref in self.borrowed:
            ref.close(timeout=0)
        assert not consumer._borrowed_release_obligations
        self.assert_held()
        prepared, self.sources, _ = consumer._prepare_task_dependencies(
            self.pending.spec, self.pending.foreign_dependency_guards,
        )
        assert prepared == self.pending.spec and len(self.sources) == owner_count
        assert tuple(item.object_id for item in self.sources) == tuple(ref.object_id for ref in self.owner_refs)
        monkeypatch.setattr(node_module, "rpc_request", self.transfer)
        self.request = protocol.RequestWorkerLease(
            LeaseID.random(), self.pending.task_id, self.pending.spec.attempt_id,
            self.pending.spec.resources, consumer.node_id, consumer.worker_id,
            preferred_node_id=consumer.node_id, target_node_id=consumer.node_id,
            dependencies=self.sources, return_ids=self.pending.output_ids,
            dependency_owner_routes=consumer._dependency_owner_routes(self.pending, self.sources),
        )
        self.grant = self.target._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease
        self.inventory = self.target._lease_dependency_custody.snapshot(self.request.lease_id)
        assert self.inventory == protocol.LeaseDependencyInventory(
            self.request, self.target.node_id, self.grant.dependencies,
        )
        assert self.target._lease_dependency_custody.has_pending()
        assert len(self.transfers) == 3 * owner_count and len(self.seals) == owner_count
        for descriptor, payload in zip(self.sources, self.payloads):
            assert self.source_node.object_store.snapshot(descriptor.object_id).pin_count == 0
            assert self.target.object_store.snapshot(descriptor.object_id).pin_count == 1
            assert self.target.object_store.get(descriptor.object_id) == payload
        self.lease_state = _LeaseRequestState(self.request, self.target_address, self.target.node_id, False)

    def resolve(self, node_id):
        assert node_id in (self.source_node.node_id, self.target.node_id)
        return self.source_address if node_id == self.source_node.node_id else self.target_address

    def owner_rpc(self, address, handler, request):
        node = self.source_node if address == self.source_address else self.target
        assert address == self.resolve(node.node_id)
        if handler == "seal_object":
            assert node is self.source_node and len(self.seals) < len(self.owners)
            reply = node._handle_seal_object(request)
            self.seals.append((request, reply))
            assert reply.sealed
            return reply
        assert handler == "drop_object_replica" and len(self.drops) < 2
        assert self.death is not None and request.owner_worker_id != self.death.worker_id
        reply = node._handle_drop_object_replica(request)
        self.drops.append((request, reply))
        assert reply.dropped
        return reply

    def transfer(self, address, handler, request, **options):
        assert address == self.source_address and len(self.transfers) < 3 * len(self.owners)
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        methods = {
            node_module.PIN_OBJECT_HANDLER: self.source_node._handle_pin_object_for_transfer,
            node_module.GET_OBJECT_CHUNK_HANDLER: self.source_node._handle_get_object_chunk,
            node_module.RELEASE_OBJECT_PIN_HANDLER: self.source_node._handle_release_object_pin,
        }
        assert handler in methods
        self.transfers.append((handler, request))
        reply = methods[handler](request)
        if handler == node_module.RELEASE_OBJECT_PIN_HANDLER:
            assert type(reply) is protocol.ReleaseObjectPinReply
            assert reply.accepted and reply.released and reply.error is None
            assert (reply.transfer_id, reply.object_id, reply.node_id) == (
                request.transfer_id, request.object_id, self.source_node.node_id,
            )
            self.transfer_closes.append((request, reply))
            assert len(self.transfer_closes) <= len(self.owners)
        return reply

    def borrow_rpc(self, address, handler, request):
        index = next(index for index, owner in enumerate(self.owners) if owner.owner_address == address)
        owner, consumer = self.owners[index], self.consumer
        assert not consumer._owner_is_dead(owner.worker_id), "contacted a confirmed-dead owner"
        assert request.owner_worker_id == owner.worker_id and request.borrower_worker_id == consumer.worker_id
        assert request.object_id == self.owner_refs[index].object_id
        self.borrow_calls.append((index, handler, request))
        assert len(self.borrow_calls) <= 16
        if handler == "report_retained_object_location":
            assert request.hold == self.guards[index].hold and request.descriptor == self.grant.dependencies[index]
            if self.before_report is not None:
                self.before_report(index)
            if self.inject_stale:
                assert len(self.owners) == 1 and not self.protocol_faults and not self.reports
                # Negative wire-boundary stimulus only. This put never changes
                # epoch; do not attribute this reply to its real owner reducer.
                reply = protocol.ReportRetainedObjectLocationReply(
                    request.object_id, request.owner_worker_id, request.borrower_worker_id,
                    request.hold, request.descriptor, protocol.RetainedLocationReportStatus.STALE_PRODUCER,
                    "explicit noncustody STALE protocol fault",
                )
                self.protocol_faults.append((request, reply))
                return reply
            reply = owner.report_retained_object_location(request)
            self.reports.append((index, request, reply))
            assert len(self.reports) <= 3
            if index == self.lose_report_index and not self.report_ack_lost:
                assert reply.accepted and reply.custody_transferred
                self.report_ack_lost = True
                raise TransportTimeout("real location report committed before ACK loss")
            return reply
        if handler == "release_owned_object_for_task":
            assert consumer.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTED
            assert request.hold == self.guards[index].hold and not self.releases
            reply = owner.release_owned_object_for_task(request)
            self.releases.append((index, request, reply))
            assert reply.accepted and reply.released
            return reply
        methods = {
            "acquire_borrowed_object": owner.acquire_exported_reference,
            "release_borrowed_object": owner.release_borrowed_reference,
            "retain_owned_object_for_task": owner.retain_owned_object_for_task,
            "get_retained_owned_object": owner.get_retained_owned_object,
        }
        assert handler in methods
        return methods[handler](request)

    def rpc(self, address, handler, request):
        self.calls.append((handler, request))
        assert len(self.calls) <= 8
        if handler == "get_worker_deaths":
            assert address == self.gcs_address
            self.death_queries += 1
            assert self.death_queries == 1
            return self.service.get_worker_deaths(request)
        assert address == self.target_address
        if handler == "request_worker_lease":
            self.lease_queries += 1
            assert self.lease_queries == 1 and request == self.request
            assert len(self.transfers) == 3 * len(self.owners)
            return self.target._handle_request_lease(request)
        if handler == "cancel_worker_lease":
            assert request == protocol.CancelWorkerLease(
                self.request.lease_id, self.request.task_id, self.request.attempt_id,
                self.request.requester_node_id, self.request.requester_worker_id, self.request.scheduling_key,
                lease_request=self.request,
            )
            reply = self.target._handle_cancel_worker_lease(request)
            self.cancels.append((request, reply))
            assert len(self.cancels) <= 2 and reply.accepted and reply.cancelled
            assert reply.released is (len(self.cancels) == 1)
            assert reply.retired_grant == self.grant and reply.dependency_inventory == self.inventory
            if self.lose_cancel_ack and not self.cancel_ack_lost:
                self.cancel_ack_lost = True
                raise TransportTimeout("real exact Cancel committed before ACK loss")
            return reply
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert request == protocol.AckLeaseDependencyCustody(self.consumer.worker_id, self.inventory)
        state = self.marker().obligation
        assert state.cancellation_reply is not None and not self.custody_acks
        for report in state.reports:
            custody = any(reply.owner_worker_id == report.guard.owner_worker_id
                          and reply.object_id == report.guard.object_id and reply.hold == report.guard.hold
                          and reply.custody_transferred for reply in state.receipts)
            delegated = any(record.worker_id == report.guard.owner_worker_id for record in state.owner_deaths)
            assert custody or delegated, "inventory ACK preceded custody or exact death authority"
        reply = self.target._handle_ack_lease_dependency_custody(request)
        self.custody_acks.append((request, reply, state))
        assert reply.accepted and reply.request == request
        return reply

    def no_push(self, *_args, **_kwargs):
        pytest.fail("failed dependency handoff reached user execution")

    def marker(self):
        marker = self.consumer._protocol_unresolved[self.pending.task_key]
        state = marker.obligation
        assert type(state) is _LocationReportState
        assert state.grant == self.grant and state.lease_request == self.request
        assert state.inventory == self.inventory
        return marker

    def take(self):
        size = self.consumer._submissions.qsize()
        assert size <= 8
        result = []
        for _ in range(size):
            item = self.consumer._submissions.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) in (_PendingTask, _DelayedReadyTask)
                    result.append(item)
            finally:
                self.consumer._submissions.task_done()
        assert self.consumer._submissions.empty() and self.consumer._submissions.unfinished_tasks == 0
        return tuple(result)

    def delayed(self):
        (delayed,) = self.take()
        assert type(delayed) is _DelayedReadyTask and delayed.ready.cancellation is None
        assert delayed.ready.location_state == self.marker().obligation
        return delayed.ready.location_state

    def execute(self, state=None):
        options = {"lease_state": self.lease_state} if state is None else {"location_state": state}
        return self.consumer._execute(self.pending, self.pending.spec, self.sources, **options)

    def assert_held(self):
        assert not self.releases
        for owner, ref, guard in zip(self.owners, self.owner_refs, self.guards):
            snapshot = owner.owner_table.snapshot(ref.object_id)
            assert snapshot.retained_tokens == frozenset((guard.hold,))
            assert not snapshot.borrowed_tokens and not snapshot.contained_holds
        assert self.consumer._foreign_lineage_registry.snapshot(self.pending.task_id) == self.lineage

    def assert_pending(self):
        self.assert_held()
        consumer = self.consumer
        assert consumer.owner_table.snapshot(self.pending.object_id).state is ObjectState.PENDING
        assert consumer._accepted_task_count == 1 and not consumer._finish_pending_task(self.pending)
        assert consumer._task_finish_barriers[self.pending.object_id] == self.pending
        assert self.target._lease_dependency_custody.has_pending() and not self.custody_acks
        assert consumer._recovery.task_record(self.pending.task_id).retries_started == 0

    def assert_cancelled(self):
        assert self.cancels
        assert self.target._leases[self.grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        assert all(self.target.object_store.snapshot(item.object_id).pin_count == 0 for item in self.sources)
        assert all(request == self.cancels[0][0] for request, _ in self.cancels)

    def install_owner_death(self, index):
        assert self.death is None and self.dead_index is None
        self.dead_index = index
        self.dead_owner_snapshot = self.owners[index].owner_table.snapshot(self.owner_refs[index].object_id)
        reply = self.service.report_worker_death(protocol.ReportWorkerDeath(
            "canonical-report-owner-exit", self.incarnations[index], -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        ))
        assert reply.disposition is protocol.WorkerDeathDisposition.APPLIED and reply.death is not None
        self.death = reply.death
        assert self.consumer._sync_worker_deaths()
        record = self.consumer.owner_table.dead_worker_record(self.death.worker_id)
        assert record is not None and self.consumer._worker_death_cursor == self.death.death_epoch
        assert self.consumer._foreign_lineage_registry.owner_death_record(self.death.worker_id) == record
        assert self.owners[index].owner_table.snapshot(self.owner_refs[index].object_id) == self.dead_owner_snapshot
        return self.death, record

    def assert_terminal(self):
        consumer = self.consumer
        assert consumer.owner_table.snapshot(self.pending.object_id).state is ObjectState.ERROR
        record = consumer._recovery.task_record(self.pending.task_id)
        assert record.state is TaskState.SYSTEM_FAILED and record.retries_started == 0
        assert record.current_attempt == self.pending.spec.attempt_id
        assert not consumer._protocol_unresolved and len(self.custody_acks) == 1
        assert not self.target._lease_dependency_custody.has_pending()
        self.assert_cancelled()
        self.assert_held()

    def fence_rpc(self, address, handler, request):
        assert handler == node_module.INSTALL_OWNER_DEATH_FENCE_HANDLER and len(self.fences) < 2
        assert self.death is not None and request.owner_death == self.death
        assert request.scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
        node = self.source_node if address == self.source_address else self.target
        assert address == self.resolve(node.node_id) and request.node_id == node.node_id
        assert any(effect.request == request for effect in self.service.owner_death_fences.pending_for_owner(self.death.worker_id))
        dead_id = self.sources[self.dead_index].object_id
        assert node.object_store.snapshot(dead_id).pin_count == 0
        reply = node._handle_install_owner_death_fence(request)
        self.fences.append((request, reply))
        assert reply.request == request and reply.accepted and reply.complete
        assert len(reply.observations) == 1
        assert reply.observations[0].descriptor == replace(self.sources[self.dead_index], node_id=node.node_id)
        assert reply.observations[0].status is protocol.OwnerDeathReplicaStatus.ABSENT
        assert not node.object_store.contains(dead_id, sealed_only=False)
        return reply

    def collect(self):
        consumer = self.consumer
        self.assert_terminal()
        assert consumer._finish_pending_task(self.pending) and consumer._finish_pending_task(self.pending)
        assert consumer._accepted_task_count == 0 and not consumer._task_finish_barriers
        self.assert_held()  # Canonical input lineage outlives execution finish.
        self.output.close(timeout=0)
        consumer._reference_mailbox.drain()
        assert consumer.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTED
        assert consumer._recovery.lineage_for_object(self.pending.object_id) is None
        assert not consumer._foreign_lineage_runtime.has_pending_obligations()
        assert not consumer._foreign_lineage_collection_receipts and not consumer._object_gc_obligations
        assert not consumer._objects and not consumer._stored_descriptors
        live_indices = tuple(index for index in range(len(self.owners)) if index != self.dead_index)
        assert tuple(index for index, _, _ in self.releases) == live_indices
        dead_ref = self.owner_refs[self.dead_index]
        assert self.owners[self.dead_index].owner_table.snapshot(dead_ref.object_id) == self.dead_owner_snapshot
        effects = self.service.owner_death_fences.pending_for_owner(self.death.worker_id)
        assert len(effects) == 2 and not self.fences
        assert {effect.key.target.node_id for effect in effects} == {self.source_node.node_id, self.target.node_id}
        for effect in effects:
            assert self.service._drive_owner_death_fence(effect)
        outbox = self.service.owner_death_fences.snapshot()
        assert not outbox.pending and len(outbox.completed) == 2
        assert {completion.effect for completion in outbox.completed} == set(effects)
        assert all(completion.reply is not None and completion.reply.complete for completion in outbox.completed)
        assert self.owners[self.dead_index].owner_table.snapshot(dead_ref.object_id) == self.dead_owner_snapshot
        for index in live_indices:
            owner, ref, source = self.owners[index], self.owner_refs[index], self.sources[index]
            assert not owner.owner_table.snapshot(ref.object_id).retained_tokens
            assert owner.owner_table.snapshot(ref.object_id).locations == frozenset((self.source_node.node_id, self.target.node_id))
            # The dead owner's sweep cannot delete another owner's replicas.
            assert self.source_node.object_store.get(source.object_id) == self.payloads[index]
            assert self.target.object_store.get(source.object_id) == self.payloads[index]
            ref.close(timeout=0)
            owner._reference_mailbox.drain()
            assert owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED
            assert not owner._objects and not owner._object_gc_obligations and not owner._stored_descriptors
        assert len(self.drops) == 2 * len(live_indices)
        # Source transfer records are replay tombstones, not live pins. The
        # real Node intentionally retains released/closed identities; deleting
        # that history would permit a delayed Pin to acquire again.
        assert len(self.transfer_closes) == len(self.owners)
        source_sessions = self.source_node._pinned_transfers
        source_closes = self.source_node._closed_transfer_pins
        assert set(source_sessions) == set(source_closes) == {
            request.transfer_id for request, _ in self.transfer_closes
        }
        for request, reply in self.transfer_closes:
            session = source_sessions[request.transfer_id]
            closed = source_closes[request.transfer_id]
            assert session.acquired and session.released and session.closing
            assert session.descriptor.object_id == request.object_id
            assert session.requester_node_id == request.requester_node_id == self.target.node_id
            assert closed.closed and closed.request == request
            assert reply.accepted and reply.released
        assert not self.target._pinned_transfers
        assert not getattr(self.target, "_closed_transfer_pins", {})
        for node in (self.source_node, self.target):
            assert node.object_store.used_bytes == 0 and not node._sealed_metadata
            assert not node._dependency_pin_cleanups
            assert not node._source_pin_outbox_locked().has_pending()
            assert node.resource_ledger.available == node.resource_ledger.total
        assert not self.target._lease_dependency_custody.has_pending()
        assert len(self.target._leases) == 1 and self.target._leases[self.grant.lease_id].completion is None
        assert self.lease_queries == 1 and self.death_queries == 1
        assert len(self.calls) == 3 + len(self.cancels)
        assert len(self.borrow_calls) == (12 if len(self.owners) == 2 else 5)
        assert not self.service.is_running and self.service._owner_death_progress_thread is None
        assert self.take() == ()

    def close(self):
        # Failure fencing only: close the actual Python handles, never call
        # normal GC on the dead owner's image or erase its retained credential.
        for ref in (*self.borrowed, self.output, *self.owner_refs):
            if ref is not None:
                ref.close(timeout=0)
        for core in (self.consumer, *self.owners):
            close_pure_core(core)


def _live_report_fixture() -> tuple[
    CoreWorker, CoreWorker, _PendingTask, _ForeignDependencyGuard,
    protocol.ObjectStoreDescriptor, protocol.GrantWorkerLease, _LeaseRequestState,
]:
    consumer = CoreWorker(
        ("127.0.0.1", 27401), NodeID.random(), event_sink=EventSink(),
        dispatch_lanes=1,
    )
    owner, _, _, source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (source,))
    guard = replace(
        guards[0], borrower_token="borrow"
    )
    _install_hold(owner, guard)
    consumer._borrow_rpc = lambda _address, handler, request: (
        owner.release_owned_object_for_task(request)
        if handler == "release_owned_object_for_task"
        else owner.get_retained_owned_object(request)
        if handler == "get_retained_owned_object"
        else owner.report_retained_object_location(request)
        if handler == "report_retained_object_location"
        else (_ for _ in ()).throw(AssertionError(handler))
    )
    pending = replace(pending, foreign_dependency_guards=(guard,))
    with consumer._state_lock:
        consumer._accepted_task_count += 1
    grant = _grant(pending, (source,), target=consumer.node_id)
    lease_request = protocol.RequestWorkerLease(
        grant.lease_id, pending.spec.task_id, pending.spec.attempt_id,
        pending.spec.resources, consumer.node_id, consumer.worker_id,
        preferred_node_id=consumer.node_id, dependencies=(source,),
    )
    return (
        consumer, owner, pending, guard, source, grant,
        _LeaseRequestState(
            lease_request, consumer.node_address, consumer.node_id, True
        ),
    )


@pytest.mark.unit
@pytest.mark.usefixtures("_no_canonical_report_runtime")
def test_execute_report_ambiguity_never_pushes_or_releases_hold_and_blocks_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _CanonicalReportFixture(monkeypatch)
    consumer, pending = f.consumer, f.pending
    try:
        f.report_losses = 1
        assert not f.execute()
        (delayed,) = f.take()
        assert isinstance(delayed, _DelayedReadyTask)
        assert delayed.ready.location_state is not None
        state = delayed.ready.location_state
        assert state.grant == f.grant and state.lease_request == f.request and state.inventory == f.inventory
        assert not state.receipts and not state.custody_acknowledged
        assert f.pushes == [] and f.releases == [] and f.custody_acks == []
        f.assert_held()
        assert not consumer._finish_pending_task(pending)
        before = consumer.owner_table.snapshot(pending.object_id)
        held = f.owner.owner_table.snapshot(f.source.object_id)
        assert before.state is ObjectState.PENDING
        assert consumer._accepted_task_count == 1
        # Exercise the actual shutdown method's unresolved-protocol early
        # return. There are no lanes to join or waits to advance; this is not
        # public process shutdown or a substitute for its integration gate.
        assert not consumer.shutdown(timeout=0.01)
        assert not consumer.can_finalize_shutdown() and consumer._owner_protocol_open
        assert consumer.owner_table.snapshot(pending.object_id) == before
        assert f.owner.owner_table.snapshot(f.source.object_id) == held
        assert f.pushes == [] and f.releases == [] and f.custody_acks == []
        assert f.take() == (_STOP,)
        assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert f.target.object_store.snapshot(f.source.object_id).pin_count == 1
        # Explicitly cancel the exact known grant and replay its retained
        # report. No local cleanup shortcut may clear this ambiguity.
        error = SystemTaskError("bounded test cancels unresolved handoff")
        assert consumer._begin_known_grant_cancellation(
            pending, pending.spec, (f.source,), f.target_address, f.grant, f.request, error,
        )
        assert len(f.cancels) == len(f.custody_acks) == 1 and f.pushes == []
        assert consumer.owner_table.snapshot(pending.object_id).state is ObjectState.ERROR
        assert consumer.owner_table.snapshot(pending.object_id).error is error
        assert not consumer._protocol_unresolved
        f.collect()
        assert consumer.can_finalize_shutdown()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_canonical_report_runtime")
def test_execute_replays_exact_report_then_pushes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _CanonicalReportFixture(monkeypatch)
    consumer, pending = f.consumer, f.pending
    try:
        f.report_losses = 1
        assert not f.execute()
        (delayed,) = f.take()
        state = delayed.ready.location_state
        assert state is not None and state.grant == f.grant
        assert state.lease_request == f.request and state.inventory == f.inventory
        assert not state.receipts and not f.pushes
        assert len([handler for handler, _ in f.calls if handler == "request_worker_lease"]) == 1
        f.assert_held()
        assert not consumer._finish_pending_task(pending)
        assert f.execute(state)
        assert len(f.reports) == 2 and f.reports[0][0] == f.reports[1][0]
        assert f.reports[0][1].status is protocol.RetainedLocationReportStatus.ADDED
        assert f.reports[1][1].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert len([handler for handler, _ in f.calls if handler == "request_worker_lease"]) == 1
        assert len(f.pushes) == len(f.custody_acks) == 1
        assert f.pushes[0].dependencies == f.grant.dependencies
        assert len(f.transfers) == 3 and len(f.target._leases) == 1
        assert f.grant.node_id in f.owner.owner_table.snapshot(f.source.object_id).locations
        snapshot = consumer.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.READY_INLINE and cloudpickle.loads(snapshot.inline_data) == "done"
        assert snapshot.output_publication is not None
        assert consumer._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert consumer._recovery.task_record(pending.task_id).retries_started == 0
        assert not consumer._protocol_unresolved
        assert consumer._finish_pending_task(pending)
        # Normal submission retains producer lineage beyond execution finish.
        # The old direct fixture incorrectly released its hold at this point.
        f.assert_held()
        f.collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_canonical_report_runtime")
def test_noncustody_stale_is_quarantined_until_authoritative_owner_death(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Formal replacement for the old "Cancel ACK implies hold release" test.
    # The typed STALE is a deliberate negative protocol input, not a claim that
    # a normal put reconstructed or that the live owner's history is stale.
    f = _DeathFencedReportFixture(monkeypatch, owner_count=1)
    consumer, owner, pending = f.consumer, f.owners[0], f.pending
    try:
        before_owner = owner.owner_table.snapshot(f.sources[0].object_id)
        f.inject_stale = f.lose_cancel_ack = True
        assert not f.execute()
        first = f.delayed()
        assert len(f.protocol_faults) == len(f.cancels) == 1 and not f.reports
        stale = f.protocol_faults[0][1]
        assert first.receipts == (stale,) and first.acknowledged_keys == ()
        assert not stale.accepted and not stale.custody_transferred
        assert first.cancellation_reply is None and first.terminal_error is not None
        assert f.cancels[0][1].released and f.cancel_ack_lost
        f.assert_cancelled()
        f.assert_pending()
        assert owner.owner_table.snapshot(f.sources[0].object_id) == before_owner

        # The second exact Cancel returns the Node's cached tombstone and
        # inventory. It revokes execution only; STALE still gives no custody.
        assert not f.execute(first)
        parked = f.marker()
        state = parked.obligation
        assert parked.phase == "location_quarantined"
        assert state.receipts == first.receipts and state.terminal_error is first.terminal_error
        assert state.cancellation_reply == f.cancels[1][1]
        assert f.cancels[0][0] == f.cancels[1][0]
        assert not f.cancels[1][1].released and len(f.cancels) == 2
        assert state.acknowledged_keys == () and state.owner_deaths == ()
        assert not state.custody_acknowledged and not f.custody_acks and not f.releases
        assert f.take() == ()
        f.assert_pending()
        assert consumer._submissions.empty() and not consumer._gc_retry_timers
        assert not consumer._has_late_replica_cleanup_locked()
        assert not owner._has_late_replica_cleanup_locked()
        assert owner.owner_table.snapshot(f.sources[0].object_id) == before_owner
        assert owner._recovery.lineage_for_object(f.sources[0].object_id) is None
        assert before_owner.current_attempt == f.sources[0].producer_attempt_id
        assert before_owner.locations == frozenset((f.source_node.node_id,))
        assert f.source_node.object_store.get(f.sources[0].object_id) == f.payloads[0]
        assert f.target.object_store.get(f.sources[0].object_id) == f.payloads[0]
        frozen_calls = tuple(f.calls), tuple(f.borrow_calls)
        assert not f.execute(first)  # A stale queued item cannot bypass quarantine.
        assert (tuple(f.calls), tuple(f.borrow_calls)) == frozen_calls
        assert f.take() == () and f.marker().phase == "location_quarantined"
        f.assert_pending()

        # This is a separate authority-changing phase: an actual registered
        # owner incarnation dies in the GCS journal. STALE or Cancel alone
        # did not authorize terminality, input release, or deletion.
        death, record = f.install_owner_death(0)
        resumed = f.delayed()
        assert resumed.receipts == state.receipts and resumed.terminal_error is state.terminal_error
        assert resumed.cancellation_reply == state.cancellation_reply
        assert f.execute(resumed)
        accepted_state = f.custody_acks[0][2]
        assert accepted_state.owner_deaths == (record,)
        assert accepted_state.receipts == (stale,) and not stale.custody_transferred
        assert accepted_state.terminal_error is first.terminal_error
        assert consumer.owner_table.snapshot(pending.object_id).error is first.terminal_error
        assert len(f.protocol_faults) == 1 and not f.reports and len(f.cancels) == 2
        assert not f.releases and len(f.custody_acks) == 1
        assert len(f.service.owner_death_fences.pending_for_owner(death.worker_id)) == 2
        f.assert_terminal()
        f.collect()
        assert len(f.transfers) == 3 and len(f.fences) == 2 and not f.drops
        assert not f.releases
        assert owner.owner_table.snapshot(f.sources[0].object_id) == f.dead_owner_snapshot
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_canonical_report_runtime")
def test_successful_report_survives_definite_push_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _CanonicalReportFixture(monkeypatch)
    consumer, pending = f.consumer, f.pending
    try:
        f.fail_push = True
        assert f.execute()
        assert len(f.pushes) == len(f.abandons) == len(f.custody_acks) == 1
        assert f.cancels == [] and f.publication is None
        assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert f.target._leases[f.grant.lease_id].completion is None
        assert f.target.object_store.snapshot(f.source.object_id).pin_count == 0
        assert not f.target._lease_dependency_custody.has_pending()
        assert f.grant.node_id in f.owner.owner_table.snapshot(f.source.object_id).locations
        snapshot = consumer.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.ERROR and isinstance(snapshot.error, TransportConnectionError)
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert consumer._recovery.task_record(pending.task_id).state is TaskState.SYSTEM_FAILED
        assert consumer._recovery.task_record(pending.task_id).retries_started == 0
        assert consumer._finish_pending_task(pending)
        # Location is a durable physical fact, independent of consumer failure.
        assert f.grant.node_id in f.owner.owner_table.snapshot(f.source.object_id).locations
        assert f.target.object_store.get(f.source.object_id) == f.payload
        f.assert_held()
        f.collect()
    finally:
        f.close()


@pytest.mark.unit
def test_consumer_retry_preserves_foreign_producer_descriptor_epoch() -> None:
    consumer = _consumer_core()
    owner, _, _, source = _owner_core(consumer.worker_id)
    pending, guards = _pending(consumer, (source,))
    guard = replace(
        guards[0], borrower_token="borrow"
    )
    _install_hold(owner, guard)
    pending = replace(
        pending,
        spec=replace(pending.spec, max_retries=1),
        foreign_dependency_guards=(guard,),
    )
    consumer._recovery_manager().register_task(pending.spec, max_retries=1)
    consumer._borrow_rpc = lambda _address, handler, request: (
        owner.get_retained_owned_object(request)
        if handler == "get_retained_owned_object"
        else (_ for _ in ()).throw(AssertionError(handler))
    )
    system_reply = protocol.TaskReply(
        pending.spec.task_id, pending.spec.attempt_id, WorkerID.random(),
        protocol.TaskReplyStatus.SYSTEM_ERROR, results=(),
        error=protocol.RemoteErrorInfo("RuntimeError", "retry"),
    )

    assert not consumer._retry_explicit_system_failure(pending, system_reply)
    retried = consumer._submissions.get_nowait()
    prepared, dependencies, _ = consumer._prepare_task_dependencies(
        retried.spec, retried.foreign_dependency_guards
    )
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    expected_hold = _retained_hold(consumer.worker_id, pending.spec.attempt_id)
    assert guard.hold == expected_hold
    assert retried.foreign_dependency_guards[0].hold == expected_hold
    assert prepared.args == pending.spec.args
    assert dependencies == (source,)
    assert dependencies[0].producer_attempt_id == source.producer_attempt_id
    assert dependencies[0].producer_attempt_id != retried.spec.attempt_id
    assert retried.foreign_dependency_guards == pending.foreign_dependency_guards


@pytest.mark.unit
def test_lost_current_epoch_report_restores_ready_stored_location() -> None:
    borrower = WorkerID.random()
    owner, object_id, attempt, source = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    owner.owner_table.retain_borrowed_reference_for_task(
        object_id, (borrower, "borrow"), hold
    )
    assert owner.owner_table.mark_lost(object_id, attempt)
    assert owner.owner_table.snapshot(object_id).state.name == "LOST"
    target = replace(source, node_id=NodeID.random())

    reply = owner.report_retained_object_location(
        protocol.ReportRetainedObjectLocation(
            object_id, owner.worker_id, borrower, hold, target
        )
    )

    assert reply.status is protocol.RetainedLocationReportStatus.ADDED
    snapshot = owner.owner_table.snapshot(object_id)
    assert snapshot.state.name == "READY_STORED"
    assert snapshot.current_attempt == attempt
    assert snapshot.locations == frozenset({target.node_id})
    assert snapshot.canonical_stored_result is not None
    assert snapshot.canonical_stored_result.node_id == source.node_id
    assert owner._stored_descriptors[object_id].node_id == target.node_id


@pytest.mark.unit
def test_foreign_replica_report_after_source_death_restores_route() -> None:
    borrower = WorkerID.random()
    owner, object_id, attempt, source = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    owner.owner_table.retain_borrowed_reference_for_task(
        object_id, (borrower, "borrow"), hold
    )
    canonical = owner._stored_descriptors[object_id]
    removal = owner.handle_node_death(
        protocol.NodeDeathRecord(
            "source-death", source.node_id, 12345, 1, 1, -9,
            protocol.NodeDeathReason.PROCESS_EXIT, "test process exited",
        ),
        1,
        True,
    )
    assert removal.lost == (object_id,)
    assert object_id not in owner._stored_descriptors
    target = replace(source, node_id=NodeID.random())

    reply = owner.report_retained_object_location(
        protocol.ReportRetainedObjectLocation(
            object_id, owner.worker_id, borrower, hold, target
        )
    )

    assert reply.status is protocol.RetainedLocationReportStatus.ADDED
    snapshot = owner.owner_table.snapshot(object_id)
    assert snapshot.state.name == "READY_STORED"
    assert snapshot.current_attempt == attempt
    assert snapshot.locations == frozenset({target.node_id})
    assert snapshot.canonical_stored_result == canonical
    assert owner._stored_descriptors[object_id] == replace(
        canonical, node_id=target.node_id
    )


@pytest.mark.unit
def test_foreign_replica_report_before_source_death_keeps_survivor_route() -> None:
    borrower = WorkerID.random()
    owner, object_id, _, source = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    owner.owner_table.retain_borrowed_reference_for_task(
        object_id, (borrower, "borrow"), hold
    )
    canonical = owner._stored_descriptors[object_id]
    target = replace(source, node_id=NodeID.random())
    reply = owner.report_retained_object_location(
        protocol.ReportRetainedObjectLocation(
            object_id, owner.worker_id, borrower, hold, target
        )
    )
    assert reply.status is protocol.RetainedLocationReportStatus.ADDED

    owner.handle_node_death(
        protocol.NodeDeathRecord(
            "source-death", source.node_id, 12345, 1, 1, -9,
            protocol.NodeDeathReason.PROCESS_EXIT, "test process exited",
        ),
        1,
        True,
    )

    snapshot = owner.owner_table.snapshot(object_id)
    assert snapshot.locations == frozenset({target.node_id})
    assert snapshot.canonical_stored_result == canonical
    assert owner._stored_descriptors[object_id] == replace(
        canonical, node_id=target.node_id
    )


@pytest.mark.unit
def test_foreign_replica_route_write_failure_has_no_owner_mutation() -> None:
    class RejectingRoutes(dict):
        def __setitem__(self, key, value):
            raise RuntimeError("route write failed")

    borrower = WorkerID.random()
    owner, object_id, _, source = _owner_core(borrower)
    hold = _retained_hold(borrower, _identity()[1])
    owner.owner_table.retain_borrowed_reference_for_task(
        object_id, (borrower, "borrow"), hold
    )
    before = owner.owner_table.snapshot(object_id)
    target = replace(source, node_id=NodeID.random())
    owner._stored_descriptors = RejectingRoutes()

    with pytest.raises(RuntimeError, match="route write failed"):
        owner.report_retained_object_location(
            protocol.ReportRetainedObjectLocation(object_id, owner.worker_id, borrower, hold, target)
        )
    assert owner.owner_table.snapshot(object_id) == before
