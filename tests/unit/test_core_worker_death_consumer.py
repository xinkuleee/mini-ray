"""Worker-death reducers with explicit threadless admission boundaries.

The original late-outbound case uses a pure Core mailbox and the actual
Node/Worker registries' ordered death suffix. The supervisor report is a typed
reducer input, not a real process exit. Late ObjectID/hold values are request
credentials only: no foreign result, admitted Task or ObjectRef is fabricated.
One death wake and one empty-suffix replay are driven explicitly; no reference
thread, process, socket, store, user code, timer or wait starts. Other bodies
and their synchronous RPC/death inputs are unchanged.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.control import NodeRegistry, WorkerRegistry
from miniray.core import (
    CoreWorker, ObjectRef, _BorrowReleaseIdentity, _BorrowReleaseObligation,
    _AttemptBorrowRelease, _ForeignDependencyGuard, _ForeignGuardReleaseRetry,
    _ObjectWaiter, _PendingTask, _WAKE_COORDINATOR, _worker_death_reference_id,
)
from miniray.errors import OwnerDiedError, OwnerUnavailableError
from miniray.transport import TransportTimeout
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.object_store import ObjectStore
from miniray.foreign_lineage import (
    ForeignLineageEdge, ForeignLineageRole, ForeignLineageRegistry,
)
from miniray.foreign_lineage_runtime import ForeignLineageRuntime
from miniray.resources import ResourceVector
from miniray.trace import MemoryEventSink
from tests.unit._pure_core import close_pure_core, make_pure_core


def _core(*, with_gcs: bool = True) -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.gcs_address = ("127.0.0.1", 28900) if with_gcs else None
    core.worker_id = WorkerID.random()
    core._owner_table = ObjectOwnerTable()
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._worker_death_cursor = 0
    core._worker_death_sync_lock = threading.Lock()
    core._object_gc_obligations = {}
    core._inline_gc_obligations = core._object_gc_obligations
    core.event_sink = MemoryEventSink()
    return core


def _death(
    worker_id: WorkerID, epoch: int, *, detection_id: str | None = None,
    reason: protocol.WorkerDeathReason = protocol.WorkerDeathReason.PROCESS_EXIT,
) -> protocol.WorkerDeathRecord:
    return protocol.WorkerDeathRecord(
        detection_id or "worker-death-{}".format(epoch),
        protocol.WorkerIncarnation(
            NodeID.random(), 41000 + epoch, 1, worker_id, 42000 + epoch
        ),
        epoch,
        -9,
        reason,
    )


def _pending_with_guard(
    core: CoreWorker, owner: WorkerID, *, submission_index: int = 0
) -> tuple[_PendingTask, _ForeignDependencyGuard]:
    job_id = JobID.random()
    task_id = TaskID.derive(
        job_id, TaskID.for_driver(job_id), submission_index
    )
    attempt_id = AttemptID(task_id, 0)
    object_id = ObjectID.for_task(task_id)
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
        task_id, attempt_id,
    )
    guard = _ForeignDependencyGuard(
        ObjectID.for_task(
            TaskID.derive(job_id, TaskID.for_driver(job_id), 100)
        ),
        owner, ("127.0.0.1", 29010), core.worker_id, "borrow", hold,
    )
    spec = protocol.TaskSpec(
        job_id, task_id, attempt_id,
        protocol.FunctionKey(job_id, __name__, "consumer", "v1"),
        (), 1, ResourceVector(), core.worker_id,
    )
    core.owner_table.register(object_id, current_attempt=attempt_id)
    core._objects = getattr(core, "_objects", {})
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    return _PendingTask(
        object_id, spec, foreign_dependency_guards=(guard,)
    ), guard


def _candidate_owned_only_by(
    core: CoreWorker, worker_id: WorkerID
) -> ObjectID:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    object_id = ObjectID.for_task(task_id)
    attempt_id = AttemptID(task_id, 0)
    core.owner_table.register(object_id, current_attempt=attempt_id)
    assert core.owner_table.publish_inline(object_id, attempt_id, b"value")
    assert core.owner_table.add_borrowed_reference(
        object_id, (worker_id, "only-live-reference")
    )
    return object_id


@pytest.mark.unit
def test_complete_suffix_applies_in_order_then_drives_existing_gc_path() -> None:
    core = _core()
    first_worker = WorkerID.random()
    second_worker = WorkerID.random()
    candidate = _candidate_owned_only_by(core, first_worker)
    deaths = (_death(first_worker, 1), _death(second_worker, 2))
    requests: list[protocol.GetWorkerDeaths] = []
    gc_candidates: list[ObjectID] = []

    def rpc(address: object, handler: str, request: object) -> object:
        assert address == core.gcs_address
        assert handler == "get_worker_deaths"
        assert isinstance(request, protocol.GetWorkerDeaths)
        requests.append(request)
        return protocol.GetWorkerDeathsReply(0, 2, deaths)

    core._rpc = rpc  # type: ignore[method-assign]
    core._drive_reference_collection_best_effort = (  # type: ignore[method-assign]
        gc_candidates.append
    )

    assert core._sync_worker_deaths()
    assert requests == [protocol.GetWorkerDeaths(0)]
    assert core._worker_death_cursor == 2
    assert core.owner_table.dead_worker_record(first_worker).death_id == (
        _worker_death_reference_id(deaths[0])
    )
    assert core.owner_table.dead_worker_record(second_worker).death_id == (
        _worker_death_reference_id(deaths[1])
    )
    assert gc_candidates == [candidate]


@pytest.mark.unit
def test_later_reducer_failure_commits_only_the_applied_prefix() -> None:
    core = _core()
    first = _death(WorkerID.random(), 1)
    second = _death(WorkerID.random(), 2)
    requested_after: list[int] = []

    def rpc(_address: object, _handler: str, request: object) -> object:
        assert isinstance(request, protocol.GetWorkerDeaths)
        requested_after.append(request.after_epoch)
        if request.after_epoch == 0:
            return protocol.GetWorkerDeathsReply(0, 2, (first, second))
        return protocol.GetWorkerDeathsReply(1, 2, (second,))

    original = core.owner_table.install_dead_worker
    reject_second_once = True

    def install(worker_id: WorkerID, death_id: str):
        nonlocal reject_second_once
        if worker_id == second.worker_id and reject_second_once:
            reject_second_once = False
            raise RuntimeError("injected reducer failure")
        return original(worker_id, death_id)

    core._rpc = rpc  # type: ignore[method-assign]
    core.owner_table.install_dead_worker = install  # type: ignore[method-assign]
    core._drive_reference_collection_best_effort = (  # type: ignore[method-assign]
        lambda _object_id: None
    )

    assert not core._sync_worker_deaths()
    assert core._worker_death_cursor == 1
    assert core.owner_table.dead_worker_record(first.worker_id) is not None
    assert core.owner_table.dead_worker_record(second.worker_id) is None

    assert core._sync_worker_deaths()
    assert requested_after == [0, 1]
    assert core._worker_death_cursor == 2
    assert core.owner_table.dead_worker_record(second.worker_id) is not None


@pytest.mark.unit
def test_expected_exit_advances_cursor_without_sweeping_owner_references() -> None:
    core = _core()
    worker_id = WorkerID.random()
    object_id = _candidate_owned_only_by(core, worker_id)
    expected = protocol.WorkerDeathRecord(
        "orderly-worker-exit",
        protocol.WorkerIncarnation(
            NodeID.random(), 45001, 1, worker_id, 46001
        ),
        1,
        0,
        protocol.WorkerDeathReason.EXPECTED,
    )
    core._rpc = lambda *_args: protocol.GetWorkerDeathsReply(  # type: ignore[method-assign]
        0, 1, (expected,)
    )
    gc_candidates: list[ObjectID] = []
    core._drive_reference_collection_best_effort = (  # type: ignore[method-assign]
        gc_candidates.append
    )

    assert core._sync_worker_deaths()
    assert core._worker_death_cursor == 1
    assert core.owner_table.dead_worker_record(worker_id) is None
    assert core.owner_table.snapshot(object_id).borrowed_tokens == frozenset(
        {(worker_id, "only-live-reference")}
    )
    assert gc_candidates == []


@pytest.mark.unit
def test_entire_suffix_is_validated_before_the_first_reducer_mutation() -> None:
    core = _core()
    first = _death(WorkerID.random(), 1)
    malformed = object.__new__(protocol.WorkerDeathRecord)
    object.__setattr__(malformed, "detection_id", "malformed-second")
    object.__setattr__(
        malformed,
        "incarnation",
        protocol.WorkerIncarnation(
            NodeID.random(), 43002, 1, WorkerID.random(), 44002
        ),
    )
    object.__setattr__(malformed, "death_epoch", 2)
    object.__setattr__(malformed, "exit_code", -9)
    object.__setattr__(malformed, "reason", "not-a-death-reason")
    reply = object.__new__(protocol.GetWorkerDeathsReply)
    object.__setattr__(reply, "after_epoch", 0)
    object.__setattr__(reply, "watermark", 2)
    object.__setattr__(reply, "deaths", (first, malformed))
    core._rpc = lambda *_args: reply  # type: ignore[method-assign]

    assert not core._sync_worker_deaths()
    assert core._worker_death_cursor == 0
    assert core.owner_table.dead_worker_record(first.worker_id) is None


@pytest.mark.parametrize(
    "outcome",
    (
        TimeoutError("GCS observation timed out"),
        object(),
        protocol.GetWorkerDeathsReply(1, 1, ()),
    ),
)
@pytest.mark.unit
def test_rpc_or_invalid_reply_never_advances_or_infers_death(
    outcome: object,
) -> None:
    core = _core()
    worker_id = WorkerID.random()
    calls = 0

    def rpc(*_args: object) -> object:
        nonlocal calls
        calls += 1
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    core._rpc = rpc  # type: ignore[method-assign]

    assert not core._sync_worker_deaths()
    assert calls == 1
    assert core._worker_death_cursor == 0
    assert core.owner_table.dead_worker_record(worker_id) is None


@pytest.mark.unit
def test_no_gcs_is_a_compatible_noop_without_an_rpc() -> None:
    core = _core(with_gcs=False)
    core._rpc = lambda *_args: pytest.fail("no-GCS Core attempted an RPC")  # type: ignore[method-assign]

    assert core._sync_worker_deaths()
    assert core._worker_death_cursor == 0


@pytest.mark.unit
def test_committed_owner_death_discharges_only_that_owners_outbound_work() -> None:
    core = _core()
    dead = WorkerID.random()
    live = WorkerID.random()
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 9)
    object_id = ObjectID.for_task(task)
    attempt = AttemptID(task, 0)

    def ordinary(owner: WorkerID, token: str) -> _BorrowReleaseObligation:
        acquire = protocol.AcquireBorrowedObject(
            object_id, owner, core.worker_id, "transfer-" + token, token
        )
        release = protocol.ReleaseBorrowedObject(
            object_id, owner, core.worker_id, token
        )
        obligation = _BorrowReleaseObligation(
            _BorrowReleaseIdentity(("127.0.0.1", 29001), acquire, release),
            release_requested=True,
        )
        core._borrowed_release_obligations = getattr(
            core, "_borrowed_release_obligations", {}
        )
        core._borrowed_release_obligations[obligation.key] = obligation
        return obligation

    dead_obligation = ordinary(dead, "dead")
    live_obligation = ordinary(live, "live")
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, core.worker_id, task, attempt
    )
    transfer = protocol.NestedReferenceTransfer(
        object_id, dead, ("127.0.0.1", 29002), hold
    )
    acquire = protocol.AcquireBorrowedObject(
        object_id, dead, core.worker_id, protocol.TaskHoldSource(hold),
        "attempt",
    )
    release = protocol.ReleaseBorrowedObject(
        object_id, dead, core.worker_id, "attempt"
    )
    attempt_obligation = _AttemptBorrowRelease(
        transfer, attempt, acquire, release, release_requested=True
    )
    core._attempt_borrow_releases = {
        attempt_obligation.key: attempt_obligation
    }
    death = _death(dead, 1)
    core._rpc = lambda *_args: protocol.GetWorkerDeathsReply(  # type: ignore[method-assign]
        0, 1, (death,)
    )
    core._drive_reference_collection_best_effort = (  # type: ignore[method-assign]
        lambda _object_id: None
    )

    assert core._sync_worker_deaths()
    assert dead_obligation.key not in core._borrowed_release_obligations
    assert live_obligation.key in core._borrowed_release_obligations
    assert attempt_obligation.key not in core._attempt_borrow_releases
    with pytest.raises(OwnerDiedError, match="confirmed dead"):
        core._borrow_rpc(
            ("127.0.0.1", 29002), "release_borrowed_object", release
        )


@pytest.mark.unit
def test_death_journal_installs_same_proof_in_foreign_lineage_runtime() -> None:
    core = _core()
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    dead = WorkerID.random()
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, core.worker_id, task_id,
        AttemptID(task_id, 0),
    )
    registry.register(
        task_id, (ObjectID.for_task(task_id),),
        (ForeignLineageEdge(
            task_id, ObjectID.for_task(TaskID.random()), dead,
            ("127.0.0.1", 29031), core.worker_id, hold,
            ForeignLineageRole.TOP_LEVEL,
        ),),
    )
    core._foreign_lineage_registry = registry
    core._foreign_lineage_runtime = ForeignLineageRuntime(
        registry, replace_retained=lambda *_args: pytest.fail("no replace"),
        get_retained=lambda *_args: pytest.fail("no get"),
        request_reconstruction=lambda *_args: pytest.fail("no reconstruct"),
        release_retained=lambda *_args: pytest.fail("no release"),
        owner_death_lookup=core.owner_table.dead_worker_record,
    )
    death = _death(dead, 1)
    core._rpc = lambda *_args: protocol.GetWorkerDeathsReply(
        0, 1, (death,)
    )

    assert core._sync_worker_deaths()
    installed = core.owner_table.dead_worker_record(dead)
    assert installed is not None
    assert registry.owner_death_record(dead) == installed


@pytest.mark.unit
def test_owner_timeout_is_unavailable_and_preserves_release_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core(with_gcs=False)
    owner = WorkerID.random()
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 10)
    object_id = ObjectID.for_task(task)
    release = protocol.ReleaseBorrowedObject(
        object_id, owner, core.worker_id, "borrow"
    )
    acquire = protocol.AcquireBorrowedObject(
        object_id, owner, core.worker_id, "transfer", "borrow"
    )
    key, _obligation, _ = core._register_borrowed_release_obligation(
        ("127.0.0.1", 29003), acquire, release
    )
    core._borrowed_release_obligations[key].release_requested = True
    monkeypatch.setattr(
        "miniray.core.rpc_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            TransportTimeout("route unavailable")
        ),
    )

    with pytest.raises(OwnerUnavailableError, match="unreachable"):
        core._borrow_rpc(
            ("127.0.0.1", 29003), "release_borrowed_object", release
        )
    assert key in core._borrowed_release_obligations
    assert core.owner_table.dead_worker_record(owner) is None


@pytest.mark.parametrize(
    "reason",
    (
        protocol.WorkerDeathReason.PROCESS_EXIT,
        protocol.WorkerDeathReason.NODE_EXIT,
    ),
)
@pytest.mark.unit
def test_committed_death_converges_all_outbound_tables_and_shutdown_state(
    reason: protocol.WorkerDeathReason,
) -> None:
    core = _core()
    dead = WorkerID.random()
    live = WorkerID.random()
    dead_pending, dead_guard = _pending_with_guard(core, dead)
    live_pending, live_guard = _pending_with_guard(
        core, live, submission_index=1
    )
    core._orphan_foreign_guard_releases = {
        core._foreign_guard_key(dead_guard): dead_guard,
        core._foreign_guard_key(live_guard): live_guard,
    }
    now = __import__("time").monotonic()
    core._foreign_guard_release_retries = {
        dead_pending.task_id: _ForeignGuardReleaseRetry(
            dead_pending, now + 10.0
        ),
        live_pending.task_id: _ForeignGuardReleaseRetry(
            live_pending, now + 10.0
        ),
    }
    core._accepted_task_count = 2
    core._active_task_finishes = set()
    core._finishing_tasks = {
        dead_pending.task_id, live_pending.task_id
    }
    core._finished_tasks = set()
    core._protocol_unresolved = {}
    core._inflight_submissions = 0
    core._inflight_puts = 0
    core._inflight_borrow_ops = 0
    core._inflight_pg_control_ops = 0
    core._actor_control_ops = 0
    core._actor_call_threads = set()
    core._accepting = False
    core._owner_protocol_open = True
    core._submissions = __import__("queue").Queue()

    death = _death(dead, 1, reason=reason)
    core._rpc = lambda *_args: protocol.GetWorkerDeathsReply(  # type: ignore[method-assign]
        0, 1, (death,)
    )
    core._drive_reference_collection_best_effort = (  # type: ignore[method-assign]
        lambda _object_id: None
    )

    assert core._sync_worker_deaths()
    assert core._foreign_guard_key(dead_guard) not in (
        core._orphan_foreign_guard_releases
    )
    assert core._foreign_guard_key(live_guard) in (
        core._orphan_foreign_guard_releases
    )
    dead_retry = core._foreign_guard_release_retries[dead_pending.task_id]
    live_retry = core._foreign_guard_release_retries[live_pending.task_id]
    assert core._foreign_guard_key(dead_guard) in dead_retry.released_keys
    assert not live_retry.released_keys

    # The next finish pass treats the death tombstone as terminal release
    # authority and no longer leaves this task/shutdown waiting forever.
    assert core._finish_pending_task(dead_pending)
    assert dead_pending.task_id not in core._foreign_guard_release_retries
    assert dead_pending.task_id not in core._finishing_tasks
    assert dead_pending.task_id in core._finished_tasks
    assert core._accepted_task_count == 1
    assert not core._shutdown_finalizable_locked(
        require_distributed_clean=False
    )  # the live owner's work remains


@pytest.mark.unit
def test_death_cleanup_discharges_owned_incoming_pins_without_raw_reply_authority() -> None:
    from miniray.contained_edges import ContainedReferenceHold

    core = _core()
    dead = WorkerID.random()
    incoming_id = _candidate_owned_only_by(core, WorkerID.random())
    outer_id = ObjectID.for_task(TaskID.random())
    incoming_hold = ContainedReferenceHold(
        outer_id, dead, "incoming-pin"
    )
    core.owner_table.add_contained_reference(incoming_id, incoming_hold)
    death = _death(dead, 1)
    core._rpc = lambda *_args: protocol.GetWorkerDeathsReply(  # type: ignore[method-assign]
        0, 1, (death,)
    )
    core._drive_reference_collection_best_effort = (  # type: ignore[method-assign]
        lambda _object_id: None
    )

    assert core._sync_worker_deaths()
    assert not hasattr(core, "_orphan_contained_edges")
    assert not core.owner_table.snapshot(incoming_id).contained_holds
    assert core.owner_table.contained_release_was_seen(
        incoming_id, incoming_hold
    )
    # Failed/malformed TaskReply metadata no longer carries a second cleanup
    # authority. The unified manifest and its owner/Node obligations own it.
    assert not hasattr(core, "_retain_orphan_contained_edges")


@pytest.mark.unit
def test_timeout_cannot_release_typed_incoming_pin_without_death_record() -> None:
    from miniray.contained_edges import ContainedReferenceHold

    core = _core()
    container_owner = WorkerID.random()
    incoming_id = _candidate_owned_only_by(core, WorkerID.random())
    hold = ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), container_owner, "timeout-pin"
    )
    core.owner_table.add_contained_reference(incoming_id, hold)
    core._rpc = lambda *_args: (_ for _ in ()).throw(TimeoutError("gcs"))

    assert not core._sync_worker_deaths()
    assert core.owner_table.snapshot(incoming_id).contained_holds == frozenset(
        {hold}
    )
    assert core.owner_table.dead_worker_record(container_owner) is None


@pytest.mark.unit
def test_uninstalled_or_expected_death_cannot_sweep_outbound_work() -> None:
    core = _core()
    owner = WorkerID.random()
    pending, guard = _pending_with_guard(core, owner)
    core._orphan_foreign_guard_releases = {
        core._foreign_guard_key(guard): guard
    }

    with pytest.raises(ValueError, match="exact installed"):
        core._discharge_dead_owner_obligations(_death(owner, 1))
    assert core._foreign_guard_key(guard) in core._orphan_foreign_guard_releases

    expected = _death(
        owner, 1, reason=protocol.WorkerDeathReason.EXPECTED
    )
    core._discharge_dead_owner_obligations(expected)
    assert core._foreign_guard_key(guard) in core._orphan_foreign_guard_releases
    assert pending.object_id not in getattr(
        core, "_foreign_guard_release_retries", {}
    )


@pytest.fixture
def _no_late_outbound_runtime(monkeypatch):
    violations = []

    def forbidden(*args, **kwargs):
        # GCS observation deliberately catches BaseException. Retain the
        # violation so a swallowed failure still fails this case at teardown.
        violations.append((args, kwargs))
        pytest.fail("late outbound admission attempted runtime work")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_initialize_reference_events"),
        (CoreWorker, "_register_submission"), (CoreWorker, "_execute"),
        (ObjectRef, "__init__"), (ObjectStore, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    yield violations, forbidden
    assert not violations


@pytest.mark.unit
def test_late_outbound_admission_is_fenced_after_owner_death(
    monkeypatch: pytest.MonkeyPatch, _no_late_outbound_runtime,
) -> None:
    core = make_pure_core()
    violations, forbidden = _no_late_outbound_runtime
    core.gcs_address = ("late-death-gcs.invalid", 1)
    # Use the real borrowing method's early owner fence. The shared pure
    # fixture normally forbids this whole method; its transport stays forbidden.
    monkeypatch.setattr(core, "_borrow_rpc", CoreWorker._borrow_rpc.__get__(core, CoreWorker))
    monkeypatch.setattr(core, "_initialize_reference_events", forbidden)
    try:
        nodes = NodeRegistry()
        assert nodes.register(
            core.node_id, core.node_address, ResourceVector({"CPU": 1}), node_pid=42101,
        )
        workers = WorkerRegistry(nodes)
        dead = WorkerID.random()
        registered_node = nodes.get(core.node_id)
        owner = protocol.WorkerIncarnation(
            core.node_id, registered_node.node_pid, registered_node.registration_epoch, dead, 42201,
        )
        borrower = protocol.WorkerIncarnation(
            core.node_id, registered_node.node_pid, registered_node.registration_epoch, core.worker_id, 42202,
        )
        assert workers.register(protocol.RegisterWorkerIncarnation(owner)).accepted
        assert workers.register(protocol.RegisterWorkerIncarnation(borrower)).accepted
        assert workers.death_watermark == 0
        assert core.owner_table.dead_worker_record(dead) is None
        # A declared supervisor observation enters the actual GCS registry.
        # Only its generated ordered suffix is delivered to the Core.
        report = protocol.ReportWorkerDeath(
            "late-owner-exit", owner, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        reported = workers.report_death(report)
        assert reported.disposition is protocol.WorkerDeathDisposition.APPLIED
        death = reported.death
        assert death is not None and death.incarnation == owner and death.death_epoch == 1
        assert core.owner_table.dead_worker_record(dead) is None
        suffix_calls = []

        def suffix(address, handler, request):
            if (address != core.gcs_address or handler != "get_worker_deaths"
                    or type(request) is not protocol.GetWorkerDeaths or len(suffix_calls) >= 2):
                forbidden("unexpected GCS/owner request", address, handler, request)
            reply = workers.deaths_after(request)
            suffix_calls.append((request, reply))
            return reply

        monkeypatch.setattr(core, "_rpc", suffix)
        assert core._sync_worker_deaths()
        assert suffix_calls == [(protocol.GetWorkerDeaths(0), protocol.GetWorkerDeathsReply(0, 1, (death,)))]
        assert core._worker_death_cursor == 1
        installed = core.owner_table.dead_worker_record(dead)
        assert installed is not None and installed.death_id == _worker_death_reference_id(death)
        assert core._foreign_lineage_registry.owner_death_record(dead) == installed
        assert core.owner_table.dead_worker_record(core.worker_id) is None
        assert workers.get(core.worker_id).state is protocol.WorkerMembershipState.ALIVE
        assert core._submissions.qsize() == 1
        assert core._submissions.get_nowait() is _WAKE_COORDINATOR
        core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0

        # These identities are late wire credentials, not a successful
        # submission or a published foreign value. No output waiter, ObjectRef,
        # accepted count, remote hold or result bytes are installed by the test.
        task = TaskID.derive(core.job_id, core.driver_task_id, 0)
        attempt = AttemptID(task, 0)
        object_id = ObjectID.for_task(TaskID.derive(core.job_id, core.driver_task_id, 100))
        hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, core.worker_id, task, attempt,
        )
        guard = _ForeignDependencyGuard(
            object_id, dead, ("late-owner.invalid", 1), core.worker_id, "borrow", hold,
        )
        release = protocol.ReleaseBorrowedObject(object_id, dead, core.worker_id, "late")
        acquire = protocol.AcquireBorrowedObject(object_id, dead, core.worker_id, "transfer", "late")
        with pytest.raises(OwnerDiedError, match="confirmed dead"):
            core._register_borrowed_release_obligation(guard.owner_address, acquire, release)
        assert not getattr(core, "_borrowed_release_obligations", {})
        core._record_orphan_foreign_guard_release(guard)
        assert not getattr(core, "_orphan_foreign_guard_releases", {})

        transfer = protocol.NestedReferenceTransfer(object_id, dead, guard.owner_address, hold)
        mailbox = core._reference_mailbox
        assert mailbox.accepting
        with pytest.raises(OwnerDiedError, match="confirmed dead"):
            core._restore_task_argument_reference(transfer, attempt)
        assert not getattr(core, "_attempt_borrow_releases", {})
        assert core._reference_mailbox is mailbox
        with pytest.raises(OwnerDiedError, match="confirmed dead"):
            core._retain_foreign_dependency_guard(guard)
        export_hold = ContainedReferenceHold(ObjectID.for_task(task), core.worker_id, "late-export")
        with pytest.raises(OwnerDiedError, match="confirmed dead"):
            core._restore_borrowed_reference(object_id, dead, guard.owner_address, export_hold)
        assert not getattr(core, "_borrowed_release_obligations", {})
        assert not violations and len(suffix_calls) == 1
        assert core.owner_table.dead_worker_record(dead) == installed

        assert core._sync_worker_deaths()
        assert suffix_calls[-1] == (protocol.GetWorkerDeaths(1), protocol.GetWorkerDeathsReply(1, 1, ()))
        assert len(suffix_calls) == 2 and core._worker_death_cursor == 1
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert core._reference_mailbox is mailbox and mailbox.accepting
        assert mailbox.pending.empty() and mailbox.pending.unfinished_tasks == 0 and not mailbox.releases
        assert core._submission_index == core._put_index == core._reference_index == 0
        assert core._accepted_task_count == core._inflight_submissions == core._inflight_borrow_ops == core._inflight_puts == 0
        assert not core._objects and not core._stored_descriptors and not core._task_finish_barriers
        assert not core._protocol_unresolved and not core._object_gc_obligations
        assert not core._finished_tasks and not core._active_task_finishes
        assert not core._foreign_lineage_registry.task_ids()
        assert not core._foreign_lineage_runtime.pending_task_ids()
        assert not core.owner_table.contains(object_id) and not core.owner_table.contains(ObjectID.for_task(task))
        assert core._recovery.lineage_for_object(object_id) is None
        assert core._recovery.lineage_for_object(ObjectID.for_task(task)) is None
        assert not getattr(core, "_attempt_borrow_releases", {})
        assert not getattr(core, "_orphan_foreign_guard_releases", {})
        assert not getattr(core, "_export_pin_release_obligations", {})
        assert not violations
    finally:
        # There is no accepted Task/handle/remote value to finish or collect.
        # Fencing the already-empty mailbox must not fabricate such work.
        close_pure_core(core)


@pytest.mark.unit
def test_release_failure_racing_committed_death_does_not_create_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core()
    dead = WorkerID.random()
    pending, guard = _pending_with_guard(core, dead)
    core._accepted_task_count = 1
    core._active_task_finishes = set()
    core._finishing_tasks = set()
    core._finished_tasks = set()
    core._protocol_unresolved = {}
    core._foreign_guard_release_retries = {}
    core._submissions = __import__("queue").Queue()
    death = _death(dead, 1)

    def fail_after_death(_guard: _ForeignDependencyGuard) -> bool:
        core.owner_table.install_dead_worker(
            dead, _worker_death_reference_id(death)
        )
        raise OwnerUnavailableError("Release reply was ambiguous")

    monkeypatch.setattr(
        core, "_release_foreign_dependency_guard", fail_after_death
    )

    assert core._finish_pending_task(pending)
    assert not core._foreign_guard_release_retries
    assert pending.task_id in core._finished_tasks
    assert core._accepted_task_count == 0


@pytest.mark.unit
def test_network_timeout_keeps_foreign_release_work_and_task_unfinished(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _core()
    owner = WorkerID.random()
    pending, guard = _pending_with_guard(core, owner)
    core._accepted_task_count = 1
    core._active_task_finishes = set()
    core._finishing_tasks = set()
    core._finished_tasks = set()
    core._protocol_unresolved = {}
    core._foreign_guard_release_retries = {}
    core._submissions = __import__("queue").Queue()
    monkeypatch.setattr(
        core, "_release_foreign_dependency_guard",
        lambda _guard: (_ for _ in ()).throw(
            OwnerUnavailableError("owner route timed out")
        ),
    )

    assert not core._finish_pending_task(pending)
    retry = core._foreign_guard_release_retries[pending.task_id]
    assert not retry.released_keys
    assert pending.task_id in core._finishing_tasks
    assert pending.task_id not in core._finished_tasks
    assert core._accepted_task_count == 1
    assert core.owner_table.dead_worker_record(owner) is None
