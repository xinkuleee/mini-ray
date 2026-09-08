"""Foreign INLINE dependency contracts with explicit execution modes.

The first three tests use owner reducers/protocol values only. All remaining
original tests construct a live CoreWorker with reference/coordinator/dispatch
threads; some also start test threads or poll real time. They remain heavy
until independently reviewed for bounded runtime execution.
"""

from __future__ import annotations

import threading
import time
from dataclasses import replace

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker, ObjectRef, _ForeignDependencyGuard, _ObjectWaiter,
    _PendingTask, _TaskFinishing,
)
from miniray.errors import RuntimeShuttingDownError, SystemTaskError, TaskError
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ConflictingRetainedTokenError, ObjectOwnerTable, OwnershipError,
    ReferenceKind, ReleasedRetainedTokenError,
)
from miniray.resources import ResourceVector
from miniray.trace import EventSink




def _object(index: int = 0) -> tuple[ObjectID, AttemptID]:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), index)
    return ObjectID.for_task(task), AttemptID(task, 0)


def _retained_hold(
    borrower: WorkerID, origin_attempt: AttemptID | None = None,
) -> protocol.TaskReferenceHold:
    if origin_attempt is None:
        _, origin_attempt = _object()
    return protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, borrower,
        origin_attempt.task_id, origin_attempt,
    )


def _owner_core(
    borrower: WorkerID | None = None,
) -> tuple[CoreWorker, ObjectID, AttemptID, WorkerID]:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 26001)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.event_sink = EventSink()
    core._owner_table = ObjectOwnerTable()
    core._stored_descriptors = {}
    core._objects = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._accepting = True
    core._owner_protocol_open = True
    core._owner_retain_admission_open = True
    core._inflight_borrow_ops = 0
    core._inflight_puts = 0
    core._inflight_submissions = 0
    object_id, attempt = _object()
    borrower = borrower or WorkerID.random()
    core.owner_table.register(object_id, current_attempt=attempt)
    core.owner_table.add_borrowed_reference(object_id, (borrower, "borrow"))
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    return core, object_id, attempt, borrower


def _retain(
    core: CoreWorker, object_id: ObjectID, borrower: WorkerID,
    hold: protocol.TaskReferenceHold | None = None,
    borrower_token: str = "borrow",
) -> protocol.RetainOwnedObjectForTask:
    hold = hold or _retained_hold(borrower)
    return protocol.RetainOwnedObjectForTask(
        object_id, core.worker_id, borrower, borrower_token, hold
    )


def _runtime_core(lanes: int = 1) -> CoreWorker:
    return CoreWorker(
        ("127.0.0.1", 26002), NodeID.random(), event_sink=EventSink(),
        dispatch_lanes=lanes,
    )


def _foreign_ref(
    object_id: ObjectID, owner: WorkerID, borrower_token: str = "borrow",
    *, borrower_core: CoreWorker | None = None,
) -> ObjectRef:
    ref = ObjectRef(object_id, owner, ("127.0.0.1", 26003))
    if borrower_core is None:
        ref._borrower_token = borrower_token
    else:
        source = protocol.ContainedTransferSource("fixture-transfer")
        acquire = protocol.AcquireBorrowedObject(
            object_id, owner, borrower_core.worker_id, source, borrower_token
        )
        release = protocol.ReleaseBorrowedObject(
            object_id, owner, borrower_core.worker_id, borrower_token
        )
        borrower_core._register_borrowed_release_obligation(
            ref.owner_address, acquire, release
        )
        ref._bind_borrowed_reference(
            borrower_core, borrower_token, acquire.source
        )
    return ref


def _owner_rpc(owner: CoreWorker, handler: str, request: object) -> object:
    if handler == "retain_owned_object_for_task":
        return owner.retain_owned_object_for_task(request)
    if handler == "get_retained_owned_object":
        return owner.get_retained_owned_object(request)
    if handler == "release_owned_object_for_task":
        return owner.release_owned_object_for_task(request)
    raise AssertionError(handler)


@pytest.mark.unit
def test_retained_hold_is_bound_idempotent_and_survives_parent_release() -> None:
    core, object_id, attempt, borrower = _owner_core()
    request = _retain(core, object_id, borrower)
    first = core.retain_owned_object_for_task(request)
    replay = core.retain_owned_object_for_task(request)
    assert first.accepted and first.retained
    assert replay.accepted and not replay.retained
    assert first.hold == replay.hold == request.hold
    with pytest.raises(ConflictingRetainedTokenError):
        core.owner_table.retain_borrowed_reference_for_task(
            object_id, (borrower, "other-borrower"), request.hold
        )

    assert core.owner_table.release_borrowed_reference(
        object_id, (borrower, "borrow")
    )
    core.owner_table.publish_inline(object_id, attempt, cloudpickle.dumps(42))
    reply = core.get_retained_owned_object(
        protocol.GetRetainedOwnedObject(
            object_id, core.worker_id, borrower, request.hold
        )
    )
    assert reply.accepted and reply.state is protocol.OwnedObjectState.READY_INLINE
    assert reply.current_attempt == attempt
    assert cloudpickle.loads(reply.data) == 42
    assert core.release_owned_object_for_task(
        protocol.ReleaseOwnedObjectForTask(
            object_id, core.worker_id, borrower, request.hold
        )
    ).accepted


@pytest.mark.unit
def test_release_before_retain_tombstones_and_generic_api_is_forbidden() -> None:
    table = ObjectOwnerTable()
    object_id, attempt = _object()
    borrower = WorkerID.random()
    table.register(object_id, current_attempt=attempt)
    table.add_borrowed_reference(object_id, (borrower, "borrow"))
    hold = _retained_hold(borrower)
    assert not table.release_retained_reference_for_task(object_id, hold)
    with pytest.raises(ReleasedRetainedTokenError):
        table.retain_borrowed_reference_for_task(
            object_id, (borrower, "borrow"), hold
        )
    with pytest.raises(OwnershipError, match="typed TaskReferenceHold"):
        table.add_reference(object_id, ReferenceKind.RETAINED, hold)
    with pytest.raises(OwnershipError, match="typed TaskReferenceHold"):
        table.release_reference(object_id, ReferenceKind.RETAINED, hold)


@pytest.mark.unit
def test_foreign_dependency_protocol_validates_full_identity_and_payload() -> None:
    object_id, _ = _object()
    owner = WorkerID.random()
    borrower = WorkerID.random()
    hold = _retained_hold(borrower)
    retain = protocol.RetainOwnedObjectForTask(
        object_id, owner, borrower, "borrow", hold
    )
    assert retain.hold == hold
    assert protocol.RetainOwnedObjectForTaskReply(
        object_id, owner, borrower, "borrow", hold, True, True
    ).hold == hold
    assert protocol.GetRetainedOwnedObjectReply(
        object_id, owner, borrower, hold, True,
        state=protocol.OwnedObjectState.PENDING,
    ).hold == hold
    assert protocol.ReleaseOwnedObjectForTaskReply(
        object_id, owner, borrower, hold, True, False
    ).hold == hold

    with pytest.raises(ProtocolError, match="borrower_token"):
        protocol.RetainOwnedObjectForTask(
            object_id, owner, borrower, "", hold
        )
    with pytest.raises(ProtocolError, match="cannot expose"):
        protocol.GetRetainedOwnedObjectReply(
            object_id, owner, borrower, hold, False,
            state=protocol.OwnedObjectState.PENDING, detail="rejected",
        )
    with pytest.raises(ProtocolError, match="rejected.*cannot remove"):
        protocol.ReleaseOwnedObjectForTaskReply(
            object_id, owner, borrower, hold, False, True, "rejected"
        )
    with pytest.raises(ProtocolError, match="RETAINED kind"):
        protocol.GetRetainedOwnedObject(
            object_id, owner, borrower,
            replace(hold, kind=protocol.TaskReferenceHoldKind.SUBMITTED),
        )
    with pytest.raises(ProtocolError, match="submitting worker"):
        protocol.ReleaseOwnedObjectForTask(
            object_id, owner, borrower,
            _retained_hold(WorkerID.random(), hold.origin_attempt_id),
        )


@pytest.mark.heavy
def test_registration_retain_is_outside_state_lock_and_shutdown_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    owner, object_id, _, borrower = _owner_core(core.worker_id)
    foreign = _foreign_ref(object_id, owner.worker_id)
    entered = threading.Event()
    allow_reply = threading.Event()
    release_calls: list[protocol.ReleaseOwnedObjectForTask] = []

    def rpc(_address: object, handler: str, request: object) -> object:
        if handler == "retain_owned_object_for_task":
            result = owner.retain_owned_object_for_task(request)
            entered.set()
            assert allow_reply.wait(1.0)
            return result
        if handler == "release_owned_object_for_task":
            release_calls.append(request)
            return owner.release_owned_object_for_task(request)
        return _owner_rpc(owner, handler, request)

    monkeypatch.setattr(core, "_borrow_rpc", rpc)
    result: list[BaseException] = []

    def register() -> None:
        try:
            core._register_submission(
                core.define_remote_function(lambda value: value),
                (foreign,), {}, ResourceVector(),
            )
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(
        target=register,
    )
    thread.start()
    try:
        assert entered.wait(1.0)
        assert core._state_lock.acquire(timeout=0.1)
        try:
            core._accepting = False
        finally:
            core._state_lock.release()
        allow_reply.set()
        thread.join(1.0)
        assert not thread.is_alive()
        assert len(result) == 1 and isinstance(
            result[0], RuntimeShuttingDownError
        )
        assert len(release_calls) == 1
        released_hold = release_calls[0].hold
        assert released_hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert released_hold.submitting_worker_id == core.worker_id
        assert released_hold.origin_attempt_id == AttemptID(
            released_hold.task_id, 0
        )
        assert not owner.owner_table.snapshot(object_id).retained_tokens
        assert not core._objects
    finally:
        allow_reply.set()
        core._accepting = True
        assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_ambiguous_retain_rolls_back_exact_token_and_retains_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    owner, object_id, attempt, _ = _owner_core(core.worker_id)
    foreign = _foreign_ref(object_id, owner.worker_id)
    releases = 0

    def rpc(_address: object, handler: str, request: object) -> object:
        nonlocal releases
        if handler == "retain_owned_object_for_task":
            # Commit at the owner, then lose every acknowledgement.
            owner.retain_owned_object_for_task(request)
            raise RuntimeError("all retain ACKs lost")
        if handler == "release_owned_object_for_task":
            releases += 1
            if releases == 1:
                raise RuntimeError("first release ACK lost")
            return owner.release_owned_object_for_task(request)
        return _owner_rpc(owner, handler, request)

    monkeypatch.setattr(core, "_borrow_rpc", rpc)
    with pytest.raises(RuntimeError, match="retain ACKs lost"):
        core._register_submission(
            core.define_remote_function(lambda value: value),
            (foreign,), {}, ResourceVector(),
        )
    assert len(core._orphan_foreign_guard_releases) == 1
    assert owner.owner_table.snapshot(object_id).retained_tokens
    core._retry_orphan_foreign_guard_releases()
    assert not core._orphan_foreign_guard_releases
    assert not owner.owner_table.snapshot(object_id).retained_tokens
    assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_foreign_pending_stays_off_lane_then_inline_rewrites_and_executes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core(lanes=1)
    owner, object_id, attempt, _ = _owner_core(core.worker_id)
    foreign = _foreign_ref(object_id, owner.worker_id)
    monkeypatch.setattr(
        core, "_borrow_rpc",
        lambda _address, handler, request: _owner_rpc(owner, handler, request),
    )
    executed: list[_PendingTask] = []

    def execute(pending, prepared, dependencies=(), **_options):
        assert dependencies == ()
        assert isinstance(prepared.args[0], protocol.InlineArg)
        assert cloudpickle.loads(prepared.args[0].data) == 7
        executed.append(pending)
        core._publish_error(
            pending.object_id, pending.spec.attempt_id, RuntimeError("done")
        )
        return True

    monkeypatch.setattr(core, "_execute", execute)
    ref = core.submit(
        core.define_remote_function(lambda value: value),
        (foreign,), {}, ResourceVector(),
    )
    try:
        deadline = time.monotonic() + 1.0
        while ref.object_id.task_id not in core._blocked_tasks:
            assert time.monotonic() < deadline
            time.sleep(0.001)
        assert core._ready_tasks.empty() and executed == []
        owner.owner_table.publish_inline(object_id, attempt, cloudpickle.dumps(7))
        with pytest.raises(RuntimeError, match="done"):
            core.get(ref, timeout=1.0)
        assert len(executed) == 1
    finally:
        ref.close()
        assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_pending_foreign_dependency_does_not_block_independent_ready_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core(lanes=1)
    owner, object_id, attempt, _ = _owner_core(core.worker_id)
    foreign = _foreign_ref(object_id, owner.worker_id)
    monkeypatch.setattr(
        core, "_borrow_rpc",
        lambda _address, handler, request: _owner_rpc(owner, handler, request),
    )
    independent_ran = threading.Event()

    def execute(pending, prepared, dependencies=(), **_options):
        if not pending.foreign_dependency_guards:
            independent_ran.set()
        core._publish_error(
            pending.object_id, pending.spec.attempt_id, RuntimeError("done")
        )
        return True

    monkeypatch.setattr(core, "_execute", execute)
    definition = core.define_remote_function(lambda value=None: value)
    blocked = core.submit(definition, (foreign,), {}, ResourceVector())
    independent = core.submit(definition, (), {}, ResourceVector())
    try:
        assert independent_ran.wait(1.0)
        with pytest.raises(RuntimeError, match="done"):
            core.get(independent, timeout=1.0)
        assert not core.owner_table.snapshot(blocked.object_id).is_ready
        owner.owner_table.publish_error(
            object_id, attempt, RuntimeError("unreachable")
        )
        with pytest.raises(TaskError, match="unreachable"):
            core.get(blocked, timeout=1.0)
    finally:
        blocked.close()
        independent.close()
        assert core.shutdown(timeout=1.0)


@pytest.mark.parametrize(
    ("state", "error_type"),
    [(protocol.OwnedObjectState.ERROR, TaskError),
     (protocol.OwnedObjectState.LOST, SystemTaskError)],
)
@pytest.mark.heavy
def test_foreign_error_or_lost_never_executes_user_path(
    monkeypatch: pytest.MonkeyPatch, state: protocol.OwnedObjectState,
    error_type: type[BaseException],
) -> None:
    core = _runtime_core()
    owner, object_id, attempt, _ = _owner_core(core.worker_id)
    foreign = _foreign_ref(object_id, owner.worker_id)
    if state is protocol.OwnedObjectState.ERROR:
        owner.owner_table.publish_error(object_id, attempt, RuntimeError("producer"))
    monkeypatch.setattr(
        core, "_borrow_rpc",
        lambda _address, handler, request: (
            protocol.GetRetainedOwnedObjectReply(
                request.object_id, request.owner_worker_id,
                request.borrower_worker_id, request.hold, True,
                state=protocol.OwnedObjectState.LOST,
            )
            if handler == "get_retained_owned_object"
            and state is protocol.OwnedObjectState.LOST
            else _owner_rpc(owner, handler, request)
        ),
    )
    monkeypatch.setattr(
        core, "_execute",
        lambda *_args, **_kwargs: pytest.fail("user path must not execute"),
    )
    ref = core.submit(
        core.define_remote_function(lambda value: value),
        (foreign,), {}, ResourceVector(),
    )
    try:
        with pytest.raises(error_type):
            core.get(ref, timeout=1.0)
    finally:
        ref.close()
        assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_retry_keeps_same_foreign_guard_and_input_handle_may_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    owner, object_id, attempt, _ = _owner_core(core.worker_id)
    owner.owner_table.publish_inline(object_id, attempt, cloudpickle.dumps(9))
    foreign = _foreign_ref(
        object_id, owner.worker_id, borrower_core=core
    )
    calls: list[tuple[str, protocol.TaskReferenceHold]] = []

    def rpc(_address: object, handler: str, request: object) -> object:
        calls.append((handler, request.hold))
        return _owner_rpc(owner, handler, request)

    monkeypatch.setattr(core, "_borrow_rpc", rpc)

    def reference_rpc(
        _address: object, handler: str, request: object, **_kwargs: object
    ) -> object:
        if handler == "release_borrowed_object":
            return owner.release_borrowed_reference(request)
        return _owner_rpc(owner, handler, request)

    monkeypatch.setattr("miniray.core.rpc_request", reference_rpc)
    monkeypatch.setattr(
        core, "_borrow_rpc",
        lambda address, handler, request: (
            reference_rpc(address, handler, request)
            if handler == "release_borrowed_object"
            else rpc(address, handler, request)
        ),
    )
    pending, result_ref = core._register_submission(
        core.define_remote_function(lambda value: value),
        (foreign,), {}, ResourceVector(), max_retries=1,
    )
    guard = pending.foreign_dependency_guards[0]
    expected_hold = _retained_hold(core.worker_id, pending.spec.attempt_id)
    assert guard.hold == expected_hold
    assert calls[0] == ("retain_owned_object_for_task", expected_hold)
    foreign.close()
    retried = replace(
        pending, spec=replace(
            pending.spec, attempt_id=pending.spec.attempt_id.next()
        )
    )
    assert retried.foreign_dependency_guards == (guard,)
    assert retried.foreign_dependency_guards[0].hold == expected_hold
    assert core._dependencies_ready(retried)
    prepared, dependencies, _ = core._prepare_task_dependencies(
        retried.spec, retried.foreign_dependency_guards
    )
    assert dependencies == ()
    assert cloudpickle.loads(prepared.args[0].data) == 9
    assert [name for name, _ in calls].count("retain_owned_object_for_task") == 1
    core._publish_error(
        pending.object_id, pending.spec.attempt_id, RuntimeError("cleanup")
    )
    assert core._finish_pending_task(pending)
    result_ref.close()
    assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_finish_is_once_and_partial_multi_guard_retry_skips_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    first, first_ref = core._register_submission(
        core.define_remote_function(lambda: None), (), {}, ResourceVector()
    )
    second, second_ref = core._register_submission(
        core.define_remote_function(lambda: None), (), {}, ResourceVector()
    )
    hold = _retained_hold(core.worker_id, first.spec.attempt_id)
    guards = tuple(
        _ForeignDependencyGuard(
            _object(index)[0], WorkerID.random(), ("127.0.0.1", 26004 + index),
            core.worker_id, "borrow", hold,
        )
        for index in range(2)
    )
    first = replace(first, foreign_dependency_guards=guards)
    with core._state_lock:
        core._accepted_task_count = 2
    calls: list[tuple[ObjectID, protocol.TaskReferenceHold]] = []
    fail_second = True

    def release(guard: _ForeignDependencyGuard) -> bool:
        nonlocal fail_second
        calls.append((guard.object_id, guard.hold))
        if guard is guards[1] and fail_second:
            fail_second = False
            raise RuntimeError("lost ACK")
        return True

    monkeypatch.setattr(core, "_release_foreign_dependency_guard", release)
    assert not core._finish_pending_task(first)
    assert core._accepted_task_count == 2
    assert core._finish_pending_task(first)
    assert calls == [
        (guards[0].object_id, hold),
        (guards[1].object_id, hold),
        (guards[1].object_id, hold),
    ]
    assert core._accepted_task_count == 1
    assert core._finish_pending_task(first)
    assert core._accepted_task_count == 1
    assert core._finish_pending_task(second)
    assert core._accepted_task_count == 0
    first_ref.close()
    second_ref.close()
    assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_finish_claim_and_protocol_send_are_mutually_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    pending, ref = core._register_submission(
        core.define_remote_function(lambda: None), (), {}, ResourceVector()
    )
    guard = _ForeignDependencyGuard(
        _object()[0], WorkerID.random(), ("127.0.0.1", 26010),
        core.worker_id, "borrow",
        _retained_hold(core.worker_id, pending.spec.attempt_id),
    )
    pending = replace(pending, foreign_dependency_guards=(guard,))
    with core._state_lock:
        core._accepted_task_count = 1
    entered = threading.Event()
    allow_release = threading.Event()
    monkeypatch.setattr(
        core, "_release_foreign_dependency_guard",
        lambda _guard: (entered.set(), allow_release.wait(1.0), True)[2],
    )
    result: list[bool] = []
    thread = threading.Thread(
        target=lambda: result.append(core._finish_pending_task(pending))
    )
    thread.start()
    try:
        assert entered.wait(1.0)
        with pytest.raises(_TaskFinishing):
            core._mark_protocol_unresolved(pending, "push_send")
        assert pending.object_id not in core._protocol_unresolved
        allow_release.set()
        thread.join(1.0)
        assert result == [True]
    finally:
        allow_release.set()
        thread.join(1.0)
        ref.close()
        assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_execute_cannot_send_after_finish_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    pending, ref = core._register_submission(
        core.define_remote_function(lambda: None), (), {}, ResourceVector()
    )
    with core._state_lock:
        core._accepted_task_count = 1
        core._finishing_tasks.add(pending.task_id)
    sends: list[object] = []
    publications: list[object] = []
    monkeypatch.setattr(
        core, "_request_lease_hop",
        lambda request: sends.append(request),
    )
    monkeypatch.setattr(
        core, "_publish_error",
        lambda *args: publications.append(args) or True,
    )
    assert core._execute(pending, pending.spec)
    assert sends == []
    assert publications == []
    with core._state_lock:
        core._finishing_tasks.discard(pending.task_id)
    assert core._finish_pending_task(pending)
    ref.close()
    assert core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_existing_protocol_fence_prevents_finish_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _runtime_core()
    pending, ref = core._register_submission(
        core.define_remote_function(lambda: None), (), {}, ResourceVector()
    )
    guard = _ForeignDependencyGuard(
        _object()[0], WorkerID.random(), ("127.0.0.1", 26011),
        core.worker_id, "borrow",
        _retained_hold(core.worker_id, pending.spec.attempt_id),
    )
    pending = replace(pending, foreign_dependency_guards=(guard,))
    with core._state_lock:
        core._accepted_task_count = 1
    core._mark_protocol_unresolved(pending, "push_send")
    monkeypatch.setattr(
        core, "_release_foreign_dependency_guard",
        lambda _guard: pytest.fail("unresolved task must not release guard"),
    )
    assert not core._finish_pending_task(pending)
    core._clear_protocol_unresolved(pending)
    monkeypatch.setattr(
        core, "_release_foreign_dependency_guard", lambda _guard: True,
    )
    assert core._finish_pending_task(pending)
    ref.close()
    assert core.shutdown(timeout=1.0)

@pytest.mark.heavy
def test_owner_shutdown_waits_for_active_retained_hold_then_closes() -> None:
    core = _runtime_core()
    object_id, attempt = _object()
    borrower = WorkerID.random()
    core.owner_table.register(object_id, current_attempt=attempt)
    core.owner_table.add_borrowed_reference(object_id, (borrower, "borrow"))
    request = _retain(core, object_id, borrower)
    assert core.retain_owned_object_for_task(request).accepted
    assert not core.shutdown(timeout=0.05)
    assert core._owner_protocol_open
    query = core.get_retained_owned_object(
        protocol.GetRetainedOwnedObject(
            object_id, core.worker_id, borrower, request.hold
        )
    )
    assert query.accepted and query.state is protocol.OwnedObjectState.PENDING
    assert core.release_owned_object_for_task(
        protocol.ReleaseOwnedObjectForTask(
            object_id, core.worker_id, borrower, request.hold
        )
    ).accepted
    assert core.owner_table.release_borrowed_reference(
        object_id, (borrower, "borrow")
    )
    assert core.shutdown(timeout=1.0)
