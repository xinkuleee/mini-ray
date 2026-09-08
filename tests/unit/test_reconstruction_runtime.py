from __future__ import annotations

import pytest

from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.protocol import (
    FunctionKey, InlineArg, NestedReferenceTransfer, RefArg, TaskReferenceHold,
    StoredArg, TaskReferenceHoldKind, TaskSpec,
)
from miniray.reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionDisposition, ReconstructionRuntimeError,
)
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _lost_runtime_for_spec(
    spec: TaskSpec, *, lost_ids: tuple[ObjectID, ...] | None = None
):
    output_ids = spec.return_ids()
    object_id = output_ids[0]
    recovery = RecoveryManager()
    recovery.register_task(spec, max_retries=spec.max_retries)
    recovery.record_task_success(spec.task_id, spec.attempt_id)
    owner = ObjectOwnerTable()
    owner.register_task_outputs(spec)
    lost = set(output_ids if lost_ids is None else lost_ids)
    for output_id in output_ids:
        owner.publish_stored(output_id, spec.attempt_id, NodeID.random())
        if output_id in lost:
            owner.mark_lost(output_id, spec.attempt_id)
    return ReconstructionCoordinator(recovery, owner), recovery, owner, object_id


def _runtime(*, max_retries: int = 1, dependency: bool = False):
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    object_id = ObjectID.for_task(task)
    owner_id = WorkerID.random()
    args = (InlineArg(b"value"),)
    if dependency:
        dep_task = TaskID.derive(job, TaskID.for_driver(job), 1)
        args = (RefArg(ObjectID.for_task(dep_task), owner_id),)
    spec = TaskSpec(
        job, task, attempt, FunctionKey(job, "m", "f", "v1"), args, 1,
        ResourceVector({"CPU": 1}), owner_id, max_retries=max_retries
    )
    coordinator, recovery, owner, object_id = _lost_runtime_for_spec(spec)
    return coordinator, recovery, owner, spec, object_id


def test_start_plan_preserves_logical_ids_and_advances_attempt_once() -> None:
    coordinator, _recovery, owner, spec, object_id = _runtime()
    outcome = coordinator.request(object_id)
    assert outcome.disposition is ReconstructionDisposition.START
    plan = outcome.plan
    assert plan is not None
    assert plan.task_id == spec.task_id
    assert plan.object_id == object_id
    assert plan.task_spec.task_id == spec.task_id
    assert plan.task_spec.return_ids() == (object_id,)
    assert plan.attempt_id == spec.attempt_id.next()
    assert plan.clear_descriptor_ids == plan.clear_waiter_ids == (object_id,)
    assert plan.accepted_count_delta == 1
    snapshot = owner.snapshot(object_id)
    assert snapshot.state is ObjectState.PENDING
    assert snapshot.current_attempt == plan.attempt_id
    assert not owner.publish_stored(object_id, spec.attempt_id, NodeID.random())


def test_prepare_is_side_effect_free_until_explicit_commit() -> None:
    coordinator, recovery, owner, spec, object_id = _runtime()

    prepared = coordinator.prepare(object_id)

    assert prepared.outcome.disposition is ReconstructionDisposition.START
    assert prepared.outcome.plan is not None
    assert owner.snapshot(object_id).state is ObjectState.LOST
    assert owner.snapshot(object_id).current_attempt == spec.attempt_id
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id
    assert record.retries_started == 0
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions

    outcome = coordinator.commit_prepared(prepared)
    assert outcome == prepared.outcome
    assert owner.snapshot(object_id).state is ObjectState.PENDING
    assert recovery.active_recovery(spec.task_id) == outcome.plan.attempt_id


def test_commit_prepared_rejects_changed_authority_without_budget_use() -> None:
    coordinator, recovery, owner, spec, object_id = _runtime()
    prepared = coordinator.prepare(object_id)
    # A concurrent owner transition invalidates the exact prepared CAS.
    owner.add_location(object_id, spec.attempt_id, NodeID.random())

    with pytest.raises(Exception):
        coordinator.commit_prepared(prepared)

    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id
    assert record.retries_started == 0
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions


def test_concurrent_equivalent_request_joins_without_second_plan() -> None:
    coordinator, _recovery, _owner, _spec, object_id = _runtime()
    first = coordinator.request(object_id)
    assert first.disposition is ReconstructionDisposition.START
    joined = coordinator.request(object_id)
    assert joined.disposition is ReconstructionDisposition.JOIN
    assert joined.plan is None
    assert joined.decision.attempt_id == first.plan.attempt_id


def test_retry_budget_exhaustion_returns_failed_without_owner_advance() -> None:
    coordinator, _recovery, owner, spec, object_id = _runtime(max_retries=0)
    outcome = coordinator.request(object_id)
    assert outcome.disposition is ReconstructionDisposition.FAILED
    assert outcome.plan is None
    snapshot = owner.snapshot(object_id)
    assert snapshot.state is ObjectState.LOST
    assert snapshot.current_attempt == spec.attempt_id


def test_single_producer_request_preserves_local_dependencies_for_core_gate() -> None:
    coordinator, recovery, owner, spec, object_id = _runtime(dependency=True)
    outcome = coordinator.request(object_id)
    assert outcome.disposition is ReconstructionDisposition.START
    assert outcome.plan is not None
    assert outcome.plan.protected_dependencies == (spec.args[0].object_id,)
    assert outcome.plan.dependency_hold == TaskReferenceHold(
        TaskReferenceHoldKind.SUBMITTED,
        spec.owner_worker_id,
        spec.task_id,
        outcome.plan.attempt_id,
    )
    assert (
        outcome.plan.dependency_hold.origin_attempt_id
        == spec.attempt_id.next()
    )
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id.next()
    assert record.retries_started == 1
    assert recovery.active_recovery(spec.task_id) == outcome.plan.attempt_id
    snapshot = owner.snapshot(object_id)
    assert snapshot.state is ObjectState.PENDING
    assert snapshot.current_attempt == outcome.plan.attempt_id


def test_request_rewrites_local_nested_manifest_to_fresh_hold() -> None:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    owner_id = WorkerID.random()
    nested_task = TaskID.derive(job, TaskID.for_driver(job), 12)
    nested_id = ObjectID.for_task(nested_task)
    original_hold = TaskReferenceHold(
        TaskReferenceHoldKind.SUBMITTED, owner_id, task, attempt,
    )
    transfer = NestedReferenceTransfer(
        nested_id, owner_id, ("127.0.0.1", 24567), original_hold,
    )
    spec = TaskSpec(
        job, task, attempt, FunctionKey(job, "m", "f", "v1"),
        (InlineArg(b"nested", nested_refs=(transfer,)),), 1,
        ResourceVector({"CPU": 1}), owner_id, max_retries=1,
    )
    coordinator, _recovery, owner, object_id = _lost_runtime_for_spec(spec)
    owner.register(nested_id, current_attempt=AttemptID(nested_task, 0))

    outcome = coordinator.request(object_id)

    assert outcome.disposition is ReconstructionDisposition.START
    assert outcome.plan is not None
    plan = outcome.plan
    assert plan.protected_dependencies == ()
    assert plan.nested_local_holds == (nested_id,)
    assert plan.dependency_hold.origin_attempt_id == plan.attempt_id
    argument = plan.task_spec.args[0]
    assert isinstance(argument, InlineArg)
    assert argument.nested_refs[0].hold == plan.dependency_hold
    assert argument.nested_refs[0].hold != original_hold


def test_stored_arg_reconstruction_rewrites_hold_and_keeps_storage_dependency() -> None:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    owner_id = WorkerID.random()
    storage_task = TaskID.derive(job, TaskID.for_driver(job), 10)
    storage_id = ObjectID.for_task(storage_task)
    nested_task = TaskID.derive(job, TaskID.for_driver(job), 11)
    nested_id = ObjectID.for_task(nested_task)
    original_hold = TaskReferenceHold(
        TaskReferenceHoldKind.SUBMITTED, owner_id, task, attempt,
    )
    transfer = NestedReferenceTransfer(
        nested_id, owner_id, ("127.0.0.1", 24568), original_hold,
    )
    stored = StoredArg(
        storage_id, owner_id, "cloudpickle", (transfer,)
    )
    spec = TaskSpec(
        job, task, attempt, FunctionKey(job, "m", "f", "v1"),
        (stored,), 1, ResourceVector({"CPU": 1}), owner_id, max_retries=1,
    )
    coordinator, _recovery, owner, object_id = _lost_runtime_for_spec(spec)
    owner.register(storage_id, current_attempt=AttemptID(storage_task, 0))
    owner.register(nested_id, current_attempt=AttemptID(nested_task, 0))

    outcome = coordinator.request(object_id)

    assert outcome.disposition is ReconstructionDisposition.START
    assert outcome.plan is not None
    plan = outcome.plan
    assert plan.protected_dependencies == (storage_id,)
    assert plan.nested_local_holds == (nested_id,)
    rewritten = plan.task_spec.args[0]
    assert isinstance(rewritten, StoredArg)
    assert rewritten.object_id == storage_id
    assert rewritten.serializer == "cloudpickle"
    assert rewritten.nested_refs[0].hold == plan.dependency_hold
    assert rewritten.nested_refs[0].hold != original_hold


def _multi_spec(*, max_retries: int = 2) -> TaskSpec:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    return TaskSpec(
        job, task, AttemptID(task, 0),
        FunctionKey(job, "m", "multi", "v1"),
        (InlineArg(b"value"),), 3, ResourceVector({"CPU": 1}),
        WorkerID.random(), max_retries=max_retries,
    )


def test_multi_return_request_from_nonzero_sibling_advances_whole_manifest() -> None:
    spec = _multi_spec()
    coordinator, recovery, owner, _ = _lost_runtime_for_spec(spec)
    requested = spec.return_ids()[2]

    outcome = coordinator.request(requested)

    assert outcome.disposition is ReconstructionDisposition.START
    assert outcome.plan is not None
    plan = outcome.plan
    assert plan.requested_object_id == requested
    assert plan.object_id == spec.return_ids()[0]
    assert plan.output_ids == spec.return_ids()
    assert plan.target_output_ids == spec.return_ids()
    assert plan.clear_descriptor_ids == spec.return_ids()
    assert plan.clear_waiter_ids == spec.return_ids()
    assert plan.task_spec.return_ids() == spec.return_ids()
    assert all(
        owner.snapshot(object_id).state is ObjectState.PENDING
        and owner.snapshot(object_id).current_attempt == plan.attempt_id
        for object_id in spec.return_ids()
    )
    assert recovery.active_recovery(spec.task_id) == plan.attempt_id

    joined = coordinator.request(spec.return_ids()[0])
    assert joined.disposition is ReconstructionDisposition.JOIN
    assert joined.decision.requested_object_id == spec.return_ids()[0]
    assert joined.decision.attempt_id == plan.attempt_id
    assert joined.decision.output_ids == spec.return_ids()


def test_partial_multi_return_loss_is_rejected_before_any_mutation() -> None:
    spec = _multi_spec()
    lost = (spec.return_ids()[1],)
    coordinator, recovery, owner, _ = _lost_runtime_for_spec(
        spec, lost_ids=lost
    )
    before = tuple(owner.snapshot(value) for value in spec.return_ids())

    with pytest.raises(
        ReconstructionRuntimeError, match="every producer output to be LOST"
    ):
        coordinator.request(lost[0])

    after = tuple(owner.snapshot(value) for value in spec.return_ids())
    assert after == before
    assert recovery.task_record(spec.task_id).retries_started == 0
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions


def test_owner_batch_rejection_does_not_commit_recovery_or_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _multi_spec()
    coordinator, recovery, owner, _ = _lost_runtime_for_spec(spec)
    before = tuple(owner.snapshot(value) for value in spec.return_ids())
    monkeypatch.setattr(owner, "commit_advance_task_outputs", lambda plan: False)

    with pytest.raises(
        ReconstructionRuntimeError, match="output-batch CAS"
    ):
        coordinator.request(spec.return_ids()[1])

    assert tuple(owner.snapshot(value) for value in spec.return_ids()) == before
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id
    assert record.retries_started == 0
    assert record.state.name == "SUCCEEDED"
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions


def test_owner_batch_exception_does_not_commit_recovery_or_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _multi_spec()
    coordinator, recovery, owner, _ = _lost_runtime_for_spec(spec)
    before = tuple(owner.snapshot(value) for value in spec.return_ids())

    def fail(_plan: object) -> bool:
        raise RuntimeError("injected owner commit failure")

    monkeypatch.setattr(owner, "commit_advance_task_outputs", fail)
    with pytest.raises(RuntimeError, match="injected owner commit failure"):
        coordinator.request(spec.return_ids()[2])

    assert tuple(owner.snapshot(value) for value in spec.return_ids()) == before
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id
    assert record.retries_started == 0
    assert record.state.name == "SUCCEEDED"
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions
