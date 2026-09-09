"""Pure single-output reconstruction reducer contracts; no runtime effects."""

from __future__ import annotations

from dataclasses import replace

import pytest

from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.protocol import (
    FunctionKey, InlineArg, NestedReferenceTransfer, RefArg, TaskReferenceHold,
    TaskReferenceHoldKind, TaskSpec,
)
from miniray.reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionDisposition, ReconstructionRuntimeError,
)
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _lost_runtime_for_spec(spec: TaskSpec):
    output_ids = spec.return_ids()
    object_id = output_ids[0]
    recovery = RecoveryManager()
    recovery.register_task(spec, max_retries=spec.max_retries)
    recovery.record_task_success(spec.task_id, spec.attempt_id)
    owner = ObjectOwnerTable()
    owner.register_task_outputs(spec)
    assert len(output_ids) == 1
    assert owner.publish_stored(object_id, spec.attempt_id, NodeID.random())
    assert owner.mark_lost(object_id, spec.attempt_id)
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
    assert outcome.admission is None  # The reducer plan is not an accepted Core queue receipt.
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
    assert outcome.admission is None


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


def test_equivalent_request_joins_without_second_plan_or_budget_charge() -> None:
    coordinator, recovery, owner, spec, object_id = _runtime()
    first = coordinator.request(object_id)
    assert first.disposition is ReconstructionDisposition.START
    record = replace(recovery.task_record(spec.task_id))
    snapshot = owner.snapshot(object_id)
    joined = coordinator.request(object_id)
    assert recovery.task_record(spec.task_id) == record
    assert owner.snapshot(object_id) == snapshot
    assert record.retries_started == 1
    assert joined.disposition is ReconstructionDisposition.JOIN
    assert joined.plan is None
    assert joined.decision.attempt_id == first.plan.attempt_id


def test_retry_budget_exhaustion_returns_failed_without_owner_advance() -> None:
    coordinator, recovery, owner, spec, object_id = _runtime(max_retries=0)
    before = replace(recovery.task_record(spec.task_id))
    outcome = coordinator.request(object_id)
    assert outcome.disposition is ReconstructionDisposition.FAILED
    assert outcome.plan is None
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == before.current_attempt
    assert record.retries_started == before.retries_started == 0
    assert record.state is TaskState.SYSTEM_FAILED
    assert recovery.active_recovery(spec.task_id) is None
    assert not coordinator._sessions
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


def test_owner_cas_rejection_does_not_commit_recovery_or_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, recovery, owner, spec, object_id = _runtime()
    before = tuple(owner.snapshot(value) for value in spec.return_ids())
    monkeypatch.setattr(owner, "commit_advance_task_outputs", lambda plan: False)

    with pytest.raises(
        ReconstructionRuntimeError, match="output CAS"
    ):
        coordinator.request(object_id)

    assert tuple(owner.snapshot(value) for value in spec.return_ids()) == before
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id
    assert record.retries_started == 0
    assert record.state.name == "SUCCEEDED"
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions


def test_owner_cas_exception_does_not_commit_recovery_or_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, recovery, owner, spec, object_id = _runtime()
    before = tuple(owner.snapshot(value) for value in spec.return_ids())

    def fail(_plan: object) -> bool:
        raise RuntimeError("injected owner commit failure")

    monkeypatch.setattr(owner, "commit_advance_task_outputs", fail)
    with pytest.raises(RuntimeError, match="injected owner commit failure"):
        coordinator.request(object_id)

    assert tuple(owner.snapshot(value) for value in spec.return_ids()) == before
    record = recovery.task_record(spec.task_id)
    assert record.current_attempt == spec.attempt_id
    assert record.retries_started == 0
    assert record.state.name == "SUCCEEDED"
    assert recovery.active_recovery(spec.task_id) is None
    assert spec.task_id not in coordinator._sessions
