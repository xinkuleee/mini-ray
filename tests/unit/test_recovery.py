from __future__ import annotations

from dataclasses import dataclass

from miniray.errors import (
    SystemTaskError,
    TaskError,
    UnreconstructableObjectError,
)
from miniray.ids import AttemptID, JobID, ObjectID, TaskID
from miniray.recovery import (
    FailureKind,
    RecoveryAction,
    RecoveryManager,
    TaskRecord,
    TaskState,
)

import pytest

pytestmark = pytest.mark.unit


def opaque_id(id_type, byte: int = 1):
    return id_type(bytes([byte]) * 16)


def task_id(byte: int = 1) -> TaskID:
    job = opaque_id(JobID, byte)
    return TaskID.derive(job, TaskID.for_driver(job), 0)


@dataclass(frozen=True)
class FakeTaskSpec:
    task_id: TaskID
    attempt_id: AttemptID
    outputs: tuple[ObjectID, ...]
    payload: str = "producer"

    def return_ids(self) -> tuple[ObjectID, ...]:
        return self.outputs


def producer_spec(*, returns: int = 1) -> FakeTaskSpec:
    tid = task_id()
    return FakeTaskSpec(
        tid,
        AttemptID(tid, 0),
        tuple(ObjectID.for_task(tid, index) for index in range(returns)),
    )


def test_application_failure_is_terminal_and_does_not_consume_retry_budget() -> None:
    tid = task_id()
    attempt = AttemptID(tid, 0)
    record = TaskRecord(tid, attempt, max_retries=3)

    decision = record.record_failure(attempt, TaskError("bad user code"))

    assert decision.action is RecoveryAction.FAIL_APPLICATION
    assert decision.failure_kind is FailureKind.APPLICATION
    assert record.current_attempt == attempt
    assert record.retries_started == 0
    assert record.retries_remaining == 3
    assert record.state is TaskState.APPLICATION_FAILED


def test_system_failure_advances_attempt_and_fences_late_old_results() -> None:
    tid = task_id()
    attempt_0 = AttemptID(tid, 0)
    record = TaskRecord(tid, attempt_0, max_retries=2)

    retry = record.record_failure(attempt_0, SystemTaskError("worker died"))
    stale_success = record.record_success(attempt_0)
    stale_unknown_failure = record.record_failure(attempt_0, object())

    assert retry.action is RecoveryAction.RETRY_TASK
    assert retry.attempt_id == AttemptID(tid, 1)
    assert record.current_attempt == AttemptID(tid, 1)
    assert stale_success.action is RecoveryAction.FENCE_STALE_ATTEMPT
    assert stale_unknown_failure.action is RecoveryAction.FENCE_STALE_ATTEMPT
    assert stale_success.is_fenced
    assert record.state is TaskState.RETRY_PENDING


def test_system_retry_budget_counts_attempts_after_the_initial_attempt() -> None:
    tid = task_id()
    attempt_0 = AttemptID(tid, 0)
    record = TaskRecord(tid, attempt_0, max_retries=1)

    first = record.record_failure(attempt_0, FailureKind.SYSTEM)
    exhausted = record.record_failure(first.attempt_id, FailureKind.SYSTEM)

    assert first.action is RecoveryAction.RETRY_TASK
    assert exhausted.action is RecoveryAction.FAIL_RETRY_EXHAUSTED
    assert exhausted.attempt_id == AttemptID(tid, 1)
    assert record.retries_started == 1
    assert record.retries_remaining == 0
    assert record.state is TaskState.SYSTEM_FAILED


def test_reconstruction_keeps_logical_id_and_joins_the_same_output() -> None:
    spec = producer_spec(returns=1)
    manager = RecoveryManager()
    manager.register_task(spec, max_retries=2)
    manager.commit_transition(manager.validate_task_success(spec.task_id, spec.attempt_id))
    start = manager.commit_transition(manager.validate_request_reconstruction(spec.outputs[0]))
    joined = manager.commit_transition(manager.validate_request_reconstruction(spec.outputs[0]))
    assert start.action is RecoveryAction.START_RECONSTRUCTION and start.should_submit
    assert start.task_id == spec.task_id and start.output_ids == spec.outputs
    assert start.attempt_id == AttemptID(spec.task_id, 1)
    assert start.producer_task_spec is spec
    assert joined.action is RecoveryAction.JOIN_RECONSTRUCTION and not joined.should_submit
    assert joined.task_id == start.task_id and joined.attempt_id == start.attempt_id
    assert manager.active_recovery(spec.task_id) == start.attempt_id
    assert manager.task_record(spec.task_id).retries_started == 1


def test_stale_reconstruction_completion_cannot_clear_the_current_recovery() -> None:
    spec = producer_spec(returns=1)
    manager = RecoveryManager()
    manager.register_producer(spec, max_retries=2)
    manager.record_task_success(spec.task_id, spec.attempt_id)
    first_recovery = manager.request_reconstruction(spec.outputs[0])

    second_recovery = manager.record_task_failure(
        spec.task_id,
        first_recovery.attempt_id,
        SystemTaskError("recovery worker died"),
    )
    stale = manager.record_task_success(
        spec.task_id, first_recovery.attempt_id
    )

    assert second_recovery.action is RecoveryAction.RETRY_TASK
    assert second_recovery.attempt_id == AttemptID(spec.task_id, 2)
    assert stale.action is RecoveryAction.FENCE_STALE_ATTEMPT
    assert manager.active_recovery(spec.task_id) == second_recovery.attempt_id


def test_single_output_terminal_reconstruction_failure_keeps_budget_and_fences_late_attempt() -> None:
    from dataclasses import replace
    from miniray import protocol
    import hashlib
    from miniray.ids import NodeID, WorkerID
    from miniray.resources import ResourceVector
    from miniray.ownership import ObjectOwnerTable, ObjectState
    from miniray.task_outputs import TaskExecutionKey

    job = opaque_id(JobID, 7)
    tid = TaskID.derive(job, TaskID.for_driver(job), 0)
    spec = protocol.TaskSpec(job, tid, AttemptID(tid, 0),
        protocol.FunctionKey(job, __name__, "single_recovery", "1"), (), 1,
        ResourceVector(), opaque_id(WorkerID, 8), max_retries=3)
    output, = spec.return_ids()
    execution = TaskExecutionKey.from_task_spec(spec)
    manager, owner = RecoveryManager(), ObjectOwnerTable()
    manager.register_task(spec, max_retries=3)
    owner.register_task_outputs(spec, local_tokens=("live",))
    node = opaque_id(NodeID, 9)
    descriptor = protocol.ResultDescriptor(output, protocol.ResultStorage.OBJECT_STORE, 1,
        spec.owner_worker_id, node, hashlib.sha256(b"x").hexdigest())
    assert owner.publish_stored(output, spec.attempt_id, node, descriptor=descriptor)
    assert owner.mark_lost(output, spec.attempt_id)
    manager.commit_transition(manager.validate_task_success(tid, spec.attempt_id))
    with owner._lock:
        start = manager.validate_request_reconstruction(output)
        advance = owner.validate_advance_task_outputs(execution, start.decision.attempt_id)
        assert advance is not None
        owner.commit_validated_advance_task_outputs(advance)
        manager.commit_validated_transition(start)
    attempt = start.decision.attempt_id
    current_execution = TaskExecutionKey(execution.manifest, attempt)
    before = replace(manager.task_record(tid))
    error = SystemTaskError("current reconstruction cannot continue")
    with owner._lock:
        plan = manager.validate_terminal_system_failure(tid, attempt, error)
        owner_plan = owner.validate_publish_task_error(current_execution, error)
        assert manager.task_record(tid) == before
        assert owner.snapshot(output).state is ObjectState.PENDING
        assert plan.decision.action is RecoveryAction.FAIL_RETRY_EXHAUSTED
        assert owner_plan is not None
        owner.commit_validated_publish_task_error(owner_plan)
        manager.commit_validated_transition(plan)
    record = replace(manager.task_record(tid))
    assert record.state is TaskState.SYSTEM_FAILED and record.current_attempt == attempt
    assert record.retries_started == 1 and record.retries_remaining == 2
    assert manager.active_recovery(tid) is None
    assert owner.snapshot(output).state is ObjectState.ERROR
    assert owner.snapshot(output).error is error
    assert manager.lineage_for_object(output).task_spec is spec
    stale = manager.validate_terminal_system_failure(tid, spec.attempt_id, SystemTaskError("late"))
    assert stale.decision.action is RecoveryAction.FENCE_STALE_ATTEMPT
    manager.commit_transition(stale)
    assert manager.task_record(tid) == record
    assert owner.validate_publish_task_error(execution, SystemTaskError("late")) is None
    repeated = manager.validate_request_reconstruction(output).decision
    assert repeated.action is RecoveryAction.FAIL_RETRY_EXHAUSTED
    assert not repeated.should_submit and repeated.output_ids == (output,)


def test_commit_validated_transition_is_assignment_only() -> None:
    spec = producer_spec(returns=1)
    manager = RecoveryManager()
    manager.register_task(spec, max_retries=1)
    plan = manager.validate_task_success(spec.task_id, spec.attempt_id)

    decision = manager.commit_validated_transition(plan)

    assert decision is plan.decision
    assert manager.task_record(spec.task_id).state is TaskState.SUCCEEDED


def test_put_object_returns_explicit_unreconstructable_decision() -> None:
    put_object = ObjectID.for_task(task_id(9), 0)
    manager = RecoveryManager()
    assert manager.register_put(put_object)

    decision = manager.request_reconstruction(put_object)

    assert decision.action is RecoveryAction.UNRECONSTRUCTABLE_OBJECT
    assert decision.requested_object_id == put_object
    assert isinstance(decision.error, UnreconstructableObjectError)
    assert not decision.should_submit


def test_exhausted_reconstruction_returns_a_stable_explicit_decision() -> None:
    spec = producer_spec(returns=1)
    manager = RecoveryManager()
    manager.register_task(spec, max_retries=0)
    manager.record_task_success(spec.task_id, spec.attempt_id)

    first = manager.request_reconstruction(spec.outputs[0])
    repeated = manager.request_reconstruction(spec.outputs[0])

    assert first.action is RecoveryAction.FAIL_RETRY_EXHAUSTED
    assert repeated.action is RecoveryAction.FAIL_RETRY_EXHAUSTED
    assert first.attempt_id == repeated.attempt_id == spec.attempt_id
    assert first.output_ids == repeated.output_ids == spec.outputs
    assert not first.should_submit


@pytest.mark.parametrize(
    "transition", ["success", "application", "retry", "terminal"]
)
def test_transition_validation_is_side_effect_free_until_commit(
    transition: str,
) -> None:
    spec = producer_spec(returns=1)
    manager = RecoveryManager()
    manager.register_task(spec, max_retries=2)
    before = manager.task_record(spec.task_id)
    before_values = (
        before.current_attempt, before.retries_started, before.state,
        before.last_error, manager.active_recovery(spec.task_id),
    )

    if transition == "success":
        plan = manager.validate_task_success(spec.task_id, spec.attempt_id)
    elif transition == "application":
        plan = manager.validate_task_failure(
            spec.task_id, spec.attempt_id, FailureKind.APPLICATION,
            error=TaskError("user"),
        )
    elif transition == "retry":
        plan = manager.validate_task_failure(
            spec.task_id, spec.attempt_id, FailureKind.SYSTEM,
            error=SystemTaskError("system"),
        )
    else:
        plan = manager.validate_terminal_system_failure(
            spec.task_id, spec.attempt_id, SystemTaskError("terminal")
        )

    current = manager.task_record(spec.task_id)
    assert (
        current.current_attempt, current.retries_started, current.state,
        current.last_error, manager.active_recovery(spec.task_id),
    ) == before_values
    assert manager.commit_transition(plan) is plan.decision
    committed = manager.task_record(spec.task_id)
    assert (
        committed.current_attempt, committed.retries_started, committed.state,
        committed.last_error, manager.active_recovery(spec.task_id),
    ) == (
        plan.after.current_attempt, plan.after.retries_started, plan.after.state,
        plan.after.last_error, plan.active_after,
    )
