"""Pure DFS preflight contracts for recursive local lineage."""

from __future__ import annotations

from dataclasses import replace

import pytest

pytestmark = pytest.mark.unit

from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.protocol import (
    FunctionKey, InlineArg, NestedReferenceTransfer, RefArg,
    TaskReferenceHold, TaskReferenceHoldKind, TaskSpec,
)
from miniray.reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionGraphAction,
    ReconstructionRuntimeError,
)
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector


def _spec(
    job: JobID, owner: WorkerID, task: TaskID, dependencies: tuple[ObjectID, ...]
) -> TaskSpec:
    return TaskSpec(
        job, task, AttemptID(task, 0),
        FunctionKey(job, "graph", task.hex, "v1"),
        tuple(RefArg(object_id, owner) for object_id in dependencies)
        or (InlineArg(b"leaf"),),
        1, ResourceVector(), owner, max_retries=2,
    )


def _register(
    recovery: RecoveryManager, owner_table: ObjectOwnerTable, spec: TaskSpec,
    *, state: str, max_retries: int = 2,
) -> ObjectID:
    object_id = spec.return_ids()[0]
    recovery.register_task(spec, max_retries=max_retries)
    recovery.record_task_success(spec.task_id, spec.attempt_id)
    owner_table.register(
        object_id, current_attempt=spec.attempt_id, producer_task_spec=spec
    )
    owner_table.publish_stored(object_id, spec.attempt_id, NodeID.random())
    if state == "LOST":
        owner_table.mark_lost(object_id, spec.attempt_id)
    elif state == "PENDING":
        decision = recovery.request_reconstruction(object_id)
        assert decision.attempt_id is not None
        owner_table.mark_lost(object_id, spec.attempt_id)
        assert owner_table.advance_attempt(
            object_id, expected_attempt=spec.attempt_id,
            next_attempt=decision.attempt_id,
        )
    elif state != "READY":
        raise ValueError(state)
    return object_id


def test_deep_dag_is_dependency_first_and_deduplicates_shared_child() -> None:
    job, worker = JobID.random(), WorkerID.random()
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    leaf_task = TaskID.derive(job, TaskID.for_driver(job), 0)
    leaf = _register(recovery, owners, _spec(job, worker, leaf_task, ()), state="LOST")
    left_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    left = _register(recovery, owners, _spec(job, worker, left_task, (leaf,)), state="LOST")
    right_task = TaskID.derive(job, TaskID.for_driver(job), 2)
    right = _register(recovery, owners, _spec(job, worker, right_task, (leaf,)), state="LOST")
    root_task = TaskID.derive(job, TaskID.for_driver(job), 3)
    root = _register(recovery, owners, _spec(job, worker, root_task, (left, right)), state="LOST")

    before = {
        task: (recovery.task_record(task).current_attempt,
               recovery.task_record(task).retries_started)
        for task in (leaf_task, left_task, right_task, root_task)
    }
    plan = ReconstructionCoordinator(recovery, owners).preflight_graph(root)

    assert tuple(node.object_id for node in plan.steps) == (leaf, left, right, root)
    assert len([node for node in plan.nodes if node.object_id == leaf]) == 1
    assert all(
        node.action is ReconstructionGraphAction.LOST_RECONSTRUCT
        for node in plan.steps
    )
    assert before == {
        task: (recovery.task_record(task).current_attempt,
               recovery.task_record(task).retries_started)
        for task in before
    }
    assert all(recovery.active_recovery(task) is None for task in before)


def test_ready_dependency_is_audit_node_but_not_execution_step() -> None:
    job, worker = JobID.random(), WorkerID.random()
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    child_task = TaskID.derive(job, TaskID.for_driver(job), 0)
    child = _register(recovery, owners, _spec(job, worker, child_task, ()), state="READY")
    root_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    root = _register(recovery, owners, _spec(job, worker, root_task, (child,)), state="LOST")

    plan = ReconstructionCoordinator(recovery, owners).preflight_graph(root)
    assert plan.node_for(child).action is ReconstructionGraphAction.READY_SKIP
    assert tuple(node.object_id for node in plan.steps) == (root,)


def test_ready_put_dependency_is_skipped_without_producer_lineage() -> None:
    job, worker = JobID.random(), WorkerID.random()
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    put_task = TaskID.derive(job, TaskID.for_driver(job), 8)
    put_id = ObjectID.for_task(put_task)
    recovery.register_put(put_id)
    owners.register(put_id, current_attempt=AttemptID(put_task, 0))
    owners.publish_inline(put_id, AttemptID(put_task, 0), b"put")
    root_task = TaskID.derive(job, TaskID.for_driver(job), 9)
    root = _register(
        recovery, owners, _spec(job, worker, root_task, (put_id,)),
        state="LOST",
    )

    plan = ReconstructionCoordinator(recovery, owners).preflight_graph(root)
    assert plan.node_for(put_id).action is ReconstructionGraphAction.READY_SKIP
    assert plan.node_for(put_id).task_spec is None
    assert tuple(node.object_id for node in plan.steps) == (root,)


def test_pending_active_dependency_is_joined_before_lost_parent() -> None:
    job, worker = JobID.random(), WorkerID.random()
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    child_task = TaskID.derive(job, TaskID.for_driver(job), 0)
    child = _register(recovery, owners, _spec(job, worker, child_task, ()), state="PENDING")
    root_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    root = _register(recovery, owners, _spec(job, worker, root_task, (child,)), state="LOST")

    plan = ReconstructionCoordinator(recovery, owners).preflight_graph(root)
    assert tuple(node.object_id for node in plan.steps) == (child, root)
    assert plan.node_for(child).action is ReconstructionGraphAction.PENDING_JOIN
    assert plan.node_for(child).active_attempt == recovery.active_recovery(child_task)


def test_cycle_is_rejected_without_mutating_recovery() -> None:
    job, worker = JobID.random(), WorkerID.random()
    first_task = TaskID.derive(job, TaskID.for_driver(job), 0)
    second_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    first_id, second_id = ObjectID.for_task(first_task), ObjectID.for_task(second_task)
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    first = _register(recovery, owners, _spec(job, worker, first_task, (second_id,)), state="LOST")
    _register(recovery, owners, _spec(job, worker, second_task, (first_id,)), state="LOST")

    with pytest.raises(ReconstructionRuntimeError, match="cycle"):
        ReconstructionCoordinator(recovery, owners).preflight_graph(first)
    assert recovery.task_record(first_task).retries_started == 0
    assert recovery.task_record(second_task).retries_started == 0


def test_pending_without_active_marker_and_exhausted_lost_budget_are_rejected() -> None:
    job, worker = JobID.random(), WorkerID.random()

    # A plain PENDING object is an executing producer, not a reconstruction
    # session that this graph request may silently join.
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    spec = _spec(job, worker, task, ())
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    recovery.register_task(spec, max_retries=2)
    owners.register(
        spec.return_ids()[0], current_attempt=spec.attempt_id,
        producer_task_spec=spec,
    )
    with pytest.raises(ReconstructionRuntimeError, match="no matching active"):
        ReconstructionCoordinator(recovery, owners).preflight_graph(
            spec.return_ids()[0]
        )
    assert recovery.task_record(task).retries_started == 0

    exhausted_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    exhausted_spec = _spec(job, worker, exhausted_task, ())
    exhausted_recovery, exhausted_owners = RecoveryManager(), ObjectOwnerTable()
    exhausted = _register(
        exhausted_recovery, exhausted_owners, exhausted_spec,
        state="LOST", max_retries=0,
    )
    with pytest.raises(ReconstructionRuntimeError, match="budget is exhausted"):
        ReconstructionCoordinator(
            exhausted_recovery, exhausted_owners
        ).preflight_graph(exhausted)
    record = exhausted_recovery.task_record(exhausted_task)
    assert record.current_attempt == exhausted_spec.attempt_id
    assert record.retries_started == 0
    assert exhausted_recovery.active_recovery(exhausted_task) is None


def test_local_nested_handle_is_lifetime_only_and_not_a_graph_dependency() -> None:
    job, worker = JobID.random(), WorkerID.random()
    nested_task = TaskID.derive(job, TaskID.for_driver(job), 10)
    nested_id = ObjectID.for_task(nested_task)
    producer_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.SUBMITTED, worker, producer_task,
        AttemptID(producer_task, 0),
    )
    transfer = NestedReferenceTransfer(
        nested_id, worker, ("127.0.0.1", 24567), hold
    )
    spec = replace(
        _spec(job, worker, producer_task, ()),
        args=(InlineArg(b"nested", nested_refs=(transfer,)),),
    )
    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    # The LOST nested object deliberately has no RecoveryManager lineage.
    # Planning succeeds only if the handle is validated for lifetime but never
    # visited as a readiness/reconstruction dependency.
    owners.register(
        nested_id, current_attempt=AttemptID(nested_task, 0)
    )
    owners.publish_stored(
        nested_id, AttemptID(nested_task, 0), NodeID.random()
    )
    owners.mark_lost(nested_id, AttemptID(nested_task, 0))
    output = _register(recovery, owners, spec, state="LOST")

    plan = ReconstructionCoordinator(recovery, owners).preflight_graph(output)

    root = plan.node_for(output)
    assert root.dependency_ids == ()
    assert root.nested_local_holds == (nested_id,)
    assert tuple(node.object_id for node in plan.nodes) == (output,)
    assert tuple(node.object_id for node in plan.steps) == (output,)
    assert recovery.task_record(producer_task).retries_started == 0


def test_accepts_foreign_edges_but_rejects_incomplete_multi_and_lost_put_without_budget_use() -> None:
    job, worker = JobID.random(), WorkerID.random()

    def rejected(spec: TaskSpec, pattern: str, *, outputs=None) -> None:
        recovery, owners = RecoveryManager(), ObjectOwnerTable()
        object_id = spec.return_ids()[0]
        recovery.register_task(spec, output_ids=outputs, max_retries=2)
        recovery.record_task_success(spec.task_id, spec.attempt_id)
        owners.register(object_id, current_attempt=spec.attempt_id, producer_task_spec=spec)
        owners.publish_stored(object_id, spec.attempt_id, NodeID.random())
        owners.mark_lost(object_id, spec.attempt_id)
        with pytest.raises(ReconstructionRuntimeError, match=pattern):
            ReconstructionCoordinator(recovery, owners).preflight_graph(object_id)
        assert recovery.task_record(spec.task_id).retries_started == 0

    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    dep = ObjectID.for_task(TaskID.derive(job, TaskID.for_driver(job), 9))
    foreign = replace(_spec(job, worker, task, (dep,)), args=(RefArg(dep, WorkerID.random()),))
    foreign_recovery, foreign_owners = RecoveryManager(), ObjectOwnerTable()
    foreign_output = _register(
        foreign_recovery, foreign_owners, foreign, state="LOST"
    )
    foreign_plan = ReconstructionCoordinator(
        foreign_recovery, foreign_owners
    ).preflight_graph(foreign_output)
    assert foreign_plan.node_for(foreign_output).dependency_ids == ()

    nested_task = TaskID.derive(job, TaskID.for_driver(job), 1)
    nested_dep = ObjectID.for_task(
        TaskID.derive(job, TaskID.for_driver(job), 10)
    )
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, worker, nested_task,
        AttemptID(nested_task, 0),
    )
    nested_transfer = NestedReferenceTransfer(
        nested_dep, WorkerID.random(), ("127.0.0.1", 24567), hold
    )
    nested = replace(
        _spec(job, worker, nested_task, ()),
        args=(InlineArg(b"nested", nested_refs=(nested_transfer,)),),
    )
    nested_recovery, nested_owners = RecoveryManager(), ObjectOwnerTable()
    nested_output = _register(
        nested_recovery, nested_owners, nested, state="LOST"
    )
    nested_plan = ReconstructionCoordinator(
        nested_recovery, nested_owners
    ).preflight_graph(nested_output)
    assert nested_plan.node_for(nested_output).nested_local_holds == ()

    multi_task = TaskID.derive(job, TaskID.for_driver(job), 2)
    multi = replace(_spec(job, worker, multi_task, ()), num_returns=2)
    rejected(
        multi, "fully registered",
        outputs=(
            ObjectID.for_task(multi_task, 0),
            ObjectID.for_task(multi_task, 1),
        ),
    )

    recovery, owners = RecoveryManager(), ObjectOwnerTable()
    put_id = ObjectID.for_task(TaskID.derive(job, TaskID.for_driver(job), 3))
    recovery.register_put(put_id)
    owners.register(put_id, current_attempt=None, producer_task_spec=None)
    owners.publish_stored(put_id, None, NodeID.random())
    owners.mark_lost(put_id, None)
    with pytest.raises(ReconstructionRuntimeError, match="put objects"):
        ReconstructionCoordinator(recovery, owners).preflight_graph(put_id)
