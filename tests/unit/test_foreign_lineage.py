"""Pure single-output foreign-lineage registry contracts.

One registry, one logical output and at most two foreign inputs/owners per
case. Collection receipts follow actual local owner and recovery commits
under one composition lock. Edge replacement/completion are registry-level
inputs, not evidence of remote hold renewal/release, RPC or physical GC.
No runtime constructor, thread, socket, timer, wait or user function runs.
"""

from __future__ import annotations

from dataclasses import replace
from threading import RLock

import pytest

from miniray.foreign_lineage import (
    ForeignLineageCollectionReceipt, ForeignLineageEdge,
    ForeignLineagePreparedCollectionReceipt, ForeignLineageRegistry,
    ForeignLineageRole,
)
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.protocol import FunctionKey, TaskReferenceHold, TaskReferenceHoldKind, TaskSpec
from miniray.recovery import RecoveryManager
from miniray.ownership import DeadWorkerReferenceRecord, ObjectOwnerTable
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _edge(
    task_id: TaskID, dependency_id: ObjectID, *,
    role: ForeignLineageRole = ForeignLineageRole.TOP_LEVEL,
) -> ForeignLineageEdge:
    borrower = WorkerID.random()
    return ForeignLineageEdge(
        task_id, dependency_id, WorkerID.random(), ("127.0.0.1", 25001),
        borrower,
        TaskReferenceHold(
            TaskReferenceHoldKind.RETAINED, borrower, task_id,
            AttemptID(task_id, 0),
        ),
        role,
    )


def _receipt(task_id: TaskID, output_id: ObjectID) -> ForeignLineageCollectionReceipt:
    """Activate the receipt only after both local collection authorities commit."""
    job, submitter = JobID.random(), WorkerID.random()
    spec = TaskSpec(
        job, task_id, AttemptID(task_id, 0),
        FunctionKey(job, __name__, "collection-fixture", "1"),
        (), 1, ResourceVector(), submitter,
    )
    assert output_id == ObjectID.for_task(task_id)
    owner, recovery = ObjectOwnerTable(), RecoveryManager()
    owner.register_task_outputs(spec)
    owner.publish_inline(output_id, spec.attempt_id, b"done")
    recovery.register_task(spec)
    recovery.commit_validated_transition(recovery.validate_task_success(task_id, spec.attempt_id))
    with RLock():
        owner_plan = owner.begin_collection(output_id, collection_id="collected-output")
        assert owner_plan is not None
        recovery_plan = recovery.validate_forget_collected_object(
            output_id, expected_task_spec=spec, expected_attempt=spec.attempt_id,
        )
        owner.validate_complete_collection(owner_plan)
        prepared = ForeignLineagePreparedCollectionReceipt(
            task_id, output_id, owner_plan, recovery_plan,
        )
        assert owner.complete_collection(owner_plan).collected
        assert recovery.commit_forget_collected_object(recovery_plan)
    return ForeignLineageCollectionReceipt(prepared)


def test_register_merges_top_level_and_nested_roles_and_replays_exactly() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id),)
    dependency = ObjectID.for_task(TaskID.random())
    top = _edge(task_id, dependency)
    nested = replace(top, roles=ForeignLineageRole.NESTED)

    other = _edge(task_id, ObjectID.for_task(TaskID.random()))
    first = registry.register(task_id, outputs, (top, nested, other))
    replay = registry.register(task_id, outputs, (other, nested, top))

    assert replay == first and first.output_ids == outputs
    assert len(first.edges) == 2
    merged = next(edge for edge in first.edges if edge.dependency_object_id == dependency)
    assert merged.roles == (ForeignLineageRole.TOP_LEVEL | ForeignLineageRole.NESTED)
    assert other in first.edges and other.owner_worker_id != top.owner_worker_id


def test_system_attempt_change_does_not_change_registered_lineage_hold() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id),)
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    record = registry.register(task_id, outputs, (edge,))

    # Physical retries are absent from the registry API by design.
    assert registry.snapshot(task_id) == record
    assert registry.snapshot(task_id).edges[0].hold == edge.hold


def test_single_output_claim_waits_for_committed_collection_receipt() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    output = ObjectID.for_task(task_id)
    edges = tuple(_edge(task_id, ObjectID.for_task(TaskID.random())) for _ in range(2))
    record = registry.register(task_id, (output,), edges)

    assert registry.claim_with_receipts(task_id, ()) is None
    assert registry.snapshot(task_id) == record and not registry.has_pending_claims()
    receipt = _receipt(task_id, output)
    assert receipt.recovery_plan.remove_task
    plan = registry.claim_with_receipts(task_id, (receipt,))
    assert plan is not None and plan.edges == tuple(sorted(edges))
    assert plan.output_ids == (output,) and plan.receipts == (receipt,)
    assert registry.claim_with_receipts(task_id, (receipt,)) is plan
    assert registry.has_pending_claims()
    assert registry.complete_claim(plan)
    assert registry.snapshot(task_id) is None
    assert not registry.has_pending_claims()


def test_abort_is_exact_and_dead_owner_marks_affected_tasks() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id),)
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    record = registry.register(task_id, outputs, (edge,))

    proof = DeadWorkerReferenceRecord(edge.owner_worker_id, "death-proof")
    assert registry.mark_owner_dead(proof) == (task_id,)
    assert registry.owner_is_dead(edge.owner_worker_id)
    with pytest.raises(ValueError, match="changed before abort"):
        registry.abort(replace(record, edges=()))
    assert registry.abort(record)
    assert registry.snapshot(task_id) is None


def test_owner_acked_edge_replacement_updates_only_credential() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id),)
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    other = _edge(task_id, ObjectID.for_task(TaskID.random()))
    registry.register(task_id, outputs, (edge, other))
    successor = replace(
        edge,
        hold=replace(
            edge.hold, origin_attempt_id=AttemptID(task_id, 1)
        ),
    )

    first = registry.commit_edge_replacement(edge, successor)
    replay = registry.commit_edge_replacement(edge, successor)

    assert first == replay == registry.snapshot(task_id)
    assert first.edges == tuple(sorted((successor, other)))
    assert other.hold.origin_attempt_id == AttemptID(task_id, 0)
    assert registry.task_ids() == (task_id,)


def test_edge_replacement_is_fenced_after_final_collection_claim() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    output = ObjectID.for_task(task_id)
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    registry.register(task_id, (output,), (edge,))
    assert registry.claim_with_receipts(
        task_id, (_receipt(task_id, output),)
    ) is not None
    successor = replace(
        edge,
        hold=replace(
            edge.hold, origin_attempt_id=AttemptID(task_id, 1)
        ),
    )

    with pytest.raises(ValueError, match="after collection claim"):
        registry.commit_edge_replacement(edge, successor)
