"""Pure TaskID-scoped foreign-lineage registry contracts."""

from __future__ import annotations

from dataclasses import replace

import pytest

from miniray.contained_edges import ObjectMetadataCollectionPlan
from miniray.foreign_lineage import (
    ForeignLineageCollectionReceipt, ForeignLineageEdge,
    ForeignLineagePreparedCollectionReceipt, ForeignLineageRegistry,
    ForeignLineageRole,
)
from miniray.ids import AttemptID, ObjectID, TaskID, WorkerID
from miniray.protocol import TaskReferenceHold, TaskReferenceHoldKind
from miniray.recovery import CollectedObjectForgetPlan
from miniray.ownership import DeadWorkerReferenceRecord


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


def _receipt(
    task_id: TaskID, output_id: ObjectID, *, final: bool
) -> ForeignLineageCollectionReceipt:
    return ForeignLineageCollectionReceipt(
        ForeignLineagePreparedCollectionReceipt(
        task_id, output_id,
        ObjectMetadataCollectionPlan(
            output_id, "collection-{}".format(output_id.return_index),
            AttemptID(task_id, 0), (), None
        ),
        CollectedObjectForgetPlan(
            output_id, "task", task_id, final, None
        ),
        )
    )


def test_register_merges_top_level_and_nested_roles_and_replays_exactly() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id, 0), ObjectID.for_task(task_id, 1))
    dependency = ObjectID.for_task(TaskID.random())
    top = _edge(task_id, dependency)
    nested = replace(top, roles=ForeignLineageRole.NESTED)

    first = registry.register(task_id, outputs, (top, nested))
    replay = registry.register(task_id, outputs, (nested, top))

    assert replay == first
    assert len(first.edges) == 1
    assert first.edges[0].roles == (
        ForeignLineageRole.TOP_LEVEL | ForeignLineageRole.NESTED
    )


def test_system_attempt_change_does_not_change_registered_lineage_hold() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id),)
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    record = registry.register(task_id, outputs, (edge,))

    # Physical retries are absent from the registry API by design.
    assert registry.snapshot(task_id) == record
    assert registry.snapshot(task_id).edges[0].hold == edge.hold


def test_only_final_sibling_claims_and_completion_deletes_registry() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    outputs = (ObjectID.for_task(task_id, 0), ObjectID.for_task(task_id, 1))
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    registry.register(task_id, outputs, (edge,))

    receipts = (
        _receipt(task_id, outputs[0], final=False),
        _receipt(task_id, outputs[1], final=True),
    )
    assert registry.claim_with_receipts(task_id, receipts[:1]) is None
    plan = registry.claim_with_receipts(task_id, receipts)
    assert plan is not None and plan.edges == (edge,)
    assert registry.claim_with_receipts(task_id, tuple(reversed(receipts))) is plan
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
    registry.register(task_id, outputs, (edge,))
    successor = replace(
        edge,
        hold=replace(
            edge.hold, origin_attempt_id=AttemptID(task_id, 1)
        ),
    )

    first = registry.commit_edge_replacement(edge, successor)
    replay = registry.commit_edge_replacement(edge, successor)

    assert first == replay == registry.snapshot(task_id)
    assert first.edges == (successor,)
    assert registry.task_ids() == (task_id,)


def test_edge_replacement_is_fenced_after_final_collection_claim() -> None:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    output = ObjectID.for_task(task_id)
    edge = _edge(task_id, ObjectID.for_task(TaskID.random()))
    registry.register(task_id, (output,), (edge,))
    assert registry.claim_with_receipts(
        task_id, (_receipt(task_id, output, final=True),)
    ) is not None
    successor = replace(
        edge,
        hold=replace(
            edge.hold, origin_attempt_id=AttemptID(task_id, 1)
        ),
    )

    with pytest.raises(ValueError, match="after collection claim"):
        registry.commit_edge_replacement(edge, successor)
