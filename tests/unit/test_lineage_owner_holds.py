"""Pure owner-table contracts for producer-to-dependency lineage holds."""

from __future__ import annotations

import pytest

from miniray.contained_edges import (
    LineageReferenceEdge,
    ObjectMetadataCollection,
)
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID
from miniray.ownership import (
    ObjectOwnerTable,
    ReferenceKind,
    ReleasedTaskReferenceHoldError,
)


pytestmark = pytest.mark.unit


def _object(index: int) -> tuple[ObjectID, AttemptID]:
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), index)
    return ObjectID.for_task(task), AttemptID(task, 0)


def test_lineage_token_is_live_idempotent_and_cannot_resurrect() -> None:
    table = ObjectOwnerTable()
    dependency, attempt = _object(0)
    table.register(dependency, current_attempt=attempt)

    assert table.add_lineage_reference(dependency, "producer:one")
    assert not table.add_reference(
        dependency, ReferenceKind.LINEAGE, "producer:one"
    )
    snapshot = table.snapshot(dependency)
    assert snapshot.lineage_tokens == frozenset({"producer:one"})
    assert snapshot.is_live and table.is_live(dependency)

    assert table.release_reference(
        dependency, ReferenceKind.LINEAGE, "producer:one"
    )
    assert not table.release_lineage_reference(dependency, "producer:one")
    snapshot = table.snapshot(dependency)
    assert snapshot.lineage_tokens == frozenset()
    assert snapshot.released_lineage_tokens == frozenset({"producer:one"})
    assert table.lineage_release_was_seen(dependency, "producer:one")
    with pytest.raises(ReleasedTaskReferenceHoldError, match="lineage token"):
        table.add_lineage_reference(dependency, "producer:one")


def test_release_before_add_tombstones_lineage_token() -> None:
    table = ObjectOwnerTable()
    dependency, attempt = _object(0)
    table.register(dependency, current_attempt=attempt)

    assert not table.release_lineage_reference(dependency, "late")
    with pytest.raises(ReleasedTaskReferenceHoldError, match="lineage token"):
        table.add_lineage_reference(dependency, "late")


def test_inline_producer_collection_returns_frozen_lineage_release_metadata() -> None:
    table = ObjectOwnerTable()
    producer, attempt = _object(0)
    dependency, _ = _object(1)
    edge = LineageReferenceEdge(producer, dependency, "lineage:edge")
    table.register(producer, current_attempt=attempt)
    table.publish_inline(producer, attempt, b"value")

    assert table.add_outgoing_lineage_edge(producer, edge)
    assert not table.add_outgoing_lineage_edge(producer, edge)
    assert not table.collect_if_unused(producer)
    collected = table.collect_unused_with_edges(producer)

    assert collected == ObjectMetadataCollection(
        producer, collected=True, lineage_releases=(edge,)
    )
    assert not table.contains(producer)


def test_stored_collection_plan_freezes_lineage_releases_without_executing_them() -> None:
    table = ObjectOwnerTable()
    producer, attempt = _object(0)
    dependency, _ = _object(1)
    edge = LineageReferenceEdge(producer, dependency, "lineage:stored")
    node = NodeID.random()
    table.register(producer, current_attempt=attempt)
    table.publish_stored(producer, attempt, node)
    table.add_outgoing_lineage_edge(producer, edge)

    plan = table.begin_collection(
        producer, collection_id="collection",
        canonical_size_bytes=5, canonical_checksum="a" * 64,
    )
    assert plan is not None
    assert plan.lineage_releases == (edge,)
    assert table.begin_collection(
        producer, collection_id="ignored replay",
        canonical_size_bytes=5, canonical_checksum="a" * 64,
    ) is plan

    collected = table.complete_collection(plan)
    assert collected.lineage_releases == (edge,)
    # The plan only freezes release work.  No dependency owner/RPC exists here.
    assert not table.contains(producer)

