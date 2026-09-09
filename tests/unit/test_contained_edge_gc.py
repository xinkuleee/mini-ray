"""Pure outer-object to contained-pin collection contracts.

These tests intentionally perform no RPC and no physical object-store delete.
They prove that outer metadata collection retains exact release obligations and
that applying those obligations to a contained owner is idempotent.
"""

from __future__ import annotations

import pytest

from miniray.contained_edges import (
    ContainedReferenceEdge,
)
from miniray.core import ObjectRef
from miniray import protocol
from dataclasses import replace
import hashlib
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable, ObjectCollectionInProgressError, InvalidObjectTransitionError
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest
from miniray.ids import LeaseID, NodeID


def _object_id(index: int) -> tuple[ObjectID, AttemptID]:
    job_id = JobID(bytes.fromhex("cc" * 16))
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return ObjectID.for_task(task_id), AttemptID(task_id, 0)


def _edge(
    container: ObjectID, contained: ObjectID, owner: WorkerID, token: str
) -> ContainedReferenceEdge:
    return ContainedReferenceEdge(
        container, contained, owner, ("127.0.0.1", 24001), token
    )


def _publish_put(table, object_id, attempt, owner, edges=(), *, local_token=None):
    table.register(object_id, current_attempt=attempt, local_token=local_token)
    payload = b'one'
    descriptor = protocol.ResultDescriptor(object_id, protocol.ResultStorage.INLINE, len(payload),
        owner, NodeID(b'n' * 16), hashlib.sha256(payload).hexdigest(), payload)
    assert table.publish_put_value(object_id, attempt, descriptor, edges)
    return descriptor


@pytest.mark.unit
def test_collection_claim_keeps_exact_outgoing_obligations_until_plan_completion():
    outer, attempt = _object_id(0)
    child, _ = _object_id(1)
    owner = WorkerID(b'o' * 16)
    table = ObjectOwnerTable()
    edge = _edge(outer, child, WorkerID(b'c' * 16), 'child-hold')
    descriptor = _publish_put(table, outer, attempt, owner, (edge,))
    assert table.publish_put_value(outer, attempt, descriptor, (edge, edge))
    plan = table.begin_collection(outer, collection_id='exact-outer-collection')
    assert plan is not None and plan.contained_releases == (edge,)
    assert table.contains(outer)
    assert table.snapshot(outer).outgoing_contained_edges == frozenset((edge,))
    assert table.begin_collection(outer, collection_id='retry-name') == plan
    before = table.snapshot(outer)
    with pytest.raises(InvalidObjectTransitionError):
        table.complete_collection(replace(plan, collection_id='wrong-plan'))
    with pytest.raises(ObjectCollectionInProgressError):
        table.publish_put_value(outer, attempt, descriptor, (edge,))
    assert table.snapshot(outer) == before


@pytest.mark.unit
def test_atomic_collection_returns_edges_only_after_outer_tokens_vanish():
    outer, attempt = _object_id(0)
    children = (_object_id(1)[0], _object_id(2)[0])
    owner = WorkerID(b'o' * 16)
    table = ObjectOwnerTable()
    edges = tuple(_edge(outer, child, WorkerID(b'c' * 16), 'child-' + str(index))
                  for index, child in enumerate(children))
    _publish_put(table, outer, attempt, owner, edges, local_token='outer-handle')
    assert table.begin_collection(outer) is None
    assert table.snapshot(outer).outgoing_contained_edges == frozenset(edges)
    assert table.release_local_reference(outer, 'outer-handle')
    plan = table.begin_collection(outer, collection_id='no-live-outer-token')
    assert plan is not None and frozenset(plan.contained_releases) == frozenset(edges)
    assert table.contains(outer)
    # Child effects are outside this local reducer; callers retain this plan.
    collected = table.complete_collection(plan)
    assert collected.collected and frozenset(collected.contained_releases) == frozenset(edges)
    assert not table.contains(outer)


@pytest.mark.unit
def test_collection_release_obligations_remove_child_holds_idempotently():
    outer, outer_attempt = _object_id(0)
    child, child_attempt = _object_id(1)
    owner, child_owner_id = WorkerID(b'o' * 16), WorkerID(b'c' * 16)
    outer_table, child_table = ObjectOwnerTable(), ObjectOwnerTable()
    edge = _edge(outer, child, child_owner_id, 'exact-child')
    _publish_put(child_table, child, child_attempt, child_owner_id)
    hold = edge.incoming_hold(owner)
    assert child_table.add_contained_reference(child, hold)
    _publish_put(outer_table, outer, outer_attempt, owner, (edge,))
    plan = outer_table.begin_collection(outer, collection_id='outer-cleanup')
    assert plan is not None and plan.contained_releases == (edge,)
    assert child_table.begin_collection(child) is None
    for release in plan.contained_releases:
        exact = release.incoming_hold(owner)
        assert child_table.release_contained_reference(release.contained_object_id, exact)
        assert not child_table.release_contained_reference(release.contained_object_id, exact)
        assert child_table.contained_release_was_seen(child, exact)
    assert outer_table.complete_collection(plan).collected
    child_plan = child_table.begin_collection(child, collection_id='child-cleanup')
    assert child_plan is not None and child_plan.contained_releases == ()
    assert child_table.complete_collection(child_plan).collected
    assert not outer_table.contains(outer) and not child_table.contains(child)


def _discovery(outer, owner):
    task = outer.task_id
    identity = OutputPublicationID(LeaseID(b"l" * 16), TaskExecutionKey(
        TaskOutputManifest.for_task(task, 1), AttemptID(task, 0)))
    header = OutputPublicationHeader(identity, JobID(b"j" * 16), owner, WorkerID(b"x" * 16),
        OutputPublicationNodeIncarnation(NodeID(b"n" * 16), 1001, 1))
    return OutputDiscoverySession(header, inline_threshold=1024, owner_address=("owner.invalid", 1))


@pytest.mark.unit
def test_single_output_discovery_binds_exact_outer_edges_without_pinning_children():
    outer, _ = _object_id(0)
    child_a, _ = _object_id(1)
    child_b, _ = _object_id(2)
    owner = WorkerID(b"o" * 16)
    refs = (ObjectRef(child_a, owner, ("owner.invalid", 1)), ObjectRef(child_b, owner, ("owner.invalid", 1)))
    table = ObjectOwnerTable()
    for object_id in (child_a, child_b):
        table.register(object_id, local_token="source")
    before = tuple(table.snapshot(object_id) for object_id in (child_a, child_b))
    session = _discovery(outer, owner)
    try:
        outputs = session.discover(({"children": refs},))
        slot, = outputs.manifest.slots
        assert len(slot.edges) == len(slot.transfers) == 2
        assert {edge.container_object_id for edge in slot.edges} == {outer}
        assert {edge.contained_object_id for edge in slot.edges} == {child_a, child_b}
        assert all(edge.contained_owner_worker_id == owner for edge in slot.edges)
        assert session.source_references == refs
        assert tuple(table.snapshot(object_id) for object_id in (child_a, child_b)) == before
        session.abort()
        assert session.source_references == () and session.discovered is None
        assert tuple(table.snapshot(object_id) for object_id in (child_a, child_b)) == before
    finally:
        for ref in refs:
            ref.close(timeout=0)


@pytest.mark.unit
def test_failed_single_output_serialization_discards_local_sources_without_child_effects():
    outer, _ = _object_id(0)
    child, _ = _object_id(1)
    owner = WorkerID(b"o" * 16)
    ref = ObjectRef(child, owner, ("owner.invalid", 1))
    class Invalid:
        def __reduce__(self):
            raise TypeError("later result reduction failed")
    session = _discovery(outer, owner)
    try:
        with pytest.raises(TypeError, match="later result reduction failed"):
            session.discover(([ref, Invalid()],))
        assert session.source_references == () and session.discovered is None
        assert not ref.closed
    finally:
        ref.close(timeout=0)
