"""Pure outer-object to contained-pin collection contracts.

These tests intentionally perform no RPC and no physical object-store delete.
They prove that outer metadata collection retains exact release obligations and
that applying those obligations to a contained owner is idempotent.
"""

from __future__ import annotations

import cloudpickle
import pytest

from miniray.contained_edges import (
    ContainedReferenceEdge,
    ObjectMetadataCollection,
)
from miniray.core import ObjectRef
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.ref_transfer import ReferenceExportSession




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


@pytest.mark.unit
def test_legacy_boolean_collection_never_drops_outgoing_obligations() -> None:
    outer, outer_attempt = _object_id(0)
    child, _ = _object_id(1)
    owner = ObjectOwnerTable()
    owner.register(outer, current_attempt=outer_attempt)
    edge = _edge(outer, child, WorkerID.random(), "transfer-1")

    assert owner.add_outgoing_contained_edge(outer, edge)
    assert not owner.add_outgoing_contained_edge(outer, edge)
    assert not owner.collect_if_unused(outer)
    assert owner.contains(outer)
    assert owner.snapshot(outer).outgoing_contained_edges == frozenset({edge})


@pytest.mark.unit
def test_atomic_collection_returns_edges_only_after_outer_tokens_vanish() -> None:
    outer, outer_attempt = _object_id(0)
    child_a, _ = _object_id(1)
    child_b, _ = _object_id(2)
    outer_owner = ObjectOwnerTable()
    outer_owner.register(
        outer, current_attempt=outer_attempt, local_token="outer-handle"
    )
    edges = (
        _edge(outer, child_a, WorkerID.random(), "transfer-a"),
        _edge(outer, child_b, WorkerID.random(), "transfer-b"),
    )
    assert outer_owner.add_outgoing_contained_edges(outer, edges) == 2
    assert outer_owner.add_outgoing_contained_edges(outer, edges) == 0

    live = outer_owner.collect_unused_with_edges(outer)
    assert live == ObjectMetadataCollection(outer, collected=False)
    assert outer_owner.contains(outer)

    assert outer_owner.release_local_reference(outer, "outer-handle")
    collected = outer_owner.collect_unused_with_edges(outer)
    assert collected.collected
    assert frozenset(collected.contained_releases) == frozenset(edges)
    assert not outer_owner.contains(outer)


@pytest.mark.unit
def test_collection_release_obligations_remove_child_pins_idempotently() -> None:
    outer, outer_attempt = _object_id(0)
    child, child_attempt = _object_id(1)
    child_owner_id = WorkerID.random()
    transfer = "transfer-child"
    edge = _edge(outer, child, child_owner_id, transfer)
    outer_owner = ObjectOwnerTable()
    child_owner = ObjectOwnerTable()
    outer_owner.register(outer, current_attempt=outer_attempt)
    child_owner.register(child, current_attempt=child_attempt)
    child_owner.add_contained_reference(child, transfer)
    outer_owner.add_outgoing_contained_edge(outer, edge)

    collection = outer_owner.collect_unused_with_edges(outer)
    assert collection.contained_releases == (edge,)
    for release in collection.contained_releases:
        assert release.contained_owner_worker_id == child_owner_id
        assert child_owner.release_contained_reference(
            release.contained_object_id, release.transfer_token
        )
        assert not child_owner.release_contained_reference(
            release.contained_object_id, release.transfer_token
        )

    assert not child_owner.snapshot(child).contained_tokens
    # This proves only logical pin release.  Physical bytes are deliberately
    # outside ObjectOwnerTable and no such deletion occurs here.
    assert child_owner.collect_if_unused(child)


@pytest.mark.unit
def test_export_commit_builds_exact_outer_edges_and_rollback_builds_none() -> None:
    outer, _ = _object_id(0)
    child_a, _ = _object_id(1)
    child_b, _ = _object_id(2)
    child_owner = WorkerID.random()
    address = ("127.0.0.1", 24002)
    pins: set[tuple[ObjectID, str]] = set()

    def pin(object_id: ObjectID, token: str) -> None:
        pins.add((object_id, token))

    def unpin(object_id: ObjectID, token: str) -> None:
        pins.discard((object_id, token))

    with ReferenceExportSession(
        child_owner, address, pin=pin, unpin=unpin
    ) as session:
        cloudpickle.dumps(
            {"nested": [ObjectRef(child_a, child_owner),
                        ObjectRef(child_b, child_owner)]}
        )
        edges = session.commit(outer)
        assert session.commit(outer) == edges

    assert len(edges) == 2
    assert len(pins) == 2
    assert session.committed_edges == edges
    assert {edge.container_object_id for edge in edges} == {outer}
    assert {edge.contained_object_id for edge in edges} == {child_a, child_b}
    assert {edge.contained_owner_worker_id for edge in edges} == {child_owner}
    assert {edge.contained_owner_address for edge in edges} == {address}
    assert {(edge.contained_object_id, edge.transfer_token) for edge in edges} == pins

    rolled_back_pins: set[tuple[ObjectID, str]] = set()
    with pytest.raises(RuntimeError, match="abort"):
        with ReferenceExportSession(
            child_owner, address,
            pin=lambda object_id, token: rolled_back_pins.add((object_id, token)),
            unpin=lambda object_id, token: rolled_back_pins.discard(
                (object_id, token)
            ),
        ) as aborted:
            cloudpickle.dumps(ObjectRef(child_a, child_owner))
            raise RuntimeError("abort")
    assert not rolled_back_pins
    with pytest.raises(RuntimeError, match="not committed"):
        _ = aborted.committed_edges


@pytest.mark.unit
def test_legacy_commit_without_outer_id_retains_pin_but_has_no_edge_metadata() -> None:
    child, _ = _object_id(1)
    child_owner = WorkerID.random()
    pins: set[tuple[ObjectID, str]] = set()
    with ReferenceExportSession(
        child_owner, ("127.0.0.1", 24003),
        pin=lambda object_id, token: pins.add((object_id, token)),
        unpin=lambda object_id, token: pins.discard((object_id, token)),
    ) as session:
        cloudpickle.dumps(ObjectRef(child, child_owner))
        assert session.commit() == ()

    assert len(pins) == 1
    assert session.committed_edges == ()
    # This is backward-compatible library behavior only.  Worker result
    # publication now commits with the outer ObjectID and emits runtime edges.


@pytest.mark.unit
def test_multi_return_rejection_rolls_back_every_discovered_export_pin() -> None:
    """A caller rejection rolls back the low-level export transaction.

    The historical test ID is retained. Ordinary Worker multi-return outputs
    now support contained refs via the unified publication path; this injected
    rejection is not a current Worker restriction or an end-to-end test.
    """
    child_a, _ = _object_id(3)
    child_b, _ = _object_id(4)
    owner = WorkerID.random()
    pins: set[tuple[ObjectID, str]] = set()

    with pytest.raises(TypeError, match="multi-return"):
        with ReferenceExportSession(
            owner, ("127.0.0.1", 24004),
            pin=lambda object_id, token: pins.add((object_id, token)),
            unpin=lambda object_id, token: pins.discard((object_id, token)),
        ) as session:
            # Discover both refs before the caller rejects this low-level
            # export. The session must compensate every acquired pin.
            tuple(
                cloudpickle.dumps(value)
                for value in (ObjectRef(child_a, owner), ObjectRef(child_b, owner))
            )
            if session.exported_count:
                raise TypeError(
                    "injected multi-return caller rejection"
                )

    assert not pins


@pytest.mark.unit
def test_export_session_retains_failed_unpin_for_explicit_replay() -> None:
    child, _ = _object_id(5)
    owner = WorkerID.random()
    pins: set[tuple[ObjectID, str]] = set()
    available = False

    def unpin(object_id: ObjectID, token: str) -> None:
        if not available:
            raise RuntimeError("release authority unavailable")
        pins.discard((object_id, token))

    session = ReferenceExportSession(
        owner, ("127.0.0.1", 24005),
        pin=lambda object_id, token: pins.add((object_id, token)),
        unpin=unpin,
    )
    with pytest.raises(RuntimeError, match="abort"):
        with session:
            cloudpickle.dumps(ObjectRef(child, owner))
            raise RuntimeError("abort")

    assert len(pins) == 1
    assert session.exported_count == 1
    available = True
    session.rollback()
    assert not pins
    assert session.exported_count == 0
