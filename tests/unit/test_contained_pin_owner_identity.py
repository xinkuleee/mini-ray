"""Pure incoming contained-pin identity and Worker-death contracts."""

from __future__ import annotations

import pytest

from miniray.contained_edges import (
    ContainedReferenceEdge,
    ContainedReferenceHold,
)
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    DeadWorkerReferenceError,
    ObjectOwnerTable,
    ReleasedBorrowerTokenError,
)


def _object(index: int) -> tuple[ObjectID, AttemptID]:
    job_id = JobID(bytes.fromhex("e3" * 16))
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return ObjectID.for_task(task_id), AttemptID(task_id, 0)


def _hold(
    container_index: int, container_owner: WorkerID, token: str
) -> ContainedReferenceHold:
    container, _ = _object(container_index)
    return ContainedReferenceHold(container, container_owner, token)


@pytest.mark.unit
def test_typed_hold_snapshot_keeps_complete_authority_for_equal_tokens() -> None:
    table = ObjectOwnerTable()
    contained, attempt = _object(0)
    container_owner = WorkerID.random()
    typed = _hold(1, container_owner, "same-token")
    other = _hold(2, container_owner, "same-token")

    table.register(contained, current_attempt=attempt)
    assert table.add_contained_reference(contained, typed)
    assert not table.add_contained_reference(contained, typed)
    assert table.add_contained_reference(contained, other)

    snapshot = table.snapshot(contained)
    assert snapshot.contained_holds == frozenset(
        {typed, other}
    )
    assert snapshot.contained_tokens == frozenset(
        {"same-token"}
    )
    assert snapshot.is_live


@pytest.mark.unit
def test_outgoing_edge_derives_exact_incoming_hold() -> None:
    container, _ = _object(1)
    contained, _ = _object(0)
    contained_owner = WorkerID.random()
    container_owner = WorkerID.random()
    edge = ContainedReferenceEdge(
        container, contained, contained_owner, ("127.0.0.1", 24001),
        "edge-token",
    )

    assert edge.incoming_hold(container_owner) == ContainedReferenceHold(
        container, container_owner, "edge-token"
    )


@pytest.mark.unit
def test_release_tombstone_is_full_identity_and_prevents_late_add() -> None:
    table = ObjectOwnerTable()
    contained, attempt = _object(0)
    first_owner = WorkerID.random()
    second_owner = WorkerID.random()
    released = _hold(1, first_owner, "shared-token")
    independent = _hold(1, second_owner, "shared-token")

    table.register(contained, current_attempt=attempt)
    # Release-before-add models an ACK/request reordering.  It must establish
    # the same no-resurrection tombstone as a release of an active hold.
    assert not table.release_contained_reference(contained, released)
    assert table.contained_release_was_seen(contained, released)
    assert not table.release_contained_reference(contained, released)
    with pytest.raises(ReleasedBorrowerTokenError, match="already released"):
        table.add_contained_reference(contained, released)

    # An equal transfer token owned by another container incarnation is a
    # distinct capability and cannot alias the released identity.
    assert table.add_contained_reference(contained, independent)
    assert independent in table.snapshot(contained).contained_holds
    assert not table.contained_release_was_seen(contained, independent)


@pytest.mark.unit
def test_death_cleanup_releases_only_matching_container_owner_holds() -> None:
    table = ObjectOwnerTable()
    first, first_attempt = _object(0)
    second, second_attempt = _object(1)
    dead = WorkerID.random()
    live = WorkerID.random()
    dead_first = _hold(2, dead, "shared-token")
    # Same outer ObjectID and token, distinguished only by the physical
    # container-owner incarnation.  Death may consume exactly one of them.
    live_first = _hold(2, live, "shared-token")
    # Same dead owner and token, but another logical container remains a
    # distinct hold and must also be reported separately.
    dead_second = _hold(4, dead, "shared-token")

    table.register(first, current_attempt=first_attempt)
    table.register(second, current_attempt=second_attempt)
    assert table.add_contained_reference(first, dead_first)
    assert table.add_contained_reference(first, live_first)
    assert table.add_contained_reference(second, dead_second)

    cleanup = table.install_dead_worker(dead, "death:container-owner")

    assert cleanup.released_contained_holds == frozenset(
        {(first, dead_first), (second, dead_second)}
    )
    assert set(cleanup.affected_object_ids) == {first, second}
    assert cleanup.collectable_object_ids == (second,)
    assert table.snapshot(first).contained_holds == frozenset({live_first})
    assert not table.snapshot(second).contained_holds
    assert table.contained_release_was_seen(first, dead_first)
    assert table.contained_release_was_seen(second, dead_second)
    assert not table.contained_release_was_seen(first, live_first)

    # The installed cleanup is immutable and exact replay returns the same
    # value rather than rescanning or consuming the live owner's equal token.
    assert table.install_dead_worker(dead, "death:container-owner") is cleanup
    assert table.snapshot(first).contained_holds == frozenset({live_first})


@pytest.mark.unit
def test_dead_fence_rejects_late_hold_without_consuming_another_owner() -> None:
    table = ObjectOwnerTable()
    contained, attempt = _object(0)
    dead = WorkerID.random()
    typed = _hold(1, dead, "typed")
    live = _hold(1, WorkerID.random(), "typed")

    table.register(contained, current_attempt=attempt)
    assert table.add_contained_reference(contained, live)
    cleanup = table.install_dead_worker(dead, "death:before-typed-pin")
    assert not cleanup.affected_object_ids
    assert table.snapshot(contained).contained_holds == frozenset({live})

    with pytest.raises(DeadWorkerReferenceError, match="dead Worker"):
        table.add_contained_reference(contained, typed)

    # Equal token/container with a different physical owner is independent.
    assert not table.add_contained_reference(contained, live)
    assert table.release_contained_reference(contained, live)
    assert table.contained_release_was_seen(contained, live)


@pytest.mark.unit
def test_death_cleanup_tombstone_blocks_exact_typed_replay() -> None:
    table = ObjectOwnerTable()
    contained, attempt = _object(0)
    dead = WorkerID.random()
    hold = _hold(1, dead, "death-released")
    table.register(contained, current_attempt=attempt)
    table.add_contained_reference(contained, hold)

    table.install_dead_worker(dead, "death:exact-replay")
    assert not table.release_contained_reference(contained, hold)
    assert table.contained_release_was_seen(contained, hold)
    with pytest.raises(DeadWorkerReferenceError):
        table.add_contained_reference(contained, hold)
