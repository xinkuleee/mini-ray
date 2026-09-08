"""Owner-side prepare and atomic promotion for stored contained refs."""

from __future__ import annotations

from dataclasses import replace

import pytest

from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ConflictingBorrowerTokenError,
    DeadWorkerReferenceError,
    InvalidObjectTransitionError,
    ObjectOwnerTable,
    ReleasedBorrowerTokenError,
    StoredContainedReferenceDisposition,
)
from miniray.protocol import ContainedTransferSource
from miniray.stored_publication import (
    BorrowedContainedSource,
    OwnedContainedSource,
    PreparedContainedTransfer,
)


pytestmark = pytest.mark.unit


def _object(index: int) -> tuple[ObjectID, AttemptID]:
    job = JobID(bytes.fromhex("d4" * 16))
    task = TaskID.derive(job, TaskID.for_driver(job), index)
    return ObjectID.for_task(task), AttemptID(task, 0)


def _owned_transfer(
    child: ObjectID, child_owner: WorkerID, executor: WorkerID | None = None
) -> PreparedContainedTransfer:
    outer, _ = _object(20)
    executor = child_owner if executor is None else executor
    return PreparedContainedTransfer(
        child, child_owner, ("127.0.0.1", 27101),
        OwnedContainedSource(child_owner),
        ContainedReferenceHold(outer, executor, "stored-pin"),
        ContainedReferenceHold(outer, WorkerID.random(), "stored-pin"),
    )


def test_owned_prepare_and_promotion_are_atomic_and_exactly_replayable() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(0)
    child_owner = WorkerID.random()
    transfer = _owned_transfer(child, child_owner)
    table.register(child, current_attempt=attempt)

    assert table.prepare_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.PREPARED
    assert table.prepare_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.ALREADY_PREPARED
    assert table.snapshot(child).contained_holds == frozenset(
        {transfer.provisional_hold}
    )

    assert table.promote_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.PROMOTED
    assert table.promote_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.ALREADY_PROMOTED
    assert table.snapshot(child).contained_holds == frozenset(
        {transfer.final_hold}
    )
    assert table.contained_release_was_seen(child, transfer.provisional_hold)


def test_authority_conflict_and_release_before_delivery_never_partially_mutate() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(1)
    child_owner = WorkerID.random()
    transfer = _owned_transfer(child, child_owner)
    table.register(child, current_attempt=attempt)

    with pytest.raises(ValueError, match="authority"):
        table.prepare_stored_contained_reference(
            transfer, authority_worker_id=WorkerID.random()
        )
    assert not table.snapshot(child).contained_holds

    table.release_contained_reference(child, transfer.provisional_hold)
    with pytest.raises(ReleasedBorrowerTokenError):
        table.prepare_stored_contained_reference(
            transfer, authority_worker_id=child_owner
        )
    assert not table.snapshot(child).contained_holds


def test_conflicting_replay_and_final_release_fence_preserve_provisional() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(2)
    child_owner = WorkerID.random()
    transfer = _owned_transfer(child, child_owner)
    conflict = replace(
        transfer,
        final_hold=replace(
            transfer.final_hold, container_owner_worker_id=WorkerID.random()
        ),
    )
    table.register(child, current_attempt=attempt)
    table.prepare_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    )

    with pytest.raises(InvalidObjectTransitionError, match="conflict"):
        table.prepare_stored_contained_reference(
            conflict, authority_worker_id=child_owner
        )
    table.release_contained_reference(child, transfer.final_hold)
    with pytest.raises(ReleasedBorrowerTokenError):
        table.promote_stored_contained_reference(
            transfer, authority_worker_id=child_owner
        )
    assert table.snapshot(child).contained_holds == frozenset(
        {transfer.provisional_hold}
    )


def test_borrowed_prepare_requires_the_complete_live_source_binding() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(3)
    child_owner = WorkerID.random()
    borrower = WorkerID.random()
    outer, _ = _object(21)
    source_hold = ContainedReferenceHold(outer, WorkerID.random(), "source")
    original_source = ContainedTransferSource(source_hold)
    borrower_token = (borrower, "borrowed-child")
    provisional = ContainedReferenceHold(outer, borrower, "stored-borrowed")
    final = ContainedReferenceHold(outer, WorkerID.random(), "stored-borrowed")
    transfer = PreparedContainedTransfer(
        child, child_owner, ("127.0.0.1", 27102),
        BorrowedContainedSource(borrower, "borrowed-child", original_source),
        provisional, final,
    )
    table.register(child, current_attempt=attempt)
    table.add_contained_reference(child, source_hold)
    table.acquire_exported_reference(child, original_source, borrower_token)

    assert table.prepare_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.PREPARED

    drift_source = ContainedTransferSource(
        ContainedReferenceHold(outer, source_hold.container_owner_worker_id, "other")
    )
    drift = replace(
        transfer,
        source=BorrowedContainedSource(
            borrower, "borrowed-child", drift_source
        ),
        provisional_hold=replace(provisional, transfer_token="other-pin"),
        final_hold=replace(final, transfer_token="other-pin"),
    )
    with pytest.raises(ConflictingBorrowerTokenError):
        table.prepare_stored_contained_reference(
            drift, authority_worker_id=child_owner
        )
    assert drift.provisional_hold not in table.snapshot(child).contained_holds


def test_released_borrower_cannot_prepare_borrowed_publication() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(31)
    child_owner = WorkerID.random()
    borrower = WorkerID.random()
    outer, _ = _object(32)
    original_hold = ContainedReferenceHold(
        outer, WorkerID.random(), "released-source"
    )
    source = ContainedTransferSource(original_hold)
    transfer = PreparedContainedTransfer(
        child, child_owner, ("127.0.0.1", 27103),
        BorrowedContainedSource(borrower, "released-token", source),
        ContainedReferenceHold(outer, borrower, "new-provisional"),
        ContainedReferenceHold(outer, WorkerID.random(), "new-provisional"),
    )
    table.register(child, current_attempt=attempt)
    table.add_contained_reference(child, original_hold)
    token = (borrower, "released-token")
    table.acquire_exported_reference(child, source, token)
    assert table.release_borrowed_reference(child, token)

    with pytest.raises(ConflictingBorrowerTokenError, match="live binding"):
        table.prepare_stored_contained_reference(
            transfer, authority_worker_id=child_owner
        )
    assert transfer.provisional_hold not in table.snapshot(child).contained_holds


def test_borrowed_pin_uses_prepare_promote_for_every_storage_tier() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(33)
    child_owner = WorkerID.random()
    borrower = WorkerID.random()
    outer, _ = _object(34)
    original_hold = ContainedReferenceHold(
        outer, WorkerID.random(), "inline-source"
    )
    source = ContainedTransferSource(original_hold)
    transfer = PreparedContainedTransfer(
        child, child_owner, ("127.0.0.1", 27104),
        BorrowedContainedSource(borrower, "inline-token", source),
        ContainedReferenceHold(outer, borrower, "inline-final"),
        ContainedReferenceHold(outer, WorkerID.random(), "inline-final"),
    )
    table.register(child, current_attempt=attempt)
    table.add_contained_reference(child, original_hold)
    table.acquire_exported_reference(
        child, source, (borrower, "inline-token")
    )

    assert table.prepare_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.PREPARED
    assert table.promote_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.PROMOTED
    assert table.promote_stored_contained_reference(
        transfer, authority_worker_id=child_owner
    ) is StoredContainedReferenceDisposition.ALREADY_PROMOTED
    assert not hasattr(table, "install_inline_contained_reference")
    assert table.snapshot(child).contained_holds.issuperset(
        {original_hold, transfer.final_hold}
    )

    conflict = replace(
        transfer,
        source=BorrowedContainedSource(
            borrower, "inline-token",
            ContainedTransferSource("wrong-source"),
        ),
    )
    with pytest.raises(InvalidObjectTransitionError, match="conflict"):
        table.prepare_stored_contained_reference(
            conflict, authority_worker_id=child_owner
        )


def test_dead_holder_fences_prepare_and_promotion_without_custody_gap() -> None:
    table = ObjectOwnerTable()
    child, attempt = _object(4)
    child_owner = WorkerID.random()
    transfer = _owned_transfer(child, child_owner)
    table.register(child, current_attempt=attempt)
    table.install_dead_worker(
        transfer.provisional_hold.container_owner_worker_id, "death:executor"
    )
    with pytest.raises(DeadWorkerReferenceError):
        table.prepare_stored_contained_reference(
            transfer, authority_worker_id=child_owner
        )

    table = ObjectOwnerTable()
    child, attempt = _object(5)
    child_owner = WorkerID.random()
    live = _owned_transfer(child, child_owner)
    table.register(child, current_attempt=attempt)
    table.prepare_stored_contained_reference(
        live, authority_worker_id=child_owner
    )
    table.install_dead_worker(
        live.final_hold.container_owner_worker_id, "death:outer-owner"
    )
    with pytest.raises(DeadWorkerReferenceError):
        table.promote_stored_contained_reference(
            live, authority_worker_id=child_owner
        )
    assert table.snapshot(child).contained_holds == frozenset(
        {live.provisional_hold}
    )
