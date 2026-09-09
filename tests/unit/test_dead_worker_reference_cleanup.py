"""Pure owner-reducer contracts for authoritative Worker death cleanup."""

from __future__ import annotations

import pytest

from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    DeadWorkerReferenceConflictError,
    DeadWorkerReferenceError,
    ObjectOwnerTable,
    ObjectState,
    ReleasedTaskReferenceHoldError,
)
from miniray.protocol import (
    ContainedTransferSource,
    TaskHoldSource,
    TaskReferenceHold,
    TaskReferenceHoldKind,
)




def _objects(count: int) -> tuple[tuple[ObjectID, AttemptID], ...]:
    job_id = JobID.random()
    driver = TaskID.for_driver(job_id)
    return tuple(
        (
            ObjectID.for_task(task_id),
            AttemptID(task_id, 0),
        )
        for task_id in (
            TaskID.derive(job_id, driver, index) for index in range(count)
        )
    )


def _hold(
    kind: TaskReferenceHoldKind,
    submitter: WorkerID,
    task_index: int,
) -> TaskReferenceHold:
    job_id = JobID.random()
    task_id = TaskID.derive(
        job_id, TaskID.for_driver(job_id), task_index
    )
    return TaskReferenceHold(
        kind, submitter, task_id, AttemptID(task_id, 0)
    )


@pytest.mark.unit
def test_death_cleanup_is_narrow_and_only_reports_gc_candidates() -> None:
    table = ObjectOwnerTable()
    (guarded_id, guarded_attempt), (candidate_id, candidate_attempt), (
        unaffected_id,
        unaffected_attempt,
    ) = _objects(3)
    dead = WorkerID.random()
    live_executor = WorkerID.random()
    other_worker = WorkerID.random()

    for object_id, attempt_id, data in (
        (guarded_id, guarded_attempt, b"guarded"),
        (candidate_id, candidate_attempt, b"candidate"),
        (unaffected_id, unaffected_attempt, b"unaffected"),
    ):
        table.register(object_id, current_attempt=attempt_id)
        assert table.publish_inline(object_id, attempt_id, data)

    # The dead Worker owns both a direct borrower and two Task holds.  Live
    # executor borrowers derived from those holds have already crossed the
    # acquire linearization point and are independent lifetime reasons.
    guarded_transfer = ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), other_worker, "guarded-transfer"
    )
    table.add_contained_reference(guarded_id, guarded_transfer)
    dead_parent = (dead, "retained-parent")
    assert table.acquire_exported_reference(
        guarded_id, ContainedTransferSource(guarded_transfer), dead_parent
    )
    submitted = _hold(TaskReferenceHoldKind.SUBMITTED, dead, 0)
    retained = _hold(TaskReferenceHoldKind.RETAINED, dead, 1)
    assert table.add_submitted_reference(guarded_id, submitted)
    assert table.retain_borrowed_reference_for_task(
        guarded_id, dead_parent, retained
    )
    submitted_child = (live_executor, "submitted-child")
    retained_child = (live_executor, "retained-child")
    assert table.acquire_exported_reference(
        guarded_id, TaskHoldSource(submitted), submitted_child
    )
    assert table.acquire_exported_reference(
        guarded_id, TaskHoldSource(retained), retained_child
    )

    candidate_transfer = ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), other_worker, "candidate-transfer"
    )
    table.add_contained_reference(candidate_id, candidate_transfer)
    dead_candidate = (dead, "candidate-borrower")
    assert table.acquire_exported_reference(
        candidate_id,
        ContainedTransferSource(candidate_transfer),
        dead_candidate,
    )
    # Leave no non-dead lifetime reason on this entry.
    assert table.release_contained_reference(
        candidate_id, candidate_transfer
    )

    live_token = (other_worker, "unaffected-borrower")
    assert table.add_borrowed_reference(unaffected_id, live_token)

    cleanup = table.install_dead_worker(dead, "death-1")

    assert set(cleanup.affected_object_ids) == {guarded_id, candidate_id}
    assert cleanup.collectable_object_ids == (candidate_id,)
    assert cleanup.released_borrower_tokens == frozenset(
        {
            (guarded_id, dead_parent),
            (candidate_id, dead_candidate),
        }
    )
    assert cleanup.released_submitted_holds == frozenset(
        {(guarded_id, submitted)}
    )
    assert cleanup.released_retained_holds == frozenset(
        {(guarded_id, retained)}
    )

    guarded = table.snapshot(guarded_id)
    assert not guarded.submitted_tokens
    assert not guarded.retained_tokens
    assert {submitted_child, retained_child} <= guarded.borrowed_tokens
    assert dead_parent in guarded.released_borrowed_tokens
    assert (
        dead_parent, ContainedTransferSource(guarded_transfer)
    ) in guarded.borrowed_sources
    assert (retained, dead_parent) in guarded.retained_borrower_tokens

    # Exact acquire replay by a live executor remains idempotent even though
    # its source hold is now terminal.  A new acquire cannot use that hold.
    assert not table.acquire_exported_reference(
        guarded_id, TaskHoldSource(submitted), submitted_child
    )
    with pytest.raises(ReleasedTaskReferenceHoldError):
        table.acquire_exported_reference(
            guarded_id,
            TaskHoldSource(submitted),
            (live_executor, "new-child"),
        )

    # A delayed ordinary release must not turn death cleanup into the normal
    # all-child cascade.
    assert not table.release_submitted_reference(guarded_id, submitted)
    assert not table.release_retained_reference_for_task(guarded_id, retained)
    assert {submitted_child, retained_child} <= table.snapshot(
        guarded_id
    ).borrowed_tokens

    # The reducer reports a candidate but does not collect metadata or alter
    # the logical result.  Core owns the later GC attempt.
    assert table.contains(candidate_id)
    candidate = table.snapshot(candidate_id)
    assert candidate.state is ObjectState.READY_INLINE
    assert candidate.inline_data == b"candidate"
    assert not candidate.is_live
    assert table.snapshot(unaffected_id).borrowed_tokens == frozenset(
        {live_token}
    )


@pytest.mark.unit
def test_dead_fence_rejects_unseen_late_adds_and_preserves_normal_cascade() -> None:
    table = ObjectOwnerTable()
    (object_id, attempt_id), = _objects(1)
    dead = WorkerID.random()
    live = WorkerID.random()
    table.register(object_id, current_attempt=attempt_id)
    transfer = ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), live, "transfer"
    )
    table.add_contained_reference(object_id, transfer)

    cleanup = table.install_dead_worker(dead, "death-before-reference")
    assert not cleanup.affected_object_ids

    with pytest.raises(DeadWorkerReferenceError):
        table.acquire_exported_reference(
            object_id,
            ContainedTransferSource(transfer),
            (dead, "unseen-acquire"),
        )
    with pytest.raises(DeadWorkerReferenceError):
        table.add_borrowed_reference(object_id, (dead, "unseen-direct"))

    dead_submitted = _hold(TaskReferenceHoldKind.SUBMITTED, dead, 0)
    with pytest.raises(DeadWorkerReferenceError):
        table.add_submitted_reference(object_id, dead_submitted)

    # Retain is fenced by the complete hold's submitting Worker, even when its
    # borrower token belongs to a live Worker and has never been seen before.
    dead_retained = _hold(TaskReferenceHoldKind.RETAINED, dead, 1)
    with pytest.raises(DeadWorkerReferenceError):
        table.retain_borrowed_reference_for_task(
            object_id, (live, "unseen-parent"), dead_retained
        )

    # The death fence does not weaken normal explicit release semantics.
    live_hold = _hold(TaskReferenceHoldKind.SUBMITTED, live, 2)
    assert table.add_submitted_reference(object_id, live_hold)
    first = (WorkerID.random(), "first-live-child")
    second = (WorkerID.random(), "second-live-child")
    assert table.acquire_exported_reference(
        object_id, TaskHoldSource(live_hold), first
    )
    assert table.acquire_exported_reference(
        object_id, TaskHoldSource(live_hold), second
    )
    assert table.release_submitted_reference(object_id, live_hold)
    snapshot = table.snapshot(object_id)
    assert first not in snapshot.borrowed_tokens
    assert second not in snapshot.borrowed_tokens
    assert {first, second} <= snapshot.released_borrowed_tokens


@pytest.mark.unit
def test_dead_worker_replay_and_proof_conflicts_are_immutable() -> None:
    table = ObjectOwnerTable()
    worker = WorkerID.random()
    other = WorkerID.random()

    first = table.install_dead_worker(worker, "death-proof")
    replay = table.install_dead_worker(worker, "death-proof")
    assert replay is first
    assert table.dead_worker_record(worker) is first.record
    assert table.dead_worker_record(other) is None

    with pytest.raises(DeadWorkerReferenceConflictError, match="conflicts"):
        table.install_dead_worker(worker, "drifted-proof")
    with pytest.raises(
        DeadWorkerReferenceConflictError, match="another Worker"
    ):
        table.install_dead_worker(other, "death-proof")

    # A rejected proof cannot partially install a negative-admission fence.
    assert table.dead_worker_record(other) is None

    with pytest.raises(TypeError, match="WorkerID"):
        table.install_dead_worker("not-a-worker", "proof")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="non-empty"):
        table.install_dead_worker(other, "")
