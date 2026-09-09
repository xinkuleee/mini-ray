"""Pure owner-table contracts for typed nested-reference borrow sources."""

from __future__ import annotations

import pytest

from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ConflictingBorrowerTokenError,
    InactiveTaskReferenceHoldError,
    ObjectOwnerTable,
    ReleasedBorrowerTokenError,
    ReleasedTaskReferenceHoldError,
)
from miniray.protocol import (
    ContainedTransferSource,
    TaskHoldSource,
    TaskReferenceHold,
    TaskReferenceHoldKind,
)


pytestmark = pytest.mark.unit


def _identity() -> tuple[ObjectID, TaskID]:
    job_id = JobID.random()
    producer = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    consumer = TaskID.derive(job_id, TaskID.for_driver(job_id), 1)
    return ObjectID.for_task(producer), consumer


def _hold(
    kind: TaskReferenceHoldKind,
    worker_id: WorkerID,
    task_id: TaskID,
    origin_attempt: int = 0,
) -> TaskReferenceHold:
    return TaskReferenceHold(
        kind, worker_id, task_id, AttemptID(task_id, origin_attempt)
    )


def _task() -> TaskID:
    job_id = JobID.random()
    return TaskID.derive(job_id, TaskID.for_driver(job_id), 0)


def test_contained_source_replays_exact_hold_and_does_not_cascade_on_release() -> None:
    table = ObjectOwnerTable()
    object_id, task_id = _identity()
    table.register(object_id)
    contained_hold = ContainedReferenceHold(ObjectID.for_task(task_id), WorkerID.random(), "transfer")
    table.add_contained_reference(object_id, contained_hold)
    hold = _hold(
        TaskReferenceHoldKind.SUBMITTED, WorkerID.random(), task_id
    )
    table.add_submitted_reference(object_id, hold)
    borrower = (WorkerID.random(), "attempt-borrower")
    contained = ContainedTransferSource(contained_hold)

    assert table.acquire_exported_reference(object_id, contained, borrower)
    assert not table.acquire_exported_reference(object_id, contained, borrower)
    snapshot = table.snapshot(object_id)
    assert (borrower, contained) in snapshot.borrowed_sources
    assert snapshot.contained_holds == frozenset({contained_hold})

    conflicting = TaskHoldSource(hold)
    with pytest.raises(ConflictingBorrowerTokenError, match="another source"):
        table.acquire_exported_reference(object_id, conflicting, borrower)

    # Existing contained-transfer semantics deliberately do not tie a borrower
    # to the lifetime of the transfer pin after the acquire acknowledgement.
    assert table.release_contained_reference(object_id, contained_hold)
    assert table.has_borrowed_reference(object_id, borrower)
    assert table.release_borrowed_reference(object_id, borrower)


def test_submitted_hold_release_atomically_retires_derived_borrowers() -> None:
    table = ObjectOwnerTable()
    object_id, task_id = _identity()
    table.register(object_id)
    hold = _hold(
        TaskReferenceHoldKind.SUBMITTED, WorkerID.random(), task_id
    )
    source = TaskHoldSource(
        hold
    )
    first = (WorkerID.random(), "attempt-0-a")
    second = (WorkerID.random(), "attempt-0-b")

    assert table.add_submitted_reference(object_id, hold)
    assert not table.add_submitted_reference(object_id, hold)
    assert table.snapshot(object_id).submitted_tokens == frozenset({hold})
    assert table.acquire_exported_reference(object_id, source, first)
    assert not table.acquire_exported_reference(object_id, source, first)
    assert table.acquire_exported_reference(object_id, source, second)

    assert table.release_submitted_reference(object_id, hold)
    snapshot = table.snapshot(object_id)
    assert not snapshot.submitted_tokens
    assert not snapshot.borrowed_tokens
    assert {first, second} <= snapshot.released_borrowed_tokens

    with pytest.raises(ReleasedBorrowerTokenError):
        table.acquire_exported_reference(object_id, source, first)
    with pytest.raises(ReleasedTaskReferenceHoldError):
        table.acquire_exported_reference(
            object_id, source, (WorkerID.random(), "late-attempt")
        )
    with pytest.raises(ReleasedTaskReferenceHoldError):
        table.add_submitted_reference(object_id, hold)


def test_retained_hold_validates_exact_namespace_and_cascades_only_its_children() -> None:
    table = ObjectOwnerTable()
    object_id, task_id = _identity()
    table.register(object_id)
    submitter = WorkerID.random()
    parent = (submitter, "parent-borrower")
    hold = _hold(TaskReferenceHoldKind.RETAINED, submitter, task_id)
    table.add_borrowed_reference(object_id, parent)
    assert table.retain_borrowed_reference_for_task(
        object_id, parent, hold
    )
    assert not table.retain_borrowed_reference_for_task(
        object_id, parent, hold
    )
    assert table.snapshot(object_id).retained_tokens == frozenset({hold})
    source = TaskHoldSource(hold)
    derived = (WorkerID.random(), "attempt-borrower")

    assert table.acquire_exported_reference(object_id, source, derived)
    wrong_submitter = TaskHoldSource(
        _hold(
            TaskReferenceHoldKind.RETAINED,
            WorkerID.random(),
            task_id,
        )
    )
    with pytest.raises(InactiveTaskReferenceHoldError):
        table.acquire_exported_reference(
            object_id, wrong_submitter, (WorkerID.random(), "wrong")
        )

    assert table.release_retained_reference_for_task(object_id, hold)
    snapshot = table.snapshot(object_id)
    assert derived not in snapshot.borrowed_tokens
    assert derived in snapshot.released_borrowed_tokens
    # The source borrower is an independent lifetime reason.
    assert parent in snapshot.borrowed_tokens
    with pytest.raises(ReleasedTaskReferenceHoldError):
        table.acquire_exported_reference(
            object_id, source, (WorkerID.random(), "late")
        )

    reconstructed = _hold(
        TaskReferenceHoldKind.RETAINED, submitter, task_id, 1
    )
    assert table.retain_borrowed_reference_for_task(
        object_id, parent, reconstructed
    )
    assert not table.retain_borrowed_reference_for_task(
        object_id, parent, reconstructed
    )
    assert table.has_retained_reference_for_task(object_id, reconstructed)
    assert table.release_retained_reference_for_task(
        object_id, reconstructed
    )
    assert table.release_borrowed_reference(object_id, parent)


@pytest.mark.parametrize(
    "kind",
    (TaskReferenceHoldKind.SUBMITTED, TaskReferenceHoldKind.RETAINED),
)
def test_release_before_task_hold_acquire_fences_late_delivery(
    kind: TaskReferenceHoldKind,
) -> None:
    table = ObjectOwnerTable()
    object_id, task_id = _identity()
    table.register(object_id)
    submitter = WorkerID.random()
    hold = _hold(kind, submitter, task_id)
    source = TaskHoldSource(hold)

    if kind is TaskReferenceHoldKind.SUBMITTED:
        assert not table.release_submitted_reference(object_id, hold)
    else:
        assert not table.release_retained_reference_for_task(object_id, hold)

    with pytest.raises(ReleasedTaskReferenceHoldError):
        table.acquire_exported_reference(
            object_id, source, (WorkerID.random(), "delayed-acquire")
        )


def test_hold_authorization_and_tombstones_bind_every_credential_field() -> None:
    table = ObjectOwnerTable()
    object_id, task_id = _identity()
    table.register(object_id)
    submitter = WorkerID.random()
    active = _hold(TaskReferenceHoldKind.SUBMITTED, submitter, task_id)
    assert table.add_submitted_reference(object_id, active)

    drifts = (
        _hold(
            TaskReferenceHoldKind.SUBMITTED, WorkerID.random(), task_id
        ),
        _hold(TaskReferenceHoldKind.SUBMITTED, submitter, _task()),
        _hold(TaskReferenceHoldKind.SUBMITTED, submitter, task_id, 1),
        _hold(TaskReferenceHoldKind.RETAINED, submitter, task_id),
    )
    for index, drift in enumerate(drifts):
        with pytest.raises(InactiveTaskReferenceHoldError):
            table.acquire_exported_reference(
                object_id,
                TaskHoldSource(drift),
                (WorkerID.random(), "drift-{}".format(index)),
            )

    with pytest.raises(ValueError, match="SUBMITTED"):
        table.add_submitted_reference(object_id, drifts[-1])
    with pytest.raises(ValueError, match="RETAINED"):
        table.release_retained_reference_for_task(object_id, active)

    assert table.release_submitted_reference(object_id, active)
    with pytest.raises(ReleasedTaskReferenceHoldError):
        table.acquire_exported_reference(
            object_id,
            TaskHoldSource(active),
            (WorkerID.random(), "released-exact"),
        )

    # A terminal tombstone fences exactly one hold incarnation.  Reconstruction
    # of the same logical Task may install a fresh origin attempt, while exact
    # replay of that new credential remains idempotent.
    reconstructed = _hold(
        TaskReferenceHoldKind.SUBMITTED, submitter, task_id, 1
    )
    assert table.add_submitted_reference(object_id, reconstructed)
    assert not table.add_submitted_reference(object_id, reconstructed)
    assert table.snapshot(object_id).submitted_tokens == frozenset(
        {reconstructed}
    )


def test_release_cascades_only_from_the_exact_complete_hold() -> None:
    table = ObjectOwnerTable()
    object_id, task_id = _identity()
    table.register(object_id)
    submitter = WorkerID.random()
    exact = _hold(TaskReferenceHoldKind.SUBMITTED, submitter, task_id)
    other_origin = _hold(
        TaskReferenceHoldKind.SUBMITTED, submitter, task_id, 1
    )
    other_submitter = _hold(
        TaskReferenceHoldKind.SUBMITTED, WorkerID.random(), task_id
    )
    retained = _hold(TaskReferenceHoldKind.RETAINED, submitter, task_id)
    parent = (submitter, "retained-parent")

    for hold in (exact, other_origin, other_submitter):
        assert table.add_submitted_reference(object_id, hold)
    table.add_borrowed_reference(object_id, parent)
    assert table.retain_borrowed_reference_for_task(
        object_id, parent, retained
    )

    children = {
        exact: (WorkerID.random(), "exact-child"),
        other_origin: (WorkerID.random(), "other-origin-child"),
        other_submitter: (WorkerID.random(), "other-submitter-child"),
        retained: (WorkerID.random(), "retained-child"),
    }
    for hold, child in children.items():
        assert table.acquire_exported_reference(
            object_id, TaskHoldSource(hold), child
        )

    assert table.release_submitted_reference(object_id, exact)
    snapshot = table.snapshot(object_id)
    assert children[exact] not in snapshot.borrowed_tokens
    assert children[exact] in snapshot.released_borrowed_tokens
    for hold in (other_origin, other_submitter, retained):
        assert children[hold] in snapshot.borrowed_tokens

    assert table.release_submitted_reference(object_id, other_origin)
    assert table.release_submitted_reference(object_id, other_submitter)
    assert table.release_retained_reference_for_task(object_id, retained)
    assert table.release_borrowed_reference(object_id, parent)
