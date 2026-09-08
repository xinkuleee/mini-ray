"""Pure owner contracts for foreign-lineage hold replacement."""

from __future__ import annotations

from dataclasses import replace
import threading

import pytest

from miniray.ids import AttemptID, ObjectID, TaskID, WorkerID
from miniray.core import CoreWorker
from miniray.ownership import (
    ObjectOwnerTable,
    RetainedHoldReplacementBusyError,
    RetainedHoldReplacementConflictError,
    RetainedHoldReplacementDisposition,
)
from miniray.protocol import (
    ReplaceRetainedObjectDisposition,
    ReplaceRetainedObjectFailure,
    ReplaceRetainedObjectForTask,
    TaskHoldSource,
    TaskReferenceHold,
    TaskReferenceHoldKind,
)
from miniray.retained_replacement import replace_retained_object_for_task


pytestmark = pytest.mark.unit


def _fixture() -> tuple[
    ObjectOwnerTable, ObjectID, WorkerID, object, TaskReferenceHold,
    TaskReferenceHold,
]:
    table = ObjectOwnerTable()
    object_id = ObjectID.for_task(TaskID.random())
    borrower = WorkerID.random()
    parent = (borrower, "parent-borrower")
    task_id = TaskID.random()
    old = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, borrower, task_id, AttemptID(task_id, 0)
    )
    new = replace(old, origin_attempt_id=AttemptID(task_id, 1))
    table.register(object_id)
    table.add_borrowed_reference(object_id, parent)
    assert table.retain_borrowed_reference_for_task(object_id, parent, old)
    return table, object_id, borrower, parent, old, new


def test_replace_is_atomic_transfers_parent_and_tombstones_old() -> None:
    table, object_id, _borrower, parent, old, new = _fixture()

    result = table.replace_retained_reference_for_task(object_id, old, new)

    assert result is RetainedHoldReplacementDisposition.REPLACED
    snapshot = table.snapshot(object_id)
    assert old not in snapshot.retained_tokens
    assert old in snapshot.released_retained_tokens
    assert new in snapshot.retained_tokens
    assert (new, parent) in snapshot.retained_borrower_tokens
    assert table.retained_release_was_seen(object_id, old)
    assert table.retained_hold_replacement(object_id, old) == new


def test_exact_replay_is_already_replaced_and_drift_conflicts() -> None:
    table, object_id, borrower, _parent, old, new = _fixture()
    assert table.replace_retained_reference_for_task(
        object_id, old, new
    ) is RetainedHoldReplacementDisposition.REPLACED

    assert table.replace_retained_reference_for_task(
        object_id, old, new
    ) is RetainedHoldReplacementDisposition.ALREADY_REPLACED
    drift = replace(
        new, origin_attempt_id=AttemptID(new.task_id, 2)
    )
    with pytest.raises(RetainedHoldReplacementConflictError):
        table.replace_retained_reference_for_task(object_id, old, drift)
    assert table.snapshot(object_id).retained_tokens == frozenset({new})
    assert new.submitting_worker_id == borrower


def test_active_attempt_borrower_makes_replacement_busy_without_mutation() -> None:
    table, object_id, _borrower, parent, old, new = _fixture()
    attempt_borrower = (WorkerID.random(), "attempt-borrower")
    assert table.acquire_exported_reference(
        object_id, TaskHoldSource(old), attempt_borrower
    )

    with pytest.raises(RetainedHoldReplacementBusyError):
        table.replace_retained_reference_for_task(object_id, old, new)

    snapshot = table.snapshot(object_id)
    assert snapshot.retained_tokens == frozenset({old})
    assert (old, parent) in snapshot.retained_borrower_tokens
    assert new not in snapshot.retained_tokens
    assert not table.retained_release_was_seen(object_id, old)
    assert table.retained_hold_replacement(object_id, old) is None

    assert table.release_borrowed_reference(object_id, attempt_borrower)
    assert table.replace_retained_reference_for_task(
        object_id, old, new
    ) is RetainedHoldReplacementDisposition.REPLACED


def test_typed_adapter_distinguishes_success_replay_busy_and_conflict() -> None:
    table, object_id, borrower, _parent, old, new = _fixture()
    owner = WorkerID.random()
    request = ReplaceRetainedObjectForTask(
        object_id, owner, borrower, old, new
    )

    first = replace_retained_object_for_task(owner, table, request)
    replay = replace_retained_object_for_task(owner, table, request)
    conflict = replace_retained_object_for_task(
        owner, table, replace(
            request, replacement_hold=replace(
                new, origin_attempt_id=AttemptID(new.task_id, 2)
            )
        )
    )

    assert first.disposition is ReplaceRetainedObjectDisposition.REPLACED
    assert replay.disposition is (
        ReplaceRetainedObjectDisposition.ALREADY_REPLACED
    )
    assert conflict.disposition is ReplaceRetainedObjectDisposition.FAILED
    assert conflict.failure is ReplaceRetainedObjectFailure.CONFLICT
    assert (
        first.object_id, first.owner_worker_id, first.borrower_worker_id,
        first.expected_hold, first.replacement_hold,
    ) == (
        request.object_id, request.owner_worker_id, request.borrower_worker_id,
        request.expected_hold, request.replacement_hold,
    )


def test_release_and_replacement_tombstones_remain_typed() -> None:
    table, object_id, borrower, _parent, old, new = _fixture()
    owner = WorkerID.random()
    assert table.release_retained_reference_for_task(object_id, old)
    released_old = replace_retained_object_for_task(
        owner, table,
        ReplaceRetainedObjectForTask(object_id, owner, borrower, old, new),
    )
    assert released_old.failure is ReplaceRetainedObjectFailure.RELEASED_OLD_HOLD

    table, object_id, borrower, _parent, old, new = _fixture()
    assert not table.release_retained_reference_for_task(object_id, new)
    released_new = replace_retained_object_for_task(
        owner, table,
        ReplaceRetainedObjectForTask(object_id, owner, borrower, old, new),
    )
    assert released_new.failure is ReplaceRetainedObjectFailure.REPLACEMENT_RELEASED


def test_committed_borrower_death_is_a_typed_replacement_failure() -> None:
    table, object_id, borrower, _parent, old, new = _fixture()
    owner = WorkerID.random()
    table.install_dead_worker(borrower, "death-proof")

    reply = replace_retained_object_for_task(
        owner, table,
        ReplaceRetainedObjectForTask(object_id, owner, borrower, old, new),
    )

    # The replacement reducer never admits a fresh credential for a dead
    # submitting Worker, even though death cleanup also tombstoned the old hold.
    assert reply.disposition is ReplaceRetainedObjectDisposition.FAILED
    assert reply.failure is ReplaceRetainedObjectFailure.DEAD_BORROWER


def test_core_owner_endpoint_counts_live_operation_and_types_shutdown() -> None:
    table, object_id, borrower, _parent, old, new = _fixture()
    owner = WorkerID.random()
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core._owner_table = table
    core._completion = threading.Condition(threading.RLock())
    core._owner_protocol_open = True
    core._inflight_borrow_ops = 0
    request = ReplaceRetainedObjectForTask(
        object_id, owner, borrower, old, new
    )

    accepted = core.replace_retained_object_for_task(request)
    assert accepted.disposition is ReplaceRetainedObjectDisposition.REPLACED
    assert core._inflight_borrow_ops == 0

    core._owner_protocol_open = False
    stopped = core.replace_retained_object_for_task(request)
    assert stopped.disposition is ReplaceRetainedObjectDisposition.FAILED
    assert stopped.failure is ReplaceRetainedObjectFailure.OWNER_STOPPED
    assert core._inflight_borrow_ops == 0
