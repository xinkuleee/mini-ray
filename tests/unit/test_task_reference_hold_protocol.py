"""Wire contracts for fully bound retained-task hold capabilities."""

from __future__ import annotations

from dataclasses import fields, replace

import pytest

from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.protocol import (
    GetRetainedOwnedObject,
    GetRetainedOwnedObjectReply,
    ObjectStoreDescriptor,
    OwnedObjectState,
    ProtocolError,
    ReplaceRetainedObjectDisposition,
    ReplaceRetainedObjectFailure,
    ReplaceRetainedObjectForTask,
    ReplaceRetainedObjectForTaskReply,
    ReleaseOwnedObjectForTask,
    ReleaseOwnedObjectForTaskReply,
    ReportRetainedObjectLocation,
    ReportRetainedObjectLocationReply,
    RetainOwnedObjectForTask,
    RetainOwnedObjectForTaskReply,
    RetainedLocationReportStatus,
    TaskReferenceHold,
    TaskReferenceHoldKind,
)


pytestmark = pytest.mark.unit


_MESSAGE_TYPES = (
    RetainOwnedObjectForTask,
    RetainOwnedObjectForTaskReply,
    GetRetainedOwnedObject,
    GetRetainedOwnedObjectReply,
    ReportRetainedObjectLocation,
    ReportRetainedObjectLocationReply,
    ReleaseOwnedObjectForTask,
    ReleaseOwnedObjectForTaskReply,
)


def _identity() -> tuple[ObjectID, WorkerID, WorkerID, TaskID]:
    producer = TaskID.random()
    return (
        ObjectID.for_task(producer),
        WorkerID.random(),
        WorkerID.random(),
        TaskID.random(),
    )


def _descriptor(
    object_id: ObjectID, owner_worker_id: WorkerID
) -> ObjectStoreDescriptor:
    return ObjectStoreDescriptor(
        object_id,
        owner_worker_id,
        AttemptID(object_id.task_id, 0),
        NodeID.random(),
        1,
        "0" * 64,
    )


def _build_message(
    message_type: type[object],
    object_id: ObjectID,
    owner_worker_id: WorkerID,
    borrower_worker_id: WorkerID,
    hold: object,
) -> object:
    if message_type is RetainOwnedObjectForTask:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, "borrow", hold
        )
    if message_type is RetainOwnedObjectForTaskReply:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, "borrow", hold,
            True, True,
        )
    if message_type is GetRetainedOwnedObject:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, hold
        )
    if message_type is GetRetainedOwnedObjectReply:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, hold, True,
            state=OwnedObjectState.PENDING,
        )
    descriptor = _descriptor(object_id, owner_worker_id)
    if message_type is ReportRetainedObjectLocation:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, hold, descriptor
        )
    if message_type is ReportRetainedObjectLocationReply:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, hold, descriptor,
            RetainedLocationReportStatus.ADDED,
        )
    if message_type is ReleaseOwnedObjectForTask:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, hold
        )
    if message_type is ReleaseOwnedObjectForTaskReply:
        return message_type(
            object_id, owner_worker_id, borrower_worker_id, hold, True, True
        )
    raise AssertionError(message_type)


@pytest.mark.parametrize("message_type", _MESSAGE_TYPES)
def test_retained_task_messages_echo_the_complete_hold(
    message_type: type[object],
) -> None:
    object_id, owner, borrower, task_id = _identity()
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED,
        borrower,
        task_id,
        AttemptID(task_id, 0),
    )

    message = _build_message(message_type, object_id, owner, borrower, hold)

    assert message.hold == hold  # type: ignore[attr-defined]
    field_names = {field.name for field in fields(message_type)}
    assert "hold" in field_names
    assert "retained_token" not in field_names


@pytest.mark.parametrize("message_type", _MESSAGE_TYPES)
def test_retained_task_messages_reject_raw_token_compatibility(
    message_type: type[object],
) -> None:
    object_id, owner, borrower, _ = _identity()

    with pytest.raises(ProtocolError, match="hold must be a TaskReferenceHold"):
        _build_message(message_type, object_id, owner, borrower, "task:raw")


@pytest.mark.parametrize("message_type", _MESSAGE_TYPES)
def test_retained_task_messages_reject_submitted_hold_kind(
    message_type: type[object],
) -> None:
    object_id, owner, borrower, task_id = _identity()
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.SUBMITTED,
        borrower,
        task_id,
        AttemptID(task_id, 0),
    )

    with pytest.raises(ProtocolError, match="must have RETAINED kind"):
        _build_message(message_type, object_id, owner, borrower, hold)


@pytest.mark.parametrize("message_type", _MESSAGE_TYPES)
def test_retained_task_messages_reject_another_submitting_worker(
    message_type: type[object],
) -> None:
    object_id, owner, borrower, task_id = _identity()
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED,
        WorkerID.random(),
        task_id,
        AttemptID(task_id, 0),
    )

    with pytest.raises(ProtocolError, match="must match borrower_worker_id"):
        _build_message(message_type, object_id, owner, borrower, hold)


def test_task_hold_origin_attempt_must_belong_to_task_id() -> None:
    borrower = WorkerID.random()
    first_task = TaskID.random()
    second_task = TaskID.random()

    with pytest.raises(ProtocolError, match="origin attempt must belong to task_id"):
        TaskReferenceHold(
            TaskReferenceHoldKind.RETAINED,
            borrower,
            second_task,
            AttemptID(first_task, 0),
        )


def test_task_hold_schema_has_origin_attempt_and_no_legacy_token_api() -> None:
    field_names = {field.name for field in fields(TaskReferenceHold)}

    assert field_names == {
        "kind", "submitting_worker_id", "task_id", "origin_attempt_id"
    }
    assert not hasattr(TaskReferenceHold, "token_for")


def test_same_task_different_origin_attempts_are_distinct_hold_incarnations() -> None:
    borrower = WorkerID.random()
    task_id = TaskID.random()

    first = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, borrower, task_id, AttemptID(task_id, 0)
    )
    reconstructed = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, borrower, task_id, AttemptID(task_id, 1)
    )

    assert first != reconstructed
    assert first.task_id == reconstructed.task_id
    assert first.origin_attempt_id != reconstructed.origin_attempt_id


def test_retained_get_echoes_current_attempt_and_fences_stored_epoch() -> None:
    object_id, owner, borrower, task_id = _identity()
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, borrower, task_id, AttemptID(task_id, 0)
    )
    current = AttemptID(object_id.task_id, 0)
    reply = GetRetainedOwnedObjectReply(
        object_id, owner, borrower, hold, True,
        state=OwnedObjectState.READY_STORED, current_attempt=current,
        descriptor=_descriptor(object_id, owner),
    )
    assert reply.current_attempt == current
    with pytest.raises(ProtocolError, match="match current attempt"):
        replace(reply, current_attempt=current.next())
    with pytest.raises(ProtocolError, match="cannot expose current attempt"):
        GetRetainedOwnedObjectReply(
            object_id, owner, borrower, hold, False,
            current_attempt=current, detail="rejected",
        )


def test_replacement_protocol_requires_monotonic_same_task_holds() -> None:
    object_id, owner, borrower, task_id = _identity()
    old = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, borrower, task_id, AttemptID(task_id, 0)
    )
    new = replace(old, origin_attempt_id=AttemptID(task_id, 1))
    request = ReplaceRetainedObjectForTask(
        object_id, owner, borrower, old, new
    )
    reply = ReplaceRetainedObjectForTaskReply(
        object_id, owner, borrower, old, new,
        ReplaceRetainedObjectDisposition.REPLACED,
    )
    assert reply.expected_hold == request.expected_hold
    assert reply.replacement_hold == request.replacement_hold
    with pytest.raises(ProtocolError, match="origin attempt must increase"):
        replace(request, replacement_hold=old)
    with pytest.raises(ProtocolError, match="share submitter and task"):
        other_task = TaskID.random()
        replace(
            request,
            replacement_hold=TaskReferenceHold(
                TaskReferenceHoldKind.RETAINED, borrower, other_task,
                AttemptID(other_task, 1),
            ),
        )
    with pytest.raises(ProtocolError, match="typed failure"):
        replace(
            reply, disposition=ReplaceRetainedObjectDisposition.FAILED,
            detail="missing type",
        )
    failed = ReplaceRetainedObjectForTaskReply(
        object_id, owner, borrower, old, new,
        ReplaceRetainedObjectDisposition.FAILED,
        failure=ReplaceRetainedObjectFailure.OLD_HOLD_BUSY,
        detail="attempt borrower remains",
    )
    assert failed.failure is ReplaceRetainedObjectFailure.OLD_HOLD_BUSY
