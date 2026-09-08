"""Pure typed adapter for owner-side retained-hold replacement."""

from __future__ import annotations

from .ownership import (
    DeadWorkerReferenceError,
    InactiveTaskReferenceHoldError,
    ObjectCollectionInProgressError,
    ObjectOwnerTable,
    ReleasedRetainedTokenError,
    RetainedHoldReplacementBusyError,
    RetainedHoldReplacementConflictError,
    RetainedHoldReplacementDisposition,
    UnknownObjectError,
)
from .protocol import (
    ReplaceRetainedObjectDisposition,
    ReplaceRetainedObjectFailure,
    ReplaceRetainedObjectForTask,
    ReplaceRetainedObjectForTaskReply,
)


def replace_retained_object_for_task(
    owner_worker_id: object,
    owner_table: ObjectOwnerTable,
    request: ReplaceRetainedObjectForTask,
) -> ReplaceRetainedObjectForTaskReply:
    """Validate owner identity and map the atomic reducer to typed wire data."""

    if not isinstance(request, ReplaceRetainedObjectForTask):
        raise TypeError(
            "retained replacement expects ReplaceRetainedObjectForTask"
        )
    if not isinstance(owner_table, ObjectOwnerTable):
        raise TypeError("owner_table must be an ObjectOwnerTable")
    if request.owner_worker_id != owner_worker_id:
        return _failure(
            request, ReplaceRetainedObjectFailure.WRONG_OWNER,
            "request targets a different object owner",
        )
    try:
        disposition = owner_table.replace_retained_reference_for_task(
            request.object_id, request.expected_hold, request.replacement_hold
        )
    except RetainedHoldReplacementBusyError as exc:
        return _failure(
            request, ReplaceRetainedObjectFailure.OLD_HOLD_BUSY, str(exc)
        )
    except DeadWorkerReferenceError as exc:
        return _failure(
            request, ReplaceRetainedObjectFailure.DEAD_BORROWER, str(exc)
        )
    except RetainedHoldReplacementConflictError as exc:
        return _failure(request, ReplaceRetainedObjectFailure.CONFLICT, str(exc))
    except ReleasedRetainedTokenError as exc:
        replacement_released = owner_table.retained_release_was_seen(
            request.object_id, request.replacement_hold
        )
        return _failure(
            request,
            (ReplaceRetainedObjectFailure.REPLACEMENT_RELEASED
             if replacement_released
             else ReplaceRetainedObjectFailure.RELEASED_OLD_HOLD),
            str(exc),
        )
    except InactiveTaskReferenceHoldError as exc:
        return _failure(
            request, ReplaceRetainedObjectFailure.INACTIVE_OLD_HOLD, str(exc)
        )
    except ObjectCollectionInProgressError as exc:
        return _failure(
            request, ReplaceRetainedObjectFailure.COLLECTION_IN_PROGRESS, str(exc)
        )
    except UnknownObjectError as exc:
        return _failure(
            request, ReplaceRetainedObjectFailure.UNKNOWN_OBJECT, str(exc)
        )

    mapped = (
        ReplaceRetainedObjectDisposition.REPLACED
        if disposition is RetainedHoldReplacementDisposition.REPLACED
        else ReplaceRetainedObjectDisposition.ALREADY_REPLACED
    )
    return ReplaceRetainedObjectForTaskReply(
        request.object_id, request.owner_worker_id, request.borrower_worker_id,
        request.expected_hold, request.replacement_hold, mapped,
    )


def _failure(
    request: ReplaceRetainedObjectForTask,
    failure: ReplaceRetainedObjectFailure,
    detail: str,
) -> ReplaceRetainedObjectForTaskReply:
    return ReplaceRetainedObjectForTaskReply(
        request.object_id, request.owner_worker_id, request.borrower_worker_id,
        request.expected_hold, request.replacement_hold,
        ReplaceRetainedObjectDisposition.FAILED,
        failure=failure, detail=detail or failure.value,
    )


__all__ = ["replace_retained_object_for_task"]
