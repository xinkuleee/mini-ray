"""Pure schema contracts for Worker blocking episode notifications."""

from __future__ import annotations

from dataclasses import replace

import pytest

from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, TaskID, WorkerID


pytestmark = pytest.mark.unit


def _identity() -> tuple[LeaseID, TaskID, AttemptID, WorkerID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return LeaseID.random(), task_id, AttemptID(task_id, 0), WorkerID.random()


def test_block_and_unblock_share_complete_execution_and_episode_identity() -> None:
    lease_id, task_id, attempt_id, worker_id = _identity()
    blocked = protocol.NotifyWorkerBlocked(
        lease_id, task_id, attempt_id, worker_id, 7
    )
    unblocked = protocol.NotifyWorkerUnblocked(
        lease_id, task_id, attempt_id, worker_id, 7
    )

    assert (
        blocked.lease_id, blocked.task_id, blocked.attempt_id,
        blocked.worker_id, blocked.sequence,
    ) == (
        unblocked.lease_id, unblocked.task_id, unblocked.attempt_id,
        unblocked.worker_id, unblocked.sequence,
    )
    assert blocked == protocol.NotifyWorkerBlocked(
        lease_id, task_id, attempt_id, worker_id, 7
    )


@pytest.mark.parametrize(
    "message_type",
    [protocol.NotifyWorkerBlocked, protocol.NotifyWorkerUnblocked],
)
def test_notification_rejects_wrong_attempt_and_invalid_sequence(
    message_type: type,
) -> None:
    lease_id, task_id, _attempt_id, worker_id = _identity()
    other_job = JobID.random()
    other_task = TaskID.derive(
        other_job, TaskID.for_driver(other_job), 0
    )

    with pytest.raises(ProtocolError, match="belong"):
        message_type(lease_id, task_id, AttemptID(other_task, 0), worker_id, 0)
    with pytest.raises(ProtocolError, match="non-negative"):
        message_type(lease_id, task_id, AttemptID(task_id, 0), worker_id, -1)
    with pytest.raises(ProtocolError, match="non-negative"):
        message_type(lease_id, task_id, AttemptID(task_id, 0), worker_id, True)


@pytest.mark.parametrize(
    "reply_type",
    [protocol.NotifyWorkerBlockedReply, protocol.NotifyWorkerUnblockedReply],
)
def test_notification_reply_encodes_first_transition_and_idempotent_replay(
    reply_type: type,
) -> None:
    identity = _identity()
    first = reply_type(
        *identity, 3, protocol.LeaseExecutionState.RUNNING, True, True
    )
    replay = replace(first, changed=False)

    assert first.accepted and first.changed
    assert replay.accepted and not replay.changed
    assert replay.sequence == first.sequence


@pytest.mark.parametrize(
    "reply_type",
    [protocol.NotifyWorkerBlockedReply, protocol.NotifyWorkerUnblockedReply],
)
def test_notification_reply_rejects_impossible_acknowledgements(
    reply_type: type,
) -> None:
    identity = _identity()

    with pytest.raises(ProtocolError, match="cannot change"):
        reply_type(
            *identity, 0, protocol.LeaseExecutionState.RUNNING,
            False, True, "stale",
        )
    with pytest.raises(ProtocolError, match="RUNNING"):
        reply_type(
            *identity, 0, protocol.LeaseExecutionState.COMPLETED,
            True, False,
        )
