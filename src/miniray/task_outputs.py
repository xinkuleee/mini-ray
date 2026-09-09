"""Identity for the single logical output of an ordinary task.

An ordinary task has exactly one ``ObjectID``, at return index zero, which
stays stable across retries and reconstruction. The manifest records that
identity, while an execution key names the physical attempt allowed to
publish it.

It deliberately contains no scheduler, object-store, or RPC behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from .ids import AttemptID, ObjectID, TaskID


MAX_TASK_RETURNS = 1


def validate_num_returns(value: object) -> int:
    """Reject output splitting: a task returns one complete Python value."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("num_returns must be an integer")
    if value != 1:
        raise ValueError("num_returns must be exactly 1")
    return value


@dataclass(frozen=True)
class TaskOutputManifest:
    """The single logical output identity of one task."""

    task_id: TaskID
    output_ids: Tuple[ObjectID, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise TypeError("task output manifest task_id must be a TaskID")
        output_ids = tuple(self.output_ids)
        validate_num_returns(len(output_ids))
        expected = (ObjectID.for_task(self.task_id, 0),)
        if output_ids != expected:
            raise ValueError(
                "task output manifest must contain only return index zero"
            )
        object.__setattr__(self, "output_ids", output_ids)

    @classmethod
    def for_task(cls, task_id: TaskID, num_returns: int) -> "TaskOutputManifest":
        validate_num_returns(num_returns)
        return cls(task_id, (ObjectID.for_task(task_id, 0),))

    @classmethod
    def from_task_spec(cls, task_spec: object) -> "TaskOutputManifest":
        try:
            task_id = task_spec.task_id
            return_ids = task_spec.return_ids
        except AttributeError as exc:
            raise TypeError(
                "task_spec must expose task_id and return_ids()"
            ) from exc
        if not callable(return_ids):
            raise TypeError("task_spec.return_ids must be callable")
        return cls(task_id, tuple(return_ids()))

    @property
    def num_returns(self) -> int:
        return len(self.output_ids)


@dataclass(frozen=True)
class TaskExecutionKey:
    """One physical attempt authorized to publish the task's output."""

    manifest: TaskOutputManifest
    attempt_id: AttemptID

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, TaskOutputManifest):
            raise TypeError("execution manifest must be a TaskOutputManifest")
        if not isinstance(self.attempt_id, AttemptID):
            raise TypeError("execution attempt_id must be an AttemptID")
        if self.attempt_id.task_id != self.manifest.task_id:
            raise ValueError("execution attempt must belong to manifest task_id")

    @classmethod
    def from_task_spec(cls, task_spec: object) -> "TaskExecutionKey":
        manifest = TaskOutputManifest.from_task_spec(task_spec)
        try:
            attempt_id = task_spec.attempt_id
        except AttributeError as exc:
            raise TypeError("task_spec must expose attempt_id") from exc
        return cls(manifest, attempt_id)

    @property
    def task_id(self) -> TaskID:
        return self.manifest.task_id

    @property
    def output_ids(self) -> Tuple[ObjectID, ...]:
        return self.manifest.output_ids

    @property
    def num_returns(self) -> int:
        return self.manifest.num_returns

    def for_attempt(self, attempt_id: AttemptID) -> "TaskExecutionKey":
        return TaskExecutionKey(self.manifest, attempt_id)
