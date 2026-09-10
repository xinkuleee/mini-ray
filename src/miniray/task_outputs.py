"""Canonical execution identity for one ordinary Task output."""
from __future__ import annotations
from dataclasses import dataclass
from .ids import AttemptID, ObjectID, TaskID

MAX_TASK_RETURNS = 1

def validate_num_returns(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("num_returns must be an integer")
    if value != 1:
        raise ValueError("num_returns must be exactly 1")
    return value

@dataclass(frozen=True)
class TaskExecution:
    """One physical attempt; TaskID/ObjectID are derived, never duplicated."""
    attempt_id: AttemptID

    def __post_init__(self) -> None:
        if type(self.attempt_id) is not AttemptID:
            raise TypeError("execution attempt_id must be an AttemptID")
        attempt = self.attempt_id
        if type(attempt.task_id) is not TaskID or type(attempt.task_id.value) is not bytes:
            raise TypeError("execution attempt task_id must be a TaskID")
        if type(attempt.attempt_number) is not int or not 0 <= attempt.attempt_number < 1 << 64:
            raise ValueError("attempt_number must be a non-negative uint64")
        object.__setattr__(self, "attempt_id", AttemptID(TaskID(attempt.task_id.value), attempt.attempt_number))

    @property
    def task_id(self) -> TaskID:
        return self.attempt_id.task_id

    @property
    def object_id(self) -> ObjectID:
        return ObjectID.for_task(self.task_id, 0)

    @classmethod
    def from_task_spec(cls, task_spec: object) -> TaskExecution:
        try:
            task_id, attempt_id, return_ids = task_spec.task_id, task_spec.attempt_id, task_spec.return_ids
        except AttributeError as exc:
            raise TypeError("task_spec must expose task_id,attempt_id and return_ids()") from exc
        if not isinstance(task_id, TaskID):
            raise TypeError("task_spec.task_id must be a TaskID")
        if not callable(return_ids):
            raise TypeError("task_spec.return_ids must be callable")
        execution = cls(attempt_id)
        if task_id != execution.task_id or tuple(return_ids()) != (execution.object_id,):
            raise ValueError("task_spec must contain only canonical return index zero")
        return execution

    def for_attempt(self, attempt_id: AttemptID) -> TaskExecution:
        execution = TaskExecution(attempt_id)
        if execution.task_id != self.task_id:
            raise ValueError("execution retry cannot change TaskID")
        return execution

    def __reduce__(self):
        return type(self), (self.attempt_id,)
