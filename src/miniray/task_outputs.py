"""Task-scoped identity for the bounded public multi-return slice.

The wire protocol already represents every return as an ``ObjectID`` whose
``return_index`` is stable across retries.  This module adds the one small
piece of shared policy needed by the public API and the owner table: an
ordinary task has a non-empty, bounded, ordered and contiguous output
manifest, and one physical attempt acts on that manifest as a unit.

It deliberately contains no scheduler, object-store, or RPC behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from .ids import AttemptID, ObjectID, TaskID


MAX_TASK_RETURNS = 16


def validate_num_returns(value: object) -> int:
    """Validate the intentionally small public ordinary-task bound."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("num_returns must be an integer")
    if not 1 <= value <= MAX_TASK_RETURNS:
        raise ValueError(
            "num_returns must be between 1 and {}".format(MAX_TASK_RETURNS)
        )
    return value


@dataclass(frozen=True)
class TaskOutputManifest:
    """The complete ordered logical output identity of one task."""

    task_id: TaskID
    output_ids: Tuple[ObjectID, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise TypeError("task output manifest task_id must be a TaskID")
        output_ids = tuple(self.output_ids)
        count = validate_num_returns(len(output_ids))
        expected = tuple(
            ObjectID.for_task(self.task_id, index) for index in range(count)
        )
        if output_ids != expected:
            raise ValueError(
                "task output manifest must be ordered, contiguous task returns"
            )
        object.__setattr__(self, "output_ids", output_ids)

    @classmethod
    def for_task(cls, task_id: TaskID, num_returns: int) -> "TaskOutputManifest":
        count = validate_num_returns(num_returns)
        return cls(
            task_id,
            tuple(ObjectID.for_task(task_id, index) for index in range(count)),
        )

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
    """One physical attempt authorized to publish a whole manifest."""

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


@dataclass(frozen=True)
class TargetOutputManifest:
    """A non-empty ordered subset of one complete task manifest.

    The complete manifest remains canonical producer lineage.  This value only
    identifies the slots one physical reconstruction is allowed to replace; it
    can therefore represent non-contiguous targets without renumbering returns.
    """

    full_manifest: TaskOutputManifest
    target_output_ids: Tuple[ObjectID, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.full_manifest, TaskOutputManifest):
            raise TypeError(
                "target output full_manifest must be a TaskOutputManifest"
            )
        targets = tuple(self.target_output_ids)
        if not targets:
            raise ValueError("target output manifest must be non-empty")
        if any(not isinstance(value, ObjectID) for value in targets):
            raise TypeError(
                "target output manifest must contain ObjectID values"
            )
        if len(targets) != len(set(targets)):
            raise ValueError("target output manifest must contain unique slots")
        selected = set(targets)
        canonical = tuple(
            value for value in self.full_manifest.output_ids
            if value in selected
        )
        if canonical != targets:
            raise ValueError(
                "target outputs must be an ordered subset of the full manifest"
            )
        object.__setattr__(self, "target_output_ids", targets)

    @classmethod
    def from_task_spec(
        cls, task_spec: object, target_output_ids: Tuple[ObjectID, ...]
    ) -> "TargetOutputManifest":
        return cls(
            TaskOutputManifest.from_task_spec(task_spec),
            tuple(target_output_ids),
        )

    @property
    def task_id(self) -> TaskID:
        return self.full_manifest.task_id

    @property
    def full_output_ids(self) -> Tuple[ObjectID, ...]:
        """The complete lineage manifest, including non-target siblings."""

        return self.full_manifest.output_ids

    @property
    def num_targets(self) -> int:
        return len(self.target_output_ids)


@dataclass(frozen=True)
class TargetExecutionKey:
    """One physical attempt authorized for only selected return slots."""

    manifest: TargetOutputManifest
    attempt_id: AttemptID

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, TargetOutputManifest):
            raise TypeError(
                "target execution manifest must be a TargetOutputManifest"
            )
        if not isinstance(self.attempt_id, AttemptID):
            raise TypeError("target execution attempt_id must be an AttemptID")
        if self.attempt_id.task_id != self.manifest.task_id:
            raise ValueError(
                "target execution attempt must belong to manifest task_id"
            )

    @classmethod
    def from_task_spec(
        cls,
        task_spec: object,
        target_output_ids: Tuple[ObjectID, ...],
        *,
        attempt_id: AttemptID | None = None,
    ) -> "TargetExecutionKey":
        manifest = TargetOutputManifest.from_task_spec(
            task_spec, tuple(target_output_ids)
        )
        if attempt_id is None:
            try:
                attempt_id = task_spec.attempt_id
            except AttributeError as exc:
                raise TypeError(
                    "task_spec must expose attempt_id"
                ) from exc
        return cls(manifest, attempt_id)

    @property
    def task_id(self) -> TaskID:
        return self.manifest.task_id

    @property
    def full_output_ids(self) -> Tuple[ObjectID, ...]:
        return self.manifest.full_output_ids

    @property
    def target_output_ids(self) -> Tuple[ObjectID, ...]:
        return self.manifest.target_output_ids

    @property
    def num_targets(self) -> int:
        return self.manifest.num_targets

    def for_attempt(self, attempt_id: AttemptID) -> "TargetExecutionKey":
        return TargetExecutionKey(self.manifest, attempt_id)
