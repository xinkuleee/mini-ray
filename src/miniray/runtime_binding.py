"""Thread-local public API binding shared by Drivers and execution Workers."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .blocking import BlockingNotifier

from .ids import AttemptID, JobID, TaskID


@dataclass
class ExecutionContext:
    """Identity of one executing parent and its deterministic child namespace."""

    job_id: JobID
    parent_task_id: TaskID
    parent_attempt_id: AttemptID
    blocking_notifier: Optional["BlockingNotifier"] = None
    _submission_index: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, JobID):
            raise TypeError("job_id must be a JobID")
        if not isinstance(self.parent_task_id, TaskID):
            raise TypeError("parent_task_id must be a TaskID")
        if not isinstance(self.parent_attempt_id, AttemptID):
            raise TypeError("parent_attempt_id must be an AttemptID")
        if self.parent_attempt_id.task_id != self.parent_task_id:
            raise ValueError("parent_attempt_id must belong to parent_task_id")
        if self._submission_index < 0:
            raise ValueError("submission index must be non-negative")

    def next_task_id(self) -> TaskID:
        # A retried physical parent must not collide with children it may have
        # submitted before failing.  Derive an attempt-scoped parent seed while
        # TaskSpec still records the stable logical parent TaskID.
        attempt_parent = TaskID.derive(
            self.job_id,
            self.parent_task_id,
            self.parent_attempt_id.attempt_number,
        )
        task_id = TaskID.derive(
            self.job_id, attempt_parent, self._submission_index
        )
        self._submission_index += 1
        return task_id


@dataclass(frozen=True)
class RuntimeBinding:
    core_worker: object
    execution_context: Optional[ExecutionContext] = None


_local = threading.local()


def current_binding() -> Optional[RuntimeBinding]:
    return getattr(_local, "binding", None)


def current_core_worker() -> object | None:
    binding = current_binding()
    return None if binding is None else binding.core_worker


def current_execution_context() -> Optional[ExecutionContext]:
    binding = current_binding()
    return None if binding is None else binding.execution_context


@contextmanager
def bind_runtime(
    core_worker: object,
    execution_context: Optional[ExecutionContext] = None,
) -> Iterator[RuntimeBinding]:
    """Bind one CoreWorker to the current thread and restore its predecessor."""

    previous = current_binding()
    binding = RuntimeBinding(core_worker, execution_context)
    _local.binding = binding
    try:
        yield binding
    finally:
        if previous is None:
            try:
                del _local.binding
            except AttributeError:
                pass
        else:
            _local.binding = previous


__all__ = [
    "ExecutionContext",
    "RuntimeBinding",
    "bind_runtime",
    "current_binding",
    "current_core_worker",
    "current_execution_context",
]
