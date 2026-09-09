"""Strong, immutable identities used by the mini-Ray protocol.

Logical and physical identities deliberately have different Python types:

* a ``TaskID`` survives retries while an ``AttemptID`` does not;
* an ``ObjectID`` names a logical return value, not a physical replica; and
* an ``ActorID`` survives restarts while ``ActorGeneration`` fences old messages.

Opaque IDs are 128-bit values.  Logical task and object IDs are derived with a
versioned BLAKE2 domain so that the same submission graph produces the same IDs.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from typing import Type, TypeVar

from .errors import InvalidIDError


_ID_BYTES = 16
_T = TypeVar("_T", bound="_OpaqueID")


def _require_non_negative(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidIDError(f"{name} must be a non-negative integer")


def _digest(domain: bytes, *parts: bytes) -> bytes:
    digest = hashlib.blake2b(digest_size=_ID_BYTES, person=b"miniray-id-v1")
    digest.update(len(domain).to_bytes(2, "big"))
    digest.update(domain)
    for part in parts:
        digest.update(len(part).to_bytes(4, "big"))
        digest.update(part)
    return digest.digest()


@dataclass(frozen=True, order=True, repr=False)
class _OpaqueID:
    value: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.value, bytes) or len(self.value) != _ID_BYTES:
            raise InvalidIDError(
                f"{type(self).__name__} must contain exactly {_ID_BYTES} bytes"
            )

    @classmethod
    def random(cls: Type[_T]) -> _T:
        return cls(os.urandom(_ID_BYTES))

    @classmethod
    def from_hex(cls: Type[_T], value: str) -> _T:
        if not isinstance(value, str):
            raise InvalidIDError("hex ID must be a string")
        try:
            raw = bytes.fromhex(value)
        except ValueError as exc:
            raise InvalidIDError(f"invalid hexadecimal {cls.__name__}") from exc
        return cls(raw)

    @property
    def hex(self) -> str:
        return self.value.hex()

    def __bytes__(self) -> bytes:
        return self.value

    def __str__(self) -> str:
        return self.hex

    def __repr__(self) -> str:
        return f"{type(self).__name__}('{self.hex}')"


@dataclass(frozen=True, order=True, repr=False)
class JobID(_OpaqueID):
    """A logical driver/job identity."""


@dataclass(frozen=True, order=True, repr=False)
class TaskID(_OpaqueID):
    """A logical task identity, stable across all attempts."""

    @classmethod
    def for_driver(cls, job_id: JobID) -> "TaskID":
        if not isinstance(job_id, JobID):
            raise InvalidIDError("job_id must be a JobID")
        return cls(_digest(b"driver-task", bytes(job_id)))

    @classmethod
    def for_put(
        cls, job_id: JobID, owner_worker_id: "WorkerID", put_index: int
    ) -> "TaskID":
        """Return the logical ID namespace used by owner-created put objects."""

        if not isinstance(job_id, JobID):
            raise InvalidIDError("job_id must be a JobID")
        if not isinstance(owner_worker_id, WorkerID):
            raise InvalidIDError("owner_worker_id must be a WorkerID")
        _require_non_negative(put_index, "put_index")
        return cls(
            _digest(
                b"put",
                bytes(job_id),
                bytes(owner_worker_id),
                put_index.to_bytes(8, "big"),
            )
        )

    @classmethod
    def derive(
        cls, job_id: JobID, parent_task_id: "TaskID", submission_index: int
    ) -> "TaskID":
        if not isinstance(job_id, JobID):
            raise InvalidIDError("job_id must be a JobID")
        if not isinstance(parent_task_id, TaskID):
            raise InvalidIDError("parent_task_id must be a TaskID")
        _require_non_negative(submission_index, "submission_index")
        return cls(
            _digest(
                b"task",
                bytes(job_id),
                bytes(parent_task_id),
                submission_index.to_bytes(8, "big"),
            )
        )


@dataclass(frozen=True, order=True)
class AttemptID:
    """One physical execution of a logical task."""

    task_id: TaskID
    attempt_number: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise InvalidIDError("task_id must be a TaskID")
        _require_non_negative(self.attempt_number, "attempt_number")

    def next(self) -> "AttemptID":
        return AttemptID(self.task_id, self.attempt_number + 1)

    def __str__(self) -> str:
        return f"{self.task_id.hex}:{self.attempt_number}"


TaskAttemptID = AttemptID


@dataclass(frozen=True, order=True)
class ObjectID:
    """A deterministic logical return slot of a task."""

    task_id: TaskID
    return_index: int

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise InvalidIDError("task_id must be a TaskID")
        _require_non_negative(self.return_index, "return_index")

    @classmethod
    def for_task(cls, task_id: TaskID, return_index: int = 0) -> "ObjectID":
        return cls(task_id, return_index)

    @property
    def hex(self) -> str:
        return f"{self.task_id.hex}{self.return_index:08x}"

    def __str__(self) -> str:
        return f"{self.task_id.hex}:{self.return_index}"


@dataclass(frozen=True, order=True, repr=False)
class ActorID(_OpaqueID):
    """A logical actor identity, stable across actor restarts."""

    @classmethod
    def derive(
        cls, job_id: JobID, parent_task_id: TaskID, submission_index: int
    ) -> "ActorID":
        if not isinstance(job_id, JobID):
            raise InvalidIDError("job_id must be a JobID")
        if not isinstance(parent_task_id, TaskID):
            raise InvalidIDError("parent_task_id must be a TaskID")
        _require_non_negative(submission_index, "submission_index")
        return cls(
            _digest(
                b"actor",
                bytes(job_id),
                bytes(parent_task_id),
                submission_index.to_bytes(8, "big"),
            )
        )


@dataclass(frozen=True, order=True)
class ActorGeneration:
    """A physical incarnation of a logical actor."""

    actor_id: ActorID
    generation: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.actor_id, ActorID):
            raise InvalidIDError("actor_id must be an ActorID")
        _require_non_negative(self.generation, "generation")

    def next(self) -> "ActorGeneration":
        return ActorGeneration(self.actor_id, self.generation + 1)

    def __str__(self) -> str:
        return f"{self.actor_id.hex}:{self.generation}"


@dataclass(frozen=True, order=True, repr=False)
class NodeID(_OpaqueID):
    """A node-manager identity."""


@dataclass(frozen=True, order=True, repr=False)
class WorkerID(_OpaqueID):
    """A worker-process identity."""


@dataclass(frozen=True, order=True, repr=False)
class LeaseID(_OpaqueID):
    """An idempotency key for a worker-lease lifecycle."""


@dataclass(frozen=True, order=True, repr=False)
class PlacementGroupID(_OpaqueID):
    """A placement-group identity."""


PGID = PlacementGroupID
