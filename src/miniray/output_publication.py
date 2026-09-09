"""Pure identities for one ordinary task's output publication.

An execution publishes its single output as one immutable manifest.
The manifest and Complete witness carry only identities and integrity metadata;
only the data-plane envelope contains INLINE payloads.  These values do not
implement publication effects or authorize a Complete.

Validation rebuilds nested protocol values instead of trusting ``frozen=True``:
deserialization or an effect-then-error test can supply a modified dataclass.
Digests use explicit length framing and the existing transfer fingerprint, not
Python repr, pickle, or generic object serialization.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields, replace
from typing import Tuple

from .contained_edges import (
    ContainedReferenceEdge, ContainedReferenceHold,
)
from .ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from .protocol import (
    ContainedTransferSource, ResultDescriptor, ResultStorage, TaskHoldSource,
    TaskReferenceHold, TaskReferenceHoldKind, TaskReplyStatus,
)
from .publication_sources import (
    BorrowedContainedSource, OwnedContainedSource, PreparedContainedTransfer,
    PublicationNodeIncarnation, prepared_contained_transfer_fingerprint,
)
from .task_outputs import TaskExecutionKey, TaskOutputManifest


ExecutionKey = TaskExecutionKey
# This publication spelling and the historical stored alias share one physical
# identity type without importing a journal or recovery authority.
OutputPublicationNodeIncarnation = PublicationNodeIncarnation

_HANDOFF_DOMAIN = b"miniray-output-publication-handoff-v1\0"
_MANIFEST_DOMAIN = b"miniray-output-publication-manifest-v2\0"


class OutputPublicationError(ValueError):
    """A publication value violates its immutable output contract."""


class OutputPublicationConflictError(OutputPublicationError):
    """A declared identity or digest disagrees with its complete manifest."""


def _require_type(value: object, expected: type, label: str) -> None:
    # Protocol values have concrete wire types; accepting an arbitrary subclass
    # can retain hidden payload attributes or override identity projections.
    if type(value) is not expected:
        raise TypeError(f"{label} must be a {expected.__name__}")


def _uint(value: object, label: str, *, positive: bool = False) -> bytes:
    lower = 1 if positive else 0
    if type(value) is not int or not lower <= value < (1 << 64):
        bound = "positive" if positive else "non-negative"
        raise ValueError(f"{label} must be a {bound} uint64")
    return value.to_bytes(8, "big")


def _string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise TypeError(f"{label} must be a non-empty string")
    return value


def _checksum(value: object, label: str) -> str:
    if (
        type(value) is not str or len(value) != 64
        or any(character not in "0123456789abcdefABCDEF" for character in value)
    ):
        raise ValueError(f"{label} must be a SHA-256 hex digest")
    return value.lower()


def _sequence(value: object, label: str) -> tuple:
    if type(value) not in (tuple, list):
        raise TypeError(f"{label} must be a tuple or list")
    return tuple(value)


def _opaque(value: object, kind: type, label: str):
    _require_type(value, kind, label)
    _require_type(value.value, bytes, f"{label}.value")
    return kind(value.value)


def _object_id(value: object) -> ObjectID:
    _require_type(value, ObjectID, "object_id")
    _uint(value.return_index, "return_index")
    return ObjectID(_opaque(value.task_id, TaskID, "object task_id"), value.return_index)


def _attempt(value: object) -> AttemptID:
    _require_type(value, AttemptID, "attempt_id")
    _uint(value.attempt_number, "attempt_number")
    return AttemptID(_opaque(value.task_id, TaskID, "attempt task_id"), value.attempt_number)


def _full_manifest(value: object) -> TaskOutputManifest:
    _require_type(value, TaskOutputManifest, "full output manifest")
    return TaskOutputManifest(
        _opaque(value.task_id, TaskID, "manifest task_id"),
        tuple(_object_id(item) for item in _sequence(value.output_ids, "output_ids")),
    )


def _execution(value: object) -> ExecutionKey:
    _require_type(value, TaskExecutionKey, "execution")
    return TaskExecutionKey(_full_manifest(value.manifest), _attempt(value.attempt_id))


def _node_incarnation(value: object) -> OutputPublicationNodeIncarnation:
    _require_type(value, OutputPublicationNodeIncarnation, "node_incarnation")
    _uint(value.node_pid, "node_pid", positive=True)
    _uint(value.registration_epoch, "registration_epoch", positive=True)
    return OutputPublicationNodeIncarnation(
        _opaque(value.node_id, NodeID, "publishing node_id"),
        value.node_pid, value.registration_epoch,
    )


def _hold(value: object) -> ContainedReferenceHold:
    _require_type(value, ContainedReferenceHold, "contained hold")
    return ContainedReferenceHold(
        _object_id(value.container_object_id),
        _opaque(value.container_owner_worker_id, WorkerID, "container owner"),
        _string(value.transfer_token, "transfer_token"),
    )


def _source(value: object):
    if type(value) is OwnedContainedSource:
        return OwnedContainedSource(_opaque(value.owner_worker_id, WorkerID, "source owner"))
    _require_type(value, BorrowedContainedSource, "contained source")
    original = value.original_source
    if type(original) is ContainedTransferSource:
        original = ContainedTransferSource(_hold(original.hold))
    elif type(original) is TaskHoldSource:
        hold = original.hold
        _require_type(hold, TaskReferenceHold, "task source hold")
        _require_type(hold.kind, TaskReferenceHoldKind, "task source hold kind")
        original = TaskHoldSource(TaskReferenceHold(
            hold.kind, _opaque(hold.submitting_worker_id, WorkerID, "task submitter"),
            _opaque(hold.task_id, TaskID, "source task_id"),
            _attempt(hold.origin_attempt_id),
        ))
    else:
        raise TypeError("borrowed original_source must be a ContainedTransferSource or TaskHoldSource")
    return BorrowedContainedSource(
        _opaque(value.borrower_worker_id, WorkerID, "source borrower"),
        _string(value.borrower_token, "borrower_token"), original,
    )


def _transfer(value: object) -> PreparedContainedTransfer:
    _require_type(value, PreparedContainedTransfer, "transfer")
    address = value.contained_owner_address
    if (
        type(address) is not tuple or len(address) != 2
        or type(address[0]) is not str or not address[0]
        or type(address[1]) is not int or not 1 <= address[1] <= 65535
    ):
        raise ValueError("contained owner address must be a bound (host, port) tuple")
    return PreparedContainedTransfer(
        _object_id(value.contained_object_id),
        _opaque(value.contained_owner_worker_id, WorkerID, "contained owner"),
        (address[0], address[1]), _source(value.source),
        _hold(value.provisional_hold), _hold(value.final_hold),
    )


def _descriptor(value: object) -> ResultDescriptor:
    _require_type(value, ResultDescriptor, "result")
    _require_type(value.storage, ResultStorage, "result storage")
    _uint(value.size_bytes, "result size_bytes")
    if value.inline_data is not None:
        _require_type(value.inline_data, bytes, "inline_data")
    # Re-enter the existing descriptor's payload-size/hash/tier validation.
    return ResultDescriptor(
        _object_id(value.object_id), value.storage, value.size_bytes,
        _opaque(value.owner_worker_id, WorkerID, "result owner"),
        _opaque(value.node_id, NodeID, "result node_id"),
        _checksum(value.checksum, "result checksum"), value.inline_data,
    )


def _framed(digest, value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _feed_object(digest, object_id: ObjectID) -> None:
    _framed(digest, bytes(object_id.task_id))
    _framed(digest, _uint(object_id.return_index, "return_index"))


def _feed_publication(digest, publication_id: "OutputPublicationID") -> None:
    _framed(digest, bytes(publication_id.lease_id))
    execution = publication_id.execution
    _framed(digest, bytes(execution.task_id))
    _framed(digest, _uint(execution.attempt_id.attempt_number, "attempt_number"))
    _feed_object(digest, publication_id.output_ids[0])


def _manifest_digest(
    header: "OutputPublicationHeader", slots: Tuple["OutputSlotManifest", ...],
) -> str:
    digest = hashlib.sha256(_MANIFEST_DOMAIN)
    _feed_publication(digest, header.publication_id)
    for value in (header.job_id, header.executor_worker_id, header.owner_worker_id):
        _framed(digest, bytes(value))
    node = header.node_incarnation
    _framed(digest, bytes(node.node_id))
    _framed(digest, _uint(node.node_pid, "node_pid", positive=True))
    _framed(digest, _uint(node.registration_epoch, "registration_epoch", positive=True))
    _framed(digest, _uint(len(slots), "slot count"))
    for slot in slots:
        _feed_object(digest, slot.object_id)
        _framed(digest, slot.tier.value.encode("utf-8"))
        _framed(digest, _uint(slot.size_bytes, "slot size_bytes"))
        _framed(digest, bytes.fromhex(slot.checksum))
        _framed(digest, _uint(len(slot.transfers), "transfer count"))
        for transfer in slot.transfers:
            _framed(digest, prepared_contained_transfer_fingerprint(transfer))
    return digest.hexdigest()


class _ValidatedWireValue:
    def __reduce__(self):
        # Unpickling must re-enter the same deep validation as local creation.
        return type(self), tuple(getattr(self, item.name) for item in fields(self))


@dataclass(frozen=True)
class OutputPublicationID(_ValidatedWireValue):
    """One lease execution authorized for the task's single output."""

    lease_id: LeaseID
    execution: ExecutionKey

    def __post_init__(self) -> None:
        object.__setattr__(self, "lease_id", _opaque(self.lease_id, LeaseID, "lease_id"))
        object.__setattr__(self, "execution", _execution(self.execution))

    @property
    def task_id(self) -> TaskID:
        return self.execution.task_id

    @property
    def attempt_id(self) -> AttemptID:
        return self.execution.attempt_id

    @property
    def full_output_ids(self) -> Tuple[ObjectID, ...]:
        return self.execution.output_ids

    @property
    def output_ids(self) -> Tuple[ObjectID, ...]:
        return self.execution.output_ids

    @property
    def transaction_id(self) -> str:
        """Stable namespace for exact handoff tokens, not commit authority."""

        validated = replace(self)
        digest = hashlib.sha256(_HANDOFF_DOMAIN)
        _feed_publication(digest, validated)
        return "output-publication-handoff:" + digest.hexdigest()


@dataclass(frozen=True)
class OutputPublicationHeader(_ValidatedWireValue):
    publication_id: OutputPublicationID
    job_id: JobID
    executor_worker_id: WorkerID
    owner_worker_id: WorkerID
    node_incarnation: OutputPublicationNodeIncarnation

    def __post_init__(self) -> None:
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        object.__setattr__(self, "publication_id", replace(self.publication_id))
        object.__setattr__(self, "job_id", _opaque(self.job_id, JobID, "job_id"))
        object.__setattr__(self, "executor_worker_id", _opaque(
            self.executor_worker_id, WorkerID, "executor_worker_id"
        ))
        object.__setattr__(self, "owner_worker_id", _opaque(
            self.owner_worker_id, WorkerID, "owner_worker_id"
        ))
        object.__setattr__(self, "node_incarnation", _node_incarnation(self.node_incarnation))


@dataclass(frozen=True)
class OutputSlotManifest(_ValidatedWireValue):
    """One return's integrity and ordered child custody, never its bytes."""

    object_id: ObjectID
    tier: ResultStorage
    size_bytes: int
    checksum: str
    transfers: Tuple[PreparedContainedTransfer, ...] = ()

    def __post_init__(self) -> None:
        object_id = _object_id(self.object_id)
        _require_type(self.tier, ResultStorage, "tier")
        _uint(self.size_bytes, "size_bytes")
        transfers = tuple(_transfer(item) for item in _sequence(self.transfers, "transfers"))
        if any(transfer.final_hold.container_object_id != object_id for transfer in transfers):
            raise OutputPublicationConflictError(
                "every transfer must belong to its output slot"
            )
        hold_keys = tuple((item.contained_object_id, item.final_hold) for item in transfers)
        if len(hold_keys) != len(set(hold_keys)):
            raise OutputPublicationConflictError(
                "one child hold cannot occupy multiple transfer slots"
            )
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "checksum", _checksum(self.checksum, "checksum"))
        object.__setattr__(self, "transfers", transfers)

    @property
    def edges(self) -> Tuple[ContainedReferenceEdge, ...]:
        return tuple(transfer.edge for transfer in self.transfers)


def _validate_manifest_inputs(header: object, slots: object):
    _require_type(header, OutputPublicationHeader, "header")
    header = replace(header)
    copied = []
    for slot in _sequence(slots, "slots"):
        _require_type(slot, OutputSlotManifest, "slot")
        copied.append(replace(slot))
    slots = tuple(copied)
    if tuple(slot.object_id for slot in slots) != header.publication_id.output_ids:
        raise OutputPublicationConflictError(
            "slots must exactly match the single execution output"
        )
    child_owners = {}
    for slot in slots:
        for transfer in slot.transfers:
            if (
                transfer.provisional_hold.container_owner_worker_id != header.executor_worker_id
                or transfer.final_hold.container_owner_worker_id != header.owner_worker_id
            ):
                raise OutputPublicationConflictError(
                    "transfer custody must name the executor and final output owner"
                )
            source = transfer.source
            source_executor = (
                source.owner_worker_id if type(source) is OwnedContainedSource
                else source.borrower_worker_id
            )
            if source_executor != header.executor_worker_id:
                raise OutputPublicationConflictError(
                    "contained source must belong to the executor"
                )
            # Shape validation is not permission to borrow.  The child owner
            # still must check original_source against its live borrower table
            # before installing either custody hold in the later runtime.
            previous = child_owners.setdefault(
                transfer.contained_object_id, transfer.contained_owner_worker_id
            )
            if previous != transfer.contained_owner_worker_id:
                raise OutputPublicationConflictError(
                    "one contained ObjectID cannot name conflicting owners"
                )
    return header, slots


@dataclass(frozen=True)
class OutputPublicationManifest(_ValidatedWireValue):
    """One metadata-only publication, including an output without references."""

    header: OutputPublicationHeader
    slots: Tuple[OutputSlotManifest, ...]
    manifest_digest: str

    def __post_init__(self) -> None:
        header, slots = _validate_manifest_inputs(self.header, self.slots)
        digest = _checksum(self.manifest_digest, "manifest_digest")
        if digest != _manifest_digest(header, slots):
            raise OutputPublicationConflictError(
                "manifest_digest does not match the complete output publication"
            )
        object.__setattr__(self, "header", header)
        object.__setattr__(self, "slots", slots)
        object.__setattr__(self, "manifest_digest", digest)

    @classmethod
    def create(
        cls, header: OutputPublicationHeader, slots: Tuple[OutputSlotManifest, ...],
    ) -> "OutputPublicationManifest":
        header, slots = _validate_manifest_inputs(header, slots)
        return cls(header, slots, _manifest_digest(header, slots))

    @property
    def publication_id(self) -> OutputPublicationID:
        return self.header.publication_id

    @property
    def execution(self) -> ExecutionKey:
        return self.publication_id.execution

    @property
    def ordered_edges(self) -> Tuple[ContainedReferenceEdge, ...]:
        return tuple(edge for slot in self.slots for edge in slot.edges)

@dataclass(frozen=True)
class OutputPublicationCompleteWitness(_ValidatedWireValue):
    """Byte-free identity of a successful Complete, not a liveness oracle."""

    publication_id: OutputPublicationID
    manifest_digest: str
    status: TaskReplyStatus = TaskReplyStatus.SUCCEEDED

    def __post_init__(self) -> None:
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        _require_type(self.status, TaskReplyStatus, "status")
        if self.status is not TaskReplyStatus.SUCCEEDED:
            raise OutputPublicationError("publication Complete must be SUCCEEDED")
        object.__setattr__(self, "publication_id", replace(self.publication_id))
        object.__setattr__(self, "manifest_digest", _checksum(
            self.manifest_digest, "manifest_digest"
        ))

    @classmethod
    def for_manifest(
        cls, manifest: OutputPublicationManifest,
    ) -> "OutputPublicationCompleteWitness":
        _require_type(manifest, OutputPublicationManifest, "manifest")
        validated = replace(manifest)
        return cls(validated.publication_id, validated.manifest_digest)


@dataclass(frozen=True)
class OutputPublicationEnvelope(_ValidatedWireValue):
    """Data-plane handoff: the result from one exact Complete."""

    manifest: OutputPublicationManifest
    complete: OutputPublicationCompleteWitness
    results: Tuple[ResultDescriptor, ...]

    def __post_init__(self) -> None:
        _require_type(self.manifest, OutputPublicationManifest, "manifest")
        _require_type(self.complete, OutputPublicationCompleteWitness, "complete")
        manifest = replace(self.manifest)
        complete = replace(self.complete)
        if (
            complete.publication_id != manifest.publication_id
            or complete.manifest_digest != manifest.manifest_digest
        ):
            raise OutputPublicationConflictError(
                "Complete witness must match the exact publication manifest"
            )
        results = tuple(_descriptor(item) for item in _sequence(self.results, "results"))
        if tuple(item.object_id for item in results) != manifest.publication_id.output_ids:
            raise OutputPublicationConflictError(
                "results must exactly match the single execution output"
            )
        for slot, descriptor in zip(manifest.slots, results):
            if (
                descriptor.storage is not slot.tier
                or descriptor.size_bytes != slot.size_bytes
                or descriptor.checksum != slot.checksum
                or descriptor.owner_worker_id != manifest.header.owner_worker_id
                or descriptor.node_id != manifest.header.node_incarnation.node_id
            ):
                raise OutputPublicationConflictError(
                    "result descriptor does not match its slot, owner, and publishing Node"
                )
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "complete", complete)
        object.__setattr__(self, "results", results)

    @property
    def publication_id(self) -> OutputPublicationID:
        return self.manifest.publication_id


__all__ = [
    "ExecutionKey", "OutputPublicationError", "OutputPublicationConflictError",
    "OutputPublicationNodeIncarnation", "OutputPublicationID",
    "OutputPublicationHeader", "OutputSlotManifest",
    "OutputPublicationManifest", "OutputPublicationCompleteWitness",
    "OutputPublicationEnvelope",
]
