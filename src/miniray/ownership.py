"""Owner-side metadata and reference accounting for logical objects.

An :class:`ObjectStore` owns bytes for one physical replica.  In contrast, an
``ObjectOwnerTable`` owns the logical object's state: which replicas exist,
which execution attempt may publish them, why the object is still live, and
which task can reconstruct it.

Object, task, and attempt identifiers are intentionally treated as hashable
opaque values here.  The concrete dataclasses live in ``miniray.ids`` and
``miniray.protocol``; this module does not inspect their representation.
"""

from __future__ import annotations

import hashlib
import uuid

from copy import deepcopy
from dataclasses import dataclass, field, fields, is_dataclass, replace
from enum import Enum
from threading import RLock
from typing import Hashable, Mapping

from .contained_edges import (
    ContainedReferenceEdge,
    ContainedReferenceHold,
    IncomingContainedReferenceHold,
    LineageReferenceEdge,
    ObjectMetadataCollection,
    ObjectMetadataCollectionPlan,
)
from .ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from .errors import ProtocolError
from .output_publication import (
    OutputPublicationEnvelope, OutputPublicationID, OutputPublicationManifest,
    _attempt, _checksum, _descriptor, _execution, _hold, _object_id, _opaque,
    _sequence, _string, _uint,
)
from .protocol import (
    BorrowSource,
    ContainedTransferSource,
    DropObjectReplica,
    DropObjectReplicaReply,
    DropObjectReplicaStatus,
    NodeDeathReason,
    NodeDeathRecord,
    ObjectStoreDescriptor,
    ReleaseContainedReference,
    ReleaseContainedReferenceReply,
    ResultDescriptor,
    ResultStorage,
    TaskHoldSource,
    TaskReferenceHold,
    TaskReferenceHoldKind,
    TaskSpec,
    WorkerDeathReason,
    WorkerDeathRecord,
)
from .task_outputs import TaskExecution

ReferenceToken = Hashable
ObjectLocation = NodeID


class OwnershipError(RuntimeError):
    """Base class for owner-table failures."""


class ObjectAlreadyRegisteredError(OwnershipError):
    """Raised when an ObjectID is registered with conflicting metadata."""


class UnknownObjectError(OwnershipError):
    """Raised when the owner does not know an ObjectID."""


class InvalidObjectTransitionError(OwnershipError):
    """Raised when a current attempt tries an illegal state transition."""


class ConflictingObjectResultError(OwnershipError):
    """Raised when one attempt publishes two different logical results."""


class UnknownTransferTokenError(OwnershipError):
    """Raised when a borrower cannot prove that the owner exported a ref."""


class ReleasedBorrowerTokenError(OwnershipError):
    """Raised when a delayed acquire would resurrect a released borrower."""


class ConflictingBorrowerTokenError(OwnershipError):
    """Raised when one borrower token is replayed with another export pin."""


class InactiveTaskReferenceHoldError(OwnershipError):
    """Raised when a nested borrower names no active Task lifetime hold."""


class ReleasedTaskReferenceHoldError(OwnershipError):
    """Raised when a delayed acquire names a terminal Task lifetime hold."""


class InactiveBorrowerTokenError(OwnershipError):
    """Raised when a task hold is not derived from a live borrower."""


class ReleasedRetainedTokenError(OwnershipError):
    """Raised when a delayed retain would resurrect a released task hold."""


class ConflictingRetainedTokenError(OwnershipError):
    """Raised when one task-hold token is rebound to another borrower."""


class RetainedHoldReplacementError(OwnershipError):
    """Base class for owner-side retained-hold replacement failures."""


class RetainedHoldReplacementConflictError(RetainedHoldReplacementError):
    """Raised when one old hold is replayed with a different successor."""


class RetainedHoldReplacementBusyError(RetainedHoldReplacementError):
    """Raised while attempt borrowers still derive from the old hold."""


class RetainedHoldReplacementDisposition(str, Enum):
    """Pure owner-table result, mapped directly to the wire disposition."""

    REPLACED = "REPLACED"
    ALREADY_REPLACED = "ALREADY_REPLACED"


class StoredContainedReferenceDisposition(str, Enum):
    """Result of one owner-side prepared contained-pin transition."""

    PREPARED = "PREPARED"
    ALREADY_PREPARED = "ALREADY_PREPARED"
    PROMOTED = "PROMOTED"
    ALREADY_PROMOTED = "ALREADY_PROMOTED"


class ObjectCollectionInProgressError(OwnershipError):
    """Raised when a new reference would resurrect claimed metadata."""


class DeadWorkerReferenceError(OwnershipError):
    """Raised when a dead Worker incarnation tries to add a reference."""


class DeadWorkerReferenceConflictError(OwnershipError):
    """Raised when one Worker death tombstone is rebound to another proof."""


class OutputOwnerPublicationConflictError(OwnershipError):
    """An output publication disagrees with its immutable identity."""


class OutputOwnerPublicationCollectionRequiredError(OwnershipError):
    """An output slot still belongs to a publication requiring exact cleanup."""


class OutputOwnerRetirementConflictError(OutputOwnerPublicationConflictError):
    """A LOST-output retirement changed its frozen identity."""


class OutputOwnerRetirementInProgressError(OwnershipError):
    """Old publication effects still own a LOST output's metadata."""


class ReferenceKind(str, Enum):
    """Reasons an owner must keep an object and its lineage alive."""

    LOCAL = "local"
    SUBMITTED = "submitted"
    BORROWED = "borrowed"
    RETAINED = "retained"
    CONTAINED = "contained"
    LINEAGE = "lineage"


class ObjectState(str, Enum):
    """Logical owner-visible state, independent of replica internals."""

    PENDING = "PENDING"
    READY_INLINE = "READY_INLINE"
    READY_STORED = "READY_STORED"
    ERROR = "ERROR"
    LOST = "LOST"


class ObjectCollectionState(str, Enum):
    """Owner-side lifetime phase, separate from result readiness."""

    ACTIVE = "ACTIVE"
    COLLECTING = "COLLECTING"
    COLLECTED = "COLLECTED"


class OutputOwnerPublicationDisposition(str, Enum):
    APPLIED = "APPLIED"
    ALREADY_APPLIED = "ALREADY_APPLIED"
    FENCED = "FENCED"


@dataclass(frozen=True)
class _StoredCollectionIdentity:
    """Byte-free owner evidence for late replicas after generic collection."""

    object_id: ObjectID
    producer_attempt_id: AttemptID
    owner_worker_id: WorkerID
    size_bytes: int
    checksum: str
    collection_id: str


@dataclass(frozen=True, order=True)
class TaskLineageObligation:
    """One TaskID-owned obligation to release a dependency hold."""

    task_id: TaskID
    dependency_object_id: ObjectID
    token: str

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(self.dependency_object_id, ObjectID):
            raise TypeError("dependency_object_id must be an ObjectID")
        if not isinstance(self.token, str) or not self.token:
            raise ValueError("token must be a non-empty string")


@dataclass(frozen=True)
class ObjectOwnerSnapshot:
    """Immutable diagnostic snapshot of one owner entry."""

    object_id: ObjectID
    state: ObjectState
    current_attempt: AttemptID | None
    inline_data: bytes | None
    error: object | None
    locations: frozenset[ObjectLocation]
    canonical_stored_result: ResultDescriptor | None
    producer_task_spec: TaskSpec | None
    local_tokens: frozenset[ReferenceToken]
    # The historical ``*_tokens`` names remain useful to snapshot consumers,
    # but Task lifetime credentials are deliberately stored whole.  A token
    # string alone cannot distinguish two submitters (or the hold kind).
    submitted_tokens: frozenset[TaskReferenceHold]
    borrowed_tokens: frozenset[ReferenceToken]
    released_borrowed_tokens: frozenset[ReferenceToken]
    borrowed_transfer_tokens: frozenset[
        tuple[ReferenceToken, ReferenceToken]
    ]
    borrowed_sources: frozenset[tuple[ReferenceToken, BorrowSource]]
    retained_tokens: frozenset[TaskReferenceHold]
    released_retained_tokens: frozenset[TaskReferenceHold]
    retained_borrower_tokens: frozenset[
        tuple[TaskReferenceHold, ReferenceToken]
    ]
    # Full identities are authoritative.  ``contained_tokens`` is retained as
    # a diagnostic compatibility projection for the original token-only API.
    contained_holds: frozenset[IncomingContainedReferenceHold]
    contained_tokens: frozenset[ReferenceToken]
    outgoing_contained_edges: frozenset[ContainedReferenceEdge]
    lineage_tokens: frozenset[ReferenceToken]
    released_lineage_tokens: frozenset[ReferenceToken]
    outgoing_lineage_edges: frozenset[LineageReferenceEdge]
    collection_pending: bool
    collection_plan: ObjectMetadataCollectionPlan | None
    output_publication: OutputOwnerPublicationMembership | None = None
    output_retirement_id: str | None = None

    @property
    def is_live(self) -> bool:
        return any(
            (
                self.local_tokens,
                self.submitted_tokens,
                self.borrowed_tokens,
                self.retained_tokens,
                self.contained_holds,
                self.lineage_tokens,
            )
        )

    @property
    def is_reconstructible(self) -> bool:
        return self.producer_task_spec is not None

    @property
    def is_ready(self) -> bool:
        """Whether wait/get may observe a completed logical result."""

        return self.state in (
            ObjectState.READY_INLINE,
            ObjectState.READY_STORED,
            ObjectState.ERROR,
        )


@dataclass(frozen=True)
class NodeLocationRemoval:
    """Atomic owner-table result of forgetting one dead physical node.

    ``lost`` contains objects whose final advertised replica disappeared.
    ``surviving`` still have at least one usable location.  ``collecting`` is
    deliberately unchanged: its immutable collection plan is owned by the Core
    GC obligation and the committed node-death proof discharges that work.
    """

    node_id: NodeID
    lost: tuple[ObjectID, ...] = ()
    surviving: tuple[ObjectID, ...] = ()
    collecting: tuple[ObjectID, ...] = ()


@dataclass(frozen=True)
class TaskOutputRegistrationPlan:
    """Validated input for atomically registering one task manifest."""

    execution: TaskExecution
    producer_task_spec: TaskSpec
    local_tokens: tuple[ReferenceToken | None, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.execution, TaskExecution):
            raise TypeError("execution must be a TaskExecution")
        if not isinstance(self.producer_task_spec, TaskSpec):
            raise TypeError("producer_task_spec must be a TaskSpec")
        if TaskExecution.from_task_spec(self.producer_task_spec) != self.execution:
            raise ValueError(
                "producer task spec must exactly match the execution manifest"
            )
        tokens = tuple(self.local_tokens)
        if len(tokens) != 1:
            raise ValueError(
                "local tokens must align with the complete output manifest"
            )
        for token in tokens:
            _require_optional_hashable(token, "local token")
        object.__setattr__(self, "local_tokens", tokens)

    @classmethod
    def for_task_spec(
        cls,
        task_spec: TaskSpec,
        *,
        local_tokens: tuple[ReferenceToken | None, ...] | None = None,
    ) -> "TaskOutputRegistrationPlan":
        execution = TaskExecution.from_task_spec(task_spec)
        tokens = (None,) if local_tokens is None else tuple(
            local_tokens
        )
        return cls(execution, task_spec, tokens)


@dataclass(frozen=True)
class TaskOutputPublicationPlan:
    """One complete, ordered Worker result manifest for owner publication."""

    execution: TaskExecution
    results: tuple[ResultDescriptor, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.execution, TaskExecution):
            raise TypeError("execution must be a TaskExecution")
        results = tuple(self.results)
        if any(not isinstance(result, ResultDescriptor) for result in results):
            raise TypeError("results must contain ResultDescriptor values")
        if tuple(result.object_id for result in results) != (self.execution.object_id,):
            raise ValueError(
                "result descriptors must exactly match the ordered output manifest"
            )
        if len({result.owner_worker_id for result in results}) != 1:
            raise ValueError("all task outputs must have the same logical owner")
        object.__setattr__(self, "results", results)


@dataclass(frozen=True)
class OutputOwnerPublicationPlan:
    """Transient output CAS input; the owner never retains its whole envelope.

    The explicit execution is a checked compatibility spelling, not a second
    authority. Final child pins must already be acknowledged by the caller;
    the typed envelope proves identity, not that those remote effects happened.
    """

    execution: TaskExecution
    envelope: OutputPublicationEnvelope

    def __post_init__(self) -> None:
        if type(self.envelope) is not OutputPublicationEnvelope:
            raise TypeError("envelope must be an OutputPublicationEnvelope")
        envelope = replace(self.envelope)
        execution = OutputPublicationID(
            envelope.publication_id.lease_id, self.execution
        ).execution
        if execution != envelope.manifest.execution:
            raise OutputOwnerPublicationConflictError(
                "owner execution must equal the envelope execution"
            )
        object.__setattr__(self, "execution", execution)
        object.__setattr__(self, "envelope", envelope)

    @property
    def publication_id(self) -> OutputPublicationID:
        return self.envelope.publication_id

    @property
    def output_ids(self) -> tuple[ObjectID, ...]:
        return (self.publication_id.object_id,)


@dataclass(frozen=True)
class OutputOwnerPublicationMembership:
    """Byte-free membership in one validated single-output manifest."""

    manifest: OutputPublicationManifest
    slot_index: int

    def __post_init__(self) -> None:
        if type(self.manifest) is not OutputPublicationManifest:
            raise TypeError("manifest must be an OutputPublicationManifest")
        if type(self.slot_index) is not int or self.slot_index != 0:
            raise ValueError("slot_index must select the only publication output")

    @property
    def object_id(self) -> ObjectID:
        return self.manifest.publication_id.object_id

    @property
    def publication_id(self) -> OutputPublicationID:
        return self.manifest.publication_id


@dataclass(frozen=True)
class OutputOwnerPublicationReceipt:
    """Transient exact commit result; historical records store metadata only."""

    plan: OutputOwnerPublicationPlan
    disposition: OutputOwnerPublicationDisposition

    def __post_init__(self) -> None:
        if type(self.plan) is not OutputOwnerPublicationPlan:
            raise TypeError("receipt plan must be an OutputOwnerPublicationPlan")
        if type(self.disposition) is not OutputOwnerPublicationDisposition:
            raise TypeError("output publication disposition is invalid")

    @property
    def committed(self) -> bool:
        return self.disposition in (
            OutputOwnerPublicationDisposition.APPLIED,
            OutputOwnerPublicationDisposition.ALREADY_APPLIED,
        )


@dataclass(frozen=True)
class OutputOwnerPublicationCollectionPlan:
    """One slot's existing metadata claim and exact batch membership."""

    membership: OutputOwnerPublicationMembership
    metadata_plan: ObjectMetadataCollectionPlan

    def __post_init__(self) -> None:
        if type(self.membership) is not OutputOwnerPublicationMembership:
            raise TypeError("membership must be an OutputOwnerPublicationMembership")
        membership = OutputOwnerPublicationMembership(
            replace(self.membership.manifest), self.membership.slot_index
        )
        if type(self.metadata_plan) is not ObjectMetadataCollectionPlan:
            raise TypeError("metadata_plan must be an ObjectMetadataCollectionPlan")
        metadata = deepcopy(replace(self.metadata_plan))
        slot = membership.manifest.value
        spec = metadata.producer_task_spec
        if (
            metadata.object_id != membership.object_id
            or metadata.producer_attempt_id != membership.publication_id.attempt_id
            or tuple(metadata.contained_releases) != tuple(sorted(slot.edges))
            or not isinstance(spec, TaskSpec)
            or tuple(spec.return_ids()) != (membership.publication_id.object_id,)
            or spec.owner_worker_id != membership.manifest.header.owner_worker_id
            or spec.job_id != membership.manifest.header.job_id
        ):
            raise OutputOwnerPublicationConflictError(
                "collection metadata must match the exact output slot and lineage"
            )
        if slot.tier is ResultStorage.INLINE:
            if (metadata.locations or metadata.canonical_size_bytes is not None
                    or metadata.canonical_checksum is not None):
                raise OutputOwnerPublicationConflictError(
                    "inline output collection cannot carry replica metadata"
                )
        elif (metadata.canonical_size_bytes != slot.size_bytes
              or metadata.canonical_checksum != slot.checksum):
            raise OutputOwnerPublicationConflictError(
                "stored output collection changed canonical integrity metadata"
            )
        object.__setattr__(self, "membership", membership)
        object.__setattr__(self, "metadata_plan", metadata)

    @property
    def object_id(self) -> ObjectID:
        return self.membership.object_id

    @property
    def collection_id(self) -> str:
        return self.metadata_plan.collection_id


@dataclass(frozen=True)
class OutputOwnerPublicationCollectionReceipt:
    plan: OutputOwnerPublicationCollectionPlan
    collection: ObjectMetadataCollection
    disposition: OutputOwnerPublicationDisposition

    def __post_init__(self) -> None:
        if type(self.plan) is not OutputOwnerPublicationCollectionPlan:
            raise TypeError("plan must be an OutputOwnerPublicationCollectionPlan")
        if type(self.collection) is not ObjectMetadataCollection:
            raise TypeError("collection must be an ObjectMetadataCollection")
        if type(self.disposition) is not OutputOwnerPublicationDisposition or self.disposition not in (
            OutputOwnerPublicationDisposition.APPLIED,
            OutputOwnerPublicationDisposition.ALREADY_APPLIED,
        ):
            raise ValueError("collection receipt requires a committed disposition")
        if (not self.collection.collected
                or self.collection.object_id != self.plan.object_id
                or self.collection.contained_releases != tuple(sorted(self.plan.membership.manifest.value.edges))):
            raise OutputOwnerPublicationConflictError(
                "collection receipt must release this output slot only"
            )


@dataclass(frozen=True)
class _OutputOwnerCollectionTombstone:
    """No result bytes, descriptors, or TaskSpec survive slot collection."""

    publication_id: OutputPublicationID
    manifest_digest: str
    slot_index: int
    collection_id: str
    metadata_digest: str
    collection: ObjectMetadataCollection

    def matches(self, plan: OutputOwnerPublicationCollectionPlan) -> bool:
        return (
            self.publication_id == plan.membership.publication_id
            and self.manifest_digest == plan.membership.manifest.manifest_digest
            and self.slot_index == plan.membership.slot_index
            and self.collection_id == plan.collection_id
            and self.metadata_digest == _collection_metadata_digest(plan.metadata_plan)
        )


@dataclass(frozen=True)
class OutputOwnerPublicationRetirementPlan:
    """Metadata-only cleanup claim; incoming references and lineage survive."""

    retirement_id: str
    memberships: tuple[OutputOwnerPublicationMembership, ...]
    replica_drops: tuple[DropObjectReplica, ...]

    def __post_init__(self) -> None:
        _string(self.retirement_id, "retirement_id")
        memberships = []
        for value in _sequence(self.memberships, "memberships"):
            if type(value) is not OutputOwnerPublicationMembership:
                raise TypeError("retirement requires typed output memberships")
            memberships.append(OutputOwnerPublicationMembership(
                replace(value.manifest), value.slot_index
            ))
        memberships = tuple(memberships)
        if len(memberships) != 1:
            raise ValueError("retirement requires exactly one output membership")
        drops = []
        by_object = {value.object_id: value for value in memberships}
        for value in _sequence(self.replica_drops, "replica_drops"):
            if type(value) is not DropObjectReplica:
                raise TypeError("retirement replica obligations must be DropObjectReplica")
            value = DropObjectReplica(
                _object_id(value.object_id), _attempt(value.producer_attempt_id),
                _opaque(value.owner_worker_id, WorkerID, "replica owner"),
                _opaque(value.node_id, NodeID, "replica node_id"),
                _checksum(value.checksum, "replica checksum"),
            )
            member = by_object.get(value.object_id)
            if member is None or member.manifest.value.tier is not ResultStorage.OBJECT_STORE:
                raise OutputOwnerRetirementConflictError("replica obligation requires the stored output membership")
            node_id = _opaque(value.node_id, NodeID, "replica node_id")
            expected = DropObjectReplica(
                member.object_id, member.publication_id.attempt_id,
                member.manifest.header.owner_worker_id, node_id, member.manifest.value.checksum,
            )
            if replace(value) != expected:
                raise OutputOwnerRetirementConflictError("replica obligation changed the old output identity")
            drops.append(expected)
        drops = tuple(drops)
        keys = tuple((value.object_id, value.node_id) for value in drops)
        if keys != tuple(sorted(set(keys))):
            raise OutputOwnerRetirementConflictError("replica obligations must be unique and ordered")
        if any(
            member.manifest.value.tier is ResultStorage.OBJECT_STORE
            and (member.object_id, member.manifest.header.node_incarnation.node_id) not in keys
            for member in memberships
        ):
            raise OutputOwnerRetirementConflictError("stored retirement must include its publishing Node")
        object.__setattr__(self, "memberships", memberships)
        object.__setattr__(self, "replica_drops", drops)

    @property
    def output_ids(self) -> tuple[ObjectID, ...]:
        return tuple(value.object_id for value in self.memberships)

    @property
    def contained_releases(self) -> tuple[ReleaseContainedReference, ...]:
        return tuple(
            ReleaseContainedReference(
                transfer.contained_object_id, transfer.contained_owner_worker_id,
                transfer.final_hold,
            )
            for member in self.memberships for transfer in member.manifest.value.transfers
        )


@dataclass(frozen=True)
class OutputOwnerPublicationRetirementReceipt:
    """Exact terminal cleanup ACK, never proof that remote effects ran here."""

    plan: OutputOwnerPublicationRetirementPlan
    released_edges: tuple[ReleaseContainedReferenceReply | WorkerDeathRecord, ...]
    dropped_replicas: tuple[DropObjectReplicaReply | NodeDeathRecord, ...]
    disposition: OutputOwnerPublicationDisposition

    def __post_init__(self) -> None:
        if type(self.plan) is not OutputOwnerPublicationRetirementPlan:
            raise TypeError("plan must be an OutputOwnerPublicationRetirementPlan")
        if type(self.disposition) is not OutputOwnerPublicationDisposition or self.disposition not in (
            OutputOwnerPublicationDisposition.APPLIED,
            OutputOwnerPublicationDisposition.ALREADY_APPLIED,
        ):
            raise ValueError("retirement receipt requires a committed disposition")
        plan = replace(self.plan)
        proofs = _validate_output_retirement_proofs(
            plan, self.released_edges, self.dropped_replicas,
        )
        object.__setattr__(self, "plan", plan)
        for name, value in zip(
            ("released_edges", "dropped_replicas"), proofs
        ):
            object.__setattr__(self, name, value)


@dataclass(frozen=True)
class TaskOutputAttemptAdvancePlan:
    """CAS one whole task manifest from one attempt to the next."""

    expected: TaskExecution
    next_execution: TaskExecution

    def __post_init__(self) -> None:
        if not isinstance(self.expected, TaskExecution) or not isinstance(
            self.next_execution, TaskExecution
        ):
            raise TypeError("attempt advance requires TaskExecution values")
        if self.expected.object_id != self.next_execution.object_id:
            raise ValueError("attempt advance cannot change the output manifest")
        if (
            self.next_execution.attempt_id.attempt_number
            <= self.expected.attempt_id.attempt_number
        ):
            raise InvalidObjectTransitionError(
                "next task attempt number must increase"
            )


@dataclass(frozen=True)
class TaskOutputErrorPlan:
    """A terminal error applied to every output of one physical attempt."""

    execution: TaskExecution
    error: object

    def __post_init__(self) -> None:
        if not isinstance(self.execution, TaskExecution):
            raise TypeError("execution must be a TaskExecution")


@dataclass(frozen=True)
class DeadWorkerReferenceRecord:
    """Minimal immutable fence for one dead physical Worker incarnation.

    ``WorkerID`` already names a physical incarnation.  ``death_id`` binds the
    reducer result to the authority proof consumed by its caller without
    importing control-plane policy into the owner table.
    """

    worker_id: WorkerID
    death_id: str

    def __post_init__(self) -> None:
        _require_worker_id(self.worker_id)
        _require_death_id(self.death_id)


@dataclass(frozen=True)
class DeadWorkerReferenceCleanup:
    """Immutable result of installing one Worker death reference fence.

    The object IDs are only GC *candidates*.  Installing a death fact never
    removes owner metadata, lineage, locations, or physical object bytes.
    """

    record: DeadWorkerReferenceRecord
    affected_object_ids: tuple[ObjectID, ...] = ()
    collectable_object_ids: tuple[ObjectID, ...] = ()
    released_borrower_tokens: frozenset[
        tuple[ObjectID, ReferenceToken]
    ] = frozenset()
    released_submitted_holds: frozenset[
        tuple[ObjectID, TaskReferenceHold]
    ] = frozenset()
    released_retained_holds: frozenset[
        tuple[ObjectID, TaskReferenceHold]
    ] = frozenset()
    released_contained_holds: frozenset[
        tuple[ObjectID, ContainedReferenceHold]
    ] = frozenset()

    @property
    def worker_id(self) -> WorkerID:
        return self.record.worker_id

    @property
    def death_id(self) -> str:
        return self.record.death_id


@dataclass
class _ObjectOwnerEntry:
    object_id: ObjectID
    current_attempt: AttemptID | None
    producer_task_spec: TaskSpec | None
    state: ObjectState = ObjectState.PENDING
    inline_data: bytes | None = None
    error: object | None = None
    # The producing attempt is stored with every location.  This prevents a
    # delayed removal from attempt N from deleting a same-node replica created
    # by attempt N+1.
    location_attempts: dict[ObjectLocation, AttemptID | None] = field(
        default_factory=dict
    )
    # The first successful task publication freezes the logical identity and
    # integrity metadata of a stored result.  Locations may later grow, but an
    # exact TaskReply replay may not silently change its original node, size,
    # checksum, owner, or ObjectID.  Attempt identity remains in
    # ``current_attempt`` and advances only after old effects are retired.
    canonical_stored_result: ResultDescriptor | None = None
    # Put has no TaskSpec or publication membership. Freeze its metadata-only
    # identity so equal inline bytes cannot replay with another owner or Node.
    put_identity: tuple | None = None
    # Publication metadata is byte-free; payloads remain in inline_data or
    # the object store described by canonical_stored_result.
    output_publication: OutputOwnerPublicationMembership | None = None
    # Separate from collection_pending: reference releases remain legal while
    # the owner retires child pins and replicas from an old publication.
    output_retirement_id: str | None = None
    local_tokens: set[ReferenceToken] = field(default_factory=set)
    submitted_tokens: set[TaskReferenceHold] = field(default_factory=set)
    borrowed_tokens: set[ReferenceToken] = field(default_factory=set)
    # Releases are tombstoned because request/reply loss can reorder a replayed
    # acquire behind its release.  Forgetting this set would resurrect a dead
    # borrower and make eventual physical collection unsafe.
    released_borrowed_tokens: set[ReferenceToken] = field(default_factory=set)
    # Keep the credential binding after release as part of the tombstone.
    # A duplicated Acquire must replay both tokens exactly; it cannot reuse a
    # once-valid borrower token with another exported edge.
    borrowed_sources: dict[ReferenceToken, BorrowSource] = field(
        default_factory=dict
    )
    # A task dependency hold is derived from an active borrower exactly once,
    # then lives independently so the submitting Python handle may close.  Its
    # complete typed credential is the key: projecting to one field would make
    # distinct submitters, tasks, kinds, or reconstruction origins alias.
    retained_tokens: set[TaskReferenceHold] = field(default_factory=set)
    released_retained_tokens: set[TaskReferenceHold] = field(default_factory=set)
    retained_borrower_tokens: dict[TaskReferenceHold, ReferenceToken] = field(
        default_factory=dict
    )
    # The complete container identity binds each hold to the Worker
    # incarnation responsible for releasing it.
    contained_holds: set[IncomingContainedReferenceHold] = field(
        default_factory=set
    )
    lineage_tokens: set[ReferenceToken] = field(default_factory=set)
    released_lineage_tokens: set[ReferenceToken] = field(default_factory=set)
    # These are obligations owned by this *container's* owner.  They are
    # intentionally separate from contained_holds, which are incoming pins
    # owned by the contained object's owner.
    outgoing_contained_edges: set[ContainedReferenceEdge] = field(
        default_factory=set
    )
    outgoing_lineage_edges: set[LineageReferenceEdge] = field(
        default_factory=set
    )
    collection_pending: bool = False
    collection_plan: ObjectMetadataCollectionPlan | None = None

    def token_set(self, kind: ReferenceKind) -> set[ReferenceToken]:
        if kind is ReferenceKind.LOCAL:
            return self.local_tokens
        if kind is ReferenceKind.SUBMITTED:
            return self.submitted_tokens
        if kind is ReferenceKind.BORROWED:
            return self.borrowed_tokens
        if kind is ReferenceKind.RETAINED:
            return self.retained_tokens
        if kind is ReferenceKind.CONTAINED:
            raise AssertionError(
                "contained references require a typed hold boundary"
            )
        if kind is ReferenceKind.LINEAGE:
            return self.lineage_tokens
        raise AssertionError(f"unhandled reference kind: {kind!r}")

    @property
    def is_live(self) -> bool:
        return any(
            (
                self.local_tokens,
                self.submitted_tokens,
                self.borrowed_tokens,
                self.retained_tokens,
                self.contained_holds,
                self.lineage_tokens,
            )
        )


@dataclass
class _TaskLineageAuthority:
    """Release authority for the producer's retained dependencies.

    These holds survive attempt replacement and are claimed by the single
    output's final metadata collection.
    """

    task_id: TaskID
    output_ids: tuple[ObjectID, ...]
    obligations: set[TaskLineageObligation] = field(default_factory=set)


class ObjectOwnerTable:
    """Authoritative logical-object metadata for a single owner.

    Reference operations use unique tokens rather than integer increments.
    Consequently duplicate add/release messages are idempotent.  Result and
    location publications carry an AttemptID; messages from superseded
    attempts return ``False`` and cannot mutate current state.
    """

    def __init__(self) -> None:
        self._entries: dict[ObjectID, _ObjectOwnerEntry] = {}
        self._released_retained_tokens: set[
            tuple[ObjectID, TaskReferenceHold]
        ] = set()
        self._retained_hold_replacements: dict[
            tuple[ObjectID, TaskReferenceHold], TaskReferenceHold
        ] = {}
        self._released_submitted_tokens: set[
            tuple[ObjectID, TaskReferenceHold]
        ] = set()
        self._released_borrowed_tokens: set[
            tuple[ObjectID, ReferenceToken]
        ] = set()
        self._released_contained_holds: set[
            tuple[ObjectID, IncomingContainedReferenceHold]
        ] = set()
        self._stored_contained_preparations: dict[
            tuple[ObjectID, ContainedReferenceHold], object
        ] = {}
        self._stored_contained_promotions: dict[
            tuple[ObjectID, ContainedReferenceHold], object
        ] = {}
        self._released_lineage_tokens: set[
            tuple[ObjectID, ReferenceToken]
        ] = set()
        # A WorkerID names one physical process incarnation.  This table is a
        # permanent negative-admission fence, not a liveness detector: only a
        # caller holding an authoritative death proof may install a record.
        self._dead_worker_cleanups: dict[
            WorkerID, DeadWorkerReferenceCleanup
        ] = {}
        self._dead_worker_ids: dict[str, WorkerID] = {}
        # A compact terminal tombstone prevents the stable logical ObjectID
        # from being accidentally re-registered after physical collection.
        # Keep only a terminal collection identity, not locations, lineage, or
        # contained edges.  Those belong to the collected metadata and must be
        # forgotten after the external obligations converge.
        self._collected: dict[ObjectID, str] = {}
        # Generic stored values have no publication manifest history. Retain
        # only their retired canonical identity, never the GC plan or payload.
        self._stored_collection_history: dict[ObjectID, _StoredCollectionIdentity] = {}
        self._task_lineage: dict[TaskID, _TaskLineageAuthority] = {}
        self._output_publication_receipts: dict[
            OutputPublicationID, OutputPublicationManifest
        ] = {}
        self._output_collection_receipts: dict[
            ObjectID, _OutputOwnerCollectionTombstone
        ] = {}
        self._output_retirement_plans: dict[
            str, OutputOwnerPublicationRetirementPlan
        ] = {}
        self._output_retirement_receipts: dict[
            str, OutputOwnerPublicationRetirementReceipt
        ] = {}
        # These facts fence late replica reports and old publication replay.
        # They contain no envelopes, descriptors or producer TaskSpecs.
        self._retired_output_slots: set[
            tuple[OutputPublicationID, ObjectID]
        ] = set()
        self._retired_output_attempts: set[tuple[ObjectID, AttemptID]] = set()
        self._output_loss_receipts: dict[OutputPublicationID, object] = {}
        self._lock = RLock()

    def register(
        self,
        object_id: ObjectID,
        *,
        current_attempt: AttemptID | None = None,
        producer_task_spec: TaskSpec | None = None,
        local_token: ReferenceToken | None = None,
    ) -> None:
        """Register a pending logical object.

        Repeating the exact registration is harmless.  A conflicting attempt
        or lineage is rejected because ObjectID denotes one logical value.
        """

        _require_object_id(object_id)
        _require_attempt(object_id, current_attempt)
        _require_optional_hashable(local_token, "local_token")
        with self._lock:
            if object_id in self._collected:
                raise ObjectAlreadyRegisteredError(
                    f"object was already collected: {object_id!r}"
                )
            existing = self._entries.get(object_id)
            if existing is not None:
                if (
                    existing.current_attempt != current_attempt
                    or existing.producer_task_spec != producer_task_spec
                ):
                    raise ObjectAlreadyRegisteredError(
                        f"conflicting registration for {object_id!r}"
                    )
                if existing.collection_pending and local_token is not None:
                    raise ObjectCollectionInProgressError(
                        f"object metadata collection is pending for {object_id!r}"
                    )
                if local_token is not None:
                    existing.local_tokens.add(local_token)
                return

            entry = _ObjectOwnerEntry(
                object_id=object_id,
                current_attempt=current_attempt,
                producer_task_spec=producer_task_spec,
            )
            if local_token is not None:
                entry.local_tokens.add(local_token)
            self._entries[object_id] = entry

    def validate_register_task_outputs(
        self,
        task_spec: TaskSpec,
        *,
        local_tokens: tuple[ReferenceToken | None, ...] | None = None,
    ) -> TaskOutputRegistrationPlan:
        """Preflight a complete ordinary-task output registration.

        No entry or token is changed. The output must be absent for first
        registration or retain the exact same lineage for registration replay.
        """

        plan = TaskOutputRegistrationPlan.for_task_spec(
            task_spec, local_tokens=local_tokens
        )
        with self._lock:
            self._validate_register_task_outputs_locked(plan)
        return plan

    def commit_register_task_outputs(
        self, plan: TaskOutputRegistrationPlan
    ) -> None:
        """Register the task's output under the owner lock."""

        if not isinstance(plan, TaskOutputRegistrationPlan):
            raise TypeError("plan must be a TaskOutputRegistrationPlan")
        with self._lock:
            existing = self._validate_register_task_outputs_locked(plan)
            if not existing:
                for object_id, local_token in zip(
                    (plan.execution.object_id,), plan.local_tokens
                ):
                    entry = _ObjectOwnerEntry(
                        object_id=object_id,
                        current_attempt=plan.execution.attempt_id,
                        producer_task_spec=plan.producer_task_spec,
                    )
                    if local_token is not None:
                        entry.local_tokens.add(local_token)
                    self._entries[object_id] = entry
                return

            # An exact registration replay may attach the caller's handle
            # only after the output identity has been validated.
            for object_id, local_token in zip(
                (plan.execution.object_id,), plan.local_tokens
            ):
                if local_token is not None:
                    self._entries[object_id].local_tokens.add(local_token)

    def register_task_outputs(
        self,
        task_spec: TaskSpec,
        *,
        local_tokens: tuple[ReferenceToken | None, ...] | None = None,
    ) -> TaskOutputRegistrationPlan:
        """Validate and atomically register one complete output manifest."""

        plan = self.validate_register_task_outputs(
            task_spec, local_tokens=local_tokens
        )
        self.commit_register_task_outputs(plan)
        return plan

    def abort_registered_task_outputs(
        self, plan: TaskOutputRegistrationPlan, *,
        outgoing_lineage_edges: tuple[LineageReferenceEdge, ...] = (),
    ) -> bool:
        """Remove an exact, still-pending initial registration.

        This is the compensating half of Core submission admission.  It is
        deliberately strict: execution, publication, reference export, or GC
        activity makes abort invalid rather than deleting live authority.
        """

        if not isinstance(plan, TaskOutputRegistrationPlan):
            raise TypeError("plan must be a TaskOutputRegistrationPlan")
        expected_edges = tuple(outgoing_lineage_edges)
        if any(
            not isinstance(edge, LineageReferenceEdge)
            for edge in expected_edges
        ):
            raise TypeError(
                "outgoing_lineage_edges must contain LineageReferenceEdge values"
            )
        with self._lock:
            execution = _execution(plan.execution)
            if execution.object_id not in self._entries:
                return False
            if any(
                edge.producer_object_id not in (plan.execution.object_id,)
                for edge in expected_edges
            ):
                raise ValueError(
                    "abort lineage edges must belong to this output manifest"
                )
            expected_tokens = {
                object_id: (
                    set() if token is None else {token}
                )
                for object_id, token in zip(
                    (plan.execution.object_id,), plan.local_tokens
                )
            }
            for object_id in (plan.execution.object_id,):
                entry = self._entries[object_id]
                if (
                    entry.current_attempt != plan.execution.attempt_id
                    or entry.producer_task_spec != plan.producer_task_spec
                    or entry.state is not ObjectState.PENDING
                    or entry.inline_data is not None
                    or entry.error is not None
                    or entry.location_attempts
                    or entry.local_tokens != expected_tokens[object_id]
                    or entry.submitted_tokens
                    or entry.borrowed_tokens
                    or entry.retained_tokens
                    or entry.contained_holds
                    or entry.lineage_tokens
                    or entry.outgoing_contained_edges
                    or entry.outgoing_lineage_edges
                    or entry.collection_pending
                ):
                    raise InvalidObjectTransitionError(
                        "task output registration is no longer abortable"
                    )
            self._abort_task_lineage_edges_locked(
                plan.execution.task_id, expected_edges,
                expected_output_ids=(plan.execution.object_id,),
            )
            for object_id in (plan.execution.object_id,):
                del self._entries[object_id]
            return True

    def _validate_register_task_outputs_locked(
        self, plan: TaskOutputRegistrationPlan
    ) -> bool:
        execution = _execution(plan.execution)
        object_id = execution.object_id
        if object_id in self._collected:
            raise ObjectAlreadyRegisteredError(
                "task output was already collected: {!r}".format(object_id)
            )
        if object_id not in self._entries:
            return False
        for object_id, local_token in zip(
            (execution.object_id,), plan.local_tokens
        ):
            entry = self._entries[object_id]
            if (
                entry.current_attempt != execution.attempt_id
                or entry.producer_task_spec != plan.producer_task_spec
            ):
                raise ObjectAlreadyRegisteredError(
                    "conflicting task output registration for {!r}".format(
                        object_id
                    )
                )
            if entry.collection_pending and local_token is not None:
                raise ObjectCollectionInProgressError(
                    "object metadata collection is pending for {!r}".format(
                        object_id
                    )
                )
        return True

    def contains(self, object_id: ObjectID) -> bool:
        with self._lock:
            return object_id in self._entries

    def dead_worker_record(
        self, worker_id: WorkerID
    ) -> DeadWorkerReferenceRecord | None:
        """Return the installed immutable death fence, if any."""

        _require_worker_id(worker_id)
        with self._lock:
            cleanup = self._dead_worker_cleanups.get(worker_id)
            return None if cleanup is None else cleanup.record

    def install_dead_worker(
        self, worker_id: WorkerID, death_id: str
    ) -> DeadWorkerReferenceCleanup:
        """Atomically retire references owned by one dead Worker.

        This is a pure reducer.  It neither infers process death nor performs
        GC.  Its caller first commits an authoritative death fact, then uses
        ``collectable_object_ids`` to drive the existing Core GC obligations.

        Death cleanup is intentionally narrower than an explicit Task-hold
        release.  A live executor may already have acquired a borrower from a
        hold whose submitter subsequently died.  Removing that live borrower's
        independent reference would create use-after-free, so this transition
        retires only borrower identities whose first tuple element is the dead
        ``WorkerID``.
        """

        record = DeadWorkerReferenceRecord(worker_id, death_id)
        affected: list[ObjectID] = []
        collectable: list[ObjectID] = []
        borrowers: set[tuple[ObjectID, ReferenceToken]] = set()
        submitted: set[tuple[ObjectID, TaskReferenceHold]] = set()
        retained: set[tuple[ObjectID, TaskReferenceHold]] = set()
        contained: set[tuple[ObjectID, ContainedReferenceHold]] = set()

        with self._lock:
            previous = self._dead_worker_cleanups.get(worker_id)
            if previous is not None:
                if previous.death_id != death_id:
                    raise DeadWorkerReferenceConflictError(
                        "Worker death proof conflicts with its tombstone"
                    )
                return previous
            previous_worker = self._dead_worker_ids.get(death_id)
            if previous_worker is not None and previous_worker != worker_id:
                raise DeadWorkerReferenceConflictError(
                    "death_id was already installed for another Worker"
                )

            for object_id, entry in sorted(
                self._entries.items(), key=lambda item: repr(item[0])
            ):
                changed = False

                for token in tuple(entry.borrowed_tokens):
                    if _borrower_worker_id(token) != worker_id:
                        continue
                    entry.borrowed_tokens.remove(token)
                    entry.released_borrowed_tokens.add(token)
                    self._released_borrowed_tokens.add((object_id, token))
                    # Keep borrowed_sources as the immutable credential
                    # binding.  It proves what this terminal borrower named
                    # without allowing the token to become active again.
                    borrowers.add((object_id, token))
                    changed = True

                for hold in tuple(entry.submitted_tokens):
                    if hold.submitting_worker_id != worker_id:
                        continue
                    entry.submitted_tokens.remove(hold)
                    self._released_submitted_tokens.add((object_id, hold))
                    submitted.add((object_id, hold))
                    changed = True

                for hold in tuple(entry.retained_tokens):
                    if hold.submitting_worker_id != worker_id:
                        continue
                    entry.retained_tokens.remove(hold)
                    entry.released_retained_tokens.add(hold)
                    self._released_retained_tokens.add((object_id, hold))
                    retained.add((object_id, hold))
                    changed = True

                for hold in tuple(entry.contained_holds):
                    if hold.container_owner_worker_id != worker_id:
                        continue
                    entry.contained_holds.remove(hold)
                    self._released_contained_holds.add((object_id, hold))
                    contained.add((object_id, hold))
                    changed = True

                if changed:
                    affected.append(object_id)
                    if not entry.is_live:
                        collectable.append(object_id)

            cleanup = DeadWorkerReferenceCleanup(
                record=record,
                affected_object_ids=tuple(affected),
                collectable_object_ids=tuple(collectable),
                released_borrower_tokens=frozenset(borrowers),
                released_submitted_holds=frozenset(submitted),
                released_retained_holds=frozenset(retained),
                released_contained_holds=frozenset(contained),
            )
            # Publish both proof indexes only after the full scan.  The lock
            # makes the cleanup and its negative-admission fence atomic.
            self._dead_worker_ids[death_id] = worker_id
            self._dead_worker_cleanups[worker_id] = cleanup
            return cleanup

    def snapshot(self, object_id: ObjectID) -> ObjectOwnerSnapshot:
        with self._lock:
            entry = self._entry(object_id)
            authority = self._task_lineage.get(object_id.task_id)
            task_edges = (
                frozenset()
                if authority is None
                else frozenset(
                    LineageReferenceEdge(
                        object_id, obligation.dependency_object_id,
                        obligation.token,
                    )
                    for obligation in authority.obligations
                )
            )
            snapshot = ObjectOwnerSnapshot(
                object_id=entry.object_id,
                state=entry.state,
                current_attempt=entry.current_attempt,
                inline_data=entry.inline_data,
                error=entry.error,
                locations=frozenset(entry.location_attempts),
                canonical_stored_result=entry.canonical_stored_result,
                producer_task_spec=entry.producer_task_spec,
                local_tokens=frozenset(entry.local_tokens),
                submitted_tokens=frozenset(entry.submitted_tokens),
                borrowed_tokens=frozenset(entry.borrowed_tokens),
                released_borrowed_tokens=frozenset(
                    entry.released_borrowed_tokens
                ),
                borrowed_transfer_tokens=frozenset(
                    (borrower_token, source.transfer_token)
                    for borrower_token, source in entry.borrowed_sources.items()
                    if isinstance(source, ContainedTransferSource)
                ),
                borrowed_sources=frozenset(entry.borrowed_sources.items()),
                retained_tokens=frozenset(entry.retained_tokens),
                released_retained_tokens=frozenset(
                    entry.released_retained_tokens
                ),
                retained_borrower_tokens=frozenset(
                    entry.retained_borrower_tokens.items()
                ),
                contained_holds=frozenset(entry.contained_holds),
                contained_tokens=frozenset(
                    hold.transfer_token for hold in entry.contained_holds
                ),
                outgoing_contained_edges=frozenset(
                    entry.outgoing_contained_edges
                ),
                lineage_tokens=frozenset(entry.lineage_tokens),
                released_lineage_tokens=frozenset(
                    entry.released_lineage_tokens
                ),
                # Project the task's retained dependencies into this output's
                # diagnostic view without duplicating release authority.
                outgoing_lineage_edges=(
                    frozenset(entry.outgoing_lineage_edges) | task_edges
                ),
                collection_pending=entry.collection_pending,
                collection_plan=entry.collection_plan,
                output_publication=entry.output_publication,
                output_retirement_id=entry.output_retirement_id,
            )
            # Unified publication metadata is a trust boundary, including the
            # slot's descriptor/edge projections. A frozen dataclass
            # can still be mutated through object.__setattr__; no public view
            # may alias the canonical manifest or its nested identity values.
            return (deepcopy(snapshot) if entry.output_publication is not None
                    or (entry.object_id, entry.current_attempt) in self._retired_output_attempts
                    else snapshot)

    def collection_state(self, object_id: ObjectID) -> ObjectCollectionState:
        """Return ACTIVE/COLLECTING/COLLECTED without reviving metadata."""

        _require_object_id(object_id)
        with self._lock:
            if object_id in self._collected:
                return ObjectCollectionState.COLLECTED
            entry = self._entry(object_id)
            if entry.collection_pending:
                return ObjectCollectionState.COLLECTING
            return ObjectCollectionState.ACTIVE

    def advance_attempt(
        self,
        object_id: ObjectID,
        *,
        expected_attempt: AttemptID | None,
        next_attempt: AttemptID,
    ) -> bool:
        """CAS the publishing attempt and reset the entry to ``PENDING``.

        A repeated call for the already-current ``next_attempt`` is accepted.
        A call based on a superseded expected attempt returns ``False``.
        """

        _require_hashable(next_attempt, "next_attempt")
        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, expected_attempt)
            _require_attempt(object_id, next_attempt, allow_none=False)
            if entry.current_attempt == next_attempt:
                return True
            if entry.current_attempt != expected_attempt:
                return False
            self._require_output_memberships_retired_locked((entry,))
            if (
                entry.current_attempt is not None
                and next_attempt.attempt_number
                <= entry.current_attempt.attempt_number
            ):
                raise InvalidObjectTransitionError(
                    f"attempt number must increase for {object_id!r}: "
                    f"current={entry.current_attempt.attempt_number}, "
                    f"next={next_attempt.attempt_number}"
                )
            if entry.state not in (ObjectState.PENDING, ObjectState.LOST):
                raise InvalidObjectTransitionError(
                    f"cannot advance {object_id!r} from state {entry.state.value}"
                )
            if entry.location_attempts:
                raise InvalidObjectTransitionError(
                    f"cannot advance {object_id!r} while replicas remain"
                )

            entry.current_attempt = next_attempt
            entry.state = ObjectState.PENDING
            entry.inline_data = None
            entry.error = None
            entry.canonical_stored_result = None
            return True

    def validate_advance_task_outputs(
        self,
        expected: TaskExecution,
        next_attempt: AttemptID,
    ) -> TaskOutputAttemptAdvancePlan:
        """Preflight a manifest-wide attempt CAS without mutation."""

        plan = TaskOutputAttemptAdvancePlan(
            expected, expected.for_attempt(next_attempt)
        )
        with self._lock:
            self._validate_advance_task_outputs_locked(plan)
        return plan

    def commit_advance_task_outputs(
        self, plan: TaskOutputAttemptAdvancePlan
    ) -> bool:
        """Reset the task output to one new publishing attempt."""

        if not isinstance(plan, TaskOutputAttemptAdvancePlan):
            raise TypeError("plan must be a TaskOutputAttemptAdvancePlan")
        with self._lock:
            action = self._validate_advance_task_outputs_locked(plan)
            if action == "stale":
                return False
            if action == "replay":
                return True
            for object_id in (plan.expected.object_id,):
                entry = self._entries[object_id]
                entry.current_attempt = plan.next_execution.attempt_id
                entry.state = ObjectState.PENDING
                entry.inline_data = None
                entry.error = None
                entry.canonical_stored_result = None
            return True

    def commit_validated_advance_task_outputs(
        self, plan: TaskOutputAttemptAdvancePlan
    ) -> None:
        """Apply a caller-held validated plan using assignment only."""

        self._require_no_output_retirements_locked(tuple(
            self._entries[object_id] for object_id in (plan.expected.object_id,)
        ))
        self._require_output_memberships_retired_locked(tuple(
            self._entries[object_id] for object_id in (plan.expected.object_id,)
        ))
        for object_id in (plan.expected.object_id,):
            entry = self._entries[object_id]
            entry.current_attempt = plan.next_execution.attempt_id
            entry.state = ObjectState.PENDING
            entry.inline_data = None
            entry.error = None
            entry.canonical_stored_result = None

    def advance_task_outputs(
        self, expected: TaskExecution, next_attempt: AttemptID
    ) -> bool:
        plan = self.validate_advance_task_outputs(expected, next_attempt)
        return self.commit_advance_task_outputs(plan)

    def _validate_advance_task_outputs_locked(
        self, plan: TaskOutputAttemptAdvancePlan
    ) -> str:
        entries, _ = self._task_output_entries_locked(plan.expected)
        entry = entries[0]
        if entry.collection_pending:
            raise ObjectCollectionInProgressError(
                "task output metadata collection is pending"
            )
        self._require_no_output_retirements_locked(entries)
        expected_attempt = plan.expected.attempt_id
        next_attempt = plan.next_execution.attempt_id
        if entry.current_attempt == next_attempt:
            return "replay"
        if entry.current_attempt != expected_attempt:
            return "stale"
        self._require_output_memberships_retired_locked(entries)
        if entry.state not in (ObjectState.PENDING, ObjectState.LOST):
            raise InvalidObjectTransitionError(
                "cannot advance task output {!r} from state {}".format(
                    entry.object_id, entry.state.value
                )
            )
        if entry.location_attempts:
            raise InvalidObjectTransitionError(
                "cannot advance a task output while replicas remain"
            )
        return "apply"

    def publish_inline(
        self, object_id: ObjectID, attempt_id: AttemptID | None, data: bytes
    ) -> bool:
        """Publish a small inline result if ``attempt_id`` is current."""

        if not isinstance(data, bytes):
            raise TypeError("inline data must be bytes")
        payload = data
        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, attempt_id)
            if entry.current_attempt != attempt_id:
                return False
            if entry.state is ObjectState.READY_INLINE:
                if entry.inline_data != payload:
                    raise ConflictingObjectResultError(
                        f"attempt published conflicting inline data for {object_id!r}"
                    )
                return True
            self._require_publishable(entry)
            entry.state = ObjectState.READY_INLINE
            entry.inline_data = payload
            entry.error = None
            entry.canonical_stored_result = None
            return True

    def publish_put_value(
        self, object_id: ObjectID, attempt: AttemptID,
        descriptor: ResultDescriptor, edges: tuple[ContainedReferenceEdge, ...],
    ) -> bool:
        """Atomically publish a put value and its child-release obligations.

        Core validates the publishing owner and final child pins before this
        local commit. An exact replay checks identity without restoring bytes,
        erased locations, or released references. Put has no producer lineage.
        """
        object_id, attempt = _object_id(object_id), _attempt(attempt)
        _require_attempt(object_id, attempt, allow_none=False)
        descriptor = _descriptor(descriptor)
        if descriptor.object_id != object_id:
            raise ValueError("put descriptor must name the registered object")
        child_edges = {}
        child_routes = {}
        for edge in _sequence(edges, "put edges"):
            if type(edge) is not ContainedReferenceEdge:
                raise TypeError("put edges require ContainedReferenceEdge values")
            edge = ContainedReferenceEdge(
                _object_id(edge.container_object_id), _object_id(edge.contained_object_id),
                _opaque(edge.contained_owner_worker_id, WorkerID, "child owner"),
                edge.contained_owner_address, _string(edge.transfer_token, "transfer_token"),
            )
            if edge.container_object_id != object_id:
                raise ValueError("put edge must name the published container")
            route = (edge.contained_owner_worker_id, edge.contained_owner_address)
            if child_routes.setdefault(edge.contained_object_id, route) != route:
                raise ConflictingObjectResultError("put repeats a child with conflicting edge owner or address")
            key = (edge.contained_object_id, edge.transfer_token)
            if child_edges.setdefault(key, edge) != edge:
                raise ConflictingObjectResultError("put repeats a child with conflicting edge identity")
        edge_set = set(child_edges.values())
        inline = descriptor.storage is ResultStorage.INLINE
        identity = (attempt, descriptor.owner_worker_id, descriptor.node_id, descriptor.storage,
                    descriptor.size_bytes, descriptor.checksum)
        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError("put metadata collection is pending")
            if entry.current_attempt != attempt:
                return False
            if (entry.producer_task_spec is not None or object_id.task_id in self._task_lineage
                    or entry.outgoing_lineage_edges or entry.output_publication is not None):
                raise InvalidObjectTransitionError("put cannot publish task lineage or publication membership")
            if descriptor.owner_worker_id in self._dead_worker_cleanups:
                return False
            if entry.put_identity is not None:
                valid = (entry.put_identity == identity and entry.outgoing_contained_edges == edge_set
                         and entry.error is None and (
                    inline and entry.state is ObjectState.READY_INLINE
                    and entry.inline_data == descriptor.inline_data
                    and entry.canonical_stored_result is None and not entry.location_attempts
                    or not inline and entry.state is ObjectState.READY_STORED
                    and entry.inline_data is None and entry.canonical_stored_result == descriptor
                    and bool(entry.location_attempts)
                    and all(epoch == attempt for epoch in entry.location_attempts.values())))
                if not valid:
                    raise ConflictingObjectResultError("put replay changed identity, edges, or available result")
                return True
            self._require_publishable(entry)
            if (entry.inline_data is not None or entry.error is not None or entry.canonical_stored_result is not None
                    or entry.location_attempts or entry.outgoing_contained_edges):
                raise ConflictingObjectResultError("put requires pristine pending result metadata")
            locations = {} if inline else {descriptor.node_id: attempt}
            entry.inline_data = descriptor.inline_data if inline else None
            entry.canonical_stored_result = None if inline else descriptor
            entry.location_attempts = locations
            entry.outgoing_contained_edges = edge_set
            entry.put_identity = identity
            entry.state = ObjectState.READY_INLINE if inline else ObjectState.READY_STORED
            return True

    def publish_stored(
        self,
        object_id: ObjectID,
        attempt_id: AttemptID | None,
        location: ObjectLocation,
        *,
        descriptor: ResultDescriptor | None = None,
    ) -> bool:
        """Publish or add one sealed replica for the current attempt.

        ``descriptor`` describes the replica at ``location``.  The first such
        descriptor freezes the logical result's canonical identity.  Later
        replicas may change only ``node_id``; object, owner, storage, size,
        checksum, and inline-data identity remain immutable.  A location-only
        report is retained for compact pure fixtures and must never clear an
        already-known canonical descriptor.
        """

        _require_location(location)
        if descriptor is not None and (
            not isinstance(descriptor, ResultDescriptor)
            or descriptor.object_id != object_id
            or descriptor.storage is not ResultStorage.OBJECT_STORE
            or descriptor.node_id != location
        ):
            raise ValueError(
                "stored descriptor must match object, storage, and location"
            )
        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, attempt_id)
            if entry.current_attempt != attempt_id:
                return False

            # Preflight every conflict check before changing any owner state.
            # In particular, a READY_STORED replay must not install its first
            # descriptor before discovering a conflicting location epoch, and
            # a LOST rediscovery must not become READY_STORED before discovering
            # a conflicting canonical descriptor.
            canonical = entry.canonical_stored_result
            if (
                descriptor is not None
                and canonical is not None
                and not _same_stored_result_identity(canonical, descriptor)
            ):
                message = (
                    "stored publication changed canonical descriptor"
                    if entry.state is ObjectState.READY_STORED
                    else "rediscovered replica changed canonical descriptor"
                )
                raise ConflictingObjectResultError(message)

            conflicting_location = next(
                (
                    known_location
                    for known_location, known_attempt in (
                        entry.location_attempts.items()
                    )
                    if known_attempt != attempt_id
                ),
                None,
            )
            if conflicting_location is not None:
                raise ConflictingObjectResultError(
                    f"location {conflicting_location!r} has a different "
                    "attempt epoch"
                )

            if entry.state is ObjectState.READY_STORED:
                if descriptor is not None and canonical is None:
                    entry.canonical_stored_result = descriptor
                entry.location_attempts[location] = attempt_id
                return True
            # A replica-location report may race with a loss notification.
            # Re-discovering a replica from the *same current attempt* is safe,
            # but LOST must never allow the logical result to change to inline
            # data or an error.
            self._require_publishable(entry, allow_lost=True)
            entry.state = ObjectState.READY_STORED
            entry.inline_data = None
            entry.error = None
            if descriptor is not None and canonical is None:
                entry.canonical_stored_result = descriptor
            entry.location_attempts[location] = attempt_id
            return True

    add_location = publish_stored

    def remove_location(
        self,
        object_id: ObjectID,
        attempt_id: AttemptID | None,
        location: ObjectLocation,
    ) -> bool:
        """Remove a matching replica epoch; stale removals are ignored."""

        with self._lock:
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, attempt_id)
            _require_location(location)
            if entry.location_attempts.get(location, _MISSING) != attempt_id:
                return False
            del entry.location_attempts[location]
            if entry.state is ObjectState.READY_STORED and not entry.location_attempts:
                entry.state = ObjectState.LOST
            return True

    def remove_node_locations(self, node_id: NodeID) -> NodeLocationRemoval:
        """Forget every replica on one authoritatively dead node atomically.

        This reducer does not decide that a node is dead.  Its caller must first
        commit the membership fence.  COLLECTING entries retain their frozen
        location set so collection identity cannot change underneath durable
        replica-drop obligations.
        """

        _require_location(node_id)
        lost: list[ObjectID] = []
        surviving: list[ObjectID] = []
        collecting: list[ObjectID] = []
        with self._lock:
            for object_id, entry in sorted(
                self._entries.items(), key=lambda item: repr(item[0])
            ):
                if node_id not in entry.location_attempts:
                    continue
                if entry.collection_pending:
                    collecting.append(object_id)
                    continue
                del entry.location_attempts[node_id]
                if entry.location_attempts:
                    surviving.append(object_id)
                else:
                    if entry.state is ObjectState.READY_STORED:
                        entry.state = ObjectState.LOST
                    lost.append(object_id)
        return NodeLocationRemoval(
            node_id, tuple(lost), tuple(surviving), tuple(collecting)
        )

    def publish_error(
        self, object_id: ObjectID, attempt_id: AttemptID | None, error: object
    ) -> bool:
        """Publish a terminal logical error for the current attempt."""

        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, attempt_id)
            if entry.current_attempt != attempt_id:
                return False
            if entry.state is ObjectState.ERROR:
                if not _safely_equal(entry.error, error):
                    raise ConflictingObjectResultError(
                        f"attempt published conflicting errors for {object_id!r}"
                    )
                return True
            self._require_publishable(entry)
            entry.state = ObjectState.ERROR
            entry.error = error
            entry.inline_data = None
            return True

    def validate_publish_task_outputs(
        self,
        execution: TaskExecution,
        results: tuple[ResultDescriptor, ...],
    ) -> TaskOutputPublicationPlan | None:
        """Preflight a complete success manifest without visible results."""

        plan = TaskOutputPublicationPlan(execution, tuple(results))
        with self._lock:
            if not self._validate_publish_task_outputs_locked(plan):
                return None
        return plan


    def commit_validated_publish_task_outputs(
        self, plan: TaskOutputPublicationPlan
    ) -> None:
        """Apply a caller-held validated success plan without revalidation."""

        self._require_no_output_retirements_locked(tuple(
            self._entries[object_id] for object_id in (plan.execution.object_id,)
        ))
        for descriptor in plan.results:
            entry = self._entries[descriptor.object_id]
            if descriptor.storage is ResultStorage.INLINE:
                entry.state = ObjectState.READY_INLINE
                entry.inline_data = descriptor.inline_data
                entry.error = None
                entry.canonical_stored_result = None
            else:
                entry.state = ObjectState.READY_STORED
                entry.inline_data = None
                entry.error = None
                entry.canonical_stored_result = descriptor
                entry.location_attempts[descriptor.node_id] = (
                    plan.execution.attempt_id
                )


    def validate_output_publication(
        self, plan: OutputOwnerPublicationPlan,
    ) -> OutputOwnerPublicationDisposition:
        """Forecast the output CAS without changing result metadata."""

        plan = self._validated_output_publication_plan(plan)
        with self._lock:
            return self._validate_output_publication_locked(plan)

    def commit_output_publication(
        self, plan: OutputOwnerPublicationPlan,
    ) -> OutputOwnerPublicationReceipt:
        """Publish the result and its contained-reference edges atomically.

        The caller owns remote child-pin acknowledgements. This method owns
        only the local CAS: complete validation and allocation precede the
        first assignment, with the same table lock held through the commit.
        """

        plan = self._validated_output_publication_plan(plan)
        manifest = plan.envelope.manifest
        with self._lock:
            disposition = self._validate_output_publication_locked(plan)
            if disposition is not OutputOwnerPublicationDisposition.APPLIED:
                return OutputOwnerPublicationReceipt(plan, disposition)
            public_receipt = OutputOwnerPublicationReceipt(
                replace(plan), OutputOwnerPublicationDisposition.APPLIED
            )
            entry = self._entries[plan.publication_id.object_id]
            membership = OutputOwnerPublicationMembership(manifest, 0)
            descriptor = plan.envelope.result
            edges = set(manifest.value.edges)
            locations = ({descriptor.node_id: plan.execution.attempt_id}
                         if descriptor.storage is ResultStorage.OBJECT_STORE else {})
            inline = descriptor.storage is ResultStorage.INLINE
            entry.state = ObjectState.READY_INLINE if inline else ObjectState.READY_STORED
            entry.inline_data = descriptor.inline_data if inline else None
            entry.canonical_stored_result = None if inline else descriptor
            entry.error = None
            entry.location_attempts = locations
            entry.outgoing_contained_edges = edges
            entry.output_publication = membership
            self._output_publication_receipts[plan.publication_id] = manifest
            return public_receipt

    @staticmethod
    def _validated_output_publication_plan(
        plan: OutputOwnerPublicationPlan,
    ) -> OutputOwnerPublicationPlan:
        if type(plan) is not OutputOwnerPublicationPlan:
            raise TypeError("plan must be an OutputOwnerPublicationPlan")
        # Frozen dataclasses are not a trust boundary for RPC/deserialized
        # values. Rebuild nested metadata and descriptor payload checks.
        return replace(plan)

    def _validate_output_publication_locked(
        self, plan: OutputOwnerPublicationPlan,
    ) -> OutputOwnerPublicationDisposition:
        manifest = plan.envelope.manifest
        previous = self._output_publication_receipts.get(plan.publication_id)
        if previous is not None and previous != manifest:
            raise OutputOwnerPublicationConflictError(
                "output publication identity was rebound"
            )
        object_id = plan.publication_id.object_id
        if (object_id in self._collected
                or (plan.publication_id, object_id) in self._retired_output_slots
                or (object_id, plan.execution.attempt_id) in self._retired_output_attempts
                or manifest.header.owner_worker_id in self._dead_worker_cleanups):
            return OutputOwnerPublicationDisposition.FENCED
        entry = self._entry(object_id)
        task_spec = entry.producer_task_spec
        if (
            not isinstance(task_spec, TaskSpec)
            or tuple(task_spec.return_ids()) != plan.output_ids
        ):
            raise OutputOwnerPublicationConflictError(
                "output publication requires the canonical full producer lineage"
            )
        if (task_spec.owner_worker_id != manifest.header.owner_worker_id
                or task_spec.job_id != manifest.header.job_id):
            raise OutputOwnerPublicationConflictError(
                "output publication owner and job must match registered lineage"
            )
        if entry.current_attempt != plan.execution.attempt_id:
            return OutputOwnerPublicationDisposition.FENCED
        if entry.collection_pending:
            raise ObjectCollectionInProgressError(
                "output collection is pending"
            )
        self._require_no_output_retirement_locked(entry)
        if previous is not None and entry.output_publication is None:
            # A success receipt cannot recreate retired result metadata.
            return OutputOwnerPublicationDisposition.FENCED
        descriptor = plan.envelope.result
        slot = manifest.value
        membership = entry.output_publication
        if membership is not None:
            if membership != OutputOwnerPublicationMembership(manifest, 0):
                raise OutputOwnerPublicationCollectionRequiredError(
                    "output still belongs to an unretired publication"
                )
            if previous is None:
                raise OutputOwnerPublicationConflictError(
                    "output membership exists without its publication receipt"
                )
            if entry.error is not None or entry.outgoing_contained_edges != set(slot.edges):
                raise OutputOwnerPublicationConflictError(
                    "output publication metadata changed after commit"
                )
            if descriptor.storage is ResultStorage.INLINE:
                valid = (
                    entry.state is ObjectState.READY_INLINE
                    and entry.inline_data == descriptor.inline_data
                    and entry.canonical_stored_result is None
                    and not entry.location_attempts
                )
            else:
                valid = (
                    entry.state in (ObjectState.READY_STORED, ObjectState.LOST)
                    and entry.inline_data is None
                    and entry.canonical_stored_result == descriptor
                    and all(epoch == plan.execution.attempt_id for epoch in entry.location_attempts.values())
                    and ((entry.state is ObjectState.READY_STORED and bool(entry.location_attempts))
                         or (entry.state is ObjectState.LOST and not entry.location_attempts))
                )
            if not valid:
                raise OutputOwnerPublicationConflictError(
                    "output publication result changed after commit"
                )
            if entry.state is ObjectState.LOST:
                return OutputOwnerPublicationDisposition.FENCED
            return OutputOwnerPublicationDisposition.ALREADY_APPLIED
        if (entry.state is not ObjectState.PENDING
                or entry.inline_data is not None or entry.error is not None
                or entry.canonical_stored_result is not None
                or entry.location_attempts or entry.outgoing_contained_edges):
            raise OutputOwnerPublicationConflictError(
                "output contains partial or incompatible publication metadata"
            )
        return OutputOwnerPublicationDisposition.APPLIED

    def output_owner_publication(
        self, object_id: ObjectID,
    ) -> OutputOwnerPublicationMembership | None:
        """Return current metadata membership, never the whole result batch."""

        _require_object_id(object_id)
        with self._lock:
            if object_id in self._collected:
                return None
            membership = self._entry(object_id).output_publication
            if membership is None:
                return None
            return OutputOwnerPublicationMembership(
                replace(membership.manifest), membership.slot_index
            )

    def output_owner_result(self, object_id: ObjectID) -> ResultDescriptor | None:
        """Rebuild only this live slot's descriptor from existing owner fields.

        A LOST stored slot still has integrity metadata, not a usable replica.
        Callers must separately consult readiness and current locations.
        """

        _require_object_id(object_id)
        with self._lock:
            if object_id in self._collected:
                return None
            entry = self._entry(object_id)
            membership = entry.output_publication
            if membership is None:
                return None
            slot = membership.manifest.value
            descriptor = ResultDescriptor(
                membership.object_id, slot.tier, slot.size_bytes,
                membership.manifest.header.owner_worker_id,
                membership.manifest.header.node_incarnation.node_id,
                slot.checksum, entry.inline_data if slot.tier is ResultStorage.INLINE else None,
            )
            if (entry.current_attempt != membership.publication_id.attempt_id
                    or slot.tier is ResultStorage.OBJECT_STORE
                    and entry.canonical_stored_result != descriptor):
                raise OutputOwnerPublicationConflictError(
                    "current result fields disagree with output membership"
                )
            return deepcopy(descriptor)

    def output_owner_publication_receipt(
        self, plan: OutputOwnerPublicationPlan,
    ) -> OutputOwnerPublicationReceipt | None:
        """Validate a caller-retained batch against byte-free commit history."""

        plan = self._validated_output_publication_plan(plan)
        with self._lock:
            if plan.publication_id not in self._output_publication_receipts:
                return None
            return OutputOwnerPublicationReceipt(
                plan, self._validate_output_publication_locked(plan)
            )

    def retired_output_replica(
        self, descriptor: ObjectStoreDescriptor, *,
        rejected_publications: tuple[OutputPublicationID, ...] = (),
    ) -> DropObjectReplica | None:
        """Validate late sealed-replica cleanup against retained publication history.

        Core supplies only its immutable, already-latched DROP choices. After
        resolution/retirement/GC, the owner tombstones supply the same fence.
        No current fetch route, TaskSpec or result bytes can authorize deletion
        of a different attempt. Returning a request does not perform cleanup.
        """
        if type(descriptor) is not ObjectStoreDescriptor:
            raise TypeError("retired replica requires an exact descriptor")
        descriptor = replace(descriptor, object_id=_object_id(descriptor.object_id),
            producer_attempt_id=_attempt(descriptor.producer_attempt_id),
            owner_worker_id=_opaque(descriptor.owner_worker_id, WorkerID, "replica owner"),
            node_id=_opaque(descriptor.node_id, NodeID, "replica Node"))
        rejected = set(rejected_publications)
        with self._lock:
            entry = self._entries.get(descriptor.object_id)
            current = None if entry is None else entry.output_publication
            if (current is not None and current.publication_id.attempt_id == descriptor.producer_attempt_id
                    and current.publication_id not in rejected
                    and (current.publication_id, descriptor.object_id) not in self._retired_output_slots
                    and not entry.collection_pending and entry.output_retirement_id is None):
                return None
            for identity, manifest in self._output_publication_receipts.items():
                if (identity.attempt_id != descriptor.producer_attempt_id
                        or descriptor.object_id not in (identity.object_id,)):
                    continue
                retired = (identity in rejected
                           or (identity, descriptor.object_id) in self._retired_output_slots
                           or (descriptor.object_id, identity.attempt_id) in self._retired_output_attempts
                           or current is not None and current.publication_id == identity
                           and (entry.collection_pending or entry.output_retirement_id is not None)
                           or descriptor.object_id in self._collected)
                if not retired:
                    continue
                slot = manifest.value
                if (slot.tier is not ResultStorage.OBJECT_STORE
                        or descriptor.owner_worker_id != manifest.header.owner_worker_id
                        or descriptor.size_bytes != slot.size_bytes or descriptor.checksum != slot.checksum):
                    raise OutputOwnerPublicationConflictError("late replica conflicts with retired publication metadata")
                return DropObjectReplica(descriptor.object_id, descriptor.producer_attempt_id,
                                         descriptor.owner_worker_id, descriptor.node_id, descriptor.checksum)
            return None

    @staticmethod
    def _stored_collection_identity(entry, plan) -> _StoredCollectionIdentity | None:
        canonical = entry.canonical_stored_result
        if entry.output_publication is not None or canonical is None:
            # Publication slots use their existing manifest history. Compact
            # location-only fixtures cannot manufacture missing owner identity.
            return None
        try:
            if (type(canonical) is not ResultDescriptor
                    or canonical.storage is not ResultStorage.OBJECT_STORE
                    or canonical.inline_data is not None
                    or entry.state not in (ObjectState.READY_STORED, ObjectState.LOST)
                    or not entry.collection_pending or entry.collection_plan != plan):
                raise ValueError("generic stored collection lacks a frozen canonical claim")
            object_id = _object_id(entry.object_id)
            attempt = _attempt(entry.current_attempt)
            owner = _opaque(canonical.owner_worker_id, WorkerID, "collected stored owner")
            _uint(canonical.size_bytes, "collected stored size")
            if (_object_id(canonical.object_id) != object_id or _object_id(plan.object_id) != object_id
                    or _attempt(plan.producer_attempt_id) != attempt
                    or type(plan.canonical_size_bytes) is not int
                    or canonical.size_bytes != plan.canonical_size_bytes
                    or canonical.checksum != plan.canonical_checksum):
                raise ValueError("generic collection plan disagrees with canonical metadata")
            # Reuse the ordinary descriptor validator for the checksum contract
            # without retaining its former fetch location in the tombstone.
            checked = ObjectStoreDescriptor(object_id, owner, attempt,
                _opaque(canonical.node_id, NodeID, "collected source Node"),
                canonical.size_bytes, canonical.checksum)
            return _StoredCollectionIdentity(_object_id(object_id), _attempt(attempt),
                _opaque(owner, WorkerID, "collected stored owner"), checked.size_bytes,
                checked.checksum, _string(plan.collection_id, "stored collection_id"))
        except (AttributeError, TypeError, ValueError, ProtocolError) as exc:
            raise OutputOwnerPublicationConflictError(str(exc)) from exc

    def retired_stored_replica(self, descriptor: ObjectStoreDescriptor) -> DropObjectReplica | None:
        """Validate a late generic replica against owner collection authority.

        COLLECTING uses its frozen canonical claim; COLLECTED uses the compact
        identity captured before deleting metadata. Neither lookup revives an
        object nor changes the original collection's locations or edge releases.
        """
        if type(descriptor) is not ObjectStoreDescriptor:
            raise TypeError("retired stored replica requires an exact descriptor")
        descriptor = replace(descriptor, object_id=_object_id(descriptor.object_id),
            producer_attempt_id=_attempt(descriptor.producer_attempt_id),
            owner_worker_id=_opaque(descriptor.owner_worker_id, WorkerID, "replica owner"),
            node_id=_opaque(descriptor.node_id, NodeID, "replica Node"))
        with self._lock:
            history = self._stored_collection_history.get(descriptor.object_id)
            if history is not None:
                if (descriptor.object_id in self._entries
                        or self._collected.get(descriptor.object_id) != history.collection_id):
                    raise OutputOwnerPublicationConflictError("stored collection history conflicts with owner state")
            else:
                entry = self._entries.get(descriptor.object_id)
                if entry is None or not entry.collection_pending:
                    return None
                history = self._stored_collection_identity(entry, entry.collection_plan)
                if history is None:
                    return None
            if (descriptor.object_id, descriptor.producer_attempt_id, descriptor.owner_worker_id,
                    descriptor.size_bytes, descriptor.checksum) != (
                    history.object_id, history.producer_attempt_id, history.owner_worker_id,
                    history.size_bytes, history.checksum):
                raise OutputOwnerPublicationConflictError("late replica conflicts with collected stored identity")
            return DropObjectReplica(descriptor.object_id, descriptor.producer_attempt_id,
                                     descriptor.owner_worker_id, descriptor.node_id, descriptor.checksum)

    def surviving_output_locations(
        self, manifest: OutputPublicationManifest, slot_index: int,
        *, unavailable_nodes: tuple[NodeID, ...] = (),
    ) -> tuple[NodeID, ...]:
        """Read adopted, current-epoch replica custody before choosing KEEP.

        This does not probe a Node. Runtime locations are grant-backed owner
        facts; Core supplies membership death fences under its composition
        lock. An envelope or a location without the exact atomic owner receipt
        cannot authorize keeping a stored publication.
        """
        if type(manifest) is not OutputPublicationManifest:
            raise TypeError("survivor selection requires a publication manifest")
        manifest = replace(manifest)
        _uint(slot_index, "slot_index")
        if slot_index != 0:
            raise OutputOwnerPublicationConflictError("survivor slot is outside the manifest")
        unavailable = {_opaque(node, NodeID, "unavailable Node")
                       for node in _sequence(unavailable_nodes, "unavailable_nodes")}
        unavailable.add(manifest.header.node_incarnation.node_id)
        slot = manifest.value
        if slot.tier is not ResultStorage.OBJECT_STORE:
            return ()
        identity = manifest.publication_id
        with self._lock:
            entry = self._entries.get(identity.object_id)
            if (entry is None or entry.state is not ObjectState.READY_STORED
                    or entry.current_attempt != identity.attempt_id
                    or entry.collection_pending or entry.collection_plan is not None
                    or entry.output_retirement_id is not None
                    or (identity, identity.object_id) in self._retired_output_slots
                    or (identity.object_id, identity.attempt_id) in self._retired_output_attempts
                    or self._output_publication_receipts.get(identity) != manifest
                    or entry.output_publication != OutputOwnerPublicationMembership(manifest, slot_index)):
                return ()
            canonical = ResultDescriptor(
                identity.object_id, slot.tier, slot.size_bytes, manifest.header.owner_worker_id,
                manifest.header.node_incarnation.node_id, slot.checksum,
            )
            if (entry.canonical_stored_result != canonical or entry.inline_data is not None
                    or entry.error is not None or entry.outgoing_contained_edges != set(slot.edges)
                    or any(epoch != identity.attempt_id for epoch in entry.location_attempts.values())):
                return ()
            return tuple(sorted(node for node in entry.location_attempts if node not in unavailable))

    def resolve_output_node_loss(
        self, manifest: OutputPublicationManifest, resolution: object,
        envelope: OutputPublicationEnvelope | None = None,
        *, unavailable_nodes: tuple[NodeID, ...] = (),
    ) -> bool:
        """Apply a local keep-or-discard receipt after publisher death.

        No remote cleanup happens here. The caller must already acknowledge
        every discarded child hold and replica. INLINE KEEP needs actual local
        bytes; STORED KEEP needs an already-adopted slot, whose replica
        locations were recorded by the normal grant/owner protocol. The caller
        chooses KEEP from live custody, then supplies its current death fences
        under the Core composition lock. A secondary lost after that choice
        leaves the kept publication LOST, not resurrected from a saved route.
        Incoming refs and canonical lineage stay.
        """
        from .output_handoff import NodeLostOutputResolution
        if type(manifest) is not OutputPublicationManifest or type(resolution) is not NodeLostOutputResolution:
            raise TypeError("Node-loss resolution requires exact typed metadata")
        manifest, resolution = replace(manifest), replace(resolution)
        resolution.validate_manifest(manifest)
        publisher = manifest.header.node_incarnation
        death = resolution.node_death
        if (death.reason is not NodeDeathReason.PROCESS_EXIT
                or (death.node_id, death.node_pid, death.registration_epoch)
                != (publisher.node_id, publisher.node_pid, publisher.registration_epoch)):
            raise OutputOwnerPublicationConflictError("Node-loss resolution changed publishing Node incarnation")
        unavailable = {_opaque(node, NodeID, "unavailable Node")
                       for node in _sequence(unavailable_nodes, "unavailable_nodes")}
        unavailable.add(publisher.node_id)
        if envelope is not None:
            envelope = replace(envelope)
            if envelope.manifest != manifest or envelope.complete != resolution.complete:
                raise OutputOwnerPublicationConflictError("retained envelope changed resolution")
        if resolution.keep and envelope is None and manifest.value.tier is ResultStorage.INLINE:
            raise OutputOwnerPublicationConflictError("KEEP requires locally retained output bytes")
        with self._lock:
            prior = self._output_loss_receipts.get(manifest.publication_id)
            if prior is not None:
                if prior != resolution:
                    raise OutputOwnerPublicationConflictError("Node-loss owner resolution was rebound")
                return False
            header = manifest.header
            identity = manifest.publication_id
            entry = self._entry(identity.object_id)
            slot = manifest.value
            spec = entry.producer_task_spec
            if (not isinstance(spec, TaskSpec)
                    or tuple(spec.return_ids()) != (identity.object_id,)
                    or spec.job_id != header.job_id or spec.owner_worker_id != header.owner_worker_id):
                raise OutputOwnerPublicationConflictError("Node-loss resolution requires exact producer lineage")
            previous_manifest = self._output_publication_receipts.get(identity)
            if previous_manifest is not None and previous_manifest != manifest:
                raise OutputOwnerPublicationConflictError("Node-loss publication changed its committed manifest")
            self._require_no_output_retirement_locked(entry)
            if (entry.current_attempt != manifest.publication_id.attempt_id
                    or entry.collection_pending or entry.collection_plan is not None
                    or entry.output_retirement_id is not None
                    or (entry.object_id, identity.attempt_id) in self._retired_output_attempts
                    or (identity, entry.object_id) in self._retired_output_slots):
                raise OutputOwnerPublicationConflictError("Node-loss owner slot is fenced")
            membership = entry.output_publication
            if membership is not None and membership != OutputOwnerPublicationMembership(manifest, 0):
                raise OutputOwnerPublicationConflictError("Node-loss slot belongs to another publication")
            if membership is None:
                # An unreceived result may replace only a pristine pending
                # slot. Metadata cleanup is never authority to erase an
                # unrelated partial publication, payload, or child hold.
                if (entry.state is not ObjectState.PENDING or entry.inline_data is not None
                        or entry.error is not None or entry.location_attempts
                        or entry.canonical_stored_result is not None or entry.outgoing_contained_edges):
                    raise OutputOwnerPublicationConflictError("unreceived Node-loss slot contains partial result metadata")
            else:
                if (previous_manifest != manifest or resolution.complete is None
                        or entry.error is not None or entry.outgoing_contained_edges != set(slot.edges)):
                    raise OutputOwnerPublicationConflictError("published Node-loss slot lost its Complete identity")
                if slot.tier is ResultStorage.INLINE:
                    valid = (entry.state is ObjectState.READY_INLINE
                             and entry.inline_data is not None
                             and len(entry.inline_data) == slot.size_bytes
                             and hashlib.sha256(entry.inline_data).hexdigest() == slot.checksum
                             and entry.canonical_stored_result is None and not entry.location_attempts)
                else:
                    expected = ResultDescriptor(identity.object_id, slot.tier, slot.size_bytes,
                        header.owner_worker_id, header.node_incarnation.node_id, slot.checksum)
                    valid = (entry.state in (ObjectState.READY_STORED, ObjectState.LOST)
                             and entry.inline_data is None and entry.canonical_stored_result == expected
                             and all(epoch == identity.attempt_id for epoch in entry.location_attempts.values())
                             and ((entry.state is ObjectState.READY_STORED and bool(entry.location_attempts))
                                  or (entry.state is ObjectState.LOST and not entry.location_attempts)))
                if not valid:
                    raise OutputOwnerPublicationConflictError("published Node-loss slot changed canonical result metadata")
            if resolution.keep and slot.tier is ResultStorage.OBJECT_STORE and membership is None:
                raise OutputOwnerPublicationConflictError("STORED KEEP requires an already-adopted output membership")
            if (not resolution.keep and slot.tier is ResultStorage.OBJECT_STORE
                    and any(node not in unavailable for node in entry.location_attempts)):
                raise OutputOwnerPublicationConflictError(
                    "discard requires surviving replicas to be dropped before owner cleanup"
                )
            entry.error = None
            if resolution.keep:
                if slot.tier is ResultStorage.INLINE:
                    entry.inline_data = envelope.result.inline_data
                    entry.state = ObjectState.READY_INLINE
                    entry.location_attempts = {}
                    entry.canonical_stored_result = None
                else:
                    # Keep canonical identity at the original publisher;
                    # only the current live replica set is a fetch route.
                    entry.location_attempts = {
                        node: epoch for node, epoch in entry.location_attempts.items()
                        if node not in unavailable
                    }
                    entry.inline_data = None
                    entry.state = (ObjectState.READY_STORED if entry.location_attempts
                                   else ObjectState.LOST)
                entry.output_publication = OutputOwnerPublicationMembership(manifest, 0)
                entry.outgoing_contained_edges = set(slot.edges)
            else:
                entry.inline_data = None
                entry.location_attempts = {}
                entry.canonical_stored_result = None
                entry.state = ObjectState.LOST if resolution.complete is not None else ObjectState.PENDING
                entry.output_publication = None
                entry.outgoing_contained_edges.clear()
                self._retired_output_slots.add((manifest.publication_id, identity.object_id))
                if resolution.complete is not None:
                    self._retired_output_attempts.add((identity.object_id, manifest.publication_id.attempt_id))
            if resolution.complete is not None:
                self._output_publication_receipts[manifest.publication_id] = manifest
            self._output_loss_receipts[manifest.publication_id] = resolution
            return True

    def begin_output_publication_retirement(
        self, memberships: tuple[OutputOwnerPublicationMembership, ...], *,
        retirement_id: str, replica_locations: Mapping[ObjectID, tuple[NodeID, ...]],
    ) -> OutputOwnerPublicationRetirementPlan:
        """Freeze exact old LOST slots, without collecting their logical IDs.

        Core supplies all old replica locations, including the publishing Node.
        This inventory is cleanup metadata, not evidence of live replicas or
        completed cleanup. No external effect is performed by this owner CAS.
        """

        values = []
        for member in _sequence(memberships, "memberships"):
            if type(member) is not OutputOwnerPublicationMembership:
                raise TypeError("retirement requires typed output memberships")
            values.append(OutputOwnerPublicationMembership(
                replace(member.manifest), member.slot_index
            ))
        if not isinstance(replica_locations, Mapping):
            raise TypeError("replica_locations must be a mapping")
        if set(replica_locations) != {member.object_id for member in values}:
            raise OutputOwnerRetirementConflictError("replica inventory must cover exactly the retiring output")
        drops = []
        for member in values:
            locations = tuple(
                _opaque(node, NodeID, "replica node_id")
                for node in _sequence(replica_locations[member.object_id], "replica locations")
            )
            if len(locations) != len(set(locations)):
                raise OutputOwnerRetirementConflictError("replica inventory repeats a Node")
            if member.manifest.value.tier is ResultStorage.INLINE and locations:
                raise OutputOwnerRetirementConflictError("inline retirement cannot drop physical replicas")
            drops.extend(DropObjectReplica(
                member.object_id, member.publication_id.attempt_id,
                member.manifest.header.owner_worker_id, node, member.manifest.value.checksum,
            ) for node in sorted(locations))
        plan = OutputOwnerPublicationRetirementPlan(retirement_id, tuple(values), tuple(drops))
        with self._lock:
            previous = self._output_retirement_plans.get(retirement_id)
            if previous is not None:
                if previous != plan:
                    raise OutputOwnerRetirementConflictError("retirement_id changed its exact cleanup plan")
                return replace(previous)
            entries = tuple(self._entry(member.object_id) for member in plan.memberships)
            for entry, member in zip(entries, plan.memberships):
                self._validate_output_retirement_entry_locked(entry, member)
            # Complete all validation/copy allocation before freezing any slot.
            public_plan = replace(plan)
            plans = dict(self._output_retirement_plans)
            plans[retirement_id] = plan
            for entry in entries:
                entry.output_retirement_id = retirement_id
            self._output_retirement_plans = plans
            return public_plan

    def _validate_output_retirement_entry_locked(
        self, entry: _ObjectOwnerEntry, member: OutputOwnerPublicationMembership,
        retirement_id: str | None = None,
    ) -> None:
        if entry.collection_pending:
            raise ObjectCollectionInProgressError("normal collection conflicts with output retirement")
        if entry.output_retirement_id != retirement_id:
            raise OutputOwnerRetirementInProgressError("another retirement owns this output")
        spec = entry.producer_task_spec
        manifest = member.manifest
        if (self._output_publication_receipts.get(member.publication_id) != manifest
                or entry.output_publication != member
                or entry.current_attempt != member.publication_id.attempt_id
                or entry.state is not ObjectState.LOST or entry.location_attempts
                or entry.error is not None or entry.collection_plan is not None
                or entry.outgoing_contained_edges != set(member.manifest.value.edges)
                or not isinstance(spec, TaskSpec)
                or tuple(spec.return_ids()) != (member.publication_id.object_id,)
                or spec.owner_worker_id != manifest.header.owner_worker_id
                or spec.job_id != manifest.header.job_id):
            raise OutputOwnerRetirementConflictError("retirement requires the exact published LOST slot and lineage")
        if member.manifest.value.tier is ResultStorage.OBJECT_STORE:
            expected = ResultDescriptor(
                member.object_id, member.manifest.value.tier, member.manifest.value.size_bytes,
                manifest.header.owner_worker_id, manifest.header.node_incarnation.node_id,
                member.manifest.value.checksum,
            )
            valid = entry.inline_data is None and entry.canonical_stored_result == expected
        else:
            # No fabricated empty payload: a LOST inline slot must have
            # genuinely lost its data and retains only publication metadata.
            valid = entry.inline_data is None and entry.canonical_stored_result is None
        if not valid:
            raise OutputOwnerRetirementConflictError("retirement result identity changed before cleanup")

    def complete_output_publication_retirement(
        self, plan: OutputOwnerPublicationRetirementPlan, *,
        released_edges: tuple[ReleaseContainedReferenceReply | WorkerDeathRecord, ...],
        dropped_replicas: tuple[DropObjectReplicaReply | NodeDeathRecord, ...],
    ) -> OutputOwnerPublicationRetirementReceipt:
        """Clear only retired result effects after every exact cleanup ACK.

        Core must first execute and validate child/replica callbacks and commit
        death proofs. A child death is accepted only when this owner already
        installed the fence for that complete immutable Worker death record.
        It settles the missing child authority, not a fabricated Release ACK.
        For secondary locations whose incarnation is not
        in the publication, Core owns that death-to-replica epoch check. These
        typed proofs establish identity, not independent remote authority here.
        """

        receipt = OutputOwnerPublicationRetirementReceipt(
            plan, released_edges, dropped_replicas,
            OutputOwnerPublicationDisposition.APPLIED,
        )
        plan = receipt.plan
        with self._lock:
            for proof in receipt.released_edges:
                if type(proof) is WorkerDeathRecord:
                    installed = self._dead_worker_cleanups.get(proof.worker_id)
                    if installed is None or installed.death_id != _retirement_child_death_id(proof):
                        raise OutputOwnerRetirementConflictError(
                            "child death must match the installed immutable owner fence"
                        )
            previous = self._output_retirement_receipts.get(plan.retirement_id)
            if previous is not None:
                if previous != receipt:
                    raise OutputOwnerRetirementConflictError("retirement completion changed its exact plan or proofs")
                return replace(previous, disposition=OutputOwnerPublicationDisposition.ALREADY_APPLIED)
            if self._output_retirement_plans.get(plan.retirement_id) != plan:
                raise OutputOwnerRetirementConflictError("retirement was not admitted with this exact cleanup plan")
            entries = tuple(self._entry(member.object_id) for member in plan.memberships)
            for entry, member in zip(entries, plan.memberships):
                self._validate_output_retirement_entry_locked(entry, member, plan.retirement_id)
            public_receipt = replace(receipt)
            retired_slots = self._retired_output_slots | {
                (member.publication_id, member.object_id) for member in plan.memberships
            }
            retired_attempts = self._retired_output_attempts | {
                (member.object_id, member.publication_id.attempt_id) for member in plan.memberships
            }
            receipts = dict(self._output_retirement_receipts)
            receipts[plan.retirement_id] = receipt
            for entry in entries:
                entry.inline_data = None
                entry.error = None
                entry.canonical_stored_result = None
                entry.location_attempts.clear()
                entry.outgoing_contained_edges.clear()
                entry.output_publication = None
                entry.output_retirement_id = None
            self._retired_output_slots = retired_slots
            self._retired_output_attempts = retired_attempts
            self._output_retirement_receipts = receipts
            return public_receipt

    def output_publication_retirement_receipt(
        self, plan: OutputOwnerPublicationRetirementPlan,
    ) -> OutputOwnerPublicationRetirementReceipt | None:
        """Rebuild an isolated terminal ACK from a caller-retained exact plan."""

        if type(plan) is not OutputOwnerPublicationRetirementPlan:
            raise TypeError("plan must be an OutputOwnerPublicationRetirementPlan")
        plan = replace(plan)
        with self._lock:
            previous = self._output_retirement_receipts.get(plan.retirement_id)
            if previous is None:
                return None
            if previous.plan != plan:
                raise OutputOwnerRetirementConflictError("retirement terminal plan identity changed")
            return replace(previous, disposition=OutputOwnerPublicationDisposition.ALREADY_APPLIED)

    def has_active_output_retirements(self) -> bool:
        with self._lock:
            return any(entry.output_retirement_id is not None for entry in self._entries.values())

    def _validate_publish_task_outputs_locked(
        self, plan: TaskOutputPublicationPlan
    ) -> bool:
        entries, task_spec = self._task_output_entries_locked(plan.execution)
        entry = entries[0]
        descriptor = plan.results[0]
        if task_spec is not None and descriptor.owner_worker_id != task_spec.owner_worker_id:
            raise ValueError(
                "result owner must match the registered task output owner"
            )
        if entry.collection_pending:
            raise ObjectCollectionInProgressError(
                "task output metadata collection is pending"
            )
        self._require_no_output_retirements_locked(entries)
        if entry.current_attempt != plan.execution.attempt_id:
            return False
        if descriptor.storage is ResultStorage.INLINE:
            if entry.state is ObjectState.READY_INLINE:
                if entry.inline_data != descriptor.inline_data:
                    raise ConflictingObjectResultError(
                        "attempt published conflicting inline task output"
                    )
                return True
            self._require_publishable(entry)
            return True
        if entry.state is ObjectState.READY_STORED:
            if entry.canonical_stored_result is None:
                raise ConflictingObjectResultError(
                    "stored task output has no canonical replay descriptor"
                )
            if entry.canonical_stored_result != descriptor:
                raise ConflictingObjectResultError(
                    "attempt replay changed stored task output identity "
                    "or integrity metadata"
                )
            if entry.location_attempts.get(descriptor.node_id, _MISSING) != plan.execution.attempt_id:
                raise ConflictingObjectResultError(
                    "task output location has a different attempt epoch"
                )
            return True
        self._require_publishable(entry, allow_lost=True)
        return True

    def validate_publish_task_error(
        self, execution: TaskExecution, error: object
    ) -> TaskOutputErrorPlan | None:
        """Preflight one terminal error for the complete output manifest."""

        plan = TaskOutputErrorPlan(execution, error)
        with self._lock:
            if not self._validate_publish_task_error_locked(plan):
                return None
        return plan

    def commit_publish_task_error(self, plan: TaskOutputErrorPlan) -> bool:
        """Publish the task failure under the owner lock."""

        if not isinstance(plan, TaskOutputErrorPlan):
            raise TypeError("plan must be a TaskOutputErrorPlan")
        with self._lock:
            self._require_no_output_retirements_locked(tuple(
                self._entries[object_id]
                for object_id in (plan.execution.object_id,)
            ))
            if not self._validate_publish_task_error_locked(plan):
                return False
            for object_id in (plan.execution.object_id,):
                entry = self._entries[object_id]
                entry.state = ObjectState.ERROR
                entry.error = plan.error
                entry.inline_data = None
                entry.canonical_stored_result = None
            return True

    def commit_validated_publish_task_error(
        self, plan: TaskOutputErrorPlan
    ) -> None:
        """Apply a caller-held validated error plan without revalidation."""

        self._require_no_output_retirements_locked(tuple(
            self._entries[object_id] for object_id in (plan.execution.object_id,)
        ))
        for object_id in (plan.execution.object_id,):
            entry = self._entries[object_id]
            entry.state = ObjectState.ERROR
            entry.error = plan.error
            entry.inline_data = None
            entry.canonical_stored_result = None

    def publish_task_error(
        self, execution: TaskExecution, error: object
    ) -> bool:
        plan = self.validate_publish_task_error(execution, error)
        return False if plan is None else self.commit_publish_task_error(plan)

    def _validate_publish_task_error_locked(
        self, plan: TaskOutputErrorPlan
    ) -> bool:
        entries, _ = self._task_output_entries_locked(plan.execution)
        entry = entries[0]
        if entry.collection_pending:
            raise ObjectCollectionInProgressError(
                "task output metadata collection is pending"
            )
        self._require_no_output_retirements_locked(entries)
        if entry.current_attempt != plan.execution.attempt_id:
            return False
        if entry.state is ObjectState.ERROR:
            if not _safely_equal(entry.error, plan.error):
                raise ConflictingObjectResultError(
                    "attempt published conflicting task errors"
                )
            return True
        self._require_publishable(entry)
        return True

    def _task_output_entries_locked(
        self, execution: TaskExecution
    ) -> tuple[tuple[_ObjectOwnerEntry, ...], TaskSpec | None]:
        """Bind the single-output operation to its registered lineage."""

        execution = _execution(execution)
        entry = self._entry(execution.object_id)
        entries = (entry,)
        task_spec = entry.producer_task_spec
        if task_spec is None:
            # Actor calls and a few deliberately lineage-free singleton
            # control paths still use the same atomic owner transition.
            return entries, None
        if not isinstance(task_spec, TaskSpec):
            raise InvalidObjectTransitionError(
                "task output requires valid producer lineage"
            )
        if (
            TaskExecution.from_task_spec(task_spec).object_id
            != execution.object_id
        ):
            raise InvalidObjectTransitionError(
                "registered producer lineage changed its output manifest"
            )
        return entries, task_spec

    def validate_publish_error(
        self, object_id: ObjectID, attempt_id: AttemptID | None
    ) -> bool:
        """Side-effect-free fence for a Core cross-authority error commit."""

        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, attempt_id)
            if entry.current_attempt != attempt_id:
                return False
            if entry.state is ObjectState.ERROR:
                return True
            return entry.state is ObjectState.PENDING

    def mark_lost(
        self, object_id: ObjectID, attempt_id: AttemptID | None
    ) -> bool:
        """Mark every stored replica for the current attempt as lost."""

        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            _require_attempt(object_id, attempt_id)
            if entry.current_attempt != attempt_id:
                return False
            if entry.state is ObjectState.LOST:
                return True
            if entry.state is not ObjectState.READY_STORED:
                raise InvalidObjectTransitionError(
                    f"cannot lose {object_id!r} from state {entry.state.value}"
                )
            entry.location_attempts.clear()
            entry.state = ObjectState.LOST
            return True

    def add_reference(
        self, object_id: ObjectID, kind: ReferenceKind | str, token: ReferenceToken
    ) -> bool:
        """Add an idempotency token; return ``True`` only when newly added."""

        resolved_kind = ReferenceKind(kind)
        if resolved_kind in (ReferenceKind.SUBMITTED, ReferenceKind.RETAINED):
            raise OwnershipError(
                "task references require typed TaskReferenceHold protocol"
            )
        if resolved_kind is ReferenceKind.CONTAINED:
            return self.add_contained_reference(object_id, token)
        if resolved_kind is ReferenceKind.LINEAGE:
            return self.add_lineage_reference(object_id, token)
        _require_hashable(token, "reference token")
        with self._lock:
            if (
                resolved_kind is ReferenceKind.BORROWED
                and self._borrower_is_dead_locked(token)
            ):
                raise DeadWorkerReferenceError(
                    "dead Worker cannot add a borrowed reference"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            if (
                resolved_kind is ReferenceKind.BORROWED
                and token in entry.released_borrowed_tokens
            ):
                raise ReleasedBorrowerTokenError(
                    f"borrower token was already released for {object_id!r}"
                )
            tokens = entry.token_set(resolved_kind)
            old_size = len(tokens)
            tokens.add(token)
            return len(tokens) != old_size

    def release_reference(
        self, object_id: ObjectID, kind: ReferenceKind | str, token: ReferenceToken
    ) -> bool:
        """Release a token; duplicate or reordered releases return ``False``."""

        resolved_kind = ReferenceKind(kind)
        if resolved_kind in (ReferenceKind.SUBMITTED, ReferenceKind.RETAINED):
            raise OwnershipError(
                "task references require typed TaskReferenceHold protocol"
            )
        if resolved_kind is ReferenceKind.CONTAINED:
            return self.release_contained_reference(object_id, token)
        if resolved_kind is ReferenceKind.LINEAGE:
            return self.release_lineage_reference(object_id, token)
        _require_hashable(token, "reference token")
        with self._lock:
            entry = self._entry(object_id)
            tokens = entry.token_set(resolved_kind)
            existed = token in tokens
            tokens.discard(token)
            if resolved_kind is ReferenceKind.BORROWED:
                entry.released_borrowed_tokens.add(token)
            return existed

    def add_local_reference(self, object_id: ObjectID, token: ReferenceToken) -> bool:
        return self.add_reference(object_id, ReferenceKind.LOCAL, token)

    def release_local_reference(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        return self.release_reference(object_id, ReferenceKind.LOCAL, token)

    def add_submitted_reference(
        self, object_id: ObjectID, hold: TaskReferenceHold
    ) -> bool:
        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.SUBMITTED
        )
        with self._lock:
            self._require_live_worker_locked(hold.submitting_worker_id)
            if (object_id, hold) in self._released_submitted_tokens:
                raise ReleasedTaskReferenceHoldError(
                    f"submitted task hold was already released for {object_id!r}"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            old_size = len(entry.submitted_tokens)
            entry.submitted_tokens.add(hold)
            return len(entry.submitted_tokens) != old_size

    def release_submitted_reference(
        self, object_id: ObjectID, hold: TaskReferenceHold
    ) -> bool:
        """Release a local Task hold and retire every borrower derived from it.

        The hold and its attempt-local borrowers share this owner lock.  A
        Worker crash can therefore never leave a borrower live after its source
        Task has become terminal, and a delayed Acquire observes the hold
        tombstone rather than resurrecting that borrower.
        """

        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.SUBMITTED
        )
        with self._lock:
            entry = self._entry(object_id)
            existed = hold in entry.submitted_tokens
            entry.submitted_tokens.discard(hold)
            self._released_submitted_tokens.add((object_id, hold))
            # An active explicit release owns the historical all-child
            # cascade.  A late replay after death cleanup must not cascade:
            # death deliberately preserves borrowers owned by live executors.
            if existed:
                self._retire_task_hold_borrowers_locked(
                    object_id,
                    entry,
                    hold,
                )
            return existed

    def add_borrowed_reference(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        _require_hashable(token, "reference token")
        with self._lock:
            if self._borrower_is_dead_locked(token):
                raise DeadWorkerReferenceError(
                    "dead Worker cannot add a borrowed reference"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            if token in entry.released_borrowed_tokens:
                raise ReleasedBorrowerTokenError(
                    f"borrower token was already released for {object_id!r}"
                )
            old_size = len(entry.borrowed_tokens)
            entry.borrowed_tokens.add(token)
            return len(entry.borrowed_tokens) != old_size

    def release_borrowed_reference(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        _require_hashable(token, "reference token")
        with self._lock:
            tombstone = (object_id, token)
            entry = self._entries.get(object_id)
            if entry is None:
                if object_id not in self._collected:
                    raise UnknownObjectError(
                        f"owner does not know object {object_id!r}"
                    )
                self._released_borrowed_tokens.add(tombstone)
                return False
            existed = token in entry.borrowed_tokens
            entry.borrowed_tokens.discard(token)
            # Release-before-acquire is a valid reordered delivery.  Record it
            # even when no active token existed so a late acquire cannot revive
            # an already-dropped Python handle.
            entry.released_borrowed_tokens.add(token)
            self._released_borrowed_tokens.add(tombstone)
            # Keep source bindings only while they are needed for exact active
            # replay/cascade.  The global released-token tombstone is the
            # permanent no-resurrection authority.  Death cleanup is stricter:
            # retain its source binding so a late release cannot erase the
            # immutable evidence captured for that dead incarnation.
            if not self._borrower_is_dead_locked(token):
                entry.borrowed_sources.pop(token, None)
            return existed

    def acquire_exported_reference(
        self,
        object_id: ObjectID,
        source: BorrowSource,
        borrower_token: ReferenceToken,
    ) -> bool:
        """Acquire one borrower from a contained pin or an active Task hold.

        Every borrower is bound to the exact typed source that authorized it, so an
        exact replay is idempotent and the same token cannot be rebound to a
        different lifetime reason.  Task-hold sources are additionally checked
        against the authoritative SUBMITTED/RETAINED hold sets.
        """

        if not isinstance(source, (ContainedTransferSource, TaskHoldSource)):
            raise TypeError(
                "borrow source must be ContainedTransferSource or TaskHoldSource"
            )
        if type(source) is ContainedTransferSource:
            source = ContainedTransferSource(_hold(source.hold))
        _require_hashable(borrower_token, "borrower token")
        with self._lock:
            if self._borrower_is_dead_locked(borrower_token):
                raise DeadWorkerReferenceError(
                    "dead Worker cannot acquire a borrowed reference"
                )
            if (object_id, borrower_token) in self._released_borrowed_tokens:
                raise ReleasedBorrowerTokenError(
                    f"borrower token was already released for {object_id!r}"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            if borrower_token in entry.released_borrowed_tokens:
                raise ReleasedBorrowerTokenError(
                    f"borrower token was already released for {object_id!r}"
                )
            previous_source = entry.borrowed_sources.get(borrower_token)
            if previous_source is not None:
                if previous_source != source:
                    raise ConflictingBorrowerTokenError(
                        f"borrower token names another source for {object_id!r}"
                    )
                # Exact replay of an active acquisition.
                if borrower_token in entry.borrowed_tokens:
                    return False
                raise ReleasedBorrowerTokenError(
                    f"borrower token is no longer active for {object_id!r}"
                )
            self._validate_borrow_source_locked(object_id, entry, source)
            if borrower_token in entry.borrowed_tokens:
                raise ConflictingBorrowerTokenError(
                    f"borrower token lacks an export binding for {object_id!r}"
                )
            entry.borrowed_sources[borrower_token] = source
            old_size = len(entry.borrowed_tokens)
            entry.borrowed_tokens.add(borrower_token)
            return len(entry.borrowed_tokens) != old_size

    def _validate_borrow_source_locked(
        self,
        object_id: ObjectID,
        entry: _ObjectOwnerEntry,
        source: BorrowSource,
    ) -> None:
        if isinstance(source, ContainedTransferSource):
            if source.hold not in entry.contained_holds:
                raise UnknownTransferTokenError(
                    f"owner did not export {object_id!r} with this contained hold"
                )
            return

        hold = source.hold
        released = (
            self._released_submitted_tokens
            if hold.kind is TaskReferenceHoldKind.SUBMITTED
            else self._released_retained_tokens
        )
        if (object_id, hold) in released:
            raise ReleasedTaskReferenceHoldError(
                f"task reference hold was already released for {object_id!r}"
            )
        active = (
            entry.submitted_tokens
            if hold.kind is TaskReferenceHoldKind.SUBMITTED
            else entry.retained_tokens
        )
        if hold not in active:
            raise InactiveTaskReferenceHoldError(
                f"task reference hold is not active for {object_id!r}"
            )

    def _retire_task_hold_borrowers_locked(
        self,
        object_id: ObjectID,
        entry: _ObjectOwnerEntry,
        hold: TaskReferenceHold,
    ) -> None:
        for borrower_token, source in tuple(entry.borrowed_sources.items()):
            if not isinstance(source, TaskHoldSource):
                continue
            if source.hold != hold:
                continue
            entry.borrowed_tokens.discard(borrower_token)
            entry.released_borrowed_tokens.add(borrower_token)
            self._released_borrowed_tokens.add((object_id, borrower_token))
            entry.borrowed_sources.pop(borrower_token, None)

    def has_borrowed_reference(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        _require_hashable(token, "borrower token")
        with self._lock:
            return token in self._entry(object_id).borrowed_tokens

    def retain_borrowed_reference_for_task(
        self,
        object_id: ObjectID,
        borrower_token: ReferenceToken,
        hold: TaskReferenceHold,
    ) -> bool:
        """Create an independent task hold from one active borrower.

        Exact replay does not re-check the parent borrower: after the first
        acknowledgement the Python handle is allowed to close while the task
        hold remains authoritative.
        """

        _require_hashable(borrower_token, "borrower token")
        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        with self._lock:
            self._require_live_worker_locked(hold.submitting_worker_id)
            if self._borrower_is_dead_locked(borrower_token):
                raise DeadWorkerReferenceError(
                    "dead Worker cannot retain a borrowed reference"
                )
            if (object_id, hold) in self._released_retained_tokens:
                raise ReleasedRetainedTokenError(
                    f"retained task hold was already released for {object_id!r}"
                )
            entry = self._entry(object_id)
            if hold in entry.released_retained_tokens:
                raise ReleasedRetainedTokenError(
                    f"retained task hold was already released for {object_id!r}"
                )
            if hold in entry.retained_borrower_tokens:
                previous = entry.retained_borrower_tokens[hold]
                if previous != borrower_token:
                    raise ConflictingRetainedTokenError(
                        f"retained task hold names another borrower for {object_id!r}"
                    )
                return False
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            if borrower_token not in entry.borrowed_tokens:
                raise InactiveBorrowerTokenError(
                    f"parent borrower token is not active for {object_id!r}"
                )
            entry.retained_borrower_tokens[hold] = borrower_token
            entry.retained_tokens.add(hold)
            return True

    def release_retained_reference_for_task(
        self, object_id: ObjectID, hold: TaskReferenceHold
    ) -> bool:
        """Release/tombstone a task hold, including release-before-retain."""

        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        with self._lock:
            tombstone = (object_id, hold)
            entry = self._entries.get(object_id)
            if entry is None:
                existed = False
                self._released_retained_tokens.add(tombstone)
                return existed
            existed = hold in entry.retained_tokens
            entry.retained_tokens.discard(hold)
            entry.released_retained_tokens.add(hold)
            self._released_retained_tokens.add(tombstone)
            if existed:
                self._retire_task_hold_borrowers_locked(
                    object_id,
                    entry,
                    hold,
                )
            return existed

    def replace_retained_reference_for_task(
        self,
        object_id: ObjectID,
        expected_hold: TaskReferenceHold,
        replacement_hold: TaskReferenceHold,
    ) -> RetainedHoldReplacementDisposition:
        """Atomically move one retained lifetime credential forward.

        Return ``REPLACED`` only for the first transition. Exact replay returns
        ``ALREADY_REPLACED`` even after the old hold is tombstoned. The
        tombstone-to-successor binding is permanent so the same old identity
        can never be redirected after an acknowledgement is lost.
        """

        _require_task_reference_hold(
            expected_hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        _require_task_reference_hold(
            replacement_hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        _require_object_id(object_id)
        if (
            expected_hold.submitting_worker_id
            != replacement_hold.submitting_worker_id
            or expected_hold.task_id != replacement_hold.task_id
        ):
            raise ValueError(
                "retained replacement holds must share submitter and task"
            )
        if (
            replacement_hold.origin_attempt_id.attempt_number
            <= expected_hold.origin_attempt_id.attempt_number
        ):
            raise ValueError(
                "retained replacement origin attempt must increase"
            )

        with self._lock:
            key = (object_id, expected_hold)
            previous = self._retained_hold_replacements.get(key)
            if previous is not None:
                if previous != replacement_hold:
                    raise RetainedHoldReplacementConflictError(
                        "retained hold was already replaced by another identity"
                    )
                return RetainedHoldReplacementDisposition.ALREADY_REPLACED
            self._require_live_worker_locked(
                replacement_hold.submitting_worker_id
            )
            if (object_id, replacement_hold) in self._released_retained_tokens:
                raise ReleasedRetainedTokenError(
                    "replacement retained hold was already released"
                )
            if (object_id, expected_hold) in self._released_retained_tokens:
                raise ReleasedRetainedTokenError(
                    "expected retained hold was already released"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            if expected_hold not in entry.retained_tokens:
                raise InactiveTaskReferenceHoldError(
                    "expected retained hold is not active"
                )
            if (
                replacement_hold in entry.retained_tokens
                or replacement_hold in entry.retained_borrower_tokens
            ):
                raise RetainedHoldReplacementConflictError(
                    "replacement retained hold already has another identity"
                )
            if any(
                isinstance(source, TaskHoldSource)
                and source.hold == expected_hold
                and borrower in entry.borrowed_tokens
                for borrower, source in entry.borrowed_sources.items()
            ):
                raise RetainedHoldReplacementBusyError(
                    "old retained hold still has active attempt borrowers"
                )
            parent = entry.retained_borrower_tokens.get(expected_hold)
            if parent is None:
                raise InactiveTaskReferenceHoldError(
                    "expected retained hold has no parent borrower binding"
                )

            # Every remaining operation is assignment/discard under one owner
            # lock.  Publish the immutable replay binding last.
            entry.retained_tokens.remove(expected_hold)
            entry.retained_borrower_tokens.pop(expected_hold, None)
            entry.released_retained_tokens.add(expected_hold)
            self._released_retained_tokens.add(key)
            entry.retained_tokens.add(replacement_hold)
            entry.retained_borrower_tokens[replacement_hold] = parent
            self._retained_hold_replacements[key] = replacement_hold
            return RetainedHoldReplacementDisposition.REPLACED

    def retained_hold_replacement(
        self, object_id: ObjectID, expected_hold: TaskReferenceHold
    ) -> TaskReferenceHold | None:
        """Return the immutable exact-replay successor, if installed."""

        _require_task_reference_hold(
            expected_hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        with self._lock:
            return self._retained_hold_replacements.get(
                (object_id, expected_hold)
            )

    def has_retained_reference_for_task(
        self, object_id: ObjectID, hold: TaskReferenceHold
    ) -> bool:
        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        with self._lock:
            return hold in self._entry(object_id).retained_tokens

    def has_active_retained_references(self) -> bool:
        """Whether any object is kept alive by a submitted foreign task."""

        with self._lock:
            return any(entry.retained_tokens for entry in self._entries.values())

    def has_active_distributed_references(self) -> bool:
        """Whether a peer still owns a borrower, task, or contained hold."""

        with self._lock:
            return any(
                entry.borrowed_tokens
                or entry.retained_tokens
                or entry.contained_holds
                for entry in self._entries.values()
            )

    def retained_reference_is_bound(
        self,
        object_id: ObjectID,
        hold: TaskReferenceHold,
        borrower_token: ReferenceToken,
    ) -> bool:
        """Whether an active hold is the exact replay of its first retain."""

        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        _require_hashable(borrower_token, "borrower token")
        with self._lock:
            entry = self._entry(object_id)
            return (
                hold in entry.retained_tokens
                and entry.retained_borrower_tokens.get(hold)
                == borrower_token
            )

    def retained_release_was_seen(
        self, object_id: ObjectID, hold: TaskReferenceHold
    ) -> bool:
        _require_task_reference_hold(
            hold, expected_kind=TaskReferenceHoldKind.RETAINED
        )
        with self._lock:
            return (object_id, hold) in self._released_retained_tokens

    def add_contained_reference(
        self,
        object_id: ObjectID,
        hold: IncomingContainedReferenceHold,
    ) -> bool:
        """Install one incoming container pin by its complete identity.

        The complete typed hold supplies its container and responsible owner.
        """

        resolved = _contained_reference_hold(hold)
        with self._lock:
            self._require_live_worker_locked(resolved.container_owner_worker_id)
            if (object_id, resolved) in self._released_contained_holds:
                raise ReleasedBorrowerTokenError(
                    f"contained hold was already released for {object_id!r}"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            old_size = len(entry.contained_holds)
            entry.contained_holds.add(resolved)
            return len(entry.contained_holds) != old_size

    def prepare_stored_contained_reference(
        self, transfer: object, *, authority_worker_id: WorkerID
    ) -> StoredContainedReferenceDisposition:
        """Validate a source capability and install its provisional pin."""

        from .publication_sources import (
            BorrowedContainedSource, OwnedContainedSource,
            PreparedContainedTransfer,
        )

        if not isinstance(transfer, PreparedContainedTransfer):
            raise TypeError("transfer must be a PreparedContainedTransfer")
        _require_worker_id(authority_worker_id)
        if authority_worker_id != transfer.contained_owner_worker_id:
            raise ValueError("authority_worker_id must be the child owner")
        object_id = transfer.contained_object_id
        key = (object_id, transfer.provisional_hold)
        with self._lock:
            previous = self._stored_contained_preparations.get(key)
            if previous is not None:
                if previous != transfer:
                    raise InvalidObjectTransitionError(
                        "stored contained preparation conflicts with replay"
                    )
                return StoredContainedReferenceDisposition.ALREADY_PREPARED
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            self._require_live_worker_locked(
                transfer.provisional_hold.container_owner_worker_id
            )
            if (object_id, transfer.provisional_hold) in (
                self._released_contained_holds
            ):
                raise ReleasedBorrowerTokenError(
                    f"contained hold was already released for {object_id!r}"
                )
            if isinstance(transfer.source, OwnedContainedSource):
                if transfer.source.owner_worker_id != authority_worker_id:
                    raise ValueError(
                        "owned source authority must equal the child owner"
                    )
            elif isinstance(transfer.source, BorrowedContainedSource):
                borrower = transfer.source.owner_table_token
                bound = entry.borrowed_sources.get(borrower)
                if (
                    borrower not in entry.borrowed_tokens
                    or bound != transfer.source.original_source
                ):
                    raise ConflictingBorrowerTokenError(
                        "borrowed source does not match its complete live binding"
                    )
            else:  # Defensive even though the transfer validates its source.
                raise TypeError("transfer source is invalid")
            entry.contained_holds.add(transfer.provisional_hold)
            self._stored_contained_preparations[key] = transfer
            return StoredContainedReferenceDisposition.PREPARED

    def promote_stored_contained_reference(
        self, transfer: object, *, authority_worker_id: WorkerID
    ) -> StoredContainedReferenceDisposition:
        """Atomically exchange a prepared executor pin for its final pin."""

        from .publication_sources import PreparedContainedTransfer

        if not isinstance(transfer, PreparedContainedTransfer):
            raise TypeError("transfer must be a PreparedContainedTransfer")
        _require_worker_id(authority_worker_id)
        if authority_worker_id != transfer.contained_owner_worker_id:
            raise ValueError("authority_worker_id must be the child owner")
        object_id = transfer.contained_object_id
        prepare_key = (object_id, transfer.provisional_hold)
        promotion_key = (object_id, transfer.final_hold)
        with self._lock:
            previous = self._stored_contained_promotions.get(promotion_key)
            if previous is not None:
                if previous != transfer:
                    raise InvalidObjectTransitionError(
                        "stored contained promotion conflicts with replay"
                    )
                return StoredContainedReferenceDisposition.ALREADY_PROMOTED
            prepared = self._stored_contained_preparations.get(prepare_key)
            if prepared is None or prepared != transfer:
                raise InvalidObjectTransitionError(
                    "stored contained reference was not exactly prepared"
                )
            entry = self._entry(object_id)
            if transfer.provisional_hold not in entry.contained_holds:
                raise InvalidObjectTransitionError(
                    "provisional contained hold is no longer active"
                )
            self._require_live_worker_locked(
                transfer.final_hold.container_owner_worker_id
            )
            if (object_id, transfer.final_hold) in self._released_contained_holds:
                raise ReleasedBorrowerTokenError(
                    f"contained hold was already released for {object_id!r}"
                )
            if (
                transfer.final_hold in entry.contained_holds
                and transfer.final_hold != transfer.provisional_hold
            ):
                raise InvalidObjectTransitionError(
                    "final contained hold already has another lifecycle"
                )
            # Preflight is complete. The remaining mutation cannot fail and is
            # one custody exchange under the owner lock.
            entry.contained_holds.remove(transfer.provisional_hold)
            self._released_contained_holds.add(prepare_key)
            entry.contained_holds.add(transfer.final_hold)
            self._stored_contained_promotions[promotion_key] = transfer
            return StoredContainedReferenceDisposition.PROMOTED

    def release_contained_reference(
        self,
        object_id: ObjectID,
        hold: IncomingContainedReferenceHold,
    ) -> bool:
        """Release or pre-tombstone one exact incoming container pin."""

        resolved = _contained_reference_hold(hold)
        with self._lock:
            tombstone = (object_id, resolved)
            entry = self._entries.get(object_id)
            if entry is None:
                if tombstone in self._released_contained_holds:
                    return False
                if object_id in self._collected:
                    # This owner has a permanent logical-object tombstone.
                    # Exact release-before-prepare remains safe even when the
                    # ephemeral child was collected before this compensation:
                    # register() cannot recreate that ObjectID afterwards.
                    self._released_contained_holds.add(tombstone)
                    return False
                raise UnknownObjectError(
                    f"owner does not know object {object_id!r}"
                )
            existed = resolved in entry.contained_holds
            entry.contained_holds.discard(resolved)
            # Release-before-add is a valid reordered delivery.  Persist the
            # full identity even when no active hold existed, so a late add
            # cannot revive this container's obligation or alias another one.
            self._released_contained_holds.add(tombstone)
            return existed

    def contained_release_was_seen(
        self,
        object_id: ObjectID,
        hold: IncomingContainedReferenceHold,
    ) -> bool:
        resolved = _contained_reference_hold(hold)
        with self._lock:
            return (object_id, resolved) in self._released_contained_holds

    def add_lineage_reference(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        """Retain dependency metadata for one producer's replayable lineage."""

        _require_hashable(token, "lineage token")
        with self._lock:
            if (object_id, token) in self._released_lineage_tokens:
                raise ReleasedTaskReferenceHoldError(
                    f"lineage token was already released for {object_id!r}"
                )
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            old_size = len(entry.lineage_tokens)
            entry.lineage_tokens.add(token)
            return len(entry.lineage_tokens) != old_size

    def release_lineage_reference(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        """Release/tombstone a lineage hold, including release-before-add."""

        _require_hashable(token, "lineage token")
        with self._lock:
            tombstone = (object_id, token)
            entry = self._entries.get(object_id)
            if entry is None:
                self._released_lineage_tokens.add(tombstone)
                return False
            existed = token in entry.lineage_tokens
            entry.lineage_tokens.discard(token)
            entry.released_lineage_tokens.add(token)
            self._released_lineage_tokens.add(tombstone)
            return existed

    def lineage_release_was_seen(
        self, object_id: ObjectID, token: ReferenceToken
    ) -> bool:
        _require_hashable(token, "lineage token")
        with self._lock:
            return (object_id, token) in self._released_lineage_tokens

    def add_outgoing_lineage_edge(
        self, object_id: ObjectID, edge: LineageReferenceEdge
    ) -> bool:
        """Record dependency release work in its TaskID authority.

        ``object_id`` remains in the API because callers naturally discover the
        edge while registering one producer output.  It selects the task and
        validates the immutable manifest; it is not the storage location of the
        obligation.
        """

        _require_object_id(object_id)
        if not isinstance(edge, LineageReferenceEdge):
            raise TypeError("edge must be a LineageReferenceEdge")
        if edge.producer_object_id != object_id:
            raise ValueError(
                "lineage edge must name the entry as its producer"
            )
        with self._lock:
            entry = self._entry(object_id)
            if entry.collection_pending:
                raise ObjectCollectionInProgressError(
                    f"object metadata collection is pending for {object_id!r}"
                )
            task_spec = entry.producer_task_spec
            if not isinstance(task_spec, TaskSpec):
                # Preserve the narrow pure owner-table seam for lineage-free
                # fixtures: a singleton producer still has an unambiguous
                # TaskID-scoped authority.
                output_ids = (object_id,)
            else:
                output_ids = tuple(task_spec.return_ids())
                if object_id not in output_ids:
                    raise InvalidObjectTransitionError(
                        "producer entry is outside its task output manifest"
                    )
            authority = self._task_lineage.get(object_id.task_id)
            if authority is None:
                authority = _TaskLineageAuthority(
                    object_id.task_id, output_ids
                )
                self._task_lineage[object_id.task_id] = authority
            elif authority.output_ids != output_ids:
                raise InvalidObjectTransitionError(
                    "task lineage output manifest changed"
                )
            obligation = TaskLineageObligation(
                object_id.task_id, edge.dependency_object_id, edge.token
            )
            old_size = len(authority.obligations)
            authority.obligations.add(obligation)
            return len(authority.obligations) != old_size

    def task_lineage_edges(
        self, task_id: TaskID
    ) -> frozenset[LineageReferenceEdge]:
        """Return an immutable diagnostic view of one task obligation."""

        if not isinstance(task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        with self._lock:
            authority = self._task_lineage.get(task_id)
            if authority is None:
                return frozenset()
            anchor = authority.output_ids[0]
            return frozenset(
                LineageReferenceEdge(
                    anchor, obligation.dependency_object_id, obligation.token
                )
                for obligation in authority.obligations
            )


    def _abort_task_lineage_edges_locked(
        self, task_id: TaskID,
        expected_edges: tuple[LineageReferenceEdge, ...],
        *, expected_output_ids: tuple[ObjectID, ...] | None = None,
    ) -> bool:
        authority = self._task_lineage.get(task_id)
        expected = {
            TaskLineageObligation(
                task_id, edge.dependency_object_id, edge.token
            )
            for edge in expected_edges
        }
        if authority is None:
            if expected:
                raise InvalidObjectTransitionError(
                    "task lineage authority is missing during abort"
                )
            return False
        if expected_output_ids is not None and (
            authority.output_ids != tuple(expected_output_ids)
        ):
            raise InvalidObjectTransitionError(
                "task lineage manifest changed during abort"
            )
        if authority.obligations != expected:
            raise InvalidObjectTransitionError(
                "task lineage obligations changed during abort"
            )
        del self._task_lineage[task_id]
        return True



    def is_live(self, object_id: ObjectID) -> bool:
        with self._lock:
            return self._entry(object_id).is_live



    def begin_collection(
        self, object_id: ObjectID, *,
        collection_id: str | None = None,
        canonical_size_bytes: int | None = None,
        canonical_checksum: str | None = None,
    ) -> ObjectMetadataCollectionPlan | None:
        """Atomically freeze all metadata needed by external GC work."""

        with self._lock:
            entry = self._entry(object_id)
            self._require_no_output_retirement_locked(entry)
            if entry.is_live:
                return None
            if entry.state is ObjectState.PENDING:
                return None
            if entry.collection_pending:
                assert entry.collection_plan is not None
                return (deepcopy(entry.collection_plan)
                        if entry.output_publication is not None else entry.collection_plan)
            # Only an exact retired output attempt proves all old child
            # and replica effects gone. Other LOST objects still need
            # canonical integrity metadata; a previous epoch cannot waive it.
            retired_output = (
                entry.state is ObjectState.LOST
                and (entry.object_id, entry.current_attempt) in self._retired_output_attempts
            )
            if retired_output and (
                entry.output_publication is not None or entry.output_retirement_id is not None
                or entry.inline_data is not None or entry.error is not None
                or entry.canonical_stored_result is not None or entry.location_attempts
                or entry.outgoing_contained_edges
            ):
                raise OutputOwnerRetirementConflictError("retired output regained result metadata before collection")
            stored = (
                entry.state is ObjectState.READY_STORED
                or entry.state is ObjectState.LOST and not retired_output
            )
            if stored:
                if not isinstance(entry.current_attempt, AttemptID):
                    raise InvalidObjectTransitionError(
                        f"stored object has no producer attempt: {object_id!r}"
                    )
                if canonical_size_bytes is None or canonical_checksum is None:
                    raise InvalidObjectTransitionError(
                        f"stored collection requires canonical metadata: {object_id!r}"
                    )
            elif canonical_size_bytes is not None or canonical_checksum is not None:
                raise InvalidObjectTransitionError(
                    f"inline/error collection cannot carry stored metadata: {object_id!r}"
                )
            releases = tuple(sorted(entry.outgoing_contained_edges))
            lineage_releases = self._final_task_lineage_locked(object_id)
            plan = ObjectMetadataCollectionPlan(
                object_id=object_id,
                collection_id=collection_id or f"collect:{uuid.uuid4().hex}",
                producer_attempt_id=entry.current_attempt,
                locations=tuple(sorted(entry.location_attempts)),
                producer_task_spec=entry.producer_task_spec,
                canonical_size_bytes=canonical_size_bytes,
                canonical_checksum=canonical_checksum,
                contained_releases=releases,
                # Freeze dependency releases with the same exact GC claim.
                lineage_releases=lineage_releases,
            )
            entry.collection_pending = True
            entry.collection_plan = plan
            return deepcopy(plan) if entry.output_publication is not None else plan

    def begin_output_publication_collection(
        self, object_id: ObjectID, *, collection_id: str,
    ) -> OutputOwnerPublicationCollectionPlan | None:
        """Freeze the output using the existing metadata GC claim.

        Different containers retain separate child holds and release duties.
        Already-collected calls return None; terminal replay needs the caller's
        exact saved plan, because the table intentionally forgot its TaskSpec.
        """

        _require_object_id(object_id)
        if not isinstance(collection_id, str) or not collection_id:
            raise ValueError("collection_id must be a non-empty string")
        with self._lock:
            terminal = self._output_collection_receipts.get(object_id)
            if terminal is not None:
                if terminal.collection_id != collection_id:
                    raise OutputOwnerPublicationConflictError(
                        "output collection identity changed after completion"
                    )
                return None
            entry = self._entry(object_id)
            membership = entry.output_publication
            if membership is None:
                raise OutputOwnerPublicationCollectionRequiredError(
                    "object has no unified output publication membership"
                )
            descriptor = self.output_owner_result(object_id)
            assert descriptor is not None
            slot = membership.manifest.value
            # Check the complete frozen identity before begin_collection makes
            # a COLLECTING claim. An inconsistent slot cannot freeze partial GC.
            if entry.outgoing_contained_edges != set(slot.edges):
                raise OutputOwnerPublicationConflictError(
                    "output slot edges changed before collection"
                )
            metadata = self.begin_collection(
                object_id, collection_id=collection_id,
                canonical_size_bytes=(
                    descriptor.size_bytes
                    if slot.tier is ResultStorage.OBJECT_STORE else None
                ),
                canonical_checksum=(
                    descriptor.checksum
                    if slot.tier is ResultStorage.OBJECT_STORE else None
                ),
            )
            if metadata is None:
                return None
            if metadata.collection_id != collection_id:
                raise OutputOwnerPublicationConflictError(
                    "output collection replay changed collection_id"
                )
            return OutputOwnerPublicationCollectionPlan(membership, metadata)

    def complete_output_publication_collection(
        self, plan: OutputOwnerPublicationCollectionPlan,
    ) -> OutputOwnerPublicationCollectionReceipt:
        """Collect the output after its child releases and replica drops.

        Core performs and validates those effects against the frozen plan
        before this local CAS. This receipt records metadata collection; it
        does not independently prove that remote effects occurred.
        """

        if type(plan) is not OutputOwnerPublicationCollectionPlan:
            raise TypeError("plan must be an OutputOwnerPublicationCollectionPlan")
        plan = replace(plan)
        metadata_digest = _collection_metadata_digest(plan.metadata_plan)
        with self._lock:
            terminal = self._output_collection_receipts.get(plan.object_id)
            if terminal is not None:
                if not terminal.matches(plan):
                    raise OutputOwnerPublicationConflictError(
                        "output collection terminal identity changed"
                    )
                return OutputOwnerPublicationCollectionReceipt(
                    plan, deepcopy(terminal.collection),
                    OutputOwnerPublicationDisposition.ALREADY_APPLIED,
                )
            entry = self._entry(plan.object_id)
            if entry.output_publication != plan.membership:
                raise OutputOwnerPublicationConflictError(
                    "current output does not own this collection membership"
                )
            self.validate_complete_collection(plan.metadata_plan)
            # _complete_collection_locked keeps its ObjectID as a tombstone
            # dictionary key. Do not let the outward receipt's plan own it.
            collection = self._complete_collection_locked(deepcopy(plan.metadata_plan))
            self._output_collection_receipts[deepcopy(plan.object_id)] = _OutputOwnerCollectionTombstone(
                replace(plan.membership.publication_id),
                plan.membership.manifest.manifest_digest,
                plan.membership.slot_index, plan.collection_id,
                metadata_digest, deepcopy(collection),
            )
            return OutputOwnerPublicationCollectionReceipt(
                plan, deepcopy(collection), OutputOwnerPublicationDisposition.APPLIED
            )

    def output_publication_collection_receipt(
        self, plan: OutputOwnerPublicationCollectionPlan,
    ) -> OutputOwnerPublicationCollectionReceipt | None:
        """Rebuild an exact collection ACK without retaining its source bytes."""

        if type(plan) is not OutputOwnerPublicationCollectionPlan:
            raise TypeError("plan must be an OutputOwnerPublicationCollectionPlan")
        plan = replace(plan)
        with self._lock:
            terminal = self._output_collection_receipts.get(plan.object_id)
            if terminal is None:
                return None
            if not terminal.matches(plan):
                raise OutputOwnerPublicationConflictError(
                    "output collection terminal identity changed"
                )
            return OutputOwnerPublicationCollectionReceipt(
                plan, deepcopy(terminal.collection), OutputOwnerPublicationDisposition.ALREADY_APPLIED
            )

    def complete_collection(
        self, plan: ObjectMetadataCollectionPlan
    ) -> ObjectMetadataCollection:
        """CAS COLLECTING -> COLLECTED using the exact frozen plan."""

        self.validate_complete_collection(plan)
        object_id = plan.object_id
        with self._lock:
            entry = self._entry(object_id)
            if entry.output_publication is not None:
                raise OutputOwnerPublicationCollectionRequiredError(
                    "unified output publication requires exact per-slot collection"
                )
            return self._complete_collection_locked(plan)

    def _complete_collection_locked(
        self, plan: ObjectMetadataCollectionPlan,
    ) -> ObjectMetadataCollection:
        # Validation and this commit are normally enclosed by Core's
        # composition lock.  Re-check the immutable owner claim under the
        # table lock so no direct caller can exploit that convention.
        object_id = plan.object_id
        entry = self._entry(object_id)
        if entry.collection_plan != plan or entry.is_live:
            raise InvalidObjectTransitionError(
                f"collection claim changed before commit for {object_id!r}"
            )
        stored_identity = self._stored_collection_identity(entry, plan)
        releases = tuple(sorted(entry.outgoing_contained_edges))
        lineage_releases = self._final_task_lineage_locked(object_id)
        if lineage_releases != plan.lineage_releases:
            raise InvalidObjectTransitionError(
                "task lineage changed after collection freeze"
            )
        self._task_lineage.pop(object_id.task_id, None)
        if stored_identity is not None:
            self._stored_collection_history[stored_identity.object_id] = stored_identity
        del self._entries[object_id]
        collected_id = _object_id(stored_identity.object_id) if stored_identity is not None else object_id
        self._collected[collected_id] = plan.collection_id
        return ObjectMetadataCollection(
            object_id, collected=True, contained_releases=releases,
            lineage_releases=lineage_releases,
        )

    def collection_lineage_releases(
        self, plan: ObjectMetadataCollectionPlan
    ) -> tuple[LineageReferenceEdge, ...]:
        """Preview edges a valid collection commit would claim."""

        self.validate_complete_collection(plan)
        with self._lock:
            if self._final_task_lineage_locked(plan.object_id) != plan.lineage_releases:
                raise InvalidObjectTransitionError(
                    "task lineage changed after collection freeze"
                )
            return plan.lineage_releases

    def _take_final_task_lineage_locked(
        self, object_id: ObjectID
    ) -> tuple[LineageReferenceEdge, ...]:
        """Claim the collected output's producer dependency obligations."""

        releases = self._final_task_lineage_locked(object_id)
        self._task_lineage.pop(object_id.task_id, None)
        return releases

    def _final_task_lineage_locked(
        self, object_id: ObjectID
    ) -> tuple[LineageReferenceEdge, ...]:
        authority = self._task_lineage.get(object_id.task_id)
        if authority is None:
            return ()
        if authority.output_ids != (object_id,):
            raise InvalidObjectTransitionError(
                "collection object is outside task lineage manifest"
            )
        return tuple(sorted(
            LineageReferenceEdge(
                object_id, obligation.dependency_object_id, obligation.token
            )
            for obligation in authority.obligations
        ))

    def validate_complete_collection(
        self, plan: ObjectMetadataCollectionPlan
    ) -> None:
        """Preflight the exact COLLECTING claim without deleting metadata."""

        if not isinstance(plan, ObjectMetadataCollectionPlan):
            raise TypeError("plan must be an ObjectMetadataCollectionPlan")
        object_id = plan.object_id
        with self._lock:
            entry = self._entry(object_id)
            if not entry.collection_pending:
                raise InvalidObjectTransitionError(
                    f"object collection was not begun for {object_id!r}"
                )
            if entry.collection_plan != plan:
                raise InvalidObjectTransitionError(
                    f"collection plan identity changed for {object_id!r}"
                )
            if entry.is_live:
                raise InvalidObjectTransitionError(
                    f"object became live during collection for {object_id!r}"
                )

    @staticmethod
    def _require_no_output_retirement_locked(
        entry: _ObjectOwnerEntry,
    ) -> None:
        # Reuse the existing result/advance/GC fence points. Incoming reference
        # acquisition and release do not use this guard and remain independent.
        if entry.output_retirement_id is not None:
            raise OutputOwnerRetirementInProgressError(
                "output retirement cleanup is still pending for {!r}".format(entry.object_id)
            )

    @classmethod
    def _require_no_output_retirements_locked(
        cls, entries: tuple[_ObjectOwnerEntry, ...],
    ) -> None:
        for entry in entries:
            cls._require_no_output_retirement_locked(entry)

    @staticmethod
    def _require_output_memberships_retired_locked(
        entries: tuple[_ObjectOwnerEntry, ...],
    ) -> None:
        if any(entry.output_publication is not None for entry in entries):
            raise OutputOwnerPublicationCollectionRequiredError(
                "output attempt cannot advance before old publication retirement"
            )

    def _entry(self, object_id: ObjectID) -> _ObjectOwnerEntry:
        _require_object_id(object_id)
        try:
            return self._entries[object_id]
        except KeyError as exc:
            raise UnknownObjectError(f"owner does not know object {object_id!r}") from exc

    def _require_live_worker_locked(self, worker_id: WorkerID) -> None:
        if worker_id in self._dead_worker_cleanups:
            raise DeadWorkerReferenceError(
                "dead Worker cannot add an object reference"
            )

    def _borrower_is_dead_locked(self, token: ReferenceToken) -> bool:
        worker_id = _borrower_worker_id(token)
        return (
            worker_id is not None
            and worker_id in self._dead_worker_cleanups
        )

    def _require_publishable(
        self, entry: _ObjectOwnerEntry, *, allow_lost: bool = False
    ) -> None:
        if (entry.object_id, entry.current_attempt) in self._retired_output_attempts:
            raise InvalidObjectTransitionError("retired output attempt cannot publish a result")
        allowed_states = (
            (ObjectState.PENDING, ObjectState.LOST)
            if allow_lost
            else (ObjectState.PENDING,)
        )
        if entry.state not in allowed_states:
            raise InvalidObjectTransitionError(
                f"cannot publish {entry.object_id!r} from state {entry.state.value}"
            )
        if entry.location_attempts:
            raise InvalidObjectTransitionError(
                f"cannot publish {entry.object_id!r} while old locations remain"
            )


_MISSING = object()


def _validate_output_retirement_proofs(plan, released_edges, dropped_replicas):
    """Normalize typed ACKs against the exact frozen cleanup obligations."""

    replies = _sequence(released_edges, "released_edges")
    expected_releases = plan.contained_releases
    if len(replies) != len(expected_releases):
        raise OutputOwnerRetirementConflictError("retirement requires every child release ACK")
    releases = []
    for reply, expected in zip(replies, expected_releases):
        if type(reply) is WorkerDeathRecord:
            from .death_proofs import owner_death
            reply = owner_death(reply)
            if (reply.worker_id != expected.owner_worker_id or reply.reason not in (
                    WorkerDeathReason.PROCESS_EXIT, WorkerDeathReason.NODE_EXIT)):
                raise OutputOwnerRetirementConflictError(
                    "child death must identify the final hold's confirmed owner exit"
                )
            releases.append(reply)
            continue
        if type(reply) is not ReleaseContainedReferenceReply:
            raise TypeError("child cleanup requires ReleaseContainedReferenceReply or WorkerDeathRecord")
        if type(reply.hold) is not ContainedReferenceHold:
            raise TypeError("child cleanup requires the exact final contained hold")
        reply = replace(reply)
        if (_object_id(reply.object_id) != expected.object_id
                or _opaque(reply.owner_worker_id, WorkerID, "child owner") != expected.owner_worker_id
                or _hold(reply.hold) != expected.hold or reply.accepted is not True
                or type(reply.released) is not bool or reply.error is not None):
            raise OutputOwnerRetirementConflictError("child release must acknowledge the exact final hold")
        releases.append(ReleaseContainedReferenceReply(
            expected.object_id, expected.owner_worker_id, expected.hold, True, reply.released,
        ))
    replicas = _validate_output_retirement_replica_proofs(plan, dropped_replicas)
    return tuple(releases), replicas


def _retirement_child_death_id(death: WorkerDeathRecord) -> str:
    """Bind all proof fields to the fence installed by Core's death consumer."""
    return "worker-death:v1:{}:{}:{}:{}:{}:{}:{}:{}:{}".format(
        death.death_epoch, death.worker_id.hex, death.node_id.hex,
        death.node_pid, death.node_registration_epoch, death.worker_pid,
        death.exit_code, death.reason.value, death.detection_id,
    )


def _validate_output_retirement_replica_proofs(plan, dropped_replicas):
    replies = _sequence(dropped_replicas, "dropped_replicas")
    if len(replies) != len(plan.replica_drops):
        raise OutputOwnerRetirementConflictError("retirement requires every replica cleanup ACK")
    members = {member.object_id: member for member in plan.memberships}
    replicas = []
    for reply, expected in zip(replies, plan.replica_drops):
        if type(reply) is DropObjectReplicaReply:
            reply = replace(reply)
            identity = DropObjectReplica(
                _object_id(reply.object_id), _attempt(reply.producer_attempt_id),
                _opaque(reply.owner_worker_id, WorkerID, "replica owner"),
                _opaque(reply.node_id, NodeID, "replica node_id"),
                _checksum(reply.checksum, "replica checksum"),
            )
            if identity != expected or reply.status not in (
                DropObjectReplicaStatus.DROPPED, DropObjectReplicaStatus.ALREADY_DROPPED,
            ):
                raise OutputOwnerRetirementConflictError("replica ACK must discharge the exact old replica")
            replicas.append(DropObjectReplicaReply(
                expected.object_id, expected.producer_attempt_id,
                expected.owner_worker_id, expected.node_id, expected.checksum, reply.status,
            ))
        elif type(reply) is NodeDeathRecord:
            reply = replace(reply)
            node = _opaque(reply.node_id, NodeID, "dead replica node_id")
            for name in ("node_pid", "registration_epoch", "death_epoch"):
                _uint(getattr(reply, name), name, positive=True)
            if type(reply.exit_code) is not int:
                raise TypeError("node death exit_code must be an int")
            incarnation = members[expected.object_id].manifest.header.node_incarnation
            if (node != expected.node_id or reply.reason is not NodeDeathReason.PROCESS_EXIT
                    or node == incarnation.node_id and (
                        reply.node_pid != incarnation.node_pid
                        or reply.registration_epoch != incarnation.registration_epoch
                    )):
                raise OutputOwnerRetirementConflictError("replica death must match its frozen Node incarnation")
            # Secondary locations have no incarnation in owner metadata. Core
            # must validate their exact committed death before supplying this.
            replicas.append(NodeDeathRecord(
                _string(reply.detection_id, "detection_id"), node, reply.node_pid,
                reply.registration_epoch, reply.death_epoch, reply.exit_code,
                reply.reason, _string(reply.detail, "node death detail"),
            ))
        else:
            raise TypeError("replica cleanup requires DropObjectReplicaReply or NodeDeathRecord")
    return tuple(replicas)


def _collection_metadata_digest(
    plan: ObjectMetadataCollectionPlan,
) -> str:
    """Hash the frozen protocol tree without retaining any payload values.

    A producer TaskSpec can contain argument and function bytes.  Retaining it
    in a terminal collection receipt would therefore defeat metadata GC.  A
    framed, type-tagged digest preserves exact structural replay without
    pickle memo aliases, repr formatting, or an implicit reference to the
    original dataclasses.  Only immutable protocol leaves are supported.
    """

    digest = hashlib.sha256(b"miniray-inline-collection-metadata-v1\0")

    def frame(tag: bytes, payload: bytes) -> None:
        digest.update(tag)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)

    def visit(value: object) -> None:
        if value is None:
            frame(b"n", b"")
        elif isinstance(value, Enum):
            frame(b"e", (
                type(value).__module__ + "." + type(value).__qualname__
            ).encode("utf-8"))
            visit(value.value)
        elif isinstance(value, bool):
            frame(b"b", b"1" if value else b"0")
        elif isinstance(value, int):
            frame(b"i", str(value).encode("ascii"))
        elif isinstance(value, str):
            frame(b"s", value.encode("utf-8"))
        elif isinstance(value, bytes):
            frame(b"p", value)
        elif isinstance(value, tuple):
            frame(b"t", str(len(value)).encode("ascii"))
            for item in value:
                visit(item)
        elif is_dataclass(value) and not isinstance(value, type):
            frame(b"d", (
                type(value).__module__ + "." + type(value).__qualname__
            ).encode("utf-8"))
            members = tuple(item for item in fields(value) if item.compare)
            frame(b"f", str(len(members)).encode("ascii"))
            for item in members:
                frame(b"k", item.name.encode("utf-8"))
                visit(getattr(value, item.name))
        else:
            raise TypeError(
                "collection metadata must contain immutable protocol values"
            )

    visit(plan)
    return digest.hexdigest()


def _require_hashable(value: object, label: str) -> None:
    try:
        hash(value)
    except TypeError as exc:
        raise TypeError(f"{label} must be hashable") from exc


def _same_stored_result_identity(
    left: ResultDescriptor, right: ResultDescriptor,
) -> bool:
    """Compare logical stored-result identity while ignoring replica Node."""

    return bool(
        isinstance(left, ResultDescriptor)
        and isinstance(right, ResultDescriptor)
        and left.object_id == right.object_id
        and left.storage is right.storage is ResultStorage.OBJECT_STORE
        and left.size_bytes == right.size_bytes
        and left.owner_worker_id == right.owner_worker_id
        and left.checksum == right.checksum
        and left.inline_data == right.inline_data is None
    )


def _contained_reference_hold(
    value: IncomingContainedReferenceHold,
) -> IncomingContainedReferenceHold:
    """Revalidate the full container identity; a raw token is insufficient."""

    return _hold(value)


def _require_optional_hashable(value: object | None, label: str) -> None:
    """Validate an optional idempotency token used during registration."""

    if value is not None:
        _require_hashable(value, label)


def _require_task_reference_hold(
    value: object, *, expected_kind: TaskReferenceHoldKind
) -> None:
    """Require the complete typed credential for one Task lifetime hold.

    The kind check belongs at each owner-table boundary.  Merely accepting a
    hashable value (or projecting a hold down to its token) would allow a
    SUBMITTED credential to alias a RETAINED credential and would discard the
    submitter/task binding established by :class:`TaskReferenceHold`.
    """

    if not isinstance(value, TaskReferenceHold):
        raise TypeError("task reference hold must be a TaskReferenceHold")
    if value.kind is not expected_kind:
        raise ValueError(
            "task reference hold kind must be {}".format(expected_kind.value)
        )


def _require_object_id(value: object) -> None:
    if not isinstance(value, ObjectID):
        raise TypeError("object_id must be an ObjectID")


def _require_attempt(
    object_id: ObjectID, value: object | None, *, allow_none: bool = True
) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, AttemptID):
        raise TypeError("attempt_id must be an AttemptID")
    if value.task_id != object_id.task_id:
        raise ValueError("attempt_id must belong to object_id.task_id")


def _require_location(value: object) -> None:
    if not isinstance(value, NodeID):
        raise TypeError("location must be a NodeID")


def _require_worker_id(value: object) -> None:
    if not isinstance(value, WorkerID):
        raise TypeError("worker_id must be a WorkerID")


def _require_death_id(value: object) -> None:
    if not isinstance(value, str) or not value:
        raise TypeError("death_id must be a non-empty string")


def _borrower_worker_id(token: ReferenceToken) -> WorkerID | None:
    """Return the explicit Worker component of Core's borrower identity.

    The second component remains deliberately opaque.  Do not infer Worker
    ownership from strings or other generic reference tokens.
    """

    if (
        isinstance(token, tuple)
        and len(token) == 2
        and isinstance(token[0], WorkerID)
    ):
        return token[0]
    return None


def _safely_equal(left: object, right: object) -> bool:
    try:
        result = left == right
        return result if isinstance(result, bool) else False
    except Exception:
        return left is right
