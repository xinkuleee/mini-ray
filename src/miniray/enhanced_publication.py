"""Single-output GCS publication facts and conservative contained graph.

This pure reducer owns admission, accurate metadata history, and graph
reservations. It owns no READY state, bytes, resource ledger, or child holds.
Its trusted loopback callers supply receipts from real Node/owner transitions;
constructing a receipt is not evidence that those transitions happened.
Membership validation and death cleanup RPCs belong to the GCS adapter.

Forward permission, execution/adoption history, and graph retirement remain
independent. Exact historical replay never reinstalls a retired edge.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields, replace
from enum import Enum
from threading import RLock
from typing import TYPE_CHECKING, TypeAlias

from . import protocol
from .contained_edges import ContainedReferenceHold
from .death_proofs import owner_death
from .ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from .output_publication import (
    OutputPublicationID, OutputPublicationManifest, OutputPublicationHeader,
    OutputValue, OutputPublicationCompleteWitness,
    OutputPublicationNodeIncarnation,
)
from .publication_sources import (
    OwnedContainedSource, BorrowedContainedSource, PreparedContainedTransfer,
    prepared_contained_transfer_fingerprint,
)
from .put_handoff import PutManifest
from .task_outputs import TaskExecution
from .transport import Address

if TYPE_CHECKING:
    from .output_publication_journal import OutputPublicationAdoptionProof


class PublicationErrorKind(str, Enum):
    CONFLICT = "CONFLICT"
    INVALID_STATE = "INVALID_STATE"
    CYCLE = "CYCLE"
    UNAVAILABLE = "UNAVAILABLE"


class PublicationError(ValueError):
    def __init__(self, kind: PublicationErrorKind, message: str):
        self.kind = kind
        super().__init__(message)


def _conflict(message):
    raise PublicationError(PublicationErrorKind.CONFLICT, message)


def _state(message):
    raise PublicationError(PublicationErrorKind.INVALID_STATE, message)


def _text(value, label):
    if type(value) is not str or not value:
        raise TypeError(f"{label} must be a non-empty string")
    return value


def _digest(value):
    if (type(value) is not str or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)):
        raise ValueError("digest must be lower-case SHA256 hex")
    return value


def _address(value):
    if (type(value) is not tuple or len(value) != 2
            or type(value[0]) is not str or not value[0]
            or type(value[1]) is not int or not 0 < value[1] < 65536):
        raise ValueError("owner address must be a bound (host, port) tuple")
    return value


_IDS = (JobID, LeaseID, NodeID, TaskID, WorkerID)
_EXTERNAL = (
    AttemptID, ObjectID, ContainedReferenceHold,
    OutputPublicationID, OutputPublicationManifest, OutputPublicationHeader,
    OutputValue, OutputPublicationCompleteWitness,
    OutputPublicationNodeIncarnation, TaskExecution,
    OwnedContainedSource, BorrowedContainedSource, PreparedContainedTransfer,
    PutManifest, protocol.TaskReferenceHold, protocol.TaskHoldSource,
    protocol.ContainedTransferSource, protocol.PrepareStoredContainedPin,
    protocol.PromoteStoredContainedPin, protocol.StoredContainedPinReply,
    protocol.ReleaseContainedReferenceReply, protocol.SealObjectReply,
    protocol.WorkerIncarnation, protocol.WorkerDeathRecord,
)


def _copy(value, expected=None):
    """Rebuild accepted metadata, including mutated frozen nested values."""
    if expected is not None and type(value) not in (expected if type(expected) is tuple else (expected,)):
        raise TypeError(f"expected {expected}, got {type(value).__name__}")
    kind = type(value)
    if value is None or kind in (str, int, bool):
        return value
    if kind in _IDS:
        return kind(value.value)
    if kind is tuple:
        return tuple(_copy(item) for item in value)
    if isinstance(value, Enum):
        if kind.__module__ not in (__name__, protocol.__name__, "miniray.ownership"):
            raise TypeError("invalid metadata enum")
        return kind(value.value)
    # The journal imports enhanced values only at local export boundaries.
    from .output_publication_journal import OutputPublicationAdoptionProof
    if kind in _WIRE_TYPES:
        # Our constructors rebuild each nested field themselves. Recursively
        # rebuilding here too doubles work at every receipt/snapshot layer.
        return kind(**{item.name: getattr(value, item.name) for item in fields(value)})
    if kind in _EXTERNAL + (OutputPublicationAdoptionProof,):
        return kind(**{item.name: _copy(getattr(value, item.name)) for item in fields(value)})
    raise TypeError(f"unsupported publication metadata: {kind.__name__}")


def _set(value, name, expected):
    object.__setattr__(value, name, _copy(getattr(value, name), expected))


def _tuple(value, expected):
    if type(value) is not tuple:
        raise TypeError("receipt sequence must be a tuple")
    return tuple(_copy(item, expected) for item in value)


def _unique(values, label):
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must not contain duplicates")


def _death(value):
    value = owner_death(_copy(value, protocol.WorkerDeathRecord))
    if value.reason not in (protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT):
        raise ValueError("cleanup requires confirmed owner death")
    return value


class _Wire:
    def __reduce__(self):
        return type(self), tuple(getattr(self, item.name) for item in fields(self))


@dataclass(frozen=True)
class PutPublicationID(_Wire):
    job_id: JobID
    owner_worker_id: WorkerID
    object_id: ObjectID

    def __post_init__(self):
        _set(self, "job_id", JobID)
        _set(self, "owner_worker_id", WorkerID)
        _set(self, "object_id", ObjectID)
        if self.object_id.return_index != 0:
            raise ValueError("put publication requires one object at index zero")

    @property
    def attempt_id(self):
        return AttemptID(self.object_id.task_id, 0)


PublicationKey: TypeAlias = OutputPublicationID | PutPublicationID


@dataclass(frozen=True)
class PublicationRef(_Wire):
    key: PublicationKey
    digest: str

    def __post_init__(self):
        _set(self, "key", (OutputPublicationID, PutPublicationID))
        _digest(self.digest)


@dataclass(frozen=True)
class TaskPublication(_Wire):
    manifest: OutputPublicationManifest
    owner_address: Address

    def __post_init__(self):
        _set(self, "manifest", OutputPublicationManifest)
        object.__setattr__(self, "owner_address", _address(self.owner_address))

    @property
    def reference(self):
        return PublicationRef(self.manifest.publication_id, self.manifest.manifest_digest)

    @property
    def object_id(self):
        return self.manifest.publication_id.object_id

    @property
    def owner_worker_id(self):
        return self.manifest.header.owner_worker_id

    @property
    def job_id(self):
        return self.manifest.header.job_id

    @property
    def transfers(self):
        return self.manifest.value.transfers


@dataclass(frozen=True)
class PutPublication(_Wire):
    job_id: JobID
    owner_address: Address
    manifest: PutManifest

    def __post_init__(self):
        _set(self, "job_id", JobID)
        _set(self, "manifest", PutManifest)
        object.__setattr__(self, "owner_address", _address(self.owner_address))

    @property
    def reference(self):
        digest = hashlib.sha256(b"miniray-enhanced-put-v1\0")
        manifest = self.manifest
        parts = (bytes(self.job_id), bytes(self.owner_worker_id),
                 bytes(self.object_id.task_id), str(self.object_id.return_index).encode(),
                 self.owner_address[0].encode(), str(self.owner_address[1]).encode(),
                 manifest.tier.value.encode(), str(manifest.size_bytes).encode(),
                 manifest.checksum.encode())
        for part in parts + tuple(prepared_contained_transfer_fingerprint(item) for item in self.transfers):
            digest.update(len(part).to_bytes(8, "big"))
            digest.update(part)
        return PublicationRef(PutPublicationID(self.job_id, self.owner_worker_id, self.object_id), digest.hexdigest())

    @property
    def object_id(self):
        return self.manifest.object_id

    @property
    def owner_worker_id(self):
        return self.manifest.owner_worker_id

    @property
    def transfers(self):
        return self.manifest.transfers


Publication: TypeAlias = TaskPublication | PutPublication


@dataclass(frozen=True)
class MaterializationReceipt(_Wire):
    reference: PublicationRef
    node_incarnation: OutputPublicationNodeIncarnation | None

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        if self.node_incarnation is not None:
            _set(self, "node_incarnation", OutputPublicationNodeIncarnation)


def _prepared_fields(value, task):
    _set(value, "reference", PublicationRef)
    if isinstance(value.reference.key, OutputPublicationID) is not task:
        raise ValueError("preparation kind changed publication identity")
    for name, request_type in (("prepare_replies", protocol.PrepareStoredContainedPin),
                               ("promote_replies", protocol.PromoteStoredContainedPin)):
        replies = _tuple(getattr(value, name), protocol.StoredContainedPinReply)
        for reply in replies:
            if (type(reply.request) is not request_type or not reply.accepted
                    or reply.error is not None or reply.error_kind is not None):
                raise ValueError("preparation requires accepted exact child replies")
        _unique(tuple(reply.request for reply in replies), name)
        object.__setattr__(value, name, replies)
    _set(value, "materialization", MaterializationReceipt)
    if value.materialization.reference != value.reference:
        raise ValueError("materialization changed publication reference")


@dataclass(frozen=True)
class TaskPreparedReceipt(_Wire):
    reference: PublicationRef
    prepare_replies: tuple[protocol.StoredContainedPinReply, ...]
    promote_replies: tuple[protocol.StoredContainedPinReply, ...]
    materialization: MaterializationReceipt

    def __post_init__(self):
        _prepared_fields(self, True)
        if self.materialization.node_incarnation is None:
            raise ValueError("Task materialization must name its publishing Node")


@dataclass(frozen=True)
class PutPreparedReceipt(_Wire):
    reference: PublicationRef
    prepare_replies: tuple[protocol.StoredContainedPinReply, ...]
    promote_replies: tuple[protocol.StoredContainedPinReply, ...]
    materialization: MaterializationReceipt
    seal_reply: protocol.SealObjectReply | None

    def __post_init__(self):
        _prepared_fields(self, False)
        if self.seal_reply is not None:
            _set(self, "seal_reply", protocol.SealObjectReply)
            if not self.seal_reply.sealed:
                raise ValueError("put preparation requires successful Seal")


PreparedReceipt: TypeAlias = TaskPreparedReceipt | PutPreparedReceipt


@dataclass(frozen=True)
class OwnerAbortReceipt(_Wire):
    reference: PublicationRef
    owner_worker_id: WorkerID
    abort_id: str

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        _set(self, "owner_worker_id", WorkerID)
        _text(self.abort_id, "abort_id")


class RetirementReason(str, Enum):
    GC = "GC"
    RECONSTRUCTION = "RECONSTRUCTION"
    PAYLOAD_LOST = "PAYLOAD_LOST"


@dataclass(frozen=True)
class OwnerRetirementReceipt(_Wire):
    reference: PublicationRef
    owner_worker_id: WorkerID
    retirement_id: str
    reason: RetirementReason

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        _set(self, "owner_worker_id", WorkerID)
        _text(self.retirement_id, "retirement_id")
        if type(self.reason) is not RetirementReason:
            raise TypeError("reason must be RetirementReason")


FenceProof: TypeAlias = OwnerAbortReceipt | OwnerRetirementReceipt | protocol.WorkerDeathRecord


@dataclass(frozen=True)
class TaskRollbackScope(_Wire):
    reference: PublicationRef
    rollback_id: str
    prepare_intents: tuple[int, ...]
    promote_intents: tuple[int, ...]
    materialization_started: bool

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        if type(self.reference.key) is not OutputPublicationID:
            raise ValueError("Task rollback scope requires a Task publication")
        _text(self.rollback_id, "rollback_id")
        for values in (self.prepare_intents, self.promote_intents):
            if (type(values) is not tuple or any(type(index) is not int or index < 0 for index in values)
                    or tuple(sorted(set(values))) != values):
                raise ValueError("rollback indices must be sorted unique non-negative integers")
        if not set(self.promote_intents).issubset(self.prepare_intents):
            raise ValueError("promotion intent requires corresponding prepare intent")
        if type(self.materialization_started) is not bool:
            raise TypeError("materialization_started must be bool")


@dataclass(frozen=True)
class ClosedContainedHolds(_Wire):
    reference: PublicationRef
    releases: tuple[protocol.ReleaseContainedReferenceReply, ...] = ()
    child_deaths: tuple[protocol.WorkerDeathRecord, ...] = ()
    rollback_scope: TaskRollbackScope | None = None

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        releases = _tuple(self.releases, protocol.ReleaseContainedReferenceReply)
        for reply in releases:
            if reply.accepted is not True or type(reply.released) is not bool or reply.error is not None:
                raise ValueError("closed holds require accepted exact Release replies")
        _unique(tuple((reply.object_id, reply.owner_worker_id, reply.hold) for reply in releases), "releases")
        object.__setattr__(self, "releases", releases)
        deaths = tuple(_death(value) for value in _tuple(self.child_deaths, protocol.WorkerDeathRecord))
        _unique(tuple(death.worker_id for death in deaths), "child deaths")
        object.__setattr__(self, "child_deaths", deaths)
        if self.rollback_scope is not None:
            _set(self, "rollback_scope", TaskRollbackScope)
            if self.rollback_scope.reference != self.reference:
                raise ValueError("rollback scope changed publication reference")


class PublicationStage(str, Enum):
    INTENT = "INTENT"
    PREPARED = "PREPARED"
    ARMED = "ARMED"
    TERMINAL = "TERMINAL"
    COMMITTED = "COMMITTED"
    ADOPTED = "ADOPTED"
    FENCED = "FENCED"
    RETIRED = "RETIRED"


@dataclass(frozen=True)
class PublicationReceipt(_Wire):
    reference: PublicationRef
    stage: PublicationStage
    sequence: int

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        if type(self.stage) is not PublicationStage:
            raise TypeError("stage must be PublicationStage")
        if type(self.sequence) is not int or self.sequence <= 0:
            raise ValueError("receipt sequence must be positive")


@dataclass(frozen=True)
class BeginPublication(_Wire):
    publication: Publication

    def __post_init__(self):
        _set(self, "publication", (TaskPublication, PutPublication))


@dataclass(frozen=True)
class PrepareGraph(_Wire):
    reference: PublicationRef

    def __post_init__(self):
        _set(self, "reference", PublicationRef)


@dataclass(frozen=True)
class ArmTask(_Wire):
    prepared: TaskPreparedReceipt

    def __post_init__(self):
        _set(self, "prepared", TaskPreparedReceipt)


@dataclass(frozen=True)
class RecordTerminal(_Wire):
    complete: OutputPublicationCompleteWitness

    def __post_init__(self):
        _set(self, "complete", OutputPublicationCompleteWitness)


@dataclass(frozen=True)
class CommitGraph(_Wire):
    reference: PublicationRef
    put_prepared: PutPreparedReceipt | None = None

    def __post_init__(self):
        _set(self, "reference", PublicationRef)
        if self.put_prepared is not None:
            _set(self, "put_prepared", PutPreparedReceipt)
            if self.put_prepared.reference != self.reference:
                raise ValueError("put preparation changed commit reference")


@dataclass(frozen=True)
class RecordAdoption(_Wire):
    proof: OutputPublicationAdoptionProof

    def __post_init__(self):
        from .output_publication_journal import OutputPublicationAdoptionProof
        _set(self, "proof", OutputPublicationAdoptionProof)


@dataclass(frozen=True)
class FencePublication(_Wire):
    publication: Publication
    proof: FenceProof

    def __post_init__(self):
        _set(self, "publication", (TaskPublication, PutPublication))
        _set(self, "proof", (OwnerAbortReceipt, OwnerRetirementReceipt, protocol.WorkerDeathRecord))
        _validate_fence(self.publication, self.proof)


@dataclass(frozen=True)
class RetireGraph(_Wire):
    closed_holds: ClosedContainedHolds

    def __post_init__(self):
        _set(self, "closed_holds", ClosedContainedHolds)


@dataclass(frozen=True)
class GetPublication(_Wire):
    reference: PublicationRef

    def __post_init__(self):
        _set(self, "reference", PublicationRef)


PublicationRequest: TypeAlias = (BeginPublication | PrepareGraph | ArmTask | RecordTerminal
                               | CommitGraph | RecordAdoption | FencePublication | RetireGraph | GetPublication)
_REQUEST_TYPES = (BeginPublication, PrepareGraph, ArmTask, RecordTerminal, CommitGraph,
                   RecordAdoption, FencePublication, RetireGraph, GetPublication)
_MUTATION_TYPES = _REQUEST_TYPES[:-1]


def _request_stage(request):
    return {BeginPublication: PublicationStage.INTENT, PrepareGraph: PublicationStage.PREPARED,
            ArmTask: PublicationStage.ARMED, RecordTerminal: PublicationStage.TERMINAL,
            CommitGraph: PublicationStage.COMMITTED, RecordAdoption: PublicationStage.ADOPTED,
            FencePublication: PublicationStage.FENCED, RetireGraph: PublicationStage.RETIRED}[type(request)]


def request_reference(request):
    """Read the one exact reference shared by the request variants."""
    if type(request) in (BeginPublication, FencePublication):
        return request.publication.reference
    if type(request) is ArmTask:
        return request.prepared.reference
    if type(request) is RetireGraph:
        return request.closed_holds.reference
    if type(request) in (RecordTerminal, RecordAdoption):
        complete = request.complete if type(request) is RecordTerminal else request.proof.complete
        return PublicationRef(complete.publication_id, complete.manifest_digest)
    if type(request) in (PrepareGraph, CommitGraph, GetPublication):
        return request.reference
    raise TypeError("unknown publication request")


@dataclass(frozen=True)
class PublicationSnapshot(_Wire):
    publication: Publication
    receipts: tuple[PublicationReceipt, ...]
    fence: FenceProof | None = None
    prepared: PreparedReceipt | None = None
    complete: OutputPublicationCompleteWitness | None = None
    adoption: OutputPublicationAdoptionProof | None = None
    closed_holds: ClosedContainedHolds | None = None

    def __post_init__(self):
        _set(self, "publication", (TaskPublication, PutPublication))
        receipts = _tuple(self.receipts, PublicationReceipt)
        _unique(tuple(receipt.stage for receipt in receipts), "receipt stages")
        if any(receipt.reference != self.reference for receipt in receipts):
            raise ValueError("snapshot receipt changed reference")
        if tuple(sorted(receipts, key=lambda receipt: receipt.sequence)) != receipts:
            raise ValueError("snapshot receipts must follow original acceptance order")
        object.__setattr__(self, "receipts", receipts)
        for name, expected in (("prepared", (TaskPreparedReceipt, PutPreparedReceipt)),
                               ("complete", OutputPublicationCompleteWitness),
                               ("closed_holds", ClosedContainedHolds)):
            if getattr(self, name) is not None:
                _set(self, name, expected)
        if self.adoption is not None:
            from .output_publication_journal import OutputPublicationAdoptionProof
            _set(self, "adoption", OutputPublicationAdoptionProof)
        if self.fence is not None:
            _set(self, "fence", (OwnerAbortReceipt, OwnerRetirementReceipt, protocol.WorkerDeathRecord))
            _validate_fence(self.publication, self.fence)
        _validate_snapshot(self)

    @property
    def reference(self):
        return self.publication.reference

    @property
    def forward_open(self):
        return self.fence is None and self.receipt(PublicationStage.RETIRED) is None

    @property
    def graph_active(self):
        return self.receipt(PublicationStage.PREPARED) is not None and self.receipt(PublicationStage.RETIRED) is None

    def receipt(self, stage):
        return next((receipt for receipt in self.receipts if receipt.stage is stage), None)


@dataclass(frozen=True)
class PublicationReply(_Wire):
    request: PublicationRequest
    accepted: bool
    snapshot: PublicationSnapshot | None = None
    receipt: PublicationReceipt | None = None
    error_kind: PublicationErrorKind | None = None
    error: str | None = None

    def __post_init__(self):
        _set(self, "request", _REQUEST_TYPES)
        if type(self.accepted) is not bool:
            raise TypeError("accepted must be bool")
        if self.snapshot is not None:
            _set(self, "snapshot", PublicationSnapshot)
            if self.snapshot.reference != request_reference(self.request):
                raise ValueError("reply snapshot changed request reference")
        if self.receipt is not None:
            _set(self, "receipt", PublicationReceipt)
            if self.snapshot is None or self.receipt not in self.snapshot.receipts:
                raise ValueError("reply receipt must belong to its exact snapshot")
        if self.accepted:
            if self.error_kind is not None or self.error is not None:
                raise ValueError("accepted publication reply cannot carry an error")
            if type(self.request) is not GetPublication:
                raise ValueError("accepted mutation requires PublicationStageAck")
        elif type(self.error_kind) is not PublicationErrorKind or not self.error or self.receipt is not None:
            raise ValueError("rejected publication reply requires typed error without success receipt")


@dataclass(frozen=True)
class PublicationStageAck(_Wire):
    """One accepted mutation, with exact facts and current closing evidence.

    This projection owns no authority. An old receipt is history, and even
    an open reply cannot replace the caller local epoch/abort check after RPC.
    """
    request: BeginPublication | PrepareGraph | ArmTask | RecordTerminal | CommitGraph | RecordAdoption | FencePublication | RetireGraph
    reference: PublicationRef
    owner_worker_id: WorkerID
    receipt: PublicationReceipt
    accepted_fact: PublicationRef | PreparedReceipt | OutputPublicationCompleteWitness | OutputPublicationAdoptionProof | FenceProof | ClosedContainedHolds
    fence: FenceProof | None = None
    fence_receipt: PublicationReceipt | None = None
    retired_receipt: PublicationReceipt | None = None

    def __post_init__(self):
        from .output_publication_journal import OutputPublicationAdoptionProof
        _set(self, "request", _MUTATION_TYPES)
        _set(self, "reference", PublicationRef)
        _set(self, "owner_worker_id", WorkerID)
        _set(self, "receipt", PublicationReceipt)
        request, reference, stage = self.request, self.reference, _request_stage(self.request)
        if (reference != request_reference(request) or self.receipt.reference != reference
                or self.receipt.stage is not stage):
            raise ValueError("stage ACK changed request reference or stage")
        if type(request) in (BeginPublication, FencePublication):
            if self.owner_worker_id != request.publication.owner_worker_id:
                raise ValueError("stage ACK changed publication owner")
        if type(reference.key) is PutPublicationID and self.owner_worker_id != reference.key.owner_worker_id:
            raise ValueError("stage ACK changed put owner")
        if type(request) in (BeginPublication, PrepareGraph):
            expected = PublicationRef
            actual = reference
        elif type(request) is ArmTask:
            expected, actual = TaskPreparedReceipt, request.prepared
        elif type(request) is RecordTerminal:
            expected, actual = OutputPublicationCompleteWitness, request.complete
        elif type(request) is CommitGraph:
            if type(reference.key) is PutPublicationID:
                if request.put_prepared is None:
                    raise ValueError("put commit ACK requires its actual preparation")
                expected, actual = PutPreparedReceipt, request.put_prepared
            else:
                if request.put_prepared is not None:
                    raise ValueError("Task commit ACK cannot contain put preparation")
                expected, actual = OutputPublicationCompleteWitness, None
        elif type(request) is RecordAdoption:
            expected, actual = OutputPublicationAdoptionProof, request.proof
            if request.proof.owner_worker_id != self.owner_worker_id:
                raise ValueError("stage ACK changed adoption owner")
        elif type(request) is FencePublication:
            expected, actual = (OwnerAbortReceipt, OwnerRetirementReceipt, protocol.WorkerDeathRecord), None
        else:
            expected, actual = ClosedContainedHolds, request.closed_holds
        _set(self, "accepted_fact", expected)
        fact = self.accepted_fact
        if actual is not None and fact != actual:
            raise ValueError("stage ACK changed its accepted fact")
        if type(fact) is OutputPublicationCompleteWitness:
            if PublicationRef(fact.publication_id, fact.manifest_digest) != reference:
                raise ValueError("stage ACK Complete changed reference")
        if type(fact) in (TaskPreparedReceipt, PutPreparedReceipt):
            for reply in fact.prepare_replies + fact.promote_replies:
                hold = reply.request.transfer.final_hold
                if (hold.container_object_id != reference.key.object_id
                        or hold.container_owner_worker_id != self.owner_worker_id):
                    raise ValueError("stage ACK preparation changed its container owner")
        if (self.fence is None) != (self.fence_receipt is None):
            raise ValueError("stage ACK fence and receipt must agree")
        if self.fence is not None:
            _set(self, "fence", (OwnerAbortReceipt, OwnerRetirementReceipt, protocol.WorkerDeathRecord))
            _set(self, "fence_receipt", PublicationReceipt)
            if (self.fence_receipt.reference != reference
                    or self.fence_receipt.stage is not PublicationStage.FENCED):
                raise ValueError("stage ACK changed fence receipt")
            if type(self.fence) is protocol.WorkerDeathRecord:
                if _death(self.fence).worker_id != self.owner_worker_id:
                    raise ValueError("stage ACK death changed owner")
            elif self.fence.reference != reference or self.fence.owner_worker_id != self.owner_worker_id:
                raise ValueError("stage ACK fence changed owner or reference")
        if self.retired_receipt is not None:
            _set(self, "retired_receipt", PublicationReceipt)
            if (self.retired_receipt.reference != reference
                    or self.retired_receipt.stage is not PublicationStage.RETIRED
                    or self.fence_receipt is None
                    or self.retired_receipt.sequence <= self.fence_receipt.sequence):
                raise ValueError("stage ACK retirement changed closing history")
        sequences = {}
        for receipt in (self.receipt, self.fence_receipt, self.retired_receipt):
            if receipt is not None and sequences.setdefault(receipt.sequence, receipt.stage) is not receipt.stage:
                raise ValueError("stage ACK distinct stages reused an acceptance sequence")
        if stage in (PublicationStage.INTENT, PublicationStage.PREPARED, PublicationStage.ARMED):
            if not self.forward_open:
                raise ValueError("forward mutation ACK cannot be closed historical replay")
        if stage is PublicationStage.COMMITTED and self.fence_receipt is not None:
            if self.receipt.sequence >= self.fence_receipt.sequence:
                raise ValueError("first graph commit must precede its forward fence")
        if stage is PublicationStage.FENCED:
            if self.receipt != self.fence_receipt or fact != self.fence:
                raise ValueError("fence ACK must name the actual first fence")
            if fact != request.proof:
                # Only the controller accepts a later committed owner death
                # while retaining a different, already accepted first fence.
                if (type(request.proof) is not protocol.WorkerDeathRecord
                        or _death(request.proof).worker_id != self.owner_worker_id):
                    raise ValueError("fence ACK rebound a requested owner decision")
        if stage is PublicationStage.RETIRED and self.receipt != self.retired_receipt:
            raise ValueError("retirement ACK must name its exact first receipt")

    @property
    def accepted(self):
        return True

    @property
    def error(self):
        return None

    @property
    def error_kind(self):
        return None

    @property
    def forward_open(self):
        return self.fence is None and self.retired_receipt is None


def stage_ack_from_snapshot(request, snapshot, receipt, *, accepted_existing_fence=False):
    """Project already validated authority state while its owning lock is held.

    The controller alone selects the later-owner-death exception after exact
    registry validation. No projected value can commit or replace that state.
    """
    if type(request) not in _MUTATION_TYPES or type(snapshot) is not PublicationSnapshot:
        raise TypeError("stage ACK requires a mutation and canonical snapshot")
    if type(receipt) is not PublicationReceipt or receipt != snapshot.receipt(_request_stage(request)):
        raise ValueError("stage ACK receipt is not the accepted authority stage")
    if request_reference(request) != snapshot.reference:
        raise ValueError("stage ACK request changed authority reference")
    if type(request) in (BeginPublication, FencePublication) and request.publication != snapshot.publication:
        raise ValueError("stage ACK request changed complete publication or owner route")
    if accepted_existing_fence:
        if (type(request) is not FencePublication or type(request.proof) is not protocol.WorkerDeathRecord
                or snapshot.fence is None or _death(request.proof).worker_id != snapshot.publication.owner_worker_id):
            raise ValueError("existing-fence ACK requires a validated later owner death")
    if type(request) in (BeginPublication, PrepareGraph):
        fact = snapshot.reference
    elif type(request) is ArmTask:
        fact = snapshot.prepared
    elif type(request) is RecordTerminal:
        fact = snapshot.complete
    elif type(request) is CommitGraph:
        fact = snapshot.prepared if type(snapshot.publication) is PutPublication else snapshot.complete
    elif type(request) is RecordAdoption:
        fact = snapshot.adoption
    elif type(request) is FencePublication:
        fact = snapshot.fence
        if fact != request.proof and not accepted_existing_fence:
            raise ValueError("stage ACK cannot replace the requested first fence")
    else:
        fact = snapshot.closed_holds
    return PublicationStageAck(request, snapshot.reference, snapshot.publication.owner_worker_id, receipt, fact,
        snapshot.fence, snapshot.receipt(PublicationStage.FENCED), snapshot.receipt(PublicationStage.RETIRED))


PUBLICATION_HANDLER = "enhanced_publication"


@dataclass(frozen=True)
class AbortOwnerPublication(_Wire):
    publication: TaskPublication
    rollback: TaskRollbackScope

    def __post_init__(self):
        _set(self, "publication", TaskPublication)
        _set(self, "rollback", TaskRollbackScope)
        if self.publication.reference != self.rollback.reference:
            raise ValueError("owner abort changed rollback reference")
        _validate_scope(self.publication, self.rollback)


@dataclass(frozen=True)
class AbortOwnerPublicationReply(_Wire):
    request: AbortOwnerPublication
    accepted: bool
    receipt: OwnerAbortReceipt | None = None
    adoption: OutputPublicationAdoptionProof | None = None
    error: str | None = None

    def __post_init__(self):
        _set(self, "request", AbortOwnerPublication)
        if type(self.accepted) is not bool:
            raise TypeError("owner abort acceptance must be bool")
        if self.receipt is not None:
            _set(self, "receipt", OwnerAbortReceipt)
            _validate_fence(self.request.publication, self.receipt)
        if self.adoption is not None:
            from .output_publication_journal import OutputPublicationAdoptionProof
            _set(self, "adoption", OutputPublicationAdoptionProof)
            if (self.adoption.complete.publication_id != self.request.publication.reference.key
                    or self.adoption.complete.manifest_digest != self.request.publication.reference.digest
                    or self.adoption.owner_worker_id != self.request.publication.owner_worker_id):
                raise ValueError("owner abort response changed adoption identity")
        if self.accepted:
            if self.receipt is None or self.adoption is not None or self.error is not None:
                raise ValueError("accepted abort needs only its actual local receipt")
        elif self.receipt is not None or (self.adoption is None and not self.error):
            raise ValueError("rejected abort needs adoption history or an error")


ABORT_OWNER_PUBLICATION_HANDLER = "abort_owner_publication"


def _validate_fence(publication, proof):
    if type(proof) is protocol.WorkerDeathRecord:
        if _death(proof).worker_id != publication.owner_worker_id:
            raise ValueError("death fence names another publication owner")
    elif proof.reference != publication.reference or proof.owner_worker_id != publication.owner_worker_id:
        raise ValueError("owner fence changed publication identity")


def _validate_prepared(publication, prepared):
    if prepared.reference != publication.reference:
        _conflict("preparation changed publication reference")
    task = type(publication) is TaskPublication
    if type(prepared) is not (TaskPreparedReceipt if task else PutPreparedReceipt):
        _conflict("preparation kind changed")
    transfers = publication.transfers
    for replies, request_type in ((prepared.prepare_replies, protocol.PrepareStoredContainedPin),
                                  (prepared.promote_replies, protocol.PromoteStoredContainedPin)):
        expected = tuple(request_type(transfer, transfer.contained_owner_worker_id) for transfer in transfers)
        if tuple(reply.request for reply in replies) != expected:
            _conflict("preparation must cover every exact manifest child in order")
    node = prepared.materialization.node_incarnation
    if task:
        if node != publication.manifest.header.node_incarnation:
            _conflict("Task materialization changed publishing Node incarnation")
    else:
        manifest = publication.manifest
        if manifest.tier is protocol.ResultStorage.INLINE:
            if node is not None or prepared.seal_reply is not None:
                _conflict("INLINE put materialization belongs to its owner")
        else:
            reply = prepared.seal_reply
            if (node is None or reply is None or not reply.sealed
                    or (reply.object_id, reply.node_id, reply.size_bytes, reply.checksum)
                    != (publication.object_id, node.node_id, manifest.size_bytes, manifest.checksum)):
                _conflict("STORED put preparation requires its exact successful Seal")


def _validate_scope(publication, scope):
    if type(publication) is not TaskPublication or scope.reference != publication.reference:
        _conflict("rollback scope changed Task publication")
    if any(index >= len(publication.transfers) for index in scope.prepare_intents + scope.promote_intents):
        _conflict("rollback scope names an absent child")


def _validate_closed(snapshot, closed):
    publication = snapshot.publication
    if closed.reference != snapshot.reference:
        _conflict("hold cleanup changed publication identity")
    scope = closed.rollback_scope
    if scope is not None:
        _validate_scope(publication, scope)
        if snapshot.complete is not None or snapshot.adoption is not None:
            _state("successful Complete cannot have a rollback scope")
        if snapshot.prepared is not None and (
                scope.prepare_intents != tuple(range(len(publication.transfers)))
                or scope.promote_intents != tuple(range(len(publication.transfers)))
                or not scope.materialization_started):
            _conflict("rollback scope contradicts recorded complete preparation")
        prepared_indices, promoted_indices = set(scope.prepare_intents), set(scope.promote_intents)
    else:
        prepared_indices = promoted_indices = set(range(len(publication.transfers)))
    all_holds, required = set(), set()
    for index, transfer in enumerate(publication.transfers):
        provisional = (transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.provisional_hold)
        final = (transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold)
        all_holds.update((provisional, final))
        if index in prepared_indices:
            required.add(provisional)
        if index in promoted_indices:
            required.add(final)
        if snapshot.prepared is not None:
            # Actual recorded promotion atomically tombstoned provisional hold.
            required.discard(provisional)
    released = {(reply.object_id, reply.owner_worker_id, reply.hold) for reply in closed.releases}
    if not released.issubset(all_holds):
        _conflict("cleanup includes a hold outside its manifest")
    owners = {transfer.contained_owner_worker_id for transfer in publication.transfers}
    dead = {death.worker_id for death in closed.child_deaths}
    if not dead.issubset(owners):
        _conflict("cleanup death names no manifest child owner")
    if any(hold not in released and hold[1] not in dead for hold in required):
        _state("graph retirement requires exact settlement of every possible child hold")


def _validate_snapshot(snapshot):
    has = lambda stage: snapshot.receipt(stage) is not None
    S = PublicationStage
    if (snapshot.fence is not None) != has(S.FENCED):
        raise ValueError("fence fact and receipt must agree")
    if (snapshot.complete is not None) != has(S.TERMINAL):
        raise ValueError("Complete fact and terminal receipt must agree")
    if (snapshot.adoption is not None) != has(S.ADOPTED):
        raise ValueError("adoption fact and receipt must agree")
    if (snapshot.closed_holds is not None) != has(S.RETIRED):
        raise ValueError("closed holds and retired receipt must agree")
    task = type(snapshot.publication) is TaskPublication
    if has(S.PREPARED) and not has(S.INTENT):
        raise ValueError("graph reservation requires INTENT history")
    if has(S.ARMED) and (not task or not has(S.PREPARED) or snapshot.prepared is None):
        raise ValueError("ARM requires Task graph and preparation history")
    if snapshot.prepared is not None:
        _validate_prepared(snapshot.publication, snapshot.prepared)
        if not has(S.ARMED if task else S.COMMITTED):
            raise ValueError("preparation has no matching acceptance receipt")
    if snapshot.complete is not None:
        complete = snapshot.complete
        if (not task or not has(S.ARMED)
                or (complete.publication_id, complete.manifest_digest) != (snapshot.reference.key, snapshot.reference.digest)):
            raise ValueError("terminal fact changed armed Task publication")
    if has(S.COMMITTED) and (not has(S.PREPARED) or (task and snapshot.complete is None)
                            or (not task and snapshot.prepared is None)):
        raise ValueError("graph commit lacks exact preparation or Complete")
    if snapshot.adoption is not None and (not has(S.COMMITTED)
            or snapshot.adoption.complete != snapshot.complete
            or snapshot.adoption.owner_worker_id != snapshot.publication.owner_worker_id):
        raise ValueError("adoption changed committed owner or Complete")
    if snapshot.closed_holds is not None:
        if snapshot.fence is None:
            raise ValueError("retirement requires a forward fence")
        _validate_closed(snapshot, snapshot.closed_holds)
    # Facts can arrive after fencing, but causal stages cannot precede their
    # prerequisites. Original acceptance sequences remain useful after GC.
    for after, before in ((S.PREPARED, S.INTENT), (S.ARMED, S.PREPARED),
                          (S.TERMINAL, S.ARMED), (S.COMMITTED, S.PREPARED),
                          (S.ADOPTED, S.COMMITTED), (S.RETIRED, S.FENCED)):
        if has(after) and snapshot.receipt(after).sequence <= snapshot.receipt(before).sequence:
            raise ValueError("publication receipts violate causal acceptance order")
    if task and has(S.COMMITTED) and snapshot.receipt(S.COMMITTED).sequence <= snapshot.receipt(S.TERMINAL).sequence:
        raise ValueError("Task graph commit must follow actual terminal acceptance")


class PublicationAuthority:
    """One in-memory authority for publication facts and active graph edges.

    GCS's composition lock surrounds membership checks and these operations.
    The internal lock makes standalone reducer calls linearizable; no callback
    or RPC executes here. The record map is history, not object visibility.
    """

    def __init__(self):
        self._records: dict[PublicationKey, PublicationSnapshot] = {}
        self._sequence = 0
        self._lock = RLock()

    def apply(self, request):
        request = _copy(request, _REQUEST_TYPES)
        reference = request_reference(request)
        with self._lock:
            try:
                return self._apply_locked(request)
            except PublicationError as exc:
                snapshot = self._records.get(reference.key)
                if snapshot is not None and snapshot.reference != reference:
                    snapshot = None
                return PublicationReply(request, False, snapshot, error_kind=exc.kind, error=str(exc))

    def _apply_locked(self, request):
        reference = request_reference(request)
        previous = self._records.get(reference.key)
        if previous is not None and previous.reference != reference:
            _conflict("publication key was rebound to another digest")
        if type(request) is GetPublication:
            return PublicationReply(request, True, previous)
        if type(request) in (BeginPublication, FencePublication):
            publication = request.publication
            if previous is not None and previous.publication != publication:
                _conflict("publication request changed its complete manifest or owner route")
            if previous is None:
                self._validate_binding(publication, admitting=type(request) is BeginPublication)
                previous = PublicationSnapshot(publication, ())
        if previous is None:
            _state("publication has not been registered")
        S = PublicationStage
        stage = _request_stage(request)
        changes = {}
        receipt = previous.receipt(stage)
        if type(request) is BeginPublication:
            self._require_forward(previous)
        elif type(request) is PrepareGraph:
            self._require_forward(previous)
            self._require_stage(previous, S.INTENT)
            if receipt is None and self._would_cycle(previous.publication):
                raise PublicationError(PublicationErrorKind.CYCLE, "contained ObjectID edges would form a cycle")
        elif type(request) is ArmTask:
            _validate_prepared(previous.publication, request.prepared)
            if previous.prepared is not None and previous.prepared != request.prepared:
                _conflict("preparation receipt was rebound")
            self._require_forward(previous)
            self._require_stage(previous, S.PREPARED)
            changes["prepared"] = request.prepared
        elif type(request) is RecordTerminal:
            if type(previous.publication) is not TaskPublication:
                _state("put has no Task Complete")
            self._require_stage(previous, S.ARMED)
            if previous.complete is not None and previous.complete != request.complete:
                _conflict("Complete history was rebound")
            # Late accurate success is history, even after forward fence.
            if previous.closed_holds is not None and previous.closed_holds.rollback_scope is not None:
                _conflict("Complete contradicts the actual rollback decision")
            changes["complete"] = request.complete
        elif type(request) is CommitGraph:
            if type(previous.publication) is PutPublication:
                if request.put_prepared is None:
                    _state("put graph commit requires actual preparation")
                _validate_prepared(previous.publication, request.put_prepared)
                if previous.prepared is not None and previous.prepared != request.put_prepared:
                    _conflict("put preparation receipt was rebound")
                changes["prepared"] = request.put_prepared
            elif request.put_prepared is not None:
                _conflict("Task commit cannot carry put preparation")
            else:
                self._require_stage(previous, S.TERMINAL)
            self._require_stage(previous, S.PREPARED)
            if receipt is None:
                self._require_forward(previous)
        elif type(request) is RecordAdoption:
            self._require_stage(previous, S.COMMITTED)
            if (request.proof.complete != previous.complete
                    or request.proof.owner_worker_id != previous.publication.owner_worker_id):
                _conflict("adoption changed exact owner or Complete")
            if previous.adoption is not None and previous.adoption != request.proof:
                _conflict("adoption history was rebound")
            if receipt is None and type(previous.fence) is OwnerAbortReceipt:
                _state("an owner abort cannot have an adoption")
            # Genuine C6 may precede a delayed C7/death/retirement. Preserve
            # that history without granting any current forward permission.
            changes["adoption"] = request.proof
        elif type(request) is FencePublication:
            if previous.fence is not None and previous.fence != request.proof:
                _conflict("first forward fence cannot be rebound")
            if type(request.proof) is OwnerAbortReceipt and previous.adoption is not None:
                _state("adopted output cannot be aborted")
            changes["fence"] = request.proof
        else:
            if previous.fence is None:
                _state("graph retirement requires an owner/death forward fence")
            _validate_closed(previous, request.closed_holds)
            if previous.closed_holds is not None and previous.closed_holds != request.closed_holds:
                _conflict("retirement cleanup receipt was rebound")
            changes["closed_holds"] = request.closed_holds
        if receipt is not None:
            return stage_ack_from_snapshot(request, previous, receipt)
        next_sequence = self._sequence + 1
        receipt = PublicationReceipt(reference, stage, next_sequence)
        updated = replace(previous, receipts=previous.receipts + (receipt,), **changes)
        # Reply validation/copy is fallible; prepare it before authority commit.
        reply = stage_ack_from_snapshot(request, updated, receipt)
        self._records[reference.key] = updated
        self._sequence = next_sequence
        return reply

    def _validate_binding(self, publication, *, admitting):
        for previous in self._records.values():
            other = previous.publication
            if other.job_id != publication.job_id:
                _conflict("the teaching GCS supports one publication job")
            if other.owner_worker_id == publication.owner_worker_id and other.owner_address != publication.owner_address:
                _conflict("owner incarnation was rebound to another route")
            if other.object_id == publication.object_id:
                if other.owner_worker_id != publication.owner_worker_id:
                    _conflict("one ObjectID cannot name two owners")
                # An exact pre-Begin fence is cleanup history, not execution
                # admission. It must be recordable beside a live successor,
                # and must not itself claim that an object epoch was started.
                if not admitting or previous.receipt(PublicationStage.INTENT) is None:
                    continue
                if previous.receipt(PublicationStage.RETIRED) is None:
                    _state("old publication must retire before the next object epoch")
                if publication.reference.key.attempt_id.attempt_number <= other.reference.key.attempt_id.attempt_number:
                    _state("retired object publication requires a newer attempt")
        owners = {}
        for previous in tuple(self._records.values()) + (PublicationSnapshot(publication, ()),):
            item = previous.publication
            entries = ((item.object_id, item.owner_worker_id),) + tuple(
                (transfer.contained_object_id, transfer.contained_owner_worker_id) for transfer in item.transfers)
            for object_id, owner in entries:
                if owners.setdefault(object_id, owner) != owner:
                    _conflict("contained ObjectID owner credentials conflict")

    def _would_cycle(self, publication):
        adjacency = {}
        for item in [record.publication for record in self._records.values() if record.graph_active] + [publication]:
            adjacency.setdefault(item.object_id, set()).update(transfer.contained_object_id for transfer in item.transfers)
        # Iterative DFS keeps graph size independent from Python call depth.
        done, active = set(), set()
        for vertex in adjacency:
            if vertex in done:
                continue
            stack = [(vertex, False)]
            while stack:
                node, leaving = stack.pop()
                if leaving:
                    active.discard(node)
                    done.add(node)
                elif node in active:
                    return True
                elif node not in done:
                    active.add(node)
                    stack.append((node, True))
                    stack.extend((child, False) for child in adjacency.get(node, ()))
        return False

    @staticmethod
    def _require_forward(snapshot):
        if not snapshot.forward_open:
            _state("publication forward permission is fenced")

    @staticmethod
    def _require_stage(snapshot, stage):
        if snapshot.receipt(stage) is None:
            _state(f"publication requires {stage.value} history")

    def snapshots(self):
        with self._lock:
            return tuple(_copy(snapshot) for snapshot in self._records.values())

    def for_owner(self, owner_worker_id):
        owner_worker_id = _copy(owner_worker_id, WorkerID)
        return tuple(snapshot for snapshot in self.snapshots() if snapshot.publication.owner_worker_id == owner_worker_id)

    def begin(self, request): return self._typed(request, BeginPublication)
    def prepare(self, request): return self._typed(request, PrepareGraph)
    def arm(self, request): return self._typed(request, ArmTask)
    def terminal(self, request): return self._typed(request, RecordTerminal)
    def commit(self, request): return self._typed(request, CommitGraph)
    def adopt(self, request): return self._typed(request, RecordAdoption)
    def fence(self, request): return self._typed(request, FencePublication)
    def retire(self, request): return self._typed(request, RetireGraph)
    def query(self, request): return self._typed(request, GetPublication)

    def _typed(self, request, expected):
        if type(request) is not expected:
            raise TypeError(f"expected {expected.__name__}")
        return self.apply(request)


_WIRE_TYPES = tuple(value for value in tuple(globals().values())
                    if isinstance(value, type) and issubclass(value, _Wire) and value is not _Wire)
__all__ = [value.__name__ for value in _WIRE_TYPES] + [
    "PublicationKey", "Publication", "PreparedReceipt", "FenceProof", "PublicationRequest",
    "PublicationStage", "RetirementReason", "PublicationErrorKind", "PublicationError",
    "PublicationAuthority", "PUBLICATION_HANDLER", "ABORT_OWNER_PUBLICATION_HANDLER", "request_reference",
    "stage_ack_from_snapshot",
]
