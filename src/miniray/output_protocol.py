"""Typed wire boundaries for the single selected-output publication.

Only PrepareOutputPublication carries serialized output bytes.  Its reply and
all recovery/retirement messages carry exact metadata, never a result cache.
Contained graph operations reuse the messages in ``protocol``.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from enum import Enum
import hashlib
from typing import Optional, Tuple, TYPE_CHECKING, Union

from .errors import ProtocolError

if TYPE_CHECKING:
    from .output_publication import (
        OutputPublicationCompleteWitness, OutputPublicationID,
        OutputPublicationManifest,
    )
    from .output_publication_journal import (
        OutputPublicationAdoptionProof, OutputPublicationRollbackTombstone,
        OutputPublicationSlotCleanupProof,
    )
    from .output_recovery import OutputRecoveryAck, OutputRecoverySnapshot


PREPARE_OUTPUT_PUBLICATION_HANDLER = "prepare_output_publication"
REPORT_OUTPUT_PUBLICATION_HANDLER = "report_output_publication"
GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER = "get_output_publication_recovery"
ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER = "ack_output_publication_adopted"


class OutputPublicationRPCErrorKind(str, Enum):
    CONFLICT = "CONFLICT"
    INVALID_STATE = "INVALID_STATE"
    CYCLE = "CYCLE"
    INVALID_REQUEST = "INVALID_REQUEST"
    UNAVAILABLE = "UNAVAILABLE"
    INTERNAL = "INTERNAL"


class _WireValue:
    def __reduce__(self):
        return type(self), tuple(getattr(self, item.name) for item in fields(self))


def _copy(value, expected, label):
    if type(value) is not expected:
        raise ProtocolError(f"{label} must be a {expected.__name__}")
    try:
        return replace(value)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ProtocolError(f"invalid {label}: {exc}") from exc


def _digest(value):
    from .output_publication import _checksum

    try:
        return _checksum(value, "manifest_digest")
    except (TypeError, ValueError) as exc:
        raise ProtocolError(str(exc)) from exc


def _error(succeeded, error_kind, error):
    if succeeded:
        if error_kind is not None or error is not None:
            raise ProtocolError("successful output publication reply cannot contain an error")
    elif type(error_kind) is not OutputPublicationRPCErrorKind or type(error) is not str or not error:
        raise ProtocolError("failed output publication reply requires a typed error and detail")


@dataclass(frozen=True)
class OutputPublicationRequestIdentity(_WireValue):
    publication_id: "OutputPublicationID"
    manifest_digest: str

    def __post_init__(self):
        from .output_publication import OutputPublicationID

        object.__setattr__(self, "publication_id", _copy(
            self.publication_id, OutputPublicationID, "publication_id"
        ))
        object.__setattr__(self, "manifest_digest", _digest(self.manifest_digest))


@dataclass(frozen=True)
class PrepareOutputPublication(_WireValue):
    manifest: "OutputPublicationManifest"
    slot_payloads: Tuple[bytes, ...]

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest

        manifest = _copy(self.manifest, OutputPublicationManifest, "manifest")
        if type(self.slot_payloads) not in (tuple, list):
            raise ProtocolError("slot_payloads must be an ordered tuple or list")
        payloads = tuple(self.slot_payloads)
        if len(payloads) != len(manifest.slots):
            raise ProtocolError("slot_payloads must cover every ordered selected output")
        for slot, payload in zip(manifest.slots, payloads):
            if type(payload) is not bytes:
                raise ProtocolError("each slot payload must be bytes")
            if len(payload) != slot.size_bytes or hashlib.sha256(payload).hexdigest() != slot.checksum:
                raise ProtocolError("slot payload size/checksum does not match its manifest")
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "slot_payloads", payloads)

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.manifest.publication_id, self.manifest.manifest_digest)


@dataclass(frozen=True)
class PreparedOutputPublicationReply(_WireValue):
    request_identity: OutputPublicationRequestIdentity
    accepted: bool
    error_kind: Optional[OutputPublicationRPCErrorKind] = None
    error: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, "request_identity", _copy(
            self.request_identity, OutputPublicationRequestIdentity, "request_identity"
        ))
        if type(self.accepted) is not bool:
            raise ProtocolError("prepared publication accepted must be a bool")
        _error(self.accepted, self.error_kind, self.error)


@dataclass(frozen=True)
class ReportOutputPublicationIntent(_WireValue):
    manifest: "OutputPublicationManifest"

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest

        object.__setattr__(self, "manifest", _copy(
            self.manifest, OutputPublicationManifest, "manifest"
        ))

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.manifest.publication_id, self.manifest.manifest_digest)


@dataclass(frozen=True)
class ArmOutputPublication(_WireValue):
    publication_id: "OutputPublicationID"
    manifest_digest: str

    def __post_init__(self):
        identity = OutputPublicationRequestIdentity(self.publication_id, self.manifest_digest)
        object.__setattr__(self, "publication_id", identity.publication_id)
        object.__setattr__(self, "manifest_digest", identity.manifest_digest)

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.publication_id, self.manifest_digest)


@dataclass(frozen=True)
class ReportOutputPublicationTerminal(_WireValue):
    witness: "OutputPublicationCompleteWitness"

    def __post_init__(self):
        from .output_publication import OutputPublicationCompleteWitness

        object.__setattr__(self, "witness", _copy(
            self.witness, OutputPublicationCompleteWitness, "witness"
        ))

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.witness.publication_id, self.witness.manifest_digest)


@dataclass(frozen=True)
class ReportOutputPublicationRollback(_WireValue):
    tombstone: "OutputPublicationRollbackTombstone"
    manifest: "OutputPublicationManifest"

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest
        from .output_publication_journal import OutputPublicationRollbackTombstone
        from .output_recovery import _validate_rollback

        manifest = _copy(self.manifest, OutputPublicationManifest, "manifest")
        tombstone = _copy(self.tombstone, OutputPublicationRollbackTombstone, "tombstone")
        try:
            tombstone = _validate_rollback(manifest, tombstone, armed=False)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ProtocolError(f"invalid rollback report: {exc}") from exc
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "tombstone", tombstone)

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.manifest.publication_id, self.manifest.manifest_digest)


@dataclass(frozen=True)
class ReportOutputPublicationAdopted(_WireValue):
    proof: "OutputPublicationAdoptionProof"

    def __post_init__(self):
        from .output_publication_journal import OutputPublicationAdoptionProof

        object.__setattr__(self, "proof", _copy(
            self.proof, OutputPublicationAdoptionProof, "adoption proof"
        ))

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.proof.complete.publication_id, self.proof.complete.manifest_digest)


@dataclass(frozen=True)
class ReportOutputPublicationSlotCollected(_WireValue):
    proof: "OutputPublicationSlotCleanupProof"

    def __post_init__(self):
        from .output_publication_journal import OutputPublicationSlotCleanupProof

        object.__setattr__(self, "proof", _copy(
            self.proof, OutputPublicationSlotCleanupProof, "slot cleanup proof"
        ))

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.proof.complete.publication_id, self.proof.complete.manifest_digest)


OutputRecoveryReport = Union[
    ReportOutputPublicationIntent, ArmOutputPublication,
    ReportOutputPublicationTerminal, ReportOutputPublicationRollback,
    ReportOutputPublicationAdopted, ReportOutputPublicationSlotCollected,
]


@dataclass(frozen=True)
class OutputRecoveryReply(_WireValue):
    request: OutputRecoveryReport
    ack: Optional["OutputRecoveryAck"] = None
    error_kind: Optional[OutputPublicationRPCErrorKind] = None
    error: Optional[str] = None

    def __post_init__(self):
        from .output_recovery import (
            OutputRecoveryAck, OutputRecoveryDisposition, OutputRecoveryStage,
        )

        stages = {
            ReportOutputPublicationIntent: OutputRecoveryStage.INTENT,
            ArmOutputPublication: OutputRecoveryStage.ARM_COMPLETE,
            ReportOutputPublicationTerminal: OutputRecoveryStage.TERMINAL,
            ReportOutputPublicationRollback: OutputRecoveryStage.ROLLED_BACK,
            ReportOutputPublicationAdopted: OutputRecoveryStage.ADOPTED,
            ReportOutputPublicationSlotCollected: OutputRecoveryStage.SLOT_COLLECTED,
        }
        if type(self.request) not in stages:
            raise ProtocolError("output recovery reply must echo a typed recovery report")
        request = _copy(self.request, type(self.request), "recovery request")
        object.__setattr__(self, "request", request)
        _error(self.ack is not None, self.error_kind, self.error)
        if self.ack is None:
            return
        ack = _copy(self.ack, OutputRecoveryAck, "recovery acknowledgement")
        snapshot = ack.snapshot
        identity = request.request_identity
        if (ack.stage is not stages[type(request)]
                or snapshot.publication_id != identity.publication_id
                or snapshot.manifest_digest != identity.manifest_digest):
            raise ProtocolError("output recovery acknowledgement changed stage or publication identity")
        if type(request) in (ReportOutputPublicationIntent, ReportOutputPublicationRollback):
            if snapshot.manifest != request.manifest:
                raise ProtocolError("output recovery acknowledgement changed the complete manifest")
        if type(request) in (ReportOutputPublicationAdopted, ReportOutputPublicationSlotCollected):
            if snapshot.manifest.header.owner_worker_id != request.proof.owner_worker_id:
                raise ProtocolError("output recovery acknowledgement changed the proof owner")
        if ack.disposition is not OutputRecoveryDisposition.FENCED:
            if type(request) is ReportOutputPublicationTerminal and snapshot.complete != request.witness:
                raise ProtocolError("terminal acknowledgement changed the Complete witness")
            if type(request) is ReportOutputPublicationRollback and snapshot.rollback != request.tombstone:
                raise ProtocolError("rollback acknowledgement changed the exact tombstone")
            if type(request) is ReportOutputPublicationAdopted and snapshot.adopted != request.proof:
                raise ProtocolError("adoption acknowledgement changed the exact proof")
            if type(request) is ReportOutputPublicationSlotCollected and request.proof not in snapshot.slot_collections:
                raise ProtocolError("slot collection acknowledgement lacks the exact proof")
        object.__setattr__(self, "ack", ack)

    @property
    def accepted(self):
        from .output_recovery import OutputRecoveryDisposition

        return self.ack is not None and self.ack.disposition is not OutputRecoveryDisposition.FENCED


@dataclass(frozen=True)
class GetOutputPublicationRecovery(_WireValue):
    publication_id: "OutputPublicationID"

    def __post_init__(self):
        from .output_publication import OutputPublicationID

        object.__setattr__(self, "publication_id", _copy(
            self.publication_id, OutputPublicationID, "publication_id"
        ))


@dataclass(frozen=True)
class GetOutputPublicationRecoveryReply(_WireValue):
    request: GetOutputPublicationRecovery
    found: bool
    snapshot: Optional["OutputRecoverySnapshot"] = None
    error_kind: Optional[OutputPublicationRPCErrorKind] = None
    error: Optional[str] = None

    def __post_init__(self):
        from .output_recovery import OutputRecoverySnapshot

        request = _copy(self.request, GetOutputPublicationRecovery, "recovery query")
        object.__setattr__(self, "request", request)
        if type(self.found) is not bool:
            raise ProtocolError("recovery query found must be a bool")
        if self.found:
            snapshot = _copy(self.snapshot, OutputRecoverySnapshot, "recovery snapshot")
            if snapshot.publication_id != request.publication_id:
                raise ProtocolError("recovery query returned another publication")
            _error(True, self.error_kind, self.error)
            object.__setattr__(self, "snapshot", snapshot)
        else:
            if self.snapshot is not None:
                raise ProtocolError("missing recovery query cannot contain a snapshot")
            _error(self.error_kind is None and self.error is None, self.error_kind, self.error)


@dataclass(frozen=True)
class AckOutputPublicationAdopted(_WireValue):
    """Owner proof permitting Node-local reply payload retirement after CAS."""

    proof: "OutputPublicationAdoptionProof"

    def __post_init__(self):
        from .output_publication_journal import OutputPublicationAdoptionProof

        object.__setattr__(self, "proof", _copy(
            self.proof, OutputPublicationAdoptionProof, "adoption proof"
        ))

    @property
    def request_identity(self):
        return OutputPublicationRequestIdentity(self.proof.complete.publication_id, self.proof.complete.manifest_digest)


@dataclass(frozen=True)
class AckOutputPublicationAdoptedReply(_WireValue):
    request: AckOutputPublicationAdopted
    accepted: bool
    error_kind: Optional[OutputPublicationRPCErrorKind] = None
    error: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, "request", _copy(
            self.request, AckOutputPublicationAdopted, "adoption acknowledgement request"
        ))
        if type(self.accepted) is not bool:
            raise ProtocolError("adoption acknowledgement accepted must be a bool")
        _error(self.accepted, self.error_kind, self.error)


GET_OUTPUT_NODE_LOSS_HANDLER = "get_output_node_loss"
DECIDE_OUTPUT_NODE_LOSS_HANDLER = "decide_output_node_loss"
PROGRESS_OUTPUT_NODE_LOSS_HANDLER = "progress_output_node_loss"


@dataclass(frozen=True)
class GetOutputNodeLoss(_WireValue):
    publication_id: "OutputPublicationID"
    owner_worker_id: object
    node_death: object

    def __post_init__(self):
        from .ids import WorkerID
        from .output_publication import OutputPublicationID, _opaque
        from .output_recovery import _node_death
        object.__setattr__(self, "publication_id", _copy(self.publication_id, OutputPublicationID, "publication_id"))
        object.__setattr__(self, "owner_worker_id", _opaque(self.owner_worker_id, WorkerID, "owner"))
        object.__setattr__(self, "node_death", _node_death(self.node_death))


@dataclass(frozen=True)
class GetOutputNodeLossReply(_WireValue):
    request: GetOutputNodeLoss
    found: bool
    work: object = None
    snapshot: object = None

    def __post_init__(self):
        from .output_recovery import OutputRecoveryWork, OutputRecoverySnapshot
        request = _copy(self.request, GetOutputNodeLoss, "Node-loss query")
        if type(self.found) is not bool:
            raise ProtocolError("found must be bool")
        if self.found:
            work = _copy(self.work, OutputRecoveryWork, "frozen work")
            snapshot = _copy(self.snapshot, OutputRecoverySnapshot, "snapshot")
            if (work.publication_id != request.publication_id or work.death != request.node_death
                    or work.manifest.header.owner_worker_id != request.owner_worker_id
                    or snapshot.manifest != work.manifest or snapshot.frozen_node_death != work.death):
                raise ProtocolError("Node-loss query reply changed exact identity")
            object.__setattr__(self, "work", work)
            object.__setattr__(self, "snapshot", snapshot)
        elif self.work is not None or self.snapshot is not None:
            raise ProtocolError("absent Node-loss query cannot carry state")
        object.__setattr__(self, "request", request)


@dataclass(frozen=True)
class DecideOutputNodeLoss(_WireValue):
    work: object
    decision: object

    def __post_init__(self):
        from .output_recovery import OutputRecoveryWork, OutputRecoveryOwnerDecisionRecord
        work = _copy(self.work, OutputRecoveryWork, "frozen work")
        decision = _copy(self.decision, OutputRecoveryOwnerDecisionRecord, "owner decision")
        if (decision.publication_id != work.publication_id
                or decision.manifest_digest != work.manifest.manifest_digest
                or decision.owner_worker_id != work.manifest.header.owner_worker_id):
            raise ProtocolError("owner decision changed frozen identity")
        object.__setattr__(self, "work", work)
        object.__setattr__(self, "decision", decision)


@dataclass(frozen=True)
class ProgressOutputNodeLoss(_WireValue):
    work: object

    def __post_init__(self):
        from .output_recovery import OutputRecoveryWork
        object.__setattr__(self, "work", _copy(self.work, OutputRecoveryWork, "frozen work"))


@dataclass(frozen=True)
class OutputNodeLossReply(_WireValue):
    request: object
    snapshot: object
    progressed: bool = False

    def __post_init__(self):
        from .output_recovery import OutputRecoverySnapshot
        if type(self.request) not in (DecideOutputNodeLoss, ProgressOutputNodeLoss):
            raise ProtocolError("Node-loss reply requires exact operation")
        request = replace(self.request)
        snapshot = _copy(self.snapshot, OutputRecoverySnapshot, "snapshot")
        if (snapshot.manifest != request.work.manifest
                or snapshot.frozen_node_death != request.work.death):
            raise ProtocolError("Node-loss reply changed frozen work")
        if type(self.progressed) is not bool:
            raise ProtocolError("progressed must be bool")
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "snapshot", snapshot)


FINALIZE_OUTPUT_OWNER_DEATH_HANDLER = "finalize_output_owner_death"


@dataclass(frozen=True)
class FinalizeOutputOwnerDeath(_WireValue):
    manifest: object
    owner_death: object

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest
        from .output_recovery import _owner_death
        manifest = _copy(self.manifest, OutputPublicationManifest, "manifest")
        death = _owner_death(self.owner_death)
        if death.worker_id != manifest.header.owner_worker_id:
            raise ProtocolError("owner death does not match output manifest")
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "owner_death", death)


@dataclass(frozen=True)
class FinalizeOutputOwnerDeathReply(_WireValue):
    request: FinalizeOutputOwnerDeath
    cleaned: bool

    def __post_init__(self):
        object.__setattr__(self, "request", _copy(self.request, FinalizeOutputOwnerDeath, "request"))
        if type(self.cleaned) is not bool:
            raise ProtocolError("owner cleanup flag must be bool")


__all__ = [
    "PREPARE_OUTPUT_PUBLICATION_HANDLER", "REPORT_OUTPUT_PUBLICATION_HANDLER",
    "GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER", "ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER",
    "OutputPublicationRPCErrorKind", "OutputPublicationRequestIdentity",
    "PrepareOutputPublication", "PreparedOutputPublicationReply",
    "ReportOutputPublicationIntent", "ArmOutputPublication",
    "ReportOutputPublicationTerminal", "ReportOutputPublicationRollback",
    "ReportOutputPublicationAdopted", "ReportOutputPublicationSlotCollected",
    "OutputRecoveryReport", "OutputRecoveryReply",
    "GetOutputPublicationRecovery", "GetOutputPublicationRecoveryReply",
    "AckOutputPublicationAdopted", "AckOutputPublicationAdoptedReply",
]
