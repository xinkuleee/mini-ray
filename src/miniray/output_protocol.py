"""Typed owner/Node boundaries for a single result handoff.

PrepareOutputPublication carries serialized bytes from Worker to Node. Owner
registration, exact Complete reporting, rollback and retirement carry metadata;
none is a GCS publication transaction or a global reference graph operation.
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


PREPARE_OUTPUT_PUBLICATION_HANDLER = "prepare_output_publication"
ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER = "ack_output_publication_adopted"
REGISTER_OUTPUT_HANDOFF_HANDLER = "register_output_handoff"
REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER = "report_output_handoff_complete"
REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER = "report_output_handoff_rollback"
GET_OUTPUT_HANDOFF_HANDLER = "get_output_handoff"


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
            raise ProtocolError("slot_payloads must contain the single output payload")
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
class RegisterOutputHandoff(_WireValue):
    manifest: object

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest
        object.__setattr__(self, "manifest", _copy(self.manifest, OutputPublicationManifest, "manifest"))


@dataclass(frozen=True)
class ReportOutputHandoffComplete(_WireValue):
    witness: object

    def __post_init__(self):
        from .output_publication import OutputPublicationCompleteWitness
        object.__setattr__(self, "witness", _copy(self.witness, OutputPublicationCompleteWitness, "witness"))


@dataclass(frozen=True)
class ReportOutputHandoffRollback(_WireValue):
    manifest: object
    tombstone: object

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest
        from .output_publication_journal import OutputPublicationRollbackTombstone
        manifest = _copy(self.manifest, OutputPublicationManifest, "manifest")
        tombstone = _copy(self.tombstone, OutputPublicationRollbackTombstone, "rollback")
        if (tombstone.plan.publication_id != manifest.publication_id
                or tombstone.plan.manifest_digest != manifest.manifest_digest):
            raise ProtocolError("rollback changed its owner handoff manifest")
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "tombstone", tombstone)


@dataclass(frozen=True)
class GetOutputHandoff(_WireValue):
    publication_id: object

    def __post_init__(self):
        from .output_publication import OutputPublicationID
        object.__setattr__(self, "publication_id", _copy(self.publication_id, OutputPublicationID, "identity"))


@dataclass(frozen=True)
class OutputHandoffReply(_WireValue):
    request: object
    accepted: bool
    snapshot: object = None
    error: Optional[str] = None

    def __post_init__(self):
        from .output_handoff import OutputHandoffSnapshot
        if type(self.request) not in (RegisterOutputHandoff, ReportOutputHandoffComplete,
                                      ReportOutputHandoffRollback, GetOutputHandoff):
            raise ProtocolError("handoff reply requires an exact request")
        request = replace(self.request)
        if type(self.accepted) is not bool or (self.accepted and self.error is not None):
            raise ProtocolError("invalid handoff acceptance")
        if not self.accepted and (type(self.error) is not str or not self.error):
            raise ProtocolError("rejected handoff requires an error")
        snapshot = self.snapshot
        if snapshot is not None:
            snapshot = _copy(snapshot, OutputHandoffSnapshot, "handoff snapshot")
            identity = (request.manifest.publication_id if isinstance(request, (RegisterOutputHandoff, ReportOutputHandoffRollback))
                        else request.witness.publication_id if isinstance(request, ReportOutputHandoffComplete)
                        else request.publication_id)
            if snapshot.publication_id != identity:
                raise ProtocolError("handoff reply changed identity")
            if isinstance(request, (RegisterOutputHandoff, ReportOutputHandoffRollback)) and snapshot.manifest != request.manifest:
                raise ProtocolError("handoff reply changed manifest")
        if self.accepted and not isinstance(request, GetOutputHandoff) and snapshot is None:
            raise ProtocolError("accepted handoff transition requires its exact receipt")
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "snapshot", snapshot)






















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














FINALIZE_OUTPUT_OWNER_DEATH_HANDLER = "finalize_output_owner_death"


@dataclass(frozen=True)
class FinalizeOutputOwnerDeath(_WireValue):
    manifest: object
    owner_death: object

    def __post_init__(self):
        from .output_publication import OutputPublicationManifest
        from .death_proofs import owner_death as _owner_death
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
