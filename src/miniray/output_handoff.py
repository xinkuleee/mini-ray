"""Owner-local metadata for a single output's handoff and exact history.

This table owns neither object visibility nor execution success. Core validates
the live task/owner/lease, installs the full manifest before remote reference
effects, and calls adopt only after its actual owner CAS. Complete witnesses
come from the Node execution authority. Aborted is a forward fence, not proof
that remote holds, replicas or source custody have been released.

No GCS stages, global graph, payload bytes, cleanup driver or death detector
live here. Records remain for exact replay until the owner/job closes; object
GC does not erase them. Core/Node retain their own unfinished effect work.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from enum import Enum
from threading import RLock

from .ids import AttemptID, WorkerID
from .death_proofs import node_death as _node_death, owner_death as _owner_death
from .output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationID,
    OutputPublicationManifest, _checksum, _hold, _object_id, _opaque, _sequence,
)
from .output_publication_journal import OutputPublicationAdoptionProof
from .protocol import (
    NodeDeathReason, NodeDeathRecord, ReleaseContainedReferenceReply,
    WorkerDeathReason, WorkerDeathRecord,
)


class OutputHandoffError(ValueError):
    """A local handoff request contradicts its lifecycle."""


class OutputHandoffConflictError(OutputHandoffError):
    """One identity was reused with a different complete request."""


class OutputHandoffStateError(OutputHandoffError):
    """The requested forward transition is not currently permitted."""


class OutputHandoffPhase(str, Enum):
    PENDING = "PENDING"
    ABORTED = "ABORTED"
    ADOPTED = "ADOPTED"


def _copy(value, kind, label):
    if type(value) is not kind:
        raise TypeError(f"{label} must be a {kind.__name__}")
    return replace(value)


def _reason(value):
    if type(value) is not str or not value:
        raise TypeError("abort reason must be a non-empty string")
    return value


@dataclass(frozen=True)
class NodeLostOutputResolution:
    """Owner-local receipt after a publishing Node's confirmed death.

    Complete records execution success. Keep requires independent byte
    custody, checked by the owner table. Cleanup preserves real child release
    replies or previously committed child-owner death records; this value
    performs no effects and grants no authority to declare a process dead.
    """

    publication_id: OutputPublicationID
    manifest_digest: str
    owner_worker_id: WorkerID
    node_death: NodeDeathRecord
    complete: OutputPublicationCompleteWitness | None = None
    keep: bool = False
    cleanup: tuple[ReleaseContainedReferenceReply | WorkerDeathRecord, ...] = ()

    def __post_init__(self):
        identity = _copy(self.publication_id, OutputPublicationID, "publication_id")
        digest = _checksum(self.manifest_digest, "manifest_digest")
        owner = _opaque(self.owner_worker_id, WorkerID, "output owner")
        death = _node_death(self.node_death)
        if death.reason is not NodeDeathReason.PROCESS_EXIT:
            raise OutputHandoffConflictError("output loss requires a confirmed publishing Node exit")
        if type(self.keep) is not bool:
            raise TypeError("keep must be a bool")
        complete = self.complete
        if complete is not None:
            complete = _copy(complete, OutputPublicationCompleteWitness, "complete")
            if complete.publication_id != identity or complete.manifest_digest != digest:
                raise OutputHandoffConflictError("Node-loss Complete changed the exact publication")
        if self.keep and complete is None:
            raise OutputHandoffConflictError("keeping an output requires its exact Complete")
        cleanup = []
        for proof in _sequence(self.cleanup, "cleanup"):
            if type(proof) is ReleaseContainedReferenceReply:
                proof = ReleaseContainedReferenceReply(
                    _object_id(proof.object_id),
                    _opaque(proof.owner_worker_id, WorkerID, "child owner"),
                    _hold(proof.hold), proof.accepted, proof.released, proof.error,
                )
                if (proof.accepted is not True or type(proof.released) is not bool
                        or proof.error is not None):
                    raise OutputHandoffConflictError("child cleanup requires an accepted exact release")
            elif type(proof) is WorkerDeathRecord:
                proof = _owner_death(proof)
                if proof.reason not in (WorkerDeathReason.PROCESS_EXIT, WorkerDeathReason.NODE_EXIT):
                    raise OutputHandoffConflictError("child cleanup requires a confirmed owner exit")
            else:
                raise TypeError("cleanup requires ReleaseContainedReferenceReply or WorkerDeathRecord")
            if proof in cleanup:
                raise OutputHandoffConflictError("Node-loss cleanup repeats an exact proof")
            cleanup.append(proof)
        if self.keep and cleanup:
            raise OutputHandoffConflictError("kept outputs cannot release their child holds")
        object.__setattr__(self, "publication_id", identity)
        object.__setattr__(self, "manifest_digest", digest)
        object.__setattr__(self, "owner_worker_id", owner)
        object.__setattr__(self, "node_death", death)
        object.__setattr__(self, "complete", complete)
        object.__setattr__(self, "cleanup", tuple(cleanup))

    def validate_manifest(self, manifest: OutputPublicationManifest) -> None:
        """Require every discarded final and provisional hold to be settled."""
        manifest = _copy(manifest, OutputPublicationManifest, "manifest")
        if (manifest.publication_id != self.publication_id
                or manifest.manifest_digest != self.manifest_digest
                or manifest.header.owner_worker_id != self.owner_worker_id):
            raise OutputHandoffConflictError("Node-loss resolution changed its manifest or owner")
        publisher = manifest.header.node_incarnation
        death = self.node_death
        if (death.node_id, death.node_pid, death.registration_epoch) != (
            publisher.node_id, publisher.node_pid, publisher.registration_epoch
        ):
            raise OutputHandoffConflictError("Node-loss receipt names another publishing incarnation")
        if self.keep:
            return
        expected = {
            (transfer.contained_object_id, transfer.contained_owner_worker_id, hold)
            for transfer in manifest.slots[0].transfers
            for hold in (transfer.final_hold, transfer.provisional_hold)
        }
        child_owners = {owner for _, owner, _ in expected}
        released = set()
        dead = {}
        for proof in self.cleanup:
            if type(proof) is ReleaseContainedReferenceReply:
                key = (proof.object_id, proof.owner_worker_id, proof.hold)
                if key not in expected or key in released:
                    raise OutputHandoffConflictError("child cleanup changed or repeated a manifest hold")
                released.add(key)
            else:
                if proof.worker_id not in child_owners:
                    raise OutputHandoffConflictError("child death proof names no manifest child owner")
                previous = dead.setdefault(proof.worker_id, proof)
                if previous != proof:
                    raise OutputHandoffConflictError("child owner death evidence changed")
        if any(key not in released and key[1] not in dead for key in expected):
            raise OutputHandoffConflictError("Node-loss cleanup must settle every final and provisional hold")

    def __reduce__(self):
        return type(self), tuple(getattr(self, item.name) for item in fields(self))


@dataclass(frozen=True)
class OutputHandoffSnapshot:
    """Exact local facts, never a forward permission or a cleanup receipt."""

    publication_id: OutputPublicationID
    manifest: OutputPublicationManifest | None
    phase: OutputHandoffPhase = OutputHandoffPhase.PENDING
    complete: OutputPublicationCompleteWitness | None = None
    adoption: OutputPublicationAdoptionProof | None = None
    abort_reason: str | None = None

    def __post_init__(self):
        identity = _copy(self.publication_id, OutputPublicationID, "publication_id")
        if type(self.phase) is not OutputHandoffPhase:
            raise TypeError("phase must be an OutputHandoffPhase")
        manifest = self.manifest
        if manifest is not None:
            manifest = _copy(manifest, OutputPublicationManifest, "manifest")
            if manifest.publication_id != identity or len(manifest.slots) != 1:
                raise OutputHandoffConflictError("handoff requires the exact single-output manifest")
        elif self.phase is not OutputHandoffPhase.ABORTED:
            raise OutputHandoffStateError("only a pre-registration abort may lack a manifest")
        complete = self.complete
        if complete is not None:
            complete = _copy(complete, OutputPublicationCompleteWitness, "complete")
            if (manifest is None or complete.publication_id != identity
                    or complete.manifest_digest != manifest.manifest_digest):
                raise OutputHandoffConflictError("Complete changed its registered manifest")
        adoption = self.adoption
        if adoption is not None:
            adoption = _copy(adoption, OutputPublicationAdoptionProof, "adoption")
            if (manifest is None or complete is None or adoption.complete != complete
                    or adoption.owner_worker_id != manifest.header.owner_worker_id):
                raise OutputHandoffConflictError("adoption changed its owner or Complete")
        if (self.phase is OutputHandoffPhase.ADOPTED) != (adoption is not None):
            raise OutputHandoffStateError("ADOPTED requires exactly one adoption receipt")
        if self.phase is OutputHandoffPhase.ABORTED:
            _reason(self.abort_reason)
        elif self.abort_reason is not None:
            raise OutputHandoffStateError("a live handoff cannot carry an abort reason")
        object.__setattr__(self, "publication_id", identity)
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "complete", complete)
        object.__setattr__(self, "adoption", adoption)

    def __reduce__(self):
        return type(self), tuple(getattr(self, item.name) for item in fields(self))


class OutputHandoffTable:
    """A manifest and terminal-receipt table owned by one Core incarnation.

    Core's composition lock must surround checks against task/object state and
    these calls. The internal lock protects this table only. It cannot make
    Core's owner CAS atomic or authorize remote Complete/child operations.
    """

    def __init__(self):
        self._records: dict[OutputPublicationID, OutputHandoffSnapshot] = {}
        self._lock = RLock()

    def register(self, manifest, current_attempt: AttemptID) -> OutputHandoffSnapshot:
        manifest = _copy(manifest, OutputPublicationManifest, "manifest")
        current_attempt = _copy(current_attempt, AttemptID, "current_attempt")
        identity = manifest.publication_id
        if len(manifest.slots) != 1 or identity.attempt_id != current_attempt:
            raise OutputHandoffStateError("registration requires the current single-output attempt")
        with self._lock:
            previous = self._records.get(identity)
            if previous is not None:
                if previous.manifest is not None and previous.manifest != manifest:
                    raise OutputHandoffConflictError("handoff identity names a different manifest")
                if previous.phase is OutputHandoffPhase.ABORTED:
                    raise OutputHandoffStateError("aborted handoff cannot register again")
                return replace(previous)
            snapshot = OutputHandoffSnapshot(identity, manifest)
            self._records[identity] = snapshot
            return replace(snapshot)

    def query(self, identity) -> OutputHandoffSnapshot | None:
        identity = _copy(identity, OutputPublicationID, "publication_id")
        with self._lock:
            snapshot = self._records.get(identity)
            return None if snapshot is None else replace(snapshot)

    def snapshots(self) -> tuple[OutputHandoffSnapshot, ...]:
        """Enumerate accurate history; phase is not a distributed clean flag."""
        with self._lock:
            return tuple(replace(value) for value in self._records.values())

    def record_complete(self, witness) -> OutputHandoffSnapshot:
        witness = _copy(witness, OutputPublicationCompleteWitness, "complete")
        with self._lock:
            previous = self._record(witness.publication_id)
            if (previous.manifest is None
                    or witness.manifest_digest != previous.manifest.manifest_digest):
                raise OutputHandoffConflictError("Complete changed its registered manifest")
            if previous.complete is not None:
                if previous.complete != witness:
                    raise OutputHandoffConflictError("Complete conflicts with the historical receipt")
                return replace(previous)
            if previous.phase is OutputHandoffPhase.ABORTED:
                raise OutputHandoffStateError("aborted handoff cannot accept a new Complete")
            snapshot = replace(previous, complete=witness)
            self._records[witness.publication_id] = snapshot
            return replace(snapshot)

    def adopt(self, proof) -> OutputHandoffSnapshot:
        """Remember an actual owner CAS receipt; this call does not do the CAS."""
        proof = _copy(proof, OutputPublicationAdoptionProof, "adoption")
        identity = proof.complete.publication_id
        with self._lock:
            previous = self._record(identity)
            if (previous.manifest is None or previous.complete != proof.complete
                    or previous.manifest.header.owner_worker_id != proof.owner_worker_id):
                raise OutputHandoffConflictError("adoption requires the registered owner and Complete")
            if previous.adoption is not None:
                if previous.adoption != proof:
                    raise OutputHandoffConflictError("adoption conflicts with the historical receipt")
                return replace(previous)
            if previous.phase is OutputHandoffPhase.ABORTED:
                raise OutputHandoffStateError("aborted handoff cannot be adopted")
            snapshot = replace(previous, phase=OutputHandoffPhase.ADOPTED, adoption=proof)
            self._records[identity] = snapshot
            return replace(snapshot)

    def abort_manifest(self, manifest, reason: str) -> OutputHandoffSnapshot:
        """Bind cleanup metadata without granting registration permission.

        An earlier identity-only abort keeps its original reason. Its first
        complete manifest is frozen here; later changes conflict. This only
        fences forward work and does not claim remote effects were cleaned.
        """
        manifest = _copy(manifest, OutputPublicationManifest, "manifest")
        reason = _reason(reason)
        identity = manifest.publication_id
        with self._lock:
            previous = self._records.get(identity)
            if previous is not None:
                if previous.manifest is not None and previous.manifest != manifest:
                    raise OutputHandoffConflictError("abort manifest conflicts with registered history")
                if previous.phase is OutputHandoffPhase.ADOPTED:
                    raise OutputHandoffStateError("adopted handoff cannot be aborted")
                if previous.phase is OutputHandoffPhase.ABORTED:
                    snapshot = replace(previous, manifest=manifest)
                else:
                    snapshot = replace(previous, phase=OutputHandoffPhase.ABORTED, abort_reason=reason)
            else:
                snapshot = OutputHandoffSnapshot(
                    identity, manifest, OutputHandoffPhase.ABORTED, abort_reason=reason,
                )
            self._records[identity] = snapshot
            return replace(snapshot)

    def abort(self, identity, reason: str) -> bool:
        """Fence forward publication, including before registration arrives.

        False means an exact abort replay or an already adopted handoff. It
        never proves cleanup. Query distinguishes those two historical facts.
        """
        identity = _copy(identity, OutputPublicationID, "publication_id")
        reason = _reason(reason)
        with self._lock:
            previous = self._records.get(identity)
            if previous is None:
                self._records[identity] = OutputHandoffSnapshot(
                    identity, None, OutputHandoffPhase.ABORTED, abort_reason=reason,
                )
                return True
            if previous.phase is OutputHandoffPhase.ADOPTED:
                return False
            if previous.phase is OutputHandoffPhase.ABORTED:
                if previous.abort_reason != reason:
                    raise OutputHandoffConflictError("abort replay changed its reason")
                return False
            self._records[identity] = replace(
                previous, phase=OutputHandoffPhase.ABORTED, abort_reason=reason,
            )
            return True

    def _record(self, identity):
        try:
            return self._records[identity]
        except KeyError:
            raise OutputHandoffStateError("handoff has not been registered") from None


__all__ = [
    "NodeLostOutputResolution",
    "OutputHandoffTable", "OutputHandoffSnapshot", "OutputHandoffPhase",
    "OutputHandoffError", "OutputHandoffConflictError", "OutputHandoffStateError",
]
