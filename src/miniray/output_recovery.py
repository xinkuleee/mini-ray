"""Metadata-only recovery facts for one selected-output publication.

INTENT precedes every external publication effect.  ARM is a recorded permission
to cross local Complete, not evidence that Complete occurred.  A dead Node with
ARM but no successful witness therefore has an unknown outcome.  Frozen work is
an immutable point-in-time record; late reports never upgrade its history.

Only the Node/owner adapters can prove payload custody or completed cleanup.
This registry validates their exact metadata proofs, never stores descriptors,
payloads or TaskSpecs, and never executes a second compensation state machine.
Normal terminals, per-slot owner choices and orthogonal owner death stay visible
instead of being collapsed into one "resolved" flag.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from enum import Enum
from threading import RLock
from typing import Optional, Tuple

from .ids import NodeID, ObjectID, WorkerID
from .output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationError, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, _checksum, _node_incarnation, _object_id,
    _opaque, _require_type, _sequence, _string, _uint,
)
from .output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationEffect,
    OutputPublicationRollbackTombstone, OutputPublicationSlotCleanupProof,
    OutputPublicationStage,
)
from .protocol import (
    NodeDeathReason, NodeDeathRecord, WorkerDeathReason, WorkerDeathRecord,
    WorkerIncarnation,
)


class OutputRecoveryConflictError(OutputPublicationConflictError):
    """A publication/death/decision identity was rebound."""


class OutputRecoveryStateError(OutputPublicationError):
    """A requested fact contradicts the monotonic recovery history."""


class UnknownOutputRecoveryError(OutputRecoveryStateError, LookupError):
    """No complete intent or rollback tombstone names this publication."""


class OutputRecoveryStage(str, Enum):
    INTENT = "INTENT"
    ARM_COMPLETE = "ARM_COMPLETE"
    TERMINAL = "TERMINAL"
    ROLLED_BACK = "ROLLED_BACK"
    ADOPTED = "ADOPTED"
    SLOT_COLLECTED = "SLOT_COLLECTED"
    OWNER_DECIDED = "OWNER_DECIDED"
    RESOLVED = "RESOLVED"
    OWNER_CLEANED = "OWNER_CLEANED"


class OutputRecoveryDisposition(str, Enum):
    APPLIED = "APPLIED"
    ALREADY_RECORDED = "ALREADY_RECORDED"
    FENCED = "FENCED"


class OutputRecoveryAction(str, Enum):
    PRECOMPLETE_ROLLBACK = "PRECOMPLETE_ROLLBACK"
    COMPLETION_UNKNOWN = "COMPLETION_UNKNOWN"
    POSTCOMPLETE_RESOLVE = "POSTCOMPLETE_RESOLVE"


class OutputRecoveryOwnerDecision(str, Enum):
    KEEP = "KEEP"
    DROP = "DROP"


class _WireValue:
    def __reduce__(self):
        return type(self), tuple(getattr(self, value.name) for value in fields(self))


def _node_death(value: object) -> NodeDeathRecord:
    _require_type(value, NodeDeathRecord, "node death")
    _require_type(value.reason, NodeDeathReason, "node death reason")
    _uint(value.node_pid, "node_pid", positive=True)
    _uint(value.registration_epoch, "registration_epoch", positive=True)
    _uint(value.death_epoch, "node death_epoch", positive=True)
    _require_type(value.exit_code, int, "node exit_code")
    return NodeDeathRecord(
        _string(value.detection_id, "node detection_id"),
        _opaque(value.node_id, NodeID, "node_id"), value.node_pid,
        value.registration_epoch, value.death_epoch, value.exit_code, value.reason,
        _string(value.detail, "node death detail"),
    )


def _owner_death(value: object) -> WorkerDeathRecord:
    _require_type(value, WorkerDeathRecord, "owner death")
    _require_type(value.incarnation, WorkerIncarnation, "owner incarnation")
    incarnation = value.incarnation
    _uint(incarnation.node_pid, "owner node_pid", positive=True)
    _uint(incarnation.node_registration_epoch, "owner node epoch", positive=True)
    _uint(incarnation.worker_pid, "owner worker_pid", positive=True)
    _uint(value.death_epoch, "owner death_epoch", positive=True)
    _require_type(value.exit_code, int, "owner exit_code")
    _require_type(value.reason, WorkerDeathReason, "owner death reason")
    return WorkerDeathRecord(
        _string(value.detection_id, "owner detection_id"),
        WorkerIncarnation(
            _opaque(incarnation.node_id, NodeID, "owner node_id"),
            incarnation.node_pid, incarnation.node_registration_epoch,
            _opaque(incarnation.worker_id, WorkerID, "owner worker_id"),
            incarnation.worker_pid,
        ), value.death_epoch, value.exit_code, value.reason,
    )


def _incarnation_for_death(death: NodeDeathRecord) -> OutputPublicationNodeIncarnation:
    return OutputPublicationNodeIncarnation(
        death.node_id, death.node_pid, death.registration_epoch
    )


def _validate_witness(manifest, witness):
    _require_type(witness, OutputPublicationCompleteWitness, "complete")
    witness = replace(witness)
    if witness != OutputPublicationCompleteWitness.for_manifest(manifest):
        raise OutputRecoveryConflictError("Complete witness changed the publication manifest")
    return witness


def _validate_owner_proof(manifest, proof, kind):
    _require_type(proof, kind, "owner proof")
    proof = replace(proof)
    _validate_witness(manifest, proof.complete)
    if proof.owner_worker_id != manifest.header.owner_worker_id:
        raise OutputRecoveryConflictError("proof names another publication owner")
    return proof


def _validate_rollback(manifest, proof, *, armed):
    _require_type(proof, OutputPublicationRollbackTombstone, "rollback")
    proof = replace(proof)
    if (proof.plan.publication_id != manifest.publication_id
            or proof.plan.manifest_digest != manifest.manifest_digest):
        raise OutputRecoveryConflictError("rollback changed the publication manifest")
    # GCS has no per-effect execution journal.  Before ARM a local Node may
    # prove any ordered subset of attempted effects was compensated.  After
    # ARM all effects must have been attempted, so every inverse is required.
    def effect(stage, slot=None, transfer=None):
        return OutputPublicationEffect(
            manifest.publication_id, manifest.manifest_digest, stage, slot, transfer
        )

    possible = []
    if manifest.ordered_edges:
        possible.append(effect(OutputPublicationStage.GRAPH_ABORT))
    possible.extend(
        effect(OutputPublicationStage.SLOT_DROP, index)
        for index in reversed(range(len(manifest.slots)))
    )
    indices = tuple(
        (slot_index, transfer_index)
        for slot_index, slot in enumerate(manifest.slots)
        for transfer_index in range(len(slot.transfers))
    )
    for stage in (OutputPublicationStage.FINAL_RELEASE,
                  OutputPublicationStage.PROVISIONAL_RELEASE):
        possible.extend(effect(stage, *index) for index in reversed(indices))
    selected = set(proof.plan.effects)
    if proof.plan.effects != tuple(item for item in possible if item in selected):
        raise OutputRecoveryConflictError("rollback contains an invalid or unordered effect")
    if armed and proof.plan.effects != tuple(possible):
        raise OutputRecoveryStateError("armed rollback requires every possible effect ACK")
    return proof


@dataclass(frozen=True)
class OutputSlotDecision(_WireValue):
    slot_index: int
    object_id: ObjectID
    decision: OutputRecoveryOwnerDecision

    def __post_init__(self):
        _uint(self.slot_index, "slot_index")
        object.__setattr__(self, "object_id", _object_id(self.object_id))
        _require_type(self.decision, OutputRecoveryOwnerDecision, "slot decision")


@dataclass(frozen=True)
class OutputRecoveryOwnerDecisionRecord(_WireValue):
    publication_id: OutputPublicationID
    manifest_digest: str
    owner_worker_id: WorkerID
    decision_id: str
    slots: Tuple[OutputSlotDecision, ...]
    complete: Optional[OutputPublicationCompleteWitness] = None

    def __post_init__(self):
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(self.publication_id)
        digest = _checksum(self.manifest_digest, "manifest_digest")
        slots = []
        for item in _sequence(self.slots, "slots"):
            _require_type(item, OutputSlotDecision, "slot decision")
            slots.append(replace(item))
        if tuple((item.slot_index, item.object_id) for item in slots) != tuple(
            enumerate(publication_id.output_ids)
        ):
            raise OutputRecoveryConflictError("decision vector must cover every ordered selected slot")
        complete = self.complete
        if complete is not None:
            _require_type(complete, OutputPublicationCompleteWitness, "decision Complete")
            complete = replace(complete)
            if (complete.publication_id != publication_id
                    or complete.manifest_digest != digest):
                raise OutputRecoveryConflictError("owner decision changed Complete identity")
        if any(item.decision is OutputRecoveryOwnerDecision.KEEP for item in slots) and complete is None:
            raise OutputRecoveryStateError("KEEP requires an exact successful Complete witness")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "manifest_digest", digest)
        object.__setattr__(self, "owner_worker_id", _opaque(
            self.owner_worker_id, WorkerID, "decision owner"
        ))
        _string(self.decision_id, "decision_id")
        object.__setattr__(self, "slots", tuple(slots))
        object.__setattr__(self, "complete", complete)


@dataclass(frozen=True)
class OutputRecoveryResolution(_WireValue):
    """Post-death cleanup/adoption decision, never a replacement snapshot."""

    publication_id: OutputPublicationID
    manifest_digest: str
    node_death: NodeDeathRecord
    owner_worker_id: WorkerID
    resolution_id: str
    kept_slots: Tuple[int, ...] = ()
    complete: Optional[OutputPublicationCompleteWitness] = None

    def __post_init__(self):
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        identity = replace(self.publication_id)
        digest = _checksum(self.manifest_digest, "manifest_digest")
        slots = _sequence(self.kept_slots, "kept_slots")
        for value in slots:
            _uint(value, "kept slot")
        if slots != tuple(sorted(set(slots))) or any(value >= len(identity.output_ids) for value in slots):
            raise OutputRecoveryConflictError("kept slots must be an ordered subset")
        if self.complete is not None:
            _require_type(self.complete, OutputPublicationCompleteWitness, "Complete")
        complete = None if self.complete is None else replace(self.complete)
        if complete is not None and (complete.publication_id != identity or complete.manifest_digest != digest):
            raise OutputRecoveryConflictError("resolution Complete identity changed")
        if slots and complete is None:
            raise OutputRecoveryStateError("kept slots require successful Complete")
        object.__setattr__(self, "publication_id", identity)
        object.__setattr__(self, "manifest_digest", digest)
        object.__setattr__(self, "node_death", _node_death(self.node_death))
        object.__setattr__(self, "owner_worker_id", _opaque(self.owner_worker_id, WorkerID, "owner"))
        _string(self.resolution_id, "resolution_id")
        object.__setattr__(self, "kept_slots", slots)
        object.__setattr__(self, "complete", complete)


@dataclass(frozen=True)
class OutputRecoverySnapshot(_WireValue):
    manifest: OutputPublicationManifest
    armed: bool = False
    complete: Optional[OutputPublicationCompleteWitness] = None
    frozen_node_death: Optional[NodeDeathRecord] = None
    owner_death: Optional[WorkerDeathRecord] = None
    rollback: Optional[OutputPublicationRollbackTombstone] = None
    adopted: Optional[OutputPublicationAdoptionProof] = None
    slot_collections: Tuple[OutputPublicationSlotCleanupProof, ...] = ()
    owner_decision: Optional[OutputRecoveryOwnerDecisionRecord] = None
    resolution: Optional[OutputRecoveryResolution] = None
    owner_cleaned: Optional[WorkerDeathRecord] = None

    def __post_init__(self):
        _require_type(self.manifest, OutputPublicationManifest, "manifest")
        manifest = replace(self.manifest)
        _require_type(self.armed, bool, "armed")
        complete = None if self.complete is None else _validate_witness(manifest, self.complete)
        if complete is not None and not self.armed:
            raise OutputRecoveryStateError("successful Complete requires the ARM fact")
        node_death = None if self.frozen_node_death is None else _node_death(self.frozen_node_death)
        if node_death is not None and (
            node_death.reason is not NodeDeathReason.PROCESS_EXIT
            or _incarnation_for_death(node_death) != manifest.header.node_incarnation
        ):
            raise OutputRecoveryConflictError("frozen death names another publishing Node incarnation")
        owner_death = None if self.owner_death is None else _owner_death(self.owner_death)
        if owner_death is not None and (
            owner_death.reason is WorkerDeathReason.EXPECTED
            or owner_death.worker_id != manifest.header.owner_worker_id
        ):
            raise OutputRecoveryConflictError("owner death must prove this exact owner's failure")
        rollback = None if self.rollback is None else _validate_rollback(
            manifest, self.rollback, armed=self.armed
        )
        adopted = None if self.adopted is None else _validate_owner_proof(
            manifest, self.adopted, OutputPublicationAdoptionProof
        )
        collections = tuple(
            _validate_owner_proof(manifest, proof, OutputPublicationSlotCleanupProof)
            for proof in _sequence(self.slot_collections, "slot_collections")
        )
        if tuple(proof.slot_index for proof in collections) != tuple(sorted(
            set(proof.slot_index for proof in collections)
        )):
            raise OutputRecoveryConflictError("slot collections must be unique and ordered")
        if rollback is not None and (complete is not None or adopted is not None or collections):
            raise OutputRecoveryStateError("rollback cannot coexist with successful publication")
        resolved_complete = (self.resolution.complete
                             if type(self.resolution) is OutputRecoveryResolution else None)
        if (adopted is not None and adopted.complete != complete
                or any(proof.complete != (complete or resolved_complete) for proof in collections)):
            raise OutputRecoveryConflictError("owner terminal proof requires the same Complete fact")
        decision = self.owner_decision
        if decision is not None:
            _require_type(decision, OutputRecoveryOwnerDecisionRecord, "owner_decision")
            decision = replace(decision)
            if (decision.publication_id != manifest.publication_id
                    or decision.manifest_digest != manifest.manifest_digest
                    or decision.owner_worker_id != manifest.header.owner_worker_id):
                raise OutputRecoveryConflictError("owner decision changed the frozen publication")
            if node_death is None or not self.armed or rollback is not None:
                raise OutputRecoveryStateError("owner decision requires armed Node-loss work")
            if complete is not None and decision.complete != complete:
                raise OutputRecoveryConflictError("owner decision contradicts known Complete")
            if self.resolution is None and any(decision.slots[proof.slot_index].decision is OutputRecoveryOwnerDecision.KEEP
                                               for proof in collections):
                raise OutputRecoveryConflictError("an already collected slot cannot be kept")
        resolution = self.resolution
        if resolution is not None:
            _require_type(resolution, OutputRecoveryResolution, "resolution")
            resolution = replace(resolution)
            if (resolution.publication_id != manifest.publication_id
                    or resolution.manifest_digest != manifest.manifest_digest
                    or resolution.node_death != node_death
                    or resolution.owner_worker_id != manifest.header.owner_worker_id):
                raise OutputRecoveryConflictError("resolution changed frozen publication")
            if decision is not None:
                kept = tuple(item.slot_index for item in decision.slots if item.decision is OutputRecoveryOwnerDecision.KEEP)
                if resolution.kept_slots != kept or resolution.complete != decision.complete:
                    raise OutputRecoveryConflictError("resolution changed owner decision")
            elif resolution.kept_slots or self.armed and rollback is None:
                raise OutputRecoveryStateError("armed resolution requires an owner decision")
        owner_cleaned = None if self.owner_cleaned is None else _owner_death(self.owner_cleaned)
        if owner_cleaned is not None and owner_cleaned != owner_death:
            raise OutputRecoveryConflictError("owner cleanup must match frozen owner death")
        for name, value in (
            ("manifest", manifest), ("complete", complete),
            ("frozen_node_death", node_death), ("owner_death", owner_death),
            ("rollback", rollback), ("adopted", adopted),
            ("slot_collections", collections), ("owner_decision", decision),
            ("resolution", resolution),
            ("owner_cleaned", owner_cleaned),
        ):
            object.__setattr__(self, name, value)

    @property
    def publication_id(self):
        return self.manifest.publication_id

    @property
    def manifest_digest(self):
        return self.manifest.manifest_digest

    @property
    def forward_allowed(self) -> bool:
        return (self.frozen_node_death is None and self.owner_death is None
                and self.rollback is None and self.complete is None
                and self.adopted is None and not self.slot_collections)

    @property
    def terminal_report_allowed(self) -> bool:
        # An owner can report adoption before the terminal outbox is delivered.
        # Same-witness terminal replay must still drain that outbox afterwards.
        return (self.armed and self.frozen_node_death is None
                and self.owner_death is None and self.rollback is None)

    @property
    def recovery_action(self) -> OutputRecoveryAction:
        if self.rollback is not None or not self.armed:
            return OutputRecoveryAction.PRECOMPLETE_ROLLBACK
        if self.complete is None:
            return OutputRecoveryAction.COMPLETION_UNKNOWN
        return OutputRecoveryAction.POSTCOMPLETE_RESOLVE


@dataclass(frozen=True)
class OutputRecoveryAck(_WireValue):
    stage: OutputRecoveryStage
    disposition: OutputRecoveryDisposition
    snapshot: OutputRecoverySnapshot

    def __post_init__(self):
        _require_type(self.stage, OutputRecoveryStage, "stage")
        _require_type(self.disposition, OutputRecoveryDisposition, "disposition")
        _require_type(self.snapshot, OutputRecoverySnapshot, "snapshot")
        snapshot = replace(self.snapshot)
        if self.disposition is not OutputRecoveryDisposition.FENCED:
            present = {
                OutputRecoveryStage.INTENT: True,
                OutputRecoveryStage.ARM_COMPLETE: snapshot.armed,
                OutputRecoveryStage.TERMINAL: snapshot.complete is not None,
                OutputRecoveryStage.ROLLED_BACK: snapshot.rollback is not None,
                OutputRecoveryStage.ADOPTED: snapshot.adopted is not None,
                OutputRecoveryStage.SLOT_COLLECTED: bool(snapshot.slot_collections),
                OutputRecoveryStage.OWNER_DECIDED: snapshot.owner_decision is not None,
                OutputRecoveryStage.RESOLVED: snapshot.resolution is not None,
                OutputRecoveryStage.OWNER_CLEANED: snapshot.owner_cleaned is not None,
            }
            if not present[self.stage]:
                raise OutputRecoveryStateError("ACK does not contain its acknowledged fact")
            if (self.stage in (OutputRecoveryStage.INTENT, OutputRecoveryStage.ARM_COMPLETE)
                    and not snapshot.forward_allowed):
                raise OutputRecoveryStateError("a fenced snapshot cannot authorize forward work")
            if self.stage is OutputRecoveryStage.TERMINAL and not snapshot.terminal_report_allowed:
                raise OutputRecoveryStateError("terminal ACK cannot bypass a frozen history")
            if self.stage is OutputRecoveryStage.OWNER_DECIDED and snapshot.owner_death is not None:
                raise OutputRecoveryStateError("owner-death work cannot accept a new custody decision")
        object.__setattr__(self, "snapshot", snapshot)


@dataclass(frozen=True)
class OutputRecoveryWork(_WireValue):
    death: NodeDeathRecord
    snapshot: OutputRecoverySnapshot
    action: OutputRecoveryAction

    def __post_init__(self):
        death = _node_death(self.death)
        _require_type(self.snapshot, OutputRecoverySnapshot, "work snapshot")
        snapshot = replace(self.snapshot)
        _require_type(self.action, OutputRecoveryAction, "work action")
        if death != snapshot.frozen_node_death or self.action is not snapshot.recovery_action:
            raise OutputRecoveryConflictError("work changed its frozen death or history")
        object.__setattr__(self, "death", death)
        object.__setattr__(self, "snapshot", snapshot)

    @property
    def publication_id(self):
        return self.snapshot.publication_id

    @property
    def manifest(self):
        return self.snapshot.manifest


@dataclass(frozen=True)
class OutputOwnerDeathWork(_WireValue):
    death: WorkerDeathRecord
    snapshot: OutputRecoverySnapshot

    def __post_init__(self):
        death = _owner_death(self.death)
        _require_type(self.snapshot, OutputRecoverySnapshot, "owner work snapshot")
        snapshot = replace(self.snapshot)
        if death != snapshot.owner_death:
            raise OutputRecoveryConflictError("owner work changed its frozen death")
        object.__setattr__(self, "death", death)
        object.__setattr__(self, "snapshot", snapshot)

    @property
    def publication_id(self):
        return self.snapshot.publication_id


class OutputPublicationRecoveryAuthority:
    """One lock linearizes admission, lifecycle facts and both death fences.

    Reports are not permissions to send arbitrary effects.  The Node adapter
    validates INTENT/ARM ACK stage, identity and disposition before advancing
    its own journal.  FENCED is an observable non-authorizing response.
    """

    def __init__(self):
        self._lock = RLock()
        self._records: dict[OutputPublicationID, OutputRecoverySnapshot] = {}
        self._by_node: dict[NodeID, set[OutputPublicationID]] = {}
        self._by_owner: dict[WorkerID, set[OutputPublicationID]] = {}
        self._node_incarnations: dict[NodeID, OutputPublicationNodeIncarnation] = {}
        self._node_deaths: dict[NodeID, NodeDeathRecord] = {}
        self._node_detections: dict[str, NodeDeathRecord] = {}
        self._node_death_epochs: dict[int, NodeDeathRecord] = {}
        self._node_work: dict[NodeID, Tuple[OutputRecoveryWork, ...]] = {}
        self._owner_deaths: dict[WorkerID, WorkerDeathRecord] = {}
        self._owner_detections: dict[str, WorkerDeathRecord] = {}
        self._owner_death_epochs: dict[int, WorkerDeathRecord] = {}
        self._owner_work: dict[WorkerID, Tuple[OutputOwnerDeathWork, ...]] = {}

    def report_intent(self, manifest: OutputPublicationManifest) -> OutputRecoveryAck:
        _require_type(manifest, OutputPublicationManifest, "manifest")
        manifest = replace(manifest)
        with self._lock:
            prior = self._records.get(manifest.publication_id)
            if prior is not None:
                self._same_manifest(prior, manifest)
                return self._ack(OutputRecoveryStage.INTENT, prior,
                                 OutputRecoveryDisposition.ALREADY_RECORDED if prior.forward_allowed
                                 else OutputRecoveryDisposition.FENCED)
            self._validate_admission(manifest)
            snapshot = OutputRecoverySnapshot(manifest)
            self._install(snapshot)
            return self._ack(OutputRecoveryStage.INTENT, snapshot)

    def arm_complete(self, publication_id: OutputPublicationID, digest: str) -> OutputRecoveryAck:
        with self._lock:
            snapshot = self._record(publication_id)
            self._same_digest(snapshot, digest)
            if not snapshot.forward_allowed:
                return self._ack(OutputRecoveryStage.ARM_COMPLETE, snapshot, OutputRecoveryDisposition.FENCED)
            if snapshot.armed:
                return self._ack(OutputRecoveryStage.ARM_COMPLETE, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            return self._commit(OutputRecoveryStage.ARM_COMPLETE, replace(snapshot, armed=True))

    def report_terminal(self, witness: OutputPublicationCompleteWitness) -> OutputRecoveryAck:
        _require_type(witness, OutputPublicationCompleteWitness, "complete")
        witness = replace(witness)
        with self._lock:
            snapshot = self._record(witness.publication_id)
            _validate_witness(snapshot.manifest, witness)
            if snapshot.rollback is not None:
                raise OutputRecoveryStateError("rolled-back publication cannot Complete")
            if snapshot.frozen_node_death is not None or snapshot.owner_death is not None:
                return self._ack(OutputRecoveryStage.TERMINAL, snapshot, OutputRecoveryDisposition.FENCED)
            if not snapshot.armed:
                raise OutputRecoveryStateError("terminal report requires ARM")
            if snapshot.complete is not None:
                return self._ack(OutputRecoveryStage.TERMINAL, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            return self._commit(OutputRecoveryStage.TERMINAL, replace(snapshot, complete=witness))

    def report_rollback(
        self, tombstone: OutputPublicationRollbackTombstone, *, manifest: OutputPublicationManifest,
    ) -> OutputRecoveryAck:
        _require_type(manifest, OutputPublicationManifest, "manifest")
        manifest = replace(manifest)
        tombstone = _validate_rollback(manifest, tombstone, armed=False)
        with self._lock:
            snapshot = self._records.get(manifest.publication_id)
            if snapshot is None:
                # Lost/unsent INTENT cannot make an exact local rollback vanish:
                # create its full manifest tombstone before a late INTENT arrives.
                self._validate_admission(manifest)
                snapshot = OutputRecoverySnapshot(manifest, rollback=tombstone)
                self._install(snapshot)
                return self._ack(OutputRecoveryStage.ROLLED_BACK, snapshot)
            self._same_manifest(snapshot, manifest)
            tombstone = _validate_rollback(manifest, tombstone, armed=snapshot.armed)
            if snapshot.complete is not None or snapshot.adopted is not None:
                raise OutputRecoveryStateError("rollback is forbidden after successful Complete")
            if snapshot.rollback is not None:
                if snapshot.rollback != tombstone:
                    raise OutputRecoveryConflictError("rollback proof was rebound")
                return self._ack(OutputRecoveryStage.ROLLED_BACK, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            if snapshot.frozen_node_death is not None or snapshot.owner_death is not None:
                return self._ack(OutputRecoveryStage.ROLLED_BACK, snapshot, OutputRecoveryDisposition.FENCED)
            return self._commit(OutputRecoveryStage.ROLLED_BACK, replace(snapshot, rollback=tombstone))

    def report_adopted(self, proof: OutputPublicationAdoptionProof) -> OutputRecoveryAck:
        _require_type(proof, OutputPublicationAdoptionProof, "adoption")
        proof = replace(proof)
        with self._lock:
            snapshot = self._record(proof.complete.publication_id)
            proof = _validate_owner_proof(snapshot.manifest, proof, OutputPublicationAdoptionProof)
            if snapshot.rollback is not None:
                raise OutputRecoveryStateError("rolled-back publication cannot be adopted")
            if snapshot.adopted is not None:
                if snapshot.adopted != proof:
                    raise OutputRecoveryConflictError("owner adoption proof was rebound")
                return self._ack(OutputRecoveryStage.ADOPTED, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            if snapshot.frozen_node_death is not None or snapshot.owner_death is not None:
                return self._ack(OutputRecoveryStage.ADOPTED, snapshot, OutputRecoveryDisposition.FENCED)
            if not snapshot.armed:
                raise OutputRecoveryStateError("adoption requires ARM")
            return self._commit(OutputRecoveryStage.ADOPTED, replace(
                snapshot, complete=proof.complete, adopted=proof
            ))

    def report_slot_collected(self, proof: OutputPublicationSlotCleanupProof) -> OutputRecoveryAck:
        _require_type(proof, OutputPublicationSlotCleanupProof, "slot collection")
        proof = replace(proof)
        with self._lock:
            snapshot = self._record(proof.complete.publication_id)
            proof = _validate_owner_proof(snapshot.manifest, proof, OutputPublicationSlotCleanupProof)
            if snapshot.rollback is not None:
                raise OutputRecoveryStateError("rolled-back publication has no completed slot")
            previous = next((item for item in snapshot.slot_collections if item.slot_index == proof.slot_index), None)
            if previous is not None:
                if previous != proof:
                    raise OutputRecoveryConflictError("slot collection proof was rebound")
                return self._ack(OutputRecoveryStage.SLOT_COLLECTED, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            if snapshot.owner_death is not None or (snapshot.frozen_node_death is not None
                    and snapshot.resolution is None and snapshot.adopted is None):
                return self._ack(OutputRecoveryStage.SLOT_COLLECTED, snapshot, OutputRecoveryDisposition.FENCED)
            if not snapshot.armed:
                raise OutputRecoveryStateError("slot collection requires ARM")
            collections = tuple(sorted(snapshot.slot_collections + (proof,), key=lambda item: item.slot_index))
            return self._commit(OutputRecoveryStage.SLOT_COLLECTED, replace(
                snapshot, complete=proof.complete if snapshot.frozen_node_death is None else snapshot.complete,
                slot_collections=collections
            ))

    def freeze_node_death(self, death: NodeDeathRecord) -> Tuple[OutputRecoveryWork, ...]:
        death = _node_death(death)
        if death.reason is NodeDeathReason.EXPECTED:
            return ()
        incarnation = _incarnation_for_death(death)
        with self._lock:
            self._check_incarnation(incarnation)
            self._check_death_identity(death, self._node_detections, self._node_death_epochs)
            previous = self._node_deaths.get(death.node_id)
            if previous is not None:
                if previous != death:
                    raise OutputRecoveryConflictError("publishing Node death was rebound")
                return tuple(replace(work) for work in self._node_work[death.node_id])
            work = tuple(
                OutputRecoveryWork(death, replace(self._records[value], frozen_node_death=death),
                                   self._records[value].recovery_action)
                for value in sorted(self._by_node.get(death.node_id, ()), key=_publication_order)
            )
            # Deep construction of every work item precedes all mutations.
            self._node_incarnations[death.node_id] = incarnation
            self._node_deaths[death.node_id] = death
            self._node_detections[death.detection_id] = death
            self._node_death_epochs[death.death_epoch] = death
            self._node_work[death.node_id] = work
            for item in work:
                self._records[item.publication_id] = item.snapshot
            return tuple(replace(item) for item in work)

    def frozen_workset(self, death: NodeDeathRecord) -> Tuple[OutputRecoveryWork, ...]:
        death = _node_death(death)
        with self._lock:
            if self._node_deaths.get(death.node_id) != death:
                raise OutputRecoveryConflictError("query requires the exact frozen Node death")
            return tuple(replace(item) for item in self._node_work[death.node_id])

    def decide_owner(
        self, work: OutputRecoveryWork, owner_worker_id: WorkerID,
        decisions: Tuple[OutputSlotDecision, ...], *, decision_id: str,
        complete: Optional[OutputPublicationCompleteWitness] = None,
    ) -> OutputRecoveryAck:
        """Record an exact per-slot custody decision after Node loss.

        A Complete witness is required for KEEP but is not proof that bytes
        exist.  The owner adapter must first validate custody and fence DROP
        slots under its delivery lock.  The registry never reconstructs bytes.
        """
        _require_type(work, OutputRecoveryWork, "work")
        work = replace(work)
        decision = OutputRecoveryOwnerDecisionRecord(
            work.publication_id, work.manifest.manifest_digest, owner_worker_id,
            decision_id, decisions, complete,
        )
        with self._lock:
            snapshot = self._exact_work(work)
            if decision.owner_worker_id != snapshot.manifest.header.owner_worker_id:
                raise OutputRecoveryConflictError("decision must come from the exact output owner")
            if work.action is OutputRecoveryAction.PRECOMPLETE_ROLLBACK:
                raise OutputRecoveryStateError("proven pre-Complete work has no owner custody choice")
            if snapshot.owner_decision is not None and snapshot.owner_decision != decision:
                raise OutputRecoveryConflictError("owner decision vector was rebound")
            if snapshot.owner_death is not None:
                return self._ack(OutputRecoveryStage.OWNER_DECIDED, snapshot, OutputRecoveryDisposition.FENCED)
            if snapshot.owner_decision is not None:
                return self._ack(OutputRecoveryStage.OWNER_DECIDED, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            return self._commit(OutputRecoveryStage.OWNER_DECIDED, replace(snapshot, owner_decision=decision))

    def freeze_owner_death(self, death: WorkerDeathRecord) -> Tuple[OutputOwnerDeathWork, ...]:
        death = _owner_death(death)
        if death.reason is WorkerDeathReason.EXPECTED:
            return ()
        incarnation = OutputPublicationNodeIncarnation(
            death.node_id, death.node_pid, death.node_registration_epoch
        )
        with self._lock:
            self._check_incarnation(incarnation)
            self._check_death_identity(death, self._owner_detections, self._owner_death_epochs)
            previous = self._owner_deaths.get(death.worker_id)
            if previous is not None:
                if previous != death:
                    raise OutputRecoveryConflictError("outer-owner death was rebound")
                return tuple(replace(item) for item in self._owner_work[death.worker_id])
            work = tuple(
                OutputOwnerDeathWork(death, replace(self._records[value], owner_death=death))
                for value in sorted(self._by_owner.get(death.worker_id, ()), key=_publication_order)
            )
            self._node_incarnations[death.node_id] = incarnation
            self._owner_deaths[death.worker_id] = death
            self._owner_detections[death.detection_id] = death
            self._owner_death_epochs[death.death_epoch] = death
            self._owner_work[death.worker_id] = work
            for item in work:
                self._records[item.publication_id] = item.snapshot
            return tuple(replace(item) for item in work)

    def resolve_node_loss(self, work: OutputRecoveryWork, resolution: OutputRecoveryResolution) -> OutputRecoveryAck:
        """Record exact cleanup completion after the control adapter's ACKs."""
        _require_type(work, OutputRecoveryWork, "work")
        _require_type(resolution, OutputRecoveryResolution, "resolution")
        work, resolution = replace(work), replace(resolution)
        with self._lock:
            snapshot = self._exact_work(work)
            if snapshot.resolution is not None:
                if snapshot.resolution != resolution:
                    raise OutputRecoveryConflictError("Node-loss resolution was rebound")
                return self._ack(OutputRecoveryStage.RESOLVED, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            if snapshot.owner_death is not None:
                return self._ack(OutputRecoveryStage.RESOLVED, snapshot, OutputRecoveryDisposition.FENCED)
            return self._commit(OutputRecoveryStage.RESOLVED, replace(snapshot, resolution=resolution))

    def resolve_owner_death(self, work: OutputOwnerDeathWork) -> OutputRecoveryAck:
        """Finish only the original owner-death work after external ACKs."""
        _require_type(work, OutputOwnerDeathWork, "owner work")
        work = replace(work)
        with self._lock:
            canonical = next((item for item in self._owner_work.get(work.death.worker_id, ())
                              if item.publication_id == work.publication_id), None)
            if canonical != work:
                raise OutputRecoveryConflictError("owner cleanup changed original frozen work")
            snapshot = self._record(work.publication_id)
            if snapshot.owner_cleaned is not None:
                return self._ack(OutputRecoveryStage.OWNER_CLEANED, snapshot, OutputRecoveryDisposition.ALREADY_RECORDED)
            return self._commit(OutputRecoveryStage.OWNER_CLEANED, replace(snapshot, owner_cleaned=work.death))

    def frozen_owner_workset(self, death: WorkerDeathRecord) -> Tuple[OutputOwnerDeathWork, ...]:
        death = _owner_death(death)
        with self._lock:
            if self._owner_deaths.get(death.worker_id) != death:
                raise OutputRecoveryConflictError("query requires the exact frozen owner death")
            return tuple(replace(item) for item in self._owner_work[death.worker_id])

    def snapshot(self, publication_id: OutputPublicationID) -> OutputRecoverySnapshot:
        with self._lock:
            return replace(self._record(publication_id))

    def publication_ids(self) -> Tuple[OutputPublicationID, ...]:
        with self._lock:
            return tuple(replace(value) for value in sorted(self._records, key=_publication_order))

    def _exact_work(self, work):
        snapshot = self._record(work.publication_id)
        expected = next((item for item in self._node_work.get(work.death.node_id, ())
                         if item.publication_id == work.publication_id), None)
        if expected is None or expected != work:
            raise OutputRecoveryConflictError("operation requires the original frozen work")
        return snapshot

    def _record(self, publication_id):
        _require_type(publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(publication_id)
        try:
            return self._records[publication_id]
        except KeyError as exc:
            raise UnknownOutputRecoveryError("publication intent is unknown") from exc

    @staticmethod
    def _same_manifest(snapshot, manifest):
        if snapshot.manifest != manifest:
            raise OutputRecoveryConflictError("publication manifest was rebound")

    @staticmethod
    def _same_digest(snapshot, digest):
        if snapshot.manifest_digest != _checksum(digest, "manifest_digest"):
            raise OutputRecoveryConflictError("publication digest changed")

    def _check_incarnation(self, incarnation):
        incarnation = _node_incarnation(incarnation)
        known = self._node_incarnations.get(incarnation.node_id)
        if known is not None and known != incarnation:
            raise OutputRecoveryConflictError("NodeID was rebound to another process incarnation")

    @staticmethod
    def _check_death_identity(death, detections, epochs):
        if any(prior is not None and prior != death for prior in (
            detections.get(death.detection_id), epochs.get(death.death_epoch),
        )):
            raise OutputRecoveryConflictError("death detection or epoch was rebound")

    def _validate_admission(self, manifest):
        self._check_incarnation(manifest.header.node_incarnation)
        if manifest.header.node_incarnation.node_id in self._node_deaths:
            raise OutputRecoveryStateError("publishing Node is death-frozen")
        if manifest.header.owner_worker_id in self._owner_deaths:
            raise OutputRecoveryStateError("publication owner is death-frozen")

    def _install(self, snapshot):
        manifest = snapshot.manifest
        node = manifest.header.node_incarnation
        self._records[snapshot.publication_id] = snapshot
        self._node_incarnations[node.node_id] = node
        self._by_node.setdefault(node.node_id, set()).add(snapshot.publication_id)
        self._by_owner.setdefault(manifest.header.owner_worker_id, set()).add(snapshot.publication_id)

    def _commit(self, stage, snapshot):
        self._records[snapshot.publication_id] = snapshot
        return self._ack(stage, snapshot)

    @staticmethod
    def _ack(stage, snapshot, disposition=OutputRecoveryDisposition.APPLIED):
        return OutputRecoveryAck(stage, disposition, snapshot)


def _publication_order(value):
    return (bytes(value.task_id), value.attempt_id.attempt_number, bytes(value.lease_id),
            tuple(item.return_index for item in value.full_output_ids),
            tuple(item.return_index for item in value.output_ids))


__all__ = [
    "OutputPublicationRecoveryAuthority", "OutputRecoveryConflictError",
    "OutputRecoveryStateError", "UnknownOutputRecoveryError", "OutputRecoveryStage",
    "OutputRecoveryDisposition", "OutputRecoveryAction", "OutputRecoveryOwnerDecision",
    "OutputSlotDecision", "OutputRecoveryOwnerDecisionRecord", "OutputRecoverySnapshot",
    "OutputRecoveryAck", "OutputRecoveryWork", "OutputOwnerDeathWork",
]
