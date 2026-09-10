"""One Node-local journal for a single-output publication.

The control history is metadata-only.  INLINE payloads live exclusively in the
active ``result`` cache and leave it after rollback or an exact retirement
proof.  Storage tier changes materialization, not the publication lifecycle.
Retirement forgets this journal's reply cache, not physical STORED replicas.
Physical replica GC has its own owner-authorized deletion path; it must not
replace an existing payload-retirement proof with a later GC proof.

Every external operation has an intent before its effect and a typed ACK
afterwards.  Missing ACKs never erase possible effects: rollback uses intents.
This module performs no RPC, object-store mutation, owner CAS, or lease-ledger
release.  A Node adapter composes those effects with this journal.  The adapter
must fence late materialization as well: rejecting a delayed ACK here cannot
undo a physical seal that raced after its compensating DROP.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, fields, replace
from enum import Enum
from threading import RLock
from typing import Optional, Tuple

from .ids import ObjectID, WorkerID
from .output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationError, OutputPublicationID,
    OutputPublicationManifest, _checksum, _descriptor, _object_id, _opaque,
    _require_type, _sequence, _string, _uint,
)
from .protocol import ResultDescriptor


class OutputPublicationJournalStateError(OutputPublicationError):
    """A request contradicts the publication's local lifecycle."""


class UnknownOutputPublicationError(OutputPublicationJournalStateError, LookupError):
    """The Node has not opened this exact publication."""


class OutputPublicationPayloadRetired(OutputPublicationJournalStateError):
    """Complete is known but the Node no longer owns all reply payloads."""

    def __init__(self, publication_id, tombstone):
        self.publication_id = replace(publication_id)
        self.tombstone = replace(tombstone)
        super().__init__("publication payload was retired; consult its typed tombstone")


class OutputPublicationJournalState(str, Enum):
    ACTIVE = "ACTIVE"
    ROLLING_BACK = "ROLLING_BACK"
    COMPLETED = "COMPLETED"
    RETIRED = "RETIRED"


class OutputPublicationStage(str, Enum):
    OWNER_REGISTER = "OWNER_REGISTER"
    PREPARE = "PREPARE"
    MATERIALIZE = "MATERIALIZE"
    PROMOTE = "PROMOTE"
    SLOT_DROP = "SLOT_DROP"
    FINAL_RELEASE = "FINAL_RELEASE"
    PROVISIONAL_RELEASE = "PROVISIONAL_RELEASE"


class OutputPublicationAckDisposition(str, Enum):
    APPLIED = "APPLIED"
    ALREADY_APPLIED = "ALREADY_APPLIED"


_TRANSFER_STAGES = frozenset((
    OutputPublicationStage.PREPARE, OutputPublicationStage.PROMOTE,
    OutputPublicationStage.FINAL_RELEASE, OutputPublicationStage.PROVISIONAL_RELEASE,
))
_ROLLBACK_STAGES = frozenset((
    OutputPublicationStage.SLOT_DROP,
    OutputPublicationStage.FINAL_RELEASE, OutputPublicationStage.PROVISIONAL_RELEASE,
))


class _WireValue:
    def __reduce__(self):
        return type(self), tuple(getattr(self, value.name) for value in fields(self))


@dataclass(frozen=True)
class OutputPublicationEffect(_WireValue):
    """Exact stage key; only child operations carry a transfer index."""

    publication_id: OutputPublicationID
    manifest_digest: str
    stage: OutputPublicationStage
    transfer_index: Optional[int] = None

    def __post_init__(self) -> None:
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        _require_type(self.stage, OutputPublicationStage, "stage")
        object.__setattr__(self, "publication_id", replace(self.publication_id))
        object.__setattr__(self, "manifest_digest", _checksum(self.manifest_digest, "manifest_digest"))
        if self.stage in _TRANSFER_STAGES:
            _uint(self.transfer_index, "transfer_index")
        elif self.transfer_index is not None:
            raise OutputPublicationConflictError("non-transfer effect cannot carry a transfer_index")


@dataclass(frozen=True)
class OutputPublicationAck(_WireValue):
    """Metadata ACK after an adapter has validated the operation's reply."""

    effect: OutputPublicationEffect
    disposition: OutputPublicationAckDisposition = OutputPublicationAckDisposition.APPLIED

    def __post_init__(self) -> None:
        _require_type(self.effect, OutputPublicationEffect, "effect")
        _require_type(self.disposition, OutputPublicationAckDisposition, "disposition")
        object.__setattr__(self, "effect", replace(self.effect))


@dataclass(frozen=True)
class OutputPublicationRollbackPlan(_WireValue):
    publication_id: OutputPublicationID
    manifest_digest: str
    rollback_id: str
    effects: Tuple[OutputPublicationEffect, ...]

    def __post_init__(self) -> None:
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(self.publication_id)
        digest = _checksum(self.manifest_digest, "manifest_digest")
        _string(self.rollback_id, "rollback_id")
        effects = []
        for value in _sequence(self.effects, "effects"):
            _require_type(value, OutputPublicationEffect, "effect")
            value = replace(value)
            if (value.publication_id != publication_id or value.manifest_digest != digest
                    or value.stage not in _ROLLBACK_STAGES):
                raise OutputPublicationConflictError("rollback effect changed publication identity")
            effects.append(value)
        if len(effects) != len(set(effects)):
            raise OutputPublicationConflictError("rollback effects must be unique")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "manifest_digest", digest)
        object.__setattr__(self, "effects", tuple(effects))


@dataclass(frozen=True)
class OutputPublicationRollbackTombstone(_WireValue):
    plan: OutputPublicationRollbackPlan
    acknowledgements: Tuple[OutputPublicationAck, ...]

    def __post_init__(self) -> None:
        _require_type(self.plan, OutputPublicationRollbackPlan, "plan")
        plan = replace(self.plan)
        values = []
        for ack in _sequence(self.acknowledgements, "acknowledgements"):
            _require_type(ack, OutputPublicationAck, "acknowledgement")
            values.append(replace(ack))
        if tuple(value.effect for value in values) != plan.effects:
            raise OutputPublicationConflictError("rollback terminal needs every ordered effect ACK")
        object.__setattr__(self, "plan", plan)
        object.__setattr__(self, "acknowledgements", tuple(values))


@dataclass(frozen=True)
class OutputPublicationAdoptionProof(_WireValue):
    """An adapter-validated whole-batch owner CAS, never a bare clear flag."""

    complete: OutputPublicationCompleteWitness
    owner_worker_id: WorkerID
    owner_commit_id: str

    def __post_init__(self) -> None:
        _require_type(self.complete, OutputPublicationCompleteWitness, "complete")
        object.__setattr__(self, "complete", replace(self.complete))
        object.__setattr__(self, "owner_worker_id", _opaque(self.owner_worker_id, WorkerID, "owner_worker_id"))
        _string(self.owner_commit_id, "owner_commit_id")
OutputPublicationRetirementProof = OutputPublicationAdoptionProof


@dataclass(frozen=True)
class OutputPublicationTombstone(_WireValue):
    publication_id: OutputPublicationID
    manifest_digest: str
    object_id: ObjectID
    proof: OutputPublicationRetirementProof

    def __post_init__(self) -> None:
        _require_type(self.publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(self.publication_id)
        digest = _checksum(self.manifest_digest, "manifest_digest")
        object_id = _object_id(self.object_id)
        proof = _retirement_proof(self.proof)
        if (
            proof.complete.publication_id != publication_id
            or proof.complete.manifest_digest != digest
            or publication_id.object_id != object_id
        ):
            raise OutputPublicationConflictError("retirement proof changed its identity")
        object.__setattr__(self, "publication_id", publication_id)
        object.__setattr__(self, "manifest_digest", digest)
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "proof", proof)


def _retirement_proof(value):
    if type(value) is not OutputPublicationAdoptionProof:
        raise TypeError("proof must be an OutputPublicationAdoptionProof")
    return replace(value)


@dataclass(frozen=True)
class OutputPublicationJournalSnapshot:
    """Metadata diagnostics; payload access is a separate Node-local method."""

    manifest: OutputPublicationManifest
    state: OutputPublicationJournalState
    intents: Tuple[OutputPublicationEffect, ...]
    acknowledgements: Tuple[OutputPublicationAck, ...]
    materialized: bool
    result_retained: bool
    complete: Optional[OutputPublicationCompleteWitness]
    rollback: Optional[OutputPublicationRollbackPlan]
    rollback_tombstone: Optional[OutputPublicationRollbackTombstone]
    retirement: Optional[OutputPublicationTombstone]
    ready_to_complete: bool

    @property
    def publication_id(self):
        return self.manifest.publication_id


@dataclass
class _Record:
    manifest: OutputPublicationManifest
    state: OutputPublicationJournalState = OutputPublicationJournalState.ACTIVE
    intents: set[OutputPublicationEffect] = field(default_factory=set)
    acknowledgements: dict[OutputPublicationEffect, OutputPublicationAck] = field(default_factory=dict)
    result: Optional[ResultDescriptor] = None
    complete: Optional[OutputPublicationCompleteWitness] = None
    rollback: Optional[OutputPublicationRollbackPlan] = None
    rollback_tombstone: Optional[OutputPublicationRollbackTombstone] = None
    retirement: Optional[OutputPublicationTombstone] = None
    adoption_proof: Optional[OutputPublicationAdoptionProof] = None
    owner_death: object = None


class OutputPublicationJournal:
    """Local intent/ACK reducer; one execution owns one frozen manifest."""

    def __init__(self) -> None:
        self._records: dict[OutputPublicationID, _Record] = {}
        self._lock = RLock()

    @contextmanager
    def linearize(self, publication_id: Optional[OutputPublicationID] = None):
        """Compose only local Complete/ledger work; never RPC under this lock.

        The Node adapter must also serialize the matching lease record and
        Worker-loss handler in one consistent lock order.  Preflight fallible
        ledger validation before complete(); once the journal records Complete,
        an effect-then-error recovery must finish that same local terminal,
        never choose rollback or a fresh execution.
        Omitting the ID permits first admission under the same journal-before-
        Node lock order, without installing an unvalidated lease manifest.
        """
        with self._lock:
            if publication_id is not None:
                self._record(publication_id)
            yield

    def open(self, manifest: OutputPublicationManifest) -> bool:
        _require_type(manifest, OutputPublicationManifest, "manifest")
        manifest = replace(manifest)
        with self._lock:
            previous = self._records.get(manifest.publication_id)
            if previous is not None:
                if previous.manifest != manifest:
                    raise OutputPublicationConflictError("publication manifest was rebound")
                return False
            self._records[manifest.publication_id] = _Record(manifest)
            return True

    def begin_owner_register(self, publication_id: OutputPublicationID) -> OutputPublicationEffect:
        return self._begin(publication_id, OutputPublicationStage.OWNER_REGISTER)

    def ack_owner_registered(self, acknowledgement: OutputPublicationAck) -> bool:
        return self._ack_forward(acknowledgement, OutputPublicationStage.OWNER_REGISTER)

    def begin_prepare(self, publication_id: OutputPublicationID, transfer_index: int) -> OutputPublicationEffect:
        return self._begin(publication_id, OutputPublicationStage.PREPARE, transfer_index)

    def ack_prepared(self, acknowledgement: OutputPublicationAck) -> bool:
        return self._ack_forward(acknowledgement, OutputPublicationStage.PREPARE)

    def begin_materialize(self, publication_id: OutputPublicationID) -> OutputPublicationEffect:
        return self._begin(publication_id, OutputPublicationStage.MATERIALIZE)

    def ack_materialized(self, acknowledgement: OutputPublicationAck, descriptor: ResultDescriptor) -> bool:
        return self._ack_forward(
            acknowledgement, OutputPublicationStage.MATERIALIZE, descriptor=descriptor
        )

    def begin_promote(self, publication_id: OutputPublicationID, transfer_index: int) -> OutputPublicationEffect:
        return self._begin(publication_id, OutputPublicationStage.PROMOTE, transfer_index)

    def ack_promoted(self, acknowledgement: OutputPublicationAck) -> bool:
        return self._ack_forward(acknowledgement, OutputPublicationStage.PROMOTE)

    def acknowledged(self, effect: OutputPublicationEffect) -> bool:
        _require_type(effect, OutputPublicationEffect, "effect")
        effect = replace(effect)
        with self._lock:
            record = self._record(effect.publication_id)
            self._require_effect(record, effect)
            return effect in record.acknowledgements

    def complete(self, publication_id: OutputPublicationID, witness: OutputPublicationCompleteWitness) -> OutputPublicationEnvelope:
        """Cross the local boundary after owner registration and all result handoffs.

        First construction of the data-plane envelope is validated before any
        state change.  A retry returns the same value while every slot remains
        retained.  After retirement it raises OutputPublicationPayloadRetired,
        preserving the successful Complete without inventing missing bytes.
        Reporting Complete to the owner happens outside this local operation.
        """
        _require_type(witness, OutputPublicationCompleteWitness, "witness")
        witness = replace(witness)
        with self._lock:
            record = self._record(publication_id)
            if witness != OutputPublicationCompleteWitness.for_manifest(record.manifest):
                raise OutputPublicationConflictError("Complete witness changed publication identity")
            if record.complete is None:
                self._require_active(record)
                if not self._ready_to_complete(record):
                    raise OutputPublicationJournalStateError(
                        "Complete requires owner registration, materialization, and child promotion ACKs"
                    )
            if record.retirement is not None:
                raise OutputPublicationPayloadRetired(
                    record.manifest.publication_id,
                    record.retirement,
                )
            if record.result is None:
                raise OutputPublicationJournalStateError("Complete requires all local results")
            envelope = OutputPublicationEnvelope(
                record.manifest, witness, record.result
            )
            record.complete = witness
            record.state = OutputPublicationJournalState.COMPLETED
            return envelope

    def begin_rollback(self, publication_id: OutputPublicationID, rollback_id: str) -> OutputPublicationRollbackPlan:
        """Fence forward work and freeze compensation from possible effects.

        Successful preparation does not prove execution completion. The alive
        Node may roll back while its local Complete is absent. After Node loss
        this in-memory journal provides no durable recovery authority.
        """
        _string(rollback_id, "rollback_id")
        with self._lock:
            record = self._record(publication_id)
            if record.complete is not None:
                raise OutputPublicationJournalStateError("rollback is forbidden after Complete")
            if record.rollback is not None:
                if record.rollback.rollback_id != rollback_id:
                    raise OutputPublicationConflictError("rollback identity was rebound")
                return replace(record.rollback)
            self._require_active(record)
            effects = []
            if self._intended(record, OutputPublicationStage.MATERIALIZE):
                effects.append(self._effect(record, OutputPublicationStage.SLOT_DROP))
            for forward, inverse in ((OutputPublicationStage.PROMOTE, OutputPublicationStage.FINAL_RELEASE),
                                     (OutputPublicationStage.PREPARE, OutputPublicationStage.PROVISIONAL_RELEASE)):
                for transfer_index in reversed(self._transfer_indices(record)):
                    if self._intended(record, forward, transfer_index):
                        effects.append(self._effect(record, inverse, transfer_index))
            plan = OutputPublicationRollbackPlan(
                record.manifest.publication_id, record.manifest.manifest_digest, rollback_id, tuple(effects)
            )
            record.rollback = plan
            record.state = OutputPublicationJournalState.ROLLING_BACK
            self._finish_rollback_if_ready(record)
            return replace(plan)

    def next_rollback_effect(self, publication_id: OutputPublicationID) -> Optional[OutputPublicationEffect]:
        with self._lock:
            record = self._record(publication_id)
            if record.rollback is None:
                raise OutputPublicationJournalStateError("rollback was not begun")
            value = self._next_rollback(record)
            return None if value is None else replace(value)

    def ack_rollback(self, acknowledgement: OutputPublicationAck) -> bool:
        _require_type(acknowledgement, OutputPublicationAck, "acknowledgement")
        acknowledgement = replace(acknowledgement)
        effect = acknowledgement.effect
        with self._lock:
            record = self._record(effect.publication_id)
            self._require_effect(record, effect)
            if record.rollback is None or effect not in record.rollback.effects:
                raise OutputPublicationJournalStateError("effect is not part of this rollback")
            if effect in record.acknowledgements:
                return False
            if self._next_rollback(record) != effect:
                raise OutputPublicationJournalStateError("rollback ACK arrived out of order")
            record.acknowledgements[effect] = acknowledgement
            if effect.stage is OutputPublicationStage.SLOT_DROP:
                record.result = None
            self._finish_rollback_if_ready(record)
            return True

    def retire_completed(self, proof: OutputPublicationAdoptionProof) -> OutputPublicationTombstone:
        """Retire the single payload after validating its exact adoption proof."""
        _require_type(proof, OutputPublicationAdoptionProof, "proof")
        proof = replace(proof)
        with self._lock:
            record = self._record(proof.complete.publication_id)
            terminal = self._prepare_retirement(record, proof)
            self._commit_retirement(record, terminal)
            return replace(terminal)

    def retire_owner_death(self, publication_id: OutputPublicationID, death: object) -> None:
        """Forget reply custody only after exact owner-death cleanup.

        The Node adapter validates fence, stopped lease and physical cleanup.
        This does not pretend a rollback happened after successful Complete.
        """
        from .death_proofs import owner_death as _owner_death
        death = _owner_death(death)
        with self._lock:
            record = self._record(publication_id)
            if death.worker_id != record.manifest.header.owner_worker_id:
                raise OutputPublicationConflictError("cleanup names another owner")
            if record.owner_death is not None and record.owner_death != death:
                raise OutputPublicationConflictError("owner-death cleanup was rebound")
            record.owner_death = death
            record.result = None
            record.state = OutputPublicationJournalState.RETIRED

    def materialized_result(self, publication_id: OutputPublicationID) -> Optional[ResultDescriptor]:
        """Node data-plane cache lookup, never a GCS/history projection."""
        with self._lock:
            record = self._record(publication_id)
            result = record.result
            return None if result is None else _descriptor(result)

    def snapshot(self, publication_id: OutputPublicationID) -> OutputPublicationJournalSnapshot:
        with self._lock:
            record = self._record(publication_id)
            intents = tuple(sorted(record.intents, key=_effect_order))
            acknowledgements = tuple(record.acknowledgements[key]
                                     for key in sorted(record.acknowledgements, key=_effect_order))
            active = record.state is OutputPublicationJournalState.ACTIVE
            return OutputPublicationJournalSnapshot(
                replace(record.manifest), record.state,
                tuple(replace(value) for value in intents),
                tuple(replace(value) for value in acknowledgements),
                self._acked(record, OutputPublicationStage.MATERIALIZE),
                record.result is not None,
                None if record.complete is None else replace(record.complete),
                None if record.rollback is None else replace(record.rollback),
                None if record.rollback_tombstone is None else replace(record.rollback_tombstone),
                None if record.retirement is None else replace(record.retirement),
                active and self._ready_to_complete(record),
            )

    def publication_ids(self) -> Tuple[OutputPublicationID, ...]:
        with self._lock:
            return tuple(replace(value) for value in self._records)

    def _begin(self, publication_id, stage, transfer_index=None):
        with self._lock:
            record = self._record(publication_id)
            self._require_active(record)
            effect = self._effect(record, stage, transfer_index)
            self._require_stage_ready(record, stage)
            record.intents.add(effect)
            return replace(effect)

    def _ack_forward(self, acknowledgement, stage, *, descriptor=None):
        _require_type(acknowledgement, OutputPublicationAck, "acknowledgement")
        acknowledgement = replace(acknowledgement)
        effect = acknowledgement.effect
        if effect.stage is not stage:
            raise OutputPublicationConflictError("ACK names another stage")
        with self._lock:
            record = self._record(effect.publication_id)
            self._require_active(record)
            self._require_effect(record, effect)
            if effect not in record.intents:
                raise OutputPublicationJournalStateError("ACK arrived before its intent")
            self._require_stage_ready(record, stage)
            if stage is OutputPublicationStage.MATERIALIZE:
                descriptor = self._validate_descriptor(record, descriptor)
            if effect in record.acknowledgements:
                if stage is OutputPublicationStage.MATERIALIZE and record.result != descriptor:
                    raise OutputPublicationConflictError("materialized result changed on replay")
                return False
            if stage is OutputPublicationStage.MATERIALIZE:
                record.result = descriptor
            record.acknowledgements[effect] = acknowledgement
            return True

    def _record(self, publication_id):
        _require_type(publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(publication_id)
        try:
            return self._records[publication_id]
        except KeyError as exc:
            raise UnknownOutputPublicationError("publication was not opened") from exc

    @staticmethod
    def _require_active(record):
        if record.state is not OutputPublicationJournalState.ACTIVE:
            raise OutputPublicationJournalStateError("forward work requires an ACTIVE publication")

    @staticmethod
    def _transfer_indices(record):
        return range(len(record.manifest.value.transfers))

    @staticmethod
    def _effect(record, stage, transfer_index=None):
        effect = OutputPublicationEffect(
            record.manifest.publication_id, record.manifest.manifest_digest,
            stage, transfer_index,
        )
        if transfer_index is not None and transfer_index >= len(record.manifest.value.transfers):
            raise OutputPublicationConflictError("transfer_index is outside the output")
        return effect

    def _require_effect(self, record, effect):
        if effect != self._effect(record, effect.stage, effect.transfer_index):
            raise OutputPublicationConflictError("effect changed the exact publication manifest")

    def _acked(self, record, stage, transfer_index=None):
        return self._effect(record, stage, transfer_index) in record.acknowledgements

    def _intended(self, record, stage, transfer_index=None):
        return self._effect(record, stage, transfer_index) in record.intents

    def _require_all_prepared(self, record):
        if not self._acked(record, OutputPublicationStage.OWNER_REGISTER):
            raise OutputPublicationJournalStateError("child/data effects require the exact owner registration ACK")
        if not all(self._acked(record, OutputPublicationStage.PREPARE, index) for index in self._transfer_indices(record)):
            raise OutputPublicationJournalStateError("materialization requires every provisional prepare ACK")

    def _all_materialized(self, record):
        return self._acked(record, OutputPublicationStage.MATERIALIZE)

    def _ready_to_complete(self, record):
        return (self._acked(record, OutputPublicationStage.OWNER_REGISTER)
                and self._all_materialized(record)
                and all(self._acked(record, OutputPublicationStage.PROMOTE, index)
                        for index in self._transfer_indices(record)))

    def _require_stage_ready(self, record, stage):
        if stage is OutputPublicationStage.OWNER_REGISTER:
            return
        if not self._acked(record, OutputPublicationStage.OWNER_REGISTER):
            raise OutputPublicationJournalStateError("operation requires the exact owner registration ACK")
        if stage is OutputPublicationStage.PREPARE:
            return
        self._require_all_prepared(record)
        if stage is OutputPublicationStage.MATERIALIZE:
            return
        if not self._all_materialized(record):
            raise OutputPublicationJournalStateError("promotion requires every materialization ACK")

    @staticmethod
    def _validate_descriptor(record, descriptor):
        descriptor = _descriptor(descriptor)
        slot = record.manifest.value
        if (descriptor.object_id != record.manifest.publication_id.object_id or descriptor.storage is not slot.tier
                or descriptor.size_bytes != slot.size_bytes or descriptor.checksum != slot.checksum
                or descriptor.owner_worker_id != record.manifest.header.owner_worker_id
                or descriptor.node_id != record.manifest.header.node_incarnation.node_id):
            raise OutputPublicationConflictError("materialized descriptor changed its slot identity")
        return descriptor

    @staticmethod
    def _next_rollback(record):
        return next((effect for effect in record.rollback.effects
                     if effect not in record.acknowledgements), None)

    def _finish_rollback_if_ready(self, record):
        if self._next_rollback(record) is not None:
            return
        if record.result is not None:
            raise OutputPublicationJournalStateError("rollback left unretired materialized payloads")
        record.rollback_tombstone = OutputPublicationRollbackTombstone(
            record.rollback, tuple(record.acknowledgements[effect] for effect in record.rollback.effects)
        )
        record.state = OutputPublicationJournalState.RETIRED

    @staticmethod
    def _prepare_retirement(record, proof):
        if record.complete is None:
            raise OutputPublicationJournalStateError("payload retirement requires local Complete")
        if (proof.complete != record.complete or proof.owner_worker_id != record.manifest.header.owner_worker_id):
            raise OutputPublicationConflictError("retirement proof changed Complete or its owner")
        if type(proof) is OutputPublicationAdoptionProof and (
            record.adoption_proof is not None and record.adoption_proof != proof
        ):
            raise OutputPublicationConflictError("whole-batch owner commit identity was rebound")
        terminal = OutputPublicationTombstone(
            record.manifest.publication_id, record.manifest.manifest_digest,
            record.manifest.publication_id.object_id, proof,
        )
        previous = record.retirement
        if previous is not None and previous != terminal:
            raise OutputPublicationConflictError("retirement proof was rebound")
        return previous if previous is not None else terminal

    @staticmethod
    def _commit_retirement(record, terminal):
        record.retirement = terminal
        record.result = None
        if type(terminal.proof) is OutputPublicationAdoptionProof:
            record.adoption_proof = terminal.proof
        record.state = OutputPublicationJournalState.RETIRED


def _effect_order(effect):
    return (tuple(OutputPublicationStage).index(effect.stage),
            -1 if effect.transfer_index is None else effect.transfer_index)


__all__ = [
    "OutputPublicationJournal", "OutputPublicationJournalState",
    "OutputPublicationJournalStateError", "UnknownOutputPublicationError",
    "OutputPublicationPayloadRetired", "OutputPublicationStage",
    "OutputPublicationEffect", "OutputPublicationAck",
    "OutputPublicationAckDisposition", "OutputPublicationRollbackPlan",
    "OutputPublicationRollbackTombstone", "OutputPublicationAdoptionProof",
    "OutputPublicationTombstone",
    "OutputPublicationJournalSnapshot",
]
