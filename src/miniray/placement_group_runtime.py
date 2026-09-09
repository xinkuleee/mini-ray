"""Pure placement-group coordinator and two-phase obligation reducer.

The coordinator never performs RPC and never mutates a node resource ledger.
It freezes a plan, exposes the exact participant operations still owed, and
applies identity-complete replies.  A later runtime adapter can carry these
values over the wire while :mod:`miniray.placement` remains the planner and
node-local reservation authority.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Mapping, Optional, Sequence, Union

from .ids import NodeID, PlacementGroupID
from .placement import (
    Bundle, BundlePlacement, PlacementPlan, PlacementPlanner, PlacementStatus,
    PlacementStrategy,
)
from .protocol import NodeDeathRecord
from .resources import NodeSnapshot, ResourceVector


class PlacementGroupRuntimeError(RuntimeError):
    pass


class PlacementGroupConflictError(PlacementGroupRuntimeError):
    pass


class PlacementGroupPhase(str, Enum):
    PLANNING = "PLANNING"
    PENDING = "PENDING"
    INFEASIBLE = "INFEASIBLE"
    PREPARING = "PREPARING"
    COMMITTING = "COMMITTING"
    ABORTING = "ABORTING"
    CREATED = "CREATED"
    LOST = "LOST"
    REMOVING = "REMOVING"
    REMOVED = "REMOVED"


class ParticipantReplyStatus(str, Enum):
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, order=True)
class PlacementGroupAttempt:
    placement_group_id: PlacementGroupID
    attempt_number: int

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise TypeError("placement_group_id must be a PlacementGroupID")
        if (
            isinstance(self.attempt_number, bool)
            or not isinstance(self.attempt_number, int)
            or self.attempt_number < 0
        ):
            raise ValueError("attempt_number must be a non-negative integer")


@dataclass(frozen=True)
class PlacementGroupSpec:
    placement_group_id: PlacementGroupID
    bundles: tuple[Bundle, ...]
    strategy: PlacementStrategy = PlacementStrategy.STRICT_PACK

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise TypeError("placement_group_id must be a PlacementGroupID")
        bundles = tuple(self.bundles)
        if not bundles:
            raise ValueError("placement group bundles must be non-empty")
        if any(not isinstance(bundle, Bundle) for bundle in bundles):
            raise TypeError("bundles must contain Bundle values")
        if len(bundles) > 2:
            raise ValueError("placement groups support at most two bundles")
        bundles = tuple(sorted(bundles, key=lambda value: value.index))
        indexes = tuple(bundle.index for bundle in bundles)
        if len(indexes) != len(set(indexes)):
            raise ValueError("bundle indexes must be unique")
        object.__setattr__(self, "bundles", bundles)
        object.__setattr__(self, "strategy", PlacementStrategy(self.strategy))


@dataclass(frozen=True)
class PlannedParticipant:
    attempt: PlacementGroupAttempt
    node_id: NodeID
    bundles: tuple[Bundle, ...]
    digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.attempt, PlacementGroupAttempt):
            raise TypeError("attempt must be a PlacementGroupAttempt")
        if not isinstance(self.node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        bundles = tuple(sorted(tuple(self.bundles), key=lambda value: value.index))
        if not bundles or any(not isinstance(bundle, Bundle) for bundle in bundles):
            raise ValueError("participant bundles must be non-empty Bundle values")
        if len({bundle.index for bundle in bundles}) != len(bundles):
            raise ValueError("participant bundle indexes must be unique")
        object.__setattr__(self, "bundles", bundles)
        expected = participant_digest(self.attempt, self.node_id, bundles)
        if self.digest != expected:
            raise ValueError("participant digest does not match its identity and bundles")


@dataclass(frozen=True)
class PlacementGroupAttemptPlan:
    attempt: PlacementGroupAttempt
    spec: PlacementGroupSpec
    placements: tuple[BundlePlacement, ...]
    participants: tuple[PlannedParticipant, ...]


@dataclass(frozen=True)
class PrepareReservation:
    participant: PlannedParticipant


@dataclass(frozen=True)
class CommitReservation:
    participant: PlannedParticipant


@dataclass(frozen=True)
class AbortReservation:
    participant: PlannedParticipant


PlacementGroupOperation = Union[
    PrepareReservation, CommitReservation, AbortReservation
]


@dataclass(frozen=True)
class ReservationReply:
    attempt: PlacementGroupAttempt
    node_id: NodeID
    digest: str
    status: ParticipantReplyStatus
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.attempt, PlacementGroupAttempt):
            raise TypeError("attempt must be a PlacementGroupAttempt")
        if not isinstance(self.node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        if not isinstance(self.digest, str) or len(self.digest) != 64:
            raise ValueError("reply digest must be a SHA-256 hex string")
        if any(character not in "0123456789abcdef" for character in self.digest):
            raise ValueError("reply digest must be a SHA-256 hex string")
        if not isinstance(self.status, ParticipantReplyStatus):
            raise TypeError("status must be a ParticipantReplyStatus")
        if self.status is ParticipantReplyStatus.REJECTED:
            if not isinstance(self.error, str) or not self.error:
                raise ValueError("a rejected reservation reply needs an error")
        elif self.error is not None:
            raise ValueError("an accepted reservation reply cannot contain an error")


@dataclass(frozen=True)
class ParticipantProgress:
    """Monotone coordinator knowledge for one frozen participant.

    ``aborted`` means that no further abort operation is owed.  It is normally
    established by an ``ABORTED`` reply, but a committed Node-death proof also
    satisfies cleanup because that physical reservation can no longer exist.
    The optional complete ``death`` fact distinguishes those two proofs.
    """

    node_id: NodeID
    prepared: bool = False
    committed: bool = False
    aborted: bool = False
    death: Optional[NodeDeathRecord] = None

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        if any(
            not isinstance(value, bool)
            for value in (self.prepared, self.committed, self.aborted)
        ):
            raise TypeError("participant progress flags must be bools")
        if self.committed and not self.prepared:
            raise ValueError("a committed participant must also be prepared")
        if self.death is not None:
            if not isinstance(self.death, NodeDeathRecord):
                raise TypeError("death must be a NodeDeathRecord or None")
            canonical = replace(self.death)
            if canonical.node_id != self.node_id:
                raise ValueError("participant death proof names another NodeID")
            if not self.aborted:
                raise ValueError("a dead participant must have cleanup satisfied")
            object.__setattr__(self, "death", canonical)


@dataclass(frozen=True)
class PlacementGroupSnapshot:
    spec: PlacementGroupSpec
    attempt: PlacementGroupAttempt
    phase: PlacementGroupPhase
    plan: Optional[PlacementGroupAttemptPlan]
    participants: tuple[ParticipantProgress, ...]
    reason: str = ""


@dataclass
class _AttemptRecord:
    spec: PlacementGroupSpec
    attempt: PlacementGroupAttempt
    phase: PlacementGroupPhase
    plan: Optional[PlacementGroupAttemptPlan]
    progress: dict[NodeID, ParticipantProgress] = field(default_factory=dict)
    reason: str = ""
    replies: dict[tuple[NodeID, ParticipantReplyStatus], ReservationReply] = field(
        default_factory=dict
    )


def participant_digest(
    attempt: PlacementGroupAttempt, node_id: NodeID, bundles: Sequence[Bundle]
) -> str:
    """Return a canonical participant payload identity."""

    digest = hashlib.sha256()
    digest.update(bytes(attempt.placement_group_id))
    digest.update(attempt.attempt_number.to_bytes(8, "big"))
    digest.update(bytes(node_id))
    for bundle in sorted(tuple(bundles), key=lambda value: value.index):
        digest.update(bundle.index.to_bytes(8, "big"))
        items = bundle.resources.to_dict()
        for name, quantity in sorted(items.items()):
            encoded_name = name.encode("utf-8")
            encoded_quantity = str(quantity).encode("ascii")
            digest.update(len(encoded_name).to_bytes(4, "big"))
            digest.update(encoded_name)
            digest.update(len(encoded_quantity).to_bytes(4, "big"))
            digest.update(encoded_quantity)
    return digest.hexdigest()


class PlacementGroupCoordinator:
    """Pure reducer for one immutable attempt per logical placement group."""

    def __init__(self, planner: Optional[PlacementPlanner] = None) -> None:
        self._planner = planner or PlacementPlanner()
        self._records: dict[PlacementGroupID, _AttemptRecord] = {}
        # These indexes bind a committed proof both by physical Node identity
        # and by the GCS detection identity.  They make a conflicting replay an
        # all-or-nothing validation failure even when one Node participates in
        # several placement groups.
        self._node_deaths: dict[NodeID, NodeDeathRecord] = {}
        self._death_detections: dict[str, NodeDeathRecord] = {}

    def create(
        self,
        spec: PlacementGroupSpec,
        nodes: Sequence[NodeSnapshot],
        *,
        attempt_number: int = 0,
    ) -> PlacementGroupSnapshot:
        if not isinstance(spec, PlacementGroupSpec):
            raise TypeError("spec must be a PlacementGroupSpec")
        existing = self._records.get(spec.placement_group_id)
        if existing is not None:
            if existing.spec != spec or existing.attempt.attempt_number != attempt_number:
                raise PlacementGroupConflictError(
                    "placement group already has another spec or attempt"
                )
            return self._snapshot(existing)
        attempt = PlacementGroupAttempt(spec.placement_group_id, attempt_number)
        decision = self._planner.plan(spec.bundles, nodes, spec.strategy)
        if decision.status is PlacementStatus.PENDING:
            record = _AttemptRecord(
                spec, attempt, PlacementGroupPhase.PENDING, None,
                reason=decision.reason,
            )
        elif decision.status is PlacementStatus.INFEASIBLE:
            record = _AttemptRecord(
                spec, attempt, PlacementGroupPhase.INFEASIBLE, None,
                reason=decision.reason,
            )
        else:
            plan = self._freeze_plan(spec, attempt, decision)
            progress = {
                participant.node_id: ParticipantProgress(participant.node_id)
                for participant in plan.participants
            }
            record = _AttemptRecord(
                spec, attempt, PlacementGroupPhase.PREPARING, plan, progress
            )
        self._records[spec.placement_group_id] = record
        return self._snapshot(record)

    def next_operations(
        self, placement_group_id: PlacementGroupID
    ) -> tuple[PlacementGroupOperation, ...]:
        record = self._record(placement_group_id)
        if record.plan is None:
            return ()
        operations: list[PlacementGroupOperation] = []
        for participant in record.plan.participants:
            progress = record.progress[participant.node_id]
            if record.phase is PlacementGroupPhase.PREPARING and not progress.prepared:
                operations.append(PrepareReservation(participant))
            elif record.phase is PlacementGroupPhase.COMMITTING and not progress.committed:
                operations.append(CommitReservation(participant))
            elif (
                record.phase in (
                    PlacementGroupPhase.ABORTING,
                    PlacementGroupPhase.LOST,
                    PlacementGroupPhase.REMOVING,
                )
                and progress.death is None
                and not progress.aborted
            ):
                operations.append(AbortReservation(participant))
        return tuple(operations)

    def retry_pending(
        self,
        placement_group_id: PlacementGroupID,
        nodes: Sequence[NodeSnapshot],
    ) -> PlacementGroupSnapshot:
        """Replan one never-started attempt against a fresh node snapshot.

        ``PENDING`` has no frozen plan and emits no participant operation, so it
        is the only phase that may safely reconsider placement without changing
        attempt identity.  Once a plan reaches PREPARING, every later replay
        must retain its original participants and digests.
        """

        record = self._record(placement_group_id)
        if record.phase is not PlacementGroupPhase.PENDING:
            raise PlacementGroupConflictError(
                "only a pending placement group can be replanned"
            )
        decision = self._planner.plan(
            record.spec.bundles, nodes, record.spec.strategy
        )
        if decision.status is PlacementStatus.PENDING:
            record.reason = decision.reason
            return self._snapshot(record)
        if decision.status is PlacementStatus.INFEASIBLE:
            record.phase = PlacementGroupPhase.INFEASIBLE
            record.reason = decision.reason
            return self._snapshot(record)

        plan = self._freeze_plan(record.spec, record.attempt, decision)
        record.plan = plan
        record.progress = {
            participant.node_id: ParticipantProgress(participant.node_id)
            for participant in plan.participants
        }
        record.phase = PlacementGroupPhase.PREPARING
        record.reason = ""
        return self._snapshot(record)

    def apply_reply(self, reply: ReservationReply) -> PlacementGroupSnapshot:
        if not isinstance(reply, ReservationReply):
            raise TypeError("reply must be a ReservationReply")
        record = self._record(reply.attempt.placement_group_id)
        if reply.attempt != record.attempt:
            raise PlacementGroupConflictError("reservation reply has a stale attempt")
        participant = self._participant(record, reply.node_id)
        if participant.digest != reply.digest:
            raise PlacementGroupConflictError("reservation reply changed payload digest")

        key = reply.node_id, reply.status
        previous = record.replies.get(key)
        if previous is not None:
            if previous != reply:
                raise PlacementGroupConflictError("reservation reply replay changed")
            return self._snapshot(record)

        if reply.status is ParticipantReplyStatus.REJECTED:
            if record.phase not in (
                PlacementGroupPhase.PREPARING, PlacementGroupPhase.COMMITTING
            ):
                raise PlacementGroupConflictError("rejection is invalid in this phase")
            record.replies[key] = reply
            record.phase = PlacementGroupPhase.ABORTING
            record.reason = reply.error or "participant rejected reservation"
            return self._snapshot(record)

        expected = {
            PlacementGroupPhase.PREPARING: ParticipantReplyStatus.PREPARED,
            PlacementGroupPhase.COMMITTING: ParticipantReplyStatus.COMMITTED,
            PlacementGroupPhase.ABORTING: ParticipantReplyStatus.ABORTED,
            PlacementGroupPhase.LOST: ParticipantReplyStatus.ABORTED,
            PlacementGroupPhase.REMOVING: ParticipantReplyStatus.ABORTED,
        }.get(record.phase)
        if reply.status is not expected:
            raise PlacementGroupConflictError("reservation reply is invalid in this phase")
        record.replies[key] = reply
        progress = record.progress[reply.node_id]
        if reply.status is ParticipantReplyStatus.PREPARED:
            record.progress[reply.node_id] = ParticipantProgress(
                reply.node_id, prepared=True, death=progress.death
            )
            if all(value.prepared for value in record.progress.values()):
                record.phase = PlacementGroupPhase.COMMITTING
        elif reply.status is ParticipantReplyStatus.COMMITTED:
            record.progress[reply.node_id] = ParticipantProgress(
                reply.node_id, prepared=True, committed=True,
                death=progress.death,
            )
            if all(value.committed for value in record.progress.values()):
                record.phase = PlacementGroupPhase.CREATED
        else:
            record.progress[reply.node_id] = ParticipantProgress(
                reply.node_id, prepared=progress.prepared,
                committed=progress.committed, aborted=True,
                death=progress.death,
            )
            if (
                record.phase is not PlacementGroupPhase.LOST
                and all(value.aborted for value in record.progress.values())
            ):
                record.phase = PlacementGroupPhase.REMOVED
        return self._snapshot(record)

    def fail_node(
        self, death: NodeDeathRecord
    ) -> tuple[PlacementGroupSnapshot, ...]:
        """Reduce one committed Node death across every affected PG.

        A ``NodeDeathRecord`` is the authority: transport failure or timeout is
        deliberately insufficient.  Validation is completed for every affected
        record before any index, phase, or participant progress is mutated.

        A participant loss while preparing, committing, or visible makes the
        immutable attempt terminal ``LOST``.  Reservations on surviving Nodes
        still need abort ACKs, while the dead participant's cleanup is satisfied
        by the death proof itself.  If cleanup was already ABORTING or REMOVING,
        its original intent is retained instead of being rewritten as loss.
        """

        if not isinstance(death, NodeDeathRecord):
            raise TypeError("death must be a NodeDeathRecord")
        # Pickle may instantiate a dataclass without invoking __post_init__.
        # Rebuild before inspecting node_id or touching reducer state.
        canonical = replace(death)
        prior_node_proof = self._node_deaths.get(canonical.node_id)
        if prior_node_proof is not None and prior_node_proof != canonical:
            raise PlacementGroupConflictError(
                "NodeID already has a different committed death proof"
            )
        prior_detection = self._death_detections.get(canonical.detection_id)
        if prior_detection is not None and prior_detection != canonical:
            raise PlacementGroupConflictError(
                "node death detection_id was reused for another proof"
            )

        active_phases = (
            PlacementGroupPhase.PREPARING,
            PlacementGroupPhase.COMMITTING,
            PlacementGroupPhase.ABORTING,
            PlacementGroupPhase.CREATED,
            PlacementGroupPhase.LOST,
            PlacementGroupPhase.REMOVING,
        )
        affected: list[_AttemptRecord] = []
        for placement_group_id in sorted(self._records):
            record = self._records[placement_group_id]
            progress = record.progress.get(canonical.node_id)
            if progress is None:
                continue
            # A fresh proof after complete cleanup has no work.  A proof that
            # participated in that cleanup remains queryable for exact replay.
            if record.phase in active_phases or progress.death is not None:
                affected.append(record)

        if not affected:
            return ()

        for record in affected:
            prior = record.progress[canonical.node_id].death
            if prior is not None and prior != canonical:
                raise PlacementGroupConflictError(
                    "participant already has a different committed death proof"
                )

        # The preflight above makes the following reduction non-failing and
        # atomic with respect to all PG records owned by this pure coordinator.
        self._node_deaths[canonical.node_id] = canonical
        self._death_detections[canonical.detection_id] = canonical
        snapshots: list[PlacementGroupSnapshot] = []
        loss_phases = (
            PlacementGroupPhase.PREPARING,
            PlacementGroupPhase.COMMITTING,
            PlacementGroupPhase.CREATED,
        )
        for record in affected:
            progress = record.progress[canonical.node_id]
            if progress.death is None:
                record.progress[canonical.node_id] = ParticipantProgress(
                    canonical.node_id,
                    prepared=progress.prepared,
                    committed=progress.committed,
                    aborted=True,
                    death=canonical,
                )
                if record.phase in loss_phases:
                    record.phase = PlacementGroupPhase.LOST
                    record.reason = (
                        "participant Node {} died ({}): {}".format(
                            canonical.node_id.hex,
                            canonical.reason.value,
                            canonical.detail,
                        )
                    )
                elif (
                    record.phase in (
                        PlacementGroupPhase.ABORTING,
                        PlacementGroupPhase.REMOVING,
                    )
                    and all(value.aborted for value in record.progress.values())
                ):
                    record.phase = PlacementGroupPhase.REMOVED
            snapshots.append(self._snapshot(record))
        return tuple(snapshots)

    def remove(
        self, placement_group_id: PlacementGroupID
    ) -> PlacementGroupSnapshot:
        record = self._record(placement_group_id)
        if record.phase is PlacementGroupPhase.REMOVED:
            return self._snapshot(record)
        if record.phase is not PlacementGroupPhase.CREATED:
            raise PlacementGroupConflictError(
                "only a created placement group can begin removal"
            )
        record.phase = PlacementGroupPhase.REMOVING
        return self._snapshot(record)

    def cancel_for_shutdown(
        self, placement_group_id: PlacementGroupID
    ) -> PlacementGroupSnapshot:
        """Close visibility and expose every cleanup obligation for shutdown.

        PENDING never produced a participant operation, so it can become REMOVED
        immediately.  INFEASIBLE and REMOVED are already clean terminal states.
        A frozen but incomplete plan aborts; a visible CREATED plan removes.
        Replaying this transition is idempotent and leaves outstanding aborts to
        :meth:`next_operations`.
        """

        record = self._record(placement_group_id)
        if record.phase is PlacementGroupPhase.PENDING:
            record.phase = PlacementGroupPhase.REMOVED
            record.reason = "cancelled for GCS shutdown before participant prepare"
        elif record.phase in (
            PlacementGroupPhase.PREPARING,
            PlacementGroupPhase.COMMITTING,
        ):
            record.phase = PlacementGroupPhase.ABORTING
            record.reason = "cancelled for GCS shutdown"
        elif record.phase is PlacementGroupPhase.CREATED:
            record.phase = PlacementGroupPhase.REMOVING
            record.reason = "removed for GCS shutdown"
        elif record.phase not in (
            PlacementGroupPhase.INFEASIBLE,
            PlacementGroupPhase.ABORTING,
            PlacementGroupPhase.LOST,
            PlacementGroupPhase.REMOVING,
            PlacementGroupPhase.REMOVED,
        ):
            raise PlacementGroupConflictError(
                "placement group cannot be cancelled from {}".format(
                    record.phase.value
                )
            )
        return self._snapshot(record)

    def snapshot(
        self, placement_group_id: PlacementGroupID
    ) -> PlacementGroupSnapshot:
        return self._snapshot(self._record(placement_group_id))

    def visible_placement(
        self, placement_group_id: PlacementGroupID
    ) -> Optional[PlacementGroupAttemptPlan]:
        record = self._record(placement_group_id)
        return record.plan if record.phase is PlacementGroupPhase.CREATED else None

    def _freeze_plan(
        self, spec: PlacementGroupSpec, attempt: PlacementGroupAttempt,
        decision: PlacementPlan,
    ) -> PlacementGroupAttemptPlan:
        placements = tuple(sorted(decision.placements, key=lambda item: item.bundle_index))
        expected = {bundle.index for bundle in spec.bundles}
        actual = {placement.bundle_index for placement in placements}
        if expected != actual or len(placements) != len(expected):
            raise PlacementGroupRuntimeError("planner returned incomplete placements")
        by_index = {bundle.index: bundle for bundle in spec.bundles}
        by_node: dict[NodeID, list[Bundle]] = {}
        for placement in placements:
            by_node.setdefault(placement.node_id, []).append(
                by_index[placement.bundle_index]
            )
        participants = tuple(
            PlannedParticipant(
                attempt, node_id, tuple(sorted(bundles, key=lambda value: value.index)),
                participant_digest(attempt, node_id, bundles),
            )
            for node_id, bundles in sorted(by_node.items(), key=lambda item: item[0])
        )
        return PlacementGroupAttemptPlan(attempt, spec, placements, participants)

    @staticmethod
    def _snapshot(record: _AttemptRecord) -> PlacementGroupSnapshot:
        participants = tuple(
            record.progress[node_id]
            for node_id in sorted(record.progress)
        )
        return PlacementGroupSnapshot(
            record.spec, record.attempt, record.phase, record.plan,
            participants, record.reason,
        )

    def _record(self, placement_group_id: PlacementGroupID) -> _AttemptRecord:
        try:
            return self._records[placement_group_id]
        except KeyError:
            raise KeyError("unknown placement group") from None

    @staticmethod
    def _participant(
        record: _AttemptRecord, node_id: NodeID
    ) -> PlannedParticipant:
        if record.plan is not None:
            for participant in record.plan.participants:
                if participant.node_id == node_id:
                    return participant
        raise PlacementGroupConflictError("reply names a non-participant node")


__all__ = [
    "AbortReservation", "CommitReservation", "ParticipantProgress",
    "ParticipantReplyStatus", "PlacementGroupAttempt",
    "PlacementGroupAttemptPlan", "PlacementGroupConflictError",
    "PlacementGroupCoordinator", "PlacementGroupOperation",
    "PlacementGroupPhase", "PlacementGroupRuntimeError",
    "PlacementGroupSnapshot", "PlacementGroupSpec",
    "PlannedParticipant", "PrepareReservation", "ReservationReply",
    "participant_digest",
]
