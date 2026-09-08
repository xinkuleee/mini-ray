"""Pure transaction model for partial-loss multi-return reconstruction.

This layer isolates the extra identity needed by targeted replay before it is
wired through Core, Node, and Worker.  A logical TaskID still owns retry budget
and allocates monotonically increasing physical AttemptIDs.  Each ObjectID slot,
however, keeps its own producer attempt; replay replaces only explicitly lost
slots and cannot perturb healthy siblings.

Admission has a short OPEN window so concurrent loss observations can merge.
``start()`` closes that set and atomically commits owner epochs plus one retry
budget charge.  Losses first observed after START are queued for the next
session.  The class performs no RPC, scheduling, queueing, or object-store I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from threading import RLock
from typing import Mapping, Optional

from .ids import AttemptID, ObjectID, TaskID
from .ownership import (
    ObjectOwnerTable, ObjectState, TargetOutputAttemptAdvancePlan,
    TargetOutputPublicationPlan, TargetOutputRetryAdvancePlan,
    TargetOutputTerminalErrorPlan,
    OutputOwnerPublicationPlan, OutputOwnerPublicationDisposition,
)
from .output_publication import OutputPublicationEnvelope
from .protocol import ResultDescriptor
from .recovery import (
    RecoveryAction, RecoveryManager, RecoveryTransitionPlan, TaskState,
)
from .task_outputs import (
    TargetExecutionKey, TargetOutputManifest, TaskOutputManifest,
)


class TargetedReconstructionError(RuntimeError):
    """A targeted reconstruction contradicts owner or recovery history."""


class TargetedSessionPhase(str, Enum):
    OPEN = "OPEN"
    STARTED = "STARTED"


class TargetedRequestDisposition(str, Enum):
    OPENED = "OPENED"
    MERGED = "MERGED"
    JOINED = "JOINED"
    QUEUED_NEXT = "QUEUED_NEXT"


@dataclass(frozen=True)
class TargetedLoss:
    """One observed lost slot and its owner-side CAS epoch."""

    object_id: ObjectID
    expected_attempt: AttemptID

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise TypeError("targeted loss object_id must be an ObjectID")
        if not isinstance(self.expected_attempt, AttemptID):
            raise TypeError("expected_attempt must be an AttemptID")
        if self.expected_attempt.task_id != self.object_id.task_id:
            raise ValueError("expected attempt belongs to another TaskID")


@dataclass(frozen=True)
class TargetedReconstructionSession:
    """Immutable diagnostic view of one OPEN or STARTED target set."""

    task_id: TaskID
    full_manifest: TaskOutputManifest
    phase: TargetedSessionPhase
    losses: tuple[TargetedLoss, ...]
    execution: Optional[TargetExecutionKey] = None

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise TypeError("targeted session task_id must be a TaskID")
        if not isinstance(self.full_manifest, TaskOutputManifest):
            raise TypeError(
                "targeted session full_manifest must be a TaskOutputManifest"
            )
        if self.full_manifest.task_id != self.task_id:
            raise ValueError("targeted session manifest belongs to another task")
        if not isinstance(self.phase, TargetedSessionPhase):
            raise TypeError("targeted session phase must be a TargetedSessionPhase")
        losses = tuple(self.losses)
        if not losses or any(not isinstance(loss, TargetedLoss) for loss in losses):
            raise TypeError("targeted session requires non-empty TargetedLoss values")
        selected = {loss.object_id for loss in losses}
        canonical = tuple(
            object_id for object_id in self.full_manifest.output_ids
            if object_id in selected
        )
        if (
            len(selected) != len(losses)
            or tuple(loss.object_id for loss in losses) != canonical
        ):
            raise ValueError(
                "targeted session losses must be a unique ordered subset"
            )
        if any(loss.object_id.task_id != self.task_id for loss in losses):
            raise ValueError("targeted session loss belongs to another task")
        if self.phase is TargetedSessionPhase.OPEN:
            if self.execution is not None:
                raise ValueError("OPEN targeted session cannot have an execution")
        else:
            if not isinstance(self.execution, TargetExecutionKey):
                raise TypeError("STARTED targeted session requires an execution")
            if (
                self.execution.manifest.full_manifest != self.full_manifest
                or self.execution.target_output_ids != canonical
            ):
                raise ValueError(
                    "targeted execution does not match session losses"
                )
        object.__setattr__(self, "losses", losses)

    @property
    def target_output_ids(self) -> tuple[ObjectID, ...]:
        return tuple(loss.object_id for loss in self.losses)

    @property
    def expected_attempts(self) -> dict[ObjectID, AttemptID]:
        return {
            loss.object_id: loss.expected_attempt for loss in self.losses
        }


@dataclass(frozen=True)
class TargetedReconstructionRequest:
    disposition: TargetedRequestDisposition
    session: TargetedReconstructionSession


@dataclass(frozen=True)
class TargetedReconstructionStartPlan:
    """Both authority plans after all fallible preflight has succeeded."""

    session: TargetedReconstructionSession
    owner_plan: TargetOutputAttemptAdvancePlan
    recovery_plan: RecoveryTransitionPlan

    @property
    def execution(self) -> TargetExecutionKey:
        assert self.session.execution is not None
        return self.session.execution


@dataclass(frozen=True)
class TargetedReconstructionSuccessPlan:
    session: TargetedReconstructionSession
    owner_plan: TargetOutputPublicationPlan
    recovery_plan: RecoveryTransitionPlan


@dataclass(frozen=True)
class TargetedOutputPublicationSuccessPlan:
    session: TargetedReconstructionSession
    owner_plan: OutputOwnerPublicationPlan
    recovery_plan: RecoveryTransitionPlan


@dataclass(frozen=True)
class TargetedReconstructionRetryPlan:
    session: TargetedReconstructionSession
    next_session: TargetedReconstructionSession
    owner_plan: TargetOutputRetryAdvancePlan
    recovery_plan: RecoveryTransitionPlan


@dataclass(frozen=True)
class TargetedReconstructionFailurePlan:
    session: TargetedReconstructionSession
    owner_plan: TargetOutputTerminalErrorPlan
    recovery_plan: RecoveryTransitionPlan


class TargetedReconstructionCoordinator:
    """Linearize target merging, START, and next-session rollover.

    All methods are intended to run beneath the same Core composition lock in
    the eventual runtime adapter.  This pure class also owns an ``RLock`` so
    its invariants remain demonstrable in standalone unit tests.
    """

    def __init__(
        self, recovery: RecoveryManager, owner: ObjectOwnerTable
    ) -> None:
        if not isinstance(recovery, RecoveryManager):
            raise TypeError("recovery must be a RecoveryManager")
        if not isinstance(owner, ObjectOwnerTable):
            raise TypeError("owner must be an ObjectOwnerTable")
        self._recovery = recovery
        self._owner = owner
        self._current: dict[TaskID, TargetedReconstructionSession] = {}
        self._queued: dict[TaskID, dict[ObjectID, AttemptID]] = {}
        self._open_failures: dict[TaskID, object] = {}
        self._lock = RLock()

    def request(
        self, object_id: ObjectID, expected_attempt: AttemptID
    ) -> TargetedReconstructionRequest:
        """Open/merge/join one loss without consuming retry budget."""

        loss = TargetedLoss(object_id, expected_attempt)
        with self._lock:
            session = self._current.get(object_id.task_id)
            if (
                session is not None
                and object_id in session.expected_attempts
            ):
                original = session.expected_attempts[object_id]
                if original == expected_attempt:
                    disposition = (
                        TargetedRequestDisposition.MERGED
                        if session.phase is TargetedSessionPhase.OPEN
                        else TargetedRequestDisposition.JOINED
                    )
                    return TargetedReconstructionRequest(disposition, session)
                # A selected slot can publish under the STARTED attempt and
                # lose that fresh replica before the session marker is closed.
                # That is a new loss, not a conflicting replay of the old one.
                if (
                    session.phase is not TargetedSessionPhase.STARTED
                    or session.execution is None
                    or expected_attempt != session.execution.attempt_id
                ):
                    raise TargetedReconstructionError(
                        "one target was observed with conflicting producer epochs"
                    )

            manifest = self._validate_loss(loss)
            if session is None:
                opened = self._open_session(manifest, {object_id: expected_attempt})
                self._current[object_id.task_id] = opened
                return TargetedReconstructionRequest(
                    TargetedRequestDisposition.OPENED, opened
                )
            if session.full_manifest != manifest:
                raise TargetedReconstructionError(
                    "active targeted session changed its complete manifest"
                )
            if session.phase is TargetedSessionPhase.OPEN:
                merged = dict(session.expected_attempts)
                merged[object_id] = expected_attempt
                updated = self._open_session(manifest, merged)
                self._current[object_id.task_id] = updated
                return TargetedReconstructionRequest(
                    TargetedRequestDisposition.MERGED, updated
                )

            queued = self._queued.setdefault(object_id.task_id, {})
            existing = queued.get(object_id)
            if existing is not None and existing != expected_attempt:
                raise TargetedReconstructionError(
                    "queued target was observed with conflicting producer epochs"
                )
            queued[object_id] = expected_attempt
            return TargetedReconstructionRequest(
                TargetedRequestDisposition.QUEUED_NEXT, session
            )

    def current_session(
        self, task_id: TaskID
    ) -> Optional[TargetedReconstructionSession]:
        with self._lock:
            return self._current.get(task_id)

    def queued_losses(
        self, task_id: TaskID
    ) -> tuple[TargetedLoss, ...]:
        with self._lock:
            queued = self._queued.get(task_id, {})
            manifest = self._manifest_for_task(task_id)
            return self._ordered_losses(manifest, queued)

    def active_task_ids(self) -> tuple[TaskID, ...]:
        """Return OPEN/STARTED TaskIDs that still require Core progress."""

        with self._lock:
            return tuple(sorted(self._current))

    def begin_open_failure(self, task_id: TaskID, error: object) -> object:
        """Latch a terminal admission choice while old output cleanup retries.

        No output metadata or recovery budget changes here. A retry of OPEN
        work must finish this choice, not admit execution after its old child
        holds have already begun irreversible retirement.
        """
        with self._lock:
            session = self._current.get(task_id)
            if session is None or session.phase is not TargetedSessionPhase.OPEN:
                raise TargetedReconstructionError("failure choice requires an OPEN session")
            return self._open_failures.setdefault(task_id, error)

    def open_failure(self, task_id: TaskID) -> object | None:
        with self._lock:
            return self._open_failures.get(task_id)

    def fail_open(self, task_id: TaskID, error: object) -> tuple[ObjectID, ...]:
        """Atomically fail an OPEN target set without consuming retry budget.

        OPEN has not advanced either the owner epochs or RecoveryManager.  A
        definitive admission failure therefore terminates only the observed
        LOST slots at their original producer epochs and leaves healthy
        siblings plus the task-level retry record untouched.
        """

        with self._lock:
            session = self._current.get(task_id)
            if session is None:
                return ()
            if session.phase is not TargetedSessionPhase.OPEN:
                raise TargetedReconstructionError(
                    "cannot fail a targeted session after START"
                )
            chosen = self._open_failures.get(task_id)
            if chosen is not None:
                error = chosen
            plan = self._owner.validate_publish_terminal_target_error(
                session.full_manifest, session.expected_attempts, error
            )
            if plan is None or not self._owner.commit_publish_terminal_target_error(
                plan
            ):
                raise TargetedReconstructionError(
                    "owner fenced OPEN targeted terminal failure"
                )
            self._current.pop(task_id, None)
            self._queued.pop(task_id, None)
            self._open_failures.pop(task_id, None)
            return session.target_output_ids

    def validate_start(
        self, task_id: TaskID
    ) -> TargetedReconstructionStartPlan:
        """Freeze OPEN targets and preflight both authorities.

        This method changes no state.  It is safe for a caller to abandon the
        result after any later composition preflight fails.
        """

        with self._lock:
            frozen, recovery_plan = self._preview_start_locked(task_id)
            owner_plan = self._owner.validate_advance_target_outputs(
                frozen.execution, frozen.expected_attempts
            )
            return TargetedReconstructionStartPlan(
                frozen, owner_plan, recovery_plan
            )

    def preview_start(self, task_id: TaskID) -> TargetedReconstructionSession:
        """Inspect lineage, selected epochs and budget before old-slot cleanup.

        The returned session is a proposal, not an installed START or an
        executable owner plan. It may still name unretired publication slots.
        Core must renew inputs, retire only those slots, then call
        ``validate_start`` and ``commit_start`` under its composition lock.
        """

        with self._lock:
            frozen, _recovery_plan = self._preview_start_locked(task_id)
            return frozen

    def _preview_start_locked(
        self, task_id: TaskID
    ) -> tuple[TargetedReconstructionSession, RecoveryTransitionPlan]:
        session = self._current.get(task_id)
        if session is None:
            raise TargetedReconstructionError(
                "targeted reconstruction has no open session"
            )
        if session.phase is not TargetedSessionPhase.OPEN:
            raise TargetedReconstructionError(
                "targeted reconstruction session already started"
            )
        if task_id in self._open_failures:
            raise TargetedReconstructionError(
                "targeted admission already chose terminal failure"
            )
        for loss in session.losses:
            if self._validate_loss(loss) != session.full_manifest:
                raise TargetedReconstructionError(
                    "targeted loss changed its complete producer manifest"
                )
        spec = self._owner.snapshot(session.target_output_ids[0]).producer_task_spec
        for object_id in session.full_manifest.output_ids:
            sibling = self._owner.snapshot(object_id)
            if sibling.producer_task_spec != spec or sibling.collection_pending:
                raise TargetedReconstructionError(
                    "targeted siblings changed lineage or entered collection"
                )
        recovery_plan = self._recovery.validate_request_reconstruction(
            session.target_output_ids[0]
        )
        decision = recovery_plan.decision
        if decision.action is not RecoveryAction.START_RECONSTRUCTION:
            raise TargetedReconstructionError(
                "recovery authority did not admit a new targeted START: {}"
                .format(decision.action.value)
            )
        if not isinstance(decision.attempt_id, AttemptID):
            raise TargetedReconstructionError(
                "targeted START has no physical attempt identity"
            )
        if tuple(decision.output_ids) != session.full_manifest.output_ids:
            raise TargetedReconstructionError(
                "recovery authority changed the complete output manifest"
            )
        execution = TargetExecutionKey(
            TargetOutputManifest(session.full_manifest, session.target_output_ids),
            decision.attempt_id,
        )
        frozen = replace(
            session, phase=TargetedSessionPhase.STARTED, execution=execution,
        )
        return frozen, recovery_plan

    def commit_start(
        self, plan: TargetedReconstructionStartPlan
    ) -> TargetedReconstructionSession:
        """Commit preflighted owner and retry state using assignments only."""

        if not isinstance(plan, TargetedReconstructionStartPlan):
            raise TypeError("plan must be a TargetedReconstructionStartPlan")
        with self._lock:
            current = self._current.get(plan.session.task_id)
            if current is None or current.phase is not TargetedSessionPhase.OPEN:
                raise TargetedReconstructionError(
                    "targeted reconstruction open session changed before commit"
                )
            if plan.session.task_id in self._open_failures:
                raise TargetedReconstructionError(
                    "targeted admission already chose terminal failure"
                )
            if (
                current.full_manifest != plan.session.full_manifest
                or current.losses != plan.session.losses
            ):
                raise TargetedReconstructionError(
                    "target set changed after START preflight"
                )
            recovery_plan = plan.recovery_plan
            self._require_recovery_plan_current(recovery_plan)
            # Owner commit rechecks its CAS under one owner lock.  A rejection
            # changes no slot, so recovery budget/session remain untouched.
            # Once it succeeds, commit_validated_transition is an
            # assignment-only commit by contract.
            if not self._owner.commit_advance_target_outputs(plan.owner_plan):
                raise TargetedReconstructionError(
                    "owner rejected targeted output CAS"
                )
            committed = self._recovery.commit_validated_transition(
                recovery_plan
            )
            if committed != recovery_plan.decision:
                raise AssertionError(
                    "RecoveryManager committed another targeted decision"
                )
            self._current[plan.session.task_id] = plan.session
            return plan.session

    def start(self, task_id: TaskID) -> TargetedReconstructionSession:
        """Validate and atomically commit one OPEN target set."""

        with self._lock:
            return self.commit_start(self.validate_start(task_id))

    def validate_success(
        self, task_id: TaskID, attempt_id: AttemptID,
        results: tuple[ResultDescriptor, ...],
    ) -> TargetedReconstructionSuccessPlan:
        """Preflight atomic target publication and task success."""

        with self._lock:
            session = self._require_started(task_id, attempt_id)
            assert session.execution is not None
            owner_plan = self._owner.validate_publish_target_outputs(
                session.execution, tuple(results)
            )
            if owner_plan is None:
                raise TargetedReconstructionError(
                    "owner fenced targeted success publication"
                )
            recovery_plan = self._recovery.validate_task_success(
                task_id, attempt_id
            )
            if recovery_plan.decision.action is not RecoveryAction.ACCEPT_SUCCESS:
                raise TargetedReconstructionError(
                    "recovery authority fenced targeted success"
                )
            return TargetedReconstructionSuccessPlan(
                session, owner_plan, recovery_plan
            )

    def commit_success(
        self, plan: TargetedReconstructionSuccessPlan
    ) -> Optional[TargetedReconstructionSession]:
        """Atomically publish all targets, close, and promote late losses."""

        if not isinstance(plan, TargetedReconstructionSuccessPlan):
            raise TypeError("plan must be a TargetedReconstructionSuccessPlan")
        with self._lock:
            attempt_id = plan.session.execution.attempt_id
            self._require_started(plan.session.task_id, attempt_id)
            self._require_recovery_plan_current(plan.recovery_plan)
            if not self._owner.commit_publish_target_outputs(plan.owner_plan):
                raise TargetedReconstructionError(
                    "owner rejected targeted success publication"
                )
            self._recovery.commit_validated_transition(plan.recovery_plan)
            return self._close_and_promote(plan.session)

    def succeed(
        self, task_id: TaskID, attempt_id: AttemptID,
        results: tuple[ResultDescriptor, ...],
    ) -> Optional[TargetedReconstructionSession]:
        with self._lock:
            return self.commit_success(
                self.validate_success(task_id, attempt_id, results)
            )

    def validate_output_publication_success(
        self, envelope: OutputPublicationEnvelope,
    ) -> TargetedOutputPublicationSuccessPlan:
        """Preflight the shared batch owner CAS instead of legacy publication."""
        with self._lock:
            identity = envelope.publication_id
            session = self._require_started(identity.task_id, identity.attempt_id)
            owner_plan = OutputOwnerPublicationPlan(session.execution, envelope)
            if self._owner.validate_output_publication(owner_plan) is OutputOwnerPublicationDisposition.FENCED:
                raise TargetedReconstructionError("owner fenced unified targeted success")
            recovery_plan = self._recovery.validate_task_success(identity.task_id, identity.attempt_id)
            if recovery_plan.decision.action is not RecoveryAction.ACCEPT_SUCCESS:
                raise TargetedReconstructionError("recovery fenced unified targeted success")
            return TargetedOutputPublicationSuccessPlan(session, owner_plan, recovery_plan)

    def commit_output_publication_success(
        self, plan: TargetedOutputPublicationSuccessPlan,
    ) -> Optional[TargetedReconstructionSession]:
        if type(plan) is not TargetedOutputPublicationSuccessPlan:
            raise TypeError("plan must be a TargetedOutputPublicationSuccessPlan")
        with self._lock:
            self._require_started(plan.session.task_id, plan.session.execution.attempt_id)
            self._require_recovery_plan_current(plan.recovery_plan)
            if not self._owner.commit_output_publication(plan.owner_plan).committed:
                raise TargetedReconstructionError("owner rejected unified targeted CAS")
            self._recovery.commit_validated_transition(plan.recovery_plan)
            return self._close_and_promote(plan.session)

    def complete_lost_output_publication(self, task_id: TaskID, attempt_id: AttemptID):
        """Close an execution proven Complete after owner KEEP/LOST resolution.

        The owner already installed the cleanup-acknowledged per-slot vector;
        publishing fresh descriptors here would revive lost replicas.
        """
        with self._lock:
            session = self._current.get(task_id)
            if session is None:
                return None
            if session.phase is TargetedSessionPhase.OPEN:
                # A previous exact completion may already have promoted queued
                # losses before its caller enqueued the wake.  Return that
                # pending handoff again; START validates/fences duplicate wakes.
                return session
            if session.execution.attempt_id != attempt_id:
                # A successor START already consumed the handoff.  Old replay
                # cannot close or republish this different execution.
                return None
            session = self._require_started(task_id, attempt_id)
            record = self._recovery.task_record(task_id)
            if record.state is not TaskState.SUCCEEDED:
                plan = self._recovery.validate_task_success(task_id, attempt_id)
                if plan.decision.action is not RecoveryAction.ACCEPT_SUCCESS:
                    raise TargetedReconstructionError("recovery fenced lost targeted Complete")
                self._recovery.commit_validated_transition(plan)
            return self._close_and_promote(session)

    def validate_system_retry(
        self, task_id: TaskID, attempt_id: AttemptID, error: object,
    ) -> TargetedReconstructionRetryPlan:
        """Preflight a retry of the same frozen target set."""

        with self._lock:
            session = self._require_started(task_id, attempt_id)
            recovery_plan = self._recovery.validate_task_failure(
                task_id, attempt_id, error
            )
            decision = recovery_plan.decision
            if decision.action is not RecoveryAction.RETRY_TASK:
                raise TargetedReconstructionError(
                    "recovery authority did not admit targeted retry: {}"
                    .format(decision.action.value)
                )
            if not isinstance(decision.attempt_id, AttemptID):
                raise TargetedReconstructionError(
                    "targeted retry has no next attempt"
                )
            assert session.execution is not None
            next_execution = session.execution.for_attempt(decision.attempt_id)
            owner_plan = self._owner.validate_retry_target_outputs(
                session.execution, decision.attempt_id
            )
            next_session = replace(session, execution=next_execution)
            return TargetedReconstructionRetryPlan(
                session, next_session, owner_plan, recovery_plan
            )

    def commit_system_retry(
        self, plan: TargetedReconstructionRetryPlan
    ) -> TargetedReconstructionSession:
        if not isinstance(plan, TargetedReconstructionRetryPlan):
            raise TypeError("plan must be a TargetedReconstructionRetryPlan")
        with self._lock:
            assert plan.session.execution is not None
            self._require_started(
                plan.session.task_id, plan.session.execution.attempt_id
            )
            self._require_recovery_plan_current(plan.recovery_plan)
            if not self._owner.commit_retry_target_outputs(plan.owner_plan):
                raise TargetedReconstructionError(
                    "owner rejected targeted retry CAS"
                )
            self._recovery.commit_validated_transition(plan.recovery_plan)
            self._current[plan.session.task_id] = plan.next_session
            return plan.next_session

    def retry_system_failure(
        self, task_id: TaskID, attempt_id: AttemptID, error: object,
    ) -> TargetedReconstructionSession:
        with self._lock:
            return self.commit_system_retry(
                self.validate_system_retry(task_id, attempt_id, error)
            )

    def validate_terminal_failure(
        self, task_id: TaskID, attempt_id: AttemptID, error: object,
    ) -> TargetedReconstructionFailurePlan:
        """Preflight an atomic terminal error over the frozen targets."""

        with self._lock:
            session = self._require_started(task_id, attempt_id)
            expected = dict(session.expected_attempts)
            # Terminal task failure cannot leave a post-START lost sibling in
            # limbo: no future retry session will run.  Fold queued losses into
            # the same atomic owner error transaction, preserving each slot's
            # own producer epoch.
            expected.update(
                self._queued.get(task_id, {})
            )
            for object_id in session.target_output_ids:
                expected[object_id] = attempt_id
            owner_plan = self._owner.validate_publish_terminal_target_error(
                session.full_manifest, expected, error
            )
            if owner_plan is None:
                raise TargetedReconstructionError(
                    "owner fenced targeted terminal failure"
                )
            recovery_plan = (
                self._recovery.validate_terminal_reconstruction_failure(
                    task_id, attempt_id, error
                )
            )
            if (
                recovery_plan.decision.action
                is not RecoveryAction.FAIL_RECONSTRUCTION_TARGETS
            ):
                raise TargetedReconstructionError(
                    "recovery authority fenced targeted terminal failure"
                )
            return TargetedReconstructionFailurePlan(
                session, owner_plan, recovery_plan
            )

    def commit_terminal_failure(
        self, plan: TargetedReconstructionFailurePlan
    ) -> None:
        if not isinstance(plan, TargetedReconstructionFailurePlan):
            raise TypeError("plan must be a TargetedReconstructionFailurePlan")
        with self._lock:
            attempt_id = plan.session.execution.attempt_id
            self._require_started(plan.session.task_id, attempt_id)
            self._require_recovery_plan_current(plan.recovery_plan)
            if not self._owner.commit_publish_terminal_target_error(
                plan.owner_plan
            ):
                raise TargetedReconstructionError(
                    "owner rejected targeted terminal error"
                )
            self._recovery.commit_validated_transition(plan.recovery_plan)
            self._current.pop(plan.session.task_id, None)
            self._queued.pop(plan.session.task_id, None)

    def complete(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> Optional[TargetedReconstructionSession]:
        """Close STARTED work and promote post-START losses to OPEN."""

        with self._lock:
            session = self._current.get(task_id)
            if (
                session is None
                or session.phase is not TargetedSessionPhase.STARTED
                or session.execution is None
                or session.execution.attempt_id != attempt_id
            ):
                return None
            record = self._recovery.task_record(task_id)
            if (
                record.current_attempt != attempt_id
                or record.state is not TaskState.SUCCEEDED
                or self._recovery.active_recovery(task_id) is not None
            ):
                return None
            # Current state may already be LOST after a successful publication.
            # Completion relies on the immutable all-target receipt, not on a
            # transient READY observation.
            if self._owner.targeted_publication_receipt(session.execution) is None:
                return None
            return self._close_and_promote(session)

    def _validate_loss(self, loss: TargetedLoss) -> TaskOutputManifest:
        owner = self._owner.snapshot(loss.object_id)
        if owner.collection_pending:
            raise TargetedReconstructionError(
                "cannot target output while collection is pending"
            )
        if owner.state is not ObjectState.LOST:
            raise TargetedReconstructionError(
                "targeted reconstruction requires a LOST output"
            )
        if owner.current_attempt != loss.expected_attempt:
            raise TargetedReconstructionError(
                "targeted loss expected attempt is stale"
            )
        spec = owner.producer_task_spec
        if spec is None:
            raise TargetedReconstructionError(
                "targeted output has no producer lineage"
            )
        manifest = TaskOutputManifest.from_task_spec(spec)
        recovery = self._recovery.reconstruction_snapshot(loss.object_id)
        if recovery.is_put or recovery.lineage is None:
            raise TargetedReconstructionError(
                "targeted output has no reconstructible producer lineage"
            )
        if recovery.lineage.task_spec != spec:
            raise TargetedReconstructionError(
                "owner and recovery disagree on producer lineage"
            )
        if recovery.lineage.output_ids != manifest.output_ids:
            raise TargetedReconstructionError(
                "owner and recovery disagree on output manifest"
            )
        return manifest

    def _require_started(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> TargetedReconstructionSession:
        session = self._current.get(task_id)
        if (
            session is None
            or session.phase is not TargetedSessionPhase.STARTED
            or session.execution is None
            or session.execution.attempt_id != attempt_id
        ):
            raise TargetedReconstructionError(
                "no matching targeted execution is active"
            )
        return session

    def _require_recovery_plan_current(
        self, plan: RecoveryTransitionPlan
    ) -> None:
        if (
            plan.task_id is None
            or plan.before is None
            or self._recovery.task_record(plan.task_id) != plan.before
            or self._recovery.active_recovery(plan.task_id)
            != plan.active_before
        ):
            raise TargetedReconstructionError(
                "recovery authority changed before targeted commit"
            )

    def _close_and_promote(
        self, session: TargetedReconstructionSession
    ) -> Optional[TargetedReconstructionSession]:
        self._current.pop(session.task_id, None)
        self._open_failures.pop(session.task_id, None)
        queued = self._queued.pop(session.task_id, None)
        if not queued:
            return None
        promoted = self._open_session(session.full_manifest, queued)
        self._current[session.task_id] = promoted
        return promoted

    def _manifest_for_task(self, task_id: TaskID) -> TaskOutputManifest:
        session = self._current.get(task_id)
        if session is None:
            raise TargetedReconstructionError("unknown targeted session")
        return session.full_manifest

    @staticmethod
    def _ordered_losses(
        manifest: TaskOutputManifest,
        values: Mapping[ObjectID, AttemptID],
    ) -> tuple[TargetedLoss, ...]:
        return tuple(
            TargetedLoss(object_id, values[object_id])
            for object_id in manifest.output_ids
            if object_id in values
        )

    @classmethod
    def _open_session(
        cls, manifest: TaskOutputManifest,
        values: Mapping[ObjectID, AttemptID],
    ) -> TargetedReconstructionSession:
        return TargetedReconstructionSession(
            manifest.task_id, manifest, TargetedSessionPhase.OPEN,
            cls._ordered_losses(manifest, values),
        )


__all__ = [
    "TargetedLoss",
    "TargetedReconstructionCoordinator",
    "TargetedReconstructionError",
    "TargetedReconstructionRequest",
    "TargetedReconstructionFailurePlan",
    "TargetedReconstructionRetryPlan",
    "TargetedReconstructionSession",
    "TargetedReconstructionStartPlan",
    "TargetedReconstructionSuccessPlan",
    "TargetedRequestDisposition",
    "TargetedSessionPhase",
]
