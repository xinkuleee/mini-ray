"""Pure task-attempt and lineage-recovery state.

This module deliberately does not submit work, contact a worker, or mutate an
object store.  It decides whether an attempt message is current, whether a
system failure may consume another retry, and which producer should be
resubmitted when a task output loses its final replica.  The caller turns the
returned :class:`RecoveryDecision` into scheduler actions.

The implementation relies only on the small structural contracts exposed by
``AttemptID`` and ``TaskSpec``.  This keeps the state machine usable with the
real protocol dataclasses and with tiny deterministic teaching fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Hashable, Iterable

from .errors import SystemTaskError, TaskError, UnreconstructableObjectError


TaskID = Hashable
AttemptID = Hashable
ObjectID = Hashable
TaskSpec = object


class RecoveryStateError(RuntimeError):
    """A caller requested a transition that contradicts task history."""


class UnknownTaskError(RecoveryStateError, KeyError):
    """The recovery manager has no producer record for a task."""


class LineageConflictError(RecoveryStateError):
    """A logical task or object was registered with different lineage."""


class FailureKind(str, Enum):
    """Failures caused by user code are not system-retry candidates."""

    APPLICATION = "APPLICATION"
    SYSTEM = "SYSTEM"


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_PENDING = "RETRY_PENDING"
    SUCCEEDED = "SUCCEEDED"
    APPLICATION_FAILED = "APPLICATION_FAILED"
    SYSTEM_FAILED = "SYSTEM_FAILED"


class RecoveryAction(str, Enum):
    """An explicit instruction for the component that owns execution."""

    ACCEPT_SUCCESS = "ACCEPT_SUCCESS"
    RETRY_TASK = "RETRY_TASK"
    FAIL_APPLICATION = "FAIL_APPLICATION"
    FAIL_RETRY_EXHAUSTED = "FAIL_RETRY_EXHAUSTED"
    FENCE_STALE_ATTEMPT = "FENCE_STALE_ATTEMPT"
    START_RECONSTRUCTION = "START_RECONSTRUCTION"
    JOIN_RECONSTRUCTION = "JOIN_RECONSTRUCTION"
    FAIL_RECONSTRUCTION_TARGETS = "FAIL_RECONSTRUCTION_TARGETS"
    UNRECONSTRUCTABLE_OBJECT = "UNRECONSTRUCTABLE_OBJECT"


@dataclass(frozen=True)
class RecoveryDecision:
    """A side-effect-free recovery or retry decision.

    ``producer_task_spec`` is the original logical specification.  A submitter
    must use ``attempt_id`` as its physical attempt while preserving the
    TaskID and output ObjectIDs in this decision.
    """

    action: RecoveryAction
    task_id: TaskID | None = None
    attempt_id: AttemptID | None = None
    requested_object_id: ObjectID | None = None
    output_ids: tuple[ObjectID, ...] = ()
    producer_task_spec: TaskSpec | None = None
    failure_kind: FailureKind | None = None
    error: object | None = None
    reason: str = ""

    @property
    def should_submit(self) -> bool:
        return self.action in (
            RecoveryAction.RETRY_TASK,
            RecoveryAction.START_RECONSTRUCTION,
        )

    @property
    def is_fenced(self) -> bool:
        return self.action is RecoveryAction.FENCE_STALE_ATTEMPT


def classify_failure(failure: object) -> FailureKind:
    """Classify a typed error, protocol status, or reply without strings.

    ``TaskReplyStatus`` is intentionally consumed structurally so this module
    does not need to own the wire protocol.  Arbitrary exception types are
    rejected rather than being silently retried as system failures.
    """

    if isinstance(failure, FailureKind):
        return failure
    if isinstance(failure, TaskError):
        return FailureKind.APPLICATION
    if isinstance(failure, SystemTaskError):
        return FailureKind.SYSTEM

    status = getattr(failure, "status", failure)
    candidates = {
        str(getattr(status, "name", "")).upper(),
        str(getattr(status, "value", "")).upper(),
    }
    if isinstance(status, str):
        candidates.add(status.upper())
    if "APPLICATION_ERROR" in candidates or "APPLICATION" in candidates:
        return FailureKind.APPLICATION
    if "SYSTEM_ERROR" in candidates or "SYSTEM" in candidates:
        return FailureKind.SYSTEM
    raise TypeError(
        "failure must be FailureKind, TaskError, SystemTaskError, or a "
        "protocol application/system failure status"
    )


@dataclass
class TaskRecord:
    """Authoritative retry state for one logical task.

    ``max_retries`` counts physical attempts after the initial attempt.  Every
    accepted system retry or reconstruction consumes one slot.  Application
    failures never consume the budget and never advance ``current_attempt``.
    """

    task_id: TaskID
    current_attempt: AttemptID
    max_retries: int = 0
    retries_started: int = 0
    state: TaskState = TaskState.PENDING
    last_error: object | None = None

    def __post_init__(self) -> None:
        _require_hashable(self.task_id, "task_id")
        _validate_attempt(self.current_attempt, self.task_id)
        _require_non_negative_int(self.max_retries, "max_retries")
        _require_non_negative_int(self.retries_started, "retries_started")
        if self.retries_started > self.max_retries:
            raise ValueError("retries_started cannot exceed max_retries")
        self.state = TaskState(self.state)

    @property
    def retries_remaining(self) -> int:
        return self.max_retries - self.retries_started

    def is_current(self, attempt_id: AttemptID) -> bool:
        _validate_attempt(attempt_id, self.task_id)
        return attempt_id == self.current_attempt

    def mark_running(self, attempt_id: AttemptID) -> bool:
        """Mark the current attempt running; stale starts are fenced."""

        if not self.is_current(attempt_id):
            return False
        if self.state in (TaskState.APPLICATION_FAILED, TaskState.SYSTEM_FAILED):
            raise RecoveryStateError(
                f"cannot run terminal task {self.task_id!r} in {self.state.value}"
            )
        if self.state is TaskState.SUCCEEDED:
            raise RecoveryStateError("a succeeded task needs reconstruction first")
        self.state = TaskState.RUNNING
        return True

    def record_success(self, attempt_id: AttemptID) -> RecoveryDecision:
        if not self.is_current(attempt_id):
            return self._fenced(attempt_id, "success came from a stale attempt")
        if self.state in (TaskState.APPLICATION_FAILED, TaskState.SYSTEM_FAILED):
            raise RecoveryStateError(
                f"success conflicts with terminal state {self.state.value}"
            )
        self.state = TaskState.SUCCEEDED
        self.last_error = None
        return RecoveryDecision(
            RecoveryAction.ACCEPT_SUCCESS,
            task_id=self.task_id,
            attempt_id=attempt_id,
            reason="current attempt completed successfully",
        )

    def record_failure(
        self,
        attempt_id: AttemptID,
        failure: object,
        *,
        error: object | None = None,
    ) -> RecoveryDecision:
        if not self.is_current(attempt_id):
            return self._fenced(attempt_id, "failure came from a stale attempt")
        # Fencing precedes payload interpretation.  A delayed obsolete message
        # must be harmless even if an old peer encoded its error differently.
        kind = classify_failure(failure)

        recorded_error = failure if error is None else error
        if kind is FailureKind.APPLICATION:
            if self.state is TaskState.SYSTEM_FAILED:
                raise RecoveryStateError(
                    "application failure conflicts with exhausted system failure"
                )
            if self.state is TaskState.SUCCEEDED:
                raise RecoveryStateError("failure conflicts with accepted success")
            self.state = TaskState.APPLICATION_FAILED
            self.last_error = recorded_error
            return RecoveryDecision(
                RecoveryAction.FAIL_APPLICATION,
                task_id=self.task_id,
                attempt_id=attempt_id,
                failure_kind=kind,
                error=recorded_error,
                reason="user-code failures are terminal and are not system-retried",
            )

        if self.state is TaskState.APPLICATION_FAILED:
            raise RecoveryStateError(
                "system failure conflicts with terminal application failure"
            )
        if self.state is TaskState.SUCCEEDED:
            raise RecoveryStateError("failure conflicts with accepted success")
        if self.state is TaskState.SYSTEM_FAILED:
            return self._retry_exhausted(recorded_error)
        return self._advance_or_exhaust(recorded_error, "system failure")

    def begin_reconstruction(self) -> RecoveryDecision:
        """Consume a retry slot to recreate outputs of a succeeded task."""

        if self.state is TaskState.SYSTEM_FAILED:
            return self._retry_exhausted(self.last_error)
        if self.state is TaskState.APPLICATION_FAILED:
            return RecoveryDecision(
                RecoveryAction.FAIL_APPLICATION,
                task_id=self.task_id,
                attempt_id=self.current_attempt,
                failure_kind=FailureKind.APPLICATION,
                error=self.last_error,
                reason="the producer ended in a terminal application failure",
            )
        if self.state is not TaskState.SUCCEEDED:
            raise RecoveryStateError(
                "lineage reconstruction requires a previously succeeded task"
            )
        return self._advance_or_exhaust(
            UnreconstructableObjectError(
                "the task retry budget cannot fund another reconstruction"
            ),
            "lineage reconstruction",
        )

    def _advance_or_exhaust(
        self, error: object, reason: str
    ) -> RecoveryDecision:
        self.last_error = error
        if self.retries_remaining == 0:
            self.state = TaskState.SYSTEM_FAILED
            return self._retry_exhausted(error)

        next_attempt = _next_attempt(self.current_attempt, self.task_id)
        self.current_attempt = next_attempt
        self.retries_started += 1
        self.state = TaskState.RETRY_PENDING
        return RecoveryDecision(
            RecoveryAction.RETRY_TASK,
            task_id=self.task_id,
            attempt_id=next_attempt,
            failure_kind=FailureKind.SYSTEM,
            error=error,
            reason=f"retry admitted after {reason}",
        )

    def _retry_exhausted(self, error: object) -> RecoveryDecision:
        return RecoveryDecision(
            RecoveryAction.FAIL_RETRY_EXHAUSTED,
            task_id=self.task_id,
            attempt_id=self.current_attempt,
            failure_kind=FailureKind.SYSTEM,
            error=error,
            reason="task retry budget is exhausted",
        )

    def _fenced(self, attempt_id: AttemptID, reason: str) -> RecoveryDecision:
        return RecoveryDecision(
            RecoveryAction.FENCE_STALE_ATTEMPT,
            task_id=self.task_id,
            attempt_id=attempt_id,
            reason=reason,
        )


@dataclass(frozen=True)
class ProducerLineage:
    task_id: TaskID
    task_spec: TaskSpec
    output_ids: tuple[ObjectID, ...]


@dataclass(frozen=True)
class ReconstructionSnapshot:
    """Side-effect-free recovery facts consumed by graph preflight.

    Dependency edges are deliberately absent: callers must derive them from
    ``lineage.task_spec`` so RecoveryManager never grows a second adjacency
    database that could disagree with producer lineage.
    """

    object_id: ObjectID
    lineage: ProducerLineage | None
    task_state: TaskState | None
    current_attempt: AttemptID | None
    active_recovery: AttemptID | None
    is_put: bool
    retries_remaining: int | None


@dataclass(frozen=True)
class CollectedObjectForgetPlan:
    """Validated, side-effect-free lineage deletion transaction."""

    object_id: ObjectID
    kind: str
    task_id: TaskID | None = None
    remove_task: bool = False
    active_attempt: AttemptID | None = None


@dataclass(frozen=True)
class RecoveryTransitionPlan:
    """Side-effect-free, compare-and-commit task recovery transition.

    ``before`` is an immutable snapshot of the mutable task record at validation
    time.  ``after`` is the complete state to install.  Composition layers may
    therefore preflight another authority before calling ``commit_transition``;
    the commit itself performs only assignments and an idempotent active-marker
    update.
    """

    task_id: TaskID | None
    before: TaskRecord | None
    after: TaskRecord | None
    active_before: AttemptID | None
    active_after: AttemptID | None
    decision: RecoveryDecision


class RecoveryManager:
    """Own producer lineage and merge reconstruction by logical TaskID."""

    def __init__(self) -> None:
        self._tasks: dict[TaskID, TaskRecord] = {}
        self._lineages: dict[TaskID, ProducerLineage] = {}
        self._object_producers: dict[ObjectID, TaskID] = {}
        self._put_objects: set[ObjectID] = set()
        self._active_recoveries: dict[TaskID, AttemptID] = {}

    def register_task(
        self,
        task_spec: TaskSpec,
        *,
        output_ids: Iterable[ObjectID] | None = None,
        max_retries: int = 0,
    ) -> TaskRecord:
        """Save immutable producer lineage before the task is executed."""

        self.validate_register_task(
            task_spec, output_ids=output_ids, max_retries=max_retries
        )
        task_id = _required_attribute(task_spec, "task_id")
        attempt_id = _required_attribute(task_spec, "attempt_id")
        _require_hashable(task_id, "task_spec.task_id")
        _validate_attempt(attempt_id, task_id)
        _require_non_negative_int(max_retries, "max_retries")
        outputs = _normalize_output_ids(task_spec, output_ids)

        old_lineage = self._lineages.get(task_id)
        if old_lineage is not None:
            old_record = self._tasks[task_id]
            if (
                not _safely_equal(old_lineage.task_spec, task_spec)
                or old_lineage.output_ids != outputs
                or old_record.max_retries != max_retries
            ):
                raise LineageConflictError(
                    f"task {task_id!r} was registered with different lineage"
                )
            return old_record

        for object_id in outputs:
            _require_hashable(object_id, "output object ID")
            object_task_id = getattr(object_id, "task_id", task_id)
            if object_task_id != task_id:
                raise ValueError(
                    f"output object {object_id!r} does not belong to task {task_id!r}"
                )
            if object_id in self._put_objects:
                raise LineageConflictError(
                    f"object {object_id!r} is already registered as put data"
                )
            producer = self._object_producers.get(object_id)
            if producer is not None and producer != task_id:
                raise LineageConflictError(
                    f"object {object_id!r} already belongs to task {producer!r}"
                )

        record = TaskRecord(task_id, attempt_id, max_retries=max_retries)
        lineage = ProducerLineage(task_id, task_spec, outputs)
        self._tasks[task_id] = record
        self._lineages[task_id] = lineage
        for object_id in outputs:
            self._object_producers[object_id] = task_id
        return record

    def validate_register_task(
        self,
        task_spec: TaskSpec,
        *,
        output_ids: Iterable[ObjectID] | None = None,
        max_retries: int = 0,
    ) -> None:
        """Run every fallible lineage-registration check without mutation."""

        task_id = _required_attribute(task_spec, "task_id")
        attempt_id = _required_attribute(task_spec, "attempt_id")
        _require_hashable(task_id, "task_spec.task_id")
        _validate_attempt(attempt_id, task_id)
        _require_non_negative_int(max_retries, "max_retries")
        outputs = _normalize_output_ids(task_spec, output_ids)
        old_lineage = self._lineages.get(task_id)
        if old_lineage is not None:
            old_record = self._tasks[task_id]
            if (
                not _safely_equal(old_lineage.task_spec, task_spec)
                or old_lineage.output_ids != outputs
                or old_record.max_retries != max_retries
            ):
                raise LineageConflictError(
                    f"task {task_id!r} was registered with different lineage"
                )
            return
        for object_id in outputs:
            _require_hashable(object_id, "output object ID")
            if getattr(object_id, "task_id", task_id) != task_id:
                raise ValueError(
                    f"output object {object_id!r} does not belong to task {task_id!r}"
                )
            if object_id in self._put_objects:
                raise LineageConflictError(
                    f"object {object_id!r} is already registered as put data"
                )
            producer = self._object_producers.get(object_id)
            if producer is not None and producer != task_id:
                raise LineageConflictError(
                    f"object {object_id!r} already belongs to task {producer!r}"
                )

    register_producer = register_task

    def abort_registered_task(
        self,
        task_spec: TaskSpec,
        *,
        output_ids: Iterable[ObjectID] | None = None,
        max_retries: int = 0,
    ) -> bool:
        """Undo an exact initial registration before execution starts.

        Core submission holds its composition lock across register and abort.
        Restricting this operation to the untouched initial ``PENDING`` record
        prevents it from becoming a general lineage-deletion back door.
        """

        task_id = _required_attribute(task_spec, "task_id")
        outputs = _normalize_output_ids(task_spec, output_ids)
        lineage = self._lineages.get(task_id)
        if lineage is None:
            return False
        record = self._tasks[task_id]
        if (
            not _safely_equal(lineage.task_spec, task_spec)
            or lineage.output_ids != outputs
            or record.current_attempt != _required_attribute(
                task_spec, "attempt_id"
            )
            or record.max_retries != max_retries
            or record.retries_started != 0
            or record.state is not TaskState.PENDING
            or record.last_error is not None
            or task_id in self._active_recoveries
            or any(
                self._object_producers.get(object_id) != task_id
                for object_id in outputs
            )
        ):
            raise RecoveryStateError(
                "registered task lineage is no longer abortable"
            )
        for object_id in outputs:
            self._object_producers.pop(object_id, None)
        self._lineages.pop(task_id, None)
        self._tasks.pop(task_id, None)
        return True

    def register_put(self, object_id: ObjectID) -> bool:
        """Record an object with bytes but no replayable producer task."""

        _require_hashable(object_id, "object_id")
        if object_id in self._object_producers:
            raise LineageConflictError(
                f"task output {object_id!r} cannot also be registered as put data"
            )
        old_size = len(self._put_objects)
        self._put_objects.add(object_id)
        return len(self._put_objects) != old_size

    def task_record(self, task_id: TaskID) -> TaskRecord:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise UnknownTaskError(f"unknown task {task_id!r}") from None

    def lineage_for_object(self, object_id: ObjectID) -> ProducerLineage | None:
        task_id = self._object_producers.get(object_id)
        return self._lineages.get(task_id) if task_id is not None else None

    def reconstruction_snapshot(
        self, object_id: ObjectID
    ) -> ReconstructionSnapshot:
        """Return graph-planning facts without consuming recovery budget."""

        _require_hashable(object_id, "object_id")
        task_id = self._object_producers.get(object_id)
        if task_id is None:
            return ReconstructionSnapshot(
                object_id, None, None, None, None,
                object_id in self._put_objects, None,
            )
        lineage = self._lineages[task_id]
        record = self._tasks[task_id]
        return ReconstructionSnapshot(
            object_id=object_id,
            lineage=lineage,
            task_state=record.state,
            current_attempt=record.current_attempt,
            active_recovery=self._active_recoveries.get(task_id),
            is_put=False,
            retries_remaining=record.retries_remaining,
        )

    def active_recovery(self, task_id: TaskID) -> AttemptID | None:
        return self._active_recoveries.get(task_id)

    def validate_forget_collected_object(
        self, object_id: ObjectID, *, expected_task_spec: TaskSpec | None,
        expected_attempt: AttemptID | None = None,
    ) -> CollectedObjectForgetPlan:
        """Validate lineage identity without mutating recovery state.

        The Core validates both recovery and owner-table transactions before
        committing either.  Because every Core recovery mutation is serialized
        by the same composition lock, the returned plan can then be applied by
        :meth:`commit_forget_collected_object` without another fallible check.
        """

        _require_hashable(object_id, "object_id")
        if object_id in self._put_objects:
            if expected_task_spec is not None:
                raise LineageConflictError(
                    "put-object collection unexpectedly carried task lineage"
                )
            return CollectedObjectForgetPlan(object_id, "put")

        task_id = self._object_producers.get(object_id)
        if task_id is None:
            # Pure owner-table fixtures and exact completion replay may not
            # have registered recovery metadata.
            return CollectedObjectForgetPlan(object_id, "none")
        lineage = self._lineages[task_id]
        if expected_task_spec is None or not _safely_equal(
            lineage.task_spec, expected_task_spec
        ):
            raise LineageConflictError(
                f"collection lineage changed for object {object_id!r}"
            )
        active_attempt = self._active_recoveries.get(task_id)
        if active_attempt is not None and active_attempt != expected_attempt:
            raise RecoveryStateError(
                f"collection attempt does not match active reconstruction "
                f"for task {task_id!r}"
            )

        remaining = any(
            other_id != object_id and producer == task_id
            for other_id, producer in self._object_producers.items()
        )
        return CollectedObjectForgetPlan(
            object_id, "task", task_id, not remaining, active_attempt
        )

    def commit_forget_collected_object(
        self, plan: CollectedObjectForgetPlan
    ) -> bool:
        """Apply a validated plan using only idempotent, non-failing ops.

        Callers must hold the Core composition lock continuously from validate
        through commit.  This method intentionally uses ``discard``/``pop`` so
        owner metadata can never be deleted while a later incidental KeyError
        leaves recovery lineage behind.
        """

        if not isinstance(plan, CollectedObjectForgetPlan):
            raise TypeError("plan must be a CollectedObjectForgetPlan")
        if plan.kind == "none":
            return False
        if plan.kind == "put":
            self._put_objects.discard(plan.object_id)
            return True
        if plan.kind != "task" or plan.task_id is None:
            raise TypeError("forget plan has an invalid kind")
        # Reconstruction is task-scoped: collecting one return slot must not
        # make a sibling start a second reconstruction for the same producer.
        # Completion clears the session through record_task_success/failure;
        # collection clears it only when the final sibling removes the task.
        if plan.active_attempt is not None and plan.remove_task:
            self._active_recoveries.pop(plan.task_id, None)
        self._object_producers.pop(plan.object_id, None)
        if plan.remove_task:
            self._lineages.pop(plan.task_id, None)
            self._tasks.pop(plan.task_id, None)
        return True

    def forget_collected_object(
        self, object_id: ObjectID, *, expected_task_spec: TaskSpec | None,
        expected_attempt: AttemptID | None = None,
    ) -> bool:
        """Convenience wrapper for pure callers outside Core composition."""

        plan = self.validate_forget_collected_object(
            object_id, expected_task_spec=expected_task_spec,
            expected_attempt=expected_attempt,
        )
        return self.commit_forget_collected_object(plan)

    def record_task_success(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> RecoveryDecision:
        return self.commit_transition(
            self.validate_task_success(task_id, attempt_id)
        )

    def validate_task_success(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> RecoveryTransitionPlan:
        """Plan one success without changing task or reconstruction state."""

        record = self.task_record(task_id)
        before = self._copy_record(record)
        after = self._copy_record(before)
        decision = self._with_lineage(after.record_success(attempt_id))
        active_before = self._active_recoveries.get(task_id)
        active_after = active_before
        if (
            decision.action is RecoveryAction.ACCEPT_SUCCESS
            and active_before == attempt_id
        ):
            active_after = None
        return RecoveryTransitionPlan(
            task_id, before, after, active_before, active_after, decision
        )

    def record_task_failure(
        self,
        task_id: TaskID,
        attempt_id: AttemptID,
        failure: object,
        *,
        error: object | None = None,
    ) -> RecoveryDecision:
        return self.commit_transition(
            self.validate_task_failure(
                task_id, attempt_id, failure, error=error
            )
        )

    def validate_task_failure(
        self,
        task_id: TaskID,
        attempt_id: AttemptID,
        failure: object,
        *,
        error: object | None = None,
    ) -> RecoveryTransitionPlan:
        """Plan application failure, retry, or exhaustion without mutation."""

        record = self.task_record(task_id)
        before = self._copy_record(record)
        after = self._copy_record(before)
        decision = self._with_lineage(
            after.record_failure(attempt_id, failure, error=error)
        )
        active_before = self._active_recoveries.get(task_id)
        active_after = active_before
        if active_before == attempt_id:
            if decision.action is RecoveryAction.RETRY_TASK:
                assert decision.attempt_id is not None
                active_after = decision.attempt_id
            elif decision.action in (
                RecoveryAction.FAIL_APPLICATION,
                RecoveryAction.FAIL_RETRY_EXHAUSTED,
            ):
                active_after = None
        return RecoveryTransitionPlan(
            task_id, before, after, active_before, active_after, decision
        )

    def record_terminal_system_failure(
        self, task_id: TaskID, attempt_id: AttemptID, error: object
    ) -> RecoveryDecision:
        """Terminate a current attempt without admitting another retry.

        Scheduling rejection, cancellation and other pre-execution terminal
        paths have already decided that this logical attempt will not be
        retried.  They still must update the same RecoveryManager authority so
        an active reconstruction cannot remain RETRY_PENDING after its owner
        object becomes ERROR.
        """

        return self.commit_transition(
            self.validate_terminal_system_failure(
                task_id, attempt_id, error
            )
        )

    def validate_terminal_system_failure(
        self, task_id: TaskID, attempt_id: AttemptID, error: object = None
    ) -> RecoveryTransitionPlan:
        """Plan a no-retry system terminal transition without mutation."""

        record = self.task_record(task_id)
        before = self._copy_record(record)
        after = self._copy_record(before)
        active_before = self._active_recoveries.get(task_id)
        if not record.is_current(attempt_id):
            decision = self._with_lineage(record._fenced(
                attempt_id, "terminal system failure came from a stale attempt"
            ))
            return RecoveryTransitionPlan(
                task_id, before, after, active_before, active_before, decision
            )
        if record.state is TaskState.SUCCEEDED:
            raise RecoveryStateError(
                "terminal failure conflicts with accepted success"
            )
        if record.state is TaskState.APPLICATION_FAILED:
            raise RecoveryStateError(
                "system failure conflicts with terminal application failure"
            )
        after.state = TaskState.SYSTEM_FAILED
        after.last_error = error
        active_after = (
            None if active_before == attempt_id else active_before
        )
        decision = self._with_lineage(RecoveryDecision(
            RecoveryAction.FAIL_RETRY_EXHAUSTED,
            task_id=task_id,
            attempt_id=attempt_id,
            failure_kind=FailureKind.SYSTEM,
            error=error,
            reason="current attempt ended in a terminal system failure",
        ))
        return RecoveryTransitionPlan(
            task_id, before, after, active_before, active_after, decision
        )

    def validate_terminal_reconstruction_failure(
        self, task_id: TaskID, attempt_id: AttemptID, error: object = None
    ) -> RecoveryTransitionPlan:
        """End one targeted reconstruction without poisoning its task.

        A physical targeted attempt may fail after some healthy siblings from
        older attempts remain usable.  Marking the whole TaskID SYSTEM_FAILED
        would make a later loss of those siblings unreconstructable even when
        retry budget remains.  This transition therefore clears only the
        active execution marker, restores the logical task to SUCCEEDED, and
        retains the already-consumed retry count.  The owner independently
        records ERROR on exactly the failed target slots.
        """

        record = self.task_record(task_id)
        before = self._copy_record(record)
        after = self._copy_record(before)
        active_before = self._active_recoveries.get(task_id)
        if not record.is_current(attempt_id):
            decision = self._with_lineage(record._fenced(
                attempt_id,
                "targeted terminal failure came from a stale attempt",
            ))
            return RecoveryTransitionPlan(
                task_id, before, after, active_before, active_before, decision
            )
        if active_before != attempt_id or record.state is not TaskState.RETRY_PENDING:
            raise RecoveryStateError(
                "targeted terminal failure requires its active reconstruction"
            )
        after.state = TaskState.SUCCEEDED
        after.last_error = error
        decision = self._with_lineage(RecoveryDecision(
            RecoveryAction.FAIL_RECONSTRUCTION_TARGETS,
            task_id=task_id,
            attempt_id=attempt_id,
            failure_kind=FailureKind.SYSTEM,
            error=error,
            reason=(
                "targeted reconstruction ended; healthy siblings retain "
                "their lineage and remaining retry budget"
            ),
        ))
        return RecoveryTransitionPlan(
            task_id, before, after, active_before, None, decision
        )

    @staticmethod
    def _copy_record(record: TaskRecord) -> TaskRecord:
        return replace(record)

    def commit_transition(
        self, plan: RecoveryTransitionPlan
    ) -> RecoveryDecision:
        """Install one fully validated transition using assignment only."""

        if not isinstance(plan, RecoveryTransitionPlan):
            raise TypeError("plan must be a RecoveryTransitionPlan")
        return self.commit_validated_transition(plan)

    def commit_validated_transition(
        self, plan: RecoveryTransitionPlan
    ) -> RecoveryDecision:
        """Apply a caller-held transition using only assignments.

        The caller must retain its composition lock continuously from every
        authority preflight through this method.  No equality check, retry
        admission, callback, or other business decision occurs here, making it
        safe as the second half of an owner+recovery atomic composition.
        """

        if not isinstance(plan, RecoveryTransitionPlan):
            raise TypeError("plan must be a RecoveryTransitionPlan")
        if plan.task_id is None:
            return plan.decision
        assert plan.before is not None and plan.after is not None
        record = self._tasks[plan.task_id]
        record.current_attempt = plan.after.current_attempt
        record.max_retries = plan.after.max_retries
        record.retries_started = plan.after.retries_started
        record.state = plan.after.state
        record.last_error = plan.after.last_error
        if plan.active_after is None:
            self._active_recoveries.pop(plan.task_id, None)
        else:
            self._active_recoveries[plan.task_id] = plan.active_after
        return plan.decision

    def validate_request_reconstruction(
        self, object_id: ObjectID
    ) -> RecoveryTransitionPlan:
        """Plan START/JOIN/failure without consuming recovery budget."""

        task_id = self._object_producers.get(object_id)
        if task_id is None:
            if object_id in self._put_objects:
                detail = "put objects have no producer task lineage"
            else:
                detail = "the object has no registered producer task lineage"
            error = UnreconstructableObjectError(detail)
            decision = RecoveryDecision(
                RecoveryAction.UNRECONSTRUCTABLE_OBJECT,
                requested_object_id=object_id,
                error=error,
                reason=detail,
            )
            return RecoveryTransitionPlan(
                None, None, None, None, None, decision
            )

        lineage = self._lineages[task_id]
        record = self._tasks[task_id]
        before = self._copy_record(record)
        active_attempt = self._active_recoveries.get(task_id)
        if active_attempt is not None:
            decision = RecoveryDecision(
                RecoveryAction.JOIN_RECONSTRUCTION,
                task_id=task_id,
                attempt_id=active_attempt,
                requested_object_id=object_id,
                output_ids=lineage.output_ids,
                producer_task_spec=lineage.task_spec,
                reason="a reconstruction for this producer task is already active",
            )
            return RecoveryTransitionPlan(
                task_id, before, self._copy_record(before),
                active_attempt, active_attempt, decision,
            )

        after = self._copy_record(before)
        retry = after.begin_reconstruction()
        if retry.action is not RecoveryAction.RETRY_TASK:
            decision = RecoveryDecision(
                retry.action,
                task_id=task_id,
                attempt_id=retry.attempt_id,
                requested_object_id=object_id,
                output_ids=lineage.output_ids,
                producer_task_spec=lineage.task_spec,
                failure_kind=retry.failure_kind,
                error=retry.error,
                reason=retry.reason,
            )
            return RecoveryTransitionPlan(
                task_id, before, after, None, None, decision
            )

        assert retry.attempt_id is not None
        decision = RecoveryDecision(
            RecoveryAction.START_RECONSTRUCTION,
            task_id=task_id,
            attempt_id=retry.attempt_id,
            requested_object_id=object_id,
            output_ids=lineage.output_ids,
            producer_task_spec=lineage.task_spec,
            failure_kind=FailureKind.SYSTEM,
            reason="resubmit the producer once for all of its lost outputs",
        )
        return RecoveryTransitionPlan(
            task_id, before, after, None, retry.attempt_id, decision
        )

    def request_reconstruction(self, object_id: ObjectID) -> RecoveryDecision:
        """Validate and commit one reconstruction decision."""

        return self.commit_transition(
            self.validate_request_reconstruction(object_id)
        )

    def _with_lineage(self, decision: RecoveryDecision) -> RecoveryDecision:
        if decision.task_id is None:
            return decision
        lineage = self._lineages.get(decision.task_id)
        if lineage is None:
            return decision
        return RecoveryDecision(
            action=decision.action,
            task_id=decision.task_id,
            attempt_id=decision.attempt_id,
            requested_object_id=decision.requested_object_id,
            output_ids=lineage.output_ids,
            producer_task_spec=lineage.task_spec,
            failure_kind=decision.failure_kind,
            error=decision.error,
            reason=decision.reason,
        )


def _normalize_output_ids(
    task_spec: TaskSpec, output_ids: Iterable[ObjectID] | None
) -> tuple[ObjectID, ...]:
    if output_ids is not None:
        outputs = tuple(output_ids)
    else:
        return_ids = getattr(task_spec, "return_ids", None)
        if callable(return_ids):
            outputs = tuple(return_ids())
        else:
            declared = getattr(task_spec, "output_ids", None)
            if declared is None:
                raise TypeError(
                    "output_ids are required when task_spec has no return_ids()"
                )
            outputs = tuple(declared)
    if len(outputs) != len(set(outputs)):
        raise ValueError("producer output IDs must be unique")
    return outputs


def _next_attempt(current: AttemptID, task_id: TaskID) -> AttemptID:
    next_method = getattr(current, "next", None)
    if not callable(next_method):
        raise TypeError("attempt IDs must provide a next() method")
    candidate = next_method()
    _validate_attempt(candidate, task_id)
    old_number = getattr(current, "attempt_number", None)
    new_number = getattr(candidate, "attempt_number", None)
    if old_number is not None and new_number != old_number + 1:
        raise ValueError("AttemptID.next() must increment attempt_number by one")
    if candidate == current:
        raise ValueError("AttemptID.next() must return a new physical attempt")
    return candidate


def _validate_attempt(attempt_id: AttemptID, task_id: TaskID) -> None:
    _require_hashable(attempt_id, "attempt_id")
    attempt_task_id = _required_attribute(attempt_id, "task_id")
    if attempt_task_id != task_id:
        raise ValueError("attempt_id must belong to task_id")


def _required_attribute(value: object, name: str) -> object:
    try:
        return getattr(value, name)
    except AttributeError:
        raise TypeError(f"{type(value).__name__} must expose {name!r}") from None


def _require_hashable(value: object, label: str) -> None:
    try:
        hash(value)
    except TypeError as exc:
        raise TypeError(f"{label} must be hashable") from exc


def _require_non_negative_int(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")


def _safely_equal(left: object, right: object) -> bool:
    try:
        result = left == right
        return result if isinstance(result, bool) else False
    except Exception:
        return left is right
