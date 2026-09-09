"""Pure bridge from lineage decisions to a CoreWorker requeue plan.

This module performs no RPC, descriptor mutation, or waiter notification. It
advances owner/recovery authority and returns a requeue plan. The composition
layer supplies the actual queue handoff; only that successful handoff can issue
the local admission receipt consumed by an owner-routed reconstruction ACK.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from threading import RLock
from typing import Callable, Optional

from .ids import AttemptID, ObjectID, TaskID
from .ownership import (
    ObjectOwnerTable, ObjectState, TaskOutputAttemptAdvancePlan,
)
from .protocol import (
    InlineArg, RefArg, TaskReferenceHold,
    TaskReferenceHoldKind, TaskSpec,
)
from .recovery import (
    ProducerLineage, RecoveryAction, RecoveryDecision, RecoveryManager,
    RecoveryTransitionPlan, TaskState,
)
from .task_outputs import TaskExecutionKey


class ReconstructionRuntimeError(RuntimeError):
    pass


class ReconstructionDisposition(str, Enum):
    START = "START"
    JOIN = "JOIN"
    FAILED = "FAILED"


class ReconstructionGraphAction(str, Enum):
    """Side-effect-free action assigned to one object in a lineage DAG."""

    READY_SKIP = "READY_SKIP"
    PENDING_JOIN = "PENDING_JOIN"
    LOST_RECONSTRUCT = "LOST_RECONSTRUCT"


@dataclass(frozen=True)
class ReconstructionGraphNode:
    object_id: ObjectID
    task_id: Optional[TaskID]
    action: ReconstructionGraphAction
    task_spec: Optional[TaskSpec]
    # A producer is planned once by TaskID even when the graph names more than
    # one of its return slots.  READY leaves remain singleton audit nodes.
    output_ids: tuple[ObjectID, ...]
    # Only top-level RefArgs are readiness edges in the reconstruction DAG.
    dependency_ids: tuple[ObjectID, ...]
    # Nested ObjectRefs are opaque handles carried by InlineArgs.  They need a
    # fresh execution-lifetime hold, but their producer state never gates this
    # task and is never traversed by the reconstruction DFS.
    nested_local_holds: tuple[ObjectID, ...]
    current_attempt: Optional[AttemptID]
    active_attempt: Optional[AttemptID] = None


@dataclass(frozen=True)
class ReconstructionGraphPlan:
    """Validated DAG with executable nodes in dependency-first order."""

    requested_object_id: ObjectID
    nodes: tuple[ReconstructionGraphNode, ...]
    steps: tuple[ReconstructionGraphNode, ...]

    def node_for(self, object_id: ObjectID) -> ReconstructionGraphNode:
        for node in self.nodes:
            if object_id in node.output_ids:
                return node
        raise KeyError(object_id)


@dataclass(frozen=True)
class ReconstructionPlan:
    """Side effects the Core layer must commit after one START decision."""

    task_id: TaskID
    requested_object_id: ObjectID
    # ``object_id`` remains the slot-zero compatibility identity used by
    # ``_PendingTask``; the request names the one producer output.
    object_id: ObjectID
    output_ids: tuple[ObjectID, ...]
    previous_attempt: AttemptID
    attempt_id: AttemptID
    task_spec: TaskSpec
    clear_descriptor_ids: tuple[ObjectID, ...]
    clear_waiter_ids: tuple[ObjectID, ...]
    dependency_hold: TaskReferenceHold
    protected_dependencies: tuple[ObjectID, ...] = ()
    nested_local_holds: tuple[ObjectID, ...] = ()
    accepted_count_delta: int = 1


_ADMISSION_SEAL = object()


def _admission_identity(disposition, decision) -> tuple:
    return (
        disposition, decision.action, decision.task_id, decision.attempt_id,
        decision.requested_object_id, tuple(decision.output_ids),
    )


@dataclass(frozen=True)
class _CommittedAdmission:
    coordinator: object = field(repr=False)
    identity: tuple
    seal: object = field(repr=False)


@dataclass(frozen=True)
class ReconstructionAdmissionReceipt:
    """Local evidence of one committed attempt accepted by its actual queue.

    Issued only by ReconstructionCoordinator.handoff after queue acceptance.
    It is transient, immutable and never a wire capability or a current-state
    lookup. Completion, collection and later attempts cannot rewrite it.
    """

    owner: object = field(repr=False)
    recovery: object = field(repr=False)
    identity: tuple
    seal: object = field(repr=False)

    def matches(self, outcome, owner, recovery) -> bool:
        return (
            self.seal is _ADMISSION_SEAL
            and self.owner is owner and self.recovery is recovery
            and self.identity == _admission_identity(
                outcome.disposition, outcome.decision
            )
        )

    def __reduce__(self):
        raise TypeError("reconstruction admission receipts are local only")


@dataclass(frozen=True)
class ReconstructionOutcome:
    disposition: ReconstructionDisposition
    decision: RecoveryDecision
    plan: Optional[ReconstructionPlan] = None
    admission: Optional[ReconstructionAdmissionReceipt] = field(
        default=None, compare=False, repr=False
    )
    _commit: Optional[_CommittedAdmission] = field(
        default=None, compare=False, repr=False
    )


@dataclass(frozen=True)
class PreparedReconstruction:
    """Side-effect-free local START transaction.

    External lifetime authorities may perform their own preflight / durable
    saga between :meth:`prepare` and :meth:`commit_prepared`.  The latter
    revalidates the complete local plan under the caller's Core composition
    lock before applying assignment-only owner and recovery commits.
    """

    outcome: ReconstructionOutcome
    recovery_plan: Optional[RecoveryTransitionPlan] = None
    owner_plan: Optional[TaskOutputAttemptAdvancePlan] = None


class ReconstructionCoordinator:
    """Linearize START/JOIN and construct a canonical requeue plan.

    ``_sessions`` answers only whether a logical reconstruction is in flight.
    The current physical attempt has exactly one authority:
    :class:`RecoveryManager`.  Keeping an attempt number in both components
    made a reconstruction SYSTEM retry capable of advancing one marker but not
    the other.
    """

    def __init__(
        self, recovery: RecoveryManager, owner: ObjectOwnerTable
    ) -> None:
        self._recovery = recovery
        self._owner = owner
        self._lock = RLock()
        self._sessions: dict[TaskID, ReconstructionPlan] = {}
        # Only current queued attempts are retained; returned receipts carry
        # their own immutable fact until the request reducer stores its ACK.
        self._handoffs: dict[AttemptID, tuple[ObjectID, ...]] = {}

    def preflight_graph(self, object_id: ObjectID) -> ReconstructionGraphPlan:
        """Validate a local top-level-Ref lineage DAG without mutation.

        DFS derives every edge from ``ProducerLineage.task_spec``.  READY
        dependencies terminate traversal, LOST dependencies recurse, and a
        PENDING object may be joined only when RecoveryManager already records
        an active reconstruction.  No recovery request or owner transition is
        performed here.
        """

        if not isinstance(object_id, ObjectID):
            raise TypeError("object_id must be an ObjectID")
        with self._lock:
            states: dict[ObjectID, int] = {}
            nodes: dict[ObjectID, ReconstructionGraphNode] = {}
            producer_states: dict[TaskID, int] = {}
            producer_nodes: dict[TaskID, ReconstructionGraphNode] = {}
            audit_nodes: list[ReconstructionGraphNode] = []
            postorder: list[ReconstructionGraphNode] = []

            def visit(current: ObjectID) -> ReconstructionGraphNode:
                state = states.get(current, 0)
                if state == 1:
                    raise ReconstructionRuntimeError(
                        "recursive lineage contains a dependency cycle"
                    )
                if state == 2:
                    return nodes[current]
                states[current] = 1

                try:
                    owner = self._owner.snapshot(current)
                except Exception as exc:
                    raise ReconstructionRuntimeError(
                        "recursive lineage owner does not know dependency {}"
                        .format(current)
                    ) from exc
                if owner.collection_pending:
                    raise ReconstructionRuntimeError(
                        "cannot plan reconstruction while collection is pending"
                    )
                if owner.state in (
                    ObjectState.READY_INLINE, ObjectState.READY_STORED
                ):
                    # A ready leaf already satisfies its parent.  It needs no
                    # producer lineage or retry budget (ray.put is the canonical
                    # example), so do not reject it merely for lacking a TaskSpec.
                    ready = ReconstructionGraphNode(
                        object_id=current,
                        task_id=None,
                        action=ReconstructionGraphAction.READY_SKIP,
                        task_spec=None,
                        output_ids=(current,),
                        dependency_ids=(),
                        nested_local_holds=(),
                        current_attempt=owner.current_attempt,
                        active_attempt=None,
                    )
                    nodes[current] = ready
                    audit_nodes.append(ready)
                    states[current] = 2
                    return ready
                recovery = self._recovery.reconstruction_snapshot(current)
                if recovery.is_put:
                    raise ReconstructionRuntimeError(
                        "lost put objects have no reconstructible producer lineage"
                    )
                lineage = recovery.lineage
                if lineage is None:
                    raise ReconstructionRuntimeError(
                        "object has no registered producer lineage"
                    )
                spec, output_ids = self._validate_graph_lineage(
                    current, lineage
                )
                producer_state = producer_states.get(spec.task_id, 0)
                if producer_state == 1:
                    raise ReconstructionRuntimeError(
                        "recursive lineage contains a dependency cycle"
                    )
                if producer_state == 2:
                    existing = producer_nodes[spec.task_id]
                    nodes[current] = existing
                    states[current] = 2
                    return existing
                producer_states[spec.task_id] = 1
                if owner.current_attempt != recovery.current_attempt:
                    raise ReconstructionRuntimeError(
                        "owner and recovery disagree on current attempt"
                    )
                assert isinstance(recovery.current_attempt, AttemptID)
                output_snapshots = self._validate_output_group(
                    spec, output_ids, recovery.current_attempt
                )

                dependencies, nested_local_holds = (
                    self._validate_lineage_inputs(spec)
                )

                output_states = {
                    snapshot.state for snapshot in output_snapshots
                }
                if output_states == {ObjectState.PENDING}:
                    if (
                        recovery.active_recovery is None
                        or recovery.active_recovery != owner.current_attempt
                    ):
                        raise ReconstructionRuntimeError(
                            "pending lineage node has no matching active reconstruction"
                        )
                    action = ReconstructionGraphAction.PENDING_JOIN
                elif output_states == {ObjectState.LOST}:
                    if recovery.task_state is not TaskState.SUCCEEDED:
                        raise ReconstructionRuntimeError(
                            "lost lineage producer was not previously successful"
                        )
                    if recovery.retries_remaining == 0:
                        raise ReconstructionRuntimeError(
                            "lineage reconstruction retry budget is exhausted"
                        )
                    action = ReconstructionGraphAction.LOST_RECONSTRUCT
                else:
                    raise ReconstructionRuntimeError(
                        "reconstruction requires the producer "
                        "output to be LOST (or PENDING in "
                        "the same active reconstruction)"
                    )

                # READY and already-active PENDING producers need no new
                # dependency work from this plan.  A LOST producer is planned
                # only after all of its own dependencies are safe.
                if action is ReconstructionGraphAction.LOST_RECONSTRUCT:
                    for dependency_id in dependencies:
                        visit(dependency_id)

                node = ReconstructionGraphNode(
                    object_id=current,
                    task_id=spec.task_id,
                    action=action,
                    task_spec=spec,
                    output_ids=output_ids,
                    dependency_ids=dependencies,
                    nested_local_holds=nested_local_holds,
                    current_attempt=recovery.current_attempt,
                    active_attempt=recovery.active_recovery,
                )
                producer_nodes[spec.task_id] = node
                producer_states[spec.task_id] = 2
                audit_nodes.append(node)
                for output_id in output_ids:
                    nodes[output_id] = node
                    states[output_id] = 2
                if action is not ReconstructionGraphAction.READY_SKIP:
                    postorder.append(node)
                return node

            visit(object_id)
            return ReconstructionGraphPlan(
                object_id, tuple(audit_nodes), tuple(postorder)
            )

    @staticmethod
    def _validate_graph_lineage(
        object_id: ObjectID, lineage: ProducerLineage
    ) -> tuple[TaskSpec, tuple[ObjectID, ...]]:
        spec = lineage.task_spec
        if not isinstance(spec, TaskSpec):
            raise ReconstructionRuntimeError("producer lineage is not a TaskSpec")
        if spec.task_id != lineage.task_id:
            raise ReconstructionRuntimeError("producer lineage changed TaskID")
        output_ids = tuple(lineage.output_ids)
        if not output_ids or object_id not in output_ids:
            raise ReconstructionRuntimeError(
                "requested object is not in its producer output manifest"
            )
        if tuple(spec.return_ids()) != output_ids:
            raise ReconstructionRuntimeError(
                "producer lineage changed its complete output manifest"
            )
        return spec, output_ids

    def _validate_output_group(
        self,
        spec: TaskSpec,
        output_ids: tuple[ObjectID, ...],
        expected_attempt: AttemptID,
    ) -> tuple[object, ...]:
        """Preflight the canonical output before recovery or owner mutation."""

        snapshots: list[object] = []
        for output_id in output_ids:
            try:
                snapshot = self._owner.snapshot(output_id)
            except Exception as exc:
                raise ReconstructionRuntimeError(
                    "producer output manifest is not fully registered"
                ) from exc
            if snapshot.collection_pending:
                raise ReconstructionRuntimeError(
                    "cannot reconstruct while producer output collection is "
                    "pending"
                )
            if snapshot.producer_task_spec != spec:
                raise ReconstructionRuntimeError(
                    "producer output metadata disagrees on canonical lineage"
                )
            if snapshot.current_attempt != expected_attempt:
                raise ReconstructionRuntimeError(
                    "producer output metadata disagrees on current attempt"
                )
            snapshots.append(snapshot)
        return tuple(snapshots)

    def _validate_lineage_inputs(
        self, spec: TaskSpec
    ) -> tuple[tuple[ObjectID, ...], tuple[ObjectID, ...]]:
        """Split readiness edges from nested-handle lifetime edges.

        A top-level ``RefArg`` participates in the reconstruction DFS.  A
        nested transfer is intentionally opaque to that graph: replaying the
        parent only needs the handle metadata to remain owned locally and out
        of collection.  The nested object's READY/ERROR/LOST state is observed
        later only if user code dereferences the restored handle.
        """

        dependencies: list[ObjectID] = []
        seen_dependencies: set[ObjectID] = set()
        nested_local_holds: list[ObjectID] = []
        seen_nested: set[ObjectID] = set()
        arguments = spec.args + tuple(value for _, value in spec.kwargs)
        for argument in arguments:
            if isinstance(argument, RefArg):
                if argument.owner_worker_id == spec.owner_worker_id:
                    if argument.object_id not in seen_dependencies:
                        seen_dependencies.add(argument.object_id)
                        dependencies.append(argument.object_id)
                else:
                    # Foreign readiness and owner-routed reconstruction are
                    # governed by the TaskID-scoped retained-lineage runtime.
                    # They are not vertices in this local owner's DFS.
                    continue
            if not isinstance(argument, InlineArg):
                continue
            for transfer in argument.nested_refs:
                if transfer.owner_worker_id != spec.owner_worker_id:
                    if transfer.hold.kind is not TaskReferenceHoldKind.RETAINED:
                        raise ReconstructionRuntimeError(
                            "foreign nested ObjectRef lineage requires a "
                            "RETAINED hold"
                        )
                    continue
                if transfer.hold.kind is not TaskReferenceHoldKind.SUBMITTED:
                    raise ReconstructionRuntimeError(
                        "local nested ObjectRef lineage requires a SUBMITTED hold"
                    )
                try:
                    nested_owner = self._owner.snapshot(transfer.object_id)
                except Exception as exc:
                    raise ReconstructionRuntimeError(
                        "nested ObjectRef owner does not know lifetime handle {}"
                        .format(transfer.object_id)
                    ) from exc
                if nested_owner.collection_pending:
                    raise ReconstructionRuntimeError(
                        "cannot reconstruct while a nested ObjectRef is being "
                        "collected"
                    )
                if transfer.object_id not in seen_nested:
                    seen_nested.add(transfer.object_id)
                    nested_local_holds.append(transfer.object_id)
        return tuple(dependencies), tuple(nested_local_holds)

    def _rewrite_nested_holds(
        self, spec: TaskSpec, hold: TaskReferenceHold
    ) -> TaskSpec:
        """Bind every nested transfer to this reconstruction incarnation.

        Canonical lineage retains the hold used by the initial submission.
        That hold has already been terminally released, so replay must not send
        it to a Worker.  Rebuilding the immutable arguments also keeps all
        duplicate occurrences byte-identical for TaskSpec validation and
        Worker-side import de-duplication.
        """

        def rewrite_argument(argument: object) -> object:
            if (
                not isinstance(argument, InlineArg)
                or not argument.nested_refs
            ):
                return argument
            return replace(
                argument,
                nested_refs=tuple(
                    (
                        replace(transfer, hold=hold)
                        if transfer.owner_worker_id == spec.owner_worker_id
                        else transfer
                    )
                    for transfer in argument.nested_refs
                ),
            )

        return replace(
            spec,
            attempt_id=hold.origin_attempt_id,
            args=tuple(rewrite_argument(argument) for argument in spec.args),
            kwargs=tuple(
                (name, rewrite_argument(argument))
                for name, argument in spec.kwargs
            ),
        )



    def prepare(self, object_id: ObjectID) -> PreparedReconstruction:
        """Construct a START/JOIN/failure outcome without local mutation."""
        return self._prepare(object_id, preflight_owner=True)

    def preview(self, object_id: ObjectID) -> ReconstructionOutcome:
        """Preview lineage/budget before retiring a lost publication.

        This returns no executable owner transition. Core may validate the
        whole graph and renew foreign input holds before any cleanup, then
        must call prepare again after exact old-membership retirement.
        """
        return self._prepare(object_id, preflight_owner=False).outcome

    def _prepare(self, object_id: ObjectID, *, preflight_owner: bool) -> PreparedReconstruction:

        if not isinstance(object_id, ObjectID):
            raise TypeError("object_id must be an ObjectID")
        with self._lock:
            before = self._owner.snapshot(object_id)
            if before.collection_pending:
                # The collector has already frozen producer epoch, locations,
                # and lineage into a durable drop obligation.  Reject before
                # RecoveryManager consumes budget or installs an active marker.
                raise ReconstructionRuntimeError(
                    "cannot reconstruct object while collection is pending"
                )
            session = self._sessions.get(object_id.task_id)
            if session is not None:
                if object_id not in session.output_ids:
                    raise ReconstructionRuntimeError(
                        "active reconstruction does not contain requested output"
                    )
                recovery_plan = (
                    self._recovery.validate_request_reconstruction(object_id)
                )
                decision = recovery_plan.decision
                if (
                    decision.action is not RecoveryAction.JOIN_RECONSTRUCTION
                    or decision.task_id != session.task_id
                    or decision.attempt_id != before.current_attempt
                    or tuple(decision.output_ids) != session.output_ids
                ):
                    raise ReconstructionRuntimeError(
                        "active reconstruction disagrees with RecoveryManager"
                    )
                # JOIN installs no new state; committing the validated identity
                # is unnecessary because its before/after snapshots are equal.
                return PreparedReconstruction(
                    ReconstructionOutcome(
                        ReconstructionDisposition.JOIN, decision
                    ),
                    recovery_plan=recovery_plan,
                )
            if before.state is not ObjectState.LOST:
                raise ReconstructionRuntimeError(
                    "reconstruction requires a LOST owner object"
                )
            # Validate the local-owner whole-manifest teaching slice before
            # RecoveryManager consumes a retry slot.  Recursive orchestration is
            # performed by ``preflight_graph`` plus the Core composition layer;
            # this method remains the sole START/JOIN mutation for one producer.
            spec = before.producer_task_spec
            if spec is None:
                recovery_plan = (
                    self._recovery.validate_request_reconstruction(object_id)
                )
                return PreparedReconstruction(
                    ReconstructionOutcome(
                        ReconstructionDisposition.FAILED,
                        recovery_plan.decision,
                    ),
                    recovery_plan=recovery_plan,
                )
            if not isinstance(spec, TaskSpec):
                raise ReconstructionRuntimeError("producer lineage is not a TaskSpec")
            recovery_before = self._recovery.reconstruction_snapshot(object_id)
            if recovery_before.lineage is None:
                raise ReconstructionRuntimeError(
                    "object has no registered producer lineage"
                )
            canonical_spec, output_ids = self._validate_graph_lineage(
                object_id, recovery_before.lineage
            )
            if canonical_spec != spec:
                raise ReconstructionRuntimeError(
                    "owner and recovery disagree on canonical producer lineage"
                )
            if recovery_before.current_attempt != before.current_attempt:
                raise ReconstructionRuntimeError(
                    "owner and recovery disagree on current attempt"
                )
            assert isinstance(before.current_attempt, AttemptID)
            output_snapshots = self._validate_output_group(
                spec, output_ids, before.current_attempt
            )
            if {snapshot.state for snapshot in output_snapshots} != {
                ObjectState.LOST
            }:
                raise ReconstructionRuntimeError(
                    "reconstruction requires the producer "
                    "output to be LOST"
                )
            dependencies, nested_local_holds = (
                self._validate_lineage_inputs(spec)
            )

            recovery_plan = (
                self._recovery.validate_request_reconstruction(object_id)
            )
            decision = recovery_plan.decision
            if decision.action is RecoveryAction.JOIN_RECONSTRUCTION:
                return PreparedReconstruction(
                    ReconstructionOutcome(
                        ReconstructionDisposition.JOIN, decision
                    ),
                    recovery_plan=recovery_plan,
                )
            if decision.action is not RecoveryAction.START_RECONSTRUCTION:
                return PreparedReconstruction(
                    ReconstructionOutcome(
                        ReconstructionDisposition.FAILED, decision
                    ),
                    recovery_plan=recovery_plan,
                )
            if decision.producer_task_spec != spec:
                raise ReconstructionRuntimeError("recovery lineage changed during request")
            if tuple(decision.output_ids) != output_ids:
                raise ReconstructionRuntimeError(
                    "recovery changed the producer output manifest"
                )
            if not isinstance(decision.attempt_id, AttemptID):
                raise ReconstructionRuntimeError("START decision has no next attempt")
            if decision.task_id != spec.task_id or decision.attempt_id.task_id != spec.task_id:
                raise ReconstructionRuntimeError("reconstruction changed logical TaskID")
            if decision.attempt_id.attempt_number <= before.current_attempt.attempt_number:
                raise ReconstructionRuntimeError("reconstruction attempt did not advance")
            # Canonical lineage keeps the initial immutable TaskSpec.  The
            # producer may already have consumed SYSTEM retries before its
            # output was lost, so the output CAS must use RecoveryManager's
            # current physical attempt rather than spec.attempt_id.
            execution = TaskExecutionKey.from_task_spec(spec).for_attempt(
                before.current_attempt
            )
            owner_plan = (self._owner.validate_advance_task_outputs(
                execution, decision.attempt_id
            ) if preflight_owner else None)

            dependency_hold = TaskReferenceHold(
                TaskReferenceHoldKind.SUBMITTED,
                spec.owner_worker_id,
                spec.task_id,
                decision.attempt_id,
            )
            retried = self._rewrite_nested_holds(spec, dependency_hold)
            plan = ReconstructionPlan(
                task_id=spec.task_id,
                requested_object_id=object_id,
                object_id=output_ids[0],
                output_ids=output_ids,
                previous_attempt=before.current_attempt,
                attempt_id=decision.attempt_id,
                task_spec=retried,
                clear_descriptor_ids=output_ids,
                clear_waiter_ids=output_ids,
                protected_dependencies=dependencies,
                nested_local_holds=nested_local_holds,
                # A reconstruction is a new logical hold incarnation.  Every
                # SYSTEM retry derived from this pending record preserves this
                # complete credential instead of projecting it to an attempt.
                dependency_hold=dependency_hold,
                accepted_count_delta=1,
            )

            return PreparedReconstruction(
                ReconstructionOutcome(
                    ReconstructionDisposition.START, decision, plan
                ),
                recovery_plan=recovery_plan, owner_plan=owner_plan,
            )

    def commit_prepared(
        self, prepared: PreparedReconstruction
    ) -> ReconstructionOutcome:
        """Revalidate and atomically expose one prepared local transition."""

        if not isinstance(prepared, PreparedReconstruction):
            raise TypeError("prepared must be a PreparedReconstruction")
        with self._lock:
            outcome = prepared.outcome
            decision = outcome.decision
            if outcome.disposition is ReconstructionDisposition.JOIN:
                active = self._sessions.get(decision.task_id)
                if (
                    active is None
                    or tuple(decision.output_ids) != active.output_ids
                    or self._recovery.active_recovery(decision.task_id)
                    != decision.attempt_id
                ):
                    raise ReconstructionRuntimeError(
                        "prepared reconstruction JOIN is no longer active"
                    )
                return self._committed_outcome(outcome)
            if outcome.disposition is ReconstructionDisposition.FAILED:
                recovery_plan = prepared.recovery_plan
                if recovery_plan is not None:
                    committed = self._recovery.commit_transition(recovery_plan)
                    if committed != decision:
                        raise AssertionError(
                            "RecoveryManager committed a different failure"
                        )
                return outcome
            plan = outcome.plan
            recovery_plan = prepared.recovery_plan
            owner_plan = prepared.owner_plan
            if plan is None or recovery_plan is None or owner_plan is None:
                raise ReconstructionRuntimeError(
                    "prepared START lacks a complete local transaction"
                )
            existing = self._sessions.get(plan.task_id)
            if existing is not None:
                if existing.attempt_id == plan.attempt_id:
                    return self._committed_outcome(ReconstructionOutcome(
                        ReconstructionDisposition.JOIN,
                        replace(decision, action=RecoveryAction.JOIN_RECONSTRUCTION),
                    ))
                raise ReconstructionRuntimeError(
                    "another reconstruction committed after prepare"
                )
            # Fresh preflights prove that no owner/recovery state changed while
            # a foreign lifetime saga ran without the Core lock.  Compare the
            # complete immutable plans before invoking assignment-only commits.
            refreshed_recovery = (
                self._recovery.validate_request_reconstruction(
                    plan.requested_object_id
                )
            )
            if not self._same_recovery_preflight(
                refreshed_recovery, recovery_plan
            ):
                raise ReconstructionRuntimeError(
                    "recovery authority changed after reconstruction prepare"
                )
            refreshed_owner = self._owner.validate_advance_task_outputs(
                owner_plan.expected, owner_plan.next_execution.attempt_id
            )
            if refreshed_owner != owner_plan:
                raise ReconstructionRuntimeError(
                    "owner authority changed after reconstruction prepare"
                )
            if not self._owner.commit_advance_task_outputs(owner_plan):
                raise ReconstructionRuntimeError(
                    "owner rejected reconstruction output CAS"
                )
            committed = self._recovery.commit_transition(recovery_plan)
            if committed != decision:
                raise AssertionError(
                    "RecoveryManager committed a different reconstruction "
                    "decision"
                )
            self._sessions[plan.task_id] = plan
            return self._committed_outcome(outcome)

    def _committed_outcome(self, outcome: ReconstructionOutcome) -> ReconstructionOutcome:
        return replace(outcome, _commit=_CommittedAdmission(
            self, _admission_identity(outcome.disposition, outcome.decision),
            _ADMISSION_SEAL,
        ))

    def handoff(
        self, outcome: ReconstructionOutcome,
        enqueue: Callable[[ReconstructionPlan], None],
    ) -> ReconstructionOutcome:
        """Accept a committed START at its actual queue, or prove a JOIN.

        Call beneath the Core composition lock. The callback must do only the
        queue acceptance, with no fallible observation after it. A callback
        failure is not evidence of acceptance. Preview/prepare values cannot
        enter here, and committed-but-unqueued state cannot authorize JOIN.
        """
        if not isinstance(outcome, ReconstructionOutcome) or not callable(enqueue):
            raise TypeError("handoff requires an outcome and a queue callback")
        with self._lock:
            proof = outcome._commit
            identity = _admission_identity(outcome.disposition, outcome.decision)
            if (not isinstance(proof, _CommittedAdmission)
                    or proof.coordinator is not self or proof.seal is not _ADMISSION_SEAL
                    or proof.identity != identity):
                raise ReconstructionRuntimeError("handoff requires an actual committed admission")
            decision = outcome.decision
            attempt = decision.attempt_id
            session = self._require_current_handoff_attempt(decision.task_id, attempt)
            if tuple(decision.output_ids) != session.output_ids:
                raise ReconstructionRuntimeError("handoff changed the committed output identity")
            if outcome.disposition is ReconstructionDisposition.START:
                if decision.action is not RecoveryAction.START_RECONSTRUCTION or outcome.plan != session:
                    raise ReconstructionRuntimeError("START handoff changed its committed plan")
                if attempt not in self._handoffs:
                    enqueue(session)
                    self._handoffs[attempt] = session.output_ids
            elif (outcome.disposition is not ReconstructionDisposition.JOIN
                    or decision.action is not RecoveryAction.JOIN_RECONSTRUCTION
                    or self._handoffs.get(attempt) != session.output_ids):
                raise ReconstructionRuntimeError("JOIN requires an already queued attempt")
            return replace(outcome, admission=ReconstructionAdmissionReceipt(
                self._owner, self._recovery, identity, _ADMISSION_SEAL,
            ))

    def _require_current_handoff_attempt(self, task_id, attempt):
        session = self._sessions.get(task_id)
        if (session is None or not isinstance(attempt, AttemptID)
                or attempt.task_id != task_id
                or self._recovery.active_recovery(task_id) != attempt):
            raise ReconstructionRuntimeError("handoff is not the current committed reconstruction")
        recovery = self._recovery.reconstruction_snapshot(session.object_id)
        owners = tuple(self._owner.snapshot(output) for output in session.output_ids)
        if (recovery.current_attempt != attempt
                or recovery.task_state not in (TaskState.RETRY_PENDING, TaskState.RUNNING)
                or any(owner.current_attempt != attempt
                       or owner.state is not ObjectState.PENDING for owner in owners)):
            raise ReconstructionRuntimeError("handoff owner/recovery transition is not committed")
        return session

    def handoff_retry(
        self, task_id: TaskID, previous_attempt: AttemptID, attempt: AttemptID,
        enqueue: Callable[[], None],
    ) -> bool:
        """Record the real queue edge of an already committed SYSTEM retry.

        This neither advances epochs nor consumes budget. Its predecessor must
        have actually entered the queue; an exact repeat never queues twice.
        """
        if not callable(enqueue):
            raise TypeError("retry handoff requires a queue callback")
        with self._lock:
            session = self._require_current_handoff_attempt(task_id, attempt)
            if (not isinstance(previous_attempt, AttemptID)
                    or previous_attempt.task_id != task_id
                    or attempt.attempt_number != previous_attempt.attempt_number + 1):
                raise ReconstructionRuntimeError("retry handoff changed predecessor identity")
            if self._handoffs.get(attempt) == session.output_ids:
                return False
            if self._handoffs.get(previous_attempt) != session.output_ids:
                raise ReconstructionRuntimeError("retry predecessor was never queued")
            enqueue()
            self._handoffs[attempt] = session.output_ids
            self._handoffs.pop(previous_attempt, None)
            return True

    @staticmethod
    def _same_recovery_preflight(
        left: RecoveryTransitionPlan, right: RecoveryTransitionPlan
    ) -> bool:
        """Compare plans while treating freshly-created error values as data.

        ``validate_request_reconstruction`` intentionally constructs a new
        ``UnreconstructableObjectError`` as the prospective ``last_error``.
        Exception identity therefore cannot be used as the CAS token.  Every
        authoritative field, decision identity and the error type/text remain
        compared explicitly.
        """

        def records_equal(left_record: object, right_record: object) -> bool:
            if left_record is None or right_record is None:
                return left_record is right_record
            fields = (
                "task_id", "current_attempt", "max_retries",
                "retries_started", "state",
            )
            return all(
                getattr(left_record, name) == getattr(right_record, name)
                for name in fields
            ) and (
                type(getattr(left_record, "last_error"))
                is type(getattr(right_record, "last_error"))
            ) and str(getattr(left_record, "last_error")) == str(
                getattr(right_record, "last_error")
            )

        left_decision = left.decision
        right_decision = right.decision
        decision_fields = (
            "action", "task_id", "attempt_id",
            "requested_object_id", "output_ids",
            "producer_task_spec", "failure_kind", "reason",
        )
        return (
            left.task_id == right.task_id
            and records_equal(left.before, right.before)
            and records_equal(left.after, right.after)
            and left.active_before == right.active_before
            and left.active_after == right.active_after
            and all(
                getattr(left_decision, name)
                == getattr(right_decision, name)
                for name in decision_fields
            )
            and type(left_decision.error) is type(right_decision.error)
            and str(left_decision.error) == str(right_decision.error)
        )

    def request(self, object_id: ObjectID) -> ReconstructionOutcome:
        """Compatibility wrapper preserving the original eager commit."""

        return self.commit_prepared(self.prepare(object_id))

    def preflight_retry(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> bool:
        """Validate whether a SYSTEM retry belongs to reconstruction.

        The Core calls this while holding its composition lock and before the
        :class:`RecoveryManager` consumes retry budget.  ``False`` denotes an
        ordinary task retry.  An active-but-different identity is an invariant
        violation, not another ordinary task: silently accepting it would let
        one physical attempt advance only one of the two merge markers.
        """

        if not isinstance(task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(attempt_id, AttemptID):
            raise TypeError("attempt_id must be an AttemptID")
        if attempt_id.task_id != task_id:
            raise ValueError("attempt_id belongs to another TaskID")
        with self._lock:
            session = self._sessions.get(task_id)
            recovery_attempt = self._recovery.active_recovery(task_id)
            if session is None:
                if recovery_attempt is not None:
                    raise ReconstructionRuntimeError(
                        "RecoveryManager has an active reconstruction without "
                        "a coordinator plan"
                    )
                return False
            if recovery_attempt != attempt_id:
                raise ReconstructionRuntimeError(
                    "reconstruction retry source disagrees with active identity"
                )
            return True

    def complete(self, task_id: TaskID, attempt_id: AttemptID) -> bool:
        """Forget the merge marker after the planned attempt is terminal."""

        with self._lock:
            if task_id not in self._sessions:
                return False
            # RecoveryManager fences stale attempt completion and clears its
            # active identity on every accepted terminal result.  Consult that
            # single authority rather than a copied attempt in the START plan.
            record = self._recovery.task_record(task_id)
            if (
                record.current_attempt != attempt_id
                or self._recovery.active_recovery(task_id) is not None
            ):
                return False
            del self._sessions[task_id]
            self._handoffs.pop(attempt_id, None)
            return True


__all__ = [
    "ReconstructionCoordinator",
    "ReconstructionDisposition",
    "ReconstructionGraphAction",
    "ReconstructionGraphNode",
    "ReconstructionGraphPlan",
    "ReconstructionOutcome",
    "ReconstructionAdmissionReceipt",
    "PreparedReconstruction",
    "ReconstructionPlan",
    "ReconstructionRuntimeError",
]
