"""Durable borrower-side saga for foreign producer-lineage inputs.

The registry in :mod:`miniray.foreign_lineage` owns the stable, TaskID-scoped
graph.  This module owns the fallible work around that graph:

* atomically replace each retained owner credential from the old logical
  execution incarnation to the proposed reconstruction incarnation;
* preserve an acknowledgement bitmap and exact requests across ambiguous RPCs;
* reconstruct lost top-level dependencies at their real owners before the
  caller may commit its own producer reconstruction; and
* release the current (possibly mixed-generation) credentials only after the
  final output sibling is collected.

No local owner/recovery state is mutated here.  The caller obtains a proposed
``AttemptID`` from a side-effect-free reconstruction preflight, drives this
saga to ``READY``, and only then commits that attempt.  Consequently a partial
multi-owner exchange can never consume local retry budget or advance local
object epochs.  Transport failure is retained as ambiguity; it is never
interpreted as owner death.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from threading import RLock
from typing import Callable, Optional, Protocol

from .foreign_lineage import (
    ForeignLineageCollectionPlan,
    ForeignLineageCollectionReceipt,
    ForeignLineageEdge,
    ForeignLineageRegistry,
    ForeignLineageRole,
)
from .ids import AttemptID, ObjectID, TaskID, WorkerID
from .protocol import (
    GetRetainedOwnedObject,
    GetRetainedOwnedObjectReply,
    OwnedObjectReconstructionDisposition,
    OwnedObjectReconstructionFailure,
    OwnedObjectState,
    ReleaseOwnedObjectForTask,
    ReleaseOwnedObjectForTaskReply,
    ReplaceRetainedObjectDisposition,
    ReplaceRetainedObjectFailure,
    ReplaceRetainedObjectForTask,
    ReplaceRetainedObjectForTaskReply,
    RequestOwnedObjectReconstruction,
    RequestOwnedObjectReconstructionReply,
    RetainedCredential,
    TaskReferenceHold,
    TaskReferenceHoldKind,
)
from .ownership import DeadWorkerReferenceRecord


OwnerAddress = tuple[str, int]
ForeignEdgeKey = tuple[ObjectID, WorkerID]


class ForeignLineageRuntimeError(RuntimeError):
    """A local composition invariant made the saga unsafe to continue."""


class ForeignLineageRenewalDisposition(str, Enum):
    WAITING = "WAITING"
    READY = "READY"
    FAILED = "FAILED"


class ForeignLineageCollectionDisposition(str, Enum):
    NOT_FINAL = "NOT_FINAL"
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"


class ReplaceRetainedRpc(Protocol):
    def __call__(
        self, address: OwnerAddress, request: ReplaceRetainedObjectForTask
    ) -> ReplaceRetainedObjectForTaskReply: ...


class GetRetainedRpc(Protocol):
    def __call__(
        self, address: OwnerAddress, request: GetRetainedOwnedObject
    ) -> GetRetainedOwnedObjectReply: ...


class RequestReconstructionRpc(Protocol):
    def __call__(
        self, address: OwnerAddress,
        request: RequestOwnedObjectReconstruction,
    ) -> RequestOwnedObjectReconstructionReply: ...


class ReleaseRetainedRpc(Protocol):
    def __call__(
        self, address: OwnerAddress, request: ReleaseOwnedObjectForTask
    ) -> ReleaseOwnedObjectForTaskReply: ...


OwnerDeathLookup = Callable[[WorkerID], object | None]


@dataclass(frozen=True)
class ForeignLineageRenewalResult:
    task_id: TaskID
    attempt_id: AttemptID
    disposition: ForeignLineageRenewalDisposition
    acknowledged: tuple[ForeignEdgeKey, ...]
    total_edges: int
    waiting_on: tuple[ForeignEdgeKey, ...] = ()
    uncertain: tuple[ForeignEdgeKey, ...] = ()
    failure: Optional[str] = None

    @property
    def obligations_pending(self) -> bool:
        return bool(self.uncertain)


@dataclass(frozen=True)
class ForeignLineageCollectionResult:
    task_id: TaskID
    disposition: ForeignLineageCollectionDisposition
    acknowledged: tuple[ForeignEdgeKey, ...]
    total_edges: int
    pending: tuple[ForeignEdgeKey, ...] = ()


@dataclass(frozen=True)
class ForeignLineageShutdownResult:
    complete: bool
    pending_task_ids: tuple[TaskID, ...]
    collections: tuple[ForeignLineageCollectionResult, ...]


@dataclass(frozen=True)
class _Replacement:
    key: ForeignEdgeKey
    expected: ForeignLineageEdge
    replacement: ForeignLineageEdge
    request: Optional[ReplaceRetainedObjectForTask]


@dataclass
class _RenewalSession:
    task_id: TaskID
    attempt_id: AttemptID
    replacements: dict[ForeignEdgeKey, _Replacement]
    acknowledged: set[ForeignEdgeKey] = field(default_factory=set)
    uncertain: set[ForeignEdgeKey] = field(default_factory=set)
    terminal_failures: dict[ForeignEdgeKey, str] = field(default_factory=dict)
    ready: bool = False
    driving: bool = False
    mutation_epoch: int = 0


@dataclass
class _CollectionSession:
    plan: ForeignLineageCollectionPlan
    acknowledged: set[ForeignEdgeKey] = field(default_factory=set)
    uncertain: set[ForeignEdgeKey] = field(default_factory=set)
    driving: bool = False


_RETRYABLE_REPLACEMENT_FAILURES = frozenset({
    ReplaceRetainedObjectFailure.OLD_HOLD_BUSY,
    ReplaceRetainedObjectFailure.COLLECTION_IN_PROGRESS,
})

_RETRYABLE_RECONSTRUCTION_FAILURES = frozenset({
    OwnedObjectReconstructionFailure.NOT_LOST,
    OwnedObjectReconstructionFailure.EXPECTED_ATTEMPT_MISMATCH,
    OwnedObjectReconstructionFailure.COLLECTION_IN_PROGRESS,
})


class ForeignLineageRuntime:
    """Compose owner RPCs without becoming another lineage authority.

    Exact RPC requests are immutable and every owner operation is idempotent.
    Calls happen without ``_lock`` held.  ``driving`` is a per-Task merge bit:
    a concurrent caller observes ``WAITING`` instead of launching a second
    traversal, while later calls replay only unresolved identities.
    """

    def __init__(
        self,
        registry: ForeignLineageRegistry,
        *,
        replace_retained: ReplaceRetainedRpc,
        get_retained: GetRetainedRpc,
        request_reconstruction: RequestReconstructionRpc,
        release_retained: ReleaseRetainedRpc,
        owner_death_lookup: Optional[OwnerDeathLookup] = None,
    ) -> None:
        if not isinstance(registry, ForeignLineageRegistry):
            raise TypeError("registry must be a ForeignLineageRegistry")
        for callback, name in (
            (replace_retained, "replace_retained"),
            (get_retained, "get_retained"),
            (request_reconstruction, "request_reconstruction"),
            (release_retained, "release_retained"),
        ):
            if not callable(callback):
                raise TypeError("{} must be callable".format(name))
        if owner_death_lookup is not None and not callable(owner_death_lookup):
            raise TypeError("owner_death_lookup must be callable")
        self._registry = registry
        self._replace = replace_retained
        self._get = get_retained
        self._reconstruct = request_reconstruction
        self._release = release_retained
        self._owner_death_lookup = owner_death_lookup or (lambda _worker: None)
        self._renewals: dict[TaskID, _RenewalSession] = {}
        self._collections: dict[TaskID, _CollectionSession] = {}
        self._completed_collections: dict[
            TaskID, ForeignLineageCollectionResult
        ] = {}
        self._collection_receipts: dict[
            TaskID, dict[ObjectID, ForeignLineageCollectionReceipt]
        ] = {}
        self._closed = False
        self._lock = RLock()

    @staticmethod
    def _edge_key(edge: ForeignLineageEdge) -> ForeignEdgeKey:
        return edge.dependency_object_id, edge.owner_worker_id

    def _owner_is_dead(self, owner_worker_id: WorkerID) -> bool:
        if self._registry.owner_is_dead(owner_worker_id):
            return True
        proof = self._owner_death_lookup(owner_worker_id)
        if proof is None:
            return False
        if not isinstance(proof, DeadWorkerReferenceRecord):
            raise ForeignLineageRuntimeError(
                "owner death lookup returned an unvalidated proof"
            )
        self.mark_owner_dead(proof)
        return True

    def _new_session(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> _RenewalSession:
        record = self._registry.snapshot(task_id)
        if record is None:
            raise ForeignLineageRuntimeError(
                "task has no registered foreign lineage"
            )
        replacements: dict[ForeignEdgeKey, _Replacement] = {}
        acknowledged: set[ForeignEdgeKey] = set()
        for edge in record.edges:
            key = self._edge_key(edge)
            current_origin = edge.hold.origin_attempt_id
            if current_origin == attempt_id:
                acknowledged.add(key)
                replacement = edge
                request = None
            elif current_origin.attempt_number < attempt_id.attempt_number:
                hold = TaskReferenceHold(
                    TaskReferenceHoldKind.RETAINED,
                    edge.borrower_worker_id, task_id, attempt_id,
                )
                replacement = replace(edge, hold=hold)
                request = ReplaceRetainedObjectForTask(
                    edge.dependency_object_id, edge.owner_worker_id,
                    edge.borrower_worker_id, edge.hold, replacement.hold,
                )
            else:
                raise ForeignLineageRuntimeError(
                    "foreign lineage is newer than proposed reconstruction"
                )
            replacements[key] = _Replacement(
                key, edge, replacement, request
            )
        return _RenewalSession(
            task_id, attempt_id, replacements, acknowledged=acknowledged
        )

    def _session(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> _RenewalSession:
        if not isinstance(task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(attempt_id, AttemptID):
            raise TypeError("attempt_id must be an AttemptID")
        if attempt_id.task_id != task_id:
            raise ValueError("attempt_id belongs to another TaskID")
        session = self._renewals.get(task_id)
        if session is None:
            if task_id in self._collections:
                raise ForeignLineageRuntimeError(
                    "foreign lineage is already claimed for collection"
                )
            session = self._new_session(task_id, attempt_id)
            self._renewals[task_id] = session
        elif session.attempt_id != attempt_id:
            raise ForeignLineageRuntimeError(
                "another foreign-lineage renewal is active for this TaskID"
            )
        return session

    def drive_renewal(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> ForeignLineageRenewalResult:
        """Make one bounded pass toward a dependency-safe reconstruction.

        ``READY`` proves every owner ACKed the hold transition and every
        top-level foreign input was owner-observed ready in this pass.  It does
        not itself authorize or commit the local reconstruction attempt.
        """

        with self._lock:
            if self._closed:
                raise ForeignLineageRuntimeError(
                    "foreign lineage runtime admission is closed"
                )
            session = self._session(task_id, attempt_id)
            if session.driving:
                return self._renewal_result(
                    session, ForeignLineageRenewalDisposition.WAITING,
                    waiting_on=tuple(sorted(
                        set(session.replacements) - session.acknowledged
                    )),
                )
            session.driving = True
            start_epoch = session.mutation_epoch
        try:
            self._drive_replacements(session)
            with self._lock:
                unresolved = tuple(sorted(session.uncertain))
                failures = dict(session.terminal_failures)
                missing = tuple(sorted(
                    set(session.replacements) - session.acknowledged
                ))
            if failures:
                return self._renewal_result(
                    session, ForeignLineageRenewalDisposition.FAILED,
                    waiting_on=missing, uncertain=unresolved,
                    failure=self._format_failures(failures),
                )
            if missing:
                return self._renewal_result(
                    session, ForeignLineageRenewalDisposition.WAITING,
                    waiting_on=missing, uncertain=unresolved,
                )

            waiting, dependency_failures = self._drive_dependencies(session)
            if dependency_failures:
                with self._lock:
                    session.terminal_failures.update(dependency_failures)
                    session.ready = False
                return self._renewal_result(
                    session, ForeignLineageRenewalDisposition.FAILED,
                    waiting_on=tuple(sorted(waiting)),
                    failure=self._format_failures(dependency_failures),
                )
            if waiting:
                with self._lock:
                    session.ready = False
                return self._renewal_result(
                    session, ForeignLineageRenewalDisposition.WAITING,
                    waiting_on=tuple(sorted(waiting)),
                )
            with self._lock:
                if (
                    session.mutation_epoch != start_epoch
                    or any(
                        self._registry.owner_is_dead(
                            replacement.expected.owner_worker_id
                        )
                        for replacement in session.replacements.values()
                    )
                ):
                    session.ready = False
                    return self._renewal_result(
                        session, ForeignLineageRenewalDisposition.FAILED,
                        failure="owner death raced dependency readiness",
                    )
                session.ready = True
            return self._renewal_result(
                session, ForeignLineageRenewalDisposition.READY
            )
        finally:
            with self._lock:
                session.driving = False

    def _drive_replacements(self, session: _RenewalSession) -> None:
        for key in sorted(session.replacements):
            with self._lock:
                if key in session.acknowledged:
                    continue
                replacement = session.replacements[key]
                # After a definitive failure, launch no new irreversible owner
                # transitions.  Exact replays of previously ambiguous sends
                # still run so the acknowledgement bitmap can converge.
                if session.terminal_failures and key not in session.uncertain:
                    continue
                ambiguous_before = key in session.uncertain
                session.uncertain.add(key)
            request = replacement.request
            if request is None:
                raise ForeignLineageRuntimeError(
                    "acknowledged replacement unexpectedly needs an RPC"
                )
            if self._owner_is_dead(replacement.expected.owner_worker_id):
                with self._lock:
                    session.uncertain.discard(key)
                    session.terminal_failures[key] = (
                        "foreign dependency owner has a committed death proof"
                    )
                continue
            try:
                reply = self._replace(
                    replacement.expected.owner_address, request
                )
            except Exception:
                # The call may have committed.  Preserve the exact request and
                # retry it; reachability says nothing about owner liveness.
                if self._owner_is_dead(replacement.expected.owner_worker_id):
                    with self._lock:
                        session.uncertain.discard(key)
                        session.terminal_failures[key] = (
                            "foreign dependency owner has a committed death proof"
                        )
                continue
            if not self._valid_replacement_reply(request, reply):
                # A malformed response is as ambiguous as a lost response.
                continue
            if not isinstance(reply, ReplaceRetainedObjectForTaskReply):
                continue
            if reply.disposition in (
                ReplaceRetainedObjectDisposition.REPLACED,
                ReplaceRetainedObjectDisposition.ALREADY_REPLACED,
            ):
                try:
                    self._registry.commit_edge_replacement(
                        replacement.expected, replacement.replacement
                    )
                except Exception as exc:
                    raise ForeignLineageRuntimeError(
                        "owner ACK could not be committed to foreign lineage"
                    ) from exc
                with self._lock:
                    session.uncertain.discard(key)
                    session.acknowledged.add(key)
                    session.terminal_failures.pop(key, None)
                continue

            failure = reply.failure
            if not isinstance(failure, ReplaceRetainedObjectFailure):
                # A reconstructed dataclass validator should make this
                # impossible; preserve ambiguity if a hostile pickle bypassed
                # construction anyway.
                continue
            # OWNER_STOPPED is authoritative only about endpoint admission.
            # If an earlier reply was lost, it cannot prove whether that older
            # call installed the successor, so retain the ambiguity.
            with self._lock:
                if not (
                    failure is ReplaceRetainedObjectFailure.OWNER_STOPPED
                    and ambiguous_before
                ):
                    session.uncertain.discard(key)
                if failure not in _RETRYABLE_REPLACEMENT_FAILURES:
                    session.terminal_failures[key] = (
                        "{}: {}".format(failure.value, reply.detail)
                    )

    @staticmethod
    def _valid_replacement_reply(
        request: ReplaceRetainedObjectForTask, reply: object
    ) -> bool:
        if not isinstance(reply, ReplaceRetainedObjectForTaskReply):
            return False
        try:
            validated = replace(reply)
        except Exception:
            return False
        return (
            validated.object_id == request.object_id
            and validated.owner_worker_id == request.owner_worker_id
            and validated.borrower_worker_id == request.borrower_worker_id
            and validated.expected_hold == request.expected_hold
            and validated.replacement_hold == request.replacement_hold
        )

    def _drive_dependencies(
        self, session: _RenewalSession
    ) -> tuple[set[ForeignEdgeKey], dict[ForeignEdgeKey, str]]:
        waiting: set[ForeignEdgeKey] = set()
        failures: dict[ForeignEdgeKey, str] = {}
        record = self._registry.snapshot(session.task_id)
        if record is None:
            raise ForeignLineageRuntimeError(
                "foreign lineage disappeared during renewal"
            )
        for edge in record.edges:
            if not edge.roles & ForeignLineageRole.TOP_LEVEL:
                continue
            key = self._edge_key(edge)
            if edge.hold.origin_attempt_id != session.attempt_id:
                raise ForeignLineageRuntimeError(
                    "dependency query would use an unacknowledged hold"
                )
            if self._owner_is_dead(edge.owner_worker_id):
                failures[key] = (
                    "foreign dependency owner has a committed death proof"
                )
                continue
            request = GetRetainedOwnedObject(
                edge.dependency_object_id, edge.owner_worker_id,
                edge.borrower_worker_id, edge.hold,
            )
            try:
                reply = self._get(edge.owner_address, request)
            except Exception:
                if self._owner_is_dead(edge.owner_worker_id):
                    failures[key] = (
                        "foreign dependency owner has a committed death proof"
                    )
                else:
                    waiting.add(key)
                continue
            if not self._valid_get_reply(request, reply):
                waiting.add(key)
                continue
            if not isinstance(reply, GetRetainedOwnedObjectReply):
                waiting.add(key)
                continue
            if not reply.accepted:
                failures[key] = reply.detail or (
                    "foreign owner rejected the retained lineage credential"
                )
                continue
            if reply.state in (
                OwnedObjectState.READY_INLINE, OwnedObjectState.READY_STORED
            ):
                continue
            if reply.state is OwnedObjectState.PENDING:
                # The owner has already admitted the dependency producer.  The
                # parent may commit its own PENDING attempt after this proof;
                # Core's ordinary dependency gate still forbids leasing it
                # until a later retained query observes READY.
                continue
            if reply.state is OwnedObjectState.ERROR:
                if reply.error is None:
                    waiting.add(key)
                    continue
                failures[key] = "{}: {}".format(
                    reply.error.type_name, reply.error.message
                )
                continue
            if reply.state is not OwnedObjectState.LOST:
                failures[key] = "foreign owner returned an unknown object state"
                continue
            if not isinstance(reply.current_attempt, AttemptID):
                failures[key] = (
                    "lost foreign dependency has no producer attempt identity"
                )
                continue
            reconstruction = RequestOwnedObjectReconstruction(
                edge.dependency_object_id, edge.owner_worker_id,
                edge.borrower_worker_id, RetainedCredential(edge.hold), None,
                reply.current_attempt,
            )
            try:
                outcome = self._reconstruct(
                    edge.owner_address, reconstruction
                )
            except Exception:
                if self._owner_is_dead(edge.owner_worker_id):
                    failures[key] = (
                        "foreign dependency owner has a committed death proof"
                    )
                else:
                    waiting.add(key)
                continue
            if not self._valid_reconstruction_reply(
                reconstruction, outcome
            ):
                waiting.add(key)
                continue
            if not isinstance(outcome, RequestOwnedObjectReconstructionReply):
                waiting.add(key)
                continue
            if outcome.disposition in (
                OwnedObjectReconstructionDisposition.STARTED,
                OwnedObjectReconstructionDisposition.JOINED,
            ):
                # Dependency-first means its real owner has durably STARTed or
                # JOINed before the parent owner/recovery CAS.  It need not
                # finish before the parent becomes locally PENDING because the
                # normal dependency gate remains authoritative for execution.
                continue
            failure = outcome.failure
            if not isinstance(failure, OwnedObjectReconstructionFailure):
                waiting.add(key)
                continue
            if failure in _RETRYABLE_RECONSTRUCTION_FAILURES:
                waiting.add(key)
            else:
                # Even an OWNER_DEAD wire reply is only remote data.  It may
                # make this reconstruction request fail, but only this Core's
                # locally consumed GCS journal may install a death tombstone
                # or discharge release obligations.
                failures[key] = "{}: {}".format(
                    failure.value, outcome.detail
                )
        return waiting, failures

    @staticmethod
    def _valid_get_reply(
        request: GetRetainedOwnedObject, reply: object
    ) -> bool:
        if not isinstance(reply, GetRetainedOwnedObjectReply):
            return False
        try:
            validated = replace(reply)
        except Exception:
            return False
        return (
            validated.object_id == request.object_id
            and validated.owner_worker_id == request.owner_worker_id
            and validated.borrower_worker_id == request.borrower_worker_id
            and validated.hold == request.hold
        )

    @staticmethod
    def _valid_reconstruction_reply(
        request: RequestOwnedObjectReconstruction, reply: object
    ) -> bool:
        if not isinstance(reply, RequestOwnedObjectReconstructionReply):
            return False
        try:
            validated = replace(reply)
        except Exception:
            return False
        return (
            validated.object_id == request.object_id
            and validated.owner_worker_id == request.owner_worker_id
            and validated.requester_worker_id == request.requester_worker_id
            and validated.credential == request.credential
            and validated.borrower_token == request.borrower_token
            and validated.expected_owner_attempt
            == request.expected_owner_attempt
        )

    def complete_renewal(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> bool:
        """Acknowledge that the caller committed the local START."""

        with self._lock:
            session = self._validate_renewal_ready_locked(
                task_id, attempt_id
            )
            del self._renewals[task_id]
            return True

    def validate_renewal_ready(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> bool:
        """Recheck READY while the Core composition lock fences death."""

        with self._lock:
            self._validate_renewal_ready_locked(task_id, attempt_id)
            return True

    def _validate_renewal_ready_locked(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> _RenewalSession:
        session = self._renewals.get(task_id)
        if session is None:
            raise ForeignLineageRuntimeError(
                "foreign lineage has no active renewal"
            )
        if session.attempt_id != attempt_id:
            raise ForeignLineageRuntimeError(
                "renewal completion names another reconstruction attempt"
            )
        if (
            self._closed or session.driving or not session.ready
            or session.uncertain or session.terminal_failures
            or len(session.acknowledged) != len(session.replacements)
            or any(
                self._registry.owner_is_dead(
                    replacement.expected.owner_worker_id
                )
                for replacement in session.replacements.values()
            )
        ):
            raise ForeignLineageRuntimeError(
                "foreign lineage is not ready for local commit"
            )
        return session

    def abandon_renewal(
        self, task_id: TaskID, attempt_id: AttemptID
    ) -> bool:
        """Drop only local saga state after a definitive failed preflight.

        This escape hatch is valid only before *any* owner-side effect.
        Owner-ACKed successors make the registry mixed-generation, while an
        ambiguous request may have committed remotely.  Either case remains a
        durable saga until a later pass converges it or final collection /
        shutdown releases the exact canonical holds.
        """

        with self._lock:
            session = self._renewals.get(task_id)
            if session is None:
                return False
            if session.attempt_id != attempt_id:
                raise ForeignLineageRuntimeError(
                    "renewal abandonment names another attempt"
                )
            changed = any(
                replacement.expected != replacement.replacement
                and key in session.acknowledged
                for key, replacement in session.replacements.items()
            )
            if session.driving or session.uncertain or changed:
                raise ForeignLineageRuntimeError(
                    "foreign replacement effects require durable convergence"
                )
            del self._renewals[task_id]
            return True

    def drive_collection(
        self,
        task_id: TaskID,
        receipt: ForeignLineageCollectionReceipt,
    ) -> ForeignLineageCollectionResult:
        """Accumulate committed receipts and release after the full manifest."""

        if not isinstance(task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(receipt, ForeignLineageCollectionReceipt):
            raise TypeError(
                "receipt must be a ForeignLineageCollectionReceipt"
            )
        if receipt.task_id != task_id:
            raise ValueError("collection receipt belongs to another task")
        with self._lock:
            completed = self._completed_collections.get(task_id)
            if completed is not None:
                return completed
            record = self._registry.snapshot(task_id)
            if record is None:
                return ForeignLineageCollectionResult(
                    task_id, ForeignLineageCollectionDisposition.NOT_FINAL,
                    (), 0,
                )
            receipts = self._collection_receipts.setdefault(task_id, {})
            previous = receipts.setdefault(receipt.output_id, receipt)
            if previous != receipt:
                raise ForeignLineageRuntimeError(
                    "collection receipt identity changed across replay"
                )
            values = tuple(receipts.values())
            if frozenset(receipts) != frozenset(record.output_ids):
                return ForeignLineageCollectionResult(
                    task_id, ForeignLineageCollectionDisposition.NOT_FINAL,
                    (), len(record.edges),
                )
            renewal = self._renewals.get(task_id)
            if renewal is not None:
                if renewal.driving or renewal.uncertain:
                    raise ForeignLineageRuntimeError(
                        "foreign replacement ambiguity must converge before "
                        "final collection"
                    )
                # Final output collection supersedes execution admission.  A
                # mixed-generation registry is safe here: each acknowledged
                # edge names its successor and each definitive rejection still
                # names its old hold.  Release exactly that canonical set.
                self._renewals.pop(task_id, None)
            session = self._collections.get(task_id)
            if session is None:
                plan = self._registry.claim_with_receipts(task_id, values)
                if plan is None:
                    record = self._registry.snapshot(task_id)
                    return ForeignLineageCollectionResult(
                        task_id, ForeignLineageCollectionDisposition.NOT_FINAL,
                        (), 0 if record is None else len(record.edges),
                    )
                session = _CollectionSession(plan)
                self._collections[task_id] = session
            elif frozenset(receipts) != frozenset(session.plan.output_ids):
                raise ForeignLineageRuntimeError(
                    "authoritative collected manifest changed after claim"
                )
            if session.driving:
                return self._collection_result(
                    session, ForeignLineageCollectionDisposition.PENDING
                )
            session.driving = True
        try:
            for edge in session.plan.edges:
                key = self._edge_key(edge)
                with self._lock:
                    if key in session.acknowledged:
                        continue
                    session.uncertain.add(key)
                if self._owner_is_dead(edge.owner_worker_id):
                    with self._lock:
                        session.uncertain.discard(key)
                        session.acknowledged.add(key)
                    continue
                request = ReleaseOwnedObjectForTask(
                    edge.dependency_object_id, edge.owner_worker_id,
                    edge.borrower_worker_id, edge.hold,
                )
                try:
                    reply = self._release(edge.owner_address, request)
                except Exception:
                    if self._owner_is_dead(edge.owner_worker_id):
                        with self._lock:
                            session.uncertain.discard(key)
                            session.acknowledged.add(key)
                    continue
                if not self._valid_release_reply(request, reply):
                    continue
                if not isinstance(reply, ReleaseOwnedObjectForTaskReply):
                    continue
                if not reply.accepted:
                    # A stopped/rejecting live endpoint cannot prove that a
                    # prior ambiguous release did or did not commit.  Keep the
                    # exact obligation until an ACK or death fence arrives.
                    continue
                with self._lock:
                    session.uncertain.discard(key)
                    session.acknowledged.add(key)
            with self._lock:
                complete = (
                    len(session.acknowledged) == len(session.plan.edges)
                    and not session.uncertain
                )
            if not complete:
                return self._collection_result(
                    session, ForeignLineageCollectionDisposition.PENDING
                )
            self._registry.complete_claim(session.plan)
            completed = self._collection_result(
                session, ForeignLineageCollectionDisposition.COMPLETE
            )
            with self._lock:
                self._collections.pop(task_id, None)
                self._collection_receipts.pop(task_id, None)
                self._completed_collections[task_id] = completed
            return completed
        finally:
            with self._lock:
                session.driving = False

    @staticmethod
    def _valid_release_reply(
        request: ReleaseOwnedObjectForTask, reply: object
    ) -> bool:
        if not isinstance(reply, ReleaseOwnedObjectForTaskReply):
            return False
        try:
            validated = replace(reply)
        except Exception:
            return False
        return (
            validated.object_id == request.object_id
            and validated.owner_worker_id == request.owner_worker_id
            and validated.borrower_worker_id == request.borrower_worker_id
            and validated.hold == request.hold
        )

    def mark_owner_dead(
        self, record: DeadWorkerReferenceRecord
    ) -> tuple[TaskID, ...]:
        """Install an external authoritative death fence and converge."""

        if not isinstance(record, DeadWorkerReferenceRecord):
            raise TypeError(
                "owner death requires a DeadWorkerReferenceRecord"
            )
        owner_worker_id = record.worker_id
        affected = self._registry.mark_owner_dead(record)
        with self._lock:
            for session in self._renewals.values():
                for key, replacement in session.replacements.items():
                    if replacement.expected.owner_worker_id != owner_worker_id:
                        continue
                    session.uncertain.discard(key)
                    session.terminal_failures[key] = (
                        "foreign dependency owner has a committed death proof"
                    )
                    session.ready = False
                    session.mutation_epoch += 1
            for session in self._collections.values():
                for edge in session.plan.edges:
                    if edge.owner_worker_id == owner_worker_id:
                        key = self._edge_key(edge)
                        session.uncertain.discard(key)
                        session.acknowledged.add(key)
        return affected

    def has_pending_obligations(self) -> bool:
        """Whether shutdown must preserve this runtime for convergence."""

        with self._lock:
            return bool(
                self._renewals or self._collections
                or self._collection_receipts
                or self._registry.task_ids()
            )

    def has_registered_lineage(self) -> bool:
        return bool(self._registry.task_ids())

    def drive_shutdown(self) -> ForeignLineageShutdownResult:
        """Make one pass toward releasing every outgoing lineage hold.

        The caller must first fence new submissions and local reconstruction
        commits.  Shutdown does not need dependency values, but it must finish
        every possibly-committed replacement before releasing a credential:
        releasing the registry's stale old hold after a lost replacement ACK
        could leak the successor forever.  Once no ambiguous send remains, a
        renewal can be abandoned safely and collection releases the exact
        mixed-generation registry snapshot.
        """

        with self._lock:
            self._closed = True
            self._registry.close_admission()
            renewal_ids = tuple(sorted(self._renewals))
        for task_id in renewal_ids:
            with self._lock:
                session = self._renewals.get(task_id)
                if session is None or session.driving:
                    continue
                session.driving = True
            try:
                self._drive_replacements(session)
            finally:
                with self._lock:
                    session.driving = False
            with self._lock:
                # Definitive failures and dependency waiting do not matter
                # after execution admission is fenced.  Ambiguous mutations do.
                if not session.uncertain:
                    self._renewals.pop(task_id, None)

        results: list[ForeignLineageCollectionResult] = []
        with self._lock:
            resumable = tuple(
                (task_id, next(iter(receipts.values())))
                for task_id, receipts in self._collection_receipts.items()
                if receipts and (
                    self._registry.snapshot(task_id) is not None
                    and frozenset(receipts) == frozenset(
                        self._registry.snapshot(task_id).output_ids  # type: ignore[union-attr]
                    )
                )
            )
        for task_id, receipt in resumable:
            try:
                results.append(self.drive_collection(task_id, receipt))
            except ForeignLineageRuntimeError:
                # A still-ambiguous replacement or a concurrently driving
                # receipt remains visible in the final pending snapshot.
                continue
        with self._lock:
            pending = (
                set(self._renewals) | set(self._collections)
                | set(self._collection_receipts)
            )
            # Registered lineage without an owner/recovery final-sibling proof
            # remains a shutdown barrier.  Shutdown never fabricates collection
            # merely because task admission is closed.
            pending.update(self._registry.task_ids())
            return ForeignLineageShutdownResult(
                not pending, tuple(sorted(pending)), tuple(results)
            )

    def close_admission(self) -> bool:
        """Linearize shutdown against registration and renewal completion."""

        with self._lock:
            changed = not self._closed
            self._closed = True
            self._registry.close_admission()
            return changed

    def pending_task_ids(self) -> tuple[TaskID, ...]:
        with self._lock:
            return tuple(sorted(
                set(self._renewals) | set(self._collections)
                | set(self._collection_receipts)
                | set(self._registry.task_ids())
            ))

    @staticmethod
    def _format_failures(failures: dict[ForeignEdgeKey, str]) -> str:
        return "; ".join(
            "{}@{}: {}".format(object_id, owner_id, failures[key])
            for key in sorted(failures)
            for object_id, owner_id in (key,)
        )

    def _renewal_result(
        self,
        session: _RenewalSession,
        disposition: ForeignLineageRenewalDisposition,
        *,
        waiting_on: tuple[ForeignEdgeKey, ...] = (),
        uncertain: tuple[ForeignEdgeKey, ...] = (),
        failure: Optional[str] = None,
    ) -> ForeignLineageRenewalResult:
        with self._lock:
            return ForeignLineageRenewalResult(
                session.task_id, session.attempt_id, disposition,
                tuple(sorted(session.acknowledged)), len(session.replacements),
                tuple(waiting_on), tuple(uncertain), failure,
            )

    def _collection_result(
        self,
        session: _CollectionSession,
        disposition: ForeignLineageCollectionDisposition,
    ) -> ForeignLineageCollectionResult:
        all_keys = {
            (edge.dependency_object_id, edge.owner_worker_id)
            for edge in session.plan.edges
        }
        with self._lock:
            return ForeignLineageCollectionResult(
                session.plan.task_id, disposition,
                tuple(sorted(session.acknowledged)), len(all_keys),
                tuple(sorted(all_keys - session.acknowledged)),
            )


__all__ = [
    "ForeignEdgeKey",
    "ForeignLineageCollectionDisposition",
    "ForeignLineageCollectionResult",
    "ForeignLineageRenewalDisposition",
    "ForeignLineageRenewalResult",
    "ForeignLineageRuntime",
    "ForeignLineageRuntimeError",
    "ForeignLineageShutdownResult",
    "GetRetainedRpc",
    "OwnerDeathLookup",
    "ReleaseRetainedRpc",
    "ReplaceRetainedRpc",
    "RequestReconstructionRpc",
]
