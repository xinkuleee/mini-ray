"""Pure owner-side admission for foreign object reconstruction.

The requester never receives producer lineage and never reconstructs a foreign
object locally.  This reducer validates an existing borrower capability and an
observed producer epoch, then delegates the only state-changing START/JOIN
decision to an injected local reconstruction authority.

The adapter intentionally performs no RPC, scheduling, queueing, or liveness
detection.  ``OWNER_DEAD`` is possible only when the supplied death lookup
returns an already-committed Worker-death proof.
"""

from __future__ import annotations

from threading import RLock
from typing import Callable, Optional

from .ids import AttemptID, ObjectID, WorkerID
from .ownership import (
    ObjectOwnerSnapshot, ObjectOwnerTable, ObjectState, UnknownObjectError,
)
from .protocol import (
    BorrowedCredential,
    OwnedObjectReconstructionDisposition,
    OwnedObjectReconstructionFailure,
    RetainedCredential,
    RequestOwnedObjectReconstruction,
    RequestOwnedObjectReconstructionReply,
)
from .reconstruction_runtime import (
    ReconstructionDisposition,
    ReconstructionOutcome,
)
from .recovery import (
    RecoveryAction, RecoveryManager, ReconstructionSnapshot, TaskState,
)


ReconstructionAdmission = Callable[[ObjectID], ReconstructionOutcome]
ReconstructionSnapshotLookup = Callable[
    [ObjectID], tuple[ObjectOwnerSnapshot, ReconstructionSnapshot]
]
WorkerDeathLookup = Callable[[WorkerID], object | None]
_TransactionKey = tuple[ObjectID, WorkerID, WorkerID, object, AttemptID]


class ReconstructionDeferred(RuntimeError):
    """Local admission is temporarily closed; no reconstruction was started."""


class OwnedObjectReconstructionReducer:
    """Validate, linearize, and remember owner-routed reconstruction.

    ``admit_reconstruction`` is the sole START/JOIN authority.  It must commit
    its local owner/recovery transition before returning an outcome.  The
    reducer only reads those authorities before and after the callback. A
    concurrent composition layer supplies ``snapshot_reconstruction`` to read
    both under its state lock; that lock must not span admission or its RPCs.

    An exact accepted request is cached before its reply leaves the owner.  A
    lost ACK can therefore be replayed after the callback advanced the attempt
    or even after the borrower was subsequently released.  Rebinding the same
    transaction identity to another source is rejected explicitly.
    """

    def __init__(
        self,
        owner_worker_id: WorkerID,
        owner_table: ObjectOwnerTable,
        recovery: RecoveryManager,
        admit_reconstruction: ReconstructionAdmission,
        *,
        worker_death_lookup: Optional[WorkerDeathLookup] = None,
        snapshot_reconstruction: Optional[ReconstructionSnapshotLookup] = None,
    ) -> None:
        if not isinstance(owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")
        if not isinstance(owner_table, ObjectOwnerTable):
            raise TypeError("owner_table must be an ObjectOwnerTable")
        if not isinstance(recovery, RecoveryManager):
            raise TypeError("recovery must be a RecoveryManager")
        if not callable(admit_reconstruction):
            raise TypeError("admit_reconstruction must be callable")
        if worker_death_lookup is not None and not callable(worker_death_lookup):
            raise TypeError("worker_death_lookup must be callable")
        if snapshot_reconstruction is not None and not callable(snapshot_reconstruction):
            raise TypeError("snapshot_reconstruction must be callable")
        self._owner_worker_id = owner_worker_id
        self._owner = owner_table
        self._recovery = recovery
        self._admit = admit_reconstruction
        # Save the callback without invoking it: Core installs this reducer
        # before constructing the composition lock used by its callback.
        self._snapshot_reconstruction = snapshot_reconstruction
        self._worker_death_lookup = (
            owner_table.dead_worker_record
            if worker_death_lookup is None
            else worker_death_lookup
        )
        self._replies: dict[
            RequestOwnedObjectReconstruction,
            RequestOwnedObjectReconstructionReply,
        ] = {}
        self._claims: dict[
            _TransactionKey, RequestOwnedObjectReconstruction
        ] = {}
        self._lock = RLock()

    def handle(
        self, request: RequestOwnedObjectReconstruction
    ) -> RequestOwnedObjectReconstructionReply:
        if not isinstance(request, RequestOwnedObjectReconstruction):
            raise TypeError(
                "owned reconstruction expects "
                "RequestOwnedObjectReconstruction"
            )
        with self._lock:
            replay = self._replies.get(request)
            if replay is not None:
                return replay

            if request.owner_worker_id != self._owner_worker_id:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.WRONG_OWNER,
                    "request targets a different object owner",
                )

            # A transport error or timeout never reaches this branch.  Only a
            # previously committed journal fact supplied by the caller may
            # classify the physical owner incarnation as dead.
            if self._worker_death_lookup(self._owner_worker_id) is not None:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.OWNER_DEAD,
                    "the owner Worker has a committed death record",
                )

            try:
                owner, recovery = self._snapshots(request.object_id)
            except UnknownObjectError:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.UNKNOWN_OBJECT,
                    "the owner does not know this logical object",
                )
            except Exception:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                    "local reconstruction authority could not read object state",
                )

            key = self._transaction_key(request)
            claimed = self._claims.get(key)
            if claimed is not None and claimed != request:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.REQUEST_CONFLICT,
                    "the reconstruction transaction was already bound to "
                    "different request fields",
                    remember=True,
                )
            credential_failure = self._validate_credential(request, owner)
            if credential_failure is not None:
                failure, detail = credential_failure
                return self._failure(request, failure, detail)

            self._claims[key] = request

            joining = self._is_join_of_active_reconstruction(
                request, owner, recovery
            )
            if (
                owner.current_attempt != request.expected_owner_attempt
                and not joining
            ):
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure
                    .EXPECTED_ATTEMPT_MISMATCH,
                    "the object owner has advanced beyond the expected "
                    "producer attempt",
                )
            if owner.collection_pending:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.COLLECTION_IN_PROGRESS,
                    "object metadata collection is already in progress",
                )
            if not joining and owner.state is not ObjectState.LOST:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.NOT_LOST,
                    "the expected owner attempt is not in LOST state",
                )
            if recovery.is_put:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.PUT_OBJECT,
                    "put objects have no replayable producer task",
                )
            if recovery.lineage is None:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.UNRECONSTRUCTABLE,
                    "the object has no replayable producer lineage",
                )
            if recovery.active_recovery is None and recovery.retries_remaining == 0:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.RETRY_EXHAUSTED,
                    "the producer retry budget is exhausted",
                )

            try:
                outcome = self._admit(request.object_id)
            except ReconstructionDeferred as exc:
                # LOST is not yet externally eligible for reconstruction while
                # the previous execution still owns its finish/accounting gate.
                # Reuse the retryable NOT_LOST projection without caching it:
                # the exact same request may START after local cleanup finishes.
                return self._failure(
                    request, OwnedObjectReconstructionFailure.NOT_LOST,
                    str(exc) or "local reconstruction admission is deferred",
                )
            except Exception as exc:
                return self._failure(
                    request,
                    OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                    "local reconstruction authority rejected the request: {}"
                    .format(exc),
                )
            return self._reply_from_outcome(request, outcome)

    def _snapshots(
        self, object_id: ObjectID,
    ) -> tuple[ObjectOwnerSnapshot, ReconstructionSnapshot]:
        if self._snapshot_reconstruction is not None:
            return self._snapshot_reconstruction(object_id)
        # Standalone reducer callers serialize their local mutations. Runtime
        # callers need the paired composition-layer callback above.
        return (
            self._owner.snapshot(object_id),
            self._recovery.reconstruction_snapshot(object_id),
        )

    @staticmethod
    def _transaction_key(
        request: RequestOwnedObjectReconstruction,
    ) -> _TransactionKey:
        return (
            request.object_id,
            request.owner_worker_id,
            request.requester_worker_id,
            (
                ("borrowed", request.credential.borrower_token)
                if isinstance(request.credential, BorrowedCredential)
                else ("retained", request.credential.hold)
            ),
            request.expected_owner_attempt,
        )

    @staticmethod
    def _validate_credential(
        request: RequestOwnedObjectReconstruction,
        owner: ObjectOwnerSnapshot,
    ) -> tuple[OwnedObjectReconstructionFailure, str] | None:
        credential = request.credential
        if isinstance(credential, BorrowedCredential):
            token = (
                request.requester_worker_id, credential.borrower_token
            )
            if token in owner.released_borrowed_tokens:
                return (
                    OwnedObjectReconstructionFailure.RELEASED_CREDENTIAL,
                    "the borrower credential was already released",
                )
            if token not in owner.borrowed_tokens:
                return (
                    OwnedObjectReconstructionFailure.INACTIVE_CREDENTIAL,
                    "the borrower credential is not active at the owner",
                )
            bindings = dict(owner.borrowed_sources)
            if bindings.get(token) != credential.source:
                return (
                    OwnedObjectReconstructionFailure.CREDENTIAL_MISMATCH,
                    "the active borrower is bound to another export source",
                )
            return None
        if isinstance(credential, RetainedCredential):
            if credential.hold in owner.released_retained_tokens:
                return (
                    OwnedObjectReconstructionFailure.RELEASED_CREDENTIAL,
                    "the retained reconstruction hold was already released",
                )
            if credential.hold not in owner.retained_tokens:
                return (
                    OwnedObjectReconstructionFailure.INACTIVE_CREDENTIAL,
                    "the retained reconstruction hold is not active",
                )
            return None
        return (
            OwnedObjectReconstructionFailure.CREDENTIAL_MISMATCH,
            "the reconstruction credential has an unknown type",
        )

    @staticmethod
    def _is_join_of_active_reconstruction(
        request: RequestOwnedObjectReconstruction,
        owner: ObjectOwnerSnapshot,
        recovery: ReconstructionSnapshot,
    ) -> bool:
        active = recovery.active_recovery
        return (
            isinstance(active, AttemptID)
            and owner.current_attempt == active
            and active.task_id == request.expected_owner_attempt.task_id
            and active.attempt_number
            > request.expected_owner_attempt.attempt_number
        )

    def _reply_from_outcome(
        self,
        request: RequestOwnedObjectReconstruction,
        outcome: ReconstructionOutcome,
    ) -> RequestOwnedObjectReconstructionReply:
        if not isinstance(outcome, ReconstructionOutcome):
            return self._failure(
                request,
                OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                "local reconstruction authority returned an invalid outcome",
            )
        if outcome.disposition is ReconstructionDisposition.FAILED:
            failure = self._failure_from_decision(request, outcome)
            detail = outcome.decision.reason or (
                "local reconstruction authority rejected the request"
            )
            return self._failure(
                request, failure, detail,
            )
        if outcome.disposition not in (
            ReconstructionDisposition.START, ReconstructionDisposition.JOIN
        ):
            return self._failure(
                request,
                OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                "local reconstruction authority returned an unknown disposition",
            )
        attempt = outcome.decision.attempt_id
        if (
            not isinstance(attempt, AttemptID)
            or outcome.decision.task_id != request.object_id.task_id
            or outcome.decision.requested_object_id != request.object_id
            or attempt.task_id != request.object_id.task_id
            or attempt.attempt_number
            <= request.expected_owner_attempt.attempt_number
        ):
            return self._failure(
                request,
                OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                "local reconstruction authority returned a mismatched identity",
            )

        # Admission can enqueue a fast execution which finishes before its
        # first ACK is constructed. Completion clears active_recovery, so the
        # ACK may use matching same-attempt terminal facts as well as an active
        # attempt. Pair the reads: a PENDING owner followed by a newer terminal
        # recovery snapshot is not a coherent admission receipt.
        try:
            owner_after, recovery_after = self._snapshots(request.object_id)
        except Exception:
            return self._failure(
                request,
                OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                "local reconstruction authority did not preserve object state",
            )
        if not self._matches_committed_attempt(owner_after, recovery_after, attempt):
            return self._failure(
                request,
                OwnedObjectReconstructionFailure.AUTHORITY_REJECTED,
                "local reconstruction authority did not commit one attempt "
                "across owner and recovery state",
            )

        disposition = (
            OwnedObjectReconstructionDisposition.STARTED
            if outcome.disposition is ReconstructionDisposition.START
            else OwnedObjectReconstructionDisposition.JOINED
        )
        reply = RequestOwnedObjectReconstructionReply(
            request.object_id,
            request.owner_worker_id,
            request.requester_worker_id,
            request.credential,
            request.borrower_token,
            request.expected_owner_attempt,
            disposition,
            reconstruction_attempt=attempt,
        )
        self._replies[request] = reply
        return reply

    @staticmethod
    def _matches_committed_attempt(
        owner: ObjectOwnerSnapshot,
        recovery: ReconstructionSnapshot,
        attempt: AttemptID,
    ) -> bool:
        if owner.current_attempt != attempt or recovery.current_attempt != attempt:
            return False
        if recovery.active_recovery == attempt:
            return (
                owner.state is ObjectState.PENDING
                and recovery.task_state in (TaskState.RETRY_PENDING, TaskState.RUNNING)
            )
        if recovery.active_recovery is not None:
            return False
        if owner.state in (ObjectState.READY_INLINE, ObjectState.READY_STORED):
            return recovery.task_state is TaskState.SUCCEEDED
        if owner.state is ObjectState.ERROR:
            return recovery.task_state in (
                TaskState.APPLICATION_FAILED, TaskState.SYSTEM_FAILED,
            )
        # This is a same-attempt state proof, not a history of admitted work.
        # In particular, later loss/retry and targeted ERROR + logical task
        # SUCCEEDED need their own evidence; do not widen acceptance here.
        return False

    def _failure_from_decision(
        self,
        request: RequestOwnedObjectReconstruction,
        outcome: ReconstructionOutcome,
    ) -> OwnedObjectReconstructionFailure:
        action = outcome.decision.action
        if action is RecoveryAction.FAIL_RETRY_EXHAUSTED:
            return OwnedObjectReconstructionFailure.RETRY_EXHAUSTED
        if action is RecoveryAction.UNRECONSTRUCTABLE_OBJECT:
            snapshot = self._recovery.reconstruction_snapshot(
                request.object_id
            )
            return (
                OwnedObjectReconstructionFailure.PUT_OBJECT
                if snapshot.is_put
                else OwnedObjectReconstructionFailure.UNRECONSTRUCTABLE
            )
        if action is RecoveryAction.FENCE_STALE_ATTEMPT:
            return (
                OwnedObjectReconstructionFailure.EXPECTED_ATTEMPT_MISMATCH
            )
        return OwnedObjectReconstructionFailure.AUTHORITY_REJECTED

    def _failure(
        self,
        request: RequestOwnedObjectReconstruction,
        failure: OwnedObjectReconstructionFailure,
        detail: str,
        *,
        remember: bool = False,
    ) -> RequestOwnedObjectReconstructionReply:
        reply = RequestOwnedObjectReconstructionReply(
            request.object_id,
            request.owner_worker_id,
            request.requester_worker_id,
            request.credential,
            request.borrower_token,
            request.expected_owner_attempt,
            OwnedObjectReconstructionDisposition.FAILED,
            failure=failure,
            detail=detail,
        )
        if remember:
            self._replies[request] = reply
        return reply


__all__ = [
    "OwnedObjectReconstructionReducer",
    "ReconstructionAdmission",
    "ReconstructionSnapshotLookup",
    "WorkerDeathLookup",
]
