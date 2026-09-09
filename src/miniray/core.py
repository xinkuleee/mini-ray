"""Submission, object ownership and recovery for Drivers and ordinary Workers.

CoreWorker combines logical identity, dependency admission, owner handoff and
lineage recovery. Tasks obtain a Node lease and submit directly to its Worker.
Ordinary results are handed to their owner; GCS retains membership/Actor/PG
control only. Node resources and physical result custody remain Node duties.
"""

from __future__ import annotations

import hashlib
import math
import queue
import sys
import threading
import time
import uuid
import weakref
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field, fields, replace
from enum import Enum
from typing import Dict, Mapping, Optional, Sequence, Tuple

import cloudpickle

from . import protocol
from .output_handoff import OutputHandoffTable, OutputHandoffPhase
from .actor_client import ActorCallFence, ActorClientTable
from .contained_edges import (
    ContainedReferenceEdge, ContainedReferenceHold,
    IncomingContainedReferenceHold,
    LineageReferenceEdge,
    ObjectMetadataCollectionPlan,
)
from .dependency import (
    encode_task_argument,
    nested_references,
    resolve_task_arguments,
    top_level_references,
)
from .errors import (
    ActorDiedError,
    ActorUnavailableError,
    BorrowedObjectUnavailableError,
    InfeasibleTaskError,
    LeaseRejectedError,
    NodeDiedError,
    OwnerDiedError,
    OwnerUnavailableError,
    PendingCapacityError,
    PlacementGroupLostError,
    RuntimeShuttingDownError,
    SystemTaskError,
    TaskError,
    UnreconstructableObjectError,
    WorkerDiedError,
    ProtocolError,
)
from .ids import (
    ActorGeneration, ActorID, AttemptID, JobID, LeaseID, NodeID, ObjectID,
    PlacementGroupID, TaskID, WorkerID,
)
from .lease_policy import ObjectLocality, preferred_lease_node
from .foreign_lineage import (
    ForeignLineageCollectionReceipt, ForeignLineageEdge,
    ForeignLineagePreparedCollectionReceipt, ForeignLineageRegistry,
    ForeignLineageRole, ForeignLineageTask,
)
from .foreign_lineage_runtime import (
    ForeignLineageCollectionDisposition, ForeignLineageRenewalDisposition,
    ForeignLineageRuntime, ForeignLineageRuntimeError,
)
from .ownership import (
    DeadWorkerReferenceRecord, NodeLocationRemoval, ObjectOwnerSnapshot, ObjectOwnerTable, ObjectState, UnknownObjectError,
)
from .ownership import (
    OutputOwnerPublicationPlan, OutputOwnerPublicationDisposition,
    OutputOwnerPublicationCollectionPlan, OutputOwnerPublicationConflictError,
)
from .output_publication import OutputPublicationEnvelope, OutputPublicationCompleteWitness, OutputPublicationID
from .output_publication_journal import OutputPublicationAdoptionProof, OutputPublicationSlotCleanupProof
from .owner_service import (
    REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER,
    StoredContainedPinOwnerAdapter,
)
from .owner_reconstruction import (
    OwnedObjectReconstructionReducer, ReconstructionDeferred,
)
from .retained_replacement import (
    replace_retained_object_for_task as _replace_retained_object_for_task,
)
from .placement import PlacementStrategy
from .recovery import (
    FailureKind, RecoveryAction, RecoveryDecision, RecoveryManager, ReconstructionSnapshot,
)
from .reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionDisposition, ReconstructionOutcome,
    ReconstructionRuntimeError,
)
from .ref_transfer import (
    current_exporter,
    importing_references,
    restore_exported_reference,
)
from .resources import ResourceVector
from .replica_cleanup import ReplicaCleanupQueue
from .node_death_view import GET_NODE_DEATH_VIEW, GetInstalledNodeDeaths, GetInstalledNodeDeathsReply
from .runtime_binding import current_execution_context
from .trace import EventSink, causal_scope, current_cause_id
from .task_outputs import TaskExecutionKey, validate_num_returns
from .transport import (
    Address,
    RemoteCallError,
    TransportConnectionError,
    TransportError,
    request as rpc_request,
)


_REQUEST_LEASE_HANDLER = "request_worker_lease"
_CANCEL_LEASE_HANDLER = "cancel_worker_lease"
_PUSH_TASK_HANDLER = "push_task"
_GET_WORKER_LEASE_OUTCOME_HANDLER = "get_worker_lease_outcome"
_GET_OBJECT_HANDLER = "get_object"
_DROP_OBJECT_REPLICA_HANDLER = "drop_object_replica"
_GET_NODE_ADDRESS_HANDLER = "get_node_address"
_CREATE_ACTOR_HANDLER = "create_actor"
_CREATE_PLACEMENT_GROUP_HANDLER = "create_placement_group"
_REMOVE_PLACEMENT_GROUP_HANDLER = "remove_placement_group"
_ACTOR_CALL_HANDLER = "actor_call"
_GET_ACTOR_STATE_HANDLER = "get_actor_state"
_GET_WORKER_DEATHS_HANDLER = "get_worker_deaths"
_ACQUIRE_BORROWED_OBJECT_HANDLER = "acquire_borrowed_object"
_RELEASE_BORROWED_OBJECT_HANDLER = "release_borrowed_object"
_GET_OWNED_OBJECT_HANDLER = "get_owned_object"
_REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER = (
    "request_owned_object_reconstruction"
)
_REQUEST_DROP_OWNED_OBJECT_HANDLER = "request_drop_owned_object"
_RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER = "retain_owned_object_for_task"
_GET_RETAINED_OWNED_OBJECT_HANDLER = "get_retained_owned_object"
_REPORT_RETAINED_OBJECT_LOCATION_HANDLER = "report_retained_object_location"
_RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER = "release_owned_object_for_task"
_RELEASE_CONTAINED_REFERENCE_HANDLER = "release_contained_reference"
_RPC_TOTAL_TIMEOUT_SECONDS = 4.0
_RPC_CONNECT_TIMEOUT_SECONDS = 0.5
_RPC_CALL_DEADLINE: ContextVar[Optional[float]] = ContextVar(
    "miniray_core_rpc_call_deadline", default=None
)
_LEASE_RPC_REPLAY_ATTEMPTS = 3
_DEFAULT_DISPATCH_LANES = 1
_MAX_DISPATCH_LANES = 2
_CAPACITY_RETRY_BASE_SECONDS = 0.01
_CAPACITY_RETRY_MAX_SECONDS = 0.25
_PUSH_RETRY_BASE_SECONDS = 0.01
_PUSH_RETRY_MAX_SECONDS = 0.25
_PLACEMENT_GROUP_RETRY_BASE_SECONDS = 0.01
_PLACEMENT_GROUP_RETRY_MAX_SECONDS = 0.25
_PLACEMENT_GROUP_CONNECT_ATTEMPTS = 3
_ACTOR_CREATE_REPLAY_ATTEMPTS = 3
_ACTOR_CREATE_RETRY_BASE_SECONDS = 0.01
_ACTOR_CREATE_RETRY_MAX_SECONDS = 0.05
_ACTOR_CALL_CONVERGENCE_SECONDS = 1.0
_ACTOR_CALL_RETRY_BASE_SECONDS = 0.01
_ACTOR_CALL_RETRY_MAX_SECONDS = 0.1
_FOREIGN_DEPENDENCY_POLL_SECONDS = 0.01
_WORKER_DEATH_POLL_SECONDS = 0.1
_DEFAULT_INLINE_THRESHOLD_BYTES = 100 * 1024
_STOP = object()
_WAKE_COORDINATOR = object()
_STOP_REFERENCE_EVENTS = object()


class _LazyBlockingGroup:
    """Adapt a notifier group without requiring it from narrow test doubles."""

    def __init__(self, notifier: object | None) -> None:
        self._notifier = notifier
        factory = getattr(notifier, "group_scope", None)
        self._group = factory() if callable(factory) else None
        self._scope = None

    def begin_blocking(self) -> None:
        if self._group is not None:
            self._group.begin_blocking()
            return
        if self._scope is None and self._notifier is not None:
            scope = self._notifier.blocking_scope()
            scope.__enter__()
            self._scope = scope

    def close(self, exc_type=None, exc=None, tb=None) -> None:
        if self._group is not None:
            self._group.close(exc_type, exc, tb)
            return
        scope, self._scope = self._scope, None
        if scope is not None:
            scope.__exit__(exc_type, exc, tb)

    def __enter__(self) -> "_LazyBlockingGroup":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(exc_type, exc, tb)
_BORROW_POLL_EVENT = threading.Event()


def _task_reference_hold(
    kind: protocol.TaskReferenceHoldKind,
    worker_id: WorkerID,
    task_id: TaskID,
    origin_attempt_id: AttemptID,
) -> protocol.TaskReferenceHold:
    """Build the complete credential for one Task-hold incarnation."""

    return protocol.TaskReferenceHold(
        kind, worker_id, task_id, origin_attempt_id
    )


def _worker_death_reference_id(
    death: protocol.WorkerDeathRecord,
) -> str:
    """Canonical identity for the complete immutable GCS death proof."""

    return "worker-death:v1:{}:{}:{}:{}:{}:{}:{}:{}:{}".format(
        death.death_epoch,
        death.worker_id.hex,
        death.node_id.hex,
        death.node_pid,
        death.node_registration_epoch,
        death.worker_pid,
        death.exit_code,
        death.reason.value,
        death.detection_id,
    )


def _lineage_hold_token(
    producer_task_id: TaskID, dependency_object_id: ObjectID
) -> str:
    """Canonical local dependency hold owned by producer lineage."""

    return "lineage:{}:{}".format(producer_task_id, dependency_object_id)


def _contains_object_ref(value: object, seen: Optional[set[int]] = None) -> bool:
    """Find ObjectRefs in the finite common containers supported by K0."""

    if isinstance(value, ObjectRef):
        return True
    if not isinstance(value, (list, tuple, dict, set, frozenset)):
        return False
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return False
    seen.add(identity)
    values = (
        tuple(value.keys()) + tuple(value.values())
        if isinstance(value, dict)
        else tuple(value)
    )
    return any(_contains_object_ref(item, seen) for item in values)


@dataclass(frozen=True)
class _LocalReferenceRelease:
    """One non-blocking finalizer event consumed by the owning CoreWorker."""

    object_id: ObjectID
    token: object
    done: threading.Event


_BorrowReleaseKey = tuple[WorkerID, ObjectID, WorkerID, str]


@dataclass(frozen=True)
class _BorrowReleaseIdentity:
    """Immutable route, source proof, and idempotent release identity."""

    owner_address: Address
    acquire: protocol.AcquireBorrowedObject
    release: protocol.ReleaseBorrowedObject

    def __post_init__(self) -> None:
        acquire_identity = (
            self.acquire.owner_worker_id,
            self.acquire.object_id,
            self.acquire.borrower_worker_id,
            self.acquire.borrower_token,
        )
        release_identity = (
            self.release.owner_worker_id,
            self.release.object_id,
            self.release.borrower_worker_id,
            self.release.borrower_token,
        )
        if acquire_identity != release_identity:
            raise ValueError("borrow acquire/release identities must match")

    @property
    def key(self) -> _BorrowReleaseKey:
        return (
            self.release.owner_worker_id,
            self.release.object_id,
            self.release.borrower_worker_id,
            self.release.borrower_token,
        )


@dataclass
class _BorrowReleaseObligation:
    """One immutable borrower identity with mutable retry progress."""

    identity: _BorrowReleaseIdentity
    release_requested: bool = False
    scheduled_round: Optional[int] = None
    retry_round: int = 0

    @property
    def owner_address(self) -> Address:
        return self.identity.owner_address

    @property
    def acquire(self) -> protocol.AcquireBorrowedObject:
        return self.identity.acquire

    @property
    def release(self) -> protocol.ReleaseBorrowedObject:
        return self.identity.release

    @property
    def key(self) -> _BorrowReleaseKey:
        return self.identity.key


@dataclass(frozen=True)
class _ReleaseBorrowedReference:
    """Drive one durable ordinary-borrower release."""

    key: _BorrowReleaseKey
    done: Optional[threading.Event] = None
    scheduled_round: Optional[int] = None


@dataclass
class _AttemptBorrowRelease:
    """Durable local obligation for one physical attempt borrower."""

    transfer: protocol.NestedReferenceTransfer
    attempt_id: AttemptID
    acquire: protocol.AcquireBorrowedObject
    release: protocol.ReleaseBorrowedObject
    release_requested: bool = False
    retry_scheduled: bool = False
    retry_round: int = 0

    @property
    def key(self) -> tuple[WorkerID, ObjectID, AttemptID]:
        return (
            self.transfer.owner_worker_id,
            self.transfer.object_id,
            self.attempt_id,
        )


@dataclass(frozen=True)
class _ReleaseAttemptBorrow:
    key: tuple[WorkerID, ObjectID, AttemptID]




@dataclass
class _ObjectGcObligation:
    """Frozen collection plan plus the exact work not yet ACKed."""

    plan: ObjectMetadataCollectionPlan
    pending_drops: dict[NodeID, protocol.DropObjectReplica]
    pending_edges: set[ContainedReferenceEdge]
    output_plan: OutputOwnerPublicationCollectionPlan | None = None
    retry_scheduled: bool = False
    retry_round: int = 0
    publication: object = None
    publication_fence: object = None
    child_receipts: dict = field(default_factory=dict)
    child_deaths: dict = field(default_factory=dict)

    @property
    def object_id(self) -> ObjectID:
        return self.plan.object_id


@dataclass(frozen=True)
class _RetryInlineGc:
    object_id: ObjectID


@dataclass(frozen=True)
class _RetryReplicaCleanup:
    """Wake one physical cleanup queue, never a logical-object GC claim."""


@dataclass(frozen=True)
class _RetryForeignLineageCollection:
    task_id: TaskID
    receipt: ForeignLineageCollectionReceipt




@dataclass(frozen=True)
class _ForeignDependencyGuard:
    """Stable owner credential held for one logical submitted task."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    owner_address: Address
    borrower_worker_id: WorkerID
    borrower_token: str
    hold: protocol.TaskReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.hold, protocol.TaskReferenceHold):
            raise TypeError("foreign dependency hold must be a TaskReferenceHold")
        if self.hold.kind is not protocol.TaskReferenceHoldKind.RETAINED:
            raise ValueError("foreign dependency hold must have RETAINED kind")
        if self.hold.submitting_worker_id != self.borrower_worker_id:
            raise ValueError(
                "foreign dependency hold submitter must match its borrower"
            )


@dataclass(frozen=True)
class _ForeignGuardReleaseRetry:
    pending: "_PendingTask"
    due_at: float
    round: int = 0
    released_keys: tuple[
        tuple[WorkerID, ObjectID, protocol.TaskReferenceHold], ...
    ] = ()


@dataclass(frozen=True)
class _ForeignLocationReport:
    """One exact post-grant report routed by a retained owner credential."""

    guard: _ForeignDependencyGuard
    request: protocol.ReportRetainedObjectLocation


@dataclass(frozen=True)
class _ReplicaLocationReceipt:
    """Local owner result; no invented foreign RETAINED credential or RPC."""

    descriptor: protocol.ObjectStoreDescriptor
    status: protocol.RetainedLocationReportStatus
    error: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status in (protocol.RetainedLocationReportStatus.ADDED,
                               protocol.RetainedLocationReportStatus.ALREADY_RECORDED)

    @property
    def custody_transferred(self) -> bool:
        return self.accepted or self.status in (protocol.RetainedLocationReportStatus.CUSTODY_ONLY,
                                                protocol.RetainedLocationReportStatus.RETIRED)


@dataclass(frozen=True)
class _LocationReportState:
    """One dependency custody handoff, with optional execution permission."""

    grant: protocol.GrantWorkerLease | None
    granting_node_address: Address
    reports: tuple[_ForeignLocationReport, ...]
    acknowledged_keys: tuple[
        tuple[WorkerID, ObjectID, protocol.TaskReferenceHold], ...
    ] = ()
    round: int = 0
    lease_request: protocol.RequestWorkerLease | None = None
    terminal_error: BaseException | None = None
    receipts: tuple[protocol.ReportRetainedObjectLocationReply, ...] = ()
    owner_deaths: tuple[DeadWorkerReferenceRecord, ...] = ()
    cancellation_reply: protocol.CancelWorkerLeaseReply | None = None
    local_receipts: tuple[_ReplicaLocationReceipt, ...] = ()
    execution_outcome: protocol.GetWorkerLeaseOutcomeReply | None = None
    inventory: protocol.LeaseDependencyInventory | None = None
    custody_acknowledged: bool = False

    @property
    def node_id(self) -> NodeID:
        return self.grant.node_id if self.grant is not None else self.inventory.node_id

    @property
    def lease_id(self) -> LeaseID:
        return self.grant.lease_id if self.grant is not None else self.inventory.lease_request.lease_id

    @property
    def descriptors(self) -> tuple[protocol.ObjectStoreDescriptor, ...]:
        return self.grant.dependencies if self.grant is not None else self.inventory.descriptors


@dataclass(frozen=True)
class _NodeDeathObserved:
    """Coordinator handoff after a committed DEAD fence is installed."""

    death: protocol.NodeDeathRecord
    membership_epoch: int


@dataclass(frozen=True)
class _HomeRoute:
    """One immutable Driver-to-Node scheduling route at a membership epoch.

    ``WorkerID`` remains the Core/owner identity.  This value is only the
    physical NodeManager through which new Driver operations enter the cluster.
    Replacing one frozen value under ``CoreWorker._state_lock`` prevents a
    migrated NodeID from ever being paired with the previous Node's address.
    """

    node_id: NodeID
    address: Address
    membership_epoch: int

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise TypeError("home route node_id must be a NodeID")
        if (
            not isinstance(self.address, tuple)
            or len(self.address) != 2
            or not isinstance(self.address[0], str)
            or not self.address[0]
            or isinstance(self.address[1], bool)
            or not isinstance(self.address[1], int)
            or not 0 < self.address[1] <= 65535
        ):
            raise ValueError("home route address must be a bound TCP address")
        if (
            isinstance(self.membership_epoch, bool)
            or not isinstance(self.membership_epoch, int)
            or self.membership_epoch < 0
        ):
            raise ValueError("home route membership_epoch must be non-negative")


def _select_home_route(
    current: _HomeRoute | None, snapshot: protocol.InstallClusterSnapshot
) -> _HomeRoute | None:
    """Keep a live home, otherwise select the canonical lowest survivor.

    Selection is deliberately policy-free.  The GCS snapshot is the liveness
    authority; stable ordering merely makes every replay choose the same route.
    """

    if not isinstance(snapshot, protocol.InstallClusterSnapshot):
        raise TypeError("snapshot must be an InstallClusterSnapshot")
    if current is not None:
        if snapshot.membership_epoch < current.membership_epoch:
            raise ValueError("cluster snapshot is older than the current home route")
    if not snapshot.nodes:
        return None
    by_id = {node.node_id: node for node in snapshot.nodes}
    if current is not None and current.node_id in by_id:
        retained = by_id[current.node_id]
        if retained.address != current.address:
            raise ValueError("live home Node changed its physical address")
        return _HomeRoute(
            retained.node_id, retained.address, snapshot.membership_epoch
        )
    selected = min(snapshot.nodes, key=lambda node: bytes(node.node_id))
    return _HomeRoute(
        selected.node_id, selected.address, snapshot.membership_epoch
    )


class _DependencyBecamePending(RuntimeError):
    """A foreign producer changed back to PENDING across the query gate."""


class _StoredFetchStateChanged(RuntimeError):
    """Owner state changed while one physical replica fetch was in flight."""


class _LocationReportRejected(SystemTaskError):
    """The owner definitively rejected one post-grant location fact."""

    def __init__(self, message: str, *, handoff_complete: bool = False) -> None:
        super().__init__(message)
        self.handoff_complete = handoff_complete


class _LocationHandoffTerminal(Exception):
    """A Node-loss path already published this consumer's terminal result."""


class _ReferenceEventMailbox:
    """Linearization point shared by finalizers and Core shutdown."""

    def __init__(
        self, owner_table: ObjectOwnerTable, borrower_worker_id: WorkerID,
        core_reference: object | None = None,
    ) -> None:
        self.owner_table = owner_table
        self.borrower_worker_id = borrower_worker_id
        self.core_reference = core_reference
        self.events = queue.Queue()  # type: queue.Queue[object]
        self.lock = threading.Lock()
        self.accepting = True
        self.stop_enqueued = False
        self.stopped = threading.Event()

    def enqueue(
        self, event: object
    ) -> bool:
        with self.lock:
            if not self.accepting:
                return False
            self.events.put_nowait(event)
            return True

    def enqueue_internal(self, event: object) -> bool:
        """Queue owner bookkeeping after external finalizer admission closes."""

        with self.lock:
            if self.stop_enqueued:
                return False
            self.events.put_nowait(event)
            return True

    def close_admission(self) -> None:
        """Fence new handle finalizers without stopping internal retries."""

        with self.lock:
            self.accepting = False

    def stop(self) -> None:
        # Holding this lock across the non-blocking put establishes the FIFO
        # rule: every accepted release precedes the stop sentinel, and every
        # later finalizer is a no-op.
        with self.lock:
            if self.stop_enqueued:
                return
            self.accepting = False
            self.stop_enqueued = True
            self.events.put_nowait(_STOP_REFERENCE_EVENTS)


def _run_reference_event_loop(mailbox: _ReferenceEventMailbox) -> None:
    """Apply local releases serially, outside Python finalizer context."""

    try:
        while True:
            event = mailbox.events.get()
            try:
                if event is _STOP_REFERENCE_EVENTS:
                    return
                if isinstance(event, _LocalReferenceRelease):
                    released = False
                    try:
                        released = mailbox.owner_table.release_local_reference(
                            event.object_id, event.token
                        )
                    except Exception:
                        # Metadata may already have been explicitly collected.
                        # A late/duplicate release is intentionally harmless.
                        pass
                    finally:
                        if released and mailbox.core_reference is not None:
                            try:
                                core = mailbox.core_reference()
                                if core is not None:
                                    core._enqueue_inline_gc_check(event.object_id)
                            except Exception:
                                # The obligation remains represented by owner
                                # metadata; finalizers never fail user code.
                                pass
                        event.done.set()
                else:
                    if isinstance(event, _RetryInlineGc):
                        try:
                            if mailbox.core_reference is not None:
                                core = mailbox.core_reference()
                                if core is not None:
                                    core._reference_released(event.object_id)
                        finally:
                            continue
                    if isinstance(event, _RetryReplicaCleanup):
                        try:
                            if mailbox.core_reference is not None:
                                core = mailbox.core_reference()
                                if core is not None:
                                    core._drive_late_replica_cleanup(from_event=True)
                        finally:
                            continue
                    if isinstance(event, _RetryForeignLineageCollection):
                        try:
                            if mailbox.core_reference is not None:
                                core = mailbox.core_reference()
                                if core is not None:
                                    core._drive_foreign_lineage_collection(
                                        event.receipt, from_retry=True
                                    )
                        finally:
                            continue
                    if isinstance(event, _ReleaseAttemptBorrow):
                        try:
                            if mailbox.core_reference is not None:
                                core = mailbox.core_reference()
                                if core is not None:
                                    core._drive_attempt_borrow_release(event.key)
                        finally:
                            continue
                    assert isinstance(event, _ReleaseBorrowedReference)
                    try:
                        if mailbox.core_reference is not None:
                            core = mailbox.core_reference()
                            if core is not None:
                                core._drive_borrowed_reference_release(
                                    event.key,
                                    scheduled_round=event.scheduled_round,
                                )
                    finally:
                        if event.done is not None:
                            event.done.set()
            finally:
                mailbox.events.task_done()
    finally:
        mailbox.stopped.set()


def _stop_reference_mailbox(mailbox: _ReferenceEventMailbox) -> None:
    """Best-effort CoreWorker GC hook; never blocks interpreter teardown."""

    try:
        mailbox.stop()
    except Exception:
        pass


def _finalize_local_reference(
    core_reference: object,
    object_id: ObjectID,
    token: object,
    done: threading.Event,
) -> None:
    """Enqueue a release without retaining or doing work in the finalizer.

    ``weakref.finalize`` keeps its callback arguments alive.  Holding only a
    weak reference to the CoreWorker avoids turning every ObjectRef into an
    accidental owner of the whole runtime.  The callback performs no RPC and
    never waits; owner-table mutation belongs to the reference event thread.
    """

    try:
        core = core_reference()  # type: ignore[operator]
        if core is not None and core._enqueue_local_reference_release(  # type: ignore[attr-defined]
            object_id, token, done
        ):
            return
    except Exception:
        # Finalizers must remain harmless during interpreter/runtime teardown.
        pass
    done.set()


def _finalize_borrowed_reference(
    core_reference: object,
    object_id: ObjectID,
    owner_worker_id: WorkerID,
    owner_address: Address,
    borrower_token: str,
    done: threading.Event,
) -> None:
    """Enqueue a remote release without doing network I/O in GC context."""

    try:
        core = core_reference()  # type: ignore[operator]
        if core is not None and core._enqueue_borrowed_reference_release(  # type: ignore[attr-defined]
            object_id, owner_worker_id, owner_address, borrower_token, done
        ):
            return
    except Exception:
        pass
    done.set()


def _finalize_attempt_borrow_reference(
    core_reference: object,
    key: tuple[WorkerID, ObjectID, AttemptID],
    done: threading.Event,
) -> None:
    """Enqueue durable attempt-borrow cleanup without network I/O in GC."""

    try:
        core = core_reference()  # type: ignore[operator]
        if core is not None and core._request_attempt_borrow_release(  # type: ignore[attr-defined]
            key, done
        ):
            return
    except Exception:
        pass
    done.set()


class ObjectRef:
    """A reference to an immutable logical return object.

    The reference contains no result bytes.  Readiness and inline bytes remain
    in the creating ``CoreWorker``, which is the object owner in v0.1.
    """

    __slots__ = (
        "_object_id",
        "_owner_worker_id",
        "_owner_address",
        "_borrower_token",
        "_borrow_source",
        "_attempt_borrow_key",
        "_local_token",
        "_release_done",
        "_finalizer",
        "_closed",
        "__weakref__",
    )

    def __init__(
        self,
        object_id: ObjectID,
        owner_worker_id: WorkerID,
        owner_address: Optional[Address] = None,
    ) -> None:
        self._object_id = object_id
        self._owner_worker_id = owner_worker_id
        self._owner_address = owner_address
        self._borrower_token = None
        # A borrowed token is authority only together with the exact source
        # that the object owner accepted.  Keep that proof on the live handle
        # so a later contained-result publication can derive a new hold
        # without querying or mutating the owner during serialization.
        self._borrow_source = None
        self._attempt_borrow_key = None
        # Direct construction and the no-exporter pickle fallback start
        # detached: owner_address is a route, not a lifetime capability.
        # Runtime paths bind owner-local tokens or, for exported foreign refs,
        # an owner-ACKed borrower token/source and release finalizer before
        # exposing the restored handle.
        self._local_token = None
        self._release_done = None
        self._finalizer = None
        self._closed = False

    def _bind_local_reference(self, core: "CoreWorker", token: object) -> None:
        """Attach the one owner token allocated for this Python handle."""

        if self._finalizer is not None or self._local_token is not None:
            raise RuntimeError("ObjectRef already has a local reference token")
        done = threading.Event()
        finalizer = weakref.finalize(
            self,
            _finalize_local_reference,
            weakref.ref(core),
            self._object_id,
            token,
            done,
        )
        # Runtime shutdown, rather than interpreter finalization order, owns
        # the reference event thread.  Ordinary GC still invokes the callback.
        finalizer.atexit = False
        self._local_token = token
        self._release_done = done
        self._finalizer = finalizer

    def _bind_borrowed_reference(
        self, core: "CoreWorker", borrower_token: str,
        source: protocol.BorrowSource,
    ) -> None:
        if self._owner_address is None:
            raise RuntimeError("borrowed ObjectRef has no owner endpoint")
        if self._finalizer is not None or self._borrower_token is not None:
            raise RuntimeError("ObjectRef already has a reference token")
        if not isinstance(
            source, (protocol.ContainedTransferSource, protocol.TaskHoldSource)
        ):
            raise TypeError(
                "borrowed ObjectRef source must be a contained transfer or "
                "task hold"
            )
        done = threading.Event()
        finalizer = weakref.finalize(
            self,
            _finalize_borrowed_reference,
            weakref.ref(core),
            self._object_id,
            self._owner_worker_id,
            self._owner_address,
            borrower_token,
            done,
        )
        finalizer.atexit = False
        self._borrower_token = borrower_token
        self._borrow_source = source
        self._release_done = done
        self._finalizer = finalizer

    def _bind_attempt_borrow_reference(
        self,
        core: "CoreWorker",
        key: tuple[WorkerID, ObjectID, AttemptID],
        borrower_token: str,
        source: protocol.BorrowSource,
    ) -> None:
        """Bind close to a durable Core obligation instead of best effort."""

        if self._owner_address is None:
            raise RuntimeError("borrowed ObjectRef has no owner endpoint")
        if self._finalizer is not None or self._borrower_token is not None:
            raise RuntimeError("ObjectRef already has a reference token")
        if not isinstance(
            source, (protocol.ContainedTransferSource, protocol.TaskHoldSource)
        ):
            raise TypeError(
                "attempt-borrowed ObjectRef source must be a contained "
                "transfer or task hold"
            )
        done = threading.Event()
        finalizer = weakref.finalize(
            self,
            _finalize_attempt_borrow_reference,
            weakref.ref(core),
            key,
            done,
        )
        finalizer.atexit = False
        self._attempt_borrow_key = key
        self._borrower_token = borrower_token
        self._borrow_source = source
        self._release_done = done
        self._finalizer = finalizer

    @property
    def object_id(self) -> ObjectID:
        return self._object_id

    @property
    def owner_worker_id(self) -> WorkerID:
        """Return the identity of the CoreWorker that owns this object."""

        return self._owner_worker_id

    @property
    def owner_address(self) -> Optional[Address]:
        """Physical route to a remote owner; WorkerID remains identity."""

        return self._owner_address

    @property
    def borrower_token(self) -> Optional[str]:
        return self._borrower_token

    @property
    def borrow_source(self) -> Optional[protocol.BorrowSource]:
        """Return the immutable owner-accepted source for this live borrow.

        It is intentionally excluded from equality and hashing: ObjectRef
        identity remains ``(owner WorkerID, ObjectID)`` while this field is a
        capability used only by protocols that transfer lifetime custody.
        """

        return self._borrow_source

    @property
    def closed(self) -> bool:
        """Whether this runtime-bound handle was explicitly closed."""

        return self._closed

    def close(self, *, timeout: Optional[float] = None) -> None:
        """Release this handle and optionally bound the receipt wait.

        GC and explicit close enqueue the same release once. ``None`` retains
        the original indefinite wait; zero is a nonblocking receipt check. A
        timeout leaves this handle closed and the original release pending.
        Calling close again waits for that same receipt, not a second release.

        This receipt concerns the local release event/retained release intent,
        not necessarily a remote owner ACK or physical replica/lineage GC.
        The runtime continues any unacknowledged remote cleanup. This never
        cancels tasks or transfers ownership. Detached handles still have
        nothing to release.
        """

        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError("close timeout must be a number or None")
            try:
                timeout = float(timeout)
            except OverflowError:
                raise ValueError("close timeout must be finite and non-negative") from None
            if not math.isfinite(timeout) or timeout < 0:
                raise ValueError("close timeout must be finite and non-negative")
        deadline = None if timeout is None else time.monotonic() + timeout
        # A synchronous release can clear the handle's binding. Retain its
        # receipt before invoking the finalizer so close cannot skip the ACK.
        finalizer, done = self._finalizer, self._release_done
        if finalizer is None:
            return
        self._closed = True
        finalizer()
        if done is None:
            return
        if deadline is None:
            done.wait()
            return
        remaining = max(0.0, deadline - time.monotonic())
        # Event.wait has a platform limit even for finite Python floats. Long
        # waits remain one logical deadline rather than raising OverflowError.
        while remaining > threading.TIMEOUT_MAX:
            if done.wait(threading.TIMEOUT_MAX):
                return
            remaining = max(0.0, deadline - time.monotonic())
        if not done.wait(remaining):
            raise TimeoutError("ObjectRef release receipt did not complete before close timeout")

    def __hash__(self) -> int:
        return hash((self._owner_worker_id, self._object_id))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ObjectRef)
            and self._owner_worker_id == other._owner_worker_id
            and self._object_id == other._object_id
        )

    def __reduce__(self) -> tuple[object, tuple[object, ...]]:
        """Serialize only the logical handle, never a CoreWorker runtime."""

        if self._closed:
            raise RuntimeError("cannot serialize a closed ObjectRef")
        exporter = current_exporter()
        if exporter is not None:
            object_id, owner_worker_id, owner_address, hold = exporter(
                self
            )
            return restore_exported_reference, (
                object_id, owner_worker_id, owner_address, hold
            )
        return type(self), (self._object_id, self._owner_worker_id)

    def __repr__(self) -> str:
        suffix = ", closed=True" if self._closed else ""
        if self._borrower_token is not None:
            suffix += ", borrowed=True"
        return "ObjectRef({}{})".format(self._object_id, suffix)


@dataclass(frozen=True)
class ActorEndpoint:
    actor_id: ActorID
    generation: ActorGeneration
    node_id: NodeID
    worker_id: WorkerID
    worker_address: Address
    method_names: tuple[str, ...]
    route_epoch: int = 0
    worker_pid: Optional[int] = None


@dataclass(frozen=True)
class _ActorInflightCall:
    """Owner-local publication fence for one physical Actor call.

    The logical ActorID belongs to the public handle, while these fields bind
    a call to one GCS-published route.  A later route snapshot may fail the
    call, but must never replay it against a new in-memory Actor incarnation.
    """

    actor_id: ActorID
    object_id: ObjectID
    attempt_id: AttemptID
    fence: ActorCallFence


@dataclass(frozen=True)
class RemoteFunctionDefinition:
    """The immutable, cloudpickled definition exported to a worker."""

    key: protocol.FunctionKey
    definition: protocol.FunctionDefinition

    @classmethod
    def from_callable(
        cls, function: object, job_id: JobID
    ) -> "RemoteFunctionDefinition":
        payload = cloudpickle.dumps(function)
        version = hashlib.sha256(payload).hexdigest()
        key = protocol.FunctionKey(
            job_id=job_id,
            module=getattr(function, "__module__", "__main__"),
            qualname=getattr(
                function, "__qualname__", getattr(function, "__name__", "remote")
            ),
            version=version,
        )
        return cls(key, protocol.FunctionDefinition.from_payload(key, payload))


@dataclass(frozen=True)
class _PendingTask:
    object_id: ObjectID
    spec: protocol.TaskSpec
    protected_dependencies: tuple[ObjectID, ...] = ()
    dependency_hold: protocol.TaskReferenceHold | None = None
    capacity_round: int = 0
    foreign_dependency_guards: tuple[_ForeignDependencyGuard, ...] = ()
    # Lifetime holds for nested handles are deliberately separate from
    # top-level execution dependencies.  They keep objects alive across queueing
    # and SYSTEM retries, but never enter readiness gating or Node pull.
    nested_local_holds: tuple[ObjectID, ...] = ()
    nested_foreign_guards: tuple[_ForeignDependencyGuard, ...] = ()
    execution: TaskExecutionKey = field(init=False)

    def __post_init__(self) -> None:
        execution = TaskExecutionKey.from_task_spec(self.spec)
        if self.object_id != execution.output_ids[0]:
            raise ValueError("pending object_id must be the task output")
        object.__setattr__(self, "execution", execution)

    @property
    def task_id(self) -> TaskID:
        return self.execution.task_id

    @property
    def output_ids(self) -> tuple[ObjectID, ...]:
        return self.execution.output_ids

    @property
    def full_output_ids(self) -> tuple[ObjectID, ...]:
        return self.execution.output_ids

    @property
    def task_key(self) -> object:
        """One logical task, including all of its physical attempts."""
        return self.task_id


class _DispatchKind(str, Enum):
    """Which existing authority owns a lane turn, not a new task state."""

    FRESH = "FRESH"
    LEASE = "LEASE"
    CANCEL = "CANCEL"
    PUSH = "PUSH"
    CUSTODY = "CUSTODY"
    OUTPUT_ADOPTION = "OUTPUT_ADOPTION"
    OUTPUT_NODE_LOSS = "OUTPUT_NODE_LOSS"
    SYSTEM_FAILURE = "SYSTEM_FAILURE"


@dataclass(frozen=True)
class _ReadyTask:
    """One fresh admission or one retained protocol continuation.

    The existing payload fields form a validated sum: at most one is present.
    Lease ambiguity belongs to that lease's replay, not a separate lane kind.
    Output continuations may outlive PENDING (even be READY/LOST/ERROR), so this
    envelope neither inspects owner state nor grants new execution permission.
    """

    pending: _PendingTask
    spec: protocol.TaskSpec
    dependencies: tuple[protocol.ObjectStoreDescriptor, ...] = ()
    lease_state: "_LeaseRequestState | None" = None
    ambiguity_round: int = 0
    cancellation: "_LeaseCancellationState | None" = None
    push_state: "_PushRequestState | None" = None
    location_state: "_LocationReportState | None" = None
    output_adoption: "_OutputAdoptionObligation | None" = None
    output_node_loss: "_OutputNodeLossObligation | None" = None
    system_failure: "_DeferredSystemFailure | None" = None
    kind: _DispatchKind = field(init=False)

    def __post_init__(self) -> None:
        present = [kind for kind, payload in (
            (_DispatchKind.LEASE, self.lease_state),
            (_DispatchKind.CANCEL, self.cancellation),
            (_DispatchKind.PUSH, self.push_state),
            (_DispatchKind.CUSTODY, self.location_state),
            (_DispatchKind.OUTPUT_ADOPTION, self.output_adoption),
            (_DispatchKind.OUTPUT_NODE_LOSS, self.output_node_loss),
            (_DispatchKind.SYSTEM_FAILURE, self.system_failure),
        ) if payload is not None]
        if len(present) > 1:
            raise ValueError("a dispatch turn must have only one continuation")
        if type(self.ambiguity_round) is not int or self.ambiguity_round < 0:
            raise ValueError("lease ambiguity round must be a non-negative integer")
        kind = present[0] if present else _DispatchKind.FRESH
        if self.ambiguity_round and kind is not _DispatchKind.LEASE:
            raise ValueError("lease ambiguity requires the original lease continuation")
        object.__setattr__(self, "kind", kind)


@dataclass(frozen=True)
class _DeferredSystemFailure:
    """An already-known terminal error waiting only for physical cleanup."""

    error: BaseException
    round: int = 0


@dataclass(frozen=True)
class _OutputAdoptionObligation:
    envelope: OutputPublicationEnvelope
    node_id: NodeID
    round: int = 0


@dataclass(frozen=True)
class _OutputNodeLossObligation:
    publication_id: OutputPublicationID
    node_death: protocol.NodeDeathRecord
    envelope: OutputPublicationEnvelope | None = None
    round: int = 0


@dataclass(frozen=True)
class _LeaseRequestState:
    """One idempotent lease RPC hop, retained across ambiguous replies."""

    request: protocol.RequestWorkerLease
    address: Address
    expected_node_id: NodeID
    allow_spillback: bool


@dataclass(frozen=True)
class _LeaseCancellationState:
    """A cancellation that must resolve before owner state may terminate."""

    request: protocol.CancelWorkerLease
    address: Address
    terminal_error: BaseException
    round: int = 0
    target_node_id: NodeID | None = None
    lease_request: protocol.RequestWorkerLease | None = None
    known_grant: protocol.GrantWorkerLease | None = None
    reply: protocol.CancelWorkerLeaseReply | None = None


@dataclass(frozen=True)
class _PushRequestState:
    """Exact granted PushTask retained after an ambiguous Worker RPC."""

    push: protocol.PushTask
    grant: protocol.GrantWorkerLease
    granting_node_address: Address
    worker_address: Address
    round: int = 0
    ambiguous: bool = False
    orphan_cleanup: "_OrphanCleanupState | None" = None
    lease_request: protocol.RequestWorkerLease | None = None


@dataclass(frozen=True)
class _OrphanCleanupState:
    """Exact replica deletion that gates a terminal lease outcome."""

    drops: tuple[protocol.DropObjectReplica, ...]
    acknowledged: tuple[protocol.DropObjectReplica, ...]
    lease_state: protocol.LeaseExecutionState
    completion_status: protocol.TaskReplyStatus | None
    round: int = 0


@dataclass(frozen=True)
class _ProtocolUnresolved:
    """One logical result whose remote protocol has not converged yet."""

    phase: str
    pending: _PendingTask
    obligation: object | None = None
    target_node_id: NodeID | None = None
    output_candidate: OutputPublicationID | None = None


@dataclass(frozen=True)
class _DelayedReadyTask:
    ready: _ReadyTask
    due_at: float


class _LeaseRequestAmbiguous(TransportError):
    """A lease hop may have committed, so only its exact ID may replay."""

    def __init__(self, state: _LeaseRequestState, cause: BaseException) -> None:
        self.state = state
        self.cause = cause
        super().__init__("worker lease outcome remained ambiguous after replay")


class _NodeDeathHandled(RuntimeError):
    """Internal lane control-flow after a committed death was consumed."""

    def __init__(self, terminal: bool) -> None:
        self.terminal = terminal
        super().__init__("committed Node death was handled by the dispatch lane")


class _TaskFinishing(RuntimeError):
    """A terminal owner cleanup fenced a new remote protocol send."""


@dataclass
class _ObjectWaiter:
    """Notification only; bytes and errors live exclusively in the owner table."""

    event: threading.Event


def _make_message(message_type: type, **values: object) -> object:
    """Construct a protocol message across the small v0.1 naming transition.

    Protocol dataclasses are the real boundary.  The aliases here only let the
    Driver land independently while ``spec``/``task_spec`` field names settle.
    """

    names = {field.name for field in fields(message_type)}
    return message_type(**{name: value for name, value in values.items() if name in names})


def _remote_error(reply: object) -> BaseException:
    error = getattr(reply, "error", None)
    remote_type = getattr(
        error, "type_name", getattr(error, "exception_type", "RemoteError")
    )
    message = getattr(error, "message", "remote task failed")
    traceback_text = getattr(
        error, "traceback", getattr(error, "traceback_text", "")
    )
    status = getattr(reply, "status", None)
    application_error = status is protocol.TaskReplyStatus.APPLICATION_ERROR or (
        status is None and getattr(reply, "ok", None) is False
    )
    error_type = TaskError if application_error else SystemTaskError
    exception = error_type("{}: {}\n{}".format(remote_type, message, traceback_text))
    exception.remote_type = remote_type  # type: ignore[attr-defined]
    exception.remote_message = message  # type: ignore[attr-defined]
    exception.remote_traceback = traceback_text  # type: ignore[attr-defined]
    return exception


class CoreWorker:
    """The Driver's task submitter and owner of inline task results."""

    def __init__(
        self,
        node_address: Address,
        node_id: NodeID,
        *,
        job_id: Optional[JobID] = None,
        worker_id: Optional[WorkerID] = None,
        event_sink: Optional[EventSink] = None,
        gcs_address: Optional[Address] = None,
        inline_threshold: int = _DEFAULT_INLINE_THRESHOLD_BYTES,
        dispatch_lanes: int = _DEFAULT_DISPATCH_LANES,
        capacity_retry_rounds: Optional[int] = None,
        owner_address: Optional[Address] = None,
        restartable_actor_owner: bool = False,
        installed_cluster_snapshot: Optional[
            protocol.InstallClusterSnapshot
        ] = None,
        poll_node_deaths: bool = False,
    ) -> None:
        # Construction starts several local runtime threads.  Keep an
        # attempted-start ledger before the first fallible initializer so a
        # partial constructor can stop only threads which may exist, without
        # calling the distributed/semantic shutdown path.
        self._startup_thread_attempts: list[threading.Thread] = []
        self._startup_abort_lock = threading.Lock()
        self._startup_abort_complete = False
        self._startup_threads_gate = threading.Event()
        self._startup_threads_committed = False
        # Ownership transfers at call entry, before validation or any other
        # fallible constructor work.  This is crucial for API-created remote
        # sinks, whose sender thread already exists before Core construction.
        self.event_sink = event_sink if event_sink is not None else EventSink()
        try:
            self._initialize(
                node_address,
                node_id,
                job_id=job_id,
                worker_id=worker_id,
                event_sink=event_sink,
                gcs_address=gcs_address,
                inline_threshold=inline_threshold,
                dispatch_lanes=dispatch_lanes,
                capacity_retry_rounds=capacity_retry_rounds,
                owner_address=owner_address,
                restartable_actor_owner=restartable_actor_owner,
                installed_cluster_snapshot=installed_cluster_snapshot,
                poll_node_deaths=poll_node_deaths,
            )
        except BaseException:
            try:
                self._abort_unpublished_startup()
            except Exception:
                # Constructor rollback is deliberately best effort at this
                # innermost boundary; always preserve the causal exception.
                pass
            raise

    def _initialize(
        self,
        node_address: Address,
        node_id: NodeID,
        *,
        job_id: Optional[JobID] = None,
        worker_id: Optional[WorkerID] = None,
        event_sink: Optional[EventSink] = None,
        gcs_address: Optional[Address] = None,
        inline_threshold: int = _DEFAULT_INLINE_THRESHOLD_BYTES,
        dispatch_lanes: int = _DEFAULT_DISPATCH_LANES,
        capacity_retry_rounds: Optional[int] = None,
        owner_address: Optional[Address] = None,
        restartable_actor_owner: bool = False,
        installed_cluster_snapshot: Optional[
            protocol.InstallClusterSnapshot
        ] = None,
        poll_node_deaths: bool = False,
    ) -> None:
        if (
            isinstance(inline_threshold, bool)
            or not isinstance(inline_threshold, int)
            or inline_threshold < 0
        ):
            raise ValueError("inline_threshold must be a non-negative integer")
        if (
            isinstance(dispatch_lanes, bool)
            or not isinstance(dispatch_lanes, int)
            or not 1 <= dispatch_lanes <= _MAX_DISPATCH_LANES
        ):
            raise ValueError("dispatch_lanes must be an integer between 1 and 2")
        if capacity_retry_rounds is not None and (
            isinstance(capacity_retry_rounds, bool)
            or not isinstance(capacity_retry_rounds, int)
            or capacity_retry_rounds < 0
        ):
            raise ValueError(
                "capacity_retry_rounds must be a non-negative integer or None"
            )
        if type(poll_node_deaths) is not bool:
            raise TypeError("poll_node_deaths must be bool")
        self._poll_node_deaths = poll_node_deaths
        self._node_death_view_sync_lock = threading.Lock()
        self._applied_node_death_view = None
        self._node_death_next_poll_at = time.monotonic()
        self.node_address = node_address
        self.node_id = node_id
        initial_home = _HomeRoute(node_id, node_address, 0)
        if installed_cluster_snapshot is not None:
            installed_cluster_snapshot = self._validated_cluster_snapshot(
                installed_cluster_snapshot
            )
            local_info = next((
                info for info in installed_cluster_snapshot.nodes
                if info.node_id == node_id
            ), None)
            if local_info is None or local_info.address != node_address:
                raise ValueError(
                    "initial cluster snapshot does not contain the home Node route"
                )
            initial_home = _HomeRoute(
                node_id, node_address,
                installed_cluster_snapshot.membership_epoch,
            )
        self._home_route: _HomeRoute | None = initial_home
        self._installed_cluster_snapshot = installed_cluster_snapshot
        # Embedded Cores need not have the Driver's bootstrap snapshot. Cache
        # only successful optional locality lookups, never membership/death or
        # object ownership. Installed snapshots and death fences always win.
        self._lease_locality_addresses: dict[NodeID, Address] = {}
        self.gcs_address = gcs_address
        self.job_id = job_id or JobID.random()
        self.worker_id = worker_id or WorkerID.random()
        self.owner_address = owner_address
        self._restartable_actor_owner = bool(restartable_actor_owner)
        self.driver_task_id = TaskID.for_driver(self.job_id)
        self.inline_threshold = inline_threshold
        self._dispatch_lane_count = dispatch_lanes
        self._capacity_retry_rounds = capacity_retry_rounds

        self._submission_index = 0
        self._put_index = 0
        self._owner_table = ObjectOwnerTable()
        self._recovery = RecoveryManager()
        self._reconstruction = ReconstructionCoordinator(
            self._recovery, self._owner_table
        )
        self._owned_reconstruction = OwnedObjectReconstructionReducer(
            self.worker_id,
            self._owner_table,
            self._recovery,
            self._admit_owned_object_reconstruction,
            snapshot_reconstruction=self._owned_reconstruction_snapshot,
        )
        self._foreign_lineage_registry = ForeignLineageRegistry()
        self._foreign_lineage_runtime = ForeignLineageRuntime(
            self._foreign_lineage_registry,
            replace_retained=self._replace_foreign_lineage_hold_rpc,
            get_retained=self._get_foreign_lineage_object_rpc,
            request_reconstruction=(
                self._request_foreign_lineage_reconstruction_rpc
            ),
            release_retained=self._release_foreign_lineage_hold_rpc,
            owner_death_lookup=self._owner_table.dead_worker_record,
        )
        self._owned_drop_replies: dict[
            str, protocol.RequestDropOwnedObjectReply
        ] = {}
        self._owned_drop_claims: dict[
            str, protocol.RequestDropOwnedObject
        ] = {}
        self._owned_drop_lock = threading.Lock()
        self._objects: Dict[ObjectID, _ObjectWaiter] = {}
        self._stored_descriptors: Dict[ObjectID, protocol.ResultDescriptor] = {}
        # Function definitions live in workers, not in the driver.  A future
        # worker pool may grant a different executor for the next task, so an
        # export is cached against both the physical worker and function key.
        self._registered_functions: set[
            tuple[WorkerID, protocol.FunctionKey]
        ] = set()
        # Membership is installed by the Driver only after GCS has committed a
        # typed NodeDeathRecord and every surviving Node acknowledged the same
        # membership epoch.  It fences data-plane facts; it is not a liveness
        # detector and never infers death from an RPC timeout.
        self._dead_nodes: dict[NodeID, protocol.NodeDeathRecord] = {}
        self._membership_epoch = initial_home.membership_epoch
        # Every Core is an independent consumer of GCS's append-only ordinary
        # Worker death journal.  The cursor advances only after the matching
        # owner-table fence commits; transport reachability is never evidence
        # that a Worker died.
        self._worker_death_cursor = 0
        self._worker_death_sync_lock = threading.Lock()
        self._worker_death_next_poll_at = (
            time.monotonic() + _WORKER_DEATH_POLL_SECONDS
        )
        self._node_death_attempts: dict[
            object, tuple[protocol.NodeDeathRecord, _PendingTask]
        ] = {}
        self._placement_group_states: dict[
            tuple[PlacementGroupID, int], protocol.PlacementGroupPhaseStatus
        ] = {}
        # State alone cannot tell whether the death of another bundle's Node
        # invalidates this bundle.  Retain the complete immutable GCS manifest
        # so one committed NodeDeathRecord can fence every old handle and task
        # in the affected group, including tasks assigned to surviving Nodes.
        self._placement_group_manifests: dict[
            tuple[PlacementGroupID, int],
            tuple[protocol.PlacementGroupSchedulingKey, ...],
        ] = {}
        self._state_lock = threading.RLock()
        self._completion = threading.Condition(self._state_lock)
        self._initialize_reference_events()
        self._actor_call_threads: set[threading.Thread] = set()
        self._inflight_puts = 0
        self._inflight_borrow_ops = 0
        self._inflight_pg_control_ops = 0
        self._attempt_borrow_releases: dict[
            tuple[WorkerID, ObjectID, AttemptID], _AttemptBorrowRelease
        ] = {}
        self._borrowed_release_obligations: dict[
            _BorrowReleaseKey, _BorrowReleaseObligation
        ] = {}
        self._inflight_submissions = 0
        self._object_gc_obligations: dict[ObjectID, _ObjectGcObligation] = {}
        # Compatibility alias for the earlier inline-only teaching slice.
        self._inline_gc_obligations = self._object_gc_obligations
        self._late_replica_cleanup = ReplicaCleanupQueue()
        self._late_replica_cleanup_scheduled = False
        self._foreign_lineage_collection_receipts: dict[
            ObjectID, ForeignLineageCollectionReceipt
        ] = {}
        self._foreign_lineage_prepared_collection_receipts: dict[
            ObjectID, ForeignLineagePreparedCollectionReceipt
        ] = {}
        self._foreign_lineage_collection_retry_scheduled: set[TaskID] = set()
        self._gc_retry_timers: set[threading.Timer] = set()
        self._gc_retry_timers_open = True
        self._owner_protocol_open = True
        self._owner_retain_admission_open = True
        # GCS is the only Actor-lifecycle authority.  This table is merely the
        # Driver owner's compare-and-install route cache: stable calls bypass
        # GCS, while RESTARTING/DEAD snapshots atomically revoke the old route.
        self._actor_clients = ActorClientTable()
        self._actor_control_ops = 0
        # Submission admission, dependency readiness, and execution are three
        # distinct stages.  The coordinator alone owns the first two queues;
        # lanes consume only dependency-ready work and may therefore block in a
        # lease/PushTask RPC without causing head-of-line dependency stalls.
        self._submissions = queue.Queue()  # type: queue.Queue[object]
        self._ready_tasks = queue.Queue()  # type: queue.Queue[object]
        self._blocked_tasks: dict[object, _PendingTask] = {}
        self._delayed_ready = queue.PriorityQueue()  # type: queue.PriorityQueue[tuple[float, int, _DelayedReadyTask]]
        self._capacity_sequence = 0
        self._accepted_task_count = 0
        # Every admitted logical execution keeps reconstruction and last-ref
        # GC behind its dependency/count finalizer.  READY reads remain legal.
        self._task_finish_barriers: dict[ObjectID, _PendingTask] = {}
        self._protocol_unresolved: dict[object, _ProtocolUnresolved] = {}
        self._foreign_guard_release_retries: dict[
            object, _ForeignGuardReleaseRetry
        ] = {}
        self._orphan_foreign_guard_releases: dict[
            tuple[WorkerID, ObjectID, protocol.TaskReferenceHold],
            _ForeignDependencyGuard,
        ] = {}
        self._active_task_finishes: set[object] = set()
        self._finishing_tasks: set[object] = set()
        self._finished_tasks: set[object] = set()
        self._accepting = True
        self._sink_closed = False
        self._coordinator = threading.Thread(
            target=self._coordinator_startup_entry,
            name="miniray-core-worker-coordinator",
            daemon=True,
        )
        self._dispatchers = tuple(
            threading.Thread(
                target=self._dispatcher_startup_entry,
                name="miniray-core-worker-dispatch-{}".format(index),
                daemon=True,
            )
            for index in range(dispatch_lanes)
        )
        # Keep the original attribute as the first lane for narrow teaching
        # fixtures and diagnostics written before dispatch became concurrent.
        self._dispatcher = self._dispatchers[0]
        for dispatcher in self._dispatchers:
            self._start_startup_thread(dispatcher)
        self._start_startup_thread(self._coordinator)
        self._startup_threads_committed = True
        self._startup_threads_gate.set()

    def _start_startup_thread(self, thread: threading.Thread) -> None:
        """Record ownership before attempting to start a runtime thread."""

        self._startup_thread_attempts.append(thread)
        thread.start()

    def _coordinator_startup_entry(self) -> None:
        self._startup_threads_gate.wait()
        if not self._startup_threads_committed:
            return
        self._coordinator_loop()

    def _dispatcher_startup_entry(self) -> None:
        self._startup_threads_gate.wait()
        if not self._startup_threads_committed:
            return
        self._dispatch_loop()

    def _abort_unpublished_startup(self, timeout: float = 1.0) -> bool:
        """Locally stop a Core which was never published to callers.

        This is intentionally not :meth:`shutdown`: no task can have been
        admitted before ``init`` publishes the Core, so rollback must not query
        GCS or converge distributed ownership protocols.  It only fences local
        admission, releases threads which may have started, and closes the
        externally supplied trace sink.  The operation is idempotent so the
        constructor and Driver startup rollback may both invoke it.
        """

        if timeout <= 0:
            raise ValueError("startup abort timeout must be positive")
        abort_lock = getattr(self, "_startup_abort_lock", None)
        if abort_lock is None:
            return True
        with abort_lock:
            if getattr(self, "_startup_abort_complete", False):
                return True
            deadline = time.monotonic() + timeout
            self._startup_threads_committed = False
            gate = getattr(self, "_startup_threads_gate", None)
            if gate is not None:
                gate.set()
            state_lock = getattr(self, "_state_lock", None)
            if state_lock is not None:
                with state_lock:
                    self._accepting = False
                    self._owner_retain_admission_open = False
                    self._gc_retry_timers_open = False
                    foreign_runtime = getattr(
                        self, "_foreign_lineage_runtime", None
                    )
                    if foreign_runtime is not None:
                        foreign_runtime.close_admission()
            submissions = getattr(self, "_submissions", None)
            if submissions is not None:
                submissions.put(_STOP)
            ready_tasks = getattr(self, "_ready_tasks", None)
            if ready_tasks is not None:
                for _thread in getattr(self, "_dispatchers", ()):
                    ready_tasks.put(_STOP)

            attempts = tuple(getattr(self, "_startup_thread_attempts", ()))
            current = threading.current_thread()
            for thread in attempts:
                if thread is current:
                    continue
                # ``ident`` is set only after CPython has started the thread.
                # A custom Thread may report liveness without it, so retain the
                # conservative is_alive fallback while tolerating NEW objects.
                try:
                    started = thread.ident is not None or thread.is_alive()
                except Exception:
                    started = False
                if not started:
                    continue
                try:
                    thread.join(max(0.0, deadline - time.monotonic()))
                except RuntimeError:
                    pass

            mailbox = getattr(self, "_reference_mailbox", None)
            if mailbox is not None:
                try:
                    mailbox.stop()
                except Exception:
                    pass
            reference_thread = getattr(self, "_reference_thread", None)
            if reference_thread is not None and reference_thread is not current:
                try:
                    if (
                        reference_thread.ident is not None
                        or reference_thread.is_alive()
                    ):
                        reference_thread.join(
                            max(0.0, deadline - time.monotonic())
                        )
                except RuntimeError:
                    pass
            finalizer = getattr(self, "_reference_runtime_finalizer", None)
            if finalizer is not None:
                finalizer.detach()

            sink = getattr(self, "event_sink", None)
            if sink is not None and not getattr(self, "_sink_closed", False):
                self._sink_closed = True
                try:
                    sink.close()
                except Exception:
                    pass

            alive = False
            for thread in attempts + ((reference_thread,) if reference_thread else ()):
                if thread is current:
                    continue
                try:
                    alive = alive or thread.is_alive()
                except Exception:
                    alive = True
            self._startup_abort_complete = not alive
            return not alive

    def define_remote_function(self, function: object) -> RemoteFunctionDefinition:
        return RemoteFunctionDefinition.from_callable(function, self.job_id)

    @staticmethod
    def _validated_cluster_snapshot(
        snapshot: object,
    ) -> protocol.InstallClusterSnapshot:
        """Re-run every wire validator before using a membership proof."""

        if not isinstance(snapshot, protocol.InstallClusterSnapshot):
            raise TypeError("snapshot must be an InstallClusterSnapshot")
        nodes = tuple(replace(node) for node in snapshot.nodes)
        return protocol.InstallClusterSnapshot(
            snapshot.membership_epoch, snapshot.snapshot_id, nodes
        )

    def _home_route_snapshot(self) -> _HomeRoute | None:
        """Read one non-torn physical home route.

        The lazy branch preserves small ``object.__new__`` teaching fixtures; a
        real Core always installs ``_home_route`` in ``__init__``.
        """

        lock = getattr(self, "_state_lock", None)
        with (lock if lock is not None else nullcontext()):
            if not hasattr(self, "_home_route"):
                if not hasattr(self, "node_address"):
                    # A few transport-only object.__new__ fixtures carry only
                    # the requester NodeID.  Real Cores always install both the
                    # address and an explicit route (including explicit None).
                    return None
                self._home_route = _HomeRoute(
                    self.node_id, self.node_address,
                    getattr(self, "_membership_epoch", 0),
                )
            return self._home_route

    def _require_home_route(self, operation: str) -> _HomeRoute:
        """Return the current physical route or a typed no-survivor error."""

        route = self._home_route_snapshot()
        if route is None:
            raise NodeDiedError(
                "{} requires a live Node, but the installed cluster view is empty"
                .format(operation)
            )
        return route

    def _first_lease_route(
        self, dependencies: tuple[protocol.ObjectStoreDescriptor, ...], *,
        home_route: _HomeRoute,
    ) -> tuple[NodeID, Address]:
        """Choose a metadata-only locality hint for a *new* ordinary lease.

        This does not place the Task or rewrite its input sources. The selected
        Node still runs Hybrid scheduling and may spill back, including to the
        requester. Once a request is sent, its existing replay state owns the
        frozen route; cancellation, capacity and Push retries never come here.
        """
        fallback = home_route.node_id, home_route.address
        if not dependencies:
            return fallback
        with self._state_lock:
            snapshot = getattr(self, "_installed_cluster_snapshot", None)
            addresses = (None if snapshot is None else
                         {info.node_id: info.address for info in snapshot.nodes})
            dead = getattr(self, "_dead_nodes", {})
            hints = []
            for descriptor in dependencies:
                locations = (descriptor.node_id,)
                if descriptor.owner_worker_id == self.worker_id:
                    try:
                        owner = self._owner_table.snapshot(descriptor.object_id)
                    except UnknownObjectError:
                        continue
                    canonical = owner.canonical_stored_result
                    if (owner.state is not ObjectState.READY_STORED
                            or owner.current_attempt != descriptor.producer_attempt_id
                            or owner.collection_pending or owner.collection_plan is not None
                            or owner.output_retirement_id is not None
                            or owner.inline_data is not None or owner.error is not None
                            or not isinstance(canonical, protocol.ResultDescriptor)
                            or canonical.storage is not protocol.ResultStorage.OBJECT_STORE
                            or canonical.inline_data is not None
                            or (canonical.object_id, canonical.owner_worker_id,
                                canonical.size_bytes, canonical.checksum) != (
                                descriptor.object_id, descriptor.owner_worker_id,
                                descriptor.size_bytes, descriptor.checksum)):
                        # Never combine an old descriptor with successor-epoch
                        # locations. Missing hints do not invent a Task error.
                        continue
                    # Canonical node_id is the original publisher, not a live
                    # routing requirement: a surviving secondary is useful too.
                    locations = owner.locations
                # Foreign owners already supplied one retained descriptor. Do
                # not add an RPC or copy their ownership table just for scoring.
                hints.append(ObjectLocality(
                    descriptor.object_id, descriptor.size_bytes,
                    tuple(node_id for node_id in locations
                          if isinstance(node_id, NodeID) and node_id not in dead
                          and (addresses is None or node_id in addresses)),
                ))
            selected = preferred_lease_node(hints, fallback_node_id=home_route.node_id)
            if selected is None or selected == home_route.node_id:
                return fallback
            if addresses is not None:
                return selected, addresses[selected]
            cached = getattr(self, "_lease_locality_addresses", {}).get(selected)
            if cached is not None:
                return selected, cached

        # A Worker without a full snapshot can use the existing address service
        # on a cold route miss; success is cached. Failure is only a missed hint,
        # not Node death or a dependency failure. No lease has been sent yet.
        deadline = time.monotonic() + 0.75
        parent_deadline = _RPC_CALL_DEADLINE.get()
        if parent_deadline is not None:
            deadline = min(deadline, parent_deadline)
        token = _RPC_CALL_DEADLINE.set(deadline)
        try:
            address = self._resolve_node_address(selected)
            # Pickle need not run the reply's __post_init__. An invalid cold
            # lookup must remain a missed hint, not poison later cached hops.
            address = protocol.GetNodeAddressReply(selected, True, address).address
        except Exception:
            return fallback
        finally:
            _RPC_CALL_DEADLINE.reset(token)
        with self._state_lock:
            # An authoritative death/snapshot may have arrived during lookup.
            # Optional cached addresses can never revive an excluded Node.
            if self._node_is_dead(selected):
                return fallback
            snapshot = getattr(self, "_installed_cluster_snapshot", None)
            if snapshot is not None:
                current = next((info for info in snapshot.nodes if info.node_id == selected), None)
                return fallback if current is None else (selected, current.address)
            cache = getattr(self, "_lease_locality_addresses", None)
            if cache is None:
                cache = self._lease_locality_addresses = {}
            cache[selected] = address
            return selected, address

    def _requester_route_or_legacy_id(
        self, operation: str
    ) -> tuple[_HomeRoute | None, NodeID]:
        """Return a real route, with a narrow ID-only fixture fallback."""

        route = self._home_route_snapshot()
        if route is not None:
            return route, route.node_id
        if not hasattr(self, "_home_route") and hasattr(self, "node_id"):
            return None, self.node_id
        raise NodeDiedError(
            "{} requires a live requester Node".format(operation)
        )

    def _preflight_home_snapshot_locked(
        self,
        snapshot: protocol.InstallClusterSnapshot,
        death: protocol.NodeDeathRecord,
    ) -> _HomeRoute | None:
        """Validate a survivor snapshot and compute its route without mutation."""

        current_epoch = getattr(self, "_membership_epoch", 0)
        if snapshot.membership_epoch < death.death_epoch:
            raise ValueError(
                "cluster snapshot does not include the committed death epoch"
            )
        if snapshot.membership_epoch < current_epoch:
            raise ValueError("node-death snapshot has a stale membership epoch")
        if any(node.node_id == death.node_id for node in snapshot.nodes):
            raise ValueError("survivor snapshot still contains the dead Node")
        dead_nodes = getattr(self, "_dead_nodes", {})
        if any(node.node_id in dead_nodes for node in snapshot.nodes):
            raise ValueError("survivor snapshot resurrects a committed DEAD Node")

        previous = getattr(self, "_installed_cluster_snapshot", None)
        if previous is not None:
            if snapshot.membership_epoch < previous.membership_epoch:
                raise ValueError("node-death snapshot is older than the installed view")
            if snapshot.membership_epoch == previous.membership_epoch:
                if snapshot != previous:
                    raise ValueError(
                        "membership epoch was reused for different snapshot contents"
                    )
            else:
                previous_by_id = {node.node_id: node for node in previous.nodes}
                if not {node.node_id for node in snapshot.nodes}.issubset(
                    previous_by_id
                ):
                    raise ValueError(
                        "node-death snapshot added a new Node incarnation"
                    )
                for node in snapshot.nodes:
                    old = previous_by_id.get(node.node_id)
                    if old is None:
                        continue
                    if (
                        node.node_pid, node.registration_epoch, node.address,
                        node.total_resources, node.state,
                    ) != (
                        old.node_pid, old.registration_epoch, old.address,
                        old.total_resources, old.state,
                    ):
                        raise ValueError(
                            "survivor snapshot changed a live Node incarnation"
                        )

        current_home = getattr(self, "_home_route", None)
        if current_home is None and not hasattr(self, "_home_route"):
            current_home = _HomeRoute(
                self.node_id, self.node_address, current_epoch
            )
        return _select_home_route(current_home, snapshot)

    def handle_node_death(
        self,
        death: protocol.NodeDeathRecord,
        installed_snapshot: protocol.InstallClusterSnapshot | int,
        snapshot_installed: Optional[bool] = None,
    ) -> NodeLocationRemoval:
        """Install one committed Node death and enqueue Core-side cleanup.

        The caller must prove both sides of the control-plane barrier: GCS has
        committed ``death`` and all surviving NodeManagers installed a snapshot
        at least as new as ``death.death_epoch``.  This method installs the dead
        fence and owner-location truth synchronously, then delegates unresolved
        task classification to the Core coordinator.  It never advances an
        AttemptID in the observer thread.
        """

        if not isinstance(death, protocol.NodeDeathRecord):
            raise TypeError("death must be a NodeDeathRecord")
        # Pickle can instantiate a dataclass without running ``__post_init__``.
        # Rebuild the proof before touching either membership or owner state.
        death = replace(death)
        snapshot: protocol.InstallClusterSnapshot | None
        if isinstance(installed_snapshot, protocol.InstallClusterSnapshot):
            if snapshot_installed not in (None, True):
                raise ValueError(
                    "a complete installed snapshot cannot be marked uninstalled"
                )
            snapshot = self._validated_cluster_snapshot(installed_snapshot)
            membership_epoch = snapshot.membership_epoch
        else:
            # Temporary compatibility for narrow reducer fixtures.  Runtime
            # orchestration must pass the complete installed survivor view.
            membership_epoch = installed_snapshot
            snapshot = None
            if (
                isinstance(membership_epoch, bool)
                or not isinstance(membership_epoch, int)
                or membership_epoch < death.death_epoch
            ):
                raise ValueError(
                    "membership_epoch must include the committed death epoch"
                )
            if snapshot_installed is not True:
                raise ValueError(
                    "surviving NodeManagers must install the membership snapshot first"
                )

        with self._state_lock:
            current_epoch = getattr(self, "_membership_epoch", 0)
            dead_nodes = getattr(self, "_dead_nodes", None)
            if dead_nodes is None:
                dead_nodes = {}
                self._dead_nodes = dead_nodes
            previous = dead_nodes.get(death.node_id)
            if previous is not None:
                if previous != death:
                    raise ValueError(
                        "NodeID already has a different committed death record"
                    )
                if membership_epoch < current_epoch:
                    raise ValueError("node-death replay has a stale membership epoch")
                next_home = (
                    None
                    if snapshot is None
                    else self._preflight_home_snapshot_locked(snapshot, death)
                )
                self._membership_epoch = max(current_epoch, membership_epoch)
                if snapshot is not None:
                    self._home_route = next_home
                    self._installed_cluster_snapshot = snapshot
                newly_lost = self._mark_placement_groups_lost_locked(death)
                if newly_lost:
                    self._submissions.put(_WAKE_COORDINATOR)
                    self._completion.notify_all()
                pending_removals = getattr(self, "_node_death_removals", {})
                if death.node_id not in pending_removals:
                    return NodeLocationRemoval(death.node_id)
            if membership_epoch < current_epoch:
                raise ValueError("node death has a stale membership epoch")
            next_home = (
                None
                if snapshot is None
                else self._preflight_home_snapshot_locked(snapshot, death)
            )

            pending_removals = getattr(self, "_node_death_removals", None)
            if pending_removals is None:
                pending_removals = self._node_death_removals = {}
            affected = pending_removals.setdefault(death.node_id, set())
            dead_nodes[death.node_id] = death
            self._membership_epoch = membership_epoch
            if snapshot is not None:
                self._home_route = next_home
                self._installed_cluster_snapshot = snapshot
            lost_placement_groups = self._mark_placement_groups_lost_locked(
                death
            )
            # Retain the affected identities before the owner reducer. If a
            # local callback or enqueue fails after its effect, replay must
            # still repair routes/waiters and enqueue classification.
            for object_id, descriptor in tuple(self._stored_descriptors.items()):
                if descriptor.node_id == death.node_id:
                    affected.add(object_id)
            removal = self._owner_table.remove_node_locations(death.node_id)
            affected.update(removal.lost + removal.surviving + removal.collecting)
            replica_cleanup = getattr(self, "_late_replica_cleanup", None)
            if replica_cleanup is not None and death.reason is protocol.NodeDeathReason.PROCESS_EXIT:
                for object_id in replica_cleanup.acknowledge_node_death(death):
                    self._enqueue_inline_gc_check(object_id)

            # A committed process death proves that bytes on this Node are gone.
            # Preserve the immutable collection plan but discharge its physical
            # drop operation without manufacturing a DropObjectReplica ACK.
            for obligation in self._gc_obligations().values():
                obligation.pending_drops.pop(death.node_id, None)

            for object_id in removal.surviving:
                descriptor = self._stored_descriptors.get(object_id)
                if descriptor is None or descriptor.node_id != death.node_id:
                    continue
                locations = self._owner_table.snapshot(object_id).locations
                if locations:
                    self._stored_descriptors[object_id] = replace(
                        descriptor, node_id=min(locations)
                    )
            for object_id in removal.lost + removal.collecting:
                self._stored_descriptors.pop(object_id, None)
                waiter = self._objects.get(object_id)
                if waiter is not None:
                    waiter.event.set()
            # Replaying after remove_node_locations applied can produce an
            # empty delta; current owner truth still repairs the saved routes.
            for object_id in affected:
                if not self._owner_table.contains(object_id):
                    self._stored_descriptors.pop(object_id, None)
                    continue
                current = self._owner_table.snapshot(object_id)
                descriptor = self._stored_descriptors.get(object_id)
                locations = tuple(node for node in current.locations if node not in dead_nodes)
                if current.state is ObjectState.READY_STORED and locations and descriptor is not None:
                    if descriptor.node_id not in locations:
                        self._stored_descriptors[object_id] = replace(descriptor, node_id=min(locations))
                elif current.state is ObjectState.LOST or current.collection_pending:
                    self._stored_descriptors.pop(object_id, None)
                    waiter = self._objects.get(object_id)
                    if waiter is not None:
                        waiter.event.set()
                    if current.collection_pending:
                        self._enqueue_inline_gc_check(object_id)

            self._submissions.put(
                _NodeDeathObserved(death, membership_epoch)
            )
            self._completion.notify_all()
            pending_removals.pop(death.node_id, None)

        self._emit(
            "node_death_observed", node_id=str(death.node_id),
            node_pid=death.node_pid, membership_epoch=membership_epoch,
            lost_objects=len(removal.lost),
            lost_placement_groups=len(lost_placement_groups),
            surviving_objects=len(removal.surviving),
        )
        for object_id in removal.collecting:
            self._enqueue_inline_gc_check(object_id)
        return removal

    def _mark_placement_groups_lost_locked(
        self, death: protocol.NodeDeathRecord
    ) -> tuple[tuple[PlacementGroupID, int], ...]:
        """Reduce one committed process death over cached full PG plans.

        ``_state_lock`` composes this transition with task publication.  A
        single bundle loss invalidates every scheduling capability from the
        immutable attempt, including keys that name surviving Nodes.  Clean
        shutdown deaths do not manufacture failure: GCS drains PG reservations
        before an EXPECTED Node unregister.
        """

        if death.reason is not protocol.NodeDeathReason.PROCESS_EXIT:
            return ()
        states = getattr(self, "_placement_group_states", None)
        manifests = getattr(self, "_placement_group_manifests", None)
        if not states or not manifests:
            return ()
        newly_lost: list[tuple[PlacementGroupID, int]] = []
        for identity, placements in tuple(manifests.items()):
            if not any(key.node_id == death.node_id for key in placements):
                continue
            if states.get(identity) is protocol.PlacementGroupPhaseStatus.CREATED:
                states[identity] = protocol.PlacementGroupPhaseStatus.LOST
                newly_lost.append(identity)
        return tuple(newly_lost)

    def _node_is_dead(self, node_id: NodeID) -> bool:
        return node_id in getattr(self, "_dead_nodes", {})

    def _node_death_view_rpc(self, request):
        """A short local metadata read, never a new failure detector."""
        deadline = time.monotonic() + 0.75
        return rpc_request(self.node_address, GET_NODE_DEATH_VIEW, request,
            connect_timeout=0.25, request_timeout=0.5, deadline=deadline)

    def _sync_node_deaths(self) -> bool:
        """Consume the local Node's Driver-certified cumulative barrier.

        None means no retained view, not proof that the cluster has no deaths.
        A missing ACK or malformed view never becomes a death fact. Full batch
        validation precedes mutations; partial local failure replays the same
        view through the idempotent owner/classification transition.
        """
        if not getattr(self, "_poll_node_deaths", False):
            return True
        with self._node_death_view_sync_lock:
            try:
                request = GetInstalledNodeDeaths(self.node_id)
                reply = self._node_death_view_rpc(request)
                if type(reply) is not GetInstalledNodeDeathsReply:
                    return False
                reply = replace(reply)
                if reply.request != request:
                    return False
                previous = self._applied_node_death_view
                if reply.view is None:
                    return previous is None
                view = reply.view
                view.validate_successor(previous)
                if previous == view:
                    return True
                for death in view.deaths:
                    self.handle_node_death(death, view.snapshot)
                self._applied_node_death_view = view
                return True
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                return False

    def _poll_node_deaths_best_effort(self) -> None:
        if not getattr(self, "_poll_node_deaths", False):
            return
        now = time.monotonic()
        if now < self._node_death_next_poll_at:
            return
        self._node_death_next_poll_at = now + _WORKER_DEATH_POLL_SECONDS
        self._sync_node_deaths()

    def _worker_death_consumer_state(
        self,
    ) -> tuple[threading.Lock, int]:
        """Lazily normalize narrow fixtures onto journal-consumer state."""

        with self._state_lock:
            lock = getattr(self, "_worker_death_sync_lock", None)
            if lock is None:
                lock = threading.Lock()
                self._worker_death_sync_lock = lock
            cursor = getattr(self, "_worker_death_cursor", 0)
            if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
                raise AssertionError("Worker death cursor must be non-negative")
            self._worker_death_cursor = cursor
            return lock, cursor

    def _sync_worker_deaths(self) -> bool:
        """Consume one fresh, complete GCS Worker-death suffix.

        A successful empty reply is still a barrier: it proves this Core was
        caught up to the GCS watermark at the reply's linearization point.
        The response is fully validated before any reducer mutation.  Each
        record is then installed and cursor-committed separately, so a later
        reducer failure preserves the exact suffix that must be replayed.

        ``False`` means only "fresh authority unavailable".  In particular,
        an RPC timeout, malformed reply, or reducer conflict never manufactures
        a death fact and never skips the uncommitted journal record.
        """

        gcs_address = getattr(self, "gcs_address", None)
        if gcs_address is None:
            return True
        lock, _ = self._worker_death_consumer_state()
        with lock:
            with self._state_lock:
                cursor = getattr(self, "_worker_death_cursor", 0)
            request = protocol.GetWorkerDeaths(cursor)
            try:
                candidate = self._rpc(
                    gcs_address, _GET_WORKER_DEATHS_HANDLER, request
                )
                if not isinstance(candidate, protocol.GetWorkerDeathsReply):
                    return False
                # Pickle may restore frozen dataclasses without invoking their
                # validators.  Reconstruct both the envelope and every nested
                # proof before allowing the first owner-table mutation.
                reply = replace(candidate)
                if reply.after_epoch != cursor:
                    return False
                deaths = tuple(replace(death) for death in reply.deaths)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                return False

            for death in deaths:
                cleanup = None
                if death.reason in (
                    protocol.WorkerDeathReason.PROCESS_EXIT,
                    protocol.WorkerDeathReason.NODE_EXIT,
                ):
                    death_id = _worker_death_reference_id(death)
                    try:
                        # The owner-table fence and every Core-side admission
                        # use this lock in the same order.  Once install wins,
                        # no racing borrower/attempt/guard may recreate work
                        # after the obligation sweep.
                        with self._state_lock:
                            cleanup = self._owner_table.install_dead_worker(
                                death.worker_id, death_id
                            )
                            records = getattr(self, '_worker_death_records', None)
                            if records is None:
                                records = self._worker_death_records = {}
                            prior = records.setdefault(death.worker_id, replace(death))
                            if prior != death:
                                raise ValueError('Worker death history was rebound')
                            foreign_runtime = self._ensure_foreign_lineage_runtime()
                            foreign_runtime.mark_owner_dead(cleanup.record)
                        self._discharge_dead_owner_obligations(death)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except BaseException:
                        # Earlier records are committed individually.  This
                        # record and the remaining suffix will be requested
                        # again from the new cursor; advancing here would
                        # create a permanent gap.
                        return False
                elif death.reason is not protocol.WorkerDeathReason.EXPECTED:
                    # ``replace(death)`` above makes this unreachable for a
                    # protocol value, but keeping the branch explicit protects
                    # the ownership boundary if the enum grows.
                    return False
                with self._completion:
                    current = getattr(self, "_worker_death_cursor", 0)
                    if death.death_epoch != current + 1:
                        # Full prevalidation and the consumer lock make this an
                        # internal invariant rather than a recoverable reply.
                        raise AssertionError(
                            "Worker death journal lost strict continuity"
                        )
                    self._worker_death_cursor = death.death_epoch
                    self._completion.notify_all()
                self._emit(
                    "worker_death_consumed",
                    worker_id=str(death.worker_id),
                    node_id=str(death.node_id),
                    detection_id=death.detection_id,
                    death_epoch=death.death_epoch,
                    reason=death.reason.value,
                    affected_objects=(
                        0 if cleanup is None
                        else len(cleanup.affected_object_ids)
                    ),
                    collectable_objects=(
                        0 if cleanup is None
                        else len(cleanup.collectable_object_ids)
                    ),
                )
                # The reducer only reports candidates.  Physical/contained
                # cleanup remains owned by the existing durable Core GC path.
                if cleanup is not None:
                    for object_id in cleanup.collectable_object_ids:
                        self._drive_reference_collection_best_effort(object_id)

            with self._state_lock:
                return self._worker_death_cursor == reply.watermark

    def _owner_is_dead(self, worker_id: WorkerID) -> bool:
        """Read only the locally installed GCS death fence."""

        try:
            return self._owner_table.dead_worker_record(worker_id) is not None
        except Exception:
            return False

    def _require_owner_alive(self, worker_id: WorkerID) -> None:
        if self._owner_is_dead(worker_id):
            raise OwnerDiedError(
                "object owner {} is confirmed dead".format(worker_id)
            )

    def _discharge_dead_owner_obligations(
        self, death: protocol.WorkerDeathRecord
    ) -> None:
        """Retire outbound work whose remote authority no longer exists.

        Incoming typed contained pins were already retired atomically by
        ``ObjectOwnerTable.install_dead_worker``.  This companion pass removes
        outbound obligations that explicitly name ``death.worker_id`` as their
        remote target; transport failure alone never reaches either reducer.
        """

        if death.reason not in (
            protocol.WorkerDeathReason.PROCESS_EXIT,
            protocol.WorkerDeathReason.NODE_EXIT,
        ):
            return
        dead = death.worker_id
        expected_death_id = _worker_death_reference_id(death)
        collection_candidates = set()  # type: set[ObjectID]
        contained_releases = 0
        with self._completion:
            installed = self._owner_table.dead_worker_record(dead)
            if installed is None or installed.death_id != expected_death_id:
                raise ValueError(
                    "outbound obligation cleanup requires the exact installed "
                    "Worker death proof"
                )
            borrowed = getattr(self, "_borrowed_release_obligations", {})
            borrowed_keys = tuple(key for key in borrowed if key[0] == dead)
            for key in borrowed_keys:
                borrowed.pop(key, None)

            attempts = getattr(self, "_attempt_borrow_releases", {})
            attempt_keys = tuple(key for key in attempts if key[0] == dead)
            for key in attempt_keys:
                attempts.pop(key, None)

            orphan_guards = getattr(
                self, "_orphan_foreign_guard_releases", {}
            )
            orphan_guard_keys = tuple(
                key for key in orphan_guards if key[0] == dead
            )
            for key in orphan_guard_keys:
                orphan_guards.pop(key, None)

            for object_id, obligation in getattr(
                self, "_object_gc_obligations", {}
            ).items():
                dead_edges = {
                    edge for edge in obligation.pending_edges
                    if edge.contained_owner_worker_id == dead
                }
                if dead_edges:
                    contained_releases += len(dead_edges)
                    obligation.child_deaths[dead] = replace(death)
                    obligation.pending_edges.difference_update(dead_edges)
                    collection_candidates.add(object_id)

            retries = getattr(self, "_foreign_guard_release_retries", {})
            for object_id, retry in tuple(retries.items()):
                dead_keys = {
                    self._foreign_guard_key(guard)
                    for guard in (
                        retry.pending.foreign_dependency_guards
                        + retry.pending.nested_foreign_guards
                    )
                    if guard.owner_worker_id == dead
                }
                if not dead_keys:
                    continue
                released = set(retry.released_keys)
                released.update(dead_keys)
                retries[object_id] = replace(
                    retry, due_at=time.monotonic(),
                    released_keys=tuple(sorted(released)),
                )

            self._completion.notify_all()
            submissions = getattr(self, "_submissions", None)
            # A quarantined post-grant reply is not polled forever. An exact
            # newly installed owner death is meaningful new authority: wake
            # that record so GCS cleanup delegation can finish the handoff.
            for marker in tuple(getattr(self, "_protocol_unresolved", {}).values()):
                state = marker.obligation
                if (marker.phase == "location_quarantined" and isinstance(state, _LocationReportState)
                        and any(report.guard.owner_worker_id == dead for report in state.reports)):
                    self._submissions.put(_DelayedReadyTask(
                        _ReadyTask(marker.pending, marker.pending.spec, state.lease_request.dependencies,
                                   location_state=state), time.monotonic(),
                    ))
        if submissions is not None:
            submissions.put(_WAKE_COORDINATOR)
        for object_id in collection_candidates:
            self._drive_reference_collection_best_effort(object_id)
        self._emit(
            "dead_owner_obligations_discharged",
            worker_id=str(dead),
            borrowed_releases=len(borrowed_keys),
            attempt_releases=len(attempt_keys),
            retained_releases=len(orphan_guard_keys),
            contained_releases=contained_releases,
        )

    def _poll_worker_deaths_best_effort(self) -> None:
        """Run the periodic journal poll without making it liveness proof."""

        if getattr(self, "gcs_address", None) is None:
            return
        now = time.monotonic()
        with self._state_lock:
            due_at = getattr(self, "_worker_death_next_poll_at", None)
            if due_at is None:
                due_at = now
                self._worker_death_next_poll_at = due_at
            if now < due_at:
                return
            # Schedule the next round before I/O.  A failed/slow RPC stays an
            # ordinary retryable observation and cannot become a tight loop.
            self._worker_death_next_poll_at = (
                now + _WORKER_DEATH_POLL_SECONDS
            )
        try:
            self._sync_worker_deaths()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            # Owner state and cursor remain the only authority.  Coordinator
            # progress must not depend on this best-effort observation.
            return

    def _require_live_node_location(
        self, node_id: NodeID, operation: str
    ) -> None:
        if self._node_is_dead(node_id):
            raise NodeDiedError(
                "{} rejected location on DEAD node {}".format(
                    operation, node_id
                )
            )

    def _initialize_reference_events(self) -> None:
        """Start the owner's local-reference event consumer."""

        # A few focused reducer tests construct CoreWorker with
        # ``object.__new__``.  Timer ownership is part of the reference-event
        # runtime, so initialize it here as well as in the full constructor.
        with self._state_lock:
            if getattr(self, "_gc_retry_timers", None) is None:
                self._gc_retry_timers = set()
            self._gc_retry_timers_open = True
        mailbox = _ReferenceEventMailbox(
            self._owner_table, self.worker_id, weakref.ref(self)
        )
        thread = threading.Thread(
            target=_run_reference_event_loop,
            args=(mailbox,),
            name="miniray-core-reference-events",
            daemon=True,
        )
        self._reference_mailbox = mailbox
        self._reference_index = 0
        self._reference_thread = thread
        self._reference_runtime_finalizer = weakref.finalize(
            self, _stop_reference_mailbox, mailbox
        )
        self._reference_runtime_finalizer.atexit = False
        thread.start()

    def _ensure_reference_events(self) -> _ReferenceEventMailbox:
        """Initialize lifecycle state for narrow ``object.__new__`` tests."""

        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            self._initialize_reference_events()
            mailbox = self._reference_mailbox
        return mailbox

    def _new_object_ref(self, object_id: ObjectID) -> ObjectRef:
        """Create one locally-accounted Python handle for a known object."""

        mailbox = self._ensure_reference_events()
        with mailbox.lock:
            if not mailbox.accepting:
                raise RuntimeError("the mini-Ray runtime is shut down")
            token = ("local-handle", self.worker_id, self._reference_index)
            self._reference_index += 1
            self._owner_table.add_local_reference(object_id, token)
            try:
                # Every public handle carries the owner's physical route.  The
                # Worker-owned path already had this information; the Driver
                # owner service makes the same invariant true for Driver-owned
                # refs and allows a nested task argument to be imported by a
                # remote Worker without consulting GCS.
                ref = ObjectRef(
                    object_id, self.worker_id,
                    getattr(self, "owner_address", None),
                )
                ref._bind_local_reference(self, token)
            except BaseException:
                self._owner_table.release_local_reference(object_id, token)
                raise
        return ref

    def _prepare_local_object_refs(
        self, object_ids: tuple[ObjectID, ...]
    ) -> tuple[tuple[ObjectRef, object], ...]:
        """Build inert handles and reserve their exact owner tokens.

        No owner-table state or finalizer is installed here.  Submission can
        therefore preflight every fallible Python allocation before making its
        output manifest visible.
        """

        mailbox = self._ensure_reference_events()
        prepared: list[tuple[ObjectRef, object]] = []
        with mailbox.lock:
            if not mailbox.accepting:
                raise RuntimeError("the mini-Ray runtime is shut down")
            for object_id in object_ids:
                token = ("local-handle", self.worker_id, self._reference_index)
                self._reference_index += 1
                prepared.append((
                    ObjectRef(
                        object_id, self.worker_id,
                        getattr(self, "owner_address", None),
                    ),
                    token,
                ))
        return tuple(prepared)

    def _bind_prepared_local_object_refs(
        self, prepared: tuple[tuple[ObjectRef, object], ...]
    ) -> tuple[ObjectRef, ...]:
        """Attach finalizers after owner tokens exist for the full batch."""

        refs: list[ObjectRef] = []
        try:
            for ref, token in prepared:
                ref._bind_local_reference(self, token)
                refs.append(ref)
        except BaseException:
            # A fault-injection wrapper may raise after _bind_local_reference
            # itself succeeded but before this loop appended the ref.  Inspect
            # every prepared handle so no half-bound finalizer can race abort.
            for ref, _token in prepared:
                finalizer = ref._finalizer
                if finalizer is not None:
                    finalizer.detach()
                ref._finalizer = None
                ref._local_token = None
                ref._release_done = None
            raise
        return tuple(refs)

    def _enqueue_local_reference_release(
        self, object_id: ObjectID, token: object, done: threading.Event
    ) -> bool:
        """Non-blocking entry point used exclusively by ObjectRef finalizers."""

        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return False
        return mailbox.enqueue(_LocalReferenceRelease(object_id, token, done))

    def _enqueue_borrowed_reference_release(
        self,
        object_id: ObjectID,
        owner_worker_id: WorkerID,
        owner_address: Address,
        borrower_token: str,
        done: threading.Event,
    ) -> bool:
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return False
        release = protocol.ReleaseBorrowedObject(
            object_id, owner_worker_id, self.worker_id, borrower_token
        )
        key = (owner_worker_id, object_id, self.worker_id, borrower_token)
        with self._state_lock:
            # Linearize the death fence and release intent under the same Core
            # lock.  A check performed before this lock would allow the death
            # consumer to sweep the table and a racing finalizer to recreate
            # an obligation for an owner that can never acknowledge it.
            if self._owner_is_dead(owner_worker_id):
                done.set()
                return True
            table = getattr(self, "_borrowed_release_obligations", None)
            if table is None:
                table = {}
                self._borrowed_release_obligations = table
            obligation = table.get(key)
            if obligation is None:
                done.set()
                return False
            if (
                obligation.owner_address != owner_address
                or obligation.release != release
            ):
                done.set()
                return False
            obligation.release_requested = True
        enqueued = mailbox.enqueue_internal(
            _ReleaseBorrowedReference(key, done)
        )
        if not enqueued:
            # The durable intent was installed before observing a stopped
            # mailbox.  A synchronous shutdown convergence pass still owns it.
            done.set()
            return False
        return enqueued

    def _register_borrowed_release_obligation(
        self,
        owner_address: Address,
        acquire: protocol.AcquireBorrowedObject,
        release: protocol.ReleaseBorrowedObject,
    ) -> tuple[_BorrowReleaseKey, _BorrowReleaseObligation, bool]:
        obligation = _BorrowReleaseObligation(
            _BorrowReleaseIdentity(owner_address, acquire, release)
        )
        key = obligation.key
        with self._state_lock:
            self._require_owner_alive(acquire.owner_worker_id)
            table = getattr(self, "_borrowed_release_obligations", None)
            if table is None:
                table = {}
                self._borrowed_release_obligations = table
            previous = table.setdefault(key, obligation)
            if (
                previous.owner_address != owner_address
                or previous.acquire != acquire
                or previous.release != release
            ):
                raise SystemTaskError(
                    "borrowed-reference identity changed before acquire"
                )
            return key, previous, previous is obligation

    def _schedule_borrowed_reference_release(
        self, key: _BorrowReleaseKey
    ) -> None:
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return
        with self._state_lock:
            obligation = getattr(
                self, "_borrowed_release_obligations", {}
            ).get(key)
            if obligation is None or obligation.scheduled_round is not None:
                return
            if self._owner_is_dead(obligation.release.owner_worker_id):
                self._borrowed_release_obligations.pop(key, None)
                self._completion.notify_all()
                return
            obligation.retry_round += 1
            obligation.scheduled_round = obligation.retry_round
            scheduled_round = obligation.retry_round
            delay = min(
                _PUSH_RETRY_MAX_SECONDS,
                _PUSH_RETRY_BASE_SECONDS
                * (2 ** min(scheduled_round - 1, 5)),
            )
        self._schedule_reference_event(
            mailbox,
            _ReleaseBorrowedReference(
                key, scheduled_round=scheduled_round
            ),
            delay,
        )

    def _drive_borrowed_reference_release(
        self,
        key: _BorrowReleaseKey,
        *,
        scheduled_round: Optional[int] = None,
    ) -> bool:
        with self._state_lock:
            obligation = getattr(
                self, "_borrowed_release_obligations", {}
            ).get(key)
            if obligation is None:
                return True
            if self._owner_is_dead(obligation.release.owner_worker_id):
                self._borrowed_release_obligations.pop(key, None)
                self._completion.notify_all()
                return True
            if scheduled_round is not None:
                if obligation.scheduled_round != scheduled_round:
                    return False
                obligation.scheduled_round = None
            if not obligation.release_requested:
                return False
        try:
            reply = self._borrow_rpc(
                obligation.owner_address,
                _RELEASE_BORROWED_OBJECT_HANDLER,
                obligation.release,
            )
        except Exception:
            with self._state_lock:
                current = getattr(
                    self, "_borrowed_release_obligations", {}
                ).get(key)
                if (
                    current is obligation
                    and self._owner_is_dead(
                        obligation.release.owner_worker_id
                    )
                ):
                    self._borrowed_release_obligations.pop(key, None)
                    self._completion.notify_all()
                    return True
            self._schedule_borrowed_reference_release(key)
            return False
        if (
            not isinstance(reply, protocol.ReleaseBorrowedObjectReply)
            or reply.object_id != obligation.release.object_id
            or reply.owner_worker_id != obligation.release.owner_worker_id
            or reply.borrower_worker_id != obligation.release.borrower_worker_id
            or reply.borrower_token != obligation.release.borrower_token
            or not reply.accepted
        ):
            self._schedule_borrowed_reference_release(key)
            return False
        with self._state_lock:
            current = getattr(
                self, "_borrowed_release_obligations", {}
            ).get(key)
            if current is not obligation:
                return False
            self._borrowed_release_obligations.pop(key, None)
            self._completion.notify_all()
        return True

    def _request_attempt_borrow_release(
        self,
        key: tuple[WorkerID, ObjectID, AttemptID],
        done: threading.Event,
    ) -> bool:
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return False
        with self._state_lock:
            obligation = getattr(self, "_attempt_borrow_releases", {}).get(key)
            if obligation is None:
                done.set()
                return True
            if self._owner_is_dead(obligation.transfer.owner_worker_id):
                self._attempt_borrow_releases.pop(key, None)
                self._completion.notify_all()
                done.set()
                return True
            obligation.release_requested = True
        enqueued = mailbox.enqueue_internal(_ReleaseAttemptBorrow(key))
        # Explicit close is deterministic with respect to *local durable
        # intent*, not remote network availability.  Even if the mailbox stop
        # sentinel won this enqueue race, the obligation remains visible to the
        # synchronous shutdown convergence pass.
        done.set()
        return enqueued

    def _schedule_attempt_borrow_release(
        self, key: tuple[WorkerID, ObjectID, AttemptID]
    ) -> None:
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return
        with self._state_lock:
            obligation = getattr(self, "_attempt_borrow_releases", {}).get(key)
            if obligation is None or obligation.retry_scheduled:
                return
            if self._owner_is_dead(obligation.transfer.owner_worker_id):
                self._attempt_borrow_releases.pop(key, None)
                self._completion.notify_all()
                return
            obligation.retry_scheduled = True
            obligation.retry_round += 1
            scheduled_round = obligation.retry_round
            delay = min(
                _PUSH_RETRY_MAX_SECONDS,
                _PUSH_RETRY_BASE_SECONDS
                * (2 ** min(obligation.retry_round - 1, 5)),
            )
        self._schedule_reference_event(
            mailbox, _ReleaseAttemptBorrow(key), delay
        )

    def _drive_attempt_borrow_release(
        self, key: tuple[WorkerID, ObjectID, AttemptID]
    ) -> bool:
        with self._state_lock:
            obligation = getattr(self, "_attempt_borrow_releases", {}).get(key)
            if obligation is None:
                return True
            if self._owner_is_dead(obligation.transfer.owner_worker_id):
                self._attempt_borrow_releases.pop(key, None)
                self._completion.notify_all()
                return True
            obligation.retry_scheduled = False
            if not obligation.release_requested:
                return False
        try:
            reply = self._borrow_rpc(
                obligation.transfer.owner_address,
                _RELEASE_BORROWED_OBJECT_HANDLER,
                obligation.release,
            )
        except Exception:
            with self._state_lock:
                current = getattr(
                    self, "_attempt_borrow_releases", {}
                ).get(key)
                if (
                    current is obligation
                    and self._owner_is_dead(
                        obligation.transfer.owner_worker_id
                    )
                ):
                    self._attempt_borrow_releases.pop(key, None)
                    self._completion.notify_all()
                    return True
            self._schedule_attempt_borrow_release(key)
            return False
        if (
            not isinstance(reply, protocol.ReleaseBorrowedObjectReply)
            or reply.object_id != obligation.release.object_id
            or reply.owner_worker_id != obligation.release.owner_worker_id
            or reply.borrower_worker_id != obligation.release.borrower_worker_id
            or reply.borrower_token != obligation.release.borrower_token
            or not reply.accepted
        ):
            self._schedule_attempt_borrow_release(key)
            return False
        with self._state_lock:
            current = getattr(self, "_attempt_borrow_releases", {}).get(key)
            if current is not obligation:
                return False
            self._attempt_borrow_releases.pop(key, None)
            self._completion.notify_all()
        return True

    def _schedule_inline_gc_retry(self, object_id: ObjectID) -> None:
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return
        with self._state_lock:
            obligation = self._gc_obligations().get(object_id)
            if (
                obligation is None
                or obligation.retry_scheduled
            ):
                return
            obligation.retry_scheduled = True
            obligation.retry_round += 1
            delay = min(0.25, 0.01 * (2 ** min(obligation.retry_round - 1, 5)))
        self._schedule_reference_event(
            mailbox, _RetryInlineGc(object_id), delay
        )





    def _gc_obligations(self) -> dict[ObjectID, _ObjectGcObligation]:
        """Lazily normalize narrow fixtures onto the unified GC table."""

        obligations = getattr(self, "_object_gc_obligations", None)
        if obligations is None:
            obligations = getattr(self, "_inline_gc_obligations", None)
        if obligations is None:
            obligations = {}
        self._object_gc_obligations = obligations
        self._inline_gc_obligations = obligations
        return obligations

    def _schedule_reference_event(
        self, mailbox: _ReferenceEventMailbox, event: object, delay: float
    ) -> None:
        core_ref = weakref.ref(self)
        timer_box: list[threading.Timer] = []

        def enqueue_later() -> None:
            core = core_ref()
            if core is None:
                return
            timer = timer_box[0]
            try:
                with core._state_lock:
                    timers = getattr(core, "_gc_retry_timers", set())
                    admitted = (
                        getattr(core, "_gc_retry_timers_open", True)
                        and timer in timers
                    )
                if admitted:
                    mailbox.enqueue_internal(event)
            finally:
                # Keep a running callback registered until its enqueue has
                # finished.  Shutdown can therefore snapshot and join it
                # before placing the mailbox stop sentinel.
                with core._state_lock:
                    getattr(core, "_gc_retry_timers", set()).discard(timer)

        timer = threading.Timer(delay, enqueue_later)
        timer.daemon = True
        timer_box.append(timer)
        with self._state_lock:
            timers = getattr(self, "_gc_retry_timers", None)
            if timers is None:
                timers = set()
                self._gc_retry_timers = timers
            if not getattr(self, "_gc_retry_timers_open", True):
                return
            if not hasattr(self, "_gc_retry_timers_open"):
                self._gc_retry_timers_open = True
            timers.add(timer)
            # Registration and start share one lock.  Teardown can never
            # snapshot an unstarted Timer that will begin after cancellation.
            try:
                timer.start()
            except BaseException:
                timers.discard(timer)
                raise

    def _enqueue_inline_gc_check(self, object_id: ObjectID) -> None:
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is not None:
            mailbox.enqueue_internal(_RetryInlineGc(object_id))

    def _has_late_replica_cleanup_locked(self, object_id: ObjectID | None = None) -> bool:
        cleanup = getattr(self, "_late_replica_cleanup", None)
        return cleanup is not None and cleanup.has_pending(object_id)

    def _retain_retired_replica_cleanup_locked(
        self, descriptor: protocol.ObjectStoreDescriptor,
    ) -> bool:
        """Fence a late location and retain deletion before rejecting its grant.

        Report/decision admission use this same Core lock. The owner validates
        exact retired metadata; its current descriptor may already be gone or
        belong to a reconstructed attempt. No deletion RPC is issued here.
        """
        rejected = tuple(identity for identity, keep in getattr(self, '_output_loss_choices', {}).items()
                         if identity.attempt_id == descriptor.producer_attempt_id
                         and descriptor.object_id in identity.output_ids and not keep)
        request = self._owner_table.retired_output_replica(descriptor, rejected_publications=rejected)
        if request is None:
            request = self._owner_table.retired_stored_replica(descriptor)
        if request is None:
            return False
        cleanup = getattr(self, "_late_replica_cleanup", None)
        if cleanup is None:
            cleanup = self._late_replica_cleanup = ReplicaCleanupQueue()
        cleanup.enqueue(request)
        death = getattr(self, "_dead_nodes", {}).get(request.node_id)
        if death is not None and death.reason is protocol.NodeDeathReason.PROCESS_EXIT:
            cleanup.acknowledge_node_death(death)
        self._schedule_late_replica_cleanup_locked()
        return True

    def _schedule_late_replica_cleanup_locked(self) -> None:
        if (not self._has_late_replica_cleanup_locked()
                or getattr(self, "_late_replica_cleanup_scheduled", False)):
            return
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is None:
            return
        self._late_replica_cleanup_scheduled = True
        try:
            admitted = mailbox.enqueue_internal(_RetryReplicaCleanup())
        except Exception:
            self._late_replica_cleanup_scheduled = False
            raise
        if not admitted:
            self._late_replica_cleanup_scheduled = False

    def _drive_late_replica_cleanup(self, *, from_event: bool = False, schedule_retry: bool = True) -> bool:
        """Retry exact physical drops without blocking the consumer's cancel.

        PINNED, stale/invalid replies and lost ACKs keep custody. A local grant
        cancellation merely unpins; only the Node deletion receipt or installed
        Node death discharges this queue. It stays independent of object GC.
        """
        with self._state_lock:
            cleanup = getattr(self, "_late_replica_cleanup", None)
            if cleanup is None:
                return True
            if from_event:
                self._late_replica_cleanup_scheduled = False
            requests = cleanup.pending()
        completed = []
        for request in requests:
            with self._state_lock:
                death = getattr(self, "_dead_nodes", {}).get(request.node_id)
                if death is not None and death.reason is protocol.NodeDeathReason.PROCESS_EXIT:
                    completed.extend(cleanup.acknowledge_node_death(death))
                    continue
                if not cleanup.claim(request):
                    continue
            try:
                reply = self._rpc(self._resolve_node_address(request.node_id), _DROP_OBJECT_REPLICA_HANDLER, request)
                with self._state_lock:
                    if cleanup.acknowledge(request, reply):
                        completed.append(request.object_id)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                pass
            finally:
                with self._state_lock:
                    cleanup.unclaim(request)
                    # A concurrent installed Node death may have supplied the
                    # proof while this RPC was in flight. Its earlier wake
                    # could still observe the claim; notify after it retires.
                    if not cleanup.has_pending(request.object_id):
                        completed.append(request.object_id)
        with self._state_lock:
            for object_id in dict.fromkeys(completed):
                self._enqueue_inline_gc_check(object_id)
            if completed:
                self._completion.notify_all()
                self._submissions.put(_WAKE_COORDINATOR)
            pending = cleanup.has_pending()
            mailbox = getattr(self, "_reference_mailbox", None)
            if (pending and schedule_retry and mailbox is not None
                    and not getattr(self, "_late_replica_cleanup_scheduled", False)
                    and getattr(self, "_gc_retry_timers_open", False)):
                self._late_replica_cleanup_scheduled = True
                try:
                    self._schedule_reference_event(mailbox, _RetryReplicaCleanup(), 0.05)
                except BaseException:
                    self._late_replica_cleanup_scheduled = False
                    raise
            return not pending

    def _restore_borrowed_reference(
        self,
        object_id: ObjectID,
        owner_worker_id: WorkerID,
        owner_address: Address,
        hold: IncomingContainedReferenceHold,
    ) -> ObjectRef:
        """Acquire one unique owner token before exposing a restored ref."""

        if owner_worker_id == self.worker_id:
            # This path is useful when an owner reads a value that contains one
            # of its own exported handles; it is a fresh local Python handle,
            # not a distributed borrower.
            return self._new_object_ref(object_id)
        mailbox = self._ensure_reference_events()
        borrower_token = "borrow:{}:{}".format(
            self.worker_id.hex, uuid.uuid4().hex
        )
        request = protocol.AcquireBorrowedObject(
            object_id, owner_worker_id, self.worker_id, protocol.ContainedTransferSource(hold),
            borrower_token,
        )
        release = protocol.ReleaseBorrowedObject(
            object_id, owner_worker_id, self.worker_id, borrower_token
        )
        key, obligation, inserted = self._register_borrowed_release_obligation(
            owner_address, request, release
        )
        # Reserve admission under the mailbox lock, but never hold it across
        # network I/O.  Shutdown may close admission while Acquire is in
        # flight; the post-ACK recheck then releases/tombstones the token.
        with mailbox.lock:
            if not mailbox.accepting:
                if inserted:
                    with self._state_lock:
                        if (
                            self._borrowed_release_obligations.get(key)
                            is obligation
                        ):
                            self._borrowed_release_obligations.pop(key, None)
                raise RuntimeError("the mini-Ray runtime is shut down")
        acquire_attempted = False
        try:
            acquire_attempted = True
            reply = self._borrow_rpc(
                owner_address, _ACQUIRE_BORROWED_OBJECT_HANDLER, request
            )
            if (
                not isinstance(reply, protocol.AcquireBorrowedObjectReply)
                or reply.object_id != object_id
                or reply.owner_worker_id != owner_worker_id
                or reply.borrower_worker_id != self.worker_id
                or reply.source != request.source
                or reply.borrower_token != borrower_token
            ):
                raise OwnerDiedError(
                    "object owner returned an invalid acquire acknowledgement"
                )
            if not reply.accepted:
                raise OwnerDiedError(
                    reply.error or "object owner rejected the borrower"
                )
            with mailbox.lock:
                if not mailbox.accepting:
                    raise RuntimeError("the mini-Ray runtime is shut down")
                ref = ObjectRef(object_id, owner_worker_id, owner_address)
                ref._bind_borrowed_reference(
                    self, borrower_token, request.source
                )
                return ref
        except BaseException:
            # This includes a fully ambiguous acquire: the owner may have added
            # the token before every ACK was lost.  Release uses the same
            # identity and creates a release-before-acquire tombstone, so a
            # delayed acquire can never resurrect the failed restore.
            if acquire_attempted:
                with self._state_lock:
                    current = self._borrowed_release_obligations.get(key)
                    if current is obligation:
                        obligation.release_requested = True
                self._drive_borrowed_reference_release(key)
            raise

    def _restore_task_argument_reference(
        self,
        transfer: protocol.NestedReferenceTransfer,
        attempt_id: AttemptID,
    ) -> ObjectRef:
        """Import one nested Task argument through its logical Task hold.

        The hold lives for the logical consumer and can span retries.  This
        method creates only the physical attempt's borrower handle.
        """

        if not isinstance(transfer, protocol.NestedReferenceTransfer):
            raise TypeError("transfer must be a NestedReferenceTransfer")
        if not isinstance(attempt_id, AttemptID):
            raise TypeError("attempt_id must be an AttemptID")
        hold = transfer.hold
        if hold.task_id != attempt_id.task_id:
            raise ValueError("nested reference hold belongs to another task")
        if transfer.owner_worker_id == self.worker_id:
            snapshot = self._owner_table.snapshot(transfer.object_id)
            active = (
                hold in snapshot.submitted_tokens
                if hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
                else hold in snapshot.retained_tokens
            )
            if not active:
                raise OwnerDiedError(
                    "nested task hold is not active at the local owner"
                )
            return self._new_object_ref(transfer.object_id)
        mailbox = self._ensure_reference_events()
        borrower_token = "task-borrow:{}:{}:{}:{}".format(
            attempt_id, self.worker_id, transfer.owner_worker_id,
            transfer.object_id,
        )
        source = protocol.TaskHoldSource(hold)
        request = protocol.AcquireBorrowedObject(
            transfer.object_id, transfer.owner_worker_id, self.worker_id,
            source, borrower_token,
        )
        release = protocol.ReleaseBorrowedObject(
            transfer.object_id, transfer.owner_worker_id, self.worker_id,
            borrower_token,
        )
        key = (transfer.owner_worker_id, transfer.object_id, attempt_id)
        obligation = _AttemptBorrowRelease(
            transfer, attempt_id, request, release
        )
        with self._state_lock:
            self._require_owner_alive(transfer.owner_worker_id)
            table = getattr(self, "_attempt_borrow_releases", None)
            if table is None:
                table = {}
                self._attempt_borrow_releases = table
            previous = table.setdefault(key, obligation)
            if (
                previous.transfer != transfer
                or previous.acquire != request
                or previous.release != release
            ):
                raise SystemTaskError(
                    "nested attempt borrower identity changed before acquire"
                )
        with mailbox.lock:
            if not mailbox.accepting:
                with self._state_lock:
                    if self._attempt_borrow_releases.get(key) is previous:
                        self._attempt_borrow_releases.pop(key, None)
                raise RuntimeError("the mini-Ray runtime is shut down")
        acquire_attempted = False
        try:
            acquire_attempted = True
            reply = self._borrow_rpc(
                transfer.owner_address, _ACQUIRE_BORROWED_OBJECT_HANDLER, request
            )
            if (
                not isinstance(reply, protocol.AcquireBorrowedObjectReply)
                or reply.object_id != transfer.object_id
                or reply.owner_worker_id != transfer.owner_worker_id
                or reply.borrower_worker_id != self.worker_id
                or reply.source != source
                or reply.borrower_token != borrower_token
                or not reply.accepted
            ):
                raise OwnerDiedError(
                    getattr(reply, "error", None)
                    or "object owner rejected nested task borrower"
                )
            with mailbox.lock:
                if not mailbox.accepting:
                    raise RuntimeError("the mini-Ray runtime is shut down")
                ref = ObjectRef(
                    transfer.object_id, transfer.owner_worker_id,
                    transfer.owner_address,
                )
                ref._bind_attempt_borrow_reference(
                    self, key, borrower_token, request.source
                )
                return ref
        except BaseException:
            if acquire_attempted:
                with self._state_lock:
                    current = self._attempt_borrow_releases.get(key)
                    if current is not None:
                        current.release_requested = True
                self._drive_attempt_borrow_release(key)
            raise

    def _loads_owned_value(self, payload: bytes) -> object:
        with importing_references(self._restore_borrowed_reference):
            return cloudpickle.loads(payload)

    def _borrow_rpc(
        self, address: Address, handler: str, message: object
    ) -> object:
        """Replay one idempotent owner RPC after ambiguous transport loss."""

        owner_worker_id = getattr(message, "owner_worker_id", None)
        if isinstance(owner_worker_id, WorkerID):
            self._require_owner_alive(owner_worker_id)
        last_error: Optional[TransportError] = None
        for _ in range(3):
            if getattr(self, "_reference_transport_closed", False):
                raise RuntimeShuttingDownError("managed cluster already exited")
            try:
                return rpc_request(
                    address, handler, message,
                    connect_timeout=_RPC_CONNECT_TIMEOUT_SECONDS,
                    request_timeout=_RPC_TOTAL_TIMEOUT_SECONDS,
                    event_sink=getattr(self, "event_sink", None),
                    trace_component="core_worker",
                )
            except TransportError as exc:
                last_error = exc
        assert last_error is not None
        if isinstance(owner_worker_id, WorkerID):
            self._require_owner_alive(owner_worker_id)
        raise OwnerUnavailableError(
            "object owner is unreachable: {}".format(last_error)
        ) from last_error

    def put(self, value: object) -> ObjectRef:
        return self._put_value(value)

    def _put_value(self, value: object) -> ObjectRef:
        from .put_handoff import discover_put
        from . import enhanced_publication as enhanced
        index = self._begin_put()
        identity = ObjectID.for_task(TaskID.for_put(self.job_id, self.worker_id, index), 0)
        attempt = AttemptID(identity.task_id, 0)
        work = None
        try:
            prepared = discover_put(value, identity, self.worker_id, self.owner_address, self.inline_threshold)
            with self._state_lock:
                self._owner_table.register(identity, current_attempt=attempt, producer_task_spec=None)
                self._objects[identity] = _ObjectWaiter(threading.Event())
                self._recovery_manager().register_put(identity)
                work = {'prepared': prepared, 'attempt': attempt, 'started': set(), 'acked': set(),
                        'releases': {}, 'seal': None, 'route': None, 'aborted': False, 'driver': True,
                        'child_replies': {}, 'publication': enhanced.PutPublication(self.job_id, self.owner_address, prepared.manifest)}
                obligations = getattr(self, '_put_handoffs', None)
                if obligations is None:
                    obligations = self._put_handoffs = {}
                obligations[identity] = work
            self._publication_client().begin(work['publication'])
            for stage, kind in (('prepare', protocol.PrepareStoredContainedPin), ('promote', protocol.PromoteStoredContainedPin)):
                for transfer in prepared.manifest.transfers:
                    request = kind(transfer=transfer, authority_worker_id=transfer.contained_owner_worker_id)
                    work['started'].add((stage, transfer))
                    reply = self._put_child_rpc(transfer, stage + '_stored_contained_pin', request)
                    if type(reply) is not protocol.StoredContainedPinReply or reply.request != request or not reply.accepted:
                        raise SystemTaskError('put child handoff did not acknowledge its exact request')
                    work['acked'].add((stage, transfer))
                    work['child_replies'][(stage, transfer)] = replace(reply)
            manifest = prepared.manifest
            node_id = self.node_id
            seal = None
            route = None
            if manifest.tier is protocol.ResultStorage.OBJECT_STORE:
                seal = protocol.SealObject.from_data(identity, attempt, self.worker_id, prepared.payload)
                route = self._require_home_route('explicit put')
            while True:
                seal_reply = None
                incarnation = None
                if seal is not None:
                    work['route'], work['seal'] = route, seal
                    try:
                        reply = self._rpc(route.address, 'seal_object', seal)
                    except BaseException:
                        with self._state_lock:
                            dead = self._node_is_dead(route.node_id)
                            next_route = self._home_route
                        if dead and next_route is not None and next_route.node_id != route.node_id:
                            route = next_route
                            continue
                        raise
                with self._state_lock:
                    if seal is not None:
                        if self._node_is_dead(route.node_id):
                            next_route = self._home_route
                            if next_route is None or next_route.node_id == route.node_id:
                                raise NodeDiedError('put Node died without a survivor')
                            route = next_route
                            continue
                        if (type(reply) is not protocol.SealObjectReply
                                or (reply.object_id, reply.node_id, reply.size_bytes, reply.checksum)
                                != (identity, route.node_id, manifest.size_bytes, manifest.checksum)):
                            raise SystemTaskError('put seal changed request identity')
                        reply = replace(reply)
                        if not reply.sealed:
                            if reply.absence_fenced:
                                work['seal'] = None
                            raise SystemTaskError(reply.error or 'put seal was rejected')
                        node_id = route.node_id
                        seal_reply = reply
                if seal is not None:
                    incarnation = self._put_node_incarnation(route.node_id)
                    with self._state_lock:
                        if self._node_is_dead(route.node_id):
                            next_route = self._home_route
                            if next_route is None or next_route.node_id == route.node_id:
                                raise NodeDiedError('put Node died without a survivor')
                            route = next_route
                            continue
                preparation = enhanced.PutPreparedReceipt(
                    work['publication'].reference,
                    tuple(work['child_replies'][('prepare', transfer)] for transfer in manifest.transfers),
                    tuple(work['child_replies'][('promote', transfer)] for transfer in manifest.transfers),
                    enhanced.MaterializationReceipt(work['publication'].reference, incarnation), seal_reply,
                )
                self._publication_client().commit_put(work['publication'], preparation)
                with self._state_lock:
                    if seal is not None and self._node_is_dead(route.node_id):
                        # C5 committed a concrete materialization attestation.
                        # A death now aborts this uninstalled put; it must not
                        # rebind that receipt to different bytes/location.
                        raise NodeDiedError('put materialization was lost before owner installation')
                    descriptor = protocol.ResultDescriptor(
                        identity, manifest.tier, manifest.size_bytes, self.worker_id, node_id, manifest.checksum,
                        prepared.payload if manifest.tier is protocol.ResultStorage.INLINE else None,
                    )
                    if work['aborted']:
                        raise SystemTaskError('put was aborted before owner installation')
                    # Death handling and owner installation share this lock.
                    # A winning death reroutes above; a later death observes
                    # a real committed owner result and follows ordinary loss.
                    if not self._owner_table.publish_put_value(identity, attempt, descriptor, manifest.edges):
                        raise SystemTaskError('put owner installation was fenced')
                    work['committed'] = True
                    if descriptor.storage is protocol.ResultStorage.OBJECT_STORE:
                        self._stored_descriptors[identity] = descriptor
                    self._put_handoffs.pop(identity, None)
                    self._wake_object(identity)
                    break
            try:
                return self._new_object_ref(identity)
            except BaseException:
                self._enqueue_inline_gc_check(identity)
                raise
        except BaseException as exc:
            if work is not None and not work.get('committed', False):
                with self._state_lock:
                    work['aborted'] = True
                    work['driver'] = False
                    work['abort_receipt'] = enhanced.OwnerAbortReceipt(
                        work['publication'].reference, self.worker_id, 'put-abort:' + identity.hex,
                    )
                self._drive_put_handoff_cleanup(identity)
                self._publish_error(identity, attempt, exc)
            raise
        finally:
            if work is not None:
                work['driver'] = False
            self._end_put()

    def _put_node_incarnation(self, node_id):
        """Read an actual registered materialization Node, never infer PID."""
        from .output_publication import OutputPublicationNodeIncarnation
        with self._state_lock:
            snapshot = getattr(self, '_installed_cluster_snapshot', None)
            info = None if snapshot is None else next((n for n in snapshot.nodes if n.node_id == node_id), None)
        if info is not None:
            return OutputPublicationNodeIncarnation(node_id, info.node_pid, info.registration_epoch)
        reply = self._rpc(self.gcs_address, 'get_node_state', protocol.GetNodeState(node_id))
        if (type(reply) is not protocol.GetNodeStateReply or not reply.found or reply.node_id != node_id):
            raise SystemTaskError('put materialization lacks registered Node identity')
        return OutputPublicationNodeIncarnation(node_id, reply.node_pid, reply.registration_epoch)

    def _put_child_rpc(self, transfer, handler, request):
        if transfer.contained_owner_worker_id == self.worker_id:
            return getattr(self, handler)(request)
        return self._borrow_rpc(transfer.contained_owner_address, handler, request)

    def _drive_put_handoff_cleanup(self, identity) -> bool:
        with self._state_lock:
            work = getattr(self, '_put_handoffs', {}).get(identity)
            if work is None:
                return True
            if not work['aborted'] or work['driver']:
                return False
            work['driver'] = True
        try:
            self._publication_client().fence(work['publication'], work['abort_receipt'])
            for transfer in work['prepared'].manifest.transfers:
                for hold in (transfer.final_hold, transfer.provisional_hold):
                    request = protocol.ReleaseContainedReference(transfer.contained_object_id, transfer.contained_owner_worker_id, hold)
                    if request in work['releases']:
                        continue
                    with self._state_lock:
                        death = getattr(self, '_worker_death_records', {}).get(transfer.contained_owner_worker_id)
                    if death is not None:
                        work['releases'][request] = death
                        continue
                    reply = self._put_child_rpc(transfer, 'release_contained_reference', request)
                    if (type(reply) is not protocol.ReleaseContainedReferenceReply or not reply.accepted
                            or (reply.object_id, reply.owner_worker_id, reply.hold) != (request.object_id, request.owner_worker_id, request.hold)):
                        return False
                    work['releases'][request] = replace(reply)
            if work['seal'] is not None:
                route, seal = work['route'], work['seal']
                with self._state_lock:
                    dead = self._node_is_dead(route.node_id)
                if not dead:
                    drop = work.get('drop')
                    if drop is None:
                        # Resolve an unknown Seal once; once Drop may have been
                        # sent, replay only Drop so a deletion fence cannot stall
                        # cleanup by correctly refusing another Seal.
                        reply = self._rpc(route.address, 'seal_object', seal)
                        if (type(reply) is not protocol.SealObjectReply
                                or reply.object_id != identity or reply.node_id != route.node_id
                                or reply.size_bytes != work['prepared'].manifest.size_bytes
                                or reply.checksum != work['prepared'].manifest.checksum):
                            return False
                        reply = replace(reply)
                        if not reply.sealed and not reply.absence_fenced:
                            return False
                        drop = protocol.DropObjectReplica(identity, work['attempt'], self.worker_id, route.node_id, reply.checksum)
                        work['drop'] = drop
                    dropped = self._rpc(route.address, _DROP_OBJECT_REPLICA_HANDLER, drop)
                    if (type(dropped) is not protocol.DropObjectReplicaReply
                            or dropped.status not in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
                            or (dropped.object_id, dropped.producer_attempt_id, dropped.owner_worker_id, dropped.node_id, dropped.checksum)
                            != (drop.object_id, drop.producer_attempt_id, drop.owner_worker_id, drop.node_id, drop.checksum)):
                        return False
            self._publication_client().retire(
                work['publication'],
                [value for value in work['releases'].values() if type(value) is protocol.ReleaseContainedReferenceReply],
                tuple(dict.fromkeys(value for value in work['releases'].values() if type(value) is protocol.WorkerDeathRecord)),
            )
            with self._state_lock:
                self._put_handoffs.pop(identity, None)
                self._completion.notify_all()
            self._enqueue_inline_gc_check(identity)
            return True
        except Exception:
            return False
        finally:
            work['driver'] = False

    def _begin_put(self) -> int:
        """Reserve one put identity and join the shutdown drain."""

        with self._completion:
            if not self._accepting:
                raise RuntimeError("the mini-Ray runtime is shutting down")
            put_index = self._put_index
            self._put_index += 1
            self._inflight_puts += 1
            return put_index

    def _end_put(self) -> None:
        """Leave the put drain exactly once, including serialization failure."""

        with self._completion:
            self._inflight_puts -= 1
            if self._inflight_puts < 0:
                raise AssertionError("CoreWorker in-flight put count became negative")
            self._completion.notify_all()


    def create_actor(
        self,
        definition: protocol.ActorClassDefinition,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        resources: ResourceVector,
        *,
        max_restarts: int = 0,
    ) -> ActorEndpoint:
        if _contains_object_ref((args, dict(kwargs))):
            raise TypeError("K0 actor constructors do not accept ObjectRef arguments")
        if self.gcs_address is None:
            raise RuntimeError("actor creation requires a GCS endpoint")
        if (
            isinstance(max_restarts, bool)
            or not isinstance(max_restarts, int)
            or max_restarts < 0
        ):
            raise ValueError("max_restarts must be a non-negative integer")
        if max_restarts > 0 and not self._restartable_actor_owner:
            raise RuntimeError(
                "restartable Actors may only be owned by the Driver"
            )
        if max_restarts > 0 and self.owner_address is None:
            raise RuntimeError(
                "restartable Actor creation requires a bound owner service"
            )
        actor_id = ActorID.random()
        generation = ActorGeneration(actor_id, 0)
        # One public create owns exactly one logical Actor identity.  GCS may
        # have committed the Node reservation even when its reply is lost or
        # reports a retryable intermediate outcome, so every retry below sends
        # this same immutable request -- never a fresh ActorID or constructor
        # serialization.
        request = protocol.CreateActorRequest(
            actor_id, generation, definition,
            cloudpickle.dumps((args, dict(kwargs))), resources,
            self.worker_id, max_restarts, self.owner_address,
        )
        self._begin_actor_control_operation()
        provisional = protocol.ActorSnapshot(
            actor_id=actor_id,
            generation=generation,
            state=protocol.ActorState.CREATING,
            route_epoch=0,
            restarts_used=0,
            max_restarts=max_restarts,
        )
        # Install the owner-side route cell before the first create RPC.  If an
        # initial reply is lost and that Actor crashes immediately, GCS can now
        # publish RESTARTING/ALIVE/DEAD into a known ActorID instead of having
        # its state-install RPC rejected as an unknown Actor.  No public handle
        # exists yet, and begin_call rejects the non-routable CREATING state.
        try:
            self._actor_clients.register(provisional, definition.method_names)
            replay_round = 0
            clean_connect_failures = 0
            transaction_observed = False
            last_error = "GCS did not resolve Actor creation"
            while True:
                if not transaction_observed and self._actor_create_is_shutting_down():
                    if self._actor_clients.remove(actor_id, provisional):
                        raise RuntimeShuttingDownError(
                            "Actor creation was handed to cluster shutdown"
                        )
                    # A concurrent GCS install proves that the request became a
                    # real control-plane obligation; shutdown may no longer
                    # abandon it merely because the create caller saw no reply.
                    transaction_observed = True
                try:
                    candidate = self._rpc(
                        self.gcs_address, _CREATE_ACTOR_HANDLER, request
                    )
                except TransportConnectionError as exc:
                    last_error = "could not connect to GCS to create Actor"
                    replay_round += 1
                    if not transaction_observed:
                        clean_connect_failures += 1
                        if clean_connect_failures >= _ACTOR_CREATE_REPLAY_ATTEMPTS:
                            if self._actor_clients.remove(actor_id, provisional):
                                raise SystemTaskError(
                                    "{} after {} attempts".format(
                                        last_error, _ACTOR_CREATE_REPLAY_ATTEMPTS
                                    )
                                ) from exc
                            # A late owner install won the rollback CAS.  Treat
                            # that as observation and converge the same ActorID.
                            transaction_observed = True
                    self._retry_actor_create(
                        replay_round
                    )
                    continue
                except TransportError as exc:
                    # Delivery or reply receipt may be ambiguous.  Replaying
                    # the same ActorID lets GCS/Node return their cached route
                    # without constructing a second logical Actor.
                    transaction_observed = True
                    last_error = "Actor creation RPC was unresolved: {}".format(
                        exc
                    )
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                # Receiving any reply proves GCS observed a request on this
                # connection, even if its payload is malformed or cross-talk.
                transaction_observed = True
                if not isinstance(candidate, protocol.CreateActorReply):
                    last_error = "GCS returned an invalid actor creation reply"
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                try:
                    # Pickle may bypass dataclass __post_init__.  Reconstruct
                    # before trusting retryable, endpoint, or generation fields.
                    reply = replace(candidate)
                except Exception as exc:
                    last_error = "GCS returned a malformed actor creation reply"
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                if reply.actor_id != request.actor_id:
                    # Cross-talk proves neither success nor failure for our
                    # ActorID.  Exact replay is safer than allocating another ID.
                    last_error = "GCS returned the wrong actor identity"
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                if not reply.accepted:
                    if not reply.retryable:
                        terminal_resolved, terminal = (
                            self._resolve_terminal_actor_create(
                                request, reply, provisional
                            )
                        )
                        if not terminal_resolved:
                            # The rejected reply proves GCS observed this ActorID,
                            # but its compact shape is not a complete tombstone.
                            # Keep exact-replaying create/query until the owner
                            # retains the authoritative DEAD snapshot.
                            replay_round += 1
                            self._retry_actor_create(replay_round)
                            continue
                        raise SystemTaskError(
                            (None if terminal is None else terminal.error)
                            or reply.error
                            or "actor creation was rejected"
                        )
                    last_error = (
                        reply.error or "actor creation remains retryable"
                    )
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                assert reply.node_id is not None and reply.worker_id is not None
                assert (
                    reply.worker_address is not None
                    and reply.worker_pid is not None
                )
                try:
                    if reply.generation == request.generation:
                        snapshot = protocol.ActorSnapshot(
                            actor_id=reply.actor_id,
                            generation=reply.generation,
                            state=protocol.ActorState.ALIVE,
                            route_epoch=reply.route_epoch,
                            restarts_used=0,
                            max_restarts=max_restarts,
                            node_id=reply.node_id,
                            worker_id=reply.worker_id,
                            worker_address=reply.worker_address,
                            worker_pid=reply.worker_pid,
                        )
                    else:
                        # The initial reply may have been lost long enough for
                        # the Actor to restart.  CreateActorReply intentionally
                        # omits the exit chain required by ActorSnapshot, so ask
                        # GCS for the complete authoritative view.
                        snapshot = self._query_actor_state(request.actor_id)
                        if (
                            snapshot.state is not protocol.ActorState.ALIVE
                            or snapshot.generation != reply.generation
                            or snapshot.route_epoch != reply.route_epoch
                            or snapshot.max_restarts != max_restarts
                            or snapshot.node_id != reply.node_id
                            or snapshot.worker_id != reply.worker_id
                            or snapshot.worker_address != reply.worker_address
                            or snapshot.worker_pid != reply.worker_pid
                        ):
                            raise SystemTaskError(
                                "GCS Actor create reply conflicts with current state"
                            )
                except Exception as exc:
                    if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                        raise
                    # The ActorID is already known to GCS.  A failed auxiliary
                    # state query or a semantically malformed accepted route is
                    # not permission to abandon it or allocate another Actor.
                    last_error = (
                        "GCS could not provide a coherent Actor create route: "
                        "{}: {}".format(type(exc).__name__, exc)
                    )
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                self._install_actor_snapshot(snapshot)
                current = self._actor_clients.snapshot(actor_id)
                if current != snapshot:
                    # A concurrent newer owner publication won.  Never replace
                    # it with the accepted-but-stale endpoint; exact replay until
                    # GCS and the owner route cell name one ALIVE incarnation.
                    replay_round += 1
                    self._retry_actor_create(replay_round)
                    continue
                return self._endpoint_from_actor_snapshot(current, definition.method_names)
        finally:
            self._end_actor_control_operation()

    def _retry_actor_create(self, round_number: int) -> None:
        """Wait before exact replay; observed transactions must converge."""

        self._wait_for_actor_create_retry(round_number)

    def _actor_create_is_shutting_down(self) -> bool:
        with self._state_lock:
            return not getattr(self, "_accepting", True)

    def _resolve_terminal_actor_create(
        self,
        request: protocol.CreateActorRequest,
        reply: protocol.CreateActorReply,
        provisional: protocol.ActorSnapshot,
    ) -> tuple[bool, protocol.ActorSnapshot | None]:
        """Retain and verify GCS's complete DEAD create tombstone.

        ``CreateActorReply`` deliberately lacks ``last_exit`` and may carry a
        pre-publication route epoch.  It is therefore only a terminal hint.  A
        previously installed DEAD snapshot or a typed GetActorState reply is
        required before the synchronous caller may observe failure.
        """

        try:
            current = self._actor_clients.snapshot(request.actor_id)
        except KeyError:
            return False, None
        if self._terminal_actor_create_matches(current, request, reply):
            return True, current

        try:
            state_reply = self._query_actor_state_reply(request.actor_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return False, None
        if not state_reply.found:
            # GCS explicitly proves that this ActorID was never registered (the
            # canonical case is admission closing before begin()).  Remove only
            # the exact provisional.  A concurrent owner install wins the CAS
            # and turns this stale unknown observation into another replay.
            return (
                (True, None)
                if self._actor_clients.remove(request.actor_id, provisional)
                else (False, None)
            )
        snapshot = state_reply.snapshot
        if snapshot is None:
            return False, None
        if not self._terminal_actor_create_matches(snapshot, request, reply):
            return False, None
        try:
            self._install_actor_snapshot(snapshot)
            current = self._actor_clients.snapshot(request.actor_id)
        except (KeyError, ValueError):
            return False, None
        return (
            (True, current)
            if self._terminal_actor_create_matches(current, request, reply)
            else (False, None)
        )

    @staticmethod
    def _terminal_actor_create_matches(
        snapshot: protocol.ActorSnapshot,
        request: protocol.CreateActorRequest,
        reply: protocol.CreateActorReply,
    ) -> bool:
        return bool(
            snapshot.actor_id == request.actor_id
            and snapshot.state is protocol.ActorState.DEAD
            and snapshot.max_restarts == request.max_restarts
            and snapshot.generation.generation >= reply.generation.generation
            and snapshot.route_epoch >= reply.route_epoch
        )

    @staticmethod
    def _wait_for_actor_create_retry(round_number: int) -> None:
        delay = min(
            _ACTOR_CREATE_RETRY_MAX_SECONDS,
            _ACTOR_CREATE_RETRY_BASE_SECONDS
            * (2 ** min(max(round_number - 1, 0), 3)),
        )
        threading.Event().wait(delay)

    def _begin_actor_control_operation(
        self, *, require_accepting: bool = True
    ) -> None:
        """Linearize Actor create/query/install with Core shutdown."""

        with self._completion:
            if require_accepting and not getattr(self, "_accepting", True):
                raise RuntimeShuttingDownError(
                    "the mini-Ray runtime is shutting down"
                )
            if not getattr(self, "_owner_protocol_open", True):
                raise RuntimeShuttingDownError(
                    "the mini-Ray owner protocol is closed"
                )
            self._actor_control_ops = getattr(self, "_actor_control_ops", 0) + 1

    def _end_actor_control_operation(self) -> None:
        with self._completion:
            self._actor_control_ops = getattr(self, "_actor_control_ops", 1) - 1
            if self._actor_control_ops < 0:
                raise AssertionError("Actor control operation count became negative")
            self._completion.notify_all()

    @staticmethod
    def _endpoint_from_actor_snapshot(
        snapshot: protocol.ActorSnapshot, method_names: tuple[str, ...]
    ) -> ActorEndpoint:
        if snapshot.state is not protocol.ActorState.ALIVE:
            raise ActorDiedError(
                "Actor is not callable while {}".format(snapshot.state.value)
            )
        assert snapshot.node_id is not None
        assert snapshot.worker_id is not None
        assert snapshot.worker_address is not None
        assert snapshot.worker_pid is not None
        return ActorEndpoint(
            snapshot.actor_id, snapshot.generation, snapshot.node_id,
            snapshot.worker_id, snapshot.worker_address, tuple(method_names),
            snapshot.route_epoch, snapshot.worker_pid,
        )

    def _begin_placement_group_control_operation(self) -> None:
        """Linearize PG control admission with Core shutdown."""

        with self._completion:
            if not getattr(self, "_accepting", True):
                raise RuntimeShuttingDownError(
                    "the mini-Ray runtime is shutting down"
                )
            self._inflight_pg_control_ops = (
                getattr(self, "_inflight_pg_control_ops", 0) + 1
            )

    def _end_placement_group_control_operation(self) -> None:
        with self._completion:
            self._inflight_pg_control_ops = (
                getattr(self, "_inflight_pg_control_ops", 1) - 1
            )
            if self._inflight_pg_control_ops < 0:
                raise AssertionError(
                    "placement-group control operation count became negative"
                )
            self._completion.notify_all()

    def assert_placement_group_task_admissible(
        self, placement_group_id: PlacementGroupID, attempt: int
    ) -> None:
        """Reject stale handles after removal has begun locally."""

        with self._state_lock:
            phase = getattr(self, "_placement_group_states", {}).get(
                (placement_group_id, attempt)
            )
        if phase is protocol.PlacementGroupPhaseStatus.LOST:
            raise PlacementGroupLostError(
                "placement group attempt is terminal LOST"
            )
        if phase is not protocol.PlacementGroupPhaseStatus.CREATED:
            detail = "unknown" if phase is None else phase.value
            raise ValueError(
                "placement group is not active for task submission: {}".format(
                    detail
                )
            )

    def create_placement_group(
        self,
        bundles: tuple[ResourceVector, ...],
        strategy: PlacementStrategy | str,
    ) -> protocol.CreatePlacementGroupReply:
        """Create one PG while holding a shutdown-visible control lease."""

        self._begin_placement_group_control_operation()
        try:
            return self._create_placement_group(bundles, strategy)
        finally:
            self._end_placement_group_control_operation()

    def _create_placement_group(
        self,
        bundles: tuple[ResourceVector, ...],
        strategy: PlacementStrategy | str,
    ) -> protocol.CreatePlacementGroupReply:
        """Create one PG through GCS and retain its committed plan identity."""

        if self.gcs_address is None:
            raise RuntimeError("placement-group creation requires a GCS endpoint")
        try:
            normalized_strategy = (
                strategy
                if isinstance(strategy, PlacementStrategy)
                else PlacementStrategy(strategy)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("unknown placement-group strategy: {!r}".format(strategy)) from exc
        if not isinstance(bundles, tuple):
            raise TypeError("bundles must be a tuple of ResourceVector values")
        bundle_values = bundles
        if not bundle_values or any(
            not isinstance(bundle, ResourceVector) for bundle in bundle_values
        ):
            raise TypeError(
                "bundles must be a non-empty tuple of ResourceVector values"
            )
        placement_group_id = PlacementGroupID.random()
        attempt = 0
        request = protocol.CreatePlacementGroupRequest(
            placement_group_id=placement_group_id,
            attempt=attempt,
            bundles=tuple(
                protocol.PlacementGroupBundle(index, bundle)
                for index, bundle in enumerate(bundle_values)
            ),
            strategy=normalized_strategy.value,
        )
        active_phases = frozenset({
            protocol.PlacementGroupPhaseStatus.PLANNING,
            protocol.PlacementGroupPhaseStatus.PENDING,
            protocol.PlacementGroupPhaseStatus.PREPARING,
            protocol.PlacementGroupPhaseStatus.COMMITTING,
            protocol.PlacementGroupPhaseStatus.ABORTING,
            protocol.PlacementGroupPhaseStatus.REMOVING,
        })
        terminal_failures = frozenset({
            protocol.PlacementGroupPhaseStatus.INFEASIBLE,
            protocol.PlacementGroupPhaseStatus.REMOVED,
        })
        expected_indexes = tuple(range(len(bundle_values)))
        retry_round = 0
        clean_connect_failures = 0
        transaction_observed = False
        last_error: Optional[str] = None

        # One synchronous call owns one immutable PG transaction identity.  A
        # participant ACK can be lost after prepare/commit/abort took effect, so
        # every ambiguous or active response replays this exact request.  Once an
        # active phase has been observed, returning before CREATED or a typed
        # terminal failure would orphan a GCS/Node cleanup obligation.
        while True:
            self._raise_if_placement_group_shutdown_takeover()
            try:
                candidate = self._rpc(
                    self.gcs_address, _CREATE_PLACEMENT_GROUP_HANDLER, request
                )
            except TransportConnectionError as exc:
                self._raise_if_placement_group_shutdown_takeover()
                if not transaction_observed:
                    clean_connect_failures += 1
                    if clean_connect_failures >= _PLACEMENT_GROUP_CONNECT_ATTEMPTS:
                        raise SystemTaskError(
                            "could not connect to GCS to create placement group"
                        ) from exc
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            except TransportError as exc:
                # Request delivery or reply receipt is ambiguous.  The GCS may
                # already own participant obligations for this exact PGID.
                transaction_observed = True
                last_error = str(exc)
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue

            transaction_observed = True
            if not isinstance(candidate, protocol.CreatePlacementGroupReply):
                last_error = "GCS returned an invalid placement-group creation reply"
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            try:
                # Transport unpickling bypasses dataclass ``__post_init__``.
                # Rebuild the value before trusting its typed phase or identity.
                reply = replace(candidate)
            except Exception:
                last_error = "GCS returned a malformed placement-group creation reply"
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            if (reply.placement_group_id, reply.attempt) != (
                request.placement_group_id, request.attempt
            ):
                last_error = "GCS returned the wrong placement-group creation identity"
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            if reply.error is not None:
                last_error = reply.error

            if reply.accepted and (
                reply.phase is protocol.PlacementGroupPhaseStatus.CREATED
            ):
                if tuple(
                    key.bundle_index for key in reply.placements
                ) != expected_indexes:
                    # CREATED proves that reservations may be live, but a
                    # truncated/reordered manifest is not a usable capability.
                    # Replay the exact create identity until GCS returns the full
                    # immutable plan; treating this as a conflict would orphan it.
                    last_error = (
                        "GCS returned an incomplete placement-group bundle plan"
                    )
                    retry_round += 1
                    self._retry_placement_group_control_or_raise(retry_round)
                    continue
                with self._state_lock:
                    states = getattr(self, "_placement_group_states", None)
                    if states is None:
                        states = {}
                        self._placement_group_states = states
                    identity = (reply.placement_group_id, reply.attempt)
                    states[identity] = reply.phase
                    manifests = getattr(
                        self, "_placement_group_manifests", None
                    )
                    if manifests is None:
                        manifests = {}
                        self._placement_group_manifests = manifests
                    manifests[identity] = tuple(reply.placements)
                return reply

            if reply.phase in active_phases:
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue

            if reply.phase is protocol.PlacementGroupPhaseStatus.CREATED:
                raise SystemTaskError(
                    last_error
                    or "GCS rejected a conflicting placement-group create replay"
                )

            if reply.phase in terminal_failures:
                raise SystemTaskError(
                    last_error
                    or "placement-group creation terminated in {}".format(
                        reply.phase.value
                    )
                )

            # The enum is closed, but keep an exact replay if a future wire phase
            # reaches an older Core.  Losing the transaction is less safe than
            # waiting for a phase this client can classify.
            last_error = "GCS returned an unknown placement-group phase"
            retry_round += 1
            self._retry_placement_group_control_or_raise(retry_round)

    def _retry_placement_group_control_or_raise(
        self, round_number: int
    ) -> None:
        """Yield between exact replays unless cluster drain took ownership."""

        self._raise_if_placement_group_shutdown_takeover()
        self._wait_for_placement_group_retry(round_number)
        # Shutdown can win while the backoff wait is blocked.  Fence again
        # before the loop is allowed to send another GCS request.
        self._raise_if_placement_group_shutdown_takeover()

    def _raise_if_placement_group_shutdown_takeover(self) -> None:
        """Stop replay once cluster drain owns PG cleanup."""

        with self._state_lock:
            if not getattr(self, "_accepting", True):
                raise RuntimeShuttingDownError(
                    "placement-group control was handed to cluster shutdown"
                )

    @staticmethod
    def _wait_for_placement_group_retry(round_number: int) -> None:
        delay = min(
            _PLACEMENT_GROUP_RETRY_MAX_SECONDS,
            _PLACEMENT_GROUP_RETRY_BASE_SECONDS
            * (2 ** min(max(round_number - 1, 0), 5)),
        )
        time.sleep(delay)

    def remove_placement_group(
        self, placement_group_id: PlacementGroupID, attempt: int
    ) -> protocol.RemovePlacementGroupReply:
        """Remove one PG while holding a shutdown-visible control lease."""

        self._begin_placement_group_control_operation()
        try:
            return self._remove_placement_group(placement_group_id, attempt)
        finally:
            self._end_placement_group_control_operation()

    def _remove_placement_group(
        self, placement_group_id: PlacementGroupID, attempt: int
    ) -> protocol.RemovePlacementGroupReply:
        """Remove a previously created PG through its exact GCS identity."""

        if self.gcs_address is None:
            raise RuntimeError("placement-group removal requires a GCS endpoint")
        if not isinstance(placement_group_id, PlacementGroupID):
            raise TypeError("placement_group_id must be a PlacementGroupID")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
            raise ValueError("placement group attempt must be non-negative")
        request = protocol.RemovePlacementGroupRequest(placement_group_id, attempt)
        state_key = placement_group_id, attempt
        with self._state_lock:
            states = getattr(self, "_placement_group_states", None)
            if states is None:
                states = {}
                self._placement_group_states = states
            local_phase = states.get(state_key)
            if local_phase is protocol.PlacementGroupPhaseStatus.CREATED:
                states[state_key] = protocol.PlacementGroupPhaseStatus.REMOVING
            elif local_phase is protocol.PlacementGroupPhaseStatus.LOST:
                raise PlacementGroupLostError(
                    "placement group attempt is terminal LOST"
                )
            elif local_phase not in (
                protocol.PlacementGroupPhaseStatus.REMOVING,
                protocol.PlacementGroupPhaseStatus.REMOVED,
            ):
                raise ValueError(
                    "unknown placement-group attempt: {}:{}".format(
                        placement_group_id, attempt
                    )
                )

        retry_round = 0
        last_error: Optional[str] = None
        while True:
            self._raise_if_placement_group_shutdown_takeover()
            try:
                candidate = self._rpc(
                    self.gcs_address, _REMOVE_PLACEMENT_GROUP_HANDLER, request
                )
            except TransportError as exc:
                last_error = str(exc)
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            if not isinstance(candidate, protocol.RemovePlacementGroupReply):
                last_error = "GCS returned an invalid placement-group removal reply"
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            try:
                reply = replace(candidate)
            except Exception:
                last_error = "GCS returned a malformed placement-group removal reply"
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            if (reply.placement_group_id, reply.attempt) != (
                request.placement_group_id, request.attempt
            ):
                last_error = "GCS returned the wrong placement-group removal identity"
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            if reply.error is not None:
                last_error = reply.error
            if (
                reply.accepted
                and reply.removed
                and reply.phase is protocol.PlacementGroupPhaseStatus.REMOVED
            ):
                with self._state_lock:
                    self._placement_group_states[state_key] = reply.phase
                return reply
            if (
                reply.accepted
                and not reply.removed
                and reply.phase is protocol.PlacementGroupPhaseStatus.REMOVING
            ):
                retry_round += 1
                self._retry_placement_group_control_or_raise(retry_round)
                continue
            raise SystemTaskError(
                last_error
                or "placement-group removal terminated in {}".format(
                    reply.phase.value
                )
            )

    def submit_actor_call(
        self, actor_id: ActorID, method_name: str,
        args: tuple[object, ...], kwargs: Mapping[str, object],
    ) -> ObjectRef:
        try:
            method_names = self._actor_clients.methods(actor_id)
        except KeyError as exc:
            raise ActorDiedError("unknown ActorID") from exc
        if method_name not in method_names:
            raise AttributeError("actor has no exported method {!r}".format(method_name))
        if _contains_object_ref((args, dict(kwargs))):
            raise TypeError("K0 actor methods do not accept ObjectRef arguments")
        with self._state_lock:
            if not self._accepting:
                raise RuntimeError("the mini-Ray runtime is shutting down")
            task_id = TaskID.derive(
                self.job_id, self.driver_task_id, self._submission_index
            )
            self._submission_index += 1
            attempt_id = AttemptID(task_id, 0)
            object_id = ObjectID.for_task(task_id, 0)
            try:
                snapshot, sequence, fence = self._actor_clients.begin_call(
                    actor_id, object_id
                )
            except (KeyError, RuntimeError) as exc:
                raise ActorDiedError(str(exc)) from exc
            endpoint = self._endpoint_from_actor_snapshot(snapshot, method_names)
            self._owner_table.register(
                object_id, current_attempt=attempt_id,
                producer_task_spec=None,
            )
            self._objects[object_id] = _ObjectWaiter(threading.Event())
            ref = self._new_object_ref(object_id)
            request = protocol.ActorCallRequest(
                actor_id, snapshot.generation, self.worker_id, sequence,
                method_name, task_id, attempt_id, self.worker_id,
                cloudpickle.dumps((args, dict(kwargs))), snapshot.worker_id,
                snapshot.route_epoch,
            )
            thread = threading.Thread(
                target=self._dispatch_actor_call,
                args=(endpoint, request, object_id, fence),
                name="miniray-actor-call-{}".format(sequence), daemon=True,
            )
            self._actor_call_threads.add(thread)
            try:
                thread.start()
            except BaseException:
                self._actor_call_threads.remove(thread)
                self._actor_clients.finish_call(object_id, fence)
                self._publish_error(
                    object_id, attempt_id,
                    SystemTaskError("could not start actor call thread"),
                )
                raise
        return ref

    def _dispatch_actor_call(
        self, endpoint: ActorEndpoint, request: protocol.ActorCallRequest,
        object_id: ObjectID, fence: ActorCallFence,
    ) -> None:
        try:
            reply = self._actor_call_rpc(endpoint, request, object_id, fence)
            if not isinstance(reply, protocol.ActorCallReply):
                raise SystemTaskError("actor worker returned an invalid reply")
            if (
                reply.actor_id, reply.generation, reply.caller_worker_id,
                reply.sequence, reply.route_epoch,
            ) != (
                request.actor_id, request.generation, request.caller_worker_id,
                request.sequence, request.route_epoch,
            ):
                raise SystemTaskError("actor reply identity does not match its call")
            task_reply = reply.task_reply
            pending = _PendingTask(object_id, protocol.TaskSpec(
                job_id=self.job_id, task_id=request.task_id,
                attempt_id=request.attempt_id, function=protocol.FunctionKey(
                    self.job_id, "actor", request.method_name, "v1"
                ), args=(), num_returns=1, resources=ResourceVector(),
                owner_worker_id=self.worker_id,
            ))
            self._validate_task_reply_identity(pending, fence.worker_id, task_reply)
            with self._state_lock:
                if not self._actor_clients.can_publish(object_id, fence):
                    return
                self._publish_actor_reply(
                    pending, task_reply, expected_node_id=endpoint.node_id
                )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._publish_error(object_id, request.attempt_id, exc)
        finally:
            with self._completion:
                self._actor_clients.finish_call(object_id, fence)
                self._actor_call_threads.discard(threading.current_thread())
                self._completion.notify_all()

    def _actor_call_rpc(
        self, endpoint: ActorEndpoint, request: protocol.ActorCallRequest,
        object_id: ObjectID, fence: ActorCallFence,
    ) -> object:
        """Replay only on the same GCS-confirmed physical Actor route.

        A transport failure proves no lifecycle transition.  Querying GCS may
        refresh the route cache, but a newer route always fences this call; it
        is never replayed against a fresh constructor state.
        """

        deadline = self._actor_call_now() + _ACTOR_CALL_CONVERGENCE_SECONDS
        retry_round = 0
        last_route_error: Optional[TransportError] = None
        last_state_error: Optional[TransportError] = None
        same_route_confirmed = False
        while True:
            try:
                return self._push_task_rpc(
                    endpoint.worker_address, _ACTOR_CALL_HANDLER, request
                )
            except TransportError as exc:
                last_route_error = exc

            remaining = deadline - self._actor_call_now()
            if remaining > 0:
                delay = min(
                    remaining,
                    _ACTOR_CALL_RETRY_MAX_SECONDS,
                    _ACTOR_CALL_RETRY_BASE_SECONDS
                    * (2 ** min(retry_round, 8)),
                )
                self._actor_call_retry_wait(delay)

            try:
                snapshot = self._query_actor_state(request.actor_id)
            except TransportError as state_error:
                # A failed GCS query cannot authorize a replay on another
                # generation.  Retain it only as control-plane diagnostics; the
                # public failure remains rooted in the original Actor route.
                last_state_error = state_error
            else:
                last_state_error = None
                self._install_actor_snapshot(snapshot)
                with self._state_lock:
                    if not self._actor_clients.can_publish(object_id, fence):
                        raise ActorDiedError(
                            "Actor route changed while method call was in flight"
                        ) from last_route_error
                same_route_confirmed = True

            if self._actor_call_now() >= deadline:
                assert last_route_error is not None
                if same_route_confirmed and last_state_error is None:
                    detail = (
                        "Actor remained on the same ALIVE route but was "
                        "unreachable before the control-plane deadline"
                    )
                else:
                    detail = (
                        "Actor route remained unreachable and GCS could not "
                        "confirm a newer generation before the deadline"
                    )
                raise ActorUnavailableError(detail) from last_route_error
            retry_round += 1

    @staticmethod
    def _actor_call_now() -> float:
        return time.monotonic()

    @staticmethod
    def _actor_call_retry_wait(delay: float) -> None:
        threading.Event().wait(delay)

    def _query_actor_state(self, actor_id: ActorID) -> protocol.ActorSnapshot:
        reply = self._query_actor_state_reply(actor_id)
        if not reply.found or reply.snapshot is None:
            raise SystemTaskError(
                reply.error or "GCS could not find the requested Actor"
            )
        return reply.snapshot

    def _query_actor_state_reply(
        self, actor_id: ActorID
    ) -> protocol.GetActorStateReply:
        """Return a validated typed reply, preserving ``found=False``."""

        if self.gcs_address is None:
            raise RuntimeError("Actor state query requires a GCS endpoint")
        self._begin_actor_control_operation(require_accepting=False)
        try:
            reply = self._rpc(
                self.gcs_address, _GET_ACTOR_STATE_HANDLER,
                protocol.GetActorState(actor_id),
            )
        finally:
            self._end_actor_control_operation()
        if (
            not isinstance(reply, protocol.GetActorStateReply)
            or reply.actor_id != actor_id
        ):
            raise SystemTaskError(
                getattr(reply, "error", None)
                or "GCS returned an invalid Actor state reply"
            )
        try:
            return replace(reply)
        except Exception as exc:
            raise SystemTaskError(
                "GCS returned a malformed Actor state reply"
            ) from exc

    def install_actor_state(
        self, request: object
    ) -> protocol.InstallActorStateReply:
        """Install one GCS-authoritative route snapshot at the owner."""

        if not isinstance(request, protocol.InstallActorState):
            raise TypeError("install_actor_state expects InstallActorState")
        if request.owner_worker_id != self.worker_id:
            return protocol.InstallActorStateReply(
                request.owner_worker_id, request.snapshot, False,
                "Actor state targets another owner",
            )
        try:
            self._begin_actor_control_operation(require_accepting=False)
        except RuntimeShuttingDownError as exc:
            return protocol.InstallActorStateReply(
                request.owner_worker_id, request.snapshot, False, str(exc)
            )
        try:
            try:
                self._install_actor_snapshot(request.snapshot)
            except (KeyError, ValueError) as exc:
                return protocol.InstallActorStateReply(
                    request.owner_worker_id, request.snapshot, False, str(exc)
                )
            return protocol.InstallActorStateReply(
                request.owner_worker_id, request.snapshot, True
            )
        finally:
            self._end_actor_control_operation()

    def _install_actor_snapshot(
        self, snapshot: protocol.ActorSnapshot
    ) -> bool:
        """Compare/install a snapshot and fail every revoked-route call."""

        with self._state_lock:
            current = self._actor_clients.snapshot(snapshot.actor_id)
            if snapshot.route_epoch < current.route_epoch:
                return False
            changed, fenced = self._actor_clients.install(snapshot)
            if not changed:
                return False
            detail = (
                snapshot.error
                if snapshot.state is protocol.ActorState.DEAD
                else "Actor generation became unavailable during restart"
            )
            for fenced_object_id in fenced:
                attempt_id = self._owner_table.snapshot(
                    fenced_object_id
                ).current_attempt
                self._publish_error(
                    fenced_object_id, attempt_id,
                    ActorDiedError(detail or "Actor is dead"),
                )
            return True

    @property
    def owner_table(self) -> ObjectOwnerTable:
        """Return the authoritative logical-object table for diagnostics."""

        return self._owner_table

    def acquire_exported_reference(
        self, request: protocol.AcquireBorrowedObject
    ) -> protocol.AcquireBorrowedObjectReply:
        """Owner-authoritative borrower admission, linearized with shutdown."""

        error: Optional[str] = None
        acquired = False
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                error = "request targets a different object owner"
            elif not self._owner_protocol_open or (
                not self._accepting
                and not isinstance(request.source, protocol.TaskHoldSource)
            ):
                error = "object owner is shutting down"
            else:
                self._inflight_borrow_ops += 1
                try:
                    acquired = self._owner_table.acquire_exported_reference(
                        request.object_id,
                        request.source,
                        (request.borrower_worker_id, request.borrower_token),
                    )
                except Exception as exc:
                    error = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        return protocol.AcquireBorrowedObjectReply(
            request.object_id, self.worker_id, request.borrower_worker_id,
            request.source, request.borrower_token,
            error is None, acquired, error,
        )

    def release_borrowed_reference(
        self, request: protocol.ReleaseBorrowedObject
    ) -> protocol.ReleaseBorrowedObjectReply:
        """Release/tombstone is accepted while shutdown drains borrowers."""

        error: Optional[str] = None
        released = False
        trigger_gc = False
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                error = "request targets a different object owner"
            elif not self._owner_protocol_open:
                error = "object owner is stopped"
            else:
                self._inflight_borrow_ops += 1
                try:
                    released = self._owner_table.release_borrowed_reference(
                        request.object_id,
                        (request.borrower_worker_id, request.borrower_token),
                    )
                    trigger_gc = released
                except Exception as exc:
                    error = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        reply = protocol.ReleaseBorrowedObjectReply(
            request.object_id, self.worker_id, request.borrower_worker_id,
            request.borrower_token, error is None, released, error,
        )
        if trigger_gc:
            self._drive_reference_collection_best_effort(request.object_id)
        return reply

    def retain_owned_object_for_task(
        self, request: protocol.RetainOwnedObjectForTask
    ) -> protocol.RetainOwnedObjectForTaskReply:
        """Install a stable task hold while its source borrower is live."""

        error: Optional[str] = None
        retained = False
        borrower = (request.borrower_worker_id, request.borrower_token)
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                error = "request targets a different object owner"
            elif not self._owner_protocol_open:
                error = "object owner is stopped"
            else:
                self._inflight_borrow_ops += 1
                try:
                    # Exact replay remains legal after the Python handle closes
                    # and while ordinary owner admission is shutting down.
                    if self._owner_table.retained_reference_is_bound(
                        request.object_id, request.hold, borrower
                    ):
                        retained = False
                    elif not getattr(
                        self, "_owner_retain_admission_open", True
                    ):
                        error = "object owner is shutting down"
                    else:
                        retained = (
                            self._owner_table.retain_borrowed_reference_for_task(
                                request.object_id, borrower, request.hold
                            )
                        )
                except Exception as exc:
                    error = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        return protocol.RetainOwnedObjectForTaskReply(
            request.object_id, self.worker_id, request.borrower_worker_id,
            request.borrower_token, request.hold,
            error is None, retained, error,
        )

    def close_owner_retain_admission(self) -> None:
        """Fence new task holds while preserving replay/query/release."""

        with self._completion:
            self._owner_retain_admission_open = False
            self._completion.notify_all()

    def release_owned_object_for_task(
        self, request: protocol.ReleaseOwnedObjectForTask
    ) -> protocol.ReleaseOwnedObjectForTaskReply:
        """Release/tombstone only the task hold, never its transfer pin."""

        error: Optional[str] = None
        released = False
        trigger_gc = False
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                error = "request targets a different object owner"
            elif not self._owner_protocol_open:
                error = "object owner is stopped"
            else:
                self._inflight_borrow_ops += 1
                try:
                    released = (
                        self._owner_table.release_retained_reference_for_task(
                            request.object_id, request.hold
                        )
                    )
                    if not self._owner_table.retained_release_was_seen(
                        request.object_id, request.hold
                    ):
                        error = "task release did not establish a tombstone"
                    trigger_gc = released
                except Exception as exc:
                    error = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        reply = protocol.ReleaseOwnedObjectForTaskReply(
            request.object_id, self.worker_id, request.borrower_worker_id,
            request.hold, error is None, released, error,
        )
        if trigger_gc:
            self._drive_reference_collection_best_effort(request.object_id)
        return reply

    def replace_retained_object_for_task(
        self, request: protocol.ReplaceRetainedObjectForTask
    ) -> protocol.ReplaceRetainedObjectForTaskReply:
        """Atomically advance one admitted foreign-lineage credential.

        The endpoint remains replayable while ordinary retain admission is
        closed, because a requester may be converging an acknowledgement for
        a replacement that already committed.  Once the complete owner
        protocol is fenced, return an explicit shutdown fact; it is never
        promoted to a Worker-death proof.
        """

        with self._completion:
            if not self._owner_protocol_open:
                return protocol.ReplaceRetainedObjectForTaskReply(
                    request.object_id, request.owner_worker_id,
                    request.borrower_worker_id, request.expected_hold,
                    request.replacement_hold,
                    protocol.ReplaceRetainedObjectDisposition.FAILED,
                    failure=protocol.ReplaceRetainedObjectFailure.OWNER_STOPPED,
                    detail="object owner protocol is stopped",
                )
            self._inflight_borrow_ops += 1
            try:
                return _replace_retained_object_for_task(
                    self.worker_id, self._owner_table, request
                )
            finally:
                self._inflight_borrow_ops -= 1
                self._completion.notify_all()

    def release_contained_reference(
        self, request: protocol.ReleaseContainedReference
    ) -> protocol.ReleaseContainedReferenceReply:
        """Release one transfer pin and recursively collect inline metadata."""

        error: Optional[str] = None
        released = False
        trigger_gc = False
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                error = "request targets a different object owner"
            elif not self._owner_protocol_open:
                error = "object owner is stopped"
            else:
                self._inflight_borrow_ops += 1
                try:
                    released = self._owner_table.release_contained_reference(
                        request.object_id, request.hold
                    )
                    # A replay after recursive collection is accepted from the
                    # owner tombstone even though metadata no longer exists.
                    seen = self._owner_table.contained_release_was_seen(
                        request.object_id, request.hold
                    )
                    trigger_gc = released
                    if not seen:
                        error = "contained release did not establish a tombstone"
                except Exception as exc:
                    error = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        reply = protocol.ReleaseContainedReferenceReply(
            request.object_id, self.worker_id, request.hold,
            error is None, released, error,
        )
        if trigger_gc:
            self._drive_reference_collection_best_effort(request.object_id)
        return reply

    def prepare_stored_contained_pin(
        self, request: protocol.PrepareStoredContainedPin
    ) -> protocol.StoredContainedPinReply:
        """Prepare one stored child pin under owner-lifecycle authority."""

        if not isinstance(request, protocol.PrepareStoredContainedPin):
            raise TypeError(
                "prepare_stored_contained_pin expects "
                "PrepareStoredContainedPin"
            )
        return self._stored_contained_pin_transition(request, prepare=True)

    def _output_handoff_table(self):
        table = getattr(self, "_output_handoffs", None)
        if table is None:
            table = self._output_handoffs = OutputHandoffTable()
        return table

    def _publication_client(self):
        from .enhanced_publication_client import PublicationClient
        with self._state_lock:
            client = getattr(self, '_enhanced_publication_client', None)
            if client is None:
                client = PublicationClient(lambda handler, request: self._rpc(self.gcs_address, handler, request))
                self._enhanced_publication_client = client
            return client

    def abort_owner_publication(self, request):
        """Serialize Node rollback's owner fence with the real adoption CAS."""
        from . import enhanced_publication as enhanced
        if type(request) is not enhanced.AbortOwnerPublication:
            raise TypeError('owner abort requires its exact publication and rollback scope')
        request = replace(request)
        publication, scope = request.publication, request.rollback
        identity = publication.manifest.publication_id
        try:
            with self._state_lock:
                if publication.owner_worker_id != self.worker_id or publication.owner_address != self.owner_address:
                    raise ValueError('owner abort changed its owner route')
                previous = self._output_handoff_table().query(identity)
                if previous is not None and previous.adoption is not None:
                    return enhanced.AbortOwnerPublicationReply(request, False, adoption=previous.adoption,
                                                               error='owner already adopted this publication')
                if previous is not None and previous.complete is not None:
                    raise ValueError('actual Complete cannot become a Node rollback')
                if previous is None and (scope.prepare_intents or scope.promote_intents or scope.materialization_started):
                    raise ValueError('unregistered owner cannot authorize publication effects')
                self._output_handoff_table().abort_manifest(publication.manifest, 'node rollback:' + scope.rollback_id)
                proof = enhanced.OwnerAbortReceipt(publication.reference, self.worker_id, scope.rollback_id)
                self._publication_client().remember(publication)
                return enhanced.AbortOwnerPublicationReply(request, True, proof)
        except Exception as exc:
            return enhanced.AbortOwnerPublicationReply(request, False, error=str(exc) or type(exc).__name__)

    def register_output_handoff(self, request):
        """Own the exact cleanup manifest before child effects can start."""
        from . import output_protocol as wire
        if type(request) is not wire.RegisterOutputHandoff:
            raise TypeError("owner registration requires RegisterOutputHandoff")
        request = replace(request)
        manifest = request.manifest
        identity = manifest.publication_id
        try:
            with self._state_lock:
                if not self._owner_protocol_open or manifest.header.owner_worker_id != self.worker_id:
                    raise ValueError("output owner is stopped or does not match")
                state = self._owner_table.snapshot(identity.output_ids[0])
                if state.current_attempt != identity.attempt_id or state.state is not ObjectState.PENDING:
                    raise ValueError("output handoff requires the current pending owner attempt")
                if self._node_is_dead(manifest.header.node_incarnation.node_id):
                    raise ValueError("publishing Node is dead")
                pending = getattr(self, "_task_finish_barriers", {}).get(identity.output_ids[0])
                if pending is None or pending.execution != identity.execution:
                    raise ValueError("output handoff has no accepted task execution")
                snapshot = self._output_handoff_table().register(manifest, state.current_attempt)
                from .enhanced_publication import TaskPublication
                self._publication_client().remember(TaskPublication(manifest, self.owner_address))
                if snapshot.phase is not OutputHandoffPhase.PENDING:
                    raise ValueError("historical handoff is not a new forward permission")
                return wire.OutputHandoffReply(request, True, snapshot)
        except Exception as exc:
            return wire.OutputHandoffReply(request, False, error=str(exc) or type(exc).__name__)

    def report_output_handoff_complete(self, request):
        from . import output_protocol as wire
        if type(request) is not wire.ReportOutputHandoffComplete:
            raise TypeError("owner completion requires its exact witness")
        request = replace(request)
        try:
            with self._state_lock:
                if not self._owner_protocol_open:
                    raise ValueError("output owner is stopped")
                snapshot = self._output_handoff_table().record_complete(request.witness)
                self._completion.notify_all()
                return wire.OutputHandoffReply(request, True, snapshot)
        except Exception as exc:
            return wire.OutputHandoffReply(request, False, error=str(exc) or type(exc).__name__)

    def report_output_handoff_rollback(self, request):
        """Accept exact Node compensation, even when registration was rejected.

        An absent owner record permits no downstream effect: the Node must
        report an empty compensation plan. An unknown registration ACK is
        resolved from the actual table; it never bypasses the Node journal's
        complete, manifest-bound compensation receipts.
        """
        from . import output_protocol as wire
        from .output_publication_journal import OutputPublicationStage
        if type(request) is not wire.ReportOutputHandoffRollback:
            raise TypeError("owner rollback requires its exact manifest and receipts")
        try:
            request = replace(request)
            manifest, tombstone = request.manifest, request.tombstone
            identity = manifest.publication_id
            if manifest.header.owner_worker_id != self.worker_id:
                raise ValueError("rollback targets another output owner")
            if len(manifest.slots) != 1:
                raise ValueError("rollback requires a single-output manifest")
            for effect in tombstone.plan.effects:
                if effect.slot_index != 0:
                    raise ValueError("rollback effect names another output")
                if (effect.stage in (OutputPublicationStage.FINAL_RELEASE, OutputPublicationStage.PROVISIONAL_RELEASE)
                        and effect.transfer_index >= len(manifest.slots[0].transfers)):
                    raise ValueError("rollback effect names an absent child transfer")
            with self._state_lock:
                table = self._output_handoff_table()
                snapshot = table.query(identity)
                if snapshot is not None and snapshot.manifest is not None and snapshot.manifest != manifest:
                    raise ValueError("rollback changed its registered handoff manifest")
                if snapshot is not None and (snapshot.adoption is not None or snapshot.complete is not None):
                    raise ValueError("successful handoff cannot be reported as a rollback")
                if (snapshot is None or snapshot.manifest is None) and tombstone.plan.effects:
                    raise ValueError("unregistered handoff cannot have authorized child or byte effects")
                receipts = getattr(self, "_output_rollback_receipts", None)
                if receipts is None:
                    receipts = self._output_rollback_receipts = {}
                previous = receipts.get(identity)
                if previous is not None and previous != tombstone:
                    raise ValueError("rollback replay changed its cleanup receipts")
                snapshot = table.abort_manifest(
                    manifest, "node rollback:" + tombstone.plan.rollback_id,
                )
                receipts[identity] = tombstone
                return wire.OutputHandoffReply(request, True, snapshot)
        except Exception as exc:
            return wire.OutputHandoffReply(request, False, error=str(exc) or type(exc).__name__)

    def get_output_handoff(self, request):
        from . import output_protocol as wire
        if type(request) is not wire.GetOutputHandoff:
            raise TypeError("owner history query requires GetOutputHandoff")
        request = replace(request)
        with self._state_lock:
            return wire.OutputHandoffReply(request, True, self._output_handoff_table().query(request.publication_id))

    def promote_stored_contained_pin(
        self, request: protocol.PromoteStoredContainedPin
    ) -> protocol.StoredContainedPinReply:
        """Promote one prepared child pin under owner-lifecycle authority."""

        if not isinstance(request, protocol.PromoteStoredContainedPin):
            raise TypeError(
                "promote_stored_contained_pin expects "
                "PromoteStoredContainedPin"
            )
        return self._stored_contained_pin_transition(request, prepare=False)

    def _stored_contained_pin_transition(
        self, request: protocol.StoredContainedPinRequest, *, prepare: bool
    ) -> protocol.StoredContainedPinReply:
        """Linearize an owner-table transition with final shutdown."""

        with self._completion:
            if request.authority_worker_id != self.worker_id:
                return protocol.StoredContainedPinReply(
                    request,
                    error_kind=(
                        protocol.StoredPublicationRPCErrorKind.INVALID_REQUEST
                    ),
                    error="request targets a different child owner",
                )
            if not self._owner_protocol_open:
                return protocol.StoredContainedPinReply(
                    request,
                    error_kind=protocol.StoredPublicationRPCErrorKind.UNAVAILABLE,
                    error="OWNER_STOPPED: child owner protocol is stopped",
                )
            self._inflight_borrow_ops += 1
            try:
                adapter = StoredContainedPinOwnerAdapter(self._owner_table)
                return adapter.prepare(request) if prepare else adapter.promote(
                    request
                )
            finally:
                self._inflight_borrow_ops -= 1
                self._completion.notify_all()

    def _drive_reference_collection_best_effort(
        self, object_id: ObjectID
    ) -> None:
        """Keep an acknowledged token release independent from GC I/O.

        Release RPCs are lifecycle facts and must return their typed ACK even
        when later replica/contained-edge collection encounters transport or
        fixture-composition failures.  If a collection claim was installed,
        preserve it and arrange exact replay where a mailbox is available.
        """

        try:
            self._reference_released(object_id)
        except Exception:
            try:
                if object_id in self._gc_obligations():
                    self._schedule_inline_gc_retry(object_id)
            except Exception:
                # The frozen owner metadata/obligation remains the authority;
                # shutdown performs a synchronous second convergence pass.
                pass

    def _reference_released(self, object_id: ObjectID) -> None:
        """Drive ACTIVE -> COLLECTING -> COLLECTED for one owner object.

        The owner lock freezes a plan and persists an obligation before any
        destructive RPC.  Each RPC is then replayed by exact immutable
        identity until a typed acknowledgement arrives.  A transport failure,
        malformed ACK, pin, stale epoch, or draining Node conservatively keeps
        the same obligation.
        """

        if object_id in getattr(self, "_put_handoffs", {}):
            if not self._drive_put_handoff_cleanup(object_id):
                return
        started_plan: ObjectMetadataCollectionPlan | None = None
        with self._state_lock:
            if self._has_late_replica_cleanup_locked(object_id):
                self._schedule_late_replica_cleanup_locked()
                return
            unresolved = getattr(self, "_protocol_unresolved", {}).get(
                object_id.task_id
            )
            if (
                object_id in getattr(self, "_task_finish_barriers", {})
                or unresolved is not None
                and isinstance(unresolved.obligation, (
                    _OutputAdoptionObligation,
                    _OutputNodeLossObligation,
                ))
            ):
                # READY may have been emitted immediately before a lost Node
                # adoption ACK.  The user can close that handle at once, but
                # reverse collection must wait until the immutable publication
                # has fully crossed the forward barrier.  Successful adoption
                # enqueues this same check after clearing the obligation.
                return
            obligations = self._gc_obligations()
            obligation = obligations.get(object_id)
            if obligation is None:
                try:
                    snapshot = self._owner_table.snapshot(object_id)
                except Exception:
                    return
                output_publication = snapshot.output_publication
                descriptor = getattr(self, "_stored_descriptors", {}).get(
                    object_id
                )
                if descriptor is None:
                    # A completed publishing-Node takeover deliberately clears
                    # the readable Core route while retaining immutable size and
                    # checksum in owner metadata.  Last-reference GC still needs
                    # that identity even though the LOST result has no location.
                    descriptor = snapshot.canonical_stored_result
                if snapshot.state is ObjectState.READY_STORED or (
                    snapshot.state is ObjectState.LOST
                    and descriptor is not None
                ):
                    if (
                        descriptor is None
                        or descriptor.storage is not protocol.ResultStorage.OBJECT_STORE
                        or descriptor.object_id != object_id
                        or descriptor.owner_worker_id != self.worker_id
                    ):
                        # Missing canonical integrity metadata makes deletion
                        # unsafe.  Preserve owner metadata for diagnosis.
                        return
                    size_bytes = descriptor.size_bytes
                    checksum = descriptor.checksum
                else:
                    size_bytes = None
                    checksum = None
                output_plan = None
                if output_publication is not None:
                    output_plan = self._owner_table.begin_output_publication_collection(
                        object_id, collection_id="collect:{}".format(uuid.uuid4().hex)
                    )
                    plan = None if output_plan is None else output_plan.metadata_plan
                else:
                    plan = self._owner_table.begin_collection(
                        object_id,
                        canonical_size_bytes=size_bytes,
                        canonical_checksum=checksum,
                    )
                if plan is None:
                    return
                if plan.locations and (
                    plan.producer_attempt_id is None
                    or plan.canonical_checksum is None
                ):
                    raise AssertionError(
                        "stored collection plan lacks immutable drop identity"
                    )
                drops = {
                    node_id: protocol.DropObjectReplica(
                        object_id=object_id,
                        producer_attempt_id=plan.producer_attempt_id,  # type: ignore[arg-type]
                        owner_worker_id=self.worker_id,
                        node_id=node_id,
                        checksum=plan.canonical_checksum,  # type: ignore[arg-type]
                    )
                    for node_id in plan.locations
                }
                obligation = _ObjectGcObligation(
                    plan=plan,
                    pending_drops=drops,
                    pending_edges=set(plan.contained_releases),
                    output_plan=output_plan,
                )
                publication = self._publication_client().current(object_id)
                if publication is not None:
                    from .enhanced_publication import OwnerRetirementReceipt, RetirementReason
                    obligation.publication = publication
                    obligation.publication_fence = OwnerRetirementReceipt(
                        publication.reference, self.worker_id, plan.collection_id, RetirementReason.GC,
                    )
                # This assignment is the durable in-memory barrier.  Never
                # issue Drop/Release before it becomes shutdown-visible.
                obligations[object_id] = obligation
                started_plan = plan
            else:
                obligation.retry_scheduled = False

        if started_plan is not None:
            self._emit(
                "object_collection_started",
                object_id=str(object_id),
                collection_id=started_plan.collection_id,
                attempt_id=(
                    None if started_plan.producer_attempt_id is None
                    else str(started_plan.producer_attempt_id)
                ),
                replica_count=len(started_plan.locations),
                contained_edge_count=len(started_plan.contained_releases),
            )

        if obligation.publication is not None:
            self._publication_client().fence(obligation.publication, obligation.publication_fence)

        # Outgoing edges represent real final child holds. Release every exact
        # hold before deleting bytes; values without child edges need only
        # their physical replica cleanup.
        for edge in tuple(obligation.pending_edges):
            request = protocol.ReleaseContainedReference(
                edge.contained_object_id, edge.contained_owner_worker_id,
                edge.incoming_hold(self.worker_id),
            )
            try:
                reply = self._borrow_rpc(
                    edge.contained_owner_address,
                    _RELEASE_CONTAINED_REFERENCE_HANDLER, request,
                )
            except OwnerDiedError:
                if not self._owner_is_dead(edge.contained_owner_worker_id):
                    continue
                with self._state_lock:
                    if self._gc_obligations().get(object_id) is obligation:
                        death = getattr(self, '_worker_death_records', {}).get(edge.contained_owner_worker_id)
                        if death is None:
                            continue
                        obligation.child_deaths[death.worker_id] = death
                        obligation.pending_edges.discard(edge)
                continue
            except Exception:
                continue
            if (
                isinstance(reply, protocol.ReleaseContainedReferenceReply)
                and reply.object_id == edge.contained_object_id
                and reply.owner_worker_id == edge.contained_owner_worker_id
                and reply.hold == request.hold
                and reply.accepted
            ):
                with self._state_lock:
                    if self._gc_obligations().get(object_id) is obligation:
                        obligation.child_receipts[request] = replace(reply)
                        obligation.pending_edges.discard(edge)


        drops_admitted = not obligation.pending_edges
        if drops_admitted and obligation.publication is not None:
            self._publication_client().retire(obligation.publication, obligation.child_receipts.values(),
                                               obligation.child_deaths.values())
        if drops_admitted:
            for node_id, request in tuple(obligation.pending_drops.items()):
                try:
                    reply = self._rpc(
                        self._resolve_node_address(node_id),
                        _DROP_OBJECT_REPLICA_HANDLER, request,
                    )
                except Exception:
                    continue
                identity_matches = (
                    isinstance(reply, protocol.DropObjectReplicaReply)
                    and reply.object_id == request.object_id
                    and reply.producer_attempt_id == request.producer_attempt_id
                    and reply.owner_worker_id == request.owner_worker_id
                    and reply.node_id == request.node_id
                    and reply.checksum == request.checksum
                )
                if identity_matches and reply.status in (
                    protocol.DropObjectReplicaStatus.DROPPED,
                    protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
                ):
                    with self._state_lock:
                        if self._gc_obligations().get(object_id) is obligation:
                            obligation.pending_drops.pop(node_id, None)
                    self._emit(
                        "object_replica_collection_acknowledged",
                        object_id=str(object_id),
                        collection_id=obligation.plan.collection_id,
                        attempt_id=str(request.producer_attempt_id),
                        node_id=str(node_id),
                        status=reply.status.value,
                    )


        completed = False
        released_lineage_dependencies: list[ObjectID] = []
        with self._state_lock:
            current = self._gc_obligations().get(object_id)
            if (
                current is obligation
                and not self._has_late_replica_cleanup_locked(object_id)
                and not obligation.pending_drops
                and not obligation.pending_edges
            ):
                recovery = self._recovery_manager()
                forget_plan = recovery.validate_forget_collected_object(
                    object_id,
                    expected_task_spec=obligation.plan.producer_task_spec,
                    expected_attempt=obligation.plan.producer_attempt_id,
                )
                lineage_releases = (
                    self._owner_table.collection_lineage_releases(
                        obligation.plan
                    )
                )
                # Validate every hold before either authority is deleted.
                for edge in lineage_releases:
                    dependency = self._owner_table.snapshot(
                        edge.dependency_object_id
                    )
                    if edge.token not in dependency.lineage_tokens:
                        raise SystemTaskError(
                            "producer lineage release has no active dependency hold"
                        )
                # Both validations are side-effect free.  The owner commit may
                # still be fault-injected in tests, so recovery is committed
                # only after it succeeds; its validated commit consists solely
                # of idempotent discard/pop operations and cannot strand
                # lineage after owner metadata has disappeared.
                foreign_prepared_receipt = None
                if getattr(
                    self, "_foreign_lineage_registry", None
                ) is not None and self._foreign_lineage_registry.snapshot(
                    object_id.task_id
                ) is not None:
                    foreign_prepared_receipt = (
                        ForeignLineagePreparedCollectionReceipt(
                        object_id.task_id, object_id, obligation.plan,
                        forget_plan,
                        )
                    )
                    prepared_receipts = getattr(
                        self,
                        "_foreign_lineage_prepared_collection_receipts",
                        None,
                    )
                    if prepared_receipts is None:
                        prepared_receipts = {}
                        self._foreign_lineage_prepared_collection_receipts = (
                            prepared_receipts
                        )
                    previous_receipt = prepared_receipts.setdefault(
                        object_id, foreign_prepared_receipt
                    )
                    if previous_receipt != foreign_prepared_receipt:
                        raise SystemTaskError(
                            "foreign lineage prepared collection receipt changed"
                        )
                # Persist the foreign release intent before either local
                # authority is deleted.  Only the activated receipt below may
                # cross the RPC boundary, and it is constructed after both
                # owner and recovery commits have succeeded.
                if obligation.output_plan is not None:
                    collected = self._owner_table.complete_output_publication_collection(
                        obligation.output_plan
                    ).collection
                else:
                    self._owner_table.validate_complete_collection(obligation.plan)
                    collected = self._owner_table.complete_collection(obligation.plan)
                if collected.lineage_releases != lineage_releases:
                    raise SystemTaskError(
                        "task lineage authority changed during collection"
                    )
                recovery.commit_forget_collected_object(forget_plan)
                foreign_receipt = (
                    None if foreign_prepared_receipt is None
                    else ForeignLineageCollectionReceipt(
                        foreign_prepared_receipt
                    )
                )
                if foreign_receipt is not None:
                    self._foreign_lineage_collection_receipts[object_id] = (
                        foreign_receipt
                    )
                    self._foreign_lineage_prepared_collection_receipts.pop(
                        object_id, None
                    )
                for edge in lineage_releases:
                    if self._owner_table.release_lineage_reference(
                        edge.dependency_object_id, edge.token
                    ):
                        released_lineage_dependencies.append(
                            edge.dependency_object_id
                        )
                self._gc_obligations().pop(object_id, None)
                getattr(self, "_objects", {}).pop(object_id, None)
                getattr(self, "_stored_descriptors", {}).pop(object_id, None)
                completed = True
                self._completion.notify_all()
        if completed:
            self._emit(
                "object_collection_completed",
                object_id=str(object_id),
                collection_id=obligation.plan.collection_id,
            )
            for dependency_id in released_lineage_dependencies:
                self._enqueue_inline_gc_check(dependency_id)
            if foreign_receipt is not None:
                self._drive_foreign_lineage_collection(foreign_receipt)
            return
        self._schedule_inline_gc_retry(object_id)

    def _drive_foreign_lineage_collection(
        self, receipt: ForeignLineageCollectionReceipt, *, from_retry: bool = False
    ) -> bool:
        """Converge a receipt-backed final foreign hold release."""

        runtime = getattr(self, "_foreign_lineage_runtime", None)
        if runtime is None:
            return True
        if from_retry:
            with self._state_lock:
                getattr(
                    self, "_foreign_lineage_collection_retry_scheduled", set()
                ).discard(receipt.task_id)
        try:
            result = runtime.drive_collection(receipt.task_id, receipt)
        except ForeignLineageRuntimeError:
            result = None
        complete = (
            result is not None
            and result.disposition
            is ForeignLineageCollectionDisposition.COMPLETE
        )
        if complete:
            with self._state_lock:
                receipts = getattr(
                    self, "_foreign_lineage_collection_receipts", {}
                )
                prepared_receipts = getattr(
                    self,
                    "_foreign_lineage_prepared_collection_receipts",
                    {},
                )
                for output_id in tuple(receipts):
                    if output_id.task_id == receipt.task_id:
                        receipts.pop(output_id, None)
                for output_id in tuple(prepared_receipts):
                    if output_id.task_id == receipt.task_id:
                        prepared_receipts.pop(output_id, None)
                getattr(
                    self, "_foreign_lineage_collection_retry_scheduled", set()
                ).discard(receipt.task_id)
                self._completion.notify_all()
            return True
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is not None:
            with self._state_lock:
                scheduled = getattr(
                    self, "_foreign_lineage_collection_retry_scheduled", None
                )
                if scheduled is None:
                    scheduled = set()
                    self._foreign_lineage_collection_retry_scheduled = scheduled
                if receipt.task_id not in scheduled:
                    scheduled.add(receipt.task_id)
                    self._schedule_reference_event(
                        mailbox, _RetryForeignLineageCollection(
                            receipt.task_id, receipt
                        ), 0.01,
                    )
        return False

    def _retry_gc_obligations_for_shutdown(self) -> bool:
        """Retry durable edge work once; report whether shutdown may proceed."""

        self._drive_late_replica_cleanup(schedule_retry=False)
        with self._state_lock:
            object_ids = tuple(self._gc_obligations())
        for object_id in object_ids:
            self._reference_released(object_id)
        with self._state_lock:
            attempt_borrows = tuple(
                key for key, obligation in getattr(
                    self, "_attempt_borrow_releases", {}
                ).items()
                if obligation.release_requested
            )
        for key in attempt_borrows:
            self._drive_attempt_borrow_release(key)
        with self._state_lock:
            borrowed_releases = tuple(
                key for key, obligation in getattr(
                    self, "_borrowed_release_obligations", {}
                ).items()
            )
            # Closing the runtime invalidates every remaining ordinary borrowed
            # handle, just as it closes local-handle admission.  Convert each
            # live handle into an exact durable Release before the barrier.
            for key in borrowed_releases:
                self._borrowed_release_obligations[
                    key
                ].release_requested = True
        for key in borrowed_releases:
            self._drive_borrowed_reference_release(key)
        with self._state_lock:
            return not bool(
                self._gc_obligations()
                or self._has_late_replica_cleanup_locked()
                or getattr(self, "_attempt_borrow_releases", {})
                or getattr(self, "_borrowed_release_obligations", {})
            )

    def get_owned_object(
        self, request: protocol.GetOwnedObject
    ) -> protocol.GetOwnedObjectReply:
        """Return a non-blocking owner snapshot to one active borrower."""

        rejection: Optional[str] = None
        snapshot = None
        descriptor = None
        finish_pending = False
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                rejection = "request targets a different object owner"
            elif not self._owner_protocol_open:
                rejection = "object owner is stopped"
            else:
                self._inflight_borrow_ops += 1
                try:
                    token = (request.borrower_worker_id, request.borrower_token)
                    if not self._owner_table.has_borrowed_reference(
                        request.object_id, token
                    ):
                        rejection = "borrower token is not active at the owner"
                    else:
                        snapshot = self._owner_table.snapshot(request.object_id)
                        finish_pending = bool(
                            snapshot.state is ObjectState.LOST
                            and request.object_id in getattr(
                                self, "_task_finish_barriers", {}
                            )
                        )
                        if snapshot.state is ObjectState.READY_STORED:
                            stored = self._stored_descriptors.get(request.object_id)
                            if (
                                stored is None
                                or stored.object_id != request.object_id
                                or stored.owner_worker_id != self.worker_id
                                or stored.storage
                                is not protocol.ResultStorage.OBJECT_STORE
                                or not isinstance(
                                    snapshot.current_attempt, AttemptID
                                )
                                or not snapshot.locations
                            ):
                                rejection = "stored owner metadata is incomplete"
                            else:
                                descriptor = protocol.ObjectStoreDescriptor(
                                    object_id=request.object_id,
                                    owner_worker_id=self.worker_id,
                                    producer_attempt_id=snapshot.current_attempt,
                                    node_id=min(snapshot.locations),
                                    size_bytes=stored.size_bytes,
                                    checksum=stored.checksum,
                                )
                except Exception as exc:
                    rejection = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        if rejection is not None:
            return protocol.GetOwnedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, False, detail=rejection,
            )
        assert snapshot is not None
        state = (
            protocol.OwnedObjectState.PENDING if finish_pending
            else protocol.OwnedObjectState(snapshot.state.value)
        )
        data = (
            snapshot.inline_data
            if snapshot.state is ObjectState.READY_INLINE
            else None
        )
        error = None
        if snapshot.state is ObjectState.ERROR:
            failure = snapshot.error
            error = protocol.RemoteErrorInfo(
                type_name=type(failure).__name__,
                message=str(failure),
                traceback=getattr(failure, "remote_traceback", ""),
            )
        return protocol.GetOwnedObjectReply(
            request.object_id, self.worker_id, request.borrower_worker_id,
            request.borrower_token, True, state=state,
            current_attempt=snapshot.current_attempt, data=data, error=error,
            descriptor=descriptor,
        )

    def request_owned_object_reconstruction(
        self, request: protocol.RequestOwnedObjectReconstruction
    ) -> protocol.RequestOwnedObjectReconstructionReply:
        """Route a foreign borrower's LOST observation to local authority.

        The reducer validates the active borrower capability and expected
        producer epoch.  Its injected callback below is the only path that may
        compose a local START/JOIN with the ordinary coordinator queues.
        """

        reducer = getattr(self, "_owned_reconstruction", None)
        if reducer is None:
            reducer = OwnedObjectReconstructionReducer(
                self.worker_id,
                self._owner_table,
                self._recovery,
                self._admit_owned_object_reconstruction,
                snapshot_reconstruction=self._owned_reconstruction_snapshot,
            )
            self._owned_reconstruction = reducer
        return reducer.handle(request)

    def _owned_reconstruction_snapshot(
        self, object_id: ObjectID,
    ) -> tuple[ObjectOwnerSnapshot, ReconstructionSnapshot]:
        """Read one coherent owner/recovery pair, without holding over RPC."""
        with self._state_lock:
            return (
                self._owner_table.snapshot(object_id),
                self._recovery.reconstruction_snapshot(object_id),
            )

    def _admit_owned_object_reconstruction(
        self, object_id: ObjectID
    ) -> object:
        """Run the one local reconstruction admission used by the reducer.

        ``_start_or_join_reconstruction`` owns every Core-side side effect.
        Returning its exact START/JOIN outcome lets the reducer prove that the
        same attempt was committed in owner and recovery state before ACKing.
        """

        waiter = self._object_waiter(object_id)
        outcome = self._start_or_join_reconstruction(
            object_id, waiter, return_requested_outcome=True
        )
        if outcome is None:
            # The local authority may defer for a task-finish barrier, a
            # concurrent owner-state change, or an unfinished lineage renewal.
            # None is never evidence that a newer attempt was STARTED/JOINED.
            raise ReconstructionDeferred(
                "the owner has not opened this attempt for reconstruction yet"
            )
        return outcome

    def get_retained_owned_object(
        self, request: protocol.GetRetainedOwnedObject
    ) -> protocol.GetRetainedOwnedObjectReply:
        """Return a non-blocking owner snapshot through a task hold."""

        rejection: Optional[str] = None
        snapshot = None
        descriptor = None
        finish_pending = False
        with self._completion:
            if request.owner_worker_id != self.worker_id:
                rejection = "request targets a different object owner"
            elif not self._owner_protocol_open:
                rejection = "object owner is stopped"
            else:
                self._inflight_borrow_ops += 1
                try:
                    if not self._owner_table.has_retained_reference_for_task(
                        request.object_id, request.hold
                    ):
                        rejection = "retained task hold is not active at the owner"
                    else:
                        snapshot = self._owner_table.snapshot(request.object_id)
                        finish_pending = bool(
                            snapshot.state is ObjectState.LOST
                            and request.object_id in getattr(
                                self, "_task_finish_barriers", {}
                            )
                        )
                        if snapshot.state is ObjectState.READY_STORED:
                            stored = self._stored_descriptors.get(request.object_id)
                            if (
                                stored is None
                                or stored.object_id != request.object_id
                                or stored.owner_worker_id != self.worker_id
                                or stored.storage
                                is not protocol.ResultStorage.OBJECT_STORE
                                or not isinstance(
                                    snapshot.current_attempt, AttemptID
                                )
                                or not snapshot.locations
                            ):
                                rejection = "stored owner metadata is incomplete"
                            else:
                                descriptor = protocol.ObjectStoreDescriptor(
                                    object_id=request.object_id,
                                    owner_worker_id=self.worker_id,
                                    producer_attempt_id=snapshot.current_attempt,
                                    node_id=min(snapshot.locations),
                                    size_bytes=stored.size_bytes,
                                    checksum=stored.checksum,
                                )
                except Exception as exc:
                    rejection = str(exc)
                finally:
                    self._inflight_borrow_ops -= 1
                    self._completion.notify_all()
        if rejection is not None:
            return protocol.GetRetainedOwnedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.hold, False, detail=rejection,
            )
        assert snapshot is not None
        state = (
            protocol.OwnedObjectState.PENDING if finish_pending
            else protocol.OwnedObjectState(snapshot.state.value)
        )
        data = (
            snapshot.inline_data
            if snapshot.state is ObjectState.READY_INLINE
            else None
        )
        error = None
        if snapshot.state is ObjectState.ERROR:
            failure = snapshot.error
            error = protocol.RemoteErrorInfo(
                type_name=type(failure).__name__,
                message=str(failure),
                traceback=getattr(failure, "remote_traceback", ""),
            )
        return protocol.GetRetainedOwnedObjectReply(
            request.object_id, self.worker_id, request.borrower_worker_id,
            request.hold, True, state=state,
            current_attempt=snapshot.current_attempt, data=data, error=error,
            descriptor=descriptor,
        )

    def report_retained_object_location(
        self, request: protocol.ReportRetainedObjectLocation
    ) -> protocol.ReportRetainedObjectLocationReply:
        """Separate custody of grant-proven bytes from permission to execute.

        A released task hold cannot authorize PushTask. Its delayed location
        report may still transfer an exact current replica to this owner, or
        transfer retired bytes to the existing physical-cleanup queue. Closing
        retain admission leaves these handoffs open until owner finalization.
        """
        if type(request) is not protocol.ReportRetainedObjectLocation:
            raise TypeError("location report requires an exact typed request")
        request = deepcopy(request)
        request = replace(request, descriptor=replace(request.descriptor))
        with self._completion:
            self._inflight_borrow_ops += 1
            try:
                receipt = self._record_replica_custody_locked(
                    request.descriptor, active_hold=lambda snapshot: request.hold in snapshot.retained_tokens,
                )
                return deepcopy(protocol.ReportRetainedObjectLocationReply(
                    request.object_id, request.owner_worker_id, request.borrower_worker_id, request.hold,
                    receipt.descriptor, receipt.status, receipt.error,
                ))
            finally:
                self._inflight_borrow_ops -= 1
                self._completion.notify_all()

    def report_abandoned_dependency_replica(self, request):
        """Accept replica custody from a Node after its submitter died.

        The frozen route/hold identify the original submission; they never
        recreate a live borrower. Only this owner's canonical/collection
        history authorizes tracking or disposal, using the existing helper.
        """
        if type(request) is not protocol.ReportAbandonedDependencyReplica:
            raise TypeError("abandoned dependency report requires an exact request")
        request = replace(request)
        status = protocol.RetainedLocationReportStatus
        if request.descriptor.owner_worker_id != self.worker_id:
            return protocol.ReportAbandonedDependencyReplicaReply(request, status.REJECTED, "wrong dependency owner")
        with self._completion:
            if not self._owner_protocol_open:
                return protocol.ReportAbandonedDependencyReplicaReply(request, status.REJECTED, "object owner is stopped")
            self._inflight_borrow_ops += 1
        # A remote assertion of death is not enough: match our ordered GCS
        # death consumer before using a custody-only admission path.
        try:
            self._sync_worker_deaths()
            with self._completion:
                installed = self._owner_table.dead_worker_record(request.submitter_death.worker_id)
                if installed is None or installed.death_id != _worker_death_reference_id(request.submitter_death):
                    return protocol.ReportAbandonedDependencyReplicaReply(request, status.REJECTED, "submitter death is not installed at owner")
                receipt = self._record_replica_custody_locked(request.descriptor, active_hold=lambda _snapshot: False)
                return protocol.ReportAbandonedDependencyReplicaReply(request, receipt.status, receipt.error)
        finally:
            with self._completion:
                self._inflight_borrow_ops -= 1
                self._completion.notify_all()

    def _record_replica_custody_locked(self, descriptor, *, active_hold) -> _ReplicaLocationReceipt:
        """One owner location transaction for local and foreign consumers.

        The caller supplies its real SUBMITTED or RETAINED hold predicate, not
        a synthetic wire credential. Typed conflicts reject before mutation;
        an unexpected local effect/notification failure propagates as unknown
        custody so the already-retained post-grant record can replay it.
        """
        descriptor = deepcopy(replace(descriptor))
        status = protocol.RetainedLocationReportStatus

        def outcome(disposition, error=None):
            return _ReplicaLocationReceipt(deepcopy(descriptor), disposition, error)

        if descriptor.owner_worker_id != self.worker_id:
            return outcome(status.REJECTED, "request targets a different object owner")
        if not self._owner_protocol_open:
            return outcome(status.REJECTED, "object owner is stopped")
        try:
            if self._retain_retired_replica_cleanup_locked(descriptor):
                return outcome(status.RETIRED,
                    "retired output replica retained for exact cleanup; cancel the consumer grant")
            snapshot = self._owner_table.snapshot(descriptor.object_id)
        except (UnknownObjectError, OutputOwnerPublicationConflictError) as exc:
            return outcome(status.REJECTED, str(exc))
        canonical = snapshot.canonical_stored_result
        if self._node_is_dead(descriptor.node_id):
            return outcome(status.REJECTED, "location report names a DEAD node")
        if descriptor.producer_attempt_id != snapshot.current_attempt:
            return outcome(status.STALE_PRODUCER, "location report names a stale producer attempt")
        if snapshot.state not in (ObjectState.READY_STORED, ObjectState.LOST):
            return outcome(status.REJECTED, "object is not a stored result in a reportable state")
        if (not isinstance(canonical, protocol.ResultDescriptor)
                or canonical.object_id != descriptor.object_id
                or canonical.owner_worker_id != self.worker_id
                or canonical.storage is not protocol.ResultStorage.OBJECT_STORE
                or canonical.size_bytes != descriptor.size_bytes
                or canonical.checksum != descriptor.checksum or canonical.inline_data is not None):
            return outcome(status.REJECTED, "location report conflicts with canonical stored metadata")
        permitted = active_hold(snapshot)
        routes = getattr(self, "_stored_descriptors", None)
        if routes is None:
            routes = self._stored_descriptors = {}
        existed = descriptor.node_id in snapshot.locations
        replica_result = replace(canonical, node_id=descriptor.node_id)
        route_present = descriptor.object_id in routes
        previous_route = routes.get(descriptor.object_id)
        install_route = (previous_route is None or previous_route.node_id not in snapshot.locations
                         or self._node_is_dead(previous_route.node_id))

        def restore_route():
            if install_route:
                if route_present:
                    routes[descriptor.object_id] = previous_route
                else:
                    routes.pop(descriptor.object_id, None)

        try:
            if install_route:
                routes[descriptor.object_id] = replica_result
            recorded = self._owner_table.add_location(
                descriptor.object_id, descriptor.producer_attempt_id, descriptor.node_id, descriptor=replica_result,
            )
        except Exception:
            # add_location may have committed before a local callback failed.
            # Never undo a now-valid route on an assumed no-effect failure.
            latest = self._owner_table.snapshot(descriptor.object_id)
            committed = (latest.current_attempt == descriptor.producer_attempt_id
                         and latest.canonical_stored_result == canonical
                         and latest.state is ObjectState.READY_STORED
                         and descriptor.node_id in latest.locations)
            if not committed:
                restore_route()
            raise
        if not recorded:
            restore_route()
            return outcome(status.STALE_PRODUCER, "location report lost the producer-attempt race")
        if not permitted:
            self._enqueue_inline_gc_check(descriptor.object_id)
            return outcome(status.CUSTODY_ONLY,
                "replica custody retained but task hold is not active; cancel the consumer grant")
        return outcome(status.ALREADY_RECORDED if existed else status.ADDED)

    @staticmethod
    def _owned_drop_reply(
        request: protocol.RequestDropOwnedObject,
        disposition: protocol.DropOwnedObjectDisposition,
        *,
        dropped_node_id: Optional[NodeID] = None,
        failure: Optional[protocol.DropOwnedObjectFailure] = None,
        detail: Optional[str] = None,
    ) -> protocol.RequestDropOwnedObjectReply:
        return protocol.RequestDropOwnedObjectReply(
            request.operation_id, request.object_id, request.owner_worker_id,
            request.requester_worker_id, request.source,
            request.borrower_token, request.expected_owner_attempt,
            request.node_id, disposition, dropped_node_id, failure, detail,
        )

    def request_drop_owned_object(
        self, request: protocol.RequestDropOwnedObject
    ) -> protocol.RequestDropOwnedObjectReply:
        """Validate a borrower, delete one replica, then update owner truth.

        ``operation_id`` binds one immutable request and caches the terminal
        owner reply.  A lost ACK therefore replays no destructive Node RPC and
        never selects a different replica.  Network I/O happens without the
        Core lock; the producer-attempt CAS fences a concurrent reconstruction.
        """

        if not isinstance(request, protocol.RequestDropOwnedObject):
            raise TypeError(
                "request_drop_owned_object expects RequestDropOwnedObject"
            )
        operation_lock = getattr(self, "_owned_drop_lock", None)
        if operation_lock is None:
            operation_lock = threading.Lock()
            self._owned_drop_lock = operation_lock
        with operation_lock:
            return self._request_drop_owned_object_serialized(request)

    def _request_drop_owned_object_serialized(
        self, request: protocol.RequestDropOwnedObject
    ) -> protocol.RequestDropOwnedObjectReply:
        with self._state_lock:
            replies = getattr(self, "_owned_drop_replies", None)
            if replies is None:
                replies = {}
                self._owned_drop_replies = replies
            claims = getattr(self, "_owned_drop_claims", None)
            if claims is None:
                claims = {}
                self._owned_drop_claims = claims
            claimed = claims.get(request.operation_id)
            if claimed is not None and claimed != request:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.REQUEST_CONFLICT,
                    detail="drop operation_id is bound to another request",
                )
            replay = replies.get(request.operation_id)
            if replay is not None:
                # A terminal reply survives credential release and epoch
                # changes, but only for the complete original request.
                if claimed is None:
                    raise RuntimeError("cached drop reply has no request binding")
                return replay
            if request.owner_worker_id != self.worker_id:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.WRONG_OWNER,
                    detail="request targets a different object owner",
                )
            if not self._owner_protocol_open:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.OWNER_STOPPED,
                    detail="object owner is stopped",
                )
            try:
                snapshot = self._owner_table.snapshot(request.object_id)
            except Exception:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.UNKNOWN_OBJECT,
                    detail="owner does not know the requested object",
                )
            token = (request.requester_worker_id, request.borrower_token)
            if token in snapshot.released_borrowed_tokens:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.RELEASED_CREDENTIAL,
                    detail="borrower credential was already released",
                )
            if token not in snapshot.borrowed_tokens:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.INACTIVE_CREDENTIAL,
                    detail="borrower credential is not active",
                )
            if dict(snapshot.borrowed_sources).get(token) != request.source:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.CREDENTIAL_MISMATCH,
                    detail="borrower credential is bound to another source",
                )
            if snapshot.current_attempt != request.expected_owner_attempt:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=(
                        protocol.DropOwnedObjectFailure
                        .EXPECTED_ATTEMPT_MISMATCH
                    ),
                    detail="owner producer attempt changed before drop",
                )
            if snapshot.state is ObjectState.LOST:
                # An exact new operation after a previous drop observes the
                # owner tombstone without requiring Node tombstone knowledge.
                reply = self._owned_drop_reply(
                    request,
                    protocol.DropOwnedObjectDisposition.ALREADY_DROPPED,
                    dropped_node_id=request.node_id,
                )
                claims[request.operation_id] = request
                replies[request.operation_id] = reply
                return reply
            if snapshot.state is not ObjectState.READY_STORED:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.NOT_STORED,
                    detail="drop_object requires a stored object",
                )
            descriptor = self._stored_descriptors.get(request.object_id)
            if descriptor is None:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.NOT_STORED,
                    detail="stored object metadata is incomplete",
                )
            target = request.node_id
            if target is None:
                target = (
                    descriptor.node_id
                    if descriptor.node_id in snapshot.locations
                    else min(snapshot.locations)
                )
            if target not in snapshot.locations:
                return self._owned_drop_reply(
                    request, protocol.DropOwnedObjectDisposition.FAILED,
                    failure=protocol.DropOwnedObjectFailure.UNKNOWN_REPLICA,
                    detail="requested node has no owner-advertised replica",
                )
            claims[request.operation_id] = request
            drop = protocol.DropObjectReplica(
                request.object_id, request.expected_owner_attempt,
                self.worker_id, target, descriptor.checksum,
            )

        try:
            node_reply = self._rpc(
                self._resolve_node_address(target),
                _DROP_OBJECT_REPLICA_HANDLER, drop,
            )
        except Exception as exc:
            raise OwnerUnavailableError(
                "owner could not resolve replica drop: {}".format(exc)
            ) from exc
        valid = (
            isinstance(node_reply, protocol.DropObjectReplicaReply)
            and node_reply.object_id == drop.object_id
            and node_reply.producer_attempt_id == drop.producer_attempt_id
            and node_reply.owner_worker_id == drop.owner_worker_id
            and node_reply.node_id == drop.node_id
            and node_reply.checksum == drop.checksum
        )
        if not valid or node_reply.status not in (
            protocol.DropObjectReplicaStatus.DROPPED,
            protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        ):
            return self._owned_drop_reply(
                request, protocol.DropOwnedObjectDisposition.FAILED,
                failure=protocol.DropOwnedObjectFailure.NODE_REJECTED,
                detail=(
                    getattr(node_reply, "error", None)
                    or "Node rejected or malformed the replica drop"
                ),
            )

        with self._completion:
            removed = self._owner_table.remove_location(
                request.object_id, request.expected_owner_attempt, target
            )
            if removed:
                current = self._stored_descriptors.get(request.object_id)
                locations = self._owner_table.snapshot(
                    request.object_id
                ).locations
                if current is not None and current.node_id == target and locations:
                    self._stored_descriptors[request.object_id] = replace(
                        current, node_id=min(locations)
                    )
            disposition = (
                protocol.DropOwnedObjectDisposition.DROPPED
                if node_reply.status is protocol.DropObjectReplicaStatus.DROPPED
                else protocol.DropOwnedObjectDisposition.ALREADY_DROPPED
            )
            reply = self._owned_drop_reply(
                request, disposition, dropped_node_id=target
            )
            # Cache only after owner metadata and the complete reply agree.
            self._owned_drop_replies[request.operation_id] = reply
            self._completion.notify_all()
            return reply

    def drop_object(
        self, ref: ObjectRef, node_id: Optional[NodeID] = None
    ) -> bool:
        """Teaching failpoint: delete one stored replica and report its loss.

        This is intentionally a debug operation, not an object-lifetime API.
        The request carries the owner's current producer attempt, owner ID, and
        checksum so a delayed drop cannot delete a reconstructed replica that
        reused the same logical ``ObjectID``.

        Return ``True`` only when this call removed physical bytes.  Replaying
        a drop after the replica is already absent returns ``False``.
        """

        self._validate_ref(ref, allow_foreign=True)
        if ref.owner_worker_id != self.worker_id:
            return self._drop_borrowed_object(ref, node_id)
        if node_id is not None and not isinstance(node_id, NodeID):
            raise TypeError("node_id must be a NodeID or None")

        with self._state_lock:
            snapshot = self._owner_table.snapshot(ref.object_id)
            if snapshot.state is ObjectState.LOST:
                return False
            if snapshot.state is not ObjectState.READY_STORED:
                raise ValueError(
                    "drop_object requires an object-store-backed ObjectRef"
                )
            descriptor = self._stored_descriptors.get(ref.object_id)
            if descriptor is None:
                raise SystemTaskError(
                    "stored object has no integrity descriptor for debug drop"
                )
            if descriptor.owner_worker_id != self.worker_id:
                raise SystemTaskError(
                    "stored object descriptor names a different owner"
                )
            if not isinstance(snapshot.current_attempt, AttemptID):
                raise SystemTaskError(
                    "stored object has no producing attempt for debug drop"
                )
            locations = snapshot.locations
            target_node_id = node_id
            if target_node_id is None:
                target_node_id = (
                    descriptor.node_id
                    if descriptor.node_id in locations
                    else min(locations)
                )
            if target_node_id not in locations:
                raise ValueError(
                    "requested node does not hold an owner-advertised replica"
                )
            request = protocol.DropObjectReplica(
                object_id=ref.object_id,
                producer_attempt_id=snapshot.current_attempt,
                owner_worker_id=self.worker_id,
                node_id=target_node_id,
                checksum=descriptor.checksum,
            )

        # Network I/O never holds the owner lock.  Both the Node and the owner
        # independently fence the attempt when the reply returns.
        reply = self._rpc(
            self._resolve_node_address(target_node_id),
            _DROP_OBJECT_REPLICA_HANDLER,
            request,
        )
        if (
            not isinstance(reply, protocol.DropObjectReplicaReply)
            or reply.object_id != ref.object_id
            or reply.producer_attempt_id != request.producer_attempt_id
            or reply.owner_worker_id != request.owner_worker_id
            or reply.node_id != target_node_id
            or reply.checksum != request.checksum
        ):
            raise SystemTaskError(
                "Node returned an invalid object-replica drop reply"
            )
        if reply.status not in (
            protocol.DropObjectReplicaStatus.DROPPED,
            protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        ):
            raise SystemTaskError(reply.error or "Node rejected object-replica drop")

        with self._completion:
            removed = self._owner_table.remove_location(
                ref.object_id, request.producer_attempt_id, target_node_id
            )
            if removed:
                current = self._stored_descriptors.get(ref.object_id)
                remaining = self._owner_table.snapshot(ref.object_id).locations
                if (
                    current is not None
                    and current.node_id == target_node_id
                    and remaining
                ):
                    # ResultDescriptor stores one preferred fetch location; the
                    # logical owner remains authoritative for the full set.
                    self._stored_descriptors[ref.object_id] = replace(
                        current, node_id=min(remaining)
                    )
                self._completion.notify_all()

        self._emit(
            "object_replica_dropped",
            object_id=str(ref.object_id),
            attempt_id=str(request.producer_attempt_id),
            node_id=str(target_node_id),
            physical_drop=reply.dropped,
            owner_location_removed=removed,
        )
        return reply.dropped

    def _drop_borrowed_object(
        self, ref: ObjectRef, node_id: Optional[NodeID]
    ) -> bool:
        if node_id is not None and not isinstance(node_id, NodeID):
            raise TypeError("node_id must be a NodeID or None")
        if ref.owner_address is None or ref.borrower_token is None:
            raise ValueError(
                "foreign ObjectRef is detached from its owner protocol"
            )
        capability = self._active_borrower_capability(ref)
        current = self._borrow_rpc(
            ref.owner_address, _GET_OWNED_OBJECT_HANDLER,
            protocol.GetOwnedObject(
                ref.object_id, ref.owner_worker_id, self.worker_id,
                ref.borrower_token,
            ),
        )
        self._validate_owned_object_reply_identity(
            ref, current, borrower_worker_id=self.worker_id
        )
        if not current.accepted:
            raise BorrowedObjectUnavailableError(
                current.detail or "object owner rejected the borrower"
            )
        attempt = current.current_attempt
        if not isinstance(attempt, AttemptID):
            raise ValueError(
                "drop_object requires an owner object with a producer attempt"
            )
        request = protocol.RequestDropOwnedObject(
            "borrowed-drop:{}:{}".format(self.worker_id, uuid.uuid4().hex),
            ref.object_id, ref.owner_worker_id, self.worker_id,
            capability.source, ref.borrower_token, attempt, node_id,
        )
        reply = self._borrow_rpc(
            ref.owner_address, _REQUEST_DROP_OWNED_OBJECT_HANDLER, request
        )
        if (
            not isinstance(reply, protocol.RequestDropOwnedObjectReply)
            or reply.operation_id != request.operation_id
            or reply.object_id != request.object_id
            or reply.owner_worker_id != request.owner_worker_id
            or reply.requester_worker_id != request.requester_worker_id
            or reply.source != request.source
            or reply.borrower_token != request.borrower_token
            or reply.expected_owner_attempt != request.expected_owner_attempt
            or reply.requested_node_id != request.node_id
        ):
            raise SystemTaskError(
                "object owner returned an invalid drop acknowledgement"
            )
        if reply.disposition is protocol.DropOwnedObjectDisposition.DROPPED:
            return True
        if (
            reply.disposition
            is protocol.DropOwnedObjectDisposition.ALREADY_DROPPED
        ):
            return False
        if reply.failure in (
            protocol.DropOwnedObjectFailure.INACTIVE_CREDENTIAL,
            protocol.DropOwnedObjectFailure.RELEASED_CREDENTIAL,
            protocol.DropOwnedObjectFailure.CREDENTIAL_MISMATCH,
            protocol.DropOwnedObjectFailure.UNKNOWN_OBJECT,
        ):
            raise BorrowedObjectUnavailableError(
                reply.detail or "borrowed object capability is unavailable"
            )
        if reply.failure in (
            protocol.DropOwnedObjectFailure.NOT_STORED,
            protocol.DropOwnedObjectFailure.UNKNOWN_REPLICA,
        ):
            raise ValueError(reply.detail or "drop_object target is not stored")
        raise SystemTaskError(reply.detail or "object owner rejected debug drop")

    def submit(
        self,
        function: RemoteFunctionDefinition,
        args: Tuple[object, ...],
        kwargs: Mapping[str, object],
        resources: ResourceVector,
        *,
        max_retries: int = 0,
        num_returns: int = 1,
        placement_group_scheduling_key: Optional[
            protocol.PlacementGroupSchedulingKey
        ] = None,
    ) -> ObjectRef | tuple[ObjectRef, ...]:
        """Prepare a submission, then return without waiting for execution.

        Serialization, lifted-argument seal and reference-lifetime handoffs
        finish synchronously before admission and may raise here. Accepted
        work waits for dependency readiness and executes asynchronously; get
        exposes its value/error, while wait observes readiness separately.
        """

        pending, refs = self._register_submission(
            function, args, kwargs, resources, max_retries=max_retries,
            num_returns=num_returns,
            placement_group_scheduling_key=placement_group_scheduling_key,
            _enqueue=True,
        )
        self._emit(
            "task_submitted",
            task_id=str(pending.spec.task_id),
            attempt_id=str(pending.spec.attempt_id),
            object_id=str(pending.object_id),
            output_ids=tuple(str(value) for value in pending.output_ids),
        )
        return refs

    def _register_submission(
        self,
        function: RemoteFunctionDefinition,
        args: Tuple[object, ...],
        kwargs: Mapping[str, object],
        resources: ResourceVector,
        *,
        max_retries: int = 0,
        num_returns: int = 1,
        placement_group_scheduling_key: Optional[
            protocol.PlacementGroupSchedulingKey
        ] = None,
        _enqueue: bool = False,
    ) -> tuple[_PendingTask, ObjectRef]:
        """Build, retain, then atomically publish one submitted task.

        The initial lock reserves logical identity.  Foreign owner RPCs happen
        with no Core lock held.  A final lock rechecks shutdown admission and
        publishes owner state plus the queue item as one local transaction.
        """

        num_returns = validate_num_returns(num_returns)
        with self._state_lock:
            if not getattr(self, "_accepting", True):
                raise RuntimeError("the mini-Ray runtime is shutting down")
            execution_context = current_execution_context()
            if execution_context is None:
                task_id = TaskID.derive(
                    self.job_id, self.driver_task_id, self._submission_index
                )
                parent_task_id = self.driver_task_id
                self._submission_index += 1
            else:
                if execution_context.job_id != self.job_id:
                    raise ValueError(
                        "execution context belongs to a different job"
                    )
                task_id = execution_context.next_task_id()
                parent_task_id = execution_context.parent_task_id
            self._inflight_submissions = (
                getattr(self, "_inflight_submissions", 0) + 1
            )
        protected: list[ObjectID] = []
        foreign_guards: list[_ForeignDependencyGuard] = []
        nested_local_holds: list[ObjectID] = []
        nested_foreign_guards: list[_ForeignDependencyGuard] = []
        installed_lineage_holds: list[tuple[ObjectID, str]] = []
        output_registration = None
        recovery_registration_attempted = False
        foreign_lineage_registration: ForeignLineageTask | None = None
        foreign_lineage_preexisting = False
        installed_output_edges: list[LineageReferenceEdge] = []
        installed_waiters: list[ObjectID] = []
        prepared_refs: tuple[tuple[ObjectRef, object], ...] = ()
        bound_refs: tuple[ObjectRef, ...] = ()
        enqueued = False
        accepted_incremented = False
        try:
            attempt_id = AttemptID(task_id, 0)
            # The hold origin identifies this admitted logical execution.  A
            # SYSTEM retry changes ``spec.attempt_id`` but preserves these exact
            # credentials; a later lineage reconstruction mints a new origin.
            submitted_hold = _task_reference_hold(
                protocol.TaskReferenceHoldKind.SUBMITTED,
                self.worker_id,
                task_id,
                attempt_id,
            )
            retained_hold = _task_reference_hold(
                protocol.TaskReferenceHoldKind.RETAINED,
                self.worker_id,
                task_id,
                attempt_id,
            )
            output_ids = (ObjectID.for_task(task_id, 0),)
            object_id = output_ids[0]
            prepared_refs = self._prepare_local_object_refs(output_ids)
            nested_transfers: dict[
                tuple[ObjectID, WorkerID], protocol.NestedReferenceTransfer
            ] = {}
            nested_sources: dict[
                tuple[ObjectID, WorkerID], ObjectRef
            ] = {}

            def export_nested_ref(value: object) -> protocol.NestedReferenceTransfer:
                if not isinstance(value, ObjectRef):
                    raise TypeError("nested task references must be ObjectRef values")
                if value.closed:
                    raise ValueError("cannot submit a closed nested ObjectRef")
                key = value.object_id, value.owner_worker_id
                previous_source = nested_sources.setdefault(key, value)
                if (
                    previous_source.owner_address != value.owner_address
                ):
                    raise ValueError(
                        "duplicate nested ObjectRef has conflicting owner credentials"
                    )
                previous = nested_transfers.get(key)
                if previous is not None:
                    return previous
                owner_address = value.owner_address
                if owner_address is None:
                    raise ValueError(
                        "nested ObjectRef has no reachable owner endpoint"
                    )
                hold = (
                    submitted_hold
                    if value.owner_worker_id == self.worker_id
                    else retained_hold
                )
                transfer = protocol.NestedReferenceTransfer(
                    value.object_id, value.owner_worker_id, owner_address,
                    hold,
                )
                # The first serialized occurrence is the handoff point.  Install
                # the logical hold before returning its manifest index so a
                # concurrent close of the source handle cannot create a lifetime
                # gap.  A later serialization/admission failure rolls this exact
                # hold back through the common cleanup path below.
                if value.owner_worker_id == self.worker_id:
                    self._object_waiter(value.object_id)
                    self._owner_table.add_submitted_reference(
                        value.object_id, submitted_hold
                    )
                    nested_local_holds.append(value.object_id)
                else:
                    if value.borrower_token is None:
                        raise ValueError(
                            "foreign nested ObjectRef is detached from its "
                            "owner protocol"
                        )
                    guard = _ForeignDependencyGuard(
                        value.object_id, value.owner_worker_id, owner_address,
                        self.worker_id, value.borrower_token, retained_hold,
                    )
                    nested_foreign_guards.append(guard)
                    self._retain_foreign_dependency_guard(guard)
                nested_transfers[key] = transfer
                return transfer

            # The teaching API requires explicit put for large by-value inputs.
            # Encoding is still once per argument; partial nested holds use the
            # common submission rollback if the cumulative budget is exceeded.
            inline_bytes = 0
            inline_budget = getattr(
                self, "inline_threshold", _DEFAULT_INLINE_THRESHOLD_BYTES
            )

            def encode_with_inline_budget(value: object) -> protocol.TaskArg:
                nonlocal inline_bytes
                argument = encode_task_argument(
                    value, serializer="cloudpickle",
                    export_nested_ref=export_nested_ref,
                )
                if not isinstance(argument, protocol.InlineArg):
                    return argument
                next_inline_bytes = inline_bytes + len(argument.data)
                if next_inline_bytes > inline_budget:
                    raise ValueError(
                        "task arguments exceed the inline budget; use ray.put(value) "
                        "and pass its ObjectRef for large inputs"
                    )
                inline_bytes = next_inline_bytes
                return argument

            encoded_args = tuple(
                encode_with_inline_budget(value) for value in args
            )
            encoded_kwargs = tuple(
                (name, encode_with_inline_budget(value))
                for name, value in kwargs.items()
            )
            spec = protocol.TaskSpec(
                job_id=self.job_id,
                task_id=task_id,
                attempt_id=attempt_id,
                function=function.key,
                args=encoded_args,
                kwargs=encoded_kwargs,
                num_returns=num_returns,
                resources=resources,
                max_retries=max_retries,
                owner_worker_id=self.worker_id,
                parent_task_id=parent_task_id,
                function_definition=function.definition,
                scheduling_key=placement_group_scheduling_key,
            )
            references = top_level_references(
                spec.args + tuple(value for _, value in spec.kwargs)
            )
            lineage_dependencies = tuple(
                reference for reference in references
                if reference.owner_worker_id == self.worker_id
            )
            nested_manifest = nested_references(
                spec.args + tuple(value for _, value in spec.kwargs)
            )
            if {
                (item.object_id, item.owner_worker_id): item
                for item in nested_manifest
            } != nested_transfers:
                raise SystemTaskError(
                    "encoded nested-reference manifest changed submission identity"
                )
            for item in nested_manifest:
                expected_hold = (
                    submitted_hold
                    if item.owner_worker_id == self.worker_id
                    else retained_hold
                )
                if item.hold != expected_hold:
                    raise SystemTaskError(
                        "nested-reference manifest changed its Task hold"
                    )
            # Producer lineage retains every local input named by the
            # canonical TaskSpec.  Top-level refs are readiness dependencies;
            # nested refs are lifetime-only handles.  Both must survive after
            # the initial execution hold is released so a later reconstruction
            # can replay the same immutable argument value.
            lineage_dependency_ids = tuple(dict.fromkeys(
                tuple(
                    reference.object_id
                    for reference in lineage_dependencies
                )
                + tuple(
                    transfer.object_id
                    for transfer in nested_manifest
                    if transfer.owner_worker_id == self.worker_id
                )
            ))
            input_refs: dict[tuple[ObjectID, WorkerID], ObjectRef] = {}
            for value in tuple(args) + tuple(kwargs.values()):
                if isinstance(value, ObjectRef):
                    key = value.object_id, value.owner_worker_id
                    previous = input_refs.get(key)
                    if previous is None or (
                        (previous.closed or previous.owner_address is None
                         or previous.borrower_token is None)
                        and not value.closed
                        and value.owner_address is not None
                        and value.borrower_token is not None
                    ):
                        input_refs[key] = value
            # Phase 1: owner-authoritative retain ACKs.  No Core lock is held.
            for reference in references:
                if reference.owner_worker_id != self.worker_id:
                    reference_key = (
                        reference.object_id, reference.owner_worker_id
                    )
                    existing_guard = next((
                        guard for guard in nested_foreign_guards
                        if (guard.object_id, guard.owner_worker_id)
                        == reference_key
                    ), None)
                    if existing_guard is not None:
                        foreign_guards.append(existing_guard)
                        continue
                    source = input_refs.get(
                        reference_key
                    )
                    if (
                        source is None
                        or source.closed
                        or source.owner_address is None
                        or source.borrower_token is None
                    ):
                        raise ValueError(
                            "foreign ObjectRef dependency is detached from its "
                            "owner protocol"
                        )
                    guard = _ForeignDependencyGuard(
                        reference.object_id, reference.owner_worker_id,
                        source.owner_address, self.worker_id,
                        source.borrower_token,
                        retained_hold,
                    )
                    foreign_guards.append(guard)
                    self._retain_foreign_dependency_guard(guard)

            # Phase 2: shutdown either wins before this lock or the complete
            # logical task and queue admission become visible together.
            with self._state_lock:
                if not getattr(self, "_accepting", True):
                    raise RuntimeShuttingDownError(
                        "the mini-Ray runtime is shutting down"
                    )
                scheduling_key = spec.scheduling_key
                if scheduling_key is not None:
                    phase = getattr(
                        self, "_placement_group_states", {}
                    ).get((
                        scheduling_key.placement_group_id,
                        scheduling_key.attempt,
                    ))
                    if phase is protocol.PlacementGroupPhaseStatus.LOST:
                        raise PlacementGroupLostError(
                            "placement group became LOST before task publication"
                        )
                    if phase is not protocol.PlacementGroupPhaseStatus.CREATED:
                        detail = "unknown" if phase is None else phase.value
                        raise ValueError(
                            "placement group is not active at task publication: "
                            + detail
                        )
                # All Recovery registration checks precede owner/hold mutation.
                # The following register call is therefore a non-failing commit
                # under the same Core composition lock.
                recovery = self._recovery_manager()
                recovery.validate_register_task(
                    spec, max_retries=max_retries
                )
                output_registration = self._owner_table.validate_register_task_outputs(
                    spec, local_tokens=tuple(
                        token for _ref, token in prepared_refs
                    ),
                )
                # Every remote owner has ACKed its retained hold before this
                # point.  Publish the TaskID-scoped foreign lineage in the same
                # composition stage as local owner/recovery state, before any
                # output or queue item becomes visible.  A duplicate nested +
                # top-level occurrence is merged by semantic roles.
                role_by_key: dict[
                    tuple[ObjectID, WorkerID], ForeignLineageRole
                ] = {}
                guard_by_key: dict[
                    tuple[ObjectID, WorkerID], _ForeignDependencyGuard
                ] = {}
                for guard in nested_foreign_guards:
                    key = guard.object_id, guard.owner_worker_id
                    guard_by_key[key] = guard
                    role_by_key[key] = role_by_key.get(
                        key, ForeignLineageRole(0)
                    ) | ForeignLineageRole.NESTED
                for guard in foreign_guards:
                    key = guard.object_id, guard.owner_worker_id
                    guard_by_key[key] = guard
                    role_by_key[key] = role_by_key.get(
                        key, ForeignLineageRole(0)
                    ) | ForeignLineageRole.TOP_LEVEL
                foreign_edges = tuple(
                    ForeignLineageEdge(
                        task_id, guard.object_id, guard.owner_worker_id,
                        guard.owner_address, guard.borrower_worker_id,
                        guard.hold, role_by_key[key],
                    )
                    for key, guard in sorted(guard_by_key.items())
                )
                if foreign_edges:
                    foreign_lineage_preexisting = (
                        self._foreign_lineage_registry.snapshot(task_id)
                        is not None
                    )
                    foreign_lineage_registration = (
                        self._foreign_lineage_registry.register(
                            task_id, output_ids, foreign_edges
                        )
                    )
                for dependency_id in lineage_dependency_ids:
                    token = _lineage_hold_token(
                        task_id, dependency_id
                    )
                    self._object_waiter(dependency_id)
                    self._owner_table.add_lineage_reference(
                        dependency_id, token
                    )
                    installed_lineage_holds.append(
                        (dependency_id, token)
                    )
                for reference in references:
                    if reference.owner_worker_id == self.worker_id:
                        self._object_waiter(reference.object_id)
                        if reference.object_id not in nested_local_holds:
                            self._owner_table.add_submitted_reference(
                                reference.object_id, submitted_hold
                            )
                        protected.append(reference.object_id)
                self._owner_table.commit_register_task_outputs(
                    output_registration
                )
                for dependency_id, token in installed_lineage_holds:
                    edge = LineageReferenceEdge(
                        object_id, dependency_id, token
                    )
                    self._owner_table.add_outgoing_lineage_edge(object_id, edge)
                    installed_output_edges.append(edge)
                recovery_registration_attempted = True
                recovery.register_task(
                    spec, max_retries=max_retries
                )
                for output_id in output_ids:
                    self._objects[output_id] = _ObjectWaiter(threading.Event())
                    installed_waiters.append(output_id)
                bound_refs = self._bind_prepared_local_object_refs(prepared_refs)
                refs = bound_refs
                pending = _PendingTask(
                    object_id=object_id,
                    spec=spec,
                    protected_dependencies=tuple(protected),
                    dependency_hold=submitted_hold,
                    foreign_dependency_guards=tuple(foreign_guards),
                    nested_local_holds=tuple(
                        object_id for object_id in nested_local_holds
                        if object_id not in protected
                    ),
                    nested_foreign_guards=tuple(
                        guard for guard in nested_foreign_guards
                        if guard not in foreign_guards
                    ),
                )
                if _enqueue:
                    self._install_task_finish_barrier_locked(pending)
                    self._accepted_task_count += 1
                    accepted_incremented = True
                    self._submissions.put(pending)
                    enqueued = True
        except BaseException:
            # The queue operation is the final commit point.  A failed put is
            # assumed not to have accepted the item (Queue.put_nowait semantics
            # for the unbounded runtime queue); never mutate a successfully
            # enqueued task behind the coordinator's back.
            if enqueued:
                raise
            for ref in bound_refs:
                finalizer = ref._finalizer
                if finalizer is not None:
                    finalizer.detach()
                ref._finalizer = None
                ref._local_token = None
                ref._release_done = None
            for output_id in reversed(installed_waiters):
                self._objects.pop(output_id, None)
            if recovery_registration_attempted:
                recovery.abort_registered_task(
                    spec, max_retries=max_retries
                )
            if (
                foreign_lineage_registration is not None
                and not foreign_lineage_preexisting
            ):
                self._foreign_lineage_registry.abort(
                    foreign_lineage_registration
                )
            if output_registration is not None:
                self._owner_table.abort_registered_task_outputs(
                    output_registration,
                    outgoing_lineage_edges=tuple(installed_output_edges),
                )
            if accepted_incremented:
                self._accepted_task_count -= 1
                barriers = getattr(self, "_task_finish_barriers", {})
                for output_id in output_ids:
                    if barriers.get(output_id) is pending:
                        barriers.pop(output_id)
            for dependency_id, token in reversed(installed_lineage_holds):
                if self._owner_table.release_lineage_reference(
                    dependency_id, token
                ):
                    self._enqueue_inline_gc_check(dependency_id)
            for dependency_id in dict.fromkeys(
                protected + nested_local_holds
            ):
                released = self._owner_table.release_submitted_reference(
                    dependency_id, submitted_hold
                )
                if released:
                    self._enqueue_inline_gc_check(dependency_id)
            seen_rollback_guards: set[
                tuple[WorkerID, ObjectID, protocol.TaskReferenceHold]
            ] = set()
            for guard in reversed(foreign_guards + nested_foreign_guards):
                key = self._foreign_guard_key(guard)
                if key in seen_rollback_guards:
                    continue
                seen_rollback_guards.add(key)
                try:
                    self._release_foreign_dependency_guard(guard)
                except Exception:
                    # Release-before-retain tombstones make this best-effort
                    # rollback safe even when a Retain ACK was ambiguous.
                    self._record_orphan_foreign_guard_release(guard)
            raise
        finally:
            # Even a defensive cleanup failure must not leave shutdown
            # waiting forever on a submission that has already resolved.
            with self._completion:
                self._inflight_submissions = (
                    getattr(self, "_inflight_submissions", 1) - 1
                )
                if self._inflight_submissions < 0:
                    raise AssertionError(
                        "CoreWorker in-flight submission count became negative"
                    )
                self._completion.notify_all()
            submissions = getattr(self, "_submissions", None)
            if (
                submissions is not None
                and getattr(self, "_coordinator", None) is not None
                and getattr(self, "_inflight_submissions", 0) == 0
                and (
                    not getattr(self, "_accepting", True)
                    or bool(
                        getattr(self, "_orphan_foreign_guard_releases", {})
                    )
                )
            ):
                submissions.put(_WAKE_COORDINATOR)
        return pending, refs[0]

    def _retain_foreign_dependency_guard(
        self, guard: _ForeignDependencyGuard
    ) -> None:
        request = protocol.RetainOwnedObjectForTask(
            guard.object_id, guard.owner_worker_id, guard.borrower_worker_id,
            guard.borrower_token, guard.hold,
        )
        reply = self._borrow_rpc(
            guard.owner_address, _RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER, request
        )
        if (
            not isinstance(reply, protocol.RetainOwnedObjectForTaskReply)
            or reply.object_id != guard.object_id
            or reply.owner_worker_id != guard.owner_worker_id
            or reply.borrower_worker_id != guard.borrower_worker_id
            or reply.borrower_token != guard.borrower_token
            or reply.hold != guard.hold
        ):
            raise OwnerDiedError(
                "object owner returned an invalid task-retain acknowledgement"
            )
        if not reply.accepted:
            raise OwnerDiedError(
                reply.error or "object owner rejected the task dependency hold"
            )

    def _replace_foreign_lineage_hold_rpc(
        self, address: Address, request: protocol.ReplaceRetainedObjectForTask
    ) -> protocol.ReplaceRetainedObjectForTaskReply:
        reply = self._borrow_rpc(
            address, REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER, request
        )
        if not isinstance(reply, protocol.ReplaceRetainedObjectForTaskReply):
            raise SystemTaskError(
                "object owner returned an invalid retained replacement reply"
            )
        try:
            return replace(reply)
        except Exception as exc:
            raise SystemTaskError(
                "object owner returned a malformed retained replacement reply"
            ) from exc

    def _get_foreign_lineage_object_rpc(
        self, address: Address, request: protocol.GetRetainedOwnedObject
    ) -> protocol.GetRetainedOwnedObjectReply:
        reply = self._borrow_rpc(
            address, _GET_RETAINED_OWNED_OBJECT_HANDLER, request
        )
        if not isinstance(reply, protocol.GetRetainedOwnedObjectReply):
            raise SystemTaskError(
                "object owner returned an invalid retained-object reply"
            )
        try:
            return replace(reply)
        except Exception as exc:
            raise SystemTaskError(
                "object owner returned a malformed retained-object reply"
            ) from exc

    def _request_foreign_lineage_reconstruction_rpc(
        self, address: Address,
        request: protocol.RequestOwnedObjectReconstruction,
    ) -> protocol.RequestOwnedObjectReconstructionReply:
        reply = self._borrow_rpc(
            address, _REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER, request
        )
        if not isinstance(
            reply, protocol.RequestOwnedObjectReconstructionReply
        ):
            raise SystemTaskError(
                "object owner returned an invalid reconstruction reply"
            )
        try:
            return replace(reply)
        except Exception as exc:
            raise SystemTaskError(
                "object owner returned a malformed reconstruction reply"
            ) from exc

    def _release_foreign_lineage_hold_rpc(
        self, address: Address, request: protocol.ReleaseOwnedObjectForTask
    ) -> protocol.ReleaseOwnedObjectForTaskReply:
        reply = self._borrow_rpc(
            address, _RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER, request
        )
        if not isinstance(reply, protocol.ReleaseOwnedObjectForTaskReply):
            raise SystemTaskError(
                "object owner returned an invalid task-release reply"
            )
        try:
            return replace(reply)
        except Exception as exc:
            raise SystemTaskError(
                "object owner returned a malformed task-release reply"
            ) from exc

    def _release_foreign_dependency_guard(
        self, guard: _ForeignDependencyGuard
    ) -> bool:
        request = protocol.ReleaseOwnedObjectForTask(
            guard.object_id, guard.owner_worker_id, guard.borrower_worker_id,
            guard.hold,
        )
        reply = self._borrow_rpc(
            guard.owner_address, _RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER, request
        )
        if (
            not isinstance(reply, protocol.ReleaseOwnedObjectForTaskReply)
            or reply.object_id != guard.object_id
            or reply.owner_worker_id != guard.owner_worker_id
            or reply.borrower_worker_id != guard.borrower_worker_id
            or reply.hold != guard.hold
            or not reply.accepted
        ):
            raise OwnerDiedError(
                getattr(reply, "error", None)
                or "object owner returned an invalid task-release acknowledgement"
            )
        return reply.released

    def _query_foreign_dependency_guard(
        self, guard: _ForeignDependencyGuard
    ) -> protocol.GetRetainedOwnedObjectReply:
        request = protocol.GetRetainedOwnedObject(
            guard.object_id, guard.owner_worker_id, guard.borrower_worker_id,
            guard.hold,
        )
        reply = self._borrow_rpc(
            guard.owner_address, _GET_RETAINED_OWNED_OBJECT_HANDLER, request
        )
        if (
            not isinstance(reply, protocol.GetRetainedOwnedObjectReply)
            or reply.object_id != guard.object_id
            or reply.owner_worker_id != guard.owner_worker_id
            or reply.borrower_worker_id != guard.borrower_worker_id
            or reply.hold != guard.hold
        ):
            raise OwnerDiedError(
                "object owner returned an invalid retained-object reply"
            )
        if not reply.accepted:
            raise OwnerDiedError(
                reply.detail or "object owner rejected the retained task hold"
            )
        return reply

    @staticmethod
    def _raise_for_foreign_dependency_terminal(
        guard: _ForeignDependencyGuard,
        reply: protocol.GetRetainedOwnedObjectReply,
    ) -> None:
        if reply.state is protocol.OwnedObjectState.ERROR:
            assert reply.error is not None
            error = TaskError(
                "{}: {}\n{}".format(
                    reply.error.type_name, reply.error.message,
                    reply.error.traceback,
                )
            )
            error.remote_type = reply.error.type_name  # type: ignore[attr-defined]
            error.remote_message = reply.error.message  # type: ignore[attr-defined]
            error.remote_traceback = reply.error.traceback  # type: ignore[attr-defined]
            raise error
        if reply.state is protocol.OwnedObjectState.LOST:
            raise SystemTaskError(
                "foreign dependency {} is lost".format(guard.object_id)
            )
        if reply.state not in (
            protocol.OwnedObjectState.PENDING,
            protocol.OwnedObjectState.READY_INLINE,
            protocol.OwnedObjectState.READY_STORED,
        ):
            raise OwnerDiedError(
                "object owner returned an unknown retained-object state"
            )

    @staticmethod
    def _foreign_guard_key(
        guard: _ForeignDependencyGuard,
    ) -> tuple[WorkerID, ObjectID, protocol.TaskReferenceHold]:
        return guard.owner_worker_id, guard.object_id, guard.hold

    def _record_orphan_foreign_guard_release(
        self, guard: _ForeignDependencyGuard
    ) -> None:
        with self._state_lock:
            # A committed death is stronger than a retained-hold Release ACK.
            # Check under the same lock used by the death consumer so a failed
            # RPC cannot recreate an orphan retry after the sweep.
            if self._owner_is_dead(guard.owner_worker_id):
                return
            table = getattr(self, "_orphan_foreign_guard_releases", None)
            if table is None:
                table = {}
                self._orphan_foreign_guard_releases = table
            table[self._foreign_guard_key(guard)] = guard
            submissions = getattr(self, "_submissions", None)
        if submissions is not None:
            submissions.put(_WAKE_COORDINATOR)

    def _retry_orphan_foreign_guard_releases(self) -> None:
        with self._state_lock:
            guards = tuple(
                getattr(self, "_orphan_foreign_guard_releases", {}).values()
            )
        for guard in guards:
            if self._owner_is_dead(guard.owner_worker_id):
                with self._state_lock:
                    table = getattr(
                        self, "_orphan_foreign_guard_releases", {}
                    )
                    table.pop(self._foreign_guard_key(guard), None)
                    self._completion.notify_all()
                continue
            try:
                self._release_foreign_dependency_guard(guard)
            except Exception:
                # The owner may have become authoritatively DEAD while the RPC
                # outcome was ambiguous.  Do not retain work that can no longer
                # receive an acknowledgement.
                if self._owner_is_dead(guard.owner_worker_id):
                    with self._state_lock:
                        table = getattr(
                            self, "_orphan_foreign_guard_releases", {}
                        )
                        table.pop(self._foreign_guard_key(guard), None)
                        self._completion.notify_all()
                continue
            with self._state_lock:
                table = getattr(self, "_orphan_foreign_guard_releases", {})
                table.pop(self._foreign_guard_key(guard), None)

    @staticmethod
    def _blocking_notifier() -> object | None:
        context = current_execution_context()
        return None if context is None else context.blocking_notifier

    def _blocking_scope(self) -> object:
        notifier = self._blocking_notifier()
        return (
            notifier.blocking_scope()
            if notifier is not None
            else nullcontext()
        )

    def get(
        self, ref: ObjectRef, timeout: Optional[float] = None,
        *, _blocking_group: Optional[_LazyBlockingGroup] = None,
    ) -> object:
        self._validate_ref(ref, allow_foreign=True)
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        if ref.owner_worker_id != self.worker_id:
            return self._get_borrowed_object(
                ref, timeout, _blocking_group=_blocking_group
            )
        deadline = None if timeout is None else time.monotonic() + timeout
        waiter = self._object_waiter(ref.object_id)
        while True:
            snapshot = self._owner_table.snapshot(ref.object_id)
            if snapshot.state is ObjectState.ERROR:
                assert isinstance(snapshot.error, BaseException)
                raise snapshot.error
            if snapshot.state is ObjectState.READY_INLINE and snapshot.inline_data is not None:
                return self._loads_owned_value(snapshot.inline_data)
            if snapshot.state is ObjectState.READY_STORED:
                try:
                    payload = self._fetch_stored_object(
                        ref.object_id, snapshot, deadline=deadline
                    )
                except _StoredFetchStateChanged:
                    continue
                return self._loads_owned_value(payload)
            if snapshot.state is ObjectState.LOST:
                if (
                    snapshot.output_retirement_id is not None
                    or ref.object_id in getattr(
                        self, "_task_finish_barriers", {}
                    )
                ):
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            "publication retirement did not finish "
                            "before timeout"
                        )
                    with self._completion:
                        # First avoid notifying when the original snapshot is
                        # already obsolete. Never acquire a notifier episode
                        # (or perform its Block/Unblock RPC) under the Core lock.
                        current = self._owner_table.snapshot(ref.object_id)
                        blocked = (
                            current.output_retirement_id is not None
                            or ref.object_id in getattr(
                                self, "_task_finish_barriers", {}
                            )
                        )
                        needs_wait = current.state is ObjectState.LOST and blocked
                    if needs_wait:
                        if _blocking_group is not None:
                            _blocking_group.begin_blocking()
                        with self._blocking_scope():
                            with self._completion:
                                # Publication/finalization can run while the
                                # notifier is entering. Recheck under the
                                # condition lock immediately before its atomic
                                # release-and-wait so that wake is not lost.
                                current = self._owner_table.snapshot(ref.object_id)
                                blocked = (
                                    current.output_retirement_id is not None
                                    or ref.object_id in getattr(
                                        self, "_task_finish_barriers", {}
                                    )
                                )
                                if current.state is ObjectState.LOST and blocked:
                                    remaining = (
                                        None if deadline is None
                                        else deadline - time.monotonic()
                                    )
                                    if remaining is not None and remaining <= 0:
                                        raise TimeoutError(
                                            "publication retirement did not finish "
                                            "before timeout"
                                        )
                                    self._completion.wait(remaining)
                    continue
                if snapshot.producer_task_spec is None:
                    raise UnreconstructableObjectError(
                        "object replica was lost and no replayable producer lineage exists"
                    )
                self._start_or_join_reconstruction(ref.object_id, waiter)
                with self._completion:
                    current = self._owner_table.snapshot(ref.object_id)
                    needs_wait = current.state is ObjectState.LOST
                if needs_wait:
                    # A recursive input may still be finalizing, or a foreign
                    # lineage exchange may be in progress. No START occurred.
                    # Keep the caller's deadline, but enter the whole blocking
                    # episode outside the lock needed by those progress paths.
                    remaining = (
                        None if deadline is None
                        else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            "object {} reconstruction admission did not "
                            "finish before timeout".format(ref.object_id)
                        )
                    if _blocking_group is not None:
                        _blocking_group.begin_blocking()
                    with self._blocking_scope():
                        with self._completion:
                            current = self._owner_table.snapshot(ref.object_id)
                            if current.state is ObjectState.LOST:
                                remaining = (
                                    None if deadline is None
                                    else deadline - time.monotonic()
                                )
                                if remaining is not None and remaining <= 0:
                                    raise TimeoutError(
                                        "object {} reconstruction admission did not "
                                        "finish before timeout".format(ref.object_id)
                                    )
                                self._completion.wait(
                                    _FOREIGN_DEPENDENCY_POLL_SECONDS
                                    if remaining is None else min(
                                        _FOREIGN_DEPENDENCY_POLL_SECONDS, remaining
                                    )
                                )
                continue
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(
                    "object {} was not ready before timeout".format(ref.object_id)
                )
            if _blocking_group is not None:
                _blocking_group.begin_blocking()
            with self._blocking_scope():
                # Entering the aggregate/inner notification episode may wait
                # for another episode or its Node ACK. It consumes this get's
                # budget, not a fresh Event.wait budget. A result signalled
                # during entry still gets the normal owner-state fast path.
                remaining = None if deadline is None else deadline - time.monotonic()
                completed = (
                    waiter.event.is_set()
                    if remaining is not None and remaining <= 0
                    else waiter.event.wait(remaining)
                )
            if not completed:
                raise TimeoutError(
                    "object {} was not ready before timeout".format(ref.object_id)
                )

    def _retire_lost_output_memberships(self, object_id: ObjectID) -> bool:
        """Retire one LOST output before reusing its stable identity.

        One exact plan is retained across RPC ambiguity.  Child holds and the
        physical replicas disappear only with their matching receipts; incoming
        refs and task lineage stay at the owner until a later attempt or normal
        GC. No user function or serializer is invoked by this cleanup.
        """
        from . import enhanced_publication as enhanced

        with self._state_lock:
            if self._has_late_replica_cleanup_locked(object_id):
                self._schedule_late_replica_cleanup_locked()
                return False
            work = getattr(self, "_output_retirement_work", None)
            if work is None:
                work = {}
                self._output_retirement_work = work
            current = work.get(object_id)
            if current is None:
                snapshot = self._owner_table.snapshot(object_id)
                if snapshot.state is not ObjectState.LOST or snapshot.output_publication is None:
                    return True
                if object_id in getattr(self, "_task_finish_barriers", {}):
                    return False
                member = snapshot.output_publication
                plan = self._owner_table.begin_output_publication_retirement(
                    (member,), retirement_id="retire-output:{}".format(uuid.uuid4().hex),
                    replica_locations={member.object_id: (
                        (member.manifest.header.node_incarnation.node_id,)
                        if member.slot.tier is protocol.ResultStorage.OBJECT_STORE else ()
                    )},
                )
                publication = enhanced.TaskPublication(member.manifest, self.owner_address)
                current = {"plan": plan, "child": {}, "replica": {}, 'deaths': {},
                           'publication': publication, 'fence': enhanced.OwnerRetirementReceipt(
                               publication.reference, self.worker_id, plan.retirement_id, enhanced.RetirementReason.RECONSTRUCTION)}
                work[object_id] = current
            tickets = getattr(self, "_output_retirement_tickets", None)
            if tickets is None:
                tickets = set()
                self._output_retirement_tickets = tickets
            if object_id in tickets:
                return False
            tickets.add(object_id)
        plan = current["plan"]
        try:
            self._publication_client().fence(current['publication'], current['fence'])
            routes = {
                (transfer.contained_object_id, transfer.final_hold): transfer.contained_owner_address
                for member in plan.memberships for transfer in member.slot.transfers
            }
            for request in plan.contained_releases:
                if request not in current["child"]:
                    with self._state_lock:
                        death = getattr(self, '_worker_death_records', {}).get(request.owner_worker_id)
                    if death is not None:
                        current['deaths'][death.worker_id] = death
                        current['child'][request] = death
                        continue
                    reply = self._borrow_rpc(routes[(request.object_id, request.hold)], _RELEASE_CONTAINED_REFERENCE_HANDLER, request)
                    if (not isinstance(reply, protocol.ReleaseContainedReferenceReply) or not reply.accepted
                            or (reply.object_id, reply.owner_worker_id, reply.hold) != (request.object_id, request.owner_worker_id, request.hold)):
                        raise SystemTaskError("output retirement child ACK mismatch")
                    current["child"][request] = reply
            for request in plan.replica_drops:
                if request in current["replica"]:
                    continue
                with self._state_lock:
                    death = getattr(self, "_dead_nodes", {}).get(request.node_id)
                if death is not None:
                    current["replica"][request] = getattr(death, "death", death)
                    continue
                reply = self._rpc(self._resolve_node_address(request.node_id), _DROP_OBJECT_REPLICA_HANDLER, request)
                if (not isinstance(reply, protocol.DropObjectReplicaReply)
                        or reply.status not in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
                        or (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum)
                        != (request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum)):
                    raise SystemTaskError("output retirement replica ACK mismatch")
                current["replica"][request] = reply
            self._publication_client().retire(current['publication'],
                [value for value in current['child'].values() if type(value) is protocol.ReleaseContainedReferenceReply],
                current['deaths'].values())
            with self._state_lock:
                if any(self._has_late_replica_cleanup_locked(member.object_id) for member in plan.memberships):
                    self._schedule_late_replica_cleanup_locked()
                    return False
                self._owner_table.complete_output_publication_retirement(
                    plan, released_edges=tuple(current["child"][request] for request in plan.contained_releases),
                    dropped_replicas=tuple(current["replica"][request] for request in plan.replica_drops),
                )
                work.pop(object_id, None)
                self._completion.notify_all()
            return True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            return False
        finally:
            with self._state_lock:
                tickets.discard(object_id)

    def _start_or_join_reconstruction(
        self, object_id: ObjectID, waiter: _ObjectWaiter,
        *, return_requested_outcome: bool = False,
    ) -> Optional[ReconstructionOutcome]:
        """Atomically turn one START plan into a normal coordinator task."""
        prepared_graph_steps: list[object] = []
        foreign_renewals: list[tuple[TaskID, AttemptID]] = []
        with self._state_lock:
            snapshot = self._owner_table.snapshot(object_id)
            if (
                snapshot.state is ObjectState.LOST
                and (object_id in getattr(self, "_task_finish_barriers", {})
                     or self._has_late_replica_cleanup_locked(object_id))
            ):
                return None
        with self._state_lock:
            snapshot = self._owner_table.snapshot(object_id)
            coordinator = self._reconstruction_coordinator()
            if (
                return_requested_outcome
                and snapshot.state is ObjectState.PENDING
            ):
                try:
                    joined = coordinator.request(object_id)
                except ReconstructionRuntimeError as exc:
                    raise SystemTaskError(str(exc)) from exc
                if joined.disposition is not ReconstructionDisposition.JOIN:
                    raise SystemTaskError(
                        "pending reconstruction did not join its active attempt"
                    )
                return coordinator.handoff(joined, lambda _plan: None)
            if snapshot.state is not ObjectState.LOST:
                return None
            producer_spec = snapshot.producer_task_spec
            if producer_spec is not None and producer_spec.scheduling_key is not None:
                phase = getattr(self, "_placement_group_states", {}).get((
                    producer_spec.scheduling_key.placement_group_id,
                    producer_spec.scheduling_key.attempt,
                ))
                if phase is protocol.PlacementGroupPhaseStatus.LOST:
                    raise PlacementGroupLostError(
                        "cannot reconstruct an output from a terminal LOST "
                        "placement-group attempt"
                    )
            coordinator_thread = getattr(self, "_coordinator", None)
            if not self._accepting or (
                coordinator_thread is not None and not coordinator_thread.is_alive()
            ):
                raise RuntimeShuttingDownError(
                    "cannot start reconstruction while CoreWorker is shutting down"
                )
            self._emit(
                "reconstruction_preflight_started",
                object_id=str(object_id),
            )
            try:
                graph = coordinator.preflight_graph(object_id)
            except ReconstructionRuntimeError as exc:
                raise SystemTaskError(str(exc)) from exc
            if any(
                step.action.value == "LOST_RECONSTRUCT"
                and (output_id in getattr(self, "_task_finish_barriers", {})
                     or self._has_late_replica_cleanup_locked(output_id))
                for step in graph.steps for output_id in step.output_ids
            ):
                return None
            started_plans = []
            requested_outcome: Optional[ReconstructionOutcome] = None
            # Preflight provides dependency-first order and performs no mutation.
            # Each request below remains the sole START/JOIN authority for its
            # producer.  Parent tasks enter the ordinary dependency gate and
            # therefore cannot lease a Worker until recovered children are ready.
            for graph_node in graph.steps:
                self._emit(
                    "reconstruction_graph_step",
                    object_id=str(graph_node.object_id),
                    action=graph_node.action.value,
                )
                if graph_node.action.value == "PENDING_JOIN":
                    if object_id in graph_node.output_ids:
                        requested_outcome = coordinator.handoff(
                            coordinator.request(graph_node.object_id), lambda _plan: None
                        )
                    continue
                if (
                    self._owner_table.snapshot(
                        graph_node.object_id
                    ).state is not ObjectState.LOST
                ):
                    continue
                try:
                    outcome = coordinator.preview(graph_node.object_id)
                except ReconstructionRuntimeError as exc:
                    raise SystemTaskError(str(exc)) from exc
                if outcome.disposition is ReconstructionDisposition.JOIN:
                    self._emit(
                        "reconstruction_graph_joined",
                        object_id=str(graph_node.object_id),
                    )
                    if object_id in graph_node.output_ids:
                        requested_outcome = coordinator.handoff(
                            coordinator.request(graph_node.object_id), lambda _plan: None
                        )
                    continue
                if outcome.disposition is ReconstructionDisposition.FAILED:
                    error = outcome.decision.error
                    if isinstance(error, BaseException):
                        raise error
                    raise UnreconstructableObjectError(outcome.decision.reason)
                plan = outcome.plan
                assert plan is not None
                prepared_graph_steps.append((graph_node, outcome))

            # Lifetime exchange is deliberately outside the Core composition
            # lock.  It may block on several owners, but cannot mutate local
            # owner/recovery state.  A READY result is only a proposal; the
            # second lock section below revalidates death, shutdown and both
            # local CAS plans before committing.
        for graph_node, outcome in prepared_graph_steps:
            plan = outcome.plan
            assert plan is not None
            if (
                getattr(self, "_foreign_lineage_registry", None) is not None
                and self._foreign_lineage_registry.snapshot(plan.task_id)
                is not None
            ):
                renewal = self._foreign_lineage_runtime.drive_renewal(
                    plan.task_id, plan.attempt_id
                )
                if (
                    renewal.disposition
                    is ForeignLineageRenewalDisposition.WAITING
                ):
                    return None
                if (
                    renewal.disposition
                    is ForeignLineageRenewalDisposition.FAILED
                ):
                    raise SystemTaskError(
                        renewal.failure
                        or "foreign lineage renewal could not converge"
                    )
                foreign_renewals.append((plan.task_id, plan.attempt_id))

        with self._state_lock:
            if not self._accepting:
                raise RuntimeShuttingDownError(
                    "cannot commit reconstruction while CoreWorker is shutting down"
                )
            if any(
                (output_id in getattr(self, "_task_finish_barriers", {})
                 or self._has_late_replica_cleanup_locked(output_id))
                for step, _prepared in prepared_graph_steps
                for output_id in step.output_ids
            ):
                return None
            # No old output metadata is retired until every graph finish gate
            # and foreign input exchange passed. A blocked descendant must
            # leave the entire earlier graph untouched, not only its attempts.
            for graph_node, outcome in prepared_graph_steps:
                plan = outcome.plan
                assert plan is not None
                if coordinator.preview(graph_node.object_id) != outcome:
                    return None
                if (plan.task_id, plan.attempt_id) in foreign_renewals:
                    self._foreign_lineage_runtime.validate_renewal_ready(plan.task_id, plan.attempt_id)

        for graph_node, _outcome in prepared_graph_steps:
            for output_id in graph_node.output_ids:
                if not self._retire_lost_output_memberships(output_id):
                    return None

        with self._state_lock:
            if not self._accepting:
                raise RuntimeShuttingDownError("cannot commit reconstruction while CoreWorker is shutting down")
            if any(output_id in getattr(self, "_task_finish_barriers", {})
                   or self._has_late_replica_cleanup_locked(output_id)
                   for step, _outcome in prepared_graph_steps for output_id in step.output_ids):
                return None
            # Preflight all assignment-only local transitions before committing
            # any producer. Retirement itself never advances an attempt.
            refreshed_steps = []
            for graph_node, preview in prepared_graph_steps:
                try:
                    prepared = coordinator.prepare(graph_node.object_id)
                except ReconstructionRuntimeError as exc:
                    raise SystemTaskError(str(exc)) from exc
                if prepared.outcome != preview:
                    return None
                refreshed_steps.append((graph_node, prepared))
            for _graph_node, prepared in refreshed_steps:
                plan = prepared.outcome.plan
                assert plan is not None
                if (plan.task_id, plan.attempt_id) in foreign_renewals:
                    self._foreign_lineage_runtime.validate_renewal_ready(plan.task_id, plan.attempt_id)
            for graph_node, prepared in refreshed_steps:
                plan = prepared.outcome.plan
                assert plan is not None
                if (plan.task_id, plan.attempt_id) in foreign_renewals:
                    self._foreign_lineage_runtime.validate_renewal_ready(
                        plan.task_id, plan.attempt_id
                    )
                try:
                    outcome = coordinator.commit_prepared(prepared)
                except ReconstructionRuntimeError as exc:
                    raise SystemTaskError(str(exc)) from exc
                if (plan.task_id, plan.attempt_id) in foreign_renewals:
                    self._foreign_lineage_runtime.complete_renewal(
                        plan.task_id, plan.attempt_id
                    )
                foreign_registry = getattr(
                    self, "_foreign_lineage_registry", None
                )
                record = (
                    None if foreign_registry is None
                    else foreign_registry.snapshot(plan.task_id)
                )
                top_guards: list[_ForeignDependencyGuard] = []
                nested_guards: list[_ForeignDependencyGuard] = []
                if record is not None:
                    for edge in record.edges:
                        guard = _ForeignDependencyGuard(
                            edge.dependency_object_id, edge.owner_worker_id,
                            edge.owner_address, edge.borrower_worker_id,
                            "lineage:{}:{}".format(
                                edge.task_id, edge.dependency_object_id
                            ),
                            edge.hold,
                        )
                        if edge.roles & ForeignLineageRole.TOP_LEVEL:
                            top_guards.append(guard)
                        if (
                            edge.roles & ForeignLineageRole.NESTED
                            and not edge.roles & ForeignLineageRole.TOP_LEVEL
                        ):
                            nested_guards.append(guard)

                def rewrite_foreign(argument: object) -> object:
                    if not isinstance(
                        argument, protocol.InlineArg
                    ):
                        return argument
                    by_key = {
                        (guard.object_id, guard.owner_worker_id): guard
                        for guard in top_guards + nested_guards
                    }
                    return replace(
                        argument, nested_refs=tuple(
                            replace(
                                transfer,
                                hold=by_key[(
                                    transfer.object_id, transfer.owner_worker_id
                                )].hold,
                            )
                            if (
                                transfer.object_id, transfer.owner_worker_id
                            ) in by_key else transfer
                            for transfer in argument.nested_refs
                        )
                    )

                if record is not None:
                    plan = replace(
                        plan, task_spec=replace(
                            plan.task_spec,
                            args=tuple(
                                rewrite_foreign(value)
                                for value in plan.task_spec.args
                            ),
                            kwargs=tuple(
                                (name, rewrite_foreign(value))
                                for name, value in plan.task_spec.kwargs
                            ),
                        )
                    )
                for stale_id in plan.clear_descriptor_ids:
                    self._stored_descriptors.pop(stale_id, None)
                for pending_id in plan.clear_waiter_ids:
                    self._object_waiter(pending_id).event.clear()
                # Reconstruction reopens a previously terminal logical task.
                # Stable ObjectID is intentional, so the finish-once tombstone
                # from the original successful execution must be retired before
                # the new Attempt enters the coordinator.  Install attempt-local
                # execution holds for readiness and lifetime-only inputs at the
                # same time; long-lived lineage holds protect both between runs.
                installed_dependencies: list[ObjectID] = []
                try:
                    for dependency_id in dict.fromkeys(
                        plan.protected_dependencies + plan.nested_local_holds
                    ):
                        self._owner_table.add_submitted_reference(
                            dependency_id, plan.dependency_hold
                        )
                        installed_dependencies.append(dependency_id)
                except BaseException:
                    # Graph preflight and this commit both run under the Core
                    # composition lock, so ordinary local collection cannot
                    # race them.  Keep rollback explicit for owner-table fault
                    # injection and invariant failures: no partial fresh hold
                    # may escape without a queued PendingTask to release it.
                    for dependency_id in reversed(installed_dependencies):
                        self._owner_table.release_submitted_reference(
                            dependency_id, plan.dependency_hold
                        )
                    raise
                getattr(self, "_finished_tasks", set()).discard(plan.task_id)
                getattr(self, "_finishing_tasks", set()).discard(plan.task_id)
                getattr(self, "_active_task_finishes", set()).discard(
                    plan.task_id
                )
                pending = _PendingTask(
                    object_id=plan.object_id,
                    spec=plan.task_spec,
                    protected_dependencies=plan.protected_dependencies,
                    dependency_hold=plan.dependency_hold,
                    nested_local_holds=tuple(
                        dependency_id
                        for dependency_id in plan.nested_local_holds
                        if dependency_id not in plan.protected_dependencies
                    ),
                    foreign_dependency_guards=tuple(top_guards),
                    nested_foreign_guards=tuple(nested_guards),
                )
                self._accepted_task_count += plan.accepted_count_delta
                self._install_task_finish_barrier_locked(pending)
                outcome = coordinator.handoff(
                    outcome, lambda _plan: self._enqueue_reconstruction_task(pending)
                )
                if object_id in graph_node.output_ids:
                    requested_outcome = outcome
                started_plans.append(plan)
            self._completion.notify_all()
            if not started_plans and not any(
                node.action.value == "PENDING_JOIN"
                for node in graph.steps
            ):
                raise SystemTaskError(
                    "recursive reconstruction preflight produced no START/JOIN work"
                )
        for plan in started_plans:
            try:
                self._emit(
                    "object_reconstruction_started",
                    object_id=str(plan.requested_object_id),
                    output_ids=tuple(str(value) for value in plan.output_ids),
                    task_id=str(plan.task_id),
                    attempt_id=str(plan.attempt_id),
                )
            except Exception:
                # Observation cannot erase the already accepted queue fact.
                pass
        if return_requested_outcome and requested_outcome is None:
            raise SystemTaskError(
                "reconstruction admission did not resolve the requested object"
            )
        return requested_outcome

    def _enqueue_reconstruction_task(self, pending: _PendingTask) -> None:
        """Publish one admitted reconstruction to the coordinator queue.

        Keeping this single edge explicit makes concurrent START/JOIN tests
        observe admission without replacing the queue consumed by the live
        coordinator thread.  Runtime callers still use the same FIFO.
        """

        if not isinstance(pending, _PendingTask):
            raise TypeError("reconstruction queue accepts only _PendingTask")
        self._submissions.put(pending)





    def _get_borrowed_object(
        self, ref: ObjectRef, timeout: Optional[float],
        *, _blocking_group: Optional[_LazyBlockingGroup] = None,
    ) -> object:
        if ref.owner_address is None or ref.borrower_token is None:
            raise ValueError(
                "foreign ObjectRef is detached from its owner protocol"
            )
        deadline = None if timeout is None else time.monotonic() + timeout
        deadline_token = (
            None if deadline is None else _RPC_CALL_DEADLINE.set(deadline)
        )
        request = protocol.GetOwnedObject(
            ref.object_id, ref.owner_worker_id, self.worker_id,
            ref.borrower_token,
        )
        blocking_scope = None
        try:
            while True:
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(
                        "object {} was not ready before timeout".format(
                            ref.object_id
                        )
                    )
                reply = self._borrow_rpc_with_deadline(
                    ref.owner_address, _GET_OWNED_OBJECT_HANDLER, request,
                    remaining,
                )
                self._validate_owned_object_reply_identity(
                    ref, reply, borrower_worker_id=self.worker_id
                )
                if not reply.accepted:
                    raise BorrowedObjectUnavailableError(
                        reply.detail or "object owner rejected the borrower"
                    )
                if reply.state is protocol.OwnedObjectState.PENDING:
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError(
                                "object {} was not ready before timeout".format(
                                    ref.object_id
                                )
                            )
                    else:
                        remaining = None
                    if _blocking_group is not None:
                        _blocking_group.begin_blocking()
                    if blocking_scope is None:
                        entered_scope = self._blocking_scope()
                        entered_scope.__enter__()
                        blocking_scope = entered_scope
                    # Notification entry has its own control-RPC bounds, but
                    # cannot grant a fresh owner-poll delay after this deadline.
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            "object {} was not ready before timeout".format(ref.object_id)
                        )
                    _BORROW_POLL_EVENT.wait(
                        0.01 if remaining is None else min(0.01, remaining)
                    )
                    continue
                if reply.state is protocol.OwnedObjectState.READY_INLINE:
                    assert reply.data is not None
                    return self._loads_owned_value(reply.data)
                if reply.state is protocol.OwnedObjectState.ERROR:
                    assert reply.error is not None
                    error = TaskError(
                        "{}: {}\n{}".format(
                            reply.error.type_name, reply.error.message,
                            reply.error.traceback,
                        )
                    )
                    error.remote_type = reply.error.type_name  # type: ignore[attr-defined]
                    error.remote_message = reply.error.message  # type: ignore[attr-defined]
                    error.remote_traceback = reply.error.traceback  # type: ignore[attr-defined]
                    raise error
                if reply.state is protocol.OwnedObjectState.READY_STORED:
                    assert reply.descriptor is not None
                    descriptor = reply.descriptor
                    if (
                        descriptor.object_id != ref.object_id
                        or descriptor.owner_worker_id != ref.owner_worker_id
                    ):
                        raise SystemTaskError(
                            "object owner returned a mismatched stored descriptor"
                        )
                    if _blocking_group is not None:
                        _blocking_group.begin_blocking()
                    if blocking_scope is None:
                        entered_scope = self._blocking_scope()
                        entered_scope.__enter__()
                        blocking_scope = entered_scope
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            "object {} was not ready before timeout".format(
                                ref.object_id
                            )
                        )
                    payload = self._fetch_borrowed_stored_object(
                        descriptor, timeout=remaining
                    )
                    # The Node reply validates the physical bytes, but only the
                    # owner can confirm that this attempt and descriptor remain
                    # current after the fetch.  Re-query with the same borrower
                    # credential and the same public deadline before exposing
                    # the value.
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            "object {} was not ready before timeout".format(
                                ref.object_id
                            )
                        )
                    self._require_owner_alive(ref.owner_worker_id)
                    final = self._borrow_rpc_with_deadline(
                        ref.owner_address, _GET_OWNED_OBJECT_HANDLER, request,
                        remaining,
                    )
                    self._validate_owned_object_reply_identity(
                        ref, final, borrower_worker_id=self.worker_id
                    )
                    if (
                        not final.accepted
                        or final.state is not protocol.OwnedObjectState.READY_STORED
                        or final.current_attempt != descriptor.producer_attempt_id
                        or final.descriptor != descriptor
                    ):
                        # Do not reinterpret a concurrent LOST/reconstruction as
                        # corrupt bytes.  Repeat the outer owner state machine.
                        continue
                    self._require_owner_alive(ref.owner_worker_id)
                    return self._loads_owned_value(payload)
                if reply.state is protocol.OwnedObjectState.LOST:
                    expected_attempt = reply.current_attempt
                    if not isinstance(expected_attempt, AttemptID):
                        raise UnreconstructableObjectError(
                            "borrowed object is LOST without a replayable "
                            "producer attempt"
                        )
                    capability = self._active_borrower_capability(ref)
                    reconstruction = (
                        protocol.RequestOwnedObjectReconstruction(
                            ref.object_id, ref.owner_worker_id, self.worker_id,
                            protocol.BorrowedCredential(
                                capability.source, ref.borrower_token
                            ), ref.borrower_token,
                            expected_attempt,
                        )
                    )
                    reconstruction_reply = self._borrow_rpc_with_deadline(
                        ref.owner_address,
                        _REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER,
                        reconstruction, remaining,
                    )
                    self._validate_owned_reconstruction_reply(
                        reconstruction, reconstruction_reply
                    )
                    if (
                        reconstruction_reply.disposition
                        is protocol.OwnedObjectReconstructionDisposition.FAILED
                    ):
                        if reconstruction_reply.failure in (
                            protocol.OwnedObjectReconstructionFailure.NOT_LOST,
                            protocol.OwnedObjectReconstructionFailure
                            .EXPECTED_ATTEMPT_MISMATCH,
                            protocol.OwnedObjectReconstructionFailure
                            .COLLECTION_IN_PROGRESS,
                        ):
                            # A stale LOST observation or a local finish gate
                            # asks for another owner poll, not a permanent error.
                            # Keep the original public deadline across retries.
                            remaining = (
                                None if deadline is None
                                else deadline - time.monotonic()
                            )
                            if remaining is not None and remaining <= 0:
                                raise TimeoutError(
                                    "object {} was not ready before timeout"
                                    .format(ref.object_id)
                                )
                            if _blocking_group is not None:
                                _blocking_group.begin_blocking()
                            if blocking_scope is None:
                                entered_scope = self._blocking_scope()
                                entered_scope.__enter__()
                                blocking_scope = entered_scope
                            remaining = (
                                None if deadline is None
                                else deadline - time.monotonic()
                            )
                            if remaining is not None and remaining <= 0:
                                raise TimeoutError(
                                    "object {} was not ready before timeout"
                                    .format(ref.object_id)
                                )
                            _BORROW_POLL_EVENT.wait(
                                0.01 if remaining is None
                                else min(0.01, remaining)
                            )
                            continue
                        self._raise_owned_reconstruction_failure(
                            reconstruction_reply
                        )
                    continue
                raise SystemTaskError("object owner returned an unknown state")
        finally:
            if deadline_token is not None:
                _RPC_CALL_DEADLINE.reset(deadline_token)
            if blocking_scope is not None:
                blocking_scope.__exit__(*sys.exc_info())

    def _active_borrower_capability(
        self, ref: ObjectRef
    ) -> protocol.AcquireBorrowedObject:
        """Return the immutable Acquire bound to this still-live handle."""

        assert ref.borrower_token is not None
        key = (
            ref.owner_worker_id, ref.object_id, self.worker_id,
            ref.borrower_token,
        )
        with self._state_lock:
            obligation = getattr(
                self, "_borrowed_release_obligations", {}
            ).get(key)
            if obligation is None or obligation.release_requested:
                raise BorrowedObjectUnavailableError(
                    "borrowed ObjectRef no longer has an active capability"
                )
            acquire = obligation.acquire
            if (
                acquire.object_id != ref.object_id
                or acquire.owner_worker_id != ref.owner_worker_id
                or acquire.borrower_worker_id != self.worker_id
                or acquire.borrower_token != ref.borrower_token
            ):
                raise SystemTaskError(
                    "borrowed ObjectRef capability identity changed"
                )
            return acquire

    @staticmethod
    def _validate_owned_object_reply_identity(
        ref: ObjectRef, reply: object, *, borrower_worker_id: Optional[WorkerID] = None
    ) -> None:
        expected_borrower = (
            borrower_worker_id
            if borrower_worker_id is not None
            else None
        )
        if (
            not isinstance(reply, protocol.GetOwnedObjectReply)
            or reply.object_id != ref.object_id
            or reply.owner_worker_id != ref.owner_worker_id
            or (
                expected_borrower is not None
                and reply.borrower_worker_id != expected_borrower
            )
            or reply.borrower_token != ref.borrower_token
        ):
            raise SystemTaskError(
                "object owner returned an invalid object reply"
            )

    @staticmethod
    def _validate_owned_reconstruction_reply(
        request: protocol.RequestOwnedObjectReconstruction,
        reply: object,
    ) -> None:
        if (
            not isinstance(
                reply, protocol.RequestOwnedObjectReconstructionReply
            )
            or reply.object_id != request.object_id
            or reply.owner_worker_id != request.owner_worker_id
            or reply.requester_worker_id != request.requester_worker_id
            or reply.credential != request.credential
            or reply.borrower_token != request.borrower_token
            or reply.expected_owner_attempt
            != request.expected_owner_attempt
        ):
            raise SystemTaskError(
                "object owner returned an invalid reconstruction reply"
            )

    def _raise_owned_reconstruction_failure(
        self, reply: protocol.RequestOwnedObjectReconstructionReply
    ) -> None:
        failure = reply.failure
        detail = reply.detail or "object owner rejected reconstruction"
        if failure is protocol.OwnedObjectReconstructionFailure.OWNER_DEAD:
            # A remote typed value is not itself the local committed death
            # proof.  Refresh the GCS journal and consult only that fence.
            self._sync_worker_deaths()
            self._require_owner_alive(reply.owner_worker_id)
            raise OwnerUnavailableError(detail)
        if failure in (
            protocol.OwnedObjectReconstructionFailure.PUT_OBJECT,
            protocol.OwnedObjectReconstructionFailure.RETRY_EXHAUSTED,
            protocol.OwnedObjectReconstructionFailure.UNRECONSTRUCTABLE,
        ):
            raise UnreconstructableObjectError(detail)
        if failure in (
            protocol.OwnedObjectReconstructionFailure.UNKNOWN_OBJECT,
            protocol.OwnedObjectReconstructionFailure.INACTIVE_CREDENTIAL,
            protocol.OwnedObjectReconstructionFailure.RELEASED_CREDENTIAL,
            protocol.OwnedObjectReconstructionFailure.CREDENTIAL_MISMATCH,
        ):
            raise BorrowedObjectUnavailableError(detail)
        raise SystemTaskError(detail)

    def _fetch_borrowed_stored_object(
        self, descriptor: protocol.ObjectStoreDescriptor,
        *, timeout: Optional[float] = None,
    ) -> bytes:
        """Fetch bytes directly from the descriptor Node with full fencing."""

        deadline = _RPC_CALL_DEADLINE.get()
        if deadline is None and timeout is not None:
            deadline = time.monotonic() + timeout
        requester_route, requester_node_id = (
            self._requester_route_or_legacy_id(
                "borrowed stored object fetch"
            )
        )
        # The deadline was created immediately above, so the first hop receives
        # the caller's full remaining budget without an unnecessary second
        # clock read.  Subsequent hops always recompute from ``deadline``.
        remaining = timeout
        node_address = self._resolve_node_address_with_timeout_at_route(
            descriptor.node_id, remaining, requester_route
        )
        message = protocol.GetObject(
                object_id=descriptor.object_id,
                requester_node_id=requester_node_id,
                expected_attempt_id=descriptor.producer_attempt_id,
                expected_owner_worker_id=descriptor.owner_worker_id,
                expected_size_bytes=descriptor.size_bytes,
                expected_checksum=descriptor.checksum,
        )
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError(
                "object {} was not ready before timeout".format(
                    descriptor.object_id
                )
            )
        try:
            if remaining is None:
                reply = self._rpc(node_address, _GET_OBJECT_HANDLER, message)
            else:
                connect_timeout = min(
                    _RPC_CONNECT_TIMEOUT_SECONDS, remaining / 2.0
                )
                reply = rpc_request(
                    node_address, _GET_OBJECT_HANDLER, message,
                    connect_timeout=connect_timeout,
                    request_timeout=remaining - connect_timeout,
                    event_sink=getattr(self, "event_sink", None),
                    trace_component="core_worker",
                    deadline=deadline,
                )
        except TransportError as exc:
            if "timed out" in str(exc).lower():
                raise TimeoutError(
                    "object {} was not ready before timeout".format(
                        descriptor.object_id
                    )
                ) from exc
            raise SystemTaskError(
                "stored object Node is unreachable: {}".format(exc)
            ) from exc
        except ProtocolError as exc:
            raise SystemTaskError(
                "Node returned a malformed borrowed object reply"
            ) from exc
        if not isinstance(reply, protocol.GetObjectReply):
            raise SystemTaskError("Node returned an invalid borrowed object reply")
        if (
            reply.object_id != descriptor.object_id
            or reply.node_id != descriptor.node_id
            or not reply.found
            or not reply.sealed
            or reply.data is None
            or reply.producer_attempt_id != descriptor.producer_attempt_id
            or reply.owner_worker_id != descriptor.owner_worker_id
            or reply.size_bytes != descriptor.size_bytes
            or reply.checksum != descriptor.checksum
            or len(reply.data) != descriptor.size_bytes
            or hashlib.sha256(reply.data).hexdigest() != descriptor.checksum
        ):
            raise SystemTaskError(
                reply.error or "borrowed stored object metadata or checksum mismatch"
            )
        return reply.data

    def _resolve_node_address_with_timeout(
        self, node_id: NodeID, timeout: Optional[float],
        *, home_route: Optional[_HomeRoute] = None,
    ) -> Address:
        route = home_route if home_route is not None else self._home_route_snapshot()
        if route is not None and node_id == route.node_id:
            return route.address
        if timeout is None:
            # Preserve the pre-deadline call shape for narrow test doubles.
            # A route known to be remote cannot benefit from the local fast
            # path in ``_resolve_node_address`` anyway.
            return self._resolve_node_address(node_id)
        if timeout <= 0:
            raise TimeoutError("node address lookup deadline expired")
        gcs_address = getattr(self, "gcs_address", None)
        if gcs_address is None:
            raise SystemTaskError(
                "cannot resolve a remote node without a GCS address"
            )
        reply = self._borrow_rpc_with_deadline(
            gcs_address, _GET_NODE_ADDRESS_HANDLER,
            protocol.GetNodeAddress(node_id), timeout,
        )
        if (
            not isinstance(reply, protocol.GetNodeAddressReply)
            or reply.node_id != node_id
        ):
            raise SystemTaskError("GCS returned an invalid node address reply")
        if not reply.found or reply.address is None:
            raise SystemTaskError(
                reply.error or "GCS could not resolve the requested node"
            )
        return reply.address

    def _resolve_node_address_with_timeout_at_route(
        self, node_id: NodeID, timeout: Optional[float],
        route: _HomeRoute | None,
    ) -> Address:
        """Resolve against one captured route, tolerating old test doubles."""

        try:
            return self._resolve_node_address_with_timeout(
                node_id, timeout, home_route=route
            )
        except TypeError as exc:
            if "home_route" not in str(exc):
                raise
            return self._resolve_node_address_with_timeout(node_id, timeout)

    def _borrow_rpc_with_deadline(
        self,
        address: Address, handler: str, message: object,
        timeout: Optional[float],
    ) -> object:
        if getattr(self, "_reference_transport_closed", False):
            raise RuntimeShuttingDownError("managed cluster already exited")
        owner_worker_id = getattr(message, "owner_worker_id", None)
        if isinstance(owner_worker_id, WorkerID):
            self._require_owner_alive(owner_worker_id)
        deadline = _RPC_CALL_DEADLINE.get()
        if deadline is None and timeout is None:
            return self._borrow_rpc(address, handler, message)
        if deadline is None:
            assert timeout is not None
            deadline = time.monotonic() + timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("owner RPC deadline expired")
        connect_timeout = min(
            _RPC_CONNECT_TIMEOUT_SECONDS, remaining / 2.0
        )
        try:
            return rpc_request(
                address, handler, message,
                connect_timeout=connect_timeout,
                request_timeout=remaining - connect_timeout,
                event_sink=getattr(self, "event_sink", None),
                trace_component="core_worker",
                deadline=deadline,
            )
        except TransportError as exc:
            # Reachability and liveness are intentionally separate facts.
            # Best-effort journal convergence may install a committed death
            # fence; absent that proof, connection loss and timeout both mean
            # only that the owner route is currently unavailable.
            if isinstance(owner_worker_id, WorkerID):
                self._sync_worker_deaths()
                self._require_owner_alive(owner_worker_id)
            raise OwnerUnavailableError(
                "object owner is unreachable: {}".format(exc)
            ) from exc
        except ProtocolError as exc:
            raise SystemTaskError(
                "owner returned a malformed protocol reply"
            ) from exc

    def get_many(
        self, refs: Sequence[ObjectRef], timeout: Optional[float] = None
    ) -> list[object]:
        deadline = None if timeout is None else time.monotonic() + timeout
        values = []
        with _LazyBlockingGroup(self._blocking_notifier()) as group:
            for ref in refs:
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                values.append(
                    self.get(ref, remaining, _blocking_group=group)
                )
        return values

    def wait(
        self,
        refs: Sequence[ObjectRef],
        *,
        num_returns: int = 1,
        timeout: Optional[float] = None,
    ) -> Tuple[list[ObjectRef], list[ObjectRef]]:
        refs = tuple(refs)
        if not refs:
            raise ValueError("wait requires at least one ObjectRef")
        if len(set(refs)) != len(refs):
            raise ValueError("wait requires unique ObjectRefs")
        if isinstance(num_returns, bool) or not 1 <= num_returns <= len(refs):
            raise ValueError("num_returns must be between 1 and len(object_refs)")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        for ref in refs:
            self._validate_ref(ref, allow_foreign=True)

        deadline = None if timeout is None else time.monotonic() + timeout
        first_scan = True
        while True:
            ready: list[ObjectRef] = []
            for ref in refs:
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if (
                    not first_scan
                    and remaining is not None
                    and remaining <= 0
                ):
                    break
                if ref.owner_worker_id == self.worker_id:
                    snapshot = self._owner_table.snapshot(ref.object_id)
                    if snapshot.is_ready:
                        ready.append(ref)
                    elif snapshot.state is ObjectState.LOST:
                        if (
                            snapshot.output_retirement_id is not None
                            or ref.object_id in getattr(
                                self, "_task_finish_barriers", {}
                            )
                        ):
                            continue
                        if snapshot.producer_task_spec is None:
                            # ``wait`` reports terminality; ``get`` exposes the
                            # actual unreconstructable-object exception.
                            ready.append(ref)
                        else:
                            self._start_or_join_reconstruction(
                                ref.object_id,
                                self._object_waiter(ref.object_id),
                            )
                    continue
                if self._foreign_ref_ready_for_wait(
                    ref,
                    (
                        remaining
                        if remaining is None or remaining > 0
                        else 0.001
                    ),
                ):
                    ready.append(ref)
            first_scan = False
            if len(ready) >= num_returns:
                selected = ready[:num_returns]
                selected_set = set(selected)
                return selected, [ref for ref in refs if ref not in selected_set]
            remaining = (
                None if deadline is None else deadline - time.monotonic()
            )
            if remaining is not None and remaining <= 0:
                selected = ready[:num_returns]
                selected_set = set(selected)
                return selected, [ref for ref in refs if ref not in selected_set]
            _BORROW_POLL_EVENT.wait(
                0.01 if remaining is None else min(0.01, remaining)
            )

    def _foreign_ref_ready_for_wait(
        self, ref: ObjectRef, remaining: Optional[float]
    ) -> bool:
        """Poll owner metadata only; never fetch inline or stored bytes."""

        if ref.owner_address is None or ref.borrower_token is None:
            raise ValueError(
                "foreign ObjectRef is detached from its owner protocol"
            )
        get_request = protocol.GetOwnedObject(
            ref.object_id, ref.owner_worker_id, self.worker_id,
            ref.borrower_token,
        )
        reply = self._borrow_rpc_with_deadline(
            ref.owner_address, _GET_OWNED_OBJECT_HANDLER,
            get_request, remaining,
        )
        self._validate_owned_object_reply_identity(
            ref, reply, borrower_worker_id=self.worker_id
        )
        if not reply.accepted:
            raise BorrowedObjectUnavailableError(
                reply.detail or "object owner rejected the borrower"
            )
        if reply.state in (
            protocol.OwnedObjectState.READY_INLINE,
            protocol.OwnedObjectState.READY_STORED,
            protocol.OwnedObjectState.ERROR,
        ):
            return True
        if reply.state is protocol.OwnedObjectState.PENDING:
            return False
        if reply.state is not protocol.OwnedObjectState.LOST:
            raise SystemTaskError("object owner returned an unknown state")
        attempt = reply.current_attempt
        if not isinstance(attempt, AttemptID):
            return True
        capability = self._active_borrower_capability(ref)
        request = protocol.RequestOwnedObjectReconstruction(
            ref.object_id, ref.owner_worker_id, self.worker_id,
            protocol.BorrowedCredential(
                capability.source, ref.borrower_token
            ), ref.borrower_token, attempt,
        )
        reconstruction = self._borrow_rpc_with_deadline(
            ref.owner_address,
            _REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER,
            request, remaining,
        )
        self._validate_owned_reconstruction_reply(request, reconstruction)
        if reconstruction.disposition in (
            protocol.OwnedObjectReconstructionDisposition.STARTED,
            protocol.OwnedObjectReconstructionDisposition.JOINED,
        ):
            return False
        if reconstruction.failure in (
            protocol.OwnedObjectReconstructionFailure.NOT_LOST,
            protocol.OwnedObjectReconstructionFailure
            .EXPECTED_ATTEMPT_MISMATCH,
            protocol.OwnedObjectReconstructionFailure.COLLECTION_IN_PROGRESS,
        ):
            return False
        if reconstruction.failure in (
            protocol.OwnedObjectReconstructionFailure.INACTIVE_CREDENTIAL,
            protocol.OwnedObjectReconstructionFailure.RELEASED_CREDENTIAL,
            protocol.OwnedObjectReconstructionFailure.CREDENTIAL_MISMATCH,
            protocol.OwnedObjectReconstructionFailure.UNKNOWN_OBJECT,
            protocol.OwnedObjectReconstructionFailure.OWNER_DEAD,
        ):
            self._raise_owned_reconstruction_failure(reconstruction)
        # Retry exhaustion, put/no-lineage, and a terminal local authority
        # failure are ready-to-observe outcomes.  ``get`` repeats the exact
        # owner request and raises the typed terminal error.
        return True

    @property
    def dispatcher_alive(self) -> bool:
        coordinator = getattr(self, "_coordinator", None)
        dispatchers = getattr(self, "_dispatchers", (self._dispatcher,))
        return bool(
            coordinator is not None
            and coordinator.is_alive()
            and any(dispatcher.is_alive() for dispatcher in dispatchers)
        )

    def shutdown(
        self, timeout: float = 6.0, *, preserve_owner_protocol: bool = False
    ) -> bool:
        """Stop accepting submissions and drain previously accepted work.

        ``timeout`` budgets thread joins and cooperative waits, not cancellation
        or a strict wall-clock return bound; observation and cleanup RPCs retain
        their own timeouts. Admission and the stop sentinel share one lock, so
        the coordinator continues draining accepted tasks and delayed retries.

        Unresolved remote work preserves owner state and exact replay identities.
        Late replies may still complete that work. The timeout path may attempt
        a shutdown error only for pending outputs allowed by the existing
        publication fences; it does not fail every unfinished object.
        False means drain or finalization is incomplete. A later Core shutdown
        call may resume it without reopening submission admission.

        With ``preserve_owner_protocol=True``, True means the drain succeeded
        while owner query/release endpoints remain available. Normal owner
        closure and reference-runtime/trace teardown use ``finalize_shutdown``;
        cluster shutdown defers that commit until its distributed barrier.
        Forced cluster-exit cleanup is separate and does not acknowledge
        unresolved obligations.
        """

        if timeout <= 0:
            raise ValueError("shutdown timeout must be greater than zero")
        deadline = time.monotonic() + timeout
        with self._state_lock:
            first_shutdown = self._accepting
            if first_shutdown:
                self._accepting = False
                self._owner_retain_admission_open = False
                foreign_runtime = getattr(
                    self, "_foreign_lineage_runtime", None
                )
                if foreign_runtime is not None:
                    foreign_runtime.close_admission()
                # Admission and this sentinel are linearized by the same lock.
                # The coordinator exits only after every accepted logical task
                # has reached a terminal state, including delayed retries.
                self._submissions.put(_STOP)

        coordinator = getattr(self, "_coordinator", None)
        if coordinator is not None:
            coordinator.join(max(0.0, deadline - time.monotonic()))
        dispatchers = getattr(self, "_dispatchers", (self._dispatcher,))
        for dispatcher in dispatchers:
            dispatcher.join(max(0.0, deadline - time.monotonic()))
        # A clean drain is also a GCS observation barrier.  This fresh suffix
        # may retire references held by a Worker that exited while the task
        # lanes were joining.  Failure merely keeps shutdown unclean; it never
        # turns GCS reachability into a death proof.
        if not self._sync_node_deaths() or not self._sync_worker_deaths():
            return False
        with self._state_lock:
            protocol_unresolved = bool(
                getattr(self, "_protocol_unresolved", {})
            )
            submission_unresolved = (
                getattr(self, "_inflight_submissions", 0) != 0
            )
            foreign_release_unresolved = bool(
                getattr(self, "_orphan_foreign_guard_releases", {})
                or getattr(self, "_foreign_guard_release_retries", {})
                or getattr(self, "_active_task_finishes", set())
                or getattr(self, "_finishing_tasks", set())
            )
        if (
            protocol_unresolved
            or submission_unresolved
            or foreign_release_unresolved
        ):
            # A remote Worker may already have executed or a cancellation may
            # already have committed, or an owner retain/release may still be
            # converging.  Preserve owner state and keep coordinator/lane/
            # reference infrastructure alive.  A later shutdown call completes
            # ordinary teardown after the exact identities converge.
            return False
        # Local export rollback is itself the operation that removes an
        # incoming contained token.  Drive it before consulting distributed
        # liveness; otherwise the token would make shutdown return early and
        # the exact release obligation would never receive its shutdown pass.
        if (
            self._owner_table.has_active_distributed_references()
            if preserve_owner_protocol
            else self._owner_table.has_active_retained_references()
        ):
            # This Core is the logical owner for another Core's borrower,
            # submitted-task, or contained hold.  Its owner service must stay
            # reachable for query/release and exact replay until every remote
            # token is gone.
            return False
        current = threading.current_thread()
        with self._completion:
            while any(
                thread is not current and thread.is_alive()
                for thread in self._actor_call_threads
            ) or self._inflight_puts or self._inflight_borrow_ops or getattr(
                self, "_inflight_submissions", 0
            ) or getattr(
                self, "_inflight_pg_control_ops", 0
            ) or getattr(
                self, "_actor_control_ops", 0
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._completion.wait(remaining)
        actor_calls_alive = any(
            thread is not current and thread.is_alive()
            for thread in self._actor_call_threads
        )
        with self._state_lock:
            puts_alive = self._inflight_puts != 0
            borrow_ops_alive = self._inflight_borrow_ops != 0
            submissions_alive = getattr(self, "_inflight_submissions", 0) != 0
            pg_control_ops_alive = (
                getattr(self, "_inflight_pg_control_ops", 0) != 0
            )
            actor_control_ops_alive = getattr(self, "_actor_control_ops", 0) != 0
        if pg_control_ops_alive:
            # An admitted create/remove may still own prepare/commit/abort
            # obligations in GCS and NodeManagers.  Preserve every Core/owner
            # service and let cluster shutdown report an unclean drain rather
            # than tearing down the authority it needs to converge.
            return False
        if actor_control_ops_alive:
            # GCS may be between RESTARTING invalidation and ALIVE/DEAD route
            # publication.  Keep the owner endpoint alive until the typed
            # install/query operation finishes.
            return False
        # Work can remain active between the first journal barrier and the
        # local in-flight drain.  Take a second fresh suffix before closing the
        # reference mailbox, then re-evaluate distributed liveness from the
        # owner authority that the records may have changed.
        if not self._sync_node_deaths() or not self._sync_worker_deaths():
            return False
        if (
            self._owner_table.has_active_distributed_references()
            if preserve_owner_protocol
            else self._owner_table.has_active_retained_references()
        ):
            return False
        # A local-handle release enqueues its GC check before signalling the
        # explicit ``ObjectRef.close`` waiter.  Drain that already-admitted
        # suffix before reading the foreign registry, but keep admission open:
        # if a live handle still exists this shutdown must return unclean and a
        # later close still needs a functioning mailbox.
        mailbox = getattr(self, "_reference_mailbox", None)
        if mailbox is not None:
            while mailbox.events.unfinished_tasks:
                if time.monotonic() >= deadline:
                    return False
                _BORROW_POLL_EVENT.wait(0.001)

        # Outgoing foreign lineage can require future local-handle collection.
        # Check it before closing the reference mailbox; an unclean shutdown
        # must leave that mailbox able to accept the final ObjectRef release.
        foreign_runtime = getattr(self, "_foreign_lineage_runtime", None)
        if foreign_runtime is not None:
            for receipt in tuple(
                getattr(
                    self, "_foreign_lineage_collection_receipts", {}
                ).values()
            ):
                self._drive_foreign_lineage_collection(receipt)
            foreign_shutdown = foreign_runtime.drive_shutdown()
            if not foreign_shutdown.complete:
                return False
        if mailbox is not None:
            mailbox.close_admission()
            # Drain every reference release admitted before the shutdown fence
            # before deciding whether GC obligations have converged.
            while mailbox.events.unfinished_tasks:
                if time.monotonic() >= deadline:
                    return False
                _BORROW_POLL_EVENT.wait(0.001)
        gc_clean = self._retry_gc_obligations_for_shutdown()
        if not gc_clean:
            # The reference mailbox remains live so a later shutdown call can
            # retry exact contained-edge identities.  Never discard durable
            # obligations merely to report a clean process exit.
            return False
        if (
            (coordinator is not None and coordinator.is_alive())
            or any(dispatcher.is_alive() for dispatcher in dispatchers)
            or actor_calls_alive
            or puts_alive
            or borrow_ops_alive
            or submissions_alive
            or pg_control_ops_alive
            or actor_control_ops_alive
        ):
            error = SystemTaskError(
                "CoreWorker shutdown timed out before task or actor completion"
            )
            failed_tasks: set[TaskID] = set()
            for object_id in tuple(self._objects):
                try:
                    snapshot = self._owner_table.snapshot(object_id)
                except Exception:
                    continue
                spec = snapshot.producer_task_spec
                if (
                    isinstance(spec, protocol.TaskSpec)
                    and spec.task_id not in failed_tasks
                    and all(
                        self._owner_table.contains(output_id)
                        for output_id in spec.return_ids()
                    )
                ):
                    failed_tasks.add(spec.task_id)
                    self._publish_task_error(
                        _PendingTask(spec.return_ids()[0], spec), error
                    )
                elif not isinstance(spec, protocol.TaskSpec):
                    self._publish_error(
                        object_id, snapshot.current_attempt, error
                    )
            return False
        if preserve_owner_protocol:
            # Incoming cleanup-only location reports remain legal until the
            # cluster's final owner fence. Keep their existing event consumer
            # alive even after the ordinary task lanes have drained.
            return True
        return self.finalize_shutdown(require_distributed_clean=False, timeout=max(0.0, deadline - time.monotonic()))

    def _shutdown_finalizable_locked(
        self, *, require_distributed_clean: bool
    ) -> bool:
        distributed_live = (
            self._owner_table.has_active_distributed_references()
            if require_distributed_clean
            else self._owner_table.has_active_retained_references()
        )
        return not (
            self._accepting
            or getattr(self, "_node_death_removals", {})
            or getattr(self, "_protocol_unresolved", {})
            or getattr(self, "_inflight_submissions", 0)
            or getattr(self, "_inflight_puts", 0)
            or getattr(self, "_put_handoffs", {})
            or getattr(self, "_inflight_borrow_ops", 0)
            or getattr(self, "_inflight_pg_control_ops", 0)
            or getattr(self, "_actor_control_ops", 0)
            or getattr(self, "_object_gc_obligations", {})
            or self._has_late_replica_cleanup_locked()
            or self._owner_table.has_active_output_retirements()
            or getattr(self, "_output_retirement_work", {})
            or getattr(self, "_output_node_cleanup", {})
            or getattr(self, "_task_finish_barriers", {})
            or getattr(self, "_orphan_foreign_guard_releases", {})
            or getattr(self, "_foreign_guard_release_retries", {})
            or getattr(self, "_attempt_borrow_releases", {})
            or getattr(self, "_borrowed_release_obligations", {})
            or getattr(self, "_foreign_lineage_collection_receipts", {})
            or getattr(
                self, "_foreign_lineage_prepared_collection_receipts", {}
            )
            or (
                getattr(self, "_foreign_lineage_runtime", None) is not None
                and self._foreign_lineage_runtime.has_pending_obligations()
            )
            or getattr(self, "_active_task_finishes", set())
            or getattr(self, "_location_handoff_drivers", set())
            or getattr(self, "_finishing_tasks", set())
            or any(
                thread.is_alive()
                for thread in getattr(self, "_actor_call_threads", set())
            )
            or distributed_live
        )

    def can_finalize_shutdown(
        self, *, require_distributed_clean: bool = True
    ) -> bool:
        """Side-effect-free fresh check used by the cluster barrier."""

        with self._completion:
            return not getattr(self, "_reference_transport_closed", False) and (
                not self._owner_protocol_open
                or self._shutdown_finalizable_locked(
                    require_distributed_clean=require_distributed_clean
                )
            )

    @property
    def owner_protocol_closed(self) -> bool:
        """Irreversible ownership cutover, distinct from thread join completion."""
        with self._completion:
            return not self._owner_protocol_open

    def finalize_shutdown(
        self, *, require_distributed_clean: bool = False, timeout: float = 1.0
    ) -> bool:
        """Close owner service only after a successful cluster drain.

        Incoming release handlers and this method share ``_completion``.  The
        final cleanliness check and protocol fence are therefore one local
        transition: no release can be admitted after finalization pretends the
        Core is clean.  Exact replay belongs to the preceding drain phase.
        Stopping the reference consumer happens only after that fence; a timed
        out join can be resumed by another finalize without reopening owners.
        """

        if timeout < 0:
            raise ValueError("finalize timeout must be non-negative")
        deadline = time.monotonic() + timeout
        with self._completion:
            if getattr(self, "_reference_transport_closed", False):
                return False
            already_closed = not self._owner_protocol_open
        # Finalization is the irreversible owner-service fence, so it must not
        # rely on an earlier periodic or drain-round observation.  A fresh GCS
        # suffix is part of this commit barrier.
        if not already_closed and (not self._sync_node_deaths() or not self._sync_worker_deaths()):
            return False
        close_sink = False
        with self._completion:
            if getattr(self, "_reference_transport_closed", False):
                return False
            if self._owner_protocol_open:
                if not self._shutdown_finalizable_locked(
                    require_distributed_clean=require_distributed_clean
                ):
                    return False
                self._owner_protocol_open = False
        references_stopped = self._stop_reference_events(deadline)
        if not references_stopped:
            return False
        with self._completion:
            if getattr(self, "_reference_transport_closed", False):
                return False
            if not self._sink_closed:
                close_sink = True
                self._sink_closed = True
        if close_sink:
            sink = getattr(self, "event_sink", None)
            if sink is not None:
                try:
                    sink.close()
                except Exception:
                    pass
        return True

    def _stop_reference_events(self, deadline: float) -> bool:
        """FIFO-drain every accepted local-reference event by ``deadline``."""

        mailbox = getattr(self, "_reference_mailbox", None)
        thread = getattr(self, "_reference_thread", None)
        if mailbox is None or thread is None:
            return True
        # ``shutdown`` reaches this point only after every durable GC/release
        # obligation has converged.  Fence delayed producers before the FIFO
        # stop sentinel so no Timer can retain this Core or enqueue behind it.
        timers_stopped = self._stop_gc_retry_timers(deadline)
        mailbox.stop()
        thread.join(max(0.0, deadline - time.monotonic()))
        if not thread.is_alive():
            runtime_finalizer = getattr(
                self, "_reference_runtime_finalizer", None
            )
            if runtime_finalizer is not None:
                runtime_finalizer.detach()
            return timers_stopped
        return False

    def _stop_gc_retry_timers(self, deadline: float) -> bool:
        """Fence, cancel, and deadline-join every delayed reference event."""

        with self._state_lock:
            self._gc_retry_timers_open = False
            timers = tuple(getattr(self, "_gc_retry_timers", set()))
            getattr(self, "_gc_retry_timers", set()).clear()
        # Timer.cancel and Thread.join must not run under Core state authority:
        # a callback may be in its final self-removal section and need the lock.
        for timer in timers:
            timer.cancel()
        current = threading.current_thread()
        for timer in timers:
            if timer is current:
                continue
            timer.join(max(0.0, deadline - time.monotonic()))
        with self._state_lock:
            # A callback already past its admission check may still be inside
            # ``enqueue_internal`` when this caller's deadline expires.  Keep
            # those threads visible to the next shutdown pass; their ``finally``
            # block will discard themselves after completing.
            survivors = {
                timer for timer in timers
                if timer is not current and timer.is_alive()
            }
            self._gc_retry_timers.update(survivors)
        return not survivors

    def stop_after_cluster_exit(self, timeout: float = 1.0) -> bool:
        """Stop local reference infrastructure after all managed Nodes exited.

        The cluster coordinator alone calls this forced-cleanup boundary. It
        neither acknowledges pending physical work nor marks Core finalized.
        Preserve exact obligations for diagnostics while stopping transport
        retries which can no longer reach any live managed Node.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        with self._completion:
            self._owner_protocol_open = False
            self._gc_retry_timers_open = False
            self._reference_transport_closed = True
        return self._stop_reference_events(deadline)

    def _coordinator_loop(self) -> None:
        """Move admitted work through dependency and retry gates."""

        stop_seen = False
        while True:
            self._poll_node_deaths_best_effort()
            self._poll_worker_deaths_best_effort()
            for put_id in tuple(getattr(self, "_put_handoffs", {})):
                self._drive_put_handoff_cleanup(put_id)
            for output_id in tuple(getattr(self, "_output_retirement_work", {})):
                self._retire_lost_output_memberships(output_id)
            self._retry_orphan_foreign_guard_releases()
            self._retry_due_foreign_guard_releases()
            self._promote_delayed_ready()
            self._promote_unblocked_tasks()
            with self._state_lock:
                complete = (
                    stop_seen
                    and self._accepted_task_count == 0
                    and getattr(self, "_inflight_submissions", 0) == 0
                    and getattr(self, "_inflight_puts", 0) == 0
                    and not getattr(self, "_put_handoffs", {})
                    and not getattr(
                        self, "_orphan_foreign_guard_releases", {}
                    )
                    and not getattr(
                        self, "_foreign_guard_release_retries", {}
                    )
                    and not getattr(self, "_active_task_finishes", set())
                    and not getattr(self, "_finishing_tasks", set())
                )
            if complete:
                for _dispatcher in self._dispatchers:
                    self._ready_tasks.put(_STOP)
                return

            timeout = self._next_coordinator_timeout()
            try:
                item = self._submissions.get(timeout=timeout)
            except queue.Empty:
                continue
            try:
                if item is _STOP:
                    stop_seen = True
                    continue
                if item is _WAKE_COORDINATOR:
                    continue
                if isinstance(item, _DelayedReadyTask):
                    self._schedule_delayed_ready(item)
                    continue
                if isinstance(item, _NodeDeathObserved):
                    self._classify_node_death(item)
                    continue
                assert isinstance(item, _PendingTask)
                self._admit_or_block(item)
            finally:
                self._submissions.task_done()


    def _admit_or_block(self, pending: _PendingTask) -> None:
        if not self._is_current_task_pending(pending):
            self._finish_pending_task(pending)
            return
        try:
            self._raise_if_placement_group_lost(pending)
            if not self._dependencies_ready(pending):
                self._blocked_tasks[pending.task_key] = pending
                return
            prepared, dependencies, _ = self._prepare_task_dependencies(
                pending.spec, pending.foreign_dependency_guards
            )
        except _DependencyBecamePending:
            self._blocked_tasks[pending.task_key] = pending
            return
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._fail_pending_task(pending, exc)
            return
        dependency_ids = tuple(
            str(reference.object_id)
            for reference in top_level_references(
                pending.spec.args
                + tuple(value for _, value in pending.spec.kwargs)
            )
        )
        self._emit(
            "dependency_ready",
            task_id=str(pending.spec.task_id),
            attempt_id=str(pending.spec.attempt_id),
            dependency_ids=dependency_ids,
            dependency_count=len(dependency_ids),
        )
        self._ready_tasks.put(_ReadyTask(pending, prepared, dependencies))

    def _placement_group_phase_for_pending(
        self, pending: _PendingTask
    ) -> protocol.PlacementGroupPhaseStatus | None:
        key = pending.spec.scheduling_key
        if key is None:
            return None
        with self._state_lock:
            return getattr(self, "_placement_group_states", {}).get((
                key.placement_group_id, key.attempt
            ))

    def _raise_if_placement_group_lost(
        self, pending: _PendingTask
    ) -> None:
        if (
            self._placement_group_phase_for_pending(pending)
            is protocol.PlacementGroupPhaseStatus.LOST
        ):
            key = pending.spec.scheduling_key
            assert key is not None
            raise PlacementGroupLostError(
                "placement group {} attempt {} is terminal LOST".format(
                    key.placement_group_id, key.attempt
                )
            )

    def _classify_node_death(self, event: _NodeDeathObserved) -> None:
        """Classify affected attempts without advancing recovery state.

        The dispatch lane that owns an in-flight RPC remains the only component
        allowed to retire that physical attempt.  The coordinator records a
        handoff fact and wakes delayed work; a later lane boundary consumes it.
        """

        node_id = event.death.node_id
        with self._state_lock:
            committed = getattr(self, "_dead_nodes", {}).get(node_id)
            if (
                committed != event.death
                or event.membership_epoch
                > getattr(self, "_membership_epoch", 0)
                or event.membership_epoch < event.death.death_epoch
            ):
                # The queue is not a death authority.  Ignore injected, stale,
                # or not-yet-installed observations rather than classifying an
                # attempt from an uncommitted proof.
                return
            pending_attempts = getattr(self, "_node_death_attempts", None)
            if pending_attempts is None:
                pending_attempts = {}
                self._node_death_attempts = pending_attempts
            for task_id, unresolved in tuple(
                getattr(self, "_protocol_unresolved", {}).items()
            ):
                target = unresolved.target_node_id
                if target != node_id:
                    continue
                pending = unresolved.pending
                # Classification is deliberately side-effect free.  In
                # particular, PG/foreign attempts are not failed here: the
                # dispatch lane that owns their in-flight RPC must cross the
                # death fence, retire the unresolved identity, and publish the
                # explicit unsupported result in one order.
                pending_attempts[task_id] = (event.death, pending)
                if isinstance(unresolved.obligation, _LocationReportState) and unresolved.phase == "location_quarantined":
                    state = unresolved.obligation
                    self._submissions.put(_DelayedReadyTask(
                        _ReadyTask(pending, pending.spec, state.lease_request.dependencies, location_state=state),
                        time.monotonic(),
                    ))
            self._completion.notify_all()
        self._submissions.put(_WAKE_COORDINATOR)

    def _fail_lost_placement_group_attempt(
        self, pending: _PendingTask
    ) -> bool:
        """Publish terminal PG loss without entering retry authority."""

        key = pending.spec.scheduling_key
        assert key is not None
        self._publish_task_error(
            pending,
            PlacementGroupLostError(
                "placement group {} attempt {} is terminal LOST".format(
                    key.placement_group_id, key.attempt
                )
            ),
        )
        return True

    def _consume_node_death_at_lane(
        self, pending: _PendingTask, target_node_id: NodeID | None
    ) -> bool | None:
        """Retire one dead-node attempt at an execution-lane boundary.

        ``None`` means the target has no committed death record.  Otherwise the
        return value is the normal dispatcher ``terminal`` flag produced by the
        existing system-retry authority.  Clearing the unresolved fence and
        advancing RecoveryManager happen in this same lane, so the old
        ``_ReadyTask`` cannot race a watcher that releases logical task holds.
        """

        if target_node_id is None:
            return None
        with self._state_lock:
            if (
                pending.spec.scheduling_key is not None
                and self._placement_group_phase_for_pending(pending)
                is protocol.PlacementGroupPhaseStatus.LOST
            ):
                unresolved = getattr(self, "_protocol_unresolved", {}).get(
                    pending.task_key
                )
                # GCS has fenced the complete group.  If this exact attempt has
                # no remote obligation, fail it immediately even when its own
                # bundle Node survived.  Otherwise the existing cancellation or
                # Node-death path below must first converge that obligation.
                if unresolved is None:
                    return self._fail_lost_placement_group_attempt(pending)
            death = getattr(self, "_dead_nodes", {}).get(target_node_id)
            if death is None:
                return None
            classified = getattr(self, "_node_death_attempts", {}).get(
                pending.task_key
            )
            if classified is not None:
                # This table is a coordinator-to-lane wakeup hint, not a second
                # death authority.  A stale classification must never mask the
                # committed per-Node tombstone selected by ``target_node_id``.
                self._node_death_attempts.pop(pending.task_key, None)
            unresolved = getattr(self, "_protocol_unresolved", {}).get(
                pending.task_key
            )
            if unresolved is not None and unresolved.output_candidate is not None:
                if unresolved.pending.spec.attempt_id != pending.spec.attempt_id:
                    return True
                previous = unresolved.obligation
                cancelled_attempt = (
                    isinstance(previous, _LeaseCancellationState)
                    and previous.target_node_id == target_node_id
                    and previous.request.lease_id == unresolved.output_candidate.lease_id
                    and previous.request.task_id == pending.task_id
                    and previous.request.attempt_id == pending.spec.attempt_id
                    and previous.request.requester_worker_id == self.worker_id
                    and previous.request.scheduling_key == pending.spec.scheduling_key
                )
                if ((isinstance(previous, _LocationReportState) or cancelled_attempt)
                        and previous.terminal_error is not None):
                    # The target death proves all its granted replicas and
                    # execution permission gone, not permission to forget an
                    # earlier cancellation/report failure and rerun user code.
                    # This also covers a validated cancel inventory waiting
                    # for its pure report builder before becoming Location.
                    self._clear_protocol_unresolved(pending)
                    self._publish_task_error(pending, previous.terminal_error)
                    return True
                takeover = (previous if isinstance(previous, _OutputNodeLossObligation) else _OutputNodeLossObligation(
                    unresolved.output_candidate, death,
                    previous.envelope if isinstance(previous, _OutputAdoptionObligation) else None,
                ))
                self._protocol_unresolved[pending.task_key] = replace(unresolved, phase="output_node_loss", obligation=takeover)
                self._ready_tasks.put(_ReadyTask(pending, pending.spec, output_node_loss=takeover))
                self._completion.notify_all()
                return False
            if (
                unresolved is not None
                and unresolved.pending.spec.attempt_id == pending.spec.attempt_id
            ):
                self._protocol_unresolved.pop(pending.task_key, None)
            self._completion.notify_all()

            if pending.spec.scheduling_key is not None:
                identity = (
                    pending.spec.scheduling_key.placement_group_id,
                    pending.spec.scheduling_key.attempt,
                )
                manifests = getattr(self, "_placement_group_manifests", {})
                if identity in manifests:
                    getattr(self, "_placement_group_states", {})[identity] = (
                        protocol.PlacementGroupPhaseStatus.LOST
                    )
                self._publish_task_error(
                    pending,
                    PlacementGroupLostError(
                        "placement-group task belongs to a terminal LOST attempt"
                    ),
                )
                return True
        return self._retry_system_failure(
            pending,
            NodeDiedError(
                "Node {} exited during task attempt {}".format(
                    target_node_id, pending.spec.attempt_id
                )
            ),
        )

    def _dependencies_ready(self, pending: _PendingTask) -> bool:
        for object_id in pending.protected_dependencies:
            snapshot = self._owner_table.snapshot(object_id)
            # ERROR and LOST are terminal dependency states, not "still
            # pending".  Let preparation surface their typed failure so an
            # explicit put -- or an internally lifted by-value argument -- can
            # never leave its consumer blocked forever after replica loss.
            if snapshot.state in (ObjectState.ERROR, ObjectState.LOST):
                continue
            if not snapshot.is_ready:
                return False
        for guard in pending.foreign_dependency_guards:
            reply = self._query_foreign_dependency_guard(guard)
            if reply.state is protocol.OwnedObjectState.PENDING:
                return False
            self._raise_for_foreign_dependency_terminal(guard, reply)
        return True

    def _promote_unblocked_tasks(self) -> None:
        for task_id, pending in tuple(self._blocked_tasks.items()):
            if not self._is_current_task_pending(pending):
                del self._blocked_tasks[task_id]
                self._finish_pending_task(pending)
            else:
                del self._blocked_tasks[task_id]
                self._admit_or_block(pending)

    def _schedule_delayed_ready(
        self, delayed: _DelayedReadyTask
    ) -> None:
        self._capacity_sequence += 1
        self._delayed_ready.put((
            delayed.due_at, self._capacity_sequence, delayed
        ))

    def _promote_delayed_ready(self) -> None:
        now = time.monotonic()
        while True:
            try:
                due_at, sequence, delayed = self._delayed_ready.queue[0]
            except IndexError:
                return
            if due_at > now:
                return
            popped_due, popped_sequence, popped = self._delayed_ready.get_nowait()
            assert (popped_due, popped_sequence) == (due_at, sequence)
            self._delayed_ready.task_done()
            self._ready_tasks.put(popped.ready)

    def _next_coordinator_timeout(self) -> Optional[float]:
        now = time.monotonic()
        candidates: list[float] = []
        try:
            candidates.append(self._delayed_ready.queue[0][0])
        except IndexError:
            pass
        with self._state_lock:
            if getattr(self, "_poll_node_deaths", False):
                candidates.append(self._node_death_next_poll_at)
            if getattr(self, "gcs_address", None) is not None:
                death_poll = getattr(
                    self, "_worker_death_next_poll_at", None
                )
                if death_poll is None:
                    death_poll = now + _WORKER_DEATH_POLL_SECONDS
                    self._worker_death_next_poll_at = death_poll
                candidates.append(death_poll)
            retries = getattr(self, "_foreign_guard_release_retries", {})
            candidates.extend(retry.due_at for retry in retries.values())
            if getattr(self, "_orphan_foreign_guard_releases", {}):
                candidates.append(now + _FOREIGN_DEPENDENCY_POLL_SECONDS)
            if getattr(self, "_output_retirement_work", {}):
                candidates.append(now + _FOREIGN_DEPENDENCY_POLL_SECONDS)
            if getattr(self, "_put_handoffs", {}):
                candidates.append(now + _FOREIGN_DEPENDENCY_POLL_SECONDS)
        if any(
            pending.foreign_dependency_guards
            for pending in self._blocked_tasks.values()
        ):
            candidates.append(now + _FOREIGN_DEPENDENCY_POLL_SECONDS)
        if not candidates:
            # Local object publication and lane termination enqueue an explicit
            # wake marker, so purely local dependencies need no poll interval.
            return None
        return max(0.0, min(candidates) - now)

    def _dispatch_loop(self) -> None:
        while True:
            item = self._ready_tasks.get()
            try:
                if item is _STOP:
                    return
                assert isinstance(item, _ReadyTask)
                pending = item.pending
                terminal = True
                if (
                    item.kind not in (_DispatchKind.OUTPUT_ADOPTION, _DispatchKind.OUTPUT_NODE_LOSS)
                    and not self._is_current_task_pending(pending)
                ):
                    terminal = True
                else:
                    try:
                        if item.kind is _DispatchKind.FRESH:
                            try:
                                self._raise_if_placement_group_lost(pending)
                            except PlacementGroupLostError as exc:
                                # Only fresh admission has no retained remote
                                # work to settle. Publication/recovery replays
                                # must reach their existing authority even if
                                # another bundle has made this PG terminal.
                                self._clear_protocol_unresolved(pending)
                                self._publish_task_error(pending, exc)
                                continue
                        if item.kind is _DispatchKind.CANCEL:
                            terminal = self._resolve_lease_cancellation(
                                pending, item.spec, item.dependencies,
                                item.cancellation,
                            )
                        else:
                            terminal = self._execute(
                                pending, item.spec, item.dependencies,
                                lease_state=item.lease_state,
                                ambiguity_round=item.ambiguity_round,
                                push_state=item.push_state,
                                location_state=item.location_state,
                                output_adoption=item.output_adoption,
                                output_node_loss=item.output_node_loss,
                                system_failure=item.system_failure,
                            )
                    except _TaskFinishing:
                        terminal = True
            finally:
                if isinstance(item, _ReadyTask) and terminal:
                    self._finish_pending_task(item.pending)
                self._ready_tasks.task_done()

    def _install_task_finish_barrier_locked(self, pending: _PendingTask) -> None:
        """Bind output lifetimes to the logical execution's finalizer.

        SYSTEM retry may replace the physical attempt while retaining the
        same task key and input hold.  Reconstruction creates a new hold only
        after the previous barrier is gone.  Synthetic owner-only recovery
        never calls this admission helper and inherits an existing barrier.
        """

        barriers = getattr(self, "_task_finish_barriers", None)
        if barriers is None:
            barriers = {}
            self._task_finish_barriers = barriers
        for object_id in pending.output_ids:
            previous = barriers.get(object_id)
            if previous is not None and (
                previous.task_key != pending.task_key
                or previous.dependency_hold != pending.dependency_hold
                or previous.spec.attempt_id.attempt_number
                > pending.spec.attempt_id.attempt_number
            ):
                raise SystemTaskError(
                    "task finish barrier conflicts with another execution"
                )
        for object_id in pending.output_ids:
            barriers[object_id] = pending

    def _finish_pending_task(self, pending: _PendingTask) -> bool:
        released_dependencies: list[ObjectID] = []
        already_released: set[
            tuple[WorkerID, ObjectID, protocol.TaskReferenceHold]
        ] = set()
        with self._completion:
            finished = getattr(self, "_finished_tasks", None)
            if finished is None:
                finished = set()
                self._finished_tasks = finished
            if pending.task_key in finished:
                return True
            for object_id in pending.output_ids:
                try:
                    snapshot = self._owner_table.snapshot(object_id)
                except UnknownObjectError:
                    continue
                if snapshot.current_attempt != pending.spec.attempt_id:
                    # Retry/reconstruction can be admitted after a lane's
                    # terminal observation. Validate the current output attempt
                    # before closing this older execution's finish barrier.
                    return False
            if pending.task_key in getattr(
                self, "_protocol_unresolved", {}
            ):
                return False
            active = getattr(self, "_active_task_finishes", None)
            if active is None:
                active = set()
                self._active_task_finishes = active
            if pending.task_key in active:
                return False
            finishing = getattr(self, "_finishing_tasks", None)
            if finishing is None:
                finishing = set()
                self._finishing_tasks = finishing
            finishing.add(pending.task_key)
            active.add(pending.task_key)
            previous_retry = getattr(
                self, "_foreign_guard_release_retries", {}
            ).get(pending.task_key)
            if previous_retry is not None:
                already_released.update(previous_retry.released_keys)
        try:
            # A reference can be both a top-level value dependency and a nested
            # handle.  Submission deliberately reuses one logical task hold;
            # terminal cleanup therefore deduplicates the two semantic views.
            foreign_guards = {
                self._foreign_guard_key(guard): guard
                for guard in (
                    pending.foreign_dependency_guards
                    + pending.nested_foreign_guards
                )
            }
            foreign_registry = getattr(
                self, "_foreign_lineage_registry", None
            )
            lineage_record = (
                None if foreign_registry is None
                else foreign_registry.snapshot(pending.task_id)
            )
            if lineage_record is not None:
                # These RETAINED credentials are producer-lineage holds, not
                # attempt-execution holds.  They survive task success/failure
                # and every SYSTEM retry, and are released only after final
                # output metadata collection.
                already_released.update(foreign_guards)
            for key, guard in foreign_guards.items():
                if key in already_released:
                    continue
                if self._owner_is_dead(guard.owner_worker_id):
                    already_released.add(key)
                    continue
                try:
                    self._release_foreign_dependency_guard(guard)
                except Exception:
                    # Death may commit while the Release is in flight.  The
                    # terminal tombstone discharges the hold even though this
                    # physical RPC cannot produce an ACK.
                    if self._owner_is_dead(guard.owner_worker_id):
                        already_released.add(key)
                        continue
                    raise
                already_released.add(key)
        except Exception:
            retry_required = True
            with self._state_lock:
                # Close the last race with the death consumer: an owner may
                # become DEAD after the in-flight Release raised but before
                # this retry record is installed.  Both transitions use this
                # lock, so either the death sweep sees the retry or this pass
                # consumes the already-installed tombstone.
                already_released.update(
                    key for key, guard in foreign_guards.items()
                    if self._owner_is_dead(guard.owner_worker_id)
                )
                retry_required = len(already_released) != len(foreign_guards)
                if not retry_required:
                    getattr(
                        self, "_foreign_guard_release_retries", {}
                    ).pop(pending.task_key, None)
                else:
                    retries = getattr(
                        self, "_foreign_guard_release_retries", None
                    )
                    if retries is None:
                        retries = {}
                        self._foreign_guard_release_retries = retries
                    previous = retries.get(pending.task_key)
                    round_number = 0 if previous is None else previous.round + 1
                    delay = min(
                        _PUSH_RETRY_MAX_SECONDS,
                        _PUSH_RETRY_BASE_SECONDS
                        * (2 ** min(round_number, 5)),
                    )
                    retries[pending.task_key] = _ForeignGuardReleaseRetry(
                        pending, time.monotonic() + delay, round_number,
                        tuple(sorted(already_released)),
                    )
                    self._active_task_finishes.discard(pending.task_key)
                    # _finishing_tasks intentionally remains set: a partial or
                    # ambiguous dependency release is an irrevocable terminal
                    # claim and must fence every future lease/push send.
            if retry_required:
                self._submissions.put(_WAKE_COORDINATOR)
                return False
        with self._completion:
            if pending.task_key in getattr(
                self, "_protocol_unresolved", {}
            ):
                self._active_task_finishes.discard(pending.task_key)
                return False
            if pending.task_key in self._finished_tasks:
                self._active_task_finishes.discard(pending.task_key)
                return True
            # Keep the unresolved check, submitted-hold release, and accepted
            # accounting in one critical section.  A racing first Push send
            # cannot install its shutdown fence between the check and release.
            for dependency_id in dict.fromkeys(
                pending.protected_dependencies + pending.nested_local_holds
            ):
                if pending.dependency_hold is None:
                    raise AssertionError(
                        "dependency objects require a Task reference hold"
                    )
                if self._owner_table.release_submitted_reference(
                    dependency_id, pending.dependency_hold
                ):
                    released_dependencies.append(dependency_id)
            if self._accepted_task_count > 0:
                self._accepted_task_count -= 1
            getattr(self, "_foreign_guard_release_retries", {}).pop(
                pending.task_key, None
            )
            self._finished_tasks.add(pending.task_key)
            self._finishing_tasks.discard(pending.task_key)
            self._active_task_finishes.discard(pending.task_key)
            barriers = getattr(self, "_task_finish_barriers", {})
            inline_released = tuple(
                object_id for object_id, blocked in tuple(barriers.items())
                if blocked.execution == pending.execution
                and blocked.dependency_hold == pending.dependency_hold
            )
            for object_id in inline_released:
                del barriers[object_id]
                if object_id in self._objects:
                    self._objects[object_id].event.set()
            self._completion.notify_all()
        self._submissions.put(_WAKE_COORDINATOR)
        for dependency_id in released_dependencies:
            self._enqueue_inline_gc_check(dependency_id)
        for object_id in inline_released:
            self._enqueue_inline_gc_check(object_id)
        return True

    def _retry_due_foreign_guard_releases(self) -> None:
        now = time.monotonic()
        with self._state_lock:
            due = tuple(
                retry.pending
                for retry in getattr(
                    self, "_foreign_guard_release_retries", {}
                ).values()
                if retry.due_at <= now
            )
        for pending in due:
            self._finish_pending_task(pending)

    def _mark_protocol_unresolved(
        self, pending: _PendingTask, phase: str, obligation: object | None = None,
        *, target_node_id: NodeID | None = None,
        output_candidate: OutputPublicationID | None = None,
    ) -> None:
        """Fence shutdown before a grant/push/cancel outcome can be unknown."""

        if not isinstance(phase, str) or not phase:
            raise ValueError("unresolved protocol phase must be non-empty")
        with self._state_lock:
            is_takeover = isinstance(obligation, _OutputNodeLossObligation)
            if (
                not is_takeover
                and (
                pending.task_key in getattr(self, "_finishing_tasks", set())
                or pending.task_key in getattr(
                    self, "_active_task_finishes", set()
                )
                or pending.task_key in getattr(
                    self, "_foreign_guard_release_retries", {}
                )
                )
            ):
                raise _TaskFinishing(
                    "task owner cleanup has already begun; remote send fenced"
                )
            if (
                not is_takeover
                and pending.task_key in getattr(self, "_finished_tasks", set())
            ):
                raise _TaskFinishing(
                    "task owner cleanup is complete; remote send fenced"
                )
            table = getattr(self, "_protocol_unresolved", None)
            if table is None:
                table = {}
                self._protocol_unresolved = table
            previous = table.get(pending.task_key)
            if (
                previous is not None
                and previous.pending.spec.attempt_id != pending.spec.attempt_id
            ):
                raise RuntimeError(
                    "unresolved protocol identity changed task attempt"
                )
            if obligation is None and previous is not None:
                obligation = previous.obligation
            if target_node_id is None and previous is not None:
                target_node_id = previous.target_node_id
            if output_candidate is None and previous is not None:
                output_candidate = previous.output_candidate
            if isinstance(obligation, _OutputAdoptionObligation):
                output_candidate = obligation.envelope.publication_id
            elif isinstance(obligation, _OutputNodeLossObligation):
                output_candidate = obligation.publication_id
            if output_candidate is not None:
                if type(output_candidate) is not OutputPublicationID or output_candidate.execution != pending.execution:
                    raise SystemTaskError("output candidate changed pending execution")
                output_candidate = replace(output_candidate)
                # A live ordinary Task has one publication domain, already
                # fixed before lease send. Never project its single slot into
                # obsolete INLINE/STORED candidates during a pre-Push failure.
            table[pending.task_key] = _ProtocolUnresolved(
                phase, pending, obligation, target_node_id, output_candidate,
            )
            self._completion.notify_all()

    def _clear_protocol_unresolved(self, pending: _PendingTask) -> bool:
        """Clear only the exact attempt after authoritative convergence."""

        with self._state_lock:
            table = getattr(self, "_protocol_unresolved", None)
            if not table:
                return False
            previous = table.get(pending.task_key)
            if (
                previous is None
                or previous.pending.spec.attempt_id != pending.spec.attempt_id
            ):
                return False
            del table[pending.task_key]
            classified = getattr(self, "_node_death_attempts", {}).get(
                pending.task_key
            )
            if (
                classified is not None
                and classified[1].spec.attempt_id == pending.spec.attempt_id
            ):
                self._node_death_attempts.pop(pending.task_key, None)
            self._completion.notify_all()
            return True

    def _fail_pending_task(self, pending: _PendingTask, exc: BaseException) -> None:
        self._publish_task_error(pending, exc)
        self._emit(
            "task_failed", task_id=str(pending.spec.task_id), error=repr(exc)
        )
        self._finish_pending_task(pending)

    def _execute(
        self,
        pending: _PendingTask,
        prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...] = (),
        *,
        lease_state: Optional[_LeaseRequestState] = None,
        ambiguity_round: int = 0,
        push_state: Optional[_PushRequestState] = None,
        location_state: Optional[_LocationReportState] = None,
        output_adoption: Optional[_OutputAdoptionObligation] = None,
        output_node_loss: Optional[_OutputNodeLossObligation] = None,
        system_failure: Optional[_DeferredSystemFailure] = None,
    ) -> bool:
        if location_state is not None and location_state.grant is None:
            # A cancelled pre-grant inventory has no Worker or execution
            # capability. It enters the same custody driver, never lease/Push.
            if (location_state.inventory is None or location_state.lease_request is None
                    or location_state.cancellation_reply is None or location_state.terminal_error is None):
                raise SystemTaskError("ungranted custody requires an exact cancelled inventory")
            with self._state_lock:
                if not self._is_current_task_pending(pending):
                    return True
                inventory = protocol.revalidate_lease_dependency_inventory(location_state.inventory)
                request = location_state.lease_request
                cancel = location_state.cancellation_reply
                if ((inventory.lease_request != request) or (request.dependencies != dependencies) or (request.task_id != pending.task_id) or (request.attempt_id != pending.spec.attempt_id) or (request.requester_worker_id != self.worker_id) or (request.return_ids != pending.output_ids) or (request.scheduling_key != pending.spec.scheduling_key) or (not cancel.accepted) or (not cancel.cancelled) or (cancel.dependency_inventory != inventory)):
                    raise SystemTaskError("ungranted custody changed its frozen attempt or inventory")
            try:
                self._report_granted_dependency_locations(pending, prepared, dependencies, location_state)
            except _LocationReportRejected as exc:
                if not exc.handoff_complete:
                    raise
                with self._state_lock:
                    marker = self._protocol_unresolved[pending.task_key]
                    error = marker.obligation.terminal_error
                    self._clear_protocol_unresolved(pending)
                    self._publish_task_error(pending, error)
                return True
            except _LocationHandoffTerminal:
                return True
            return False
        if system_failure is not None:
            return self._retry_system_failure(pending, system_failure.error, deferred=system_failure)
        if output_node_loss is not None:
            return self._drive_output_node_loss(pending, output_node_loss)
        if output_adoption is not None:
            return self._drive_output_publication_adoption(pending, output_adoption)
        # The dispatcher passes canonical per-argument TaskSpec values.
        # Store-backed arguments therefore stay byte-free through lease/push.
        prepared_spec = prepared
        if prepared_spec.scheduling_key != pending.spec.scheduling_key:
            raise SystemTaskError(
                "prepared TaskSpec changed the placement-group scheduling key"
            )
        if (push_state is not None) and ((push_state.push.spec.scheduling_key != pending.spec.scheduling_key) or (push_state.grant.scheduling_key != pending.spec.scheduling_key)):
            raise SystemTaskError(
                "PushTask replay changed the placement-group scheduling key"
            )
        if location_state is not None:
            # A logical consumer retry has a new attempt identity and therefore
            # requires a new lease.  Never carry a prior attempt's grant/report
            # state across that boundary.
            if (
                (location_state.grant.task_id != pending.spec.task_id) or (location_state.grant.attempt_id != pending.spec.attempt_id) or (location_state.grant.scheduling_key
                != pending.spec.scheduling_key)
            ):
                raise SystemTaskError(
                    "location-report replay state belongs to another task attempt or scheduling key"
                )
        lease_id = (
            LeaseID.random()
            if (
                lease_state is None
                and push_state is None
                and location_state is None
            )
            else push_state.push.lease_id if push_state is not None
            else location_state.grant.lease_id if location_state is not None
            else lease_state.request.lease_id
        )
        grant: Optional[protocol.GrantWorkerLease] = None
        granting_node_address: Optional[Address] = None
        push: Optional[protocol.PushTask] = None
        push_started = False
        output_candidate = OutputPublicationID(lease_id, pending.execution)
        try:
            if push_state is not None:
                return self._replay_push(pending, push_state)
            if location_state is not None:
                grant = location_state.grant
                granting_node_address = location_state.granting_node_address
                request = location_state.lease_request
                if request is None:
                    raise SystemTaskError(
                        "location-report replay has no frozen lease request"
                    )
                with self._state_lock:
                    marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
                    if marker is not None and isinstance(marker.obligation, _LocationReportState):
                        saved = marker.obligation
                        if (saved.grant != grant or saved.reports != location_state.reports
                                or saved.lease_request != location_state.lease_request
                                or saved.granting_node_address != location_state.granting_node_address
                                or marker.pending.dependency_hold != pending.dependency_hold
                                or marker.pending.protected_dependencies != pending.protected_dependencies):
                            raise SystemTaskError("post-grant replay changed its retained identity")
                        location_state = saved
                    if (pending.execution, grant.lease_id) in getattr(self, "_location_handoff_drivers", set()):
                        return False
                    self._mark_protocol_unresolved(
                        pending, "location_report_replay", location_state,
                        target_node_id=grant.node_id,
                        output_candidate=output_candidate,
                    )
                dead_terminal = self._consume_node_death_at_lane(
                    pending, grant.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                self._validate_granted_dependencies(dependencies, grant)
            elif lease_state is None:
                # Structural credentials are checked before a new request can
                # create remote state. An ambiguous replay must retain its
                # original request even if later local inventory work fails.
                self._validate_location_report_guards(
                    dependencies, pending.foreign_dependency_guards, task_id=pending.task_id,
                )
                scheduling_key = pending.spec.scheduling_key
                target_node_id = (
                    None if scheduling_key is None else scheduling_key.node_id
                )
                home_route = self._home_route_snapshot()
                if home_route is None:
                    if scheduling_key is None:
                        raise NodeDiedError(
                            "task lease request requires a live home Node"
                        )
                    # A PG key is itself a GCS-issued capability for one exact
                    # target.  The requester identity is audit/idempotency data,
                    # not the route used by this targeted first hop.
                    home_route = _HomeRoute(
                        self.node_id, self.node_address,
                        getattr(self, "_membership_epoch", 0),
                    )
                if scheduling_key is None:
                    first_node_id, request_address = self._first_lease_route(
                        dependencies, home_route=home_route,
                    )
                else:
                    first_node_id = scheduling_key.node_id
                    request_address = (
                        home_route.address if first_node_id == home_route.node_id else None
                    )
                request = protocol.RequestWorkerLease(
                    lease_id=lease_id,
                    task_id=pending.spec.task_id,
                    attempt_id=pending.spec.attempt_id,
                    resources=pending.spec.resources,
                    requester_node_id=home_route.node_id,
                    requester_worker_id=self.worker_id,
                    preferred_node_id=first_node_id,
                    target_node_id=target_node_id,
                    dependencies=dependencies,
                    return_ids=pending.output_ids,
                    scheduling_key=scheduling_key,

                    dependency_owner_routes=self._dependency_owner_routes(pending, dependencies),
                    requester_owner_address=self.owner_address,
                )
                if request_address is None:
                    dead_terminal = self._consume_node_death_at_lane(
                        pending, target_node_id
                    )
                    if dead_terminal is not None:
                        return dead_terminal
                    try:
                        try:
                            request_address = self._resolve_node_address(
                                target_node_id, home_route=home_route
                            )
                        except TypeError as exc:
                            # Narrow fixtures may replace the resolver with its
                            # pre-route one-argument form.
                            if "home_route" not in str(exc):
                                raise
                            request_address = self._resolve_node_address(
                                target_node_id
                            )
                    except BaseException:
                        dead_terminal = self._consume_node_death_at_lane(
                            pending, target_node_id
                        )
                        if dead_terminal is not None:
                            return dead_terminal
                        raise
                lease_state = _LeaseRequestState(
                    request,
                    request_address,
                    first_node_id,
                    scheduling_key is None,
                )
            elif location_state is None:
                request = lease_state.request
            canonical_scheduling_key = pending.spec.scheduling_key
            if location_state is None and (
                request.scheduling_key != canonical_scheduling_key
                or (
                    canonical_scheduling_key is not None
                    and (
                        request.target_node_id != canonical_scheduling_key.node_id
                        or lease_state.expected_node_id
                        != canonical_scheduling_key.node_id
                        or lease_state.allow_spillback
                    )
                )
            ):
                raise SystemTaskError(
                    "worker lease routing changed the task scheduling key"
                )
            # A lease request can allocate resources before its reply reaches
            # this Core.  Fence shutdown before the first byte is sent and keep
            # the fence continuously through grant validation and PushTask.
            if location_state is None:
                self._mark_protocol_unresolved(
                    pending, "lease_send",
                    target_node_id=lease_state.expected_node_id,
                    output_candidate=output_candidate,
                )
                dead_terminal = self._consume_node_death_at_lane(
                    pending, lease_state.expected_node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                self._emit(
                "lease_requested", task_id=str(pending.spec.task_id),
                attempt_id=str(pending.spec.attempt_id), lease_id=str(lease_id),
                replay=ambiguity_round > 0,
                )
                try:
                    lease_reply = self._request_lease_hop(lease_state)
                except TransportConnectionError:
                    dead_terminal = self._consume_node_death_at_lane(
                        pending, lease_state.expected_node_id
                    )
                    if dead_terminal is not None:
                        return dead_terminal
                # Every connection attempt failed before request bytes reached
                # the Node, so this hop is authoritatively absent.
                    self._clear_protocol_unresolved(pending)
                    raise
                dead_terminal = self._consume_node_death_at_lane(
                    pending, lease_state.expected_node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                self._validate_lease_reply_identity(request, lease_reply)
                self._clear_protocol_unresolved(pending)

            if location_state is None and isinstance(
                lease_reply, protocol.SpillbackWorkerLease
            ):
                if pending.spec.scheduling_key is not None:
                    raise SystemTaskError(
                        "placement-group worker lease attempted to spill back"
                    )
                target_node_id = lease_reply.target_node_id
                if target_node_id == lease_state.expected_node_id:
                    raise SystemTaskError(
                        "lease node returned a spillback to itself"
                    )
                target_address = lease_reply.target_address
                if target_address is None:
                    try:
                        dead_terminal = self._consume_node_death_at_lane(
                            pending, target_node_id
                        )
                        if dead_terminal is not None:
                            return dead_terminal
                        target_address = self._resolve_node_address(target_node_id)
                    except BaseException:
                        # The first Node returned an explicit spillback and no
                        # target request has been sent, so no grant is hidden.
                        self._clear_protocol_unresolved(pending)
                        raise
                self._emit(
                    "lease_spilled_back",
                    task_id=str(pending.spec.task_id),
                    lease_id=str(lease_id),
                    target_node_id=str(target_node_id),
                )

                # Spillback changes only placement.  The lease, task, and
                # attempt identities remain stable, and target_node_id makes
                # this second hop a final local validation rather than a new
                # cluster-wide scheduling decision.
                targeted_request = replace(
                    request, target_node_id=target_node_id
                )
                target_state = _LeaseRequestState(
                    targeted_request, target_address, target_node_id, False
                )
                # Retain the actual final hop, including its target, for
                # cancellation and any recovered historical grant inventory.
                request, lease_state = targeted_request, target_state
                # Overwrite the phase without clearing the owner-side fence: a
                # target grant may commit as soon as this call starts.
                self._mark_protocol_unresolved(
                    pending, "lease_target_send",
                    target_node_id=target_state.expected_node_id,
                    output_candidate=output_candidate,
                )
                dead_terminal = self._consume_node_death_at_lane(
                    pending, target_state.expected_node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                try:
                    targeted_reply = self._request_lease_hop(target_state)
                except TransportConnectionError:
                    dead_terminal = self._consume_node_death_at_lane(
                        pending, target_state.expected_node_id
                    )
                    if dead_terminal is not None:
                        return dead_terminal
                    self._clear_protocol_unresolved(pending)
                    raise
                dead_terminal = self._consume_node_death_at_lane(
                    pending, target_state.expected_node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                self._validate_lease_reply_identity(
                    targeted_request, targeted_reply
                )
                self._clear_protocol_unresolved(pending)
                if isinstance(
                    targeted_reply, protocol.SpillbackWorkerLease
                ):
                    raise SystemTaskError(
                        "target node attempted a second spillback"
                    )
                if isinstance(targeted_reply, protocol.RejectWorkerLease):
                    self._clear_protocol_unresolved(pending)
                    return self._handle_lease_rejection(
                        pending, prepared_spec, dependencies, targeted_reply,
                        lease_state=target_state,
                    )
                grant = self._require_lease_grant(targeted_reply, expected_node_id=target_node_id)
                granting_node_address = target_address
            elif location_state is None:
                if isinstance(lease_reply, protocol.RejectWorkerLease):
                    self._clear_protocol_unresolved(pending)
                    return self._handle_lease_rejection(
                        pending, prepared_spec, dependencies, lease_reply,
                        lease_state=lease_state,
                    )
                grant = self._require_lease_grant(
                    lease_reply, expected_node_id=lease_state.expected_node_id
                )
                granting_node_address = lease_state.address

            assert grant is not None and granting_node_address is not None
            if (
                (grant.task_id != pending.spec.task_id) or (grant.attempt_id != pending.spec.attempt_id)
            ):
                raise SystemTaskError(
                    "worker grant identity changed before location reporting"
                )
            self._mark_protocol_unresolved(
                pending, "lease_granted", target_node_id=grant.node_id,
                output_candidate=output_candidate,
            )
            dead_terminal = self._consume_node_death_at_lane(
                pending, grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
            worker_address = grant.worker_address
            worker_id = grant.worker_id
            self._validate_granted_dependencies(dependencies, grant)
            try:
                if location_state is None:
                    reports = self._build_location_reports(
                        dependencies, grant, pending.foreign_dependency_guards
                    )
                    location_state = _LocationReportState(
                        deepcopy(grant), granting_node_address, deepcopy(reports),
                        lease_request=deepcopy(request),
                        # No dependency bytes means no Node custody to hand
                        # off; ordinary scalar tasks need no extra ACK RPC.
                        inventory=(protocol.LeaseDependencyInventory(request, grant.node_id, grant.dependencies)
                                   if request.dependencies else None),
                    )
                if not self._report_granted_dependency_locations(
                    pending, prepared_spec, dependencies, location_state
                ):
                    return False
            except _LocationHandoffTerminal:
                return True
            except (_LocationReportRejected, OwnerDiedError) as exc:
                # A typed rejection or an authoritative owner-death fence is
                # terminal for this report.  The grant remains a remote
                # obligation, so converge its exact cancellation before the
                # task may become terminal and release retained holds.
                if isinstance(exc, _LocationReportRejected) and exc.handoff_complete:
                    marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
                    failure = (marker.obligation.terminal_error
                               if marker is not None and isinstance(marker.obligation, _LocationReportState)
                               and marker.obligation.terminal_error is not None else exc)
                    self._clear_protocol_unresolved(pending)
                    self._publish_task_error(pending, failure)
                    return True
                return self._begin_known_grant_cancellation(
                    pending, prepared_spec, dependencies,
                    granting_node_address, grant, request, exc,
                )
            self._emit(
                "lease_granted",
                task_id=str(pending.spec.task_id),
                lease_id=str(lease_id),
                worker_id=str(worker_id),
                node_id=str(grant.node_id),
            )

            function_cache_key = (worker_id, pending.spec.function)
            with self._state_lock:
                first_export = function_cache_key not in self._registered_functions
            spec = replace(
                prepared_spec,
                function_definition=(
                    pending.spec.function_definition if first_export else None
                ),
            )
            push_message = _make_message(
                protocol.PushTask,
                lease_id=lease_id,
                worker_id=worker_id,
                spec=spec,
                dependencies=grant.dependencies,
                task_spec=spec,
                attempt_id=pending.spec.attempt_id,

            )
            if not isinstance(push_message, protocol.PushTask):
                raise SystemTaskError("could not construct a typed PushTask")
            push = push_message
            # Handoff success is not a reusable execution capability: its
            # ticket was released before this lane constructed PushTask. A
            # cancellation may already have revoked the canonical record, or
            # another lane may even have published ERROR without finishing yet.
            # Check and mark admission under the same lock as cancellation.
            # Once push_send wins, a later cancellation is ordered after Push
            # admission and Node Start/Cancel arbitrates execution; no promise
            # of suppressing already-admitted network bytes is made.
            resume_location = None
            with self._state_lock:
                if not self._is_current_task_pending(pending):
                    return True
                marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
                canonical = None if marker is None else marker.obligation
                if (marker is None or marker.pending.execution != pending.execution
                        or not isinstance(canonical, _LocationReportState)):
                    # A different protocol driver owns any existing obligation.
                    # This stale success must neither overwrite it nor send.
                    return False
                if (canonical.grant != grant or canonical.lease_request != request
                        or canonical.granting_node_address != granting_node_address
                        or marker.pending.dependency_hold != pending.dependency_hold
                        or marker.pending.protected_dependencies != pending.protected_dependencies
                        or marker.pending.foreign_dependency_guards != pending.foreign_dependency_guards):
                    raise SystemTaskError("Push admission changed canonical handoff identity")
                if canonical.terminal_error is not None:
                    resume_location = canonical
                elif (marker.phase != "locations_reported"
                        or canonical.cancellation_reply is not None
                        or canonical.execution_outcome is not None
                        or (pending.execution, grant.lease_id)
                        in getattr(self, "_location_handoff_drivers", set())):
                    return False
                else:
                    self._mark_protocol_unresolved(
                        pending, "push_send", canonical, target_node_id=grant.node_id,
                        output_candidate=output_candidate,
                    )
            if resume_location is not None:
                # Resume outside the lock: exact cancellation/owner reports
                # may perform RPC, and the shared driver retains their receipts.
                return self._execute(
                    pending, prepared_spec, dependencies, location_state=resume_location,
                )
            dead_terminal = self._consume_node_death_at_lane(
                pending, grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
            self._emit("task_pushed", task_id=str(pending.spec.task_id), worker_id=str(worker_id))
            try:
                dead_terminal = self._consume_node_death_at_lane(
                    pending, grant.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                push_started = True
                reply = self._push_task_rpc(
                    worker_address, _PUSH_TASK_HANDLER, push
                )
            except BaseException as exc:
                dead_terminal = self._consume_node_death_at_lane(
                    pending, grant.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                # A connection failure means no request bytes reached the
                # Worker.  A RemoteCallError is an explicit Worker rejection,
                # so user code did not start either.  In both cases the Node
                # may atomically abandon the still-GRANTED lease.  Conversely,
                # send/receive timeouts are ambiguous and must leave recovery
                # to Worker completion or Worker-loss detection.
                if (
                    granting_node_address is not None
                    and self._push_definitely_did_not_start(exc)
                ):
                    dead_terminal = self._consume_node_death_at_lane(
                        pending, grant.node_id
                    )
                    if dead_terminal is not None:
                        return dead_terminal
                    released = self._abandon_granted_lease_best_effort(
                        granting_node_address, grant
                    )
                    if not released:
                        return self._begin_known_grant_cancellation(
                            pending, prepared_spec, dependencies,
                            granting_node_address, grant, request, exc,
                        )
                    self._clear_protocol_unresolved(pending)
                    raise
                return self._schedule_ambiguous_push(
                    pending, prepared_spec, dependencies,
                    _PushRequestState(
                        push, grant, granting_node_address, worker_address,
                        round=1, ambiguous=True, lease_request=request,
                    ),
                )
            self._validate_task_reply_identity(pending, worker_id, reply)
            self._validate_ordinary_reply_domain(reply)
            # A Node-completed INLINE publication carries its exact bytes and
            # handoff identity in the reply. Once received, publishing-Node death
            # cannot turn that irreversible hand-off into an ordinary retry;
            # Core must resolve that exact handoff and its child holds.
            if reply.output_publication is None:
                dead_terminal = self._consume_node_death_at_lane(
                    pending, grant.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
            # Ordinary TaskReply converges the remote execution here.  A
            # completed publication still needs its owner adoption ACK; keep
            # the Push fence continuously until `_publish_reply` overwrites it
            # with the exact adoption obligation.
            if reply.output_publication is None:
                self._clear_protocol_unresolved(pending)
            if reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR:
                return self._retry_explicit_system_failure(pending, reply)
            with self._state_lock:
                self._registered_functions.add(function_cache_key)
            published = self._publish_reply(
                pending, reply, expected_node_id=grant.node_id,
                expected_lease_id=grant.lease_id,
            )
            if not published:
                dead_terminal = self._consume_node_death_at_lane(
                    pending, grant.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                if self._is_protocol_unresolved(pending):
                    return False
            self._clear_protocol_unresolved(pending)
            if published:
                self._emit_published_task_reply(pending, reply)
            return True
        except _LeaseRequestAmbiguous as exc:
            return self._handle_ambiguous_lease(
                pending, prepared_spec, dependencies, exc, ambiguity_round
            )
        except _NodeDeathHandled as exc:
            return exc.terminal
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            if isinstance(exc, _TaskFinishing):
                # The terminal owner claim won before this path's next remote
                # send.  No new outcome exists to publish; the claiming path
                # owns dependency release and accepted-task accounting.
                return True
            physical_target = (
                grant.node_id if grant is not None
                else lease_state.expected_node_id
                if lease_state is not None
                else None
            )
            dead_terminal = self._consume_node_death_at_lane(
                pending, physical_target
            )
            if dead_terminal is not None:
                return dead_terminal
            if (
                isinstance(exc, TransportConnectionError)
                and grant is None
                and not push_started
                and ambiguity_round > 0
                and lease_state is not None
            ):
                # This round made no connection, but an earlier round for the
                # exact LeaseID was ambiguous and may already have granted.
                return self._handle_ambiguous_lease(
                    pending, prepared_spec, dependencies,
                    _LeaseRequestAmbiguous(lease_state, exc), ambiguity_round,
                )
            if (
                push_started
                and push is not None
                and grant is not None
                and granting_node_address is not None
                and self._is_protocol_unresolved(pending)
            ):
                # A malformed/cross-talk reply is not an authoritative TaskReply.
                # The original request may still have executed, so retain its
                # exact grant and replay identity instead of publishing ERROR.
                return self._schedule_ambiguous_push(
                    pending, prepared_spec, dependencies,
                    _PushRequestState(
                        push, grant, granting_node_address,
                        grant.worker_address, round=1, ambiguous=True,
                        lease_request=request,
                    ),
                )
            if (
                grant is not None
                and granting_node_address is not None
                and not push_started
            ):
                dead_terminal = self._consume_node_death_at_lane(
                    pending, grant.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                if dependencies:
                    # A builder/driver failure may have handed off only part
                    # of the inventory. Cancel retains the complete grant and
                    # resumes the same custody driver; Release alone only
                    # unpins and cannot authorize forgetting these bytes.
                    return self._begin_known_grant_cancellation(
                        pending, prepared_spec, dependencies,
                        granting_node_address, grant, request, exc,
                    )
                released = self._abandon_granted_lease_best_effort(
                    granting_node_address, grant
                )
                if not released:
                    return self._begin_known_grant_cancellation(
                        pending, prepared_spec, dependencies,
                        granting_node_address, grant, request, exc,
                    )
                self._clear_protocol_unresolved(pending)
            elif grant is None and not push_started:
                # _request_lease_hop reports this path only when every attempt
                # failed before sending, or after an explicit spillback before
                # the target RPC.  No Node can hold a hidden grant.
                self._clear_protocol_unresolved(pending)
            self._publish_task_error(pending, exc)
            self._emit("task_failed", task_id=str(pending.spec.task_id), error=repr(exc))
            return True

    def _request_lease_hop(self, state: _LeaseRequestState) -> object:
        """Resolve one lease hop without changing identity after ambiguity."""

        ambiguous_error: Optional[BaseException] = None
        for _attempt in range(_LEASE_RPC_REPLAY_ATTEMPTS):
            with self._state_lock:
                pending = next((
                    unresolved.pending
                    for unresolved in tuple(
                        getattr(self, "_protocol_unresolved", {}).values()
                    )
                    if (
                        unresolved.pending.spec.task_id == state.request.task_id
                        and unresolved.pending.spec.attempt_id
                        == state.request.attempt_id
                        and unresolved.target_node_id == state.expected_node_id
                    )
                ), None)
            if pending is not None:
                dead_terminal = self._consume_node_death_at_lane(
                    pending, state.expected_node_id
                )
                if dead_terminal is not None:
                    raise _NodeDeathHandled(dead_terminal)
            try:
                reply = self._rpc(
                    state.address, _REQUEST_LEASE_HANDLER, state.request
                )
                try:
                    if isinstance(reply, protocol.GrantWorkerLease):
                        reply = protocol.revalidate_worker_lease_grant(reply)
                        self._validate_granted_dependencies(state.request.dependencies, reply)
                    self._validate_lease_reply_identity(state.request, reply)
                    if (isinstance(reply, protocol.GrantWorkerLease)
                            and reply.node_id != state.expected_node_id):
                        raise SystemTaskError("worker lease was granted by the wrong node")
                    if not state.allow_spillback and isinstance(reply, protocol.SpillbackWorkerLease):
                        raise SystemTaskError("target node attempted a second spillback")
                except Exception as exc:
                    # A missing field or corrupt nested ID is no more proof
                    # of absence than a lost reply. Do not let AttributeError
                    # escape into the no-grant terminal cleanup branch.
                    raise SystemTaskError("worker lease reply failed complete validation") from exc
                return reply
            except TransportConnectionError:
                # No request bytes reached the Node, so the same hop is safe to
                # retry.  It does not, however, erase an earlier ambiguous
                # send/receive failure for this same LeaseID: that earlier RPC
                # may already have committed a grant at the Node.
                continue
            except TransportError as exc:
                # Send/receive failure is ambiguous.  Replaying the exact
                # RequestWorkerLease is safe because Node outcomes are keyed by
                # LeaseID and request equality.
                ambiguous_error = exc
                continue
            except SystemTaskError as exc:
                # A malformed/cross-talk response does not prove that the
                # request had no side effect.  Replay the exact LeaseID; if the
                # Node cannot produce a valid cached outcome, cancellation is
                # the only safe terminal path.
                ambiguous_error = exc
                continue
        if ambiguous_error is not None:
            raise _LeaseRequestAmbiguous(state, ambiguous_error)
        raise TransportConnectionError(
            "could not connect to NodeManager for worker lease"
        )

    def _handle_lease_rejection(
        self,
        pending: _PendingTask,
        prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        reply: protocol.RejectWorkerLease,
        *,
        lease_state: _LeaseRequestState,
    ) -> bool:
        """Translate a typed lease outcome into wait or terminal failure."""

        def terminate(error):
            if not dependencies:
                self._publish_task_error(pending, error)
                return True
            # Localization precedes resource allocation. Even INFEASIBLE or
            # DEPENDENCY_UNAVAILABLE may have sealed an ordered input subset.
            # Cancel freezes that inventory before the same owner handoff.
            request = lease_state.request
            cancellation = _LeaseCancellationState(
                protocol.CancelWorkerLease(
                    request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
                    request.requester_worker_id, request.scheduling_key, lease_request=request,
                ),
                lease_state.address, error, target_node_id=lease_state.expected_node_id, lease_request=request,
            )
            return self._resolve_lease_cancellation(pending, prepared, dependencies, cancellation)

        reason = reply.reason
        if (
            pending.spec.scheduling_key is not None
            and self._placement_group_phase_for_pending(pending)
            is protocol.PlacementGroupPhaseStatus.LOST
        ):
            return terminate(PlacementGroupLostError("placement group attempt is terminal LOST"))
        if reason is protocol.LeaseRejectReason.PENDING_CAPACITY:
            next_round = pending.capacity_round + 1
            capacity_limit = getattr(self, "_capacity_retry_rounds", None)
            if capacity_limit is not None and next_round > capacity_limit:
                error = PendingCapacityError(
                    "worker capacity did not become available after {} rounds".format(
                        pending.capacity_round + 1
                    )
                )
                return terminate(error)
            retried = replace(pending, capacity_round=next_round)
            delay = min(
                _CAPACITY_RETRY_MAX_SECONDS,
                _CAPACITY_RETRY_BASE_SECONDS
                * (2 ** min(max(next_round - 1, 0), 8)),
            )
            self._submissions.put(
                _DelayedReadyTask(
                    # Capacity is a transient state of this scheduling
                    # decision, not a new decision.  Replaying the exact hop
                    # keeps Node-side idempotency storage O(1) and, after a
                    # spillback, retries the selected target directly instead
                    # of returning to the home Node for a second redirect.
                    _ReadyTask(
                        retried, prepared, dependencies,
                        lease_state=lease_state,
                    ),
                    time.monotonic() + delay,
                )
            )
            self._emit(
                "lease_waiting_for_capacity",
                task_id=str(pending.spec.task_id),
                attempt_id=str(pending.spec.attempt_id),
                rejected_lease_id=str(reply.lease_id),
                capacity_round=next_round,
            )
            return False
        if reason is protocol.LeaseRejectReason.INFEASIBLE:
            error: BaseException = InfeasibleTaskError(
                reply.detail or "no live node is feasible for the task resources"
            )
        elif reason in (
            protocol.LeaseRejectReason.NODE_DRAINING,
            protocol.LeaseRejectReason.SHUTTING_DOWN,
        ):
            error = RuntimeShuttingDownError(
                reply.detail or "NodeManager is not accepting worker leases"
            )
        else:
            error = LeaseRejectedError(
                "worker lease rejected: {} {}".format(reason.value, reply.detail)
            )
        return terminate(error)

    def _schedule_ambiguous_push(
        self,
        pending: _PendingTask,
        prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        state: _PushRequestState,
    ) -> bool:
        """Return the lane and replay the exact push after bounded delay."""

        self._mark_protocol_unresolved(
            pending, "push_replay_wait", target_node_id=state.grant.node_id,
            output_candidate=OutputPublicationID(state.push.lease_id, pending.execution),
        )
        dead_terminal = self._consume_node_death_at_lane(
            pending, state.grant.node_id
        )
        if dead_terminal is not None:
            return dead_terminal
        delay = min(
            _PUSH_RETRY_MAX_SECONDS,
            _PUSH_RETRY_BASE_SECONDS
            * (2 ** min(max(state.round - 1, 0), 8)),
        )
        self._submissions.put(
            _DelayedReadyTask(
                _ReadyTask(
                    pending, prepared, dependencies, push_state=state
                ),
                time.monotonic() + delay,
            )
        )
        try:
            self._emit(
                "push_replay_scheduled",
                task_id=str(pending.spec.task_id),
                attempt_id=str(pending.spec.attempt_id),
                lease_id=str(state.push.lease_id),
                worker_id=str(state.push.worker_id),
                replay_round=state.round,
            )
        except Exception:
            # Observability is never allowed to turn an ambiguous push into a
            # terminal task failure, including narrow object.__new__ fixtures.
            pass
        return False

    def _replay_push(
        self, pending: _PendingTask, state: _PushRequestState
    ) -> bool:
        """Replay one byte-for-byte PushTask without requesting another lease."""

        dead_terminal = self._consume_node_death_at_lane(
            pending, state.grant.node_id
        )
        if dead_terminal is not None:
            return dead_terminal
        if state.orphan_cleanup is not None:
            # A terminal Node outcome has superseded the old executor.  Only
            # the frozen deletion obligation may now be replayed.
            return self._resolve_orphan_cleanup(pending, state)
        self._mark_protocol_unresolved(
            pending, "push_replay_send", target_node_id=state.grant.node_id,
            output_candidate=OutputPublicationID(state.push.lease_id, pending.execution),
        )
        dead_terminal = self._consume_node_death_at_lane(
            pending, state.grant.node_id
        )
        if dead_terminal is not None:
            return dead_terminal
        try:
            reply = self._push_task_rpc(
                state.worker_address, _PUSH_TASK_HANDLER, state.push
            )
        except (RemoteCallError, TransportError):
            dead_terminal = self._consume_node_death_at_lane(
                pending, state.grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
            # No replay failure is itself a death proof.  Ask the granting Node
            # for OS-backed process/lease truth, including when a replacement
            # reused the old TCP port and rejected this old WorkerID with a
            # typed RemoteCallError.  A live/RUNNING outcome below preserves the
            # exact Push; only an authoritative terminal outcome may advance it.
            return self._resolve_ambiguous_push_outcome(pending, state)

        try:
            self._validate_task_reply_identity(
                pending, state.push.worker_id, reply
            )
            self._validate_ordinary_reply_domain(reply)
        except SystemTaskError:
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )
        if reply.output_publication is None:
            dead_terminal = self._consume_node_death_at_lane(
                pending, state.grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
        if reply.output_publication is None:
            self._clear_protocol_unresolved(pending)
        if reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR:
            return self._retry_explicit_system_failure(pending, reply)
        function_cache_key = (state.push.worker_id, pending.spec.function)
        with self._state_lock:
            self._registered_functions.add(function_cache_key)
        published = self._publish_reply(
            pending, reply, expected_node_id=state.grant.node_id,
            expected_lease_id=state.grant.lease_id,
        )
        if not published:
            dead_terminal = self._consume_node_death_at_lane(
                pending, state.grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
            if self._is_protocol_unresolved(pending):
                return False
        self._clear_protocol_unresolved(pending)
        if published:
            self._emit_published_task_reply(
                pending, reply, replay=True
            )
        return True

    def _resolve_ambiguous_push_outcome(
        self, pending: _PendingTask, state: _PushRequestState
    ) -> bool:
        """Converge a dead-end Push through the granting Node authority."""

        request = protocol.GetWorkerLeaseOutcome(
            lease_id=state.push.lease_id,
            task_id=pending.spec.task_id,
            attempt_id=pending.spec.attempt_id,
            executor_worker_id=state.push.worker_id,
            owner_worker_id=pending.spec.owner_worker_id,
            object_ids=pending.output_ids,
            scheduling_key=pending.spec.scheduling_key,

        )
        dead_terminal = self._consume_node_death_at_lane(
            pending, state.grant.node_id
        )
        if dead_terminal is not None:
            return dead_terminal
        try:
            reply = self._rpc(
                state.granting_node_address,
                _GET_WORKER_LEASE_OUTCOME_HANDLER,
                request,
            )
        except (ProtocolError, RemoteCallError, TransportError):
            dead_terminal = self._consume_node_death_at_lane(
                pending, state.grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )
        if not self._valid_worker_lease_outcome(request, state, reply):
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )
        if reply.cleanup_pending:
            # The worker/CPU can already be released while its output saga
            # still owes child-hold and replica cleanup. The exact Node ACK is
            # required before any new attempt, even after Worker loss.
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )
        if reply.output_publication is None and reply.output_completion is None:
            dead_terminal = self._consume_node_death_at_lane(
                pending, state.grant.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
        assert isinstance(reply, protocol.GetWorkerLeaseOutcomeReply)
        if not reply.found or reply.state is None:
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )
        # The lease state is the execution authority; ``worker_alive`` only
        # describes the reusable Worker process.  In particular, a Worker
        # normally remains alive after completing a task, so letting process
        # liveness override COMPLETED would hide a durable Node outcome forever
        # after the direct TaskReply was lost.
        if reply.state in (
            protocol.LeaseExecutionState.GRANTED,
            protocol.LeaseExecutionState.RUNNING,
        ):
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )

        if reply.state is protocol.LeaseExecutionState.COMPLETED:
            output_publication = getattr(reply, "output_publication", None)
            if reply.completion_status is protocol.TaskReplyStatus.SUCCEEDED and output_publication is not None:
                recovered = protocol.TaskReply(
                    pending.task_id, pending.spec.attempt_id, state.push.worker_id,
                    protocol.TaskReplyStatus.SUCCEEDED, output_publication.results,
                     output_publication=output_publication,
                )
                return self._publish_reply(pending, recovered, expected_node_id=reply.node_id, expected_lease_id=state.grant.lease_id)
            output_completion = getattr(reply, "output_completion", None)
            if output_completion is not None:
                # Node may have retired its payload after another owner lane
                # committed adoption. This successful witness cannot enter the
                # old descriptor-only fallback and become a SYSTEM retry.
                envelope = self._locally_retained_output_completion(pending, output_completion)
                if envelope is not None:
                    return self._drive_output_publication_adoption(
                        pending, _OutputAdoptionObligation(envelope, reply.node_id),
                    )
                with self._state_lock:
                    if self._output_replay_is_obsolete_locked(pending, output_completion.publication_id):
                        return True
                # An unresolved handoff still belongs to this exact Worker /
                # Node outcome. Metadata cannot supply bytes; exact replay may
                # recover the retained serialized result at that Worker.
                return self._schedule_ambiguous_push(
                    pending, state.push.spec, state.push.dependencies,
                    replace(state, round=state.round + 1, ambiguous=True),
                )
            if reply.completion_status is protocol.TaskReplyStatus.SUCCEEDED:
                # A success without the unified envelope/witness is not a
                # recoverable result. Never manufacture publication authority
                # from replica descriptors or select a retired tier protocol.
                return self._schedule_ambiguous_push(
                    pending, state.push.spec, state.push.dependencies,
                    replace(state, round=state.round + 1, ambiguous=True),
                )
        elif reply.state not in (
            protocol.LeaseExecutionState.WORKER_LOST,
            protocol.LeaseExecutionState.ABANDONED,
        ):
            return self._schedule_ambiguous_push(
                pending, state.push.spec, state.push.dependencies,
                replace(state, round=state.round + 1, ambiguous=True),
            )

        if reply.orphan_descriptors:
            cleanup = _OrphanCleanupState(
                drops=tuple(
                    protocol.DropObjectReplica(
                        object_id=descriptor.object_id,
                        producer_attempt_id=descriptor.producer_attempt_id,
                        owner_worker_id=descriptor.owner_worker_id,
                        node_id=descriptor.node_id,
                        checksum=descriptor.checksum,
                    )
                    for descriptor in reply.orphan_descriptors
                ),
                acknowledged=(),
                lease_state=reply.state,
                completion_status=reply.completion_status,
            )
            cleanup_state = replace(state, orphan_cleanup=cleanup)
            # Persist exact destructive identities before the first RPC.
            self._mark_protocol_unresolved(
                pending, "orphan_cleanup", cleanup,
                target_node_id=state.grant.node_id,
            )
            return self._resolve_orphan_cleanup(pending, cleanup_state)

        self._clear_protocol_unresolved(pending)
        return self._finish_terminal_lease_outcome(
            pending, reply.state, reply.completion_status
        )

    def _schedule_orphan_cleanup(
        self, pending: _PendingTask, state: _PushRequestState
    ) -> bool:
        """Requeue only the unacknowledged immutable replica drops."""

        cleanup = state.orphan_cleanup
        assert cleanup is not None
        next_cleanup = replace(cleanup, round=cleanup.round + 1)
        self._mark_protocol_unresolved(
            pending, "orphan_cleanup_wait", next_cleanup,
            target_node_id=state.grant.node_id,
        )
        dead_terminal = self._consume_node_death_at_lane(
            pending, state.grant.node_id
        )
        if dead_terminal is not None:
            return dead_terminal
        delay = min(
            _PUSH_RETRY_MAX_SECONDS,
            _PUSH_RETRY_BASE_SECONDS
            * (2 ** min(max(next_cleanup.round - 1, 0), 8)),
        )
        self._submissions.put(
            _DelayedReadyTask(
                _ReadyTask(
                    pending, state.push.spec, state.push.dependencies,
                    push_state=replace(state, orphan_cleanup=next_cleanup),
                ),
                time.monotonic() + delay,
            )
        )
        return False

    def _resolve_orphan_cleanup(
        self, pending: _PendingTask, state: _PushRequestState
    ) -> bool:
        """Delete every old-attempt replica before retry or publication."""

        cleanup = state.orphan_cleanup
        assert cleanup is not None
        acknowledged = set(cleanup.acknowledged)
        current = cleanup
        for request in cleanup.drops:
            if request in acknowledged:
                continue
            # Keep shutdown-visible progress ahead of each destructive send.
            self._mark_protocol_unresolved(
                pending, "orphan_cleanup_send", current,
                target_node_id=request.node_id,
            )
            dead_terminal = self._consume_node_death_at_lane(
                pending, request.node_id
            )
            if dead_terminal is not None:
                return dead_terminal
            try:
                reply = self._rpc(
                    self._resolve_node_address(request.node_id),
                    _DROP_OBJECT_REPLICA_HANDLER, request,
                )
            except Exception:
                dead_terminal = self._consume_node_death_at_lane(
                    pending, request.node_id
                )
                if dead_terminal is not None:
                    return dead_terminal
                return self._schedule_orphan_cleanup(
                    pending, replace(state, orphan_cleanup=current)
                )
            identity_matches = (
                isinstance(reply, protocol.DropObjectReplicaReply)
                and reply.object_id == request.object_id
                and reply.producer_attempt_id == request.producer_attempt_id
                and reply.owner_worker_id == request.owner_worker_id
                and reply.node_id == request.node_id
                and reply.checksum == request.checksum
            )
            if not identity_matches or reply.status not in (
                protocol.DropObjectReplicaStatus.DROPPED,
                protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
            ):
                return self._schedule_orphan_cleanup(
                    pending, replace(state, orphan_cleanup=current)
                )
            acknowledged.add(request)
            current = replace(
                current,
                acknowledged=tuple(
                    item for item in current.drops if item in acknowledged
                ),
            )
            self._mark_protocol_unresolved(
                pending, "orphan_cleanup", current,
                target_node_id=state.grant.node_id,
            )

        self._clear_protocol_unresolved(pending)
        return self._finish_terminal_lease_outcome(
            pending, current.lease_state, current.completion_status
        )

    def _finish_terminal_lease_outcome(
        self, pending: _PendingTask,
        lease_state: protocol.LeaseExecutionState,
        completion_status: protocol.TaskReplyStatus | None,
    ) -> bool:
        """Apply retry policy after orphan cleanup has converged."""

        if lease_state is protocol.LeaseExecutionState.COMPLETED:
            if completion_status is protocol.TaskReplyStatus.APPLICATION_ERROR:
                error = TaskError(
                    "remote application error completed, but its detail was "
                    "lost before reaching the object owner"
                )
                self._publish_task_error(
                    pending, error, failure_kind=FailureKind.APPLICATION
                )
                return True
            if completion_status is protocol.TaskReplyStatus.CANCELLED:
                error = SystemTaskError(
                    "task was cancelled before its terminal payload reached "
                    "the object owner"
                )
                self._publish_task_error(pending, error)
                return True
            if completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR:
                error: BaseException = SystemTaskError(
                    "remote system-error payload was lost before reaching "
                    "the object owner"
                )
            else:
                error = SystemTaskError(
                    "task execution completed but its successful result "
                    "payload was lost before reaching the object owner"
                )
        elif lease_state is protocol.LeaseExecutionState.WORKER_LOST:
            error = WorkerDiedError(
                "worker died before the object owner received a task result"
            )
        else:
            assert lease_state is protocol.LeaseExecutionState.ABANDONED
            error = WorkerDiedError(
                "worker lease was abandoned before a task result was delivered"
            )
        return self._retry_system_failure(pending, error)

    @staticmethod
    def _valid_worker_lease_outcome(
        request: protocol.GetWorkerLeaseOutcome,
        state: _PushRequestState,
        reply: object,
    ) -> bool:
        if not isinstance(reply, protocol.GetWorkerLeaseOutcomeReply):
            return False
        try:
            # Unpickling does not invoke dataclass __post_init__.  Rebuilding
            # revalidates all descriptor identities and outcome invariants.
            replace(reply)
        except Exception:
            return False
        return (
            (isinstance(reply, protocol.GetWorkerLeaseOutcomeReply)) and (reply.lease_id == request.lease_id) and (reply.task_id == request.task_id) and (reply.attempt_id == request.attempt_id) and (reply.executor_worker_id == request.executor_worker_id) and (reply.owner_worker_id == request.owner_worker_id) and (reply.object_ids == request.object_ids) and (reply.node_id == state.grant.node_id) and (reply.scheduling_key == request.scheduling_key) and (state.grant.scheduling_key == request.scheduling_key)
        )

    def _handle_ambiguous_lease(
        self,
        pending: _PendingTask,
        prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        failure: _LeaseRequestAmbiguous,
        ambiguity_round: int,
    ) -> bool:
        """Requeue the exact lease hop; never manufacture a second grant."""

        dead_terminal = self._consume_node_death_at_lane(
            pending, failure.state.expected_node_id
        )
        if dead_terminal is not None:
            return dead_terminal
        if (
            pending.spec.scheduling_key is not None
            and self._placement_group_phase_for_pending(pending)
            is protocol.PlacementGroupPhaseStatus.LOST
        ):
            # Delivery is ambiguous, so cancel this exact LeaseID on its
            # surviving Node.  Only the cancellation ACK permits terminal PG
            # publication; it also proves no queued user code can later start.
            failure = _LeaseRequestAmbiguous(
                failure.state,
                PlacementGroupLostError(
                    "placement group attempt is terminal LOST"
                ),
            )
            return self._begin_ambiguous_lease_cancellation(
                pending, prepared, dependencies, failure
            )
        next_round = ambiguity_round + 1
        if next_round > _LEASE_RPC_REPLAY_ATTEMPTS:
            return self._begin_ambiguous_lease_cancellation(
                pending, prepared, dependencies, failure
            )
        self._mark_protocol_unresolved(
            pending, "lease_replay_wait",
            target_node_id=failure.state.expected_node_id,
            output_candidate=OutputPublicationID(failure.state.request.lease_id, pending.execution),
        )
        delay = min(
            _CAPACITY_RETRY_MAX_SECONDS,
            _CAPACITY_RETRY_BASE_SECONDS * (2 ** (next_round - 1)),
        )
        self._submissions.put(
            _DelayedReadyTask(
                _ReadyTask(
                    pending, prepared, dependencies,
                    lease_state=failure.state,
                    ambiguity_round=next_round,
                ),
                time.monotonic() + delay,
            )
        )
        return False

    def _begin_ambiguous_lease_cancellation(
        self, pending: _PendingTask, prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        failure: _LeaseRequestAmbiguous,
    ) -> bool:
        state = failure.state
        terminal_error: BaseException = failure
        with self._state_lock:
            if (pending.spec.scheduling_key is not None
                    and self._placement_group_phase_for_pending(pending)
                    is protocol.PlacementGroupPhaseStatus.LOST
                    and isinstance(failure.cause, PlacementGroupLostError)):
                # PG loss selected the user-visible failure; ambiguity only
                # describes why this exact lease still needs cancellation.
                # Retain that same cause through inventory/ACK replay instead
                # of publishing the transport wrapper after custody finishes.
                # Ordinary ambiguity and previously latched errors are intact.
                terminal_error = failure.cause
        cancellation = _LeaseCancellationState(
            protocol.CancelWorkerLease(
                state.request.lease_id, state.request.task_id,
                state.request.attempt_id, state.request.requester_node_id,
                state.request.requester_worker_id,
                state.request.scheduling_key,
                lease_request=state.request,
            ),
            state.address,
            terminal_error,
            target_node_id=state.expected_node_id,
            lease_request=deepcopy(state.request),
        )
        return self._resolve_lease_cancellation(
            pending, prepared, dependencies, cancellation
        )

    def _begin_known_grant_cancellation(
        self, pending: _PendingTask, prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        address: Address, grant: protocol.GrantWorkerLease,
        lease_request: protocol.RequestWorkerLease,
        terminal_error: BaseException,
    ) -> bool:
        if (
            (lease_request.lease_id != grant.lease_id) or (lease_request.task_id != grant.task_id) or (lease_request.attempt_id != grant.attempt_id) or (lease_request.requester_worker_id != self.worker_id) or (lease_request.scheduling_key != grant.scheduling_key)
        ):
            raise SystemTaskError(
                "known grant does not match its frozen lease request"
            )
        cancellation = _LeaseCancellationState(
            protocol.CancelWorkerLease(
                grant.lease_id, grant.task_id, grant.attempt_id,
                lease_request.requester_node_id,
                lease_request.requester_worker_id,
                grant.scheduling_key,
                lease_request=lease_request,
            ),
            address, terminal_error, target_node_id=grant.node_id,
            lease_request=deepcopy(lease_request), known_grant=deepcopy(grant),
        )
        return self._resolve_lease_cancellation(
            pending, prepared, dependencies, cancellation
        )

    def _resolve_lease_cancellation(
        self, pending: _PendingTask, prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        cancellation: _LeaseCancellationState,
    ) -> bool:
        """Fence execution, then hand off any historical granted replicas.

        Cancel can recover a grant whose every reply was lost. Its historical
        inventory is custody evidence, never permission to Push. The same
        location driver owns local/foreign receipts and their eventual cleanup.
        """
        expected = (
            cancellation.request.lease_id, cancellation.request.task_id,
            cancellation.request.attempt_id,
            cancellation.request.requester_node_id,
            cancellation.request.requester_worker_id,
            cancellation.request.scheduling_key,
        )

        def retain(phase, candidate):
            """Select/advance one canonical record under the same Core lock.

            A cancellation may race a later custody driver across any RPC or
            local effect. Reading its marker and deciding whether to write a
            cancellation is one transaction, never a check-then-mark gap.
            """
            with self._state_lock:
                if not self._is_current_task_pending(pending):
                    return None
                marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
                prior = None if marker is None else marker.obligation
                if marker is not None and (
                    marker.pending.foreign_dependency_guards != pending.foreign_dependency_guards
                    or marker.pending.dependency_hold != pending.dependency_hold
                    or marker.pending.protected_dependencies != pending.protected_dependencies
                ):
                    raise SystemTaskError("lease cancellation changed its retained dependency credentials")
                if isinstance(prior, _LocationReportState):
                    request = prior.lease_request
                    if (request is None or prior.granting_node_address != candidate.address
                            or (request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
                                request.requester_worker_id, request.scheduling_key) != expected
                            or (candidate.lease_request is not None and request != candidate.lease_request)
                            or (candidate.known_grant is not None and prior.grant != candidate.known_grant)):
                        raise SystemTaskError("lease cancellation changed retained custody identity")
                    if prior.terminal_error is None:
                        prior = replace(prior, terminal_error=candidate.terminal_error)
                        self._mark_protocol_unresolved(pending, "location_cancel", prior, target_node_id=prior.node_id)
                    return prior
                if isinstance(prior, _OutputNodeLossObligation):
                    return prior
                if isinstance(prior, _LeaseCancellationState):
                    if (prior.request != candidate.request or prior.address != candidate.address
                            or prior.lease_request != candidate.lease_request
                            or prior.known_grant != candidate.known_grant
                            or prior.target_node_id != candidate.target_node_id):
                        raise SystemTaskError("lease cancellation replay changed its frozen request")
                    candidate = replace(prior, round=max(prior.round, candidate.round),
                                        reply=prior.reply if prior.reply is not None else candidate.reply)
                self._mark_protocol_unresolved(
                    pending, phase, candidate, target_node_id=candidate.target_node_id,
                    output_candidate=OutputPublicationID(candidate.request.lease_id, pending.execution),
                )
                return candidate

        def resume(state):
            if state is None:
                return True
            if isinstance(state, _LocationReportState):
                return self._execute(pending, prepared, dependencies, location_state=state)
            assert isinstance(state, _OutputNodeLossObligation)
            return self._drive_output_node_loss(pending, state)

        retained = retain("lease_cancel_send", cancellation)
        if not isinstance(retained, _LeaseCancellationState):
            return resume(retained)
        cancellation = retained
        dead_terminal = self._consume_node_death_at_lane(pending, cancellation.target_node_id)
        if dead_terminal is not None:
            return dead_terminal

        def replay_cancel():
            # An old queued cancellation may arrive after the custody driver
            # has made progress. Never overwrite its actual receipts or fence.
            next_state = retain("lease_cancel_wait", replace(cancellation, round=cancellation.round + 1))
            if not isinstance(next_state, _LeaseCancellationState):
                return resume(next_state)
            delay = min(_CAPACITY_RETRY_MAX_SECONDS,
                        _CAPACITY_RETRY_BASE_SECONDS * 2 ** min(next_state.round, 8))
            self._submissions.put(
                _DelayedReadyTask(
                    _ReadyTask(pending, prepared, dependencies, cancellation=next_state),
                    time.monotonic() + delay,
                )
            )
            return False

        try:
            candidate = cancellation.reply
            if candidate is None:
                candidate = self._rpc(cancellation.address, _CANCEL_LEASE_HANDLER, cancellation.request)
            if type(candidate) is not protocol.CancelWorkerLeaseReply:
                return replay_cancel()
            reply = deepcopy(replace(candidate))
            actual = (reply.lease_id, reply.task_id, reply.attempt_id,
                      reply.requester_node_id, reply.requester_worker_id, reply.scheduling_key)
            if actual != expected:
                return replay_cancel()
            dead_terminal = self._consume_node_death_at_lane(pending, cancellation.target_node_id)
            if dead_terminal is not None:
                return dead_terminal
            cancelled = reply.accepted is True and reply.cancelled is True
            historical = reply.retired_grant
            inventory = reply.dependency_inventory
            worker_lost = (reply.state is protocol.LeaseExecutionState.WORKER_LOST
                           and historical is not None and not reply.accepted and not reply.cancelled)
            if not cancelled and not worker_lost:
                return replay_cancel()
            if cancellation.lease_request is not None:
                if inventory is None:
                    return replay_cancel()
                inventory = protocol.revalidate_lease_dependency_inventory(inventory)
                if (inventory.lease_request != cancellation.lease_request
                        or inventory.node_id != cancellation.target_node_id
                        or inventory.lease_request.dependencies != dependencies):
                    return replay_cancel()
            if historical is not None:
                request = cancellation.lease_request
                if request is None:
                    return replay_cancel()
                historical = protocol.revalidate_worker_lease_grant(historical)
                self._validate_lease_reply_identity(request, historical)
                self._require_lease_grant(historical, expected_node_id=cancellation.target_node_id)
                self._validate_granted_dependencies(request.dependencies, historical)
                if ((request.dependencies != dependencies) or (request.task_id != pending.task_id) or (request.attempt_id != pending.spec.attempt_id) or (request.requester_worker_id != self.worker_id) or (request.return_ids != pending.output_ids) or (request.scheduling_key != pending.spec.scheduling_key) or ((cancellation.known_grant is not None) and (historical != cancellation.known_grant))):
                    return replay_cancel()
            elif cancellation.known_grant is not None or (dependencies and reply.released):
                # A known committed grant cannot disappear from an ACK. An
                # absent historical grant otherwise means only no committed
                # grant, not a proof that partial localization created no bytes.
                return replay_cancel()
            retained = retain("lease_cancel_inventory", replace(cancellation, reply=reply))
            if not isinstance(retained, _LeaseCancellationState):
                return resume(retained)
            cancellation = retained
            reply = cancellation.reply
            historical = reply.retired_grant
            inventory = reply.dependency_inventory
            cancelled = reply.accepted is True and reply.cancelled is True
            if historical is not None or inventory is not None:
                request = cancellation.lease_request
                reports = (self._build_location_reports(dependencies, historical, pending.foreign_dependency_guards)
                           if historical is not None else self._build_inventory_location_reports(
                               inventory, pending.foreign_dependency_guards))
                state = _LocationReportState(
                    historical, cancellation.address, reports, lease_request=request,
                    terminal_error=cancellation.terminal_error,
                    cancellation_reply=reply if cancelled else None,
                    inventory=inventory,
                )
                # Even WORKER_LOST inventory is not an execution proof. The
                # shared driver queries its exact outcome before completion.
                return self._execute(pending, prepared, dependencies, location_state=state)
        except (KeyboardInterrupt, SystemExit, _TaskFinishing):
            raise
        except Exception:
            dead_terminal = self._consume_node_death_at_lane(pending, cancellation.target_node_id)
            if dead_terminal is not None:
                return dead_terminal
            return replay_cancel()
        terminal_error = cancellation.terminal_error
        if (
            pending.spec.scheduling_key is not None
            and self._placement_group_phase_for_pending(pending)
            is protocol.PlacementGroupPhaseStatus.LOST
        ):
            terminal_error = PlacementGroupLostError(
                "placement group attempt is terminal LOST"
            )
        self._clear_protocol_unresolved(pending)
        self._publish_task_error(pending, terminal_error)
        return True

    def _retry_explicit_system_failure(
        self, pending: _PendingTask, reply: protocol.TaskReply
    ) -> bool:
        """Retry only an explicit Worker SYSTEM_ERROR completion.

        Receipt of TaskReply proves the Worker completed its lease handshake.
        Transport-ambiguous failures deliberately never enter this path.
        """

        return self._retry_system_failure(pending, _remote_error(reply))

    def _retry_system_failure(
        self, pending: _PendingTask, error: BaseException, *,
        deferred: _DeferredSystemFailure | None = None,
    ) -> bool:
        """Apply one typed system failure through the shared retry authority."""

        with self._state_lock:
            # PG death uses this same lock.  Checking its phase and committing
            # owner/recovery retry state must have one winner: an unlocked peek
            # could consume retry budget after LOST had already committed.
            if (
                pending.spec.scheduling_key is not None
                and self._placement_group_phase_for_pending(pending)
                is protocol.PlacementGroupPhaseStatus.LOST
            ):
                # Any remote ambiguity/grant is cleared or cancelled before
                # callers enter retry authority. Terminal PG loss dominates the
                # ordinary task retry budget and never advances AttemptID.
                self._clear_protocol_unresolved(pending)
                try:
                    self._raise_if_placement_group_lost(pending)
                except PlacementGroupLostError as lost:
                    self._publish_task_error(pending, lost)
                    return True
            if not isinstance(error, SystemTaskError):
                error = SystemTaskError(str(error))
            if not self._is_current_task_pending(pending):
                return True
            if any(self._has_late_replica_cleanup_locked(output) for output in pending.output_ids):
                failure = _DeferredSystemFailure(error, 1 if deferred is None else deferred.round + 1)
                self._mark_protocol_unresolved(pending, "system_failure_cleanup_wait", failure)
                self._schedule_late_replica_cleanup_locked()
                self._submissions.put(_DelayedReadyTask(
                    _ReadyTask(pending, pending.spec, system_failure=failure), time.monotonic() + 0.05,
                ))
                return False
            if deferred is not None:
                self._clear_protocol_unresolved(pending)
            recovery = self._recovery_manager()
            coordinator = self._reconstruction_coordinator()
            try:
                reconstruction_retry = coordinator.preflight_retry(
                    pending.spec.task_id, pending.spec.attempt_id
                )
            except ReconstructionRuntimeError as exc:
                # Preflight precedes the retry state transition so an identity
                # disagreement cannot consume budget without a queued attempt.
                raise SystemTaskError(str(exc)) from exc
            recovery_plan = recovery.validate_task_failure(
                pending.spec.task_id, pending.spec.attempt_id, error,
                error=error,
            )
            decision = recovery_plan.decision
            if decision.action is RecoveryAction.RETRY_TASK:
                assert decision.attempt_id is not None
                owner_plan = self._owner_table.validate_advance_task_outputs(
                    pending.execution, decision.attempt_id
                )
                # Both authorities are now fully preflighted.  Their validated
                # commits contain assignments only and cannot consume retry
                # budget without advancing the logical output attempt.
                self._owner_table.commit_validated_advance_task_outputs(
                    owner_plan
                )
                recovery.commit_transition(recovery_plan)
                retried = replace(
                    pending,
                    spec=replace(pending.spec, attempt_id=decision.attempt_id),
                )
                self._install_task_finish_barrier_locked(retried)
                if reconstruction_retry:
                    coordinator.handoff_retry(
                        pending.task_id, pending.spec.attempt_id, decision.attempt_id,
                        lambda: self._submissions.put(retried),
                    )
                else:
                    self._submissions.put(retried)
                self._emit(
                    "task_retried",
                    task_id=str(pending.spec.task_id),
                    old_attempt_id=str(pending.spec.attempt_id),
                    attempt_id=str(decision.attempt_id),
                )
                return False
            if decision.action is RecoveryAction.FENCE_STALE_ATTEMPT:
                return True

        self._publish_task_error(
            pending, error, recovery_plan=recovery_plan
        )
        self._emit(
            "task_failed",
            task_id=str(pending.spec.task_id),
            attempt_id=str(pending.spec.attempt_id),
            error=repr(error),
        )
        return True

    def _recovery_manager(self) -> RecoveryManager:
        """Return the retry authority, lazily supporting small pure fixtures."""

        manager = getattr(self, "_recovery", None)
        if manager is None:
            manager = RecoveryManager()
            self._recovery = manager
        return manager

    def _reconstruction_coordinator(self) -> ReconstructionCoordinator:
        coordinator = getattr(self, "_reconstruction", None)
        if coordinator is None:
            coordinator = ReconstructionCoordinator(
                self._recovery_manager(), self._owner_table
            )
            self._reconstruction = coordinator
        return coordinator


    def _ensure_foreign_lineage_runtime(self) -> ForeignLineageRuntime:
        """Lazily compose lineage state for narrow ``object.__new__`` tests."""

        runtime = getattr(self, "_foreign_lineage_runtime", None)
        if runtime is not None:
            return runtime
        registry = getattr(self, "_foreign_lineage_registry", None)
        if registry is None:
            registry = ForeignLineageRegistry()
            self._foreign_lineage_registry = registry
        runtime = ForeignLineageRuntime(
            registry,
            replace_retained=self._replace_foreign_lineage_hold_rpc,
            get_retained=self._get_foreign_lineage_object_rpc,
            request_reconstruction=(
                self._request_foreign_lineage_reconstruction_rpc
            ),
            release_retained=self._release_foreign_lineage_hold_rpc,
            owner_death_lookup=self._owner_table.dead_worker_record,
        )
        self._foreign_lineage_runtime = runtime
        return runtime

    @staticmethod
    def _validate_lease_reply_identity(
        request: protocol.RequestWorkerLease, reply: object
    ) -> None:
        """Reject malformed or cross-talk lease replies before acting on them."""

        if not isinstance(
            reply,
            (
                protocol.GrantWorkerLease,
                protocol.SpillbackWorkerLease,
                protocol.RejectWorkerLease,
            ),
        ):
            raise SystemTaskError(
                "node returned an unknown worker lease reply"
            )
        expected = (request.lease_id, request.task_id, request.attempt_id)
        actual = (reply.lease_id, reply.task_id, reply.attempt_id)
        if (
            (actual != expected) or (reply.scheduling_key != request.scheduling_key)
        ):
            raise SystemTaskError(
                "worker lease reply identity or scheduling key does not match its request"
            )

    @staticmethod
    def _require_lease_grant(
        reply: object, *, expected_node_id: NodeID
    ) -> protocol.GrantWorkerLease:
        """Return a grant or translate a terminal scheduling response."""

        if isinstance(reply, protocol.RejectWorkerLease):
            raise LeaseRejectedError(
                "worker lease rejected: {} {}".format(
                    reply.reason, reply.detail
                )
            )
        if not isinstance(reply, protocol.GrantWorkerLease):
            raise SystemTaskError("node did not return a worker lease grant")
        if reply.node_id != expected_node_id:
            raise SystemTaskError(
                "worker lease was granted by the wrong node"
            )
        return reply

    @staticmethod
    def _validate_task_reply_identity(
        pending: _PendingTask, worker_id: WorkerID, reply: object
    ) -> None:
        """Ensure a direct Worker reply belongs to the pushed attempt."""

        if not isinstance(reply, protocol.TaskReply):
            raise SystemTaskError("worker returned an invalid task reply")
        if (
            (reply.task_id != pending.spec.task_id) or (reply.attempt_id != pending.spec.attempt_id) or (reply.worker_id != worker_id)
        ):
            raise SystemTaskError(
                "worker task reply identity does not match the pushed task"
            )

    @staticmethod
    def _validate_ordinary_reply_domain(reply: protocol.TaskReply) -> None:
        """Reject obsolete success formats before clearing the Push fence."""
        if (reply.status is protocol.TaskReplyStatus.SUCCEEDED
                and type(reply.output_publication) is not OutputPublicationEnvelope):
            raise SystemTaskError("ordinary Task success requires its exact single-output envelope")

    def _decode_reply(
        self,
        pending: _PendingTask,
        reply: object,
        *,
        expected_node_id: Optional[NodeID] = None,
    ) -> tuple[protocol.ResultDescriptor, ...]:
        status = getattr(reply, "status", None)
        if status is not None and status is not protocol.TaskReplyStatus.SUCCEEDED:
            raise _remote_error(reply)
        if status is None and not getattr(reply, "ok", False):
            raise _remote_error(reply)

        results = tuple(getattr(reply, "results", ()))
        if tuple(
            getattr(result, "object_id", None) for result in results
        ) != pending.output_ids:
            raise SystemTaskError(
                "worker result manifest must exactly match ordered task returns"
            )
        for result in results:
            if not isinstance(result, protocol.ResultDescriptor):
                raise SystemTaskError(
                    "worker returned an invalid result descriptor"
                )
            if result.owner_worker_id != self.worker_id:
                raise SystemTaskError("worker published the wrong object owner")
            if (
                expected_node_id is not None
                and result.node_id != expected_node_id
            ):
                raise SystemTaskError(
                    "worker published a result from the wrong node"
                )
            if result.storage is protocol.ResultStorage.INLINE:
                assert result.inline_data is not None
                if (
                    hashlib.sha256(result.inline_data).hexdigest()
                    != result.checksum
                ):
                    raise SystemTaskError(
                        "inline task result checksum does not match"
                    )
            elif result.storage is not protocol.ResultStorage.OBJECT_STORE:
                raise SystemTaskError(
                    "worker selected an unknown result storage kind"
                )
        self._preflight_stored_result_replays(pending, results)
        return results

    def _preflight_stored_result_replays(
        self,
        pending: _PendingTask,
        results: tuple[protocol.ResultDescriptor, ...],
    ) -> None:
        """Reject a drifted stored replay before any batch authority mutates.

        ``ObjectOwnerTable`` independently freezes the same identity.  This
        Core-side check protects the owner table's immutable canonical result:
        a changed replay cannot overwrite the existing descriptor before owner
        publication validation rejects the manifest.
        The producing attempt is bound by the owner snapshot; the descriptor
        itself binds object, owner, original publication node, storage, size,
        and checksum.  ``_stored_descriptors`` is deliberately not consulted:
        its node_id is the mutable preferred fetch route after replica failover.
        """

        # A stale TaskReply is an ordinary fenced replay, not a descriptor
        # conflict.  Leave that decision to the existing owner-attempt gate.
        if not all(
            self._owner_table.snapshot(object_id).current_attempt
            == pending.spec.attempt_id
            for object_id in pending.output_ids
        ):
            return
        with self._state_lock:
            for result in results:
                if result.storage is not protocol.ResultStorage.OBJECT_STORE:
                    continue
                snapshot = self._owner_table.snapshot(result.object_id)
                canonical = snapshot.canonical_stored_result
                if snapshot.state is not ObjectState.READY_STORED:
                    # Initial publication has no canonical descriptor yet.
                    # Some narrow recovery/GC fixtures deliberately retain a
                    # prior physical descriptor beside PENDING/LOST owner
                    # metadata; publication replaces it only after the owner
                    # batch succeeds.
                    continue
                if snapshot.current_attempt != pending.spec.attempt_id:
                    raise SystemTaskError(
                        "stored task output replay changed producer attempt"
                    )
                if canonical is None or canonical != result:
                    raise SystemTaskError(
                        "stored task output replay changed canonical descriptor"
                    )

    def _rpc(self, address: Address, handler: str, message: object) -> object:
        # Graceful shutdown bounds the caller's join, not an already-started
        # operation.  Each RPC retains its normal finite timeout while the
        # dispatcher drains accepted submissions in order.
        if getattr(self, "_reference_transport_closed", False):
            raise RuntimeShuttingDownError("managed cluster already exited")
        deadline = _RPC_CALL_DEADLINE.get()
        total_timeout = (
            _RPC_TOTAL_TIMEOUT_SECONDS
            if deadline is None
            else deadline - time.monotonic()
        )
        if total_timeout <= 0:
            raise TimeoutError("RPC deadline expired")
        connect_timeout = min(_RPC_CONNECT_TIMEOUT_SECONDS, total_timeout / 2.0)
        request_timeout = total_timeout - connect_timeout
        return rpc_request(
            address,
            handler,
            message,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
            event_sink=self.event_sink,
            trace_component="core_worker",
            deadline=deadline,
        )

    def _push_task_rpc(
        self, address: Address, handler: str, message: object
    ) -> object:
        """Push user work without imposing a task-duration timeout."""

        return rpc_request(
            address,
            handler,
            message,
            connect_timeout=_RPC_CONNECT_TIMEOUT_SECONDS,
            request_timeout=None,
            event_sink=self.event_sink,
            trace_component="core_worker",
        )

    @staticmethod
    def _push_definitely_did_not_start(exc: BaseException) -> bool:
        """Whether a failed push is known not to have invoked user code."""

        # A typed connection-establishment failure occurs before request bytes
        # can reach the Worker.  Send and receive failures deliberately use
        # other exception types because their execution state is ambiguous.
        # RemoteCallError is ambiguous too: a Worker may have executed and
        # cached the result before its CompleteLease acknowledgement failed,
        # after which the TCP handler surfaces that late failure remotely.
        return isinstance(exc, TransportConnectionError)

    def _abandon_granted_lease_best_effort(
        self, node_address: Address, grant: protocol.GrantWorkerLease
    ) -> bool:
        """Ask the authoritative Node to CAS GRANTED to ABANDONED."""

        try:
            reply = self._rpc(
                node_address,
                "release_worker_lease",
                protocol.ReleaseWorkerLease(
                    lease_id=grant.lease_id,
                    worker_id=grant.worker_id,
                    allocation_token=grant.allocation_token,
                ),
            )
            self._emit(
                "lease_abandon_requested",
                lease_id=str(grant.lease_id),
                node_id=str(grant.node_id),
                released=bool(getattr(reply, "released", False)),
            )
            # Only a typed positive release acknowledgement proves that this
            # Core may terminate the logical attempt.  Human-readable details
            # such as "unknown" or "already released" are intentionally
            # not treated as proof; the identity-fenced cancellation protocol
            # resolves every other case.
            return bool(getattr(reply, "released", False))
        except Exception as abandon_error:
            self._emit(
                "lease_abandon_failed",
                lease_id=str(grant.lease_id),
                error=repr(abandon_error),
            )
            return False


    def _drive_output_node_loss(self, pending: _PendingTask, obligation: _OutputNodeLossObligation) -> bool:
        """Resolve one exact dead publisher without synthesizing lost bytes."""
        identity = obligation.publication_id
        with self._state_lock:
            if self._output_replay_is_obsolete_locked(pending, identity):
                return True
            custody = getattr(self, "_output_result_custody", None)
            if custody is None:
                custody = {}
                self._output_result_custody = custody
            if obligation.envelope is not None:
                decision = getattr(self, "_output_loss_choices", {}).get(identity)
                if decision is None:
                    existing = custody.setdefault(identity, obligation.envelope)
                    if existing != obligation.envelope:
                        raise SystemTaskError("output custody envelope was rebound")
            retained = custody.get(identity)
            if retained is not None and obligation.envelope is None:
                obligation = replace(obligation, envelope=retained)
            tickets = getattr(self, "_output_loss_drivers", None)
            if tickets is None:
                tickets = set()
                self._output_loss_drivers = tickets
            if identity in tickets:
                return False
            tickets.add(identity)
        try:
            return self._drive_output_node_loss_once(pending, obligation)
        finally:
            with self._state_lock:
                tickets.discard(identity)

    def _drive_output_node_loss_once(self, pending: _PendingTask, obligation: _OutputNodeLossObligation) -> bool:
        from .output_handoff import NodeLostOutputResolution
        from . import enhanced_publication as enhanced
        identity = obligation.publication_id
        try:
            with self._state_lock:
                known_handoff = self._output_handoff_table().query(identity)
                publication = None if known_handoff is None or known_handoff.manifest is None else enhanced.TaskPublication(
                    known_handoff.manifest, self.owner_address)
            central = None if publication is None else self._publication_client().query(publication)
            with self._state_lock:
                if self._output_replay_is_obsolete_locked(pending, identity):
                    return True
                self._mark_protocol_unresolved(pending, 'output_node_loss', obligation, target_node_id=obligation.node_death.node_id)
                table = self._output_handoff_table()
                handoff = table.query(identity)
                envelope = getattr(self, '_output_result_custody', {}).get(identity, obligation.envelope)
                if handoff is None or handoff.manifest is None:
                    # No owner registration ACK means no authorized child effect.
                    table.abort(identity, 'unregistered publisher Node died')
                    self._clear_protocol_unresolved(pending)
                    return self._retry_system_failure(pending, NodeDiedError('publisher died before owner handoff registration'))
                manifest = handoff.manifest
                if manifest.header.owner_worker_id != self.worker_id or manifest.publication_id.execution != pending.execution:
                    raise SystemTaskError('Node-loss handoff changed owner execution')
                latched = getattr(self, '_output_node_cleanup', {}).get(identity)
                complete = (latched['complete'] if latched is not None else
                            handoff.complete or (None if envelope is None else envelope.complete) or
                            (None if central is None else central.complete))
                if complete is not None and handoff.complete is None and handoff.phase is not OutputHandoffPhase.ABORTED:
                    table.record_complete(complete)
                works = getattr(self, '_output_node_cleanup', None)
                if works is None:
                    works = self._output_node_cleanup = {}
                work = works.get(identity)
                if work is None:
                    slot = manifest.slots[0]
                    keep = bool(complete is not None and (
                        envelope is not None and slot.tier is protocol.ResultStorage.INLINE
                        or self._owner_table.surviving_output_locations(
                            manifest, 0, unavailable_nodes=tuple(getattr(self, '_dead_nodes', {})),
                        )
                    ))
                    work = {'manifest': manifest, 'complete': complete, 'keep': keep, 'acks': {}}
                    works[identity] = work
                    self._output_loss_choices = getattr(self, '_output_loss_choices', {})
                    self._output_loss_choices[identity] = keep
                    if not keep:
                        if handoff.phase is not OutputHandoffPhase.ADOPTED:
                            table.abort(identity, 'publishing Node died before payload adoption')
                        work['fence'] = enhanced.OwnerRetirementReceipt(
                            publication.reference, self.worker_id, 'node-loss:' + identity.transaction_id,
                            enhanced.RetirementReason.PAYLOAD_LOST,
                        ) if complete is not None else enhanced.OwnerAbortReceipt(
                            publication.reference, self.worker_id, 'node-loss:' + identity.transaction_id)
            if work['keep']:
                self._publication_client().commit_task(publication, work['complete'])
            else:
                self._publication_client().fence(publication, work['fence'])
            if not work['keep']:
                for transfer in manifest.slots[0].transfers:
                    for hold in (transfer.final_hold, transfer.provisional_hold):
                        request = protocol.ReleaseContainedReference(transfer.contained_object_id, transfer.contained_owner_worker_id, hold)
                        if request in work['acks']:
                            continue
                        with self._state_lock:
                            owner_death = getattr(self, '_worker_death_records', {}).get(transfer.contained_owner_worker_id)
                        if owner_death is not None:
                            work['acks'][request] = owner_death
                            continue
                        reply = self._borrow_rpc(transfer.contained_owner_address, _RELEASE_CONTAINED_REFERENCE_HANDLER, request)
                        if (type(reply) is not protocol.ReleaseContainedReferenceReply or not reply.accepted
                                or (reply.object_id, reply.owner_worker_id, reply.hold) != (request.object_id, request.owner_worker_id, request.hold)):
                            raise SystemTaskError('Node-loss child release changed exact identity')
                        work['acks'][request] = replace(reply)
                self._publication_client().retire(
                    publication, [r for r in work['acks'].values() if type(r) is protocol.ReleaseContainedReferenceReply],
                    tuple(dict.fromkeys(r for r in work['acks'].values() if type(r) is protocol.WorkerDeathRecord)),
                )
            resolution = NodeLostOutputResolution(
                identity, manifest.manifest_digest, self.worker_id, obligation.node_death,
                complete=work['complete'], keep=work['keep'], cleanup=tuple(dict.fromkeys(work['acks'].values())),
            )
            proof = None
            with self._state_lock:
                if self._output_replay_is_obsolete_locked(pending, identity):
                    return True
                self._owner_table.resolve_output_node_loss(
                    manifest, resolution, envelope if work['keep'] else None,
                    unavailable_nodes=tuple(getattr(self, '_dead_nodes', {})),
                )
                current = self._owner_table.snapshot(pending.object_id)
                if work['keep'] and current.state in (ObjectState.READY_INLINE, ObjectState.READY_STORED):
                    # The local resolution adopted surviving bytes. LOST is
                    # deliberately not a payload-adoption acknowledgement.
                    proof = OutputPublicationAdoptionProof(
                        resolution.complete, self.worker_id,
                        'output-owner:{}:{}'.format(identity.transaction_id, manifest.manifest_digest),
                    )
                    self._output_handoff_table().adopt(proof)
                elif work['keep']:
                    proof = self._output_handoff_table().query(identity).adoption
                if current.state is ObjectState.READY_STORED and current.locations:
                    self._stored_descriptors[pending.object_id] = replace(current.canonical_stored_result, node_id=min(current.locations))
                else:
                    self._stored_descriptors.pop(pending.object_id, None)
                if resolution.complete is not None:
                    recovery = self._recovery_manager()
                    record = recovery.task_record(pending.task_id)
                    if record.state.value != 'SUCCEEDED':
                        transition = recovery.validate_task_success(pending.task_id, pending.spec.attempt_id)
                        if transition.decision.action is not RecoveryAction.ACCEPT_SUCCESS:
                            raise SystemTaskError('owner fenced known-success Node-loss receipt')
                        recovery.commit_validated_transition(transition)
                    self._reconstruction_coordinator().complete(pending.task_id, pending.spec.attempt_id)
                    self._wake_object(pending.object_id)
            if proof is not None:
                self._publication_client().adopt(proof)
            with self._state_lock:
                self._clear_protocol_unresolved(pending)
                self._output_loss_completed = getattr(self, '_output_loss_completed', set())
                self._output_loss_completed.add(identity)
                getattr(self, '_output_result_custody', {}).pop(identity, None)
                works.pop(identity, None)
            if resolution.complete is None:
                return self._retry_system_failure(pending, NodeDiedError('publisher completion unknown after exact child cleanup'))
            return True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            retried = replace(obligation, round=obligation.round + 1)
            with self._state_lock:
                self._mark_protocol_unresolved(pending, 'output_node_loss_wait', retried, target_node_id=obligation.node_death.node_id)
                self._submissions.put(_DelayedReadyTask(
                    _ReadyTask(pending, pending.spec, output_node_loss=retried), time.monotonic() + 0.05,
                ))
            return False

    def _output_replay_is_obsolete_locked(self, pending, identity) -> bool:
        """Fence old work at each local boundary, including after RPC.

        The recovery authority, exact task marker and current object attempt
        must all identify this execution. Never clear a successor's marker
        when an old reply arrives.
        """
        if (pending.task_key in getattr(self, "_finished_tasks", set())
                or identity in getattr(self, "_output_loss_completed", set())):
            return True
        unresolved = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
        if unresolved is not None and unresolved.pending.spec.attempt_id != pending.spec.attempt_id:
            return True
        recovery = getattr(self, "_recovery", None)
        if recovery is not None:
            record = recovery.task_record(pending.task_id)
            if record.current_attempt != pending.spec.attempt_id:
                return True
        return any(self._owner_table.contains(output_id)
                   and self._owner_table.snapshot(output_id).current_attempt != pending.spec.attempt_id
                   for output_id in pending.output_ids)

    def _known_output_completion_locked(self, pending, identity):
        """Read exact committed owner membership, not Task-level success."""
        manifests = []
        for output_id in pending.output_ids:
            if not self._owner_table.contains(output_id):
                return None
            snapshot = self._owner_table.snapshot(output_id)
            membership = snapshot.output_publication
            if (membership is None or membership.publication_id != identity
                    or snapshot.current_attempt != pending.spec.attempt_id):
                return None
            manifests.append(membership.manifest)
        if not manifests or any(value != manifests[0] for value in manifests):
            return None
        return OutputPublicationCompleteWitness.for_manifest(manifests[0])

    def _locally_retained_output_completion(self, pending, witness, *, allow_lost_stored=False):
        """Recover a handoff only from actual owner custody, never GCS bytes."""
        if type(witness) is not OutputPublicationCompleteWitness:
            raise SystemTaskError("output outcome requires an exact Complete witness")
        witness = replace(witness)
        identity = witness.publication_id
        if identity.execution != pending.execution:
            raise SystemTaskError("output completion changed its selected execution")
        with self._state_lock:
            envelope = getattr(self, "_output_result_custody", {}).get(identity)
            marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
            if envelope is None and marker is not None and isinstance(
                marker.obligation, (_OutputAdoptionObligation, _OutputNodeLossObligation)
            ):
                envelope = marker.obligation.envelope
            if envelope is not None:
                envelope = replace(envelope)
                if envelope.complete != witness:
                    raise SystemTaskError("retained output custody changed Complete witness")
                return envelope
            memberships, results = [], []
            for output_id in pending.output_ids:
                if not self._owner_table.contains(output_id):
                    return None
                snapshot = self._owner_table.snapshot(output_id)
                membership = snapshot.output_publication
                stored_loss_metadata = (allow_lost_stored and snapshot.state is ObjectState.LOST
                                        and membership is not None
                                        and membership.slot.tier is protocol.ResultStorage.OBJECT_STORE)
                if (membership is None or membership.publication_id != identity
                        or snapshot.current_attempt != pending.spec.attempt_id
                        or snapshot.state not in (ObjectState.READY_INLINE, ObjectState.READY_STORED)
                        and not stored_loss_metadata):
                    return None
                memberships.append(membership)
                results.append(self._owner_table.output_owner_result(output_id))
            manifest = memberships[0].manifest
            if any(member.manifest != manifest for member in memberships):
                raise SystemTaskError("owner output slots belong to different manifests")
            return OutputPublicationEnvelope(manifest, witness, tuple(results))

    def _drive_output_publication_adoption(
        self, pending: _PendingTask, obligation: _OutputAdoptionObligation,
    ) -> bool:
        """Adopt one Complete through the registered handoff and owner CAS.

        The retained envelope is the sole payload custody.  Every remote call
        replays exact metadata, and a failed acknowledgement keeps the task's
        existing finish barrier rather than starting another execution.
        """
        from . import output_protocol as wire

        envelope = obligation.envelope
        identity = envelope.publication_id
        with self._state_lock:
            if self._output_replay_is_obsolete_locked(pending, identity):
                return True
            custody = getattr(self, "_output_result_custody", None)
            if custody is None:
                custody = {}
                self._output_result_custody = custody
            # A latched DROP is final even if an old Push returns bytes later.
            # The same lock couples delivery with Node-loss custody selection.
            if identity not in getattr(self, "_output_loss_choices", {}):
                prior = custody.setdefault(identity, envelope)
                if prior != envelope:
                    raise SystemTaskError("output result custody changed")
            self._mark_protocol_unresolved(pending, "output_adoption", obligation, target_node_id=obligation.node_id)
            death = getattr(self, "_dead_nodes", {}).get(obligation.node_id)
        proof = OutputPublicationAdoptionProof(
            envelope.complete, self.worker_id,
            "output-owner:{}:{}".format(identity.transaction_id, envelope.manifest.manifest_digest),
        )
        plan = OutputOwnerPublicationPlan(pending.execution, envelope)
        if death is not None:
            return self._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death, envelope))
        try:
            with self._state_lock:
                handoff = self._output_handoff_table().query(identity)
                if handoff is None or handoff.manifest != envelope.manifest or handoff.phase is OutputHandoffPhase.ABORTED:
                    raise SystemTaskError('output has no current registered owner handoff')
                self._output_handoff_table().record_complete(envelope.complete)
                previous = self._owner_table.output_owner_publication_receipt(plan)
            from .enhanced_publication import TaskPublication
            publication = self._publication_client().remember(TaskPublication(envelope.manifest, self.owner_address))
            self._publication_client().commit_task(publication, envelope.complete)
            ready_observation = None
            with self._state_lock:
                # This check is deliberately after every external step.  A
                # cached registration snapshot is never authority for bytes.
                if self._output_replay_is_obsolete_locked(pending, identity):
                    return True
                if self._node_is_dead(obligation.node_id):
                    raise SystemTaskError("output publication needs Node-loss resolution")
                previous = self._owner_table.output_owner_publication_receipt(plan)
                if previous is None:
                    self._owner_table.validate_output_publication(plan)
                    recovery = self._recovery_manager()
                    success = recovery.validate_task_success(pending.task_id, pending.spec.attempt_id)
                    if success.decision.action is not RecoveryAction.ACCEPT_SUCCESS:
                        raise SystemTaskError("recovery fenced output publication")
                    if not self._owner_table.commit_output_publication(plan).committed:
                        raise SystemTaskError("owner fenced output batch CAS")
                    recovery.commit_validated_transition(success)
                    for result in envelope.results:
                        if result.storage is protocol.ResultStorage.OBJECT_STORE:
                            self._stored_descriptors[result.object_id] = result
                    for output_id in pending.output_ids:
                        self._wake_object(output_id)
                    # Capture immutable facts only. The trace sink runs after
                    # releasing the authority lock, never inside owner CAS.
                    ready_observation = (identity, envelope.manifest.manifest_digest,
                                         len(pending.output_ids))
                    ordinary = getattr(self, "_reconstruction", None)
                    if ordinary is not None:
                        ordinary.complete(pending.task_id, pending.spec.attempt_id)
                elif not previous.committed:
                    raise SystemTaskError("output owner receipt was fenced")
                else:
                    # Owner CAS may commit before a local injected exception.
                    # Its exact receipt repairs the remaining local facts; it
                    # must not skip recovery transition, route cache or wake.
                    recovery = self._recovery_manager()
                    current_record = recovery.task_record(pending.task_id)
                    repair_current = current_record.current_attempt == pending.spec.attempt_id
                    if repair_current and current_record.state.value != "SUCCEEDED":
                        success = recovery.validate_task_success(pending.task_id, pending.spec.attempt_id)
                        if success.decision.action is not RecoveryAction.ACCEPT_SUCCESS:
                            raise SystemTaskError("owner receipt cannot repair stale recovery")
                        recovery.commit_validated_transition(success)
                    if repair_current:
                        for result in envelope.results:
                            if result.storage is protocol.ResultStorage.OBJECT_STORE:
                                self._stored_descriptors[result.object_id] = result
                        for output_id in pending.output_ids:
                            self._wake_object(output_id)
                    ordinary = getattr(self, "_reconstruction", None)
                    if ordinary is not None:
                        ordinary.complete(pending.task_id, pending.spec.attempt_id)
                self._output_handoff_table().adopt(proof)
            if ready_observation is not None:
                try:
                    ready_identity, ready_digest, ready_count = ready_observation
                    # First owner CAS/wake precedes adopted ACK and is not
                    # the later object_ready completion-tail notification.
                    self._emit("output_owner_ready",
                               task_id=str(ready_identity.task_id),
                               attempt_id=str(ready_identity.attempt_id),
                               lease_id=str(ready_identity.lease_id),
                               manifest_digest=ready_digest, return_count=ready_count)
                except BaseException:
                    pass
            gcs_adoption = self._publication_client().adopt(proof)
            request = wire.AckOutputPublicationAdopted(proof, gcs_adoption)
            with causal_scope(current_cause_id()):
                reply = self._rpc(self._resolve_node_address(obligation.node_id), wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER, request)
                if type(reply) is not wire.AckOutputPublicationAdoptedReply:
                    raise SystemTaskError("Node did not ACK output payload retirement")
                reply = replace(reply)
                if reply.request != request or reply.accepted is not True:
                    raise SystemTaskError("Node did not ACK output payload retirement")
                try:
                    # This ACK retires Node journal payload custody, not the
                    # object-store replica or the owner's logical ObjectRef.
                    self._emit("output_payload_retired",
                               task_id=str(identity.task_id),
                               attempt_id=str(identity.attempt_id),
                               lease_id=str(identity.lease_id),
                               manifest_digest=envelope.manifest.manifest_digest)
                except BaseException:
                    pass
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            next_obligation = replace(obligation, round=obligation.round + 1)
            with self._state_lock:
                if self._output_replay_is_obsolete_locked(pending, identity):
                    return True
                self._mark_protocol_unresolved(pending, "output_adoption_wait", next_obligation, target_node_id=obligation.node_id)
                delay = min(_PUSH_RETRY_MAX_SECONDS, _PUSH_RETRY_BASE_SECONDS * 2 ** min(next_obligation.round, 8))
                self._submissions.put(_DelayedReadyTask(
                    _ReadyTask(pending, pending.spec, output_adoption=next_obligation),
                    time.monotonic() + delay,
                ))
            return False
        self._clear_protocol_unresolved(pending)
        with self._state_lock:
            getattr(self, "_output_result_custody", {}).pop(identity, None)
        for output_id in pending.output_ids:
            self._enqueue_inline_gc_check(output_id)
        return True

    def _publish_reply(
        self, pending: _PendingTask, reply: object, *,
        expected_node_id: Optional[NodeID] = None,
        expected_lease_id: Optional[LeaseID] = None,
    ) -> bool:
        """Publish ordinary Task results through the single-output path."""
        if (not isinstance(reply, protocol.TaskReply)
                or reply.task_id != pending.task_id
                or reply.attempt_id != pending.spec.attempt_id):
            return False
        reply = replace(reply)
        if reply.status is not protocol.TaskReplyStatus.SUCCEEDED:
            return self._publish_task_error(
                pending, _remote_error(reply),
                failure_kind=FailureKind.APPLICATION
                if reply.status is protocol.TaskReplyStatus.APPLICATION_ERROR else None,
            )
        envelope = reply.output_publication
        if (type(envelope) is not OutputPublicationEnvelope
                or envelope.manifest.execution != pending.execution
                or envelope.manifest.header.owner_worker_id != self.worker_id
                or envelope.manifest.header.job_id != pending.spec.job_id
                or envelope.manifest.header.executor_worker_id != reply.worker_id
                or expected_lease_id is not None and envelope.publication_id.lease_id != expected_lease_id
                or expected_node_id is not None and envelope.manifest.header.node_incarnation.node_id != expected_node_id):
            raise SystemTaskError("ordinary Task success requires its exact single-output envelope")
        with self._state_lock:
            if self._output_replay_is_obsolete_locked(pending, envelope.publication_id):
                return False
        results = self._decode_reply(pending, reply, expected_node_id=expected_node_id)
        if envelope.results != results:
            raise SystemTaskError("output envelope changed its result descriptors")
        return self._drive_output_publication_adoption(pending, _OutputAdoptionObligation(
            envelope, envelope.manifest.header.node_incarnation.node_id,
        ))

    def _publish_actor_reply(
        self, pending: _PendingTask, reply: object, *,
        expected_node_id: Optional[NodeID] = None,
    ) -> bool:
        """Publish a fenced serial Actor call without Task lineage or a lease.

        ActorWorker owns its lifetime allocation. Its plain result descriptors
        use the same owner table / physical store, not the ordinary Task output
        publication protocol. Nested output references are outside this slice;
        an invalid reply cannot authorize contained-reference cleanup effects.
        """
        if not isinstance(reply, protocol.TaskReply):
            raise SystemTaskError("Actor returned an invalid task reply")
        reply = replace(reply)
        if ((reply.task_id != pending.task_id) or (reply.attempt_id != pending.spec.attempt_id) or (len(pending.output_ids) != 1) or (reply.output_publication is not None)):
            raise SystemTaskError("Actor reply cannot publish ordinary Task or contained-reference output")
        if reply.status is not protocol.TaskReplyStatus.SUCCEEDED:
            self._publish_error(pending.object_id, pending.spec.attempt_id, _remote_error(reply))
            return True
        results = self._decode_reply(pending, reply, expected_node_id=expected_node_id)
        with self._state_lock:
            if not self._is_current_task_pending(pending):
                return False
            if any(result.storage is protocol.ResultStorage.OBJECT_STORE and self._node_is_dead(result.node_id)
                   for result in results):
                return False
            plan = self._owner_table.validate_publish_task_outputs(pending.execution, results)
            if plan is None:
                return False
            self._owner_table.commit_validated_publish_task_outputs(plan)
            for result in results:
                if result.storage is protocol.ResultStorage.OBJECT_STORE:
                    self._stored_descriptors[result.object_id] = result
                self._wake_object(result.object_id)
        self._enqueue_inline_gc_check(pending.object_id)
        return True

    def _fetch_stored_object(
        self, object_id: ObjectID, snapshot: object,
        *, deadline: Optional[float] = None,
    ) -> bytes:
        current_attempt = getattr(snapshot, "current_attempt", None)
        attempted_nodes: set[NodeID] = set()
        while True:
            # Owner metadata and its mutable preferred route form one Core-level
            # read.  Taking either half from an older snapshot can reject a route
            # that a concurrent Node-death/location-report transition just made
            # authoritative.
            with self._state_lock:
                current_snapshot = self._owner_table.snapshot(object_id)
                expected = self._stored_descriptors.get(object_id)
            if (
                current_snapshot.state is not ObjectState.READY_STORED
                or current_snapshot.current_attempt != current_attempt
            ):
                raise _StoredFetchStateChanged()
            locations = current_snapshot.locations
            canonical = current_snapshot.canonical_stored_result
            if expected is None:
                raise SystemTaskError("stored object has no result descriptor")
            if (
                not isinstance(canonical, protocol.ResultDescriptor)
                or canonical.object_id != object_id
                or canonical.storage is not protocol.ResultStorage.OBJECT_STORE
                or expected.object_id != object_id
                or expected.storage is not protocol.ResultStorage.OBJECT_STORE
                or (
                    expected.owner_worker_id, expected.size_bytes,
                    expected.checksum, expected.inline_data,
                ) != (
                    canonical.owner_worker_id, canonical.size_bytes,
                    canonical.checksum, canonical.inline_data,
                )
            ):
                raise SystemTaskError(
                    "stored fetch route conflicts with canonical result identity"
                )
            storage_node_id = expected.node_id
            try:
                self._require_live_node_location(
                    storage_node_id, "stored object fetch"
                )
                if storage_node_id not in locations:
                    raise SystemTaskError(
                        "stored object descriptor is absent from owner locations"
                    )
                requester_route = self._require_home_route(
                    "stored object fetch"
                )
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(
                        "object {} was not ready before timeout".format(object_id)
                    )
                # Address resolution is already an access attempt for this
                # physical route.  Mark it before consulting GCS so an unchanged
                # unavailable route cannot spin forever without a deadline.
                attempted_nodes.add(storage_node_id)
                storage_node_address = self._resolve_node_address_with_timeout_at_route(
                    storage_node_id, remaining, requester_route
                )
                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(
                        "object {} was not ready before timeout".format(object_id)
                    )
                request = protocol.GetObject(
                    object_id, requester_route.node_id,
                    expected_attempt_id=current_attempt,
                    expected_owner_worker_id=expected.owner_worker_id,
                    expected_size_bytes=expected.size_bytes,
                    expected_checksum=expected.checksum,
                )
                token = _RPC_CALL_DEADLINE.set(deadline)
                try:
                    reply = self._rpc(
                        storage_node_address, _GET_OBJECT_HANDLER, request
                    )
                except TransportError as exc:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError(
                            "object {} was not ready before timeout".format(
                                object_id
                            )
                        ) from exc
                    raise
                finally:
                    _RPC_CALL_DEADLINE.reset(token)
                if not isinstance(reply, protocol.GetObjectReply):
                    raise SystemTaskError(
                        "Node returned an invalid object reply"
                    )
                try:
                    # Pickle does not invoke dataclass ``__post_init__``.
                    reply = replace(reply)
                except Exception as exc:
                    raise SystemTaskError(
                        "Node returned a malformed object reply"
                    ) from exc
                if (
                    reply.object_id != object_id
                    or reply.node_id != storage_node_id
                    or not reply.found
                    or not reply.sealed
                    or reply.data is None
                    or reply.checksum is None
                    or reply.producer_attempt_id != current_attempt
                    or reply.owner_worker_id != expected.owner_worker_id
                    or reply.size_bytes != expected.size_bytes
                ):
                    raise SystemTaskError(
                        reply.error or "stored object is unavailable"
                    )
                if (
                    reply.checksum != expected.checksum
                    or len(reply.data) != expected.size_bytes
                    or hashlib.sha256(reply.data).hexdigest()
                    != expected.checksum
                ):
                    raise SystemTaskError(
                        "stored object checksum does not match"
                    )
                # The RPC proves what the selected replica returned, not that
                # its route/attempt remained authoritative while bytes were in
                # flight.  Re-fence under the same Core lock before exposing
                # those bytes to the caller.
                with self._state_lock:
                    final = self._owner_table.snapshot(object_id)
                    final_route = self._stored_descriptors.get(object_id)
                    route_still_authoritative = (
                        final.state is ObjectState.READY_STORED
                        and final.current_attempt == current_attempt
                        and storage_node_id in final.locations
                        and not self._node_is_dead(storage_node_id)
                        and final_route is not None
                        and final_route.node_id == storage_node_id
                    )
                if not route_still_authoritative:
                    raise _StoredFetchStateChanged()
                return reply.data
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                # Node-death handling updates the owner location set and mutable
                # fetch route under this Core lock.  Retry only when that
                # authoritative state has already moved the same logical attempt
                # to a distinct, untried replica.  A transport failure with an
                # unchanged route remains just a transport failure.
                with self._state_lock:
                    latest = self._owner_table.snapshot(object_id)
                    latest_route = self._stored_descriptors.get(object_id)
                if (
                    latest.state is ObjectState.READY_STORED
                    and latest.current_attempt == current_attempt
                    and latest_route is not None
                    and latest_route.node_id in latest.locations
                    and latest_route.node_id not in attempted_nodes
                ):
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError(
                            "object {} was not ready before timeout".format(
                                object_id
                            )
                        ) from exc
                    continue
                if (
                    latest.state is not ObjectState.READY_STORED
                    or latest.current_attempt != current_attempt
                ):
                    raise _StoredFetchStateChanged() from exc
                raise

    def _resolve_node_address(
        self, node_id: NodeID, *, home_route: Optional[_HomeRoute] = None
    ) -> Address:
        """Resolve a physical Node endpoint without treating it as identity."""

        self._require_live_node_location(node_id, "node address lookup")
        route = home_route if home_route is not None else self._home_route_snapshot()
        if route is not None and node_id == route.node_id:
            return route.address
        gcs_address = getattr(self, "gcs_address", None)
        if gcs_address is None:
            raise SystemTaskError(
                "cannot resolve a remote node without a GCS address"
            )
        reply = self._rpc(
            gcs_address,
            _GET_NODE_ADDRESS_HANDLER,
            protocol.GetNodeAddress(node_id),
        )
        if not isinstance(reply, protocol.GetNodeAddressReply):
            raise SystemTaskError(
                "GCS returned an invalid node address reply"
            )
        if reply.node_id != node_id:
            raise SystemTaskError(
                "GCS node address reply has the wrong NodeID"
            )
        if not reply.found or reply.address is None:
            raise SystemTaskError(
                reply.error or "GCS could not resolve the requested node"
            )
        return reply.address

    def _publish_error(
        self, object_id: ObjectID, attempt_id: AttemptID | None, error: BaseException,
    ) -> bool:
        # All CoreWorker result transitions serialize on _state_lock.  The
        # owner table also validates transitions, but checking PENDING here
        # turns expected shutdown/result races into no-ops instead of
        # ConflictingObjectResultError exceptions.  Publication is best-effort
        # at this boundary so a bookkeeping failure cannot kill the dispatcher.
        with self._state_lock:
            if object_id.task_id in getattr(self, "_protocol_unresolved", {}):
                # A local timeout or malformed reply cannot override a remote
                # execution/cancellation whose outcome is still unknown.
                return False
            if not self._is_current_pending(object_id, attempt_id):
                return False
            recovery_plan = None
            try:
                recovery = self._recovery_manager()
                lineage = recovery.lineage_for_object(object_id)
                if isinstance(attempt_id, AttemptID) and lineage is not None:
                    record = recovery.task_record(object_id.task_id)
                    # A direct error publication is retry-unaware.  Update the
                    # recovery authority only while this attempt is still
                    # nonterminal; explicit application/system result paths
                    # have already recorded their typed decision.
                    if record.state.value in {
                        "PENDING", "RUNNING", "RETRY_PENDING"
                    }:
                        if not self._owner_table.validate_publish_error(
                            object_id, attempt_id
                        ):
                            return False
                        recovery_plan = recovery.validate_terminal_system_failure(
                            object_id.task_id, attempt_id, error
                        )
                        if (
                            recovery_plan.decision.action
                            is RecoveryAction.FENCE_STALE_ATTEMPT
                        ):
                            return False
                published = self._owner_table.publish_error(
                    object_id, attempt_id, error
                )
                if published and recovery_plan is not None:
                    recovery.commit_transition(recovery_plan)
            except Exception:
                return False
            if published:
                self._wake_object(object_id)
                coordinator = getattr(self, "_reconstruction", None)
                if coordinator is not None and isinstance(attempt_id, AttemptID):
                    coordinator.complete(object_id.task_id, attempt_id)
        if published:
            self._enqueue_inline_gc_check(object_id)
        return published

    def _publish_task_error(
        self, pending: _PendingTask, error: BaseException,
        *, failure_kind: FailureKind | None = None,
        recovery_plan: object | None = None,
    ) -> bool:
        """Publish one terminal task error to every output atomically."""

        attempt_id = pending.spec.attempt_id
        with self._state_lock:
            if pending.task_key in getattr(self, "_protocol_unresolved", {}):
                return False
            if not self._is_current_task_pending(pending):
                return False
            try:
                published = self._publish_whole_task_error_locked(
                    pending, error, failure_kind, recovery_plan
                )
            except Exception:
                return False
            if published:
                for object_id in pending.output_ids:
                    self._wake_object(object_id)
                coordinator = getattr(self, "_reconstruction", None)
                if coordinator is not None:
                    coordinator.complete(pending.task_id, attempt_id)
        if published:
            for object_id in pending.output_ids:
                self._enqueue_inline_gc_check(object_id)
        return published

    def _publish_whole_task_error_locked(
        self, pending: _PendingTask, error: BaseException,
        failure_kind: FailureKind | None, recovery_plan: object | None,
    ) -> bool:
        """Existing whole-manifest error transaction under state lock."""

        try:
            attempt_id = pending.spec.attempt_id
            recovery = self._recovery_manager()
            lineage = recovery.lineage_for_object(pending.object_id)
            if recovery_plan is None and lineage is not None:
                record = recovery.task_record(pending.task_id)
                if record.state.value in {
                    "PENDING", "RUNNING", "RETRY_PENDING"
                }:
                    recovery_plan = (
                        recovery.validate_task_failure(
                            pending.task_id, attempt_id, failure_kind,
                            error=error,
                        )
                        if failure_kind is FailureKind.APPLICATION
                        else recovery.validate_terminal_system_failure(
                            pending.task_id, attempt_id, error
                        )
                    )
            owner_plan = self._owner_table.validate_publish_task_error(
                pending.execution, error
            )
            if owner_plan is None:
                return False
            if recovery_plan is not None and (
                recovery_plan.decision.action
                is RecoveryAction.FENCE_STALE_ATTEMPT
            ):
                return False
            tuple(self._objects[object_id] for object_id in pending.output_ids)
            self._owner_table.commit_validated_publish_task_error(owner_plan)
            if recovery_plan is not None:
                recovery.commit_validated_transition(recovery_plan)
            return True
        except Exception:
            return False

    def _is_protocol_unresolved(self, pending: _PendingTask) -> bool:
        with self._state_lock:
            record = getattr(self, "_protocol_unresolved", {}).get(
                pending.task_key
            )
            return (
                record is not None
                and record.pending.spec.attempt_id == pending.spec.attempt_id
            )

    def _is_current_pending(
        self, object_id: ObjectID, attempt_id: AttemptID | None
    ) -> bool:
        """Return whether ``attempt_id`` may still publish this object."""

        try:
            snapshot = self._owner_table.snapshot(object_id)
        except Exception:
            return False
        return (
            snapshot.current_attempt == attempt_id
            and snapshot.state is ObjectState.PENDING
        )

    def _is_current_task_pending(self, pending: _PendingTask) -> bool:
        """Whether the declared output admits this physical attempt."""

        return all(
            self._is_current_pending(object_id, pending.spec.attempt_id)
            for object_id in pending.output_ids
        )

    def _wake_object(self, object_id: ObjectID) -> None:
        with self._completion:
            self._objects[object_id].event.set()
            self._completion.notify_all()
        submissions = getattr(self, "_submissions", None)
        if submissions is not None and hasattr(self, "_blocked_tasks"):
            submissions.put(_WAKE_COORDINATOR)

    def _object_waiter(self, object_id: ObjectID) -> _ObjectWaiter:
        with self._state_lock:
            try:
                return self._objects[object_id]
            except KeyError:
                raise ValueError("unknown ObjectRef: {}".format(object_id)) from None

    def _validate_ref(
        self, ref: ObjectRef, *, allow_foreign: bool = False
    ) -> None:
        if not isinstance(ref, ObjectRef):
            raise TypeError("expected an ObjectRef")
        if ref.closed:
            raise ValueError("ObjectRef is closed")
        if ref.owner_worker_id != self.worker_id:
            if allow_foreign:
                return
            raise NotImplementedError(
                "foreign ObjectRef dependencies are outside the first "
                "owner/borrower slice"
            )
        self._object_waiter(ref.object_id)

    def _resolve_task_dependencies(
        self,
        spec: protocol.TaskSpec,
        dependency_hold: protocol.TaskReferenceHold,
    ) -> tuple[tuple[object, ...], dict[str, object], tuple[ObjectID, ...]]:
        """Protect and materialize local top-level refs before leasing."""

        if (
            dependency_hold.kind
            is not protocol.TaskReferenceHoldKind.SUBMITTED
            or dependency_hold.submitting_worker_id != spec.owner_worker_id
            or dependency_hold.task_id != spec.task_id
        ):
            raise ValueError(
                "dependency_hold does not belong to the submitted task"
            )

        arguments = spec.args + tuple(argument for _, argument in spec.kwargs)
        references = top_level_references(arguments)
        protected: list[ObjectID] = []
        try:
            for reference in references:
                if reference.owner_worker_id != self.worker_id:
                    raise NotImplementedError(
                        "foreign ObjectRef dependencies are outside the first "
                        "owner/borrower slice"
                    )
                self._object_waiter(reference.object_id)
                self._owner_table.add_submitted_reference(
                    reference.object_id, dependency_hold
                )
                protected.append(reference.object_id)

            with self._completion:
                while any(
                    not self._owner_table.snapshot(ref.object_id).is_ready
                    for ref in references
                ):
                    self._completion.wait()

            def is_ready(object_id: object) -> bool:
                return self._owner_table.snapshot(object_id).is_ready  # type: ignore[arg-type]

            def load_payload(
                object_id: object, owner_worker_id: object
            ) -> bytes:
                if owner_worker_id != self.worker_id:
                    raise ValueError("ObjectRef owner does not match this CoreWorker")
                snapshot = self._owner_table.snapshot(object_id)  # type: ignore[arg-type]
                if snapshot.state is ObjectState.ERROR:
                    assert isinstance(snapshot.error, BaseException)
                    raise snapshot.error
                if snapshot.state is ObjectState.READY_INLINE:
                    if snapshot.inline_data is None:
                        raise SystemTaskError("inline dependency has no bytes")
                    payload = snapshot.inline_data
                elif snapshot.state is ObjectState.READY_STORED:
                    payload = self._fetch_stored_object(object_id, snapshot)  # type: ignore[arg-type]
                else:
                    raise SystemTaskError("ready dependency has no result bytes")
                return payload

            def materialize(object_id: object, owner_worker_id: object) -> object:
                return cloudpickle.loads(
                    load_payload(object_id, owner_worker_id)
                )

            positional = resolve_task_arguments(
                spec.args, is_ready=is_ready, materialize_ref=materialize,
            )
            keyword_args = tuple(argument for _, argument in spec.kwargs)
            keyword = resolve_task_arguments(
                keyword_args, is_ready=is_ready, materialize_ref=materialize,
            )
            assert positional.values is not None and keyword.values is not None
            return (
                positional.values,
                {
                    name: value
                    for (name, _), value in zip(spec.kwargs, keyword.values)
                },
                tuple(protected),
            )
        except BaseException:
            for object_id in protected:
                released = self._owner_table.release_submitted_reference(
                    object_id, dependency_hold
                )
                if released:
                    self._enqueue_inline_gc_check(object_id)
            raise

    def _prepare_task_dependencies(
        self, spec: protocol.TaskSpec,
        foreign_guards: tuple[_ForeignDependencyGuard, ...] = (),
    ) -> tuple[
        protocol.TaskSpec,
        tuple[protocol.ObjectStoreDescriptor, ...],
        tuple[ObjectID, ...],
    ]:
        """Wait before leasing and keep stored dependencies byte-free.

        Ready inline results become ordinary InlineArgs.  Ready stored results
        remain RefArgs and are accompanied by immutable source metadata; the
        selected Node pulls and seals those bytes before returning its grant.
        """

        arguments = spec.args + tuple(value for _, value in spec.kwargs)
        references = top_level_references(arguments)
        protected = tuple(reference.object_id for reference in references)
        try:
            local_references = tuple(
                ref for ref in references
                if ref.owner_worker_id == self.worker_id
            )
            with self._completion:
                while any(
                    (
                        snapshot := self._owner_table.snapshot(ref.object_id)
                    ).state is ObjectState.PENDING
                    for ref in local_references
                ):
                    self._completion.wait()

            foreign_by_id = {guard.object_id: guard for guard in foreign_guards}
            foreign_inline: dict[ObjectID, bytes] = {}
            foreign_stored: dict[ObjectID, protocol.ObjectStoreDescriptor] = {}
            for guard in foreign_guards:
                reply = self._query_foreign_dependency_guard(guard)
                self._raise_for_foreign_dependency_terminal(guard, reply)
                if reply.state is protocol.OwnedObjectState.PENDING:
                    # The producer may have entered reconstruction between the
                    # readiness gate and this second owner-authoritative query.
                    # Return to the dependency queue; do not fail the consumer.
                    raise _DependencyBecamePending(
                        "foreign dependency became pending during preparation"
                    )
                if reply.state is protocol.OwnedObjectState.READY_INLINE:
                    assert reply.data is not None
                    foreign_inline[guard.object_id] = reply.data
                elif reply.state is protocol.OwnedObjectState.READY_STORED:
                    assert reply.descriptor is not None
                    foreign_stored[guard.object_id] = reply.descriptor
                else:
                    raise SystemTaskError(
                        "foreign dependency has no ready retained payload"
                    )

            descriptors: list[protocol.ObjectStoreDescriptor] = []
            described: set[ObjectID] = set()

            def prepare(argument: protocol.TaskArg) -> protocol.TaskArg:
                if not isinstance(
                    argument, protocol.RefArg
                ):
                    return argument

                def ready_inline(payload: bytes) -> protocol.TaskArg:
                    return protocol.InlineArg(
                        payload, serializer="cloudpickle"
                    )
                if argument.owner_worker_id != self.worker_id:
                    guard = foreign_by_id.get(argument.object_id)
                    payload = foreign_inline.get(argument.object_id)
                    descriptor = foreign_stored.get(argument.object_id)
                    if guard is None or guard.owner_worker_id != argument.owner_worker_id:
                        raise SystemTaskError(
                            "foreign dependency has no matching retained credential"
                        )
                    if payload is not None:
                        return ready_inline(payload)
                    if descriptor is None:
                        raise SystemTaskError(
                            "foreign stored dependency has no retained descriptor"
                        )
                    if (
                        descriptor.object_id != argument.object_id
                        or descriptor.owner_worker_id != argument.owner_worker_id
                    ):
                        raise SystemTaskError(
                            "foreign stored dependency descriptor changed identity"
                        )
                    if descriptor.object_id not in described:
                        described.add(descriptor.object_id)
                        descriptors.append(descriptor)
                    return argument
                snapshot = self._owner_table.snapshot(argument.object_id)
                if snapshot.state is ObjectState.ERROR:
                    assert isinstance(snapshot.error, BaseException)
                    raise snapshot.error
                if snapshot.state is ObjectState.LOST:
                    if snapshot.producer_task_spec is None:
                        raise UnreconstructableObjectError(
                            "dependency replica was lost and no replayable "
                            "producer lineage exists"
                        )
                    raise SystemTaskError(
                        "dependency is lost while reconstruction is pending"
                    )
                if snapshot.state is ObjectState.READY_INLINE:
                    if snapshot.inline_data is None:
                        raise SystemTaskError(
                            "inline dependency has no bytes"
                        )
                    return ready_inline(snapshot.inline_data)
                if snapshot.state is not ObjectState.READY_STORED:
                    raise SystemTaskError(
                        "ready dependency has no result metadata"
                    )
                with self._state_lock:
                    result = self._stored_descriptors.get(argument.object_id)
                if result is None:
                    raise SystemTaskError(
                        "stored dependency has no result descriptor"
                    )
                if result.node_id not in snapshot.locations:
                    raise SystemTaskError(
                        "stored dependency source is absent from owner locations"
                    )
                descriptor = protocol.ObjectStoreDescriptor(
                        object_id=result.object_id,
                        owner_worker_id=result.owner_worker_id,
                        producer_attempt_id=snapshot.current_attempt,
                        node_id=result.node_id,
                        size_bytes=result.size_bytes,
                        checksum=result.checksum,
                    )
                if descriptor.object_id not in described:
                    described.add(descriptor.object_id)
                    descriptors.append(descriptor)
                return argument

            prepared_args = tuple(prepare(argument) for argument in spec.args)
            prepared_kwargs = tuple(
                (name, prepare(argument)) for name, argument in spec.kwargs
            )
            return (
                replace(spec, args=prepared_args, kwargs=prepared_kwargs),
                tuple(descriptors),
                protected,
            )
        except BaseException:
            # Tokens were acquired atomically with submission registration and
            # are released exactly once by the dispatcher's terminal finally.
            raise

    @staticmethod
    def _validate_granted_dependencies(
        requested: tuple[protocol.ObjectStoreDescriptor, ...],
        grant: protocol.GrantWorkerLease,
    ) -> None:
        """Verify that a grant proves every requested object is local."""
        try:
            requested = tuple(requested)
            granted = tuple(grant.dependencies)
            if any(type(item) is not protocol.ObjectStoreDescriptor for item in requested + granted):
                raise ValueError("dependency inventory must contain descriptors")
            requested = tuple(replace(item) for item in requested)
            granted = tuple(replace(item) for item in granted)
            if len({item.object_id for item in requested}) != len(requested):
                raise ValueError("requested dependency inventory contains duplicate objects")
            # Node localization preserves the request's complete ordered
            # inventory. Never let dict deduplication hide repeated slots or
            # allow an unrelated/omitted replica to vanish during handoff.
            expected = tuple(replace(item, node_id=grant.node_id) for item in requested)
            if granted != expected:
                raise ValueError("grant changed ordered dependency metadata")
        except Exception as exc:
            raise SystemTaskError(
                "lease grant did not prove all requested dependencies local"
            ) from exc

    def _validate_location_report_guards(
        self, requested: tuple[protocol.ObjectStoreDescriptor, ...],
        foreign_guards: tuple[_ForeignDependencyGuard, ...], *, task_id: TaskID | None = None,
    ) -> dict[ObjectID, _ForeignDependencyGuard]:
        """Check structural owner credentials before any lease side effect.

        This is not a liveness check: an owner/hold may retire after preflight,
        and the custody driver must still reconcile every sealed replica.
        INLINE-only guards may legitimately have no store dependency.
        """
        guards = {}
        for guard in foreign_guards:
            if not isinstance(guard, _ForeignDependencyGuard) or guard.object_id in guards:
                raise SystemTaskError("foreign dependency credentials must name unique objects")
            try:
                validated = replace(guard, hold=replace(guard.hold))
            except Exception as exc:
                raise SystemTaskError("foreign dependency credential is malformed") from exc
            if (validated.borrower_worker_id != self.worker_id
                    or (task_id is not None and validated.hold.task_id != task_id)):
                raise SystemTaskError("foreign dependency credential belongs to another consumer")
            guards[validated.object_id] = validated
        for descriptor in requested:
            if descriptor.owner_worker_id == self.worker_id:
                continue
            guard = guards.get(descriptor.object_id)
            if guard is None or guard.owner_worker_id != descriptor.owner_worker_id:
                raise SystemTaskError("foreign dependency grant has no retained owner credential")
        return guards

    def _dependency_owner_routes(self, pending, descriptors):
        """Freeze existing owner routes/holds, not a new borrowing protocol."""
        guards = self._validate_location_report_guards(descriptors, pending.foreign_dependency_guards, task_id=pending.task_id)
        routes = []
        for descriptor in descriptors:
            if descriptor.owner_worker_id == self.worker_id:
                hold, address = pending.dependency_hold, self.owner_address
                if hold is None:
                    raise SystemTaskError("local dependency has no submitted hold")
            else:
                guard = guards[descriptor.object_id]
                hold, address = guard.hold, guard.owner_address
            routes.append(protocol.DependencyOwnerRoute(descriptor.object_id, descriptor.owner_worker_id, address, hold))
        return tuple(routes)

    def _build_location_reports(
        self,
        requested: tuple[protocol.ObjectStoreDescriptor, ...],
        grant: protocol.GrantWorkerLease,
        foreign_guards: tuple[_ForeignDependencyGuard, ...] = (),
    ) -> tuple[_ForeignLocationReport, ...]:
        """Build the complete immutable foreign inventory without local effects.

        The grant already contains local descriptors. Retain both halves in
        _LocationReportState before any owner location or fetch route changes.
        """

        self._validate_granted_dependencies(requested, grant)
        guards = self._validate_location_report_guards(requested, foreign_guards, task_id=grant.task_id)
        return self._location_reports_for_descriptors(grant.dependencies, guards)

    def _build_inventory_location_reports(self, inventory, foreign_guards):
        inventory = protocol.revalidate_lease_dependency_inventory(inventory)
        guards = self._validate_location_report_guards(
            inventory.lease_request.dependencies, foreign_guards, task_id=inventory.lease_request.task_id,
        )
        return self._location_reports_for_descriptors(inventory.descriptors, guards)

    def _location_reports_for_descriptors(self, descriptors, guards):
        reports: list[_ForeignLocationReport] = []
        for descriptor in descriptors:
            if descriptor.owner_worker_id == self.worker_id:
                continue
            guard = guards[descriptor.object_id]
            request = protocol.ReportRetainedObjectLocation(
                descriptor.object_id, descriptor.owner_worker_id,
                guard.borrower_worker_id, guard.hold, descriptor,
            )
            reports.append(_ForeignLocationReport(guard, request))
        return tuple(reports)

    def _report_foreign_dependency_location(
        self, report: _ForeignLocationReport
    ) -> protocol.ReportRetainedObjectLocationReply:
        """Send and validate one exact report without classifying ambiguity."""

        request = report.request
        reply = self._borrow_rpc(
            report.guard.owner_address,
            _REPORT_RETAINED_OBJECT_LOCATION_HANDLER,
            request,
        )
        if not isinstance(reply, protocol.ReportRetainedObjectLocationReply):
            raise ProtocolError(
                "object owner returned an invalid location-report acknowledgement"
            )
        try:
            from copy import deepcopy
            # Pickle may restore a frozen dataclass without invoking its
            # validator.  Reconstruct the ACK before trusting either its typed
            # disposition or its echoed report identity.
            reply = deepcopy(replace(reply))
        except Exception as exc:
            raise ProtocolError(
                "object owner returned a malformed location-report acknowledgement"
            ) from exc
        if (
            reply.object_id != request.object_id
            or reply.owner_worker_id != request.owner_worker_id
            or reply.borrower_worker_id != request.borrower_worker_id
            or reply.hold != request.hold
            or reply.descriptor != request.descriptor
        ):
            raise ProtocolError(
                "object owner returned an invalid location-report acknowledgement"
            )
        return reply

    def _schedule_location_report_replay(
        self,
        pending: _PendingTask,
        prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        state: _LocationReportState,
        acknowledged: set[
            tuple[WorkerID, ObjectID, protocol.TaskReferenceHold]
        ],
    ) -> None:
        next_state = replace(
            state,
            acknowledged_keys=tuple(sorted(acknowledged)),
            round=state.round + 1,
        )
        delay = min(
            _PUSH_RETRY_MAX_SECONDS,
            _PUSH_RETRY_BASE_SECONDS
            * (2 ** min(max(next_state.round - 1, 0), 8)),
        )
        self._mark_protocol_unresolved(
            pending, "location_report_wait", next_state,
            target_node_id=state.node_id,
            output_candidate=OutputPublicationID(state.lease_id, pending.execution),
        )
        self._submissions.put(
            _DelayedReadyTask(
                _ReadyTask(
                    pending, prepared, dependencies,
                    location_state=next_state,
                ),
                time.monotonic() + delay,
            )
        )
        try:
            self._emit(
                "dependency_location_report_replay_scheduled",
                task_id=str(pending.spec.task_id),
                attempt_id=str(pending.spec.attempt_id),
                lease_id=str(state.lease_id),
                acknowledged=len(acknowledged),
                total=len(state.reports),
                replay_round=next_state.round,
            )
        except Exception:
            pass

    def _report_granted_dependency_locations(
        self,
        pending: _PendingTask,
        prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...],
        state: _LocationReportState,
    ) -> bool:
        """Serialize exact post-grant progress without locking across RPC."""
        key = pending.execution, state.lease_id
        with self._state_lock:
            active = getattr(self, "_location_handoff_drivers", None)
            if active is None:
                active = self._location_handoff_drivers = set()
            if key in active:
                return False
            marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
            if marker is not None and isinstance(marker.obligation, _LocationReportState):
                canonical = marker.obligation
                if (canonical.grant != state.grant or canonical.reports != state.reports
                        or canonical.inventory != state.inventory
                        or canonical.lease_request != state.lease_request
                        or canonical.granting_node_address != state.granting_node_address
                        or marker.pending.dependency_hold != pending.dependency_hold
                        or marker.pending.protected_dependencies != pending.protected_dependencies):
                    raise SystemTaskError("post-grant replay changed custody identity")
                state = canonical
            active.add(key)
        try:
            return self._drive_location_handoff(pending, prepared, dependencies, state)
        finally:
            with self._state_lock:
                active.discard(key)
                self._completion.notify_all()

    def _drive_location_handoff(
        self, pending: _PendingTask, prepared: protocol.TaskSpec,
        dependencies: tuple[protocol.ObjectStoreDescriptor, ...], state: _LocationReportState,
    ) -> bool:
        """Hand off all sealed dependencies; cancellation is separate progress.

        The first definitive failure revokes this grant before more owner RPCs.
        Its exact cancellation and every report receipt stay in one record.
        Missing ACKs replay; an exact rejection without custody authority is
        quarantined, not treated as permission to delete shared owner bytes.
        No new thread, lease, producer execution or publication protocol exists.
        """
        receipts = {(reply.owner_worker_id, reply.object_id, reply.hold): reply for reply in state.receipts}
        local_receipts = {reply.descriptor.object_id: reply for reply in state.local_receipts}
        local_descriptors = tuple(item for item in state.descriptors if item.owner_worker_id == self.worker_id)
        deaths = {record.worker_id: record for record in state.owner_deaths}
        cancellation_attempted = False

        def checkpoint(phase):
            nonlocal state
            with self._state_lock:
                marker = getattr(self, "_protocol_unresolved", {}).get(pending.task_key)
                canonical = None if marker is None else marker.obligation
                if isinstance(canonical, _LocationReportState):
                    if (canonical.grant != state.grant or canonical.lease_request != state.lease_request
                            or canonical.inventory != state.inventory
                            or canonical.reports != state.reports
                            or canonical.granting_node_address != state.granting_node_address):
                        raise SystemTaskError("active location handoff changed its retained identity")
                    # An older cancellation may revoke execution while this
                    # ticket's driver is outside the lock doing an owner RPC.
                    # Its sticky failure must join the driver's next checkpoint,
                    # not be overwritten by an earlier success-only snapshot.
                    if canonical.terminal_error is not None:
                        state = replace(state, terminal_error=canonical.terminal_error)
                acknowledged = tuple(sorted(self._foreign_guard_key(report.guard) for report in state.reports
                    if report.guard.owner_worker_id in deaths
                    or ((receipt := receipts.get(self._foreign_guard_key(report.guard))) is not None
                        and receipt.custody_transferred)))
                state = replace(state, acknowledged_keys=acknowledged,
                                receipts=tuple(receipts.values()), owner_deaths=tuple(deaths.values()),
                                local_receipts=tuple(local_receipts.values()))
                self._mark_protocol_unresolved(pending, phase, state, target_node_id=state.node_id,
                                              output_candidate=OutputPublicationID(state.lease_id, pending.execution))

        def reject(error):
            nonlocal state
            if state.terminal_error is None:
                state = replace(state, terminal_error=error)
            checkpoint("location_rejected")

        def observe_owner(owner):
            record = self._owner_table.dead_worker_record(owner)
            if record is not None:
                deaths[owner] = record
                reject(OwnerDiedError("dependency owner {} is confirmed dead".format(owner)))
                return True
            return False

        def observe_target():
            with self._state_lock:
                dead = self._node_is_dead(state.node_id)
                if dead and state.terminal_error is not None:
                    # All target bytes and permission are gone. Preserve the
                    # prior failure rather than falling into consumer retry.
                    checkpoint("location_target_lost")
                    raise _LocationReportRejected(str(state.terminal_error), handoff_complete=True)
            return dead

        def consume_target():
            terminal = self._consume_node_death_at_lane(pending, state.node_id)
            if terminal:
                raise _LocationHandoffTerminal()
            return False

        def local_permission(snapshot):
            hold = pending.dependency_hold
            return (hold is not None and hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
                    and hold.submitting_worker_id == self.worker_id and hold.task_id == pending.task_id
                    and snapshot.object_id in pending.protected_dependencies and hold in snapshot.submitted_tokens)

        def local_unknown(descriptor, exc):
            local_receipts.pop(descriptor.object_id, None)
            reject(SystemTaskError("local replica handoff remains unconfirmed: {}".format(exc)))

        def recheck_local_retirement():
            # Completed local receipts can become retired while foreign RPCs
            # are in flight. Unknown effects remain replayable, not implicit
            # permission to forget a local replica or overwrite its epoch.
            for descriptor in local_descriptors:
                if descriptor.object_id not in local_receipts:
                    continue
                try:
                    if self._retain_retired_replica_cleanup_locked(descriptor):
                        local_receipts[descriptor.object_id] = _ReplicaLocationReceipt(
                            descriptor, protocol.RetainedLocationReportStatus.RETIRED, "local dependency retired",
                        )
                        reject(_LocationReportRejected("retired local output replicas retained for exact cleanup; cancel the consumer grant"))
                    elif local_receipts[descriptor.object_id].accepted:
                        snapshot = self._owner_table.snapshot(descriptor.object_id)
                        if not local_permission(snapshot):
                            local_receipts[descriptor.object_id] = _ReplicaLocationReceipt(
                                descriptor, protocol.RetainedLocationReportStatus.CUSTODY_ONLY,
                                "local submitted hold is not active",
                            )
                            self._enqueue_inline_gc_check(descriptor.object_id)
                            reject(_LocationReportRejected("local submitted hold is not active"))
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as exc:
                    local_unknown(descriptor, exc)

        def cancel_once():
            nonlocal state, cancellation_attempted
            if (state.terminal_error is None or state.cancellation_reply is not None
                    or state.execution_outcome is not None or cancellation_attempted):
                return
            if observe_target():
                return
            request = state.lease_request
            if request is None:
                raise SystemTaskError("post-grant cancellation lost its original lease request")
            cancellation = protocol.CancelWorkerLease(
                request.lease_id, request.task_id, request.attempt_id,
                request.requester_node_id, request.requester_worker_id, request.scheduling_key,
                lease_request=request,
            )
            cancellation_attempted = True
            checkpoint("location_cancel_send")
            try:
                reply = self._rpc(state.granting_node_address, _CANCEL_LEASE_HANDLER, cancellation)
                if type(reply) is protocol.CancelWorkerLeaseReply:
                    from copy import deepcopy
                    reply = deepcopy(replace(reply))
                    expected = (cancellation.lease_id, cancellation.task_id, cancellation.attempt_id,
                                cancellation.requester_node_id, cancellation.requester_worker_id, cancellation.scheduling_key)
                    actual = (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
                              reply.requester_worker_id, reply.scheduling_key)
                    if actual == expected and reply.accepted is True and reply.cancelled is True:
                        state = replace(state, cancellation_reply=reply)
                        checkpoint("location_cancelled")
                    elif actual == expected and reply.state is protocol.LeaseExecutionState.WORKER_LOST:
                        # Worker death can win before Cancel on a living Node.
                        # A rejected Cancel alone is not completion authority;
                        # read the exact lease/executor outcome, retaining its
                        # distinct proof instead of manufacturing cancelled.
                        query = protocol.GetWorkerLeaseOutcome(
                            state.grant.lease_id, pending.task_id, pending.spec.attempt_id,
                            state.grant.worker_id, self.worker_id, pending.output_ids,
                            state.grant.scheduling_key,
                        )
                        candidate = self._rpc(state.granting_node_address, _GET_WORKER_LEASE_OUTCOME_HANDLER, query)
                        if type(candidate) is protocol.GetWorkerLeaseOutcomeReply:
                            candidate = deepcopy(replace(candidate))
                            exact_outcome = (
                                candidate.lease_id, candidate.task_id, candidate.attempt_id,
                                candidate.executor_worker_id, candidate.owner_worker_id,
                                candidate.object_ids, candidate.node_id, candidate.scheduling_key,
                            ) == (
                                query.lease_id, query.task_id, query.attempt_id,
                                query.executor_worker_id, query.owner_worker_id,
                                query.object_ids, state.grant.node_id, query.scheduling_key,
                            )
                            if (exact_outcome and candidate.found is True
                                    and candidate.worker_alive is False
                                    and candidate.state is protocol.LeaseExecutionState.WORKER_LOST
                                    and candidate.completion_status is None and not candidate.cleanup_pending
                                    and not candidate.descriptors and not candidate.orphan_descriptors
                                    and candidate.output_publication is None and candidate.output_completion is None):
                                state = replace(state, execution_outcome=candidate)
                                checkpoint("location_executor_lost")
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                pass
            observe_target()

        checkpoint("location_handoff")
        # Scan death and local retirement before any owner call, including on
        # replay of previously accepted reports. The complete grant is already
        # retained in the marker, so cancellation cannot discard later slots.
        with self._state_lock:
            for report in state.reports:
                observe_owner(report.guard.owner_worker_id)
        if observe_target():
            return consume_target()
        cancel_once()
        for descriptor in local_descriptors:
            with self._state_lock:
                if descriptor.object_id not in local_receipts:
                    checkpoint("local_location_handoff")
                    try:
                        receipt = self._record_replica_custody_locked(descriptor, active_hold=local_permission)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        local_unknown(descriptor, exc)
                    else:
                        local_receipts[descriptor.object_id] = receipt
                        if not receipt.accepted:
                            reject(_LocationReportRejected(receipt.error or "local owner rejected replica"))
                        else:
                            checkpoint("local_location_recorded")
            cancel_once()
        for report in state.reports:
            key = self._foreign_guard_key(report.guard)
            with self._state_lock:
                owner_dead = observe_owner(report.guard.owner_worker_id)
            if owner_dead or key in receipts:
                cancel_once()
                continue
            checkpoint("location_report_send")
            if observe_target():
                return consume_target()
            reply = None
            try:
                reply = self._report_foreign_dependency_location(report)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                # Try later owners in this same finite round. A missing ACK
                # for one replica cannot hide their independently sealed bytes.
                pass
            with self._state_lock:
                owner_dead = observe_owner(report.guard.owner_worker_id)
                if reply is not None:
                    receipts[key] = reply
                    if not reply.accepted:
                        reject(_LocationReportRejected(reply.error or "owner rejected dependency location"))
                    else:
                        checkpoint("location_report_received")
            if observe_target():
                return consume_target()
            cancel_once()

        with self._state_lock:
            for report in state.reports:
                observe_owner(report.guard.owner_worker_id)
            recheck_local_retirement()
        cancel_once()
        if observe_target():
            return consume_target()
        with self._state_lock:
            checkpoint("location_handoff_pending")
            # Quarantine has no polling retry. Recheck authoritative deaths and
            # install its phase under the same lock as the death-wakeup path,
            # so a just-consumed death cannot be lost before parking forever.
            for report in state.reports:
                observe_owner(report.guard.owner_worker_id)
            recheck_local_retirement()
            if observe_target():
                return consume_target()
            unknown = (any(self._foreign_guard_key(report.guard) not in receipts
                           and report.guard.owner_worker_id not in deaths for report in state.reports)
                       or any(item.object_id not in local_receipts for item in local_descriptors))
            quarantine = (any(not reply.custody_transferred and reply.owner_worker_id not in deaths
                              for reply in receipts.values())
                          or any(not reply.custody_transferred for reply in local_receipts.values()))
            execution_fenced = state.cancellation_reply is not None or state.execution_outcome is not None
            if unknown or quarantine or (state.terminal_error is not None and not execution_fenced):
                if execution_fenced and not unknown and quarantine:
                    checkpoint("location_quarantined")
                    return False
                self._schedule_location_report_replay(pending, prepared, dependencies, state, set(state.acknowledged_keys))
                return False

        # All owner receipts (or installed owner death delegation) are now
        # retained. Tell the Node only after that cut; a lost ACK replays the
        # exact inventory without acquiring references or creating new work.
        # This acknowledgement is metadata, never a second publish backend.
        if state.inventory is not None and not state.custody_acknowledged:
            checkpoint("dependency_custody_ack_send")
            if observe_target():
                return consume_target()
            request = protocol.AckLeaseDependencyCustody(self.worker_id, state.inventory)
            try:
                reply = self._rpc(state.granting_node_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER, request)
                if type(reply) is not protocol.AckLeaseDependencyCustodyReply:
                    raise SystemTaskError("Node did not acknowledge dependency custody")
                reply = replace(reply)
                if reply.request != request or reply.accepted is not True:
                    raise SystemTaskError("Node did not acknowledge the exact dependency inventory")
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                if observe_target():
                    return consume_target()
                self._schedule_location_report_replay(pending, prepared, dependencies, state, set(state.acknowledged_keys))
                return False
            state = replace(state, custody_acknowledged=True)
            checkpoint("dependency_custody_acknowledged")

        with self._state_lock:
            checkpoint("location_handoff_acknowledged")
            for report in state.reports:
                observe_owner(report.guard.owner_worker_id)
            recheck_local_retirement()
            if observe_target():
                return consume_target()
            if state.terminal_error is not None:
                execution_fenced = state.cancellation_reply is not None or state.execution_outcome is not None
                if execution_fenced:
                    raise _LocationReportRejected(str(state.terminal_error), handoff_complete=True)
                self._schedule_location_report_replay(pending, prepared, dependencies, state, set(state.acknowledged_keys))
                return False
            checkpoint("locations_reported")
        return True

    def _emit(self, name: str, **attributes: object) -> None:
        sink = getattr(self, "event_sink", None)
        if sink is None:
            return
        try:
            sink.emit(name, component="core_worker", attributes=attributes)
        except Exception:
            # Trace is observational.  In particular, GC obligations must never
            # disappear merely because a sink or narrow test fixture is absent.
            return

    def _emit_published_task_reply(
        self,
        pending: _PendingTask,
        reply: protocol.TaskReply,
        **terminal_attributes: object,
    ) -> None:
        """Trace one accepted terminal reply and its actual ready outputs.

        ``reply.results`` is the publication manifest validated immediately
        before this helper is called. Initial execution and reconstruction use
        the same single-output manifest. Keeping ObjectReady after TaskFinished makes
        that semantic order independent of the initial/replay transport path.
        """

        event_name = (
            "task_failed"
            if reply.status is protocol.TaskReplyStatus.APPLICATION_ERROR
            else "task_finished"
        )
        self._emit(
            event_name,
            task_id=str(pending.spec.task_id),
            attempt_id=str(pending.spec.attempt_id),
            object_id=str(pending.object_id),
            status=reply.status.value,
            failure_kind=(
                "APPLICATION"
                if reply.status is protocol.TaskReplyStatus.APPLICATION_ERROR
                else ""
            ),
            **terminal_attributes,
        )
        if reply.status is not protocol.TaskReplyStatus.SUCCEEDED:
            return
        for result in reply.results:
            self._emit(
                "object_ready",
                task_id=str(pending.spec.task_id),
                attempt_id=str(pending.spec.attempt_id),
                object_id=str(result.object_id),
                return_index=result.object_id.return_index,
                storage=result.storage.value,
                node_id=str(result.node_id),
            )


__all__ = [
    "ActorEndpoint", "CoreWorker", "ObjectRef",
    "RemoteFunctionDefinition",
]
