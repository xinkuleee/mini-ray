"""Node-local resources, worker leases, object storage and transfer.

NodeServer owns a small Worker pool, resource ledgers, sealed replicas and
dependency localization. The submitter pushes tasks directly to Workers.
The Node registers each result with its owner before child effects, commits
Complete locally, and independently reports the exact receipt to that owner.
"""

from __future__ import annotations

import multiprocessing as mp
import hashlib
import os
import threading
import time
import traceback
import uuid
from contextlib import ExitStack
from contextvars import ContextVar
from dataclasses import dataclass, replace
from multiprocessing.connection import Connection
from multiprocessing.connection import wait as connection_wait
from typing import Optional

from . import ids, protocol, resources
from .errors import ObjectStoreError
from .control import (
    GET_NODE_STATE_HANDLER as GCS_GET_NODE_STATE_HANDLER,
    GET_WORKER_STATE_HANDLER as GCS_GET_WORKER_STATE_HANDLER,
    REGISTER_WORKER_INCARNATION_HANDLER as GCS_REGISTER_WORKER_INCARNATION_HANDLER,
    REPORT_WORKER_DEATH_HANDLER as GCS_REPORT_WORKER_DEATH_HANDLER,
)
from .owner_service import (
    PREPARE_STORED_CONTAINED_PIN_HANDLER,
    PROMOTE_STORED_CONTAINED_PIN_HANDLER,
    RELEASE_CONTAINED_REFERENCE_HANDLER,
)
from .publication_gate import (
    GraphReservationOutcome, OutputPublicationGate, OutputPublicationGateArrival,
    OutputPublicationGateConfig, OutputPublicationGatePhase,
)
from .placement import Bundle, BundleReservationLedger, ReservationState
from .lease_dependencies import DependencyCustodyConflict, LeaseDependencyCustody
from .transfer_pins import TransferPinOutbox
from .node_death_view import (
    GET_NODE_DEATH_VIEW, PUBLISH_NODE_DEATH_VIEW, GetInstalledNodeDeaths,
    GetInstalledNodeDeathsReply, PublishInstalledNodeDeaths, PublishInstalledNodeDeathsReply,
)
from .placement_group_runtime import PlacementGroupAttempt, participant_digest
from .object_store import ObjectStore
from .object_manager import ObjectManager, PullAction
from .output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation,
    _attempt as _output_attempt, _checksum as _output_checksum,
    _descriptor as _output_descriptor, _node_incarnation as _output_node_incarnation,
    _hold as _output_hold, _object_id as _output_object_id, _opaque as _output_opaque,
)
from .output_publication_journal import (
    OutputPublicationEffect, OutputPublicationJournal, OutputPublicationJournalState,
    OutputPublicationJournalStateError, OutputPublicationStage,
    OutputPublicationPayloadRetired,
)
from .output_publication_node import (
    OutputPublicationNodeAdapter, OutputPublicationBusy, OutputPublicationRemoteError,
)
from .transport import LOOPBACK_HOST, Address, TCPServer, request as rpc_request
from .worker import (
    BEGIN_DRAIN_HANDLER as WORKER_BEGIN_DRAIN_HANDLER,
    DRAIN_STATUS_HANDLER as WORKER_DRAIN_STATUS_HANDLER,
    FINALIZE_SHUTDOWN_HANDLER as WORKER_FINALIZE_SHUTDOWN_HANDLER,
    SHUTDOWN_HANDLER,
    WorkerFailpointConfig,
    worker_main,
)
from .trace import TraceSinkConfig
from .trace_collector import sink_from_config


REQUEST_LEASE_HANDLER = "request_worker_lease"


class _ActorWorkerStartupError(RuntimeError):
    """A typed failure sent by the child before endpoint publication."""

    def __init__(self, failure, detail):
        super().__init__(detail)
        self.failure = failure


CANCEL_LEASE_HANDLER = "cancel_worker_lease"
RELEASE_LEASE_HANDLER = "release_worker_lease"
START_WORKER_LEASE_HANDLER = "start_worker_lease"
COMPLETE_WORKER_LEASE_HANDLER = "complete_worker_lease"
GET_WORKER_LEASE_OUTCOME_HANDLER = "get_worker_lease_outcome"
PREPARE_OUTPUT_PUBLICATION_HANDLER = "prepare_output_publication"
ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER = "ack_output_publication_adopted"
FINALIZE_OUTPUT_OWNER_DEATH_HANDLER = "finalize_output_owner_death"
NOTIFY_WORKER_BLOCKED_HANDLER = "notify_worker_blocked"
NOTIFY_WORKER_UNBLOCKED_HANDLER = "notify_worker_unblocked"
SHUTDOWN_HANDLER_NAME = "shutdown"
SHUTDOWN_STATUS_HANDLER = "shutdown_status"
BEGIN_DRAIN_HANDLER = "begin_drain"
DRAIN_STATUS_HANDLER = "drain_status"
FINALIZE_SHUTDOWN_HANDLER = "finalize_shutdown"
SEAL_OBJECT_HANDLER = "seal_object"
GET_OBJECT_HANDLER = "get_object"
DEFAULT_INLINE_THRESHOLD_BYTES = 100 * 1024
DEFAULT_OBJECT_STORE_BYTES = 16 * 1024 * 1024
WORKER_START_TIMEOUT_SECONDS = 5.0
WORKER_STOP_TIMEOUT_SECONDS = 3.0
WORKER_REPLACEMENT_RETRY_BASE_SECONDS = 0.01
WORKER_REPLACEMENT_RETRY_MAX_SECONDS = 0.25
ACTOR_SUPERVISOR_POLL_SECONDS = 0.05
GCS_REGISTER_NODE_HANDLER = "register_node"
GCS_UPDATE_NODE_RESOURCES_HANDLER = "update_node_resources"
GCS_UNREGISTER_NODE_HANDLER = "unregister_node"
GCS_REPORT_ACTOR_WORKER_EXIT_HANDLER = "report_actor_worker_exit"
INSTALL_CLUSTER_SNAPSHOT_HANDLER = "install_cluster_snapshot"
PIN_OBJECT_HANDLER = "pin_object_for_transfer"
GET_OBJECT_CHUNK_HANDLER = "get_object_chunk"
RELEASE_OBJECT_PIN_HANDLER = "release_object_pin"
DROP_OBJECT_REPLICA_HANDLER = "drop_object_replica"
INSTALL_OWNER_DEATH_FENCE_HANDLER = "install_owner_death_fence"
RESERVE_ACTOR_WORKER_HANDLER = "reserve_actor_worker"
PREPARE_PLACEMENT_GROUP_HANDLER = "prepare_placement_group"
COMMIT_PLACEMENT_GROUP_HANDLER = "commit_placement_group"
ABORT_PLACEMENT_GROUP_HANDLER = "abort_placement_group"
OBJECT_TRANSFER_CHUNK_BYTES = 16 * 1024
_LOCALIZING_LEASE: ContextVar[Optional[protocol.RequestWorkerLease]] = ContextVar(
    "miniray_localizing_lease", default=None,
)


@dataclass
class _LeaseRecord:
    request: protocol.RequestWorkerLease
    allocation_token: resources.AllocationToken
    grant: protocol.GrantWorkerLease
    # Ordinary leases debit the root Node ledger.  Placement-group leases debit
    # one committed bundle child ledger whose capacity was already charged to
    # the root exactly once during prepare.  Every later lifecycle transition
    # must use this same authority rather than rediscovering it from mutable PG
    # state (which may already be REMOVING).
    allocation_ledger: Optional[resources.ResourceLedger] = None
    state: protocol.LeaseExecutionState = protocol.LeaseExecutionState.GRANTED
    completion: Optional[protocol.CompleteWorkerLease] = None
    # Target-local dependency replicas are pinned from grant publication until
    # the lease reaches an authoritative terminal state.  Transfer pins protect
    # only source reads; these independent pins protect Worker materialization.
    dependency_pins: tuple[tuple[ids.ObjectID, object], ...] = ()
    # Blocking episodes are per physical execution attempt.  ``sequence`` is
    # the greatest observed episode, ``open`` records whether its Blocked was
    # accepted without a matching Unblocked, and ``cpu_yielded`` distinguishes
    # ordinary CPU tasks from zero-CPU tasks whose episode is still fenced.
    blocking_sequence: int = -1
    blocking_open: bool = False
    cpu_yielded: bool = False
    # Single-output binding; payloads belong to its journal only.
    output_publication_id: Optional[OutputPublicationID] = None
    output_complete_inflight: Optional[protocol.CompleteWorkerLease] = None


@dataclass(frozen=True)
class _LeaseOutcome:
    request: protocol.RequestWorkerLease
    reply: object


@dataclass(frozen=True)
class _LeaseCancellation:
    request: protocol.CancelWorkerLease
    reply: protocol.CancelWorkerLeaseReply


@dataclass
class _PinnedTransfer:
    descriptor: protocol.ObjectStoreDescriptor
    requester_node_id: ids.NodeID
    pin_token: object
    released: bool = False
    acquired: bool = False
    closing: bool = False


@dataclass
class _ClosedTransferPin:
    request: protocol.ReleaseObjectPin
    closed: bool = False


@dataclass(frozen=True)
class _OutputReplicaWriteClaim:
    """Exact custody of an uncommitted write in the ordinary Node store.

    This is not another replica directory or store.  It bridges only the
    create/write/seal -> sealed-metadata gap so compensation cannot delete a
    pre-existing partial object that this publication never created.
    """

    effect: OutputPublicationEffect
    expected_metadata: tuple[ids.AttemptID, ids.WorkerID, int, str]

    def matches_drop(self, request: protocol.DropObjectReplica) -> bool:
        """A physical completion may retire only its exact writer's claim."""
        metadata = self.expected_metadata
        return (
            self.effect.stage is OutputPublicationStage.MATERIALIZE
            and self.effect.transfer_index is None
            and self.effect.publication_id.object_id == request.object_id
            and self.effect.publication_id.attempt_id == request.producer_attempt_id
            and (metadata[0], metadata[1], metadata[3])
            == (request.producer_attempt_id, request.owner_worker_id, request.checksum)
        )


@dataclass
class _DependencyPinCleanup:
    """Exact target-replica unpin work retained after a local failure."""

    object_id: ids.ObjectID
    pin_token: object
    retry_round: int = 0
    retry_after: float = 0.0
    last_error: Optional[str] = None


@dataclass(frozen=True)
class _OwnerDeathFenceOutcome:
    """One request-ID binding and its immutable Node witness."""

    request: protocol.InstallOwnerDeathFence
    reply: protocol.InstallOwnerDeathFenceReply


@dataclass
class _ActorWorkerRecord:
    request: protocol.ReserveActorWorkerRequest
    allocation_token: resources.AllocationToken
    process: object
    startup: protocol.ActorWorkerStartup
    reply: protocol.ReserveActorWorkerReply


@dataclass
class _ActorGenerationOutcome:
    """Durable Node evidence for one terminal Actor incarnation.

    The live record is removed before this outcome is published.  Keeping the
    exact request and exit record lets the GCS replay the one authorized
    ``generation + 1`` reservation without asking the Node to infer policy.
    """

    request: protocol.ReserveActorWorkerRequest
    reply: protocol.ReserveActorWorkerReply
    exit_record: protocol.ActorWorkerExitRecord
    report: protocol.ReportActorWorkerExit
    report_reply: Optional[protocol.ReportActorWorkerExitReply] = None
    last_report_error: Optional[str] = None
    report_inflight: bool = False


@dataclass(frozen=True)
class _ActorStopResult:
    """Terminal evidence for one dedicated Actor Worker."""

    actor_id: ids.ActorID
    worker_id: ids.WorkerID
    pid: int
    exitcode: Optional[int]
    clean: bool
    forced: bool


@dataclass
class _WorkerSlot:
    """One bounded ordinary Worker owned exclusively by this Node."""

    worker_id: ids.WorkerID
    process: object | None = None
    address: Optional[Address] = None
    pid: Optional[int] = None
    exitcode: Optional[int] = None
    forced: bool = False
    active_lease_id: Optional[ids.LeaseID] = None
    # A dead incarnation leaves its stable slot as an explicit tombstone until
    # a fresh WorkerID is published.  Startup failures are transient Node-local
    # work, not a reason to silently lose the slot or its sole supervisor.
    replacement_retry_round: int = 0
    replacement_retry_after: float = 0.0
    replacement_error: Optional[str] = None
    # Set only after GCS has exactly acknowledged this physical Worker.  A
    # non-None endpoint without this identity would let a lease escape before
    # cluster membership knows which owner incarnation may later die.
    incarnation: Optional[protocol.WorkerIncarnation] = None


@dataclass
class _WorkerDeathReportOutcome:
    """Stable Node outbox entry for one unexpected ordinary Worker exit."""

    report: protocol.ReportWorkerDeath
    # The address disappears from the schedulable slot during local reclaim.
    # Retain it here with the PID-bearing incarnation so tests and diagnostics
    # can prove that a replay never drifts to the replacement endpoint.
    worker_address: Address
    last_report_error: Optional[str] = None
    report_inflight: bool = False


@dataclass(frozen=True)
class _WorkerStopResult:
    worker_id: ids.WorkerID
    pid: Optional[int]
    exitcode: Optional[int]
    clean: bool
    forced: bool


def _new_id(id_type: type) -> object:
    for name in ("new", "random", "generate"):
        factory = getattr(id_type, name, None)
        if callable(factory):
            return factory()
    raise TypeError("{} has no random ID factory".format(id_type.__name__))


class NodeServer:
    """Own one worker process, one resource ledger, and idempotent leases."""

    def __init__(
        self,
        node_id: ids.NodeID,
        total_resources: resources.ResourceVector,
        *,
        worker_id: Optional[ids.WorkerID] = None,
        host: str = LOOPBACK_HOST,
        port: int = 0,
        request_timeout: float = 10.0,
        multiprocessing_context: Optional[mp.context.BaseContext] = None,
        inline_threshold: int = DEFAULT_INLINE_THRESHOLD_BYTES,
        object_store_bytes: int = DEFAULT_OBJECT_STORE_BYTES,
        gcs_address: Optional[Address] = None,
        scheduling_policy: Optional[resources.HybridPolicy] = None,
        worker_failpoint: Optional[WorkerFailpointConfig] = None,
        trace_config: Optional[TraceSinkConfig] = None,
        num_workers_per_node: int = 1,
        output_publication_gate: Optional[OutputPublicationGateConfig] = None,
    ) -> None:
        if (
            isinstance(inline_threshold, bool)
            or not isinstance(inline_threshold, int)
            or inline_threshold < 0
        ):
            raise ValueError("inline_threshold must be a non-negative integer")
        if (
            isinstance(num_workers_per_node, bool)
            or not isinstance(num_workers_per_node, int)
            or num_workers_per_node not in (1, 2)
        ):
            raise ValueError("num_workers_per_node must be 1 or 2")
        if output_publication_gate is not None and type(output_publication_gate) is not OutputPublicationGateConfig:
            raise TypeError("output_publication_gate must be an OutputPublicationGateConfig or None")
        self.node_id = node_id
        first_worker_id = worker_id or _new_id(ids.WorkerID)
        worker_ids = (first_worker_id,) + tuple(
            _new_id(ids.WorkerID) for _ in range(num_workers_per_node - 1)
        )
        self._worker_order = worker_ids
        self._workers = {
            worker: _WorkerSlot(worker) for worker in worker_ids
        }
        self.num_workers_per_node = num_workers_per_node
        self._host = host
        self._context = multiprocessing_context or mp.get_context("spawn")
        self._ledger = resources.ResourceLedger(total_resources)
        self._bundle_reservations = BundleReservationLedger(self._ledger)
        # BundleReservationLedger deliberately keys only by (PG, attempt) and
        # bundle contents.  The protocol additionally freezes a canonical
        # participant digest; retain that identity separately so an exact key
        # cannot be replayed with drifted coordinator metadata.
        self._placement_group_digests: dict[
            tuple[ids.PlacementGroupID, int], str
        ] = {}
        self._gcs_address = gcs_address
        self.event_sink = sink_from_config(trace_config)
        self._trace_config = trace_config
        # Construct the policy inside the Node process.  HybridPolicy owns a
        # lock and should not be passed through multiprocessing ``spawn``.
        self._scheduling_policy = scheduling_policy or resources.HybridPolicy(
            seed=0
        )
        self._registered_with_gcs = False
        self._node_pid = os.getpid()
        self._registration_epoch = 0
        # GCS may tell a newly registered Node that the global membership epoch
        # already advanced because sibling Nodes registered first.  Observing
        # that number is not the same as installing its corresponding immutable
        # scheduling snapshot.  Keep the two facts separate so the first
        # bootstrap install at that same epoch is not mistaken for a conflicting
        # replay.
        self._membership_epoch = 0
        self._installed_membership_epoch = 0
        self._resource_report_version = 0
        self._resource_reported_version = 0
        self._cluster_snapshot_id: Optional[str] = None
        self._cluster_nodes = (
            resources.NodeSnapshot(
                self.node_id, self._ledger.total, self._ledger.available
            ),
        )
        self._cluster_addresses: dict[ids.NodeID, Address] = {}
        self._installed_snapshot_nodes: tuple[protocol.NodeInfo, ...] | None = None
        self._certified_node_deaths = None
        self._inline_threshold = inline_threshold
        self._worker_failpoint = worker_failpoint
        self._output_publication_gate = (
            None if output_publication_gate is None
            else OutputPublicationGate(output_publication_gate)
        )
        self._object_store = ObjectStore(object_store_bytes)
        self._object_manager = ObjectManager(self.node_id, self._object_store)
        self._sealed_metadata: dict[
            ids.ObjectID, tuple[ids.AttemptID, ids.WorkerID, int, str]
        ] = {}
        # Deletion fences retain the highest retiring producer epoch.
        # This is not a completion receipt: bytes, metadata or a write claim
        # may still need cleanup after an interrupted operation.
        self._dropped_metadata: dict[
            ids.ObjectID, tuple[ids.AttemptID, ids.WorkerID, str]
        ] = {}
        # The watermark fences future writes; exact successful drop receipts
        # separately answer an old lost ACK after a newer epoch occupies the
        # same ObjectID. Immutable scalar keys never retain caller-owned DTOs.
        self._replica_drop_receipts: set[tuple[bytes, int, int, bytes, bytes, str]] = set()
        self._local_replica_write_claims: dict[
            ids.ObjectID, _OutputReplicaWriteClaim
        ] = {}
        self._pinned_transfers: dict[str, _PinnedTransfer] = {}
        self._closed_transfer_pins: dict[str, _ClosedTransferPin] = {}
        self._source_pin_releases = TransferPinOutbox()
        self._transfer_node_deaths: dict[ids.NodeID, protocol.NodeDeathRecord] = {}
        self._dependency_pin_cleanups: dict[
            tuple[ids.ObjectID, object], _DependencyPinCleanup
        ] = {}
        self._object_localization_locks: dict[ids.ObjectID, threading.Lock] = {}
        self._lease_dependency_custody = LeaseDependencyCustody(self.node_id)
        self._dead_dependency_submitters: dict[ids.WorkerID, protocol.WorkerDeathRecord] = {}
        self._dependency_handoff_drivers: set[ids.LeaseID] = set()
        self._localization_seal_witnesses: dict[
            tuple[ids.LeaseID, ids.ObjectID], protocol.ObjectStoreDescriptor
        ] = {}
        # The first committed proof permanently fences this Worker owner on
        # the Node.  Separate request outcomes let multiple publications owned
        # by that same Worker obtain independent, frozen replica scans.
        self._owner_death_fences: dict[
            ids.WorkerID, protocol.WorkerDeathRecord
        ] = {}
        self._owner_death_fence_outcomes: dict[
            str, _OwnerDeathFenceOutcome
        ] = {}
        self._output_publication_journal = OutputPublicationJournal()
        self._output_publications = self._make_output_publication_adapter()
        self._actor_workers: dict[object, _ActorWorkerRecord] = {}
        self._actor_creation_locks: dict[object, threading.Lock] = {}
        # Actor Workers are lifetime processes, not fungible ordinary Worker
        # slots.  Their supervisor only turns an exact child sentinel into a
        # durable exit record.  Restart policy and budget remain in GCS.
        self._actor_generation_outcomes: dict[
            ids.ActorGeneration, _ActorGenerationOutcome
        ] = {}
        self._actor_worker_ids_seen: set[ids.WorkerID] = set()
        self._actor_worker_pids_seen: set[int] = set()
        self._actor_supervisor_stop = threading.Event()
        self._actor_supervisor_thread: Optional[threading.Thread] = None
        self._actor_supervisor_wait = connection_wait
        self._actor_lifecycle_lock = threading.Lock()
        self._actor_finalize_request_id: Optional[str] = None
        self._actor_finalize_results: dict[ids.ActorID, _ActorStopResult] = {}
        self._shutdown_request_id: Optional[str] = None
        self._worker_drain_statuses: dict[ids.WorkerID, protocol.DrainStatus] = {}
        self._worker_finalize_results: dict[ids.WorkerID, _WorkerStopResult] = {}
        # WorkerID identifies one physical process incarnation.  A stable slot
        # position may receive a fresh WorkerID after an unexpected exit; old
        # lease records and this compact death table remain queryable by Core.
        self._dead_worker_exitcodes: dict[ids.WorkerID, Optional[int]] = {}
        # Unexpected PROCESS_EXIT facts are Node-owned durable obligations.
        # Local lease reclamation never depends on GCS availability, while the
        # exact frozen report remains here until its matching tombstone ACK.
        self._worker_death_reports: dict[
            ids.WorkerID, _WorkerDeathReportOutcome
        ] = {}
        self._worker_supervisor_stop = threading.Event()
        self._worker_supervisor_thread: Optional[threading.Thread] = None
        self._worker_replacements_inflight = 0
        self._worker_lifecycle_lock = threading.Lock()
        self._finalize_exit_scheduled = False
        self._leases: dict[ids.LeaseID, _LeaseRecord] = {}
        self._lease_outcomes: dict[ids.LeaseID, _LeaseOutcome] = {}
        self._lease_cancellations: dict[ids.LeaseID, _LeaseCancellation] = {}
        self._lease_request_locks: dict[ids.LeaseID, threading.Lock] = {}
        self._inflight_lease_requests = 0
        self._state_lock = threading.RLock()
        self._scheduling_lock = threading.Lock()
        self._gcs_lifecycle_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._server = TCPServer(
            {
                REQUEST_LEASE_HANDLER: self._handle_request_lease,
                CANCEL_LEASE_HANDLER: self._handle_cancel_worker_lease,
                protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER: self._handle_ack_lease_dependency_custody,
                RELEASE_LEASE_HANDLER: self._handle_release_lease,
                START_WORKER_LEASE_HANDLER: self._handle_start_worker_lease,
                COMPLETE_WORKER_LEASE_HANDLER: self._handle_complete_worker_lease,
                GET_WORKER_LEASE_OUTCOME_HANDLER: (
                    self._handle_get_worker_lease_outcome
                ),
                PREPARE_OUTPUT_PUBLICATION_HANDLER: self._handle_prepare_output_publication,
                ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER: self._handle_ack_output_publication_adopted,
                FINALIZE_OUTPUT_OWNER_DEATH_HANDLER: self._handle_finalize_output_owner_death,
                NOTIFY_WORKER_BLOCKED_HANDLER: self._handle_notify_worker_blocked,
                NOTIFY_WORKER_UNBLOCKED_HANDLER: self._handle_notify_worker_unblocked,
                SEAL_OBJECT_HANDLER: self._handle_seal_object,
                GET_OBJECT_HANDLER: self._handle_get_object,
                INSTALL_CLUSTER_SNAPSHOT_HANDLER: self._handle_install_cluster_snapshot,
                PUBLISH_NODE_DEATH_VIEW: self._handle_publish_installed_node_deaths,
                GET_NODE_DEATH_VIEW: self._handle_get_installed_node_deaths,
                PIN_OBJECT_HANDLER: self._handle_pin_object_for_transfer,
                GET_OBJECT_CHUNK_HANDLER: self._handle_get_object_chunk,
                RELEASE_OBJECT_PIN_HANDLER: self._handle_release_object_pin,
                DROP_OBJECT_REPLICA_HANDLER: self._handle_drop_object_replica,
                INSTALL_OWNER_DEATH_FENCE_HANDLER: (
                    self._handle_install_owner_death_fence
                ),
                RESERVE_ACTOR_WORKER_HANDLER: self._handle_reserve_actor_worker,
                PREPARE_PLACEMENT_GROUP_HANDLER: (
                    self._handle_prepare_placement_group
                ),
                COMMIT_PLACEMENT_GROUP_HANDLER: (
                    self._handle_commit_placement_group
                ),
                ABORT_PLACEMENT_GROUP_HANDLER: (
                    self._handle_abort_placement_group
                ),
                BEGIN_DRAIN_HANDLER: self._handle_begin_drain,
                DRAIN_STATUS_HANDLER: self._handle_drain_status,
                FINALIZE_SHUTDOWN_HANDLER: self._handle_finalize_shutdown,
                SHUTDOWN_HANDLER_NAME: self._handle_shutdown,
                SHUTDOWN_STATUS_HANDLER: self._handle_shutdown_status,
            },
            host=host,
            port=port,
            request_timeout=request_timeout,
            event_sink=self.event_sink,
            trace_component="node",
        )

    @property
    def address(self) -> Address:
        return self._server.address

    @property
    def worker_id(self) -> ids.WorkerID:
        """Read the first deterministic pool slot's current incarnation."""
        with self._state_lock:
            return self._worker_order[0]

    @property
    def worker_address(self) -> Optional[Address]:
        with self._state_lock:
            return self._workers[self._worker_order[0]].address

    @property
    def worker_pid(self) -> Optional[int]:
        with self._state_lock:
            return self._workers[self._worker_order[0]].pid

    @property
    def worker_ids(self) -> tuple[ids.WorkerID, ...]:
        with self._state_lock:
            return tuple(self._worker_order)

    @property
    def worker_pids(self) -> tuple[int, ...]:
        with self._state_lock:
            pids = tuple(self._workers[worker_id].pid for worker_id in self._worker_order)
            if any(pid is None for pid in pids):
                return ()
            return tuple(pid for pid in pids if pid is not None)

    @property
    def worker_addresses(self) -> tuple[Address, ...]:
        with self._state_lock:
            addresses = tuple(self._workers[worker_id].address for worker_id in self._worker_order)
            if any(address is None for address in addresses):
                return ()
            return tuple(address for address in addresses if address is not None)

    @property
    def is_running(self) -> bool:
        return self._server.is_running

    @property
    def resource_ledger(self) -> object:
        return self._ledger

    @property
    def object_store(self) -> ObjectStore:
        return self._object_store

    def start(self) -> Address:
        address = self._server.start()
        self._emit(
            "process_ready",
            node_id=str(self.node_id),
            address=str(address),
        )
        try:
            with self._state_lock:
                self._cluster_addresses = {self.node_id: address}
            # WorkerIncarnation is fenced by the Node registration epoch.  The
            # Node must therefore exist in GCS before a child is spawned, and a
            # ready child is not published into a leaseable slot until its own
            # exact registration has been acknowledged.
            self._register_with_gcs()
            self._start_worker_pool()
            self._start_worker_supervisor()
            # Registration installs the immutable Node PID/incarnation epoch
            # embedded in every Actor exit proof.  A child created in the tiny
            # interval after GCS publication is still present in the live map
            # when this watcher takes its first sentinel snapshot.
            self._start_actor_supervisor()
        except BaseException:
            self._stop_actor_supervisor(sweep=True)
            self._stop_worker_supervisor()
            self._stop_workers()
            self._server.stop()
            raise
        return address

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._stop_event.wait(timeout)

    def stop(self) -> protocol.ShutdownAck:
        self._emit("process_stopping", node_id=str(self.node_id))
        self._stop_event.set()
        with self._state_lock:
            self._begin_remove_all_placement_groups_locked()
        self._flush_pending_resource_report()
        self._stop_actor_supervisor(sweep=True)
        self._stop_worker_supervisor()
        # A child may have exited just before the supervisor observed its
        # sentinel.  Reduce that already-terminal process before the remaining
        # live children cross the intentional-stop cut.
        self._sweep_exited_workers()
        self._flush_pending_worker_death_reports()
        self._drive_transfer_pins(force=True)
        self._release_all_transfer_pins()
        self._server.stop()
        actor_results = self._stop_all_actor_workers()
        results = self._stop_workers()
        self._retry_dependency_pin_cleanups(force=True)
        child_clean = all(result.clean for result in results)
        actor_clean = all(result.clean for result in actor_results)
        forced = any(result.forced for result in results) or any(
            result.forced for result in actor_results
        )
        with self._state_lock:
            resources_clean = self._resources_clean_locked()
        reported = tuple(result for result in results if result.pid is not None)
        child_pids = tuple(result.pid for result in reported)
        child_exitcodes = tuple(result.exitcode for result in reported)
        child_cleans = tuple(result.clean for result in reported)
        child_forced = tuple(result.forced for result in reported)
        return protocol.ShutdownAck(
            request_id=self._shutdown_request_id or "node-finalizer",
            component="node:{}".format(self.node_id),
            clean=actor_clean and child_clean and resources_clean,
            detail=(
                "node and worker stopped"
                if resources_clean
                else "node stopped with an active resource allocation"
            ),
            child_pid=child_pids[0] if child_pids else None,
            child_exitcode=child_exitcodes[0] if child_exitcodes else None,
            child_clean=child_clean,
            forced=forced,
            resources_clean=resources_clean,
            child_pids=child_pids,
            child_exitcodes=child_exitcodes,
            child_cleans=child_cleans,
            child_forced=child_forced,
        )

    def _register_with_gcs(self) -> None:
        """Publish this node's address and initial resource summary.

        Registration is a startup requirement when a GCS address is supplied.
        Later cluster snapshots are explicit scheduling hints; the local
        ResourceLedger remains the allocation authority.
        """

        with self._gcs_lifecycle_lock:
            if self._gcs_address is None:
                return
            with self._state_lock:
                if self._registered_with_gcs:
                    return
                available = self._ledger.available
            reply = self._background_rpc(
                self._gcs_address,
                GCS_REGISTER_NODE_HANDLER,
                protocol.RegisterNode(
                    node_id=self.node_id,
                    node_pid=self._node_pid,
                    address=self.address,
                    total_resources=self._ledger.total,
                    available_resources=available,
                ),
            )
            if isinstance(reply, protocol.RegisterNodeReply):
                if (
                    reply.node_id != self.node_id
                    or reply.node_pid != self._node_pid
                    or not reply.accepted
                ):
                    raise RuntimeError(reply.error or "GCS rejected node registration")
            else:
                raise RuntimeError("GCS returned an invalid node registration reply")
            with self._state_lock:
                self._registration_epoch = reply.registration_epoch
                self._membership_epoch = max(
                    self._membership_epoch, reply.membership_epoch
                )
                self._registered_with_gcs = True


    def _report_resources_to_gcs_best_effort(self) -> bool:
        """Compatibility spelling for the versioned report flusher."""

        with self._state_lock:
            self._mark_resource_report_pending_locked()
        return self._flush_pending_resource_report()

    def _mark_resource_report_pending_locked(self) -> None:
        self._resource_report_version = (
            getattr(self, "_resource_report_version", 0) + 1
        )

    def _flush_pending_resource_report(self) -> bool:
        """Consume one locally committed root-availability notification."""

        # Serialize snapshot and send with registration/unregistration.  The
        # snapshot is read only after this lock is acquired, so an old flusher
        # cannot overtake a newer availability value.
        lifecycle_lock = getattr(self, "_gcs_lifecycle_lock", None)
        if lifecycle_lock is None:
            # Compatibility for narrow object.__new__ state-machine fixtures.
            # Real Nodes construct this lock eagerly before serving RPCs.
            with self._state_lock:
                lifecycle_lock = getattr(self, "_gcs_lifecycle_lock", None)
                if lifecycle_lock is None:
                    lifecycle_lock = threading.Lock()
                    self._gcs_lifecycle_lock = lifecycle_lock
        with lifecycle_lock:
            with self._state_lock:
                version = getattr(self, "_resource_report_version", 0)
                reported = getattr(self, "_resource_reported_version", 0)
                if version <= reported:
                    return False
                if (
                    getattr(self, "_gcs_address", None) is None
                    or not getattr(self, "_registered_with_gcs", False)
                ):
                    return False
                gcs_address = self._gcs_address
                available = self._ledger.available
            try:
                reply = self._background_rpc(
                    gcs_address,
                    GCS_UPDATE_NODE_RESOURCES_HANDLER,
                    protocol.UpdateNodeResources(
                        self.node_id, self._node_pid, self._registration_epoch,
                        version, available,
                    ),
                )
            except Exception:
                # Do not advance reported_version: later Node traffic retries
                # the same-or-newer root truth.
                return False
            if (
                not isinstance(reply, protocol.UpdateNodeResourcesReply)
                or reply.node_id != self.node_id
                or reply.node_pid != self._node_pid
                or reply.registration_epoch != self._registration_epoch
                or reply.report_seq != version
                or not reply.updated
            ):
                return False
            with self._state_lock:
                self._resource_reported_version = max(
                    getattr(self, "_resource_reported_version", 0), version
                )
            return True

    @staticmethod
    def _as_scheduling_snapshot(node: object) -> resources.NodeSnapshot:
        if isinstance(node, resources.NodeSnapshot):
            return node
        converter = getattr(node, "to_scheduling_snapshot", None)
        if callable(converter):
            converted = converter()
            if isinstance(converted, resources.NodeSnapshot):
                return converted
        total = getattr(node, "total_resources", getattr(node, "total", None))
        available = getattr(
            node, "available_resources", getattr(node, "available", None)
        )
        return resources.NodeSnapshot(
            node_id=getattr(node, "node_id"),
            total=total,
            available=available,
            alive=getattr(node, "alive", True),
        )

    def _get_cluster_nodes(
        self,
    ) -> tuple[tuple[resources.NodeSnapshot, ...], dict[ids.NodeID, Address]]:
        """Copy the immutable local scheduling view without network I/O."""

        with self._state_lock:
            return self._cluster_nodes, dict(self._cluster_addresses)

    def _refresh_local_cached_availability_locked(self) -> None:
        """Replace only this node's availability in the installed view.

        Remote entries remain deliberately stale until an explicit snapshot
        refresh.  Their target NodeManager ledger is the final safety check.
        """

        available = self._ledger.available
        self._cluster_nodes = tuple(
            resources.NodeSnapshot(
                node.node_id, node.total, available, node.alive, node.labels
            )
            if node.node_id == self.node_id
            else node
            for node in self._cluster_nodes
        )

    def _ensure_bundle_reservations_locked(self) -> BundleReservationLedger:
        """Return the one PG authority rooted in this Node's real ledger."""

        reservations = getattr(self, "_bundle_reservations", None)
        if reservations is None:
            reservations = BundleReservationLedger(self._ledger)
            self._bundle_reservations = reservations
        if not hasattr(self, "_placement_group_digests"):
            self._placement_group_digests = {}
        return reservations

    @staticmethod
    def _placement_bundles(
        request: protocol.PlacementGroupParticipantRequest,
    ) -> tuple[Bundle, ...]:
        # Protocol DTOs remain transport-only; Bundle is the ledger authority type.
        return tuple(
            Bundle(bundle.bundle_index, bundle.resources)
            for bundle in request.bundles
        )

    def _placement_group_reply(
        self,
        request: protocol.PlacementGroupParticipantRequest,
        *,
        accepted: bool,
        applied: bool,
        error: Optional[str] = None,
    ) -> protocol.PlacementGroupParticipantReply:
        reply_type = {
            protocol.PlacementGroupParticipantPhase.PREPARE:
                protocol.PreparePlacementGroupReply,
            protocol.PlacementGroupParticipantPhase.COMMIT:
                protocol.CommitPlacementGroupReply,
            protocol.PlacementGroupParticipantPhase.ABORT:
                protocol.AbortPlacementGroupReply,
        }[request.phase]
        return reply_type(
            request.placement_group_id, request.attempt, request.node_id,
            request.plan_digest, request.phase, accepted, applied, error,
        )

    def _placement_request_rejection(
        self, request: protocol.PlacementGroupParticipantRequest
    ) -> Optional[protocol.PlacementGroupParticipantReply]:
        if request.node_id != self.node_id:
            return self._placement_group_reply(
                request, accepted=False, applied=False,
                error="placement-group participant request targets another node",
            )
        key = (request.placement_group_id, request.attempt)
        bundles = self._placement_bundles(request)
        expected_digest = participant_digest(
            PlacementGroupAttempt(request.placement_group_id, request.attempt),
            request.node_id,
            bundles,
        )
        if request.plan_digest != expected_digest:
            return self._placement_group_reply(
                request, accepted=False, applied=False,
                error="placement-group participant digest does not match bundles",
            )
        previous_digest = self._placement_group_digests.get(key)
        if previous_digest is not None and previous_digest != request.plan_digest:
            return self._placement_group_reply(
                request, accepted=False, applied=False,
                error="placement-group participant digest changed",
            )
        return None

    def _handle_prepare_placement_group(self, request: object) -> object:
        if not isinstance(request, protocol.PreparePlacementGroupRequest):
            raise TypeError(
                "prepare_placement_group expects PreparePlacementGroupRequest"
            )
        report = False
        with self._state_lock:
            self._ensure_bundle_reservations_locked()
            rejected = self._placement_request_rejection(request)
            if rejected is not None:
                return rejected
            if self._stop_event.is_set() or self._shutdown_request_id is not None:
                return self._placement_group_reply(
                    request, accepted=False, applied=False,
                    error="node is shutting down",
                )
            key = (request.placement_group_id, request.attempt)
            available_before = self._ledger.available
            try:
                prepared = self._bundle_reservations.prepare(
                    request.placement_group_id, request.attempt,
                    self._placement_bundles(request),
                )
            except Exception as exc:
                return self._placement_group_reply(
                    request, accepted=False, applied=False,
                    error="{}: {}".format(type(exc).__name__, exc),
                )
            self._placement_group_digests.setdefault(key, request.plan_digest)
            self._refresh_local_cached_availability_locked()
            if self._ledger.available != available_before:
                self._mark_resource_report_pending_locked()
                report = True
            reply = self._placement_group_reply(
                request, accepted=prepared, applied=prepared,
                error=None if prepared else "placement-group capacity unavailable",
            )
            observed_available = tuple(
                (name, str(quantity))
                for name, quantity in self._ledger.available.to_dict().items()
            )
        if report:
            self._flush_pending_resource_report()
        self._emit(
            "placement_group_prepare_applied",
            placement_group_id=str(request.placement_group_id),
            attempt=request.attempt,
            node_id=str(self.node_id),
            plan_digest=request.plan_digest,
            status=("PREPARED" if prepared else "REJECTED"),
            applied=reply.applied,
            root_available=observed_available,
        )
        return reply

    def _handle_commit_placement_group(self, request: object) -> object:
        if not isinstance(request, protocol.CommitPlacementGroupRequest):
            raise TypeError(
                "commit_placement_group expects CommitPlacementGroupRequest"
            )
        with self._state_lock:
            self._ensure_bundle_reservations_locked()
            rejected = self._placement_request_rejection(request)
            if rejected is not None:
                return rejected
            if (request.placement_group_id, request.attempt) not in self._placement_group_digests:
                return self._placement_group_reply(
                    request, accepted=False, applied=False,
                    error="placement group was not prepared on this node",
                )
            committed = self._bundle_reservations.commit(
                request.placement_group_id, request.attempt
            )
            return self._placement_group_reply(
                request, accepted=committed, applied=committed,
                error=None if committed else "placement group cannot commit",
            )

    def _handle_abort_placement_group(self, request: object) -> object:
        if not isinstance(request, protocol.AbortPlacementGroupRequest):
            raise TypeError(
                "abort_placement_group expects AbortPlacementGroupRequest"
            )
        report = False
        with self._state_lock:
            self._ensure_bundle_reservations_locked()
            rejected = self._placement_request_rejection(request)
            if rejected is not None:
                return rejected
            key = (request.placement_group_id, request.attempt)
            self._placement_group_digests.setdefault(key, request.plan_digest)
            available_before = self._ledger.available
            removed = self._bundle_reservations.abort(
                request.placement_group_id, request.attempt
            )
            self._refresh_local_cached_availability_locked()
            if removed:
                if self._ledger.available != available_before:
                    self._mark_resource_report_pending_locked()
                    report = True
                reply = self._placement_group_reply(
                    request, accepted=True, applied=True
                )
            else:
                snapshot = self._bundle_reservations.snapshot(
                    request.placement_group_id, request.attempt
                )
                removing = (
                    snapshot is not None and snapshot.state is ReservationState.REMOVING
                )
                reply = self._placement_group_reply(
                    # Keep the ABORT obligation until child leases drain.
                    request, accepted=removing, applied=False,
                    error=None if removing else "placement group cannot abort",
                )
            observed_available = tuple(
                (name, str(quantity))
                for name, quantity in self._ledger.available.to_dict().items()
            )
        if report:
            self._flush_pending_resource_report()
        self._emit(
            "placement_group_abort_applied",
            placement_group_id=str(request.placement_group_id),
            attempt=request.attempt,
            node_id=str(self.node_id),
            plan_digest=request.plan_digest,
            status=("ABORTED" if reply.applied else "PENDING"),
            applied=reply.applied,
            root_available=observed_available,
        )
        return reply

    # -- single-output publication --------------------------------------

    def _make_output_publication_adapter(self) -> OutputPublicationNodeAdapter:
        from . import output_protocol as wire

        def owner_request(manifest, handler, message):
            with self._state_lock:
                record = self._output_lease_record_locked(manifest)
                address = record.request.requester_owner_address
            if address is None:
                raise OutputPublicationRemoteError('executable lease has no owner endpoint')
            reply = self._background_rpc(address, handler, message)
            if type(message) is wire.ReportOutputHandoffComplete:
                if type(reply) is not wire.OutputHandoffCompleteAck:
                    raise OutputPublicationConflictError('owner returned an invalid Complete acknowledgement')
                reply = replace(reply)
                if reply.witness != message.witness or not reply.accepted:
                    raise OutputPublicationRemoteError(reply.error or 'owner rejected output Complete')
                return reply
            if type(reply) is not wire.OutputHandoffReply:
                raise OutputPublicationConflictError('owner returned an invalid handoff reply')
            reply = replace(reply)
            if reply.request != message or not reply.accepted:
                raise OutputPublicationRemoteError(reply.error or 'owner rejected output handoff')
            return reply

        def register(manifest):
            from .output_handoff import OutputHandoffPhase
            reply = owner_request(manifest, wire.REGISTER_OUTPUT_HANDOFF_HANDLER, wire.RegisterOutputHandoff(manifest))
            if reply.snapshot.manifest != manifest or reply.snapshot.phase is not OutputHandoffPhase.PENDING:
                raise OutputPublicationRemoteError('owner registration is not forward permission')

        def complete(witness):
            manifest = self._output_publication_journal.snapshot(witness.publication_id).manifest
            owner_request(manifest, wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER, wire.ReportOutputHandoffComplete(witness))

        def rollback(tombstone, *, manifest):
            from .output_handoff import OutputHandoffPhase
            reply = owner_request(manifest, wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER, wire.ReportOutputHandoffRollback(manifest, tombstone))
            if reply.snapshot.phase is not OutputHandoffPhase.ABORTED:
                raise OutputPublicationRemoteError('owner did not fence rolled-back handoff')

        def publication_value(manifest):
            from .enhanced_publication import TaskPublication
            with self._state_lock:
                record = self._output_lease_record_locked(manifest, allow_owner_dead=True)
                address = record.request.requester_owner_address
            if address is None:
                raise OutputPublicationRemoteError('publication lease has no owner endpoint')
            return TaskPublication(manifest, address)

        def publication_rpc(request):
            from .enhanced_publication import (PUBLICATION_HANDLER, RecordTerminal,
                                               PublicationRef, PublicationStageAck, PublicationStage)
            with self._state_lock:
                address = self._gcs_address
            if address is None:
                raise OutputPublicationRemoteError('enhanced publication requires a GCS endpoint')
            gate = getattr(self, '_output_publication_gate', None)
            phase = None if gate is None else gate.config.phase
            terminal_gate = type(request) is RecordTerminal and phase in (
                OutputPublicationGatePhase.BEFORE_TERMINAL_REPORT,
                OutputPublicationGatePhase.AFTER_TERMINAL_ACCEPTED_BEFORE_ACK)
            if terminal_gate:
                manifest = self._output_publication_journal.snapshot(request.complete.publication_id).manifest
                self._validate_terminal_gate_complete(manifest, request.complete)
                if phase is OutputPublicationGatePhase.BEFORE_TERMINAL_REPORT:
                    gate.checkpoint(OutputPublicationGateArrival.from_manifest(manifest, phase))
            reply = self._background_rpc(address, PUBLICATION_HANDLER, request)
            if terminal_gate and phase is OutputPublicationGatePhase.AFTER_TERMINAL_ACCEPTED_BEFORE_ACK:
                if type(reply) is not PublicationStageAck:
                    raise OutputPublicationConflictError('terminal gate requires the actual GCS reply')
                reply = replace(reply)
                reference = PublicationRef(manifest.publication_id, manifest.manifest_digest)
                if (reply.request != request or reply.accepted_fact != request.complete
                        or reply.reference != reference or reply.receipt.reference != reference
                        or reply.owner_worker_id != manifest.header.owner_worker_id
                        or reply.receipt.stage is not PublicationStage.TERMINAL):
                    raise OutputPublicationConflictError('terminal gate requires actual GCS acceptance')
                # Intercept the real reply before the adapter records its ACK.
                # Both direct-result and supervisor paths share this gate; a
                # release deliberately drops the reply, never returns success.
                gate.checkpoint(OutputPublicationGateArrival.from_manifest(manifest, phase))
                raise TimeoutError('test transport discarded accepted GCS terminal reply')
            return reply

        def abort_owner(publication, scope):
            from .enhanced_publication import (AbortOwnerPublication, AbortOwnerPublicationReply,
                                               ABORT_OWNER_PUBLICATION_HANDLER)
            request = AbortOwnerPublication(publication, scope)
            reply = self._background_rpc(publication.owner_address, ABORT_OWNER_PUBLICATION_HANDLER, request)
            if type(reply) is not AbortOwnerPublicationReply:
                raise OutputPublicationConflictError('owner returned invalid abort proof')
            reply = replace(reply)
            if reply.request != request or not reply.accepted or reply.receipt is None:
                raise OutputPublicationRemoteError(reply.error or 'owner did not acknowledge exact abort')
            return reply.receipt

        return OutputPublicationNodeAdapter(
            self._output_publication_journal, register_owner=register,
            report_complete=complete, report_rollback=rollback,
            publication_value=publication_value, publication_rpc=publication_rpc, abort_owner=abort_owner,
            prepare_child=lambda address, request: self._background_rpc(address, PREPARE_STORED_CONTAINED_PIN_HANDLER, request),
            promote_child=lambda address, request: self._background_rpc(address, PROMOTE_STORED_CONTAINED_PIN_HANDLER, request),
            release_child=self._release_output_child_pin,
            seal_replica=self._seal_output_publication_replica,
            drop_replica=self._drop_output_publication_replica,
            test_checkpoint=(self._test_output_publication_checkpoint
                             if getattr(self, '_output_publication_gate', None) is not None else None),
        )

    @staticmethod
    def _copy_output_child_release(request: protocol.ReleaseContainedReference):
        """Freeze the exact child and typed container hold before any RPC."""
        if type(request) is not protocol.ReleaseContainedReference:
            raise TypeError("output child cleanup requires ReleaseContainedReference")
        return protocol.ReleaseContainedReference(
            _output_object_id(request.object_id),
            _output_opaque(request.owner_worker_id, ids.WorkerID, "child owner"),
            _output_hold(request.hold),
        )

    def _release_output_child_pin(
        self, address: Address, request: protocol.ReleaseContainedReference,
    ) -> protocol.ReleaseContainedReferenceReply | protocol.GetWorkerStateReply:
        """Release a child hold, or return an independently proven owner death.

        Losing the route, a rejected ACK, and an observed local exit are not
        ownership terminal proofs.  Only GCS's exact registered Worker
        incarnation and ordered PROCESS_EXIT/NODE_EXIT record can discharge an
        unreachable child owner.  Keep that typed evidence intact instead of
        manufacturing a successful ReleaseContainedReferenceReply.
        """
        request = self._copy_output_child_release(request)
        try:
            candidate = self._background_rpc(
                address, RELEASE_CONTAINED_REFERENCE_HANDLER,
                self._copy_output_child_release(request),
            )
            if type(candidate) is not protocol.ReleaseContainedReferenceReply:
                raise OutputPublicationConflictError(
                    "child release returned an invalid reply type"
                )
            reply = protocol.ReleaseContainedReferenceReply(
                _output_object_id(candidate.object_id),
                _output_opaque(candidate.owner_worker_id, ids.WorkerID, "child owner"),
                _output_hold(candidate.hold),
                candidate.accepted, candidate.released, candidate.error,
            )
            if (reply.object_id != request.object_id
                    or reply.owner_worker_id != request.owner_worker_id
                    or reply.hold != request.hold):
                raise OutputPublicationConflictError(
                    "child release ACK changed the exact requested hold"
                )
            if not reply.accepted:
                raise OutputPublicationRemoteError(reply.error or "child release rejected")
            return reply
        except Exception:
            # A failed query or unusable proof must preserve the original
            # release failure and the journal's still-unacknowledged effect.
            try:
                proof = self._get_output_child_death_proof(request.owner_worker_id)
            except Exception:
                pass
            else:
                if proof is not None:
                    return proof
            raise

    @staticmethod
    def _copy_output_worker_incarnation(value: protocol.WorkerIncarnation):
        if type(value) is not protocol.WorkerIncarnation:
            raise TypeError("registered child owner must have a WorkerIncarnation")
        for name in ("node_pid", "node_registration_epoch", "worker_pid"):
            number = getattr(value, name)
            if type(number) is not int or number <= 0:
                raise ValueError("registered child-owner {} must be positive".format(name))
        return protocol.WorkerIncarnation(
            _output_opaque(value.node_id, ids.NodeID, "child NodeID"),
            value.node_pid, value.node_registration_epoch,
            _output_opaque(value.worker_id, ids.WorkerID, "child WorkerID"),
            value.worker_pid,
        )

    def _get_output_child_death_proof(
        self, owner_worker_id: ids.WorkerID,
    ) -> protocol.GetWorkerStateReply | None:
        """Read GCS death authority without holding a Node/journal lock."""
        from .death_proofs import owner_death as _owner_death

        owner_worker_id = _output_opaque(owner_worker_id, ids.WorkerID, "child owner")
        with self._state_lock:
            gcs_address = getattr(self, "_gcs_address", None)
            if gcs_address is None:
                return None
            slot = getattr(self, "_workers", {}).get(owner_worker_id)
            pending_exit = getattr(self, "_worker_death_reports", {}).get(owner_worker_id)
            registered = tuple(
                self._copy_output_worker_incarnation(value)
                for value in (
                    None if slot is None else slot.incarnation,
                    None if pending_exit is None else pending_exit.report.incarnation,
                ) if value is not None
            )
        candidate = self._background_rpc(
            gcs_address, GCS_GET_WORKER_STATE_HANDLER,
            protocol.GetWorkerState(_output_opaque(owner_worker_id, ids.WorkerID, "child owner")),
            connect_timeout=0.25, request_timeout=0.5, deadline=time.monotonic() + 0.75,
        )
        if type(candidate) is not protocol.GetWorkerStateReply:
            raise TypeError("GCS child-owner query returned an invalid reply type")
        # GetWorkerStateReply alone only checks shallow incarnation equality.
        # Rebuild both independent registrations and death records first, so
        # frozen dataclass corruption cannot turn False into epoch/PID zero.
        reply = protocol.GetWorkerStateReply(
            _output_opaque(candidate.worker_id, ids.WorkerID, "GCS child WorkerID"),
            candidate.found, candidate.watermark, candidate.state,
            None if candidate.incarnation is None else
            self._copy_output_worker_incarnation(candidate.incarnation),
            None if candidate.death is None else _owner_death(candidate.death),
            candidate.error,
        )
        if reply.worker_id != owner_worker_id:
            raise OutputPublicationConflictError("GCS proof names another child owner")
        if (not reply.found or reply.state is not protocol.WorkerMembershipState.DEAD
                or reply.death is None or reply.death.reason not in (
                    protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT,
                )):
            return None
        if any(value != reply.incarnation for value in registered):
            raise OutputPublicationConflictError(
                "GCS child death disagrees with its locally registered incarnation"
            )
        return reply

    def _output_lease_record_locked(self, manifest: OutputPublicationManifest, *, allow_owner_dead=False) -> _LeaseRecord:
        header, identity = manifest.header, manifest.publication_id
        record = self._leases.get(identity.lease_id)
        if record is None:
            raise OutputPublicationConflictError("output publication has no local lease")
        request = record.request
        if ((request.task_id != identity.task_id) or (request.attempt_id != identity.attempt_id) or (request.requester_worker_id != header.owner_worker_id) or (record.grant.worker_id != header.executor_worker_id) or (record.grant.node_id != header.node_incarnation.node_id) or (request.return_ids != (identity.object_id,)) or (header.node_incarnation != OutputPublicationNodeIncarnation(
                    self.node_id, self._node_pid, self._registration_epoch))):
            raise OutputPublicationConflictError("output manifest changed lease execution binding")
        if record.output_publication_id not in (None, identity):
            raise OutputPublicationConflictError("lease output publication was rebound")
        if not allow_owner_dead and header.owner_worker_id in getattr(self, "_owner_death_fences", {}):
            raise OutputPublicationJournalStateError("output owner is death-fenced")
        return record

    def _handle_prepare_output_publication(self, request: object):
        from . import output_protocol as wire

        if type(request) is not wire.PrepareOutputPublication:
            raise TypeError("prepare_output_publication requires its typed request")
        request = replace(request)
        manifest, identity = request.manifest, request.manifest.publication_id
        # Journal open is local and owns no remote effect.  Its lock must never
        # be acquired after the Node state lock used by local materialization.
        try:
            with self._output_publication_journal.linearize(), self._state_lock:
                record = self._output_lease_record_locked(manifest)
                if record.state is not protocol.LeaseExecutionState.RUNNING:
                    raise OutputPublicationJournalStateError("output preparation requires a running lease")
                if record.output_complete_inflight is not None:
                    raise OutputPublicationJournalStateError("output Complete choice already fenced preparation")
                self._output_publication_journal.open(manifest)
                record.output_publication_id = identity
        except Exception as exc:
            return wire.PreparedOutputPublicationReply(
                request.request_identity, False, wire.OutputPublicationRPCErrorKind.CONFLICT,
                str(exc) or type(exc).__name__,
            )
        try:
            self._output_publications.prepare(manifest, request.payload)
        except OutputPublicationBusy:
            raise
        except (OutputPublicationRemoteError, ObjectStoreError) as exc:
            # An exact rejection may follow effects.  The Worker chooses an
            # explicit failed Complete and waits for compensation acknowledgements.
            # A local store refusal is known failure, not an unknown RPC
            # to retry forever.
            return wire.PreparedOutputPublicationReply(
                request.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE,
                str(exc) or type(exc).__name__,
            )
        # Transport/invalid ACK exceptions propagate: replying success or an
        # invented no-effect failure would erase the pending publication.
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    def _commit_output_lease_locked(self, manifest, completion) -> bool:
        """Local-only ledger transition; caller holds journal then Node lock."""
        record = self._output_lease_record_locked(manifest)
        if record.completion is not None and record.completion != completion:
            raise OutputPublicationConflictError("output lease already has another terminal")
        if record.state is protocol.LeaseExecutionState.RUNNING:
            released = self._release_record_locked(record, protocol.LeaseExecutionState.COMPLETED)
        elif record.state in (protocol.LeaseExecutionState.WORKER_LOST, protocol.LeaseExecutionState.ABANDONED):
            record.state = protocol.LeaseExecutionState.COMPLETED
            released = False
        elif record.state is protocol.LeaseExecutionState.COMPLETED:
            released = False
        else:
            raise OutputPublicationJournalStateError("output lease never started")
        record.completion = completion
        record.output_complete_inflight = None
        return released

    def _handle_complete_output_worker_lease(self, request, identity):
        journal, adapter = self._output_publication_journal, self._output_publications
        manifest = journal.snapshot(identity).manifest
        released = False

        def commit(witness):
            nonlocal released
            if witness != OutputPublicationCompleteWitness.for_manifest(manifest):
                raise OutputPublicationConflictError("local Complete witness changed")
            # Caller holds journal then Node state throughout local Complete.
            released = self._commit_output_lease_locked(manifest, request) or released

        with journal.linearize(identity), self._state_lock:
            record = self._output_lease_record_locked(manifest)
            if ((not self._lease_identity_matches(record, request.task_id, request.attempt_id, request.worker_id)) or (request.scheduling_key != record.request.scheduling_key)):
                return self._completion_reply(request, record.state, accepted=False, released=False, error="output Complete identity mismatch")
            snapshot = journal.snapshot(identity)
            expected = record.completion or record.output_complete_inflight
            if expected is not None and expected != request:
                return self._completion_reply(request, record.state, accepted=False, released=False, error="output Complete terminal was already chosen")
            if snapshot.complete is not None and request.status is not protocol.TaskReplyStatus.SUCCEEDED:
                return self._completion_reply(request, record.state, accepted=False, released=False, error="successful output cannot roll back")
            if (snapshot.complete is None and request.status is protocol.TaskReplyStatus.SUCCEEDED
                    and not snapshot.ready_to_complete):
                return self._completion_reply(request, record.state, accepted=False, released=False, error="output Complete requires owner registration, child/materialization and GCS ARM ACKs")
            if (snapshot.complete is None and request.status is protocol.TaskReplyStatus.SUCCEEDED
                    and record.state is not protocol.LeaseExecutionState.RUNNING):
                # A pre-boundary local exception may have left this proposal,
                # but a committed Worker loss now proves it can never win.
                # Remove only that uncrossed proposal so supervisor rollback
                # can proceed; an existing witness is handled above instead.
                record.output_complete_inflight = None
                return self._completion_reply(request, record.state, accepted=False, released=False, error="first successful Complete requires a live RUNNING lease")
            if record.completion is None:
                record.output_complete_inflight = request
            if request.status is protocol.TaskReplyStatus.SUCCEEDED:
                # No RPC occurs here.  Holding both local locks closes the
                # gap in which Worker death/outcome could publish WORKER_LOST
                # between a RUNNING preflight and the irreversible witness.
                try:
                    envelope = adapter.complete(identity, commit_lease=commit)
                except OutputPublicationBusy:
                    if journal.snapshot(identity).complete is None:
                        record.output_complete_inflight = None
                    raise
                except OutputPublicationPayloadRetired:
                    witness = journal.snapshot(identity).complete
                    if witness is None:
                        raise
                    return self._completion_reply(request, protocol.LeaseExecutionState.COMPLETED, accepted=True, released=released, output_completion=witness)
                except Exception:
                    if journal.snapshot(identity).complete is None:
                        record.output_complete_inflight = None
                    raise
                return self._completion_reply(request, protocol.LeaseExecutionState.COMPLETED, accepted=True, released=released, output_publication=envelope)
            rollback_id = "output-failed-lease:{}:{}".format(identity.lease_id, request.status.value)
            journal.begin_rollback(identity, rollback_id)
            released = self._commit_output_lease_locked(manifest, request)
        tombstone = adapter.rollback(identity, rollback_id, max_effects=1)
        if tombstone is None:
            return self._completion_reply(request, protocol.LeaseExecutionState.COMPLETED, accepted=False, released=False, error="output compensation remains pending")
        return self._completion_reply(request, protocol.LeaseExecutionState.COMPLETED, accepted=True, released=released)

    def _handle_ack_output_publication_adopted(self, request: object):
        from . import output_protocol as wire

        if type(request) is not wire.AckOutputPublicationAdopted:
            raise TypeError("output adoption requires its typed proof")
        request = replace(request)
        identity = request.proof.complete.publication_id
        journal = self._output_publication_journal
        with journal.linearize(identity), self._state_lock:
            snapshot = journal.snapshot(identity)
            record = self._output_lease_record_locked(snapshot.manifest)
            if (record.state is not protocol.LeaseExecutionState.COMPLETED
                    or record.completion is None
                    or record.completion.status is not protocol.TaskReplyStatus.SUCCEEDED):
                raise OutputPublicationJournalStateError("output adoption preceded local lease completion")
            from .enhanced_publication import PublicationReceipt, PublicationStage, PublicationRef
            if (type(request.gcs_adoption) is not PublicationReceipt
                    or request.gcs_adoption.stage is not PublicationStage.ADOPTED
                    or request.gcs_adoption.reference != PublicationRef(identity, snapshot.manifest.manifest_digest)):
                raise OutputPublicationJournalStateError("reply retirement requires exact GCS adoption receipt")
            journal.retire_completed(request.proof)
        return wire.AckOutputPublicationAdoptedReply(request, True)

    def _handle_get_output_worker_lease_outcome(self, request, identity):
        journal = self._output_publication_journal
        with journal.linearize(identity):
            snapshot = journal.snapshot(identity)
            manifest = snapshot.manifest
            with self._state_lock:
                record = self._output_lease_record_locked(manifest)
                if ((request.task_id != identity.task_id) or (request.attempt_id != identity.attempt_id) or (request.executor_worker_id != manifest.header.executor_worker_id) or (request.owner_worker_id != manifest.header.owner_worker_id) or (request.object_ids != (identity.object_id,)) or (request.scheduling_key != record.request.scheduling_key)):
                    raise OutputPublicationConflictError("output outcome query changed execution")
                if snapshot.complete is not None:
                    completion = protocol.CompleteWorkerLease(
                        identity.lease_id, identity.task_id, identity.attempt_id,
                        manifest.header.executor_worker_id, protocol.TaskReplyStatus.SUCCEEDED,
                        record.request.scheduling_key,
                    )
                    self._commit_output_lease_locked(manifest, completion)
                state, completion = record.state, record.completion
                slot = self._workers.get(request.executor_worker_id)
                process = None if slot is None else slot.process
                try:
                    worker_alive = process is not None and process.is_alive()
                except (AssertionError, ValueError):
                    worker_alive = False
            if (snapshot.complete is None
                    and state in (protocol.LeaseExecutionState.COMPLETED,
                                  protocol.LeaseExecutionState.WORKER_LOST,
                                  protocol.LeaseExecutionState.ABANDONED)
                    and not self._output_publications.rollback_reported(identity)):
                # CPU release is already real, but the failed publication is
                # not safe for owner retry until every compensation and its
                # owner ACK converges. Keep execution/resource truth terminal,
                # but expose the outstanding cleanup barrier so Core stays on
                # the exact Push/outcome recovery path instead of retrying.
                return protocol.GetWorkerLeaseOutcomeReply(
                    request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
                    request.owner_worker_id, request.object_ids, self.node_id, True, worker_alive,
                    state=state, completion_status=None if completion is None else completion.status,
                    cleanup_pending=True,
                    scheduling_key=request.scheduling_key,
                )
            envelope = None
            if snapshot.complete is not None:
                try:
                    envelope = journal.complete(identity, snapshot.complete)
                except OutputPublicationPayloadRetired:
                    # A completed lease still has a success fact, but the Node
                    # must not rebuild retired bytes from its metadata.
                    pass
            descriptors = () if envelope is None else tuple(
                protocol.ObjectStoreDescriptor(
                    result.object_id, result.owner_worker_id, identity.attempt_id,
                    result.node_id, result.size_bytes, result.checksum,
                ) for result in (envelope.result,)
                if result.storage is protocol.ResultStorage.OBJECT_STORE
            )
            return protocol.GetWorkerLeaseOutcomeReply(
                request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
                request.owner_worker_id, request.object_ids, self.node_id, True, worker_alive,
                state=state, completion_status=None if completion is None else completion.status,
                descriptors=descriptors, scheduling_key=request.scheduling_key,
                 output_publication=envelope,
                output_completion=snapshot.complete if envelope is None else None,
            )

    def _handle_finalize_output_owner_death(self, request):
        from . import output_protocol as wire
        if type(request) is not wire.FinalizeOutputOwnerDeath:
            raise TypeError("output owner-death finalize requires exact metadata")
        request = replace(request)
        manifest, death = request.manifest, request.owner_death
        identity = manifest.publication_id
        journal, adapter = self._output_publication_journal, self._output_publications
        with self._state_lock:
            if getattr(self, "_owner_death_fences", {}).get(death.worker_id) != death:
                raise OutputPublicationConflictError("output cleanup lacks exact installed owner fence")
            if manifest.header.node_incarnation != OutputPublicationNodeIncarnation(
                    self.node_id, self._node_pid, self._registration_epoch):
                raise OutputPublicationConflictError("output cleanup targets another Node incarnation")

        def cleanup():
            return self._finalize_output_owner_custody(request)

        try:
            cleaned = adapter.finish_owner_death(manifest, death, cleanup=cleanup)
        except OutputPublicationBusy:
            cleaned = False
        closed = adapter.owner_death_closed_holds(manifest.publication_id) if cleaned else None
        return wire.FinalizeOutputOwnerDeathReply(request, cleaned, closed)

    def _finalize_output_owner_custody(self, request) -> bool:
        """Local cleanup plus exact Worker ACK under the adapter's ticket."""
        from . import output_protocol as wire
        manifest, death = request.manifest, request.owner_death
        identity = manifest.publication_id
        journal = self._output_publication_journal
        with journal.linearize(identity), self._state_lock:
            snapshot = journal.snapshot(identity)
            if snapshot.manifest != manifest:
                raise OutputPublicationConflictError("owner cleanup changed output manifest")
            record = self._leases.get(identity.lease_id)
            if record is None or record.output_publication_id != identity:
                raise OutputPublicationConflictError("owner cleanup changed lease identity")
            if snapshot.complete is not None:
                completion = protocol.CompleteWorkerLease(
                    identity.lease_id, identity.task_id, identity.attempt_id,
                    manifest.header.executor_worker_id, protocol.TaskReplyStatus.SUCCEEDED,
                    record.request.scheduling_key,
                )
                # Death retires custody, never rewrites an existing Complete.
                if record.state is protocol.LeaseExecutionState.RUNNING:
                    self._release_record_locked(record, protocol.LeaseExecutionState.COMPLETED)
                elif record.state in (protocol.LeaseExecutionState.WORKER_LOST, protocol.LeaseExecutionState.ABANDONED):
                    record.state = protocol.LeaseExecutionState.COMPLETED
                record.completion = completion
                record.output_complete_inflight = None
            elif record.state in (protocol.LeaseExecutionState.GRANTED, protocol.LeaseExecutionState.RUNNING):
                # The exact owner fence makes result publication irrevocably
                # impossible. Retire this lease after exact child-hold cleanup;
                # never allow its delayed Complete to win.
                self._release_record_locked(record, protocol.LeaseExecutionState.ABANDONED)
                record.output_complete_inflight = None
        # No admitted publisher can create new bytes after its owner fence.
        # Clean exact partial writes with their recorded local claim.
        for slot in (manifest.value,):
            if slot.tier is not protocol.ResultStorage.OBJECT_STORE:
                continue  # INLINE custody has no Node-local physical replica.
            drop = protocol.DropObjectReplica(
                _output_object_id(manifest.publication_id.object_id), _output_attempt(identity.attempt_id),
                _output_opaque(manifest.header.owner_worker_id, ids.WorkerID, "drop owner"),
                _output_opaque(self.node_id, ids.NodeID, "drop Node"), slot.checksum,
            )
            expected_effect = OutputPublicationEffect(
                identity, manifest.manifest_digest, OutputPublicationStage.MATERIALIZE,
            )
            expected_claim = _OutputReplicaWriteClaim(expected_effect, (
                identity.attempt_id, manifest.header.owner_worker_id,
                slot.size_bytes, slot.checksum,
            ))
            with self._state_lock:
                lock = self._object_localization_locks.setdefault(manifest.publication_id.object_id, threading.Lock())
            with lock, self._state_lock:
                if self._replica_drop_completed_locked(drop):
                    continue
                claim = self._local_replica_write_claims.get(manifest.publication_id.object_id)
                metadata = self._sealed_metadata.get(manifest.publication_id.object_id)
                if metadata is not None:
                    return False  # the owner-wide sweep still owns sealed cleanup
                present = self._object_store.contains(manifest.publication_id.object_id, sealed_only=False)
                if expected_effect not in snapshot.intents:
                    if present or claim is not None:
                        return False
                    continue  # No MATERIALIZE means no physical deletion proof.
                if claim is not None and claim != expected_claim:
                    return False
                if present:
                    if claim != expected_claim:
                        return False
                    stored = self._object_store.snapshot(manifest.publication_id.object_id)
                    if (stored.pin_count or stored.size_bytes != slot.size_bytes
                            or stored.sealed and hashlib.sha256(
                                self._object_store.get(manifest.publication_id.object_id)
                            ).hexdigest() != slot.checksum):
                        return False
                    removed = (self._object_store.delete(manifest.publication_id.object_id) if stored.sealed
                               else self._object_store.abort(manifest.publication_id.object_id))
                    if not removed:
                        return False
                if claim is None:
                    self._local_replica_write_claims[manifest.publication_id.object_id] = expected_claim
                # Do not retire the claim, fabricate a rollback ACK, or proceed
                # to Worker finalization until physical/manager cleanup agrees.
                self._finish_replica_drop_locked(drop)
        with self._state_lock:
            worker = self._workers.get(manifest.header.executor_worker_id)
            process = None if worker is None else worker.process
            try:
                alive = process is not None and process.is_alive()
            except (AssertionError, ValueError):
                return False  # an unavailable observer is not Worker-death proof
            address = None if worker is None else worker.address
            if (not alive and (worker is None or process is None)
                    and manifest.header.executor_worker_id not in getattr(self, "_dead_worker_exitcodes", {})):
                return False
        if alive:
            if address is None:
                return False
            reply = self._background_rpc(address, wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER, request)
            if type(reply) is not wire.FinalizeOutputOwnerDeathReply:
                return False
            reply = replace(reply)
            if reply.request != request or reply.cleaned is not True:
                return False
        return True

    def _drive_output_publications(self) -> bool:
        """One bounded supervisor/drain round; no lock spans remote effects."""
        journal = getattr(self, "_output_publication_journal", None)
        adapter = getattr(self, "_output_publications", None)
        if journal is None or adapter is None:
            return True
        clean = True
        for identity in journal.publication_ids():
            if adapter.owner_death_finished(identity):
                continue
            snapshot = journal.snapshot(identity)
            manifest = snapshot.manifest
            with self._state_lock:
                record = self._leases.get(identity.lease_id)
                if record is None or record.output_publication_id != identity:
                    clean = False
                    continue
                owner_death = getattr(self, "_owner_death_fences", {}).get(manifest.header.owner_worker_id)
                state = record.state
                completion = record.completion or record.output_complete_inflight
            try:
                if owner_death is not None:
                    from . import output_protocol as wire
                    reply = self._handle_finalize_output_owner_death(wire.FinalizeOutputOwnerDeath(manifest, owner_death))
                    clean = reply.cleaned and clean
                    continue
                if snapshot.complete is not None:
                    request = protocol.CompleteWorkerLease(
                        identity.lease_id, identity.task_id, identity.attempt_id,
                        manifest.header.executor_worker_id, protocol.TaskReplyStatus.SUCCEEDED,
                        record.request.scheduling_key,
                    )

                    def commit(_witness):
                        with self._state_lock:
                            self._commit_output_lease_locked(manifest, request)

                    adapter.converge_completed(identity, commit_lease=commit)
                    adapter.report_terminal(identity)
                elif snapshot.rollback is not None:
                    if adapter.rollback(identity, snapshot.rollback.rollback_id, max_effects=1) is None:
                        clean = False
                elif completion is not None:
                    reply = self._handle_complete_output_worker_lease(completion, identity)
                    clean = reply.accepted and clean
                elif state in (protocol.LeaseExecutionState.WORKER_LOST, protocol.LeaseExecutionState.ABANDONED):
                    rollback_id = "output-worker-lost:{}".format(identity.lease_id)
                    if adapter.rollback(identity, rollback_id, max_effects=1) is None:
                        clean = False
                else:
                    clean = False
            except Exception:
                clean = False
        return (clean and not adapter.pending_terminal_reports()
                and not adapter.pending_lease_completions() and not adapter.pending_rollbacks())


    def _validate_output_replica_effect(
        self, effect: OutputPublicationEffect, expected_stage: OutputPublicationStage,
    ):
        """Validate local storage authority, with the journal serializer held.

        The single result has slot index zero. This reads the existing journal
        only; it never begins or ACKs an effect.
        Callers must not enter from under ``_state_lock``.
        """
        if type(effect) is not OutputPublicationEffect:
            raise TypeError("effect must be an OutputPublicationEffect")
        effect = replace(effect)  # Revalidate nested wire identities.
        if (expected_stage not in (OutputPublicationStage.MATERIALIZE,
                                   OutputPublicationStage.SLOT_DROP)
                or effect.stage is not expected_stage):
            raise OutputPublicationConflictError("wrong local replica effect stage")
        journal = self._output_publication_journal
        snapshot = journal.snapshot(effect.publication_id)
        manifest = snapshot.manifest
        incarnation = _output_node_incarnation(OutputPublicationNodeIncarnation(
            self.node_id, self._node_pid, self._registration_epoch,
        ))
        if manifest.header.node_incarnation != incarnation:
            raise OutputPublicationConflictError("publication names another Node incarnation")
        canonical = OutputPublicationEffect(
            manifest.publication_id, manifest.manifest_digest, expected_stage,
        )
        if effect != canonical:
            raise OutputPublicationConflictError("replica effect changed its manifest")
        materialize = replace(effect, stage=OutputPublicationStage.MATERIALIZE)
        if materialize not in snapshot.intents:
            raise OutputPublicationJournalStateError("replica effect requires materialization intent")
        if expected_stage is OutputPublicationStage.MATERIALIZE:
            if snapshot.state is not OutputPublicationJournalState.ACTIVE:
                raise OutputPublicationJournalStateError("replica sealing requires ACTIVE publication")
        elif (snapshot.state not in (OutputPublicationJournalState.ROLLING_BACK,
                                      OutputPublicationJournalState.RETIRED)
              or snapshot.rollback is None or effect not in snapshot.rollback.effects
              or (not journal.acknowledged(effect)
                  and journal.next_rollback_effect(effect.publication_id) != effect)):
            raise OutputPublicationJournalStateError("replica drop is not the next rollback effect or exact ACK replay")
        return effect, snapshot

    def _seal_output_publication_replica(
        self, effect: OutputPublicationEffect, descriptor: protocol.ResultDescriptor,
        payload: bytes,
    ) -> protocol.ResultDescriptor:
        """Materialize one journal-authorized slot in the ordinary local store.

        The only extra record is a write-ahead custody claim, installed after
        proving ABSENT.  A failed create/write/seal keeps that claim for exact
        rollback; it never grants custody of somebody else's partial write.
        No RPC or publication ACK occurs inside these local critical sections.
        """
        if type(effect) is not OutputPublicationEffect:
            raise TypeError("effect must be an OutputPublicationEffect")
        effect = replace(effect)
        descriptor = _output_descriptor(descriptor)
        if type(payload) is not bytes:
            raise TypeError("replica payload must be bytes")
        with self._output_publication_journal.linearize(effect.publication_id):
            effect, snapshot = self._validate_output_replica_effect(
                effect, OutputPublicationStage.MATERIALIZE,
            )
            manifest = snapshot.manifest
            slot = manifest.value
            if (slot.tier is not protocol.ResultStorage.OBJECT_STORE
                    or descriptor != protocol.ResultDescriptor(
                        manifest.publication_id.object_id, slot.tier, slot.size_bytes,
                        manifest.header.owner_worker_id, self.node_id, slot.checksum,
                    )
                    or len(payload) != slot.size_bytes
                    or hashlib.sha256(payload).hexdigest() != slot.checksum):
                raise OutputPublicationConflictError("replica bytes or descriptor changed its output slot")
            object_id = manifest.publication_id.object_id
            expected_metadata = (
                effect.publication_id.attempt_id, descriptor.owner_worker_id,
                descriptor.size_bytes, descriptor.checksum,
            )
            expected_claim = _OutputReplicaWriteClaim(effect, expected_metadata)
            # Only look up the serializer here; never wait for it with the Node
            # state lock held.  Mutation order is journal -> object -> state -> store.
            with self._state_lock:
                object_lock = self._object_localization_locks.setdefault(
                    object_id, threading.Lock(),
                )
            with object_lock, self._state_lock:
                incarnation = manifest.header.node_incarnation
                if (self.node_id != incarnation.node_id
                        or self._node_pid != incarnation.node_pid
                        or self._registration_epoch != incarnation.registration_epoch):
                    raise OutputPublicationConflictError("Node incarnation changed before replica seal")
                if descriptor.owner_worker_id in getattr(self, "_owner_death_fences", {}):
                    raise OutputPublicationJournalStateError("replica owner is fenced by Worker death")
                tombstone = self._dropped_metadata.get(object_id)
                if (tombstone is not None and effect.publication_id.attempt_id.attempt_number
                        <= tombstone[0].attempt_number):
                    raise OutputPublicationJournalStateError("replica attempt was fenced by deletion")
                claim = self._local_replica_write_claims.get(object_id)
                if claim is not None and claim != expected_claim:
                    raise OutputPublicationConflictError("replica has another uncommitted writer")
                metadata = self._sealed_metadata.get(object_id)
                if metadata is not None and metadata != expected_metadata:
                    raise OutputPublicationConflictError("replica is sealed by another result")
                present = self._object_store.contains(object_id, sealed_only=False)
                if present:
                    stored = self._object_store.snapshot(object_id)
                    if (not stored.sealed or stored.size_bytes != slot.size_bytes
                            or (metadata is None and claim != expected_claim)):
                        raise OutputPublicationJournalStateError("uncommitted or unknown local replica requires cleanup")
                    if self._object_store.get(object_id) != payload:
                        raise OutputPublicationConflictError("sealed replica bytes changed")
                    # Includes seal-then-error before the metadata assignment.
                    self._sealed_metadata[object_id] = expected_metadata
                    self._local_replica_write_claims.pop(object_id, None)
                    return _output_descriptor(descriptor)
                if metadata is not None:
                    raise OutputPublicationJournalStateError("sealed replica metadata has no bytes")
                self._local_replica_write_claims[object_id] = expected_claim
                self._object_store.create(object_id, slot.size_bytes)
                self._object_store.write(object_id, payload)
                self._object_store.seal(object_id)
                if self._object_store.get(object_id) != payload:
                    raise OutputPublicationConflictError("newly sealed replica bytes changed")
                self._sealed_metadata[object_id] = expected_metadata
                self._local_replica_write_claims.pop(object_id, None)
                return _output_descriptor(descriptor)

    def _drop_output_publication_replica(
        self, effect: OutputPublicationEffect, request: protocol.DropObjectReplica,
    ) -> protocol.DropObjectReplicaReply:
        """Compensate exact write custody, or the ordinary sealed replica.

        Journal intent authorizes an absent-entry deletion fence, but never
        deletion of unknown partial bytes.  Tombstones and ObjectManager cleanup
        are the same ones used by ordinary replica GC.  No RPC is performed.
        """
        if type(effect) is not OutputPublicationEffect:
            raise TypeError("effect must be an OutputPublicationEffect")
        if type(request) is not protocol.DropObjectReplica:
            raise TypeError("request must be a DropObjectReplica")
        effect = replace(effect)
        request = protocol.DropObjectReplica(
            _output_object_id(request.object_id), _output_attempt(request.producer_attempt_id),
            _output_opaque(request.owner_worker_id, ids.WorkerID, "drop owner"),
            _output_opaque(request.node_id, ids.NodeID, "drop node"),
            _output_checksum(request.checksum, "drop checksum"),
        )
        status = protocol.DropObjectReplicaStatus

        def outcome(value, error=None):
            return protocol.DropObjectReplicaReply(
                _output_object_id(request.object_id), _output_attempt(request.producer_attempt_id),
                _output_opaque(request.owner_worker_id, ids.WorkerID, "reply owner"),
                _output_opaque(request.node_id, ids.NodeID, "reply node"),
                request.checksum, value, error,
            )

        with self._output_publication_journal.linearize(effect.publication_id):
            effect, snapshot = self._validate_output_replica_effect(
                effect, OutputPublicationStage.SLOT_DROP,
            )
            manifest = snapshot.manifest
            slot = manifest.value
            if (slot.tier is not protocol.ResultStorage.OBJECT_STORE
                    or request != protocol.DropObjectReplica(
                        manifest.publication_id.object_id, effect.publication_id.attempt_id,
                        manifest.header.owner_worker_id, self.node_id, slot.checksum,
                    )):
                raise OutputPublicationConflictError("drop changed its selected replica identity")
            object_id = manifest.publication_id.object_id
            expected_metadata = (
                request.producer_attempt_id, request.owner_worker_id,
                slot.size_bytes, request.checksum,
            )
            expected_tombstone = (
                request.producer_attempt_id, request.owner_worker_id, request.checksum,
            )
            expected_claim = _OutputReplicaWriteClaim(
                replace(effect, stage=OutputPublicationStage.MATERIALIZE), expected_metadata,
            )
            with self._state_lock:
                object_lock = self._object_localization_locks.setdefault(
                    object_id, threading.Lock(),
                )
            with object_lock, self._state_lock:
                incarnation = manifest.header.node_incarnation
                if (self.node_id != incarnation.node_id
                        or self._node_pid != incarnation.node_pid
                        or self._registration_epoch != incarnation.registration_epoch):
                    raise OutputPublicationConflictError("Node incarnation changed before replica drop")
                # Authority still comes from the journal, slot and incarnation
                # above.  Completed physical cleanup, however, is shared with
                # ordinary GC and owner-death cleanup, even across newer epochs.
                if self._replica_drop_completed_locked(request):
                    return outcome(status.ALREADY_DROPPED)
                tombstone = self._dropped_metadata.get(object_id)
                if tombstone is not None:
                    if tombstone[0].attempt_number > request.producer_attempt_id.attempt_number:
                        return outcome(status.STALE_EPOCH, "a newer replica deletion is fenced")
                    if (tombstone[0].attempt_number == request.producer_attempt_id.attempt_number
                            and tombstone != expected_tombstone):
                        return outcome(status.INCONSISTENT, "same-epoch deletion identity differs")
                claim = self._local_replica_write_claims.get(object_id)
                if claim is not None and claim != expected_claim:
                    return outcome(status.STALE_EPOCH, "replica has another uncommitted writer")
                metadata = self._sealed_metadata.get(object_id)
                if metadata is not None and metadata != expected_metadata:
                    return outcome(status.STALE_EPOCH, "replica is sealed by another result")
                present = self._object_store.contains(object_id, sealed_only=False)
                if present and metadata is None and claim is None:
                    return outcome(status.INCONSISTENT, "local bytes have no matching write custody")
                if present:
                    try:
                        stored = self._object_store.snapshot(object_id)
                        if (stored.size_bytes != slot.size_bytes
                                or (metadata is not None and not stored.sealed)
                                or (stored.sealed and hashlib.sha256(
                                    self._object_store.get(object_id)
                                ).hexdigest() != slot.checksum)):
                            return outcome(status.INCONSISTENT, "replica bytes disagree with write identity")
                        if stored.pin_count:
                            return outcome(status.PINNED, "replica is pinned")
                    except Exception as exc:
                        return outcome(status.INCONSISTENT, str(exc) or type(exc).__name__)
                if metadata is not None:
                    # Lock-aware ordinary deletion also retains its metadata work
                    # marker when manager cleanup fails after deleting the bytes.
                    reply = self._drop_sealed_replica_locked(protocol.ObjectStoreDescriptor(
                        object_id, request.owner_worker_id, request.producer_attempt_id,
                        self.node_id, slot.size_bytes, request.checksum,
                    ))
                    return outcome(reply.status, reply.error)
                if claim is None:
                    # Retain the journal's already validated materialization
                    # identity while even never-created absence is reconciled.
                    # It proves custody, not that any bytes were ever written.
                    self._local_replica_write_claims[object_id] = expected_claim
                if present:
                    try:
                        removed = (self._object_store.delete(object_id) if stored.sealed
                                   else self._object_store.abort(object_id))
                        if not removed:
                            return outcome(status.INCONSISTENT, "claimed replica removal failed")
                    except Exception as exc:
                        return outcome(status.INCONSISTENT, str(exc) or type(exc).__name__)
                # Journal-authorized never-created absence is also fenced.
                # A failed manager cleanup retains the claim and no receipt.
                try:
                    self._finish_replica_drop_locked(request)
                except Exception as exc:
                    return outcome(status.INCONSISTENT, str(exc) or type(exc).__name__)
                return outcome(status.ALREADY_DROPPED if tombstone == expected_tombstone
                               and not present else status.DROPPED)

    def _handle_install_cluster_snapshot(self, request: object) -> object:
        if not isinstance(request, protocol.InstallClusterSnapshot):
            raise TypeError(
                "install_cluster_snapshot expects InstallClusterSnapshot"
            )

        with self._state_lock:
            installed_epoch = getattr(
                self, "_installed_membership_epoch", None
            )
            if installed_epoch is None:
                # Compatibility for narrow object.__new__ fixtures written
                # before observed and installed membership were separated.
                installed_epoch = (
                    getattr(self, "_membership_epoch", 0)
                    if getattr(self, "_cluster_snapshot_id", None) is not None
                    and getattr(self, "_installed_snapshot_nodes", None) is not None
                    else 0
                )
            observed_epoch = getattr(self, "_membership_epoch", 0)
            if request.membership_epoch < observed_epoch:
                return protocol.InstallClusterSnapshotReply(
                    request.membership_epoch,
                    request.snapshot_id,
                    self.node_id,
                    installed=False,
                    error="cluster membership epoch is stale",
                )
            if self._stop_event.is_set():
                return protocol.InstallClusterSnapshotReply(
                    request.membership_epoch,
                    request.snapshot_id,
                    self.node_id,
                    installed=False,
                    error="node is shutting down",
                )
            local_info = next(
                (node for node in request.nodes if node.node_id == self.node_id),
                None,
            )
            if local_info is None:
                return protocol.InstallClusterSnapshotReply(
                    request.membership_epoch,
                    request.snapshot_id,
                    self.node_id,
                    installed=False,
                    error="cluster snapshot does not contain this node",
                )
            if (
                local_info.state is not protocol.NodeMembershipState.ALIVE
                or local_info.node_pid != getattr(self, "_node_pid", os.getpid())
                or local_info.registration_epoch
                != getattr(self, "_registration_epoch", 0)
                or
                local_info.address != self.address
                or local_info.total_resources != self._ledger.total
            ):
                return protocol.InstallClusterSnapshotReply(
                    request.membership_epoch,
                    request.snapshot_id,
                    self.node_id,
                    installed=False,
                    error="cluster snapshot conflicts with local node identity or capacity",
                )
            if request.membership_epoch == installed_epoch:
                if (
                    self._cluster_snapshot_id != request.snapshot_id
                    or self._installed_snapshot_nodes != request.nodes
                ):
                    return protocol.InstallClusterSnapshotReply(
                        request.membership_epoch,
                        request.snapshot_id,
                        self.node_id,
                        installed=False,
                        error="membership epoch was reused for different contents",
                    )
                return protocol.InstallClusterSnapshotReply(
                    request.membership_epoch, request.snapshot_id,
                    self.node_id, installed=True
                )

            if self._shutdown_request_id is not None:
                previous = getattr(self, "_installed_snapshot_nodes", None)
                previous_by_id = (
                    {} if previous is None
                    else {info.node_id: info for info in previous}
                )
                next_by_id = {info.node_id: info for info in request.nodes}
                # Drain closes task admission, not control-plane convergence.
                # Accept only a strictly newer live-set contraction whose
                # surviving physical incarnations are byte-for-byte unchanged.
                contraction = (
                    previous is not None
                    and set(next_by_id).issubset(previous_by_id)
                    and set(next_by_id) != set(previous_by_id)
                    and all(
                        (
                            previous_by_id[node_id].node_id,
                            previous_by_id[node_id].node_pid,
                            previous_by_id[node_id].registration_epoch,
                            previous_by_id[node_id].address,
                            previous_by_id[node_id].total_resources,
                            previous_by_id[node_id].state,
                        )
                        == (
                            info.node_id, info.node_pid,
                            info.registration_epoch, info.address,
                            info.total_resources, info.state,
                        )
                        for node_id, info in next_by_id.items()
                    )
                )
                if not contraction:
                    return protocol.InstallClusterSnapshotReply(
                        request.membership_epoch, request.snapshot_id,
                        self.node_id, installed=False,
                        error=(
                            "draining node accepts only a newer membership "
                            "contraction"
                        ),
                    )

            snapshots = tuple(
                self._as_scheduling_snapshot(node) for node in request.nodes
            )
            addresses = {node.node_id: node.address for node in request.nodes}
            # Preserve the local ledger as the strongest view of this node.
            snapshots = tuple(
                resources.NodeSnapshot(
                    node.node_id, node.total, self._ledger.available, node.alive, node.labels
                )
                if node.node_id == self.node_id
                else node
                for node in snapshots
            )
            self._cluster_snapshot_id = request.snapshot_id
            self._installed_membership_epoch = request.membership_epoch
            self._membership_epoch = max(
                getattr(self, "_membership_epoch", 0),
                request.membership_epoch,
            )
            self._cluster_nodes = snapshots
            self._cluster_addresses = addresses
            self._installed_snapshot_nodes = request.nodes
            return protocol.InstallClusterSnapshotReply(
                request.membership_epoch, request.snapshot_id,
                self.node_id, installed=True
            )

    def _handle_publish_installed_node_deaths(self, request: object):
        """Retain the Driver's completed survivor barrier, not a new guess."""
        if type(request) is not PublishInstalledNodeDeaths:
            raise TypeError("installed Node deaths require a typed publication")
        request = replace(request)
        with self._state_lock:
            try:
                if request.node_id != self.node_id:
                    raise ValueError("death view publication targets another Node")
                if self._stop_event.is_set():
                    raise ValueError("Node service is finalized")
                installed = protocol.InstallClusterSnapshot(
                    self._installed_membership_epoch, self._cluster_snapshot_id, self._installed_snapshot_nodes,
                )
                if request.view.snapshot != installed:
                    raise ValueError("death view does not match the locally installed survivor snapshot")
                request.view.validate_successor(getattr(self, "_certified_node_deaths", None))
            except (AttributeError, TypeError, ValueError) as exc:
                return PublishInstalledNodeDeathsReply(request, False, str(exc))
            self._certified_node_deaths = request.view
            return PublishInstalledNodeDeathsReply(request, True)

    def _handle_get_installed_node_deaths(self, request: object):
        """Serve only retained complete certificates, including during drain."""
        if type(request) is not GetInstalledNodeDeaths:
            raise TypeError("Node death view query requires its typed request")
        request = replace(request)
        if request.node_id != self.node_id:
            raise ValueError("Node death view query targets another Node")
        with self._state_lock:
            return GetInstalledNodeDeathsReply(request, getattr(self, "_certified_node_deaths", None))



    def _worker_slot_locked(self, worker_id: ids.WorkerID) -> Optional[_WorkerSlot]:
        return self._workers.get(worker_id)

    def _idle_worker_slot_locked(self) -> Optional[_WorkerSlot]:
        for worker_id in self._worker_order:
            slot = self._workers[worker_id]
            process = slot.process
            try:
                alive = process is not None and process.is_alive()
            except (ValueError, AssertionError):
                alive = False
            if alive and slot.address is not None and slot.active_lease_id is None:
                return slot
        return None

    def _all_worker_slots_idle_locked(self) -> bool:
        return all(
            slot.active_lease_id is None for slot in self._workers.values()
        )

    def _cleanup_plane_quiescent_locked(self) -> bool:
        self._finalize_removing_placement_groups_locked()
        actor_supervisor = getattr(self, "_actor_supervisor_thread", None)
        return (
            self._all_worker_slots_idle_locked()
            and getattr(self, "_inflight_lease_requests", 0) == 0
            and getattr(self, "_worker_replacements_inflight", 0) == 0
            and not getattr(self, "_worker_death_reports", {})
            and not getattr(self, "_dependency_pin_cleanups", {})
            and not self._dependency_custody_registry_locked().has_pending()
            and not getattr(self, "_dependency_handoff_drivers", set())
            and not self._source_pin_outbox_locked().has_pending()
            and self._output_publications_clean_locked()
            and not any(
                not session.released
                for session in getattr(self, "_pinned_transfers", {}).values()
            )
            and not any(not closed.closed for closed in getattr(self, "_closed_transfer_pins", {}).values())
            and all(
                outcome.report_reply is not None
                for outcome in getattr(
                    self, "_actor_generation_outcomes", {}
                ).values()
            )
            and (
                getattr(self, "_shutdown_request_id", None) is None
                or actor_supervisor is None
                or not actor_supervisor.is_alive()
            )
        )

    def _output_publications_clean_locked(self) -> bool:
        """Read current custody without waiting in the reverse lock order.

        Callers hold Node state; publishers acquire journal before Node state.
        Nonblocking acquisition of both publication locks makes contention an
        unclean observation instead of deadlocking against those publishers.
        No RPC, progress mutation or second lifecycle bitmap is involved.
        """
        journal = getattr(self, "_output_publication_journal", None)
        adapter = getattr(self, "_output_publications", None)
        if journal is None:
            # Membership/resource-only object.__new__ fixtures have neither.
            return adapter is None
        if adapter is None or not journal._lock.acquire(blocking=False):
            return False
        try:
            if not adapter._lock.acquire(blocking=False):
                return False
            try:
                if (adapter._tickets or adapter.pending_terminal_reports()
                        or adapter.pending_lease_completions() or adapter.pending_rollbacks()):
                    return False
                for identity in journal.publication_ids():
                    snapshot = journal.snapshot(identity)
                    record = self._leases.get(identity.lease_id)
                    if (record is None or record.output_publication_id != identity
                            or record.output_complete_inflight is not None
                            or record.state not in (
                                protocol.LeaseExecutionState.COMPLETED,
                                protocol.LeaseExecutionState.WORKER_LOST,
                                protocol.LeaseExecutionState.ABANDONED,
                            )
                            or snapshot.state is not OutputPublicationJournalState.RETIRED
                            or snapshot.result_retained):
                        return False
                    if adapter.owner_death_finished(identity):
                        continue
                    if snapshot.manifest.header.owner_worker_id in getattr(
                        self, "_owner_death_fences", {}
                    ):
                        # Retired delivery bytes do not discharge the later
                        # owner-death child, replica and Worker custody cleanup.
                        return False
                    if snapshot.complete is not None:
                        # Every selected payload needs its immutable adoption /
                        # collection tombstone, not merely a released CPU lease.
                        if (record.state is not protocol.LeaseExecutionState.COMPLETED
                                or record.completion is None
                                or record.completion.status is not protocol.TaskReplyStatus.SUCCEEDED
                                or snapshot.retirement is None):
                            return False
                    elif (snapshot.rollback_tombstone is None
                          or not adapter.rollback_reported(identity)):
                        return False
                return True
            finally:
                adapter._lock.release()
        finally:
            journal._lock.release()

    def _drain_resources_clean_locked(self) -> bool:
        """Allow only the exact Actor lifetime allocations during phase one."""

        if not self._cleanup_plane_quiescent_locked():
            return False
        if self._has_live_placement_group_reservations_locked():
            return False
        actor_records = tuple(getattr(self, "_actor_workers", {}).values())
        expected = {
            record.allocation_token: record.request.resources
            for record in actor_records
        }
        if len(expected) != len(actor_records):
            return False

        snapshot = self._ledger.snapshot()
        active = tuple(
            record
            for record in snapshot.allocations
            if record.state is not resources.AllocationState.RELEASED
        )
        if len(active) != len(expected) or snapshot.cpu_debt != 0:
            return False
        held = resources.ResourceVector.empty()
        for record in active:
            requested = expected.get(record.token)
            if (
                requested is None
                or record.state is not resources.AllocationState.ACTIVE
                or record.resources != requested
            ):
                return False
            held = held + record.held_resources
        return snapshot.available + held == snapshot.total

    def _resources_clean_locked(self) -> bool:
        """Strict post-finalize resource cleanliness."""

        if (
            not self._cleanup_plane_quiescent_locked()
            or getattr(self, "_actor_workers", {})
            or self._has_live_placement_group_reservations_locked()
        ):
            return False
        snapshot = self._ledger.snapshot()
        return (
            snapshot.available == snapshot.total
            and snapshot.cpu_debt == 0
            and all(
                record.state is resources.AllocationState.RELEASED
                for record in snapshot.allocations
            )
        )

    def _has_live_placement_group_reservations_locked(self) -> bool:
        reservations = getattr(self, "_bundle_reservations", None)
        if reservations is None:
            return False
        for placement_group_id, attempt in tuple(
            getattr(self, "_placement_group_digests", {})
        ):
            snapshot = reservations.snapshot(placement_group_id, attempt)
            if snapshot is not None and snapshot.state is not ReservationState.ABORTED:
                return True
        return False

    def _retain_dependency_pin_cleanup_locked(
        self, object_id: ids.ObjectID, pin_token: object, exc: BaseException
    ) -> None:
        cleanups = getattr(self, "_dependency_pin_cleanups", None)
        if cleanups is None:
            cleanups = {}
            self._dependency_pin_cleanups = cleanups
        key = object_id, pin_token
        previous = cleanups.get(key)
        round_number = 0 if previous is None else previous.retry_round + 1
        cleanups[key] = _DependencyPinCleanup(
            object_id, pin_token, round_number,
            time.monotonic() + min(0.25, 0.01 * (2 ** min(round_number, 5))),
            "{}: {}".format(type(exc).__name__, exc),
        )

    def _unpin_dependency_or_retain_cleanup_locked(
        self, object_id: ids.ObjectID, pin_token: object
    ) -> bool:
        """Apply one exact unpin or preserve a shutdown-visible obligation."""

        key = object_id, pin_token
        try:
            released = self._object_store.unpin(object_id, pin_token)
        except Exception as exc:
            self._retain_dependency_pin_cleanup_locked(
                object_id, pin_token, exc
            )
            return False
        getattr(self, "_dependency_pin_cleanups", {}).pop(key, None)
        # ``False`` is an idempotent exact release: the token is already absent.
        return released

    def _retry_dependency_pin_cleanups(
        self, *, force: bool = False
    ) -> int:
        """Retry due cleanup identities without weakening Node cleanliness."""

        with self._state_lock:
            now = time.monotonic()
            cleanups = tuple(
                cleanup
                for cleanup in getattr(
                    self, "_dependency_pin_cleanups", {}
                ).values()
                if force or cleanup.retry_after <= now
            )
            for cleanup in cleanups:
                self._unpin_dependency_or_retain_cleanup_locked(
                    cleanup.object_id, cleanup.pin_token
                )
            return len(cleanups)

    def _start_worker_pool(self) -> None:
        """Start every bounded slot, rolling back the exact successful prefix."""

        with self._state_lock:
            worker_ids = tuple(self._worker_order)
        started: list[ids.WorkerID] = []
        try:
            for worker_id in worker_ids:
                with self._state_lock:
                    slot = self._workers[worker_id]
                    process = slot.process
                    try:
                        already_alive = process is not None and process.is_alive()
                    except (ValueError, AssertionError):
                        already_alive = False
                if already_alive:
                    continue
                self._start_worker_slot(worker_id)
                started.append(worker_id)
        except BaseException:
            if started:
                self._stop_workers(worker_ids=tuple(started))
            raise

    def _register_unpublished_worker(
        self, worker_id: ids.WorkerID, process: object
    ) -> Optional[protocol.WorkerIncarnation]:
        """Bind a ready child in GCS before exposing its endpoint.

        ``gcs_address is None`` is retained as a narrow standalone teaching and
        pure-test mode.  Every public multi-process runtime supplies GCS and
        therefore takes the strict registration path below.
        """

        with self._state_lock:
            gcs_address = getattr(self, "_gcs_address", None)
            if gcs_address is None:
                return None
            node_pid = getattr(self, "_node_pid", None)
            registration_epoch = getattr(self, "_registration_epoch", 0)
            registered = getattr(self, "_registered_with_gcs", False)
        worker_pid = getattr(process, "pid", None)
        if (
            not registered
            or isinstance(node_pid, bool)
            or not isinstance(node_pid, int)
            or node_pid <= 0
            or isinstance(registration_epoch, bool)
            or not isinstance(registration_epoch, int)
            or registration_epoch <= 0
        ):
            raise RuntimeError(
                "ordinary Worker cannot register before its Node incarnation"
            )
        incarnation = protocol.WorkerIncarnation(
            self.node_id, node_pid, registration_epoch, worker_id, worker_pid
        )
        reply = self._background_rpc(
            gcs_address,
            GCS_REGISTER_WORKER_INCARNATION_HANDLER,
            protocol.RegisterWorkerIncarnation(incarnation),
            request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
        )
        if (
            not isinstance(reply, protocol.RegisterWorkerIncarnationReply)
            or reply.incarnation != incarnation
            or not reply.accepted
        ):
            detail = (
                reply.error
                if isinstance(reply, protocol.RegisterWorkerIncarnationReply)
                else None
            )
            raise RuntimeError(
                detail or "GCS did not acknowledge exact Worker incarnation"
            )
        return incarnation

    def _spawn_worker_process(
        self, worker_id: ids.WorkerID
    ) -> tuple[object, Address]:
        """Start one unpublished Worker incarnation and await readiness."""

        parent_connection, child_connection = self._context.Pipe(duplex=False)
        process = self._context.Process(
            target=worker_main,
            args=(
                worker_id,
                child_connection,
                self._host,
                0,
                self.node_id,
                self.address,
                self._inline_threshold,
                self._worker_failpoint,
                self._trace_config.for_role("worker:{}".format(worker_id)) if self._trace_config else None,
                self._gcs_address,
            ),
            name="miniray-worker-{}".format(worker_id),
            daemon=False,
        )
        try:
            process.start()
            child_connection.close()
            if not parent_connection.poll(WORKER_START_TIMEOUT_SECONDS):
                raise RuntimeError("worker did not report readiness before timeout")
            ok, value = parent_connection.recv()
            if not ok:
                raise RuntimeError("worker failed during startup:\n{}".format(value))
            return process, value
        except BaseException:
            if process.is_alive():
                process.terminate()
                process.join(WORKER_STOP_TIMEOUT_SECONDS)
            process.close()
            raise
        finally:
            parent_connection.close()

    def _start_worker_slot(self, worker_id: ids.WorkerID) -> None:
        lifecycle_lock = getattr(self, "_worker_lifecycle_lock", None)
        if lifecycle_lock is None:
            with self._state_lock:
                lifecycle_lock = getattr(self, "_worker_lifecycle_lock", None)
                if lifecycle_lock is None:
                    lifecycle_lock = threading.Lock()
                    self._worker_lifecycle_lock = lifecycle_lock
        with lifecycle_lock:
            with self._state_lock:
                if (
                    getattr(self, "_shutdown_request_id", None) is not None
                    or getattr(self, "_stop_event", threading.Event()).is_set()
                ):
                    raise RuntimeError(
                        "Worker startup was fenced by Node drain"
                    )
            process, address = self._spawn_worker_process(worker_id)
            try:
                incarnation = self._register_unpublished_worker(
                    worker_id, process
                )
            except BaseException:
                self._stop_unpublished_worker(process, address)
                raise
            with self._state_lock:
                slot = self._workers[worker_id]
                publish = (
                    slot.process is None
                    and getattr(self, "_shutdown_request_id", None) is None
                    and not getattr(
                        self, "_stop_event", threading.Event()
                    ).is_set()
                )
                if not publish:
                    # Direct stop may race startup without using BeginDrain.
                    # The final check keeps even that narrow path unpublished.
                    stop_unpublished = True
                else:
                    stop_unpublished = False
                    slot.process = process
                    slot.address = address
                    slot.pid = process.pid
                    slot.exitcode = None
                    slot.forced = False
                    slot.incarnation = incarnation
            if stop_unpublished:
                self._stop_unpublished_worker(process, address)
                raise RuntimeError(
                    "Worker publication was fenced by Node drain"
                )

    def _start_worker_supervisor(self) -> None:
        """Start one Node-owned watcher for all direct Worker children."""

        with self._state_lock:
            thread = getattr(self, "_worker_supervisor_thread", None)
            if thread is not None and thread.is_alive():
                return
            stop = getattr(self, "_worker_supervisor_stop", None)
            if stop is None:
                stop = threading.Event()
                self._worker_supervisor_stop = stop
            stop.clear()
            thread = threading.Thread(
                target=self._worker_supervisor_loop,
                name="miniray-worker-supervisor-{}".format(self.node_id),
                daemon=True,
            )
            self._worker_supervisor_thread = thread
        thread.start()

    def _ensure_actor_lifecycle_state_locked(self) -> None:
        """Populate Actor restart state for narrow legacy test fixtures."""

        if not hasattr(self, "_actor_workers"):
            self._actor_workers = {}
        if not hasattr(self, "_actor_creation_locks"):
            self._actor_creation_locks = {}
        if not hasattr(self, "_actor_generation_outcomes"):
            self._actor_generation_outcomes = {}
        if not hasattr(self, "_actor_worker_ids_seen"):
            self._actor_worker_ids_seen = {
                record.startup.worker_id
                for record in getattr(self, "_actor_workers", {}).values()
            }
        if not hasattr(self, "_actor_worker_pids_seen"):
            self._actor_worker_pids_seen = {
                record.startup.worker_pid
                for record in getattr(self, "_actor_workers", {}).values()
            }
        if not hasattr(self, "_actor_supervisor_stop"):
            self._actor_supervisor_stop = threading.Event()
        if not hasattr(self, "_actor_supervisor_thread"):
            self._actor_supervisor_thread = None
        if not hasattr(self, "_actor_supervisor_wait"):
            self._actor_supervisor_wait = connection_wait
        if not hasattr(self, "_actor_lifecycle_lock"):
            self._actor_lifecycle_lock = threading.Lock()

    def _start_actor_supervisor(self) -> None:
        """Watch dedicated Actor children without making restart decisions."""

        with self._state_lock:
            self._ensure_actor_lifecycle_state_locked()
            thread = self._actor_supervisor_thread
            if thread is not None and thread.is_alive():
                return
            self._actor_supervisor_stop.clear()
            thread = threading.Thread(
                target=self._actor_supervisor_loop,
                name="miniray-actor-supervisor-{}".format(self.node_id),
                daemon=True,
            )
            self._actor_supervisor_thread = thread
        thread.start()

    def _stop_actor_supervisor(self, *, sweep: bool = False) -> bool:
        """Fence observation, then synchronously reap a missed sentinel.

        ``BeginDrain`` calls this after installing its admission fence.  Thus a
        death which raced the stop is still reduced and reported, while a GCS
        callback cannot publish a replacement into the draining Node.
        """

        with self._state_lock:
            self._ensure_actor_lifecycle_state_locked()
            stop = self._actor_supervisor_stop
            thread = self._actor_supervisor_thread
            stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(WORKER_STOP_TIMEOUT_SECONDS)
        stopped = thread is None or not thread.is_alive()
        if stopped:
            with self._state_lock:
                if self._actor_supervisor_thread is thread:
                    self._actor_supervisor_thread = None
        if sweep:
            self._sweep_exited_actor_workers()
            self._flush_pending_actor_exit_reports()
        return stopped

    def _actor_supervisor_loop(self) -> None:
        """Wait on child sentinels; transport failure is never death proof."""

        while not self._actor_supervisor_stop.is_set():
            self._flush_pending_actor_exit_reports()
            with self._state_lock:
                incarnations = tuple(
                    (actor_id, record.request.generation, record.process)
                    for actor_id, record in self._actor_workers.items()
                )
                wait_function = self._actor_supervisor_wait
            by_sentinel: dict[object, tuple[ids.ActorID, ids.ActorGeneration, object]] = {}
            for actor_id, generation, process in incarnations:
                try:
                    sentinel = process.sentinel
                except (AttributeError, AssertionError, ValueError):
                    continue
                by_sentinel[sentinel] = (actor_id, generation, process)
            if not by_sentinel:
                self._actor_supervisor_stop.wait(ACTOR_SUPERVISOR_POLL_SECONDS)
                continue
            try:
                ready = wait_function(
                    tuple(by_sentinel), ACTOR_SUPERVISOR_POLL_SECONDS
                )
            except (OSError, ValueError):
                ready = ()
            for sentinel in ready:
                incarnation = by_sentinel.get(sentinel)
                if incarnation is not None:
                    self._handle_unexpected_actor_worker_exit(*incarnation)

    def _sweep_exited_actor_workers(self) -> None:
        """Recheck exact live Process objects after the watcher is joined."""

        with self._state_lock:
            incarnations = tuple(
                (actor_id, record.request.generation, record.process)
                for actor_id, record in self._actor_workers.items()
            )
        for actor_id, generation, process in incarnations:
            try:
                alive = process.is_alive()
            except (AssertionError, ValueError):
                alive = False
            if not alive:
                self._handle_unexpected_actor_worker_exit(
                    actor_id, generation, process
                )

    def _handle_unexpected_actor_worker_exit(
        self,
        actor_id: ids.ActorID,
        generation: ids.ActorGeneration,
        process: object,
    ) -> bool:
        """Commit one exact Actor child exit and release its token once."""

        with self._state_lock:
            self._ensure_actor_lifecycle_state_locked()
            lifecycle_lock = self._actor_lifecycle_lock
        with lifecycle_lock:
            # ``join(0)`` plus the exact managed Process identity is the only
            # death proof accepted here.  A failed Actor RPC never enters this
            # reducer.
            try:
                process.join(0)
                alive = process.is_alive()
            except (AssertionError, ValueError, TypeError):
                return False
            if alive:
                return False
            exitcode = getattr(process, "exitcode", None)
            if isinstance(exitcode, bool) or not isinstance(exitcode, int):
                return False
            with self._state_lock:
                record = self._actor_workers.get(actor_id)
                if (
                    record is None
                    or record.process is not process
                    or record.request.generation != generation
                ):
                    return False
                exit_record = protocol.ActorWorkerExitRecord(
                    detection_id="actor-exit-{}".format(uuid.uuid4().hex),
                    actor_id=actor_id,
                    generation=generation,
                    route_epoch=record.request.route_epoch,
                    node_id=self.node_id,
                    node_pid=self._node_pid,
                    registration_epoch=self._registration_epoch,
                    worker_id=record.startup.worker_id,
                    worker_pid=record.startup.worker_pid,
                    exit_code=exitcode,
                )
                report = protocol.ReportActorWorkerExit(exit_record)
                self._actor_workers.pop(actor_id, None)
                released = self._ledger.release(record.allocation_token)
                if not released:
                    raise AssertionError(
                        "live Actor generation had a released lifetime token"
                    )
                self._refresh_local_cached_availability_locked()
                self._mark_resource_report_pending_locked()
                self._actor_generation_outcomes[generation] = (
                    _ActorGenerationOutcome(
                        record.request, record.reply, exit_record, report
                    )
                )
            self._close_exited_process(process)
            self._emit(
                "actor_worker_exited",
                actor_id=str(actor_id),
                generation=generation.generation,
                worker_id=str(exit_record.worker_id),
                worker_pid=exit_record.worker_pid,
                exitcode=exitcode,
                detection_id=exit_record.detection_id,
            )
        self._flush_pending_resource_report()
        self._flush_pending_actor_exit_reports()
        return True

    @staticmethod
    def _actor_exit_reply_acknowledges(
        report: protocol.ReportActorWorkerExit, reply: object
    ) -> bool:
        return bool(
            isinstance(reply, protocol.ReportActorWorkerExitReply)
            and reply.record == report.record
            and reply.disposition
            in (
                protocol.ActorWorkerExitDisposition.APPLIED,
                protocol.ActorWorkerExitDisposition.ALREADY_APPLIED,
            )
        )

    def _flush_pending_actor_exit_reports(self) -> bool:
        """Replay stable exit reports until GCS accepts their exact proof."""

        with self._state_lock:
            self._ensure_actor_lifecycle_state_locked()
            if getattr(self, "_gcs_address", None) is None:
                return False
            candidate = next(
                (
                    outcome
                    for outcome in self._actor_generation_outcomes.values()
                    if outcome.report_reply is None and not outcome.report_inflight
                ),
                None,
            )
            if candidate is None:
                return False
            candidate.report_inflight = True
            gcs_address = self._gcs_address
            report = candidate.report
        try:
            reply = self._background_rpc(
                gcs_address,
                GCS_REPORT_ACTOR_WORKER_EXIT_HANDLER,
                report,
                request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
            )
            acknowledged = self._actor_exit_reply_acknowledges(report, reply)
            error = None if acknowledged else "GCS did not acknowledge exact actor exit"
        except Exception as exc:
            reply = None
            acknowledged = False
            error = "{}: {}".format(type(exc).__name__, exc)
        with self._state_lock:
            current = self._actor_generation_outcomes.get(
                report.record.generation
            )
            if current is candidate:
                candidate.report_inflight = False
                candidate.last_report_error = error
                if acknowledged:
                    candidate.report_reply = reply
        return acknowledged

    @staticmethod
    def _worker_death_reply_acknowledges(
        report: protocol.ReportWorkerDeath, reply: object
    ) -> bool:
        """Accept only the GCS tombstone for this exact frozen proof."""

        if (
            not isinstance(reply, protocol.ReportWorkerDeathReply)
            or reply.detection_id != report.detection_id
            or reply.worker_id != report.worker_id
            or reply.disposition
            not in (
                protocol.WorkerDeathDisposition.APPLIED,
                protocol.WorkerDeathDisposition.ALREADY_DEAD,
            )
            or reply.death is None
        ):
            return False
        death = reply.death
        return bool(
            death.detection_id == report.detection_id
            and death.incarnation == report.incarnation
            and death.exit_code == report.exit_code
            and death.reason is report.reason
        )

    def _flush_pending_worker_death_reports(self) -> bool:
        """Replay one stable PROCESS_EXIT proof until GCS exactly ACKs it."""

        with self._state_lock:
            gcs_address = getattr(self, "_gcs_address", None)
            if gcs_address is None:
                return False
            reports = getattr(self, "_worker_death_reports", {})
            candidate = next(
                (
                    outcome
                    for outcome in reports.values()
                    if not outcome.report_inflight
                ),
                None,
            )
            if candidate is None:
                return False
            candidate.report_inflight = True
            report = candidate.report
        try:
            reply = self._background_rpc(
                gcs_address,
                GCS_REPORT_WORKER_DEATH_HANDLER,
                report,
                request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
            )
            acknowledged = self._worker_death_reply_acknowledges(report, reply)
            error = (
                None
                if acknowledged
                else "GCS did not acknowledge exact Worker death"
            )
        except Exception as exc:
            acknowledged = False
            error = "{}: {}".format(type(exc).__name__, exc)
        with self._state_lock:
            current = getattr(self, "_worker_death_reports", {}).get(
                report.worker_id
            )
            if current is candidate:
                candidate.report_inflight = False
                candidate.last_report_error = error
                if acknowledged:
                    self._worker_death_reports.pop(report.worker_id, None)
        return acknowledged

    def _stop_worker_supervisor(self) -> None:
        """Stop observing children before intentional Worker finalization."""

        stop = getattr(self, "_worker_supervisor_stop", None)
        thread = getattr(self, "_worker_supervisor_thread", None)
        if stop is not None:
            stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(WORKER_STOP_TIMEOUT_SECONDS)
        with self._state_lock:
            if (
                getattr(self, "_worker_supervisor_thread", None) is thread
                and (thread is None or not thread.is_alive())
            ):
                self._worker_supervisor_thread = None

    def _worker_supervisor_loop(self) -> None:
        """Poll only OS child state; a transport timeout is never death proof."""

        stop = self._worker_supervisor_stop
        while not stop.is_set():
            # Output completion never waits for GCS in its request handler.
            # Its local resource release leaves this versioned metadata outbox
            # for the existing supervisor, even while the executor stays alive.
            self._flush_pending_resource_report()
            self._flush_pending_worker_death_reports()
            # Publication cleanup and terminal reporting progress independently
            # of the executor's lifetime and the resource-report outbox.
            self._drive_output_publications()
            self._drive_transfer_pins()
            self._drive_abandoned_dependency_custody()
            now = time.monotonic()
            wait_seconds = WORKER_REPLACEMENT_RETRY_BASE_SECONDS
            with self._state_lock:
                incarnations = tuple(
                    (
                        worker_id,
                        self._workers[worker_id],
                        self._workers[worker_id].process,
                        worker_id in getattr(self, "_dead_worker_exitcodes", {}),
                    )
                    for worker_id in self._worker_order
                )
            for worker_id, slot, process, is_dead_tombstone in incarnations:
                if stop.is_set():
                    return
                if process is None:
                    if not is_dead_tombstone:
                        continue
                    retry_after = slot.replacement_retry_after
                    if retry_after > now:
                        wait_seconds = min(
                            wait_seconds,
                            max(0.001, retry_after - now),
                        )
                        continue
                    # A failed fresh-ID allocation or child startup leaves this
                    # stable slot as a retryable tombstone.  Contain each failure
                    # inside the supervisor; one bad spawn must not disable
                    # observation and replacement for the whole Node.
                    self._retry_vacant_worker_replacement(worker_id, slot)
                    continue
                try:
                    alive = process.is_alive()
                except (AssertionError, ValueError):
                    alive = False
                if not alive:
                    self._handle_unexpected_worker_exit(worker_id, process)
            self._flush_pending_worker_death_reports()
            self._drive_output_publications()
            self._flush_pending_resource_report()
            stop.wait(
                min(
                    WORKER_REPLACEMENT_RETRY_MAX_SECONDS,
                    max(0.001, wait_seconds),
                )
            )

    def _sweep_exited_workers(self) -> None:
        """Synchronously reduce exact children that already exited."""

        with self._state_lock:
            incarnations = tuple(
                (worker_id, self._workers[worker_id].process)
                for worker_id in self._worker_order
            )
        for worker_id, process in incarnations:
            if process is None:
                continue
            try:
                alive = process.is_alive()
            except (AssertionError, ValueError):
                # An unusable process handle is not a positive exit-status
                # proof; the reducer will reject it unless an integer exitcode
                # is observable.
                alive = False
            if not alive:
                self._handle_unexpected_worker_exit(worker_id, process)

    @staticmethod
    def _close_exited_process(process: object) -> None:
        try:
            process.join(0)
        except (AssertionError, ValueError, TypeError):
            pass
        try:
            process.close()
        except (AssertionError, ValueError):
            pass

    def _stop_unpublished_worker(
        self, process: object, address: Address
    ) -> None:
        try:
            alive = process.is_alive()
        except (AssertionError, ValueError):
            alive = False
        if alive:
            try:
                self._background_rpc(
                    address, SHUTDOWN_HANDLER,
                    protocol.Shutdown.create("replacement fenced by drain"),
                    request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
                )
            except Exception:
                pass
            process.join(WORKER_STOP_TIMEOUT_SECONDS)
        try:
            alive = process.is_alive()
        except (AssertionError, ValueError):
            alive = False
        if alive:
            process.terminate()
            process.join(WORKER_STOP_TIMEOUT_SECONDS)
        try:
            process.close()
        except (AssertionError, ValueError):
            pass

    def _handle_unexpected_worker_exit(
        self, worker_id: ids.WorkerID, process: object
    ) -> bool:
        try:
            handled = self._handle_unexpected_worker_exit_inner(
                worker_id, process
            )
            if handled:
                # The inner reducer owns only OS/lease facts and returns after
                # both lifecycle and Node-state locks are released.  Publication
                # cleanup may call GCS/owners only outside those locks.
                self._flush_pending_worker_death_reports()
                self._drive_output_publications()
            return handled
        finally:
            # Worker-loss may drain the final child allocation of a REMOVING PG.
            # Flush only after the lifecycle and Node-state critical sections end.
            self._flush_pending_resource_report()

    def _handle_unexpected_worker_exit_inner(
        self, worker_id: ids.WorkerID, process: object
    ) -> bool:
        """Reclaim one dead incarnation and replace its stable slot once."""

        lifecycle_lock = getattr(self, "_worker_lifecycle_lock", None)
        if lifecycle_lock is None:
            with self._state_lock:
                lifecycle_lock = getattr(self, "_worker_lifecycle_lock", None)
                if lifecycle_lock is None:
                    lifecycle_lock = threading.Lock()
                    self._worker_lifecycle_lock = lifecycle_lock
        with lifecycle_lock:
            try:
                alive = process.is_alive()
            except (AssertionError, ValueError):
                alive = False
            if alive:
                return False
            exitcode = getattr(process, "exitcode", None)
            if isinstance(exitcode, bool) or not isinstance(exitcode, int):
                # A missing OS exit status is not an admissible death proof.
                return False
            with self._state_lock:
                slot = self._workers.get(worker_id)
                if slot is None or slot.process is not process:
                    return False
                slot_index = self._worker_order.index(worker_id)
                dead_address = slot.address
                incarnation = slot.incarnation
                if incarnation is None and getattr(self, "_gcs_address", None) is not None:
                    # Strict runtimes publish only registered slots.  Refuse to
                    # synthesize a weaker proof if a malformed fixture/state
                    # somehow violates that publication invariant.
                    return False
                if incarnation is not None and dead_address is None:
                    return False
                slot.process = None
                slot.address = None
                slot.exitcode = exitcode
                slot.replacement_retry_after = 0.0
                slot.replacement_error = None
                dead = getattr(self, "_dead_worker_exitcodes", None)
                if dead is None:
                    dead = {}
                    self._dead_worker_exitcodes = dead
                dead.setdefault(worker_id, exitcode)
                self._reclaim_active_lease_after_worker_exit_locked(worker_id)
                if incarnation is not None:
                    reports = getattr(self, "_worker_death_reports", None)
                    if reports is None:
                        reports = {}
                        self._worker_death_reports = reports
                    reports.setdefault(
                        worker_id,
                        _WorkerDeathReportOutcome(
                            protocol.ReportWorkerDeath(
                                "worker-exit-{}".format(uuid.uuid4().hex),
                                incarnation,
                                exitcode,
                                protocol.WorkerDeathReason.PROCESS_EXIT,
                            ),
                            dead_address,
                        ),
                    )
                replace_worker = (
                    self._shutdown_request_id is None
                    and not self._stop_event.is_set()
                    and not self._worker_supervisor_stop.is_set()
                )
            self._close_exited_process(process)
            self._emit(
                "worker_exited", worker_id=str(worker_id),
                exitcode=exitcode, unexpected=True,
            )
            if not replace_worker:
                return True
            self._retry_vacant_worker_replacement(
                worker_id, slot, lifecycle_lock_held=True
            )
            return True

    def _retry_vacant_worker_replacement(
        self,
        worker_id: ids.WorkerID,
        expected_slot: _WorkerSlot,
        *,
        lifecycle_lock_held: bool = False,
    ) -> bool:
        """Try one fresh incarnation without losing a failed stable slot.

        Every failure is recorded on the vacant slot and retried by the single
        supervisor with bounded backoff.  The inflight counter covers fresh-ID
        selection, spawn, and publication, so exceptions cannot make shutdown
        cleanliness permanently false.  BeginDrain uses the same lifecycle lock
        and therefore either observes a published replacement or fences the
        entire attempt before it starts.
        """

        lifecycle_lock = self._worker_lifecycle_lock

        def attempt() -> bool:
            counted = False
            replacement_process = None
            replacement_address = None
            replacement_id = None
            try:
                with self._state_lock:
                    slot = self._workers.get(worker_id)
                    if (
                        slot is not expected_slot
                        or slot.process is not None
                        or worker_id not in getattr(
                            self, "_dead_worker_exitcodes", {}
                        )
                        or self._shutdown_request_id is not None
                        or self._stop_event.is_set()
                        or self._worker_supervisor_stop.is_set()
                    ):
                        return False
                    slot_index = self._worker_order.index(worker_id)
                    self._worker_replacements_inflight += 1
                    counted = True
                    # Selection belongs to the guarded transaction too: an ID
                    # collision/exhaustion must balance the inflight counter.
                    replacement_id = self._fresh_worker_id_locked()

                replacement_process, replacement_address = (
                    self._spawn_worker_process(replacement_id)
                )
                try:
                    replacement_incarnation = self._register_unpublished_worker(
                        replacement_id, replacement_process
                    )
                except BaseException:
                    self._stop_unpublished_worker(
                        replacement_process, replacement_address
                    )
                    replacement_process = None
                    replacement_address = None
                    raise
                with self._state_lock:
                    publish = (
                        self._shutdown_request_id is None
                        and not self._stop_event.is_set()
                        and not self._worker_supervisor_stop.is_set()
                        and self._worker_order[slot_index] == worker_id
                        and self._workers.get(worker_id) is expected_slot
                        and expected_slot.process is None
                    )
                    if publish:
                        replacement = _WorkerSlot(
                            replacement_id,
                            process=replacement_process,
                            address=replacement_address,
                            pid=replacement_process.pid,
                            incarnation=replacement_incarnation,
                        )
                        order = list(self._worker_order)
                        order[slot_index] = replacement_id
                        del self._workers[worker_id]
                        self._workers[replacement_id] = replacement
                        self._worker_order = tuple(order)
                if not publish:
                    self._stop_unpublished_worker(
                        replacement_process, replacement_address
                    )
                    return False
                self._emit(
                    "worker_replaced",
                    old_worker_id=str(worker_id),
                    worker_id=str(replacement_id),
                    worker_pid=replacement_process.pid,
                )
                return True
            except Exception as exc:
                if replacement_process is not None and replacement_address is not None:
                    try:
                        self._stop_unpublished_worker(
                            replacement_process, replacement_address
                        )
                    except Exception:
                        pass
                with self._state_lock:
                    slot = self._workers.get(worker_id)
                    retrying = (
                        slot is expected_slot
                        and slot.process is None
                        and self._shutdown_request_id is None
                        and not self._stop_event.is_set()
                        and not self._worker_supervisor_stop.is_set()
                    )
                    if retrying:
                        slot.replacement_retry_round += 1
                        delay = min(
                            WORKER_REPLACEMENT_RETRY_MAX_SECONDS,
                            WORKER_REPLACEMENT_RETRY_BASE_SECONDS
                            * (2 ** min(slot.replacement_retry_round - 1, 5)),
                        )
                        slot.replacement_retry_after = time.monotonic() + delay
                        slot.replacement_error = "{}: {}".format(
                            type(exc).__name__, exc
                        )
                self._emit(
                    "worker_replacement_failed",
                    old_worker_id=str(worker_id),
                    error="{}: {}".format(type(exc).__name__, exc),
                    retrying=retrying,
                )
                return False
            finally:
                if counted:
                    with self._state_lock:
                        self._worker_replacements_inflight -= 1
                        if self._worker_replacements_inflight < 0:
                            raise AssertionError(
                                "worker replacement count became negative"
                            )

        if lifecycle_lock_held:
            return attempt()
        with lifecycle_lock:
            return attempt()

    def _fresh_worker_id_locked(self) -> ids.WorkerID:
        """Allocate an identity unused by any live or dead incarnation."""

        unavailable = set(self._workers) | set(
            getattr(self, "_dead_worker_exitcodes", {})
        )
        for _attempt in range(32):
            candidate = ids.WorkerID.random()
            if candidate not in unavailable:
                return candidate
        raise RuntimeError(
            "could not allocate a fresh WorkerID for replacement"
        )

    def _install_worker_drain_fence(self, request_id: str) -> None:
        """Linearize BeginDrain against replacement, then stop its watcher."""

        lifecycle_lock = getattr(self, "_worker_lifecycle_lock", None)
        if lifecycle_lock is None:
            with self._state_lock:
                lifecycle_lock = getattr(self, "_worker_lifecycle_lock", None)
                if lifecycle_lock is None:
                    lifecycle_lock = threading.Lock()
                    self._worker_lifecycle_lock = lifecycle_lock
        with lifecycle_lock:
            with self._state_lock:
                if (
                    self._shutdown_request_id is not None
                    and self._shutdown_request_id != request_id
                ):
                    raise ValueError(
                        "node drain already has a different request ID"
                    )
                self._shutdown_request_id = request_id
                # Shutdown owns removal of every local participant.  Install
                # REMOVING before Workers drain so no new PG lease can enter;
                # root reservations remain held until their child leases finish.
                self._begin_remove_all_placement_groups_locked()
                stop = getattr(self, "_worker_supervisor_stop", None)
                if stop is not None:
                    stop.set()
        # Joining while holding the lifecycle lock would deadlock a watcher that
        # observed an exit just before the fence and is waiting to reclaim it.
        self._stop_worker_supervisor()
        # Actor restart admission is fenced by the same request ID.  Stop its
        # watcher only after that fence, then reduce/report a death whose
        # sentinel raced the stop signal.
        self._stop_actor_supervisor(sweep=True)
        # The stop signal may win just before the watcher examines a child that
        # already exited.  Reap such an incarnation synchronously after the
        # watcher is joined.  The installed epoch makes this path reclaim only:
        # it cannot publish a replacement during cluster drain.
        self._sweep_exited_workers()


    def _stop_worker_slot(self, worker_id: ids.WorkerID) -> _WorkerStopResult:
        with self._state_lock:
            slot = self._workers[worker_id]
            process = slot.process
            address = slot.address
            # Removing the endpoint prevents a new grant even during startup
            # rollback, before the normal shutdown fence exists.
            slot.process = None
            slot.address = None

        if process is None:
            with self._state_lock:
                self._reclaim_active_lease_after_worker_exit_locked(worker_id)
                slot = self._workers[worker_id]
                clean = slot.exitcode == 0 and not slot.forced
                return _WorkerStopResult(
                    worker_id, slot.pid, slot.exitcode, clean, slot.forced
                )

        child_pid = process.pid
        forced = False
        try:
            alive = process.is_alive()
        except (ValueError, AssertionError):
            alive = False
        if alive and address is not None:
            try:
                self._background_rpc(
                    address, SHUTDOWN_HANDLER,
                    protocol.Shutdown.create("node stopping"),
                    request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
                )
            except Exception:
                pass
        process.join(WORKER_STOP_TIMEOUT_SECONDS)
        if process.is_alive():
            forced = True
            process.terminate()
            process.join(WORKER_STOP_TIMEOUT_SECONDS)
        exitcode = process.exitcode
        process.close()
        with self._state_lock:
            slot = self._workers[worker_id]
            slot.pid = child_pid
            slot.exitcode = exitcode
            slot.forced = forced
            self._reclaim_active_lease_after_worker_exit_locked(worker_id)
        return _WorkerStopResult(
            worker_id, child_pid, exitcode, exitcode == 0 and not forced, forced
        )

    def _stop_workers(
        self, *, worker_ids: Optional[tuple[ids.WorkerID, ...]] = None
    ) -> tuple[_WorkerStopResult, ...]:
        """Stop ordinary Workers concurrently and return slot-ordered results."""

        with self._state_lock:
            order = tuple(self._worker_order if worker_ids is None else worker_ids)
        results: dict[ids.WorkerID, _WorkerStopResult] = {}
        result_lock = threading.Lock()

        def stop_one(worker_id: ids.WorkerID) -> None:
            result = self._stop_worker_slot(worker_id)
            with result_lock:
                results[worker_id] = result

        threads = tuple(
            threading.Thread(
                target=stop_one, args=(worker_id,),
                name="miniray-stop-worker-{}".format(worker_id), daemon=True,
            )
            for worker_id in order
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return tuple(results[worker_id] for worker_id in order)


    def _handle_seal_object(self, request: object) -> object:
        if type(request) is not protocol.SealObject:
            raise TypeError("seal_object expects SealObject")
        # Pickle does not run dataclass validation. Freeze the full identity
        # and verify bytes before reading fences or creating a local replica.
        if type(request.data) is not bytes:
            raise TypeError("seal_object data must be bytes")
        if type(request.checksum) is not str:
            raise TypeError("seal_object checksum must be a string")
        request = protocol.SealObject(
            _output_object_id(request.object_id), _output_attempt(request.attempt_id),
            _output_opaque(request.owner_worker_id, ids.WorkerID, "seal owner"),
            request.data, request.checksum,
        )

        with self._state_lock:
            owner_fence = getattr(self, "_owner_death_fences", {}).get(
                request.owner_worker_id
            )
            if owner_fence is not None:
                return protocol.SealObjectReply(
                    request.object_id,
                    False,
                    self.node_id,
                    len(request.data),
                    request.checksum,
                    error=(
                        "object owner is fenced by Worker death {}".format(
                            owner_fence.detection_id
                        )
                    ),
                )
            dropped_metadata = getattr(self, "_dropped_metadata", {}).get(
                request.object_id
            )
            if (dropped_metadata is not None and request.attempt_id.attempt_number
                    <= dropped_metadata[0].attempt_number):
                absence_fenced = (
                    dropped_metadata == (request.attempt_id, request.owner_worker_id, request.checksum)
                    and self._replica_drop_completed_locked(protocol.DropObjectReplica(
                        request.object_id, request.attempt_id, request.owner_worker_id, self.node_id, request.checksum,
                    ))
                )
                return protocol.SealObjectReply(
                    request.object_id, False, self.node_id, len(request.data),
                    request.checksum, error="producer attempt was fenced by replica deletion",
                    absence_fenced=absence_fenced,
                )
            if request.object_id in getattr(self, "_local_replica_write_claims", {}):
                return protocol.SealObjectReply(
                    request.object_id, False, self.node_id, len(request.data),
                    request.checksum, error="object has unfinished publication write or cleanup",
                )
            previous = self._sealed_metadata.get(request.object_id)
            metadata = (
                request.attempt_id,
                request.owner_worker_id,
                len(request.data),
                request.checksum,
            )
            if previous is not None:
                if previous == metadata:
                    return protocol.SealObjectReply(
                        request.object_id,
                        True,
                        self.node_id,
                        previous[2],
                        previous[3],
                    )
                return protocol.SealObjectReply(
                    request.object_id,
                    False,
                    self.node_id,
                    len(request.data),
                    request.checksum,
                    error="object ID is already sealed by a different result",
                )
            try:
                self._object_store.put(request.object_id, request.data)
            except Exception as exc:
                absence_fenced = False
                if not self._object_store.contains(request.object_id, sealed_only=False):
                    # Exact failed one-shot writes leave a tombstone so an
                    # unknown duplicate cannot materialize after rejection.
                    self._finish_replica_drop_locked(protocol.DropObjectReplica(
                        request.object_id, request.attempt_id, request.owner_worker_id, self.node_id, request.checksum,
                    ))
                    absence_fenced = True
                return protocol.SealObjectReply(
                    request.object_id,
                    False,
                    self.node_id,
                    len(request.data),
                    request.checksum,
                    error="{}: {}".format(type(exc).__name__, exc),
                    absence_fenced=absence_fenced,
                )
            self._sealed_metadata[request.object_id] = metadata
            return protocol.SealObjectReply(
                request.object_id,
                True,
                self.node_id,
                len(request.data),
                request.checksum,
            )

    @staticmethod
    def _owner_death_proof_matches(
        first: protocol.WorkerDeathRecord,
        second: protocol.WorkerDeathRecord,
    ) -> bool:
        """Compare the complete immutable GCS Worker-death fact."""

        return first == second

    def _observe_owner_death_replica_locked(
        self, descriptor: protocol.ObjectStoreDescriptor,
    ) -> protocol.OwnerDeathReplicaObservation:
        """Observe one replica while its localization lock and state lock hold."""

        metadata = self._sealed_metadata.get(descriptor.object_id)
        present = self._object_store.contains(
            descriptor.object_id, sealed_only=False
        )
        if metadata is None:
            return protocol.OwnerDeathReplicaObservation(
                descriptor,
                (
                    protocol.OwnerDeathReplicaStatus.CONFLICT
                    if present
                    else protocol.OwnerDeathReplicaStatus.ABSENT
                ),
            )
        if (
            not present
            or not self._descriptor_matches_source(
                descriptor, node_id=self.node_id, metadata=metadata
            )
        ):
            return protocol.OwnerDeathReplicaObservation(
                descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT
            )
        try:
            snapshot = self._object_store.snapshot(descriptor.object_id)
        except Exception:
            return protocol.OwnerDeathReplicaObservation(
                descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT
            )
        if not snapshot.sealed or snapshot.size_bytes != descriptor.size_bytes:
            return protocol.OwnerDeathReplicaObservation(
                descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT
            )
        try:
            payload = self._object_store.get(descriptor.object_id)
            if (len(payload) != descriptor.size_bytes
                    or hashlib.sha256(payload).hexdigest() != descriptor.checksum):
                return protocol.OwnerDeathReplicaObservation(
                    descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT
                )
        except Exception:
            return protocol.OwnerDeathReplicaObservation(
                descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT
            )
        if snapshot.pin_count:
            return protocol.OwnerDeathReplicaObservation(
                descriptor, protocol.OwnerDeathReplicaStatus.PINNED,
                snapshot.pin_count,
            )
        return protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.PRESENT
        )

    @staticmethod
    def _replica_drop_key(request: protocol.DropObjectReplica):
        """Byte-free physical identity, independent of cleanup authority."""
        return (
            bytes(request.object_id.task_id), request.object_id.return_index,
            request.producer_attempt_id.attempt_number, bytes(request.owner_worker_id),
            bytes(request.node_id), request.checksum,
        )

    def _replica_drop_completed_locked(self, request: protocol.DropObjectReplica) -> bool:
        """Read exact history only after the caller validates its authority."""
        receipts = getattr(self, "_replica_drop_receipts", None)
        if receipts is None:
            receipts = self._replica_drop_receipts = set()
        return self._replica_drop_key(request) in receipts

    def _finish_replica_drop_locked(self, request: protocol.DropObjectReplica) -> None:
        """Finish confirmed absence under the object's and Node's locks.

        The caller proves deletion authority and physical absence.  A watermark
        fences old writes, but is not a completion receipt: metadata/claims stay
        as retry work until ObjectManager cleanup succeeds.  Only then may any
        authority acknowledge this exact deletion without touching newer state.
        """
        object_id = request.object_id
        expected = (request.producer_attempt_id, request.owner_worker_id, request.checksum)
        if request.node_id != self.node_id:
            raise RuntimeError("replica cleanup targets another Node")
        if self._object_store.contains(object_id, sealed_only=False):
            raise RuntimeError("replica cleanup cannot complete while bytes remain")
        metadata = self._sealed_metadata.get(object_id)
        if metadata is not None and (metadata[0], metadata[1], metadata[3]) != expected:
            raise RuntimeError("replica cleanup cannot retire another producer's metadata")
        claims = getattr(self, "_local_replica_write_claims", {})
        claim = claims.get(object_id)
        if claim is not None and not claim.matches_drop(request):
            raise RuntimeError("replica cleanup cannot retire another writer's claim")
        dropped = getattr(self, "_dropped_metadata", None)
        if dropped is None:
            dropped = self._dropped_metadata = {}
        prior = dropped.get(object_id)
        if prior is not None and (
                prior[0].attempt_number > request.producer_attempt_id.attempt_number
                or (prior[0].attempt_number == request.producer_attempt_id.attempt_number
                    and prior != expected)):
            raise RuntimeError("replica cleanup conflicts with its deletion watermark")
        dropped[object_id] = expected
        manager = getattr(self, "_object_manager", None)
        if manager is not None:
            manager.forget_local_replica(object_id, attempt_id=request.producer_attempt_id)
        self._sealed_metadata.pop(object_id, None)
        claims.pop(object_id, None)
        receipts = getattr(self, "_replica_drop_receipts", None)
        if receipts is None:
            receipts = self._replica_drop_receipts = set()
        receipts.add(self._replica_drop_key(request))

    def _drop_sealed_replica_locked(
        self, descriptor: protocol.ObjectStoreDescriptor,
    ) -> protocol.DropObjectReplicaReply:
        """Delete one metadata-bound replica with its object lock held.

        Used after ordinary GC, an owner-wide fence or publication rollback
        validates its authority.  Keeping the descriptor local avoids
        inventing a cluster object directory while preserving the existing
        attempt/owner/checksum tombstone identity.
        """

        request = protocol.DropObjectReplica(
            _output_object_id(descriptor.object_id), _output_attempt(descriptor.producer_attempt_id),
            _output_opaque(descriptor.owner_worker_id, ids.WorkerID, "drop owner"),
            _output_opaque(descriptor.node_id, ids.NodeID, "drop node"),
            _output_checksum(descriptor.checksum, "drop checksum"),
        )

        def outcome(
            status: protocol.DropObjectReplicaStatus,
            error: Optional[str] = None,
        ) -> protocol.DropObjectReplicaReply:
            return protocol.DropObjectReplicaReply(
                request.object_id, request.producer_attempt_id,
                request.owner_worker_id, request.node_id, request.checksum,
                status, error,
            )

        if request.node_id != self.node_id:
            return outcome(protocol.DropObjectReplicaStatus.REJECTED, "replica targets another Node")
        if self._replica_drop_completed_locked(request):
            return outcome(protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
        tombstone = getattr(self, "_dropped_metadata", {}).get(request.object_id)
        expected_tombstone = (request.producer_attempt_id, request.owner_worker_id, request.checksum)
        if tombstone is not None:
            if tombstone[0].attempt_number > request.producer_attempt_id.attempt_number:
                return outcome(protocol.DropObjectReplicaStatus.STALE_EPOCH, "a newer replica deletion is fenced")
            if (tombstone[0].attempt_number == request.producer_attempt_id.attempt_number
                    and tombstone != expected_tombstone):
                return outcome(protocol.DropObjectReplicaStatus.INCONSISTENT, "same-epoch deletion identity differs")
        metadata = self._sealed_metadata.get(request.object_id)
        expected_metadata = (
            request.producer_attempt_id, request.owner_worker_id,
            descriptor.size_bytes, request.checksum,
        )
        if metadata != expected_metadata:
            return outcome(
                protocol.DropObjectReplicaStatus.STALE_EPOCH,
                "sweep candidate changed producer metadata",
            )
        claim = getattr(self, "_local_replica_write_claims", {}).get(request.object_id)
        if claim is not None and (claim.expected_metadata != expected_metadata
                or not claim.matches_drop(request)):
            return outcome(protocol.DropObjectReplicaStatus.INCONSISTENT, "replica has another writer's claim")
        if not self._object_store.contains(
            request.object_id, sealed_only=False
        ):
            if tombstone != expected_tombstone:
                return outcome(
                    protocol.DropObjectReplicaStatus.INCONSISTENT,
                    "sealed metadata exists without local object bytes",
                )
            try:
                self._finish_replica_drop_locked(request)
            except Exception as exc:
                return outcome(
                    protocol.DropObjectReplicaStatus.INCONSISTENT,
                    "{}: {}".format(type(exc).__name__, exc),
                )
            return outcome(protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
        try:
            snapshot = self._object_store.snapshot(request.object_id)
        except Exception as exc:
            return outcome(
                protocol.DropObjectReplicaStatus.INCONSISTENT,
                "{}: {}".format(type(exc).__name__, exc),
            )
        if not snapshot.sealed or snapshot.size_bytes != descriptor.size_bytes:
            return outcome(
                protocol.DropObjectReplicaStatus.INCONSISTENT,
                "sweep candidate is not the exact sealed replica",
            )
        # Sealed metadata identifies the deletion candidate, but cannot prove
        # its bytes are still intact. Match publication cleanup: preserve a
        # corrupt replica for explicit repair before writing any watermark or
        # completion receipt. Already-completed absence returned above without
        # reading a possible successor at the same ObjectID.
        try:
            payload = self._object_store.get(request.object_id)
            if (len(payload) != descriptor.size_bytes
                    or hashlib.sha256(payload).hexdigest() != request.checksum):
                return outcome(
                    protocol.DropObjectReplicaStatus.INCONSISTENT,
                    "replica bytes disagree with sealed metadata",
                )
        except Exception as exc:
            return outcome(
                protocol.DropObjectReplicaStatus.INCONSISTENT,
                str(exc) or type(exc).__name__,
            )
        if snapshot.pin_count:
            return outcome(
                protocol.DropObjectReplicaStatus.PINNED,
                "sealed replica is pinned",
            )
        # Fence before delete so an exception after its effect remains
        # distinguishable from unexplained missing bytes.  Metadata is kept
        # until the shared completion tail succeeds; the fence is not an ACK.
        if not hasattr(self, "_dropped_metadata"):
            self._dropped_metadata = {}
        self._dropped_metadata[request.object_id] = expected_tombstone
        try:
            removed = self._object_store.delete(request.object_id)
            if not removed and self._object_store.contains(request.object_id, sealed_only=False):
                if self._object_store.snapshot(request.object_id).pin_count:
                    return outcome(protocol.DropObjectReplicaStatus.PINNED, "sealed replica became pinned")
        except Exception as exc:
            return outcome(protocol.DropObjectReplicaStatus.INCONSISTENT, str(exc) or type(exc).__name__)
        if not removed:
            return outcome(
                protocol.DropObjectReplicaStatus.INCONSISTENT,
                "object-store deletion failed unexpectedly",
            )
        try:
            self._finish_replica_drop_locked(request)
        except Exception as exc:
            return outcome(
                protocol.DropObjectReplicaStatus.INCONSISTENT,
                "{}: {}".format(type(exc).__name__, exc),
            )
        return outcome(protocol.DropObjectReplicaStatus.DROPPED)

    def _handle_install_owner_death_fence(self, request: object) -> object:
        """Permanently fence one owner and process its requested scope.

        Per-object localization locks serialize this scan with dependency pull
        completion and physical drop.  ``_state_lock`` is then the common
        linearization boundary for fence installation, ordinary SealObject,
        and localization's final metadata publication.  Publication-exact
        requests only witness their immutable manifest.  Owner-wide requests
        discover every remaining ordinary replica from Node sealed metadata and
        delete all unpinned matches in the same replayable operation.
        """

        if not isinstance(request, protocol.InstallOwnerDeathFence):
            raise TypeError(
                "install_owner_death_fence expects InstallOwnerDeathFence"
            )
        if request.node_id != self.node_id:
            return protocol.InstallOwnerDeathFenceReply(
                request, protocol.OwnerDeathFenceDisposition.CONFLICT,
                error="owner-death fence targets another node",
            )

        owner_wide = (
            request.scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
        )
        with self._state_lock:
            outcomes = getattr(
                self, "_owner_death_fence_outcomes", None
            )
            if outcomes is None:
                outcomes = {}
                self._owner_death_fence_outcomes = outcomes
            previous = outcomes.get(request.request_id)
            if previous is not None:
                if previous.request == request:
                    return previous.reply
                return protocol.InstallOwnerDeathFenceReply(
                    request, protocol.OwnerDeathFenceDisposition.CONFLICT,
                    error=(
                        "owner-death fence request_id is already bound "
                        "to another exact request"
                    ),
                )
            fences = getattr(self, "_owner_death_fences", None)
            if fences is None:
                fences = {}
                self._owner_death_fences = fences
            installed = fences.get(request.owner_worker_id)
            if installed is not None and not self._owner_death_proof_matches(
                installed, request.owner_death
            ):
                return protocol.InstallOwnerDeathFenceReply(
                    request, protocol.OwnerDeathFenceDisposition.CONFLICT,
                    error=(
                        "owner is already fenced by a different Worker "
                        "death proof"
                    ),
                )
            # The owner-wide sweep must fence before discovering its unbounded
            # local workset: every earlier seal is then visible and every later
            # seal/pull commit is rejected.  Publication-exact requests retain
            # their older object-lock ordering so a pull already holding that
            # exact object lock may finish and become part of its witness.
            if owner_wide and installed is None:
                fences[request.owner_worker_id] = request.owner_death
            localization_locks = getattr(
                self, "_object_localization_locks", None
            )
            if localization_locks is None:
                localization_locks = {}
                self._object_localization_locks = localization_locks
            candidates = (
                tuple(
                    protocol.ObjectStoreDescriptor(
                        object_id, request.owner_worker_id, metadata[0],
                        self.node_id, metadata[2], metadata[3],
                    )
                    for object_id, metadata in sorted(
                        self._sealed_metadata.items(),
                        key=lambda item: item[0],
                    )
                    if metadata[1] == request.owner_worker_id
                )
                if owner_wide else request.expected_replicas
            )
            object_locks = tuple(
                localization_locks.setdefault(
                    descriptor.object_id, threading.Lock()
                )
                for descriptor in sorted(
                    candidates,
                    key=lambda value: value.object_id,
                )
            )

        # Canonical lock ordering permits overlapping publication manifests
        # without deadlock while preserving caller order in the reply.
        with ExitStack() as stack:
            for object_lock in object_locks:
                stack.enter_context(object_lock)
            with self._state_lock:
                outcomes = getattr(
                    self, "_owner_death_fence_outcomes", None
                )
                if outcomes is None:
                    outcomes = {}
                    self._owner_death_fence_outcomes = outcomes
                previous = outcomes.get(request.request_id)
                if previous is not None:
                    if previous.request == request:
                        return previous.reply
                    return protocol.InstallOwnerDeathFenceReply(
                        request, protocol.OwnerDeathFenceDisposition.CONFLICT,
                        error=(
                            "owner-death fence request_id is already bound "
                            "to another exact request"
                        ),
                    )

                fences = getattr(self, "_owner_death_fences", None)
                if fences is None:
                    raise AssertionError(
                        "owner-death fence disappeared after installation"
                    )
                installed = fences.get(request.owner_worker_id)
                if installed is not None and not self._owner_death_proof_matches(
                    installed, request.owner_death
                ):
                    return protocol.InstallOwnerDeathFenceReply(
                        request, protocol.OwnerDeathFenceDisposition.CONFLICT,
                        error=(
                            "owner is already fenced by a different Worker "
                            "death proof"
                        ),
                    )

                # Install before observing.  Every SealObject and localization
                # commit that follows this lock acquisition sees the fence.
                if installed is None:
                    if owner_wide:
                        raise AssertionError(
                            "owner-death fence disappeared before replica sweep"
                        )
                    fences[request.owner_worker_id] = request.owner_death
                if owner_wide:
                    observations = []
                    for descriptor in candidates:
                        observed = self._observe_owner_death_replica_locked(
                            descriptor
                        )
                        if observed.status is not (
                            protocol.OwnerDeathReplicaStatus.ABSENT
                        ):
                            drop = self._drop_sealed_replica_locked(
                                descriptor
                            )
                            if drop.status in (
                                protocol.DropObjectReplicaStatus.DROPPED,
                                protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
                            ):
                                observed = protocol.OwnerDeathReplicaObservation(
                                    descriptor,
                                    protocol.OwnerDeathReplicaStatus.ABSENT,
                                )
                            elif drop.status is (
                                protocol.DropObjectReplicaStatus.PINNED
                            ):
                                current = self._object_store.snapshot(
                                    descriptor.object_id
                                )
                                observed = protocol.OwnerDeathReplicaObservation(
                                    descriptor,
                                    protocol.OwnerDeathReplicaStatus.PINNED,
                                    current.pin_count,
                                )
                            else:
                                observed = protocol.OwnerDeathReplicaObservation(
                                    descriptor,
                                    protocol.OwnerDeathReplicaStatus.CONFLICT,
                                )
                        observations.append(observed)
                    observations = tuple(observations)
                else:
                    observations = tuple(
                        self._observe_owner_death_replica_locked(descriptor)
                        for descriptor in request.expected_replicas
                    )
                reply = protocol.InstallOwnerDeathFenceReply(
                    request, protocol.OwnerDeathFenceDisposition.FENCED,
                    observations,
                )
                # A pinned or inconsistent ordinary replica is not a final ACK.
                # Do not cache it: exact replay must rescan after the pin drops or
                # explicit repair restores metadata consistency.  Publication
                # witnesses remain frozen on first observation as before.
                if reply.complete:
                    outcomes[request.request_id] = _OwnerDeathFenceOutcome(
                        request, reply
                    )
                return reply

    def _handle_get_object(self, request: object) -> object:
        if not isinstance(request, protocol.GetObject):
            raise TypeError("get_object expects GetObject")
        with self._state_lock:
            metadata = self._sealed_metadata.get(request.object_id)
            if metadata is None:
                return protocol.GetObjectReply(
                    request.object_id,
                    self.node_id,
                    found=False,
                    sealed=False,
                    error="object is not present in this node's object store",
                )
            data = self._object_store.get(request.object_id)
            actual_checksum = hashlib.sha256(data).hexdigest()
            if len(data) != metadata[2] or actual_checksum != metadata[3]:
                return protocol.GetObjectReply(
                    request.object_id,
                    self.node_id,
                    found=False,
                    sealed=False,
                    error="stored object bytes do not match sealed metadata",
                )
            expectations = (
                request.expected_attempt_id,
                request.expected_owner_worker_id,
                request.expected_size_bytes,
                request.expected_checksum,
            )
            if any(value is not None for value in expectations) and expectations != metadata:
                return protocol.GetObjectReply(
                    request.object_id,
                    self.node_id,
                    found=False,
                    sealed=False,
                    error="stored object does not match requested producer metadata",
                )
            return protocol.GetObjectReply(
                request.object_id,
                self.node_id,
                found=True,
                sealed=True,
                data=data,
                checksum=actual_checksum,
                producer_attempt_id=metadata[0],
                owner_worker_id=metadata[1],
                size_bytes=metadata[2],
            )

    @staticmethod
    def _descriptor_matches_source(
        descriptor: protocol.ObjectStoreDescriptor,
        *,
        node_id: ids.NodeID,
        metadata: tuple[ids.AttemptID, ids.WorkerID, int, str],
    ) -> bool:
        return (
            descriptor.node_id == node_id
            and descriptor.producer_attempt_id == metadata[0]
            and descriptor.owner_worker_id == metadata[1]
            and descriptor.size_bytes == metadata[2]
            and descriptor.checksum == metadata[3]
        )

    def _handle_pin_object_for_transfer(self, request: object) -> object:
        if not isinstance(request, protocol.PinObjectForTransfer):
            raise TypeError(
                "pin_object_for_transfer expects PinObjectForTransfer"
            )
        request = replace(request)
        descriptor = request.descriptor
        with self._state_lock:
            closed = getattr(self, "_closed_transfer_pins", {}).get(request.transfer_id)
            if closed is not None:
                return protocol.PinObjectForTransferReply(
                    request.transfer_id, descriptor, False, "transfer ID has a permanent close fence",
                )
            if request.requester_node_id in getattr(self, "_transfer_node_deaths", {}):
                return protocol.PinObjectForTransferReply(
                    request.transfer_id, descriptor, False, "requesting Node is confirmed dead",
                )
            previous = self._pinned_transfers.get(request.transfer_id)
            if previous is not None:
                if (
                    previous.descriptor != descriptor
                    or previous.requester_node_id != request.requester_node_id
                ):
                    return protocol.PinObjectForTransferReply(
                        request.transfer_id,
                        descriptor,
                        pinned=False,
                        error="transfer ID is already bound to different metadata",
                    )
                if previous.released or previous.closing:
                    return protocol.PinObjectForTransferReply(
                        request.transfer_id,
                        descriptor,
                        pinned=False,
                        error="transfer pin was already released",
                    )
                if previous.acquired:
                    return protocol.PinObjectForTransferReply(request.transfer_id, descriptor, pinned=True)
            owner_death = getattr(
                self, "_owner_death_fences", {}
            ).get(descriptor.owner_worker_id)
            if owner_death is not None:
                return protocol.PinObjectForTransferReply(
                    request.transfer_id,
                    descriptor,
                    pinned=False,
                    error=(
                        "object owner is fenced by Worker death {}".format(
                            owner_death.detection_id
                        )
                    ),
                )
            if self._shutdown_request_id is not None or self._stop_event.is_set():
                return protocol.PinObjectForTransferReply(
                    request.transfer_id,
                    descriptor,
                    pinned=False,
                    error="node is draining and rejects new transfer sessions",
                )
            try:
                self._require_dependency_not_deleted_locked(descriptor)
            except RuntimeError as exc:
                # An interrupted delete may still have bytes.  Its old epoch
                # cannot acquire a new reader and indefinitely postpone cleanup.
                return protocol.PinObjectForTransferReply(
                    request.transfer_id, descriptor, False, str(exc),
                )
            metadata = self._sealed_metadata.get(descriptor.object_id)
            if metadata is None or not self._descriptor_matches_source(
                descriptor, node_id=self.node_id, metadata=metadata
            ):
                return protocol.PinObjectForTransferReply(
                    request.transfer_id,
                    descriptor,
                    pinned=False,
                    error="source replica does not match the advertised descriptor",
                )
            if previous is None:
                # Persist the exact token before ObjectStore.pin can apply and
                # then raise. UNKNOWN acquisition is neither success nor absent.
                previous = _PinnedTransfer(descriptor, request.requester_node_id, request.transfer_id)
                self._pinned_transfers[request.transfer_id] = previous
            try:
                pin_token = self._object_store.pin(
                    descriptor.object_id, request.transfer_id
                )
            except Exception as exc:
                return protocol.PinObjectForTransferReply(
                    request.transfer_id,
                    descriptor,
                    pinned=False,
                    error="{}: {}".format(type(exc).__name__, exc),
                )
            previous.pin_token = pin_token
            previous.acquired = True
            return protocol.PinObjectForTransferReply(
                request.transfer_id, descriptor, pinned=True
            )

    def _handle_get_object_chunk(self, request: object) -> object:
        if not isinstance(request, protocol.GetObjectChunk):
            raise TypeError("get_object_chunk expects GetObjectChunk")
        if request.size_bytes > OBJECT_TRANSFER_CHUNK_BYTES:
            return protocol.GetObjectChunkReply(
                request.transfer_id,
                request.object_id,
                self.node_id,
                request.offset,
                ok=False,
                error="chunk exceeds {} byte limit".format(
                    OBJECT_TRANSFER_CHUNK_BYTES
                ),
            )
        with self._state_lock:
            session = self._pinned_transfers.get(request.transfer_id)
            if (
                session is None
                or session.released
                or session.closing
                or not session.acquired
                or session.descriptor.object_id != request.object_id
                or session.requester_node_id != request.requester_node_id
            ):
                return protocol.GetObjectChunkReply(
                    request.transfer_id,
                    request.object_id,
                    self.node_id,
                    request.offset,
                    ok=False,
                    error="unknown or mismatched pinned transfer session",
                )
            descriptor = session.descriptor
            if request.offset > descriptor.size_bytes:
                return protocol.GetObjectChunkReply(
                    request.transfer_id,
                    request.object_id,
                    self.node_id,
                    request.offset,
                    ok=False,
                    error="chunk offset exceeds object size",
                )
            payload = self._object_store.get(request.object_id)
            end = min(
                descriptor.size_bytes, request.offset + request.size_bytes
            )
            data = payload[request.offset:end]
        return protocol.GetObjectChunkReply(
            request.transfer_id,
            request.object_id,
            self.node_id,
            request.offset,
            data=data,
        )

    def _handle_release_object_pin(self, request: object) -> object:
        if not isinstance(request, protocol.ReleaseObjectPin):
            raise TypeError("release_object_pin expects ReleaseObjectPin")
        request = replace(request)
        with self._state_lock:
            closed = getattr(self, "_closed_transfer_pins", None)
            if closed is None:
                closed = self._closed_transfer_pins = {}
            previous = closed.get(request.transfer_id)
            if previous is not None:
                if previous.request != request:
                    return protocol.ReleaseObjectPinReply(
                        request.transfer_id, request.object_id, self.node_id, False, False,
                        "transfer close identity does not match",
                    )
                if previous.closed:
                    return protocol.ReleaseObjectPinReply(
                        request.transfer_id, request.object_id, self.node_id, True, False,
                    )
            session = self._pinned_transfers.get(request.transfer_id)
            if (
                session is not None and (session.descriptor.object_id != request.object_id
                or session.requester_node_id != request.requester_node_id)
            ):
                return protocol.ReleaseObjectPinReply(
                    request.transfer_id,
                    request.object_id,
                    self.node_id,
                    accepted=False,
                    released=False,
                    error="mismatched pinned transfer session",
                )
            if previous is None:
                previous = closed[request.transfer_id] = _ClosedTransferPin(request)
            if session is None:
                # This Close may beat a delayed Pin on another connection.
                # The permanent fence makes absence durable before the ACK.
                previous.closed = True
                return protocol.ReleaseObjectPinReply(
                    request.transfer_id, request.object_id, self.node_id, True, False,
                )
            if session.released:
                previous.closed = True
                return protocol.ReleaseObjectPinReply(
                    request.transfer_id,
                    request.object_id,
                    self.node_id,
                    accepted=True,
                    released=False,
                )
            session.closing = True
            try:
                released = (self._object_store.unpin(request.object_id, session.pin_token)
                            if self._object_store.contains(request.object_id, sealed_only=False) else False)
            except Exception as exc:
                return protocol.ReleaseObjectPinReply(
                    request.transfer_id, request.object_id, self.node_id, False, False,
                    "source unpin remains unconfirmed: {}".format(exc),
                )
            session.released = True
            previous.closed = True
            return protocol.ReleaseObjectPinReply(
                request.transfer_id,
                request.object_id,
                self.node_id,
                accepted=True,
                released=released,
            )

    def _source_pin_outbox_locked(self) -> TransferPinOutbox:
        outbox = getattr(self, "_source_pin_releases", None)
        if outbox is None:
            outbox = self._source_pin_releases = TransferPinOutbox()
        return outbox

    def _source_pin_release_attempt(self, record, request):
        gone = record.source_death is not None
        if gone:
            return True
        try:
            reply = rpc_request(record.source_address, RELEASE_OBJECT_PIN_HANDLER, request,
                connect_timeout=0.25, request_timeout=0.5, deadline=time.monotonic() + 0.75)
            if type(reply) is not protocol.ReleaseObjectPinReply:
                return False
            reply = replace(reply)
            return (reply.accepted is True and reply.transfer_id == request.transfer_id
                and reply.object_id == request.object_id and reply.node_id == record.pin.descriptor.node_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            return False

    def _drive_source_pin_releases(self, *, force=False, max_effects=1, transfer_id=None) -> bool:
        """Replay bounded close work without holding the Node lock over RPC."""
        progressed = False
        for _ in range(max_effects):
            with self._state_lock:
                outbox = self._source_pin_outbox_locked()
                record = outbox.acquire(time.monotonic(), transfer_id=transfer_id, force=force)
                if record is None:
                    break
                request = record.release
            acknowledged = False
            try:
                acknowledged = self._source_pin_release_attempt(record, request)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                acknowledged = False
            finally:
                with self._state_lock:
                    progressed |= outbox.settle(record, acknowledged=acknowledged, now=time.monotonic())
        return progressed

    def _finish_source_pin_read(self, transfer_id):
        """Atomically hand the reader's pin to one bounded close driver."""
        with self._state_lock:
            outbox = self._source_pin_outbox_locked()
            record = outbox.close_and_acquire(transfer_id, time.monotonic())
            if record is None:
                raise RuntimeError("source pin reader lost its close ticket")
        acknowledged = False
        try:
            # Keep the same ticket across immediate retries. A background
            # driver cannot turn a successful live transfer into a spurious
            # failure by stealing the close between these bounded attempts.
            for _ in range(3):
                if self._source_pin_release_attempt(record, record.release):
                    acknowledged = True
                    break
        finally:
            with self._state_lock:
                settled = outbox.settle(record, acknowledged=acknowledged, now=time.monotonic())
        return settled

    def _query_transfer_node_death(self, node_id):
        """Get an exact GCS Node death, never infer it from unreachability."""
        with self._state_lock:
            cache = getattr(self, "_transfer_node_deaths", None)
            if cache is None:
                cache = self._transfer_node_deaths = {}
            if node_id in cache:
                self._source_pin_outbox_locked().mark_source_dead(cache[node_id])
                return cache[node_id]
            gcs = getattr(self, "_gcs_address", None)
        if gcs is None:
            return None
        try:
            reply = self._background_rpc(gcs, GCS_GET_NODE_STATE_HANDLER, protocol.GetNodeState(node_id),
                                         connect_timeout=0.25, request_timeout=0.5)
            if type(reply) is not protocol.GetNodeStateReply:
                return None
            reply = replace(reply)
            if (reply.node_id != node_id or reply.found is not True
                    or reply.state is not protocol.NodeMembershipState.DEAD or reply.death is None):
                return None
            proof = reply.death
            death = protocol.NodeDeathRecord(
                proof.detection_id, _output_opaque(proof.node_id, ids.NodeID, "dead transfer Node"),
                proof.node_pid, proof.registration_epoch, proof.death_epoch, proof.exit_code, proof.reason, proof.detail,
            )
            if (death.node_id != node_id or death.node_pid != reply.node_pid
                    or death.registration_epoch != reply.registration_epoch or death.death_epoch > reply.membership_epoch):
                return None
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            return None
        with self._state_lock:
            prior = cache.setdefault(node_id, death)
            if prior != death:
                return None
            self._source_pin_outbox_locked().mark_source_dead(death)
            return prior

    def _drive_transfer_pins(self, *, force=False) -> bool:
        """Use the existing supervisor/drain for source and target obligations.

        Living target: close its source pin after the reader leaves. Dead
        target: the living source owns the physical unpin and must do it here.
        GCS NodeIDs cannot be re-registered after DEAD, so a cached exact death
        is a permanent fence; Worker death or a missing snapshot is not one.
        """
        progressed = self._drive_source_pin_releases(force=force)
        with self._state_lock:
            closing = tuple(item.request for item in getattr(self, "_closed_transfer_pins", {}).values() if not item.closed)[:1]
        for request in closing:
            reply = self._handle_release_object_pin(request)
            progressed |= reply.accepted
        with self._state_lock:
            now = time.monotonic()
            if not force and now < getattr(self, "_transfer_death_probe_after", 0.0):
                return progressed
            self._transfer_death_probe_after = now + 0.1
            peers = {record.pin.descriptor.node_id for record in self._source_pin_outbox_locked().pending()}
            peers.update(session.requester_node_id for session in self._pinned_transfers.values() if not session.released)
        for peer in sorted(peers):
            death = self._query_transfer_node_death(peer)
            if death is None:
                continue
            with self._state_lock:
                close = tuple(protocol.ReleaseObjectPin(transfer_id, session.descriptor.object_id, session.requester_node_id)
                    for transfer_id, session in self._pinned_transfers.items()
                    if not session.released and session.requester_node_id == death.node_id)
            for request in close:
                progressed |= self._handle_release_object_pin(request).accepted
        return progressed

    def _handle_drop_object_replica(self, request: object) -> object:
        """Delete one matching local replica without crossing epochs.

        The reply is deliberately typed: only ``DROPPED`` and an exact
        tombstone replay (``ALREADY_DROPPED``) acknowledge a durable GC
        obligation. Successful receipts survive newer physical epochs without
        deleting their bytes. Every other outcome preserves enough identity for the
        owner to retry or fence the obligation without parsing error strings.
        """

        if not isinstance(request, protocol.DropObjectReplica):
            # A value of another protocol type has no valid drop identity to
            # echo.  This is a transport/handler contract violation, unlike a
            # well-formed request that this Node rejects with a typed status.
            raise TypeError(
                "drop_object_replica expects DropObjectReplica"
            )
        request = replace(request)
        request = protocol.DropObjectReplica(
            _output_object_id(request.object_id),
            _output_attempt(request.producer_attempt_id),
            _output_opaque(request.owner_worker_id, ids.WorkerID, "drop owner"),
            _output_opaque(request.node_id, ids.NodeID, "drop node"),
            _output_checksum(request.checksum, "drop checksum"),
        )
        def outcome(
            status: protocol.DropObjectReplicaStatus,
            error: Optional[str] = None,
        ) -> protocol.DropObjectReplicaReply:
            return protocol.DropObjectReplicaReply(
                object_id=_output_object_id(request.object_id),
                producer_attempt_id=_output_attempt(request.producer_attempt_id),
                owner_worker_id=_output_opaque(request.owner_worker_id, ids.WorkerID, "drop owner"),
                node_id=_output_opaque(request.node_id, ids.NodeID, "drop node"),
                checksum=request.checksum,
                status=status,
                error=error,
            )

        if request.node_id != self.node_id:
            return outcome(
                protocol.DropObjectReplicaStatus.REJECTED,
                error="drop request targets another node",
            )

        # Dependency localization performs object-store work outside the
        # global node lock.  Share its per-object lock so a debug drop cannot
        # observe the brief seal-before-metadata interval.
        with self._state_lock:
            localization_locks = getattr(
                self, "_object_localization_locks", None
            )
            if localization_locks is None:
                localization_locks = {}
                self._object_localization_locks = localization_locks
            object_lock = localization_locks.setdefault(
                request.object_id, threading.Lock()
            )
        with object_lock, self._state_lock:
            if self._replica_drop_completed_locked(request):
                # All old physical and ObjectManager cleanup already completed.
                # Replay may not inspect, delete, or forget a newer replica.
                return outcome(protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
            stop_event = getattr(self, "_stop_event", None)
            if stop_event is not None and stop_event.is_set():
                return outcome(
                    protocol.DropObjectReplicaStatus.NODE_DRAINING,
                    error="node is shutting down",
                )

            metadata = self._sealed_metadata.get(request.object_id)
            tombstone = getattr(self, "_dropped_metadata", {}).get(
                request.object_id
            )
            requested_metadata = (
                request.producer_attempt_id,
                request.owner_worker_id,
                request.checksum,
            )
            if metadata is None:
                if self._object_store.contains(
                    request.object_id, sealed_only=False
                ):
                    return outcome(
                        protocol.DropObjectReplicaStatus.INCONSISTENT,
                        error=(
                            "local object bytes exist without matching sealed "
                            "metadata"
                        ),
                    )
                if tombstone != requested_metadata:
                    if (
                        tombstone is not None
                        and tombstone[0].attempt_number
                        > request.producer_attempt_id.attempt_number
                    ):
                        return outcome(
                            protocol.DropObjectReplicaStatus.STALE_EPOCH,
                            error=(
                                "a newer producer epoch has already been "
                                "deleted on this node"
                            ),
                        )
                    return outcome(
                        protocol.DropObjectReplicaStatus.REJECTED,
                        error="no matching deleted-replica tombstone exists",
                    )
                # A replay after a successful deletion is harmless.  Clear a
                # matching FAILED/READY coordinator record if the bytes are
                # already absent, but never clear an in-flight/newer epoch.
                try:
                    self._finish_replica_drop_locked(request)
                except Exception as exc:
                    return outcome(
                        protocol.DropObjectReplicaStatus.INCONSISTENT,
                        error="{}: {}".format(type(exc).__name__, exc),
                    )
                return outcome(
                    protocol.DropObjectReplicaStatus.ALREADY_DROPPED
                )

            expected = (
                request.producer_attempt_id,
                request.owner_worker_id,
                request.checksum,
            )
            actual = (metadata[0], metadata[1], metadata[3])
            if actual != expected:
                return outcome(
                    protocol.DropObjectReplicaStatus.STALE_EPOCH,
                    error=(
                        "sealed replica does not match requested producer "
                        "attempt, owner, and checksum"
                    ),
                )

            # All authorities use the same physical tail.  In particular a
            # failed ObjectManager forget keeps sealed metadata as retry work,
            # blocking a newer seal until exact cleanup has really finished.
            reply = self._drop_sealed_replica_locked(protocol.ObjectStoreDescriptor(
                request.object_id, request.owner_worker_id, request.producer_attempt_id,
                self.node_id, metadata[2], request.checksum,
            ))
            return outcome(reply.status, reply.error)

    def _release_all_transfer_pins(self) -> int:
        """Release every source pin exactly once before object service stops."""

        with self._state_lock:
            requests = tuple(protocol.ReleaseObjectPin(transfer_id, session.descriptor.object_id, session.requester_node_id)
                             for transfer_id, session in self._pinned_transfers.items() if not session.released)
        return sum(self._handle_release_object_pin(request).released for request in requests)

    def _handle_reserve_actor_worker(self, request: object) -> object:
        if not isinstance(request, protocol.ReserveActorWorkerRequest):
            raise TypeError(
                "reserve_actor_worker expects ReserveActorWorkerRequest"
            )
        try:
            # Pickle may bypass dataclass __post_init__; rebuild the complete
            # restart proof before any resource or process side effect.
            request = replace(request)
        except Exception as exc:
            return self._rejected_actor_worker(
                request, "invalid Actor reservation: {}".format(exc)
            )
        if request.target_node_id != self.node_id:
            return self._rejected_actor_worker(request, "actor targets another node")
        with self._state_lock:
            self._ensure_actor_lifecycle_state_locked()
            creation_lock = self._actor_creation_locks.setdefault(
                request.actor_id, threading.Lock()
            )
        with creation_lock:
            with self._state_lock:
                terminal = self._actor_generation_outcomes.get(
                    request.generation
                )
                if terminal is not None:
                    if terminal.request == request:
                        # Settle ambiguity for the exact old reservation without
                        # restoring it to the live Actor index.  GCS route_epoch
                        # fencing prevents this cached endpoint from becoming
                        # current again.
                        return terminal.reply
                    return self._rejected_actor_worker(
                        request,
                        "actor generation is terminal with different metadata",
                    )
                previous = self._actor_workers.get(request.actor_id)
                if previous is not None:
                    if previous.request == request:
                        return previous.reply
                if self._stop_event.is_set() or self._shutdown_request_id is not None:
                    return self._rejected_actor_worker(
                        request, "node is shutting down",
                        failure=protocol.ActorWorkerFailure.NODE_STOPPING,
                    )
                if previous is not None:
                    return self._rejected_actor_worker(
                        request,
                        "ActorID has a different live generation or metadata",
                    )
                if request.generation.generation == 0:
                    if any(
                        generation.actor_id == request.actor_id
                        for generation in self._actor_generation_outcomes
                    ):
                        return self._rejected_actor_worker(
                            request, "initial actor generation is stale"
                        )
                else:
                    previous_generation = ids.ActorGeneration(
                        request.actor_id, request.generation.generation - 1
                    )
                    previous_outcome = self._actor_generation_outcomes.get(
                        previous_generation
                    )
                    if (
                        previous_outcome is None
                        or request.restart != previous_outcome.exit_record
                    ):
                        return self._rejected_actor_worker(
                            request,
                            "restart proof is not this Node's exact prior tombstone",
                        )
                    if request.route_epoch != request.restart.route_epoch + 2:
                        return self._rejected_actor_worker(
                            request,
                            "restart route epoch is not the GCS-authorized successor",
                        )
                    if not self._same_actor_lifetime_spec(
                        previous_outcome.request, request
                    ):
                        return self._rejected_actor_worker(
                            request, "actor restart changed immutable metadata"
                        )
                allocation_token = resources.AllocationToken.random()
                allocated = self._ledger.try_allocate(
                    request.resources, allocation_token
                )
                if allocated is None:
                    return self._rejected_actor_worker(
                        request, "actor lifetime resources are unavailable",
                        failure=protocol.ActorWorkerFailure.CAPACITY_UNAVAILABLE,
                    )
                self._refresh_local_cached_availability_locked()

            try:
                startup, process = self._spawn_actor_worker(request)
                if (
                    startup.actor_id != request.actor_id
                    or startup.generation != request.generation
                    or startup.worker_pid != getattr(process, "pid", None)
                ):
                    raise RuntimeError("actor worker returned inconsistent startup identity")
                reply = protocol.ReserveActorWorkerReply(
                    request.actor_id,
                    request.generation,
                    accepted=True,
                    node_id=self.node_id,
                    worker_id=startup.worker_id,
                    worker_address=startup.worker_address,
                    worker_pid=startup.worker_pid,
                )
                record = _ActorWorkerRecord(
                    request, allocated, process, startup, reply
                )
                with self._state_lock:
                    # Shutdown may have started while the constructor ran.  Do
                    # not publish an actor endpoint into a draining node.
                    if (
                        self._stop_event.is_set()
                        or self._shutdown_request_id is not None
                    ):
                        raise _ActorWorkerStartupError(
                            protocol.ActorWorkerFailure.NODE_STOPPING,
                            "node began shutting down during actor creation",
                        )
                    if startup.worker_id in self._actor_worker_ids_seen:
                        raise RuntimeError(
                            "actor restart reused a physical WorkerID"
                        )
                    if startup.worker_pid in self._actor_worker_pids_seen:
                        raise RuntimeError(
                            "actor restart reused a physical worker PID"
                        )
                    self._actor_worker_ids_seen.add(startup.worker_id)
                    self._actor_worker_pids_seen.add(startup.worker_pid)
                    self._actor_workers[request.actor_id] = record
                self._report_resources_to_gcs_best_effort()
                return reply
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                process = locals().get("process")
                startup = locals().get("startup")
                self._stop_actor_process_best_effort(process, startup)
                with self._state_lock:
                    self._ledger.release(allocated)
                    self._refresh_local_cached_availability_locked()
                self._report_resources_to_gcs_best_effort()
                return self._rejected_actor_worker(
                    request, "{}: {}".format(type(exc).__name__, exc),
                    failure=(exc.failure if isinstance(exc, _ActorWorkerStartupError)
                             else protocol.ActorWorkerFailure.STARTUP_FAILED),
                )

    @staticmethod
    def _same_actor_lifetime_spec(
        previous: protocol.ReserveActorWorkerRequest,
        candidate: protocol.ReserveActorWorkerRequest,
    ) -> bool:
        """Compare the immutable logical Actor definition across restart."""

        return (
            previous.actor_id == candidate.actor_id
            and previous.class_definition == candidate.class_definition
            and previous.constructor_payload == candidate.constructor_payload
            and previous.resources == candidate.resources
            and previous.owner_worker_id == candidate.owner_worker_id
        )

    def _spawn_actor_worker(
        self, request: protocol.ReserveActorWorkerRequest
    ) -> tuple[protocol.ActorWorkerStartup, object]:
        from .actor_worker import actor_worker_main

        worker_id = _new_id(ids.WorkerID)
        parent_connection, child_connection = self._context.Pipe(duplex=False)
        process = self._context.Process(
            target=actor_worker_main,
            args=(
                request.actor_id,
                request.generation,
                worker_id,
                request.class_definition,
                request.constructor_payload,
                self.node_id,
                self.address,
                child_connection,
                self._host,
                0,
                self._inline_threshold,
                self._trace_config.for_role("actor:{}".format(request.actor_id)) if self._trace_config else None,
            ),
            name="miniray-actor-{}".format(request.actor_id),
            daemon=False,
        )
        try:
            process.start()
            child_connection.close()
            if not parent_connection.poll(WORKER_START_TIMEOUT_SECONDS):
                raise RuntimeError("actor worker did not report readiness before timeout")
            ok, value = parent_connection.recv()
            if not ok:
                if not isinstance(value, protocol.ActorWorkerStartupFailure):
                    raise RuntimeError("actor worker returned an invalid startup failure")
                value = replace(value)
                raise _ActorWorkerStartupError(value.failure, value.error)
            if not isinstance(value, protocol.ActorWorkerStartup):
                raise RuntimeError("actor worker returned an invalid startup descriptor")
            return value, process
        except BaseException:
            self._stop_actor_process_best_effort(process, None)
            raise
        finally:
            parent_connection.close()

    @staticmethod
    def _rejected_actor_worker(
        request: protocol.ReserveActorWorkerRequest, error: str, *, failure=None,
    ) -> protocol.ReserveActorWorkerReply:
        return protocol.ReserveActorWorkerReply(
            request.actor_id, request.generation, accepted=False, error=error,
            failure=(protocol.ActorWorkerFailure.INVALID_REQUEST if failure is None else failure),
        )

    def _stop_actor_process_best_effort(
        self, process: object | None, startup: object | None
    ) -> None:
        """Force-clean an unpublished Actor process during startup rollback."""

        if process is None:
            return
        address = getattr(startup, "worker_address", None)
        if getattr(process, "is_alive")() and address is not None:
            try:
                self._background_rpc(
                    address,
                    SHUTDOWN_HANDLER,
                    protocol.Shutdown.create("node stopping actor"),
                    request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
                )
            except Exception:
                pass
        getattr(process, "join")(WORKER_STOP_TIMEOUT_SECONDS)
        if getattr(process, "is_alive")():
            getattr(process, "terminate")()
            getattr(process, "join")(WORKER_STOP_TIMEOUT_SECONDS)
        try:
            getattr(process, "close")()
        except (AssertionError, ValueError):
            pass

    def _stop_actor_worker(
        self,
        actor_id: ids.ActorID,
        record: _ActorWorkerRecord,
        request_id: str,
    ) -> _ActorStopResult:
        """Stop one published Actor and retain exact graceful-stop evidence."""

        process = record.process
        startup = record.startup
        acknowledged = False
        forced = False
        try:
            alive = bool(getattr(process, "is_alive")())
        except (AssertionError, ValueError):
            alive = False
        if alive:
            try:
                candidate = self._background_rpc(
                    startup.worker_address,
                    SHUTDOWN_HANDLER,
                    protocol.Shutdown(request_id, "node finalizing actor"),
                    request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
                )
                acknowledged = bool(
                    isinstance(candidate, protocol.ShutdownAck)
                    and candidate.request_id == request_id
                    and candidate.component
                    == "actor-worker:{}".format(startup.worker_id)
                    and candidate.clean
                    and not candidate.forced
                )
            except Exception:
                acknowledged = False
            getattr(process, "join")(WORKER_STOP_TIMEOUT_SECONDS)
            try:
                alive = bool(getattr(process, "is_alive")())
            except (AssertionError, ValueError):
                alive = False
        if alive:
            forced = True
            getattr(process, "terminate")()
            getattr(process, "join")(WORKER_STOP_TIMEOUT_SECONDS)
            try:
                alive = bool(getattr(process, "is_alive")())
            except (AssertionError, ValueError):
                alive = False
        exitcode = getattr(process, "exitcode", None)
        clean = acknowledged and not alive and exitcode == 0 and not forced
        try:
            getattr(process, "close")()
        except (AssertionError, ValueError):
            pass
        return _ActorStopResult(
            actor_id=actor_id,
            worker_id=startup.worker_id,
            pid=startup.worker_pid,
            exitcode=exitcode,
            clean=clean,
            forced=forced,
        )

    def _stop_all_actor_workers(
        self, request_id: Optional[str] = None
    ) -> tuple[_ActorStopResult, ...]:
        # Wait for every creation handler that existed when shutdown began.
        # Creation handlers check the shutdown fence before publishing, so each
        # lock resolves to either a committed record or a fully released failure.
        epoch = request_id or self._shutdown_request_id or "node-finalizer"
        with self._state_lock:
            previous_epoch = getattr(self, "_actor_finalize_request_id", None)
            if previous_epoch is not None and previous_epoch != epoch:
                raise ValueError(
                    "Actor finalization already belongs to a different request ID"
                )
            self._actor_finalize_request_id = epoch
            creation_locks = tuple(self._actor_creation_locks.values())
        for creation_lock in creation_locks:
            creation_lock.acquire()
        try:
            with self._state_lock:
                records = tuple(self._actor_workers.items())
        finally:
            for creation_lock in reversed(creation_locks):
                creation_lock.release()

        for actor_id, record in records:
            result = self._stop_actor_worker(actor_id, record, epoch)
            with self._state_lock:
                if self._actor_workers.get(actor_id) is record:
                    self._actor_workers.pop(actor_id, None)
                    self._ledger.release(record.allocation_token)
                    self._refresh_local_cached_availability_locked()
                self._actor_finalize_results[actor_id] = result
        if records:
            self._report_resources_to_gcs_best_effort()
        with self._state_lock:
            return tuple(self._actor_finalize_results.values())

    def _handle_request_lease(self, request: object) -> object:
        if not isinstance(request, protocol.RequestWorkerLease):
            raise TypeError("request_worker_lease expects RequestWorkerLease")
        request = protocol.revalidate_worker_lease_request(request)
        # Count every well-formed RPC, including an exact replay waiting behind
        # the same LeaseID lock.  Shutdown must not report a quiescent Node while
        # a pre-fence request is still localizing dependencies or committing its
        # cached outcome.  We deliberately do not reject at this outer boundary:
        # an exact cached outcome remains replayable after BeginDrain.
        with self._state_lock:
            self._inflight_lease_requests = (
                getattr(self, "_inflight_lease_requests", 0) + 1
            )
        try:
            return self._handle_counted_request_lease(request)
        finally:
            with self._state_lock:
                self._inflight_lease_requests -= 1
                if self._inflight_lease_requests < 0:
                    raise AssertionError(
                        "Node in-flight lease request count became negative"
                    )
            # This also retries any earlier failed report, including a report
            # whose originating mutation has already reached terminal state.
            self._flush_pending_resource_report()

    def _handle_counted_request_lease(
        self, request: protocol.RequestWorkerLease
    ) -> object:
        # Ordinary Node traffic opportunistically advances transient unpin
        # cleanup after its bounded backoff.  Shutdown and later terminal lease
        # transitions force an immediate retry.
        self._retry_dependency_pin_cleanups()
        self._drive_source_pin_releases()
        self._emit(
            "lease_requested",
            lease_id=str(request.lease_id),
            task_id=str(request.task_id),
            attempt_id=str(request.attempt_id),
            node_id=str(self.node_id),
        )
        with self._state_lock:
            if not hasattr(self, "_lease_cancellations"):
                self._lease_cancellations = {}
            request_lock = self._lease_request_locks.setdefault(
                request.lease_id, threading.Lock()
            )
        # One lease ID names one scheduling decision.  Serializing the complete
        # check/snapshot/commit path prevents concurrent duplicates from
        # committing a grant and a spillback from different resource snapshots.
        with request_lock:
            with self._state_lock:
                if request.requester_worker_id in getattr(self, "_dead_dependency_submitters", {}):
                    return self._rejected(request, protocol.LeaseRejectReason.STALE_ATTEMPT, "dependency submitter is confirmed dead")
                cancellation = self._lease_cancellations.get(request.lease_id)
                if cancellation is not None:
                    cancel = cancellation.request
                    if (
                        cancel.task_id != request.task_id
                        or cancel.attempt_id != request.attempt_id
                        or cancel.requester_node_id != request.requester_node_id
                        or cancel.requester_worker_id != request.requester_worker_id
                        or cancel.scheduling_key != request.scheduling_key
                    ):
                        return self._rejected(
                            request, protocol.LeaseRejectReason.STALE_ATTEMPT,
                            "lease ID was cancelled by a different requester",
                        )
                    # The cancellation tombstone wins even if this request was
                    # delayed in the transport before reaching the Node.
                    return self._rejected(
                        request, protocol.LeaseRejectReason.STALE_ATTEMPT,
                        "worker lease was cancelled before execution",
                    )
                try:
                    self._dependency_custody_registry_locked().bind(request)
                except (DependencyCustodyConflict, protocol.ProtocolError) as exc:
                    return self._rejected(request, protocol.LeaseRejectReason.STALE_ATTEMPT, str(exc))
                previous = self._lease_outcomes.get(request.lease_id)
                if previous is not None:
                    if previous.request != request:
                        return self._rejected(
                            request,
                            protocol.LeaseRejectReason.STALE_ATTEMPT,
                            "lease ID reused with a different request",
                        )
                    # PENDING_CAPACITY is an observation, not a committed
                    # scheduling decision.  The same logical lease may be
                    # polled after capacity changes, under this same per-ID
                    # lock, and the single cached outcome is overwritten.
                    # Every other outcome is frozen: replaying a spillback,
                    # grant, infeasible decision, or shutdown rejection must
                    # never run scheduling twice for the same LeaseID.
                    transient_capacity = (
                        isinstance(
                            previous.reply, protocol.RejectWorkerLease
                        )
                        and previous.reply.reason
                        is protocol.LeaseRejectReason.PENDING_CAPACITY
                    )
                    if not transient_capacity:
                        record = self._leases.get(request.lease_id)
                        if (
                            record is not None
                            and record.state
                            is not protocol.LeaseExecutionState.GRANTED
                        ):
                            return self._rejected(
                                request,
                                protocol.LeaseRejectReason.STALE_ATTEMPT,
                                "lease is no longer grantable",
                            )
                        return previous.reply
            reply = self._handle_request_lease_serialized(request)
            with self._state_lock:
                # A grant outcome is published atomically with its record, pins,
                # and slot by the serialized transaction.  Other outcomes have
                # no remote execution capability and are cached here.
                committed_outcome = self._lease_outcomes.get(request.lease_id)
                if committed_outcome is None:
                    self._lease_outcomes[request.lease_id] = _LeaseOutcome(
                        request, reply
                    )
                elif (
                    committed_outcome.request != request
                    or committed_outcome.reply != reply
                ):
                    transient = (
                        committed_outcome.request == request
                        and isinstance(
                            committed_outcome.reply, protocol.RejectWorkerLease
                        )
                        and committed_outcome.reply.reason
                        is protocol.LeaseRejectReason.PENDING_CAPACITY
                    )
                    if transient and not isinstance(
                        reply, protocol.GrantWorkerLease
                    ):
                        self._lease_outcomes[request.lease_id] = _LeaseOutcome(
                            request, reply
                        )
                    else:
                        raise AssertionError(
                            "serialized lease transaction changed its cached outcome"
                        )
            return reply

    def _handle_cancel_worker_lease(self, request: object) -> object:
        try:
            return self._handle_cancel_worker_lease_inner(request)
        finally:
            self._flush_pending_resource_report()

    def _handle_cancel_worker_lease_inner(self, request: object) -> object:
        """Serialize cancellation against grant and retain a tombstone."""

        if not isinstance(request, protocol.CancelWorkerLease):
            raise TypeError("cancel_worker_lease expects CancelWorkerLease")
        request = replace(request)
        with self._state_lock:
            if not hasattr(self, "_lease_cancellations"):
                self._lease_cancellations = {}
            request_lock = self._lease_request_locks.setdefault(
                request.lease_id, threading.Lock()
            )
        with request_lock:
            with self._state_lock:
                previous = self._lease_cancellations.get(request.lease_id)
                if previous is not None:
                    if previous.request != request:
                        return self._cancel_reply(
                            request, protocol.LeaseExecutionState.ABANDONED,
                            accepted=False, cancelled=False, released=False,
                            error="lease ID was cancelled with different identity",
                        )
                    return replace(previous.reply, released=False)
                bound = self._dependency_custody_registry_locked().request(request.lease_id)
                if bound is not None and (
                    bound.task_id != request.task_id or bound.attempt_id != request.attempt_id
                    or bound.requester_node_id != request.requester_node_id
                    or bound.requester_worker_id != request.requester_worker_id
                    or bound.scheduling_key != request.scheduling_key
                    or request.lease_request is not None and bound != request.lease_request
                ):
                    return self._cancel_reply(
                        request, protocol.LeaseExecutionState.ABANDONED,
                        accepted=False, cancelled=False, released=False,
                        error="cancellation identity conflicts with localization inventory",
                    )
            self._reconcile_localization_candidates(request.lease_id)
            with self._state_lock:
                registry = self._dependency_custody_registry_locked()
                original = registry.request(request.lease_id)
                if request.lease_request is not None:
                    try:
                        registry.bind(request.lease_request)
                    except (DependencyCustodyConflict, protocol.ProtocolError) as exc:
                        return self._cancel_reply(
                            request, protocol.LeaseExecutionState.ABANDONED,
                            accepted=False, cancelled=False, released=False, error=str(exc),
                        )
                    original = registry.request(request.lease_id)
                if original is not None and (
                    original.task_id != request.task_id or original.attempt_id != request.attempt_id
                    or original.requester_node_id != request.requester_node_id
                    or original.requester_worker_id != request.requester_worker_id
                    or original.scheduling_key != request.scheduling_key
                ):
                    return self._cancel_reply(
                        request, protocol.LeaseExecutionState.ABANDONED,
                        accepted=False, cancelled=False, released=False,
                        error="cancellation identity conflicts with localization inventory",
                    )
                outcome = self._lease_outcomes.get(request.lease_id)
                record = self._leases.get(request.lease_id)
                if any(
                    original is not None and (
                        original.task_id != request.task_id
                        or original.attempt_id != request.attempt_id
                        or original.requester_node_id != request.requester_node_id
                        or original.requester_worker_id != request.requester_worker_id
                        or original.scheduling_key != request.scheduling_key
                    )
                    for original in (
                        None if outcome is None else outcome.request,
                        None if record is None else record.request,
                    )
                ):
                    return self._cancel_reply(
                        request, protocol.LeaseExecutionState.ABANDONED,
                        accepted=False, cancelled=False, released=False,
                        error="worker lease cancellation identity does not match request",
                    )

                released = False
                if record is not None:
                    if record.state is protocol.LeaseExecutionState.RUNNING:
                        return self._cancel_reply(
                            request, record.state, accepted=False,
                            cancelled=False, released=False,
                            error="running worker lease cannot be cancelled",
                        )
                    if record.state is protocol.LeaseExecutionState.GRANTED:
                        released = self._release_record_locked(
                            record, protocol.LeaseExecutionState.ABANDONED
                        )
                    elif record.state is not protocol.LeaseExecutionState.ABANDONED:
                        return self._cancel_reply(
                            request, record.state, accepted=False,
                            cancelled=False, released=False,
                            error="terminal worker lease cannot be cancelled",
                            retired_grant=(
                                record.grant
                                if record.state is protocol.LeaseExecutionState.WORKER_LOST
                                else None
                            ),
                            dependency_inventory=(registry.snapshot(request.lease_id)
                                if record.state is protocol.LeaseExecutionState.WORKER_LOST else None),
                        )

                # No record proves only that no grant committed. A partially
                # completed localization may still have sealed replicas; None
                # is not an absence proof for those pre-grant effects. This
                # tombstone fences any subsequently delivered lease request.
                reply = self._cancel_reply(
                    request, protocol.LeaseExecutionState.ABANDONED,
                    accepted=True, cancelled=True, released=released,
                    retired_grant=None if record is None else record.grant,
                    dependency_inventory=registry.snapshot(request.lease_id),
                )
                self._lease_cancellations[request.lease_id] = _LeaseCancellation(
                    request, reply
                )
                # The cached terminal projection must not share nested grant
                # objects with an in-process transport consumer.
                return replace(reply)

    @staticmethod
    def _cancel_reply(
        request: protocol.CancelWorkerLease,
        state: protocol.LeaseExecutionState,
        *,
        accepted: bool,
        cancelled: bool,
        released: bool,
        error: Optional[str] = None,
        retired_grant: Optional[protocol.GrantWorkerLease] = None,
        dependency_inventory: Optional[protocol.LeaseDependencyInventory] = None,
    ) -> protocol.CancelWorkerLeaseReply:
        return protocol.CancelWorkerLeaseReply(
            request.lease_id, request.task_id, request.attempt_id,
            request.requester_node_id, request.requester_worker_id, state,
            accepted, cancelled, released, error, request.scheduling_key,
            retired_grant, dependency_inventory,
        )

    def _dependency_custody_registry_locked(self) -> LeaseDependencyCustody:
        registry = getattr(self, "_lease_dependency_custody", None)
        if registry is None:
            registry = self._lease_dependency_custody = LeaseDependencyCustody(self.node_id)
        return registry

    def _drive_abandoned_dependency_custody(self, *, force=False) -> bool:
        """Hand a dead submitter's replicas to their real, unchanged owners.

        One lease and at most one replica per round use existing threads. A
        request lock freezes localization; the Node state lock arbitrates
        Start versus pre-execution abandonment. RUNNING workers keep their pins
        and resources until the normal execution authority finishes them.
        """
        with self._state_lock:
            registry = self._dependency_custody_registry_locked()
            requests = registry.abandoned_candidates()
            if not requests:
                return False
            now = time.monotonic()
            if not force and now < getattr(self, "_dependency_handoff_probe_after", 0.0):
                return False
            self._dependency_handoff_probe_after = now + 0.1
            # Rotate candidates so an alive submitter cannot starve a later
            # dead one. This is metadata progress, not task placement policy.
            index = getattr(self, "_dependency_handoff_cursor", 0) % len(requests)
            self._dependency_handoff_cursor = index + 1
            request = requests[index]
            tickets = getattr(self, "_dependency_handoff_drivers", None)
            if tickets is None:
                tickets = self._dependency_handoff_drivers = set()
            if request.lease_id in tickets:
                return False
            tickets.add(request.lease_id)
        try:
            with self._state_lock:
                deaths = getattr(self, "_dead_dependency_submitters", None)
                if deaths is None:
                    deaths = self._dead_dependency_submitters = {}
                death = deaths.get(request.requester_worker_id)
            if death is None:
                reply = self._get_output_child_death_proof(request.requester_worker_id)
                if reply is None:
                    return False
                death = reply.death
                with self._state_lock:
                    deaths[death.worker_id] = death
            with self._state_lock:
                request_lock = self._lease_request_locks.setdefault(request.lease_id, threading.Lock())
            if not request_lock.acquire(blocking=False):
                return False
            try:
                self._reconcile_localization_candidates(request.lease_id)
                with self._state_lock:
                    registry.abandon(request.lease_id, death)
                    record = self._leases.get(request.lease_id)
                    if record is not None and record.state is protocol.LeaseExecutionState.GRANTED:
                        self._release_record_locked(record, protocol.LeaseExecutionState.ABANDONED)
                    inventory = registry.snapshot(request.lease_id)
            finally:
                request_lock.release()
            routes = {route.object_id: route for route in request.dependency_owner_routes}
            with self._state_lock:
                descriptor = registry.next_abandoned_descriptor(inventory)
            if descriptor is not None:
                if descriptor.owner_worker_id == death.worker_id:
                    owner_death = death
                else:
                    owner_death = None
                route = routes.get(descriptor.object_id)
                receipt = None
                if owner_death is None and route is not None:
                    report = protocol.ReportAbandonedDependencyReplica(inventory, descriptor, death)
                    try:
                        candidate = self._background_rpc(route.owner_address, protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER, report,
                            connect_timeout=0.25, request_timeout=0.5, deadline=time.monotonic() + 0.75)
                        if type(candidate) is protocol.ReportAbandonedDependencyReplicaReply:
                            candidate = replace(candidate)
                            if candidate.request == report and candidate.custody_transferred:
                                receipt = candidate
                    except Exception:
                        pass
                if receipt is None and owner_death is None:
                    try:
                        reply = self._get_output_child_death_proof(descriptor.owner_worker_id)
                        owner_death = None if reply is None else reply.death
                    except Exception:
                        # One owner's unavailable death authority cannot
                        # prevent the following owners from receiving custody.
                        owner_death = None
                if receipt is None and owner_death is not None:
                    sweep = protocol.InstallOwnerDeathFence(
                        "abandoned-input:{}:{}:{}".format(request.lease_id, owner_death.worker_id, owner_death.detection_id),
                        owner_death, self.node_id, scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
                    )
                    try:
                        candidate = self._handle_install_owner_death_fence(sweep)
                        if candidate.complete:
                            receipt = candidate
                    except Exception:
                        pass
                if receipt is not None:
                    with self._state_lock:
                        registry.record_abandoned_receipt(inventory, descriptor, receipt)
            with self._state_lock:
                registry.complete_abandoned(inventory)
            return True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            # Missing routes, unknown ACKs and conflicting canonical metadata
            # retain this inventory and keep drain unclean; none authorizes Drop.
            return False
        finally:
            with self._state_lock:
                tickets.discard(request.lease_id)
            self._flush_pending_resource_report()

    def _retain_localization_seal_witness_locked(self, descriptor) -> None:
        request = _LOCALIZING_LEASE.get()
        if request is not None:
            witnesses = getattr(self, "_localization_seal_witnesses", None)
            if witnesses is None:
                witnesses = self._localization_seal_witnesses = {}
            witnesses[request.lease_id, descriptor.object_id] = descriptor

    def _reconcile_localization_candidates(self, lease_id) -> None:
        """Retry only witnessed local effects before returning Cancel inventory.

        The caller owns this LeaseID's request lock. Each candidate then uses
        the ordinary object/state locks, so this repair cannot race another
        transfer, overwrite a newer replica, or invent bytes from metadata.
        """
        with self._state_lock:
            registry = self._dependency_custody_registry_locked()
            request = registry.request(lease_id)
            candidates = registry.candidates(lease_id)
        for descriptor in candidates:
            with self._state_lock:
                lock = self._object_localization_locks.setdefault(descriptor.object_id, threading.Lock())
            with lock, self._state_lock:
                if not self._object_store.contains(descriptor.object_id, sealed_only=True):
                    registry.discard_unsealed(request, descriptor)
                    getattr(self, "_localization_seal_witnesses", {}).pop((lease_id, descriptor.object_id), None)
                    continue
                snapshot = self._object_store.snapshot(descriptor.object_id)
                data = self._object_store.get(descriptor.object_id)
                if (not snapshot.sealed or snapshot.size_bytes != descriptor.size_bytes
                        or len(data) != descriptor.size_bytes
                        or hashlib.sha256(data).hexdigest() != descriptor.checksum):
                    continue
                expected = (descriptor.producer_attempt_id, descriptor.owner_worker_id, descriptor.size_bytes, descriptor.checksum)
                metadata = self._sealed_metadata.get(descriptor.object_id)
                witness = getattr(self, "_localization_seal_witnesses", {}).get((lease_id, descriptor.object_id))
                if metadata is None and witness == descriptor:
                    self._require_dependency_not_deleted_locked(descriptor)
                    self._sealed_metadata[descriptor.object_id] = expected
                    metadata = expected
                if metadata == expected:
                    registry.record(request, descriptor)
                    getattr(self, "_localization_seal_witnesses", {}).pop((lease_id, descriptor.object_id), None)

    def _handle_ack_lease_dependency_custody(self, request: object) -> object:
        if type(request) is not protocol.AckLeaseDependencyCustody:
            raise TypeError("ack_lease_dependency_custody expects an exact custody acknowledgement")
        request = replace(request)
        lease_id = request.inventory.lease_request.lease_id
        with self._state_lock:
            lock = self._lease_request_locks.setdefault(lease_id, threading.Lock())
        # Match Cancel/Grant ordering: an ACK cannot seal the inventory while
        # this request is still localizing a later dependency. No owner RPC here.
        with lock, self._state_lock:
            try:
                if (lease_id not in self._leases
                        and lease_id not in getattr(self, "_lease_cancellations", {})):
                    raise DependencyCustodyConflict("inventory is not sealed by a grant or cancellation")
                self._dependency_custody_registry_locked().acknowledge(request.inventory)
            except (DependencyCustodyConflict, protocol.ProtocolError) as exc:
                return protocol.AckLeaseDependencyCustodyReply(request, False, str(exc))
            return protocol.AckLeaseDependencyCustodyReply(request, True)

    def _handle_request_lease_serialized(
        self, request: protocol.RequestWorkerLease
    ) -> object:
        lease_id = request.lease_id
        target_node_id = request.target_node_id
        scheduling_key = request.scheduling_key

        # Shutdown and target fencing precede idempotency: a cached grant must
        # not resurrect a worker that is already stopping, and a targeted
        # request delivered to the wrong node must never consume resources.
        with self._state_lock:
            if self._stop_event.is_set() or self._shutdown_request_id is not None:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.SHUTTING_DOWN,
                    "node is shutting down",
                )
            if request.requester_worker_id in getattr(self, "_dead_dependency_submitters", {}):
                return self._rejected(request, protocol.LeaseRejectReason.STALE_ATTEMPT, "dependency submitter is confirmed dead")
        if target_node_id is not None and target_node_id != self.node_id:
            return self._rejected(
                request,
                protocol.LeaseRejectReason.WRONG_TARGET,
                "lease targets node {}, not {}".format(
                    target_node_id, self.node_id
                ),
            )

        if scheduling_key is not None:
            if scheduling_key.node_id != self.node_id:
                return self._rejected(
                    request, protocol.LeaseRejectReason.WRONG_TARGET,
                    "placement-group lease targets another node",
                )
            # Placement has already been decided by the PG coordinator.  A PG
            # task must never enter ordinary hybrid scheduling or spill back.
        elif target_node_id is None and self._gcs_address is not None:
            # Serialize policy RNG use and the snapshot/decision pair.  The GCS
            # view may already be stale when this returns; the selected node's
            # ledger performs the final validation on the targeted retry.
            with self._scheduling_lock:
                try:
                    snapshots, addresses = self._get_cluster_nodes()
                except Exception as exc:
                    return self._rejected(
                        request,
                        protocol.LeaseRejectReason.PENDING_CAPACITY,
                        "cannot read the installed local scheduling snapshot: {}"
                        .format(exc),
                    )
                decision = self._scheduling_policy.schedule(
                    request.resources,
                    snapshots,
                    preferred_node_id=(
                        request.preferred_node_id or self.node_id
                    ),
                    require_available=True,
                )
            if decision.status is resources.SchedulingStatus.INFEASIBLE:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.INFEASIBLE,
                    "no live node has the requested total resources",
                )
            if decision.status is resources.SchedulingStatus.PENDING_CAPACITY:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.PENDING_CAPACITY,
                    "feasible nodes exist but their reported capacity is busy",
                )
            selected_node_id = decision.node_id
            if selected_node_id is None:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.PENDING_CAPACITY,
                    "hybrid scheduling returned no target node",
                )
            if selected_node_id != self.node_id:
                spillback = protocol.SpillbackWorkerLease(
                    lease_id=request.lease_id,
                    task_id=request.task_id,
                    attempt_id=request.attempt_id,
                    target_node_id=selected_node_id,
                    target_address=addresses.get(selected_node_id),
                    reason="hybrid policy selected a different node",

                )
                with self._state_lock:
                    if (
                        self._stop_event.is_set()
                        or self._shutdown_request_id is not None
                    ):
                        return self._rejected(
                            request,
                            protocol.LeaseRejectReason.SHUTTING_DOWN,
                            "node began shutting down during scheduling",
                        )
                return spillback

        localization = _LOCALIZING_LEASE.set(request)
        try:
            localized_dependencies = self._localize_dependencies(
                request.dependencies
            )
        except Exception as exc:
            return self._rejected(
                request,
                protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE,
                "dependency localization failed: {}: {}".format(
                    type(exc).__name__, exc
                ),
            )
        finally:
            _LOCALIZING_LEASE.reset(localization)

        with self._state_lock:
            if self._stop_event.is_set() or self._shutdown_request_id is not None:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.SHUTTING_DOWN,
                    "node is shutting down",
                )
            if request.requester_worker_id in getattr(self, "_dead_dependency_submitters", {}):
                return self._rejected(request, protocol.LeaseRejectReason.STALE_ATTEMPT, "dependency submitter died during localization")
            previous = self._leases.get(lease_id)
            if previous is not None:
                if previous.request != request:
                    return self._rejected(
                        request,
                        protocol.LeaseRejectReason.STALE_ATTEMPT,
                        "lease ID reused with a different request",
                    )
                if previous.state is not protocol.LeaseExecutionState.GRANTED:
                    return self._rejected(
                        request,
                        protocol.LeaseRejectReason.STALE_ATTEMPT,
                        "lease is no longer grantable",
                    )
                return previous.grant

            allocation_ledger = self._ledger
            if scheduling_key is not None:
                self._ensure_bundle_reservations_locked()
                digest = self._placement_group_digests.get(
                    (
                        scheduling_key.placement_group_id,
                        scheduling_key.attempt,
                    )
                )
                if digest != scheduling_key.plan_digest:
                    return self._rejected(
                        request, protocol.LeaseRejectReason.STALE_ATTEMPT,
                        "placement-group scheduling key is not committed here",
                    )
                try:
                    allocation_ledger = self._bundle_reservations.ledger_for(
                        scheduling_key.placement_group_id,
                        scheduling_key.attempt,
                        scheduling_key.bundle_index,
                    )
                except (KeyError, RuntimeError):
                    # COMMITTED is required.  REMOVING deliberately reaches this
                    # same fence so no new child lease can race root release.
                    return self._rejected(
                        request, protocol.LeaseRejectReason.STALE_ATTEMPT,
                        "placement-group bundle is not accepting new leases",
                    )

            if not request.resources.fits_in(allocation_ledger.total):
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.INFEASIBLE,
                    "requested resources exceed this node's total capacity",
                )
            slot = self._idle_worker_slot_locked()
            if slot is None:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.PENDING_CAPACITY,
                    "all ordinary Workers are busy or unavailable",
                )
            token = resources.AllocationToken.random()
            allocated = allocation_ledger.try_allocate(request.resources, token)
            if allocated is None:
                return self._rejected(
                    request,
                    protocol.LeaseRejectReason.PENDING_CAPACITY,
                    "requested resources are temporarily unavailable",
                )
            dependency_pins: list[tuple[ids.ObjectID, object]] = []
            committed = False
            try:
                for descriptor in localized_dependencies:
                    owner_fence = getattr(
                        self, "_owner_death_fences", {}
                    ).get(descriptor.owner_worker_id)
                    if owner_fence is not None:
                        raise RuntimeError(
                            "target dependency owner is fenced by Worker "
                            "death {}".format(owner_fence.detection_id)
                        )
                    self._require_dependency_not_deleted_locked(descriptor)
                    metadata = self._sealed_metadata.get(descriptor.object_id)
                    if (
                        metadata is None
                        or not self._descriptor_matches_source(
                            descriptor, node_id=self.node_id, metadata=metadata
                        )
                    ):
                        raise RuntimeError(
                            "target dependency changed producer epoch before pin"
                        )
                    pin_token = (
                        "lease-dependency", lease_id, descriptor.object_id,
                        descriptor.producer_attempt_id,
                    )
                    self._object_store.pin(descriptor.object_id, pin_token)
                    dependency_pins.append((descriptor.object_id, pin_token))
                grant = protocol.GrantWorkerLease(
                    lease_id=lease_id,
                    task_id=request.task_id,
                    attempt_id=request.attempt_id,
                    node_id=self.node_id,
                    worker_id=slot.worker_id,
                    worker_address=slot.address,
                    allocation_token=allocated,
                    dependencies=localized_dependencies,
                    scheduling_key=scheduling_key,

                )
                record = _LeaseRecord(
                    request=request,
                    allocation_token=allocated,
                    grant=grant,
                    allocation_ledger=allocation_ledger,
                    dependency_pins=tuple(dependency_pins),
                )
                if slot.active_lease_id is not None:
                    raise RuntimeError(
                        "selected Worker became busy before lease commit"
                    )
                self._leases[lease_id] = record
                slot.active_lease_id = lease_id
                # This is the publication linearization point.  Shutdown and
                # worker-loss cleanup use the same state lock, so no caller can
                # observe a grant whose pins were already terminally released.
                self._lease_outcomes[lease_id] = _LeaseOutcome(request, grant)
                self._refresh_local_cached_availability_locked()
                if allocation_ledger is self._ledger:
                    self._mark_resource_report_pending_locked()
                committed = True
            except Exception as exc:
                # Commit is all-or-nothing: no exception before ``committed``
                # may leave resources, target pins, a lease record, or a slot
                # binding behind.  _handle_request_lease caches only this typed
                # rejection, never a half-built grant.
                if not committed:
                    self._lease_outcomes.pop(lease_id, None)
                    self._leases.pop(lease_id, None)
                    if slot.active_lease_id == lease_id:
                        slot.active_lease_id = None
                    for object_id, pin_token in reversed(dependency_pins):
                        self._unpin_dependency_or_retain_cleanup_locked(
                            object_id, pin_token
                        )
                    allocation_ledger.release(allocated)
                    self._refresh_local_cached_availability_locked()
                return self._rejected(
                    request, protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE,
                    "target dependency grant transaction failed: {}: {}".format(
                        type(exc).__name__, exc
                    ),
                )
            self._emit(
                "lease_granted",
                lease_id=str(lease_id),
                task_id=str(request.task_id),
                attempt_id=str(request.attempt_id),
                node_id=str(self.node_id),
                worker_id=str(slot.worker_id),
            )
        return grant

    def _require_dependency_not_deleted_locked(
        self, descriptor: protocol.ObjectStoreDescriptor
    ) -> None:
        """Fence dependency custody with this Node's committed drop watermark.

        A surviving source may still advertise the old bytes after this Node
        deleted its replica. The per-object localization/drop lock serializes
        those effects; callers also hold ``_state_lock`` when reading this
        watermark at pull admission, final seal and lease pin publication.
        This guard never deletes data or interprets a rejected drop as success.
        """
        tombstone = getattr(self, "_dropped_metadata", {}).get(descriptor.object_id)
        if (tombstone is not None
                and descriptor.producer_attempt_id.attempt_number
                <= tombstone[0].attempt_number):
            raise RuntimeError(
                "dependency producer attempt was fenced by replica deletion"
            )

    def _localize_dependencies(
        self, dependencies: tuple[protocol.ObjectStoreDescriptor, ...]
    ) -> tuple[protocol.ObjectStoreDescriptor, ...]:
        """Seal every dependency locally before the lease can allocate resources.

        All network calls occur outside ``_state_lock``.  The source pin is
        released in ``finally`` even when a chunk or checksum validation fails.
        """

        localized = []
        for descriptor in dependencies:
            localized.append(self._localize_one_dependency(descriptor))
        return tuple(localized)

    def _localize_one_dependency(
        self, descriptor: protocol.ObjectStoreDescriptor
    ) -> protocol.ObjectStoreDescriptor:
        with self._state_lock:
            object_lock = self._object_localization_locks.setdefault(
                descriptor.object_id, threading.Lock()
            )
        # Acquire only one object lock at a time so dependency requests in
        # opposite orders cannot deadlock.  Owner-death scans acquire a sorted
        # set of the same locks, so pull sealing and witness capture linearize.
        with object_lock:
            request = _LOCALIZING_LEASE.get()
            local = replace(descriptor, node_id=self.node_id)
            with self._state_lock:
                previous = self._sealed_metadata.get(descriptor.object_id)
                present_before = self._object_store.contains(descriptor.object_id, sealed_only=False)
                if request is not None:
                    self._dependency_custody_registry_locked().begin(request, local)
            try:
                result = self._localize_one_dependency_locked(descriptor)
                return result
            except Exception:
                with self._state_lock:
                    metadata = self._sealed_metadata.get(descriptor.object_id)
                    sealed = self._object_store.contains(descriptor.object_id, sealed_only=True)
                    exact = metadata is not None and self._descriptor_matches_source(local, node_id=self.node_id, metadata=metadata)
                    if sealed and not present_before and (metadata is None or exact):
                        # finish_pull may have applied before metadata dict
                        # assignment raised. The object lock plus the frozen
                        # operation proves this was our transfer, not a shared
                        # newer replica. Validate its bytes before repairing.
                        payload = self._object_store.get(descriptor.object_id)
                        pull = self._object_manager.snapshot(descriptor.object_id)
                        if (pull.attempt_id == descriptor.producer_attempt_id
                                and len(payload) == descriptor.size_bytes
                                and hashlib.sha256(payload).hexdigest() == descriptor.checksum):
                            self._retain_localization_seal_witness_locked(local)
                            self._sealed_metadata[descriptor.object_id] = (
                                descriptor.producer_attempt_id, descriptor.owner_worker_id,
                                descriptor.size_bytes, descriptor.checksum,
                            )
                            exact = True
                    if sealed and exact:
                        self._record_localized_dependency_locked(local)
                    elif request is not None and (not sealed or present_before and previous != (
                            descriptor.producer_attempt_id, descriptor.owner_worker_id, descriptor.size_bytes, descriptor.checksum)):
                        self._dependency_custody_registry_locked().discard_unsealed(request, local)
                    # Otherwise retain the candidate as explicitly unresolved;
                    # neither Cancel nor drain may pretend the bytes are absent.
                raise

    def _localize_one_dependency_locked(
        self, descriptor: protocol.ObjectStoreDescriptor
    ) -> protocol.ObjectStoreDescriptor:
            local_descriptor = protocol.ObjectStoreDescriptor(
                descriptor.object_id,
                descriptor.owner_worker_id,
                descriptor.producer_attempt_id,
                self.node_id,
                descriptor.size_bytes,
                descriptor.checksum,
            )
            with self._state_lock:
                owner_fence = getattr(
                    self, "_owner_death_fences", {}
                ).get(descriptor.owner_worker_id)
                if owner_fence is not None:
                    raise RuntimeError(
                        "dependency owner is fenced by Worker death {}".format(
                            owner_fence.detection_id
                        )
                    )
                self._require_dependency_not_deleted_locked(descriptor)
                if descriptor.object_id in getattr(self, "_local_replica_write_claims", {}):
                    raise RuntimeError("local dependency replica has unfinished publication custody")
                metadata = self._sealed_metadata.get(descriptor.object_id)
                local_present = self._object_store.contains(
                    descriptor.object_id, sealed_only=False
                )
                local_ready = metadata is not None and self._descriptor_matches_source(
                    local_descriptor, node_id=self.node_id, metadata=metadata
                )
                source_address = self._cluster_addresses.get(descriptor.node_id)
            if local_ready:
                # ObjectManager validates physical size/checksum as part of the
                # fast path, catching stale metadata before granting the lease.
                decision = self._object_manager.request_pull(
                    descriptor.object_id,
                    locations=(self.node_id,),
                    waiter_token=("lease-local", descriptor.object_id),
                    expected_size=descriptor.size_bytes,
                    expected_checksum=descriptor.checksum,
                    attempt_id=descriptor.producer_attempt_id,
                )
                if decision.action is not PullAction.LOCAL_READY:
                    raise RuntimeError("local dependency replica is not ready")
                with self._state_lock:
                    self._record_localized_dependency_locked(local_descriptor)
                return local_descriptor
            if local_present or metadata is not None:
                # ObjectID is stable across reconstruction attempts.  A sealed
                # local replica with another owner/epoch must never satisfy an
                # old descriptor merely because its bytes have the same size and
                # checksum; byte equality is not logical identity.  Metadata
                # retained after deletion also fences a newer pull until its
                # ObjectManager cleanup completes, even with no bytes present.
                raise RuntimeError(
                    "local dependency replica conflicts with producer metadata"
                )
            if descriptor.node_id == self.node_id:
                raise RuntimeError("advertised local dependency replica is unavailable")
            if source_address is None:
                raise RuntimeError("dependency source node is absent from cluster snapshot")

            decision = self._object_manager.request_pull(
                descriptor.object_id,
                locations=(descriptor.node_id,),
                waiter_token=("lease-pull", descriptor.object_id),
                expected_size=descriptor.size_bytes,
                expected_checksum=descriptor.checksum,
                attempt_id=descriptor.producer_attempt_id,
            )
            if decision.action is PullAction.LOCAL_READY:
                with self._state_lock:
                    self._record_localized_dependency_locked(local_descriptor)
                return local_descriptor
            if decision.action is PullAction.FAILED:
                self._object_manager.reset_failed_pull(descriptor.object_id)
                decision = self._object_manager.request_pull(
                    descriptor.object_id,
                    locations=(descriptor.node_id,),
                    waiter_token=("lease-retry", descriptor.object_id),
                    expected_size=descriptor.size_bytes,
                    expected_checksum=descriptor.checksum,
                    attempt_id=descriptor.producer_attempt_id,
                )
            if decision.action is not PullAction.START_PULL or decision.transfer_id is None:
                raise RuntimeError("dependency pull did not become transferable")
            transfer_id = decision.transfer_id
            pin_request = protocol.PinObjectForTransfer(
                transfer_id, descriptor, self.node_id
            )
            with self._state_lock:
                if descriptor.node_id in getattr(self, "_transfer_node_deaths", {}):
                    self._object_manager.fail_pull(descriptor.object_id, "source Node is confirmed dead")
                    raise RuntimeError("source Node is confirmed dead")
                self._source_pin_outbox_locked().begin(source_address, pin_request)
            try:
                pin_reply = rpc_request(
                    source_address, PIN_OBJECT_HANDLER, pin_request
                )
                if type(pin_reply) is protocol.PinObjectForTransferReply:
                    pin_reply = replace(pin_reply)
                if (
                    not isinstance(pin_reply, protocol.PinObjectForTransferReply)
                    or pin_reply.transfer_id != transfer_id
                    or pin_reply.descriptor != descriptor
                    or not pin_reply.pinned
                ):
                    raise RuntimeError(
                        getattr(pin_reply, "error", None)
                        or "source rejected dependency pin"
                    )
                offset = 0
                while offset < descriptor.size_bytes:
                    size = min(
                        OBJECT_TRANSFER_CHUNK_BYTES, descriptor.size_bytes - offset
                    )
                    chunk_reply = rpc_request(
                        source_address,
                        GET_OBJECT_CHUNK_HANDLER,
                        protocol.GetObjectChunk(
                            transfer_id,
                            descriptor.object_id,
                            self.node_id,
                            offset,
                            size,
                        ),
                    )
                    if (
                        not isinstance(chunk_reply, protocol.GetObjectChunkReply)
                        or not chunk_reply.ok
                        or chunk_reply.transfer_id != transfer_id
                        or chunk_reply.object_id != descriptor.object_id
                        or chunk_reply.node_id != descriptor.node_id
                        or chunk_reply.offset != offset
                        or len(chunk_reply.data) != size
                    ):
                        raise RuntimeError(
                            getattr(chunk_reply, "error", None)
                            or "source returned an invalid dependency chunk"
                        )
                    self._object_manager.receive_chunk(
                        descriptor.object_id,
                        chunk_reply.data,
                        offset=offset,
                        transfer_id=transfer_id,
                        source_location=descriptor.node_id,
                    )
                    offset += size
                with self._state_lock:
                    owner_fence = getattr(
                        self, "_owner_death_fences", {}
                    ).get(descriptor.owner_worker_id)
                    if owner_fence is not None:
                        raise RuntimeError(
                            "dependency owner is fenced by Worker death {}"
                            .format(owner_fence.detection_id)
                        )
                    self._require_dependency_not_deleted_locked(descriptor)
                    # Seal and metadata publication share the same state-lock
                    # boundary as global owner-fence installation.  A fence
                    # therefore either observes the complete replica or wins
                    # first and leaves no target replica publishable.
                    self._object_manager.finish_pull(
                        descriptor.object_id,
                        transfer_id=transfer_id,
                        source_location=descriptor.node_id,
                    )
                    self._retain_localization_seal_witness_locked(local_descriptor)
                    self._sealed_metadata[descriptor.object_id] = (
                        descriptor.producer_attempt_id,
                        descriptor.owner_worker_id,
                        descriptor.size_bytes,
                        descriptor.checksum,
                    )
                    # Record before source-pin release in finally: an unknown
                    # remote release ACK must not erase already-sealed custody.
                    self._record_localized_dependency_locked(local_descriptor)
                return local_descriptor
            except Exception as exc:
                self._object_manager.fail_pull(descriptor.object_id, exc)
                raise
            finally:
                # Pin ACK loss does not establish absence. Close the exact
                # session even if Pin never arrived; source tombstones prevent
                # a late request on another connection from resurrecting it.
                if not self._finish_source_pin_read(transfer_id):
                    raise RuntimeError("source pin release failed after 3 attempts; exact obligation retained")

    def _record_localized_dependency_locked(self, descriptor: protocol.ObjectStoreDescriptor) -> None:
        """Witness exact bytes while the caller owns object and Node locks.

        LOCAL_READY is not enough by itself: byte equality cannot establish
        ownership or producer identity. This method also records reused replicas
        so a failed first consumer cannot forget a second consumer's custody.
        """
        self._require_dependency_not_deleted_locked(descriptor)
        metadata = self._sealed_metadata.get(descriptor.object_id)
        if metadata is None or not self._descriptor_matches_source(descriptor, node_id=self.node_id, metadata=metadata):
            raise RuntimeError("localized dependency lacks exact owner metadata")
        snapshot = self._object_store.snapshot(descriptor.object_id)
        if not snapshot.sealed or snapshot.size_bytes != descriptor.size_bytes:
            raise RuntimeError("localized dependency lacks exact sealed bytes")
        request = _LOCALIZING_LEASE.get()
        if request is not None:
            self._dependency_custody_registry_locked().record(request, descriptor)
            getattr(self, "_localization_seal_witnesses", {}).pop((request.lease_id, descriptor.object_id), None)

    @staticmethod
    def _lease_identity_matches(
        record: _LeaseRecord,
        task_id: ids.TaskID,
        attempt_id: ids.AttemptID,
        worker_id: ids.WorkerID,
    ) -> bool:
        return (
            record.request.task_id == task_id
            and record.request.attempt_id == attempt_id
            and record.grant.worker_id == worker_id
        )

    def _handle_start_worker_lease(self, request: object) -> object:
        if not isinstance(request, protocol.StartWorkerLease):
            raise TypeError("start_worker_lease expects StartWorkerLease")
        with self._state_lock:
            record = self._leases.get(request.lease_id)
            if record is None:
                return protocol.StartWorkerLeaseReply(
                    request.lease_id,
                    protocol.LeaseExecutionState.ABANDONED,
                    accepted=False,
                    error="unknown worker lease",
                    scheduling_key=request.scheduling_key,

                )
            if (not self._lease_identity_matches(
                record, request.task_id, request.attempt_id, request.worker_id
            )) or ((request.scheduling_key != record.request.scheduling_key)):
                return protocol.StartWorkerLeaseReply(
                    request.lease_id,
                    record.state,
                    accepted=False,
                    error="worker lease execution identity does not match grant",
                    scheduling_key=request.scheduling_key,

                )
            if record.state is protocol.LeaseExecutionState.GRANTED:
                if record.request.requester_worker_id in getattr(self, "_dead_dependency_submitters", {}):
                    self._release_record_locked(record, protocol.LeaseExecutionState.ABANDONED)
                    return protocol.StartWorkerLeaseReply(
                        request.lease_id, record.state, False, "dependency submitter is confirmed dead",
                        scheduling_key=request.scheduling_key,
                    )
                record.state = protocol.LeaseExecutionState.RUNNING
                self._emit(
                    "lease_running",
                    lease_id=str(request.lease_id),
                    task_id=str(request.task_id),
                    attempt_id=str(request.attempt_id),
                    node_id=str(self.node_id),
                )
            if record.state is protocol.LeaseExecutionState.RUNNING:
                from .output_publication import OutputPublicationNodeIncarnation

                # Public runtimes register the Node before spawning a Worker.
                # Narrow standalone reducer fixtures may have no registration;
                # keep that absence explicit and let output discovery fail
                # closed instead of fabricating a PID or epoch.
                incarnation = (
                    OutputPublicationNodeIncarnation(
                        self.node_id, self._node_pid, self._registration_epoch
                    )
                    if getattr(self, "_registered_with_gcs", False)
                    else None
                )
                return protocol.StartWorkerLeaseReply(
                    request.lease_id, record.state, accepted=True,
                    scheduling_key=request.scheduling_key,

                    node_incarnation=incarnation,
                )
            return protocol.StartWorkerLeaseReply(
                request.lease_id,
                record.state,
                accepted=False,
                error="worker lease cannot start from {}".format(record.state.value),
                scheduling_key=request.scheduling_key,

            )

    def _release_record_locked(
        self, record: _LeaseRecord, state: protocol.LeaseExecutionState
    ) -> bool:
        self._retry_dependency_pin_cleanups(force=True)
        allocation_ledger = record.allocation_ledger or self._ledger
        released = allocation_ledger.release(record.allocation_token)
        if released and allocation_ledger is self._ledger:
            self._mark_resource_report_pending_locked()
        for object_id, pin_token in record.dependency_pins:
            # Resource release may proceed, but a failed physical unpin remains
            # an explicit Node-cleanliness obligation until exact replay works.
            self._unpin_dependency_or_retain_cleanup_locked(
                object_id, pin_token
            )
        record.dependency_pins = ()
        record.state = state
        # ``ResourceLedger.release`` understands ACTIVE versus CPU_YIELDED, so
        # this single finalizer is correct for completion, abandonment, and
        # worker loss.  Close the logical episode only after that mutation.
        record.blocking_open = False
        record.cpu_yielded = False
        slot = self._worker_slot_locked(record.grant.worker_id)
        if slot is None:
            raise AssertionError("lease grant names an unknown ordinary Worker")
        if slot.active_lease_id == record.request.lease_id:
            slot.active_lease_id = None
        self._finalize_removing_placement_groups_locked()
        self._refresh_local_cached_availability_locked()
        return released

    def _finalize_removing_placement_groups_locked(self) -> int:
        """Release root PG reservations whose child leases are all terminal."""

        reservations = getattr(self, "_bundle_reservations", None)
        if reservations is None:
            return 0
        finalized = 0
        for placement_group_id, attempt in tuple(
            getattr(self, "_placement_group_digests", {})
        ):
            snapshot = reservations.snapshot(placement_group_id, attempt)
            if snapshot is None or snapshot.state is not ReservationState.REMOVING:
                continue
            if reservations.finalize_remove(placement_group_id, attempt):
                finalized += 1
        if finalized:
            self._mark_resource_report_pending_locked()
        return finalized

    def _begin_remove_all_placement_groups_locked(self) -> None:
        """Fence all PG child ledgers and release only already-idle roots."""

        reservations = getattr(self, "_bundle_reservations", None)
        if reservations is None:
            return
        available_before = self._ledger.available
        for placement_group_id, attempt in tuple(
            getattr(self, "_placement_group_digests", {})
        ):
            reservations.begin_remove(placement_group_id, attempt)
        self._finalize_removing_placement_groups_locked()
        if self._ledger.available != available_before:
            self._mark_resource_report_pending_locked()

    def _handle_notify_worker_blocked(self, request: object) -> object:
        """Yield only CPU for one fenced blocking episode.

        Every transition shares ``_state_lock`` with completion and worker-loss
        cleanup.  Consequently a notification either mutates a live RUNNING
        lease or observes its terminal state; it can never change resources
        after terminal cleanup.
        """

        if not isinstance(request, protocol.NotifyWorkerBlocked):
            raise TypeError(
                "notify_worker_blocked expects NotifyWorkerBlocked"
            )
        with self._state_lock:
            record = self._leases.get(request.lease_id)
            rejection = self._blocking_notification_rejection_locked(
                request, record, operation="block"
            )
            if rejection is not None:
                return rejection
            assert record is not None

            sequence = request.sequence
            if sequence < record.blocking_sequence:
                return self._blocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="blocking episode sequence is stale",
                )
            if sequence == record.blocking_sequence:
                if record.blocking_open:
                    return self._blocked_reply(
                        request, record.state, accepted=True, changed=False
                    )
                # An Unblocked tombstone, or a completed episode, wins over a
                # delayed Blocked carrying the same sequence.
                return self._blocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="blocking episode is already closed",
                )
            if sequence != record.blocking_sequence + 1:
                return self._blocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="blocking episode sequence skipped an episode",
                )
            if record.blocking_open:
                return self._blocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="the preceding blocking episode is still open",
                )

            cpu_expected = record.request.resources.units(resources.CPU) > 0
            allocation_ledger = record.allocation_ledger or self._ledger
            cpu_yielded = allocation_ledger.yield_cpu(record.allocation_token)
            if cpu_yielded != cpu_expected:
                raise AssertionError(
                    "running lease CPU allocation disagrees with its request"
                )
            record.blocking_sequence = sequence
            record.blocking_open = True
            record.cpu_yielded = cpu_yielded
            self._refresh_local_cached_availability_locked()
            return self._blocked_reply(
                request, record.state, accepted=True, changed=True
            )

    def _handle_notify_worker_unblocked(self, request: object) -> object:
        """Close one episode and restore CPU immediately, allowing debt."""

        if not isinstance(request, protocol.NotifyWorkerUnblocked):
            raise TypeError(
                "notify_worker_unblocked expects NotifyWorkerUnblocked"
            )
        with self._state_lock:
            record = self._leases.get(request.lease_id)
            rejection = self._blocking_notification_rejection_locked(
                request, record, operation="unblock"
            )
            if rejection is not None:
                return rejection
            assert record is not None

            sequence = request.sequence
            if sequence < record.blocking_sequence:
                return self._unblocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="blocking episode sequence is stale",
                )
            if sequence == record.blocking_sequence:
                if not record.blocking_open:
                    return self._unblocked_reply(
                        request, record.state, accepted=True, changed=False
                    )
                allocation_ledger = record.allocation_ledger or self._ledger
                if record.cpu_yielded and not allocation_ledger.reacquire_cpu(
                    record.allocation_token
                ):
                    raise AssertionError(
                        "open blocking episode could not reacquire its CPU"
                    )
                record.blocking_open = False
                record.cpu_yielded = False
                self._refresh_local_cached_availability_locked()
                return self._unblocked_reply(
                    request, record.state, accepted=True, changed=True
                )
            if sequence != record.blocking_sequence + 1:
                return self._unblocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="blocking episode sequence skipped an episode",
                )
            if record.blocking_open:
                return self._unblocked_reply(
                    request, record.state, accepted=False, changed=False,
                    error="unblock does not match the open blocking episode",
                )

            # Close-before-open is a tombstone for an ambiguous Blocked RPC.
            # If that older request arrives later, the equal-sequence branch in
            # the Blocked handler rejects it without changing resources.
            record.blocking_sequence = sequence
            record.blocking_open = False
            record.cpu_yielded = False
            return self._unblocked_reply(
                request, record.state, accepted=True, changed=True
            )

    def _blocking_notification_rejection_locked(
        self, request: object, record: Optional[_LeaseRecord], *, operation: str
    ) -> Optional[object]:
        """Return a typed rejection for unknown, wrong, or terminal leases."""

        if record is None:
            return self._blocking_reply_for(
                request, protocol.LeaseExecutionState.ABANDONED,
                accepted=False, changed=False, error="unknown worker lease",
            )
        if not self._lease_identity_matches(
            record, request.task_id, request.attempt_id, request.worker_id
        ):
            return self._blocking_reply_for(
                request, record.state, accepted=False, changed=False,
                error="worker lease {} identity does not match grant".format(
                    operation
                ),
            )
        if record.state is not protocol.LeaseExecutionState.RUNNING:
            return self._blocking_reply_for(
                request, record.state, accepted=False, changed=False,
                error="worker lease cannot {} from {}".format(
                    operation, record.state.value
                ),
            )
        return None

    @staticmethod
    def _blocking_reply_for(
        request: object, state: protocol.LeaseExecutionState, *,
        accepted: bool, changed: bool, error: Optional[str] = None,
    ) -> object:
        values = (
            request.lease_id, request.task_id, request.attempt_id,
            request.worker_id, request.sequence, state, accepted, changed, error,
        )
        if isinstance(request, protocol.NotifyWorkerBlocked):
            return protocol.NotifyWorkerBlockedReply(*values)
        assert isinstance(request, protocol.NotifyWorkerUnblocked)
        return protocol.NotifyWorkerUnblockedReply(*values)

    @staticmethod
    def _blocked_reply(
        request: protocol.NotifyWorkerBlocked,
        state: protocol.LeaseExecutionState, *, accepted: bool, changed: bool,
        error: Optional[str] = None,
    ) -> protocol.NotifyWorkerBlockedReply:
        return protocol.NotifyWorkerBlockedReply(
            request.lease_id, request.task_id, request.attempt_id,
            request.worker_id, request.sequence, state, accepted, changed, error,
        )

    @staticmethod
    def _unblocked_reply(
        request: protocol.NotifyWorkerUnblocked,
        state: protocol.LeaseExecutionState, *, accepted: bool, changed: bool,
        error: Optional[str] = None,
    ) -> protocol.NotifyWorkerUnblockedReply:
        return protocol.NotifyWorkerUnblockedReply(
            request.lease_id, request.task_id, request.attempt_id,
            request.worker_id, request.sequence, state, accepted, changed, error,
        )

    def _reclaim_active_lease_after_worker_exit_locked(
        self, worker_id: Optional[ids.WorkerID] = None
    ) -> bool:
        """Fence and reclaim an allocation only after the Worker has exited.

        Completion and shutdown both hold ``_state_lock`` while transitioning
        the lease and releasing its allocation.  Whichever transition wins is
        therefore the only one that can call the ledger release operation.
        """

        selected_worker_id = worker_id or self._worker_order[0]
        slot = self._workers.get(selected_worker_id)
        if slot is None:
            return False
        lease_id = slot.active_lease_id
        if lease_id is None:
            return False
        record = self._leases.get(lease_id)
        if record is None:
            raise AssertionError("active lease has no lease record")
        if record.state in (
            protocol.LeaseExecutionState.GRANTED,
            protocol.LeaseExecutionState.RUNNING,
        ):
            return self._release_record_locked(
                record, protocol.LeaseExecutionState.WORKER_LOST
            )
        if record.state in (
            protocol.LeaseExecutionState.COMPLETED,
            protocol.LeaseExecutionState.ABANDONED,
            protocol.LeaseExecutionState.WORKER_LOST,
        ):
            slot.active_lease_id = None
            return False
        raise AssertionError("unknown lease execution state: {!r}".format(record.state))

    @staticmethod
    def _completion_reply(
        request: protocol.CompleteWorkerLease,
        state: protocol.LeaseExecutionState,
        *,
        accepted: bool,
        released: bool,
        error: Optional[str] = None,
        output_publication: Optional[OutputPublicationEnvelope] = None,
        output_completion: Optional[OutputPublicationCompleteWitness] = None,
    ) -> protocol.CompleteWorkerLeaseReply:
        """Echo the complete logical execution identity in every reply."""

        return protocol.CompleteWorkerLeaseReply(
            request.lease_id,
            request.task_id,
            request.attempt_id,
            request.worker_id,
            request.status,
            state,
            accepted,
            released,
            error,
            scheduling_key=request.scheduling_key,

            output_publication=output_publication,
            output_completion=output_completion,
        )

    def _handle_complete_worker_lease(self, request: object) -> object:
        try:
            reply = self._handle_complete_worker_lease_inner(request)
            try:
                if (type(reply) is protocol.CompleteWorkerLeaseReply
                        and reply.accepted is True
                        and reply.status is protocol.TaskReplyStatus.SUCCEEDED
                        and reply.state is protocol.LeaseExecutionState.COMPLETED
                        and type(reply.output_publication) is OutputPublicationEnvelope):
                    envelope = reply.output_publication
                    identity = envelope.publication_id
                    # The inner handler has released its locks after the real
                    # local Complete/resource transition. No GCS ACK is awaited
                    # by this observation; a replay may report released=False.
                    self._emit("output_lease_completed",
                               task_id=str(identity.task_id),
                               attempt_id=str(identity.attempt_id),
                               lease_id=str(identity.lease_id),
                               manifest_digest=envelope.manifest.manifest_digest,
                               released=reply.released, status=reply.status.value,
                               state=reply.state.value)
            except BaseException:
                pass
            self._test_output_result_delivery_checkpoint(reply)
            return reply
        finally:
            with self._state_lock:
                record = self._leases.get(getattr(request, "lease_id", None))
                output_bound = (
                    record is not None and record.output_publication_id is not None
                )
            if not output_bound:
                self._flush_pending_resource_report()
            # Output Complete is a local result hand-off, so even the ordinary
            # availability report must not delay its reply on GCS I/O.  Local
            # release marked the report pending; the Worker supervisor and
            # drain driver flush it independently.

    def _test_output_publication_checkpoint(self, manifest, phase,
            graph_outcome=GraphReservationOutcome.UNOBSERVED) -> None:
        """Observe one acknowledged preparation phase outside authority locks."""
        gate = getattr(self, "_output_publication_gate", None)
        if gate is None:
            return
        identity = manifest.publication_id
        with self._output_publication_journal.linearize(identity), self._state_lock:
            record = self._output_lease_record_locked(manifest)
            snapshot = self._output_publication_journal.snapshot(identity)
            if snapshot.complete is not None or record.state is not protocol.LeaseExecutionState.RUNNING:
                raise RuntimeError("output preparation gate requires a RUNNING publication")
            if phase is OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK:
                effect = OutputPublicationEffect(identity, manifest.manifest_digest, OutputPublicationStage.OWNER_REGISTER)
                ready = (self._output_publication_journal.acknowledged(effect)
                         and all(item.stage is OutputPublicationStage.OWNER_REGISTER for item in snapshot.intents))
            elif phase is OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK:
                self._output_publication_journal.preparation_receipt(identity)
                ready = True
            elif phase in (OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                           OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY):
                ready = all(item.stage is OutputPublicationStage.OWNER_REGISTER for item in snapshot.intents)
            else:
                raise ValueError("preparation checkpoint cannot report Complete")
            if not ready:
                raise RuntimeError("output gate preceded its acknowledged phase")
        gate.checkpoint(OutputPublicationGateArrival.from_manifest(manifest, phase, graph_outcome))
        with self._output_publication_journal.linearize(identity), self._state_lock:
            self._output_lease_record_locked(manifest)
            if self._output_publication_journal.snapshot(identity).state is not OutputPublicationJournalState.ACTIVE:
                raise RuntimeError("output test-gated publication was fenced before resuming")

    def _test_output_result_delivery_checkpoint(self, reply: object) -> None:
        """Pause Complete AND outcome delivery after local resource release.

        Only a configured gate synchronously reports terminal metadata. The
        normal Complete path never waits for this test-only owner report.
        Both leaders and followers revalidate the owner/incarnation after the
        gate opens, without holding any authority lock during its wait.
        """
        from . import output_protocol as wire
        gate = getattr(self, "_output_publication_gate", None)
        envelope = getattr(reply, "output_publication", None)
        witness = getattr(reply, "output_completion", None)
        if gate is None or (envelope is None and witness is None):
            return
        if envelope is not None:
            if type(envelope) is not OutputPublicationEnvelope:
                raise TypeError("output result gate requires an exact envelope")
            envelope = replace(envelope)
            witness = envelope.complete
        if type(witness) is not OutputPublicationCompleteWitness:
            raise TypeError("output result gate requires an exact Complete witness")
        witness = replace(witness)
        identity = witness.publication_id
        journal = self._output_publication_journal
        manifest = journal.snapshot(identity).manifest
        if gate.config.phase in (OutputPublicationGatePhase.BEFORE_TERMINAL_REPORT,
                                  OutputPublicationGatePhase.AFTER_TERMINAL_ACCEPTED_BEFORE_ACK):
            # The same outgoing terminal-report gate fences every Complete or
            # outcome delivery. Neither the Worker nor owner can retain C3
            # while the finite test observes GCS-only terminal knowledge.
            self._validate_terminal_gate_complete(manifest, witness)
            self._output_publications.report_terminal(identity)
            return

        def validate_delivery():
            with journal.linearize(identity), self._state_lock:
                record = self._output_lease_record_locked(manifest)
                snapshot = journal.snapshot(identity)
                if (snapshot.complete != witness or record.state is not protocol.LeaseExecutionState.COMPLETED
                        or record.completion is None
                        or record.completion.status is not protocol.TaskReplyStatus.SUCCEEDED
                        or record.output_complete_inflight is not None):
                    raise RuntimeError("output test gate lost its completed lease identity")
                if envelope is not None:
                    if (envelope.manifest != manifest
                            or journal.materialized_result(identity) != envelope.result):
                        raise RuntimeError("output test-gated payload was retired before delivery")

        def ensure_terminal(deadline: float) -> None:
            validate_delivery()
            if deadline <= time.monotonic() or not self._output_publications.report_terminal(identity):
                raise RuntimeError('owner Complete report remains pending')

        validate_delivery()
        gate.checkpoint(OutputPublicationGateArrival.from_manifest(
            manifest, OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY,
        ), ensure_terminal)
        validate_delivery()


    def _validate_terminal_gate_complete(self, manifest, witness):
        """Observe actual C3 and local resource release before W2 I/O."""
        identity = manifest.publication_id
        with self._output_publication_journal.linearize(identity), self._state_lock:
            record = self._output_lease_record_locked(manifest)
            snapshot = self._output_publication_journal.snapshot(identity)
            if (snapshot.complete != witness or record.state is not protocol.LeaseExecutionState.COMPLETED
                    or record.completion is None or record.completion.status is not protocol.TaskReplyStatus.SUCCEEDED
                    or record.output_complete_inflight is not None):
                raise RuntimeError('terminal report checkpoint preceded actual local Complete')

    def _handle_complete_worker_lease_inner(self, request: object) -> object:
        if not isinstance(request, protocol.CompleteWorkerLease):
            raise TypeError("complete_worker_lease expects CompleteWorkerLease")
        with self._state_lock:
            candidate = self._leases.get(request.lease_id)
            output_publication_id = (
                None if candidate is None else candidate.output_publication_id
            )
        if output_publication_id is not None:
            return self._handle_complete_output_worker_lease(request, output_publication_id)
        with self._state_lock:
            self._retry_dependency_pin_cleanups(force=True)
            record = self._leases.get(request.lease_id)
            if record is None:
                return self._completion_reply(
                    request,
                    protocol.LeaseExecutionState.ABANDONED,
                    accepted=False,
                    released=False,
                    error="unknown worker lease",
                )
            if (not self._lease_identity_matches(
                record, request.task_id, request.attempt_id, request.worker_id
            )) or ((request.scheduling_key != record.request.scheduling_key)):
                return self._completion_reply(
                    request,
                    record.state,
                    accepted=False,
                    released=False,
                    error="worker lease completion identity does not match grant",
                )
            if request.status is protocol.TaskReplyStatus.SUCCEEDED:
                return self._completion_reply(
                    request, record.state, accepted=False, released=False,
                    error="successful output completion requires its prepared publication",
                )
            if record.state is protocol.LeaseExecutionState.COMPLETED:
                if record.completion == request:
                    return self._completion_reply(
                        request, record.state, accepted=True, released=False
                    )
                return self._completion_reply(
                    request,
                    record.state,
                    accepted=False,
                    released=False,
                    error="completed lease replay changed terminal status",
                )
            if record.state is not protocol.LeaseExecutionState.RUNNING:
                return self._completion_reply(
                    request,
                    record.state,
                    accepted=False,
                    released=False,
                    error="worker lease cannot complete from {}".format(
                        record.state.value
                    ),
                )
            released = self._release_record_locked(
                record, protocol.LeaseExecutionState.COMPLETED
            )
            self._emit(
                "lease_completed",
                lease_id=str(request.lease_id),
                task_id=str(request.task_id),
                attempt_id=str(request.attempt_id),
                node_id=str(self.node_id),
                status=request.status.value,
            )
            record.completion = request
            return self._completion_reply(
                request, record.state, accepted=True, released=released
            )

    def _handle_get_worker_lease_outcome(self, request: object) -> object:
        """Return lease/process truth and matching local replica metadata.

        This handler intentionally remains available while the Node drains.
        ``worker_alive`` is derived only from the managed child process state; a
        failed or timed-out Worker RPC is never used as death evidence.
        """

        if not isinstance(request, protocol.GetWorkerLeaseOutcome):
            raise TypeError(
                "get_worker_lease_outcome expects GetWorkerLeaseOutcome"
            )

        def missing(detail: str) -> protocol.GetWorkerLeaseOutcomeReply:
            return protocol.GetWorkerLeaseOutcomeReply(
                request.lease_id,
                request.task_id,
                request.attempt_id,
                request.executor_worker_id,
                request.owner_worker_id,
                request.object_ids,
                self.node_id,
                found=False,
                worker_alive=False,
                error=detail,
                scheduling_key=request.scheduling_key,

            )

        with self._state_lock:
            candidate = self._leases.get(request.lease_id)
            output_publication_id = (
                None if candidate is None else candidate.output_publication_id
            )
        if output_publication_id is not None:
            reply = self._handle_get_output_worker_lease_outcome(request, output_publication_id)
            self._test_output_result_delivery_checkpoint(reply)
            return reply

        with self._state_lock:
            record = self._leases.get(request.lease_id)
            if record is None:
                return missing("node does not know this worker lease")
            if (
                record.request.task_id != request.task_id
                or record.request.attempt_id != request.attempt_id
                or record.grant.worker_id != request.executor_worker_id
                or record.request.requester_worker_id != request.owner_worker_id
                or record.request.scheduling_key != request.scheduling_key
            ):
                return missing("worker lease outcome identity does not match grant")
            if request.object_ids != record.request.return_ids:
                return missing(
                    "worker lease outcome return manifest does not match grant"
                )

            slot = self._workers.get(request.executor_worker_id)
            process = None if slot is None else slot.process
            try:
                worker_alive = process is not None and process.is_alive()
            except (AssertionError, ValueError):
                worker_alive = False

            completion_status = (
                record.completion.status
                if (
                    record.state is protocol.LeaseExecutionState.COMPLETED
                    and record.completion is not None
                )
                else None
            )
            if completion_status is protocol.TaskReplyStatus.SUCCEEDED:
                return missing(
                    "successful output outcome requires its prepared publication"
                )
            matching_descriptors: list[protocol.ObjectStoreDescriptor] = []
            if record.state in (
                protocol.LeaseExecutionState.COMPLETED,
                protocol.LeaseExecutionState.WORKER_LOST,
                protocol.LeaseExecutionState.ABANDONED,
            ):
                for object_id in request.object_ids:
                    metadata = self._sealed_metadata.get(object_id)
                    if metadata is None:
                        continue
                    attempt_id, owner_worker_id, size_bytes, checksum = metadata
                    if (
                        attempt_id != request.attempt_id
                        or owner_worker_id != request.owner_worker_id
                        or not self._object_store.contains(object_id)
                    ):
                        continue
                    matching_descriptors.append(
                        protocol.ObjectStoreDescriptor(
                            object_id, owner_worker_id, attempt_id, self.node_id,
                            size_bytes, checksum,
                        )
                    )
            return protocol.GetWorkerLeaseOutcomeReply(
                request.lease_id,
                request.task_id,
                request.attempt_id,
                request.executor_worker_id,
                request.owner_worker_id,
                request.object_ids,
                self.node_id,
                found=True,
                worker_alive=worker_alive,
                state=record.state,
                completion_status=completion_status,
                orphan_descriptors=tuple(matching_descriptors),
                scheduling_key=request.scheduling_key,

            )

    def _handle_release_lease(self, request: object) -> object:
        try:
            return self._handle_release_lease_inner(request)
        finally:
            self._flush_pending_resource_report()

    def _handle_release_lease_inner(self, request: object) -> object:
        if not isinstance(request, protocol.ReleaseWorkerLease):
            raise TypeError("release_worker_lease expects ReleaseWorkerLease")

        with self._state_lock:
            self._retry_dependency_pin_cleanups(force=True)
            record = self._leases.get(request.lease_id)
            if record is None:
                return protocol.ReleaseReply(
                    released=False, detail="lease already released or unknown"
                )

            if request.worker_id != record.grant.worker_id:
                raise ValueError("worker ID does not match lease")
            if request.allocation_token != record.allocation_token:
                raise ValueError("allocation token does not match lease")

            if record.state is protocol.LeaseExecutionState.GRANTED:
                released = self._release_record_locked(
                    record, protocol.LeaseExecutionState.ABANDONED
                )
                return protocol.ReleaseReply(
                    released=released, detail="unstarted lease abandoned"
                )
            if record.state is protocol.LeaseExecutionState.RUNNING:
                return protocol.ReleaseReply(
                    released=False, detail="running lease must complete at the worker"
                )
            return protocol.ReleaseReply(
                released=False,
                detail="lease is already terminal: {}".format(record.state.value),
            )


    def _drain_workers_once(
        self, request: protocol.BeginDrain
    ) -> tuple[protocol.DrainStatus, ...]:
        """Poll every idle ordinary Worker concurrently, without stopping it."""

        with self._state_lock:
            slots = tuple(
                (worker_id, self._workers[worker_id])
                for worker_id in self._worker_order
            )
            statuses = getattr(self, "_worker_drain_statuses", None)
            if statuses is None:
                statuses = {}
                self._worker_drain_statuses = statuses

        results: dict[ids.WorkerID, protocol.DrainStatus] = {}
        result_lock = threading.Lock()

        def poll_one(worker_id: ids.WorkerID, slot: _WorkerSlot) -> None:
            status: protocol.DrainStatus
            finalized = getattr(self, "_worker_finalize_results", {}).get(
                worker_id
            )
            if finalized is not None and finalized.clean:
                status = protocol.DrainStatus(
                    request.request_id,
                    "worker:{}".format(worker_id),
                    drain_started=True,
                    clean=True,
                    detail="worker already finalized in this drain epoch",
                )
                with result_lock:
                    results[worker_id] = status
                return
            process = slot.process
            try:
                alive = process is not None and process.is_alive()
            except (AssertionError, ValueError):
                alive = False
            if not alive or slot.address is None:
                status = protocol.DrainStatus(
                    request.request_id,
                    "worker:{}".format(worker_id),
                    drain_started=worker_id in statuses,
                    clean=False,
                    detail="ordinary Worker is not reachable during drain",
                )
            elif slot.active_lease_id is not None and worker_id not in statuses:
                # A grant accepted before the Node fence may not have reached
                # PushTask yet.  Do not close Worker admission until that exact
                # lease completes or is authoritatively abandoned.
                status = protocol.DrainStatus(
                    request.request_id,
                    "worker:{}".format(worker_id),
                    drain_started=False,
                    clean=False,
                    detail="pre-drain worker lease is still active",
                )
            else:
                handler = (
                    WORKER_DRAIN_STATUS_HANDLER
                    if worker_id in statuses
                    else WORKER_BEGIN_DRAIN_HANDLER
                )
                try:
                    candidate = self._background_rpc(
                        slot.address,
                        handler,
                        request,
                        request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
                    )
                    if (
                        not isinstance(candidate, protocol.DrainStatus)
                        or candidate.request_id != request.request_id
                        or candidate.component
                        != "worker:{}".format(worker_id)
                    ):
                        raise RuntimeError("Worker returned invalid drain status")
                    status = candidate
                except Exception as exc:
                    status = protocol.DrainStatus(
                        request.request_id,
                        "worker:{}".format(worker_id),
                        drain_started=worker_id in statuses,
                        clean=False,
                        detail="Worker drain RPC failed: {}: {}".format(
                            type(exc).__name__, exc
                        ),
                    )
            with result_lock:
                results[worker_id] = status

        threads = tuple(
            threading.Thread(
                target=poll_one,
                args=(worker_id, slot),
                name="miniray-drain-worker-{}".format(worker_id),
                daemon=True,
            )
            for worker_id, slot in slots
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        ordered = tuple(results[worker_id] for worker_id, _slot in slots)
        with self._state_lock:
            for worker_id, status in zip(self._worker_order, ordered):
                if status.drain_started:
                    self._worker_drain_statuses[worker_id] = status
        return ordered

    def _node_drain_status(
        self, request: protocol.BeginDrain, *, drive: bool
    ) -> protocol.DrainStatus:
        with self._state_lock:
            requested = self._shutdown_request_id == request.request_id
        if not requested:
            return protocol.DrainStatus(
                request.request_id,
                "node:{}".format(self.node_id),
                drain_started=False,
                clean=False,
                resources_clean=False,
                detail="node has not begun this drain epoch",
            )
        # K0 Actor Workers do not embed owner Cores.  Keep their callable and
        # replay endpoints alive while ordinary submitter Cores drain; they are
        # stopped only after the global barrier in FinalizeShutdown.
        actor_clean = True
        worker_statuses = (
            self._drain_workers_once(request)
            if drive
            else tuple(
                getattr(self, "_worker_drain_statuses", {}).get(
                    worker_id,
                    protocol.DrainStatus(
                        request.request_id,
                        "worker:{}".format(worker_id),
                        False,
                        False,
                        detail="worker drain has not started",
                    ),
                )
                for worker_id in self._worker_order
            )
        )
        # A sentinel observed at the BeginDrain cut may have produced an
        # unresolved GCS report.  Drain polling is the bounded convergence
        # driver after the Actor supervisor itself has stopped.
        self._flush_pending_actor_exit_reports()
        self._flush_pending_worker_death_reports()
        self._retry_dependency_pin_cleanups(force=True)
        self._drive_transfer_pins(force=True)
        self._drive_abandoned_dependency_custody(force=True)
        publications_clean = True
        if drive:
            # Drive remote work before the state-lock snapshot.  Progress alone
            # does not prove payload retirement; the derived predicate below
            # also fences unadopted results and unfinished owner cleanup.
            publications_clean = self._drive_output_publications()
        with self._state_lock:
            resources_clean = (
                publications_clean and self._drain_resources_clean_locked()
            )
            pids = tuple(
                self._workers[worker_id].pid
                for worker_id in self._worker_order
                if self._workers[worker_id].pid is not None
            )
        self._flush_pending_resource_report()
        child_cleans = tuple(status.clean for status in worker_statuses)
        clean = (
            actor_clean
            and len(child_cleans) == len(self._worker_order)
            and all(child_cleans)
            and resources_clean
        )
        return protocol.DrainStatus(
            request.request_id,
            "node:{}".format(self.node_id),
            drain_started=True,
            clean=clean,
            resources_clean=resources_clean,
            detail=(
                "all Workers, leases, pins, and resources drained"
                if clean
                else "node drain still has Worker, lease, pin, or Actor work"
            ),
            child_pids=pids,
            child_cleans=child_cleans,
        )

    def _handle_begin_drain(self, request: object) -> object:
        if not isinstance(request, protocol.BeginDrain):
            raise TypeError("begin_drain expects BeginDrain")
        self._install_worker_drain_fence(request.request_id)
        self._flush_pending_resource_report()
        # This first phase is intentionally only a fence.  In particular, GCS
        # membership, object service, accepted leases, and Worker owner TCP
        # endpoints remain live until the global barrier.
        return self._node_drain_status(request, drive=False)

    def _handle_drain_status(self, request: object) -> object:
        if not isinstance(request, protocol.BeginDrain):
            raise TypeError("drain_status expects BeginDrain")
        lock = getattr(self, "_shutdown_phase_lock", None)
        if lock is None:
            with self._state_lock:
                lock = getattr(self, "_shutdown_phase_lock", None)
                if lock is None:
                    lock = threading.Lock()
                    self._shutdown_phase_lock = lock
        with lock:
            return self._node_drain_status(request, drive=True)

    def _finalize_workers(
        self, request: protocol.FinalizeShutdown
    ) -> tuple[_WorkerStopResult, ...]:
        """Stop clean Workers concurrently; never force one in this phase."""

        with self._state_lock:
            slots = tuple(
                (worker_id, self._workers[worker_id])
                for worker_id in self._worker_order
            )
        results: dict[ids.WorkerID, _WorkerStopResult] = {}
        result_lock = threading.Lock()

        def finalize_one(worker_id: ids.WorkerID, slot: _WorkerSlot) -> None:
            previous = getattr(self, "_worker_finalize_results", {}).get(
                worker_id
            )
            if previous is not None and previous.clean:
                with result_lock:
                    results[worker_id] = previous
                return
            process = slot.process
            address = slot.address
            ack = None
            try:
                alive = process is not None and process.is_alive()
            except (AssertionError, ValueError):
                alive = False
            if alive and address is not None:
                try:
                    candidate = self._background_rpc(
                        address,
                        WORKER_FINALIZE_SHUTDOWN_HANDLER,
                        request,
                        request_timeout=WORKER_STOP_TIMEOUT_SECONDS,
                    )
                    if (
                        isinstance(candidate, protocol.ShutdownAck)
                        and candidate.request_id == request.request_id
                        and candidate.component
                        == "worker:{}".format(worker_id)
                        and candidate.clean
                    ):
                        ack = candidate
                except Exception:
                    pass
                process.join(WORKER_STOP_TIMEOUT_SECONDS)
                try:
                    alive = process.is_alive()
                except (AssertionError, ValueError):
                    alive = False
            exitcode = None if process is None else process.exitcode
            clean = not alive and exitcode == 0 and ack is not None
            result = _WorkerStopResult(
                worker_id, slot.pid, exitcode, clean, forced=False
            )
            if clean and process is not None:
                try:
                    process.close()
                except (AssertionError, ValueError):
                    pass
                with self._state_lock:
                    current = self._workers[worker_id]
                    if current.process is process:
                        current.process = None
                        current.address = None
                        current.exitcode = exitcode
                        current.forced = False
            if clean:
                with self._state_lock:
                    finalized = getattr(self, "_worker_finalize_results", None)
                    if finalized is None:
                        finalized = {}
                        self._worker_finalize_results = finalized
                    finalized[worker_id] = result
            with result_lock:
                results[worker_id] = result

        threads = tuple(
            threading.Thread(
                target=finalize_one,
                args=(worker_id, slot),
                name="miniray-finalize-worker-{}".format(worker_id),
                daemon=True,
            )
            for worker_id, slot in slots
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return tuple(results[worker_id] for worker_id, _slot in slots)

    def _handle_finalize_shutdown(self, request: object) -> object:
        if not isinstance(request, protocol.FinalizeShutdown):
            raise TypeError("finalize_shutdown expects FinalizeShutdown")
        lock = getattr(self, "_shutdown_phase_lock", None)
        if lock is None:
            with self._state_lock:
                lock = getattr(self, "_shutdown_phase_lock", None)
                if lock is None:
                    lock = threading.Lock()
                    self._shutdown_phase_lock = lock
        with lock:
            return self._finalize_shutdown_serialized(request)

    def _finalize_shutdown_serialized(
        self, request: protocol.FinalizeShutdown
    ) -> protocol.ShutdownAck:
        begin = protocol.BeginDrain(request.request_id, "finalize")
        before = self._node_drain_status(begin, drive=True)
        if not before.clean:
            return protocol.ShutdownAck(
                request.request_id,
                "node:{}".format(self.node_id),
                clean=False,
                detail="node cannot finalize before its drain is clean",
                resources_clean=before.resources_clean,
                child_pids=before.child_pids,
                child_exitcodes=(None,) * len(before.child_pids),
                child_cleans=before.child_cleans,
                child_forced=(False,) * len(before.child_pids),
            )
        # Actor replay endpoints are needed until every ordinary submitter Core
        # has crossed the clean barrier.  They own no Core/borrow state in K0,
        # so their ordinary mailbox shutdown belongs to finalization.
        actor_results = self._stop_all_actor_workers(request.request_id)
        results = self._finalize_workers(request)
        actor_clean = all(result.clean for result in actor_results)
        actor_forced = any(result.forced for result in actor_results)
        child_clean = all(result.clean for result in results)
        with self._state_lock:
            resources_clean = self._resources_clean_locked()
        finalized = actor_clean and child_clean and resources_clean
        if finalized:
            self._schedule_finalize_exit()
        return protocol.ShutdownAck(
            request.request_id,
            "node:{}".format(self.node_id),
            clean=finalized,
            detail=(
                "node finalized after the cluster barrier"
                if finalized
                else "one or more Workers did not finalize cleanly"
            ),
            child_pid=results[0].pid if results else None,
            child_exitcode=results[0].exitcode if results else None,
            child_clean=results[0].clean if results else True,
            forced=actor_forced or any(result.forced for result in results),
            resources_clean=resources_clean,
            child_pids=tuple(
                result.pid for result in results if result.pid is not None
            ),
            child_exitcodes=tuple(
                result.exitcode for result in results if result.pid is not None
            ),
            child_cleans=tuple(
                result.clean for result in results if result.pid is not None
            ),
            child_forced=tuple(
                False for result in results if result.pid is not None
            ),
        )

    def _schedule_finalize_exit(self) -> None:
        """Wake node_main only after the finalization ACK is on the wire."""

        with self._state_lock:
            if getattr(self, "_finalize_exit_scheduled", False):
                return
            self._finalize_exit_scheduled = True
        handler_thread = threading.current_thread()
        if handler_thread.daemon:
            threading.Thread(
                target=self._release_wait_after_finalize_handler,
                args=(handler_thread,),
                name="miniray-node-finalize",
                daemon=False,
            ).start()
        else:
            self._stop_event.set()

    def _release_wait_after_finalize_handler(
        self, handler_thread: threading.Thread
    ) -> None:
        handler_thread.join()
        self._stop_event.set()

    def _handle_shutdown(self, request: object) -> object:
        if not isinstance(request, protocol.Shutdown):
            raise TypeError("shutdown expects Shutdown")
        self._install_worker_drain_fence(request.request_id)
        self._flush_pending_resource_report()
        self._release_all_transfer_pins()
        actor_results = self._stop_all_actor_workers(request.request_id)
        results = self._stop_workers()
        publications_clean = self._drive_output_publications()
        self._retry_dependency_pin_cleanups(force=True)
        self._drive_transfer_pins(force=True)
        child_clean = all(result.clean for result in results)
        actor_clean = all(result.clean for result in actor_results)
        forced = any(result.forced for result in results) or any(
            result.forced for result in actor_results
        )
        reported = tuple(result for result in results if result.pid is not None)
        child_pids = tuple(result.pid for result in reported)
        child_exitcodes = tuple(result.exitcode for result in reported)
        child_cleans = tuple(result.clean for result in reported)
        child_forced = tuple(result.forced for result in reported)
        with self._state_lock:
            resources_clean = (
                publications_clean and self._resources_clean_locked()
            )
        return protocol.ShutdownAck(
            request_id=request.request_id,
            component="node:{}".format(self.node_id),
            clean=actor_clean and child_clean and resources_clean,
            detail="worker stopped; node awaits finalize",
            child_pid=child_pids[0] if child_pids else None,
            child_exitcode=child_exitcodes[0] if child_exitcodes else None,
            child_clean=child_clean,
            forced=forced,
            resources_clean=resources_clean,
            child_pids=child_pids,
            child_exitcodes=child_exitcodes,
            child_cleans=child_cleans,
            child_forced=child_forced,
        )

    def _handle_shutdown_status(self, request: object) -> object:
        if not isinstance(request, protocol.ShutdownStatusRequest):
            raise TypeError("shutdown_status expects ShutdownStatusRequest")
        self._flush_pending_actor_exit_reports()
        self._flush_pending_worker_death_reports()
        self._retry_dependency_pin_cleanups(force=True)
        self._drive_transfer_pins(force=True)
        self._drive_abandoned_dependency_custody(force=True)
        with self._state_lock:
            requested = self._shutdown_request_id == request.request_id
            slots = tuple(
                self._workers[worker_id] for worker_id in self._worker_order
            )
            reported = tuple(slot for slot in slots if slot.pid is not None)
            child_pids = tuple(slot.pid for slot in reported)
            child_exitcodes = tuple(slot.exitcode for slot in reported)
            child_cleans = tuple(
                slot.exitcode == 0 and not slot.forced for slot in reported
            )
            child_pid = child_pids[0] if child_pids else None
            child_exitcode = child_exitcodes[0] if child_exitcodes else None
            child_clean = all(child_cleans)
            resources_clean = self._resources_clean_locked()
        # Finalization records that the matching second-phase request was
        # accepted.  Cleanup quality is reported separately; an unclean or
        # forcibly-stopped child must not strand the Node process.
        finalized = bool(request.finalize and requested)
        if finalized:
            self._stop_event.set()
        return protocol.ShutdownStatus(
            request_id=request.request_id,
            component="node:{}".format(self.node_id),
            shutdown_requested=requested,
            child_pid=child_pid,
            child_exitcode=child_exitcode,
            child_clean=child_clean,
            finalized=finalized,
            resources_clean=resources_clean,
            child_pids=child_pids,
            child_exitcodes=child_exitcodes,
            child_cleans=child_cleans,
        )

    @staticmethod
    def _rejected(
        request: protocol.RequestWorkerLease,
        reason: protocol.LeaseRejectReason,
        detail: str,
    ) -> protocol.RejectWorkerLease:
        return protocol.RejectWorkerLease(
            lease_id=request.lease_id,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
            reason=reason,
            detail=detail,
            scheduling_key=request.scheduling_key,

        )

    def _emit(self, name: str, **attributes: object) -> None:
        """Record an observational event without joining runtime correctness."""

        sink = getattr(self, "event_sink", None)
        if sink is None:
            return
        try:
            sink.emit(name, component="node", attributes=attributes)
        except Exception:
            # Tracing is best-effort.  In particular, narrow state-machine
            # fixtures built with object.__new__ need not construct a sink.
            return

    def _background_rpc(
        self, address: Address, handler: str, message: object,
        **options: object,
    ) -> object:
        """Send Node-owned work with this process's trace identity.

        Handler-originated nested RPCs inherit their sink and predecessor from
        the transport causal context.  Startup, supervisor, outbox, rollback,
        and child-drain work can run outside that context (or on a fresh
        thread), so those paths use this one explicit boundary instead.

        ``object.__new__`` fixtures predating tracing may omit ``event_sink``;
        retaining their legacy call shape keeps this observational concern from
        changing narrow state-machine tests or runtime semantics.
        """

        sink = getattr(self, "event_sink", None)
        if sink is not None:
            options["event_sink"] = sink
            options["trace_component"] = "node"
        return rpc_request(address, handler, message, **options)


def node_main(
    node_id: ids.NodeID,
    total_resources: resources.ResourceVector,
    ready_connection: Optional[Connection] = None,
    worker_id: Optional[ids.WorkerID] = None,
    host: str = LOOPBACK_HOST,
    port: int = 0,
    inline_threshold: int = DEFAULT_INLINE_THRESHOLD_BYTES,
    object_store_bytes: int = DEFAULT_OBJECT_STORE_BYTES,
    gcs_address: Optional[Address] = None,
    worker_failpoint: Optional[WorkerFailpointConfig] = None,
    trace_config: Optional[TraceSinkConfig] = None,
    num_workers_per_node: int = 1,
    output_publication_gate: Optional[OutputPublicationGateConfig] = None,
) -> None:
    """Spawn-safe node process entry point."""

    server: Optional[NodeServer] = None
    try:
        server = NodeServer(
            node_id,
            total_resources,
            worker_id=worker_id,
            host=host,
            port=port,
            inline_threshold=inline_threshold,
            object_store_bytes=object_store_bytes,
            gcs_address=gcs_address,
            worker_failpoint=worker_failpoint,
            trace_config=trace_config,
            num_workers_per_node=num_workers_per_node,
            output_publication_gate=output_publication_gate,
        )
        address = server.start()
        worker_ids = server.worker_ids
        worker_pids = server.worker_pids
        worker_addresses = server.worker_addresses
        if not (
            worker_ids
            and len(worker_ids) == len(worker_pids) == len(worker_addresses)
        ):
            raise RuntimeError("node Workers did not publish startup identities")
        startup = protocol.NodeStartup(
            node_id=node_id,
            node_pid=os.getpid(),
            node_address=address,
            worker_ids=worker_ids,
            worker_pids=worker_pids,
            worker_addresses=worker_addresses,
        )
        if ready_connection is not None:
            ready_connection.send((True, startup))
            ready_connection.close()
            ready_connection = None
        server.wait()
    except BaseException:
        if ready_connection is not None:
            try:
                ready_connection.send((False, traceback.format_exc()))
            finally:
                ready_connection.close()
        raise
    finally:
        if server is not None:
            server.stop()
            server.event_sink.close()


__all__ = [
    "CANCEL_LEASE_HANDLER",
    "COMPLETE_WORKER_LEASE_HANDLER",
    "DROP_OBJECT_REPLICA_HANDLER",
    "GET_OBJECT_CHUNK_HANDLER",
    "NodeServer",
    "OBJECT_TRANSFER_CHUNK_BYTES",
    "PIN_OBJECT_HANDLER",
    "GET_OBJECT_HANDLER",
    "NOTIFY_WORKER_BLOCKED_HANDLER",
    "NOTIFY_WORKER_UNBLOCKED_HANDLER",
    "RELEASE_LEASE_HANDLER",
    "RELEASE_OBJECT_PIN_HANDLER",
    "RESERVE_ACTOR_WORKER_HANDLER",
    "REQUEST_LEASE_HANDLER",
    "SEAL_OBJECT_HANDLER",
    "SHUTDOWN_HANDLER_NAME",
    "SHUTDOWN_STATUS_HANDLER",
    "START_WORKER_LEASE_HANDLER",
    "node_main",
]
