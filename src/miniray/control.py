"""The deliberately small cluster control plane used by mini-Ray.

The real Ray GCS owns cluster-wide *control* state, not ordinary-task routing
or result bytes. This module records membership/resource summaries, exported
functions and Actor lifecycle state; it never queues or forwards ordinary
Task or Actor-method submissions.

Ordinary result publication belongs to the object owner and executing Node.
GCS holds no per-task publication manifest, result-stage history or contained
reference graph. It commits membership/owner-death facts and retries their
owner-wide Node fences until exact acknowledgements make Nodes safe to use.

Placement groups use a small GCS-side runtime adapter around the pure
``PlacementGroupCoordinator``.  GCS freezes the plan and converges participant
obligations, while each NodeManager remains the only resource-allocation
authority.
"""

from __future__ import annotations

import os
import traceback
from dataclasses import dataclass, replace
from multiprocessing.connection import Connection
from threading import Event, RLock, Thread, current_thread
from types import MappingProxyType
from typing import Callable, Hashable, Mapping, Optional, Tuple

from . import protocol
from .errors import FunctionNotRegisteredError
from .function_registry import (
    FunctionRegistrationConflictError,
    FunctionRegistry,
    FunctionSnapshot,
    UnknownFunctionError,
)
from .ids import ActorGeneration, ActorID, NodeID, PlacementGroupID, WorkerID
from .placement import Bundle, PlacementStrategy
from .placement_group_runtime import (
    AbortReservation,
    CommitReservation,
    ParticipantReplyStatus,
    PlacementGroupConflictError,
    PlacementGroupCoordinator,
    PlacementGroupOperation,
    PlacementGroupPhase,
    PlacementGroupSnapshot,
    PlacementGroupSpec,
    PrepareReservation,
    ReservationReply,
)
from .protocol import CreateActorReply, CreateActorRequest, FunctionDefinition, FunctionKey, FunctionRegistrationReply, FunctionReply, GCSStartup, GetNodeAddress, GetNodeAddressReply, GetNodes, GetNodesReply, GetFunction, RegisterNode, RegisterNodeReply, RegisterFunction, ReserveActorWorkerReply, ReserveActorWorkerRequest, Shutdown, ShutdownAck, UnregisterNode, UnregisterNodeReply, UpdateNodeResources, UpdateNodeResourcesReply
from .resources import (
    HybridPolicy,
    NodeSnapshot as SchedulingNodeSnapshot,
    ResourceQuantity,
    ResourceVector,
    SchedulingStatus,
)
from .owner_death_fence_registry import (
    OwnerDeathFenceEffect, OwnerDeathFenceRegistry,
    OwnerFenceNodeIncarnation,
)
from .trace import EventSink
from .trace import TraceSinkConfig
from .trace_collector import sink_from_config
from .transport import Address, LOOPBACK_HOST, TCPServer, request as rpc_request


CONTROL_HANDLER_NAME = "control"
REGISTER_NODE_HANDLER = "register_node"
UPDATE_NODE_RESOURCES_HANDLER = "update_node_resources"
UNREGISTER_NODE_HANDLER = "unregister_node"
GET_NODES_HANDLER = "get_nodes"
GET_NODE_ADDRESS_HANDLER = "get_node_address"
REPORT_NODE_DEATH_HANDLER = "report_node_death"
GET_NODE_STATE_HANDLER = "get_node_state"
REGISTER_WORKER_INCARNATION_HANDLER = "register_worker_incarnation"
REPORT_WORKER_DEATH_HANDLER = "report_worker_death"
GET_WORKER_STATE_HANDLER = "get_worker_state"
GET_WORKER_DEATHS_HANDLER = "get_worker_deaths"
GCS_SHUTDOWN_HANDLER = "shutdown"
# NodeManager RPC name used by the Driver after it validates one GCS snapshot.
INSTALL_CLUSTER_SNAPSHOT_HANDLER = "install_cluster_snapshot"
# Actor creation is coordinated by GCS, but the reservation itself is a direct
# GCS -> NodeManager control RPC.  Actor method calls use neither handler.
CREATE_ACTOR_HANDLER = "create_actor"
RESERVE_ACTOR_WORKER_HANDLER = "reserve_actor_worker"
REPORT_ACTOR_WORKER_EXIT_HANDLER = "report_actor_worker_exit"
GET_ACTOR_STATE_HANDLER = "get_actor_state"
INSTALL_ACTOR_STATE_HANDLER = "install_actor_state"
# Placement-group orchestration is a GCS control-plane path.  The participant
# handlers are direct GCS -> NodeManager RPCs; Task admission never uses them.
CREATE_PLACEMENT_GROUP_HANDLER = "create_placement_group"
GET_PLACEMENT_GROUP_HANDLER = "get_placement_group"
REMOVE_PLACEMENT_GROUP_HANDLER = "remove_placement_group"
DRAIN_PLACEMENT_GROUPS_HANDLER = "drain_placement_groups"
DRAIN_ACTORS_HANDLER = "drain_actors"
PREPARE_PLACEMENT_GROUP_HANDLER = "prepare_placement_group"
COMMIT_PLACEMENT_GROUP_HANDLER = "commit_placement_group"
ABORT_PLACEMENT_GROUP_HANDLER = "abort_placement_group"
# Owner-wide death propagation is membership cleanup, not result publication.
DRAIN_OWNER_DEATH_FENCES_HANDLER = "drain_owner_death_fences"
INSTALL_OWNER_DEATH_FENCE_HANDLER = "install_owner_death_fence"


class ControlPlaneError(RuntimeError):
    """Base class for GCS-lite state errors."""


class UnknownNodeError(ControlPlaneError, KeyError):
    """Raised when a requested node has not been registered."""


class DeadNodeError(ControlPlaneError):
    """Raised when a live-only operation targets a DEAD tombstone."""


class NodeRegistrationConflictError(ControlPlaneError):
    """Raised when one node ID is re-used for a different node."""


class UnknownActorError(ControlPlaneError, KeyError):
    """Raised when an Actor has not entered the creation registry."""


class ActorRegistrationConflictError(ControlPlaneError):
    """Raised when one ActorID is re-used for a different specification."""


class ActorCreationStateError(ControlPlaneError):
    """Raised when a terminal Actor record would be changed."""


@dataclass(frozen=True)
class NodeSnapshot:
    """An immutable GCS membership record for one physical NodeID.

    ``total_resources`` is registration-time capacity.  ``available_resources``
    is only a cluster summary; each node manager remains authoritative for
    leases and its local resource balance.  DEAD records remain as tombstones
    and are excluded from every scheduling view.
    """

    node_id: NodeID
    node_pid: int
    registration_epoch: int
    address: Address
    total_resources: ResourceVector
    available_resources: ResourceVector
    state: protocol.NodeMembershipState = protocol.NodeMembershipState.ALIVE
    resource_report_seq: int = 0
    death: Optional[protocol.NodeDeathRecord] = None

    @property
    def total(self) -> ResourceVector:
        return self.total_resources

    @property
    def available(self) -> ResourceVector:
        return self.available_resources

    def to_scheduling_snapshot(self) -> SchedulingNodeSnapshot:
        """Drop the control-only address and return the scheduler's view.

        The registry stores only live nodes, so an entry exported from it is an
        alive scheduling candidate.  Keeping this conversion explicit avoids a
        second, subtly different implementation of feasibility and utilization
        in the control plane.
        """

        return SchedulingNodeSnapshot(
            node_id=self.node_id,
            total=self.total_resources,
            available=self.available_resources,
            alive=self.state is protocol.NodeMembershipState.ALIVE,
        )

    def to_node_info(self) -> protocol.NodeInfo:
        if self.state is not protocol.NodeMembershipState.ALIVE:
            raise DeadNodeError("dead nodes have no live scheduling record")
        return protocol.NodeInfo(
            self.node_id, self.node_pid, self.registration_epoch, self.address,
            self.total_resources, self.available_resources,
        )


@dataclass(frozen=True)
class WorkerSnapshot:
    """One ordinary Worker incarnation and its immutable death fact.

    This is deliberately separate from ``NodeSnapshot``: Worker lifetime is
    consumed by ownership recovery, while Node resources are only scheduling
    hints.  GCS never stores object IDs or ownership records here.
    """

    incarnation: protocol.WorkerIncarnation
    state: protocol.WorkerMembershipState
    death: Optional[protocol.WorkerDeathRecord] = None

    def __post_init__(self) -> None:
        if not isinstance(self.incarnation, protocol.WorkerIncarnation):
            raise TypeError("incarnation must be a WorkerIncarnation")
        if not isinstance(self.state, protocol.WorkerMembershipState):
            raise TypeError("state must be a WorkerMembershipState")
        if self.state is protocol.WorkerMembershipState.DEAD:
            if (
                not isinstance(self.death, protocol.WorkerDeathRecord)
                or self.death.incarnation != self.incarnation
            ):
                raise ValueError("DEAD Worker requires its matching death record")
        elif self.death is not None:
            raise ValueError("ALIVE Worker cannot contain a death record")


@dataclass(frozen=True)
class FeatureStatus:
    """Implementation status for a deliberately deferred GCS feature."""

    name: str
    implemented: bool
    detail: str


@dataclass(frozen=True)
class ControlSnapshot:
    """A consistent diagnostic view of GCS-lite state."""

    nodes: Tuple[NodeSnapshot, ...]
    functions: Tuple[FunctionSnapshot, ...]
    actor_support: FeatureStatus
    placement_group_support: FeatureStatus

    @property
    def scheduling_nodes(self) -> Tuple[SchedulingNodeSnapshot, ...]:
        """Return node views accepted by the scheduling algorithms."""

        return tuple(node.to_scheduling_snapshot() for node in self.nodes)


class NodeRegistry:
    """Thread-safe node membership and eventually-consistent resources.

    Registration is idempotent for identical identity data.  Resource reports
    have a separate update path because availability changes continuously.
    GCS-lite never grants a worker lease from this summary.
    """

    def __init__(
        self, *, scheduling_visible: Optional[Callable[[NodeID], bool]] = None,
    ) -> None:
        if scheduling_visible is not None and not callable(scheduling_visible):
            raise TypeError("scheduling_visible must be callable or None")
        self._scheduling_visible = scheduling_visible or (lambda _node_id: True)
        self._nodes: dict[NodeID, NodeSnapshot] = {}
        # A detection ID is the idempotency key for the observation itself, not
        # merely for one Node row.  Binding it globally prevents a buggy
        # observer from reusing the same proof to kill another incarnation.
        self._death_detections: dict[
            str, tuple[
                protocol.ReportNodeDeath, protocol.ReportNodeDeathReply
            ]
        ] = {}
        self._membership_epoch = 0
        self._registration_epoch = 0
        self._lock = RLock()

    @property
    def membership_epoch(self) -> int:
        with self._lock:
            return self._membership_epoch

    def register(
        self,
        node_id: NodeID,
        address: Address,
        total_resources: ResourceVector | Mapping[str, ResourceQuantity],
        *,
        node_pid: int,
        available_resources: Optional[
            ResourceVector | Mapping[str, ResourceQuantity]
        ] = None,
    ) -> bool:
        """Register a node, returning ``True`` only for the first insert.

        Replaying the same registration is harmless.  Re-using an existing
        node ID for a different address or capacity is rejected so callers do
        not silently redirect leases to the wrong process.
        """

        _require_node_id(node_id)
        _require_node_pid(node_pid)
        checked_address = _validate_address(address)
        totals = _resource_vector(total_resources)
        available = (
            totals
            if available_resources is None
            else _resource_vector(available_resources)
        )
        _require_available_within_total(available, totals)

        with self._lock:
            old = self._nodes.get(node_id)
            if old is not None:
                if old.state is protocol.NodeMembershipState.DEAD:
                    raise NodeRegistrationConflictError(
                        "dead NodeID {!r} cannot be registered again".format(node_id)
                    )
                if (
                    old.node_pid != node_pid
                    or old.address != checked_address
                    or old.total_resources != totals
                ):
                    raise NodeRegistrationConflictError(
                        "node {!r} is already registered with pid {} at {!r} "
                        "with resources {!r}".format(
                            node_id, old.node_pid, old.address, old.total_resources
                        )
                    )
                return False
            self._registration_epoch += 1
            self._membership_epoch += 1
            self._nodes[node_id] = NodeSnapshot(
                node_id, node_pid, self._registration_epoch, checked_address,
                totals, available, resource_report_seq=0,
            )
            return True

    def register_message(
        self, message: protocol.RegisterNode
    ) -> protocol.RegisterNodeReply:
        """Atomically register and return both incarnation and global epochs."""

        if not isinstance(message, protocol.RegisterNode):
            raise TypeError("message must be a RegisterNode")
        with self._lock:
            try:
                self.register(
                    message.node_id, message.address, message.total_resources,
                    node_pid=message.node_pid,
                    available_resources=message.available_resources,
                )
            except NodeRegistrationConflictError as exc:
                existing = self._nodes[message.node_id]
                return protocol.RegisterNodeReply(
                    message.node_id, message.node_pid, False,
                    existing.registration_epoch, self._membership_epoch, str(exc),
                )
            registered = self._nodes[message.node_id]
            return protocol.RegisterNodeReply(
                message.node_id, message.node_pid, True,
                registered.registration_epoch, self._membership_epoch,
            )

    # Explicit names make call sites and teaching traces easier to read.
    register_node = register

    def update_resources(
        self,
        node_id: NodeID,
        node_pid: int,
        registration_epoch: int,
        report_seq: int,
        available_resources: ResourceVector | Mapping[str, ResourceQuantity],
    ) -> bool:
        """Apply or exactly replay one incarnation-fenced resource report."""

        _require_node_pid(node_pid)
        _require_positive_epoch(registration_epoch, "registration_epoch")
        _require_non_negative(report_seq, "report_seq")
        available = _resource_vector(available_resources)
        with self._lock:
            old = self._get_locked(node_id)
            self._require_alive_locked(old)
            if (
                old.node_pid != node_pid
                or old.registration_epoch != registration_epoch
            ):
                raise NodeRegistrationConflictError(
                    "resource report does not match the registered Node incarnation"
                )
            _require_available_within_total(available, old.total_resources)
            if report_seq < old.resource_report_seq:
                raise ControlPlaneError(
                    "resource report sequence is stale: {} < {}".format(
                        report_seq, old.resource_report_seq
                    )
                )
            if report_seq == old.resource_report_seq:
                if available != old.available_resources:
                    raise ControlPlaneError(
                        "resource report sequence was reused with another payload"
                    )
                return False
            self._nodes[node_id] = NodeSnapshot(
                old.node_id, old.node_pid, old.registration_epoch, old.address,
                old.total_resources, available, old.state, report_seq, old.death,
            )
            return True

    update_resource_snapshot = update_resources

    def unregister(
        self, node_id: NodeID, node_pid: int, registration_epoch: int,
        detection_id: str,
    ) -> protocol.ReportNodeDeathReply:
        """Record expected termination as the same immutable death fact."""

        return self.report_death(
            protocol.ReportNodeDeath(
                detection_id, node_id, node_pid, registration_epoch, 0,
                protocol.NodeDeathReason.EXPECTED, "node unregistered normally",
            )
        )

    unregister_node = unregister

    def address(self, node_id: NodeID) -> Address:
        with self._lock:
            node = self._get_locked(node_id)
            self._require_alive_locked(node)
            return node.address

    get_address = address

    def resources(self, node_id: NodeID) -> ResourceVector:
        """Return an immutable copy of the last availability report."""

        with self._lock:
            node = self._get_locked(node_id)
            self._require_alive_locked(node)
            return node.available_resources

    resource_snapshot = resources

    def get(self, node_id: NodeID) -> NodeSnapshot:
        with self._lock:
            return self._get_locked(node_id)

    def snapshot(self) -> Tuple[NodeSnapshot, ...]:
        with self._lock:
            # repr is used only for stable diagnostics; IDs need not be mutually
            # orderable (for example, UUIDs and strings can coexist).
            return tuple(
                sorted(
                    (
                        node for node in self._nodes.values()
                        if (
                            node.state is protocol.NodeMembershipState.ALIVE
                            and self._scheduling_visible(node.node_id)
                        )
                    ),
                    key=lambda node: repr(node.node_id),
                )
            )

    def all_snapshot(self) -> Tuple[NodeSnapshot, ...]:
        with self._lock:
            return tuple(
                sorted(self._nodes.values(), key=lambda node: repr(node.node_id))
            )


    def live_snapshot(self) -> tuple[int, Tuple[protocol.NodeInfo, ...]]:
        with self._lock:
            return self._membership_epoch, self._live_node_infos_locked()

    def get_state_reply(
        self, request: protocol.GetNodeState
    ) -> protocol.GetNodeStateReply:
        if not isinstance(request, protocol.GetNodeState):
            raise TypeError("request must be a GetNodeState")
        with self._lock:
            node = self._nodes.get(request.node_id)
            if node is None:
                return protocol.GetNodeStateReply(
                    request.node_id, False, self._membership_epoch,
                    error="unknown node: {!r}".format(request.node_id),
                )
            return protocol.GetNodeStateReply(
                request.node_id, True, self._membership_epoch, node.state,
                node.node_pid, node.registration_epoch, node.death,
            )

    def report_death(
        self, request: protocol.ReportNodeDeath
    ) -> protocol.ReportNodeDeathReply:
        if not isinstance(request, protocol.ReportNodeDeath):
            raise TypeError("request must be a ReportNodeDeath")
        with self._lock:
            detections = getattr(self, "_death_detections", None)
            if detections is None:
                # Compatibility for a handful of old object.__new__ fixtures.
                detections = {}
                self._death_detections = detections
            prior_detection = detections.get(request.detection_id)
            if prior_detection is not None:
                prior_request, prior_reply = prior_detection
                if prior_request == request:
                    assert prior_reply.death is not None
                    return protocol.ReportNodeDeathReply(
                        prior_reply.detection_id, prior_reply.node_id,
                        prior_reply.node_pid,
                        protocol.NodeDeathDisposition.ALREADY_DEAD,
                        prior_reply.membership_epoch, prior_reply.live_nodes,
                        prior_reply.death,
                    )
                return self._death_reply_locked(
                    request, protocol.NodeDeathDisposition.CONFLICT,
                    error="node death detection_id was reused for another proof",
                )
            old = self._nodes.get(request.node_id)
            if old is None:
                return self._death_reply_locked(
                    request, protocol.NodeDeathDisposition.UNKNOWN,
                    error="unknown NodeID",
                )
            identity_matches = (
                old.node_pid == request.node_pid
                and old.registration_epoch == request.expected_registration_epoch
            )
            if old.state is protocol.NodeMembershipState.DEAD:
                assert old.death is not None
                exact = identity_matches and (
                    old.death.detection_id == request.detection_id
                    and old.death.exit_code == request.exit_code
                    and old.death.reason is request.reason
                    and old.death.detail == request.detail
                )
                if exact:
                    return self._death_reply_locked(
                        request, protocol.NodeDeathDisposition.ALREADY_DEAD,
                        death=old.death,
                    )
                return self._death_reply_locked(
                    request, protocol.NodeDeathDisposition.CONFLICT,
                    error="node death report conflicts with the immutable tombstone",
                )
            if not identity_matches:
                return self._death_reply_locked(
                    request, protocol.NodeDeathDisposition.CONFLICT,
                    error="node death report targets another Node incarnation",
                )

            self._membership_epoch += 1
            death = protocol.NodeDeathRecord(
                request.detection_id, request.node_id, request.node_pid,
                request.expected_registration_epoch, self._membership_epoch,
                request.exit_code, request.reason, request.detail,
            )
            self._nodes[request.node_id] = NodeSnapshot(
                old.node_id, old.node_pid, old.registration_epoch, old.address,
                old.total_resources, ResourceVector.empty(),
                protocol.NodeMembershipState.DEAD, old.resource_report_seq, death,
            )
            reply = self._death_reply_locked(
                request, protocol.NodeDeathDisposition.APPLIED, death=death
            )
            detections[request.detection_id] = (request, reply)
            return reply

    def scheduling_snapshot(self) -> Tuple[SchedulingNodeSnapshot, ...]:
        """Return a stable snapshot in the shared scheduler data model."""

        return tuple(node.to_scheduling_snapshot() for node in self.snapshot())

    def _death_reply_locked(
        self, request: protocol.ReportNodeDeath,
        disposition: protocol.NodeDeathDisposition, *,
        death: Optional[protocol.NodeDeathRecord] = None,
        error: Optional[str] = None,
    ) -> protocol.ReportNodeDeathReply:
        return protocol.ReportNodeDeathReply(
            request.detection_id, request.node_id, request.node_pid, disposition,
            self._membership_epoch, self._live_node_infos_locked(), death, error,
        )

    def _live_node_infos_locked(self) -> Tuple[protocol.NodeInfo, ...]:
        return tuple(
            node.to_node_info()
            for node in sorted(self._nodes.values(), key=lambda value: value.node_id.hex)
            if node.state is protocol.NodeMembershipState.ALIVE
        )

    def _get_locked(self, node_id: NodeID) -> NodeSnapshot:
        try:
            return self._nodes[node_id]
        except KeyError:
            raise UnknownNodeError("unknown node: {!r}".format(node_id)) from None

    @staticmethod
    def _require_alive_locked(node: NodeSnapshot) -> None:
        if node.state is protocol.NodeMembershipState.DEAD:
            raise DeadNodeError("node is DEAD: {!r}".format(node.node_id))


class WorkerRegistry:
    """GCS authority for ordinary Worker liveness facts.

    The registry has exactly two jobs: bind a physical ordinary Worker to an
    already-registered Node incarnation, and serialize immutable death facts
    into one cluster-wide journal.  It intentionally knows nothing about
    objects, tasks, leases, or Actor Workers.
    """

    def __init__(self, nodes: NodeRegistry) -> None:
        if not isinstance(nodes, NodeRegistry):
            raise TypeError("nodes must be a NodeRegistry")
        self._nodes = nodes
        self._workers: dict[WorkerID, WorkerSnapshot] = {}
        self._death_detections: dict[
            str, tuple[protocol.ReportWorkerDeath, protocol.WorkerDeathRecord]
        ] = {}
        self._death_journal: list[protocol.WorkerDeathRecord] = []
        self._node_death_worksets: dict[
            tuple[NodeID, int, int, str],
            Tuple[protocol.WorkerDeathRecord, ...],
        ] = {}
        self._death_epoch = 0
        self._lock = RLock()

    @property
    def death_watermark(self) -> int:
        with self._lock:
            return self._death_epoch

    def register(
        self, request: protocol.RegisterWorkerIncarnation
    ) -> protocol.RegisterWorkerIncarnationReply:
        """Insert or exactly replay one live ordinary Worker incarnation."""

        if not isinstance(request, protocol.RegisterWorkerIncarnation):
            raise TypeError(
                "request must be a RegisterWorkerIncarnation"
            )
        incarnation = request.incarnation
        try:
            node = self._nodes.get(incarnation.node_id)
        except (UnknownNodeError, DeadNodeError) as exc:
            return protocol.RegisterWorkerIncarnationReply(
                incarnation, False, str(exc)
            )
        if (
            node.state is not protocol.NodeMembershipState.ALIVE
            or node.node_pid != incarnation.node_pid
            or node.registration_epoch != incarnation.node_registration_epoch
        ):
            return protocol.RegisterWorkerIncarnationReply(
                incarnation, False,
                "worker registration targets another Node incarnation",
            )

        with self._lock:
            old = self._workers.get(incarnation.worker_id)
            if old is None:
                self._workers[incarnation.worker_id] = WorkerSnapshot(
                    incarnation, protocol.WorkerMembershipState.ALIVE
                )
                return protocol.RegisterWorkerIncarnationReply(
                    incarnation, True
                )
            if old.incarnation != incarnation:
                return protocol.RegisterWorkerIncarnationReply(
                    incarnation, False,
                    "WorkerID is already bound to another incarnation",
                )
            if old.state is protocol.WorkerMembershipState.DEAD:
                return protocol.RegisterWorkerIncarnationReply(
                    incarnation, False,
                    "dead WorkerID cannot be registered again",
                )
            return protocol.RegisterWorkerIncarnationReply(incarnation, True)

    register_worker = register

    def report_death(
        self, request: protocol.ReportWorkerDeath
    ) -> protocol.ReportWorkerDeathReply:
        """Reduce one exact supervisor proof and append at most one fact."""

        if not isinstance(request, protocol.ReportWorkerDeath):
            raise TypeError("request must be a ReportWorkerDeath")
        with self._lock:
            prior = self._death_detections.get(request.detection_id)
            if prior is not None:
                prior_request, prior_death = prior
                if prior_request == request:
                    return self._death_reply_locked(
                        request, protocol.WorkerDeathDisposition.ALREADY_DEAD,
                        death=prior_death,
                    )
                return self._death_reply_locked(
                    request, protocol.WorkerDeathDisposition.CONFLICT,
                    error=(
                        "worker death detection_id was reused for another proof"
                    ),
                )

            old = self._workers.get(request.worker_id)
            if old is None:
                return self._death_reply_locked(
                    request, protocol.WorkerDeathDisposition.UNKNOWN,
                    error="unknown WorkerID",
                )
            if old.state is protocol.WorkerMembershipState.DEAD:
                assert old.death is not None
                if (
                    old.death.detection_id == request.detection_id
                    and old.death.incarnation == request.incarnation
                    and old.death.exit_code == request.exit_code
                    and old.death.reason is request.reason
                ):
                    return self._death_reply_locked(
                        request, protocol.WorkerDeathDisposition.ALREADY_DEAD,
                        death=old.death,
                    )
                return self._death_reply_locked(
                    request, protocol.WorkerDeathDisposition.CONFLICT,
                    error=(
                        "worker death report conflicts with the immutable "
                        "tombstone"
                    ),
                )
            if old.incarnation != request.incarnation:
                return self._death_reply_locked(
                    request, protocol.WorkerDeathDisposition.CONFLICT,
                    error="worker death report targets another incarnation",
                )

            self._death_epoch += 1
            death = protocol.WorkerDeathRecord(
                request.detection_id, request.incarnation, self._death_epoch,
                request.exit_code, request.reason,
            )
            self._workers[request.worker_id] = WorkerSnapshot(
                request.incarnation, protocol.WorkerMembershipState.DEAD, death
            )
            self._death_journal.append(death)
            self._death_detections[request.detection_id] = (request, death)
            return self._death_reply_locked(
                request, protocol.WorkerDeathDisposition.APPLIED, death=death
            )

    report_worker_death = report_death

    def get(self, worker_id: WorkerID) -> WorkerSnapshot:
        if not isinstance(worker_id, WorkerID):
            raise TypeError("worker_id must be a WorkerID")
        with self._lock:
            try:
                return self._workers[worker_id]
            except KeyError:
                raise KeyError("unknown WorkerID: {!r}".format(worker_id)) from None

    def get_state_reply(
        self, request: protocol.GetWorkerState
    ) -> protocol.GetWorkerStateReply:
        if not isinstance(request, protocol.GetWorkerState):
            raise TypeError("request must be a GetWorkerState")
        with self._lock:
            worker = self._workers.get(request.worker_id)
            if worker is None:
                return protocol.GetWorkerStateReply(
                    request.worker_id, False, self._death_epoch,
                    error="unknown WorkerID",
                )
            return protocol.GetWorkerStateReply(
                request.worker_id, True, self._death_epoch, worker.state,
                worker.incarnation, worker.death,
            )

    def deaths_after(
        self, request: protocol.GetWorkerDeaths
    ) -> protocol.GetWorkerDeathsReply:
        """Return the complete ordered suffix after a consumer watermark."""

        if not isinstance(request, protocol.GetWorkerDeaths):
            raise TypeError("request must be a GetWorkerDeaths")
        with self._lock:
            if request.after_epoch > self._death_epoch:
                raise ValueError(
                    "worker death cursor exceeds the current watermark"
                )
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, self._death_epoch,
                tuple(self._death_journal[request.after_epoch:]),
            )

    def fail_node(
        self, death: protocol.NodeDeathRecord
    ) -> Tuple[protocol.WorkerDeathRecord, ...]:
        """Commit ``NODE_EXIT`` for every live Worker on one dead Node.

        ``NodeRegistry`` has already committed ``death`` before this reducer is
        called.  The exact Node PID and registration epoch fence an older or
        unrelated Node incarnation.  Stable WorkerID ordering makes the global
        death journal deterministic for teaching traces and unit tests.

        Expected Node finalization deliberately produces no Worker-death facts:
        orderly Worker drain must release references explicitly and may not use
        a liveness tombstone to hide a broken shutdown protocol.
        """

        if not isinstance(death, protocol.NodeDeathRecord):
            raise TypeError("death must be a NodeDeathRecord")
        if death.reason is not protocol.NodeDeathReason.PROCESS_EXIT:
            return ()

        with self._lock:
            worksets = getattr(self, "_node_death_worksets", None)
            if worksets is None:
                worksets = {}
                self._node_death_worksets = worksets
            workset_key = (
                death.node_id, death.node_pid, death.registration_epoch,
                death.detection_id,
            )
            previous = worksets.get(workset_key)
            if previous is not None:
                return previous
            candidates = tuple(
                snapshot.incarnation
                for snapshot in sorted(
                    self._workers.values(),
                    key=lambda item: item.incarnation.worker_id.hex,
                )
                if (
                    snapshot.state is protocol.WorkerMembershipState.ALIVE
                    and snapshot.incarnation.node_id == death.node_id
                    and snapshot.incarnation.node_pid == death.node_pid
                    and snapshot.incarnation.node_registration_epoch
                    == death.registration_epoch
                )
            )
            committed = []  # type: list[protocol.WorkerDeathRecord]
            for incarnation in candidates:
                request = protocol.ReportWorkerDeath(
                    "node-exit:{}:{}".format(
                        death.detection_id, incarnation.worker_id.hex
                    ),
                    incarnation,
                    death.exit_code,
                    protocol.WorkerDeathReason.NODE_EXIT,
                )
                reply = self.report_death(request)
                if (
                    reply.disposition
                    not in (
                        protocol.WorkerDeathDisposition.APPLIED,
                        protocol.WorkerDeathDisposition.ALREADY_DEAD,
                    )
                    or reply.death is None
                ):
                    raise AssertionError(
                        "committed Node death could not fence its live Worker"
                    )
                if reply.disposition is protocol.WorkerDeathDisposition.APPLIED:
                    committed.append(reply.death)
            result = tuple(committed)
            worksets[workset_key] = result
            return result

    def snapshot(self) -> Tuple[WorkerSnapshot, ...]:
        with self._lock:
            return tuple(
                sorted(
                    self._workers.values(),
                    key=lambda worker: worker.incarnation.worker_id.hex,
                )
            )

    def _death_reply_locked(
        self, request: protocol.ReportWorkerDeath,
        disposition: protocol.WorkerDeathDisposition, *,
        death: Optional[protocol.WorkerDeathRecord] = None,
        error: Optional[str] = None,
    ) -> protocol.ReportWorkerDeathReply:
        return protocol.ReportWorkerDeathReply(
            request.detection_id, request.worker_id, disposition,
            self._death_epoch, death, error,
        )


ActorCreationState = protocol.ActorState
ActorSnapshot = protocol.ActorSnapshot


@dataclass
class _ActorRecord:
    request: CreateActorRequest
    generation: ActorGeneration
    state: protocol.ActorState = protocol.ActorState.CREATING
    route_epoch: int = 0
    restarts_used: int = 0
    last_exit: Optional[protocol.ActorWorkerExitRecord] = None
    node_id: Optional[NodeID] = None
    worker_id: Optional[WorkerID] = None
    worker_address: Optional[Address] = None
    worker_pid: Optional[int] = None
    error: Optional[str] = None
    initial_reservation: Optional[ReserveActorWorkerRequest] = None
    restart_reservation: Optional[ReserveActorWorkerRequest] = None


@dataclass(frozen=True)
class _ActorExitOutcome:
    """The immutable first reduction of one detection identity.

    The snapshot is retained even after later generations advance.  A Node
    replaying an older exact report can therefore receive proof that *that*
    exit was consumed without GCS re-exposing a now-stale physical endpoint.
    """

    record: protocol.ActorWorkerExitRecord
    reduction_snapshot: ActorSnapshot


class ActorRegistry:
    """The sole reducer for logical Actor generation and route state.

    Serialized construction data stays private.  Public snapshots never expose
    a physical endpoint outside ``ALIVE``.  Restart budget and generation are
    consumed together before any side effect, so replaying a worker-exit proof
    cannot create two generations.
    """

    def __init__(self) -> None:
        self._actors: dict[ActorID, _ActorRecord] = {}
        self._exit_detections: dict[str, _ActorExitOutcome] = {}
        self._lock = RLock()

    def begin(self, request: CreateActorRequest) -> bool:
        """Insert ``PENDING`` or validate an exact idempotent replay."""

        _require_create_actor_request(request)
        with self._lock:
            old = self._actors.get(request.actor_id)
            if old is None:
                self._actors[request.actor_id] = _ActorRecord(
                    request, request.generation
                )
                return True
            if old.request != request:
                raise ActorRegistrationConflictError(
                    "ActorID {} is already registered with different metadata".format(
                        request.actor_id
                    )
                )
            return False

    def contains(self, actor_id: ActorID) -> bool:
        """Return whether ``actor_id`` already owns a registry record."""

        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        with self._lock:
            return actor_id in self._actors

    def active_actor_ids(self) -> Tuple[ActorID, ...]:
        """Return every unresolved create/restart obligation in stable order."""

        with self._lock:
            return tuple(
                sorted(
                    (
                        actor_id
                        for actor_id, record in self._actors.items()
                        if record.state in (
                            protocol.ActorState.CREATING,
                            protocol.ActorState.RESTARTING,
                        )
                    ),
                    key=lambda value: value.hex,
                )
            )

    def drain_candidate(
        self, actor_id: ActorID
    ) -> tuple[CreateActorRequest, Optional[protocol.ActorWorkerExitRecord]]:
        """Snapshot the immutable driver for one active drain obligation."""

        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        with self._lock:
            record = self._actors.get(actor_id)
            if record is None or record.state not in (
                protocol.ActorState.CREATING, protocol.ActorState.RESTARTING
            ):
                raise ActorCreationStateError("Actor is not an active drain candidate")
            return record.request, record.last_exit

    def cancel_unreserved_create(
        self, request: CreateActorRequest, error: str
    ) -> Optional[ActorSnapshot]:
        """Cancel only a CREATING record with no frozen Node reservation."""

        _require_create_actor_request(request)
        if not isinstance(error, str) or not error:
            raise ValueError("cancelled Actor creation requires an error")
        with self._lock:
            record = self._get_matching_locked(request)
            if record.state is not protocol.ActorState.CREATING:
                return None
            if record.initial_reservation is not None:
                return None
            record.state = protocol.ActorState.DEAD
            record.route_epoch += 1
            record.error = error
            return self._snapshot_locked(record)

    def reply_for(self, request: CreateActorRequest) -> Optional[CreateActorReply]:
        """Return the current route, never a cached stale incarnation."""

        _require_create_actor_request(request)
        with self._lock:
            record = self._get_matching_locked(request)
            if record.state is protocol.ActorState.CREATING:
                return None
            if record.state is protocol.ActorState.ALIVE:
                return CreateActorReply(
                    record.request.actor_id, record.generation, True,
                    record.node_id, record.worker_id, record.worker_address,
                    record.worker_pid, route_epoch=record.route_epoch,
                )
            return CreateActorReply(
                record.request.actor_id, record.generation, False,
                error=record.error or "Actor route is not currently available",
                retryable=record.state is protocol.ActorState.RESTARTING,
                route_epoch=record.route_epoch,
            )

    def initial_reservation_for(
        self, request: CreateActorRequest
    ) -> Optional[ReserveActorWorkerRequest]:
        with self._lock:
            return self._get_matching_locked(request).initial_reservation

    def install_initial_reservation(
        self, request: CreateActorRequest, reservation: ReserveActorWorkerRequest
    ) -> ReserveActorWorkerRequest:
        if not isinstance(reservation, ReserveActorWorkerRequest):
            raise TypeError("reservation must be a ReserveActorWorkerRequest")
        with self._lock:
            record = self._get_matching_locked(request)
            if record.state is not protocol.ActorState.CREATING:
                raise ActorCreationStateError(
                    "only a creating Actor can install its initial reservation"
                )
            if record.initial_reservation is None:
                record.initial_reservation = reservation
            elif record.initial_reservation != reservation:
                raise ActorCreationStateError(
                    "Actor already has a different initial reservation"
                )
            return record.initial_reservation

    def finish(
        self, request: CreateActorRequest, reply: CreateActorReply
    ) -> bool:
        """Reduce one initial-create outcome.

        Retryable replies describe transport ambiguity and deliberately leave
        the record in CREATING so an exact request can replay the same frozen
        Node reservation.
        """

        _require_create_actor_request(request)
        if not isinstance(reply, CreateActorReply):
            raise TypeError("reply must be a CreateActorReply")
        if reply.actor_id != request.actor_id or reply.generation != request.generation:
            raise ActorCreationStateError(
                "actor creation reply does not match its request identity"
            )
        with self._lock:
            record = self._get_matching_locked(request)
            if record.state is not protocol.ActorState.CREATING:
                current = self.reply_for(request)
                if current == reply:
                    return False
                raise ActorCreationStateError(
                    "only a creating Actor can publish its initial outcome"
                )
            if reply.retryable:
                return False
            record.route_epoch = max(record.route_epoch + 1, reply.route_epoch)
            if reply.accepted:
                record.state = protocol.ActorState.ALIVE
                record.node_id = reply.node_id
                record.worker_id = reply.worker_id
                record.worker_address = reply.worker_address
                record.worker_pid = reply.worker_pid
                record.error = None
            else:
                record.state = protocol.ActorState.DEAD
                record.error = reply.error
            return True

    def accept_worker_exit(
        self,
        exit_record: protocol.ActorWorkerExitRecord,
        node: NodeSnapshot,
        *,
        restart_allowed: bool = True,
    ) -> protocol.ActorWorkerExitDisposition:
        """Atomically validate an exit and allocate at most one generation."""

        if not isinstance(exit_record, protocol.ActorWorkerExitRecord):
            raise TypeError("exit_record must be an ActorWorkerExitRecord")
        if not isinstance(node, NodeSnapshot):
            raise TypeError("node must be a NodeSnapshot")
        with self._lock:
            prior = self._exit_detections.get(exit_record.detection_id)
            if prior is not None:
                return (
                    protocol.ActorWorkerExitDisposition.ALREADY_APPLIED
                    if prior.record == exit_record
                    else protocol.ActorWorkerExitDisposition.CONFLICT
                )
            record = self._actors.get(exit_record.actor_id)
            if record is None:
                return protocol.ActorWorkerExitDisposition.UNKNOWN
            if record.state is not protocol.ActorState.ALIVE:
                if exit_record.generation.generation < record.generation.generation:
                    return protocol.ActorWorkerExitDisposition.STALE
                return protocol.ActorWorkerExitDisposition.CONFLICT
            if exit_record.generation.generation < record.generation.generation:
                return protocol.ActorWorkerExitDisposition.STALE
            if exit_record.generation != record.generation:
                return protocol.ActorWorkerExitDisposition.CONFLICT
            if (
                node.state is not protocol.NodeMembershipState.ALIVE
                or node.node_id != exit_record.node_id
                or node.node_pid != exit_record.node_pid
                or node.registration_epoch != exit_record.registration_epoch
                or record.node_id != exit_record.node_id
                or record.worker_id != exit_record.worker_id
                or record.worker_pid != exit_record.worker_pid
                or record.route_epoch != exit_record.route_epoch
            ):
                return protocol.ActorWorkerExitDisposition.CONFLICT

            record.last_exit = exit_record
            record.node_id = None
            record.worker_id = None
            record.worker_address = None
            record.worker_pid = None
            record.route_epoch += 1
            if (
                not restart_allowed
                or record.restarts_used >= record.request.max_restarts
            ):
                record.state = protocol.ActorState.DEAD
                record.error = (
                    "Actor restart admission is closed"
                    if not restart_allowed
                    else "Actor restart budget exhausted"
                )
                record.restart_reservation = None
                self._exit_detections[exit_record.detection_id] = (
                    _ActorExitOutcome(exit_record, self._snapshot_locked(record))
                )
                return protocol.ActorWorkerExitDisposition.APPLIED

            record.restarts_used += 1
            record.generation = record.generation.next()
            record.state = protocol.ActorState.RESTARTING
            record.error = None
            record.restart_reservation = ReserveActorWorkerRequest(
                record.request.actor_id, record.generation,
                record.request.class_definition, record.request.constructor_payload,
                record.request.resources, record.request.owner_worker_id,
                exit_record.node_id, record.route_epoch + 1, exit_record,
            )
            self._exit_detections[exit_record.detection_id] = _ActorExitOutcome(
                exit_record, self._snapshot_locked(record)
            )
            return protocol.ActorWorkerExitDisposition.APPLIED

    def restart_reservation_for(
        self, exit_record: protocol.ActorWorkerExitRecord
    ) -> Optional[ReserveActorWorkerRequest]:
        with self._lock:
            record = self._get_restart_record_locked(exit_record)
            return record.restart_reservation

    def replay_state_for_exit(
        self, exit_record: protocol.ActorWorkerExitRecord
    ) -> tuple[ActorSnapshot, bool]:
        """Return an ACK-safe snapshot and whether this is the latest exit."""

        with self._lock:
            outcome = self._get_exit_outcome_locked(exit_record)
            record = self._actors[exit_record.actor_id]
            if record.last_exit == exit_record:
                return self._snapshot_locked(record), True
            return outcome.reduction_snapshot, False

    def publish_restart(
        self,
        exit_record: protocol.ActorWorkerExitRecord,
        reply: ReserveActorWorkerReply,
    ) -> ActorSnapshot:
        if not isinstance(reply, ReserveActorWorkerReply):
            raise TypeError("reply must be a ReserveActorWorkerReply")
        with self._lock:
            record = self._get_restart_record_locked(exit_record)
            reservation = record.restart_reservation
            if record.state is protocol.ActorState.ALIVE:
                return self._snapshot_locked(record)
            if (
                record.state is not protocol.ActorState.RESTARTING
                or reservation is None
                or not reply.accepted
                or reply.actor_id != reservation.actor_id
                or reply.generation != reservation.generation
                or reply.node_id != reservation.target_node_id
            ):
                raise ActorCreationStateError(
                    "Actor restart reply does not match the frozen reservation"
                )
            record.state = protocol.ActorState.ALIVE
            record.route_epoch = reservation.route_epoch
            record.node_id = reply.node_id
            record.worker_id = reply.worker_id
            record.worker_address = reply.worker_address
            record.worker_pid = reply.worker_pid
            record.error = None
            return self._snapshot_locked(record)

    def fail_restart(
        self, exit_record: protocol.ActorWorkerExitRecord, error: str
    ) -> ActorSnapshot:
        """Commit one frozen, explicitly rejected restart as terminal.

        This is a strict compare-and-set: only the RESTARTING record created by
        ``exit_record`` may transition.  Transport ambiguity never calls this
        reducer.  The terminal publication consumes the reservation's route
        epoch, so it orders after the already-installed RESTARTING fence.
        """

        if not isinstance(
            exit_record,
            protocol.ActorWorkerExitRecord,
        ):
            raise TypeError("exit_record must be an Actor restart proof")
        if not isinstance(error, str) or not error:
            raise ValueError("failed Actor restart requires an error")
        with self._lock:
            record = self._get_restart_record_locked(exit_record)
            reservation = record.restart_reservation
            if (
                record.state is not protocol.ActorState.RESTARTING
                or record.last_exit != exit_record
                or reservation is None
                or reservation.restart != exit_record
                or reservation.actor_id != record.request.actor_id
                or reservation.generation != record.generation
            ):
                raise ActorCreationStateError(
                    "Actor restart failure does not match the frozen reservation"
                )
            record.state = protocol.ActorState.DEAD
            record.route_epoch = reservation.route_epoch
            record.restart_reservation = None
            record.error = error
            return self._snapshot_locked(record)

    def fail_actor_on_node(
        self, actor_id: ActorID, node_id: NodeID, error: str
    ) -> Optional[ActorSnapshot]:
        """Fail one Actor iff its current or pending route uses ``node_id``."""

        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        if not isinstance(node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        if not isinstance(error, str) or not error:
            raise ValueError("actor node failure requires an error")
        with self._lock:
            record = self._actors.get(actor_id)
            if record is None:
                return None
            target = record.node_id
            if (
                target is None
                and record.state is protocol.ActorState.CREATING
                and record.initial_reservation is not None
            ):
                target = record.initial_reservation.target_node_id
            if target is None and record.restart_reservation is not None:
                target = record.restart_reservation.target_node_id
            if (
                target != node_id
                or record.state not in (
                    protocol.ActorState.CREATING, protocol.ActorState.ALIVE,
                    protocol.ActorState.RESTARTING,
                )
            ):
                return None
            record.state = protocol.ActorState.DEAD
            if record.initial_reservation is not None:
                record.route_epoch = max(
                    record.route_epoch + 1,
                    record.initial_reservation.route_epoch,
                )
            else:
                record.route_epoch += 1
            record.node_id = None
            record.worker_id = None
            record.worker_address = None
            record.worker_pid = None
            record.initial_reservation = None
            record.restart_reservation = None
            record.error = error
            return self._snapshot_locked(record)

    def get(self, actor_id: ActorID) -> ActorSnapshot:
        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        with self._lock:
            try:
                return self._snapshot_locked(self._actors[actor_id])
            except KeyError:
                raise UnknownActorError(
                    "unknown Actor: {}".format(actor_id)
                ) from None

    def get_state_reply(
        self, request: protocol.GetActorState
    ) -> protocol.GetActorStateReply:
        if not isinstance(request, protocol.GetActorState):
            raise TypeError("request must be a GetActorState")
        try:
            snapshot = self.get(request.actor_id)
        except UnknownActorError as exc:
            return protocol.GetActorStateReply(
                request.actor_id, False, error=str(exc)
            )
        return protocol.GetActorStateReply(request.actor_id, True, snapshot)

    def owner_install_target(
        self, actor_id: ActorID
    ) -> tuple[WorkerID, Optional[Address]]:
        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        with self._lock:
            try:
                request = self._actors[actor_id].request
            except KeyError:
                raise UnknownActorError(
                    "unknown Actor: {}".format(actor_id)
                ) from None
            return request.owner_worker_id, request.owner_address

    def snapshot(self) -> Tuple[ActorSnapshot, ...]:
        with self._lock:
            return tuple(
                self._snapshot_locked(record)
                for _actor_id, record in sorted(
                    self._actors.items(), key=lambda item: item[0].hex
                )
            )

    def _get_matching_locked(self, request: CreateActorRequest) -> _ActorRecord:
        try:
            record = self._actors[request.actor_id]
        except KeyError:
            raise UnknownActorError(
                "unknown Actor: {}".format(request.actor_id)
            ) from None
        if record.request != request:
            raise ActorRegistrationConflictError(
                "ActorID {} is already registered with different metadata".format(
                    request.actor_id
                )
            )
        return record

    def _get_exit_record_locked(
        self, exit_record: protocol.ActorWorkerExitRecord
    ) -> _ActorRecord:
        self._get_exit_outcome_locked(exit_record)
        return self._actors[exit_record.actor_id]

    def _get_restart_record_locked(
        self, proof: protocol.ActorWorkerExitRecord
    ) -> _ActorRecord:
        if not isinstance(proof, protocol.ActorWorkerExitRecord):
            raise TypeError("proof must be an ActorWorkerExitRecord")
        return self._get_exit_record_locked(proof)

    def _get_exit_outcome_locked(
        self, exit_record: protocol.ActorWorkerExitRecord
    ) -> _ActorExitOutcome:
        prior = self._exit_detections.get(exit_record.detection_id)
        if prior is None or prior.record != exit_record:
            raise ActorCreationStateError(
                "Actor exit was not accepted by this registry"
            )
        return prior

    @staticmethod
    def _snapshot_locked(record: _ActorRecord) -> ActorSnapshot:
        alive = record.state is protocol.ActorState.ALIVE
        return protocol.ActorSnapshot(
            actor_id=record.request.actor_id,
            generation=record.generation,
            state=record.state,
            route_epoch=record.route_epoch,
            restarts_used=record.restarts_used,
            max_restarts=record.request.max_restarts,
            last_exit=record.last_exit,
            node_id=record.node_id if alive else None,
            worker_id=record.worker_id if alive else None,
            worker_address=record.worker_address if alive else None,
            worker_pid=record.worker_pid if alive else None,
            error=record.error,
        )


ActorReserve = Callable[[Address, ReserveActorWorkerRequest], object]
ActorStateInstall = Callable[[Address, protocol.InstallActorState], object]
PlacementGroupParticipantRPC = Callable[[Address, str, object], object]
OwnerFenceRPC = Callable[[Address, str, object], object]


@dataclass(frozen=True)
class PlacementGroupPrepareFailureConfig:
    """Spawn-safe, test-only typed PREPARE rejection.

    ``participant_ordinal`` is one-based in the coordinator's immutable,
    NodeID-sorted participant order.  Every earlier participant still crosses
    the real Node RPC boundary and acknowledges PREPARED before this seam can
    reject the selected participant.  The first PG identity observed becomes
    immutable: exact create replays reproduce the same typed rejection, while
    another PG is unaffected.
    """

    participant_ordinal: int
    error: str = "injected placement-group prepare rejection"

    def __post_init__(self) -> None:
        if (
            isinstance(self.participant_ordinal, bool)
            or not isinstance(self.participant_ordinal, int)
            or self.participant_ordinal <= 0
        ):
            raise ValueError(
                "participant_ordinal must be a positive integer"
            )
        if not isinstance(self.error, str) or not self.error:
            raise ValueError("prepare failure error must be non-empty")


class _PlacementGroupPrepareFailureRPC:
    """Identity-bound wrapper around the production participant RPC."""

    def __init__(
        self,
        config: PlacementGroupPrepareFailureConfig,
        participant_rpc: "PlacementGroupParticipantRPC",
        event_sink: Optional[EventSink] = None,
    ) -> None:
        self._config = config
        self._participant_rpc = participant_rpc
        self._event_sink = event_sink
        self._attempt: Optional[tuple[PlacementGroupID, int]] = None
        self._participants: list[NodeID] = []
        self._prepared: set[NodeID] = set()
        self._rejected_request: Optional[
            protocol.PreparePlacementGroupRequest
        ] = None
        self._rejected_reply: Optional[
            protocol.PreparePlacementGroupReply
        ] = None
        self._rejection_observed = False

    def __call__(self, address: Address, handler: str, request: object) -> object:
        if (
            handler != PREPARE_PLACEMENT_GROUP_HANDLER
            or not isinstance(request, protocol.PreparePlacementGroupRequest)
        ):
            return self._participant_rpc(address, handler, request)

        identity = (request.placement_group_id, request.attempt)
        if self._attempt is None:
            self._attempt = identity
        if identity != self._attempt:
            return self._participant_rpc(address, handler, request)

        if request.node_id not in self._participants:
            self._participants.append(request.node_id)
        ordinal = self._participants.index(request.node_id) + 1
        if ordinal != self._config.participant_ordinal:
            reply = self._participant_rpc(address, handler, request)
            if (
                not isinstance(reply, protocol.PreparePlacementGroupReply)
                or not reply.accepted
                or not reply.applied
            ):
                return reply
            self._prepared.add(request.node_id)
            return reply

        if len(self._prepared) < self._config.participant_ordinal - 1:
            raise RuntimeError(
                "prepare-failure checkpoint reached before its participant "
                "prefix acknowledged PREPARED"
            )

        previous = self._rejected_request
        if previous is not None and previous != request:
            raise PlacementGroupConflictError(
                "prepare-failure replay changed participant identity"
            )
        if previous is not None:
            assert self._rejected_reply is not None
            return self._rejected_reply
        self._rejected_request = request
        reply = protocol.PreparePlacementGroupReply(
            request.placement_group_id,
            request.attempt,
            request.node_id,
            request.plan_digest,
            request.phase,
            False,
            False,
            self._config.error,
        )
        self._rejected_reply = reply
        return reply

    def observe_rejection(
        self, request: protocol.PreparePlacementGroupRequest
    ) -> None:
        """Emit once after the reducer has committed PREPARING→ABORTING."""

        if (
            self._rejection_observed
            or self._rejected_request is None
            or request != self._rejected_request
        ):
            return
        self._rejection_observed = True
        if self._event_sink is not None:
            try:
                self._event_sink.emit(
                    "placement_group_prepare_rejected_by_failpoint",
                    component="gcs",
                    placement_group_id=str(request.placement_group_id),
                    attempt=request.attempt,
                    node_id=str(request.node_id),
                    plan_digest=request.plan_digest,
                    participant_ordinal=self._config.participant_ordinal,
                    prepared_prefix=tuple(
                        str(node_id)
                        for node_id in self._participants[
                            : self._config.participant_ordinal - 1
                        ]
                    ),
                    status="REJECTED",
                    applied=False,
                )
            except Exception:
                # Test observation, like every runtime trace, has no authority
                # over the typed participant decision.
                pass


class ActorCoordinator:
    """Select a NodeManager and atomically publish its Actor endpoint.

    Hybrid scheduling uses one immutable registry snapshot.  The selected node
    remains the final resource authority: it may reject this eventually
    consistent view.  A per-Actor lock serializes duplicate RPCs without
    blocking creation of unrelated Actors.
    """

    def __init__(
        self,
        nodes: NodeRegistry,
        actors: ActorRegistry,
        *,
        policy: Optional[HybridPolicy] = None,
        reserve_actor_worker: Optional[ActorReserve] = None,
        install_actor_state: Optional[ActorStateInstall] = None,
    ) -> None:
        if not isinstance(nodes, NodeRegistry):
            raise TypeError("nodes must be a NodeRegistry")
        if not isinstance(actors, ActorRegistry):
            raise TypeError("actors must be an ActorRegistry")
        self._nodes = nodes
        self._actors = actors
        self._policy = policy if policy is not None else HybridPolicy(seed=0)
        self._reserve_actor_worker = (
            reserve_actor_worker
            if reserve_actor_worker is not None
            else _reserve_actor_worker_rpc
        )
        self._install_actor_state = (
            install_actor_state
            if install_actor_state is not None
            else _install_actor_state_rpc
        )
        self._actor_locks: dict[ActorID, RLock] = {}
        self._locks_lock = RLock()
        self._admission_lock = RLock()
        self._admission_open = True
        self._node_failure_publications: dict[ActorID, ActorSnapshot] = {}

    def create(self, request: CreateActorRequest) -> CreateActorReply:
        _require_create_actor_request(request)
        with self._lock_for(request.actor_id):
            with self._admission_lock:
                # Admission fences only *new* logical Actors.  Once an ActorID
                # owns a record, exact replay must remain able to query a
                # terminal route or redrive its frozen, ambiguity-safe initial
                # reservation during shutdown.  Holding this lock across the
                # probe and insert linearizes a first create with close_admission.
                if (
                    not self._admission_open
                    and not self._actors.contains(request.actor_id)
                ):
                    return _reject_actor(
                        request, "Actor creation admission is closed"
                    )
                try:
                    created = self._actors.begin(request)
                except ActorRegistrationConflictError as exc:
                    return _reject_actor(request, str(exc))
            if not created:
                cached = self._actors.reply_for(request)
                if cached is not None:
                    return cached
            reply = self._create_once(request)
            self._actors.finish(request, reply)
            return reply

    def _create_once(self, request: CreateActorRequest) -> CreateActorReply:
        reservation = self._actors.initial_reservation_for(request)
        selected = None
        if reservation is None:
            nodes = self._nodes.snapshot()
            decision = self._policy.schedule(
                request.resources,
                tuple(node.to_scheduling_snapshot() for node in nodes),
                require_available=True,
            )
            if decision.status is SchedulingStatus.INFEASIBLE:
                return _reject_actor(
                    request, "no live node is feasible for the Actor resources"
                )
            if decision.status is SchedulingStatus.PENDING_CAPACITY:
                return _reject_actor(
                    request,
                    "Actor resources are feasible but currently unavailable",
                    retryable=True,
                )
            selected = next(
                (node for node in nodes if node.node_id == decision.node_id), None
            )
            if selected is None:
                return _reject_actor(
                    request, "scheduler selected an unknown node", retryable=True
                )
            reservation = self._actors.install_initial_reservation(
                request,
                ReserveActorWorkerRequest(
                    request.actor_id, request.generation, request.class_definition,
                    request.constructor_payload, request.resources,
                    request.owner_worker_id, selected.node_id, 1, None,
                ),
            )
        if selected is None:
            try:
                selected = self._nodes.get(reservation.target_node_id)
            except (UnknownNodeError, DeadNodeError):
                return _reject_actor(
                    request, "Actor reservation target is no longer live",
                    retryable=True,
                )
        try:
            reserved = self._reserve_actor_worker(selected.address, reservation)
        except Exception as exc:
            return _reject_actor(
                request,
                "NodeManager Actor reservation failed: {}: {}".format(
                    type(exc).__name__, exc
                ),
                retryable=True,
            )
        if not isinstance(reserved, ReserveActorWorkerReply):
            return _reject_actor(
                request, "NodeManager returned an invalid Actor reservation reply",
                retryable=True,
            )
        if (
            reserved.actor_id != request.actor_id
            or reserved.generation != request.generation
        ):
            return _reject_actor(
                request, "NodeManager returned the wrong Actor identity",
                retryable=True,
            )
        if not reserved.accepted:
            return _reject_actor(
                request, reserved.error or "NodeManager rejected Actor creation"
            )
        if reserved.node_id != selected.node_id:
            return _reject_actor(
                request, "NodeManager returned the wrong Actor node"
            )

        return CreateActorReply(
            request.actor_id,
            request.generation,
            accepted=True,
            node_id=reserved.node_id,
            worker_id=reserved.worker_id,
            worker_address=reserved.worker_address,
            worker_pid=reserved.worker_pid,
            route_epoch=reservation.route_epoch,
        )

    def report_worker_exit(
        self, message: protocol.ReportActorWorkerExit
    ) -> protocol.ReportActorWorkerExitReply:
        if not isinstance(message, protocol.ReportActorWorkerExit):
            raise TypeError("message must be a ReportActorWorkerExit")
        with self._lock_for(message.record.actor_id):
            return self._report_worker_exit_locked(message.record)

    def _report_worker_exit_locked(
        self, exit_record: protocol.ActorWorkerExitRecord
    ) -> protocol.ReportActorWorkerExitReply:
        try:
            node = self._nodes.get(exit_record.node_id)
        except UnknownNodeError:
            return self._exit_error(
                exit_record, protocol.ActorWorkerExitDisposition.UNKNOWN,
                "Actor exit names an unknown Node",
            )
        with self._admission_lock:
            restart_allowed = self._admission_open
        disposition = self._actors.accept_worker_exit(
            exit_record, node, restart_allowed=restart_allowed
        )
        if disposition not in (
            protocol.ActorWorkerExitDisposition.APPLIED,
            protocol.ActorWorkerExitDisposition.ALREADY_APPLIED,
        ):
            return self._exit_error(
                exit_record, disposition,
                "Actor exit is stale or conflicts with authoritative state",
            )

        snapshot, current_exit = self._actors.replay_state_for_exit(exit_record)
        if not current_exit:
            return protocol.ReportActorWorkerExitReply(
                exit_record,
                protocol.ActorWorkerExitDisposition.ALREADY_APPLIED,
                snapshot,
            )
        if snapshot.state is protocol.ActorState.RESTARTING:
            install_error = self._install_snapshot(exit_record.actor_id, snapshot)
            if install_error is not None:
                return self._exit_error(
                    exit_record, protocol.ActorWorkerExitDisposition.RETRYABLE,
                    install_error, snapshot,
                )
            reservation = self._actors.restart_reservation_for(exit_record)
            assert reservation is not None
            try:
                node = self._nodes.get(reservation.target_node_id)
                if (
                    node.state is not protocol.NodeMembershipState.ALIVE
                    or node.node_pid != exit_record.node_pid
                    or node.registration_epoch != exit_record.registration_epoch
                ):
                    raise DeadNodeError("restart target incarnation is not live")
                reserved = self._reserve_actor_worker(node.address, reservation)
            except Exception as exc:
                return self._exit_error(
                    exit_record, protocol.ActorWorkerExitDisposition.RETRYABLE,
                    "Actor restart reservation unresolved: {}: {}".format(
                        type(exc).__name__, exc
                    ), snapshot,
                )
            if (
                isinstance(reserved, ReserveActorWorkerReply)
                and reserved.actor_id == reservation.actor_id
                and reserved.generation == reservation.generation
                and not reserved.accepted
            ):
                snapshot = self._actors.fail_restart(
                    exit_record,
                    reserved.error or "NodeManager rejected the Actor restart",
                )
                install_error = self._install_snapshot(
                    exit_record.actor_id, snapshot
                )
                if install_error is not None:
                    return self._exit_error(
                        exit_record,
                        protocol.ActorWorkerExitDisposition.RETRYABLE,
                        install_error,
                        snapshot,
                    )
                return protocol.ReportActorWorkerExitReply(
                    exit_record, disposition, snapshot
                )
            if (
                not isinstance(reserved, ReserveActorWorkerReply)
                or not reserved.accepted
                or reserved.actor_id != reservation.actor_id
                or reserved.generation != reservation.generation
                or reserved.node_id != reservation.target_node_id
                or reserved.worker_id == exit_record.worker_id
                or reserved.worker_pid == exit_record.worker_pid
            ):
                return self._exit_error(
                    exit_record, protocol.ActorWorkerExitDisposition.RETRYABLE,
                    "NodeManager did not confirm the frozen Actor restart",
                    snapshot,
                )
            current_node = self._nodes.get(reservation.target_node_id)
            if (
                current_node.state is not protocol.NodeMembershipState.ALIVE
                or current_node.node_pid != exit_record.node_pid
                or current_node.registration_epoch != exit_record.registration_epoch
            ):
                return self._exit_error(
                    exit_record, protocol.ActorWorkerExitDisposition.RETRYABLE,
                    "Actor restart target died before route publication",
                    snapshot,
                )
            snapshot = self._actors.publish_restart(exit_record, reserved)

        install_error = self._install_snapshot(exit_record.actor_id, snapshot)
        if install_error is not None:
            return self._exit_error(
                exit_record, protocol.ActorWorkerExitDisposition.RETRYABLE,
                install_error, snapshot,
            )
        return protocol.ReportActorWorkerExitReply(
            exit_record, disposition, snapshot
        )

    def fail_node(
        self, node_id: NodeID, error: str, *, require_owner_ack: bool = True
    ) -> Tuple[ActorSnapshot, ...]:
        if not isinstance(require_owner_ack, bool):
            raise TypeError("require_owner_ack must be a bool")
        for visible in self._actors.snapshot():
            with self._lock_for(visible.actor_id):
                snapshot = self._actors.fail_actor_on_node(
                    visible.actor_id, node_id, error
                )
                if snapshot is not None:
                    if require_owner_ack:
                        self._node_failure_publications[snapshot.actor_id] = snapshot
                    else:
                        # EXPECTED unregister happens only after the Driver owner
                        # crossed the clean barrier.  Retire any older obligation
                        # without contacting that intentionally closed endpoint.
                        self._node_failure_publications.pop(
                            snapshot.actor_id, None
                        )
        return (
            self._flush_node_failure_publications()
            if require_owner_ack
            else ()
        )

    def node_failure_states_converged(self) -> bool:
        """DEAD is acknowledged only after every queued owner install completes."""

        return not self._node_failure_publications

    def _flush_node_failure_publications(self) -> Tuple[ActorSnapshot, ...]:
        """Try every pending DEAD install once; delete only exact ACKs."""

        installed = []
        for actor_id, snapshot in tuple(self._node_failure_publications.items()):
            if self._install_snapshot(actor_id, snapshot) is None:
                if self._node_failure_publications.get(actor_id) == snapshot:
                    del self._node_failure_publications[actor_id]
                installed.append(snapshot)
        return tuple(installed)

    def active_operation_ids(self) -> Tuple[ActorID, ...]:
        """Union lifecycle work and pending owner publications."""

        return tuple(
            sorted(
                set(self._actors.active_actor_ids()).union(
                    self._node_failure_publications
                ),
                key=lambda value: value.hex,
            )
        )

    def close_admission(self) -> None:
        """Fence new Actor creation and restart intents during shutdown."""

        with self._admission_lock:
            self._admission_open = False

    def drain_once(self) -> Tuple[ActorID, ...]:
        """Drive every active Actor obligation once without short-circuiting."""

        self.close_admission()
        for actor_id in self._actors.active_actor_ids():
            with self._lock_for(actor_id):
                try:
                    request, last_exit = self._actors.drain_candidate(actor_id)
                except ActorCreationStateError:
                    continue
                snapshot = self._actors.get(actor_id)
                if snapshot.state is protocol.ActorState.CREATING:
                    if self._actors.initial_reservation_for(request) is None:
                        self._actors.cancel_unreserved_create(
                            request, "Actor creation cancelled by drain"
                        )
                        continue
                    reply = self._create_once(request)
                    self._actors.finish(request, reply)
                    continue
                if (
                    snapshot.state is protocol.ActorState.RESTARTING
                    and last_exit is not None
                ):
                    self._report_worker_exit_locked(last_exit)
        self._flush_node_failure_publications()
        return self.active_operation_ids()

    def has_active_operations(self) -> bool:
        return bool(self.active_operation_ids())

    def _install_snapshot(
        self, actor_id: ActorID, snapshot: ActorSnapshot
    ) -> Optional[str]:
        owner_worker_id, address = self._actors.owner_install_target(actor_id)
        if address is None:
            return None
        request = protocol.InstallActorState(
            owner_worker_id, snapshot
        )
        try:
            reply = self._install_actor_state(address, request)
        except Exception as exc:
            return "Actor owner state install failed: {}: {}".format(
                type(exc).__name__, exc
            )
        if (
            not isinstance(reply, protocol.InstallActorStateReply)
            or reply.owner_worker_id != request.owner_worker_id
            or reply.snapshot != snapshot
            or not reply.installed
        ):
            return "Actor owner returned an invalid state-install reply"
        return None

    @staticmethod
    def _exit_error(
        exit_record: protocol.ActorWorkerExitRecord,
        disposition: protocol.ActorWorkerExitDisposition,
        error: str,
        snapshot: Optional[ActorSnapshot] = None,
    ) -> protocol.ReportActorWorkerExitReply:
        return protocol.ReportActorWorkerExitReply(
            exit_record, disposition, snapshot, error
        )

    def _lock_for(self, actor_id: ActorID) -> RLock:
        with self._locks_lock:
            return self._actor_locks.setdefault(actor_id, RLock())


class PlacementGroupControlCoordinator:
    """Drive one frozen PG plan across its authoritative Node participants.

    The contained :class:`PlacementGroupCoordinator` is the only GCS state
    reducer.  This adapter performs typed RPC and feeds only identity-complete
    replies back into it.  Transport ambiguity leaves the operation in
    ``next_operations`` so an exact create/remove replay converges it.  Node
    availability is deliberately never adjusted here: root and committed child
    ledgers remain NodeManager-owned allocation truth.
    """

    def __init__(
        self,
        nodes: NodeRegistry,
        *,
        coordinator: Optional[PlacementGroupCoordinator] = None,
        participant_rpc: Optional[PlacementGroupParticipantRPC] = None,
    ) -> None:
        if not isinstance(nodes, NodeRegistry):
            raise TypeError("nodes must be a NodeRegistry")
        self._nodes = nodes
        self._coordinator = coordinator or PlacementGroupCoordinator()
        self._participant_rpc = (
            participant_rpc
            if participant_rpc is not None
            else _placement_group_participant_rpc
        )
        self._known_requests: dict[
            PlacementGroupID, protocol.CreatePlacementGroupRequest
        ] = {}
        # The pure reducer owns indexes spanning all PGs (notably committed
        # Node-death proofs), so reducer calls must be globally serialized even
        # though independent participant RPCs retain their per-PG locks.
        self._coordinator_lock = RLock()
        self._accepting_new = True
        self._admission_lock = RLock()
        self._locks: dict[PlacementGroupID, RLock] = {}
        self._locks_lock = RLock()

    def create(
        self, request: protocol.CreatePlacementGroupRequest
    ) -> protocol.CreatePlacementGroupReply:
        if not isinstance(request, protocol.CreatePlacementGroupRequest):
            raise TypeError("create expects CreatePlacementGroupRequest")
        with self._lock_for(request.placement_group_id):
            with self._admission_lock:
                admission_open = self._accepting_new
                existing_request = self._known_requests.get(
                    request.placement_group_id
                )
                exact_replay = existing_request == request
                if existing_request is None:
                    if not self._accepting_new:
                        return protocol.CreatePlacementGroupReply(
                            request.placement_group_id, request.attempt, False,
                            protocol.PlacementGroupPhaseStatus.REMOVED,
                            error=(
                                "GCS placement-group admission is closed for "
                                "shutdown"
                            ),
                        )
                    self._known_requests[request.placement_group_id] = request
                elif existing_request != request and not self._accepting_new:
                    try:
                        phase = self._phase_status(
                            self._coordinator.snapshot(
                                request.placement_group_id
                            ).phase
                        )
                    except KeyError:
                        phase = protocol.PlacementGroupPhaseStatus.REMOVED
                    return protocol.CreatePlacementGroupReply(
                        request.placement_group_id, request.attempt, False, phase,
                        error=(
                            "GCS shutdown permits only an exact replay of an "
                            "existing placement-group request"
                        ),
                    )
            spec = PlacementGroupSpec(
                request.placement_group_id,
                tuple(
                    Bundle(bundle.bundle_index, bundle.resources)
                    for bundle in request.bundles
                ),
                PlacementStrategy(request.strategy),
            )
            try:
                snapshot = self._coordinator.create(
                    spec, self._nodes.scheduling_snapshot(),
                    attempt_number=request.attempt,
                )
            except PlacementGroupConflictError as exc:
                try:
                    conflict_phase = self._phase_status(
                        self._coordinator.snapshot(
                            request.placement_group_id
                        ).phase
                    )
                except KeyError:
                    # A conflict normally proves an existing record.  Keep a
                    # typed terminal sentinel for defensive custom reducers that
                    # raise before publishing one; no placement is exposed.
                    conflict_phase = protocol.PlacementGroupPhaseStatus.REMOVED
                return protocol.CreatePlacementGroupReply(
                    request.placement_group_id, request.attempt, False,
                    conflict_phase,
                    error=str(exc),
                )
            if (
                exact_replay
                and admission_open
                and snapshot.phase is PlacementGroupPhase.PENDING
            ):
                snapshot = self._coordinator.retry_pending(
                    request.placement_group_id,
                    self._nodes.scheduling_snapshot(),
                )

            snapshot, drive_error = self._drive(snapshot)
            if snapshot.phase is PlacementGroupPhase.CREATED:
                return protocol.CreatePlacementGroupReply(
                    request.placement_group_id, request.attempt, True,
                    self._phase_status(snapshot.phase),
                    self._placement_keys(snapshot),
                )
            return protocol.CreatePlacementGroupReply(
                request.placement_group_id, request.attempt, False,
                self._phase_status(snapshot.phase),
                error=(
                    drive_error or snapshot.reason
                    or "placement group is {}".format(snapshot.phase.value)
                ),
            )

    def get(
        self, request: protocol.GetPlacementGroupRequest
    ) -> protocol.GetPlacementGroupReply:
        if not isinstance(request, protocol.GetPlacementGroupRequest):
            raise TypeError("get expects GetPlacementGroupRequest")
        with self._lock_for(request.placement_group_id):
            try:
                snapshot = self._coordinator.snapshot(request.placement_group_id)
            except KeyError:
                return protocol.GetPlacementGroupReply(
                    request.placement_group_id, False,
                    error="unknown placement group",
                )
            placements = (
                self._placement_keys(snapshot)
                if snapshot.phase is PlacementGroupPhase.CREATED
                else ()
            )
            return protocol.GetPlacementGroupReply(
                request.placement_group_id, True,
                attempt=snapshot.attempt.attempt_number,
                phase=self._phase_status(snapshot.phase),
                placements=placements,
            )

    def remove(
        self, request: protocol.RemovePlacementGroupRequest
    ) -> protocol.RemovePlacementGroupReply:
        if not isinstance(request, protocol.RemovePlacementGroupRequest):
            raise TypeError("remove expects RemovePlacementGroupRequest")
        with self._lock_for(request.placement_group_id):
            try:
                snapshot = self._coordinator.snapshot(request.placement_group_id)
            except KeyError:
                return protocol.RemovePlacementGroupReply(
                    request.placement_group_id, request.attempt, False, False,
                    protocol.PlacementGroupPhaseStatus.REMOVED,
                    "unknown placement group",
                )
            if snapshot.attempt.attempt_number != request.attempt:
                return protocol.RemovePlacementGroupReply(
                    request.placement_group_id, request.attempt, False, False,
                    self._phase_status(snapshot.phase),
                    "placement group removal names a stale attempt",
                )
            if snapshot.phase is PlacementGroupPhase.CREATED:
                # This transition closes scheduling-key visibility before the
                # first participant cleanup RPC can block or report busy.
                snapshot = self._coordinator.remove(request.placement_group_id)
            elif snapshot.phase is PlacementGroupPhase.REMOVED:
                return protocol.RemovePlacementGroupReply(
                    request.placement_group_id, request.attempt, True, True,
                    protocol.PlacementGroupPhaseStatus.REMOVED,
                )
            elif snapshot.phase is not PlacementGroupPhase.REMOVING:
                return protocol.RemovePlacementGroupReply(
                    request.placement_group_id, request.attempt, False, False,
                    self._phase_status(snapshot.phase),
                    "placement group cannot begin removal from {}".format(
                        snapshot.phase.value
                    ),
                )

            snapshot, drive_error = self._drive(snapshot)
            if snapshot.phase is PlacementGroupPhase.REMOVED:
                return protocol.RemovePlacementGroupReply(
                    request.placement_group_id, request.attempt, True, True,
                    protocol.PlacementGroupPhaseStatus.REMOVED,
                )
            return protocol.RemovePlacementGroupReply(
                request.placement_group_id, request.attempt, True, False,
                protocol.PlacementGroupPhaseStatus.REMOVING,
            )

    def snapshot(self, placement_group_id: PlacementGroupID) -> PlacementGroupSnapshot:
        with self._lock_for(placement_group_id):
            return self._coordinator.snapshot(placement_group_id)

    def visible_placement(
        self, placement_group_id: PlacementGroupID
    ) -> object:
        with self._lock_for(placement_group_id):
            return self._coordinator.visible_placement(placement_group_id)

    def fail_node(
        self, death: protocol.NodeDeathRecord
    ) -> tuple[PlacementGroupSnapshot, ...]:
        """Fence affected attempts and drive one survivor-abort round.

        The caller supplies the immutable record returned *after*
        :class:`NodeRegistry` committed the membership transition.  The pure
        reducer retains every outstanding survivor operation, so replaying the
        same death record is also the recovery path for an ambiguous abort ACK.
        No replacement placement is planned: an affected attempt remains
        terminal ``LOST`` even after all physical reservations are gone.
        """

        if not isinstance(death, protocol.NodeDeathRecord):
            raise TypeError("death must be a NodeDeathRecord")
        canonical = replace(death)
        # Freeze every PG which existed at the NodeRegistry commit boundary.
        # A later create plans from the already-live-only scheduling snapshot.
        # Sorted acquisition gives concurrent death reports one lock order; an
        # in-flight participant RPC is bounded and simply delays this reduction.
        with self._admission_lock:
            placement_group_ids = tuple(sorted(self._known_requests))
        locks = tuple(self._lock_for(value) for value in placement_group_ids)
        for lock in locks:
            lock.acquire()
        try:
            with self._coordinator_lock:
                affected = self._coordinator.fail_node(canonical)
            driven: list[PlacementGroupSnapshot] = []
            for reduced in affected:
                placement_group_id = reduced.spec.placement_group_id
                # Re-read because a preceding exact replay may already have
                # advanced some survivor ACKs.  ``_drive`` performs one bounded
                # best-effort cleanup round for LOST, just like removal.
                current = self._coordinator.snapshot(placement_group_id)
                current, _drive_error = self._drive(current)
                driven.append(current)
            return tuple(driven)
        finally:
            for lock in reversed(locks):
                lock.release()

    def close_admission(self) -> None:
        """Fence creation of new logical PGs while preserving exact replay."""

        with self._admission_lock:
            self._accepting_new = False

    def begin_shutdown_cleanup(self) -> bool:
        """Fence new PGs and advance one cleanup round for every known PG.

        Each invocation is bounded to one best-effort abort round per group.  A
        timed-out or busy participant remains an exact outstanding obligation;
        later groups and later participants are still attempted, and replaying
        GCS shutdown advances only what remains.
        """

        self.close_admission()
        with self._admission_lock:
            placement_group_ids = tuple(sorted(self._known_requests))
        clean = True
        terminal = {
            PlacementGroupPhase.INFEASIBLE, PlacementGroupPhase.REMOVED
        }
        for placement_group_id in placement_group_ids:
            with self._lock_for(placement_group_id):
                try:
                    snapshot = self._coordinator.cancel_for_shutdown(
                        placement_group_id
                    )
                except KeyError:
                    continue
                if snapshot.phase not in terminal:
                    snapshot, _drive_error = self._drive(snapshot)
                if (
                    snapshot.phase not in terminal
                    and not (
                        snapshot.phase is PlacementGroupPhase.LOST
                        and not self._coordinator.next_operations(
                            placement_group_id
                        )
                    )
                ):
                    clean = False
        return clean

    def has_active_operations(self) -> bool:
        """Whether a participant obligation can still mutate Node state."""

        with self._admission_lock:
            placement_group_ids = tuple(self._known_requests)
        terminal = {
            PlacementGroupPhase.INFEASIBLE, PlacementGroupPhase.REMOVED
        }
        for placement_group_id in placement_group_ids:
            with self._lock_for(placement_group_id):
                try:
                    phase = self._coordinator.snapshot(placement_group_id).phase
                except KeyError:
                    continue
                if (
                    phase not in terminal
                    and not (
                        phase is PlacementGroupPhase.LOST
                        and not self._coordinator.next_operations(
                            placement_group_id
                        )
                    )
                ):
                    return True
        return False

    def _drive(
        self, snapshot: PlacementGroupSnapshot
    ) -> tuple[PlacementGroupSnapshot, Optional[str]]:
        """Drive exact outstanding operations until terminal or ambiguous."""

        placement_group_id = snapshot.spec.placement_group_id
        while snapshot.phase in (
            PlacementGroupPhase.PREPARING,
            PlacementGroupPhase.COMMITTING,
            PlacementGroupPhase.ABORTING,
            PlacementGroupPhase.LOST,
            PlacementGroupPhase.REMOVING,
        ):
            operations = self._coordinator.next_operations(placement_group_id)
            if not operations:
                return snapshot, "placement group has no operation for an active phase"
            phase_before = snapshot.phase
            cleanup_round = phase_before in (
                PlacementGroupPhase.ABORTING,
                PlacementGroupPhase.LOST,
                PlacementGroupPhase.REMOVING,
            )
            cleanup_error: Optional[str] = None
            for operation in operations:
                try:
                    wire_request, handler, reply_type = self._wire_operation(operation)
                    address = self._nodes.address(operation.participant.node_id)
                    wire_reply = self._participant_rpc(
                        address, handler, wire_request
                    )
                    # Pickle does not call dataclass __post_init__; reconstructing
                    # validates both shape and phase before identity comparison.
                    if not isinstance(wire_reply, reply_type):
                        raise TypeError(
                            "participant returned {} instead of {}".format(
                                type(wire_reply).__name__, reply_type.__name__
                            )
                        )
                    wire_reply = replace(wire_reply)
                    self._validate_wire_reply(wire_request, wire_reply)
                except Exception as exc:
                    # No synthetic rejection: the exact reducer obligation stays
                    # outstanding for an idempotent request replay.  Cleanup is
                    # best-effort across the complete participant set: one
                    # unresolved Node must not retain reservations on later
                    # Nodes that can already apply the abort.
                    error = "participant RPC is unresolved: {}: {}".format(
                        type(exc).__name__, exc
                    )
                    if cleanup_round:
                        cleanup_error = cleanup_error or error
                        continue
                    return snapshot, error

                if not wire_reply.accepted:
                    if isinstance(operation, AbortReservation):
                        error = wire_reply.error or "participant abort rejected"
                        cleanup_error = cleanup_error or error
                        continue
                    snapshot = self._coordinator.apply_reply(
                        ReservationReply(
                            operation.participant.attempt,
                            operation.participant.node_id,
                            operation.participant.digest,
                            ParticipantReplyStatus.REJECTED,
                            wire_reply.error or "participant rejected reservation",
                        )
                    )
                    observer = getattr(
                        self._participant_rpc, "observe_rejection", None
                    )
                    if callable(observer):
                        observer(wire_request)
                    # PREPARE/COMMIT rejection changes the phase to ABORTING.
                    break
                if not wire_reply.applied:
                    error = (
                        "participant {} is busy; operation remains pending"
                        .format(operation.participant.node_id)
                    )
                    if cleanup_round:
                        cleanup_error = cleanup_error or error
                        continue
                    return snapshot, error

                status = (
                    ParticipantReplyStatus.PREPARED
                    if isinstance(operation, PrepareReservation)
                    else ParticipantReplyStatus.COMMITTED
                    if isinstance(operation, CommitReservation)
                    else ParticipantReplyStatus.ABORTED
                )
                snapshot = self._coordinator.apply_reply(
                    ReservationReply(
                        operation.participant.attempt,
                        operation.participant.node_id,
                        operation.participant.digest,
                        status,
                    )
                )
                if snapshot.phase is not phase_before:
                    break
            if cleanup_round:
                # A cleanup call owns exactly one best-effort round.  Applied
                # replies above are durable in the reducer; unresolved/busy
                # participants remain in next_operations for the same request
                # identity to replay.
                return snapshot, cleanup_error
        return snapshot, None

    @staticmethod
    def _wire_operation(
        operation: PlacementGroupOperation,
    ) -> tuple[object, str, type]:
        participant = operation.participant
        bundles = tuple(
            protocol.PlacementGroupBundle(bundle.index, bundle.resources)
            for bundle in participant.bundles
        )
        values = (
            participant.attempt.placement_group_id,
            participant.attempt.attempt_number,
            participant.node_id,
            participant.digest,
        )
        if isinstance(operation, PrepareReservation):
            return (
                protocol.PreparePlacementGroupRequest(
                    *values, protocol.PlacementGroupParticipantPhase.PREPARE, bundles
                ),
                PREPARE_PLACEMENT_GROUP_HANDLER,
                protocol.PreparePlacementGroupReply,
            )
        if isinstance(operation, CommitReservation):
            return (
                protocol.CommitPlacementGroupRequest(
                    *values, protocol.PlacementGroupParticipantPhase.COMMIT, bundles
                ),
                COMMIT_PLACEMENT_GROUP_HANDLER,
                protocol.CommitPlacementGroupReply,
            )
        if isinstance(operation, AbortReservation):
            return (
                protocol.AbortPlacementGroupRequest(
                    *values, protocol.PlacementGroupParticipantPhase.ABORT, bundles
                ),
                ABORT_PLACEMENT_GROUP_HANDLER,
                protocol.AbortPlacementGroupReply,
            )
        raise TypeError("unknown placement-group operation")

    @staticmethod
    def _validate_wire_reply(request: object, reply: object) -> None:
        expected = (
            request.placement_group_id, request.attempt, request.node_id,
            request.plan_digest, request.phase,
        )
        actual = (
            reply.placement_group_id, reply.attempt, reply.node_id,
            reply.plan_digest, reply.phase,
        )
        if actual != expected:
            raise PlacementGroupConflictError(
                "participant reply changed PG/attempt/node/digest/phase identity"
            )

    @staticmethod
    def _placement_keys(
        snapshot: PlacementGroupSnapshot,
    ) -> tuple[protocol.PlacementGroupSchedulingKey, ...]:
        plan = snapshot.plan
        if plan is None:
            return ()
        participants = {value.node_id: value for value in plan.participants}
        return tuple(
            protocol.PlacementGroupSchedulingKey(
                snapshot.spec.placement_group_id,
                snapshot.attempt.attempt_number,
                placement.bundle_index,
                placement.node_id,
                participants[placement.node_id].digest,
            )
            for placement in plan.placements
        )

    @staticmethod
    def _phase_status(
        phase: PlacementGroupPhase,
    ) -> protocol.PlacementGroupPhaseStatus:
        return protocol.PlacementGroupPhaseStatus(phase.value)

    def _lock_for(self, placement_group_id: PlacementGroupID) -> RLock:
        if not isinstance(placement_group_id, PlacementGroupID):
            raise TypeError("placement_group_id must be a PlacementGroupID")
        with self._locks_lock:
            return self._locks.setdefault(placement_group_id, RLock())


@dataclass(frozen=True)
class GetControlSnapshot:
    pass


@dataclass(frozen=True)
class GetActorStatus:
    actor_id: Optional[Hashable] = None


@dataclass(frozen=True)
class CreateActor:
    specification: object


@dataclass(frozen=True)
class GetPlacementGroupStatus:
    placement_group_id: Optional[Hashable] = None


@dataclass(frozen=True)
class CreatePlacementGroup:
    specification: object


ACTOR_STATUS = FeatureStatus(
    name="actors",
    implemented=True,
    detail=(
        "GCS coordinates idempotent Actor creation and publishes the dedicated "
        "worker endpoint; Actor method calls go directly to that worker."
    ),
)
PLACEMENT_GROUP_STATUS = FeatureStatus(
    name="placement_groups",
    implemented=True,
    detail=(
        "GCS freezes a placement plan and converges typed prepare/commit/abort "
        "obligations; NodeManager ledgers remain resource authority."
    ),
)


class GCSLite:
    """In-memory GCS-lite component with an optional loopback RPC server.

    The handler table contains control-plane operations only.  In particular,
    there is deliberately no ``submit_task`` or ``push_task`` handler: clients
    obtain node/worker addresses elsewhere and send ordinary tasks directly to
    the execution path.
    """

    def __init__(
        self,
        *,
        host: str = LOOPBACK_HOST,
        port: int = 0,
        event_sink: Optional[EventSink] = None,
        actor_reserve: Optional[ActorReserve] = None,
        actor_state_install: Optional[ActorStateInstall] = None,
        on_node_dead: Optional[Callable[[protocol.NodeDeathRecord], None]] = None,
        placement_group_participant_rpc: Optional[
            PlacementGroupParticipantRPC
        ] = None,
        placement_group_prepare_failure: Optional[
            PlacementGroupPrepareFailureConfig
        ] = None,
        owner_fence_rpc: Optional[OwnerFenceRPC] = None,
    ) -> None:
        if (
            placement_group_prepare_failure is not None
            and not isinstance(
                placement_group_prepare_failure,
                PlacementGroupPrepareFailureConfig,
            )
        ):
            raise TypeError(
                "placement_group_prepare_failure must be a "
                "PlacementGroupPrepareFailureConfig or None"
            )
        # This lock is the GCS composition boundary for membership, immutable
        # Worker-death facts, and the independent owner-level fence outbox.
        # Node RPCs are deliberately driven after leaving it.
        self._owner_death_control_lock = RLock()
        self.owner_death_fences = OwnerDeathFenceRegistry()
        self._owner_death_progress_wakeup = Event()
        self._owner_death_progress_stop = Event()
        self._owner_death_progress_thread: Optional[Thread] = None
        self._owner_fence_progress_cursor = 0
        self.nodes = NodeRegistry(scheduling_visible=lambda node_id: node_id in (
            self.owner_death_fences.cleanup_safe_node_ids()
        ))
        self.workers = WorkerRegistry(self.nodes)
        if owner_fence_rpc is not None and not callable(owner_fence_rpc):
            raise TypeError("owner_fence_rpc must be callable or None")
        self._owner_fence_rpc = (
            owner_fence_rpc if owner_fence_rpc is not None else rpc_request
        )
        self._on_node_dead = on_node_dead
        self.functions = FunctionRegistry()
        self.actors = ActorRegistry()
        self.actor_coordinator = ActorCoordinator(
            self.nodes, self.actors, reserve_actor_worker=actor_reserve,
            install_actor_state=actor_state_install,
        )
        self.event_sink = event_sink if event_sink is not None else EventSink()
        participant_rpc = (
            placement_group_participant_rpc
            if placement_group_participant_rpc is not None
            else _placement_group_participant_rpc
        )
        if placement_group_prepare_failure is not None:
            participant_rpc = _PlacementGroupPrepareFailureRPC(
                placement_group_prepare_failure, participant_rpc, self.event_sink
            )
        self.placement_groups = PlacementGroupControlCoordinator(
            self.nodes, participant_rpc=participant_rpc
        )
        self._snapshot_lock = RLock()
        self._stop_event = Event()
        self._shutdown_request_id: Optional[str] = None
        self._shutdown_exit_scheduled = False
        self._placement_group_drain_request_id: Optional[str] = None
        self._actor_drain_request_id: Optional[str] = None
        self._server = TCPServer(
            self.handlers, host=host, port=port,
            event_sink=self.event_sink, trace_component="gcs",
        )

    @property
    def handlers(self) -> Mapping[str, Callable[[object], object]]:
        """Return the explicit RPC surface (never a task data path)."""

        return MappingProxyType(
            {
                CONTROL_HANDLER_NAME: self.handle,
                REGISTER_NODE_HANDLER: self.register_node,
                UPDATE_NODE_RESOURCES_HANDLER: self.update_node_resources,
                UNREGISTER_NODE_HANDLER: self.unregister_node,
                GET_NODES_HANDLER: self.get_nodes,
                GET_NODE_ADDRESS_HANDLER: self.get_node_address,
                REPORT_NODE_DEATH_HANDLER: self.report_node_death,
                GET_NODE_STATE_HANDLER: self.get_node_state,
                REGISTER_WORKER_INCARNATION_HANDLER:
                    self.register_worker_incarnation,
                REPORT_WORKER_DEATH_HANDLER: self.report_worker_death,
                GET_WORKER_STATE_HANDLER: self.get_worker_state,
                GET_WORKER_DEATHS_HANDLER: self.get_worker_deaths,
                REPORT_ACTOR_WORKER_EXIT_HANDLER: self.report_actor_worker_exit,
                GET_ACTOR_STATE_HANDLER: self.get_actor_state,
                GCS_SHUTDOWN_HANDLER: self.shutdown,
                "register_function": self.register_function,
                "get_function": self.get_function,
                "snapshot": lambda _message: self.snapshot(),
                "actor_status": lambda message: self.actor_status(
                    getattr(message, "actor_id", None)
                ),
                CREATE_ACTOR_HANDLER: self.create_actor,
                "placement_group_status": self.get_placement_group,
                GET_PLACEMENT_GROUP_HANDLER: self.get_placement_group,
                CREATE_PLACEMENT_GROUP_HANDLER: self.create_placement_group,
                REMOVE_PLACEMENT_GROUP_HANDLER: self.remove_placement_group,
                DRAIN_PLACEMENT_GROUPS_HANDLER: self.drain_placement_groups,
                DRAIN_ACTORS_HANDLER: self.drain_actors,
                DRAIN_OWNER_DEATH_FENCES_HANDLER: self.drain_owner_death_fences,
            }
        )

    @property
    def address(self) -> Address:
        return self._server.address

    @property
    def is_running(self) -> bool:
        return self._server.is_running

    def start(self) -> Address:
        address = self._server.start()
        self._start_owner_death_progress()
        self._emit("started", address=address)
        return address

    def stop(self) -> None:
        self._stop_owner_death_progress()
        self._server.stop()
        self._emit("stopped", address=self.address)

    def _start_owner_death_progress(self) -> None:
        """Retry owner-wide Node fences independently from death reports.

        Death-report handlers commit membership and pending fence identities.
        The separate driver keeps retrying pinned replicas or lost replies;
        only exact complete Node acknowledgements retire pending effects.
        """

        wakeup, stop = self._owner_death_progress_events()
        with self._owner_fence_lock():
            thread = getattr(self, "_owner_death_progress_thread", None)
            if thread is not None and thread.is_alive():
                return
            stop.clear()
            thread = Thread(
                target=self._owner_death_progress_loop,
                name="miniray-owner-death-progress",
                daemon=True,
            )
            self._owner_death_progress_thread = thread
            thread.start()

    def _stop_owner_death_progress(self) -> None:
        wakeup, stop = self._owner_death_progress_events()
        stop.set()
        wakeup.set()
        thread = getattr(self, "_owner_death_progress_thread", None)
        if thread is not None and thread is not current_thread():
            thread.join(1.0)
        self._owner_death_progress_thread = None

    def _wake_owner_death_progress(self) -> None:
        self._owner_death_progress_events()[0].set()

    def _owner_death_progress_events(self) -> Tuple[Event, Event]:
        """Normalize narrow ``object.__new__`` fixtures lazily."""

        wakeup = getattr(self, "_owner_death_progress_wakeup", None)
        if wakeup is None:
            wakeup = Event()
            self._owner_death_progress_wakeup = wakeup
        stop = getattr(self, "_owner_death_progress_stop", None)
        if stop is None:
            stop = Event()
            self._owner_death_progress_stop = stop
        if not hasattr(self, "_owner_death_progress_thread"):
            self._owner_death_progress_thread = None
        return wakeup, stop

    def _owner_death_progress_loop(self) -> None:
        wakeup, stop = self._owner_death_progress_events()
        delay = 0.01
        while not stop.is_set():
            wakeup.wait(delay)
            wakeup.clear()
            if stop.is_set():
                return
            progressed = False
            try:
                progressed = self._drive_owner_death_fence_once()
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                # A failed attempt leaves its exact effect in the outbox.
                self._emit(
                    "owner_death_fence_progress_failed",
                    error="{}: {}".format(type(exc).__name__, exc),
                )
            try:
                active = self._owner_fence_registry().has_active_operations()
            except Exception as exc:
                self._emit(
                    "owner_death_fence_state_failed",
                    error="{}: {}".format(type(exc).__name__, exc),
                )
                # Missing state evidence cannot establish a clean outbox.
                active = True
            delay = (0.01 if progressed else min(0.25, delay * 2)) if active else 0.25

    def wait(self) -> None:
        """Block the process entry point until a shutdown request arrives."""

        self._stop_event.wait()


    def drain_owner_death_fences(
        self, request: object,
    ) -> protocol.DrainOwnerDeathFencesReply:
        if not isinstance(request, protocol.DrainOwnerDeathFences):
            raise TypeError("drain_owner_death_fences expects DrainOwnerDeathFences")
        self._drive_owner_death_fence_once()
        active = len(self._owner_fence_registry().pending())
        return protocol.DrainOwnerDeathFencesReply(request.request_id, active == 0, active)

    def _drive_owner_death_fence_once(self) -> bool:
        # Advance on attempts so a pinned or unreachable target cannot starve
        # independent owner fences. The remote call stays outside this lock.
        with self._owner_fence_lock():
            pending = self._owner_fence_registry().pending()
            if not pending:
                return False
            cursor = getattr(self, "_owner_fence_progress_cursor", 0)
            effect = pending[cursor % len(pending)]
            self._owner_fence_progress_cursor = cursor + 1
        return self._drive_owner_death_fence(effect)

    def _drive_owner_death_fence(
        self, effect: OwnerDeathFenceEffect,
    ) -> bool:
        """Send one immutable owner-level fence and acknowledge its echo."""

        try:
            candidate = self._owner_fence_node_rpc(
                effect.key.target.node_id,
                INSTALL_OWNER_DEATH_FENCE_HANDLER, effect.request,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            # Reachability is not a death proof.  A committed Node tombstone
            # discharges this entry in report_node_death; otherwise exact replay
            # remains pending.
            return False
        if (
            not isinstance(candidate, protocol.InstallOwnerDeathFenceReply)
            or candidate.request != effect.request
            or not candidate.accepted
        ):
            return False
        if not candidate.complete:
            # A pinned or inconsistent ordinary replica remains the same exact
            # outbox effect.  The background driver retries after Node-local
            # state changes; it must not publish a false terminal completion.
            if not candidate.retryable:
                self._emit(
                    "owner_death_sweep_conflict",
                    owner_worker_id=str(effect.key.owner_worker_id),
                    node_id=str(effect.key.target.node_id),
                    request_id=effect.request.request_id,
                )
            return False
        self._owner_fence_registry().acknowledge(effect, candidate)
        return True



    def _owner_fence_node_rpc(
        self, node_id: NodeID, handler: str, request: object,
    ) -> object:
        return self._owner_fence_rpc(
            self.nodes.address(node_id), handler, request
        )






    def __enter__(self) -> "GCSLite":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()

    def _owner_fence_registry(self) -> OwnerDeathFenceRegistry:
        """Return the outbox, including for legacy object.__new__ fixtures."""

        registry = getattr(self, "owner_death_fences", None)
        if registry is None:
            registry = OwnerDeathFenceRegistry()
            self.owner_death_fences = registry
        return registry

    def _owner_fence_lock(self) -> RLock:
        lock = getattr(self, "_owner_death_control_lock", None)
        if lock is None:
            lock = RLock()
            self._owner_death_control_lock = lock
        return lock

    def register_node(self, message: object) -> RegisterNodeReply:
        if not isinstance(message, RegisterNode):
            raise TypeError("register_node expects RegisterNode")
        with self._owner_fence_lock():
            reply = self.nodes.register_message(message)
            if reply.accepted:
                self._owner_fence_registry().register_node(
                    OwnerFenceNodeIncarnation(
                        reply.node_id, reply.node_pid, reply.registration_epoch
                    )
                )
        if reply.accepted:
            self._wake_owner_death_progress()
        self._emit(
            "node_registered" if reply.accepted else "node_registration_conflict",
            node_id=message.node_id,
        )
        return reply

    def update_node_resources(self, message: object) -> UpdateNodeResourcesReply:
        if not isinstance(message, UpdateNodeResources):
            raise TypeError("update_node_resources expects UpdateNodeResources")
        try:
            self.nodes.update_resources(
                message.node_id, message.node_pid, message.registration_epoch,
                message.report_seq, message.available_resources,
            )
        except (
            UnknownNodeError, DeadNodeError, NodeRegistrationConflictError,
            ControlPlaneError, ValueError,
        ) as exc:
            return UpdateNodeResourcesReply(
                message.node_id, message.node_pid, message.registration_epoch,
                message.report_seq, updated=False, error=str(exc)
            )
        self._emit("node_resources_updated", node_id=message.node_id)
        return UpdateNodeResourcesReply(
            message.node_id, message.node_pid, message.registration_epoch,
            message.report_seq, updated=True,
        )

    def unregister_node(self, message: object) -> UnregisterNodeReply:
        if not isinstance(message, UnregisterNode):
            raise TypeError("unregister_node expects UnregisterNode")
        death_reply = self.nodes.unregister(
            message.node_id, message.node_pid, message.registration_epoch,
            message.detection_id,
        )
        removed = death_reply.disposition in (
            protocol.NodeDeathDisposition.APPLIED,
            protocol.NodeDeathDisposition.ALREADY_DEAD,
        )
        if (
            death_reply.disposition in (
                protocol.NodeDeathDisposition.APPLIED,
                protocol.NodeDeathDisposition.ALREADY_DEAD,
            )
            and death_reply.death is not None
        ):
            pg_fail_node = getattr(
                getattr(self, "placement_groups", None), "fail_node", None
            )
            if (
                death_reply.death.reason
                is protocol.NodeDeathReason.PROCESS_EXIT
                and callable(pg_fail_node)
            ):
                pg_fail_node(death_reply.death)
            actor_coordinator = getattr(self, "actor_coordinator", None)
            if actor_coordinator is not None:
                actor_coordinator.fail_node(
                    message.node_id,
                    "Actor Node exited; Phase B1 does not migrate Nodes",
                    require_owner_ack=(
                        death_reply.death.reason
                        is protocol.NodeDeathReason.PROCESS_EXIT
                    ),
                )
            if death_reply.disposition is protocol.NodeDeathDisposition.APPLIED:
                self._notify_node_dead(death_reply.death)
        self._emit("node_unregistered", node_id=message.node_id, removed=removed)
        return UnregisterNodeReply(
            message.node_id, message.node_pid, message.registration_epoch,
            message.detection_id, removed, death_reply.membership_epoch,
            death_reply.death, death_reply.error,
        )

    def get_nodes(self, message: object = None) -> GetNodesReply:
        if message is not None and not isinstance(message, GetNodes):
            raise TypeError("get_nodes expects GetNodes")
        with self._owner_fence_lock():
            membership_epoch, live_nodes = self.nodes.live_snapshot()
            safe = set(self._owner_fence_registry().cleanup_safe_node_ids())
            return GetNodesReply(
                membership_epoch,
                tuple(node for node in live_nodes if node.node_id in safe),
            )

    def report_node_death(
        self, message: object
    ) -> protocol.ReportNodeDeathReply:
        if not isinstance(message, protocol.ReportNodeDeath):
            raise TypeError("report_node_death expects ReportNodeDeath")
        # Membership and owner-wide fence admission share one local boundary.
        # If fence admission fails after membership commits, exact ALREADY_DEAD
        # replay repeats that admission before acknowledging the death report.
        with self._owner_fence_lock():
            reply = self.nodes.report_death(message)
            committed_worker_deaths = ()
            if (
                reply.disposition in (
                    protocol.NodeDeathDisposition.APPLIED,
                    protocol.NodeDeathDisposition.ALREADY_DEAD,
                )
                and reply.death is not None
            ):
                workers = getattr(self, "workers", None)
                if workers is not None:
                    committed_worker_deaths = workers.fail_node(reply.death)
                    for death in committed_worker_deaths:
                        self._owner_fence_registry().commit_owner_death(death)
                if (
                    reply.death.reason
                    is protocol.NodeDeathReason.PROCESS_EXIT
                ):
                    registry = self._owner_fence_registry()
                    registered = {
                        target.node_id for target in registry.snapshot().live_nodes
                    }
                    if reply.death.node_id in registered:
                        registry.mark_node_dead(reply.death)
        if committed_worker_deaths:
            self._wake_owner_death_progress()
        actor_state_converged = True
        if (
            reply.disposition in (
                protocol.NodeDeathDisposition.APPLIED,
                protocol.NodeDeathDisposition.ALREADY_DEAD,
            )
            and reply.death is not None
        ):
            pg_fail_node = getattr(
                getattr(self, "placement_groups", None), "fail_node", None
            )
            if (
                reply.death.reason is protocol.NodeDeathReason.PROCESS_EXIT
                and callable(pg_fail_node)
            ):
                # APPLIED is the membership commit boundary.  ALREADY_DEAD is
                # intentionally included: an earlier survivor abort may have
                # committed at the Node while its reply was lost.
                pg_fail_node(reply.death)
            actor_coordinator = getattr(self, "actor_coordinator", None)
            if actor_coordinator is not None:
                if reply.death.reason is protocol.NodeDeathReason.PROCESS_EXIT:
                    actor_coordinator.fail_node(
                        reply.death.node_id, "Actor Node exited; cross-Node migration is unsupported"
                    )
                    actor_state_converged = actor_coordinator.node_failure_states_converged()
                else:
                    actor_coordinator.fail_node(
                        message.node_id, "Actor Node exited normally",
                        require_owner_ack=False,
                    )
            if reply.disposition is protocol.NodeDeathDisposition.APPLIED:
                self._notify_node_dead(reply.death)
        self._emit(
            "node_dead" if reply.disposition is protocol.NodeDeathDisposition.APPLIED
            else "node_death_replayed",
            node_id=message.node_id, detection_id=message.detection_id,
            disposition=reply.disposition.value,
        )
        if (
            reply.death is not None
            and reply.death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        ):
            reply = replace(
                reply, actor_state_converged=actor_state_converged
            )
        return reply

    def get_node_state(
        self, message: object
    ) -> protocol.GetNodeStateReply:
        if not isinstance(message, protocol.GetNodeState):
            raise TypeError("get_node_state expects GetNodeState")
        return self.nodes.get_state_reply(message)

    def register_worker_incarnation(
        self, message: object
    ) -> protocol.RegisterWorkerIncarnationReply:
        if not isinstance(message, protocol.RegisterWorkerIncarnation):
            raise TypeError(
                "register_worker_incarnation expects "
                "RegisterWorkerIncarnation"
            )
        reply = self.workers.register(message)
        self._emit(
            "worker_registered" if reply.accepted
            else "worker_registration_rejected",
            node_id=message.incarnation.node_id,
            worker_id=message.incarnation.worker_id,
        )
        return reply

    def report_worker_death(
        self, message: object
    ) -> protocol.ReportWorkerDeathReply:
        if not isinstance(message, protocol.ReportWorkerDeath):
            raise TypeError("report_worker_death expects ReportWorkerDeath")
        with self._owner_fence_lock():
            reply = self.workers.report_death(message)
            if (
                reply.disposition in (
                    protocol.WorkerDeathDisposition.APPLIED,
                    protocol.WorkerDeathDisposition.ALREADY_DEAD,
                )
                and reply.death is not None
                and reply.death.reason in (
                    protocol.WorkerDeathReason.PROCESS_EXIT,
                    protocol.WorkerDeathReason.NODE_EXIT,
                )
            ):
                self._owner_fence_registry().commit_owner_death(reply.death)
        if (
            reply.death is not None
            and reply.death.reason in (
                protocol.WorkerDeathReason.PROCESS_EXIT,
                protocol.WorkerDeathReason.NODE_EXIT,
            )
        ):
            self._wake_owner_death_progress()
        self._emit(
            "worker_dead"
            if reply.disposition is protocol.WorkerDeathDisposition.APPLIED
            else "worker_death_replayed",
            node_id=message.node_id,
            worker_id=message.worker_id,
            detection_id=message.detection_id,
            disposition=reply.disposition.value,
            death_epoch=(
                reply.death.death_epoch if reply.death is not None else None
            ),
        )
        return reply

    def get_worker_state(
        self, message: object
    ) -> protocol.GetWorkerStateReply:
        if not isinstance(message, protocol.GetWorkerState):
            raise TypeError("get_worker_state expects GetWorkerState")
        return self.workers.get_state_reply(message)

    def get_worker_deaths(
        self, message: object
    ) -> protocol.GetWorkerDeathsReply:
        if not isinstance(message, protocol.GetWorkerDeaths):
            raise TypeError("get_worker_deaths expects GetWorkerDeaths")
        return self.workers.deaths_after(message)

    def get_node_address(self, message: object) -> GetNodeAddressReply:
        if not isinstance(message, GetNodeAddress):
            raise TypeError("get_node_address expects GetNodeAddress")
        try:
            address = self.nodes.address(message.node_id)
        except (UnknownNodeError, DeadNodeError) as exc:
            return GetNodeAddressReply(
                message.node_id, found=False, error=str(exc)
            )
        return GetNodeAddressReply(message.node_id, found=True, address=address)

    def _notify_node_dead(self, death: protocol.NodeDeathRecord) -> None:
        callback = self._on_node_dead
        if callback is not None:
            callback(death)

    def drain_placement_groups(
        self, message: object
    ) -> protocol.DrainPlacementGroupsReply:
        """Advance the independent PG cleanup barrier without stopping GCS."""

        if not isinstance(message, protocol.DrainPlacementGroupsRequest):
            raise TypeError(
                "drain_placement_groups expects DrainPlacementGroupsRequest"
            )
        # The drain epoch is independent from final Shutdown.  Check it before
        # closing admission or sending any participant RPC so a wrong epoch has
        # no control-plane side effect.
        with self._snapshot_lock:
            if (
                self._placement_group_drain_request_id is not None
                and self._placement_group_drain_request_id != message.request_id
            ):
                return protocol.DrainPlacementGroupsReply(
                    message.request_id, False, False,
                    "placement-group drain already has a different request ID",
                )
            self._placement_group_drain_request_id = message.request_id
        clean = self.placement_groups.begin_shutdown_cleanup()
        self._emit(
            "placement_group_drain_progress",
            request_id=message.request_id, clean=clean,
        )
        # This barrier never schedules _release_wait_after_handler and never
        # touches _stop_event.  Nodes and GCS must remain reachable until every
        # participant abort is acknowledged.
        return protocol.DrainPlacementGroupsReply(
            message.request_id, True, clean
        )

    def drain_actors(self, message: object) -> protocol.DrainActorsReply:
        """Advance the independent Actor-control cleanup barrier once."""

        if not isinstance(message, protocol.DrainActorsRequest):
            raise TypeError("drain_actors expects DrainActorsRequest")
        with self._snapshot_lock:
            current = getattr(self, "_actor_drain_request_id", None)
            if current is not None and current != message.request_id:
                return protocol.DrainActorsReply(
                    message.request_id, False, False, (),
                    "actor drain already has a different request ID",
                )
            self._actor_drain_request_id = message.request_id
        actor_coordinator = getattr(self, "actor_coordinator", None)
        active = (
            ()
            if actor_coordinator is None
            else actor_coordinator.drain_once()
        )
        self._emit(
            "actor_drain_progress", request_id=message.request_id,
            clean=not active, active_actor_ids=active,
        )
        return protocol.DrainActorsReply(
            message.request_id, True, not active, active
        )

    def shutdown(self, message: object) -> ShutdownAck:
        """Acknowledge shutdown, then let the process main thread exit.

        Transport handlers run in daemon threads.  Waking ``gcs_main`` before
        the current handler has sent its reply would let the process exit and
        lose the ACK.  A non-daemon joiner therefore releases the main-thread
        wait only after this handler thread has finished.  Direct in-process
        calls do not need that hand-off.
        """

        if not isinstance(message, Shutdown):
            raise TypeError("shutdown expects Shutdown")
        # Establish the shutdown epoch before mutating PG state.  A request with
        # another identity is rejected without fencing admission or advancing
        # participant cleanup; an exact replay is the only call allowed to drive
        # another convergence round.
        with self._snapshot_lock:
            if (
                self._shutdown_request_id is not None
                and self._shutdown_request_id != message.request_id
            ):
                raise ValueError("GCS shutdown already has a different request ID")
            self._shutdown_request_id = message.request_id
        actor_coordinator = getattr(self, "actor_coordinator", None)
        if actor_coordinator is not None:
            actor_coordinator.close_admission()
        # Final shutdown is deliberately not the PG cleanup driver.  The cluster
        # shutdown path must first replay ``drain_placement_groups`` while Node
        # endpoints remain live.  Here we only fence admission and refuse a clean
        # exit while any PG is still visible or owns a participant obligation.
        self.placement_groups.close_admission()
        placement_groups_clean = not self.placement_groups.has_active_operations()
        actors_clean = (
            actor_coordinator is None
            or not actor_coordinator.has_active_operations()
        )
        owner_death_fences_clean = (
            not self._owner_fence_registry().has_active_operations()
        )
        with self._snapshot_lock:
            schedule_exit = (
                placement_groups_clean and actors_clean
                and owner_death_fences_clean
                and not self._shutdown_exit_scheduled
            )
            if schedule_exit:
                self._shutdown_exit_scheduled = True
        self._emit("shutdown_requested", request_id=message.request_id)
        if schedule_exit:
            handler_thread = current_thread()
            if handler_thread.daemon:
                Thread(
                    target=self._release_wait_after_handler,
                    args=(handler_thread,),
                    name="miniray-gcs-shutdown",
                    daemon=False,
                ).start()
            else:
                self._stop_event.set()
        return ShutdownAck(
            request_id=message.request_id,
            component="gcs",
            clean=(
                placement_groups_clean and actors_clean
                and owner_death_fences_clean
            ),
            detail=(
                "GCS stopping"
                if (
                    placement_groups_clean and actors_clean
                    and owner_death_fences_clean
                )
                else "GCS retained for active control-plane cleanup"
            ),
        )

    def _release_wait_after_handler(self, handler_thread: Thread) -> None:
        handler_thread.join()
        self._stop_event.set()

    def register_function(self, message: object) -> object:
        definition = getattr(message, "definition", None)
        if isinstance(definition, FunctionDefinition):
            try:
                created = self.functions.register_definition(definition)
            except FunctionRegistrationConflictError as exc:
                self._emit(
                    "function_registration_conflict", function_id=definition.key
                )
                return FunctionRegistrationReply(
                    definition.key, accepted=False, error=str(exc)
                )
            self._emit(
                "function_registered"
                if created
                else "function_registration_replayed",
                function_id=definition.key,
            )
            # An identical replay is accepted even though nothing was inserted.
            return FunctionRegistrationReply(definition.key, accepted=True)

        function_id = getattr(message, "function_id")
        created = self.functions.register(function_id, getattr(message, "payload"))
        self._emit(
            "function_registered" if created else "function_registration_replayed",
            function_id=function_id,
        )
        return created

    def get_function(self, message: object) -> object:
        key = getattr(message, "key", None)
        if isinstance(key, FunctionKey):
            try:
                definition = self.functions.get_definition(key)
            except FunctionNotRegisteredError as exc:
                return FunctionReply(key, error=str(exc))
            return FunctionReply(key, definition=definition)
        return self.functions.get(getattr(message, "function_id"))

    def actor_status(self, actor_id: Optional[Hashable] = None) -> object:
        """Preserve the old capability query and expose per-Actor state."""

        if actor_id is None:
            return ACTOR_STATUS
        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        return self.actors.get(actor_id)

    def create_actor(self, message: object) -> CreateActorReply:
        if isinstance(message, CreateActor):
            message = message.specification
        if not isinstance(message, CreateActorRequest):
            raise TypeError("create_actor expects CreateActorRequest")
        reply = self.actor_coordinator.create(message)
        if reply.accepted:
            self._emit(
                "actor_alive",
                actor_id=message.actor_id,
                generation=message.generation,
                node_id=reply.node_id,
                worker_id=reply.worker_id,
            )
        else:
            self._emit(
                "actor_creation_rejected",
                actor_id=message.actor_id,
                generation=message.generation,
                error=reply.error,
            )
        return reply

    def report_actor_worker_exit(
        self, message: object
    ) -> protocol.ReportActorWorkerExitReply:
        if not isinstance(message, protocol.ReportActorWorkerExit):
            raise TypeError(
                "report_actor_worker_exit expects ReportActorWorkerExit"
            )
        reply = self.actor_coordinator.report_worker_exit(message)
        self._emit(
            "actor_worker_exit_reduced",
            actor_id=message.record.actor_id,
            generation=message.record.generation,
            detection_id=message.record.detection_id,
            disposition=reply.disposition.value,
        )
        return reply

    def get_actor_state(
        self, message: object
    ) -> protocol.GetActorStateReply:
        if not isinstance(message, protocol.GetActorState):
            raise TypeError("get_actor_state expects GetActorState")
        return self.actors.get_state_reply(message)

    def placement_group_status(
        self, placement_group_id: Optional[Hashable] = None
    ) -> object:
        """Compatibility capability query or typed per-PG snapshot."""

        if placement_group_id is None:
            return PLACEMENT_GROUP_STATUS
        if not isinstance(placement_group_id, PlacementGroupID):
            raise TypeError("placement_group_id must be a PlacementGroupID")
        return self.placement_groups.get(
            protocol.GetPlacementGroupRequest(placement_group_id)
        )

    def get_placement_group(self, message: object) -> protocol.GetPlacementGroupReply:
        if isinstance(message, GetPlacementGroupStatus):
            placement_group_id = message.placement_group_id
            if placement_group_id is None:
                raise TypeError("placement_group_status requires placement_group_id")
            message = protocol.GetPlacementGroupRequest(placement_group_id)
        if not isinstance(message, protocol.GetPlacementGroupRequest):
            raise TypeError("get_placement_group expects GetPlacementGroupRequest")
        return self.placement_groups.get(message)

    def create_placement_group(
        self, message: object
    ) -> protocol.CreatePlacementGroupReply:
        if isinstance(message, CreatePlacementGroup):
            message = message.specification
        if not isinstance(message, protocol.CreatePlacementGroupRequest):
            raise TypeError(
                "create_placement_group expects CreatePlacementGroupRequest"
            )
        reply = self.placement_groups.create(message)
        self._emit(
            "placement_group_created" if reply.accepted
            else "placement_group_create_pending",
            placement_group_id=message.placement_group_id,
            attempt=message.attempt,
            error=reply.error,
        )
        return reply

    def remove_placement_group(
        self, message: object
    ) -> protocol.RemovePlacementGroupReply:
        if not isinstance(message, protocol.RemovePlacementGroupRequest):
            raise TypeError(
                "remove_placement_group expects RemovePlacementGroupRequest"
            )
        reply = self.placement_groups.remove(message)
        self._emit(
            "placement_group_removed" if reply.removed
            else "placement_group_remove_pending",
            placement_group_id=message.placement_group_id,
            attempt=message.attempt,
            error=reply.error,
        )
        return reply

    def snapshot(self) -> ControlSnapshot:
        # Registries are independently synchronized.  This lock serializes GCS
        # snapshots with other snapshots; it intentionally does not turn node
        # resource reports into a distributed transaction.
        with self._snapshot_lock:
            return ControlSnapshot(
                nodes=self.nodes.snapshot(),
                functions=self.functions.snapshot(),
                actor_support=ACTOR_STATUS,
                placement_group_support=PLACEMENT_GROUP_STATUS,
            )

    def handle(self, message: object) -> object:
        """Dispatch a local or envelope-style RPC message by semantic type.

        Equivalent dataclasses from :mod:`miniray.protocol` are accepted by
        class name and duck-typed fields.  Keeping this one adapter at the
        boundary avoids importing control state into task submission.
        """

        name = type(message).__name__
        dispatch: Mapping[str, Callable[[object], object]] = {
            "RegisterNode": self.register_node,
            "RegisterNodeRequest": self.register_node,
            "UpdateNodeResources": self.update_node_resources,
            "UpdateNodeResourcesRequest": self.update_node_resources,
            "UnregisterNode": self.unregister_node,
            "UnregisterNodeRequest": self.unregister_node,
            "GetNodes": self.get_nodes,
            "GetNodesRequest": self.get_nodes,
            "GetNodeAddress": self.get_node_address,
            "GetNodeAddressRequest": self.get_node_address,
            "ReportNodeDeath": self.report_node_death,
            "GetNodeState": self.get_node_state,
            "RegisterWorkerIncarnation": self.register_worker_incarnation,
            "ReportWorkerDeath": self.report_worker_death,
            "GetWorkerState": self.get_worker_state,
            "GetWorkerDeaths": self.get_worker_deaths,
            "RegisterFunction": self.register_function,
            "RegisterFunctionRequest": self.register_function,
            "GetFunction": self.get_function,
            "GetFunctionRequest": self.get_function,
            "GetControlSnapshot": lambda _message: self.snapshot(),
            "GetControlSnapshotRequest": lambda _message: self.snapshot(),
            "GetActorStatus": lambda value: self.actor_status(
                getattr(value, "actor_id", None)
            ),
            "CreateActor": self.create_actor,
            "CreateActorRequest": self.create_actor,
            "ReportActorWorkerExit": self.report_actor_worker_exit,
            "GetActorState": self.get_actor_state,
            "GetPlacementGroupStatus": self.get_placement_group,
            "GetPlacementGroupRequest": self.get_placement_group,
            "CreatePlacementGroup": self.create_placement_group,
            "CreatePlacementGroupRequest": self.create_placement_group,
            "RemovePlacementGroupRequest": self.remove_placement_group,
            "DrainPlacementGroupsRequest": self.drain_placement_groups,
            "DrainActorsRequest": self.drain_actors,
            "DrainOwnerDeathFences": self.drain_owner_death_fences,
        }
        try:
            handler = dispatch[name]
        except KeyError:
            raise TypeError("unsupported GCS-lite message: {}".format(name)) from None
        return handler(message)

    def _emit(self, name: str, **attributes: object) -> None:
        self.event_sink.emit(name, component="gcs", attributes=attributes)


def gcs_main(
    ready_connection: Optional[Connection] = None,
    host: str = LOOPBACK_HOST,
    port: int = 0,
    trace_config: Optional[TraceSinkConfig] = None,
    placement_group_prepare_failure: Optional[
        PlacementGroupPrepareFailureConfig
    ] = None,
) -> None:
    """Spawn-safe GCS process entry point.

    The child constructs its own socket server, then publishes the bound
    address over a one-way ready pipe.  Normal task and object bytes never
    enter this service.
    """

    service: Optional[GCSLite] = None
    trace_sink = sink_from_config(trace_config)
    try:
        service = GCSLite(
            host=host,
            port=port,
            event_sink=trace_sink,
            placement_group_prepare_failure=placement_group_prepare_failure,
        )
        address = service.start()
        startup = GCSStartup(gcs_pid=os.getpid(), gcs_address=address)
        if ready_connection is not None:
            ready_connection.send((True, startup))
            ready_connection.close()
            ready_connection = None
        service.wait()
    except BaseException:
        if ready_connection is not None:
            try:
                ready_connection.send((False, traceback.format_exc()))
            finally:
                ready_connection.close()
        raise
    finally:
        if service is not None:
            service.stop()
        trace_sink.close()


# Descriptive aliases keep call sites readable without inventing another layer.
GCSLiteService = GCSLite
GCSService = GCSLite
FunctionConflictError = FunctionRegistrationConflictError
NodeConflictError = NodeRegistrationConflictError
ActorConflictError = ActorRegistrationConflictError


def _require_create_actor_request(value: object) -> None:
    if not isinstance(value, CreateActorRequest):
        raise TypeError("request must be a CreateActorRequest")


def _reject_actor(
    request: CreateActorRequest, error: str, *, retryable: bool = False
) -> CreateActorReply:
    return CreateActorReply(
        request.actor_id, request.generation, accepted=False, error=error,
        retryable=retryable,
    )


def _reserve_actor_worker_rpc(
    address: Address, request: ReserveActorWorkerRequest
) -> object:
    return rpc_request(address, RESERVE_ACTOR_WORKER_HANDLER, request)


def _install_actor_state_rpc(
    address: Address, request: protocol.InstallActorState
) -> object:
    return rpc_request(address, INSTALL_ACTOR_STATE_HANDLER, request)


def _placement_group_participant_rpc(
    address: Address, handler: str, request: object
) -> object:
    return rpc_request(address, handler, request)


def _require_node_id(value: object) -> None:
    if not isinstance(value, NodeID):
        raise TypeError("node_id must be a NodeID")


def _require_node_pid(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("node_pid must be a positive integer")


def _require_positive_epoch(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("{} must be a positive integer".format(name))


def _require_non_negative(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("{} must be a non-negative integer".format(name))


def _validate_address(address: object) -> Address:
    if (
        not isinstance(address, tuple)
        or len(address) != 2
        or not isinstance(address[0], str)
        or isinstance(address[1], bool)
        or not isinstance(address[1], int)
        or not 0 <= address[1] <= 65535
    ):
        raise ValueError("address must be a (host, port) tuple")
    return address[0], address[1]


def _resource_vector(
    value: ResourceVector | Mapping[str, ResourceQuantity],
) -> ResourceVector:
    return value if isinstance(value, ResourceVector) else ResourceVector(value)


def _require_available_within_total(
    available: ResourceVector, total: ResourceVector
) -> None:
    if not available.fits_in(total):
        raise ValueError("available resources cannot exceed total resources")


__all__ = [
    "ACTOR_STATUS",
    "ABORT_PLACEMENT_GROUP_HANDLER",
    "COMMIT_PLACEMENT_GROUP_HANDLER",
    "CREATE_ACTOR_HANDLER",
    "CREATE_PLACEMENT_GROUP_HANDLER",
    "CONTROL_HANDLER_NAME",
    "DRAIN_OWNER_DEATH_FENCES_HANDLER",
    "DRAIN_PLACEMENT_GROUPS_HANDLER",
    "DRAIN_ACTORS_HANDLER",
    "GCS_SHUTDOWN_HANDLER",
    "GET_NODE_ADDRESS_HANDLER",
    "GET_ACTOR_STATE_HANDLER",
    "GET_NODE_STATE_HANDLER",
    "GET_NODES_HANDLER",
    "GET_PLACEMENT_GROUP_HANDLER",
    "GET_WORKER_DEATHS_HANDLER",
    "GET_WORKER_STATE_HANDLER",
    "INSTALL_CLUSTER_SNAPSHOT_HANDLER",
    "INSTALL_OWNER_DEATH_FENCE_HANDLER",
    "PLACEMENT_GROUP_STATUS",
    "PREPARE_PLACEMENT_GROUP_HANDLER",
    "REGISTER_NODE_HANDLER",
    "REGISTER_WORKER_INCARNATION_HANDLER",
    "REPORT_NODE_DEATH_HANDLER",
    "REPORT_WORKER_DEATH_HANDLER",
    "REPORT_ACTOR_WORKER_EXIT_HANDLER",
    "RESERVE_ACTOR_WORKER_HANDLER",
    "INSTALL_ACTOR_STATE_HANDLER",
    "REMOVE_PLACEMENT_GROUP_HANDLER",
    "UNREGISTER_NODE_HANDLER",
    "UPDATE_NODE_RESOURCES_HANDLER",
    "ActorConflictError",
    "ActorCoordinator",
    "ActorCreationState",
    "ActorCreationStateError",
    "ActorRegistry",
    "ActorRegistrationConflictError",
    "ActorSnapshot",
    "ControlPlaneError",
    "DeadNodeError",
    "ControlSnapshot",
    "CreateActor",
    "CreatePlacementGroup",
    "FeatureStatus",
    "FunctionConflictError",
    "FunctionRegistrationConflictError",
    "FunctionRegistry",
    "FunctionSnapshot",
    "GCSLite",
    "GCSLiteService",
    "GCSService",
    "GetActorStatus",
    "GetControlSnapshot",
    "GetFunction",
    "GetNodeAddress",
    "GetNodes",
    "GetPlacementGroupStatus",
    "NodeConflictError",
    "NodeRegistrationConflictError",
    "NodeRegistry",
    "NodeSnapshot",
    "PlacementGroupControlCoordinator",
    "PlacementGroupPrepareFailureConfig",
    "RegisterFunction",
    "RegisterNode",
    "UnknownActorError",
    "UnknownFunctionError",
    "UnknownNodeError",
    "UnregisterNode",
    "UpdateNodeResources",
    "WorkerRegistry",
    "WorkerSnapshot",
    "gcs_main",
]
