"""The intentionally small public Python API for mini-Ray v0.1."""

from __future__ import annotations

import atexit
import hashlib
import math
import multiprocessing as mp
from multiprocessing.connection import wait as connection_wait
import os
import signal
import threading
import time
from dataclasses import dataclass, field, replace
from numbers import Real
from typing import Callable, Mapping, Optional, Sequence, Tuple, TypeVar, Union

import cloudpickle

from . import protocol
from .control import (
    DRAIN_ACTORS_HANDLER,
    DRAIN_PLACEMENT_GROUPS_HANDLER,
    DRAIN_OWNER_DEATH_FENCES_HANDLER,
    GCS_SHUTDOWN_HANDLER,
    GET_NODES_HANDLER,
    INSTALL_CLUSTER_SNAPSHOT_HANDLER,
    PlacementGroupPrepareFailureConfig,
    REPORT_NODE_DEATH_HANDLER,
    gcs_main,
)
from .core import ActorEndpoint, CoreWorker, ObjectRef, RemoteFunctionDefinition
from .ids import NodeID, PlacementGroupID, WorkerID
from .node import (
    BEGIN_DRAIN_HANDLER as NODE_BEGIN_DRAIN_HANDLER,
    DRAIN_STATUS_HANDLER as NODE_DRAIN_STATUS_HANDLER,
    FINALIZE_SHUTDOWN_HANDLER as NODE_FINALIZE_SHUTDOWN_HANDLER,
    SHUTDOWN_HANDLER_NAME,
    SHUTDOWN_STATUS_HANDLER,
    node_main,
)
from .owner_service import OwnerService
from .node_monitor import ManagedNodeMonitor
from .node_death_view import (
    InstalledNodeDeathView, PUBLISH_NODE_DEATH_VIEW,
    PublishInstalledNodeDeaths, PublishInstalledNodeDeathsReply,
)
from .publication_gate import OutputPublicationGateConfig
from .worker import WorkerFailpointConfig
from .resources import CPU, ResourceQuantity, ResourceVector
from .placement import PlacementStrategy
from .runtime_binding import ExecutionContext, current_core_worker
from .transport import Address, request as rpc_request
from .trace import TraceSinkConfig, write_trace_records_jsonl
from .trace_collector import TraceCollector, remote_event_sink
from .task_outputs import validate_num_returns


_START_TIMEOUT_SECONDS = 8.0
_STOP_TIMEOUT_SECONDS = 5.0
_SUPPORTED_REMOTE_OPTIONS = frozenset({
    "num_cpus", "resources", "max_retries", "max_restarts",
    "placement_group", "bundle_index", "num_returns",
})
_T = TypeVar("_T", bound=Callable[..., object])
_DEFAULT_INLINE_THRESHOLD_BYTES = 100 * 1024
_DEFAULT_OBJECT_STORE_BYTES = 16 * 1024 * 1024
_MAX_INLINE_THRESHOLD_BYTES = 16 * 1024 * 1024
_MAX_OBJECT_STORE_BYTES = 256 * 1024 * 1024
_TERMINATE_GRACE_SECONDS = 1.0
_CLUSTER_SHUTDOWN_TIMEOUT_SECONDS = 12.0
_NODE_DEATH_RPC_TIMEOUT_SECONDS = 1.0
_NODE_DEATH_RETRY_INTERVAL_SECONDS = 0.01


def _validate_runtime_address(value: object, name: str) -> Address:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], str)
        or not value[0]
        or isinstance(value[1], bool)
        or not isinstance(value[1], int)
        or not 1 <= value[1] <= 65535
    ):
        raise ValueError("{} must be a bound (host, port) tuple".format(name))
    return value


@dataclass(frozen=True)
class NodeRuntimeContext:
    """Startup identity and endpoints for one NodeManager and Worker pool.

    Worker tuples preserve the NodeManager's deterministic slot order.  The
    singular compatibility properties name the first ordinary Worker only;
    Actor Workers are intentionally absent from this diagnostic surface.  A
    later crash replacement is a new physical Worker incarnation and therefore
    appears in :class:`ShutdownReport`, not by mutating this startup snapshot.
    """

    node_id: NodeID
    node_address: Address
    node_pid: int
    worker_ids: Tuple[WorkerID, ...]
    worker_pids: Tuple[int, ...]
    worker_addresses: Tuple[Address, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise ValueError("runtime node_id must be a NodeID")
        _validate_runtime_address(self.node_address, "runtime node_address")
        if (
            isinstance(self.node_pid, bool)
            or not isinstance(self.node_pid, int)
            or self.node_pid <= 0
        ):
            raise ValueError("runtime node_pid must be a positive integer")
        object.__setattr__(self, "worker_ids", tuple(self.worker_ids))
        object.__setattr__(self, "worker_pids", tuple(self.worker_pids))
        object.__setattr__(self, "worker_addresses", tuple(self.worker_addresses))
        if not 1 <= len(self.worker_ids) <= 2 or not (
            len(self.worker_ids)
            == len(self.worker_pids)
            == len(self.worker_addresses)
        ):
            raise ValueError(
                "runtime Worker identity tuples must be non-empty and align"
            )
        if any(not isinstance(worker_id, WorkerID) for worker_id in self.worker_ids):
            raise ValueError("runtime worker_ids must contain WorkerID values")
        if len(set(self.worker_ids)) != len(self.worker_ids):
            raise ValueError("runtime worker_ids must be unique within a node")
        if any(
            isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0
            for pid in self.worker_pids
        ):
            raise ValueError("runtime worker_pids must be positive integers")
        for index, address in enumerate(self.worker_addresses):
            _validate_runtime_address(
                address, "runtime worker_addresses[{}]".format(index)
            )

    @property
    def worker_id(self) -> WorkerID:
        return self.worker_ids[0]

    @property
    def worker_pid(self) -> int:
        return self.worker_pids[0]

    @property
    def worker_address(self) -> Address:
        return self.worker_addresses[0]


@dataclass(frozen=True)
class RuntimeContext:
    """Startup diagnostic identity for the GCS and every Node/Worker pool.

    The singular properties retain the original one-node API and always refer
    to the driver's local (first) node and its first ordinary Worker.  The
    per-node ``nodes`` tuple is authoritative; plural Worker properties are
    node-major flattened views over it.
    """

    gcs_pid: int
    gcs_address: Address
    nodes: Tuple[NodeRuntimeContext, ...]
    trace_address: Optional[Address] = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.gcs_pid, bool)
            or not isinstance(self.gcs_pid, int)
            or self.gcs_pid <= 0
        ):
            raise ValueError("runtime gcs_pid must be a positive integer")
        _validate_runtime_address(self.gcs_address, "runtime gcs_address")
        if self.trace_address is not None:
            _validate_runtime_address(self.trace_address, "runtime trace_address")
        object.__setattr__(self, "nodes", tuple(self.nodes))
        if not 1 <= len(self.nodes) <= 2 or any(
            not isinstance(node, NodeRuntimeContext) for node in self.nodes
        ):
            raise ValueError(
                "runtime nodes must contain one or two NodeRuntimeContext values"
            )
        if len(set(self.node_ids)) != len(self.node_ids):
            raise ValueError("runtime node IDs must be unique")

    @property
    def node_ids(self) -> Tuple[NodeID, ...]:
        return tuple(node.node_id for node in self.nodes)

    @property
    def node_addresses(self) -> Tuple[Address, ...]:
        return tuple(node.node_address for node in self.nodes)

    @property
    def node_pids(self) -> Tuple[int, ...]:
        return tuple(node.node_pid for node in self.nodes)

    @property
    def worker_ids(self) -> Tuple[WorkerID, ...]:
        return tuple(
            worker_id
            for node in self.nodes
            for worker_id in node.worker_ids
        )

    @property
    def worker_pids(self) -> Tuple[int, ...]:
        return tuple(
            worker_pid
            for node in self.nodes
            for worker_pid in node.worker_pids
        )

    @property
    def worker_addresses(self) -> Tuple[Address, ...]:
        return tuple(
            worker_address
            for node in self.nodes
            for worker_address in node.worker_addresses
        )

    @property
    def node_id(self) -> NodeID:
        return self.node_ids[0]

    @property
    def node_address(self) -> Address:
        return self.node_addresses[0]

    @property
    def node_pid(self) -> int:
        return self.node_pids[0]

    @property
    def worker_id(self) -> WorkerID:
        return self.nodes[0].worker_id

    @property
    def worker_pid(self) -> int:
        return self.nodes[0].worker_pid

    @property
    def worker_address(self) -> Address:
        return self.nodes[0].worker_address


@dataclass(frozen=True)
class ShutdownReport:
    """Cleanup result for the CoreWorker, all nodes, and the GCS."""

    core_stopped: bool
    gcs_pid: int
    gcs_exitcode: Optional[int]
    gcs_clean: bool
    node_pids: Tuple[int, ...]
    node_exitcodes: Tuple[Optional[int], ...]
    node_cleans: Tuple[bool, ...]
    worker_pids: Tuple[int, ...]
    worker_exitcodes: Tuple[Optional[int], ...]
    worker_cleans: Tuple[bool, ...]
    worker_forced: Tuple[bool, ...]
    node_forced: Tuple[bool, ...]
    node_finalized: Tuple[bool, ...]
    node_shutdown_ack_clean: Tuple[bool, ...]
    node_resources_clean: Tuple[bool, ...]
    gcs_forced: bool = False
    node_deaths: Tuple[Optional[protocol.NodeDeathRecord], ...] = ()

    def __post_init__(self) -> None:
        node_fields = (
            "node_pids",
            "node_exitcodes",
            "node_cleans",
            "node_forced",
            "node_finalized",
            "node_shutdown_ack_clean",
            "node_resources_clean",
            "node_deaths",
        )
        worker_fields = (
            "worker_pids",
            "worker_exitcodes",
            "worker_cleans",
            "worker_forced",
        )
        for name in node_fields + worker_fields:
            object.__setattr__(self, name, tuple(getattr(self, name)))
        if not self.node_deaths:
            object.__setattr__(
                self, "node_deaths", (None,) * len(self.node_pids)
            )
        if not self.node_pids or any(
            len(getattr(self, name)) != len(self.node_pids)
            for name in node_fields
        ):
            raise ValueError("shutdown Node result tuples must align")
        if not self.worker_pids or any(
            len(getattr(self, name)) != len(self.worker_pids)
            for name in worker_fields
        ):
            raise ValueError("shutdown Worker result tuples must align")

    @property
    def node_pid(self) -> int:
        return self.node_pids[0]

    @property
    def node_exitcode(self) -> Optional[int]:
        return self.node_exitcodes[0]

    @property
    def worker_pid(self) -> int:
        return self.worker_pids[0]

    @property
    def worker_exitcode(self) -> Optional[int]:
        return self.worker_exitcodes[0]

    @property
    def first_worker_forced(self) -> bool:
        return self.worker_forced[0]

    @property
    def worker_clean(self) -> bool:
        return all(self.worker_cleans)

    @property
    def node_clean(self) -> bool:
        return all(self.node_cleans)

    @property
    def forced(self) -> bool:
        return (
            self.gcs_forced
            or any(self.node_forced)
            or any(self.worker_forced)
        )

    @property
    def finalized(self) -> bool:
        return all(self.node_finalized)

    @property
    def shutdown_ack_clean(self) -> bool:
        return self.gcs_clean and all(self.node_shutdown_ack_clean)

    @property
    def resources_clean(self) -> bool:
        return all(self.node_resources_clean)


@dataclass(frozen=True)
class _NodeRuntime:
    node_id: NodeID
    resources: ResourceVector
    process: mp.Process
    startup: protocol.NodeStartup
    registration_epoch: int = 0


def _startup_node_ready_checkpoint(
    index: int,
    startup: protocol.NodeStartup,
    fail_after_node_ready: Optional[int],
) -> None:
    """Exact test seam between child readiness and prefix publication.

    The child has already published a fully validated Node/Worker identity, but
    ``init`` has not yet appended it to the committed startup prefix.  Failing
    here therefore exercises both rollback branches: an unpublished Node
    process group and every earlier, fully known Node.
    """

    if fail_after_node_ready == index:
        raise RuntimeError(
            "injected startup failure after node {} reported readiness".format(
                index
            )
        )


@dataclass(frozen=True)
class _NodeShutdownResult:
    node_exitcode: Optional[int]
    node_clean: bool
    worker_pids: Tuple[int, ...]
    worker_exitcodes: Tuple[Optional[int], ...]
    worker_cleans: Tuple[bool, ...]
    worker_forced: Tuple[bool, ...]
    forced: bool
    finalized: bool
    shutdown_ack_clean: bool
    resources_clean: bool


@dataclass
class _Runtime:
    core_worker: CoreWorker
    gcs_process: mp.Process
    gcs_startup: protocol.GCSStartup
    nodes: Tuple[_NodeRuntime, ...]
    owner_service: Optional[OwnerService] = None
    trace_collector: Optional[TraceCollector] = None
    latest_snapshot: Optional[protocol.InstallClusterSnapshot] = None
    latest_snapshot_acks: Tuple[protocol.InstallClusterSnapshotReply, ...] = ()
    node_monitor: Optional[ManagedNodeMonitor] = None
    node_deaths: dict[NodeID, protocol.NodeDeathRecord] = field(
        default_factory=dict
    )
    node_death_errors: dict[NodeID, BaseException] = field(default_factory=dict)
    node_expected_exits: dict[NodeID, protocol.NodeDeathRecord] = field(
        default_factory=dict
    )
    node_finalize_acks: dict[NodeID, protocol.ShutdownAck] = field(
        default_factory=dict
    )
    node_finalize_decisions: dict[NodeID, threading.Event] = field(
        default_factory=dict
    )
    node_death_requests: dict[NodeID, protocol.ReportNodeDeath] = field(
        default_factory=dict
    )
    node_death_events: dict[NodeID, threading.Event] = field(
        default_factory=dict
    )
    node_gcs_deaths: dict[NodeID, protocol.NodeDeathRecord] = field(
        default_factory=dict
    )
    node_death_notified: set[NodeID] = field(default_factory=set)
    node_death_threads: dict[NodeID, threading.Thread] = field(
        default_factory=dict
    )
    node_death_lock: threading.RLock = field(default_factory=threading.RLock)
    node_death_transaction_lock: threading.RLock = field(
        default_factory=threading.RLock
    )
    latest_membership_epoch: int = 0
    latest_live_nodes: Tuple[protocol.NodeInfo, ...] = ()
    shutting_down: bool = False
    core_finalized: bool = False
    node_monitor_stopped: bool = False


def _dispatch_lanes_for(nodes: Sequence[_NodeRuntime]) -> int:
    """Bound Driver dispatch concurrency by ordinary Worker capacity."""

    worker_count = sum(len(node.startup.worker_ids) for node in nodes)
    if worker_count <= 0:
        raise ValueError("at least one ordinary Worker is required")
    return min(2, worker_count)


_runtime: Optional[_Runtime] = None
_runtime_lock = threading.RLock()


def _validate_num_cpus(value: object) -> Real:
    if (
        isinstance(value, bool)
        or not isinstance(value, Real)
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError("num_cpus must be a non-negative number")
    return value


def _validate_num_workers_per_node(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("num_workers_per_node must be an integer")
    if value not in (1, 2):
        raise ValueError("mini-Ray supports one or two Workers per node")
    return value


def _validate_max_retries(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("max_retries must be a non-negative integer")
    return value


def _validate_max_restarts(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("max_restarts must be a non-negative integer")
    return value


def _validate_remote_options(options: Mapping[str, object]) -> dict[str, object]:
    unknown = set(options).difference(_SUPPORTED_REMOTE_OPTIONS)
    if unknown:
        raise TypeError(
            "unsupported remote option(s): {}".format(
                ", ".join(sorted(unknown))
            )
        )
    checked = dict(options)
    if "num_cpus" in checked:
        checked["num_cpus"] = _validate_num_cpus(checked["num_cpus"])
    if "resources" in checked:
        checked["resources"] = _validate_resource_mapping(
            "resources", checked["resources"]
        )
    if "max_retries" in checked:
        checked["max_retries"] = _validate_max_retries(checked["max_retries"])
    if "max_restarts" in checked:
        checked["max_restarts"] = _validate_max_restarts(
            checked["max_restarts"]
        )
    if "num_returns" in checked:
        checked["num_returns"] = validate_num_returns(
            checked["num_returns"]
        )
    has_group = "placement_group" in checked
    has_index = "bundle_index" in checked
    if has_group != has_index:
        raise ValueError(
            "placement_group and bundle_index must be specified together"
        )
    if has_group:
        group = checked["placement_group"]
        if not isinstance(group, PlacementGroup):
            raise TypeError("placement_group must be a PlacementGroup")
        index = checked["bundle_index"]
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError("bundle_index must be a non-negative integer")
    return checked


def _validate_resource_mapping(name: str, value: object) -> ResourceVector:
    if isinstance(value, ResourceVector):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a resource mapping".format(name))
    return ResourceVector(value)


def _merge_task_resources(
    num_cpus: Real, custom_resources: ResourceVector
) -> ResourceVector:
    cpu_resources = ResourceVector({CPU: num_cpus})
    if CPU in custom_resources:
        if custom_resources.units(CPU) != cpu_resources.units(CPU):
            raise ValueError(
                "resources['CPU'] conflicts with num_cpus; use num_cpus for CPU"
            )
        return custom_resources
    return custom_resources + cpu_resources


def _validate_node_resources(
    num_nodes: int,
    num_cpus: Real,
    node_resources: Optional[Sequence[Mapping[str, ResourceQuantity] | ResourceVector]],
) -> Tuple[ResourceVector, ...]:
    if node_resources is None:
        return tuple(ResourceVector({CPU: num_cpus}) for _ in range(num_nodes))
    if isinstance(node_resources, (str, bytes)) or not isinstance(
        node_resources, Sequence
    ):
        raise TypeError("node_resources must be a sequence of resource mappings")
    if len(node_resources) != num_nodes:
        raise ValueError("node_resources length must equal num_nodes")
    return tuple(
        _validate_resource_mapping("node_resources[{}]".format(index), resources)
        for index, resources in enumerate(node_resources)
    )


def _validate_byte_limit(name: str, value: object, *, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > maximum
    ):
        raise ValueError(
            "{} must be an integer between 0 and {} bytes".format(name, maximum)
        )
    return value


def _cluster_snapshot_id(
    membership_epoch: int, nodes: Sequence[protocol.NodeInfo]
) -> str:
    """Return a stable content ID for a canonical node snapshot."""

    digest = hashlib.sha256()
    digest.update(b"miniray-cluster-snapshot-v2\0")
    digest.update(membership_epoch.to_bytes(8, "big"))
    for node in sorted(nodes, key=lambda item: bytes(item.node_id)):
        digest.update(bytes(node.node_id))
        digest.update(node.node_pid.to_bytes(8, "big"))
        digest.update(node.registration_epoch.to_bytes(8, "big"))
        host, port = node.address
        host_bytes = host.encode("utf-8")
        digest.update(len(host_bytes).to_bytes(4, "big"))
        digest.update(host_bytes)
        digest.update(port.to_bytes(2, "big"))
        for vector in (node.total_resources, node.available_resources):
            items = tuple(
                (name, vector.units(name)) for name in sorted(vector)
            )
            digest.update(len(items).to_bytes(4, "big"))
            for name, units in items:
                name_bytes = name.encode("utf-8")
                digest.update(len(name_bytes).to_bytes(4, "big"))
                digest.update(name_bytes)
                digest.update(str(units).encode("ascii"))
                digest.update(b"\0")
    return digest.hexdigest()


def _fetch_and_validate_cluster_snapshot(
    gcs_address: Address, nodes: Sequence[_NodeRuntime]
) -> protocol.InstallClusterSnapshot:
    reply = rpc_request(
        gcs_address,
        GET_NODES_HANDLER,
        protocol.GetNodes(),
        request_timeout=_START_TIMEOUT_SECONDS,
    )
    if not isinstance(reply, protocol.GetNodesReply):
        raise RuntimeError("GCS returned an invalid cluster bootstrap snapshot")

    registered = tuple(reply.nodes)
    if (
        isinstance(reply.membership_epoch, bool)
        or not isinstance(reply.membership_epoch, int)
        or reply.membership_epoch < 0
    ):
        raise RuntimeError("GCS returned an invalid membership epoch")
    registered_by_id = {node.node_id: node for node in registered}
    expected_by_id = {node.node_id: node for node in nodes}
    if len(registered_by_id) != len(registered):
        raise RuntimeError("GCS bootstrap snapshot contains duplicate NodeIDs")
    if set(registered_by_id) != set(expected_by_id):
        raise RuntimeError("GCS bootstrap snapshot does not match started nodes")

    for node_id, expected in expected_by_id.items():
        actual = registered_by_id[node_id]
        if actual.node_pid != expected.startup.node_pid:
            raise RuntimeError(
                "GCS registered the wrong process for node {}".format(node_id)
            )
        if actual.registration_epoch <= 0:
            raise RuntimeError(
                "GCS registered an invalid incarnation for node {}".format(node_id)
            )
        if actual.address != expected.startup.node_address:
            raise RuntimeError("GCS registered the wrong address for node {}".format(node_id))
        if actual.total_resources != expected.resources:
            raise RuntimeError("GCS registered the wrong capacity for node {}".format(node_id))
        if not actual.available_resources.fits_in(actual.total_resources):
            raise RuntimeError("GCS reported invalid availability for node {}".format(node_id))
        object.__setattr__(expected, "registration_epoch", actual.registration_epoch)

    canonical = tuple(sorted(registered, key=lambda item: bytes(item.node_id)))
    return protocol.InstallClusterSnapshot(
        membership_epoch=reply.membership_epoch,
        snapshot_id=_cluster_snapshot_id(reply.membership_epoch, canonical),
        nodes=canonical,
    )


def _install_cluster_snapshot(
    nodes: Sequence[_NodeRuntime], snapshot: protocol.InstallClusterSnapshot
) -> None:
    """Install the validated view on every Node before publishing runtime."""

    for node in nodes:
        reply = rpc_request(
            node.startup.node_address,
            INSTALL_CLUSTER_SNAPSHOT_HANDLER,
            snapshot,
            request_timeout=_START_TIMEOUT_SECONDS,
        )
        if not isinstance(reply, protocol.InstallClusterSnapshotReply):
            raise RuntimeError("node returned an invalid cluster snapshot reply")
        if (
            reply.membership_epoch != snapshot.membership_epoch
            or reply.snapshot_id != snapshot.snapshot_id
            or reply.node_id != node.node_id
            or not reply.installed
        ):
            raise RuntimeError(
                reply.error
                or "node {} rejected cluster snapshot {}".format(
                    node.node_id, snapshot.snapshot_id
                )
            )


def _managed_node_for_process(
    runtime: _Runtime, process: object
) -> _NodeRuntime:
    """Resolve a death observation by managed Process object identity."""

    matches = tuple(node for node in runtime.nodes if node.process is process)
    if len(matches) != 1:
        raise RuntimeError(
            "node monitor reported a process outside the managed runtime"
        )
    node = matches[0]
    if node.process.pid != node.startup.node_pid:
        raise RuntimeError(
            "managed Node Process PID changed from its startup identity"
        )
    return node


def _node_death_detection_id(node: _NodeRuntime, exit_code: int) -> str:
    """Derive one stable idempotency key for a physical Node incarnation."""

    digest = hashlib.sha256()
    digest.update(b"miniray-managed-node-death-v1\0")
    digest.update(bytes(node.node_id))
    digest.update(node.startup.node_pid.to_bytes(8, "big"))
    digest.update(node.registration_epoch.to_bytes(8, "big"))
    digest.update(str(exit_code).encode("ascii"))
    return digest.hexdigest()


def _node_death_request(
    node: _NodeRuntime, exit_code: int, *, expected: bool = False
) -> protocol.ReportNodeDeath:
    if node.registration_epoch <= 0:
        raise RuntimeError(
            "cannot report Node death before GCS registration is verified"
        )
    detection_id = (
        "node-expected-exit-{}-{}".format(
            node.node_id, node.registration_epoch
        )
        if expected
        else _node_death_detection_id(node, exit_code)
    )
    return protocol.ReportNodeDeath(
        detection_id=detection_id,
        node_id=node.node_id,
        node_pid=node.startup.node_pid,
        expected_registration_epoch=node.registration_epoch,
        exit_code=exit_code,
        reason=(
            protocol.NodeDeathReason.EXPECTED
            if expected else protocol.NodeDeathReason.PROCESS_EXIT
        ),
        detail=(
            "managed Node finalized and exited normally"
            if expected else "managed Node process exited"
        ),
    )


def _validate_node_death_reply(
    runtime: _Runtime,
    node: _NodeRuntime,
    request: protocol.ReportNodeDeath,
    reply: object,
) -> protocol.ReportNodeDeathReply:
    """Validate the GCS death fact and its atomic live-only snapshot."""

    if not isinstance(reply, protocol.ReportNodeDeathReply):
        raise RuntimeError("GCS returned an invalid Node death reply")
    if (
        reply.detection_id != request.detection_id
        or reply.node_id != request.node_id
        or reply.node_pid != request.node_pid
    ):
        raise RuntimeError("GCS changed Node death request identity")
    if reply.disposition not in (
        protocol.NodeDeathDisposition.APPLIED,
        protocol.NodeDeathDisposition.ALREADY_DEAD,
    ):
        raise RuntimeError(reply.error or "GCS rejected Node death report")
    death = reply.death
    if death is None or (
        death.detection_id != request.detection_id
        or death.node_id != request.node_id
        or death.node_pid != request.node_pid
        or death.registration_epoch != request.expected_registration_epoch
        or death.exit_code != request.exit_code
        or death.reason is not request.reason
        or death.detail != request.detail
        or reply.membership_epoch < death.death_epoch
    ):
        raise RuntimeError("GCS returned a conflicting Node death tombstone")

    managed = {managed_node.node_id: managed_node for managed_node in runtime.nodes}
    actual_live = {info.node_id for info in reply.live_nodes}
    if node.node_id in actual_live or not actual_live.issubset(managed):
        raise RuntimeError(
            "GCS Node death reply contains an invalid managed live set"
        )
    for info in reply.live_nodes:
        expected = managed[info.node_id]
        if (
            info.node_pid != expected.startup.node_pid
            or info.registration_epoch != expected.registration_epoch
            or info.address != expected.startup.node_address
            or info.total_resources != expected.resources
            or info.state is not protocol.NodeMembershipState.ALIVE
        ):
            raise RuntimeError(
                "GCS live snapshot changed a managed Node incarnation"
            )
    return reply


def _install_recovery_snapshot(
    runtime: _Runtime,
    membership_epoch: int,
    live_nodes: Sequence[protocol.NodeInfo],
) -> Optional[protocol.InstallClusterSnapshot]:
    """Replay one death-epoch snapshot until every survivor acknowledges."""

    canonical = tuple(sorted(live_nodes, key=lambda item: bytes(item.node_id)))
    snapshot = protocol.InstallClusterSnapshot(
        membership_epoch=membership_epoch,
        snapshot_id=_cluster_snapshot_id(membership_epoch, canonical),
        nodes=canonical,
    )
    managed = {node.node_id: node for node in runtime.nodes}
    acknowledgements = []
    for info in canonical:
        node = managed[info.node_id]
        retry_deadline = time.monotonic() + _STOP_TIMEOUT_SECONDS
        while True:
            with runtime.node_death_lock:
                if runtime.latest_membership_epoch > membership_epoch:
                    return None
            if not node.process.is_alive():
                # A later physical exit is handled by its own observation.  Do
                # not deadlock this transaction trying to install an obsolete
                # view on a process whose exact sentinel is already ready.
                return None
            try:
                candidate = rpc_request(
                    node.startup.node_address,
                    INSTALL_CLUSTER_SNAPSHOT_HANDLER,
                    snapshot,
                    request_timeout=_NODE_DEATH_RPC_TIMEOUT_SECONDS,
                )
            except Exception:
                if time.monotonic() >= retry_deadline:
                    raise TimeoutError(
                        "surviving Node snapshot ACK did not converge"
                    )
                threading.Event().wait(_NODE_DEATH_RETRY_INTERVAL_SECONDS)
                continue
            if not isinstance(candidate, protocol.InstallClusterSnapshotReply):
                raise RuntimeError(
                    "surviving Node returned an invalid snapshot ACK"
                )
            if (
                candidate.membership_epoch != snapshot.membership_epoch
                or candidate.snapshot_id != snapshot.snapshot_id
                or candidate.node_id != node.node_id
            ):
                raise RuntimeError(
                    "surviving Node changed recovery snapshot identity"
                )
            if not candidate.installed:
                raise RuntimeError(
                    candidate.error or "surviving Node rejected recovery snapshot"
                )
            acknowledgements.append(candidate)
            break
    # Keep the actual ACK vector; a local installed flag cannot prove that
    # every other survivor reached the same control-plane barrier.
    runtime.latest_snapshot_acks = tuple(acknowledgements)
    return snapshot


def _publish_installed_node_deaths(runtime, snapshot, deaths) -> bool:
    """Retain a completed barrier on every survivor for embedded owners."""
    view = InstalledNodeDeathView(snapshot, runtime.latest_snapshot_acks, tuple(sorted(deaths, key=lambda item: item.death_epoch)))
    for node in snapshot.nodes:
        request = PublishInstalledNodeDeaths(node.node_id, view)
        deadline = time.monotonic() + _STOP_TIMEOUT_SECONDS
        while True:
            with runtime.node_death_lock:
                if runtime.latest_membership_epoch != snapshot.membership_epoch:
                    return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("certified Node-death view retention did not converge")
            try:
                reply = rpc_request(node.address, PUBLISH_NODE_DEATH_VIEW, request,
                    connect_timeout=min(0.25, remaining),
                    request_timeout=min(_NODE_DEATH_RPC_TIMEOUT_SECONDS, remaining), deadline=deadline)
                if type(reply) is not PublishInstalledNodeDeathsReply:
                    raise RuntimeError("Node did not ACK the certified death view")
                reply = replace(reply)
                if reply.request != request or not reply.accepted:
                    raise RuntimeError("Node rejected the exact certified death view")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                threading.Event().wait(min(_NODE_DEATH_RETRY_INTERVAL_SECONDS, max(0.0, deadline - time.monotonic())))
    return True


def _observe_managed_node_exit(runtime: _Runtime, process: object) -> None:
    """Commit one exact process death, refresh survivors, then notify Core."""

    node: Optional[_NodeRuntime] = None
    event: Optional[threading.Event] = None
    try:
        node = _managed_node_for_process(runtime, process)
        with runtime.node_death_lock:
            event = runtime.node_death_events.setdefault(
                node.node_id, threading.Event()
            )
            if node.node_id in runtime.node_deaths:
                event.set()
                return

        node.process.join(0)
        exit_code = node.process.exitcode
        if exit_code is None:
            exit_code = _await_managed_process_exitcode(
                node.process, time.monotonic() + _STOP_TIMEOUT_SECONDS
            )
        with runtime.node_death_lock:
            finalize_decision = runtime.node_finalize_decisions.get(node.node_id)
        if exit_code == 0 and finalize_decision is not None:
            # The server can flush its ACK and exit before the Driver's RPC
            # thread publishes that ACK locally.  Wait for the explicit Driver
            # decision so sentinel/ACK reordering cannot turn a graceful exit
            # into PROCESS_EXIT.
            # The final-ACK loop owns this event and sets it for every target in
            # all terminal paths.  Do not replace that protocol decision with a
            # timing guess: doing so could permanently misclassify a delayed
            # clean ACK as PROCESS_EXIT in the GCS tombstone.
            finalize_decision.wait()
        with runtime.node_death_lock:
            request = runtime.node_death_requests.get(node.node_id)
            if request is None:
                finalize_ack = runtime.node_finalize_acks.get(node.node_id)
                expected = bool(
                    exit_code == 0
                    and finalize_ack is not None
                    and finalize_ack.clean
                    and not finalize_ack.forced
                )
                request = _node_death_request(
                    node, exit_code, expected=expected
                )
                runtime.node_death_requests[node.node_id] = request

        retry_deadline = time.monotonic() + _STOP_TIMEOUT_SECONDS
        while True:
            try:
                raw_reply = rpc_request(
                    runtime.gcs_startup.gcs_address,
                    REPORT_NODE_DEATH_HANDLER,
                    request,
                    request_timeout=_NODE_DEATH_RPC_TIMEOUT_SECONDS,
                )
            except Exception:
                if time.monotonic() >= retry_deadline:
                    raise TimeoutError(
                        "GCS Node death report did not converge"
                    )
                threading.Event().wait(_NODE_DEATH_RETRY_INTERVAL_SECONDS)
                continue
            reply = _validate_node_death_reply(
                runtime, node, request, raw_reply
            )
            if not reply.actor_state_converged:
                if time.monotonic() >= retry_deadline:
                    raise TimeoutError(
                        "GCS Node death Actor state publication did not converge"
                    )
                threading.Event().wait(_NODE_DEATH_RETRY_INTERVAL_SECONDS)
                continue
            break

        assert reply.death is not None
        if reply.death.reason is protocol.NodeDeathReason.EXPECTED:
            with runtime.node_death_lock:
                runtime.node_expected_exits[node.node_id] = reply.death
        with runtime.node_death_lock:
            prior = runtime.node_gcs_deaths.get(node.node_id)
            if prior is not None and prior != reply.death:
                raise RuntimeError(
                    "GCS returned two death facts for one Node incarnation"
                )
            # Death commitment and latest membership-view adoption are separate:
            # an older reply may arrive after a later concurrent death while its
            # own immutable tombstone remains authoritative.
            inserted_death = prior is None
            runtime.node_gcs_deaths[node.node_id] = reply.death
            if (
                reply.membership_epoch == runtime.latest_membership_epoch
                and reply.live_nodes != runtime.latest_live_nodes
            ):
                if inserted_death:
                    runtime.node_gcs_deaths.pop(node.node_id, None)
                raise RuntimeError(
                    "GCS reused a membership epoch for different live Nodes"
                )
            if reply.membership_epoch > runtime.latest_membership_epoch:
                runtime.latest_membership_epoch = reply.membership_epoch
                runtime.latest_live_nodes = reply.live_nodes
            core_finalized = runtime.core_finalized

        if core_finalized:
            # Shutdown has already committed the Driver Core and fenced all new
            # work.  A late PROCESS_EXIT still needs an immutable GCS/runtime
            # fact, but no survivor scheduling snapshot or Task recovery can be
            # consumed anymore.
            with runtime.node_death_lock:
                runtime.node_death_notified.add(reply.death.node_id)
                runtime.node_deaths[reply.death.node_id] = reply.death
                runtime.node_death_errors.pop(reply.death.node_id, None)
                runtime.node_death_events.setdefault(
                    reply.death.node_id, threading.Event()
                ).set()
            return

        # Serialize snapshot installation and Core publication.  Concurrent
        # sentinel callbacks may commit GCS tombstones in parallel, but only the
        # latest live-only epoch is installed/published to the data plane.
        with runtime.node_death_transaction_lock:
            transaction_deadline = time.monotonic() + _STOP_TIMEOUT_SECONDS
            while True:
                with runtime.node_death_lock:
                    epoch = runtime.latest_membership_epoch
                    live_nodes = runtime.latest_live_nodes
                managed_by_id = {item.node_id: item for item in runtime.nodes}
                if any(
                    not managed_by_id[info.node_id].process.is_alive()
                    for info in live_nodes
                ):
                    # Its sentinel callback can report to GCS concurrently; do
                    # not install an epoch which is already physically stale.
                    threading.Event().wait(
                        _NODE_DEATH_RETRY_INTERVAL_SECONDS
                    )
                    if time.monotonic() >= transaction_deadline:
                        raise TimeoutError(
                            "latest membership view still contains an exited Node"
                        )
                    continue
                snapshot = _install_recovery_snapshot(
                    runtime, epoch, live_nodes
                )
                if snapshot is None:
                    if time.monotonic() >= transaction_deadline:
                        raise TimeoutError(
                            "membership snapshot was superseded without convergence"
                        )
                    continue
                with runtime.node_death_lock:
                    if epoch == runtime.latest_membership_epoch:
                        committed = tuple(runtime.node_gcs_deaths.values())
                    else:
                        continue
                if not _publish_installed_node_deaths(runtime, snapshot, committed):
                    if time.monotonic() >= transaction_deadline:
                        raise TimeoutError("certified death view was superseded without convergence")
                    continue
                break

            for committed_death in sorted(
                committed, key=lambda value: value.death_epoch
            ):
                with runtime.node_death_lock:
                    if committed_death.node_id in runtime.node_death_notified:
                        continue
                if not runtime.core_finalized:
                    runtime.core_worker.handle_node_death(
                        committed_death, snapshot
                    )
                with runtime.node_death_lock:
                    runtime.node_death_notified.add(committed_death.node_id)
                    runtime.node_deaths[committed_death.node_id] = committed_death
                    runtime.node_death_errors.pop(committed_death.node_id, None)
                    runtime.node_death_events.setdefault(
                        committed_death.node_id, threading.Event()
                    ).set()
            with runtime.node_death_lock:
                runtime.latest_snapshot = snapshot
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        if node is None:
            return
        with runtime.node_death_lock:
            runtime.node_death_errors[node.node_id] = exc
            event = runtime.node_death_events.setdefault(
                node.node_id, threading.Event()
            )
            event.set()


def _dispatch_managed_node_exit(
    runtime: _Runtime, process: object, *, allow_shutdown_cutover: bool = False
) -> None:
    """Keep the sentinel monitor nonblocking while one death transaction waits."""

    node = _managed_node_for_process(runtime, process)
    with runtime.node_death_lock:
        if (
            runtime.shutting_down and not allow_shutdown_cutover
        ) or node.node_id in runtime.node_death_threads:
            return

        def run() -> None:
            try:
                while True:
                    _observe_managed_node_exit(runtime, process)
                    with runtime.node_death_lock:
                        pending = (node.node_id in runtime.node_death_errors
                                   and node.node_id not in runtime.node_deaths
                                   and not runtime.shutting_down)
                    if not pending:
                        break
                    # The OS sentinel is reported only once. Retain this
                    # existing observer as the retry owner for an incomplete
                    # GCS/survivor/certificate transaction, rather than losing
                    # a committed death after a temporary control-RPC failure.
                    threading.Event().wait(0.1)
            finally:
                with runtime.node_death_lock:
                    runtime.node_death_threads.pop(node.node_id, None)

        thread = threading.Thread(
            target=run,
            name="miniray-node-death-{}".format(node.node_id),
            daemon=True,
        )
        runtime.node_death_threads[node.node_id] = thread
        thread.start()


def _stop_node_monitor(runtime: _Runtime) -> None:
    """Stop and reconcile exit observation before forced teardown/handle close.

    On the clean path, Node Finalize and graceful exit waits precede this
    cutover. Exact Finalize ACKs, not stopping the monitor, classify expected
    exits. Pending managed sentinels and death observers must still converge.
    """

    with runtime.node_death_lock:
        if runtime.node_monitor_stopped:
            return
        runtime.shutting_down = True
        monitor = runtime.node_monitor
    if monitor is not None and not monitor.stop(_STOP_TIMEOUT_SECONDS):
        raise RuntimeError("managed Node monitor did not stop before shutdown")

    # Closing the monitor wake pipe can win the same wait as a process sentinel.
    # Reconcile every ready managed sentinel after the monitor thread stops so a
    # cutover cannot lose an unexpected death observation.
    sentinels = tuple(node.process.sentinel for node in runtime.nodes)
    ready = set(connection_wait(sentinels, timeout=0))
    for node in runtime.nodes:
        if node.process.sentinel in ready:
            _dispatch_managed_node_exit(
                runtime, node.process, allow_shutdown_cutover=True
            )
    deadline = time.monotonic() + _STOP_TIMEOUT_SECONDS
    while True:
        with runtime.node_death_lock:
            threads = tuple(runtime.node_death_threads.values())
        if not threads:
            break
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        with runtime.node_death_lock:
            alive = tuple(thread for thread in threads if thread.is_alive())
        if alive:
            raise RuntimeError("Node death transaction did not quiesce")
        break
    with runtime.node_death_lock:
        runtime.node_monitor_stopped = True


def _close_process(process: mp.Process) -> None:
    try:
        process.close()
    except ValueError:
        pass


def _attempt_startup_cleanup(operation: Callable[[], object]) -> None:
    """Run one rollback obligation without suppressing the remaining ones.

    Startup owns heterogeneous resources whose cleanup methods deliberately
    have different failure surfaces (RPC, process joins, sockets, and pipe
    handles).  Once startup has failed, one secondary cleanup exception must
    not strand resources acquired earlier or later in the transaction.  The
    original startup exception remains the public failure.
    """

    try:
        operation()
    except Exception:
        pass


def _await_managed_process_exitcode(process: object, deadline: float) -> int:
    """Wait for one sentinel-ready managed child to publish ``exitcode``.

    On Darwin a sentinel may become readable one scheduling turn before the
    shared ``multiprocessing.Process`` handle exposes its terminal return code,
    especially when the monitor and a test failpoint join the same child.  This
    polls only that exact managed handle within the caller's existing deadline.
    """

    while True:
        exit_code = getattr(process, "exitcode", None)
        if exit_code is not None:
            if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                raise RuntimeError("managed process published an invalid exit code")
            return exit_code
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                "managed process sentinel did not publish a terminal exit code"
            )
        step = min(_NODE_DEATH_RETRY_INTERVAL_SECONDS, remaining)
        process.join(step)
        if getattr(process, "exitcode", None) is None:
            threading.Event().wait(step)


def _node_process_main(*args: object) -> None:
    """Give a Node and its nested Worker one exact POSIX process group."""

    if hasattr(os, "setsid"):
        os.setsid()
    node_main(*args)


def _process_group_exists(process_group_id: int) -> bool:
    if not hasattr(os, "killpg"):
        return False
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_process_group(process_group_id: int) -> None:
    """Best-effort signal one exact child-owned process group.

    On macOS a group whose members crossed into exit/zombie state may still be
    observable by signal 0 while a following SIGKILL returns ``EPERM``.  That
    is neither permission to broaden the target nor a reason to abandon the
    remaining startup rollback.  The caller still joins the exact managed
    child and later cleanup verifies concrete PIDs/endpoints.
    """

    if (
        isinstance(process_group_id, bool)
        or not isinstance(process_group_id, int)
        or process_group_id <= 0
    ):
        raise ValueError("process_group_id must be a positive integer")
    if not hasattr(os, "killpg"):
        return
    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        # Preserve the same exact PGID and continue to the bounded existence
        # check/SIGKILL attempt.  Never substitute a PID, parent, or wildcard.
        pass
    deadline = time.monotonic() + _TERMINATE_GRACE_SECONDS
    while _process_group_exists(process_group_id):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.02, remaining))
    if _process_group_exists(process_group_id):
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            # EPERM can be a terminal-state race on Darwin.  The direct child
            # join in _force_stop_process and the rest of rollback must still
            # run; callers observe actual exit state rather than this syscall.
            pass


def _force_stop_process(process: mp.Process, *, process_group: bool = False) -> bool:
    """Join a process and terminate it only after the graceful bound."""

    process_id = process.pid
    try:
        process.join(_STOP_TIMEOUT_SECONDS)
    except (AssertionError, ValueError):
        return False
    group_alive = bool(
        process_group
        and process_id is not None
        and _process_group_exists(process_id)
    )
    forced = process.is_alive() or group_alive
    if group_alive and process_id is not None:
        _terminate_process_group(process_id)
        process.join(_TERMINATE_GRACE_SECONDS)
        # If the process-group syscall raced with a Darwin EPERM, the managed
        # Node is still an exact direct child target.  Reap it if SIGTERM won;
        # otherwise terminate that same child so later rollback (notably GCS)
        # is never skipped.  Its finally/OS group signal remains responsible
        # for the nested Worker; the bounded smoke verifies both concrete PIDs.
        if process.is_alive():
            try:
                process.terminate()
            except (ProcessLookupError, PermissionError):
                pass
            process.join(_TERMINATE_GRACE_SECONDS)
    elif process.is_alive():
        process.terminate()
        process.join(_STOP_TIMEOUT_SECONDS)
    return forced


def _shutdown_node(node: _NodeRuntime) -> _NodeShutdownResult:
    ack = None
    status = None
    requested_force = False
    try:
        if node.process.is_alive():
            request = protocol.Shutdown.create("driver shutdown")
            try:
                candidate = rpc_request(
                    node.startup.node_address,
                    SHUTDOWN_HANDLER_NAME,
                    request,
                    request_timeout=_STOP_TIMEOUT_SECONDS,
                )
                if not isinstance(candidate, protocol.ShutdownAck):
                    raise RuntimeError("node returned an invalid shutdown ack")
                if candidate.request_id != request.request_id:
                    raise RuntimeError(
                        "node returned a shutdown ack for another request"
                    )
                if len(candidate.child_pids) != len(node.startup.worker_pids):
                    raise RuntimeError(
                        "node shutdown ack reports the wrong Worker slot count"
                    )
                ack = candidate
                candidate_status = rpc_request(
                    node.startup.node_address,
                    SHUTDOWN_STATUS_HANDLER,
                    protocol.ShutdownStatusRequest(request.request_id, finalize=True),
                    request_timeout=_STOP_TIMEOUT_SECONDS,
                )
                if not isinstance(candidate_status, protocol.ShutdownStatus):
                    raise RuntimeError("node returned an invalid shutdown status")
                if candidate_status.request_id != request.request_id:
                    raise RuntimeError(
                        "node returned shutdown status for another request"
                    )
                if candidate_status.child_pids != candidate.child_pids:
                    raise RuntimeError(
                        "node shutdown status disagrees with its Worker identities"
                    )
                if (
                    candidate_status.child_exitcodes != candidate.child_exitcodes
                    or candidate_status.child_cleans != candidate.child_cleans
                ):
                    raise RuntimeError(
                        "node shutdown status disagrees with its Worker ACK"
                    )
                status = candidate_status
            except Exception:
                # The exact PID is still retained below and force-cleaned; the
                # report deliberately records missing/invalid acknowledgements.
                pass
        requested_force = _force_stop_process(node.process, process_group=True)
        node_exitcode = node.process.exitcode
        forced = requested_force or bool(getattr(ack, "forced", False))
        resources_clean = bool(
            getattr(
                status,
                "resources_clean",
                getattr(ack, "resources_clean", False),
            )
        )
        return _NodeShutdownResult(
            node_exitcode=node_exitcode,
            node_clean=node_exitcode == 0 and not forced,
            worker_pids=(
                ack.child_pids
                if ack is not None
                else node.startup.worker_pids
            ),
            worker_exitcodes=(
                ack.child_exitcodes
                if ack is not None
                else (None,) * len(node.startup.worker_pids)
            ),
            worker_cleans=(
                ack.child_cleans
                if ack is not None
                else (False,) * len(node.startup.worker_pids)
            ),
            worker_forced=(
                ack.child_forced
                if ack is not None
                else (requested_force,) * len(node.startup.worker_pids)
            ),
            forced=forced,
            finalized=bool(getattr(status, "finalized", False)),
            shutdown_ack_clean=bool(getattr(ack, "clean", False)),
            resources_clean=resources_clean,
        )
    finally:
        _close_process(node.process)


def _shutdown_gcs(
    process: mp.Process, startup: protocol.GCSStartup
) -> Tuple[Optional[int], bool, bool]:
    ack = None
    try:
        if process.is_alive():
            request = protocol.Shutdown.create("driver shutdown")
            try:
                candidate = rpc_request(
                    startup.gcs_address,
                    GCS_SHUTDOWN_HANDLER,
                    request,
                    request_timeout=_STOP_TIMEOUT_SECONDS,
                )
                if not isinstance(candidate, protocol.ShutdownAck):
                    raise RuntimeError("GCS returned an invalid shutdown ack")
                if candidate.request_id != request.request_id:
                    raise RuntimeError("GCS returned an ack for another request")
                ack = candidate
            except Exception:
                pass
        # A process that exited cleanly before this call needs no ACK from this
        # shutdown attempt; this primarily matters during startup rollback.
        already_stopped_cleanly = not process.is_alive() and process.exitcode == 0
        forced = _force_stop_process(process)
        exitcode = process.exitcode
        clean = (
            exitcode == 0
            and not forced
            and (already_stopped_cleanly or bool(getattr(ack, "clean", False)))
        )
        return exitcode, clean, forced
    finally:
        _close_process(process)


def _parallel_node_rpc(
    nodes: Sequence[_NodeRuntime],
    handler: str,
    message: object,
    *,
    deadline: float,
) -> dict[NodeID, object]:
    """Send one shutdown phase to all live Nodes under one deadline."""

    replies: dict[NodeID, object] = {}
    lock = threading.Lock()

    def call(node: _NodeRuntime) -> None:
        if not node.process.is_alive():
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            reply = rpc_request(
                node.startup.node_address,
                handler,
                message,
                request_timeout=max(0.001, min(_STOP_TIMEOUT_SECONDS, remaining)),
            )
        except Exception:
            return
        with lock:
            replies[node.node_id] = reply

    threads = tuple(
        threading.Thread(
            target=call,
            args=(node,),
            name="miniray-cluster-shutdown-{}".format(node.node_id),
            daemon=True,
        )
        for node in nodes
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    return replies


def _drain_cluster_round(
    runtime: _Runtime,
    request: protocol.BeginDrain,
    *,
    deadline: float,
    nodes: Optional[Sequence[_NodeRuntime]] = None,
) -> tuple[bool, dict[NodeID, protocol.DrainStatus]]:
    """Drive Driver Core and every Node/Worker concurrently for one round."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False, {}
    round_deadline = time.monotonic() + min(2.25, remaining)
    core_result: list[bool] = []

    def drain_core() -> None:
        try:
            core_result.append(
                runtime.core_worker.shutdown(
                    max(0.001, round_deadline - time.monotonic()),
                    preserve_owner_protocol=True,
                )
            )
        except Exception:
            core_result.append(False)

    core_thread = threading.Thread(
        target=drain_core, name="miniray-driver-core-drain", daemon=True
    )
    core_thread.start()
    participating_nodes = runtime.nodes if nodes is None else tuple(nodes)
    raw = _parallel_node_rpc(
        participating_nodes,
        NODE_DRAIN_STATUS_HANDLER,
        request,
        deadline=round_deadline,
    )
    core_thread.join(max(0.0, round_deadline - time.monotonic()))
    statuses: dict[NodeID, protocol.DrainStatus] = {}
    for node in participating_nodes:
        candidate = raw.get(node.node_id)
        if (
            isinstance(candidate, protocol.DrainStatus)
            and candidate.request_id == request.request_id
            and candidate.component == "node:{}".format(node.node_id)
        ):
            statuses[node.node_id] = candidate
    return bool(core_result and core_result[-1]), statuses


def _force_nodes_concurrently(
    nodes: Sequence[_NodeRuntime],
    *,
    known_dead: frozenset[NodeID] = frozenset(),
) -> dict[NodeID, bool]:
    """Force all still-live Node process groups as one hygiene phase."""

    forced: dict[NodeID, bool] = {}
    lock = threading.Lock()

    def force(node: _NodeRuntime) -> None:
        process = node.process
        process_id = process.pid
        alive = process.is_alive()
        group_alive = bool(
            process_id is not None and _process_group_exists(process_id)
        )
        # A dead Node parent may leave Worker descendants in its isolated
        # process group.  Cleaning that orphaned group is a force action even
        # though ``Process.is_alive()`` is already false.
        # Killing descendants left behind by a Node whose death is already a
        # committed cluster fact is crash cleanup, not a shutdown force of the
        # Node incarnation itself.
        did_force = (alive or group_alive) and node.node_id not in known_dead
        if group_alive and process_id is not None:
            _terminate_process_group(process_id)
        elif alive:
            process.terminate()
        try:
            process.join(_TERMINATE_GRACE_SECONDS)
        except (AssertionError, ValueError):
            pass
        with lock:
            forced[node.node_id] = did_force

    threads = tuple(
        threading.Thread(
            target=force,
            args=(node,),
            name="miniray-force-node-{}".format(node.node_id),
            daemon=True,
        )
        for node in nodes
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(_STOP_TIMEOUT_SECONDS)
    return forced


@dataclass(frozen=True)
class PlacementGroup:
    """Immutable handle for one fully committed placement-group attempt.

    The GCS reply is the authority for the plan.  The public handle merely binds
    that committed plan to the CoreWorker which created it and maps a bundle
    index to the exact scheduling capability carried by a Task.
    """

    placement_group_id: PlacementGroupID
    attempt: int
    placements: Tuple[protocol.PlacementGroupSchedulingKey, ...]
    _core_worker: CoreWorker

    def __post_init__(self) -> None:
        if not isinstance(self.placement_group_id, PlacementGroupID):
            raise TypeError("placement_group_id must be a PlacementGroupID")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 0:
            raise ValueError("placement group attempt must be non-negative")
        placements = tuple(self.placements)
        if not placements:
            raise ValueError("a PlacementGroup must contain at least one bundle")
        expected_indexes = tuple(range(len(placements)))
        actual_indexes = tuple(key.bundle_index for key in placements)
        if actual_indexes != expected_indexes:
            raise ValueError(
                "placement group bundle indexes must be contiguous and ordered"
            )
        if any(
            not isinstance(key, protocol.PlacementGroupSchedulingKey)
            or key.placement_group_id != self.placement_group_id
            or key.attempt != self.attempt
            for key in placements
        ):
            raise ValueError("placement group placements changed committed identity")
        if not isinstance(self._core_worker, CoreWorker):
            raise TypeError("PlacementGroup must be bound to a CoreWorker")
        object.__setattr__(self, "placements", placements)

    @property
    def bundle_count(self) -> int:
        return len(self.placements)

    def _scheduling_key_for(
        self, core_worker: CoreWorker, bundle_index: int
    ) -> protocol.PlacementGroupSchedulingKey:
        if core_worker is not self._core_worker:
            raise ValueError(
                "PlacementGroup belongs to a different mini-Ray runtime"
            )
        # The immutable handle proves bundle identity, not continuing liveness.
        # Removal closes admission in the creating Core before any remote RPC,
        # so this local check fences stale handles even while remove is retrying.
        core_worker.assert_placement_group_task_admissible(
            self.placement_group_id, self.attempt
        )
        if (
            isinstance(bundle_index, bool)
            or not isinstance(bundle_index, int)
            or not 0 <= bundle_index < len(self.placements)
        ):
            raise ValueError(
                "bundle_index must identify a bundle in the PlacementGroup"
            )
        return self.placements[bundle_index]


class RemoteFunction:
    """A callable definition whose ``remote`` method submits a task."""

    def __init__(
        self,
        function: Callable[..., object],
        *,
        num_cpus: Real = 1,
        resources: Optional[Mapping[str, ResourceQuantity] | ResourceVector] = None,
        max_retries: int = 0,
        num_returns: int = 1,
        placement_group: Optional[PlacementGroup] = None,
        bundle_index: Optional[int] = None,
    ) -> None:
        if not callable(function) or isinstance(function, type):
            raise TypeError("ray.remote currently supports functions only")
        self._function = function
        self._num_cpus = _validate_num_cpus(num_cpus)
        self._custom_resources = _validate_resource_mapping(
            "resources", {} if resources is None else resources
        )
        self._resources = _merge_task_resources(
            self._num_cpus, self._custom_resources
        )
        self._max_retries = _validate_max_retries(max_retries)
        self._num_returns = validate_num_returns(num_returns)
        if (placement_group is None) != (bundle_index is None):
            raise ValueError(
                "placement_group and bundle_index must be specified together"
            )
        self._placement_group = placement_group
        self._bundle_index = bundle_index
        self._definition_owner: Optional[CoreWorker] = None
        self._definition: Optional[RemoteFunctionDefinition] = None
        self._definition_lock = threading.Lock()
        self.__name__ = getattr(function, "__name__", type(function).__name__)
        self.__qualname__ = getattr(function, "__qualname__", self.__name__)
        self.__doc__ = getattr(function, "__doc__", None)
        self.__module__ = getattr(function, "__module__", __name__)

    def __getstate__(self) -> dict[str, object]:
        """Serialize definition, not process-local Core caches or locks."""

        state = dict(self.__dict__)
        state["_definition_owner"] = None
        state["_definition"] = None
        state.pop("_definition_lock", None)
        return state

    def __setstate__(self, state: Mapping[str, object]) -> None:
        self.__dict__.update(state)
        self._definition_owner = None
        self._definition = None
        self._definition_lock = threading.Lock()

    def remote(
        self, *args: object, **kwargs: object
    ) -> ObjectRef | tuple[ObjectRef, ...]:
        core_worker = _active_core_worker()
        scheduling_key = (
            None
            if self._placement_group is None
            else self._placement_group._scheduling_key_for(
                core_worker, self._bundle_index
            )
        )
        definition = self._definition_for(core_worker)
        return core_worker.submit(
            definition,
            args,
            kwargs,
            self._resources,
            max_retries=self._max_retries,
            num_returns=self._num_returns,
            placement_group_scheduling_key=scheduling_key,
        )

    def options(self, **options: object) -> "RemoteFunction":
        checked = _validate_remote_options(options)
        if "max_restarts" in checked:
            raise TypeError(
                "max_restarts applies to Actor classes, not remote functions"
            )
        return RemoteFunction(
            self._function,
            num_cpus=checked.get("num_cpus", self._num_cpus),
            resources=checked.get("resources", self._custom_resources),
            max_retries=checked.get("max_retries", self._max_retries),
            num_returns=checked.get("num_returns", self._num_returns),
            placement_group=checked.get(
                "placement_group", self._placement_group
            ),
            bundle_index=checked.get("bundle_index", self._bundle_index),
        )

    def _definition_for(self, core_worker: CoreWorker) -> RemoteFunctionDefinition:
        with self._definition_lock:
            if self._definition_owner is not core_worker:
                self._definition = core_worker.define_remote_function(self._function)
                self._definition_owner = core_worker
            assert self._definition is not None
            return self._definition

    def __repr__(self) -> str:
        return "RemoteFunction({})".format(self.__qualname__)


class ActorMethod:
    def __init__(self, handle: "ActorHandle", name: str) -> None:
        self._handle = handle
        self._name = name

    def remote(self, *args: object, **kwargs: object) -> ObjectRef:
        return self._handle._remote_method(self._name, args, kwargs)


class ActorHandle:
    """Stable logical Actor identity; physical routes live in CoreWorker."""

    def __init__(self, core_worker: CoreWorker, endpoint: ActorEndpoint) -> None:
        self._core_worker = core_worker
        self._actor_id = endpoint.actor_id
        self._method_names = endpoint.method_names

    @property
    def actor_id(self) -> protocol.ActorID:
        """Return the stable logical identity, never a physical route."""

        return self._actor_id

    def debug_snapshot(self) -> protocol.ActorSnapshot:
        """Read the current GCS-authoritative Actor lifecycle snapshot.

        The handle deliberately does not cache this result.  In particular, an
        Actor restart may replace generation, WorkerID, PID, and address while
        the public handle and its ``ActorID`` remain unchanged.  This method is
        an observational hook consumed by :mod:`miniray.debug`; method calls
        continue to route through CoreWorker's independently fenced route cell.
        """

        return self._core_worker._query_actor_state(self._actor_id)

    def __getattr__(self, name: str) -> ActorMethod:
        if name not in self._method_names:
            raise AttributeError(name)
        return ActorMethod(self, name)

    def _remote_method(
        self, name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> ObjectRef:
        return self._core_worker.submit_actor_call(
            self._actor_id, name, args, kwargs
        )


class ActorClass:
    """A serializable class whose construction is coordinated by GCS."""

    def __init__(
        self, actor_class: type, *, num_cpus: Real = 1,
        resources: Optional[Mapping[str, ResourceQuantity] | ResourceVector] = None,
        max_restarts: int = 0,
    ) -> None:
        if not isinstance(actor_class, type):
            raise TypeError("ActorClass requires a Python class")
        self._class = actor_class
        self._num_cpus = _validate_num_cpus(num_cpus)
        self._custom_resources = _validate_resource_mapping(
            "resources", {} if resources is None else resources
        )
        self._resources = _merge_task_resources(
            self._num_cpus, self._custom_resources
        )
        self._max_restarts = _validate_max_restarts(max_restarts)
        self.__name__ = actor_class.__name__
        self.__qualname__ = actor_class.__qualname__

    def remote(self, *args: object, **kwargs: object) -> ActorHandle:
        core = _active_core_worker()
        payload = cloudpickle.dumps(self._class)
        methods = tuple(sorted(
            name for name, value in vars(self._class).items()
            if not name.startswith("_") and callable(value)
        ))
        key = protocol.FunctionKey(
            core.job_id, self._class.__module__, self._class.__qualname__,
            hashlib.sha256(payload).hexdigest(),
        )
        definition = protocol.ActorClassDefinition(
            key, payload, hashlib.sha256(payload).hexdigest(), methods
        )
        endpoint = core.create_actor(
            definition, args, kwargs, self._resources,
            max_restarts=self._max_restarts,
        )
        return ActorHandle(core, endpoint)

    def options(self, **options: object) -> "ActorClass":
        if "num_returns" in options:
            raise TypeError(
                "num_returns applies to remote functions, not Actor classes"
            )
        if "max_retries" in options:
            raise TypeError(
                "max_retries applies to remote functions, not Actor classes"
            )
        if "placement_group" in options or "bundle_index" in options:
            raise NotImplementedError(
                "placement-group Actor creation is outside the first PG slice"
            )
        checked = _validate_remote_options(options)
        return ActorClass(
            self._class,
            num_cpus=checked.get("num_cpus", self._num_cpus),
            resources=checked.get("resources", self._custom_resources),
            max_restarts=checked.get("max_restarts", self._max_restarts),
        )


def init(
    *,
    num_cpus: Real = 1,
    num_nodes: int = 1,
    num_workers_per_node: int = 1,
    node_resources: Optional[
        Sequence[Mapping[str, ResourceQuantity] | ResourceVector]
    ] = None,
    inline_threshold: int = _DEFAULT_INLINE_THRESHOLD_BYTES,
    object_store_bytes: int = _DEFAULT_OBJECT_STORE_BYTES,
    _test_worker_failpoint: Optional[WorkerFailpointConfig] = None,
    _test_fail_after_node_ready: Optional[int] = None,
    _test_placement_group_prepare_failure: Optional[
        PlacementGroupPrepareFailureConfig
    ] = None,
    _test_output_publication_gate: Optional[OutputPublicationGateConfig] = None,
    enable_tracing: bool = True,
) -> RuntimeContext:
    """Start one GCS and one Worker-pool-owning NodeManager per node.

    ``num_cpus`` is the capacity of *each* node when ``node_resources`` is not
    supplied.  Passing ``node_resources`` exposes heterogeneous and custom
    resources without adding a second cluster-configuration abstraction.
    ``num_workers_per_node`` is deliberately fixed to one or two so the
    teaching runtime exposes real local slot contention without becoming a
    production process-pool manager.
    """

    if current_core_worker() is not None:
        raise RuntimeError(
            "miniray.init() is Driver-only; a Worker is already attached to "
            "the current cluster"
        )

    checked_cpus = _validate_num_cpus(num_cpus)
    if isinstance(num_nodes, bool) or not isinstance(num_nodes, int):
        raise TypeError("num_nodes must be an integer")
    if num_nodes not in (1, 2):
        raise ValueError("mini-Ray supports one or two logical nodes")
    checked_workers_per_node = _validate_num_workers_per_node(
        num_workers_per_node
    )
    checked_node_resources = _validate_node_resources(
        num_nodes, checked_cpus, node_resources
    )
    checked_inline_threshold = _validate_byte_limit(
        "inline_threshold",
        inline_threshold,
        maximum=_MAX_INLINE_THRESHOLD_BYTES,
    )
    checked_object_store_bytes = _validate_byte_limit(
        "object_store_bytes",
        object_store_bytes,
        maximum=_MAX_OBJECT_STORE_BYTES,
    )
    if _test_worker_failpoint is not None and not isinstance(
        _test_worker_failpoint, WorkerFailpointConfig
    ):
        raise TypeError(
            "_test_worker_failpoint must be a WorkerFailpointConfig or None"
        )
    if (
        _test_fail_after_node_ready is not None
        and (
            isinstance(_test_fail_after_node_ready, bool)
            or not isinstance(_test_fail_after_node_ready, int)
        )
    ):
        raise TypeError(
            "_test_fail_after_node_ready must be a node index or None"
        )
    if (
        _test_fail_after_node_ready is not None
        and not 0 <= _test_fail_after_node_ready < num_nodes
    ):
        raise ValueError(
            "_test_fail_after_node_ready must identify a configured node"
        )
    if (
        _test_placement_group_prepare_failure is not None
        and not isinstance(
            _test_placement_group_prepare_failure,
            PlacementGroupPrepareFailureConfig,
        )
    ):
        raise TypeError(
            "_test_placement_group_prepare_failure must be a "
            "PlacementGroupPrepareFailureConfig or None"
        )
    if (
        _test_placement_group_prepare_failure is not None
        and _test_placement_group_prepare_failure.participant_ordinal
        > num_nodes
    ):
        raise ValueError(
            "placement-group prepare failure participant exceeds configured "
            "node bound"
        )
    if _test_output_publication_gate is not None:
        if type(_test_output_publication_gate) is not OutputPublicationGateConfig:
            raise TypeError("_test_output_publication_gate must be an OutputPublicationGateConfig or None")
        _test_output_publication_gate = replace(_test_output_publication_gate)
        if _test_output_publication_gate.node_index >= num_nodes:
            raise ValueError("_test_output_publication_gate must identify a configured node")

    global _runtime
    with _runtime_lock:
        if _runtime is not None:
            raise RuntimeError("mini-Ray is already initialized")

        if not isinstance(enable_tracing, bool):
            raise TypeError("enable_tracing must be a bool")
        trace_collector: Optional[TraceCollector] = None
        trace_address: Optional[Address] = None
        gcs_receive = None
        gcs_send = None
        gcs_process: Optional[mp.Process] = None
        all_node_processes: list[mp.Process] = []
        nodes: list[_NodeRuntime] = []
        gcs_startup = None
        core_worker: Optional[CoreWorker] = None
        owner_service: Optional[OwnerService] = None
        driver_event_sink = None
        runtime: Optional[_Runtime] = None
        try:
            # Acquisition begins inside the rollback domain.  A collector
            # owns a bound socket after construction, and Pipe/Process may
            # succeed independently, so record each handle before continuing.
            trace_collector = TraceCollector() if enable_tracing else None
            trace_address = (
                trace_collector.start() if trace_collector is not None else None
            )
            trace_config = (
                TraceSinkConfig(trace_address, "gcs")
                if trace_address is not None
                else None
            )
            context = mp.get_context("spawn")
            gcs_receive, gcs_send = context.Pipe(duplex=False)
            gcs_process = context.Process(
                target=gcs_main,
                args=(
                    gcs_send,
                    "127.0.0.1",
                    0,
                    trace_config,
                    _test_placement_group_prepare_failure,
                ),
                name="miniray-gcs",
                daemon=False,
            )
            gcs_process.start()
            gcs_send.close()
            if not gcs_receive.poll(_START_TIMEOUT_SECONDS):
                raise RuntimeError("GCS did not report readiness before timeout")
            ok, value = gcs_receive.recv()
            if not ok:
                raise RuntimeError("GCS failed during startup:\n{}".format(value))
            if not isinstance(value, protocol.GCSStartup):
                raise RuntimeError("GCS returned an invalid startup descriptor")
            if value.gcs_pid != gcs_process.pid:
                raise RuntimeError("GCS returned an inconsistent startup descriptor")
            gcs_startup = value

            for index, total_resources in enumerate(checked_node_resources):
                node_id = NodeID.random()
                receive_connection = None
                send_connection = None
                try:
                    # Start the per-Node rollback boundary before Pipe.  A
                    # Process-constructor failure must still close both ends.
                    receive_connection, send_connection = context.Pipe(
                        duplex=False
                    )
                    process = context.Process(
                        target=_node_process_main,
                        args=(
                            node_id,
                            total_resources,
                            send_connection,
                            None,
                            "127.0.0.1",
                            0,
                            checked_inline_threshold,
                            checked_object_store_bytes,
                            gcs_startup.gcs_address,
                            _test_worker_failpoint,
                            (
                                trace_config.for_role(
                                    "node:{}".format(node_id)
                                )
                                if trace_config
                                else None
                            ),
                            checked_workers_per_node,
                            (
                                _test_output_publication_gate
                                if _test_output_publication_gate is not None
                                and _test_output_publication_gate.node_index == index
                                else None
                            ),
                        ),
                        name="miniray-node-{}-{}".format(index, node_id),
                        daemon=False,
                    )
                    all_node_processes.append(process)
                    process.start()
                    send_connection.close()
                    # NodeServer starts its deliberately tiny fixed Worker pool
                    # slot by slot.  Give each configured slot one bounded
                    # startup window; otherwise a valid two-Worker startup can
                    # exceed the historical one-Worker Driver deadline.
                    node_start_timeout = (
                        _START_TIMEOUT_SECONDS * checked_workers_per_node
                    )
                    if not receive_connection.poll(node_start_timeout):
                        raise RuntimeError(
                            "node {} did not report readiness before timeout".format(
                                index
                            )
                        )
                    ok, value = receive_connection.recv()
                    if not ok:
                        raise RuntimeError(
                            "node {} failed during startup:\n{}".format(index, value)
                        )
                    if not isinstance(value, protocol.NodeStartup):
                        raise RuntimeError(
                            "node {} returned an invalid startup descriptor".format(
                                index
                            )
                        )
                    startup = value
                    if startup.node_id != node_id or startup.node_pid != process.pid:
                        raise RuntimeError(
                            "node {} returned an inconsistent startup descriptor".format(
                                index
                            )
                        )
                    if len(startup.worker_ids) != checked_workers_per_node:
                        raise RuntimeError(
                            "node {} reported {} Workers; expected {}".format(
                                index,
                                len(startup.worker_ids),
                                checked_workers_per_node,
                            )
                        )
                    _startup_node_ready_checkpoint(
                        index, startup, _test_fail_after_node_ready
                    )
                    nodes.append(
                        _NodeRuntime(node_id, total_resources, process, startup)
                    )
                finally:
                    if receive_connection is not None:
                        _attempt_startup_cleanup(receive_connection.close)
                    if send_connection is not None:
                        _attempt_startup_cleanup(send_connection.close)

            # Bootstrap is an explicit control-plane barrier.  Fetch the GCS
            # directory once, validate it against the exact children we
            # started, then install the same immutable view on every node.
            # Only after all acknowledgements may task submission become
            # visible through ``_runtime``.
            cluster_snapshot = _fetch_and_validate_cluster_snapshot(
                gcs_startup.gcs_address, nodes
            )
            _install_cluster_snapshot(nodes, cluster_snapshot)

            local_node = nodes[0]
            driver_event_sink = (
                remote_event_sink(trace_address, "driver-core")
                if trace_address
                else None
            )
            try:
                core_worker = CoreWorker(
                    local_node.startup.node_address,
                    local_node.node_id,
                    gcs_address=gcs_startup.gcs_address,
                    inline_threshold=checked_inline_threshold,
                    dispatch_lanes=_dispatch_lanes_for(nodes),
                    event_sink=driver_event_sink,
                    restartable_actor_owner=True,
                    installed_cluster_snapshot=cluster_snapshot,
                )
            except BaseException:
                # Core takes sink ownership at constructor entry.  This close
                # is an idempotent outer guard for failures before that call.
                if driver_event_sink is not None:
                    _attempt_startup_cleanup(driver_event_sink.close)
                raise
            # Driver-owned ObjectRefs need the same owner protocol as
            # Worker-owned refs.  The endpoint is published only after its
            # socket is listening, and before the runtime becomes observable.
            owner_service = OwnerService(core_worker)
            owner_address = owner_service.start()
            core_worker.owner_address = owner_address
            runtime = _Runtime(
                core_worker=core_worker,
                owner_service=owner_service,
                gcs_process=gcs_process,
                gcs_startup=gcs_startup,
                nodes=tuple(nodes),
                trace_collector=trace_collector,
                latest_snapshot=cluster_snapshot,
            )
            runtime.latest_membership_epoch = cluster_snapshot.membership_epoch
            runtime.latest_live_nodes = cluster_snapshot.nodes
            runtime.node_monitor = ManagedNodeMonitor(
                tuple(node.process for node in nodes),
                lambda process, runtime=runtime: _dispatch_managed_node_exit(
                    runtime, process
                ),
                mp_context=context,
            )
            # Start observation before exposing the runtime.  A Node which dies
            # in this narrow window is still reported through the same exact
            # managed Process identity instead of leaving a detection gap.
            runtime.node_monitor.start()
            public_context = RuntimeContext(
                gcs_pid=gcs_startup.gcs_pid,
                gcs_address=gcs_startup.gcs_address,
                nodes=tuple(
                    NodeRuntimeContext(
                        node_id=node.node_id,
                        node_address=node.startup.node_address,
                        node_pid=node.startup.node_pid,
                        worker_ids=node.startup.worker_ids,
                        worker_pids=node.startup.worker_pids,
                        worker_addresses=node.startup.worker_addresses,
                    )
                    for node in nodes
                ),
                trace_address=trace_address,
            )
            # RuntimeContext validation is the final fallible construction.
            # Publish only after it succeeds, so a failed init is never visible.
            _runtime = runtime
            return public_context
        except BaseException as startup_error:
            if _runtime is runtime:
                _runtime = None
            if runtime is not None:
                try:
                    _stop_node_monitor(runtime)
                except Exception as monitor_error:
                    # A live monitor/death transaction still owns these exact
                    # Process handles.  Preserve the runtime instead of closing
                    # resources underneath it; a caller can retry shutdown.
                    _runtime = runtime
                    raise RuntimeError(
                        "startup rollback could not quiesce Node monitoring"
                    ) from monitor_error
            if core_worker is not None:
                _attempt_startup_cleanup(
                    lambda: core_worker._abort_unpublished_startup(
                        _STOP_TIMEOUT_SECONDS
                    )
                )
            elif driver_event_sink is not None:
                _attempt_startup_cleanup(driver_event_sink.close)
            if owner_service is not None:
                _attempt_startup_cleanup(owner_service.stop)
            known_nodes = {id(node.process): node for node in nodes}
            for process in reversed(all_node_processes):
                node = known_nodes.get(id(process))
                if node is not None:
                    _attempt_startup_cleanup(lambda node=node: _shutdown_node(node))
                else:
                    _attempt_startup_cleanup(
                        lambda process=process: _force_stop_process(
                            process, process_group=True
                        )
                    )
                    _attempt_startup_cleanup(
                        lambda process=process: _close_process(process)
                    )
            if gcs_process is not None:
                if gcs_startup is not None:
                    _attempt_startup_cleanup(
                        lambda: _shutdown_gcs(gcs_process, gcs_startup)
                    )
                else:
                    _attempt_startup_cleanup(
                        lambda: _force_stop_process(gcs_process)
                    )
                    _attempt_startup_cleanup(
                        lambda: _close_process(gcs_process)
                    )
            if trace_collector is not None:
                _attempt_startup_cleanup(trace_collector.stop)
            raise
        finally:
            if gcs_receive is not None:
                _attempt_startup_cleanup(gcs_receive.close)
            if gcs_send is not None:
                _attempt_startup_cleanup(gcs_send.close)


def shutdown() -> Optional[ShutdownReport]:
    """Drain the whole cluster behind one barrier, then finalize it."""

    if current_core_worker() is not None:
        raise RuntimeError(
            "miniray.shutdown() is Driver-only; Worker tasks do not own the cluster"
        )

    global _runtime
    with _runtime_lock:
        runtime = _runtime
        _runtime = None
    if runtime is None:
        return None

    # Keep the sentinel monitor live throughout drain and, on the clean path,
    # Node finalization and graceful exit waits. During drain, refresh survivors
    # from committed Core-visible deaths. Stop and reconcile the monitor before
    # forced process cleanup or closing Process handles; exact Finalize ACKs,
    # not monitor shutdown, distinguish EXPECTED exits from PROCESS_EXIT.
    with runtime.node_death_lock:
        dead_nodes = dict(runtime.node_deaths)
    live_nodes = tuple(
        node for node in runtime.nodes if node.node_id not in dead_nodes
    )

    deadline = time.monotonic() + _CLUSTER_SHUTDOWN_TIMEOUT_SECONDS
    drain = protocol.BeginDrain.create("driver cluster shutdown")
    begun: set[NodeID] = set()
    latest_status: dict[NodeID, protocol.DrainStatus] = {}

    # Phase 1a: every Node installs the admission fence before any participant
    # starts dismantling its Core.  Replaying one epoch handles a lost ACK.
    while len(begun) != len(live_nodes) and time.monotonic() < deadline:
        with runtime.node_death_lock:
            refreshed_dead = dict(runtime.node_deaths)
        refreshed_live = tuple(
            node for node in runtime.nodes if node.node_id not in refreshed_dead
        )
        if tuple(node.node_id for node in refreshed_live) != tuple(
            node.node_id for node in live_nodes
        ):
            dead_nodes = refreshed_dead
            live_nodes = refreshed_live
            begun.intersection_update(node.node_id for node in live_nodes)
        replies = _parallel_node_rpc(
            live_nodes, NODE_BEGIN_DRAIN_HANDLER, drain, deadline=deadline
        )
        for node in live_nodes:
            candidate = replies.get(node.node_id)
            if (
                isinstance(candidate, protocol.DrainStatus)
                and candidate.request_id == drain.request_id
                and candidate.component == "node:{}".format(node.node_id)
                and candidate.drain_started
            ):
                begun.add(node.node_id)
        if len(begun) != len(live_nodes):
            threading.Event().wait(0.01)

    # Phases 1b/1c: while every survivor endpoint is still alive, first converge
    # independent Actor, PG, and publication owner-death control obligations,
    # then observe Driver Core and every Worker/Core clean twice.  A membership
    # change invalidates *all* of those observations and exact-replays every GCS
    # drain for the survivor set.
    actor_drain_clean = False
    pg_drain_clean = False
    owner_death_drain_clean = False
    consecutive_clean_rounds = 0
    core_clean = False
    while (
        len(begun) == len(live_nodes)
        and consecutive_clean_rounds < 2
        and time.monotonic() < deadline
    ):
        with runtime.node_death_lock:
            refreshed_dead = dict(runtime.node_deaths)
        refreshed_live = tuple(
            node for node in runtime.nodes if node.node_id not in refreshed_dead
        )
        if tuple(node.node_id for node in refreshed_live) != tuple(
            node.node_id for node in live_nodes
        ):
            dead_nodes = refreshed_dead
            live_nodes = refreshed_live
            begun.intersection_update(node.node_id for node in live_nodes)
            actor_drain_clean = False
            pg_drain_clean = False
            owner_death_drain_clean = False
            consecutive_clean_rounds = 0
            latest_status = {
                node_id: status for node_id, status in latest_status.items()
                if node_id in {node.node_id for node in live_nodes}
            }
            continue

        control_called = False
        if not actor_drain_clean:
            control_called = True
            try:
                actor_candidate = rpc_request(
                    runtime.gcs_startup.gcs_address,
                    DRAIN_ACTORS_HANDLER,
                    protocol.DrainActorsRequest(drain.request_id),
                    request_timeout=max(0.001, deadline - time.monotonic()),
                )
            except Exception:
                actor_candidate = None
            actor_drain_clean = bool(
                isinstance(actor_candidate, protocol.DrainActorsReply)
                and actor_candidate.request_id == drain.request_id
                and actor_candidate.accepted
                and actor_candidate.clean
                and not actor_candidate.active_actor_ids
            )
        if not pg_drain_clean:
            control_called = True
            try:
                pg_candidate = rpc_request(
                    runtime.gcs_startup.gcs_address,
                    DRAIN_PLACEMENT_GROUPS_HANDLER,
                    protocol.DrainPlacementGroupsRequest(drain.request_id),
                    request_timeout=max(0.001, deadline - time.monotonic()),
                )
            except Exception:
                pg_candidate = None
            pg_drain_clean = bool(
                isinstance(
                    pg_candidate, protocol.DrainPlacementGroupsReply
                )
                and pg_candidate.request_id == drain.request_id
                and pg_candidate.accepted
                and pg_candidate.clean
            )
        if not owner_death_drain_clean:
            control_called = True
            try:
                owner_death_candidate = rpc_request(
                    runtime.gcs_startup.gcs_address,
                    DRAIN_OWNER_DEATH_FENCES_HANDLER,
                    protocol.DrainOwnerDeathFences(drain.request_id),
                    request_timeout=max(0.001, deadline - time.monotonic()),
                )
            except Exception:
                owner_death_candidate = None
            owner_death_drain_clean = bool(
                isinstance(
                    owner_death_candidate,
                    protocol.DrainOwnerDeathFencesReply,
                )
                and owner_death_candidate.request_id == drain.request_id
                and owner_death_candidate.clean
                and owner_death_candidate.active_fences == 0
            )
        if control_called:
            # Recheck membership at the top before treating these acknowledgments
            # as a barrier.  This closes a death race between the GCS control
            # replies and the first Core/Node clean observation.
            if not (
                actor_drain_clean
                and pg_drain_clean
                and owner_death_drain_clean
            ):
                threading.Event().wait(0.01)
            continue

        core_clean, statuses = _drain_cluster_round(
            runtime, drain, deadline=deadline, nodes=live_nodes
        )
        latest_status.update(statuses)
        all_nodes_clean = len(statuses) == len(live_nodes) and all(
            statuses[node.node_id].clean for node in live_nodes
        )
        consecutive_clean_rounds = (
            consecutive_clean_rounds + 1
            if core_clean and all_nodes_clean
            else 0
        )
        if consecutive_clean_rounds < 2:
            threading.Event().wait(0.01)

    barrier_clean = consecutive_clean_rounds >= 2
    core_stopped = False
    final_acks: dict[NodeID, protocol.ShutdownAck] = {}
    try:
        with runtime.node_death_transaction_lock:
            with runtime.node_death_lock:
                committed_dead = dict(runtime.node_deaths)
                pending_committed = set(runtime.node_gcs_deaths).difference(
                    runtime.node_death_notified
                )
            committed_live = tuple(
                node for node in runtime.nodes
                if node.node_id not in committed_dead
            )
            if tuple(node.node_id for node in committed_live) != tuple(
                node.node_id for node in live_nodes
            ) or pending_committed:
                # A death transaction completed after the second clean
                # observation.  Do not commit Core from the obsolete set.
                barrier_clean = False
            driver_finalizable = (
                barrier_clean
                and runtime.core_worker.can_finalize_shutdown(
                    require_distributed_clean=True
                )
            )
            # Core commit shares the death-publication linearization lock.
            # The fresh membership/death check and two clean observations
            # precede owner closure; the monitor stays live through the later
            # Node Finalize/graceful exits, before its own cutover.
            if driver_finalizable and core_clean:
                # Owner cutover is irreversible even when stopping its
                # reference thread needs another bounded join. Do not send a
                # later Node death back into a committed Core during that gap.
                while True:
                    try:
                        core_stopped = runtime.core_worker.finalize_shutdown(
                            require_distributed_clean=True,
                            timeout=max(0.0, min(0.1, deadline - time.monotonic())),
                        )
                    finally:
                        if getattr(runtime.core_worker, "owner_protocol_closed", False):
                            runtime.core_finalized = True
                    if core_stopped:
                        runtime.core_finalized = True
                        break
                    if not runtime.core_finalized or time.monotonic() >= deadline:
                        break
    except Exception:
        core_stopped = False
    # The owner route may be closed only after Core finalization commits.  A
    # failed fresh check leaves live query/release/replay obligations; stopping
    # their only endpoint would turn a conservative unclean shutdown into an
    # unrecoverable reference leak while Nodes are still draining.
    if core_stopped and runtime.owner_service is not None:
        runtime.owner_service.stop()
    barrier_clean = barrier_clean and core_stopped
    if barrier_clean:
        finalize_targets = tuple(live_nodes)
        with runtime.node_death_lock:
            for node in finalize_targets:
                runtime.node_finalize_decisions.setdefault(
                    node.node_id, threading.Event()
                )
        finalize = protocol.FinalizeShutdown(drain.request_id)
        try:
            while time.monotonic() < deadline:
                with runtime.node_death_lock:
                    unexpected = set(runtime.node_deaths)
                    expected = set(runtime.node_expected_exits)
                pending_finalize = tuple(
                    node for node in finalize_targets
                    if node.node_id not in final_acks
                    and node.node_id not in unexpected
                    and node.node_id not in expected
                )
                if not pending_finalize:
                    break
                replies = _parallel_node_rpc(
                    pending_finalize,
                    NODE_FINALIZE_SHUTDOWN_HANDLER,
                    finalize,
                    deadline=deadline,
                )
                for node in pending_finalize:
                    candidate = replies.get(node.node_id)
                    if (
                        isinstance(candidate, protocol.ShutdownAck)
                        and candidate.request_id == drain.request_id
                        and candidate.component == "node:{}".format(node.node_id)
                        and node.node_id in latest_status
                        and candidate.child_pids
                        == latest_status[node.node_id].child_pids
                        and candidate.clean
                        and candidate.resources_clean
                        and candidate.children_clean
                        and len(candidate.child_exitcodes)
                        == len(node.startup.worker_pids)
                        and len(candidate.child_forced)
                        == len(node.startup.worker_pids)
                        and not candidate.forced
                        and all(
                            exit_code == 0 for exit_code in candidate.child_exitcodes
                        )
                    ):
                        final_acks[node.node_id] = candidate
                        with runtime.node_death_lock:
                            runtime.node_finalize_acks[node.node_id] = candidate
                            runtime.node_finalize_decisions[node.node_id].set()
                if pending_finalize:
                    threading.Event().wait(0.01)
        finally:
            with runtime.node_death_lock:
                for node in finalize_targets:
                    runtime.node_finalize_decisions[node.node_id].set()
    else:
        # No graceful Finalize will make a Node exit, but teardown below may
        # force one.  Stop the monitor before closing any Process handle.
        try:
            _stop_node_monitor(runtime)
        except Exception:
            with _runtime_lock:
                if _runtime is None:
                    _runtime = runtime
            raise

    # Let every successfully finalized Node exit in parallel while the monitor
    # still classifies EXPECTED versus PROCESS_EXIT terminal facts.  No Node is
    # force-stopped while another Node is still being given graceful rounds.
    def join_node(node: _NodeRuntime) -> None:
        try:
            node.process.join(max(0.0, deadline - time.monotonic()))
        except (AssertionError, ValueError):
            pass

    join_threads = tuple(
        threading.Thread(
            target=join_node,
            args=(node,),
            name="miniray-join-node-{}".format(node.node_id),
            daemon=True,
        )
        for node in live_nodes
    )
    for thread in join_threads:
        thread.start()
    for thread in join_threads:
        thread.join(max(0.0, deadline - time.monotonic()))

    try:
        _stop_node_monitor(runtime)
    except Exception:
        with _runtime_lock:
            if _runtime is None:
                _runtime = runtime
        raise
    with runtime.node_death_lock:
        dead_nodes = dict(runtime.node_deaths)

    forced_by_node = _force_nodes_concurrently(
        runtime.nodes, known_dead=frozenset(dead_nodes)
    )
    node_results_list: list[_NodeShutdownResult] = []
    for node in runtime.nodes:
        ack = final_acks.get(node.node_id)
        status = latest_status.get(node.node_id)
        forced = forced_by_node.get(node.node_id, False)
        exitcode = node.process.exitcode
        death = dead_nodes.get(node.node_id)
        worker_pids = (
            status.child_pids
            if status is not None
            and len(status.child_pids) == len(node.startup.worker_pids)
            else node.startup.worker_pids
        )
        worker_count = len(worker_pids)
        expected_death = bool(
            death is not None
            and death.reason is protocol.NodeDeathReason.EXPECTED
            and death.exit_code == 0
        )
        unexpected_death = death is not None and not expected_death
        if unexpected_death:
            # The monitor already proved this exact process group exited and
            # GCS committed the immutable tombstone.  It is an expected crash
            # outcome for this report, not a failed graceful-shutdown ACK.
            worker_exitcodes = (None,) * worker_count
            worker_cleans = (False,) * worker_count
            worker_forced = (False,) * worker_count
        elif ack is not None and not forced:
            worker_exitcodes = ack.child_exitcodes
            worker_cleans = ack.child_cleans
            worker_forced = ack.child_forced
        else:
            worker_exitcodes = (None,) * worker_count
            worker_cleans = (False,) * worker_count
            worker_forced = (forced,) * worker_count
        node_results_list.append(
            _NodeShutdownResult(
                node_exitcode=exitcode,
                node_clean=(
                    not unexpected_death
                    and (
                        barrier_clean
                        and ack is not None
                        and ack.clean
                        and exitcode == 0
                        and not forced
                    )
                ),
                worker_pids=(
                    ack.child_pids
                    if ack is not None and not forced
                    else worker_pids
                ),
                worker_exitcodes=worker_exitcodes,
                worker_cleans=worker_cleans,
                worker_forced=worker_forced,
                forced=forced,
                finalized=(
                    not unexpected_death
                    and ack is not None and ack.clean and not forced
                ),
                shutdown_ack_clean=(
                    not unexpected_death
                    and barrier_clean
                    and ack is not None
                    and ack.clean
                    and not forced
                ),
                resources_clean=bool(
                    not unexpected_death
                    and (
                        barrier_clean
                        and ack is not None
                        and ack.resources_clean
                        and status is not None
                        and status.resources_clean
                        and not forced
                    )
                ),
            )
        )
        _close_process(node.process)
    node_results = tuple(node_results_list)

    # An unclean path keeps the Driver owner route alive while Nodes still have
    # a chance to discharge accepted borrower/hold obligations.  Once every
    # remaining Node has exited or been explicitly forced, no remote owner
    # client remains; close the transport as forced cleanup without pretending
    # that Core finalization committed.
    if not core_stopped and runtime.owner_service is not None:
        runtime.owner_service.stop()
    if not core_stopped:
        # The normal preserve-owner drain keeps internal cleanup alive. Now
        # every managed Node has exited, so stop only the local transport
        # machinery without converting pending obligations into successful GC.
        stop_local = getattr(runtime.core_worker, "stop_after_cluster_exit", None)
        if stop_local is not None:
            stop_local(timeout=1.0)

    # The Driver Core crossed its local commit while Node cleanup endpoints
    # were still available.  Node finalization below that barrier cannot make a
    # failed Core commit look successful merely because child processes exited.
    gcs_exitcode, gcs_clean, gcs_forced = _shutdown_gcs(
        runtime.gcs_process, runtime.gcs_startup
    )
    report = ShutdownReport(
        core_stopped=core_stopped,
        gcs_pid=runtime.gcs_startup.gcs_pid,
        gcs_exitcode=gcs_exitcode,
        gcs_clean=gcs_clean,
        node_pids=tuple(node.startup.node_pid for node in runtime.nodes),
        node_exitcodes=tuple(result.node_exitcode for result in node_results),
        node_cleans=tuple(result.node_clean for result in node_results),
        worker_pids=tuple(
            worker_pid
            for result in node_results
            for worker_pid in result.worker_pids
        ),
        worker_exitcodes=tuple(
            worker_exitcode
            for result in node_results
            for worker_exitcode in result.worker_exitcodes
        ),
        worker_cleans=tuple(
            worker_clean
            for result in node_results
            for worker_clean in result.worker_cleans
        ),
        worker_forced=tuple(
            worker_forced
            for result in node_results
            for worker_forced in result.worker_forced
        ),
        node_forced=tuple(result.forced for result in node_results),
        node_finalized=tuple(result.finalized for result in node_results),
        node_shutdown_ack_clean=tuple(
            result.shutdown_ack_clean for result in node_results
        ),
        node_resources_clean=tuple(
            result.resources_clean for result in node_results
        ),
        node_deaths=tuple(dead_nodes.get(node.node_id) for node in runtime.nodes),
        gcs_forced=gcs_forced,
    )
    if runtime.trace_collector is not None:
        runtime.trace_collector.stop()
    return report


def is_initialized() -> bool:
    if current_core_worker() is not None:
        return True
    with _runtime_lock:
        return _runtime is not None


def trace() -> Tuple[protocol.TraceRecord, ...]:
    """Return the Driver collector's observational arrival-order snapshot."""

    if current_core_worker() is not None:
        raise RuntimeError(
            "miniray.trace() is Driver-only; Worker tasks do not own the collector"
        )

    runtime = _get_runtime()
    if runtime.trace_collector is None:
        return ()
    return runtime.trace_collector.records


def export_trace(path: object) -> None:
    """Atomically export the Driver's current trace snapshot as JSON Lines.

    Each line is one deterministically ordered ``TraceRecord`` with explicit
    event, causal, semantic-entity, and field data.  The destination is
    replaced rather than appended; exporting an empty or disabled trace
    therefore creates an empty file.  Like :func:`trace`, this is a
    Driver-only operation and requires an initialized runtime.
    """

    if current_core_worker() is not None:
        raise RuntimeError(
            "miniray.export_trace() is Driver-only; Worker tasks do not own "
            "the collector"
        )
    runtime = _get_runtime()
    records = (
        ()
        if runtime.trace_collector is None
        else runtime.trace_collector.records
    )
    write_trace_records_jsonl(path, records)


def remote(
    function: Optional[_T] = None, **options: object
) -> object:
    """Wrap a Python function for asynchronous remote execution."""

    checked = _validate_remote_options(options)

    def decorate(target: _T) -> object:
        if isinstance(target, type):
            if "num_returns" in checked:
                raise TypeError(
                    "num_returns applies to remote functions, not Actor classes"
                )
            if "max_retries" in checked:
                raise TypeError(
                    "max_retries applies to remote functions, not Actor classes"
                )
            if "placement_group" in checked or "bundle_index" in checked:
                raise NotImplementedError(
                    "placement-group Actor creation is outside the first PG slice"
                )
            return ActorClass(
                target,
                num_cpus=checked.get("num_cpus", 1),
                resources=checked.get("resources"),
                max_restarts=checked.get("max_restarts", 0),
            )
        if "max_restarts" in checked:
            raise TypeError(
                "max_restarts applies to Actor classes, not remote functions"
            )
        return RemoteFunction(
            target,
            num_cpus=checked.get("num_cpus", 1),
            resources=checked.get("resources"),
            max_retries=checked.get("max_retries", 0),
            num_returns=checked.get("num_returns", 1),
            placement_group=checked.get("placement_group"),
            bundle_index=checked.get("bundle_index"),
        )

    if function is None:
        return decorate
    return decorate(function)


def get(
    object_refs: Union[ObjectRef, Sequence[ObjectRef]],
    *,
    timeout: Optional[float] = None,
) -> object:
    """Resolve one ObjectRef, or a sequence while preserving input order."""

    core_worker = _active_core_worker()
    if isinstance(object_refs, ObjectRef):
        return core_worker.get(object_refs, timeout)
    if isinstance(object_refs, (str, bytes)) or not isinstance(object_refs, Sequence):
        raise TypeError("get expects an ObjectRef or a sequence of ObjectRefs")
    return core_worker.get_many(tuple(object_refs), timeout)


def put(value: object) -> ObjectRef:
    """Store a value in this runtime and return its immutable ObjectRef."""

    return _active_core_worker().put(value)


def placement_group(
    bundles: Sequence[Mapping[str, ResourceQuantity] | ResourceVector],
    *,
    strategy: str = "STRICT_PACK",
) -> PlacementGroup:
    """Synchronously create a fully committed placement group.

    No synthetic readiness ObjectRef is exposed: a successful return means GCS
    has observed every participant commit ACK and supplied one immutable key for
    every bundle.
    """

    if isinstance(bundles, (str, bytes)) or not isinstance(bundles, Sequence):
        raise TypeError("bundles must be a sequence of resource mappings")
    if not 1 <= len(bundles) <= 2:
        raise ValueError("placement_group requires one or two bundles")
    resources = tuple(
        _validate_resource_mapping("bundles[{}]".format(index), bundle)
        for index, bundle in enumerate(bundles)
    )
    try:
        checked_strategy = PlacementStrategy(strategy)
    except (TypeError, ValueError):
        raise ValueError(
            "strategy must be STRICT_PACK or STRICT_SPREAD"
        ) from None
    core = _active_core_worker()
    reply = core.create_placement_group(resources, checked_strategy)
    if not isinstance(reply, protocol.CreatePlacementGroupReply):
        raise RuntimeError("GCS returned an invalid placement-group reply")
    if not reply.accepted:
        raise RuntimeError(reply.error or "placement-group creation was rejected")
    if reply.phase is not protocol.PlacementGroupPhaseStatus.CREATED:
        raise RuntimeError(
            "GCS accepted placement-group creation before the group was committed"
        )
    if len(reply.placements) != len(resources):
        raise RuntimeError(
            "GCS returned an incomplete committed placement-group plan"
        )
    placements = tuple(sorted(reply.placements, key=lambda key: key.bundle_index))
    return PlacementGroup(
        reply.placement_group_id, reply.attempt, placements, core
    )


def remove_placement_group(group: PlacementGroup) -> bool:
    """Remove a group through the same CoreWorker that created its handle."""

    if not isinstance(group, PlacementGroup):
        raise TypeError("remove_placement_group expects a PlacementGroup")
    core = _active_core_worker()
    if core is not group._core_worker:
        raise ValueError("PlacementGroup belongs to a different mini-Ray runtime")
    reply = core.remove_placement_group(
        group.placement_group_id, group.attempt
    )
    if not isinstance(reply, protocol.RemovePlacementGroupReply):
        raise RuntimeError("GCS returned an invalid placement-group removal reply")
    if (
        reply.placement_group_id != group.placement_group_id
        or reply.attempt != group.attempt
    ):
        raise RuntimeError("GCS changed placement-group identity during removal")
    if not reply.accepted:
        raise RuntimeError(reply.error or "placement-group removal was rejected")
    return reply.removed


def drop_object(
    object_ref: ObjectRef, node_id: Optional[NodeID] = None
) -> bool:
    """Teaching failpoint that drops one physical object replica.

    This is a deliberately small debug surface for recovery experiments, not
    normal object-lifetime management.  The logical ObjectRef remains valid:
    a later ``get`` either reports an unreconstructable ``put`` object or, once
    K1 reconstruction is enabled, replays its producer lineage.
    """

    return _active_core_worker().drop_object(object_ref, node_id=node_id)


def _test_crash_node(
    node_id: NodeID, *, timeout: float = _STOP_TIMEOUT_SECONDS
) -> protocol.NodeDeathRecord:
    """Crash one exact managed Node and await the full recovery barrier.

    This private failpoint accepts a logical ``NodeID`` rather than a PID so a
    test cannot accidentally signal an unrelated process.  Returning proves
    the managed Process sentinel, GCS tombstone, all-survivor membership ACKs,
    and Core notification have all converged.
    """

    if not isinstance(node_id, NodeID):
        raise TypeError("_test_crash_node expects a NodeID")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, Real)
        or not math.isfinite(float(timeout))
        or timeout <= 0
    ):
        raise ValueError("timeout must be a positive finite number")
    runtime = _get_runtime()
    matches = tuple(node for node in runtime.nodes if node.node_id == node_id)
    if len(matches) != 1:
        raise ValueError("unknown managed NodeID: {}".format(node_id))
    node = matches[0]
    process = node.process
    if process.pid != node.startup.node_pid:
        raise RuntimeError("managed Node process identity changed")
    if not process.is_alive():
        raise RuntimeError("managed Node is not alive")
    if not hasattr(os, "killpg") or not hasattr(os, "getpgid"):
        raise RuntimeError("Node crash failpoint requires POSIX process groups")
    if os.getpgid(node.startup.node_pid) != node.startup.node_pid:
        raise RuntimeError("managed Node does not lead its isolated process group")

    with runtime.node_death_lock:
        if runtime.shutting_down:
            raise RuntimeError("cannot crash a Node during cluster shutdown")
        event = runtime.node_death_events.setdefault(node_id, threading.Event())
    os.killpg(node.startup.node_pid, signal.SIGKILL)
    crash_deadline = time.monotonic() + float(timeout)
    ready = connection_wait((process.sentinel,), timeout=float(timeout))
    if process.sentinel not in ready:
        raise TimeoutError("managed Node process did not exit before timeout")
    process.join(max(0.0, crash_deadline - time.monotonic()))
    if process.exitcode is None:
        _await_managed_process_exitcode(process, crash_deadline)
    remaining = max(0.0, crash_deadline - time.monotonic())
    if not event.wait(remaining):
        raise TimeoutError("Node death recovery barrier did not converge")
    with runtime.node_death_lock:
        error = runtime.node_death_errors.get(node_id)
        death = runtime.node_deaths.get(node_id)
    if error is not None:
        raise RuntimeError("Node death recovery failed") from error
    if death is None:
        raise RuntimeError("Node death barrier finished without a tombstone")
    return death


def wait(
    object_refs: Sequence[ObjectRef],
    *,
    num_returns: int = 1,
    timeout: Optional[float] = None,
) -> Tuple[list[ObjectRef], list[ObjectRef]]:
    """Return ready and remaining refs without fetching their values."""

    if isinstance(object_refs, (str, bytes)) or not isinstance(object_refs, Sequence):
        raise TypeError("wait expects a sequence of ObjectRefs")
    return _active_core_worker().wait(
        tuple(object_refs), num_returns=num_returns, timeout=timeout
    )


def _get_runtime() -> _Runtime:
    with _runtime_lock:
        if _runtime is None:
            raise RuntimeError("call miniray.init() before using the runtime")
        return _runtime


def _active_core_worker() -> CoreWorker:
    """Return the thread-bound Worker Core or the process Driver Core."""

    bound = current_core_worker()
    if bound is not None:
        if not isinstance(bound, CoreWorker):
            raise RuntimeError("active runtime binding does not contain a CoreWorker")
        return bound
    return _get_runtime().core_worker


atexit.register(shutdown)


__all__ = [
    "ActorClass",
    "ActorHandle",
    "NodeRuntimeContext",
    "ObjectRef",
    "PlacementGroup",
    "RemoteFunction",
    "RuntimeContext",
    "ShutdownReport",
    "ExecutionContext",
    "export_trace",
    "get",
    "init",
    "is_initialized",
    "drop_object",
    "put",
    "placement_group",
    "remote",
    "remove_placement_group",
    "shutdown",
    "trace",
    "wait",
]
