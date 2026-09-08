"""Compatibility facade for pure resource-yield and node-loss exercises.

The classes in this module perform no RPC, process management, or sleeping.  They
make two K1 invariants explicit and deterministic:

* a task blocked in ``get`` may yield only its CPU allocation while continuing
  to own GPU, memory, and custom resources; and
* declaring a node dead releases every live lease allocation on that node once.

``ResourceLedger`` is the only resource-accounting authority.  ``Allocation``
delegates every mutation to it so the pure model cannot drift from the reducer
that will be attached to Node lease records.  ``RuntimeState`` itself is a
deprecated, non-runtime teaching facade pending that Node wiring; production
paths must not install it as a second node/lease table.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import threading
from typing import Dict, Optional, Tuple

from .ids import LeaseID, NodeID
from .resources import AllocationToken, CPU, ResourceLedger, ResourceVector


class RuntimeStateError(RuntimeError):
    """Base class for invalid runtime-state operations."""


class UnknownRuntimeNodeError(RuntimeStateError, KeyError):
    """The node is not registered in this state model."""


class DeadRuntimeNodeError(RuntimeStateError):
    """A terminally dead node cannot grant new work."""


class UnknownLeaseError(RuntimeStateError, KeyError):
    """The lease is not known to this state model."""


class LeaseConflictError(RuntimeStateError):
    """An idempotency key was replayed with a different lease payload."""


class AllocationTransitionError(RuntimeStateError):
    """An allocation cannot perform the requested transition."""


class AllocationState(str, Enum):
    ACTIVE = "ACTIVE"
    CPU_YIELDED = "CPU_YIELDED"
    CPU_REACQUIRED = "CPU_REACQUIRED"
    RELEASED = "RELEASED"


@dataclass(frozen=True)
class AllocationSnapshot:
    token: AllocationToken
    resources: ResourceVector
    held_resources: ResourceVector
    state: AllocationState


def _without_cpu(resources: ResourceVector) -> ResourceVector:
    return ResourceVector(
        {name: quantity for name, quantity in resources.to_dict().items() if name != CPU}
    )


def _only_cpu(resources: ResourceVector) -> ResourceVector:
    quantity = resources.quantity(CPU)
    return ResourceVector({CPU: quantity}) if quantity else ResourceVector.empty()


class Allocation:
    """One task's resources and its blocking-get CPU transition.

    The allocation may perform multiple sequential yield/reacquire cycles.
    Duplicate transition calls are harmless, and final release credits exactly
    the resources still held according to ``ResourceLedger``.
    """

    def __init__(
        self,
        ledger: ResourceLedger,
        resources: ResourceVector,
        token: AllocationToken,
    ) -> None:
        if not isinstance(ledger, ResourceLedger):
            raise TypeError("ledger must be a ResourceLedger")
        if not isinstance(resources, ResourceVector):
            raise TypeError("resources must be a ResourceVector")
        if not isinstance(token, AllocationToken):
            raise TypeError("token must be an AllocationToken")
        self._ledger = ledger
        self._resources = resources
        self._cpu_resources = _only_cpu(resources)
        self._retained_resources = _without_cpu(resources)
        self._token = token
        self._state = AllocationState.ACTIVE
        self._lock = threading.RLock()
        self._ledger.allocate(resources, token)

    @classmethod
    def acquire(
        cls,
        ledger: ResourceLedger,
        resources: ResourceVector,
        token: Optional[AllocationToken] = None,
    ) -> "Allocation":
        return cls(ledger, resources, token or AllocationToken.random())

    @property
    def token(self) -> AllocationToken:
        return self._token

    @property
    def resources(self) -> ResourceVector:
        return self._resources

    @property
    def state(self) -> AllocationState:
        with self._lock:
            return self._state

    @property
    def held_resources(self) -> ResourceVector:
        with self._lock:
            record = self._ledger.record(self._token)
            if record is None:
                raise AllocationTransitionError("allocation token disappeared")
            return record.held_resources

    @property
    def cpu_is_yielded(self) -> bool:
        with self._lock:
            return self._state is AllocationState.CPU_YIELDED

    @property
    def released(self) -> bool:
        with self._lock:
            return self._state is AllocationState.RELEASED

    def yield_cpu(self) -> bool:
        """Release CPU exactly once, retaining every non-CPU resource.

        The return value reports whether this call performed the transition.
        Replayed yield messages return ``False``, including after CPU has already
        been reacquired.
        """

        with self._lock:
            if self._state in (
                AllocationState.CPU_YIELDED, AllocationState.RELEASED
            ):
                return False
            if not self._ledger.yield_cpu(self._token):
                return False
            self._state = AllocationState.CPU_YIELDED
            return True

    def reacquire(self) -> bool:
        """Immediately restore the original CPU share.

        Like production Ray, this may create CPU debt when another task consumed
        the yielded capacity.  ``False`` means only that the allocation was
        finally released; an already-active replay returns ``True``.
        """

        with self._lock:
            if self._state in (
                AllocationState.ACTIVE,
                AllocationState.CPU_REACQUIRED,
            ):
                return True
            if self._state is AllocationState.RELEASED:
                return False
            if not self._ledger.reacquire_cpu(self._token):
                raise AllocationTransitionError(
                    "resource ledger rejected a yielded CPU reacquire"
                )
            self._state = AllocationState.CPU_REACQUIRED
            return True

    reacquire_cpu = reacquire

    def release(self) -> bool:
        """Finally release every resource still held, exactly once."""

        with self._lock:
            if self._state is AllocationState.RELEASED:
                return False
            self._ledger.release(self._token)
            self._state = AllocationState.RELEASED
            return True

    def snapshot(self) -> AllocationSnapshot:
        with self._lock:
            return AllocationSnapshot(
                self._token, self._resources, self.held_resources, self._state
            )


class NodeRuntimeStatus(str, Enum):
    ALIVE = "ALIVE"
    DEAD = "DEAD"


class LeaseRuntimeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"
    NODE_LOST = "NODE_LOST"


@dataclass(frozen=True)
class LeaseSnapshot:
    lease_id: LeaseID
    node_id: NodeID
    resources: ResourceVector
    status: LeaseRuntimeStatus
    allocation: AllocationSnapshot


@dataclass(frozen=True)
class NodeRuntimeSnapshot:
    node_id: NodeID
    status: NodeRuntimeStatus
    total: ResourceVector
    available: ResourceVector
    lease_ids: Tuple[LeaseID, ...]


@dataclass
class _NodeRecord:
    ledger: ResourceLedger
    status: NodeRuntimeStatus = NodeRuntimeStatus.ALIVE


@dataclass
class _LeaseRecord:
    lease_id: LeaseID
    node_id: NodeID
    resources: ResourceVector
    allocation: Allocation
    status: LeaseRuntimeStatus = LeaseRuntimeStatus.ACTIVE


class RuntimeState:
    """Pure node/lease table backed by node-local resource ledgers."""

    def __init__(self) -> None:
        self._nodes: Dict[NodeID, _NodeRecord] = {}
        self._leases: Dict[LeaseID, _LeaseRecord] = {}
        self._lock = threading.RLock()

    def register_node(self, node_id: NodeID, ledger: ResourceLedger) -> bool:
        if not isinstance(node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        if not isinstance(ledger, ResourceLedger):
            raise TypeError("ledger must be a ResourceLedger")
        with self._lock:
            existing = self._nodes.get(node_id)
            if existing is not None:
                if existing.ledger is not ledger:
                    raise RuntimeStateError(
                        "node ID is already registered with another ledger"
                    )
                return False
            self._nodes[node_id] = _NodeRecord(ledger)
            return True

    def grant_lease(
        self,
        lease_id: LeaseID,
        node_id: NodeID,
        resources: ResourceVector,
        *,
        allocation_token: Optional[AllocationToken] = None,
    ) -> LeaseSnapshot:
        if not isinstance(lease_id, LeaseID):
            raise TypeError("lease_id must be a LeaseID")
        if not isinstance(resources, ResourceVector):
            raise TypeError("resources must be a ResourceVector")
        with self._lock:
            existing = self._leases.get(lease_id)
            if existing is not None:
                if existing.node_id != node_id or existing.resources != resources:
                    raise LeaseConflictError(
                        "lease ID was replayed with a different node or resource vector"
                    )
                return self._lease_snapshot(existing)

            node = self._node(node_id)
            if node.status is NodeRuntimeStatus.DEAD:
                raise DeadRuntimeNodeError("a dead node cannot grant a lease")
            token = allocation_token or AllocationToken(
                "lease:" + lease_id.hex
            )
            allocation = Allocation.acquire(node.ledger, resources, token)
            record = _LeaseRecord(lease_id, node_id, resources, allocation)
            self._leases[lease_id] = record
            return self._lease_snapshot(record)

    def yield_cpu(self, lease_id: LeaseID) -> bool:
        with self._lock:
            lease = self._lease(lease_id)
            if lease.status is not LeaseRuntimeStatus.ACTIVE:
                return False
            return lease.allocation.yield_cpu()

    def reacquire_cpu(self, lease_id: LeaseID) -> bool:
        with self._lock:
            lease = self._lease(lease_id)
            if lease.status is not LeaseRuntimeStatus.ACTIVE:
                return False
            return lease.allocation.reacquire()

    def release_lease(self, lease_id: LeaseID) -> bool:
        with self._lock:
            lease = self._lease(lease_id)
            if lease.status is not LeaseRuntimeStatus.ACTIVE:
                return False
            lease.allocation.release()
            lease.status = LeaseRuntimeStatus.RELEASED
            return True

    def mark_node_dead(self, node_id: NodeID) -> Tuple[LeaseID, ...]:
        """Release and fence every active lease on ``node_id``."""

        with self._lock:
            node = self._node(node_id)
            if node.status is NodeRuntimeStatus.DEAD:
                return ()
            node.status = NodeRuntimeStatus.DEAD
            affected = []
            for lease in sorted(
                self._leases.values(), key=lambda item: item.lease_id.hex
            ):
                if (
                    lease.node_id == node_id
                    and lease.status is LeaseRuntimeStatus.ACTIVE
                ):
                    lease.allocation.release()
                    lease.status = LeaseRuntimeStatus.NODE_LOST
                    affected.append(lease.lease_id)
            return tuple(affected)

    node_died = mark_node_dead

    def lease_snapshot(self, lease_id: LeaseID) -> LeaseSnapshot:
        with self._lock:
            return self._lease_snapshot(self._lease(lease_id))

    def node_snapshot(self, node_id: NodeID) -> NodeRuntimeSnapshot:
        with self._lock:
            node = self._node(node_id)
            lease_ids = tuple(
                sorted(
                    (
                        lease.lease_id
                        for lease in self._leases.values()
                        if lease.node_id == node_id
                    ),
                    key=lambda lease_id: lease_id.hex,
                )
            )
            return NodeRuntimeSnapshot(
                node_id,
                node.status,
                node.ledger.total,
                node.ledger.available,
                lease_ids,
            )

    def _node(self, node_id: NodeID) -> _NodeRecord:
        try:
            return self._nodes[node_id]
        except KeyError:
            raise UnknownRuntimeNodeError("unknown node: {!r}".format(node_id)) from None

    def _lease(self, lease_id: LeaseID) -> _LeaseRecord:
        try:
            return self._leases[lease_id]
        except KeyError:
            raise UnknownLeaseError("unknown lease: {!r}".format(lease_id)) from None

    @staticmethod
    def _lease_snapshot(lease: _LeaseRecord) -> LeaseSnapshot:
        return LeaseSnapshot(
            lease.lease_id,
            lease.node_id,
            lease.resources,
            lease.status,
            lease.allocation.snapshot(),
        )


NodeLeaseState = RuntimeState
