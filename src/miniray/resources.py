"""Exact resource accounting and a small Ray-style hybrid policy.

All resource quantities are stored as integer milli-units.  Floats are accepted at
the API boundary for convenience, but are converted through their decimal string
and never participate in accounting arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import math
import random
import threading
import uuid
from typing import Dict, Iterator, Mapping, Optional, Sequence, Tuple, Union

from .errors import (
    AllocationAlreadyReleasedError,
    AllocationTokenError,
    InsufficientResourcesError,
    InvalidResourceError,
)
from .ids import NodeID


RESOURCE_SCALE = 1_000
CPU = "CPU"
GPU = "GPU"
MEMORY = "memory"
ResourceQuantity = Union[int, float, str, Decimal]


def _resource_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise InvalidResourceError("resource names must be non-empty strings")
    return name.strip()


def _to_units(value: ResourceQuantity) -> int:
    if isinstance(value, bool):
        raise InvalidResourceError("boolean is not a resource quantity")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidResourceError(f"invalid resource quantity: {value!r}") from exc
    if not number.is_finite() or number < 0:
        raise InvalidResourceError("resource quantities must be finite and non-negative")
    scaled = number * RESOURCE_SCALE
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise InvalidResourceError(
            f"resource quantities support at most {int(math.log10(RESOURCE_SCALE))} "
            "decimal places"
        )
    return int(integral)


@dataclass(frozen=True, init=False)
class ResourceVector(Mapping[str, Decimal]):
    """An immutable, normalized sparse resource vector."""

    _items: Tuple[Tuple[str, int], ...]

    def __init__(
        self, resources: Optional[Mapping[str, ResourceQuantity]] = None
    ) -> None:
        normalized: Dict[str, int] = {}
        if resources is not None:
            if not isinstance(resources, Mapping):
                raise InvalidResourceError("resources must be a mapping")
            for raw_name, value in resources.items():
                name = _resource_name(raw_name)
                units = _to_units(value)
                if units:
                    normalized[name] = units
        object.__setattr__(self, "_items", tuple(sorted(normalized.items())))

    @classmethod
    def _from_units(cls, resources: Mapping[str, int]) -> "ResourceVector":
        instance = object.__new__(cls)
        object.__setattr__(
            instance,
            "_items",
            tuple(sorted((name, units) for name, units in resources.items() if units)),
        )
        return instance

    @classmethod
    def empty(cls) -> "ResourceVector":
        return cls()

    @classmethod
    def of(cls, **resources: ResourceQuantity) -> "ResourceVector":
        return cls(resources)

    def __iter__(self) -> Iterator[str]:
        return (name for name, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, name: str) -> Decimal:
        units = self.units(name)
        if not units and name not in dict(self._items):
            raise KeyError(name)
        return Decimal(units) / RESOURCE_SCALE

    def units(self, name: str) -> int:
        normalized = _resource_name(name)
        return dict(self._items).get(normalized, 0)

    def quantity(self, name: str) -> Decimal:
        return Decimal(self.units(name)) / RESOURCE_SCALE

    def to_dict(self) -> Dict[str, Decimal]:
        return {name: Decimal(units) / RESOURCE_SCALE for name, units in self._items}

    def to_float_dict(self) -> Dict[str, float]:
        return {name: units / RESOURCE_SCALE for name, units in self._items}

    def is_zero(self) -> bool:
        return not self._items

    def fits_in(self, capacity: "ResourceVector") -> bool:
        if not isinstance(capacity, ResourceVector):
            return False
        return all(units <= capacity.units(name) for name, units in self._items)

    def __add__(self, other: "ResourceVector") -> "ResourceVector":
        if not isinstance(other, ResourceVector):
            return NotImplemented
        names = set(self) | set(other)
        return ResourceVector._from_units(
            {name: self.units(name) + other.units(name) for name in names}
        )

    def __sub__(self, other: "ResourceVector") -> "ResourceVector":
        if not isinstance(other, ResourceVector):
            return NotImplemented
        if not other.fits_in(self):
            raise InsufficientResourcesError(
                f"cannot subtract {other.to_dict()} from {self.to_dict()}"
            )
        names = set(self) | set(other)
        return ResourceVector._from_units(
            {name: self.units(name) - other.units(name) for name in names}
        )

    def __repr__(self) -> str:
        contents = ", ".join(
            f"{name}={Decimal(units) / RESOURCE_SCALE}" for name, units in self._items
        )
        return f"ResourceVector({contents})"


@dataclass(frozen=True, order=True)
class AllocationToken:
    """An opaque idempotency key for one ledger allocation."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value:
            raise AllocationTokenError("allocation token must be a non-empty string")

    @classmethod
    def random(cls) -> "AllocationToken":
        return cls(uuid.uuid4().hex)

    def __str__(self) -> str:
        return self.value


class AllocationState(str, Enum):
    ACTIVE = "ACTIVE"
    CPU_YIELDED = "CPU_YIELDED"
    RELEASED = "RELEASED"


@dataclass(frozen=True)
class AllocationRecord:
    token: AllocationToken
    resources: ResourceVector
    state: AllocationState

    @property
    def held_resources(self) -> ResourceVector:
        """Return the resources still charged to this allocation.

        A blocking task returns only its canonical ``CPU`` quantity.  GPU,
        memory, and custom resources remain held until the lease terminates.
        """

        if self.state is AllocationState.RELEASED:
            return ResourceVector.empty()
        if self.state is AllocationState.CPU_YIELDED:
            return ResourceVector._from_units(
                {
                    name: units
                    for name, units in self.resources._items
                    if name != CPU
                }
            )
        return self.resources


@dataclass(frozen=True)
class ResourceLedgerSnapshot:
    total: ResourceVector
    available: ResourceVector
    allocations: Tuple[AllocationRecord, ...]
    cpu_debt: Decimal = Decimal(0)


class ResourceLedger:
    """Thread-safe local source of truth for resource allocations.

    Repeating a live allocation with the same token and request is a no-op.
    Repeating a release is also a no-op.  A delayed acquire after release is rejected
    rather than resurrecting the token, which is the safe behavior for duplicate RPCs.

    Internally, canonical CPU availability is signed.  When a blocked parent
    returns its CPU, another task may consume it; unblocking the parent then
    restores the parent's logical allocation immediately and records negative
    CPU availability (``cpu_debt``).  Scheduling observes the clamped
    :attr:`available` vector and therefore cannot grant more CPU until running
    tasks repay that debt.  Non-CPU resources are never allowed to go negative.
    """

    def __init__(self, total: ResourceVector) -> None:
        if not isinstance(total, ResourceVector):
            raise InvalidResourceError("total must be a ResourceVector")
        self._total = total
        self._available_units = dict(total._items)
        self._records: Dict[AllocationToken, AllocationRecord] = {}
        self._lock = threading.RLock()

    @property
    def total(self) -> ResourceVector:
        return self._total

    @property
    def available(self) -> ResourceVector:
        """Return the non-negative capacity visible to schedulers."""

        with self._lock:
            return self._public_available_locked()

    @property
    def signed_cpu_available(self) -> Decimal:
        """Return CPU availability including unblock debt."""

        with self._lock:
            return Decimal(self._available_units.get(CPU, 0)) / RESOURCE_SCALE

    @property
    def cpu_debt(self) -> Decimal:
        """Return CPU owed by resumed workers, always non-negative."""

        with self._lock:
            units = self._available_units.get(CPU, 0)
            return Decimal(max(0, -units)) / RESOURCE_SCALE

    def can_allocate(self, request: ResourceVector) -> bool:
        if not isinstance(request, ResourceVector):
            raise InvalidResourceError("request must be a ResourceVector")
        with self._lock:
            return self._can_allocate_locked(request)

    def allocate(
        self,
        request: ResourceVector,
        token: Optional[AllocationToken] = None,
    ) -> AllocationToken:
        if not isinstance(request, ResourceVector):
            raise InvalidResourceError("request must be a ResourceVector")
        allocation_token = token or AllocationToken.random()
        if not isinstance(allocation_token, AllocationToken):
            raise AllocationTokenError("token must be an AllocationToken")
        with self._lock:
            existing = self._records.get(allocation_token)
            if existing is not None:
                if existing.resources != request:
                    raise AllocationTokenError(
                        "the same allocation token cannot name different resources"
                    )
                if existing.state is AllocationState.RELEASED:
                    raise AllocationAlreadyReleasedError(
                        "a released allocation token cannot be reacquired"
                    )
                return allocation_token
            if not self._can_allocate_locked(request):
                raise InsufficientResourcesError(
                    f"requested {request.to_dict()}, available "
                    f"{self._public_available_locked().to_dict()}"
                )
            self._change_available_locked(request, sign=-1)
            self._records[allocation_token] = AllocationRecord(
                allocation_token, request, AllocationState.ACTIVE
            )
            self._assert_invariants_locked()
            return allocation_token

    def try_allocate(
        self,
        request: ResourceVector,
        token: Optional[AllocationToken] = None,
    ) -> Optional[AllocationToken]:
        try:
            return self.allocate(request, token)
        except InsufficientResourcesError:
            return None

    def release(self, token: AllocationToken) -> bool:
        if not isinstance(token, AllocationToken):
            raise AllocationTokenError("token must be an AllocationToken")
        with self._lock:
            existing = self._records.get(token)
            if existing is None:
                raise AllocationTokenError("unknown allocation token")
            if existing.state is AllocationState.RELEASED:
                return False
            # A yielded allocation has already returned its CPU.  Releasing it
            # must therefore credit only the resources it still holds.
            self._change_available_locked(existing.held_resources, sign=1)
            self._records[token] = AllocationRecord(
                token, existing.resources, AllocationState.RELEASED
            )
            self._assert_invariants_locked()
            return True

    def yield_cpu(self, token: AllocationToken) -> bool:
        """Return one live allocation's CPU while retaining all other resources.

        The operation is transition-idempotent.  ``False`` means that the
        allocation is already yielded/released or owns no canonical CPU.
        Episode ordering belongs to the Node lease state machine; this reducer
        only performs the authoritative resource mutation.
        """

        if not isinstance(token, AllocationToken):
            raise AllocationTokenError("token must be an AllocationToken")
        with self._lock:
            existing = self._record_locked(token)
            if existing.state is not AllocationState.ACTIVE:
                return False
            cpu_units = existing.resources.units(CPU)
            if cpu_units == 0:
                return False
            self._available_units[CPU] = (
                self._available_units.get(CPU, 0) + cpu_units
            )
            self._records[token] = AllocationRecord(
                token, existing.resources, AllocationState.CPU_YIELDED
            )
            self._assert_invariants_locked()
            return True

    def reacquire_cpu(self, token: AllocationToken) -> bool:
        """Restore yielded CPU immediately, allowing signed CPU debt.

        This intentionally does not wait for physical capacity.  Waiting would
        deadlock a parent behind the child that consumed its yielded CPU and
        would diverge from Ray's unblock semantics.
        """

        if not isinstance(token, AllocationToken):
            raise AllocationTokenError("token must be an AllocationToken")
        with self._lock:
            existing = self._record_locked(token)
            if existing.state is not AllocationState.CPU_YIELDED:
                return False
            cpu_units = existing.resources.units(CPU)
            assert cpu_units > 0
            self._available_units[CPU] = (
                self._available_units.get(CPU, 0) - cpu_units
            )
            self._records[token] = AllocationRecord(
                token, existing.resources, AllocationState.ACTIVE
            )
            self._assert_invariants_locked()
            return True

    def record(self, token: AllocationToken) -> Optional[AllocationRecord]:
        with self._lock:
            return self._records.get(token)

    def snapshot(self) -> ResourceLedgerSnapshot:
        with self._lock:
            records = tuple(
                sorted(self._records.values(), key=lambda record: record.token.value)
            )
            return ResourceLedgerSnapshot(
                self._total, self._public_available_locked(), records,
                Decimal(max(0, -self._available_units.get(CPU, 0)))
                / RESOURCE_SCALE,
            )

    def _record_locked(self, token: AllocationToken) -> AllocationRecord:
        try:
            return self._records[token]
        except KeyError:
            raise AllocationTokenError("unknown allocation token") from None

    def _can_allocate_locked(self, request: ResourceVector) -> bool:
        return all(
            units <= self._available_units.get(name, 0)
            for name, units in request._items
        )

    def _change_available_locked(
        self, resources: ResourceVector, *, sign: int
    ) -> None:
        assert sign in (-1, 1)
        for name, units in resources._items:
            self._available_units[name] = (
                self._available_units.get(name, 0) + sign * units
            )

    def _public_available_locked(self) -> ResourceVector:
        # CPU debt is an accounting fact, not schedulable negative capacity.
        return ResourceVector._from_units(
            {
                name: max(0, units) if name == CPU else units
                for name, units in self._available_units.items()
            }
        )

    def _assert_invariants_locked(self) -> None:
        for name in set(self._available_units) | set(self._total):
            available = self._available_units.get(name, 0)
            total = self._total.units(name)
            if available > total:
                raise AssertionError(
                    "resource ledger invariant violated: over-release"
                )
            if name != CPU and available < 0:
                raise AssertionError(
                    "resource ledger invariant violated: non-CPU debt"
                )


@dataclass(frozen=True)
class NodeSnapshot:
    """An immutable, possibly stale scheduling view of one node."""

    node_id: NodeID
    total: ResourceVector
    available: ResourceVector
    alive: bool = True
    labels: Tuple[Tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise InvalidResourceError("node_id must be a NodeID")
        if not isinstance(self.total, ResourceVector) or not isinstance(
            self.available, ResourceVector
        ):
            raise InvalidResourceError("total and available must be ResourceVectors")
        if not self.available.fits_in(self.total):
            raise InvalidResourceError("available resources cannot exceed total resources")
        object.__setattr__(self, "labels", tuple(sorted(tuple(self.labels))))

    @property
    def has_gpu(self) -> bool:
        return self.total.units(GPU) > 0

    def is_feasible(self, request: ResourceVector) -> bool:
        return self.alive and request.fits_in(self.total)

    def is_available(self, request: ResourceVector) -> bool:
        return self.is_feasible(request) and request.fits_in(self.available)

    def critical_resource_utilization(self) -> Decimal:
        utilizations = []
        for name, total_units in self.total._items:
            if total_units:
                used = total_units - self.available.units(name)
                utilizations.append(Decimal(used) / Decimal(total_units))
        return max(utilizations, default=Decimal(0))


class SchedulingStatus(str, Enum):
    SELECTED = "SELECTED"
    PENDING_CAPACITY = "PENDING_CAPACITY"
    INFEASIBLE = "INFEASIBLE"


@dataclass(frozen=True)
class SchedulingDecision:
    status: SchedulingStatus
    node_id: Optional[NodeID] = None
    selected_is_available: bool = False
    candidates: Tuple[NodeID, ...] = ()

    @property
    def selected(self) -> bool:
        return self.status is SchedulingStatus.SELECTED


class HybridPolicy:
    """A compact version of Ray's hybrid node scheduling policy.

    Live total-feasible nodes are considered first.  Currently available nodes beat
    busy-but-feasible nodes; CPU-only requests first try non-GPU nodes.  Nodes are
    ranked by critical utilization with low utilization truncated to zero, then a
    seeded random choice is made among the best ``top_k`` entries.
    """

    def __init__(
        self,
        *,
        spread_threshold: ResourceQuantity = "0.5",
        top_k: int = 1,
        top_k_fraction: ResourceQuantity = 0,
        seed: int = 0,
        avoid_gpu_nodes: bool = True,
    ) -> None:
        try:
            threshold = Decimal(str(spread_threshold))
            fraction = Decimal(str(top_k_fraction))
        except InvalidOperation as exc:
            raise InvalidResourceError("invalid scheduling-policy parameter") from exc
        if not threshold.is_finite() or not Decimal(0) <= threshold <= Decimal(1):
            raise InvalidResourceError("spread_threshold must be in [0, 1]")
        if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k < 1:
            raise InvalidResourceError("top_k must be a positive integer")
        if not fraction.is_finite() or not Decimal(0) <= fraction <= Decimal(1):
            raise InvalidResourceError("top_k_fraction must be in [0, 1]")
        self._spread_threshold = threshold
        self._top_k = top_k
        self._top_k_fraction = fraction
        self._avoid_gpu_nodes = bool(avoid_gpu_nodes)
        self._random = random.Random(seed)
        self._lock = threading.Lock()

    def _score(self, node: NodeSnapshot) -> Decimal:
        utilization = node.critical_resource_utilization()
        return Decimal(0) if utilization < self._spread_threshold else utilization

    def _prefer_non_gpu(
        self, request: ResourceVector, nodes: Sequence[NodeSnapshot]
    ) -> Tuple[NodeSnapshot, ...]:
        candidates = tuple(nodes)
        if (
            self._avoid_gpu_nodes
            and request.units(GPU) == 0
            and any(not node.has_gpu for node in candidates)
        ):
            return tuple(node for node in candidates if not node.has_gpu)
        return candidates

    def _choose(
        self,
        nodes: Sequence[NodeSnapshot],
        preferred_node_id: Optional[NodeID],
        cluster_size: int,
    ) -> Tuple[NodeSnapshot, Tuple[NodeID, ...]]:
        ranked = sorted(nodes, key=lambda node: (self._score(node), node.node_id.hex))
        minimum_score = self._score(ranked[0])
        if preferred_node_id is not None:
            for node in ranked:
                if node.node_id == preferred_node_id and self._score(node) == minimum_score:
                    return node, tuple(candidate.node_id for candidate in ranked)
        # Ray defines the fractional lower bound against the cluster snapshot,
        # not the already filtered candidate pool.  The final ``min`` clamps it
        # when fewer feasible/available nodes remain.
        fraction_count = int(Decimal(cluster_size) * self._top_k_fraction)
        count = min(len(ranked), max(1, self._top_k, fraction_count))
        with self._lock:
            selected = self._random.choice(ranked[:count])
        return selected, tuple(candidate.node_id for candidate in ranked)

    def schedule(
        self,
        request: ResourceVector,
        nodes: Sequence[NodeSnapshot],
        *,
        preferred_node_id: Optional[NodeID] = None,
        require_available: bool = True,
    ) -> SchedulingDecision:
        if not isinstance(request, ResourceVector):
            raise InvalidResourceError("request must be a ResourceVector")
        snapshots = tuple(nodes)
        feasible = tuple(node for node in snapshots if node.is_feasible(request))
        if not feasible:
            return SchedulingDecision(SchedulingStatus.INFEASIBLE)

        available = tuple(node for node in feasible if node.is_available(request))
        if available:
            pool = self._prefer_non_gpu(request, available)
            selected, ranked = self._choose(
                pool, preferred_node_id, len(snapshots)
            )
            return SchedulingDecision(
                SchedulingStatus.SELECTED, selected.node_id, True, ranked
            )

        if require_available:
            return SchedulingDecision(
                SchedulingStatus.PENDING_CAPACITY,
                candidates=tuple(node.node_id for node in feasible),
            )

        pool = self._prefer_non_gpu(request, feasible)
        selected, ranked = self._choose(pool, preferred_node_id, len(snapshots))
        return SchedulingDecision(
            SchedulingStatus.SELECTED, selected.node_id, False, ranked
        )
