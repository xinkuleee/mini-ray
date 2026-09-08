"""Pure placement-group planning and node-local reservation state.

The planner in this module operates only on snapshots.  It never mutates the
real resource ledger.  ``BundleReservationLedger`` is the complementary
node-local authority: it turns a plan into idempotent prepare/commit/abort
operations.  Keeping those roles separate makes stale cluster snapshots safe
and makes the gang-scheduling invariant visible in a small implementation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Mapping, Sequence

from .ids import NodeID, PlacementGroupID
from .resources import (
    AllocationState, AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector,
)


class PlacementStrategy(str, Enum):
    PACK = "PACK"
    SPREAD = "SPREAD"
    STRICT_PACK = "STRICT_PACK"
    STRICT_SPREAD = "STRICT_SPREAD"


class PlacementStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PENDING = "PENDING"
    INFEASIBLE = "INFEASIBLE"


@dataclass(frozen=True)
class Bundle:
    """One independently addressable resource bundle in a placement group."""

    index: int
    resources: ResourceVector


@dataclass(frozen=True)
class BundlePlacement:
    bundle_index: int
    node_id: NodeID


@dataclass(frozen=True)
class PlacementPlan:
    status: PlacementStatus
    placements: tuple[BundlePlacement, ...] = ()
    reason: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status is PlacementStatus.SUCCESS

    def node_for(self, bundle_index: int) -> NodeID:
        for placement in self.placements:
            if placement.bundle_index == bundle_index:
                return placement.node_id
        raise KeyError(bundle_index)


class _SearchLimitReached(RuntimeError):
    pass


def _id_key(value: object) -> str:
    return str(getattr(value, "value", value))


def _available(node: NodeSnapshot) -> ResourceVector:
    # ``available`` is the intended foundation API.  ``free`` makes the pure
    # algorithm convenient to reuse with small teaching fixtures.
    return getattr(node, "available", getattr(node, "free", None))


def _fits(capacity: ResourceVector, request: ResourceVector) -> bool:
    fits = getattr(capacity, "fits", None)
    if callable(fits):
        return bool(fits(request))
    return bool(request.fits_in(capacity))


def _subtract(capacity: ResourceVector, request: ResourceVector) -> ResourceVector:
    if hasattr(capacity, "subtract"):
        return capacity.subtract(request)
    return capacity - request


def _resource_items(vector: ResourceVector) -> tuple[tuple[str, int | float], ...]:
    """Best-effort adapter used only for placement ranking, never correctness."""

    for name in ("to_mapping", "as_mapping", "as_dict", "to_dict"):
        method = getattr(vector, name, None)
        if callable(method):
            return tuple(method().items())
    items = getattr(vector, "items", None)
    if callable(items):
        return tuple(items())
    if items is not None:
        return tuple(items)
    values = getattr(vector, "values", None)
    if isinstance(values, Mapping):
        return tuple(values.items())
    return ()


def _get(vector: ResourceVector, resource: str) -> int | float:
    getter = getattr(vector, "get")
    try:
        return getter(resource, 0)
    except TypeError:
        return getter(resource)


def _post_placement_slack(
    total: ResourceVector, remaining: ResourceVector, request: ResourceVector
) -> float:
    """Lower is a tighter (more PACK-like) placement.

    The score affects preference only.  Feasibility always comes from
    ``ResourceVector.fits`` and remains correct if a custom vector exposes no
    item iterator.
    """

    score = 0.0
    seen = False
    for resource, amount in _resource_items(request):
        if amount <= 0:
            continue
        denominator = float(_get(total, resource))
        if denominator > 0:
            score += float(_get(remaining, resource)) / denominator
            seen = True
    return score if seen else 0.0


def _normalize_bundles(
    bundles: Sequence[Bundle | ResourceVector | Mapping[str, int | float]],
) -> tuple[Bundle, ...]:
    normalized: list[Bundle] = []
    for position, item in enumerate(bundles):
        if isinstance(item, Bundle):
            bundle = item
        elif isinstance(item, ResourceVector):
            bundle = Bundle(position, item)
        else:
            if isinstance(item, Mapping):
                constructor = getattr(ResourceVector, "from_mapping", None)
                resources = (
                    constructor(item) if callable(constructor) else ResourceVector(item)
                )
            else:
                resources = item
            bundle = Bundle(position, resources)
        normalized.append(bundle)

    indexes = [bundle.index for bundle in normalized]
    if any(index < 0 for index in indexes):
        raise ValueError("bundle indexes must be non-negative")
    if len(indexes) != len(set(indexes)):
        raise ValueError("bundle indexes must be unique")
    return tuple(sorted(normalized, key=lambda bundle: bundle.index))


class PlacementPlanner:
    """A deterministic, strategy-guided shadow planner.

    PACK and SPREAD are multidimensional bin-packing problems.  For a teaching
    runtime it is useful to use bounded backtracking rather than pretend that a
    greedy failure proves infeasibility.  Candidate ordering expresses the
    strategy; the search budget keeps pathological examples bounded.
    """

    def __init__(self, *, max_search_states: int = 100_000) -> None:
        if max_search_states <= 0:
            raise ValueError("max_search_states must be positive")
        self._max_search_states = max_search_states

    def plan(
        self,
        bundles: Sequence[Bundle | ResourceVector | Mapping[str, int | float]],
        nodes: Sequence[NodeSnapshot],
        strategy: PlacementStrategy | str = PlacementStrategy.PACK,
    ) -> PlacementPlan:
        normalized = _normalize_bundles(bundles)
        strategy = PlacementStrategy(strategy)
        if not normalized:
            return PlacementPlan(PlacementStatus.SUCCESS)

        alive = tuple(
            sorted(
                (node for node in nodes if getattr(node, "alive", True)),
                key=lambda node: _id_key(node.node_id),
            )
        )
        if not alive:
            return PlacementPlan(
                PlacementStatus.INFEASIBLE, reason="there are no live nodes"
            )

        for bundle in normalized:
            if not any(_fits(node.total, bundle.resources) for node in alive):
                return PlacementPlan(
                    PlacementStatus.INFEASIBLE,
                    reason=f"bundle {bundle.index} cannot fit on any live node",
                )

        available_capacities = {node.node_id: _available(node) for node in alive}
        try:
            placements = self._search(
                normalized, alive, available_capacities, strategy
            )
        except _SearchLimitReached:
            placements = None

        if placements is not None:
            return PlacementPlan(PlacementStatus.SUCCESS, placements)

        # A plan against total capacity distinguishes temporary pressure from a
        # permanently impossible request.  A bounded-search timeout is unknown,
        # and therefore conservatively PENDING rather than falsely INFEASIBLE.
        total_capacities = {node.node_id: node.total for node in alive}
        try:
            total_plan = self._search(normalized, alive, total_capacities, strategy)
        except _SearchLimitReached:
            return PlacementPlan(
                PlacementStatus.PENDING,
                reason="placement search budget exhausted; infeasibility is unproven",
            )

        if total_plan is not None:
            return PlacementPlan(
                PlacementStatus.PENDING,
                reason="the group is feasible in total capacity but not currently available",
            )
        return PlacementPlan(
            PlacementStatus.INFEASIBLE,
            reason=f"no {strategy.value} placement exists in total capacity",
        )

    def _search(
        self,
        bundles: tuple[Bundle, ...],
        nodes: tuple[NodeSnapshot, ...],
        capacities: dict[NodeID, ResourceVector],
        strategy: PlacementStrategy,
    ) -> tuple[BundlePlacement, ...] | None:
        if strategy is PlacementStrategy.STRICT_PACK:
            return self._strict_pack(bundles, nodes, capacities)
        if strategy is PlacementStrategy.STRICT_SPREAD:
            return self._strict_spread(bundles, nodes, capacities)
        return self._soft_search(bundles, nodes, capacities, strategy)

    def _strict_pack(
        self,
        bundles: tuple[Bundle, ...],
        nodes: tuple[NodeSnapshot, ...],
        capacities: dict[NodeID, ResourceVector],
    ) -> tuple[BundlePlacement, ...] | None:
        candidates: list[tuple[float, str, NodeID]] = []
        for node in nodes:
            remaining = capacities[node.node_id]
            possible = True
            for bundle in bundles:
                if not _fits(remaining, bundle.resources):
                    possible = False
                    break
                remaining = _subtract(remaining, bundle.resources)
            if possible:
                # Use the final bundle merely as a stable best-fit hint.
                score = _post_placement_slack(
                    node.total, remaining, bundles[-1].resources
                )
                candidates.append((score, _id_key(node.node_id), node.node_id))
        if not candidates:
            return None
        node_id = min(candidates)[2]
        return tuple(BundlePlacement(bundle.index, node_id) for bundle in bundles)

    def _strict_spread(
        self,
        bundles: tuple[Bundle, ...],
        nodes: tuple[NodeSnapshot, ...],
        capacities: dict[NodeID, ResourceVector],
    ) -> tuple[BundlePlacement, ...] | None:
        if len(bundles) > len(nodes):
            return None

        # Most constrained bundle first is an ordering optimization; the
        # augmenting-path matching remains exact.
        ordered = sorted(
            bundles,
            key=lambda bundle: (
                sum(
                    _fits(capacities[node.node_id], bundle.resources)
                    for node in nodes
                ),
                bundle.index,
            ),
        )
        edges = {
            bundle.index: tuple(
                node.node_id
                for node in nodes
                if _fits(capacities[node.node_id], bundle.resources)
            )
            for bundle in ordered
        }
        node_to_bundle: dict[NodeID, int] = {}
        assignment: dict[int, NodeID] = {}

        def augment(bundle_index: int, visited: set[NodeID]) -> bool:
            for node_id in edges[bundle_index]:
                if node_id in visited:
                    continue
                visited.add(node_id)
                displaced = node_to_bundle.get(node_id)
                if displaced is None or augment(displaced, visited):
                    node_to_bundle[node_id] = bundle_index
                    assignment[bundle_index] = node_id
                    return True
            return False

        for bundle in ordered:
            if not augment(bundle.index, set()):
                return None
        return tuple(
            BundlePlacement(index, assignment[index])
            for index in sorted(assignment)
        )

    def _soft_search(
        self,
        bundles: tuple[Bundle, ...],
        nodes: tuple[NodeSnapshot, ...],
        capacities: dict[NodeID, ResourceVector],
        strategy: PlacementStrategy,
    ) -> tuple[BundlePlacement, ...] | None:
        # Static total-feasibility count approximates "most constrained first"
        # without coupling the planner to CPU/GPU-specific resource names.
        ordered = sorted(
            bundles,
            key=lambda bundle: (
                sum(
                    _fits(capacities[node.node_id], bundle.resources)
                    for node in nodes
                ),
                bundle.index,
            ),
        )
        shadow = dict(capacities)
        used: dict[NodeID, int] = {node.node_id: 0 for node in nodes}
        assignment: dict[int, NodeID] = {}
        states = 0

        def visit(position: int) -> bool:
            nonlocal states
            states += 1
            if states > self._max_search_states:
                raise _SearchLimitReached
            if position == len(ordered):
                return True

            bundle = ordered[position]
            candidates = [
                node
                for node in nodes
                if _fits(shadow[node.node_id], bundle.resources)
            ]
            if strategy is PlacementStrategy.PACK:
                candidates.sort(
                    key=lambda node: (
                        0 if used[node.node_id] else 1,
                        _post_placement_slack(
                            node.total,
                            _subtract(shadow[node.node_id], bundle.resources),
                            bundle.resources,
                        ),
                        _id_key(node.node_id),
                    )
                )
            else:
                candidates.sort(
                    key=lambda node: (used[node.node_id], _id_key(node.node_id))
                )

            for node in candidates:
                node_id = node.node_id
                before = shadow[node_id]
                shadow[node_id] = _subtract(before, bundle.resources)
                used[node_id] += 1
                assignment[bundle.index] = node_id
                if visit(position + 1):
                    return True
                del assignment[bundle.index]
                used[node_id] -= 1
                shadow[node_id] = before
            return False

        if not visit(0):
            return None
        return tuple(
            BundlePlacement(index, assignment[index])
            for index in sorted(assignment)
        )


class ReservationState(str, Enum):
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    REMOVING = "REMOVING"
    ABORTED = "ABORTED"


@dataclass(frozen=True)
class BundleAllocation:
    """One atomic node-local allocation covering a set of bundles."""

    bundle_indexes: tuple[int, ...]
    resources: ResourceVector
    token: AllocationToken


@dataclass(frozen=True)
class ReservationSnapshot:
    placement_group_id: PlacementGroupID
    attempt: int
    state: ReservationState
    bundles: tuple[Bundle, ...]
    allocations: tuple[BundleAllocation, ...]


@dataclass
class _Reservation:
    bundles: tuple[Bundle, ...]
    allocations: tuple[BundleAllocation, ...]
    state: ReservationState
    child_ledgers: dict[int, ResourceLedger] = field(default_factory=dict)


class ReservationConflictError(ValueError):
    """The same transaction key was reused with a different payload."""


class BundleReservationLedger:
    """Node-local idempotent participant in PG two-phase reservation.

    ``prepare`` is atomic for all bundles assigned to this node.  ``abort`` may
    roll back either PREPARED or COMMITTED state so a coordinator can recover
    after another node fails during commit.  ABORTED tombstones fence late
    prepare/commit messages for the same scheduling attempt.
    """

    def __init__(self, resource_ledger: ResourceLedger) -> None:
        self._resources = resource_ledger
        self._transactions: dict[tuple[PlacementGroupID, int], _Reservation] = {}
        self._lock = RLock()

    def prepare(
        self,
        placement_group_id: PlacementGroupID,
        attempt: int,
        bundles: Sequence[Bundle | ResourceVector | Mapping[str, int | float]],
    ) -> bool:
        if attempt < 0:
            raise ValueError("attempt must be non-negative")
        normalized = _normalize_bundles(bundles)
        key = (placement_group_id, attempt)
        with self._lock:
            previous = self._transactions.get(key)
            if previous is not None:
                if previous.bundles and not _same_bundles(
                    previous.bundles, normalized
                ):
                    raise ReservationConflictError(
                        "transaction key reused with different bundle resources"
                    )
                if previous.state is ReservationState.ABORTED:
                    return False
                return True

            allocations: tuple[BundleAllocation, ...] = ()
            requested = _sum_resources(normalized)
            token = None if requested is None else _allocate(self._resources, requested)
            if requested is not None and token is None:
                self._transactions[key] = _Reservation(
                    normalized, (), ReservationState.ABORTED
                )
                return False
            if token is not None:
                allocations = (
                    BundleAllocation(
                        tuple(bundle.index for bundle in normalized), requested, token
                    ),
                )

            self._transactions[key] = _Reservation(
                normalized, allocations, ReservationState.PREPARED
            )
            return True

    def commit(self, placement_group_id: PlacementGroupID, attempt: int) -> bool:
        with self._lock:
            transaction = self._transactions.get((placement_group_id, attempt))
            if transaction is None or transaction.state in (
                ReservationState.ABORTED, ReservationState.REMOVING
            ):
                return False
            if transaction.state is ReservationState.COMMITTED:
                return True
            transaction.child_ledgers = {
                bundle.index: ResourceLedger(bundle.resources)
                for bundle in transaction.bundles
            }
            transaction.state = ReservationState.COMMITTED
            return True

    def ledger_for(
        self, placement_group_id: PlacementGroupID, attempt: int, bundle_index: int
    ) -> ResourceLedger:
        """Return one committed bundle's allocation authority.

        Child ledgers model capacity already charged once to the root during
        prepare.  Allocating from them must therefore never touch the root
        ledger again.  REMOVING fences every new lookup before cleanup begins.
        """

        if isinstance(bundle_index, bool) or not isinstance(bundle_index, int):
            raise TypeError("bundle_index must be an integer")
        with self._lock:
            transaction = self._transactions.get((placement_group_id, attempt))
            if transaction is None:
                raise KeyError((placement_group_id, attempt))
            if transaction.state is not ReservationState.COMMITTED:
                raise RuntimeError(
                    "bundle ledger is available only for a committed reservation"
                )
            try:
                return transaction.child_ledgers[bundle_index]
            except KeyError:
                raise KeyError(bundle_index) from None

    def begin_remove(self, placement_group_id: PlacementGroupID, attempt: int) -> bool:
        """Fence child-ledger lookup without releasing reserved root capacity."""

        key = (placement_group_id, attempt)
        with self._lock:
            transaction = self._transactions.get(key)
            if transaction is None:
                self._transactions[key] = _Reservation(
                    (), (), ReservationState.ABORTED
                )
                return True
            if transaction.state is ReservationState.ABORTED:
                return True
            if transaction.state is ReservationState.PREPARED:
                self._release_root_locked(transaction)
                transaction.state = ReservationState.ABORTED
                return True
            transaction.state = ReservationState.REMOVING
            return True

    def finalize_remove(
        self, placement_group_id: PlacementGroupID, attempt: int
    ) -> bool:
        """Release root reservation only after every child allocation is terminal."""

        with self._lock:
            transaction = self._transactions.get((placement_group_id, attempt))
            if transaction is None:
                return False
            if transaction.state is ReservationState.ABORTED:
                return True
            if transaction.state is not ReservationState.REMOVING:
                return False
            if any(
                _ledger_has_live_allocations(ledger)
                for ledger in transaction.child_ledgers.values()
            ):
                return False
            self._release_root_locked(transaction)
            transaction.child_ledgers.clear()
            transaction.state = ReservationState.ABORTED
            return True

    def abort(self, placement_group_id: PlacementGroupID, attempt: int) -> bool:
        key = (placement_group_id, attempt)
        with self._lock:
            transaction = self._transactions.get(key)
            if transaction is None:
                # An abort-before-prepare tombstone is intentional: a delayed
                # prepare for this attempt must not resurrect the transaction.
                self._transactions[key] = _Reservation(
                    (), (), ReservationState.ABORTED
                )
                return True
            if transaction.state is ReservationState.ABORTED:
                return True
            if transaction.state is ReservationState.PREPARED:
                self._release_root_locked(transaction)
                transaction.state = ReservationState.ABORTED
                return True
            transaction.state = ReservationState.REMOVING
            return self.finalize_remove(placement_group_id, attempt)

    def _release_root_locked(self, transaction: _Reservation) -> None:
        for allocation in reversed(transaction.allocations):
            _release(self._resources, allocation.token)
        transaction.allocations = ()

    def snapshot(
        self, placement_group_id: PlacementGroupID, attempt: int
    ) -> ReservationSnapshot | None:
        with self._lock:
            transaction = self._transactions.get((placement_group_id, attempt))
            if transaction is None:
                return None
            return ReservationSnapshot(
                placement_group_id,
                attempt,
                transaction.state,
                transaction.bundles,
                transaction.allocations,
            )


def _same_bundles(left: tuple[Bundle, ...], right: tuple[Bundle, ...]) -> bool:
    if len(left) != len(right):
        return False
    return all(
        a.index == b.index and a.resources == b.resources
        for a, b in zip(left, right)
    )


def _sum_resources(bundles: tuple[Bundle, ...]) -> ResourceVector | None:
    if not bundles:
        return None
    total = bundles[0].resources
    for bundle in bundles[1:]:
        add = getattr(total, "add", None)
        total = add(bundle.resources) if callable(add) else total + bundle.resources
    return total


def _allocate(
    ledger: ResourceLedger, resources: ResourceVector
) -> AllocationToken | None:
    # Prefer the explicitly non-throwing operation supplied by ResourceLedger.
    # A tiny fixture may instead provide allocate() returning None.
    for name in ("try_allocate", "allocate", "acquire"):
        method = getattr(ledger, name, None)
        if callable(method):
            return method(resources)
    raise TypeError("ResourceLedger must provide allocate(resources)")


def _release(ledger: ResourceLedger, token: AllocationToken) -> None:
    method = getattr(ledger, "release", None)
    if not callable(method):
        raise TypeError("ResourceLedger must provide release(token)")
    method(token)


def _ledger_has_live_allocations(ledger: ResourceLedger) -> bool:
    """Return whether any child allocation has not reached its terminal state.

    ``held_resources`` is deliberately not the liveness predicate.  A CPU-only
    allocation temporarily holds an empty vector while ``CPU_YIELDED``, and a
    zero-resource task is ``ACTIVE`` with an empty vector for its whole run.
    Both still own a live lease and must fence placement-group root release.
    """

    snapshot = ledger.snapshot()
    return any(
        record.state is not AllocationState.RELEASED
        for record in snapshot.allocations
    )
