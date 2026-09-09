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
from itertools import permutations
from threading import RLock
from typing import Mapping, Sequence

from .ids import NodeID, PlacementGroupID
from .resources import (
    AllocationState, AllocationToken, NodeSnapshot, ResourceLedger, ResourceQuantity,
    ResourceVector,
)


class PlacementStrategy(str, Enum):
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

    def __post_init__(self) -> None:
        if isinstance(self.index, bool) or not isinstance(self.index, int):
            raise TypeError("bundle index must be an integer")
        if self.index < 0:
            raise ValueError("bundle index must be non-negative")
        if not isinstance(self.resources, ResourceVector):
            raise TypeError("bundle resources must be a ResourceVector")


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


def _normalize_bundles(
    bundles: Sequence[Bundle | ResourceVector | Mapping[str, ResourceQuantity]],
) -> tuple[Bundle, ...]:
    if not 1 <= len(bundles) <= 2:
        raise ValueError("placement groups require one or two bundles")
    normalized: list[Bundle] = []
    for position, item in enumerate(bundles):
        if isinstance(item, Bundle):
            bundle = item
        elif isinstance(item, ResourceVector):
            bundle = Bundle(position, item)
        elif isinstance(item, Mapping):
            bundle = Bundle(position, ResourceVector(item))
        else:
            raise TypeError("bundles must contain Bundle, ResourceVector, or mapping values")
        normalized.append(bundle)
    indexes = [bundle.index for bundle in normalized]
    if len(indexes) != len(set(indexes)):
        raise ValueError("bundle indexes must be unique")
    return tuple(sorted(normalized, key=lambda bundle: bundle.index))


class PlacementPlanner:
    """Exact shadow planning for at most two bundles under hard constraints.

    STRICT_PACK tests each node against the aggregate request. STRICT_SPREAD
    enumerates distinct node assignments (at most pairs), so a CPU-first choice
    cannot hide a feasible placement for a more constrained bundle. NodeID order
    makes ties deterministic; only the participant ledger can reserve resources.
    """

    def plan(
        self,
        bundles: Sequence[Bundle | ResourceVector | Mapping[str, ResourceQuantity]],
        nodes: Sequence[NodeSnapshot],
        strategy: PlacementStrategy | str = PlacementStrategy.STRICT_PACK,
    ) -> PlacementPlan:
        normalized = _normalize_bundles(bundles)
        strategy = PlacementStrategy(strategy)
        if any(not isinstance(node, NodeSnapshot) for node in nodes):
            raise TypeError("placement nodes must be NodeSnapshot values")
        if len({node.node_id for node in nodes}) != len(nodes):
            raise ValueError("placement node IDs must be unique")
        alive = tuple(sorted(
            (node for node in nodes if node.alive), key=lambda node: str(node.node_id)
        ))
        if not alive:
            return PlacementPlan(
                PlacementStatus.INFEASIBLE, reason="there are no live nodes"
            )
        available = {node.node_id: node.available for node in alive}
        placements = self._place(normalized, alive, available, strategy)
        if placements is not None:
            return PlacementPlan(PlacementStatus.SUCCESS, placements)

        # Both searches are exact. A total-capacity plan distinguishes current
        # pressure from impossibility, without a search-budget UNKNOWN state.
        total = {node.node_id: node.total for node in alive}
        if self._place(normalized, alive, total, strategy) is not None:
            return PlacementPlan(
                PlacementStatus.PENDING,
                reason="the group is feasible in total capacity but not currently available",
            )
        return PlacementPlan(
            PlacementStatus.INFEASIBLE,
            reason=f"no {strategy.value} placement exists in total capacity",
        )

    @staticmethod
    def _place(
        bundles: tuple[Bundle, ...],
        nodes: tuple[NodeSnapshot, ...],
        capacities: Mapping[NodeID, ResourceVector],
        strategy: PlacementStrategy,
    ) -> tuple[BundlePlacement, ...] | None:
        if strategy is PlacementStrategy.STRICT_PACK:
            requested = _sum_resources(bundles)
            for node in nodes:
                if requested.fits_in(capacities[node.node_id]):
                    return tuple(BundlePlacement(bundle.index, node.node_id) for bundle in bundles)
            return None

        for assignment in permutations(nodes, len(bundles)):
            if all(
                bundle.resources.fits_in(capacities[node.node_id])
                for bundle, node in zip(bundles, assignment)
            ):
                return tuple(
                    BundlePlacement(bundle.index, node.node_id)
                    for bundle, node in zip(bundles, assignment)
                )
        return None


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
        bundles: Sequence[Bundle | ResourceVector | Mapping[str, ResourceQuantity]],
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
            token = self._resources.try_allocate(requested)
            if token is None:
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
            self._resources.release(allocation.token)
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


def _sum_resources(bundles: tuple[Bundle, ...]) -> ResourceVector:
    total = ResourceVector.empty()
    for bundle in bundles:
        total = total + bundle.resources
    return total


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
