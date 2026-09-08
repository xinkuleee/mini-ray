"""Pure TaskID-scoped lifetime authority for foreign producer inputs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag
from threading import RLock
from typing import Tuple

from .contained_edges import ObjectMetadataCollectionPlan
from .ids import ObjectID, TaskID, WorkerID
from .ownership import DeadWorkerReferenceRecord
from .protocol import TaskReferenceHold, TaskReferenceHoldKind
from .recovery import CollectedObjectForgetPlan


OwnerAddress = Tuple[str, int]


class ForeignLineageRole(IntFlag):
    TOP_LEVEL = 1
    NESTED = 2


@dataclass(frozen=True, order=True)
class ForeignLineageEdge:
    task_id: TaskID
    dependency_object_id: ObjectID
    owner_worker_id: WorkerID
    owner_address: OwnerAddress
    borrower_worker_id: WorkerID
    hold: TaskReferenceHold
    roles: ForeignLineageRole

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(self.dependency_object_id, ObjectID):
            raise TypeError("dependency_object_id must be an ObjectID")
        if not isinstance(self.owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")
        if (
            not isinstance(self.owner_address, tuple)
            or len(self.owner_address) != 2
            or not isinstance(self.owner_address[0], str)
            or not self.owner_address[0]
            or isinstance(self.owner_address[1], bool)
            or not isinstance(self.owner_address[1], int)
            or not 1 <= self.owner_address[1] <= 65535
        ):
            raise ValueError("owner_address must be a bound address")
        if not isinstance(self.borrower_worker_id, WorkerID):
            raise TypeError("borrower_worker_id must be a WorkerID")
        if not isinstance(self.hold, TaskReferenceHold):
            raise TypeError("hold must be a TaskReferenceHold")
        if self.hold.kind is not TaskReferenceHoldKind.RETAINED:
            raise ValueError("foreign lineage hold must be RETAINED")
        if (
            self.hold.task_id != self.task_id
            or self.hold.submitting_worker_id != self.borrower_worker_id
        ):
            raise ValueError("foreign lineage hold must belong to its task and borrower")
        roles = ForeignLineageRole(self.roles)
        if not roles or roles & ~(
            ForeignLineageRole.TOP_LEVEL | ForeignLineageRole.NESTED
        ):
            raise ValueError("foreign lineage edge has invalid roles")
        object.__setattr__(self, "roles", roles)


@dataclass(frozen=True)
class ForeignLineageTask:
    task_id: TaskID
    output_ids: tuple[ObjectID, ...]
    edges: tuple[ForeignLineageEdge, ...]


@dataclass(frozen=True)
class ForeignLineageCollectionPlan:
    task_id: TaskID
    output_ids: tuple[ObjectID, ...]
    edges: tuple[ForeignLineageEdge, ...]
    receipts: tuple["ForeignLineageCollectionReceipt", ...]


@dataclass(frozen=True, order=True)
class ForeignLineagePreparedCollectionReceipt:
    """Durable intent recorded before local collection authorities commit."""

    task_id: TaskID
    output_id: ObjectID
    owner_plan: ObjectMetadataCollectionPlan
    recovery_plan: CollectedObjectForgetPlan

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, TaskID):
            raise TypeError("task_id must be a TaskID")
        if not isinstance(self.output_id, ObjectID):
            raise TypeError("output_id must be an ObjectID")
        if self.output_id.task_id != self.task_id:
            raise ValueError("collection receipt output belongs to another task")
        if not isinstance(self.owner_plan, ObjectMetadataCollectionPlan):
            raise TypeError(
                "owner_plan must be an ObjectMetadataCollectionPlan"
            )
        if not isinstance(self.recovery_plan, CollectedObjectForgetPlan):
            raise TypeError(
                "recovery_plan must be a CollectedObjectForgetPlan"
            )
        if (
            self.owner_plan.object_id != self.output_id
            or self.recovery_plan.object_id != self.output_id
            or self.recovery_plan.kind != "task"
            or self.recovery_plan.task_id != self.task_id
        ):
            raise ValueError(
                "prepared receipt owner and recovery plans must name the "
                "same task output"
            )


@dataclass(frozen=True, order=True)
class ForeignLineageCollectionReceipt:
    """Proof activated only after owner and recovery collection commit."""

    prepared: ForeignLineagePreparedCollectionReceipt

    def __post_init__(self) -> None:
        if not isinstance(
            self.prepared, ForeignLineagePreparedCollectionReceipt
        ):
            raise TypeError(
                "prepared must be a ForeignLineagePreparedCollectionReceipt"
            )

    @property
    def task_id(self) -> TaskID:
        return self.prepared.task_id

    @property
    def output_id(self) -> ObjectID:
        return self.prepared.output_id

    @property
    def owner_plan(self) -> ObjectMetadataCollectionPlan:
        return self.prepared.owner_plan

    @property
    def recovery_plan(self) -> CollectedObjectForgetPlan:
        return self.prepared.recovery_plan


class ForeignLineageRegistry:
    """Register once, preserve across attempts, claim at final sibling GC."""

    def __init__(self) -> None:
        self._tasks: dict[TaskID, ForeignLineageTask] = {}
        self._claimed: dict[TaskID, ForeignLineageCollectionPlan] = {}
        self._dead_owners: dict[WorkerID, DeadWorkerReferenceRecord] = {}
        self._admission_open = True
        self._lock = RLock()

    @staticmethod
    def normalize_edges(
        task_id: TaskID, edges: tuple[ForeignLineageEdge, ...]
    ) -> tuple[ForeignLineageEdge, ...]:
        merged: dict[tuple[ObjectID, WorkerID], ForeignLineageEdge] = {}
        for edge in tuple(edges):
            if not isinstance(edge, ForeignLineageEdge):
                raise TypeError("edges must contain ForeignLineageEdge values")
            if edge.task_id != task_id:
                raise ValueError("foreign lineage edge belongs to another task")
            key = edge.dependency_object_id, edge.owner_worker_id
            prior = merged.get(key)
            if prior is None:
                merged[key] = edge
                continue
            if (
                prior.owner_address != edge.owner_address
                or prior.borrower_worker_id != edge.borrower_worker_id
                or prior.hold != edge.hold
            ):
                raise ValueError("duplicate foreign lineage edge conflicts")
            merged[key] = ForeignLineageEdge(
                prior.task_id, prior.dependency_object_id,
                prior.owner_worker_id, prior.owner_address,
                prior.borrower_worker_id, prior.hold,
                prior.roles | edge.roles,
            )
        return tuple(sorted(merged.values()))

    def register(
        self, task_id: TaskID, output_ids: tuple[ObjectID, ...],
        edges: tuple[ForeignLineageEdge, ...],
    ) -> ForeignLineageTask:
        outputs = tuple(output_ids)
        if not outputs or any(
            not isinstance(value, ObjectID) or value.task_id != task_id
            for value in outputs
        ):
            raise ValueError("output_ids must be a non-empty task manifest")
        if len(outputs) != len(set(outputs)):
            raise ValueError("output_ids must be unique")
        normalized = self.normalize_edges(task_id, edges)
        record = ForeignLineageTask(task_id, outputs, normalized)
        with self._lock:
            if task_id in self._claimed:
                raise ValueError("foreign lineage was already claimed")
            previous = self._tasks.get(task_id)
            if previous is not None and previous != record:
                raise ValueError("task was registered with conflicting foreign lineage")
            if previous is None and not self._admission_open:
                raise RuntimeError("foreign lineage admission is closed")
            self._tasks[task_id] = record
            return previous or record

    def close_admission(self) -> bool:
        """Fence new task registrations, preserving exact replays."""

        with self._lock:
            changed = self._admission_open
            self._admission_open = False
            return changed

    def admission_is_open(self) -> bool:
        with self._lock:
            return self._admission_open

    def abort(self, expected: ForeignLineageTask) -> bool:
        if not isinstance(expected, ForeignLineageTask):
            raise TypeError("expected must be a ForeignLineageTask")
        with self._lock:
            current = self._tasks.get(expected.task_id)
            if current is None:
                return False
            if current != expected:
                raise ValueError("foreign lineage changed before abort")
            del self._tasks[expected.task_id]
            return True

    def snapshot(self, task_id: TaskID) -> ForeignLineageTask | None:
        with self._lock:
            return self._tasks.get(task_id)

    def task_ids(self) -> tuple[TaskID, ...]:
        """Return the registered logical tasks in deterministic order.

        The registry deliberately exposes identities rather than its mutable
        mapping.  Shutdown composition can therefore drive final collection
        without gaining a second way to mutate lineage authority.
        """

        with self._lock:
            return tuple(sorted(self._tasks))

    def commit_edge_replacement(
        self,
        expected: ForeignLineageEdge,
        replacement: ForeignLineageEdge,
    ) -> ForeignLineageTask:
        """Record one owner-ACKed ``old hold -> new hold`` transition.

        A multi-owner renewal is necessarily distributed: one owner can ACK
        while another reply is lost.  Recording each ACK immediately keeps
        the registry equal to the credentials that are *actually* live at
        owners.  The higher-level renewal service still withholds parent-task
        reconstruction until every edge has ACKed.

        This method performs only an in-memory compare-and-assignment commit.
        All network work and reply validation belong to the runtime service.
        Exact replay is idempotent; an attempt to redirect the same edge to a
        different successor is rejected.
        """

        if not isinstance(expected, ForeignLineageEdge):
            raise TypeError("expected must be a ForeignLineageEdge")
        if not isinstance(replacement, ForeignLineageEdge):
            raise TypeError("replacement must be a ForeignLineageEdge")
        if (
            expected.task_id != replacement.task_id
            or expected.dependency_object_id
            != replacement.dependency_object_id
            or expected.owner_worker_id != replacement.owner_worker_id
            or expected.owner_address != replacement.owner_address
            or expected.borrower_worker_id != replacement.borrower_worker_id
            or expected.roles != replacement.roles
        ):
            raise ValueError(
                "foreign lineage replacement may change only the hold"
            )
        if (
            replacement.hold.origin_attempt_id.attempt_number
            <= expected.hold.origin_attempt_id.attempt_number
        ):
            raise ValueError(
                "foreign lineage replacement origin attempt must increase"
            )

        task_id = expected.task_id
        key = expected.dependency_object_id, expected.owner_worker_id
        with self._lock:
            if task_id in self._claimed:
                raise ValueError(
                    "foreign lineage cannot change after collection claim"
                )
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(task_id)
            by_key = {
                (edge.dependency_object_id, edge.owner_worker_id): edge
                for edge in record.edges
            }
            current = by_key.get(key)
            if current == replacement:
                return record
            if current != expected:
                raise ValueError(
                    "foreign lineage edge changed before replacement commit"
                )
            by_key[key] = replacement
            updated = ForeignLineageTask(
                task_id, record.output_ids, tuple(sorted(by_key.values()))
            )
            self._tasks[task_id] = updated
            return updated

    def claim_with_receipts(
        self,
        task_id: TaskID,
        receipts: tuple[ForeignLineageCollectionReceipt, ...],
    ) -> ForeignLineageCollectionPlan | None:
        """Claim from a full set of committed per-output receipts."""

        values = tuple(receipts)
        if any(
            not isinstance(value, ForeignLineageCollectionReceipt)
            for value in values
        ):
            raise TypeError(
                "receipts must contain ForeignLineageCollectionReceipt values"
            )
        if any(value.task_id != task_id for value in values):
            raise ValueError("collection receipt belongs to another TaskID")
        if len({value.output_id for value in values}) != len(values):
            raise ValueError("collection receipts must have unique outputs")
        with self._lock:
            claimed = self._claimed.get(task_id)
            if claimed is not None:
                return claimed
            record = self._tasks.get(task_id)
            if record is None:
                return None
            if frozenset(value.output_id for value in values) != frozenset(
                record.output_ids
            ):
                return None
            final = tuple(
                value for value in values if value.recovery_plan.remove_task
            )
            if len(final) != 1:
                raise ValueError(
                    "complete collection receipts require exactly one final "
                    "recovery plan"
                )
            plan = ForeignLineageCollectionPlan(
                record.task_id, record.output_ids, record.edges,
                tuple(sorted(values, key=lambda value: value.output_id)),
            )
            self._claimed[task_id] = plan
            return plan

    def complete_claim(self, plan: ForeignLineageCollectionPlan) -> bool:
        if not isinstance(plan, ForeignLineageCollectionPlan):
            raise TypeError("plan must be a ForeignLineageCollectionPlan")
        with self._lock:
            if self._claimed.get(plan.task_id) != plan:
                raise ValueError("foreign lineage collection claim changed")
            self._claimed.pop(plan.task_id, None)
            self._tasks.pop(plan.task_id, None)
            return True

    def mark_owner_dead(
        self, record: DeadWorkerReferenceRecord
    ) -> tuple[TaskID, ...]:
        """Install only a locally validated owner-table death proof."""

        if not isinstance(record, DeadWorkerReferenceRecord):
            raise TypeError(
                "owner death requires a DeadWorkerReferenceRecord"
            )
        owner_worker_id = record.worker_id
        with self._lock:
            previous = self._dead_owners.get(owner_worker_id)
            if previous is not None and previous != record:
                raise ValueError("owner death proof conflicts with tombstone")
            self._dead_owners[owner_worker_id] = record
            return tuple(sorted(
                task_id for task_id, record in self._tasks.items()
                if any(
                    edge.owner_worker_id == owner_worker_id
                    for edge in record.edges
                )
            ))

    def owner_is_dead(self, owner_worker_id: WorkerID) -> bool:
        with self._lock:
            return owner_worker_id in self._dead_owners

    def owner_death_record(
        self, owner_worker_id: WorkerID
    ) -> DeadWorkerReferenceRecord | None:
        with self._lock:
            return self._dead_owners.get(owner_worker_id)

    def has_pending_claims(self) -> bool:
        with self._lock:
            return bool(self._claimed)


__all__ = [
    "ForeignLineageCollectionPlan", "ForeignLineageCollectionReceipt",
    "ForeignLineagePreparedCollectionReceipt",
    "ForeignLineageEdge",
    "ForeignLineageRegistry", "ForeignLineageRole",
    "ForeignLineageTask",
]
