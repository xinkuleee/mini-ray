"""Exact physical deletion custody, independent of logical object collection.

A rejected late location still names real sealed bytes. This small queue owns
only their immutable Drop identities and receipts; it neither chooses which
objects are retired nor changes references, lineage or publication decisions.
Core supplies that admission authority and executes RPC outside these locks.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from threading import RLock

from .ids import ObjectID
from .protocol import (
    DropObjectReplica, DropObjectReplicaReply, DropObjectReplicaStatus,
    NodeDeathReason, NodeDeathRecord,
)


def _drop(value: DropObjectReplica) -> DropObjectReplica:
    if type(value) is not DropObjectReplica:
        raise TypeError("replica cleanup requires an exact DropObjectReplica")
    return replace(
        value, object_id=replace(value.object_id, task_id=replace(value.object_id.task_id)),
        producer_attempt_id=replace(value.producer_attempt_id, task_id=replace(value.producer_attempt_id.task_id)),
        owner_worker_id=replace(value.owner_worker_id), node_id=replace(value.node_id),
    )


@dataclass(frozen=True)
class ReplicaCleanupSnapshot:
    request: DropObjectReplica
    proof: DropObjectReplicaReply | NodeDeathRecord | None = None
    in_flight: bool = False


class ReplicaCleanupQueue:
    """One pending identity survives PINNED, missing ACKs and concurrent drains.

    Completed identities remain metadata-only replay receipts. The Node's
    deletion watermark prevents the same producer replica from being resealed;
    otherwise remembering a successful cleanup would be unsound.
    """

    def __init__(self) -> None:
        self._records: dict[DropObjectReplica, ReplicaCleanupSnapshot] = {}
        self._lock = RLock()

    def enqueue(self, request: DropObjectReplica) -> bool:
        request = _drop(request)
        with self._lock:
            if request in self._records:
                return False
            self._records[request] = ReplicaCleanupSnapshot(request)
            return True

    def pending(self, object_id: ObjectID | None = None) -> tuple[DropObjectReplica, ...]:
        with self._lock:
            return tuple(_drop(record.request) for record in self._records.values()
                         if record.proof is None and (object_id is None or record.request.object_id == object_id))

    def has_pending(self, object_id: ObjectID | None = None) -> bool:
        with self._lock:
            return any((record.proof is None or record.in_flight)
                       and (object_id is None or record.request.object_id == object_id)
                       for record in self._records.values())

    def claim(self, request: DropObjectReplica) -> bool:
        request = _drop(request)
        with self._lock:
            current = self._records[request]
            if current.proof is not None or current.in_flight:
                return False
            self._records[request] = replace(current, in_flight=True)
            return True

    def unclaim(self, request: DropObjectReplica) -> None:
        request = _drop(request)
        with self._lock:
            current = self._records[request]
            self._records[request] = replace(current, in_flight=False)

    def acknowledge(self, request: DropObjectReplica, reply: DropObjectReplicaReply) -> bool:
        request = _drop(request)
        if type(reply) is not DropObjectReplicaReply:
            raise TypeError("replica cleanup requires an exact typed reply")
        reply = replace(reply)
        echoed = _drop(DropObjectReplica(reply.object_id, reply.producer_attempt_id,
                                         reply.owner_worker_id, reply.node_id, reply.checksum))
        if echoed != request:
            raise ValueError("replica cleanup reply changed its immutable identity")
        reply = replace(reply, object_id=echoed.object_id, producer_attempt_id=echoed.producer_attempt_id,
                        owner_worker_id=echoed.owner_worker_id, node_id=echoed.node_id)
        if reply.status not in (DropObjectReplicaStatus.DROPPED, DropObjectReplicaStatus.ALREADY_DROPPED):
            return False
        with self._lock:
            current = self._records[request]
            if current.proof is not None:
                return False
            self._records[request] = replace(current, proof=reply)
            return True

    def acknowledge_node_death(self, death: NodeDeathRecord) -> tuple[ObjectID, ...]:
        """Consume Core's installed death, never infer it from RPC failure.

        NodeID cannot be re-registered after death in this runtime. Core must
        validate/commit the incarnation before passing this typed fact here.
        """
        if type(death) is not NodeDeathRecord:
            raise TypeError("replica cleanup needs an installed Node death")
        death = replace(death, node_id=replace(death.node_id))
        if death.reason is not NodeDeathReason.PROCESS_EXIT:
            raise ValueError("expected shutdown is not replica-loss evidence")
        with self._lock:
            objects = []
            for request, current in self._records.items():
                if request.node_id == death.node_id and current.proof is None:
                    self._records[request] = replace(current, proof=death)
                    objects.append(request.object_id)
            return deepcopy(tuple(dict.fromkeys(objects)))

    def snapshot(self) -> tuple[ReplicaCleanupSnapshot, ...]:
        with self._lock:
            return deepcopy(tuple(self._records.values()))
