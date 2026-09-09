"""Owner-side Actor route cache and generation fencing.

GCS owns Actor lifecycle.  This small reducer stores only the latest snapshot
installed by that authority so stable method calls can remain direct.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Dict

from . import protocol
from .ids import ActorID, ObjectID, WorkerID


@dataclass(frozen=True)
class ActorCallFence:
    actor_id: ActorID
    route_epoch: int
    generation: object
    worker_id: WorkerID


@dataclass
class ActorClientEntry:
    snapshot: protocol.ActorSnapshot
    method_names: tuple[str, ...]
    next_sequence: int = 0
    inflight: Dict[ObjectID, ActorCallFence] = field(default_factory=dict)


class ActorClientTable:
    """Monotonic GCS-snapshot cache used by one owning CoreWorker."""

    def __init__(self) -> None:
        self._entries: dict[ActorID, ActorClientEntry] = {}
        self._lock = RLock()

    def register(
        self, snapshot: protocol.ActorSnapshot, method_names: tuple[str, ...]
    ) -> None:
        if snapshot.state not in (
            protocol.ActorState.CREATING, protocol.ActorState.ALIVE
        ):
            raise ValueError(
                "initial Actor client snapshot must be CREATING or ALIVE"
            )
        with self._lock:
            if snapshot.actor_id in self._entries:
                raise ValueError("Actor client is already registered")
            self._entries[snapshot.actor_id] = ActorClientEntry(
                snapshot, tuple(method_names)
            )

    def remove(
        self, actor_id: ActorID, expected: protocol.ActorSnapshot
    ) -> bool:
        """CAS-remove an unexposed create entry during safe rollback.

        Only the exact CREATING provisional is removable.  DEAD is an
        authoritative tombstone which must remain available for exact GCS
        install replay; ALIVE/RESTARTING may own a live or unresolved GCS/Node
        obligation.
        """

        if not isinstance(actor_id, ActorID):
            raise TypeError("actor_id must be an ActorID")
        if not isinstance(expected, protocol.ActorSnapshot):
            raise TypeError("expected must be an ActorSnapshot")
        if expected.actor_id != actor_id:
            raise ValueError("expected snapshot belongs to another ActorID")
        if expected.state is not protocol.ActorState.CREATING:
            raise ValueError(
                "only an unexposed CREATING Actor may be removed"
            )
        with self._lock:
            entry = self._entries.get(actor_id)
            if entry is None or entry.snapshot != expected:
                return False
            if entry.inflight:
                raise RuntimeError(
                    "an Actor with in-flight calls cannot be rolled back"
                )
            del self._entries[actor_id]
            return True

    def install(
        self, snapshot: protocol.ActorSnapshot
    ) -> tuple[bool, tuple[ObjectID, ...]]:
        """Install one monotonic route update and return calls it fenced."""

        with self._lock:
            entry = self._entries.get(snapshot.actor_id)
            if entry is None:
                raise KeyError(snapshot.actor_id)
            current = entry.snapshot
            if snapshot.route_epoch < current.route_epoch:
                return False, ()
            if snapshot.route_epoch == current.route_epoch:
                if snapshot != current:
                    raise ValueError("Actor route epoch was reused with new contents")
                return False, ()
            fenced = tuple(
                object_id for object_id, call in entry.inflight.items()
                if call.route_epoch < snapshot.route_epoch
            )
            for object_id in fenced:
                entry.inflight.pop(object_id, None)
            if snapshot.generation != current.generation:
                entry.next_sequence = 0
            entry.snapshot = snapshot
            return True, fenced

    def begin_call(
        self, actor_id: ActorID, object_id: ObjectID
    ) -> tuple[protocol.ActorSnapshot, int, ActorCallFence]:
        with self._lock:
            entry = self._entries[actor_id]
            snapshot = entry.snapshot
            if snapshot.state is not protocol.ActorState.ALIVE:
                raise RuntimeError(
                    "Actor is not callable while {}".format(snapshot.state.value)
                )
            assert snapshot.worker_id is not None
            sequence = entry.next_sequence
            entry.next_sequence += 1
            fence = ActorCallFence(
                actor_id, snapshot.route_epoch, snapshot.generation, snapshot.worker_id
            )
            entry.inflight[object_id] = fence
            return snapshot, sequence, fence

    def can_publish(self, object_id: ObjectID, fence: ActorCallFence) -> bool:
        with self._lock:
            entry = self._entries.get(fence.actor_id)
            if entry is None or entry.inflight.get(object_id) != fence:
                return False
            snapshot = entry.snapshot
            return (
                snapshot.state is protocol.ActorState.ALIVE
                and snapshot.route_epoch == fence.route_epoch
                and snapshot.generation == fence.generation
                and snapshot.worker_id == fence.worker_id
            )

    def finish_call(self, object_id: ObjectID, fence: ActorCallFence) -> bool:
        with self._lock:
            entry = self._entries.get(fence.actor_id)
            if entry is None or entry.inflight.get(object_id) != fence:
                return False
            entry.inflight.pop(object_id, None)
            return True

    def snapshot(self, actor_id: ActorID) -> protocol.ActorSnapshot:
        with self._lock:
            return self._entries[actor_id].snapshot

    def methods(self, actor_id: ActorID) -> tuple[str, ...]:
        with self._lock:
            return self._entries[actor_id].method_names


__all__ = ["ActorCallFence", "ActorClientEntry", "ActorClientTable"]
