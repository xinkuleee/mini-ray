"""Pure GCS outbox for cluster-wide logical-owner death sweeps.

An owner may have no publication record at all and still have stale object bytes
or a dependency pull in flight on any live Node.  Publication-specific cleanup
therefore cannot be the authority that decides which Nodes receive the owner
fence.  This registry keeps the independent, owner-level fanout; each effect
uses the explicit owner-wide scope so a Node also removes ordinary stored
replicas which have no publication record.

Both relevant races linearize on one lock:

* owner death first: every later Node registration receives every historical
  owner fence before that Node is reported bootstrap-safe;
* Node registration first: a later owner death creates a pending fence for that
  exact live Node incarnation.

The registry performs no RPC.  It exposes immutable effects containing the
existing :class:`~miniray.protocol.InstallOwnerDeathFence` wire request.  A
driver sends one effect outside the GCS composition lock and acknowledges only
an exact echoed reply.  Lost acknowledgements replay the same request ID.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from enum import Enum
from threading import RLock
from typing import Optional, Tuple

from . import protocol
from .ids import NodeID, WorkerID


class OwnerDeathFenceRegistryError(RuntimeError):
    """Base error for owner-level fence outbox state."""


class OwnerDeathFenceRegistryConflictError(OwnerDeathFenceRegistryError):
    """An immutable death, Node incarnation, or effect was rebound."""


class OwnerDeathFenceRegistryStateError(OwnerDeathFenceRegistryError):
    """An acknowledgement or query contradicts current outbox state."""


class UnknownOwnerDeathFenceEffectError(
    OwnerDeathFenceRegistryStateError, LookupError
):
    """The supplied effect was never admitted by this registry."""


@dataclass(frozen=True, order=True)
class OwnerFenceNodeIncarnation:
    """Exact live Node target retained across outbox replay."""

    node_id: NodeID
    node_pid: int
    registration_epoch: int

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        _positive_int(self.node_pid, "node_pid")
        _positive_int(self.registration_epoch, "registration_epoch")

    @classmethod
    def from_node_info(
        cls, node: protocol.NodeInfo
    ) -> "OwnerFenceNodeIncarnation":
        if not isinstance(node, protocol.NodeInfo):
            raise TypeError("node must be a NodeInfo")
        return cls(node.node_id, node.node_pid, node.registration_epoch)


@dataclass(frozen=True, order=True)
class OwnerDeathFenceKey:
    """One owner death crossed with one exact live Node incarnation."""

    owner_worker_id: WorkerID
    target: OwnerFenceNodeIncarnation

    def __post_init__(self) -> None:
        if not isinstance(self.owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")
        if not isinstance(self.target, OwnerFenceNodeIncarnation):
            raise TypeError("target must be an OwnerFenceNodeIncarnation")


@dataclass(frozen=True)
class OwnerDeathFenceEffect:
    """Immutable outbox item for one owner/Node cross product entry."""

    key: OwnerDeathFenceKey
    owner_death: protocol.WorkerDeathRecord
    request: protocol.InstallOwnerDeathFence

    def __post_init__(self) -> None:
        if not isinstance(self.key, OwnerDeathFenceKey):
            raise TypeError("key must be an OwnerDeathFenceKey")
        death = _worker_death(self.owner_death)
        if death.worker_id != self.key.owner_worker_id:
            raise OwnerDeathFenceRegistryConflictError(
                "effect death proof names another owner"
            )
        expected = protocol.InstallOwnerDeathFence(
            _request_id(death, self.key.target), death,
            self.key.target.node_id, (),
            protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
        )
        if self.request != expected:
            raise OwnerDeathFenceRegistryConflictError(
                "effect wire request changed its exact owner/Node identity"
            )
        object.__setattr__(self, "owner_death", death)
        object.__setattr__(self, "request", expected)

    @classmethod
    def create(
        cls, death: protocol.WorkerDeathRecord,
        target: OwnerFenceNodeIncarnation,
    ) -> "OwnerDeathFenceEffect":
        checked = _worker_death(death)
        if not isinstance(target, OwnerFenceNodeIncarnation):
            raise TypeError("target must be an OwnerFenceNodeIncarnation")
        key = OwnerDeathFenceKey(checked.worker_id, target)
        request = protocol.InstallOwnerDeathFence(
            _request_id(checked, target), checked, target.node_id, (),
            protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
        )
        return cls(key, checked, request)


class OwnerDeathFenceTerminal(str, Enum):
    ACKNOWLEDGED = "ACKNOWLEDGED"
    NODE_DEAD = "NODE_DEAD"


@dataclass(frozen=True)
class OwnerDeathFenceCompletion:
    """Terminal proof for one exact owner-level fence effect."""

    effect: OwnerDeathFenceEffect
    terminal: OwnerDeathFenceTerminal
    reply: Optional[protocol.InstallOwnerDeathFenceReply] = None
    node_death: Optional[protocol.NodeDeathRecord] = None

    def __post_init__(self) -> None:
        if not isinstance(self.effect, OwnerDeathFenceEffect):
            raise TypeError("effect must be an OwnerDeathFenceEffect")
        if not isinstance(self.terminal, OwnerDeathFenceTerminal):
            raise TypeError("terminal must be an OwnerDeathFenceTerminal")
        if self.terminal is OwnerDeathFenceTerminal.ACKNOWLEDGED:
            if not isinstance(
                self.reply, protocol.InstallOwnerDeathFenceReply
            ):
                raise TypeError("ACKNOWLEDGED requires a typed fence reply")
            reply = replace(self.reply)
            if (
                reply.request != self.effect.request
                or not reply.accepted
                or not reply.complete
            ):
                raise OwnerDeathFenceRegistryStateError(
                    "fence acknowledgement did not terminally clean the exact effect"
                )
            if self.node_death is not None:
                raise ValueError(
                    "an acknowledged live-Node fence cannot carry Node death"
                )
            object.__setattr__(self, "reply", reply)
            return
        if self.reply is not None:
            raise ValueError("NODE_DEAD completion cannot carry a fence reply")
        death = _node_death(self.node_death)
        target = self.effect.key.target
        if (
            death.node_id != target.node_id
            or death.node_pid != target.node_pid
            or death.registration_epoch != target.registration_epoch
        ):
            raise OwnerDeathFenceRegistryConflictError(
                "Node death proof names another fence target incarnation"
            )
        object.__setattr__(self, "node_death", death)


@dataclass(frozen=True)
class OwnerDeathFenceRegistrySnapshot:
    """Immutable diagnostic view of history, pending work, and terminals."""

    live_nodes: Tuple[OwnerFenceNodeIncarnation, ...]
    owner_deaths: Tuple[protocol.WorkerDeathRecord, ...]
    pending: Tuple[OwnerDeathFenceEffect, ...]
    completed: Tuple[OwnerDeathFenceCompletion, ...]


class OwnerDeathFenceRegistry:
    """Thread-safe owner-death fanout and Node-bootstrap outbox."""

    def __init__(self) -> None:
        self._live_nodes: dict[NodeID, OwnerFenceNodeIncarnation] = {}
        self._node_deaths: dict[NodeID, protocol.NodeDeathRecord] = {}
        self._owner_deaths: dict[WorkerID, protocol.WorkerDeathRecord] = {}
        self._detection_ids: dict[str, protocol.WorkerDeathRecord] = {}
        self._death_epochs: dict[int, protocol.WorkerDeathRecord] = {}
        self._effects: dict[
            OwnerDeathFenceKey, OwnerDeathFenceEffect
        ] = {}
        self._completions: dict[
            OwnerDeathFenceKey, OwnerDeathFenceCompletion
        ] = {}
        self._lock = RLock()

    def register_node(
        self, target: OwnerFenceNodeIncarnation,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        """Register a live Node and atomically enqueue historical fences.

        The returned tuple is the remaining bootstrap work, not merely newly
        inserted work.  Exact registration replay after partial delivery can
        therefore resume without another query.
        """

        if not isinstance(target, OwnerFenceNodeIncarnation):
            raise TypeError("target must be an OwnerFenceNodeIncarnation")
        with self._lock:
            previous = self._live_nodes.get(target.node_id)
            if previous is not None and previous != target:
                raise OwnerDeathFenceRegistryConflictError(
                    "live NodeID is already bound to another incarnation"
                )
            dead = self._node_deaths.get(target.node_id)
            if dead is not None:
                raise OwnerDeathFenceRegistryConflictError(
                    "dead NodeID cannot be registered again"
                )
            self._live_nodes[target.node_id] = target
            for death in self._ordered_owner_deaths_locked():
                self._install_effect_locked(death, target)
            return self._pending_for_node_locked(target)

    def commit_owner_death(
        self, death: protocol.WorkerDeathRecord,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        """Freeze one owner death and enqueue every current live Node."""

        checked = _worker_death(death)
        with self._lock:
            previous = self._owner_deaths.get(checked.worker_id)
            if previous is not None:
                if previous != checked:
                    raise OwnerDeathFenceRegistryConflictError(
                        "owner Worker is already bound to another death proof"
                    )
                return self._pending_for_owner_locked(checked.worker_id)
            by_detection = self._detection_ids.get(checked.detection_id)
            if by_detection is not None and by_detection != checked:
                raise OwnerDeathFenceRegistryConflictError(
                    "Worker death detection_id was rebound"
                )
            by_epoch = self._death_epochs.get(checked.death_epoch)
            if by_epoch is not None and by_epoch != checked:
                raise OwnerDeathFenceRegistryConflictError(
                    "Worker death epoch was rebound"
                )
            self._owner_deaths[checked.worker_id] = checked
            self._detection_ids[checked.detection_id] = checked
            self._death_epochs[checked.death_epoch] = checked
            for target in self._ordered_live_nodes_locked():
                self._install_effect_locked(checked, target)
            return self._pending_for_owner_locked(checked.worker_id)

    def acknowledge(
        self, effect: OwnerDeathFenceEffect,
        reply: protocol.InstallOwnerDeathFenceReply,
    ) -> OwnerDeathFenceCompletion:
        """Acknowledge only the exact canonical effect and echoed reply."""

        if not isinstance(effect, OwnerDeathFenceEffect):
            raise TypeError("effect must be an OwnerDeathFenceEffect")
        if not isinstance(reply, protocol.InstallOwnerDeathFenceReply):
            raise TypeError("reply must be an InstallOwnerDeathFenceReply")
        checked_reply = replace(reply)
        with self._lock:
            canonical = self._effects.get(effect.key)
            if canonical is None:
                raise UnknownOwnerDeathFenceEffectError(
                    "owner-death fence effect was not admitted"
                )
            if canonical != effect:
                raise OwnerDeathFenceRegistryConflictError(
                    "owner-death fence effect changed on acknowledgement"
                )
            completion = OwnerDeathFenceCompletion(
                canonical, OwnerDeathFenceTerminal.ACKNOWLEDGED,
                reply=checked_reply,
            )
            previous = self._completions.get(effect.key)
            if previous is not None:
                if previous.terminal is OwnerDeathFenceTerminal.NODE_DEAD:
                    # The ACK may have left the Node immediately before its
                    # death committed at GCS.  Both facts discharge the same
                    # outbox entry, but the death-bound terminal is stronger and
                    # must remain monotonic.  The exact echoed ACK was validated
                    # above; accept it as a late observation without rebinding
                    # the canonical completion.
                    return previous
                if previous != completion:
                    raise OwnerDeathFenceRegistryConflictError(
                        "owner-death fence terminal proof changed on replay"
                    )
                return previous
            self._completions[effect.key] = completion
            return completion

    def mark_node_dead(
        self, death: protocol.NodeDeathRecord,
    ) -> Tuple[OwnerDeathFenceCompletion, ...]:
        """Discharge unreachable target effects with exact Node death."""

        checked = _node_death(death)
        with self._lock:
            target = self._live_nodes.get(checked.node_id)
            previous = self._node_deaths.get(checked.node_id)
            if previous is not None:
                if previous != checked:
                    raise OwnerDeathFenceRegistryConflictError(
                        "NodeID is already bound to another death proof"
                    )
                return tuple(
                    completion
                    for completion in self._ordered_completions_locked()
                    if completion.effect.key.target.node_id == checked.node_id
                    and completion.terminal is OwnerDeathFenceTerminal.NODE_DEAD
                )
            if target is None:
                raise OwnerDeathFenceRegistryStateError(
                    "Node death does not name a registered live target"
                )
            if (
                checked.node_pid != target.node_pid
                or checked.registration_epoch != target.registration_epoch
            ):
                raise OwnerDeathFenceRegistryConflictError(
                    "Node death proof names another registered incarnation"
                )
            self._live_nodes.pop(checked.node_id)
            self._node_deaths[checked.node_id] = checked
            completed = []
            for effect in self._ordered_effects_locked():
                if (
                    effect.key.target != target
                    or effect.key in self._completions
                ):
                    continue
                completion = OwnerDeathFenceCompletion(
                    effect, OwnerDeathFenceTerminal.NODE_DEAD,
                    node_death=checked,
                )
                self._completions[effect.key] = completion
                completed.append(completion)
            return tuple(completed)

    def pending(self) -> Tuple[OwnerDeathFenceEffect, ...]:
        with self._lock:
            return tuple(
                effect for effect in self._ordered_effects_locked()
                if effect.key not in self._completions
            )

    def pending_for_node(
        self, target: OwnerFenceNodeIncarnation,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        if not isinstance(target, OwnerFenceNodeIncarnation):
            raise TypeError("target must be an OwnerFenceNodeIncarnation")
        with self._lock:
            current = self._live_nodes.get(target.node_id)
            if current != target:
                raise OwnerDeathFenceRegistryStateError(
                    "target is not the exact registered live Node incarnation"
                )
            return self._pending_for_node_locked(target)

    def pending_for_owner(
        self, owner_worker_id: WorkerID,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        if not isinstance(owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")
        with self._lock:
            if owner_worker_id not in self._owner_deaths:
                raise OwnerDeathFenceRegistryStateError(
                    "owner Worker has no committed death proof"
                )
            return self._pending_for_owner_locked(owner_worker_id)

    def node_bootstrap_complete(
        self, target: OwnerFenceNodeIncarnation,
    ) -> bool:
        """Whether this live incarnation has every historical owner fence."""

        if not isinstance(target, OwnerFenceNodeIncarnation):
            raise TypeError("target must be an OwnerFenceNodeIncarnation")
        with self._lock:
            if self._live_nodes.get(target.node_id) != target:
                return False
            return not self._pending_for_node_locked(target)

    def cleanup_safe_node_ids(self) -> Tuple[NodeID, ...]:
        """Return live Nodes whose historical fence bootstrap is complete."""

        with self._lock:
            return tuple(
                target.node_id for target in self._ordered_live_nodes_locked()
                if not self._pending_for_node_locked(target)
            )

    def owner_death(
        self, owner_worker_id: WorkerID,
    ) -> Optional[protocol.WorkerDeathRecord]:
        if not isinstance(owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")
        with self._lock:
            return self._owner_deaths.get(owner_worker_id)

    def has_active_operations(self) -> bool:
        with self._lock:
            return len(self._completions) != len(self._effects)

    def has_active_obligations(self) -> bool:
        return self.has_active_operations()

    def snapshot(self) -> OwnerDeathFenceRegistrySnapshot:
        with self._lock:
            return OwnerDeathFenceRegistrySnapshot(
                self._ordered_live_nodes_locked(),
                self._ordered_owner_deaths_locked(),
                tuple(
                    effect for effect in self._ordered_effects_locked()
                    if effect.key not in self._completions
                ),
                self._ordered_completions_locked(),
            )

    def _install_effect_locked(
        self, death: protocol.WorkerDeathRecord,
        target: OwnerFenceNodeIncarnation,
    ) -> OwnerDeathFenceEffect:
        effect = OwnerDeathFenceEffect.create(death, target)
        previous = self._effects.get(effect.key)
        if previous is not None:
            if previous != effect:
                raise OwnerDeathFenceRegistryConflictError(
                    "owner/Node fence identity changed on replay"
                )
            return previous
        self._effects[effect.key] = effect
        return effect

    def _pending_for_node_locked(
        self, target: OwnerFenceNodeIncarnation,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        return tuple(
            effect for effect in self._ordered_effects_locked()
            if effect.key.target == target
            and effect.key not in self._completions
        )

    def _pending_for_owner_locked(
        self, owner_worker_id: WorkerID,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        return tuple(
            effect for effect in self._ordered_effects_locked()
            if effect.key.owner_worker_id == owner_worker_id
            and effect.key not in self._completions
        )

    def _ordered_live_nodes_locked(
        self,
    ) -> Tuple[OwnerFenceNodeIncarnation, ...]:
        return tuple(sorted(self._live_nodes.values()))

    def _ordered_owner_deaths_locked(
        self,
    ) -> Tuple[protocol.WorkerDeathRecord, ...]:
        return tuple(sorted(
            self._owner_deaths.values(),
            key=lambda death: (death.death_epoch, bytes(death.worker_id)),
        ))

    def _ordered_effects_locked(
        self,
    ) -> Tuple[OwnerDeathFenceEffect, ...]:
        return tuple(sorted(
            self._effects.values(),
            key=lambda effect: (
                effect.owner_death.death_epoch,
                bytes(effect.key.owner_worker_id),
                bytes(effect.key.target.node_id),
                effect.key.target.registration_epoch,
            ),
        ))

    def _ordered_completions_locked(
        self,
    ) -> Tuple[OwnerDeathFenceCompletion, ...]:
        return tuple(
            self._completions[effect.key]
            for effect in self._ordered_effects_locked()
            if effect.key in self._completions
        )


def _positive_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("{} must be a positive integer".format(label))


def _worker_death(value: object) -> protocol.WorkerDeathRecord:
    if not isinstance(value, protocol.WorkerDeathRecord):
        raise TypeError("death must be a WorkerDeathRecord")
    incarnation = protocol.WorkerIncarnation(
        value.incarnation.node_id, value.incarnation.node_pid,
        value.incarnation.node_registration_epoch,
        value.incarnation.worker_id, value.incarnation.worker_pid,
    )
    death = protocol.WorkerDeathRecord(
        value.detection_id, incarnation, value.death_epoch,
        value.exit_code, value.reason,
    )
    if death.reason not in (
        protocol.WorkerDeathReason.PROCESS_EXIT,
        protocol.WorkerDeathReason.NODE_EXIT,
    ):
        raise OwnerDeathFenceRegistryStateError(
            "owner fence requires PROCESS_EXIT or NODE_EXIT"
        )
    return death


def _node_death(value: object) -> protocol.NodeDeathRecord:
    if not isinstance(value, protocol.NodeDeathRecord):
        raise TypeError("node_death must be a NodeDeathRecord")
    death = protocol.NodeDeathRecord(
        value.detection_id, value.node_id, value.node_pid,
        value.registration_epoch, value.death_epoch, value.exit_code,
        value.reason, value.detail,
    )
    if death.reason is not protocol.NodeDeathReason.PROCESS_EXIT:
        raise OwnerDeathFenceRegistryStateError(
            "owner fence Node-loss proof requires PROCESS_EXIT"
        )
    return death


def _frame(digest: object, payload: bytes) -> None:
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _request_id(
    death: protocol.WorkerDeathRecord,
    target: OwnerFenceNodeIncarnation,
) -> str:
    digest = hashlib.sha256()
    digest.update(b"miniray-owner-death-global-fence-v1\0")
    for value in (
        death.detection_id.encode("utf-8"),
        bytes(death.worker_id), bytes(death.node_id),
        str(death.node_pid).encode("ascii"),
        str(death.node_registration_epoch).encode("ascii"),
        str(death.worker_pid).encode("ascii"),
        str(death.death_epoch).encode("ascii"),
        str(death.exit_code).encode("ascii"),
        death.reason.value.encode("ascii"), bytes(target.node_id),
        str(target.node_pid).encode("ascii"),
        str(target.registration_epoch).encode("ascii"),
    ):
        _frame(digest, value)
    return "owner-death-global-fence:" + digest.hexdigest()


__all__ = [
    "OwnerDeathFenceCompletion",
    "OwnerDeathFenceEffect",
    "OwnerDeathFenceKey",
    "OwnerDeathFenceRegistry",
    "OwnerDeathFenceRegistryConflictError",
    "OwnerDeathFenceRegistryError",
    "OwnerDeathFenceRegistrySnapshot",
    "OwnerDeathFenceRegistryStateError",
    "OwnerDeathFenceTerminal",
    "OwnerFenceNodeIncarnation",
    "UnknownOwnerDeathFenceEffectError",
]
