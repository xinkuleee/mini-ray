"""Pure single-threaded actor mailbox semantics.

This module deliberately contains no RPC or execution code.  It owns the
ordering facts that transport retries must not change: FIFO per caller,
deduplication by ``(generation, caller, sequence)``, and generation fencing.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Hashable

from .ids import ActorGeneration, ActorID, WorkerID


class ActorSubmitStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    BUFFERED = "BUFFERED"
    DUPLICATE = "DUPLICATE"
    FENCED = "FENCED"


class ActorCallState(str, Enum):
    BUFFERED = "BUFFERED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True)
class ActorCallKey:
    caller_id: WorkerID
    sequence: int


@dataclass(frozen=True)
class ActorCall:
    actor_id: ActorID
    generation: ActorGeneration
    caller_id: WorkerID
    sequence: int
    payload: object
    fingerprint: Hashable | None = None

    @property
    def key(self) -> ActorCallKey:
        return ActorCallKey(self.caller_id, self.sequence)


@dataclass(frozen=True)
class ActorSubmission:
    status: ActorSubmitStatus
    admitted: tuple[ActorCall, ...] = ()
    cached_result: object | None = None
    has_cached_result: bool = False


@dataclass
class _CallRecord:
    call: ActorCall
    state: ActorCallState
    result: object | None = None
    has_result: bool = False


class ActorCallConflictError(ValueError):
    """A caller reused a sequence number for a different logical call."""


class ActorMailboxStateError(RuntimeError):
    pass


_ACTOR_CALL_TRANSITIONS = {
    ActorCallState.BUFFERED: frozenset({ActorCallState.QUEUED}),
    ActorCallState.QUEUED: frozenset({ActorCallState.RUNNING}),
    ActorCallState.RUNNING: frozenset({ActorCallState.COMPLETED}),
    ActorCallState.COMPLETED: frozenset(),
}


def _transition_call(record: _CallRecord, target: ActorCallState) -> None:
    """Apply one legal mailbox edge and reject skipped regressions."""

    if not isinstance(target, ActorCallState):
        raise TypeError("actor call transition target must be ActorCallState")
    if target not in _ACTOR_CALL_TRANSITIONS[record.state]:
        raise ActorMailboxStateError(
            "illegal actor call transition {} -> {}".format(
                record.state.value, target.value
            )
        )
    record.state = target


class ActorMailbox:
    """Deterministic mailbox for a default-concurrency (one) actor.

    FIFO is guaranteed independently for every caller.  Calls from different
    callers are ordered when they become admissible at this mailbox; no global
    client-side wall-clock order is claimed.
    """

    def __init__(self, actor_id: ActorID, generation: ActorGeneration) -> None:
        if generation.actor_id != actor_id:
            raise ValueError("actor generation belongs to a different actor")
        self.actor_id = actor_id
        self.generation = generation
        self._next_sequence: dict[WorkerID, int] = {}
        self._records: dict[ActorCallKey, _CallRecord] = {}
        self._buffered: dict[WorkerID, dict[int, ActorCallKey]] = {}
        self._ready: list[ActorCallKey] = []
        self._running: ActorCallKey | None = None

    def submit(
        self,
        *,
        generation: ActorGeneration,
        caller_id: WorkerID,
        sequence: int,
        payload: object,
        fingerprint: Hashable | None = None,
    ) -> ActorSubmission:
        return self.submit_call(
            ActorCall(
                self.actor_id,
                generation,
                caller_id,
                sequence,
                payload,
                fingerprint,
            )
        )

    def submit_call(self, call: ActorCall) -> ActorSubmission:
        if call.actor_id != self.actor_id or call.generation != self.generation:
            return ActorSubmission(ActorSubmitStatus.FENCED)
        if call.sequence < 0:
            raise ValueError("actor call sequence must be non-negative")

        existing = self._records.get(call.key)
        if existing is not None:
            if not _same_logical_call(existing.call, call):
                raise ActorCallConflictError(
                    "caller sequence was reused with a different call payload"
                )
            return ActorSubmission(
                ActorSubmitStatus.DUPLICATE,
                cached_result=existing.result,
                has_cached_result=existing.has_result,
            )

        expected = self._next_sequence.get(call.caller_id, 0)
        if call.sequence < expected:
            # Records are retained for deduplication, so this can only happen
            # after explicit pruning by a future extension.  Fence it rather
            # than accidentally execute the old sequence again.
            return ActorSubmission(ActorSubmitStatus.DUPLICATE)

        if call.sequence > expected:
            self._records[call.key] = _CallRecord(call, ActorCallState.BUFFERED)
            self._buffered.setdefault(call.caller_id, {})[call.sequence] = call.key
            return ActorSubmission(ActorSubmitStatus.BUFFERED)

        admitted = self._admit_contiguous(call)
        return ActorSubmission(ActorSubmitStatus.ACCEPTED, admitted=admitted)

    def _admit_contiguous(self, first: ActorCall) -> tuple[ActorCall, ...]:
        admitted: list[ActorCall] = []
        caller = first.caller_id
        call = first
        while True:
            record = self._records.get(call.key)
            if record is None:
                record = _CallRecord(call, ActorCallState.QUEUED)
                self._records[call.key] = record
            else:
                _transition_call(record, ActorCallState.QUEUED)
            self._ready.append(call.key)
            admitted.append(call)

            next_sequence = call.sequence + 1
            self._next_sequence[caller] = next_sequence
            waiting = self._buffered.get(caller)
            next_key = waiting.pop(next_sequence, None) if waiting else None
            if waiting == {}:
                del self._buffered[caller]
            if next_key is None:
                break
            call = self._records[next_key].call
        return tuple(admitted)

    def take_next(self) -> ActorCall | None:
        """Start the next call, preserving default actor serial execution."""

        if self._running is not None or not self._ready:
            return None
        key = self._ready.pop(0)
        record = self._records[key]
        _transition_call(record, ActorCallState.RUNNING)
        self._running = key
        return record.call

    def complete_current(self, result: object) -> ActorCall:
        if self._running is None:
            raise ActorMailboxStateError("there is no running actor call")
        key = self._running
        record = self._records[key]
        if record.state is not ActorCallState.RUNNING:
            raise ActorMailboxStateError("running call has an invalid state")
        _transition_call(record, ActorCallState.COMPLETED)
        record.result = result
        record.has_result = True
        self._running = None
        return record.call

    def call_state(self, key: ActorCallKey) -> ActorCallState | None:
        record = self._records.get(key)
        return record.state if record is not None else None

    @property
    def ready_count(self) -> int:
        return len(self._ready)

    @property
    def running(self) -> ActorCall | None:
        if self._running is None:
            return None
        return self._records[self._running].call

    def advance_generation(
        self, new_generation: ActorGeneration
    ) -> tuple[ActorCall, ...]:
        """Fence the old incarnation and return calls abandoned by restart.

        The actor registry owns monotonic generation allocation.  The mailbox
        verifies that the new fencing token belongs to this actor and advances
        monotonically.
        Sequence numbers restart at zero because generation is part of every
        logical call key.
        """

        if new_generation.actor_id != self.actor_id:
            raise ValueError("new actor generation belongs to a different actor")
        if new_generation.generation <= self.generation.generation:
            raise ValueError("new actor generation must advance monotonically")
        abandoned = tuple(
            record.call
            for record in self._records.values()
            if record.state is not ActorCallState.COMPLETED
        )
        self.generation = new_generation
        self._next_sequence.clear()
        self._records.clear()
        self._buffered.clear()
        self._ready.clear()
        self._running = None
        return abandoned


def _same_logical_call(left: ActorCall, right: ActorCall) -> bool:
    if (
        left.actor_id != right.actor_id
        or left.generation != right.generation
        or left.key != right.key
    ):
        return False
    if left.fingerprint is not None or right.fingerprint is not None:
        return (
            left.fingerprint is not None
            and right.fingerprint is not None
            and left.fingerprint == right.fingerprint
        )
    try:
        equal = left.payload == right.payload
        return equal if isinstance(equal, bool) else bool(equal)
    except (TypeError, ValueError):
        return left.payload is right.payload
