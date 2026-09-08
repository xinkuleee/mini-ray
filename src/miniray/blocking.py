"""Worker-side CPU-yield notifications for blocking runtime calls."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterator, Optional

from . import protocol
from .ids import AttemptID, LeaseID, TaskID, WorkerID
from .transport import Address, TransportError, request as rpc_request


BLOCKED_HANDLER = "notify_worker_blocked"
UNBLOCKED_HANDLER = "notify_worker_unblocked"
_BLOCK_ATTEMPTS = 3
_UNBLOCK_ATTEMPTS = 8
_BACKOFF_BASE = 0.005
_BACKOFF_MAX = 0.1
_CONNECT_TIMEOUT_SECONDS = 0.5
_REQUEST_TIMEOUT_SECONDS = 1.0


def _blocking_rpc(address: Address, handler: str, message: object) -> object:
    """Use a short control-RPC bound for local block notifications."""

    return rpc_request(
        address, handler, message,
        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        request_timeout=_REQUEST_TIMEOUT_SECONDS,
    )


class BlockingNotificationError(RuntimeError):
    """CPU ownership could not be established safely."""


class _TerminalLease(RuntimeError):
    """The Node has already finalized the physical execution lease."""


@dataclass(frozen=True)
class BlockingIdentity:
    lease_id: LeaseID
    task_id: TaskID
    attempt_id: AttemptID
    worker_id: WorkerID
    node_address: Address

    def __post_init__(self) -> None:
        # Reuse the typed wire constructor for full identity validation.
        protocol.NotifyWorkerBlocked(
            self.lease_id, self.task_id, self.attempt_id, self.worker_id, 0
        )


class BlockingNotifier:
    """Issue exact, monotonic Block/Unblock episodes for one running lease."""

    def __init__(
        self,
        identity: BlockingIdentity,
        *,
        rpc: Callable[[Address, str, object], object] = _blocking_rpc,
        stopping: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.identity = identity
        self._rpc = rpc
        self._stopping = stopping or (lambda: False)
        self._sequence = -1
        # One physical Worker executes one user task at a time, but user code
        # may create threads.  Node's episode state is a single ordered stream,
        # so serialize the complete outer episode, not just sequence allocation.
        self._episode_lock = threading.Lock()
        self._local = threading.local()
        self._wait_event = threading.Event()

    def group_scope(self) -> "BlockingGroupScope":
        return BlockingGroupScope(self)

    @contextmanager
    def blocking_scope(self) -> Iterator[None]:
        depth = getattr(self._local, "depth", 0)
        self._local.depth = depth + 1
        if depth:
            try:
                yield
            finally:
                self._local.depth -= 1
            return

        try:
            # Lock acquisition can itself be interrupted before a Block exists.
            # The outer finally also covers that pre-RPC boundary; a later get
            # on this thread must not mistake it for an entered nested episode.
            with self._episode_lock:
                sequence = self._sequence + 1
                blocked = protocol.NotifyWorkerBlocked(
                    self.identity.lease_id, self.identity.task_id,
                    self.identity.attempt_id, self.identity.worker_id, sequence,
                )
                # Local message construction failure spends no Node episode.
                # Once sending may start, keep this sequence even on failure.
                self._sequence = sequence
                block_converged = False
                block_attempted = False
                try:
                    # `_converge_block` begins with an RPC attempt. Record that
                    # before entry so terminal/validation failure still creates
                    # the matching Unblock tombstone in `finally`.
                    block_attempted = True
                    block_converged, block_attempted = self._converge_block(blocked)
                    if not block_converged:
                        raise BlockingNotificationError(
                            "blocked notification did not converge"
                        )
                    yield
                finally:
                    # Once Block may have reached Node, exact Unblock must
                    # converge before user code resumes, even if its ACKs were
                    # all lost. Keep depth active through this same episode.
                    if block_converged or block_attempted:
                        unblocked = protocol.NotifyWorkerUnblocked(
                            self.identity.lease_id, self.identity.task_id,
                            self.identity.attempt_id, self.identity.worker_id, sequence,
                        )
                        self._converge_unblock(unblocked)
        finally:
            self._local.depth = 0

    def _converge_block(
        self, message: protocol.NotifyWorkerBlocked
    ) -> tuple[bool, bool]:
        attempted = False
        for attempt in range(_BLOCK_ATTEMPTS):
            try:
                attempted = True
                reply = self._rpc(self.identity.node_address, BLOCKED_HANDLER, message)
                self._validate_reply(
                    message, reply, protocol.NotifyWorkerBlockedReply,
                    terminal_is_converged=False,
                )
                return True, attempted
            except _TerminalLease as exc:
                raise BlockingNotificationError(str(exc)) from exc
            except BlockingNotificationError:
                # A typed non-terminal rejection is deterministic.  Retrying
                # cannot repair a stale sequence or mismatched execution ID.
                raise
            except TransportError:
                if attempt + 1 < _BLOCK_ATTEMPTS:
                    if self._stopping():
                        break
                    self._wait_event.wait(self._delay(attempt))
        return False, attempted

    def _converge_unblock(self, message: protocol.NotifyWorkerUnblocked) -> None:
        last_error: Optional[BaseException] = None
        for attempt in range(_UNBLOCK_ATTEMPTS):
            try:
                reply = self._rpc(self.identity.node_address, UNBLOCKED_HANDLER, message)
                self._validate_reply(
                    message, reply, protocol.NotifyWorkerUnblockedReply,
                    terminal_is_converged=True,
                )
                return
            except BlockingNotificationError:
                # A well-formed non-terminal rejection is deterministic; exact
                # replay cannot repair wrong identity or sequence.
                raise
            except TransportError as exc:
                last_error = exc
                if self._stopping():
                    raise BlockingNotificationError(
                        "unblocked notification cannot converge during shutdown"
                    ) from exc
                if attempt + 1 < _UNBLOCK_ATTEMPTS:
                    self._wait_event.wait(self._delay(attempt))
        raise BlockingNotificationError(
            "unblocked notification did not converge after exact replays"
        ) from last_error

    @staticmethod
    def _delay(attempt: int) -> float:
        return min(_BACKOFF_MAX, _BACKOFF_BASE * (2 ** min(attempt, 5)))

    @staticmethod
    def _validate_reply(
        request: object, reply: object, reply_type: type, *,
        terminal_is_converged: bool,
    ) -> None:
        if not isinstance(reply, reply_type):
            raise BlockingNotificationError("Node returned an invalid blocking reply")
        expected = (request.lease_id, request.task_id, request.attempt_id, request.worker_id, request.sequence)
        actual = (reply.lease_id, reply.task_id, reply.attempt_id, reply.worker_id, reply.sequence)
        if actual != expected:
            raise BlockingNotificationError(
                "Node returned a blocking reply for another execution"
            )
        if not reply.accepted:
            terminal = reply.state in (
                protocol.LeaseExecutionState.COMPLETED,
                protocol.LeaseExecutionState.ABANDONED,
                protocol.LeaseExecutionState.WORKER_LOST,
            )
            detail = reply.error or "blocking notification was rejected"
            if terminal:
                detail = "lease became terminal: {}".format(detail)
                if terminal_is_converged:
                    return
                raise _TerminalLease(detail)
            raise BlockingNotificationError(detail)


class BlockingGroupScope:
    """Lazily create one episode for an aggregate get/get_many operation."""

    def __init__(self, notifier: BlockingNotifier) -> None:
        self._notifier = notifier
        self._scope = None

    def begin_blocking(self) -> None:
        if self._scope is None:
            scope = self._notifier.blocking_scope()
            scope.__enter__()
            # Failed entry creates neither a reusable episode nor an exit
            # obligation. A caller may catch it and try this group again.
            self._scope = scope

    def close(self, exc_type=None, exc=None, tb=None) -> None:
        scope, self._scope = self._scope, None
        if scope is not None:
            scope.__exit__(exc_type, exc, tb)

    def __enter__(self) -> "BlockingGroupScope":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(exc_type, exc, tb)


__all__ = [
    "BLOCKED_HANDLER", "UNBLOCKED_HANDLER", "BlockingGroupScope",
    "BlockingIdentity", "BlockingNotificationError", "BlockingNotifier",
]
