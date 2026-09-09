"""Dedicated worker process for one stateful mini-Ray Actor.

An Actor worker is deliberately separate from the ordinary task worker.  It
owns one long-lived Python instance and one :class:`ActorMailbox`; it never
participates in the short-lived worker-lease pool.  ``TCPServer`` may invoke
handlers concurrently, while the mailbox plus the executor flag below provide
the two Actor guarantees that matter here:

* calls are admitted in FIFO order independently for every caller; and
* exactly one thread invokes user methods at a time.

The transport is still allowed to retry an ambiguous RPC.  A completed
``ActorCallReply`` is cached in the mailbox under
``(generation, caller_worker_id, sequence)``, so such a retry returns the old
reply without executing the method again.
"""

from __future__ import annotations

import hashlib
import math
import os
import threading
import traceback
from multiprocessing.connection import Connection
from typing import Optional, Tuple

import cloudpickle

from . import ids, protocol
from .actor_state import (
    ActorCall as MailboxCall,
    ActorCallConflictError,
    ActorCallState,
    ActorMailbox,
    ActorSubmitStatus,
)
from .errors import RuntimeShuttingDownError, StaleGenerationError
from .transport import LOOPBACK_HOST, Address, TCPServer, request as rpc_request
from .trace import EventSink, TraceSinkConfig
from .trace_collector import sink_from_config
from .worker import DEFAULT_INLINE_THRESHOLD_BYTES, SEAL_OBJECT_HANDLER


ACTOR_CALL_HANDLER = "actor_call"
SHUTDOWN_HANDLER = "shutdown"


def _decode_call_payload(payload: bytes) -> tuple[tuple[object, ...], dict[str, object]]:
    """Decode the one intentionally small Actor argument wire format."""

    value = cloudpickle.loads(payload)
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], tuple)
        or not isinstance(value[1], dict)
        or any(not isinstance(name, str) for name in value[1])
    ):
        raise TypeError(
            "actor arguments must encode (tuple(args), dict(kwargs))"
        )
    return value[0], dict(value[1])


def _call_fingerprint(request: protocol.ActorCallRequest) -> tuple[object, ...]:
    """Hash all logical-call fields not already present in the mailbox key."""

    return (
        request.method_name,
        request.task_id,
        request.attempt_id,
        request.owner_worker_id,
        hashlib.sha256(request.arguments).digest(),
    )


class ActorWorkerServer:
    """A direct-call server owning one Actor instance.

    Network request threads never hold ``_condition`` while user code runs.
    This is essential for an out-of-order call: a handler waiting for sequence
    0 must be able to enter while the sequence-1 handler is blocked.
    """

    def __init__(
        self,
        actor_id: ids.ActorID,
        generation: ids.ActorGeneration,
        worker_id: ids.WorkerID,
        class_definition: protocol.ActorClassDefinition,
        instance: object,
        node_id: ids.NodeID,
        node_address: Address,
        *,
        host: str = LOOPBACK_HOST,
        port: int = 0,
        request_timeout: float = 5.0,
        inline_threshold: int = DEFAULT_INLINE_THRESHOLD_BYTES,
        event_sink: Optional[EventSink] = None,
    ) -> None:
        if not isinstance(class_definition, protocol.ActorClassDefinition):
            raise TypeError("class_definition must be an ActorClassDefinition")
        self._initialize_actor_state(
            actor_id=actor_id,
            generation=generation,
            worker_id=worker_id,
            instance=instance,
            method_names=class_definition.method_names,
            node_id=node_id,
            node_address=node_address,
            inline_threshold=inline_threshold,
            request_timeout=request_timeout,
        )
        self.event_sink = event_sink or EventSink()
        self._server = TCPServer(
            {
                ACTOR_CALL_HANDLER: self._handle_actor_call,
                SHUTDOWN_HANDLER: self._handle_shutdown,
            },
            host=host,
            port=port,
            request_timeout=request_timeout,
            event_sink=self.event_sink,
            trace_component="actor_worker",
        )

    def _initialize_actor_state(
        self,
        *,
        actor_id: ids.ActorID,
        generation: ids.ActorGeneration,
        worker_id: ids.WorkerID,
        instance: object,
        method_names: Tuple[str, ...],
        node_id: ids.NodeID,
        node_address: Address,
        inline_threshold: int,
        request_timeout: float = 5.0,
    ) -> None:
        """Initialize execution state independently of the listening socket.

        Keeping this seam small lets pure unit tests exercise mailbox and user
        execution semantics without binding a port.
        """

        if not isinstance(actor_id, ids.ActorID):
            raise TypeError("actor_id must be an ActorID")
        if (
            not isinstance(generation, ids.ActorGeneration)
            or generation.actor_id != actor_id
        ):
            raise ValueError("generation must belong to actor_id")
        if not isinstance(worker_id, ids.WorkerID):
            raise TypeError("worker_id must be a WorkerID")
        if not isinstance(node_id, ids.NodeID):
            raise TypeError("node_id must be a NodeID")
        if (
            not isinstance(node_address, tuple)
            or len(node_address) != 2
            or not isinstance(node_address[0], str)
            or isinstance(node_address[1], bool)
            or not isinstance(node_address[1], int)
        ):
            raise TypeError("node_address must be a (host, port) tuple")
        if (
            isinstance(inline_threshold, bool)
            or not isinstance(inline_threshold, int)
            or inline_threshold < 0
        ):
            raise ValueError("inline_threshold must be a non-negative integer")
        if (
            isinstance(request_timeout, bool)
            or not isinstance(request_timeout, (int, float))
            or not math.isfinite(request_timeout)
            or request_timeout <= 0
        ):
            raise ValueError("request_timeout must be a positive finite number")
        exported_methods = tuple(method_names)
        if (
            not exported_methods
            or any(not isinstance(name, str) or not name for name in exported_methods)
            or len(exported_methods) != len(set(exported_methods))
        ):
            raise ValueError("method_names must contain unique non-empty strings")

        self.actor_id = actor_id
        self.generation = generation
        self.worker_id = worker_id
        self.node_id = node_id
        self.node_address = node_address
        self.inline_threshold = inline_threshold
        self._request_timeout = float(request_timeout)
        self._instance = instance
        self._method_names = frozenset(exported_methods)
        self._mailbox = ActorMailbox(actor_id, generation)
        self._condition = threading.Condition(threading.RLock())
        self._executor_active = False
        self._accepting = True
        self._stop_event = threading.Event()
        self._shutdown_exit_scheduled = False

    @property
    def address(self) -> Address:
        return self._server.address

    @property
    def is_running(self) -> bool:
        return self._server.is_running

    def start(self) -> Address:
        return self._server.start()

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._stop_event.wait(timeout)

    def stop(self) -> None:
        with self._condition:
            self._accepting = False
            self._stop_event.set()
            self._condition.notify_all()
        self._server.stop()

    def _handle_actor_call(self, value: object) -> object:
        if not isinstance(value, protocol.ActorCallRequest):
            raise TypeError("actor_call expects ActorCallRequest")
        request = value

        if (
            request.actor_id != self.actor_id
            or request.generation != self.generation
            or (
                request.target_worker_id is not None
                and request.target_worker_id != self.worker_id
            )
        ):
            return self._error_reply(
                request,
                protocol.TaskReplyStatus.SYSTEM_ERROR,
                StaleGenerationError(
                    "actor call targets {}, current generation is {}".format(
                        request.generation, self.generation
                    )
                ),
            )
        call = MailboxCall(
            actor_id=request.actor_id,
            generation=request.generation,
            caller_id=request.caller_worker_id,
            sequence=request.sequence,
            payload=request,
            fingerprint=_call_fingerprint(request),
        )

        while True:
            run_executor = False
            with self._condition:
                # New work is rejected after shutdown begins.  An already
                # completed invocation still gets its cached reply so a
                # concurrent shutdown cannot turn success into an error.
                known_call = self._mailbox.call_state(call.key) is not None
                if not self._accepting and not known_call:
                    return self._error_reply(
                        request,
                        protocol.TaskReplyStatus.SYSTEM_ERROR,
                        RuntimeShuttingDownError("actor worker is shutting down"),
                    )
                try:
                    submission = self._mailbox.submit_call(call)
                except ActorCallConflictError as exc:
                    return self._error_reply(
                        request, protocol.TaskReplyStatus.SYSTEM_ERROR, exc
                    )

                # The explicit generation check above makes this defensive,
                # while retaining a typed reply if mailbox state is advanced
                # by a future Actor-restart extension.
                if submission.status is ActorSubmitStatus.FENCED:
                    return self._error_reply(
                        request,
                        protocol.TaskReplyStatus.SYSTEM_ERROR,
                        StaleGenerationError("actor generation was fenced"),
                    )
                if submission.has_cached_result:
                    cached = submission.cached_result
                    if not isinstance(cached, protocol.ActorCallReply):
                        raise RuntimeError("actor mailbox cached an invalid reply")
                    return cached
                call_state = self._mailbox.call_state(call.key)
                if not self._accepting and call_state is ActorCallState.BUFFERED:
                    # A missing earlier sequence can never arrive after the
                    # admission gate closes.  Abandon this gap waiter instead
                    # of making shutdown wait forever for impossible work.
                    return self._error_reply(
                        request,
                        protocol.TaskReplyStatus.SYSTEM_ERROR,
                        RuntimeShuttingDownError("actor worker is shutting down"),
                    )

                if not self._executor_active and self._mailbox.ready_count:
                    self._executor_active = True
                    run_executor = True
                else:
                    # A gap-buffered call or a duplicate of queued/running
                    # work waits without holding the state lock.  Another TCP
                    # handler can therefore submit the missing sequence.
                    self._condition.wait()

            if run_executor:
                self._drain_mailbox()

    def _drain_mailbox(self) -> None:
        """Run admitted calls until the shared ready queue is empty."""

        while True:
            with self._condition:
                call = self._mailbox.take_next()
                if call is None:
                    self._executor_active = False
                    self._condition.notify_all()
                    return

            request = call.payload
            if not isinstance(request, protocol.ActorCallRequest):
                raise RuntimeError("actor mailbox contained an invalid call payload")
            reply = self._execute_call(request)

            with self._condition:
                completed = self._mailbox.complete_current(reply)
                if completed.key != call.key:
                    raise RuntimeError("actor mailbox completed a different call")
                self._condition.notify_all()

    def _execute_call(
        self, request: protocol.ActorCallRequest
    ) -> protocol.ActorCallReply:
        if request.method_name not in self._method_names:
            # Invalid calls still pass through the mailbox and consume their
            # caller sequence.  Rejecting sequence 0 before admission would
            # otherwise leave a later valid sequence 1 buffered forever.
            return self._error_reply(
                request,
                protocol.TaskReplyStatus.SYSTEM_ERROR,
                AttributeError(
                    "actor method is not exported: {!r}".format(
                        request.method_name
                    )
                ),
            )
        try:
            arguments, keyword_arguments = _decode_call_payload(request.arguments)
            method = getattr(self._instance, request.method_name)
            if not callable(method):
                raise TypeError(
                    "exported actor attribute is not callable: {!r}".format(
                        request.method_name
                    )
                )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return self._error_reply(
                request,
                protocol.TaskReplyStatus.SYSTEM_ERROR,
                exc,
                traceback.format_exc(),
            )

        try:
            result = method(*arguments, **keyword_arguments)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            # A user exception terminates only this mailbox entry.  The
            # executor continues with the next call on the same instance.
            return self._error_reply(
                request,
                protocol.TaskReplyStatus.APPLICATION_ERROR,
                exc,
                traceback.format_exc(),
            )

        try:
            descriptor = self._encode_result(request, result)
            task_reply = protocol.TaskReply(
                task_id=request.task_id,
                attempt_id=request.attempt_id,
                worker_id=self.worker_id,
                status=protocol.TaskReplyStatus.SUCCEEDED,
                results=(descriptor,),
                error=None,
            )
            return self._wrap_reply(request, task_reply)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return self._error_reply(
                request,
                protocol.TaskReplyStatus.SYSTEM_ERROR,
                exc,
                traceback.format_exc(),
            )

    def _encode_result(
        self, request: protocol.ActorCallRequest, value: object
    ) -> protocol.ResultDescriptor:
        payload = cloudpickle.dumps(value)
        checksum = hashlib.sha256(payload).hexdigest()
        object_id = ids.ObjectID(request.task_id, 0)
        if len(payload) <= self.inline_threshold:
            return protocol.ResultDescriptor(
                object_id=object_id,
                storage=protocol.ResultStorage.INLINE,
                size_bytes=len(payload),
                owner_worker_id=request.owner_worker_id,
                node_id=self.node_id,
                checksum=checksum,
                inline_data=payload,
            )

        seal_reply = rpc_request(
            self.node_address,
            SEAL_OBJECT_HANDLER,
            protocol.SealObject.from_data(
                object_id, request.attempt_id, request.owner_worker_id, payload
            ),
        )
        if not isinstance(seal_reply, protocol.SealObjectReply):
            raise RuntimeError("Node returned an invalid object seal reply")
        if (
            not seal_reply.sealed
            or seal_reply.object_id != object_id
            or seal_reply.node_id != self.node_id
            or seal_reply.size_bytes != len(payload)
            or seal_reply.checksum != checksum
        ):
            raise RuntimeError(
                seal_reply.error or "Node rejected or corrupted the object seal"
            )
        return protocol.ResultDescriptor(
            object_id=object_id,
            storage=protocol.ResultStorage.OBJECT_STORE,
            size_bytes=len(payload),
            owner_worker_id=request.owner_worker_id,
            node_id=self.node_id,
            checksum=checksum,
            inline_data=None,
        )

    def _error_reply(
        self,
        request: protocol.ActorCallRequest,
        status: protocol.TaskReplyStatus,
        exc: BaseException,
        traceback_text: str = "",
    ) -> protocol.ActorCallReply:
        return self._wrap_reply(
            request,
            protocol.TaskReply(
                task_id=request.task_id,
                attempt_id=request.attempt_id,
                worker_id=self.worker_id,
                status=status,
                results=(),
                error=protocol.RemoteErrorInfo(
                    type_name=type(exc).__name__,
                    message=str(exc),
                    traceback=traceback_text,
                ),
            ),
        )

    @staticmethod
    def _wrap_reply(
        request: protocol.ActorCallRequest, task_reply: protocol.TaskReply
    ) -> protocol.ActorCallReply:
        return protocol.ActorCallReply(
            actor_id=request.actor_id,
            generation=request.generation,
            caller_worker_id=request.caller_worker_id,
            sequence=request.sequence,
            task_reply=task_reply,
            route_epoch=request.route_epoch,
        )

    def _handle_shutdown(self, value: object) -> object:
        if not isinstance(value, protocol.Shutdown):
            raise TypeError("shutdown expects Shutdown")
        with self._condition:
            self._accepting = False
            self._condition.notify_all()
            drained = self._condition.wait_for(
                self._mailbox_drained_locked,
                timeout=self._request_timeout,
            )
        if drained:
            self._schedule_shutdown_exit()
        return protocol.ShutdownAck(
            request_id=value.request_id,
            component="actor-worker:{}".format(self.worker_id),
            clean=drained,
            detail=(
                "actor mailbox drained; actor worker stopping"
                if drained
                else "actor mailbox drain timed out; NodeManager must terminate it"
            ),
        )

    def _schedule_shutdown_exit(self) -> None:
        """Wake ``actor_worker_main`` only after its clean ACK is sent."""

        with self._condition:
            if self._shutdown_exit_scheduled:
                return
            self._shutdown_exit_scheduled = True
        handler_thread = threading.current_thread()
        if handler_thread.daemon:
            threading.Thread(
                target=self._release_wait_after_shutdown_handler,
                args=(handler_thread,),
                name="miniray-actor-worker-shutdown",
                daemon=False,
            ).start()
        else:
            # Direct in-process callers have no daemon transport handler whose
            # reply must be flushed before actor_worker_main may wake.
            self._stop_event.set()

    def _release_wait_after_shutdown_handler(
        self, handler_thread: threading.Thread
    ) -> None:
        """Non-daemon handoff that outlives the TCP handler if necessary."""

        handler_thread.join()
        self._stop_event.set()

    def _mailbox_drained_locked(self) -> bool:
        """Whether all admitted work is terminal; gap-buffered calls do not count."""

        return (
            not self._executor_active
            and self._mailbox.running is None
            and self._mailbox.ready_count == 0
        )


def _construct_actor(
    class_definition: protocol.ActorClassDefinition, constructor_payload: bytes
) -> object:
    actor_class = cloudpickle.loads(class_definition.payload)
    if not isinstance(actor_class, type):
        raise TypeError("actor class payload did not decode to a class")
    arguments, keyword_arguments = _decode_call_payload(constructor_payload)
    instance = actor_class(*arguments, **keyword_arguments)
    for method_name in class_definition.method_names:
        if not callable(getattr(instance, method_name, None)):
            raise TypeError(
                "exported actor method is missing or not callable: {!r}".format(
                    method_name
                )
            )
    return instance


def actor_worker_main(
    actor_id: ids.ActorID,
    generation: ids.ActorGeneration,
    worker_id: ids.WorkerID,
    class_definition: protocol.ActorClassDefinition,
    constructor_payload: bytes,
    node_id: ids.NodeID,
    node_address: Address,
    ready_connection: Optional[Connection] = None,
    host: str = LOOPBACK_HOST,
    port: int = 0,
    inline_threshold: int = DEFAULT_INLINE_THRESHOLD_BYTES,
    trace_config: Optional[TraceSinkConfig] = None,
) -> None:
    """Spawn-safe entry point; construction precedes endpoint publication."""

    server: Optional[ActorWorkerServer] = None
    trace_sink = sink_from_config(trace_config)
    startup_failure = protocol.ActorWorkerFailure.CONSTRUCTOR_FAILED
    try:
        instance = _construct_actor(class_definition, constructor_payload)
        startup_failure = protocol.ActorWorkerFailure.STARTUP_FAILED
        server = ActorWorkerServer(
            actor_id,
            generation,
            worker_id,
            class_definition,
            instance,
            node_id,
            node_address,
            host=host,
            port=port,
            inline_threshold=inline_threshold,
            event_sink=trace_sink,
        )
        address = server.start()
        trace_sink.emit(
            "actor_ready", component="actor_worker",
            actor_id=str(actor_id), generation=str(generation), worker_id=str(worker_id)
        )
        startup = protocol.ActorWorkerStartup(
            actor_id=actor_id,
            generation=generation,
            worker_id=worker_id,
            worker_pid=os.getpid(),
            worker_address=address,
        )
        if ready_connection is not None:
            ready_connection.send((True, startup))
            ready_connection.close()
            ready_connection = None
        server.wait()
    except BaseException:
        if ready_connection is not None:
            try:
                ready_connection.send((False, protocol.ActorWorkerStartupFailure(
                    startup_failure, traceback.format_exc()
                )))
            finally:
                ready_connection.close()
        raise
    finally:
        trace_sink.emit(
            "process_stopping", component="actor_worker", actor_id=str(actor_id)
        )
        if server is not None:
            server.stop()
        trace_sink.close()


__all__ = [
    "ACTOR_CALL_HANDLER",
    "SHUTDOWN_HANDLER",
    "ActorWorkerServer",
    "actor_worker_main",
]
