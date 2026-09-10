"""Ordinary task execution with an embedded owner/submitter CoreWorker.

WorkerServer accepts direct PushTask requests and obtains StartLease before
decoding and executing user code. Its lazy Core supports nested submissions
and owned references. Successful outputs use the owner/Node handoff
protocol; ambiguous acknowledgements replay retained bytes rather than user
code. The Node retains lease/resource authority; stateful Actors use the
separate actor_worker module. Module-level process entry points support spawn.
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
import traceback
from contextlib import nullcontext
from dataclasses import dataclass, replace
from enum import Enum
from multiprocessing.connection import Connection
from typing import Hashable, Optional, Tuple

import cloudpickle

from . import ids, protocol
from .core import CoreWorker
from .owner_service import (
    PREPARE_STORED_CONTAINED_PIN_HANDLER,
    PROMOTE_STORED_CONTAINED_PIN_HANDLER,
    REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER,
)
from .dependency import NestedReferenceImportSession, decode_inline_argument
from .output_discovery import OutputDiscoverySession, PreparedOutput
from .output_protocol import (
    PREPARE_OUTPUT_PUBLICATION_HANDLER, PrepareOutputPublication,
    PreparedOutputPublicationReply,
    FINALIZE_OUTPUT_OWNER_DEATH_HANDLER, FinalizeOutputOwnerDeath, FinalizeOutputOwnerDeathReply,
)
from .output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation,
)
from .task_outputs import TaskExecution
from .ref_transfer import importing_references
from .runtime_binding import ExecutionContext, bind_runtime
from .blocking import (
    BlockingIdentity, BlockingNotificationError, BlockingNotifier,
)
from .transport import (
    LOOPBACK_HOST,
    Address,
    TCPServer,
    TransportError,
    request as rpc_request,
)
from .trace import EventSink, NonOwningEventSink, TraceSinkConfig
from .trace_collector import sink_from_config


PUSH_TASK_HANDLER = "push_task"
REGISTER_FUNCTION_HANDLER = "register_function"
SHUTDOWN_HANDLER = "shutdown"
BEGIN_DRAIN_HANDLER = "begin_drain"
DRAIN_STATUS_HANDLER = "drain_status"
FINALIZE_SHUTDOWN_HANDLER = "finalize_shutdown"
SEAL_OBJECT_HANDLER = "seal_object"
GET_OBJECT_HANDLER = "get_object"
START_WORKER_LEASE_HANDLER = "start_worker_lease"
COMPLETE_WORKER_LEASE_HANDLER = "complete_worker_lease"
GET_WORKER_LEASE_OUTCOME_HANDLER = "get_worker_lease_outcome"
ACQUIRE_BORROWED_OBJECT_HANDLER = "acquire_borrowed_object"
RELEASE_BORROWED_OBJECT_HANDLER = "release_borrowed_object"
RELEASE_CONTAINED_REFERENCE_HANDLER = "release_contained_reference"
GET_OWNED_OBJECT_HANDLER = "get_owned_object"
REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER = (
    "request_owned_object_reconstruction"
)
REQUEST_DROP_OWNED_OBJECT_HANDLER = "request_drop_owned_object"
RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER = "retain_owned_object_for_task"
GET_RETAINED_OWNED_OBJECT_HANDLER = "get_retained_owned_object"
REPORT_RETAINED_OBJECT_LOCATION_HANDLER = "report_retained_object_location"
REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER = protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER
RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER = "release_owned_object_for_task"
DEFAULT_INLINE_THRESHOLD_BYTES = 100 * 1024
COMPLETION_RPC_ATTEMPTS = 3
EMBEDDED_CORE_STOP_TIMEOUT_SECONDS = 2.0
CRASH_AFTER_NESTED_IMPORT_EXIT_CODE = 24


class WorkerFailpointMode(str, Enum):
    SYSTEM_ERROR = "system_error"
    CRASH = "crash"
    CRASH_AFTER_NESTED_IMPORT = "crash_after_nested_import"


class _OutputCompletionPending(RuntimeError):
    """A discovered output must resume, never execute or serialize again."""


class _CompletionRejected(RuntimeError):
    """A validated matching Complete reply did not admit the requested terminal."""


@dataclass
class _PreparedOutputReply:
    """One local byte/source custody record across publication ambiguity.

    Only the Node owns publication authority.  These local acknowledgements
    retain once-serialized bytes and unfinished source/import cleanup across
    exact retries; they do not implement another remote lifecycle.
    """

    request: protocol.PushTask
    discovery: OutputDiscoverySession
    outputs: PreparedOutput
    nested_imports: Optional[NestedReferenceImportSession] = None
    prepare_acked: bool = False
    complete_envelope: Optional[OutputPublicationEnvelope] = None
    failure_reply: Optional[protocol.TaskReply] = None

    def release_promoted_sources(self) -> None:
        """Idempotently drain local custody, independent of remote progress.

        A promotion or Complete witness does not prove these callbacks returned.
        Keep the import transaction attached until close returns successfully,
        including a callback that takes effect and then raises.
        """

        self.discovery.release_sources_after_promotions()
        if self.nested_imports is not None:
            self.nested_imports.close()
            self.nested_imports = None

    def release_aborted_sources(self) -> None:
        self.discovery.abort()
        if self.nested_imports is not None:
            self.nested_imports.close()
            self.nested_imports = None


@dataclass(frozen=True)
class WorkerFailpointConfig:
    """Private deterministic fail-once hook for bounded runtime tests."""

    attempt_number: int = 0
    max_triggers: int = 1
    mode: WorkerFailpointMode | str = WorkerFailpointMode.SYSTEM_ERROR

    def __post_init__(self) -> None:
        if self.attempt_number != 0 or self.max_triggers != 1:
            raise ValueError(
                "the bounded worker failpoint supports only attempt 0 and one trigger"
            )
        try:
            mode = WorkerFailpointMode(self.mode)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "worker failpoint mode must be 'system_error', 'crash', "
                "or 'crash_after_nested_import'"
            ) from exc
        object.__setattr__(self, "mode", mode)


def _attempt_key(request: protocol.PushTask) -> Tuple[Hashable, Hashable]:
    """Key one physical execution, including its granted lease."""

    return request.spec.attempt_id, request.lease_id


def _error_reply(
    spec: protocol.TaskSpec,
    worker_id: ids.WorkerID,
    status: protocol.TaskReplyStatus,
    exc: BaseException,
) -> protocol.TaskReply:
    return protocol.TaskReply(
        task_id=spec.task_id,
        attempt_id=spec.attempt_id,
        worker_id=worker_id,
        status=status,
        results=(),
        error=protocol.RemoteErrorInfo(
            type_name=type(exc).__name__,
            message=str(exc),
            traceback=traceback.format_exc(),
        ),
    )


def _decode_argument(
    argument: protocol.TaskArg,
    *,
    dependencies: dict[ids.ObjectID, protocol.ObjectStoreDescriptor],
    node_id: ids.NodeID,
    node_address: Address,
    nested_imports: Optional[NestedReferenceImportSession] = None,
) -> object:
    if isinstance(argument, protocol.InlineArg):
        return decode_inline_argument(
            argument, import_nested_ref=nested_imports
        )
    if isinstance(argument, protocol.RefArg):
        try:
            descriptor = dependencies[argument.object_id]
        except KeyError as exc:
            raise RuntimeError(
                "stored task argument has no target-local dependency descriptor"
            ) from exc
        if descriptor.owner_worker_id != argument.owner_worker_id:
            raise RuntimeError(
                "task argument owner does not match dependency descriptor"
            )
        if descriptor.node_id != node_id:
            raise RuntimeError(
                "dependency descriptor is not local to this worker's node"
            )
        reply = rpc_request(
            node_address,
            GET_OBJECT_HANDLER,
            protocol.GetObject(
                object_id=argument.object_id,
                requester_node_id=node_id,
                expected_attempt_id=descriptor.producer_attempt_id,
                expected_owner_worker_id=descriptor.owner_worker_id,
                expected_size_bytes=descriptor.size_bytes,
                expected_checksum=descriptor.checksum,
            ),
        )
        if not isinstance(reply, protocol.GetObjectReply):
            raise RuntimeError("Node returned an invalid dependency object reply")
        if reply.object_id != argument.object_id:
            raise RuntimeError("Node returned a different dependency object")
        if reply.node_id != node_id:
            raise RuntimeError("dependency object came from a different node")
        if not reply.found or not reply.sealed or reply.data is None:
            raise RuntimeError(reply.error or "local dependency is not sealed")
        if reply.producer_attempt_id != descriptor.producer_attempt_id:
            raise RuntimeError(
                "dependency producer attempt does not match descriptor"
            )
        if reply.owner_worker_id != descriptor.owner_worker_id:
            raise RuntimeError("dependency owner does not match descriptor")
        if reply.size_bytes != descriptor.size_bytes:
            raise RuntimeError("dependency reply size does not match descriptor")
        if len(reply.data) != descriptor.size_bytes:
            raise RuntimeError("dependency object size does not match descriptor")
        checksum = hashlib.sha256(reply.data).hexdigest()
        if checksum != descriptor.checksum or reply.checksum != descriptor.checksum:
            raise RuntimeError("dependency object checksum does not match descriptor")
        return cloudpickle.loads(reply.data)
    raise TypeError("unknown task argument type: {}".format(type(argument).__name__))


def _decode_call(
    request: protocol.PushTask,
    *,
    node_id: ids.NodeID,
    node_address: Address,
    nested_imports: Optional[NestedReferenceImportSession] = None,
) -> tuple[tuple[object, ...], dict[str, object]]:
    """Materialize each canonical TaskArg exactly once.

    A Python value shaped like ``(args, kwargs)`` is still one user argument.
    Inferring an old packed-call encoding from its shape would both corrupt
    that value and make changing the inline budget change call semantics.
    """

    spec = request.spec
    dependencies = {item.object_id: item for item in request.dependencies}

    positional = tuple(
        _decode_argument(
            argument,
            dependencies=dependencies,
            node_id=node_id,
            node_address=node_address,
            nested_imports=nested_imports,
        )
        for argument in spec.args
    )
    keyword = {
        name: _decode_argument(
            argument,
            dependencies=dependencies,
            node_id=node_id,
            node_address=node_address,
            nested_imports=nested_imports,
        )
        for name, argument in spec.kwargs
    }
    return positional, keyword


class WorkerServer:
    """A single-executor TCP task worker.

    ``TCPServer`` dispatches connections on threads, but the execution lock
    preserves normal Ray worker semantics: one user task executes at a time.
    Replies are cached per logical task attempt, making a retried ``PushTask``
    RPC idempotent without pretending that distinct attempts are exactly-once.
    """

    def __init__(
        self,
        worker_id: ids.WorkerID,
        *,
        node_id: Optional[ids.NodeID] = None,
        host: str = LOOPBACK_HOST,
        port: int = 0,
        request_timeout: float = 5.0,
        node_address: Optional[Address] = None,
        inline_threshold: int = DEFAULT_INLINE_THRESHOLD_BYTES,
        failpoint: Optional[WorkerFailpointConfig] = None,
        event_sink: Optional[EventSink] = None,
        gcs_address: Optional[Address] = None,
    ) -> None:
        if (
            isinstance(inline_threshold, bool)
            or not isinstance(inline_threshold, int)
            or inline_threshold < 0
        ):
            raise ValueError("inline_threshold must be a non-negative integer")
        self.worker_id = worker_id
        self.node_id = node_id or ids.NodeID.random()
        self.node_address = node_address
        self.gcs_address = gcs_address
        self.inline_threshold = inline_threshold
        self._request_timeout = float(request_timeout)
        if failpoint is not None and not isinstance(failpoint, WorkerFailpointConfig):
            raise TypeError("failpoint must be a WorkerFailpointConfig or None")
        self._failpoint = failpoint
        self.event_sink = event_sink or EventSink()
        self._failpoint_triggers = 0
        self._crash_after_complete: set[Tuple[Hashable, Hashable]] = set()
        self._stop_event = threading.Event()
        self._execution_lock = threading.Lock()
        self._lifecycle = threading.Condition(threading.RLock())
        self._accepting_tasks = True
        self._active_tasks = 0
        # Admission is remembered before StartLease.  If that RPC or a later
        # completion acknowledgement is lost, the submitter may recover only by
        # replaying this exact immutable PushTask.  Obligations are the subset
        # that have not yet cached a reply *and* completed their Node lease.
        self._accepted_pushes: dict[
            Tuple[Hashable, Hashable], protocol.PushTask
        ] = {}
        self._push_obligations: set[Tuple[Hashable, Hashable]] = set()
        self._embedded_core_lock = threading.Lock()
        self._embedded_core_drain_lock = threading.Lock()
        self._embedded_core: Optional[CoreWorker] = None
        self._embedded_core_job_id: Optional[ids.JobID] = None
        self._embedded_core_stopped = False
        self._owner_retain_admission_open = True
        self._drain_request_id: Optional[str] = None
        self._drain_clean = False
        self._finalize_exit_scheduled = False
        self._worker_core_enabled = True
        self._replies: dict[
            Tuple[Hashable, Hashable], protocol.TaskReply
        ] = {}
        self._cached_pushes: dict[
            Tuple[Hashable, Hashable], protocol.PushTask
        ] = {}
        # A cached terminal reply and a completion acknowledgement are
        # intentionally separate facts.  If CompleteWorkerLease takes effect
        # but its RPC reply is lost, a repeated PushTask must retry only the
        # idempotent completion notification; it must never invoke user code a
        # second time.
        self._completion_acked: set[Tuple[Hashable, Hashable]] = set()
        # One result-discovery custody ledger for every output shape and tier.
        # It is populated before the first publication effect and retains both
        # source refs and their attempt import transaction until exact ACKs.
        self._prepared_output_replies: dict[
            Tuple[Hashable, Hashable], _PreparedOutputReply
        ] = {}
        # A compensated failure no longer retains discovery bytes or a success
        # envelope, but owner-death cleanup must still match its exact manifest.
        self._cached_output_manifests: dict[
            Tuple[Hashable, Hashable], OutputPublicationManifest
        ] = {}
        self._owner_abandoned_outputs: dict[
            Tuple[Hashable, Hashable], FinalizeOutputOwnerDeath
        ] = {}
        self._lease_bindings: dict[ids.LeaseID, ids.AttemptID] = {}
        self._attempt_leases: dict[ids.AttemptID, ids.LeaseID] = {}
        self._functions: dict[protocol.FunctionKey, bytes] = {}
        self._server = TCPServer(
            {
                REGISTER_FUNCTION_HANDLER: self._handle_register_function,
                PUSH_TASK_HANDLER: self._handle_push_task,
                FINALIZE_OUTPUT_OWNER_DEATH_HANDLER: self._handle_finalize_output_owner_death,
                ACQUIRE_BORROWED_OBJECT_HANDLER: (
                    self._handle_acquire_borrowed_object
                ),
                RELEASE_BORROWED_OBJECT_HANDLER: (
                    self._handle_release_borrowed_object
                ),
                RELEASE_CONTAINED_REFERENCE_HANDLER: (
                    self._handle_release_contained_reference
                ),
                GET_OWNED_OBJECT_HANDLER: self._handle_get_owned_object,
                "register_output_handoff": lambda request: self._handle_output_handoff("register_output_handoff", request),
                "report_output_handoff_complete": lambda request: self._handle_output_handoff("report_output_handoff_complete", request),
                "report_output_handoff_rollback": lambda request: self._handle_output_handoff("report_output_handoff_rollback", request),
                "get_output_handoff": lambda request: self._handle_output_handoff("get_output_handoff", request),
                REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER: (
                    self._handle_request_owned_object_reconstruction
                ),
                REQUEST_DROP_OWNED_OBJECT_HANDLER: (
                    self._handle_request_drop_owned_object
                ),
                RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER: (
                    self._handle_retain_owned_object_for_task
                ),
                GET_RETAINED_OWNED_OBJECT_HANDLER: (
                    self._handle_get_retained_owned_object
                ),
                REPORT_RETAINED_OBJECT_LOCATION_HANDLER: (
                    self._handle_report_retained_object_location
                ),
                REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER: self._handle_report_abandoned_dependency_replica,
                RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER: (
                    self._handle_release_owned_object_for_task
                ),
                REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER: (
                    self._handle_replace_retained_object_for_task
                ),
                PREPARE_STORED_CONTAINED_PIN_HANDLER: (
                    self._handle_prepare_stored_contained_pin
                ),
                PROMOTE_STORED_CONTAINED_PIN_HANDLER: (
                    self._handle_promote_stored_contained_pin
                ),
                BEGIN_DRAIN_HANDLER: self._handle_begin_drain,
                DRAIN_STATUS_HANDLER: self._handle_drain_status,
                FINALIZE_SHUTDOWN_HANDLER: self._handle_finalize_shutdown,
                SHUTDOWN_HANDLER: self._handle_shutdown,
            },
            host=host,
            port=port,
            request_timeout=request_timeout,
            event_sink=self.event_sink,
            trace_component="worker",
        )

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
        condition = getattr(self, "_lifecycle", None)
        if condition is not None:
            with condition:
                self._accepting_tasks = False
                condition.notify_all()
        self._shutdown_embedded_core(EMBEDDED_CORE_STOP_TIMEOUT_SECONDS)
        self._stop_event.set()
        self._server.stop()

    def _emit(self, name: str, **attributes: object) -> None:
        """Record an observational event without joining task execution."""

        sink = getattr(self, "event_sink", None)
        if sink is None:
            return
        try:
            sink.emit(name, component="worker", attributes=attributes)
        except Exception:
            # Trace delivery must not affect lease or task state transitions.
            return

    def _handle_push_task(self, request: object) -> object:
        if not isinstance(request, protocol.PushTask):
            raise TypeError("push_task expects PushTask")
        if not self._begin_task(request):
            raise RuntimeError("worker is shutting down")
        try:
            # Serve a durable reply before re-entering StartLease/function
            # decoding.  This is required for exact recovery after a lost
            # CompleteLease ACK and also keeps closed-admission cache replay
            # independent of new-work setup.  A concurrent duplicate waits for
            # the first handler to cache its reply under this same lock.
            execution_lock = getattr(self, "_execution_lock", None)
            replay_guard = (
                execution_lock if execution_lock is not None else nullcontext()
            )
            with replay_guard:
                key = _attempt_key(request)
                if key in getattr(self, "_owner_abandoned_outputs", {}):
                    raise RuntimeError(
                        "output owner died; accepted execution is terminally abandoned"
                    )
                cached = getattr(self, "_replies", {}).get(key)
                if cached is not None:
                    if request.worker_id != self.worker_id:
                        raise RuntimeError("PushTask targets a different worker")
                    if getattr(self, "_cached_pushes", {}).get(key) != request:
                        raise RuntimeError(
                            "cached PushTask replay changed the original request"
                        )
                    self._ensure_completion_acked(request, cached, key)
                    return cached
                prepared = getattr(self, "_prepared_output_replies", {}).get(key)
                if prepared is not None:
                    if prepared.request != request:
                        raise RuntimeError(
                            "prepared output PushTask replay changed request"
                        )
                    return self._resume_discovered_outputs(prepared, key)
            return self._handle_admitted_push_task(request)
        finally:
            self._end_task()

    def _handle_admitted_push_task(
        self, request: protocol.PushTask
    ) -> protocol.TaskReply:
        """Execute one request already admitted by the lifecycle gate."""

        with self._execution_lock:
            key = _attempt_key(request)
            if key in getattr(self, "_owner_abandoned_outputs", {}):
                raise RuntimeError(
                    "output owner died; accepted execution is terminally abandoned"
                )
            spec = request.spec
            self._validate_embedded_core_job(spec.job_id)
            self._emit(
                "task_started",
                task_id=str(spec.task_id),
                attempt_id=str(spec.attempt_id),
                worker_id=str(self.worker_id),
            )
            if self.node_address is None:
                raise RuntimeError(
                    "PushTask execution requires a NodeManager endpoint"
                )

            # Validate the physical destination and lease/attempt binding
            # before consulting the deduplication cache.  Otherwise a push to
            # the wrong worker (or a reused lease) could obtain a cached reply
            # without ever being fenced.  A fencing failure is a rejected RPC,
            # not a terminal task result: this worker never started that lease
            # and therefore cannot legitimately complete it.
            self._validate_lease_binding(request)

            cached = self._replies.get(key)
            if cached is not None:
                if self._cached_pushes.get(key) != request:
                    raise RuntimeError(
                        "cached PushTask replay changed the original request"
                    )
                self._ensure_completion_acked(request, cached, key)
                return cached
            prepared = getattr(self, "_prepared_output_replies", {}).get(key)
            if prepared is not None:
                if prepared.request != request:
                    raise RuntimeError(
                        "prepared output PushTask replay changed request"
                    )
                return self._resume_discovered_outputs(prepared, key)

            # Start is idempotent at the NodeManager.  The acknowledgement must
            # precede all function decoding and user execution so an abandoned
            # or stale grant can never run after its resources were reclaimed.
            node_incarnation = self._start_worker_lease(request)
            self._commit_lease_binding(request)
            failpoint_mode = self._claim_failpoint(spec.attempt_id)
            if failpoint_mode is WorkerFailpointMode.CRASH:
                # Crash only after the status-only CompleteLease ACK, before
                # the surrounding TCP handler can send TaskReply to the owner.
                # This deterministically exposes the hardest delivery window.
                self._crash_after_complete.add(key)
            if failpoint_mode is WorkerFailpointMode.SYSTEM_ERROR:
                reply = _error_reply(
                    spec,
                    self.worker_id,
                    protocol.TaskReplyStatus.SYSTEM_ERROR,
                    RuntimeError("injected system failure before user-code decode"),
                )
                return self._cache_complete_and_return(request, reply, key)

            nested_imports: Optional[NestedReferenceImportSession] = None
            # Everything before invoking the user callable is runtime work:
            # registry lookup, protocol decoding, and deserialization failures
            # are SYSTEM_ERROR and are eligible for system-level recovery.
            try:
                try:
                    nested_imports = self._nested_argument_import_session(request)
                    definition = spec.function_definition
                    if definition is not None:
                        function_payload = definition.payload
                        registered = self._functions.get(definition.key)
                        if registered is not None and registered != function_payload:
                            raise RuntimeError(
                                "function key is registered with different bytes"
                            )
                    else:
                        try:
                            function_payload = self._functions[spec.function]
                        except KeyError as exc:
                            raise RuntimeError(
                                "task has no embedded function definition and function "
                                "is not registered: {!r}".format(spec.function)
                            ) from exc
                    function = cloudpickle.loads(function_payload)
                    if not callable(function):
                        raise TypeError(
                            "task function payload did not decode to a callable"
                        )
                    if definition is not None:
                        self._functions[definition.key] = function_payload
                    assert self.node_address is not None
                    # Materialized RefArg and ready-INLINE dependency bytes
                    # may contain result-reference reducers, even when their
                    # TaskArg has no explicit nested manifest. Both formats
                    # share one import lifetime, scoped to argument decoding.
                    with importing_references(nested_imports.resolve_exported):
                        arguments, keyword_arguments = _decode_call(
                            request,
                            node_id=self.node_id,
                            node_address=self.node_address,
                            nested_imports=nested_imports,
                        )
                    if nested_imports is not None:
                        nested_imports.commit()
                    if (
                        failpoint_mode
                        is WorkerFailpointMode.CRASH_AFTER_NESTED_IMPORT
                    ):
                        if (
                            nested_imports is None
                            or not nested_imports.acquired
                        ):
                            raise RuntimeError(
                                "crash_after_nested_import failpoint did not "
                                "acquire a nested ObjectRef borrower"
                            )
                        # The import transaction is committed and every
                        # attempt-scoped ObjectRef is still live here.  A real
                        # os._exit performs no stack unwinding, so neither the
                        # session finalizer below nor user code can release the
                        # borrower first.  Node/GCS death authority must clean
                        # the physical Worker incarnation.
                        os._exit(CRASH_AFTER_NESTED_IMPORT_EXIT_CODE)
                    execution_binding = self._execution_binding(request)
                except BaseException as exc:
                    if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                        raise
                    if nested_imports is not None:
                        nested_imports.rollback()
                    reply = _error_reply(
                        spec,
                        self.worker_id,
                        protocol.TaskReplyStatus.SYSTEM_ERROR,
                        exc,
                    )
                else:
                    try:
                        with execution_binding:
                            value = function(*arguments, **keyword_arguments)
                    except BaseException as exc:
                        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                            raise
                        reply = _error_reply(
                            spec,
                            self.worker_id,
                            (
                                protocol.TaskReplyStatus.SYSTEM_ERROR
                                if isinstance(exc, BlockingNotificationError)
                                else protocol.TaskReplyStatus.APPLICATION_ERROR
                            ),
                            exc,
                        )
                    else:
                        try:
                            # A task returns one Python value, including a tuple or list.
                            discovery = self._output_discovery_session(
                                request, node_incarnation
                            )
                            outputs = discovery.discover(value)
                            prepared = _PreparedOutputReply(
                                request, discovery, outputs, nested_imports
                            )
                            table = getattr(self, "_prepared_output_replies", None)
                            if table is None:
                                table = {}
                                self._prepared_output_replies = table
                            # Transfer the result stream and argument borrowers
                            # before the first Node effect; the complete result
                            # shares one publication identity.
                            table[key] = prepared
                            nested_imports = None
                            return self._resume_discovered_outputs(prepared, key)
                        except BaseException as exc:
                            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                                raise
                            if isinstance(exc, _OutputCompletionPending):
                                raise
                            reply = _error_reply(
                                spec,
                                self.worker_id,
                                protocol.TaskReplyStatus.SYSTEM_ERROR,
                                exc,
                            )
            finally:
                # Attempt-scoped borrowers never depend on Python GC.  This runs
                # after decode, user code, and result serialization, but before
                # the terminal reply is cached and CompleteLease may release the
                # physical Worker lease.
                if nested_imports is not None:
                    nested_imports.close()

            return self._cache_complete_and_return(request, reply, key)

    def _begin_task(self, request: protocol.PushTask) -> bool:
        """Linearize new admission and exact replay against shutdown.

        Once a key has been admitted, its byte-for-byte request may enter even
        after ordinary admission closes.  This is recovery of already accepted
        work, not admission of a new task.
        """

        with self._lifecycle:
            key = _attempt_key(request)
            accepted = self._accepted_pushes
            if key in self._owner_abandoned_outputs:
                raise RuntimeError("output owner died; accepted execution is terminally abandoned")
            obligations = self._push_obligations

            previous = accepted.get(key)
            if previous is not None:
                if previous != request:
                    raise RuntimeError(
                        "accepted PushTask replay changed the original request"
                    )
                self._active_tasks += 1
                return True
            if not self._accepting_tasks:
                return False
            if request.worker_id != self.worker_id:
                raise RuntimeError("PushTask targets a different worker")
            # This is the replay credential and must be durable in Worker memory
            # before the first StartLease RPC can reach the Node.
            accepted[key] = request
            obligations.add(key)
            self._active_tasks += 1
            return True

    def _end_task(self) -> None:
        with self._lifecycle:
            self._active_tasks -= 1
            if self._active_tasks < 0:
                raise AssertionError("worker active-task count became negative")
            self._lifecycle.notify_all()

    def _resolve_push_obligation(
        self, request: protocol.PushTask, key: Tuple[Hashable, Hashable]
    ) -> None:
        """Clear one obligation only after reply cache and completion ACK."""

        with self._lifecycle:
            accepted = self._accepted_pushes
            previous = accepted.get(key)
            if previous is not None and previous != request:
                raise RuntimeError(
                    "accepted PushTask replay changed the original request"
                )
            if (
                key not in self._replies
                or key not in self._completion_acked
            ):
                return
            self._push_obligations.discard(key)
            self._lifecycle.notify_all()

    def _execution_binding(self, request: protocol.PushTask) -> object:
        """Bind the current task to this Worker's lazily-created CoreWorker."""

        if not getattr(self, "_worker_core_enabled", False):
            return nullcontext()
        spec = request.spec
        core = self._embedded_core_for(spec.job_id)
        if self.node_address is None:
            raise RuntimeError("blocking notifier requires a NodeManager endpoint")
        notifier = BlockingNotifier(
            BlockingIdentity(
                request.lease_id, spec.task_id, spec.attempt_id,
                self.worker_id, self.node_address,
            ),
            stopping=lambda: self._stop_event.is_set(),
        )
        context = ExecutionContext(
            job_id=spec.job_id,
            parent_task_id=spec.task_id,
            parent_attempt_id=spec.attempt_id,
            blocking_notifier=notifier,
        )
        return bind_runtime(core, context)

    def _nested_argument_import_session(
        self, request: protocol.PushTask
    ) -> NestedReferenceImportSession:
        """Create one attempt-wide importer before decoding any argument.

        Nested references are data, not execution dependencies, so the Node and
        dependency gate never materialize them.  The execution Worker imports
        them through its embedded Core only after StartLease, sharing this one
        transaction across positional and keyword arguments.
        """

        spec = request.spec
        arguments = spec.args + tuple(value for _, value in spec.kwargs)
        has_manifest = any(
            isinstance(argument, protocol.InlineArg)
            and argument.nested_refs
            for argument in arguments
        )
        restore = None
        if has_manifest:
            core = self._embedded_core_for(spec.job_id)
            restore = getattr(core, "_restore_task_argument_reference", None)
            if not callable(restore):
                raise RuntimeError(
                    "Worker Core cannot import nested task-argument ObjectRefs"
                )

        def import_nested(transfer):
            if restore is None:
                raise RuntimeError("Task argument has no declared nested-reference manifest")
            return restore(transfer, spec.attempt_id)

        def import_exported(object_id, owner_worker_id, owner_address, hold):
            # Plain arguments/results do not start an embedded Core. Only an
            # actual result-reference reducer needs the borrower protocol.
            core = self._embedded_core_for(spec.job_id)
            return core._restore_borrowed_reference(
                object_id, owner_worker_id, owner_address, hold,
            )

        return NestedReferenceImportSession(
            import_nested, import_exported_ref=import_exported,
        )

    def _embedded_core_for(self, job_id: ids.JobID) -> CoreWorker:
        """Return the one owner/submitter Core embedded in this Worker."""

        lock = self._embedded_core_lock
        with lock:
            if self._embedded_core_stopped:
                raise RuntimeError("worker CoreWorker is shut down")
            core = self._embedded_core
            if core is not None:
                if self._embedded_core_job_id != job_id:
                    raise RuntimeError(
                        "mini-Ray teaching Workers support one active job"
                    )
                return core
            if self.node_address is None:
                raise RuntimeError(
                    "Worker-side Core requires a NodeManager endpoint"
                )
            core = CoreWorker(
                self.node_address,
                self.node_id,
                job_id=job_id,
                worker_id=self.worker_id,
                gcs_address=self.gcs_address,
                inline_threshold=self.inline_threshold,
                dispatch_lanes=1,
                owner_address=self._owner_address(),
                poll_node_deaths=True,
                # The Worker process owns one physical trace source and closes
                # it in worker_main.  Its embedded Core uses a non-owning view:
                # child-task events share the same sequence/sender while the
                # distinct component exposes where submission originated.
                event_sink=NonOwningEventSink(
                    self.event_sink, component="worker_core"
                ),
            )
            try:
                # Do not expose a lazy owner with stale replica/Node state.
                # Its coordinator and this bootstrap share the same sync lock.
                if not core._sync_node_deaths():
                    raise RuntimeError("Worker Core could not read certified local Node deaths")
                if not getattr(self, "_owner_retain_admission_open", True):
                    close_retain = getattr(
                        core, "close_owner_retain_admission", None
                    )
                    if close_retain is not None:
                        close_retain()
            except BaseException:
                # The Core has not entered the Worker cache, so no task or
                # owner protocol can reference it.  Use the same local-only
                # startup abort as Driver init and preserve the causal error.
                try:
                    core._abort_unpublished_startup()
                except Exception:
                    pass
                raise
            self._embedded_core = core
            self._embedded_core_job_id = job_id
            return core

    def _output_discovery_session(
        self, request: protocol.PushTask,
        node_incarnation: OutputPublicationNodeIncarnation,
    ) -> OutputDiscoverySession:
        """The discovery entry for the single result, at either storage tier."""

        if (
            type(node_incarnation) is not OutputPublicationNodeIncarnation
            or node_incarnation.node_id != self.node_id
        ):
            raise RuntimeError("output discovery has no exact accepted Node incarnation")
        execution = TaskExecution.from_task_spec(
            request.spec
        )
        header = OutputPublicationHeader(
            OutputPublicationID(request.lease_id, execution),
            request.spec.job_id, self.worker_id, request.spec.owner_worker_id,
            node_incarnation,
        )
        return OutputDiscoverySession(
            header, inline_threshold=self.inline_threshold,
            owner_address=self._owner_address,
        )

    def _owner_address(self) -> Optional[Address]:
        """Return the pre-bound TCP route without requiring a started server."""

        server = getattr(self, "_server", None)
        return None if server is None else server.address

    def _validate_embedded_core_job(self, job_id: ids.JobID) -> None:
        """Reject another job before starting its lease or decoding payloads."""

        lock = getattr(self, "_embedded_core_lock", None)
        if lock is None:
            return
        with lock:
            active_job = self._embedded_core_job_id
            if self._embedded_core is not None and active_job != job_id:
                raise RuntimeError(
                    "mini-Ray teaching Workers support one active job"
                )

    def _shutdown_embedded_core(
        self, timeout: float, *, preserve_owner_protocol: bool = False
    ) -> bool:
        """Drain accepted child work with the current Core shutdown contract."""

        lock = self._embedded_core_lock
        with lock:
            if self._embedded_core_stopped and not preserve_owner_protocol:
                return True
            core = self._embedded_core
            if core is None:
                self._embedded_core_stopped = True
                return True
            drain_lock = self._embedded_core_drain_lock

        # Never hold the owner lookup lock across CoreWorker.shutdown().  Peer
        # release/query handlers use that lock to obtain ``core`` and may be
        # exactly the work that lets this drain converge.
        with drain_lock:
            with lock:
                if self._embedded_core is not core:
                    return False
                if self._embedded_core_stopped and not preserve_owner_protocol:
                    return True
            try:
                stopped = core.shutdown(
                    timeout=max(0.001, timeout),
                    preserve_owner_protocol=preserve_owner_protocol,
                )
            except Exception:
                stopped = False
            # A timed-out drain may later complete; only cache actual success so
            # Node finalization or an idempotent Shutdown replay can retry it.
            # A preserved owner service can receive a late release that creates
            # fresh physical-GC work.  Re-run its drain on every cluster round
            # instead of treating an earlier clean observation as permanent.
            with lock:
                if self._embedded_core is core:
                    self._embedded_core_stopped = (
                        stopped and not preserve_owner_protocol
                    )
            return stopped

    def _claim_failpoint(
        self, attempt_id: ids.AttemptID
    ) -> Optional[WorkerFailpointMode]:
        """Consume one attempt-scoped failpoint under the execution lock."""

        config = getattr(self, "_failpoint", None)
        triggers = getattr(self, "_failpoint_triggers", 0)
        if config is None or triggers >= config.max_triggers:
            return None
        if attempt_id.attempt_number != config.attempt_number:
            return None
        self._failpoint_triggers = triggers + 1
        return config.mode


    def _start_worker_lease(
        self, request: protocol.PushTask,
    ) -> OutputPublicationNodeIncarnation:
        """Obtain the NodeManager's permission before executing a task."""

        if self.node_address is None:
            raise RuntimeError(
                "PushTask execution requires a NodeManager endpoint"
            )
        reply = rpc_request(
            self.node_address,
            START_WORKER_LEASE_HANDLER,
            protocol.StartWorkerLease(
                lease_id=request.lease_id,
                task_id=request.spec.task_id,
                attempt_id=request.spec.attempt_id,
                worker_id=self.worker_id,
                scheduling_key=request.spec.scheduling_key,
            ),
        )
        if not isinstance(reply, protocol.StartWorkerLeaseReply):
            raise RuntimeError(
                "Node returned an invalid worker-lease start reply"
            )
        if (
            reply.lease_id != request.lease_id
            or reply.scheduling_key != request.spec.scheduling_key
        ):
            raise RuntimeError(
                "Node returned a start reply for a different worker lease or scheduling key"
            )
        if (
            not reply.accepted
            or reply.state is not protocol.LeaseExecutionState.RUNNING
        ):
            raise RuntimeError(
                reply.error or "Node rejected the worker-lease start"
            )
        # Node supplies this after matching the exact lease and moving it to
        # RUNNING.  No Driver/GCS lookup or guessed epoch may replace it.
        incarnation = reply.node_incarnation
        if type(incarnation) is not OutputPublicationNodeIncarnation:
            raise RuntimeError("accepted Start omitted the registered Node incarnation")
        incarnation = replace(incarnation)
        if incarnation.node_id != self.node_id:
            raise RuntimeError("accepted Start names another publishing Node")
        return incarnation

    def _cache_complete_and_return(
        self,
        request: protocol.PushTask,
        reply: protocol.TaskReply,
        key: Tuple[Hashable, Hashable],
    ) -> protocol.TaskReply:
        """Persist an error choice or an already-Complete output handoff."""

        # Early errors keep their terminal choice before their Complete RPC.
        # Success reaches here only with the actual Node Complete envelope and
        # drained source/import custody, so cache replay cannot skip cleanup.
        self._cached_pushes[key] = request
        self._replies[key] = reply
        self._ensure_completion_acked(request, reply, key)
        prepared = getattr(self, "_prepared_output_replies", {}).get(key)
        if prepared is not None and reply.output_publication is None:
            manifests = getattr(self, "_cached_output_manifests", None)
            if manifests is None:
                manifests = {}
                self._cached_output_manifests = manifests
            manifests[key] = prepared.outputs.manifest
        getattr(self, "_prepared_output_replies", {}).pop(key, None)
        if key in getattr(self, "_crash_after_complete", set()):
            self._crash_after_complete.discard(key)
            # The Node supervisor, not a fabricated TaskReply, observes this
            # direct-child exit and preserves the completed lease outcome.
            os._exit(23)
        event_name = (
            "task_succeeded"
            if reply.status is protocol.TaskReplyStatus.SUCCEEDED
            else "task_application_failed"
            if reply.status is protocol.TaskReplyStatus.APPLICATION_ERROR
            else "task_system_failed"
        )
        self._emit(
            event_name,
            task_id=str(request.spec.task_id),
            attempt_id=str(request.spec.attempt_id),
            worker_id=str(self.worker_id),
        )
        return reply

    def _resume_discovered_outputs(
        self, prepared: _PreparedOutputReply, key: Tuple[Hashable, Hashable],
    ) -> protocol.TaskReply:
        """Resume publication from exact bytes, never from user execution."""

        request = prepared.request
        if _attempt_key(request) != key or request.worker_id != self.worker_id:
            raise RuntimeError("prepared outputs belong to another Worker execution")
        # Drain also enters here directly.  A failed local owner-death cleanup
        # must resume that cleanup, never publish or complete the abandoned work.
        if key in getattr(self, "_owner_abandoned_outputs", {}):
            raise RuntimeError(
                "output owner died; accepted execution is terminally abandoned"
            )
        try:
            if prepared.failure_reply is not None:
                return self._complete_aborted_outputs(prepared, key)
            if not prepared.prepare_acked and prepared.complete_envelope is None:
                prepare = PrepareOutputPublication(
                    prepared.outputs.manifest, prepared.outputs.payload,
                )
                acknowledgement = self._prepare_output_publication(prepare)
                if not acknowledgement.accepted:
                    # A matching rejected step is NOT a no-effect proof.  The
                    # Worker chooses an explicit abort, freezes that terminal
                    # choice here, then asks the same Node Complete to prove
                    # no success won and acknowledge all compensation.
                    prepared.failure_reply = _error_reply(
                        request.spec, self.worker_id,
                        protocol.TaskReplyStatus.SYSTEM_ERROR,
                        RuntimeError(acknowledgement.error or "Node rejected output publication"),
                    )
                    return self._complete_aborted_outputs(prepared, key)
                prepared.prepare_acked = True
            # The Node ACK proves materialization and child promotions, not that these local
            # callbacks returned.  Retry cleanup even after Complete is known.
            prepared.release_promoted_sources()
            return self._complete_output_and_return(prepared, key)
        except (KeyboardInterrupt, SystemExit):
            raise
        except _OutputCompletionPending:
            raise
        except BaseException as exc:
            raise _OutputCompletionPending(
                "discovered output custody requires exact PushTask replay of publication/Complete"
            ) from exc

    def _prepare_output_publication(
        self, request: PrepareOutputPublication,
    ) -> PreparedOutputPublicationReply:
        """Retry only transport ambiguity, preserving the exact output bytes."""

        reply = self._output_rpc(PREPARE_OUTPUT_PUBLICATION_HANDLER, request)
        if type(reply) is not PreparedOutputPublicationReply:
            raise RuntimeError("Node returned an invalid output preparation reply")
        reply = replace(reply)
        if reply.request_identity != request.request_identity:
            raise RuntimeError("Node acknowledged a different output publication")
        return reply

    def _output_rpc(self, handler: str, request: object, *, attempts: int = COMPLETION_RPC_ATTEMPTS):
        """Bound publication replay to the current drain poll, if any."""

        deadline = getattr(self, "_output_drain_deadline", None)
        if deadline is not None:
            attempts = 1
        for attempt in range(attempts):
            try:
                if deadline is None:
                    return rpc_request(self.node_address, handler, request)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportError("output publication drain deadline exhausted")
                return rpc_request(
                    self.node_address, handler, request, deadline=deadline,
                    connect_timeout=remaining, request_timeout=remaining,
                )
            except TransportError:
                if attempt + 1 == attempts:
                    raise

    def _complete_aborted_outputs(
        self, prepared: _PreparedOutputReply, key: Tuple[Hashable, Hashable],
    ) -> protocol.TaskReply:
        """Release borrowed/source custody only after compensation Complete ACK."""

        reply = prepared.failure_reply
        assert reply is not None
        # Do not expose/cache TaskReply while compensation is unknown: the
        # exact failure choice remains in the same prepared custody record.
        try:
            self._ensure_completion_acked(prepared.request, reply, key)
        except _CompletionRejected:
            if self._recover_successful_output_before_abort(prepared):
                return self._resume_discovered_outputs(prepared, key)
            raise
        prepared.release_aborted_sources()
        return self._cache_complete_and_return(prepared.request, reply, key)

    def _recover_successful_output_before_abort(
        self, prepared: _PreparedOutputReply,
    ) -> bool:
        """A positive exact successful Complete overrides a proposed abort.

        A rejected failure-Complete is not proof of success.  Query the same
        Node and accept only its completed success envelope matching all
        locally retained bytes/custody.  Unknown/negative outcomes retain the
        exact abort choice and its source/import obligations.
        """

        request = prepared.request
        query = protocol.GetWorkerLeaseOutcome(
            request.lease_id, request.spec.task_id, request.spec.attempt_id,
            self.worker_id, request.spec.owner_worker_id,
            (prepared.outputs.manifest.publication_id.object_id,),
            request.spec.scheduling_key,
        )
        outcome = self._output_rpc(GET_WORKER_LEASE_OUTCOME_HANDLER, query, attempts=1)
        if type(outcome) is not protocol.GetWorkerLeaseOutcomeReply:
            raise RuntimeError("Node returned an invalid output completion outcome")
        outcome = replace(outcome)
        expected = (
            query.lease_id, query.task_id, query.attempt_id, query.executor_worker_id,
            query.owner_worker_id, query.object_ids, self.node_id,
            query.scheduling_key,
        )
        observed = (
            outcome.lease_id, outcome.task_id, outcome.attempt_id, outcome.executor_worker_id,
            outcome.owner_worker_id, outcome.object_ids, outcome.node_id,
            outcome.scheduling_key,
        )
        if expected != observed:
            raise RuntimeError("Node output completion outcome changed identity")
        if (
            not outcome.found
            or outcome.state is not protocol.LeaseExecutionState.COMPLETED
            or outcome.completion_status is not protocol.TaskReplyStatus.SUCCEEDED
        ):
            return False
        envelope = self._validated_output_envelope(
            prepared, outcome.output_publication, outcome.output_completion,
        )
        prepared.failure_reply = None
        prepared.prepare_acked = True
        prepared.complete_envelope = envelope
        self._completion_acked.add(_attempt_key(request))
        # Local custody is drained by the common success path on every resume.
        # Recording this irreversible success proof must never make a failed
        # cleanup callback disappear or send the publication back to abort.
        return True

    @staticmethod
    def _validated_output_envelope(
        prepared: _PreparedOutputReply, envelope: object, witness: object = None,
    ) -> OutputPublicationEnvelope:
        """Bind Node Complete to local once-serialized bytes, even after retirement."""

        if witness is not None:
            if envelope is not None or type(witness) is not OutputPublicationCompleteWitness:
                raise RuntimeError("Node output completion returned conflicting or invalid witnesses")
            witness = replace(witness)
            # Metadata does not manufacture bytes: this branch requires the
            # entire original Worker data-plane cache and revalidates it.
            if type(prepared.outputs) is not PreparedOutput:
                raise RuntimeError("metadata Complete requires retained local output bytes")
            outputs = replace(prepared.outputs)
            if witness != OutputPublicationCompleteWitness.for_manifest(outputs.manifest):
                raise RuntimeError("Node output completion witness changed the retained publication")
            header = outputs.manifest.header
            value = outputs.manifest.value
            result = protocol.ResultDescriptor(
                outputs.manifest.publication_id.object_id,
                value.tier, value.size_bytes, header.owner_worker_id,
                header.node_incarnation.node_id, value.checksum,
                outputs.payload if value.tier is protocol.ResultStorage.INLINE else None,
            )
            envelope = OutputPublicationEnvelope(outputs.manifest, witness, result)
        if type(envelope) is not OutputPublicationEnvelope:
            raise RuntimeError("Node successful output completion lacks its unified envelope")
        envelope = replace(envelope)
        if envelope.manifest != prepared.outputs.manifest:
            raise RuntimeError("Node output completion changed the discovered manifest")
        result = envelope.result
        if (result.storage is protocol.ResultStorage.INLINE
                and result.inline_data != prepared.outputs.payload):
            raise RuntimeError("Node output completion changed retained INLINE bytes")
        if prepared.complete_envelope is not None and envelope != prepared.complete_envelope:
            raise RuntimeError("Node output completion changed its previous Complete envelope")
        return envelope

    def _complete_output_and_return(
        self, prepared: _PreparedOutputReply, key: Tuple[Hashable, Hashable],
    ) -> protocol.TaskReply:
        """Only a validated Node Complete can create the durable success reply."""

        request = prepared.request
        envelope = self._ensure_completion_acked(
            request, None, key, status=protocol.TaskReplyStatus.SUCCEEDED,
            prepared_output=prepared,
        )
        envelope = self._validated_output_envelope(prepared, envelope)
        reply = protocol.TaskReply(
            task_id=request.spec.task_id,
            attempt_id=request.spec.attempt_id,
            worker_id=self.worker_id,
            status=protocol.TaskReplyStatus.SUCCEEDED,
            results=(envelope.result,),
            error=None,
            output_publication=envelope,
        )
        return self._cache_complete_and_return(request, reply, key)

    def _ensure_completion_acked(
        self,
        request: protocol.PushTask,
        task_reply: Optional[protocol.TaskReply],
        key: Tuple[Hashable, Hashable],
        *,
        status: Optional[protocol.TaskReplyStatus] = None,
        prepared_output: Optional[_PreparedOutputReply] = None,
    ) -> object:
        """Complete a lease once, retrying safely after ambiguous RPC loss."""

        expected_envelope = None if task_reply is None else task_reply.output_publication
        if key in self._completion_acked:
            cached = self._replies.get(key)
            if cached is not None:
                self._resolve_push_obligation(request, key)
                return cached.output_publication
            if prepared_output is not None:
                # This contains an actual Node Complete witness, with bytes
                # supplied either by Node or this Worker's retained discovery.
                return self._validated_output_envelope(
                    prepared_output, prepared_output.complete_envelope,
                )
            return None
        if self.node_address is None:
            raise RuntimeError(
                "PushTask execution requires a NodeManager endpoint"
            )
        completion = protocol.CompleteWorkerLease(
            lease_id=request.lease_id,
            task_id=request.spec.task_id,
            attempt_id=request.spec.attempt_id,
            worker_id=self.worker_id,
            status=(task_reply.status if task_reply is not None else status),
            scheduling_key=request.spec.scheduling_key,
        )
        reply = self._output_rpc(COMPLETE_WORKER_LEASE_HANDLER, completion)
        if type(reply) is not protocol.CompleteWorkerLeaseReply:
            raise RuntimeError(
                "Node returned an invalid worker-lease completion reply"
            )
        reply = replace(reply)
        reply_identity = (
            reply.lease_id,
            reply.task_id,
            reply.attempt_id,
            reply.worker_id,
            reply.status,
            reply.scheduling_key,
        )
        completion_identity = (
            completion.lease_id,
            completion.task_id,
            completion.attempt_id,
            completion.worker_id,
            completion.status,
            completion.scheduling_key,
        )
        if reply_identity != completion_identity:
            raise RuntimeError(
                "Node returned a reply for a different worker-lease completion"
            )
        # released=True is the first successful transition.  released=False is
        # the required idempotent acknowledgement when the same completion had
        # already taken effect but its earlier reply was lost.
        if (
            not reply.accepted
            or reply.state is not protocol.LeaseExecutionState.COMPLETED
        ):
            raise _CompletionRejected(
                reply.error or "Node rejected the worker-lease completion"
            )
        completed_envelope = reply.output_publication
        if prepared_output is not None:
            completed_envelope = self._validated_output_envelope(
                prepared_output, reply.output_publication, reply.output_completion,
            )
            prepared_output.complete_envelope = completed_envelope
        elif expected_envelope is not None:
            if reply.output_completion is not None:
                if reply.output_completion != expected_envelope.complete:
                    raise RuntimeError("Node completion changed the cached output witness")
                completed_envelope = expected_envelope
            elif reply.output_publication != expected_envelope:
                raise RuntimeError("Node completion changed the cached output publication")
        elif reply.output_publication is not None or reply.output_completion is not None:
            raise RuntimeError("ordinary failure completion unexpectedly returned a publication")
        self._completion_acked.add(key)
        self._resolve_push_obligation(request, key)
        return completed_envelope

    def _validate_lease_binding(self, request: protocol.PushTask) -> None:
        if request.worker_id != self.worker_id:
            raise RuntimeError("PushTask targets a different worker")

        attempt_id = request.spec.attempt_id
        previous_attempt = self._lease_bindings.get(request.lease_id)
        if previous_attempt is not None and previous_attempt != attempt_id:
            raise RuntimeError("worker lease is already bound to another attempt")
        previous_lease = self._attempt_leases.get(attempt_id)
        if previous_lease is not None and previous_lease != request.lease_id:
            raise RuntimeError("task attempt is already bound to another worker lease")

    def _commit_lease_binding(self, request: protocol.PushTask) -> None:
        """Record only a lease whose Start RPC was accepted by the Node."""

        self._lease_bindings[request.lease_id] = request.spec.attempt_id
        self._attempt_leases[request.spec.attempt_id] = request.lease_id

    def _handle_register_function(self, request: object) -> object:
        if not isinstance(request, protocol.RegisterFunction):
            raise TypeError("register_function expects RegisterFunction")
        definition = request.definition
        with self._execution_lock:
            previous = self._functions.get(definition.key)
            if previous is not None and previous != definition.payload:
                return protocol.FunctionRegistrationReply(
                    key=definition.key,
                    accepted=False,
                    error="function key is already registered with different bytes",
                )
            self._functions[definition.key] = definition.payload
        return protocol.FunctionRegistrationReply(
            key=definition.key, accepted=True
        )

    def _borrow_owner_core(self) -> Optional[CoreWorker]:
        """Return the existing logical owner without creating a new runtime."""

        lock = getattr(self, "_embedded_core_lock", None)
        if lock is None:
            return getattr(self, "_embedded_core", None)
        with lock:
            return self._embedded_core

    def _handle_output_handoff(self, method, request):
        from .output_protocol import OutputHandoffReply
        core = self._borrow_owner_core()
        if core is None:
            return OutputHandoffReply(request, False, error="object owner CoreWorker is not available")
        return getattr(core, method)(request)

    def _handle_acquire_borrowed_object(self, request: object) -> object:
        if not isinstance(request, protocol.AcquireBorrowedObject):
            raise TypeError(
                "acquire_borrowed_object expects AcquireBorrowedObject"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.AcquireBorrowedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.source, request.borrower_token, False, False,
                "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.AcquireBorrowedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.source, request.borrower_token, False, False,
                "object owner CoreWorker is not available",
            )
        return core.acquire_exported_reference(request)

    def _handle_release_borrowed_object(self, request: object) -> object:
        if not isinstance(request, protocol.ReleaseBorrowedObject):
            raise TypeError(
                "release_borrowed_object expects ReleaseBorrowedObject"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.ReleaseBorrowedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, False, False,
                "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.ReleaseBorrowedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, False, False,
                "object owner CoreWorker is not available",
            )
        return core.release_borrowed_reference(request)

    def _handle_retain_owned_object_for_task(self, request: object) -> object:
        if not isinstance(request, protocol.RetainOwnedObjectForTask):
            raise TypeError(
                "retain_owned_object_for_task expects RetainOwnedObjectForTask"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.RetainOwnedObjectForTaskReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, request.hold, False, False,
                "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.RetainOwnedObjectForTaskReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, request.hold, False, False,
                "object owner CoreWorker is not available",
            )
        return core.retain_owned_object_for_task(request)

    def _handle_get_retained_owned_object(self, request: object) -> object:
        if not isinstance(request, protocol.GetRetainedOwnedObject):
            raise TypeError(
                "get_retained_owned_object expects GetRetainedOwnedObject"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.GetRetainedOwnedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.hold, False,
                detail="request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.GetRetainedOwnedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.hold, False,
                detail="object owner CoreWorker is not available",
            )
        return core.get_retained_owned_object(request)

    def _handle_release_owned_object_for_task(self, request: object) -> object:
        if not isinstance(request, protocol.ReleaseOwnedObjectForTask):
            raise TypeError(
                "release_owned_object_for_task expects ReleaseOwnedObjectForTask"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.ReleaseOwnedObjectForTaskReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.hold, False, False,
                "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.ReleaseOwnedObjectForTaskReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.hold, False, False,
                "object owner CoreWorker is not available",
            )
        return core.release_owned_object_for_task(request)

    def _handle_replace_retained_object_for_task(
        self, request: object
    ) -> object:
        if not isinstance(request, protocol.ReplaceRetainedObjectForTask):
            raise TypeError(
                "replace_retained_object_for_task expects "
                "ReplaceRetainedObjectForTask"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.ReplaceRetainedObjectForTaskReply(
                request.object_id, request.owner_worker_id,
                request.borrower_worker_id, request.expected_hold,
                request.replacement_hold,
                protocol.ReplaceRetainedObjectDisposition.FAILED,
                failure=protocol.ReplaceRetainedObjectFailure.WRONG_OWNER,
                detail="request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.ReplaceRetainedObjectForTaskReply(
                request.object_id, request.owner_worker_id,
                request.borrower_worker_id, request.expected_hold,
                request.replacement_hold,
                protocol.ReplaceRetainedObjectDisposition.FAILED,
                failure=protocol.ReplaceRetainedObjectFailure.OWNER_STOPPED,
                detail="object owner CoreWorker is not available",
            )
        return core.replace_retained_object_for_task(request)

    def _handle_prepare_stored_contained_pin(
        self, request: object
    ) -> protocol.StoredContainedPinReply:
        if not isinstance(request, protocol.PrepareStoredContainedPin):
            raise TypeError(
                "prepare_stored_contained_pin expects "
                "PrepareStoredContainedPin"
            )
        return self._handle_stored_contained_pin(request, prepare=True)

    def _handle_promote_stored_contained_pin(
        self, request: object
    ) -> protocol.StoredContainedPinReply:
        if not isinstance(request, protocol.PromoteStoredContainedPin):
            raise TypeError(
                "promote_stored_contained_pin expects "
                "PromoteStoredContainedPin"
            )
        return self._handle_stored_contained_pin(request, prepare=False)

    def _handle_stored_contained_pin(
        self, request: protocol.StoredContainedPinRequest, *, prepare: bool
    ) -> protocol.StoredContainedPinReply:
        if request.authority_worker_id != self.worker_id:
            return protocol.StoredContainedPinReply(
                request,
                error_kind=protocol.StoredPublicationRPCErrorKind.INVALID_REQUEST,
                error="request targets a different child owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.StoredContainedPinReply(
                request,
                error_kind=protocol.StoredPublicationRPCErrorKind.UNAVAILABLE,
                error="OWNER_STOPPED: object owner CoreWorker is not available",
            )
        operation = (
            core.prepare_stored_contained_pin
            if prepare else core.promote_stored_contained_pin
        )
        return operation(request)

    def _handle_report_retained_object_location(
        self, request: object
    ) -> object:
        if not isinstance(request, protocol.ReportRetainedObjectLocation):
            raise TypeError(
                "report_retained_object_location expects "
                "ReportRetainedObjectLocation"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.ReportRetainedObjectLocationReply(
                request.object_id, request.owner_worker_id,
                request.borrower_worker_id,
                request.hold, request.descriptor,
                protocol.RetainedLocationReportStatus.REJECTED,
                "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.ReportRetainedObjectLocationReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.hold, request.descriptor,
                protocol.RetainedLocationReportStatus.REJECTED,
                "object owner CoreWorker is not available",
            )
        return core.report_retained_object_location(request)

    def _handle_report_abandoned_dependency_replica(self, request: object) -> object:
        if type(request) is not protocol.ReportAbandonedDependencyReplica:
            raise TypeError("abandoned replica report requires its exact typed request")
        request = replace(request)
        if request.descriptor.owner_worker_id != self.worker_id:
            return protocol.ReportAbandonedDependencyReplicaReply(
                request, protocol.RetainedLocationReportStatus.REJECTED, "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None or not callable(getattr(core, "report_abandoned_dependency_replica", None)):
            return protocol.ReportAbandonedDependencyReplicaReply(
                request, protocol.RetainedLocationReportStatus.REJECTED, "object owner CoreWorker is not available",
            )
        if getattr(core, "worker_id", None) != self.worker_id:
            return protocol.ReportAbandonedDependencyReplicaReply(
                request, protocol.RetainedLocationReportStatus.REJECTED, "existing CoreWorker has a different owner identity",
            )
        return core.report_abandoned_dependency_replica(request)

    def _handle_release_contained_reference(self, request: object) -> object:
        if not isinstance(request, protocol.ReleaseContainedReference):
            raise TypeError(
                "release_contained_reference expects ReleaseContainedReference"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.ReleaseContainedReferenceReply(
                request.object_id, self.worker_id, request.hold,
                False, False, "request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.ReleaseContainedReferenceReply(
                request.object_id, self.worker_id, request.hold,
                False, False, "object owner CoreWorker is not available",
            )
        return core.release_contained_reference(request)

    def _handle_get_owned_object(self, request: object) -> object:
        if not isinstance(request, protocol.GetOwnedObject):
            raise TypeError("get_owned_object expects GetOwnedObject")
        if request.owner_worker_id != self.worker_id:
            return protocol.GetOwnedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, False,
                detail="request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.GetOwnedObjectReply(
                request.object_id, self.worker_id, request.borrower_worker_id,
                request.borrower_token, False,
                detail="object owner CoreWorker is not available",
            )
        return core.get_owned_object(request)

    def _handle_request_owned_object_reconstruction(
        self, request: object
    ) -> object:
        if not isinstance(
            request, protocol.RequestOwnedObjectReconstruction
        ):
            raise TypeError(
                "request_owned_object_reconstruction expects "
                "RequestOwnedObjectReconstruction"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.RequestOwnedObjectReconstructionReply(
                request.object_id, request.owner_worker_id,
                request.requester_worker_id, request.credential,
                request.borrower_token, request.expected_owner_attempt,
                protocol.OwnedObjectReconstructionDisposition.FAILED,
                failure=(
                    protocol.OwnedObjectReconstructionFailure.WRONG_OWNER
                ),
                detail="request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.RequestOwnedObjectReconstructionReply(
                request.object_id, request.owner_worker_id,
                request.requester_worker_id, request.credential,
                request.borrower_token, request.expected_owner_attempt,
                protocol.OwnedObjectReconstructionDisposition.FAILED,
                failure=(
                    protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED
                ),
                detail="object owner CoreWorker is not available",
            )
        return core.request_owned_object_reconstruction(request)

    def _handle_request_drop_owned_object(self, request: object) -> object:
        if not isinstance(request, protocol.RequestDropOwnedObject):
            raise TypeError(
                "request_drop_owned_object expects RequestDropOwnedObject"
            )
        if request.owner_worker_id != self.worker_id:
            return protocol.RequestDropOwnedObjectReply(
                request.operation_id, request.object_id,
                request.owner_worker_id, request.requester_worker_id,
                request.source, request.borrower_token,
                request.expected_owner_attempt, request.node_id,
                protocol.DropOwnedObjectDisposition.FAILED,
                failure=protocol.DropOwnedObjectFailure.WRONG_OWNER,
                detail="request targets a different object owner",
            )
        core = self._borrow_owner_core()
        if core is None:
            return protocol.RequestDropOwnedObjectReply(
                request.operation_id, request.object_id,
                request.owner_worker_id, request.requester_worker_id,
                request.source, request.borrower_token,
                request.expected_owner_attempt, request.node_id,
                protocol.DropOwnedObjectDisposition.FAILED,
                failure=protocol.DropOwnedObjectFailure.OWNER_STOPPED,
                detail="object owner CoreWorker is not available",
            )
        return core.request_drop_owned_object(request)


    def _handle_finalize_output_owner_death(self, request):
        """Retire accepted local custody after Node's exact owner-death cleanup."""
        if type(request) is not FinalizeOutputOwnerDeath:
            raise TypeError("output owner cleanup requires exact metadata")
        request = replace(request)
        manifest = request.manifest
        if manifest.header.executor_worker_id != self.worker_id:
            raise RuntimeError("output cleanup targets another executor")
        if manifest.header.node_incarnation.node_id != self.node_id:
            raise RuntimeError("output cleanup targets another publishing Node")
        identity = manifest.publication_id
        key = identity.attempt_id, identity.lease_id
        if not self._execution_lock.acquire(blocking=False):
            return FinalizeOutputOwnerDeathReply(request, False)
        try:
            tombstones = getattr(self, "_owner_abandoned_outputs", {})
            previous = tombstones.get(key)
            if previous is not None and previous != request:
                raise RuntimeError("output cleanup changed frozen owner-death request")
            bound_attempt = self._lease_bindings.get(identity.lease_id)
            bound_lease = self._attempt_leases.get(identity.attempt_id)
            if ((bound_attempt is not None and bound_attempt != identity.attempt_id)
                    or (bound_lease is not None and bound_lease != identity.lease_id)):
                raise RuntimeError("output cleanup changed accepted lease binding")

            # Validate every retained identity before any custody callback.
            # Reply caching can have taken effect before a local exception, so
            # pending discovery and a cached reply may coexist.
            pending = getattr(self, "_prepared_output_replies", {}).get(key)
            cached = self._replies.get(key)
            retained = [getattr(self, "_cached_output_manifests", {}).get(key)]
            if pending is not None:
                retained.append(pending.outputs.manifest)
                if pending.complete_envelope is not None:
                    retained.append(pending.complete_envelope.manifest)
            if cached is not None and cached.output_publication is not None:
                retained.append(cached.output_publication.manifest)
            if any(value is not None and value != manifest for value in retained):
                raise RuntimeError("output cleanup changed retained publication manifest")
            if previous is None and not any(value is not None for value in retained):
                raise RuntimeError("output cleanup has no retained publication")
            pushes = (
                None if pending is None else pending.request,
                self._cached_pushes.get(key),
                getattr(self, "_accepted_pushes", {}).get(key),
            )
            first_push = next((push for push in pushes if push is not None), None)
            if any(push is not None and push != first_push for push in pushes):
                raise RuntimeError("output cleanup changed retained PushTask identity")
            for push in pushes:
                if push is None:
                    continue
                execution = TaskExecution.from_task_spec(push.spec)
                if (_attempt_key(push) != key or push.worker_id != self.worker_id
                        or push.spec.job_id != manifest.header.job_id
                        or push.spec.owner_worker_id != manifest.header.owner_worker_id
                        or execution != manifest.execution):
                    raise RuntimeError("output cleanup changed retained PushTask identity")
            if cached is not None and (
                cached.task_id != identity.task_id
                or cached.attempt_id != identity.attempt_id
                or cached.worker_id != self.worker_id
            ):
                raise RuntimeError("output cleanup changed cached reply identity")

            # Freeze exact metadata before callbacks: an effect-then-error
            # retains custody for Finalize replay, but permanently fences Push
            # and drain publication.  No bytes are retained by this tombstone.
            if previous is None:
                tombstones[key] = request
                self._owner_abandoned_outputs = tombstones
            if pending is not None:
                pending.release_aborted_sources()
                self._prepared_output_replies.pop(key, None)
            self._replies.pop(key, None)
            self._cached_pushes.pop(key, None)
            getattr(self, "_cached_output_manifests", {}).pop(key, None)
            self._completion_acked.discard(key)
            condition = getattr(self, "_lifecycle", None)
            with condition if condition is not None else nullcontext():
                getattr(self, "_accepted_pushes", {}).pop(key, None)
                getattr(self, "_push_obligations", set()).discard(key)
                if condition is not None:
                    condition.notify_all()
            return FinalizeOutputOwnerDeathReply(request, True)
        finally:
            self._execution_lock.release()

    def _progress_prepared_output_drain(self, deadline: float) -> bool:
        """Resume at most one accepted byte cache, without new task admission."""

        execution_lock = getattr(self, "_execution_lock", None)
        if execution_lock is None or not execution_lock.acquire(blocking=False):
            return False
        previous = getattr(self, "_output_drain_deadline", None)
        try:
            if time.monotonic() >= deadline:
                return False
            pending = getattr(self, "_prepared_output_replies", {})
            if not pending:
                return False
            key, prepared = next(iter(pending.items()))
            accepted = getattr(self, "_accepted_pushes", {}).get(key)
            if accepted is not None and accepted != prepared.request:
                return False
            self._output_drain_deadline = deadline
            try:
                # The execution lock fences concurrent PushTask replay.  No
                # _begin_task, StartLease, function decode or discovery occurs.
                self._resume_discovered_outputs(prepared, key)
            except Exception:
                # Retained bytes and exact local choices remain the next poll's
                # work.  A timeout is never evidence that Complete was absent.
                pass
            return True
        finally:
            self._output_drain_deadline = previous
            execution_lock.release()

    def _drain_status(
        self, request_id: str, *, timeout: Optional[float] = None
    ) -> protocol.DrainStatus:
        previous = getattr(self, "_drain_request_id", None)
        if previous != request_id:
            return protocol.DrainStatus(
                request_id, "worker:{}".format(self.worker_id), False, False,
                detail="worker has not begun this drain epoch",
            )
        timeout = getattr(
            self, "_request_timeout", EMBEDDED_CORE_STOP_TIMEOUT_SECONDS
        ) if timeout is None else timeout
        deadline = time.monotonic() + min(
            float(timeout), EMBEDDED_CORE_STOP_TIMEOUT_SECONDS
        )
        self._progress_prepared_output_drain(deadline)
        condition = getattr(self, "_lifecycle", None)

        def tasks_drained():
            return (getattr(self, "_active_tasks", 0) == 0
                    and not getattr(self, "_push_obligations", set())
                    and not getattr(self, "_prepared_output_replies", {}))

        drained = tasks_drained()
        if condition is not None:
            with condition:
                # Inactive retained work has no background producer to wait
                # for; return unclean so the coordinator can poll it again.
                if not tasks_drained() and self._active_tasks == 0:
                    drained = False
                else:
                    drained = condition.wait_for(
                        tasks_drained, timeout=max(0.0, deadline - time.monotonic()),
                    )
        # An executing parent may still call ``remote`` or ``get``.  Its Core is
        # part of that admitted task and cannot be closed until the task drains.
        core_stopped = drained and self._shutdown_embedded_core(
            max(0.001, deadline - time.monotonic()),
            preserve_owner_protocol=True,
        )
        clean = drained and core_stopped
        self._drain_clean = clean
        return protocol.DrainStatus(
            request_id=request_id,
            component="worker:{}".format(self.worker_id),
            drain_started=True,
            clean=clean,
            detail=(
                "worker task and child-submission Core drained; owner service live"
                if clean
                else "worker drain has unresolved task, lease, or owner work"
            ),
        )

    def _handle_begin_drain(self, request: object) -> object:
        if not isinstance(request, protocol.BeginDrain):
            raise TypeError("begin_drain expects BeginDrain")
        condition = getattr(self, "_lifecycle", None)
        if condition is None:
            self._drain_request_id = request.request_id
        else:
            with condition:
                previous = getattr(self, "_drain_request_id", None)
                if previous is not None and previous != request.request_id:
                    raise ValueError(
                        "worker drain already has a different request ID"
                    )
                self._drain_request_id = request.request_id
                self._accepting_tasks = False
                self._owner_retain_admission_open = False
                core = self._borrow_owner_core()
                if core is not None:
                    close_retain = getattr(
                        core, "close_owner_retain_admission", None
                    )
                    if close_retain is not None:
                        close_retain()
                condition.notify_all()
        return protocol.DrainStatus(
            request.request_id,
            "worker:{}".format(self.worker_id),
            drain_started=True,
            clean=False,
            detail="worker drain fence installed; status polling drives cleanup",
        )

    def _handle_drain_status(self, request: object) -> object:
        if not isinstance(request, protocol.BeginDrain):
            raise TypeError("drain_status expects BeginDrain")
        return self._drain_status(request.request_id)

    def _handle_finalize_shutdown(self, request: object) -> object:
        if not isinstance(request, protocol.FinalizeShutdown):
            raise TypeError("finalize_shutdown expects FinalizeShutdown")
        if getattr(self, "_drain_request_id", None) != request.request_id:
            raise ValueError("worker has not begun this drain epoch")
        # The cluster coordinator calls finalize only after observing a clean
        # DrainStatus barrier.  Re-running Core.shutdown here is redundant and
        # breaks adapters whose drain operation is intentionally stateful.
        if not getattr(self, "_drain_clean", False):
            return protocol.ShutdownAck(
                request.request_id, "worker:{}".format(self.worker_id), False,
                detail="worker cannot finalize before a clean drain",
            )
        core = self._borrow_owner_core()
        if core is not None:
            finalize = getattr(core, "finalize_shutdown", None)
            if finalize is not None and not finalize(
                require_distributed_clean=True,
                timeout=min(0.1, getattr(self, "_request_timeout", EMBEDDED_CORE_STOP_TIMEOUT_SECONDS)),
            ):
                return protocol.ShutdownAck(
                    request.request_id, "worker:{}".format(self.worker_id), False,
                    detail="embedded Core owner service is not finalizable",
                )
        self._schedule_finalize_exit()
        return protocol.ShutdownAck(
            request.request_id, "worker:{}".format(self.worker_id), True,
            detail="worker finalized after the cluster barrier",
        )

    def _schedule_finalize_exit(self) -> None:
        """Wake worker_main only after the current TCP ACK was sent."""

        condition = getattr(self, "_lifecycle", None)
        if condition is not None:
            with condition:
                if getattr(self, "_finalize_exit_scheduled", False):
                    return
                self._finalize_exit_scheduled = True
        handler_thread = threading.current_thread()
        if handler_thread.daemon:
            threading.Thread(
                target=self._release_wait_after_finalize_handler,
                args=(handler_thread,),
                name="miniray-worker-finalize",
                daemon=False,
            ).start()
        else:
            self._stop_event.set()

    def _release_wait_after_finalize_handler(
        self, handler_thread: threading.Thread
    ) -> None:
        handler_thread.join()
        self._stop_event.set()

    def _handle_shutdown(self, request: object) -> object:
        """Legacy single-component shutdown used outside cluster teardown."""

        if not isinstance(request, protocol.Shutdown):
            raise TypeError("shutdown expects Shutdown")
        drain_id = getattr(self, "_drain_request_id", None) or request.request_id
        # Legacy direct shutdown preserves the original one-call Core contract.
        # Cluster teardown uses the typed Begin/Status/Finalize handlers above.
        condition = getattr(self, "_lifecycle", None)
        if condition is not None:
            with condition:
                self._drain_request_id = drain_id
                self._accepting_tasks = False
                self._owner_retain_admission_open = False
                # Match the typed drain fence on the already-created owner Core.
                # Do not construct or replace a Core here: an in-flight parent
                # still owns this instance, whose exact retain replays, queries,
                # and releases must remain available while task drain times out.
                core = self._borrow_owner_core()
                if core is not None:
                    close_retain = getattr(
                        core, "close_owner_retain_admission", None
                    )
                    if close_retain is not None:
                        close_retain()
                condition.notify_all()
                drained = condition.wait_for(
                    lambda: self._active_tasks == 0
                    and not getattr(self, "_push_obligations", set())
                    and not getattr(
                        self, "_prepared_output_replies", {}
                    ),
                    timeout=getattr(
                        self, "_request_timeout",
                        EMBEDDED_CORE_STOP_TIMEOUT_SECONDS,
                    ),
                )
        else:
            drained = True
        core_stopped = drained and self._shutdown_embedded_core(
            getattr(
                self, "_request_timeout", EMBEDDED_CORE_STOP_TIMEOUT_SECONDS
            ),
            preserve_owner_protocol=False,
        )
        status = protocol.DrainStatus(
            drain_id,
            "worker:{}".format(self.worker_id),
            True,
            drained and core_stopped,
            detail=(
                "worker task and Core drained"
                if drained and core_stopped
                else "worker shutdown drain timed out"
            ),
        )
        if status.clean:
            self._schedule_finalize_exit()
            return protocol.ShutdownAck(
                request.request_id,
                "worker:{}".format(self.worker_id),
                True,
                detail="worker stopped after legacy direct shutdown",
            )
        return protocol.ShutdownAck(
            request.request_id, "worker:{}".format(self.worker_id), False,
            detail=status.detail,
        )


def worker_main(
    worker_id: ids.WorkerID,
    ready_connection: Optional[Connection] = None,
    host: str = LOOPBACK_HOST,
    port: int = 0,
    node_id: Optional[ids.NodeID] = None,
    node_address: Optional[Address] = None,
    inline_threshold: int = DEFAULT_INLINE_THRESHOLD_BYTES,
    failpoint: Optional[WorkerFailpointConfig] = None,
    trace_config: Optional[TraceSinkConfig] = None,
    gcs_address: Optional[Address] = None,
) -> None:
    """Spawn-safe worker process entry point.

    When supplied, ``ready_connection`` receives ``(True, address)`` after the
    socket is listening, or ``(False, error_text)`` if startup fails.
    """

    server: Optional[WorkerServer] = None
    trace_sink = sink_from_config(trace_config)
    try:
        server = WorkerServer(
            worker_id,
            node_id=node_id,
            host=host,
            port=port,
            node_address=node_address,
            inline_threshold=inline_threshold,
            failpoint=failpoint,
            event_sink=trace_sink,
            gcs_address=gcs_address,
        )
        address = server.start()
        trace_sink.emit(
            "process_ready", component="worker",
            worker_id=str(worker_id), node_id=str(server.node_id)
        )
        if ready_connection is not None:
            ready_connection.send((True, address))
            ready_connection.close()
            ready_connection = None
        server.wait()
    except BaseException:
        if ready_connection is not None:
            try:
                ready_connection.send((False, traceback.format_exc()))
            finally:
                ready_connection.close()
        raise
    finally:
        trace_sink.emit(
            "process_stopping", component="worker", worker_id=str(worker_id)
        )
        if server is not None:
            server.stop()
        trace_sink.close()


__all__ = [
    "ACQUIRE_BORROWED_OBJECT_HANDLER",
    "BEGIN_DRAIN_HANDLER",
    "DRAIN_STATUS_HANDLER",
    "FINALIZE_SHUTDOWN_HANDLER",
    "GET_OWNED_OBJECT_HANDLER",
    "GET_RETAINED_OWNED_OBJECT_HANDLER",
    "REPORT_RETAINED_OBJECT_LOCATION_HANDLER",
    "PUSH_TASK_HANDLER",
    "PREPARE_STORED_CONTAINED_PIN_HANDLER",
    "PROMOTE_STORED_CONTAINED_PIN_HANDLER",
    "REGISTER_FUNCTION_HANDLER",
    "RELEASE_BORROWED_OBJECT_HANDLER",
    "RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER",
    "REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER",
    "RELEASE_CONTAINED_REFERENCE_HANDLER",
    "REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER",
    "REQUEST_DROP_OWNED_OBJECT_HANDLER",
    "SHUTDOWN_HANDLER",
    "RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER",
    "CRASH_AFTER_NESTED_IMPORT_EXIT_CODE",
    "WorkerServer",
    "WorkerFailpointConfig",
    "WorkerFailpointMode",
    "worker_main",
]
