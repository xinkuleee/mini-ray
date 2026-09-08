"""Pure Worker contracts for attempt-scoped nested ObjectRef imports.

No listener or child process is created.  The embedded Core and Node RPCs are
small in-memory fakes so these tests isolate the Worker transaction boundary.
"""

from __future__ import annotations

import hashlib
import threading
from contextlib import nullcontext
from dataclasses import dataclass, replace
from typing import Callable

import cloudpickle
import pytest

import miniray.worker as worker_module
from miniray import output_protocol as wire, protocol
from miniray.dependency import (
    ContainedRef, NestedReferenceImportSession, encode_task_argument,
)
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.trace import EventSink
from miniray.transport import TransportTimeout
from miniray.worker import (
    CRASH_AFTER_NESTED_IMPORT_EXIT_CODE,
    COMPLETE_WORKER_LEASE_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerFailpointConfig,
    WorkerFailpointMode,
    WorkerServer,
)
from tests.unit._unified_worker_rpc import UnifiedWorkerRPC
from tests.unit.test_worker_unified_output import _install_no_runtime


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    _install_no_runtime(monkeypatch)


@dataclass(frozen=True)
class _TaskIdentity:
    job_id: JobID
    task_id: TaskID
    attempt_id: AttemptID
    task_owner_id: WorkerID


class _ImportedHandle:
    def __init__(self, label: str, events: list[object]) -> None:
        self.label = label
        self._events = events
        self.closed = False

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._events.append(("close", self.label))


def _identity(attempt_number: int = 0) -> _TaskIdentity:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return _TaskIdentity(
        job_id, task_id, AttemptID(task_id, attempt_number), WorkerID.random()
    )


def _transfer(
    identity: _TaskIdentity, label: str, index: int
) -> protocol.NestedReferenceTransfer:
    owner_id = WorkerID.random()
    object_task = TaskID.derive(
        identity.job_id, TaskID.for_driver(identity.job_id), index + 1
    )
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED,
        identity.task_owner_id,
        identity.task_id,
        AttemptID(identity.task_id, 0),
    )
    return protocol.NestedReferenceTransfer(
        ObjectID.for_task(object_task),
        owner_id,
        ("127.0.0.1", 24000 + index),
        hold,
    )


def _inline(
    value: object,
    transfers: dict[tuple[object, object], protocol.NestedReferenceTransfer],
) -> protocol.InlineArg:
    argument = encode_task_argument(
        value,
        serializer="cloudpickle",
        export_nested_ref=lambda reference: transfers[
            (reference.object_id, reference.owner_worker_id)
        ],
    )
    assert isinstance(argument, protocol.InlineArg)
    return argument


def _stored(
    identity: _TaskIdentity, argument: protocol.InlineArg,
    node_id: NodeID, *, index: int = 0,
) -> tuple[protocol.StoredArg, protocol.ObjectStoreDescriptor]:
    storage_task = TaskID.for_put(
        identity.job_id, identity.task_owner_id, index
    )
    storage_id = ObjectID.for_task(storage_task)
    return (
        protocol.StoredArg(
            storage_id, identity.task_owner_id,
            argument.serializer, argument.nested_refs,
        ),
        protocol.ObjectStoreDescriptor(
            storage_id, identity.task_owner_id, AttemptID(storage_task, 0),
            node_id, len(argument.data),
            hashlib.sha256(argument.data).hexdigest(),
        ),
    )


def _push(
    worker_id: WorkerID,
    identity: _TaskIdentity,
    *,
    args: tuple[protocol.TaskArg, ...],
    kwargs: tuple[tuple[str, protocol.TaskArg], ...] = (),
    dependencies: tuple[protocol.ObjectStoreDescriptor, ...] = (),
) -> tuple[protocol.PushTask, bytes]:
    function_payload = b"nested-task-argument-function"
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(
            identity.job_id, __name__, "nested_argument_task", "v1"
        ),
        function_payload,
    )
    spec = protocol.TaskSpec(
        job_id=identity.job_id,
        task_id=identity.task_id,
        attempt_id=identity.attempt_id,
        function=definition.key,
        args=args,
        kwargs=kwargs,
        num_returns=1,
        resources=ResourceVector({"CPU": 1}),
        owner_worker_id=identity.task_owner_id,
        function_definition=definition,
    )
    return protocol.PushTask(
        LeaseID.random(), worker_id, spec, dependencies
    ), function_payload


def _worker() -> WorkerServer:
    worker = object.__new__(WorkerServer)
    worker.worker_id = WorkerID.random()
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 23998)
    worker.gcs_address = ("127.0.0.1", 23999)
    worker.inline_threshold = 1024
    worker._request_timeout = 0.01
    worker._failpoint = None
    worker._failpoint_triggers = 0
    worker._crash_after_complete = set()
    worker.event_sink = EventSink()
    worker._stop_event = threading.Event()
    worker._execution_lock = threading.Lock()
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepting_tasks = True
    worker._active_tasks = 0
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core = None
    worker._embedded_core_job_id = None
    worker._embedded_core_stopped = False
    worker._owner_retain_admission_open = True
    worker._worker_core_enabled = False
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    return worker


def _start_reply(
    message: protocol.StartWorkerLease, node_id: NodeID,
) -> protocol.StartWorkerLeaseReply:
    return protocol.StartWorkerLeaseReply(
        message.lease_id, protocol.LeaseExecutionState.RUNNING, accepted=True,
        node_incarnation=OutputPublicationNodeIncarnation(node_id, 21001, 3),
    )


def _completion_reply(
    message: protocol.CompleteWorkerLease,
) -> protocol.CompleteWorkerLeaseReply:
    return protocol.CompleteWorkerLeaseReply(
        message.lease_id,
        message.task_id,
        message.attempt_id,
        message.worker_id,
        message.status,
        protocol.LeaseExecutionState.COMPLETED,
        accepted=True,
        released=True,
    )


def _install_runtime_fakes(
    monkeypatch: pytest.MonkeyPatch,
    worker: WorkerServer,
    function_payload: bytes,
    function: Callable[..., object],
    restore: Callable[[protocol.NestedReferenceTransfer, AttemptID], object],
    events: list[object],
    *,
    completion: Callable[[protocol.CompleteWorkerLease], object] | None = None,
    preparation: Callable[[wire.PrepareOutputPublication], object] | None = None,
    stored_payloads: dict[ObjectID, bytes] | None = None,
) -> None:
    publication = UnifiedWorkerRPC()

    class _Core:
        def _restore_task_argument_reference(
            self, transfer: protocol.NestedReferenceTransfer, attempt_id: AttemptID
        ) -> object:
            return restore(transfer, attempt_id)

    core = _Core()

    def embedded_core(job_id: JobID) -> object:
        assert isinstance(job_id, JobID)
        events.append("embedded_core")
        return core

    original_loads = cloudpickle.loads

    def loads(payload: bytes) -> object:
        if payload == function_payload:
            events.append("function_decode")
            return function
        return original_loads(payload)

    def rpc(_address: object, handler: str, message: object) -> object:
        if handler == START_WORKER_LEASE_HANDLER:
            events.append("start")
            assert isinstance(message, protocol.StartWorkerLease)
            return _start_reply(message, worker.node_id)
        if handler == worker_module.GET_OBJECT_HANDLER:
            assert isinstance(message, protocol.GetObject)
            assert stored_payloads is not None
            data = stored_payloads[message.object_id]
            events.append(("get_object", message.object_id))
            return protocol.GetObjectReply(
                message.object_id, worker.node_id, True, True, data,
                hashlib.sha256(data).hexdigest(),
                producer_attempt_id=message.expected_attempt_id,
                owner_worker_id=message.expected_owner_worker_id,
                size_bytes=len(data),
            )
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            events.append("prepare")
            return publication.prepare(message) if preparation is None else preparation(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        events.append("complete")
        assert isinstance(message, protocol.CompleteWorkerLease)
        reply = publication.complete(message) if completion is None else completion(message)
        if (completion is not None and type(reply) is protocol.CompleteWorkerLeaseReply
                and reply.accepted and reply.status is protocol.TaskReplyStatus.SUCCEEDED):
            reply = replace(reply, output_publication=publication.envelope(message))
        return reply

    monkeypatch.setattr(worker, "_embedded_core_for", embedded_core)
    monkeypatch.setattr(worker_module.cloudpickle, "loads", loads)
    monkeypatch.setattr(worker_module, "rpc_request", rpc)


@pytest.mark.parametrize(
    "storage_layout", ["inline", "positional", "keyword", "both"]
)
def test_one_import_session_spans_args_and_kwargs_and_deduplicates(
    monkeypatch: pytest.MonkeyPatch,
    storage_layout: str,
) -> None:
    worker = _worker()
    identity = _identity()
    transfer = _transfer(identity, "shared", 0)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    transfers = {(reference.object_id, reference.owner_worker_id): transfer}
    positional = _inline({"items": [reference, reference]}, transfers)
    keyword = _inline({"again": reference}, transfers)
    stored_payloads: dict[ObjectID, bytes] = {}
    dependencies: list[protocol.ObjectStoreDescriptor] = []
    if storage_layout in {"positional", "both"}:
        stored, descriptor = _stored(identity, positional, worker.node_id)
        stored_payloads[stored.object_id] = positional.data
        dependencies.append(descriptor)
        positional = stored
    if storage_layout in {"keyword", "both"}:
        stored, descriptor = _stored(
            identity, keyword, worker.node_id, index=1
        )
        stored_payloads[stored.object_id] = keyword.data
        dependencies.append(descriptor)
        keyword = stored
    push, function_payload = _push(
        worker.worker_id, identity, args=(positional,),
        kwargs=(("named", keyword),), dependencies=tuple(dependencies),
    )
    events: list[object] = []
    acquired: list[tuple[protocol.NestedReferenceTransfer, AttemptID]] = []
    observed: list[bool] = []

    def restore(
        item: protocol.NestedReferenceTransfer, attempt_id: AttemptID
    ) -> _ImportedHandle:
        events.append(("acquire", "shared"))
        acquired.append((item, attempt_id))
        return _ImportedHandle("shared", events)

    def function(value: object, *, named: object) -> int:
        first, second = value["items"]
        third = named["again"]
        observed.append(first is second is third)
        events.append("user")
        return 7

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function, restore, events,
        stored_payloads=stored_payloads,
    )

    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert observed == [True]
    assert acquired == [(transfer, identity.attempt_id)]
    assert events.count(("acquire", "shared")) == 1
    assert events.count(("close", "shared")) == 1
    assert events.index("embedded_core") < events.index("function_decode")
    assert events.index("embedded_core") < events.index(("acquire", "shared"))
    assert events.index("user") < events.index(("close", "shared"))
    assert events.index(("close", "shared")) < events.index("complete")


@pytest.mark.parametrize("stored_argument", [False, True])
def test_single_call_shaped_value_stays_one_argument_and_decodes_once(
    monkeypatch: pytest.MonkeyPatch, stored_argument: bool,
) -> None:
    worker = _worker()
    identity = _identity()
    value = ((1,), {"named": 2})
    argument = _inline(value, {})
    stored_payloads: dict[ObjectID, bytes] = {}
    dependencies: tuple[protocol.ObjectStoreDescriptor, ...] = ()
    if stored_argument:
        stored, descriptor = _stored(identity, argument, worker.node_id)
        stored_payloads[stored.object_id] = argument.data
        argument = stored
        dependencies = (descriptor,)
    push, function_payload = _push(
        worker.worker_id, identity, args=(argument,), dependencies=dependencies,
    )
    events: list[object] = []
    observed: list[object] = []
    decodes: list[protocol.InlineArg] = []
    original_decode = worker_module.decode_inline_argument

    def decode(argument: protocol.InlineArg, **kwargs: object) -> object:
        decodes.append(argument)
        return original_decode(argument, **kwargs)

    def function(single_value: object) -> int:
        observed.append(single_value)
        return 1

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function,
        lambda *_args: pytest.fail("plain value must not import ObjectRefs"),
        events, stored_payloads=stored_payloads,
    )
    monkeypatch.setattr(worker_module, "decode_inline_argument", decode)
    reply = worker._handle_push_task(push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert observed == [value]
    assert len(decodes) == 1


def test_inline_and_stored_arguments_share_rollback_on_later_decode_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    identity = _identity()
    transfers = tuple(
        _transfer(identity, label, index)
        for index, label in enumerate(("inline", "stored"))
    )
    references = tuple(
        ContainedRef(item.object_id, item.owner_worker_id) for item in transfers
    )
    by_identity = {
        (reference.object_id, reference.owner_worker_id): transfer
        for reference, transfer in zip(references, transfers)
    }
    first = _inline({"nested": references[0]}, by_identity)
    stored_stream = _inline({"nested": references[1]}, by_identity)
    stored, descriptor = _stored(identity, stored_stream, worker.node_id)
    push, function_payload = _push(
        worker.worker_id, identity, args=(first, stored),
        kwargs=(("invalid", protocol.InlineArg(b"not a serialized value")),),
        dependencies=(descriptor,),
    )
    events: list[object] = []

    def restore(
        item: protocol.NestedReferenceTransfer, attempt_id: AttemptID,
    ) -> _ImportedHandle:
        assert attempt_id == identity.attempt_id
        label = "inline" if item == transfers[0] else "stored"
        events.append(("acquire", label))
        return _ImportedHandle(label, events)

    _install_runtime_fakes(
        monkeypatch, worker, function_payload,
        lambda *_args, **_kwargs: pytest.fail("invalid call reached user code"),
        restore, events, stored_payloads={stored.object_id: stored_stream.data},
    )
    reply = worker._handle_push_task(push)
    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert [
        event for event in events
        if isinstance(event, tuple) and event[0] in {"acquire", "close"}
    ] == [
        ("acquire", "inline"), ("acquire", "stored"),
        ("close", "stored"), ("close", "inline"),
    ]
    assert events.index(("close", "inline")) < events.index("complete")


@pytest.mark.parametrize(
    "corruption", ["checksum", "pickle", "unused_manifest"]
)
def test_stored_stream_failure_rolls_back_previously_imported_inline_handle(
    monkeypatch: pytest.MonkeyPatch, corruption: str,
) -> None:
    worker = _worker()
    identity = _identity()
    transfer = _transfer(identity, "before-store", 0)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    first = _inline(
        {"nested": reference},
        {(reference.object_id, reference.owner_worker_id): transfer},
    )
    stream = (
        protocol.InlineArg(b"invalid pickle")
        if corruption == "pickle"
        else protocol.InlineArg(
            cloudpickle.dumps(7), nested_refs=(transfer,)
        )
        if corruption == "unused_manifest"
        else protocol.InlineArg(cloudpickle.dumps(7))
    )
    stored, descriptor = _stored(identity, stream, worker.node_id)
    payload = (
        bytes([stream.data[0] ^ 1]) + stream.data[1:]
        if corruption == "checksum" else stream.data
    )
    push, function_payload = _push(
        worker.worker_id, identity, args=(first,),
        kwargs=(("stored", stored),), dependencies=(descriptor,),
    )
    events: list[object] = []

    def restore(
        item: protocol.NestedReferenceTransfer, attempt_id: AttemptID,
    ) -> _ImportedHandle:
        assert item == transfer
        assert attempt_id == identity.attempt_id
        events.append(("acquire", "before-store"))
        return _ImportedHandle("before-store", events)

    _install_runtime_fakes(
        monkeypatch, worker, function_payload,
        lambda *_args, **_kwargs: pytest.fail("invalid stream reached user code"),
        restore, events, stored_payloads={stored.object_id: payload},
    )
    reply = worker._handle_push_task(push)
    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert events.count(("acquire", "before-store")) == 1
    assert events.count(("close", "before-store")) == 1
    assert events.index(("close", "before-store")) < events.index("complete")


def test_stored_argument_decodes_pulled_stream_with_nested_import_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = _identity()
    transfer = _transfer(identity, "stored", 19)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    inline = _inline(
        {"nested": [reference, reference]},
        {(reference.object_id, reference.owner_worker_id): transfer},
    )
    storage_task = TaskID.derive(
        identity.job_id, TaskID.for_driver(identity.job_id), 20
    )
    storage_id = ObjectID.for_task(storage_task)
    storage_owner = WorkerID.random()
    node_id = NodeID.random()
    descriptor = protocol.ObjectStoreDescriptor(
        storage_id, storage_owner, AttemptID(storage_task, 0), node_id,
        len(inline.data), hashlib.sha256(inline.data).hexdigest(),
    )
    stored = protocol.StoredArg(
        storage_id, storage_owner, inline.serializer, inline.nested_refs
    )
    events: list[object] = []
    imported = _ImportedHandle("stored", events)
    session = NestedReferenceImportSession(
        lambda item: (
            imported if item == transfer
            else pytest.fail("wrong nested transfer")
        )
    )

    def rpc(_address: object, handler: str, request: object) -> object:
        assert handler == worker_module.GET_OBJECT_HANDLER
        assert request == protocol.GetObject(
            storage_id, node_id,
            expected_attempt_id=descriptor.producer_attempt_id,
            expected_owner_worker_id=storage_owner,
            expected_size_bytes=len(inline.data),
            expected_checksum=descriptor.checksum,
        )
        return protocol.GetObjectReply(
            storage_id, node_id, True, True, inline.data,
            descriptor.checksum,
            producer_attempt_id=descriptor.producer_attempt_id,
            owner_worker_id=storage_owner, size_bytes=len(inline.data),
        )

    monkeypatch.setattr(worker_module, "rpc_request", rpc)
    monkeypatch.setattr(
        worker_module.cloudpickle, "loads",
        lambda _payload: pytest.fail(
            "StoredArg must not use plain cloudpickle.loads"
        ),
    )

    decoded = worker_module._decode_argument(
        stored, dependencies={storage_id: descriptor}, node_id=node_id,
        node_address=("127.0.0.1", 24999), nested_imports=session,
    )
    session.commit()
    assert decoded["nested"][0] is imported
    assert decoded["nested"][1] is imported
    assert session.acquired == (imported,)
    session.close()
    assert events == [("close", "stored")]


def test_partial_import_failure_rolls_back_prior_handles_in_reverse_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    identity = _identity()
    transfers = tuple(_transfer(identity, label, index) for index, label in enumerate("ABC"))
    references = tuple(
        ContainedRef(transfer.object_id, transfer.owner_worker_id)
        for transfer in transfers
    )
    by_identity = {
        (reference.object_id, reference.owner_worker_id): transfer
        for reference, transfer in zip(references, transfers)
    }
    argument = _inline(list(references), by_identity)
    push, function_payload = _push(
        worker.worker_id, identity, args=(argument,)
    )
    events: list[object] = []
    user_calls = 0

    def restore(
        item: protocol.NestedReferenceTransfer, attempt_id: AttemptID
    ) -> _ImportedHandle:
        label = "ABC"[transfers.index(item)]
        assert attempt_id == identity.attempt_id
        events.append(("acquire", label))
        if label == "C":
            raise RuntimeError("third nested acquire failed")
        return _ImportedHandle(label, events)

    def function(_value: object) -> None:
        nonlocal user_calls
        user_calls += 1

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function, restore, events
    )

    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert user_calls == 0
    assert [
        event for event in events if isinstance(event, tuple)
    ] == [
        ("acquire", "A"),
        ("acquire", "B"),
        ("acquire", "C"),
        ("close", "B"),
        ("close", "A"),
    ]
    assert events.index(("close", "A")) < events.index("complete")


@pytest.mark.parametrize(
    ("failure_stage", "expected_status"),
    [
        ("decode", protocol.TaskReplyStatus.SYSTEM_ERROR),
        ("user", protocol.TaskReplyStatus.APPLICATION_ERROR),
        ("result", protocol.TaskReplyStatus.SYSTEM_ERROR),
        ("success", protocol.TaskReplyStatus.SUCCEEDED),
    ],
)
def test_imported_handles_close_at_the_correct_completion_boundary_on_every_attempt_path(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    expected_status: protocol.TaskReplyStatus,
) -> None:
    worker = _worker()
    identity = _identity()
    transfer = _transfer(identity, failure_stage, 3)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    argument = _inline(
        {"ref": reference},
        {(reference.object_id, reference.owner_worker_id): transfer},
    )
    push, function_payload = _push(
        worker.worker_id, identity, args=(argument,)
    )
    events: list[object] = []

    def restore(
        _item: protocol.NestedReferenceTransfer, _attempt_id: AttemptID
    ) -> _ImportedHandle:
        events.append(("acquire", failure_stage))
        return _ImportedHandle(failure_stage, events)

    def function(_value: object) -> int:
        events.append("user")
        if failure_stage == "user":
            raise ValueError("user failed")
        return 11

    def reject_result(request: wire.PrepareOutputPublication) -> object:
        assert ("close", failure_stage) not in events
        return wire.PreparedOutputPublicationReply(
            request.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE,
            "result publication rejected",
        )

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function, restore, events,
        preparation=reject_result if failure_stage == "result" else None,
    )
    if failure_stage == "decode":
        def fail_decode(
            _argument: protocol.InlineArg, *, import_nested_ref: object
        ) -> object:
            import_nested_ref.resolve(transfer)
            raise ValueError("argument decode failed")

        monkeypatch.setattr(worker_module, "decode_inline_argument", fail_decode)
    reply = worker._handle_push_task(push)

    assert reply.status is expected_status
    assert events.count(("acquire", failure_stage)) == 1
    assert events.count(("close", failure_stage)) == 1
    if failure_stage == "result":
        # A rejected publication can have effects; source/import custody is
        # retained through the Node's failed-Complete compensation ACK.
        assert events.index("prepare") < events.index("complete")
        assert events.index("complete") < events.index(("close", failure_stage))
    else:
        assert events.index(("close", failure_stage)) < events.index("complete")
    if failure_stage == "decode":
        assert "user" not in events


def test_exact_cached_push_replay_never_reimports_nested_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    identity = _identity()
    transfer = _transfer(identity, "replay", 4)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    argument = _inline(
        [reference, reference],
        {(reference.object_id, reference.owner_worker_id): transfer},
    )
    push, function_payload = _push(
        worker.worker_id, identity, args=(argument,)
    )
    events: list[object] = []
    acquisitions = 0
    executions = 0
    completion_calls = 0

    def restore(
        _item: protocol.NestedReferenceTransfer, _attempt_id: AttemptID
    ) -> _ImportedHandle:
        nonlocal acquisitions
        acquisitions += 1
        events.append(("acquire", "replay"))
        return _ImportedHandle("replay", events)

    def function(value: object) -> int:
        nonlocal executions
        executions += 1
        assert value[0] is value[1]
        return 13

    def completion(message: protocol.CompleteWorkerLease) -> object:
        nonlocal completion_calls
        completion_calls += 1
        if completion_calls <= 3:
            raise TransportTimeout("completion acknowledgement was lost")
        return _completion_reply(message)

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function, restore, events,
        completion=completion,
    )

    with pytest.raises(RuntimeError, match="exact PushTask replay") as pending:
        worker._handle_push_task(push)
    assert isinstance(pending.value.__cause__, TransportTimeout)
    assert "completion acknowledgement" in str(pending.value.__cause__)

    key = (push.spec.attempt_id, push.lease_id)
    assert key not in worker._replies
    assert worker._prepared_output_replies[key].prepare_acked
    assert worker._prepared_output_replies[key].complete_envelope is None
    assert acquisitions == 1
    assert executions == 1
    assert events.count(("close", "replay")) == 1

    cached = worker._handle_push_task(push)

    assert cached is worker._replies[key]
    assert acquisitions == 1
    assert executions == 1
    assert events.count("embedded_core") == 1
    assert events.count(("close", "replay")) == 1
    assert completion_calls == 4
    assert events.count("prepare") == 1
    assert key not in worker._prepared_output_replies
    assert worker._completion_acked == {key}


class _NestedImportCrash(SystemExit):
    pass


def test_crash_after_nested_import_observes_live_borrower_before_user_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    worker._failpoint = WorkerFailpointConfig(
        mode=WorkerFailpointMode.CRASH_AFTER_NESTED_IMPORT
    )
    identity = _identity()
    transfer = _transfer(identity, "crash", 5)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    argument = _inline(
        {"nested": reference},
        {(reference.object_id, reference.owner_worker_id): transfer},
    )
    push, function_payload = _push(
        worker.worker_id, identity, args=(argument,)
    )
    events: list[object] = []
    handles: list[_ImportedHandle] = []
    exit_observations: list[tuple[int, bool, tuple[object, ...]]] = []

    def restore(
        _item: protocol.NestedReferenceTransfer, attempt_id: AttemptID
    ) -> _ImportedHandle:
        assert attempt_id == identity.attempt_id
        handle = _ImportedHandle("crash", events)
        handles.append(handle)
        events.append("acquire")
        return handle

    def function(_value: object) -> int:
        events.append("user")
        return 17

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function, restore, events
    )
    original_commit = worker_module.NestedReferenceImportSession.commit

    def commit(session: object) -> tuple[object, ...]:
        acquired = original_commit(session)
        events.append("commit")
        return acquired

    def execution_binding(_request: protocol.PushTask) -> object:
        events.append("execution_binding")
        return nullcontext()

    def exit_now(code: int) -> None:
        assert len(handles) == 1
        exit_observations.append((code, handles[0].closed, tuple(events)))
        events.append("exit")
        raise _NestedImportCrash(code)

    monkeypatch.setattr(
        worker_module.NestedReferenceImportSession, "commit", commit
    )
    monkeypatch.setattr(worker, "_execution_binding", execution_binding)
    monkeypatch.setattr(worker_module.os, "_exit", exit_now)

    with pytest.raises(_NestedImportCrash):
        worker._handle_push_task(push)

    assert exit_observations == [
        (
            CRASH_AFTER_NESTED_IMPORT_EXIT_CODE,
            False,
            ("start", "embedded_core", "function_decode", "acquire", "commit"),
        )
    ]
    assert events.index("acquire") < events.index("commit") < events.index("exit")
    assert "execution_binding" not in events
    assert "user" not in events
    assert "complete" not in events
    # The patched SystemExit necessarily unwinds this unit-test stack.  The
    # observation captured inside os._exit above is the real-process boundary:
    # at that point the attempt borrower was still live.
    assert handles[0].closed
    assert worker._failpoint_triggers == 1


def test_crash_after_nested_import_without_nested_ref_is_system_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    worker._failpoint = WorkerFailpointConfig(
        mode=WorkerFailpointMode.CRASH_AFTER_NESTED_IMPORT
    )
    identity = _identity()
    argument = protocol.InlineArg(cloudpickle.dumps({"plain": 1}))
    push, function_payload = _push(
        worker.worker_id, identity, args=(argument,)
    )
    events: list[object] = []

    def function(_value: object) -> int:
        events.append("user")
        return 19

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function,
        lambda *_args: pytest.fail("plain argument attempted nested import"),
        events,
    )
    monkeypatch.setattr(
        worker_module.os, "_exit",
        lambda _code: pytest.fail("plain argument triggered process crash"),
    )

    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert reply.error is not None
    assert reply.error.type_name == "RuntimeError"
    # A result payload may contain a reference even without a Task manifest.
    # Check acquired custody after decoding, not merely nested_refs metadata.
    assert "did not acquire a nested ObjectRef borrower" in reply.error.message
    assert events == ["start", "function_decode", "complete"]
    assert worker._failpoint_triggers == 1


def test_new_physical_attempt_creates_a_fresh_import_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    first_identity = _identity()
    second_identity = _TaskIdentity(
        first_identity.job_id,
        first_identity.task_id,
        first_identity.attempt_id.next(),
        first_identity.task_owner_id,
    )
    transfer = _transfer(first_identity, "attempt", 5)
    reference = ContainedRef(transfer.object_id, transfer.owner_worker_id)
    transfers = {(reference.object_id, reference.owner_worker_id): transfer}
    first_argument = _inline({"ref": reference}, transfers)
    second_argument = _inline({"ref": reference}, transfers)
    first_push, function_payload = _push(
        worker.worker_id, first_identity, args=(first_argument,)
    )
    second_push, second_payload = _push(
        worker.worker_id, second_identity, args=(second_argument,)
    )
    assert second_payload == function_payload
    events: list[object] = []
    acquired_attempts: list[AttemptID] = []
    handles: list[_ImportedHandle] = []

    def restore(
        _item: protocol.NestedReferenceTransfer, attempt_id: AttemptID
    ) -> _ImportedHandle:
        handle = _ImportedHandle(
            "attempt-{}".format(attempt_id.attempt_number), events
        )
        acquired_attempts.append(attempt_id)
        handles.append(handle)
        return handle

    def function(value: object) -> int:
        assert not value["ref"].closed
        return 17

    _install_runtime_fakes(
        monkeypatch, worker, function_payload, function, restore, events
    )

    first_reply = worker._handle_push_task(first_push)
    second_reply = worker._handle_push_task(second_push)

    assert first_reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert second_reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert acquired_attempts == [
        first_identity.attempt_id, second_identity.attempt_id,
    ]
    assert len(handles) == 2 and handles[0] is not handles[1]
    assert all(handle.closed for handle in handles)
    assert events.count(("close", "attempt-0")) == 1
    assert events.count(("close", "attempt-1")) == 1
