"""Worker execution contracts for targeted multi-return replay."""

from __future__ import annotations

import threading

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.task_outputs import TargetExecutionKey
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, START_WORKER_LEASE_HANDLER, WorkerServer,
)
from tests.unit._unified_worker_rpc import UnifiedWorkerRPC


pytestmark = pytest.mark.unit


class _MustNotSerialize:
    def __reduce__(self):
        raise AssertionError("healthy non-target output was serialized")


def _push(function: object, *, target_indices: tuple[int, ...]):
    worker_id = WorkerID.random()
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 8)
    attempt = AttemptID(task, 2)
    payload = cloudpickle.dumps(function)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job, __name__, "targeted", "v1"), payload
    )
    spec = protocol.TaskSpec(
        job, task, attempt, definition.key, (), 3, ResourceVector({"CPU": 1}),
        WorkerID.random(), function_definition=definition, max_retries=3,
    )
    targets = tuple(spec.return_ids()[index] for index in target_indices)
    execution = TargetExecutionKey.from_task_spec(
        spec, targets, attempt_id=attempt
    )
    return worker_id, protocol.PushTask(
        LeaseID.random(), worker_id, spec, target_execution=execution
    )


def _worker(worker_id: WorkerID) -> WorkerServer:
    worker = object.__new__(WorkerServer)
    worker.worker_id = worker_id
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 22001)
    worker.inline_threshold = 1024
    worker._execution_lock = threading.Lock()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._embedded_core = None
    worker._embedded_core_job_id = None
    worker._worker_core_enabled = False
    worker._failpoint = None
    worker._failpoint_triggers = 0
    worker._crash_after_complete = set()
    worker.event_sink = None
    return worker


def _rpc_log(monkeypatch: pytest.MonkeyPatch, worker: WorkerServer):
    messages: list[object] = []
    publication = UnifiedWorkerRPC()

    def rpc(address, handler, message):
        assert address == worker.node_address
        messages.append(message)
        if handler == START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                message.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                target_execution=message.target_execution,
                node_incarnation=OutputPublicationNodeIncarnation(worker.node_id, 21001, 3),
            )
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return publication.complete(message)

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    return messages


def test_worker_validates_full_arity_but_serializes_only_target_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def producer():
        return 10, _MustNotSerialize(), 30

    worker_id, push = _push(producer, target_indices=(0, 2))
    worker = _worker(worker_id)
    messages = _rpc_log(monkeypatch, worker)

    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.target_execution == push.target_execution
    assert tuple(result.object_id for result in reply.results) == (
        push.spec.return_ids()[0], push.spec.return_ids()[2],
    )
    assert tuple(cloudpickle.loads(result.inline_data) for result in reply.results) == (
        10, 30,
    )
    assert isinstance(messages[0], protocol.StartWorkerLease)
    assert isinstance(messages[-1], protocol.CompleteWorkerLease)
    assert isinstance(messages[1], wire.PrepareOutputPublication)
    assert messages[1].manifest.execution == push.target_execution
    assert all(message.target_execution == push.target_execution
               for message in messages if not isinstance(message, wire.PrepareOutputPublication))
    assert reply.output_publication.manifest == messages[1].manifest


def test_worker_rejects_wrong_full_return_arity_before_target_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id, push = _push(lambda: (10, 30), target_indices=(0, 2))
    worker = _worker(worker_id)
    messages = _rpc_log(monkeypatch, worker)

    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert reply.results == ()
    assert reply.target_execution == push.target_execution
    assert reply.error is not None
    assert reply.error.type_name == "ValueError"
    assert isinstance(messages[-1], protocol.CompleteWorkerLease)
    assert messages[-1].target_execution == push.target_execution


def test_worker_exact_replay_never_reexecutes_and_mask_drift_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def producer():
        nonlocal calls
        calls += 1
        return 1, 2, 3

    worker_id, push = _push(producer, target_indices=(0, 2))
    worker = _worker(worker_id)
    _rpc_log(monkeypatch, worker)
    monkeypatch.setattr(
        "miniray.worker.cloudpickle.loads", lambda _payload: producer
    )
    first = worker._handle_push_task(push)
    replay = worker._handle_push_task(push)
    changed_execution = TargetExecutionKey.from_task_spec(
        push.spec, (push.spec.return_ids()[1],),
        attempt_id=push.spec.attempt_id,
    )
    changed = protocol.PushTask(
        push.lease_id, push.worker_id, push.spec,
        target_execution=changed_execution,
    )

    with pytest.raises(RuntimeError, match="changed the original request"):
        worker._handle_push_task(changed)

    assert replay is first
    assert calls == 1


def test_worker_rejects_start_or_completion_ack_with_target_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id, push = _push(lambda: (1, 2, 3), target_indices=(0, 2))
    worker = _worker(worker_id)
    other = TargetExecutionKey.from_task_spec(
        push.spec, (push.spec.return_ids()[1],),
        attempt_id=push.spec.attempt_id,
    )

    def bad_start(_address, handler, message):
        assert handler == START_WORKER_LEASE_HANDLER
        return protocol.StartWorkerLeaseReply(
            message.lease_id, protocol.LeaseExecutionState.RUNNING, True,
            target_execution=other,
            node_incarnation=OutputPublicationNodeIncarnation(worker.node_id, 21001, 3),
        )

    monkeypatch.setattr("miniray.worker.rpc_request", bad_start)
    with pytest.raises(RuntimeError, match="different worker lease"):
        worker._handle_push_task(push)

    worker = _worker(worker_id)
    publication = UnifiedWorkerRPC()

    def bad_complete(_address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                message.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                target_execution=message.target_execution,
                node_incarnation=OutputPublicationNodeIncarnation(worker.node_id, 21001, 3),
            )
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return protocol.CompleteWorkerLeaseReply(
            message.lease_id, message.task_id, message.attempt_id,
            message.worker_id, message.status,
            protocol.LeaseExecutionState.COMPLETED, True, True,
            target_execution=other,
        )

    monkeypatch.setattr("miniray.worker.rpc_request", bad_complete)
    with pytest.raises(RuntimeError, match="exact PushTask replay") as pending:
        worker._handle_push_task(push)
    assert "different worker-lease completion" in str(pending.value.__cause__)
    key = (push.spec.attempt_id, push.lease_id)
    assert key not in worker._completion_acked and key not in worker._replies
    assert worker._prepared_output_replies[key].outputs.manifest.execution == push.target_execution
