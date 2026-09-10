"""Mixed Worker exact-Push obligations and real shutdown waiting.

The three synchronous cases use a bare Worker and an in-memory unified
Prepare/Complete peer. Unknown ACKs preserve the original discovered bytes.
The shutdown case actually waits on Condition.wait_for while an obligation is
pending, even with no server or background thread, and remains heavy until its
exact bounded runtime review. No original contract is deleted or skipped.
"""

from __future__ import annotations

import threading
import socket
import time
from dataclasses import replace

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.trace import EventSink
from miniray.transport import TransportTimeout
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerServer,
)
from tests.unit.test_worker_completion_paths import _SingleOutputRPC


from tests.support._worker_protocol import initialize_worker_protocol, complete_boundary as _complete_boundary

@pytest.fixture(autouse=True)
def _pure_cases_do_not_wait(request, monkeypatch):
    if request.node.get_closest_marker("heavy") is not None:
        return

    def forbidden(*_args, **_kwargs):
        pytest.fail("synchronous Worker replay attempted runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Event, "wait"), (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _push(worker_id: WorkerID, function: object) -> protocol.PushTask:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    attempt_id = AttemptID(task_id, 0)
    payload = cloudpickle.dumps(function)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job_id, __name__, "obligation_task", "v1"),
        payload,
    )
    spec = protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=attempt_id,
        function=definition.key,
        args=(),
        num_returns=1,
        resources=ResourceVector({"CPU": 1}),
        owner_worker_id=WorkerID.random(),
        function_definition=definition,
    )
    return protocol.PushTask(LeaseID.random(), worker_id, spec)


def _worker() -> WorkerServer:
    worker = object.__new__(WorkerServer)
    initialize_worker_protocol(worker)
    worker.worker_id = WorkerID.random()
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 22001)
    worker.gcs_address = ("127.0.0.1", 22002)
    worker.inline_threshold = 1024
    worker._request_timeout = 0.01
    worker._failpoint = None
    worker._failpoint_triggers = 0
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
    worker._worker_core_enabled = False
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    return worker


def _start_reply(message: protocol.StartWorkerLease, node_id: NodeID) -> protocol.StartWorkerLeaseReply:
    return protocol.StartWorkerLeaseReply(
        message.lease_id, protocol.LeaseExecutionState.RUNNING, accepted=True,
        node_incarnation=OutputPublicationNodeIncarnation(node_id, 21001, 3),
    )


def _completion_reply(
    message: protocol.CompleteWorkerLease,
) -> protocol.CompleteWorkerLeaseReply:
    return protocol.CompleteWorkerLeaseReply(
        message.lease_id, message.task_id, message.attempt_id,
        message.worker_id, message.status, protocol.LeaseExecutionState.COMPLETED,
        accepted=True, released=True,
    )


@pytest.mark.unit
def test_start_ack_loss_then_closed_exact_replay_executes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    executions = 0

    def function() -> int:
        nonlocal executions
        executions += 1
        return 7

    push = _push(worker.worker_id, function)
    key = (push.spec.attempt_id, push.lease_id)
    start_calls = 0
    outputs = _SingleOutputRPC()
    original_loads = cloudpickle.loads

    def rpc(_address, handler, message):
        nonlocal start_calls
        if handler == START_WORKER_LEASE_HANDLER:
            start_calls += 1
            if start_calls == 1:
                # Model a Node that committed RUNNING before its ACK was lost.
                raise TransportTimeout("start acknowledgement was lost")
            return _start_reply(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return outputs.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return _complete_boundary(outputs, message)

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    monkeypatch.setattr("miniray.worker.cloudpickle.loads", lambda _payload: function)

    with pytest.raises(TransportTimeout, match="start acknowledgement"):
        worker._handle_push_task(push)
    assert executions == 0
    assert worker._accepted_pushes[key] == push
    assert worker._push_obligations == {key}
    assert worker._active_tasks == 0

    with worker._lifecycle:
        worker._accepting_tasks = False
    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert original_loads(reply.results[0].inline_data) == 7
    assert executions == 1
    assert start_calls == 2
    assert len(outputs.prepare_requests) == 1 and reply.output_publication is not None
    assert worker._push_obligations == set()
    assert worker._active_tasks == 0


@pytest.mark.heavy
def test_unresolved_obligation_blocks_clean_shutdown_then_late_replay_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    push = _push(worker.worker_id, lambda: 9)
    key = (push.spec.attempt_id, push.lease_id)
    lose_start = True
    outputs = _SingleOutputRPC()

    def rpc(_address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            if lose_start:
                raise TransportTimeout("start acknowledgement was lost")
            return _start_reply(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return outputs.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return _complete_boundary(outputs, message)

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    with pytest.raises(TransportTimeout):
        worker._handle_push_task(push)

    first = worker._handle_shutdown(protocol.Shutdown.create("obligation pending"))
    assert not first.clean
    assert not worker._stop_event.is_set()
    assert worker._push_obligations == {key}

    lose_start = False
    assert worker._handle_push_task(push).status is protocol.TaskReplyStatus.SUCCEEDED
    assert worker._push_obligations == set()

    second = worker._handle_shutdown(protocol.Shutdown.create("replay resolved"))
    assert second.clean
    assert worker._stop_event.is_set()


@pytest.mark.unit
def test_closed_admission_rejects_changed_and_new_pushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    push = _push(worker.worker_id, lambda: 1)
    key = (push.spec.attempt_id, push.lease_id)
    worker._accepted_pushes[key] = push
    worker._push_obligations.add(key)
    worker._accepting_tasks = False

    changed = protocol.PushTask(
        push.lease_id, push.worker_id,
        protocol.TaskSpec(
            job_id=push.spec.job_id, task_id=push.spec.task_id,
            attempt_id=push.spec.attempt_id, function=push.spec.function,
            args=(protocol.InlineArg(cloudpickle.dumps("changed")),),
            num_returns=1, resources=push.spec.resources,
            owner_worker_id=push.spec.owner_worker_id,
            function_definition=push.spec.function_definition,
        ),
    )
    with pytest.raises(RuntimeError, match="changed the original request"):
        worker._handle_push_task(changed)

    new_push = _push(worker.worker_id, lambda: 2)
    with pytest.raises(RuntimeError, match="shutting down"):
        worker._handle_push_task(new_push)
    assert worker._active_tasks == 0
    assert worker._accepted_pushes == {key: push}
    assert worker._push_obligations == {key}


@pytest.mark.unit
def test_complete_ack_loss_keeps_obligation_and_cached_replay_clears_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    executions = 0

    def function() -> int:
        nonlocal executions
        executions += 1
        return 11

    push = _push(worker.worker_id, function)
    key = (push.spec.attempt_id, push.lease_id)
    completion_calls = 0
    outputs = _SingleOutputRPC()
    completion_requests, completions = [], []

    def rpc(_address, handler, message):
        nonlocal completion_calls
        if handler == START_WORKER_LEASE_HANDLER:
            return _start_reply(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return outputs.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        completion_calls += 1
        actual = _complete_boundary(outputs, message)
        completion_requests.append(message)
        completions.append(actual)
        if completion_calls <= 3:
            raise TransportTimeout("completion acknowledgement was lost")
        return actual

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    monkeypatch.setattr("miniray.worker.cloudpickle.loads", lambda _payload: function)
    with pytest.raises(RuntimeError, match="discovered output custody") as caught:
        worker._handle_push_task(push)

    assert executions == 1
    assert isinstance(caught.value.__cause__, TransportTimeout)
    assert key not in worker._replies and key not in worker._cached_pushes
    retained = worker._prepared_output_replies[key]
    assert retained.request == push and retained.prepare_acked
    assert retained.complete_envelope is None
    assert len(outputs.prepare_requests) == 1 and len(completions) == 3
    completion = protocol.CompleteWorkerLease(
        push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED, push.spec.scheduling_key,
    )
    assert completion_requests == [completion] * 3
    # released describes this call's effect, not an immutable completion fact:
    # only the first Complete commits; exact replays retain the same envelope.
    assert tuple(reply.released for reply in completions) == (True, False, False)
    assert all(replace(reply, released=True) == completions[0] for reply in completions)
    envelope = completions[0].output_publication
    assert envelope is not None and envelope.manifest == retained.outputs.manifest
    committed = {(completion.lease_id, completion.task_id, completion.attempt_id,
                  completion.worker_id): (completion, envelope.complete)}
    assert outputs.completions == committed
    assert outputs.journal.snapshot(envelope.publication_id).complete == envelope.complete
    assert key not in worker._completion_acked
    assert worker._push_obligations == {key}

    with worker._lifecycle:
        worker._accepting_tasks = False
    cached = worker._handle_push_task(push)
    assert cached is worker._replies[key]
    assert worker._cached_pushes[key] == push and key not in worker._prepared_output_replies
    assert cached.output_publication == envelope
    assert cached.output_publication.manifest == retained.outputs.manifest
    assert len(outputs.prepare_requests) == 1
    assert executions == 1
    assert completion_calls == 4
    assert completion_requests == [completion] * 4
    assert tuple(reply.released for reply in completions) == (True, False, False, False)
    assert all(replace(reply, released=True) == completions[0] for reply in completions)
    assert outputs.completions == committed
    assert worker._completion_acked == {key}
    assert worker._push_obligations == set()
