from __future__ import annotations

import threading
from dataclasses import replace

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerFailpointConfig,
    WorkerServer,
)
from tests.unit.test_worker_completion_paths import _SingleOutputRPC

pytestmark = pytest.mark.unit


from tests.support._worker_protocol import initialize_worker_protocol, complete_boundary as _complete_boundary

def test_failpoint_emits_one_completed_system_error_before_decode(monkeypatch) -> None:
    worker = object.__new__(WorkerServer)
    initialize_worker_protocol(worker)
    worker.worker_id = WorkerID.random()
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 19000)
    worker.inline_threshold = 1024
    worker._execution_lock = threading.Lock()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._failpoint = WorkerFailpointConfig()
    worker._failpoint_triggers = 0

    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job, __name__, "never_decode", "v1"), b"payload"
    )
    spec = protocol.TaskSpec(
        job, task, AttemptID(task, 0), definition.key, (), 1,
        ResourceVector(), WorkerID.random(), function_definition=definition,
        max_retries=1,
    )
    push = protocol.PushTask(LeaseID.random(), worker.worker_id, spec)
    decoded = False
    completions = []
    publication = _SingleOutputRPC()

    def decode(_payload):
        nonlocal decoded
        decoded = True
        raise AssertionError("failpoint must precede decode")

    def rpc(_address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                message.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                node_incarnation=OutputPublicationNodeIncarnation(worker.node_id, 21001, 3),
            )
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        completions.append(message)
        return _complete_boundary(publication, message)

    monkeypatch.setattr("miniray.worker.cloudpickle.loads", decode)
    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert not decoded
    assert worker._failpoint_triggers == 1
    assert len(completions) == 1
    assert publication.prepare_requests == []

    # The fail-once injection must not affect a fresh physical retry.  Its
    # success now requires unified Prepare plus the exact Node envelope.
    calls = []

    def callable_once():
        calls.append(True)
        return 7

    monkeypatch.setattr("miniray.worker.cloudpickle.loads", lambda _payload: callable_once)
    retry = protocol.PushTask(
        LeaseID.random(), worker.worker_id, replace(spec, attempt_id=spec.attempt_id.next()),
    )
    succeeded = worker._handle_push_task(retry)
    assert succeeded.status is protocol.TaskReplyStatus.SUCCEEDED
    assert succeeded.output_publication.manifest == publication.prepare_requests[0].manifest
    assert calls == [True] and worker._failpoint_triggers == 1
    assert len(completions) == 2 and len(publication.prepare_requests) == 1
    assert worker._handle_push_task(retry) is succeeded and calls == [True]


def test_failpoint_config_is_deliberately_bounded() -> None:
    with pytest.raises(ValueError):
        WorkerFailpointConfig(attempt_number=1)
    with pytest.raises(ValueError):
        WorkerFailpointConfig(max_triggers=2)
