"""Pure Worker tests for target-local RefArg materialization."""

from __future__ import annotations

import hashlib
import threading

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    GET_OBJECT_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerServer,
)
from tests.unit._unified_worker_rpc import UnifiedWorkerRPC


pytestmark = pytest.mark.unit


def _worker() -> WorkerServer:
    worker = object.__new__(WorkerServer)
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
    return worker


def _push(worker: WorkerServer, payload: bytes) -> protocol.PushTask:
    producer_job = JobID.random()
    producer_task = TaskID.derive(
        producer_job, TaskID.for_driver(producer_job), 0
    )
    object_id = ObjectID.for_task(producer_task, 0)
    owner = WorkerID.random()
    dependency = protocol.ObjectStoreDescriptor(
        object_id=object_id,
        owner_worker_id=owner,
        producer_attempt_id=AttemptID(producer_task, 0),
        node_id=worker.node_id,
        size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(),
    )
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    function_payload = cloudpickle.dumps(lambda value: value + 1)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job_id, __name__, "consume", "v1"),
        function_payload,
    )
    spec = protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=AttemptID(task_id, 0),
        function=definition.key,
        args=(protocol.RefArg(object_id, owner),),
        num_returns=1,
        resources=ResourceVector({"CPU": 1}),
        owner_worker_id=WorkerID.random(),
        function_definition=definition,
    )
    return protocol.PushTask(
        LeaseID.random(), worker.worker_id, spec, dependencies=(dependency,)
    )


@pytest.mark.parametrize(
    "corruption",
    [None, "size", "checksum", "node", "attempt", "owner", "echo_size"],
)
def test_worker_reads_only_verified_local_dependency_before_execution(
    monkeypatch: pytest.MonkeyPatch, corruption: str | None
) -> None:
    worker = _worker()
    payload = cloudpickle.dumps(41)
    push = _push(worker, payload)
    events: list[str] = []
    completions: list[protocol.CompleteWorkerLease] = []
    publication = UnifiedWorkerRPC()

    def fake_rpc(address, handler, message):
        assert address == worker.node_address
        if handler == START_WORKER_LEASE_HANDLER:
            events.append("start")
            return protocol.StartWorkerLeaseReply(
                message.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                node_incarnation=OutputPublicationNodeIncarnation(worker.node_id, 21001, 3),
            )
        if handler == GET_OBJECT_HANDLER:
            events.append("get")
            descriptor = push.dependencies[0]
            assert message == protocol.GetObject(
                object_id=descriptor.object_id,
                requester_node_id=worker.node_id,
                expected_attempt_id=descriptor.producer_attempt_id,
                expected_owner_worker_id=descriptor.owner_worker_id,
                expected_size_bytes=descriptor.size_bytes,
                expected_checksum=descriptor.checksum,
            )
            data = (
                payload + b"x"
                if corruption in {"size", "echo_size"}
                else payload
            )
            checksum = None if corruption == "checksum" else hashlib.sha256(data).hexdigest()
            node_id = NodeID.random() if corruption == "node" else worker.node_id
            attempt = (
                descriptor.producer_attempt_id.next()
                if corruption == "attempt"
                else descriptor.producer_attempt_id
            )
            owner = (
                WorkerID.random()
                if corruption == "owner"
                else descriptor.owner_worker_id
            )
            echoed_size = (
                descriptor.size_bytes + 1
                if corruption == "echo_size"
                else len(data)
            )
            return protocol.GetObjectReply(
                message.object_id,
                node_id,
                True,
                True,
                data,
                checksum,
                producer_attempt_id=attempt,
                owner_worker_id=owner,
                size_bytes=echoed_size,
            )
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            events.append("prepare")
            assert corruption is None
            return publication.prepare(message)
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            events.append("complete")
            completions.append(message)
            return publication.complete(message)
        raise AssertionError(handler)

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)
    reply = worker._handle_push_task(push)

    assert events == (["start", "get", "prepare", "complete"] if corruption is None
                      else ["start", "get", "complete"])
    assert len(completions) == 1
    if corruption is None:
        assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
        assert cloudpickle.loads(reply.results[0].inline_data) == 42
        assert completions[0].status is protocol.TaskReplyStatus.SUCCEEDED
        assert reply.output_publication.manifest == publication.prepare_requests[0].manifest
    else:
        assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert completions[0].status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert publication.prepare_requests == []


def test_worker_rejects_nonlocal_dependency_without_fetching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker()
    push = _push(worker, cloudpickle.dumps(41))
    remote = protocol.ObjectStoreDescriptor(
        object_id=push.dependencies[0].object_id,
        owner_worker_id=push.dependencies[0].owner_worker_id,
        producer_attempt_id=push.dependencies[0].producer_attempt_id,
        node_id=NodeID.random(),
        size_bytes=push.dependencies[0].size_bytes,
        checksum=push.dependencies[0].checksum,
    )
    push = protocol.PushTask(
        push.lease_id, push.worker_id, push.spec, dependencies=(remote,)
    )
    handlers: list[str] = []

    def fake_rpc(address, handler, message):
        handlers.append(handler)
        if handler == START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                message.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                node_incarnation=OutputPublicationNodeIncarnation(worker.node_id, 21001, 3),
            )
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            return protocol.CompleteWorkerLeaseReply(
                message.lease_id,
                message.task_id,
                message.attempt_id,
                message.worker_id,
                message.status,
                protocol.LeaseExecutionState.COMPLETED,
                True,
                True,
            )
        raise AssertionError("nonlocal dependency must not be fetched")

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)
    reply = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert handlers == [START_WORKER_LEASE_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
