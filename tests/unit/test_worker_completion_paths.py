"""Pure Worker-side tests for terminal lease completion paths.

No server is started here.  ``rpc_request`` is replaced with the smallest
in-memory NodeManager acknowledgement needed to expose message ordering.
"""

from __future__ import annotations

import threading

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.transport import TransportTimeout
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerServer,
)
from tests.unit._unified_worker_rpc import UnifiedWorkerRPC


pytestmark = pytest.mark.unit


def _push(
    worker_id: WorkerID, payload: bytes, *, num_returns: int = 1,
    attempt_number: int = 0,
) -> protocol.PushTask:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    attempt_id = AttemptID(task_id, attempt_number)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job_id, __name__, "callable", "v1"), payload
    )
    spec = protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=attempt_id,
        function=definition.key,
        args=(),
        num_returns=num_returns,
        resources=ResourceVector({"CPU": 1}),
        owner_worker_id=WorkerID.random(),
        function_definition=definition,
    )
    return protocol.PushTask(LeaseID.random(), worker_id, spec)


def _worker(worker_id: WorkerID, *, inline_threshold: int = 1024) -> WorkerServer:
    worker = object.__new__(WorkerServer)
    worker.worker_id = worker_id
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 19000)
    worker.inline_threshold = inline_threshold
    worker._execution_lock = threading.Lock()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    return worker


def _accepted_start(message: protocol.StartWorkerLease, node_id: NodeID) -> protocol.StartWorkerLeaseReply:
    return protocol.StartWorkerLeaseReply(
        message.lease_id, protocol.LeaseExecutionState.RUNNING, accepted=True,
        node_incarnation=OutputPublicationNodeIncarnation(node_id, 21001, 3),
    )


def _accepted_completion(
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


def test_worker_does_not_decode_execute_or_complete_a_rejected_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(WorkerID.random())
    push = _push(worker.worker_id, b"not reached")
    handlers: list[str] = []
    decoded = False

    def unexpected_decode(payload: bytes) -> object:
        nonlocal decoded
        decoded = True
        raise AssertionError("a rejected lease must not decode the callable")

    def fake_rpc(address, handler, message):
        handlers.append(handler)
        assert address == worker.node_address
        assert handler == START_WORKER_LEASE_HANDLER
        return protocol.StartWorkerLeaseReply(
            message.lease_id,
            protocol.LeaseExecutionState.ABANDONED,
            accepted=False,
            error="lease was abandoned",
        )

    monkeypatch.setattr("miniray.worker.cloudpickle.loads", unexpected_decode)
    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)

    with pytest.raises(RuntimeError, match="lease was abandoned"):
        worker._handle_push_task(push)

    assert not decoded
    assert handlers == [START_WORKER_LEASE_HANDLER]
    assert worker._replies == {}
    assert worker._cached_pushes == {}
    assert worker._completion_acked == set()


@pytest.mark.parametrize(
    ("failure_stage", "expected_status"),
    [
        ("application", protocol.TaskReplyStatus.APPLICATION_ERROR),
        ("decode", protocol.TaskReplyStatus.SYSTEM_ERROR),
        ("prepare", protocol.TaskReplyStatus.SYSTEM_ERROR),
    ],
)
def test_worker_completes_every_error_reply_before_returning_it(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    expected_status: protocol.TaskReplyStatus,
) -> None:
    worker = _worker(WorkerID.random(), inline_threshold=0)
    push = _push(worker.worker_id, b"callable payload")
    events: list[str] = []
    completions: list[protocol.CompleteWorkerLease] = []

    def application_failure() -> object:
        events.append("execute")
        raise ValueError("user failure")

    def large_result() -> bytes:
        events.append("execute")
        return b"stored result"

    def fake_loads(payload: bytes) -> object:
        events.append("decode")
        if failure_stage == "decode":
            raise ValueError("bad function bytes")
        if failure_stage == "application":
            return application_failure
        return large_result

    def fake_rpc(address, handler, message):
        assert address == worker.node_address
        if handler == START_WORKER_LEASE_HANDLER:
            events.append("start")
            return _accepted_start(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            events.append("prepare")
            assert failure_stage == "prepare"
            return wire.PreparedOutputPublicationReply(
                message.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE,
                "Node rejected materialization during publication prepare",
            )
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            key = (push.spec.attempt_id, push.lease_id)
            if failure_stage == "prepare":
                # Post-effect rejection is frozen locally, but cannot be a
                # deliverable failure until Node acknowledges compensation.
                assert key not in worker._replies
                assert worker._prepared_output_replies[key].failure_reply.status is expected_status
            else:
                assert worker._replies[key].status is expected_status
            events.append("complete")
            completions.append(message)
            return _accepted_completion(message)
        raise AssertionError("unexpected RPC handler: {!r}".format(handler))

    monkeypatch.setattr("miniray.worker.cloudpickle.loads", fake_loads)
    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)

    reply = worker._handle_push_task(push)

    assert reply.status is expected_status
    assert len(completions) == 1
    assert completions[0].status is expected_status
    assert events[0] == "start"
    assert events[-1] == "complete"
    if failure_stage == "decode":
        assert "execute" not in events
        assert "prepare" not in events
    elif failure_stage == "application":
        assert "execute" in events
        assert "prepare" not in events
    else:
        assert events.index("execute") < events.index("prepare")
        assert events.index("prepare") < events.index("complete")
    assert (push.spec.attempt_id, push.lease_id) in worker._completion_acked


def test_worker_requires_node_address_before_binding_a_real_task() -> None:
    worker = _worker(WorkerID.random())
    worker.node_address = None
    push = _push(worker.worker_id, cloudpickle.dumps(lambda: "unused"))

    with pytest.raises(RuntimeError, match="NodeManager endpoint"):
        worker._handle_push_task(push)

    assert worker._lease_bindings == {}
    assert worker._attempt_leases == {}
    assert worker._replies == {}
    assert worker._cached_pushes == {}


def test_rejected_start_does_not_bind_attempt_to_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(WorkerID.random())
    first = _push(worker.worker_id, cloudpickle.dumps(lambda: "first"))
    second = protocol.PushTask(LeaseID.random(), worker.worker_id, first.spec)
    starts = 0
    publication = UnifiedWorkerRPC()

    def fake_rpc(address, handler, message):
        nonlocal starts
        if handler == START_WORKER_LEASE_HANDLER:
            starts += 1
            if starts == 1:
                return protocol.StartWorkerLeaseReply(
                    message.lease_id,
                    protocol.LeaseExecutionState.ABANDONED,
                    accepted=False,
                    error="first grant expired",
                )
            return _accepted_start(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            return publication.complete(message)
        raise AssertionError(handler)

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)

    with pytest.raises(RuntimeError, match="first grant expired"):
        worker._handle_push_task(first)
    assert worker._lease_bindings == {}
    assert worker._attempt_leases == {}

    reply = worker._handle_push_task(second)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert worker._attempt_leases[first.spec.attempt_id] == second.lease_id


def test_same_cache_key_rejects_a_changed_push_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(WorkerID.random())
    push = _push(worker.worker_id, cloudpickle.dumps(lambda: "original"))
    publication = UnifiedWorkerRPC()

    def fake_rpc(address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return _accepted_start(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            return publication.complete(message)
        raise AssertionError(handler)

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)
    worker._handle_push_task(push)
    changed = protocol.PushTask(
        push.lease_id,
        push.worker_id,
        protocol.TaskSpec(
            job_id=push.spec.job_id,
            task_id=push.spec.task_id,
            attempt_id=push.spec.attempt_id,
            function=push.spec.function,
            args=(protocol.InlineArg(cloudpickle.dumps("changed")),),
            num_returns=push.spec.num_returns,
            resources=push.spec.resources,
            owner_worker_id=push.spec.owner_worker_id,
            function_definition=push.spec.function_definition,
        ),
    )

    with pytest.raises(RuntimeError, match="changed the original request"):
        worker._handle_push_task(changed)


def test_completion_retries_transport_failures_three_times_without_rerun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(WorkerID.random())
    executions = 0

    def callable_once() -> str:
        nonlocal executions
        executions += 1
        return "done"

    push = _push(worker.worker_id, cloudpickle.dumps(callable_once))
    completions: list[protocol.CompleteWorkerLease] = []
    publication = UnifiedWorkerRPC()
    monkeypatch.setattr(
        "miniray.worker.cloudpickle.loads", lambda payload: callable_once
    )

    def fake_rpc(address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return _accepted_start(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        completions.append(message)
        raise TransportTimeout("lost completion ack")

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)

    with pytest.raises(RuntimeError, match="exact PushTask replay") as pending:
        worker._handle_push_task(push)
    assert isinstance(pending.value.__cause__, TransportTimeout)
    assert "lost completion ack" in str(pending.value.__cause__)

    assert executions == 1
    assert len(completions) == 3
    assert completions[0] == completions[1] == completions[2]
    key = (push.spec.attempt_id, push.lease_id)
    prepared = worker._prepared_output_replies[key]
    assert prepared.request == push and prepared.prepare_acked
    assert prepared.complete_envelope is None and prepared.failure_reply is None
    assert key not in worker._replies and key not in worker._cached_pushes
    assert len(publication.prepare_requests) == 1
    assert worker._completion_acked == set()


def test_worker_rejects_completion_ack_with_changed_full_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(WorkerID.random())
    push = _push(worker.worker_id, cloudpickle.dumps(lambda: "done"))
    publication = UnifiedWorkerRPC()

    def fake_rpc(_address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return _accepted_start(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return protocol.CompleteWorkerLeaseReply(
            message.lease_id,
            message.task_id,
            message.attempt_id.next(),
            message.worker_id,
            message.status,
            protocol.LeaseExecutionState.COMPLETED,
            True,
            True,
        )

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)

    with pytest.raises(RuntimeError, match="exact PushTask replay") as pending:
        worker._handle_push_task(push)
    assert "different worker-lease completion" in str(pending.value.__cause__)

    key = (push.spec.attempt_id, push.lease_id)
    assert key not in worker._completion_acked
    assert key not in worker._replies
    assert worker._prepared_output_replies[key].complete_envelope is None
    assert worker._prepared_output_replies[key].failure_reply is None


def test_reconstructed_multi_return_executes_once_and_returns_full_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(WorkerID.random())
    executions = 0

    def reconstructed_producer() -> tuple[int, int, int]:
        nonlocal executions
        executions += 1
        return 10, 20, 30

    push = _push(
        worker.worker_id, cloudpickle.dumps(reconstructed_producer),
        num_returns=3, attempt_number=2,
    )
    real_loads = cloudpickle.loads
    publication = UnifiedWorkerRPC()

    def fake_rpc(_address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return _accepted_start(message, worker.node_id)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            return publication.prepare(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return publication.complete(message)

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)
    monkeypatch.setattr(
        "miniray.worker.cloudpickle.loads",
        lambda _payload: reconstructed_producer,
    )
    reply = worker._handle_push_task(push)
    replay = worker._handle_push_task(push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert replay is reply
    assert executions == 1
    assert tuple(result.object_id for result in reply.results) == (
        push.spec.return_ids()
    )
    assert tuple(
        real_loads(result.inline_data) for result in reply.results
    ) == (10, 20, 30)
    assert reply.attempt_id == push.spec.attempt_id
