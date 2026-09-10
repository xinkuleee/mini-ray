"""Pure contracts for the worker-lease execution handshake.

The tests call protocol constructors and the real Node/Worker handlers directly.
They deliberately create no TCP listener and no child process.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _WorkerSlot
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_handoff import OutputHandoffTable
from miniray.resources import (
    AllocationToken,
    NodeSnapshot,
    ResourceLedger,
    ResourceVector,
)
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerServer,
)
from tests.unit._pure_node_output_current import prepare_ref_free_output


pytestmark = pytest.mark.unit


from tests.support._worker_protocol import initialize_worker_protocol

@pytest.fixture
def _no_worker_runtime(monkeypatch):
    """Guard explicit Node/Worker publication cases without a file-wide fixture."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("Worker handshake attempted real runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr("miniray.worker.CoreWorker.__init__", forbidden)
    monkeypatch.setattr("miniray.worker.TCPServer.__init__", forbidden)
    monkeypatch.setattr(NodeServer, "__init__", forbidden)
    monkeypatch.setattr(WorkerServer, "__init__", forbidden)


class _AliveWorker:
    def is_alive(self) -> bool:
        return True


class _CountingLedger(ResourceLedger):
    """A real ledger with an observable physical release boundary."""

    def __init__(self, total: ResourceVector) -> None:
        super().__init__(total)
        self.release_calls = 0

    def release(self, token: AllocationToken) -> bool:
        self.release_calls += 1
        return super().release(token)


def _task_identity(index: int = 0) -> tuple[TaskID, AttemptID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return task_id, AttemptID(task_id, 0)


def _node_without_transport(
    node_id: NodeID, worker_id: WorkerID, total: ResourceVector
) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = node_id
    node._ledger = _CountingLedger(total)
    node._gcs_address = None
    node._registered_with_gcs = True
    node._node_pid = 21001
    node._registration_epoch = 3
    node._cluster_nodes = (NodeSnapshot(node_id, total, total),)
    node._cluster_addresses = {}
    node._workers = {worker_id: _WorkerSlot(worker_id, process=_AliveWorker(),
        address=("127.0.0.1", 19001), pid=21002)}
    node._worker_order = (worker_id,)
    node._next_worker_cursor = 0
    node._shutdown_request_id = None
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node._stop_event = threading.Event()
    return node


def _grant(
    node: NodeServer, resources: ResourceVector, *, num_returns: int = 0,
) -> tuple[protocol.RequestWorkerLease, protocol.GrantWorkerLease]:
    task_id, attempt_id = _task_identity()
    request = protocol.RequestWorkerLease(
        lease_id=LeaseID.random(),
        task_id=task_id,
        attempt_id=attempt_id,
        resources=resources,
        requester_node_id=NodeID.random(),
        requester_worker_id=WorkerID.random(),
        target_node_id=node.node_id,
        return_ids=tuple(ObjectID.for_task(task_id, index) for index in range(num_returns)),
    )
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    return request, grant


def _start(
    request: protocol.RequestWorkerLease, grant: protocol.GrantWorkerLease
) -> protocol.StartWorkerLease:
    return protocol.StartWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id
    )


def _complete(
    request: protocol.RequestWorkerLease,
    grant: protocol.GrantWorkerLease,
    status: protocol.TaskReplyStatus = protocol.TaskReplyStatus.SUCCEEDED,
) -> protocol.CompleteWorkerLease:
    return protocol.CompleteWorkerLease(
        request.lease_id,
        request.task_id,
        request.attempt_id,
        grant.worker_id,
        status,
    )


def test_protocol_rejects_cross_task_execution_identity() -> None:
    task_id, _ = _task_identity(0)
    other_task_id, other_attempt_id = _task_identity(1)
    lease_id = LeaseID.random()
    worker_id = WorkerID.random()

    assert task_id != other_task_id
    with pytest.raises(ProtocolError, match="attempt_id must belong to task_id"):
        protocol.StartWorkerLease(
            lease_id, task_id, other_attempt_id, worker_id
        )
    with pytest.raises(ProtocolError, match="attempt_id must belong to task_id"):
        protocol.CompleteWorkerLease(
            lease_id,
            task_id,
            other_attempt_id,
            worker_id,
            protocol.TaskReplyStatus.SYSTEM_ERROR,
        )


def test_node_fences_wrong_worker_task_and_attempt_identity() -> None:
    node_id = NodeID.random()
    worker_id = WorkerID.random()
    resources = ResourceVector({"CPU": 1})
    node = _node_without_transport(node_id, worker_id, resources)
    request, grant = _grant(node, resources)

    wrong_worker = protocol.StartWorkerLease(
        request.lease_id,
        request.task_id,
        request.attempt_id,
        WorkerID.random(),
    )
    wrong_worker_reply = node._handle_start_worker_lease(wrong_worker)
    assert not wrong_worker_reply.accepted
    assert wrong_worker_reply.state is protocol.LeaseExecutionState.GRANTED

    other_task_id, other_attempt_id = _task_identity(2)
    wrong_task = protocol.StartWorkerLease(
        request.lease_id, other_task_id, other_attempt_id, grant.worker_id
    )
    wrong_task_reply = node._handle_start_worker_lease(wrong_task)
    assert not wrong_task_reply.accepted
    assert wrong_task_reply.state is protocol.LeaseExecutionState.GRANTED

    wrong_attempt = protocol.StartWorkerLease(
        request.lease_id,
        request.task_id,
        request.attempt_id.next(),
        grant.worker_id,
    )
    wrong_attempt_reply = node._handle_start_worker_lease(wrong_attempt)
    assert not wrong_attempt_reply.accepted
    assert wrong_attempt_reply.state is protocol.LeaseExecutionState.GRANTED
    assert node.resource_ledger.available.is_zero()

    started = node._handle_start_worker_lease(_start(request, grant))
    assert started.accepted
    assert started.state is protocol.LeaseExecutionState.RUNNING

    wrong_completion = protocol.CompleteWorkerLease(
        request.lease_id,
        request.task_id,
        request.attempt_id,
        WorkerID.random(),
        protocol.TaskReplyStatus.SUCCEEDED,
    )
    completion_reply = node._handle_complete_worker_lease(wrong_completion)
    assert not completion_reply.accepted
    assert not completion_reply.released
    assert completion_reply.state is protocol.LeaseExecutionState.RUNNING
    assert node.resource_ledger.available.is_zero()
    assert node._ledger.release_calls == 0


def test_start_delivered_to_a_different_node_is_rejected() -> None:
    resources = ResourceVector({"CPU": 1})
    granting_node = _node_without_transport(
        NodeID.random(), WorkerID.random(), resources
    )
    other_node = _node_without_transport(
        NodeID.random(), WorkerID.random(), resources
    )
    request, grant = _grant(granting_node, resources)

    reply = other_node._handle_start_worker_lease(_start(request, grant))

    assert not reply.accepted
    assert reply.state is protocol.LeaseExecutionState.ABANDONED
    assert "unknown worker lease" in reply.error
    assert other_node.resource_ledger.available == resources
    assert granting_node._leases[request.lease_id].state is (
        protocol.LeaseExecutionState.GRANTED
    )


@pytest.mark.usefixtures("_no_worker_runtime")
def test_granted_running_completed_releases_exactly_once() -> None:
    node_id = NodeID.random()
    worker_id = WorkerID.random()
    resources = ResourceVector({"CPU": 1})
    node = _node_without_transport(node_id, worker_id, resources)
    request, grant = _grant(node, resources, num_returns=1)

    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.GRANTED
    assert node.resource_ledger.available.is_zero()

    first_start = node._handle_start_worker_lease(_start(request, grant))
    duplicate_start = node._handle_start_worker_lease(_start(request, grant))
    assert first_start.accepted and duplicate_start == first_start
    assert first_start.state is protocol.LeaseExecutionState.RUNNING

    premature_release = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            request.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert not premature_release.released
    assert "must complete" in premature_release.detail
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.RUNNING
    assert node.resource_ledger.available.is_zero()
    assert node._ledger.release_calls == 0

    publication = prepare_ref_free_output(node, request, grant)
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.RUNNING
    assert node._ledger.release_calls == 0
    assert node.resource_ledger.available.is_zero()
    completion = _complete(request, grant)
    first_complete = node._handle_complete_worker_lease(completion)
    duplicate_complete = node._handle_complete_worker_lease(completion)

    assert first_complete.accepted and first_complete.released
    assert first_complete.state is protocol.LeaseExecutionState.COMPLETED
    assert duplicate_complete.accepted and not duplicate_complete.released
    assert duplicate_complete.state is protocol.LeaseExecutionState.COMPLETED
    assert first_complete.output_publication.manifest == publication.manifest
    assert duplicate_complete.output_publication == first_complete.output_publication
    assert publication.journal.snapshot(publication.manifest.publication_id).complete == (
        first_complete.output_publication.complete
    )
    assert publication.handoffs.query(publication.manifest.publication_id).complete is None
    assert node._ledger.release_calls == 1
    assert node.resource_ledger.available == resources
    assert node._workers[node.worker_id].active_lease_id is None

    post_complete_release = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            request.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert not post_complete_release.released
    assert node._ledger.release_calls == 1


@pytest.mark.usefixtures("_no_worker_runtime")
def test_completion_replay_cannot_change_terminal_status() -> None:
    resources = ResourceVector({"CPU": 1})
    node = _node_without_transport(
        NodeID.random(), WorkerID.random(), resources
    )
    request, grant = _grant(node, resources, num_returns=1)
    assert node._handle_start_worker_lease(_start(request, grant)).accepted
    publication = prepare_ref_free_output(node, request, grant)
    completion = _complete(request, grant)
    first_complete = node._handle_complete_worker_lease(completion)
    assert first_complete.accepted
    assert first_complete.output_publication.manifest == publication.manifest
    witness = first_complete.output_publication.complete

    changed = node._handle_complete_worker_lease(
        _complete(request, grant, protocol.TaskReplyStatus.SYSTEM_ERROR)
    )
    assert not changed.accepted
    assert not changed.released
    assert changed.state is protocol.LeaseExecutionState.COMPLETED
    assert "terminal was already chosen" in changed.error
    assert node._leases[request.lease_id].completion == completion
    assert publication.journal.snapshot(publication.manifest.publication_id).complete == witness
    replay = node._handle_complete_worker_lease(completion)
    assert replay.accepted and not replay.released
    assert replay.output_publication == first_complete.output_publication
    assert node._ledger.release_calls == 1
    assert node.resource_ledger.available == resources


@pytest.mark.usefixtures("_no_worker_runtime")
def test_worker_retries_cached_completion_without_rerunning_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ambiguous Complete reply loss retries metadata, not user code."""

    resources = ResourceVector({"CPU": 1})
    node = _node_without_transport(
        NodeID.random(), WorkerID.random(), resources
    )
    request, grant = _grant(node, resources, num_returns=1)
    handoffs = OutputHandoffTable()
    node._output_publication_journal = OutputPublicationJournal()

    def unexpected_effect(*_args, **_kwargs):
        pytest.fail("ref-free INLINE handshake attempted a child/graph/store effect")

    def register_owner(manifest):
        snapshot = handoffs.register(manifest, manifest.publication_id.attempt_id)
        reply = wire.OutputHandoffReply(wire.RegisterOutputHandoff(manifest), True, snapshot)
        assert reply.snapshot.manifest == manifest

    def report_complete(witness):
        snapshot = handoffs.record_complete(witness)
        reply = wire.OutputHandoffReply(wire.ReportOutputHandoffComplete(witness), True, snapshot)
        assert reply.snapshot.complete == witness

    node._output_publications = OutputPublicationNodeAdapter(
        node._output_publication_journal, register_owner=register_owner,
        report_complete=report_complete, report_rollback=unexpected_effect,
        prepare_child=unexpected_effect, promote_child=unexpected_effect,
        release_child=unexpected_effect, seal_replica=unexpected_effect, drop_replica=unexpected_effect,
    )
    job_id = JobID.random()
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job_id, __name__, "counting_callable", "v1"),
        b"test callable payload",
    )
    spec = protocol.TaskSpec(
        job_id=job_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
        function=definition.key,
        args=(),
        num_returns=1,
        resources=resources,
        owner_worker_id=request.requester_worker_id,
        function_definition=definition,
    )
    push = protocol.PushTask(request.lease_id, grant.worker_id, spec)

    worker = object.__new__(WorkerServer)
    initialize_worker_protocol(worker)
    worker.worker_id = grant.worker_id
    worker.node_id = node.node_id
    worker.node_address = ("127.0.0.1", 19000)
    worker.inline_threshold = 1024
    worker._execution_lock = threading.Lock()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._accepting_tasks = True
    worker._active_tasks = 0

    executions = 0
    reductions = 0
    rpc_events: list[str] = []
    completion_calls = 0
    prepares = []
    completion_replies = []

    class OnceResult:
        def __reduce__(self):
            nonlocal reductions
            reductions += 1
            return str, ("once",)

    def counting_callable():
        nonlocal executions
        executions += 1
        return OnceResult()

    real_cloudpickle_loads = cloudpickle.loads
    monkeypatch.setattr(
        "miniray.worker.cloudpickle.loads", lambda _: counting_callable
    )

    def fake_rpc(address, handler, message):
        nonlocal completion_calls
        assert address == worker.node_address
        rpc_events.append(handler)
        if handler == START_WORKER_LEASE_HANDLER:
            return node._handle_start_worker_lease(message)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            assert type(message) is wire.PrepareOutputPublication
            prepares.append(message)
            return node._handle_prepare_output_publication(message)
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            completion_calls += 1
            reply = node._handle_complete_worker_lease(message)
            assert reply.accepted and reply.output_publication is not None
            completion_replies.append(reply)
            if completion_calls == 1:
                # The Node committed and released, but the response was lost.
                raise ConnectionError("completion acknowledgement was lost")
            return reply
        raise AssertionError("unexpected worker RPC handler: {!r}".format(handler))

    monkeypatch.setattr("miniray.worker.rpc_request", fake_rpc)

    with pytest.raises(RuntimeError, match="exact PushTask replay") as lost:
        worker._handle_push_task(push)

    assert isinstance(lost.value.__cause__, ConnectionError)
    key = request.attempt_id, request.lease_id
    pending = worker._prepared_output_replies[key]
    assert executions == reductions == 1 and len(prepares) == 1
    assert pending.outputs.manifest == prepares[0].manifest
    assert pending.outputs.slot_payloads == prepares[0].slot_payloads
    assert pending.prepare_acked and pending.complete_envelope is None
    assert not worker._replies and not worker._cached_pushes
    assert not worker._completion_acked
    assert node._leases[request.lease_id].state is (
        protocol.LeaseExecutionState.COMPLETED
    )
    assert node._ledger.release_calls == 1
    assert node.resource_ledger.available == resources

    cached = worker._handle_push_task(push)
    cached_again = worker._handle_push_task(push)

    assert cached is worker._replies[(request.attempt_id, request.lease_id)]
    assert cached_again is cached
    assert cached.output_publication == completion_replies[0].output_publication
    assert cached.output_publication == completion_replies[1].output_publication
    assert cached.output_publication.manifest == pending.outputs.manifest
    assert not hasattr(cached, "stored_publication") and not hasattr(cached, "inline_publication")
    assert all(not hasattr(reply, field) for reply in completion_replies
               for field in ("stored_publication", "inline_publication"))
    assert cached.results[0].inline_data == pending.outputs.slot_payloads[0]
    assert real_cloudpickle_loads(cached.results[0].inline_data) == "once"
    assert executions == reductions == 1 and len(prepares) == 1
    assert not worker._prepared_output_replies
    assert completion_calls == 2
    assert rpc_events == [
        START_WORKER_LEASE_HANDLER,
        wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
        COMPLETE_WORKER_LEASE_HANDLER,
        COMPLETE_WORKER_LEASE_HANDLER,
    ]
    assert worker._completion_acked == {
        (request.attempt_id, request.lease_id)
    }
    assert node._ledger.release_calls == 1
    assert completion_replies[0].released and not completion_replies[1].released
    # Node local Complete is authoritative even before the owner terminal outbox.
    publication = pending.outputs.manifest.publication_id
    assert handoffs.query(publication).complete is None
    assert node._output_publications.report_terminal(publication)
    assert handoffs.query(publication).complete == cached.output_publication.complete
