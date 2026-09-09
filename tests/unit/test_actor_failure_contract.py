"""Pure Node admission, typed startup failures, and local restart evidence.

The real Node handlers and spawn handshake run synchronously. The process
context below only exchanges in-memory startup messages: no process, socket,
thread, waiting loop, or imported test fixture is started by these cases.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import protocol
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, WorkerID
from miniray.node import NodeServer
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector


class _Ledger(ResourceLedger):
    def __init__(self) -> None:
        super().__init__(ResourceVector({"CPU": 1}))
        self.allocation_calls = 0
        self.release_calls = 0

    def try_allocate(self, request, token=None):
        self.allocation_calls += 1
        return super().try_allocate(request, token)

    def release(self, token: AllocationToken) -> bool:
        self.release_calls += 1
        return super().release(token)


class _Connection:
    def __init__(self) -> None:
        self.message = None
        self.closed = False

    def poll(self, _timeout):
        return self.message is not None

    def recv(self):
        assert self.message is not None
        return self.message

    def close(self) -> None:
        self.closed = True


class _Process:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.alive = True
        self.exitcode = None
        self.started = False
        self.closed = False
        self.join_calls = 0

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self.alive

    def join(self, _timeout=0) -> None:
        self.join_calls += 1

    def terminate(self) -> None:
        self.alive = False
        self.exitcode = -15

    def close(self) -> None:
        self.closed = True


class _Context:
    def __init__(self, failure=None) -> None:
        self.failure = failure
        self.parent = _Connection()
        self.child = _Connection()
        self.pipe_calls = 0
        self.processes = []

    def Pipe(self, *, duplex):
        assert duplex is False
        self.pipe_calls += 1
        self.parent = _Connection()
        self.child = _Connection()
        return self.parent, self.child

    def Process(self, *, target, args, name, daemon):
        assert callable(target) and name.startswith("miniray-actor-")
        assert daemon is False and args[7] is self.child
        process = _Process(9101 + len(self.processes))
        self.processes.append(process)
        if self.failure is not None:
            self.parent.message = (False, self.failure)
            process.alive = False
            process.exitcode = 1
        else:
            self.parent.message = (True, protocol.ActorWorkerStartup(
                args[0], args[1], args[2], process.pid, ("127.0.0.1", 14001)
            ))
        return process


def _node(*, failure=None) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._node_pid = 9001
    node._registration_epoch = 3
    node._state_lock = threading.RLock()
    node._ledger = _Ledger()
    node._cluster_nodes = ()
    node._gcs_address = None
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._server = SimpleNamespace(address=("127.0.0.1", 13001))
    node._host = "127.0.0.1"
    node._inline_threshold = 1024
    node._trace_config = None
    node._context = _Context(failure)
    return node


def _request(node_id: NodeID) -> protocol.ReserveActorWorkerRequest:
    actor_id = ActorID.random()
    payload = cloudpickle.dumps(type("Counter", (), {}))
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(JobID.random(), __name__, "Counter", "v1"),
        payload, hashlib.sha256(payload).hexdigest(), ("increment",),
    )
    return protocol.ReserveActorWorkerRequest(
        actor_id, ActorGeneration(actor_id, 0), definition,
        cloudpickle.dumps(((), {})), ResourceVector({"CPU": 1}),
        WorkerID.random(), node_id,
    )


def _retire_first_generation(node):
    request = _request(node.node_id)
    reply = node._handle_reserve_actor_worker(request)
    assert reply.accepted
    process = node._context.processes[0]
    process.alive = False
    process.exitcode = 17
    assert node._handle_unexpected_actor_worker_exit(
        request.actor_id, request.generation, process
    )
    proof = node._actor_generation_outcomes[request.generation].exit_record
    return request, reply, proof


@pytest.mark.unit
def test_capacity_rejection_is_typed_before_spawn() -> None:
    node = _node()
    request = replace(_request(node.node_id), resources=ResourceVector({"CPU": 2}))

    reply = node._handle_reserve_actor_worker(request)

    assert not reply.accepted
    assert reply.failure is protocol.ActorWorkerFailure.CAPACITY_UNAVAILABLE
    assert node._ledger.allocation_calls == 1
    assert node._ledger.release_calls == 0
    assert node._ledger.available == node._ledger.total
    assert node._context.pipe_calls == 0
    assert not node._actor_workers


@pytest.mark.unit
@pytest.mark.parametrize("stopping", ["stop_event", "drain_request"])
def test_stopping_node_rejects_before_allocation(stopping: str) -> None:
    node = _node()
    if stopping == "stop_event":
        node._stop_event.set()
    else:
        node._shutdown_request_id = "drain-1"

    reply = node._handle_reserve_actor_worker(_request(node.node_id))

    assert not reply.accepted
    assert reply.failure is protocol.ActorWorkerFailure.NODE_STOPPING
    assert node._ledger.allocation_calls == 0
    assert node._ledger.release_calls == 0
    assert node._context.pipe_calls == 0
    assert not node._actor_workers


@pytest.mark.unit
@pytest.mark.parametrize("failure", [
    protocol.ActorWorkerFailure.CONSTRUCTOR_FAILED,
    protocol.ActorWorkerFailure.STARTUP_FAILED,
])
def test_spawn_preserves_typed_failure_despite_resource_error_text(failure) -> None:
    message = "ValueError: resource unavailable in constructor application data"
    node = _node(failure=protocol.ActorWorkerStartupFailure(failure, message))

    reply = node._handle_reserve_actor_worker(_request(node.node_id))

    assert not reply.accepted
    assert reply.failure is failure
    assert message in reply.error
    assert node._ledger.allocation_calls == 1
    assert node._ledger.release_calls == 1
    assert node._ledger.available == node._ledger.total
    assert not node._actor_workers
    assert not node._actor_worker_ids_seen
    assert not node._actor_worker_pids_seen
    assert node._context.pipe_calls == 1
    assert node._context.parent.closed and node._context.child.closed
    child = node._context.processes[0]
    assert child.started and child.closed and child.join_calls == 1


@pytest.mark.unit
def test_foreign_node_exit_cannot_authorize_local_restart_or_allocate() -> None:
    node = _node()
    request, _, local_proof = _retire_first_generation(node)
    foreign_node = NodeID.random()
    foreign_proof = replace(local_proof, node_id=foreign_node)
    incoming = replace(
        request, generation=request.generation.next(), route_epoch=2,
        target_node_id=foreign_node, restart=foreign_proof,
    )
    # A decoded pickle can bypass __post_init__; Node must validate again.
    object.__setattr__(incoming, "target_node_id", node.node_id)

    reply = node._handle_reserve_actor_worker(incoming)

    assert not reply.accepted
    assert reply.failure is protocol.ActorWorkerFailure.INVALID_REQUEST
    assert "same-Node restart" in reply.error
    assert node._ledger.allocation_calls == 1
    assert node._ledger.available == node._ledger.total
    assert len(node._context.processes) == 1
    assert node._actor_generation_outcomes[request.generation].exit_record == local_proof
    assert not node._actor_workers


@pytest.mark.unit
def test_local_restart_requires_exact_retained_tombstone_before_allocation() -> None:
    node = _node()
    request, _, proof = _retire_first_generation(node)
    incoming = replace(
        request, generation=request.generation.next(), route_epoch=2,
        restart=replace(proof, detection_id="different-exit-proof"),
    )

    reply = node._handle_reserve_actor_worker(incoming)

    assert not reply.accepted
    assert reply.failure is protocol.ActorWorkerFailure.INVALID_REQUEST
    assert "exact prior tombstone" in reply.error
    assert node._ledger.allocation_calls == 1
    assert node._ledger.available == node._ledger.total
    assert len(node._context.processes) == 1
    assert node._actor_generation_outcomes[request.generation].exit_record == proof
    assert not node._actor_workers


@pytest.mark.unit
def test_exact_local_restart_replays_without_allocating_a_third_incarnation() -> None:
    node = _node()
    request, first_reply, proof = _retire_first_generation(node)
    incoming = replace(
        request, generation=request.generation.next(), route_epoch=2, restart=proof
    )

    reply = node._handle_reserve_actor_worker(incoming)

    assert reply.accepted and reply.failure is None
    assert reply.worker_id != first_reply.worker_id
    assert reply.worker_pid != first_reply.worker_pid
    assert node._handle_reserve_actor_worker(incoming) == reply
    assert node._handle_reserve_actor_worker(request) == first_reply
    assert node._ledger.allocation_calls == 2
    assert node._ledger.release_calls == 1
    assert node._ledger.available == ResourceVector({"CPU": 0})
    assert len(node._context.processes) == 2
    assert node._actor_workers[request.actor_id].request == incoming
