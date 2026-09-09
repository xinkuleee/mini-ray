from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.control import (
    ActorCreationStateError, ActorRegistry, NodeSnapshot,
)
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _request(*, max_restarts: int = 2) -> protocol.CreateActorRequest:
    actor_id = _id(ActorID, 1)
    payload = b"actor-node-loss"
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(_id(JobID, 2), "demo", "Counter", "v1"),
        payload, hashlib.sha256(payload).hexdigest(), ("inc",),
    )
    return protocol.CreateActorRequest(
        actor_id, ActorGeneration(actor_id, 0), definition, b"constructor",
        ResourceVector({"CPU": 1}), _id(WorkerID, 3), max_restarts,
        ("127.0.0.1", 18103) if max_restarts else None,
    )


def _alive(*, max_restarts: int = 2):
    request = _request(max_restarts=max_restarts)
    source = _id(NodeID, 4)
    worker = _id(WorkerID, 5)
    actors = ActorRegistry()
    assert actors.begin(request)
    actors.install_initial_reservation(
        request,
        protocol.ReserveActorWorkerRequest(
            request.actor_id, request.generation, request.class_definition,
            request.constructor_payload, request.resources,
            request.owner_worker_id, source, 1,
        ),
    )
    assert actors.finish(
        request,
        protocol.CreateActorReply(
            request.actor_id, request.generation, True, source, worker,
            ("127.0.0.1", 18105), 8105, route_epoch=1,
        ),
    )
    return actors, request, source, worker


def _node(node_id: NodeID, pid: int, epoch: int) -> NodeSnapshot:
    return NodeSnapshot(
        node_id, pid, epoch, ("127.0.0.1", 18000 + epoch),
        ResourceVector({"CPU": 2}), ResourceVector({"CPU": 2}),
    )


@pytest.mark.parametrize("max_restarts", (0, 2))
def test_node_failure_commits_dead_without_consuming_restart(max_restarts) -> None:
    actors, request, source, _worker = _alive(max_restarts=max_restarts)
    dead = actors.fail_actor_on_node(request.actor_id, source, "managed Node exited")
    assert dead.state is protocol.ActorState.DEAD
    assert dead.generation == request.generation
    assert dead.restarts_used == 0
    assert dead.route_epoch == 2
    assert dead.node_id is dead.worker_id is dead.worker_address is dead.worker_pid is None
    assert dead.last_exit is None
    assert actors.active_actor_ids() == ()


def test_repeated_or_unrelated_node_failure_cannot_mutate_terminal_state() -> None:
    actors, request, source, _worker = _alive()
    before = actors.get(request.actor_id)
    assert actors.fail_actor_on_node(request.actor_id, _id(NodeID, 6), "other") is None
    assert actors.get(request.actor_id) == before
    dead = actors.fail_actor_on_node(request.actor_id, source, "Node exited")
    assert actors.fail_actor_on_node(request.actor_id, source, "replayed") is None
    assert actors.get(request.actor_id) == dead


def test_node_failure_during_same_node_restart_is_dead_without_replanning() -> None:
    actors, request, source, worker = _alive()
    node = _node(source, 7101, 1)
    exited = protocol.ActorWorkerExitRecord(
        "worker-exit", request.actor_id, request.generation, 1, source,
        node.node_pid, node.registration_epoch, worker, 8105, 17,
    )
    assert actors.accept_worker_exit(exited, node) is protocol.ActorWorkerExitDisposition.APPLIED
    reservation = actors.restart_reservation_for(exited)
    assert reservation.target_node_id == source
    pending = actors.get(request.actor_id)
    dead = actors.fail_actor_on_node(request.actor_id, source, "Node died during restart")
    assert dead.state is protocol.ActorState.DEAD
    assert dead.generation == pending.generation and dead.restarts_used == 1
    assert dead.route_epoch > pending.route_epoch
    assert dead.last_exit == exited
    assert actors.restart_reservation_for(exited) is None
    late = protocol.ReserveActorWorkerReply(
        request.actor_id, pending.generation, True, source,
        _id(WorkerID, 9), ("127.0.0.1", 18109), 8109,
    )
    with pytest.raises(ActorCreationStateError):
        actors.publish_restart(exited, late)
    assert actors.get(request.actor_id) == dead


def test_node_failure_fences_a_pending_initial_reservation() -> None:
    actors = ActorRegistry()
    request = _request()
    source = _id(NodeID, 4)
    assert actors.begin(request)
    actors.install_initial_reservation(
        request, protocol.ReserveActorWorkerRequest(
            request.actor_id, request.generation, request.class_definition,
            request.constructor_payload, request.resources, request.owner_worker_id,
            source, 1,
        ),
    )
    dead = actors.fail_actor_on_node(request.actor_id, source, "Node died during create")
    assert dead.state is protocol.ActorState.DEAD
    assert dead.generation == request.generation and dead.restarts_used == 0
    assert actors.initial_reservation_for(request) is None


def test_worker_exit_proof_cannot_authorize_cross_node_restart() -> None:
    _actors, request, source, worker = _alive()
    exited = protocol.ActorWorkerExitRecord(
        "worker", request.actor_id, request.generation, 1, source,
        7101, 1, worker, 8105, 17,
    )
    with pytest.raises(protocol.ProtocolError, match="same-Node"):
        protocol.ReserveActorWorkerRequest(
            request.actor_id, request.generation.next(), request.class_definition,
            request.constructor_payload, request.resources, request.owner_worker_id,
            _id(NodeID, 11), 3, exited,
        )
