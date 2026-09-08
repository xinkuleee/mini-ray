from __future__ import annotations

from dataclasses import replace
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


def _death(
    node_id: NodeID, detection: str, death_epoch: int, *, expected: bool = False
) -> protocol.NodeDeathRecord:
    return protocol.NodeDeathRecord(
        detection, node_id, 7000 + death_epoch, death_epoch, death_epoch,
        0 if expected else -9,
        (
            protocol.NodeDeathReason.EXPECTED
            if expected
            else protocol.NodeDeathReason.PROCESS_EXIT
        ),
        "expected finalization" if expected else "managed Node exited",
    )


def _node(node_id: NodeID, pid: int, epoch: int) -> NodeSnapshot:
    return NodeSnapshot(
        node_id, pid, epoch, ("127.0.0.1", 18000 + epoch),
        ResourceVector({"CPU": 2}), ResourceVector({"CPU": 2}),
    )


def test_process_exit_consumes_one_restart_and_fences_the_old_route() -> None:
    actors, request, source, worker = _alive()
    death = _death(source, "source-death", 1)

    disposition = actors.accept_node_loss(request.actor_id, death)

    assert disposition is protocol.ActorNodeLossDisposition.APPLIED
    snapshot = actors.get(request.actor_id)
    assert snapshot.state is protocol.ActorState.RESTARTING
    assert snapshot.generation == request.generation.next()
    assert snapshot.restarts_used == 1
    assert snapshot.route_epoch == 2
    assert snapshot.node_id is snapshot.worker_id is None
    assert isinstance(snapshot.last_exit, protocol.ActorNodeLossRecord)
    assert snapshot.last_exit.node_death == death
    assert snapshot.last_exit.worker_id == worker
    assert actors.restart_reservation_for(snapshot.last_exit) is None


def test_exact_replay_conflict_expected_and_unrelated_are_zero_mutation() -> None:
    actors, request, source, _worker = _alive()
    before = actors.get(request.actor_id)
    unrelated = _death(_id(NodeID, 6), "unrelated", 1)
    expected = _death(source, "expected", 2, expected=True)

    assert actors.accept_node_loss(
        request.actor_id, unrelated
    ) is protocol.ActorNodeLossDisposition.UNRELATED
    assert actors.accept_node_loss(
        request.actor_id, expected
    ) is protocol.ActorNodeLossDisposition.IGNORED_EXPECTED
    assert actors.get(request.actor_id) == before

    death = _death(source, "source-death", 3)
    assert actors.accept_node_loss(
        request.actor_id, death
    ) is protocol.ActorNodeLossDisposition.APPLIED
    applied = actors.get(request.actor_id)
    assert actors.accept_node_loss(
        request.actor_id, death
    ) is protocol.ActorNodeLossDisposition.ALREADY_APPLIED
    conflict = replace(death, exit_code=-15, detail="different proof")
    assert actors.accept_node_loss(
        request.actor_id, conflict
    ) is protocol.ActorNodeLossDisposition.CONFLICT
    assert actors.get(request.actor_id) == applied


@pytest.mark.parametrize(
    ("max_restarts", "restart_allowed", "error"),
    [(0, True, "budget"), (1, False, "admission")],
)
def test_budget_or_closed_admission_commits_dead_without_new_generation(
    max_restarts: int, restart_allowed: bool, error: str,
) -> None:
    actors, request, source, _worker = _alive(max_restarts=max_restarts)

    assert actors.accept_node_loss(
        request.actor_id, _death(source, "terminal", 1),
        restart_allowed=restart_allowed,
    ) is protocol.ActorNodeLossDisposition.APPLIED

    snapshot = actors.get(request.actor_id)
    assert snapshot.state is protocol.ActorState.DEAD
    assert snapshot.generation == request.generation
    assert snapshot.restarts_used == 0
    assert snapshot.route_epoch == 2
    assert error in (snapshot.error or "").lower()


def test_frozen_target_death_replans_same_generation_and_budget() -> None:
    actors, request, source, _worker = _alive()
    source_death = _death(source, "source-death", 1)
    assert actors.accept_node_loss(
        request.actor_id, source_death
    ) is protocol.ActorNodeLossDisposition.APPLIED
    loss = actors.get(request.actor_id).last_exit
    assert isinstance(loss, protocol.ActorNodeLossRecord)

    first_target = _id(NodeID, 7)
    first_node = _node(first_target, 7002, 2)
    first_reservation = actors.install_migration_reservation(
        request.actor_id, first_node
    )
    assert first_reservation.generation == request.generation.next()
    assert first_reservation.route_epoch == 3
    assert first_reservation.restart == loss
    assert first_reservation.migration_failures == ()
    assert actors.install_migration_reservation(
        request.actor_id, first_node
    ) is first_reservation

    before_failure = actors.get(request.actor_id)
    target_death = _death(first_target, "target-death", 2)
    assert actors.accept_node_loss(
        request.actor_id, target_death
    ) is protocol.ActorNodeLossDisposition.APPLIED
    after_failure = actors.get(request.actor_id)
    assert (
        after_failure.generation, after_failure.restarts_used,
        after_failure.route_epoch, after_failure.state,
    ) == (
        before_failure.generation, before_failure.restarts_used,
        before_failure.route_epoch, protocol.ActorState.RESTARTING,
    )
    assert actors.restart_reservation_for(loss) is None

    second_target = _id(NodeID, 8)
    second_node = _node(second_target, 7003, 3)
    second_reservation = actors.install_migration_reservation(
        request.actor_id, second_node
    )
    assert second_reservation.generation == first_reservation.generation
    assert second_reservation.route_epoch == first_reservation.route_epoch
    assert second_reservation.restart == loss
    assert second_reservation.migration_failures == (target_death,)
    assert actors.accept_node_loss(
        request.actor_id, target_death
    ) is protocol.ActorNodeLossDisposition.ALREADY_APPLIED
    assert actors.get(request.actor_id) == after_failure
    conflicting_target = replace(
        target_death, detection_id="conflicting-target", exit_code=-15,
        detail="different target proof",
    )
    assert actors.accept_node_loss(
        request.actor_id, conflicting_target
    ) is protocol.ActorNodeLossDisposition.CONFLICT
    assert actors.get(request.actor_id) == after_failure

    live = actors.publish_restart(
        loss,
        protocol.ReserveActorWorkerReply(
            request.actor_id, second_reservation.generation, True,
            second_target, _id(WorkerID, 9),
            ("127.0.0.1", 18109), 8109,
        ),
    )
    assert live.state is protocol.ActorState.ALIVE
    assert live.node_id == second_target
    assert live.generation == request.generation.next()
    assert live.restarts_used == 1


def test_target_death_epoch_regression_is_rejected_before_mutation() -> None:
    actors, request, source, _worker = _alive()
    source_death = _death(source, "source-death", 3)
    assert actors.accept_node_loss(
        request.actor_id, source_death
    ) is protocol.ActorNodeLossDisposition.APPLIED
    target = _id(NodeID, 13)
    actors.install_migration_reservation(
        request.actor_id, _node(target, 7002, 2)
    )
    before = actors.get(request.actor_id)
    regressed = _death(target, "regressed-target", 2)

    assert actors.accept_node_loss(
        request.actor_id, regressed
    ) is protocol.ActorNodeLossDisposition.CONFLICT
    assert actors.get(request.actor_id) == before


def test_worker_exit_proof_is_same_node_only_then_node_loss_upgrades_it() -> None:
    actors, request, source, worker = _alive(max_restarts=2)
    node = NodeSnapshot(
        source, 7101, 1, ("127.0.0.1", 18104),
        ResourceVector({"CPU": 2}), ResourceVector({"CPU": 2}),
    )
    worker_exit = protocol.ActorWorkerExitRecord(
        "worker-exit", request.actor_id, request.generation, 1, source,
        node.node_pid, node.registration_epoch, worker, 8105, 17,
    )
    assert actors.accept_worker_exit(
        worker_exit, node
    ) is protocol.ActorWorkerExitDisposition.APPLIED
    same_node = actors.restart_reservation_for(worker_exit)
    assert same_node is not None and same_node.target_node_id == source
    with pytest.raises(ActorCreationStateError, match="Node-loss"):
        actors.install_migration_reservation(
            request.actor_id, _node(_id(NodeID, 10), 7110, 10)
        )
    wrong_incarnation = protocol.NodeDeathRecord(
        "wrong-node-incarnation", source, node.node_pid + 1,
        node.registration_epoch + 1, 2, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "other Node incarnation",
    )
    before_conflict = actors.get(request.actor_id)
    assert actors.accept_node_loss(
        request.actor_id, wrong_incarnation
    ) is protocol.ActorNodeLossDisposition.CONFLICT
    assert actors.get(request.actor_id) == before_conflict

    source_death = protocol.NodeDeathRecord(
        "node-after-worker", source, node.node_pid, node.registration_epoch,
        2, -9, protocol.NodeDeathReason.PROCESS_EXIT, "Node exited",
    )
    generation = actors.get(request.actor_id).generation
    budget = actors.get(request.actor_id).restarts_used
    assert actors.accept_node_loss(
        request.actor_id, source_death
    ) is protocol.ActorNodeLossDisposition.APPLIED
    upgraded = actors.get(request.actor_id)
    assert isinstance(upgraded.last_exit, protocol.ActorNodeLossRecord)
    assert upgraded.generation == generation
    assert upgraded.restarts_used == budget
    assert actors.restart_reservation_for(upgraded.last_exit) is None
    cross_node = actors.install_migration_reservation(
        request.actor_id, _node(_id(NodeID, 10), 7110, 10)
    )
    assert cross_node.target_node_id != source
    assert cross_node.generation == generation


def test_protocol_rejects_cross_node_worker_proof_and_dead_migration_target() -> None:
    actors, request, source, worker = _alive()
    worker_exit = protocol.ActorWorkerExitRecord(
        "worker", request.actor_id, request.generation, 1, source,
        7101, 1, worker, 8105, 17,
    )
    with pytest.raises(Exception, match="same-Node"):
        protocol.ReserveActorWorkerRequest(
            request.actor_id, request.generation.next(),
            request.class_definition, request.constructor_payload,
            request.resources, request.owner_worker_id, _id(NodeID, 11),
            3, worker_exit,
        )

    source_death = _death(source, "source", 1)
    loss = protocol.ActorNodeLossRecord(
        request.actor_id, request.generation, 1, worker, 8105, source_death,
        request.class_definition, request.constructor_payload, request.resources,
        request.owner_worker_id,
    )
    with pytest.raises(Exception, match="cross-Node"):
        protocol.ReserveActorWorkerRequest(
            request.actor_id, request.generation.next(),
            request.class_definition, request.constructor_payload,
            request.resources, request.owner_worker_id, source, 3, loss,
        )
    target = _id(NodeID, 12)
    target_death = _death(target, "target", 2)
    with pytest.raises(Exception, match="already fenced"):
        protocol.ReserveActorWorkerRequest(
            request.actor_id, request.generation.next(),
            request.class_definition, request.constructor_payload,
            request.resources, request.owner_worker_id, target, 3, loss,
            (target_death,),
        )
