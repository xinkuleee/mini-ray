from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.control import ActorCoordinator, ActorRegistry, GCSLite, NodeRegistry
from miniray.trace import EventSink
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _fixture(*, survivor_available: bool = True):
    nodes = NodeRegistry()
    source, survivor = _id(NodeID, 1), _id(NodeID, 2)
    nodes.register(
        source, ("127.0.0.1", 19101), ResourceVector({"CPU": 1}),
        node_pid=9101,
    )
    nodes.register(
        survivor, ("127.0.0.1", 19102), ResourceVector({"CPU": 1}),
        node_pid=9102,
        available_resources=(
            ResourceVector({"CPU": 1})
            if survivor_available else ResourceVector()
        ),
    )
    actor = _id(ActorID, 3)
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(_id(JobID, 4), "demo", "Counter", "v1"),
        b"actor", hashlib.sha256(b"actor").hexdigest(), ("inc",),
    )
    request = protocol.CreateActorRequest(
        actor, ActorGeneration(actor, 0), definition, b"ctor",
        ResourceVector({"CPU": 1}), _id(WorkerID, 5), 2,
        ("127.0.0.1", 19105),
    )
    actors = ActorRegistry()
    actors.begin(request)
    actors.install_initial_reservation(
        request, protocol.ReserveActorWorkerRequest(
            actor, request.generation, definition, request.constructor_payload,
            request.resources, request.owner_worker_id, source, 1,
        ),
    )
    old_worker = _id(WorkerID, 6)
    actors.finish(
        request, protocol.CreateActorReply(
            actor, request.generation, True, source, old_worker,
            ("127.0.0.1", 19106), 9106, route_epoch=1,
        ),
    )
    registration = nodes.get(source)
    death_reply = nodes.report_death(protocol.ReportNodeDeath(
        "source-death", source, registration.node_pid,
        registration.registration_epoch, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "source exited",
    ))
    assert death_reply.death is not None
    return nodes, actors, request, old_worker, survivor, death_reply.death


def _accepted(reservation: protocol.ReserveActorWorkerRequest):
    return protocol.ReserveActorWorkerReply(
        reservation.actor_id, reservation.generation, True,
        reservation.target_node_id, _id(WorkerID, 7),
        ("127.0.0.1", 19107), 9107,
    )


def test_node_loss_orders_owner_fence_reserve_and_alive_publication() -> None:
    nodes, actors, request, old_worker, survivor, death = _fixture()
    events = []

    def install(_address, message):
        events.append(("install", message.snapshot))
        return protocol.InstallActorStateReply(
            message.owner_worker_id, message.snapshot, True
        )

    def reserve(address, reservation):
        events.append(("reserve", address, reservation))
        return _accepted(reservation)

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve,
        install_actor_state=install,
    )

    assert coordinator.migrate_node_loss(death)

    assert [item[0] for item in events] == ["install", "reserve", "install"]
    restarting = events[0][1]
    reservation = events[1][2]
    alive = events[2][1]
    assert restarting.state is protocol.ActorState.RESTARTING
    assert restarting.node_id is restarting.worker_id is None
    assert reservation.target_node_id == survivor
    assert isinstance(reservation.restart, protocol.ActorNodeLossRecord)
    assert reservation.restart.worker_id == old_worker
    assert alive.state is protocol.ActorState.ALIVE
    assert alive.node_id == survivor and alive.generation == request.generation.next()
    assert coordinator.migrate_node_loss(death)
    assert len(events) == 3


def test_capacity_pending_redrives_without_advancing_generation_or_budget() -> None:
    nodes, actors, request, _old, survivor, death = _fixture(
        survivor_available=False
    )
    installs = []
    reserves = []
    coordinator = ActorCoordinator(
        nodes, actors,
        reserve_actor_worker=lambda *_args: reserves.append(_args),
        install_actor_state=lambda _address, message: (
            installs.append(message.snapshot)
            or protocol.InstallActorStateReply(
                message.owner_worker_id, message.snapshot, True
            )
        ),
    )

    assert not coordinator.migrate_node_loss(death)
    pending = actors.get(request.actor_id)
    assert pending.state is protocol.ActorState.RESTARTING
    assert pending.generation == request.generation.next()
    assert pending.restarts_used == 1
    assert len(installs) == 1 and reserves == []
    assert not coordinator.migrate_node_loss(death)
    assert actors.get(request.actor_id) == pending
    assert len(installs) == 1 and reserves == []

    survivor_record = nodes.get(survivor)
    nodes.update_resources(
        survivor, survivor_record.node_pid, survivor_record.registration_epoch,
        1, ResourceVector({"CPU": 1}),
    )
    coordinator._reserve_actor_worker = lambda _address, reservation: (
        reserves.append(reservation) or _accepted(reservation)
    )
    assert coordinator.migrate_node_loss(death)
    assert len(reserves) == 1
    assert actors.get(request.actor_id).restarts_used == 1


def test_shutdown_cancels_frozen_migration_and_publishes_dead() -> None:
    nodes, actors, request, _old, _survivor, death = _fixture(
        survivor_available=False
    )
    publications = []
    coordinator = ActorCoordinator(
        nodes, actors,
        install_actor_state=lambda _address, message: (
            publications.append(message.snapshot)
            or protocol.InstallActorStateReply(
                message.owner_worker_id, message.snapshot, True
            )
        ),
    )
    assert not coordinator.migrate_node_loss(death)

    assert coordinator.drain_once() == ()

    dead = actors.get(request.actor_id)
    assert dead.state is protocol.ActorState.DEAD
    assert "cancelled by drain" in (dead.error or "")
    assert [item.state for item in publications] == [
        protocol.ActorState.RESTARTING, protocol.ActorState.DEAD,
    ]


def test_gcs_node_death_reply_is_not_ack_clean_until_migration_converges() -> None:
    nodes, actors, request, _old, survivor, death = _fixture(
        survivor_available=False
    )
    coordinator = ActorCoordinator(
        nodes, actors,
        install_actor_state=lambda _address, message:
            protocol.InstallActorStateReply(
                message.owner_worker_id, message.snapshot, True
            ),
    )
    gcs = object.__new__(GCSLite)
    gcs.nodes = nodes
    gcs.actors = actors
    gcs.actor_coordinator = coordinator
    gcs.placement_groups = None
    gcs.workers = None
    gcs._on_node_dead = None
    gcs.event_sink = EventSink()
    request_death = protocol.ReportNodeDeath(
        death.detection_id, death.node_id, death.node_pid,
        death.registration_epoch, death.exit_code, death.reason, death.detail,
    )

    first = gcs.report_node_death(request_death)
    assert not first.actor_migration_converged
    pending = actors.get(request.actor_id)
    assert pending.state is protocol.ActorState.RESTARTING

    survivor_record = nodes.get(survivor)
    nodes.update_resources(
        survivor, survivor_record.node_pid, survivor_record.registration_epoch,
        1, ResourceVector({"CPU": 1}),
    )
    coordinator._reserve_actor_worker = (
        lambda _address, reservation: _accepted(reservation)
    )
    replay = gcs.report_node_death(request_death)
    assert replay.actor_migration_converged
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert actors.get(request.actor_id).state is protocol.ActorState.ALIVE
