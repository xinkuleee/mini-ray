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


def _ack(_address, message):
    return protocol.InstallActorStateReply(message.owner_worker_id, message.snapshot, True)


def _gcs(nodes, actors, coordinator):
    # No transport, process, timer, or background service is constructed.
    gcs = object.__new__(GCSLite)
    gcs.nodes = nodes
    gcs.actors = actors
    gcs.actor_coordinator = coordinator
    gcs.placement_groups = None
    gcs.workers = None
    gcs._on_node_dead = None
    gcs.event_sink = EventSink()
    return gcs


def _report(death):
    return protocol.ReportNodeDeath(
        death.detection_id, death.node_id, death.node_pid,
        death.registration_epoch, death.exit_code, death.reason, death.detail,
    )


def test_node_loss_publishes_dead_without_reserving_a_survivor() -> None:
    nodes, actors, request, _old, _survivor, death = _fixture()
    installs, reserves = [], []
    def install(address, message):
        installs.append(message.snapshot)
        return _ack(address, message)
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *args: reserves.append(args),
        install_actor_state=install,
    )
    coordinator.fail_node(death.node_id, "managed Node exited")
    dead = actors.get(request.actor_id)
    assert dead.state is protocol.ActorState.DEAD
    assert dead.generation == request.generation and dead.restarts_used == 0
    assert dead.node_id is dead.worker_id is None
    assert installs == [dead] and reserves == []
    assert coordinator.node_failure_states_converged()
    assert coordinator.fail_node(death.node_id, "managed Node exited") == ()
    assert installs == [dead]


@pytest.mark.parametrize("available", (False, True))
def test_survivor_capacity_does_not_change_terminal_node_loss(available) -> None:
    nodes, actors, request, _old, _survivor, death = _fixture(survivor_available=available)
    reserves = []
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *args: reserves.append(args),
        install_actor_state=_ack,
    )
    coordinator.fail_node(death.node_id, "Node exited")
    assert actors.get(request.actor_id).state is protocol.ActorState.DEAD
    assert reserves == []


def test_lost_owner_install_ack_retains_dead_state_until_exact_replay() -> None:
    nodes, actors, request, _old, _survivor, death = _fixture()
    calls = []
    def install(address, message):
        calls.append(message)
        if len(calls) == 1:
            raise TimeoutError("owner accepted DEAD but ACK was lost")
        return _ack(address, message)
    coordinator = ActorCoordinator(nodes, actors, install_actor_state=install)
    coordinator.fail_node(death.node_id, "Node exited")
    dead = actors.get(request.actor_id)
    assert dead.state is protocol.ActorState.DEAD
    assert not coordinator.node_failure_states_converged()
    assert coordinator.active_operation_ids() == (request.actor_id,)
    assert coordinator.fail_node(death.node_id, "Node exited") == (dead,)
    assert calls[0] == calls[1]
    assert coordinator.node_failure_states_converged()
    assert coordinator.active_operation_ids() == ()


def test_gcs_node_death_reply_waits_for_dead_owner_install_ack() -> None:
    nodes, actors, request, _old, _survivor, death = _fixture()
    calls = []
    def install(address, message):
        calls.append(message)
        if len(calls) == 1:
            return protocol.InstallActorStateReply(message.owner_worker_id, message.snapshot, False, "busy")
        return _ack(address, message)
    coordinator = ActorCoordinator(nodes, actors, install_actor_state=install)
    gcs = _gcs(nodes, actors, coordinator)
    first = gcs.report_node_death(_report(death))
    assert not first.actor_state_converged
    dead = actors.get(request.actor_id)
    assert dead.state is protocol.ActorState.DEAD
    replay = gcs.report_node_death(_report(death))
    assert replay.actor_state_converged
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert actors.get(request.actor_id) == dead
    assert calls[0] == calls[1]


def test_drain_retries_dead_install_without_starting_any_restart() -> None:
    nodes, actors, request, _old, _survivor, death = _fixture()
    calls, reserves = [], []
    def install(address, message):
        calls.append(message)
        if len(calls) == 1:
            raise TimeoutError("lost ACK")
        return _ack(address, message)
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *args: reserves.append(args),
        install_actor_state=install,
    )
    coordinator.fail_node(death.node_id, "Node exited")
    assert coordinator.drain_once() == ()
    assert calls[0] == calls[1] and reserves == []
    assert actors.get(request.actor_id).state is protocol.ActorState.DEAD
