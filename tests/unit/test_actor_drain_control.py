from __future__ import annotations

import hashlib
from threading import Event, RLock

import pytest

from miniray import protocol
from miniray.control import ActorCoordinator, ActorRegistry, GCSLite, NodeRegistry
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, WorkerID
from miniray.resources import ResourceVector
from miniray.trace import EventSink


pytestmark = pytest.mark.unit


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _request(byte: int, *, max_restarts: int = 0) -> protocol.CreateActorRequest:
    actor_id = _id(ActorID, byte)
    payload = b"actor-class-" + bytes([byte])
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(_id(JobID, 1), "demo", "Counter", str(byte)),
        payload, hashlib.sha256(payload).hexdigest(), ("inc",),
    )
    return protocol.CreateActorRequest(
        actor_id, ActorGeneration(actor_id, 0), definition, b"ctor",
        ResourceVector({"CPU": 1}), _id(WorkerID, 2), max_restarts,
        ("127.0.0.1", 15999) if max_restarts else None,
    )


def _nodes() -> tuple[NodeRegistry, NodeID]:
    nodes = NodeRegistry()
    node_id = _id(NodeID, 3)
    nodes.register(
        node_id, ("127.0.0.1", 15003), ResourceVector({"CPU": 8}),
        node_pid=5003,
    )
    return nodes, node_id


def _reservation(request, node_id):
    return protocol.ReserveActorWorkerRequest(
        request.actor_id, request.generation, request.class_definition,
        request.constructor_payload, request.resources, request.owner_worker_id,
        node_id, 1,
    )


def _accepted(request, worker_byte=4):
    return protocol.ReserveActorWorkerReply(
        request.actor_id, request.generation, True, request.target_node_id,
        _id(WorkerID, worker_byte), ("127.0.0.1", 15004 + worker_byte),
        6000 + worker_byte,
    )


def _gcs(coordinator):
    gcs = object.__new__(GCSLite)
    gcs.actor_coordinator = coordinator
    gcs._actor_drain_request_id = None
    gcs._snapshot_lock = RLock()
    gcs.event_sink = EventSink()
    return gcs


def test_drain_cancels_only_unreserved_creating_actor() -> None:
    nodes, _node_id = _nodes()
    actors = ActorRegistry()
    request = _request(10)
    actors.begin(request)
    coordinator = ActorCoordinator(nodes, actors)

    assert coordinator.drain_once() == ()
    snapshot = actors.get(request.actor_id)
    assert snapshot.state is protocol.ActorState.DEAD
    assert "cancelled by drain" in (snapshot.error or "")


@pytest.mark.parametrize(("outcome", "state"), [
    ("accepted", protocol.ActorState.ALIVE),
    ("rejected", protocol.ActorState.DEAD),
    ("ambiguous", protocol.ActorState.CREATING),
])
def test_drain_redrives_frozen_create_to_each_typed_outcome(outcome, state) -> None:
    nodes, node_id = _nodes()
    actors = ActorRegistry()
    request = _request(11)
    actors.begin(request)
    frozen = actors.install_initial_reservation(request, _reservation(request, node_id))
    calls = []

    def reserve(_address, received):
        calls.append(received)
        if outcome == "accepted":
            return _accepted(received)
        if outcome == "rejected":
            return protocol.ReserveActorWorkerReply(
                received.actor_id, received.generation, False, error="ctor failed"
            )
        raise TimeoutError("reply unresolved")

    coordinator = ActorCoordinator(nodes, actors, reserve_actor_worker=reserve)
    active = coordinator.drain_once()

    assert calls == [frozen]
    assert actors.get(request.actor_id).state is state
    assert active == ((request.actor_id,) if state is protocol.ActorState.CREATING else ())


def test_drain_restarts_all_candidates_without_first_pending_short_circuit() -> None:
    nodes, node_id = _nodes()
    actors = ActorRegistry()
    first = _request(12)
    second = _request(13)
    for request in (first, second):
        actors.begin(request)
        actors.install_initial_reservation(request, _reservation(request, node_id))
    calls = []

    def reserve(_address, request):
        calls.append(request.actor_id)
        if request.actor_id == first.actor_id:
            raise TimeoutError("still unresolved")
        return _accepted(request, 9)

    coordinator = ActorCoordinator(nodes, actors, reserve_actor_worker=reserve)
    active = coordinator.drain_once()

    assert calls == [first.actor_id, second.actor_id]
    assert active == (first.actor_id,)
    assert actors.get(second.actor_id).state is protocol.ActorState.ALIVE


def test_drain_redrives_existing_restarting_obligation() -> None:
    nodes, node_id = _nodes()
    node = nodes.get(node_id)
    actors = ActorRegistry()
    request = _request(16, max_restarts=1)
    actors.begin(request)
    actors.install_initial_reservation(request, _reservation(request, node_id))
    initial = _accepted(_reservation(request, node_id), 16)
    actors.finish(
        request, protocol.CreateActorReply(
            request.actor_id, request.generation, True, node_id,
            initial.worker_id, initial.worker_address, initial.worker_pid,
            route_epoch=1,
        ),
    )
    exit_record = protocol.ActorWorkerExitRecord(
        "drain-restart", request.actor_id, request.generation, 1, node_id,
        node.node_pid, node.registration_epoch, initial.worker_id,
        initial.worker_pid, 19,
    )
    reserve_calls = []
    reject_owner = True

    def install(_address, install_request):
        if reject_owner:
            return protocol.InstallActorStateReply(
                install_request.owner_worker_id, install_request.snapshot, False,
                "owner temporarily unavailable",
            )
        return protocol.InstallActorStateReply(
            install_request.owner_worker_id, install_request.snapshot, True
        )

    def reserve(_address, reservation):
        reserve_calls.append(reservation)
        return _accepted(reservation, 17)

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve, install_actor_state=install
    )
    pending = coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(exit_record)
    )
    assert pending.disposition is protocol.ActorWorkerExitDisposition.RETRYABLE
    assert actors.get(request.actor_id).state is protocol.ActorState.RESTARTING
    assert reserve_calls == []

    reject_owner = False
    assert coordinator.drain_once() == ()
    assert len(reserve_calls) == 1
    assert reserve_calls[0].restart == exit_record
    assert actors.get(request.actor_id).state is protocol.ActorState.ALIVE


def test_gcs_actor_drain_exact_replay_drives_and_conflicting_id_is_rejected() -> None:
    nodes, node_id = _nodes()
    actors = ActorRegistry()
    request = _request(14)
    actors.begin(request)
    actors.install_initial_reservation(request, _reservation(request, node_id))
    attempts = 0

    def reserve(_address, received):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("pending")
        return _accepted(received)

    gcs = _gcs(ActorCoordinator(nodes, actors, reserve_actor_worker=reserve))
    drain = protocol.DrainActorsRequest("actors-1")
    first = gcs.drain_actors(drain)
    conflict = gcs.drain_actors(protocol.DrainActorsRequest("actors-2"))
    replay = gcs.drain_actors(drain)

    assert first.accepted and not first.clean
    assert first.active_actor_ids == (request.actor_id,)
    assert not conflict.accepted and not conflict.clean
    assert conflict.active_actor_ids == () and conflict.error
    assert replay.accepted and replay.clean and replay.active_actor_ids == ()


def test_final_shutdown_refuses_active_actor_operation() -> None:
    nodes, node_id = _nodes()
    actors = ActorRegistry()
    request = _request(15)
    actors.begin(request)
    actors.install_initial_reservation(request, _reservation(request, node_id))
    coordinator = ActorCoordinator(
        nodes, actors,
        reserve_actor_worker=lambda *_args: (_ for _ in ()).throw(
            TimeoutError("unresolved")
        ),
    )
    coordinator.close_admission()
    gcs = object.__new__(GCSLite)
    gcs.actor_coordinator = coordinator
    gcs.placement_groups = type("CleanPG", (), {
        "close_admission": lambda self: None,
        "has_active_operations": lambda self: False,
    })()
    gcs._snapshot_lock = RLock()
    gcs._shutdown_request_id = None
    gcs._shutdown_exit_scheduled = False
    gcs._stop_event = Event()
    gcs.event_sink = EventSink()

    reply = gcs.shutdown(protocol.Shutdown("shutdown-actors", "test"))

    assert not reply.clean
    assert not gcs._stop_event.is_set()


def test_actor_drain_protocol_has_strict_reply_shape() -> None:
    actor_id = _id(ActorID, 20)
    pending = protocol.DrainActorsReply(
        "drain", True, False, (actor_id,)
    )
    clean = protocol.DrainActorsReply("drain", True, True, ())
    rejected = protocol.DrainActorsReply(
        "other", False, False, (), "different request ID"
    )
    assert pending.active_actor_ids == (actor_id,)
    assert clean.clean and rejected.error
    with pytest.raises(Exception, match="non-empty"):
        protocol.DrainActorsRequest("")
    with pytest.raises(Exception, match="clean"):
        protocol.DrainActorsReply("drain", True, True, (actor_id,))
    with pytest.raises(Exception, match="active Actors"):
        protocol.DrainActorsReply("drain", True, False, ())
    with pytest.raises(Exception, match="rejected"):
        protocol.DrainActorsReply(
            "drain", False, False, (actor_id,), "conflict"
        )


def test_frozen_creating_actor_target_node_death_is_terminal_and_replay_installs() -> None:
    nodes, target = _nodes()
    other = _id(NodeID, 21)
    nodes.register(
        other, ("127.0.0.1", 15021), ResourceVector({"CPU": 1}),
        node_pid=5021,
    )
    actors = ActorRegistry()
    request = _request(22, max_restarts=1)
    actors.begin(request)
    actors.install_initial_reservation(request, _reservation(request, target))
    installs = []
    fail_first = True

    def install(_address, state_request):
        nonlocal fail_first
        installs.append(state_request.snapshot)
        if fail_first:
            fail_first = False
            raise TimeoutError("owner ACK lost")
        return protocol.InstallActorStateReply(
            state_request.owner_worker_id, state_request.snapshot, True
        )

    coordinator = ActorCoordinator(nodes, actors, install_actor_state=install)

    assert coordinator.fail_node(other, "other died") == ()
    assert actors.get(request.actor_id).state is protocol.ActorState.CREATING
    assert coordinator.fail_node(target, "target died") == ()
    dead = actors.get(request.actor_id)
    assert dead.state is protocol.ActorState.DEAD
    assert dead.route_epoch == 1
    assert dead.node_id is None and dead.worker_id is None
    assert actors.initial_reservation_for(request) is None
    assert coordinator.has_active_operations() is True

    replayed = coordinator.fail_node(target, "target died")
    assert replayed == (dead,)
    assert installs == [dead, dead]
    # Once the exact outstanding owner publication is acknowledged, another
    # death replay is an idempotent no-op.
    assert coordinator.fail_node(target, "target died") == ()
    assert coordinator.has_active_operations() is False


def test_actor_drain_flushes_pending_dead_publication_and_blocks_shutdown() -> None:
    nodes, target = _nodes()
    actors = ActorRegistry()
    request = _request(23, max_restarts=1)
    actors.begin(request)
    actors.install_initial_reservation(request, _reservation(request, target))
    attempts = 0

    def install(_address, state_request):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TimeoutError("owner ACK unavailable")
        return protocol.InstallActorStateReply(
            state_request.owner_worker_id, state_request.snapshot, True
        )

    coordinator = ActorCoordinator(nodes, actors, install_actor_state=install)
    assert coordinator.fail_node(target, "target died") == ()
    assert coordinator.active_operation_ids() == (request.actor_id,)

    gcs = object.__new__(GCSLite)
    gcs.actor_coordinator = coordinator
    gcs._actor_drain_request_id = None
    gcs.placement_groups = type("CleanPG", (), {
        "close_admission": lambda self: None,
        "has_active_operations": lambda self: False,
    })()
    gcs._snapshot_lock = RLock()
    gcs._shutdown_request_id = None
    gcs._shutdown_exit_scheduled = False
    gcs._stop_event = Event()
    gcs.event_sink = EventSink()

    shutdown = gcs.shutdown(protocol.Shutdown("pending-dead", "test"))
    assert not shutdown.clean and not gcs._stop_event.is_set()

    drain = protocol.DrainActorsRequest("dead-publication")
    first = gcs.drain_actors(drain)
    second = gcs.drain_actors(drain)

    assert first.accepted and not first.clean
    assert first.active_actor_ids == (request.actor_id,)
    assert second.accepted and second.clean
    assert second.active_actor_ids == ()
    assert attempts == 3
    assert not coordinator.has_active_operations()


def test_expected_node_unregister_retires_alive_actor_without_owner_rpc() -> None:
    nodes, target = _nodes()
    node = nodes.get(target)
    actors = ActorRegistry()
    request = _request(24, max_restarts=1)
    actors.begin(request)
    reservation = actors.install_initial_reservation(
        request, _reservation(request, target)
    )
    endpoint = _accepted(reservation, 24)
    actors.finish(
        request, protocol.CreateActorReply(
            request.actor_id, request.generation, True, target,
            endpoint.worker_id, endpoint.worker_address, endpoint.worker_pid,
            route_epoch=1,
        ),
    )
    owner_calls = []
    coordinator = ActorCoordinator(
        nodes, actors,
        install_actor_state=lambda *_args: owner_calls.append(_args),
    )
    gcs = object.__new__(GCSLite)
    gcs.nodes = nodes
    gcs.actor_coordinator = coordinator
    gcs._on_node_dead = None
    gcs.event_sink = EventSink()
    gcs._snapshot_lock = RLock()
    gcs._shutdown_request_id = None
    gcs._shutdown_exit_scheduled = False
    gcs._stop_event = Event()
    gcs.placement_groups = type("CleanPG", (), {
        "close_admission": lambda self: None,
        "has_active_operations": lambda self: False,
    })()

    unregister = protocol.UnregisterNode(
        target, node.node_pid, node.registration_epoch, "expected-finalize"
    )
    first = gcs.unregister_node(unregister)
    replay = gcs.unregister_node(unregister)

    assert first.removed and replay.removed
    dead = actors.get(request.actor_id)
    assert dead.state is protocol.ActorState.DEAD
    assert not coordinator.has_active_operations()
    assert owner_calls == []
    shutdown = gcs.shutdown(protocol.Shutdown("after-expected", "test"))
    assert shutdown.clean and gcs._stop_event.is_set()
