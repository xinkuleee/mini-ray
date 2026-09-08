"""Actor control contracts with separate synchronous and threaded scopes.

The unit cases use only tiny registries and injected synchronous callbacks.
The pending-creation case starts a real thread and waits on Events; it remains
heavy until its exact lifecycle receives a separate bounded-execution review.
"""

from __future__ import annotations

import hashlib
import threading

import pytest

from miniray import protocol
from miniray.control import (
    ACTOR_STATUS,
    ActorCoordinator,
    ActorCreationState,
    ActorRegistry,
    GCSLite,
    NodeRegistry,
)
from miniray.ids import (
    ActorGeneration,
    ActorID,
    JobID,
    NodeID,
    WorkerID,
)
from miniray.resources import ResourceVector


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _request(
    actor_byte: int = 1,
    *,
    constructor_payload: bytes = b"constructor",
    resources: ResourceVector = ResourceVector({"CPU": 1}),
) -> protocol.CreateActorRequest:
    actor_id = _id(ActorID, actor_byte)
    payload = b"actor-class"
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(_id(JobID, 2), "example", "Counter", "v1"),
        payload,
        hashlib.sha256(payload).hexdigest(),
        ("inc", "get"),
    )
    return protocol.CreateActorRequest(
        actor_id,
        ActorGeneration(actor_id, 0),
        definition,
        constructor_payload,
        resources,
        _id(WorkerID, 3),
    )


def _accepted_reservation(
    request: protocol.ReserveActorWorkerRequest, worker_byte: int = 9
) -> protocol.ReserveActorWorkerReply:
    return protocol.ReserveActorWorkerReply(
        request.actor_id,
        request.generation,
        accepted=True,
        node_id=request.target_node_id,
        worker_id=_id(WorkerID, worker_byte),
        worker_address=("127.0.0.1", 12009),
        worker_pid=9009,
    )


@pytest.mark.unit
def test_actor_creation_selects_only_available_feasible_node_and_is_idempotent() -> None:
    nodes = NodeRegistry()
    infeasible = _id(NodeID, 4)
    available = _id(NodeID, 5)
    busy = _id(NodeID, 6)
    nodes.register(
        infeasible,
        ("127.0.0.1", 12004),
        ResourceVector({"CPU": 8}),
        node_pid=4004,
    )
    nodes.register(
        available,
        ("127.0.0.1", 12005),
        ResourceVector({"CPU": 2, "actor": 1}),
        node_pid=4005,
    )
    nodes.register(
        busy,
        ("127.0.0.1", 12006),
        ResourceVector({"CPU": 2, "actor": 1}),
        node_pid=4006,
        available_resources=ResourceVector({"CPU": 2}),
    )
    calls: list[tuple[tuple[str, int], protocol.ReserveActorWorkerRequest]] = []

    def reserve(address, request):
        calls.append((address, request))
        return _accepted_reservation(request)

    actors = ActorRegistry()
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve
    )
    request = _request(resources=ResourceVector({"CPU": 1, "actor": 1}))

    first = coordinator.create(request)
    replay = coordinator.create(request)

    assert first == replay
    assert first.accepted and first.node_id == available
    assert calls == [
        (
            ("127.0.0.1", 12005),
            protocol.ReserveActorWorkerRequest(
                request.actor_id,
                request.generation,
                request.class_definition,
                request.constructor_payload,
                request.resources,
                request.owner_worker_id,
                available,
                1,
            ),
        )
    ]
    assert actors.get(request.actor_id).state is ActorCreationState.ALIVE


@pytest.mark.unit
def test_same_actor_id_with_different_specification_is_rejected_without_rpc() -> None:
    nodes = NodeRegistry()
    target = _id(NodeID, 7)
    nodes.register(
        target, ("127.0.0.1", 12007), ResourceVector({"CPU": 2}),
        node_pid=4007,
    )
    calls = []

    def reserve(_address, request):
        calls.append(request)
        return _accepted_reservation(request)

    coordinator = ActorCoordinator(
        nodes, ActorRegistry(), reserve_actor_worker=reserve
    )
    original = _request()
    conflicting = _request(constructor_payload=b"different-constructor")

    assert coordinator.create(original).accepted
    rejected = coordinator.create(conflicting)

    assert not rejected.accepted
    assert "different metadata" in (rejected.error or "")
    assert len(calls) == 1


@pytest.mark.heavy
def test_actor_is_pending_until_node_reply_then_endpoint_is_published_alive() -> None:
    nodes = NodeRegistry()
    target = _id(NodeID, 8)
    nodes.register(
        target, ("127.0.0.1", 12008), ResourceVector({"CPU": 1}),
        node_pid=4008,
    )
    entered = threading.Event()
    release = threading.Event()

    def reserve(_address, request):
        entered.set()
        assert release.wait(1.0)
        return _accepted_reservation(request)

    actors = ActorRegistry()
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve
    )
    request = _request(actor_byte=8)
    replies = []
    thread = threading.Thread(target=lambda: replies.append(coordinator.create(request)))
    thread.start()
    assert entered.wait(1.0)

    pending = actors.get(request.actor_id)
    assert pending.state is ActorCreationState.PENDING
    assert pending.node_id is None
    assert pending.worker_address is None

    release.set()
    thread.join(1.0)
    assert not thread.is_alive()
    assert replies[0].accepted
    alive = actors.get(request.actor_id)
    assert alive.state is ActorCreationState.ALIVE
    assert alive.worker_address == ("127.0.0.1", 12009)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("total", "available", "error"),
    [
        (ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1}), "feasible"),
        (
            ResourceVector({"CPU": 2, "actor": 1}),
            ResourceVector({"CPU": 2}),
            "currently unavailable",
        ),
    ],
)
def test_unschedulable_actor_returns_typed_failure_and_never_calls_node(
    total: ResourceVector, available: ResourceVector, error: str
) -> None:
    nodes = NodeRegistry()
    nodes.register(
        _id(NodeID, 10),
        ("127.0.0.1", 12010),
        total,
        node_pid=4010,
        available_resources=available,
    )

    def unexpected_reserve(_address, _request):
        raise AssertionError("unschedulable Actor must not call a NodeManager")

    actors = ActorRegistry()
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=unexpected_reserve
    )
    request = _request(resources=ResourceVector({"CPU": 1, "actor": 1}))

    reply = coordinator.create(request)

    assert isinstance(reply, protocol.CreateActorReply)
    assert not reply.accepted and error in (reply.error or "")
    state = actors.get(request.actor_id).state
    if "currently unavailable" in error:
        assert reply.retryable
        assert state is ActorCreationState.CREATING
    else:
        assert not reply.retryable
        assert state is ActorCreationState.DEAD


@pytest.mark.unit
def test_gcs_capability_status_is_updated_and_method_calls_are_not_handlers() -> None:
    # Inspecting this mapping does not construct or bind the TCP server.
    gcs = object.__new__(GCSLite)

    assert ACTOR_STATUS.implemented
    assert gcs.actor_status() == ACTOR_STATUS
    assert "create_actor" in gcs.handlers
    assert "actor_call" not in gcs.handlers
    assert all("method" not in name for name in gcs.handlers)


@pytest.mark.unit
def test_closed_actor_admission_rejects_unknown_without_registering_or_reserving() -> None:
    nodes = NodeRegistry()
    target = _id(NodeID, 20)
    nodes.register(
        target, ("127.0.0.1", 12020), ResourceVector({"CPU": 1}),
        node_pid=4020,
    )
    calls = []
    actors = ActorRegistry()
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *_args: calls.append(_args)
    )
    request = _request(actor_byte=20)
    coordinator.close_admission()

    reply = coordinator.create(request)

    assert not reply.accepted and not reply.retryable
    assert "admission is closed" in (reply.error or "")
    assert not actors.contains(request.actor_id)
    assert calls == []


@pytest.mark.unit
def test_closed_admission_exact_creating_replay_redrives_frozen_reservation() -> None:
    nodes = NodeRegistry()
    target = _id(NodeID, 21)
    nodes.register(
        target, ("127.0.0.1", 12021), ResourceVector({"CPU": 1}),
        node_pid=4021,
    )
    requests = []

    def reserve(_address, request):
        requests.append(request)
        if len(requests) == 1:
            raise RuntimeError("reply lost")
        return _accepted_reservation(request, worker_byte=21)

    actors = ActorRegistry()
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve
    )
    request = _request(actor_byte=21)
    ambiguous = coordinator.create(request)
    assert ambiguous.retryable
    frozen = actors.initial_reservation_for(request)
    assert frozen is not None
    coordinator.close_admission()

    resolved = coordinator.create(request)

    assert resolved.accepted
    assert requests == [frozen, frozen]
    assert actors.get(request.actor_id).state is ActorCreationState.ALIVE


@pytest.mark.unit
def test_closed_admission_exact_alive_replay_returns_current_route_without_reserve() -> None:
    nodes = NodeRegistry()
    target = _id(NodeID, 22)
    nodes.register(
        target, ("127.0.0.1", 12022), ResourceVector({"CPU": 1}),
        node_pid=4022,
    )
    requests = []

    def reserve(_address, request):
        requests.append(request)
        return _accepted_reservation(request, worker_byte=22)

    coordinator = ActorCoordinator(
        nodes, ActorRegistry(), reserve_actor_worker=reserve
    )
    request = _request(actor_byte=22)
    created = coordinator.create(request)
    coordinator.close_admission()

    replay = coordinator.create(request)

    assert replay == created
    assert len(requests) == 1


@pytest.mark.unit
def test_closed_admission_existing_actor_spec_drift_remains_typed_conflict() -> None:
    nodes = NodeRegistry()
    target = _id(NodeID, 23)
    nodes.register(
        target, ("127.0.0.1", 12023), ResourceVector({"CPU": 1}),
        node_pid=4023,
    )
    calls = []

    def reserve(_address, request):
        calls.append(request)
        return _accepted_reservation(request, worker_byte=23)

    coordinator = ActorCoordinator(
        nodes, ActorRegistry(), reserve_actor_worker=reserve
    )
    request = _request(actor_byte=23)
    assert coordinator.create(request).accepted
    coordinator.close_admission()

    conflict = coordinator.create(
        _request(actor_byte=23, constructor_payload=b"different")
    )

    assert not conflict.accepted and not conflict.retryable
    assert "different metadata" in (conflict.error or "")
    assert len(calls) == 1
