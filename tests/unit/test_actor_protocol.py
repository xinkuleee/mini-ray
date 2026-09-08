from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import (
    ActorGeneration,
    ActorID,
    AttemptID,
    JobID,
    NodeID,
    TaskID,
    WorkerID,
)
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _values():
    job = _id(JobID, 1)
    parent = TaskID.for_driver(job)
    task = TaskID.derive(job, parent, 0)
    actor = _id(ActorID, 2)
    generation = ActorGeneration(actor, 0)
    owner = _id(WorkerID, 3)
    payload = b"actor-class"
    key = protocol.FunctionKey(job, "demo", "Counter", "v1")
    definition = protocol.ActorClassDefinition(
        key, payload, hashlib.sha256(payload).hexdigest(), ("inc", "get")
    )
    return task, actor, generation, owner, definition


def test_actor_definition_and_create_endpoint_contract() -> None:
    _task, actor, generation, owner, definition = _values()
    request = protocol.CreateActorRequest(
        actor, generation, definition, b"args", ResourceVector({"CPU": 1}), owner
    )
    reply = protocol.CreateActorReply(
        actor, generation, True, _id(NodeID, 4), _id(WorkerID, 5),
        ("127.0.0.1", 12001), 1234, route_epoch=1
    )
    assert request.class_definition.method_names == ("inc", "get")
    assert reply.accepted

    with pytest.raises(ProtocolError, match="complete endpoint"):
        protocol.CreateActorReply(actor, generation, True, node_id=_id(NodeID, 4))
    with pytest.raises(ProtocolError, match="cannot contain an endpoint"):
        protocol.CreateActorReply(
            actor, generation, False, node_id=_id(NodeID, 4), error="failed"
        )


def test_actor_generation_and_attempt_are_fenced() -> None:
    task, actor, generation, owner, definition = _values()
    other_actor = _id(ActorID, 6)
    with pytest.raises(ProtocolError, match="generation must belong"):
        protocol.CreateActorRequest(
            actor, ActorGeneration(other_actor, 0), definition, b"",
            ResourceVector(), owner
        )

    other_task = TaskID.derive(_id(JobID, 7), TaskID.for_driver(_id(JobID, 7)), 0)
    with pytest.raises(ProtocolError, match="attempt_id must belong"):
        protocol.ActorCallRequest(
            actor, generation, owner, 0, "inc", task, AttemptID(other_task, 0),
            owner, b"args"
        )


def test_actor_methods_and_sequences_are_validated() -> None:
    task, actor, generation, owner, definition = _values()
    with pytest.raises(ProtocolError, match="unique"):
        protocol.ActorClassDefinition(
            definition.key, definition.payload, definition.sha256, ("inc", "inc")
        )
    with pytest.raises(ProtocolError, match="method_name"):
        protocol.ActorCallRequest(
            actor, generation, owner, 0, "", task, AttemptID(task, 0), owner, b""
        )
    with pytest.raises(ProtocolError, match="non-negative"):
        protocol.ActorCallRequest(
            actor, generation, owner, -1, "inc", task, AttemptID(task, 0), owner, b""
        )


def test_reserve_startup_and_call_reply_round_trip() -> None:
    task, actor, generation, owner, definition = _values()
    target = _id(NodeID, 8)
    worker = _id(WorkerID, 9)
    request = protocol.ReserveActorWorkerRequest(
        actor, generation, definition, b"args", ResourceVector({"CPU": 1}),
        owner, target
    )
    reserved = protocol.ReserveActorWorkerReply(
        actor, generation, True, target, worker, ("127.0.0.1", 12002), 2222
    )
    startup = protocol.ActorWorkerStartup(
        actor, generation, worker, 2222, ("127.0.0.1", 12002)
    )
    task_reply = protocol.TaskReply(
        task, AttemptID(task, 0), worker, protocol.TaskReplyStatus.SUCCEEDED
    )
    call_reply = protocol.ActorCallReply(
        actor, generation, owner, 1, task_reply
    )
    assert request.target_node_id == reserved.node_id
    assert startup.worker_address == reserved.worker_address
    assert call_reply.task_reply.task_id == task
