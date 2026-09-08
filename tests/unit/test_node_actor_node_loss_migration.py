"""Actor-migration reducers with explicit rollback transport classification.

Accepted migration and proof rejection use fake processes without runtime I/O.
Rejecting a reused Worker incarnation reaches the real unpublished-Actor
shutdown RPC despite its fake process, so that parameterized case is heavy.
"""

from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, WorkerID
from miniray.resources import ResourceVector
from tests.unit.test_node_actor_lifecycle import _ActorProcess, _node


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _reservation(target: NodeID) -> protocol.ReserveActorWorkerRequest:
    actor = _id(ActorID, 1)
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(_id(JobID, 2), "demo", "Counter", "v1"),
        b"actor", hashlib.sha256(b"actor").hexdigest(), ("inc",),
    )
    resources = ResourceVector({"CPU": 1})
    owner = _id(WorkerID, 3)
    source = _id(NodeID, 4)
    death = protocol.NodeDeathRecord(
        "source-death", source, 7404, 1, 2, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "source exited",
    )
    proof = protocol.ActorNodeLossRecord(
        actor, ActorGeneration(actor, 0), 1, _id(WorkerID, 5), 8505, death,
        definition, b"constructor", resources, owner,
    )
    return protocol.ReserveActorWorkerRequest(
        actor, ActorGeneration(actor, 1), definition, b"constructor",
        resources, owner, target, 3, proof,
    )


@pytest.mark.unit
def test_survivor_accepts_cross_node_proof_without_local_predecessor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _id(NodeID, 6)
    node = _node(target)
    node._node_pid = 7606
    node._registration_epoch = 6
    node._installed_snapshot_nodes = (
        protocol.NodeInfo(
            target, 7606, 6, ("127.0.0.1", 17606),
            ResourceVector({"CPU": 2}), ResourceVector({"CPU": 2}),
        ),
    )
    request = _reservation(target)
    process = _ActorProcess(8607)
    startup = protocol.ActorWorkerStartup(
        request.actor_id, request.generation, _id(WorkerID, 7), process.pid,
        ("127.0.0.1", 18607),
    )
    monkeypatch.setattr(
        node, "_spawn_actor_worker", lambda received: (startup, process)
    )

    first = node._handle_reserve_actor_worker(request)
    replay = node._handle_reserve_actor_worker(request)

    assert first.accepted and replay == first
    assert first.node_id == target
    assert first.worker_id != request.restart.worker_id
    assert first.worker_pid != request.restart.worker_pid
    assert request.generation not in node._actor_generation_outcomes
    assert node.resource_ledger.available == ResourceVector({"CPU": 1})


@pytest.mark.unit
def test_migration_rejects_changed_proof_spec_without_spawning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _id(NodeID, 8)
    node = _node(target)
    request = _reservation(target)
    object.__setattr__(request, "resources", ResourceVector({"CPU": 2}))
    monkeypatch.setattr(
        node, "_spawn_actor_worker",
        lambda _request: (_ for _ in ()).throw(AssertionError("must not spawn")),
    )

    rejected = node._handle_reserve_actor_worker(request)

    assert not rejected.accepted
    assert "authorized lifetime spec" in (rejected.error or "")
    assert node.resource_ledger.available == node.resource_ledger.total


@pytest.mark.heavy
@pytest.mark.parametrize("reuse", ("worker_id", "worker_pid"))
def test_migration_rejects_reused_source_worker_incarnation(
    monkeypatch: pytest.MonkeyPatch, reuse: str,
) -> None:
    target = _id(NodeID, 9)
    node = _node(target)
    request = _reservation(target)
    process = _ActorProcess(
        request.restart.worker_pid if reuse == "worker_pid" else 8610
    )
    worker_id = (
        request.restart.worker_id
        if reuse == "worker_id" else _id(WorkerID, 10)
    )
    startup = protocol.ActorWorkerStartup(
        request.actor_id, request.generation, worker_id, process.pid,
        ("127.0.0.1", 18610),
    )
    monkeypatch.setattr(
        node, "_spawn_actor_worker", lambda _request: (startup, process)
    )

    rejected = node._handle_reserve_actor_worker(request)

    assert not rejected.accepted
    assert "dead source Worker incarnation" in (rejected.error or "")
    assert process.stopped
    assert node.resource_ledger.available == node.resource_ledger.total
