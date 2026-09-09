"""Node Actor lifecycle contracts with explicit runtime classification.

The unit cases use an unstarted Node and fake process/RPC boundaries. Full Node
finalization still starts ordinary-Worker drain/finalize threads even with fake
processes; that case and the concurrent constructor/shutdown case remain heavy
pending separate exact bounded-lifecycle review.
"""

from __future__ import annotations

import hashlib
import threading

import cloudpickle
import pytest

from miniray import protocol
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, TaskID, WorkerID
from miniray.node import NodeServer, _ActorStopResult, _WorkerSlot, _WorkerStopResult
from miniray.resources import HybridPolicy, ResourceLedger, ResourceVector


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _request(node_id: NodeID) -> protocol.ReserveActorWorkerRequest:
    job_id = _id(JobID, 1)
    actor_id = ActorID.derive(job_id, TaskID.for_driver(job_id), 0)
    payload = cloudpickle.dumps(type("Counter", (), {}))
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(job_id, __name__, "Counter", "v1"),
        payload,
        hashlib.sha256(payload).hexdigest(),
        ("increment",),
    )
    return protocol.ReserveActorWorkerRequest(
        actor_id,
        ActorGeneration(actor_id, 0),
        definition,
        cloudpickle.dumps(((), {})),
        ResourceVector({"CPU": 1}),
        _id(WorkerID, 2),
        node_id,
    )


class _ActorProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.stopped = False
        self.exitcode = None
        self.closed = False

    def is_alive(self) -> bool:
        return not self.stopped

    def join(self, timeout: float) -> None:
        del timeout

    def terminate(self) -> None:
        self.stopped = True
        self.exitcode = -15

    def close(self) -> None:
        self.closed = True


def _node(node_id: NodeID) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = node_id
    node.worker_id = WorkerID.random()
    node._state_lock = threading.RLock()
    node._ledger = ResourceLedger(ResourceVector({"CPU": 2}))
    node._cluster_nodes = ()
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._inflight_lease_requests = 0
    node._actor_workers = {}
    node._actor_creation_locks = {}
    node._actor_finalize_request_id = None
    node._actor_finalize_results = {}
    node._worker_process = None
    node._worker_address = None
    node._worker_pid = 8000
    node._worker_exitcode = 0
    node._worker_forced = False
    node._active_lease_id = None
    node._pinned_transfers = {}
    node._dependency_pin_cleanups = {}
    node._scheduling_policy = HybridPolicy(seed=0)
    node._gcs_address = None
    node._registered_with_gcs = False
    node._gcs_lifecycle_lock = threading.Lock()
    return node


@pytest.mark.unit
def test_reserve_actor_worker_commits_after_typed_startup_and_is_idempotent(
    monkeypatch,
) -> None:
    node_id = _id(NodeID, 3)
    node = _node(node_id)
    request = _request(node_id)
    process = _ActorProcess(1234)
    startup = protocol.ActorWorkerStartup(
        request.actor_id,
        request.generation,
        _id(WorkerID, 4),
        process.pid,
        ("127.0.0.1", 14004),
    )
    calls = 0

    def spawn(_request):
        nonlocal calls
        calls += 1
        return startup, process

    monkeypatch.setattr(node, "_spawn_actor_worker", spawn)
    first = node._handle_reserve_actor_worker(request)
    replay = node._handle_reserve_actor_worker(request)

    assert first.accepted and replay == first
    assert calls == 1
    assert first.worker_pid == process.pid
    assert node.resource_ledger.available == ResourceVector({"CPU": 1})

    conflicting = protocol.ReserveActorWorkerRequest(
        request.actor_id,
        request.generation,
        request.class_definition,
        request.constructor_payload,
        ResourceVector({"CPU": 2}),
        request.owner_worker_id,
        request.target_node_id,
    )
    rejected = node._handle_reserve_actor_worker(conflicting)
    assert not rejected.accepted
    assert node.resource_ledger.available == ResourceVector({"CPU": 1})


@pytest.mark.unit
def test_actor_start_failure_releases_lifetime_resources(monkeypatch) -> None:
    node_id = _id(NodeID, 5)
    node = _node(node_id)
    request = _request(node_id)
    monkeypatch.setattr(
        node,
        "_spawn_actor_worker",
        lambda _request: (_ for _ in ()).throw(RuntimeError("constructor failed")),
    )

    reply = node._handle_reserve_actor_worker(request)

    assert not reply.accepted
    assert node.resource_ledger.available == node.resource_ledger.total
    assert request.actor_id not in node._actor_workers


@pytest.mark.unit
def test_node_stops_actor_workers_and_releases_lifetime_tokens(monkeypatch) -> None:
    node_id = _id(NodeID, 6)
    node = _node(node_id)
    request = _request(node_id)
    process = _ActorProcess(5678)
    startup = protocol.ActorWorkerStartup(
        request.actor_id,
        request.generation,
        _id(WorkerID, 7),
        process.pid,
        ("127.0.0.1", 14007),
    )
    monkeypatch.setattr(
        node, "_spawn_actor_worker", lambda _request: (startup, process)
    )
    assert node._handle_reserve_actor_worker(request).accepted

    stopped = []
    monkeypatch.setattr(
        node,
        "_stop_actor_worker",
        lambda actor_id, record, request_id: (
            stopped.append((actor_id, record, request_id))
            or _ActorStopResult(
                actor_id, record.startup.worker_id, record.startup.worker_pid,
                0, True, False,
            )
        ),
    )
    first = node._stop_all_actor_workers("shutdown-1")
    second = node._stop_all_actor_workers("shutdown-1")

    assert len(stopped) == 1
    assert stopped[0][0] == request.actor_id
    assert stopped[0][2] == "shutdown-1"
    assert second == first and first[0].clean
    assert node.resource_ledger.available == node.resource_ledger.total
    assert not node._actor_workers


def _install_actor(node: NodeServer, byte: int = 10):
    request = _request(node.node_id)
    process = _ActorProcess(7000 + byte)
    startup = protocol.ActorWorkerStartup(
        request.actor_id, request.generation, _id(WorkerID, byte), process.pid,
        ("127.0.0.1", 14100 + byte),
    )
    allocation = node._ledger.allocate(request.resources)
    reply = protocol.ReserveActorWorkerReply(
        request.actor_id, request.generation, True, node.node_id,
        startup.worker_id, startup.worker_address, startup.worker_pid,
    )
    from miniray.node import _ActorWorkerRecord

    record = _ActorWorkerRecord(request, allocation, process, startup, reply)
    node._actor_workers[request.actor_id] = record
    return request, process, record


@pytest.mark.unit
def test_actor_lifetime_allocation_is_valid_only_during_drain() -> None:
    node = _node(_id(NodeID, 10))
    request, process, _record = _install_actor(node, 11)

    assert node._drain_resources_clean_locked()
    assert not node._resources_clean_locked()
    assert process.is_alive()
    assert node._ledger.record(
        node._actor_workers[request.actor_id].allocation_token
    ).resources == request.resources

    # A live allocation not owned by an Actor is never hidden by phase one.
    extra = node._ledger.allocate(ResourceVector({"CPU": 1}))
    assert not node._drain_resources_clean_locked()
    node._ledger.release(extra)
    assert node._drain_resources_clean_locked()


@pytest.mark.unit
@pytest.mark.parametrize(
    "reply_factory",
    (
        lambda epoch, worker: None,
        lambda epoch, worker: protocol.ShutdownAck(
            "wrong-epoch", "actor-worker:{}".format(worker), True
        ),
        lambda epoch, worker: protocol.ShutdownAck(
            epoch, "actor-worker:{}".format(WorkerID.random()), True
        ),
        lambda epoch, worker: protocol.ShutdownAck(
            epoch, "actor-worker:{}".format(worker), False
        ),
    ),
)
def test_actor_stop_requires_exact_clean_ack(
    monkeypatch: pytest.MonkeyPatch, reply_factory
) -> None:
    node = _node(NodeID.random())
    request, process, record = _install_actor(node, 12)
    epoch = "actor-finalize"

    def fake_rpc(*_args, **_kwargs):
        reply = reply_factory(epoch, record.startup.worker_id)
        process.stopped = True
        process.exitcode = 0
        if reply is None:
            raise RuntimeError("lost actor shutdown ACK")
        return reply

    monkeypatch.setattr("miniray.node.rpc_request", fake_rpc)
    result = node._stop_all_actor_workers(epoch)[0]

    assert result.actor_id == request.actor_id
    assert not result.clean
    assert not result.forced
    assert node.resource_ledger.available == node.resource_ledger.total
    assert not node._actor_workers


@pytest.mark.heavy
def test_forced_actor_stop_reclaims_token_but_node_finalize_is_unclean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node(NodeID.random())
    request, process, _record = _install_actor(node, 13)
    worker_id = WorkerID.random()
    node.worker_id = worker_id
    node._worker_order = (worker_id,)
    node._workers = {
        worker_id: _WorkerSlot(
            worker_id, process=None, address=None, pid=8100, exitcode=0
        )
    }
    node._legacy_worker_compat = False
    node._worker_process = None
    node._worker_address = None
    node._worker_pid = 8100
    node._worker_exitcode = 0
    node._worker_forced = False
    node._active_lease_id = None
    node._worker_drain_statuses = {
        worker_id: protocol.DrainStatus(
            "epoch", "worker:{}".format(worker_id), True, True
        )
    }
    node._worker_finalize_results = {
        worker_id: _WorkerStopResult(worker_id, 8100, 0, True, False)
    }
    node._pinned_transfers = {}
    node._dependency_pin_cleanups = {}
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_request_locks = {}
    node._shutdown_request_id = "epoch"
    node._shutdown_phase_lock = threading.Lock()
    node._finalize_exit_scheduled = False
    monkeypatch.setattr(
        "miniray.node.rpc_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("actor shutdown unreachable")
        ),
    )

    reply = node._handle_finalize_shutdown(protocol.FinalizeShutdown("epoch"))

    assert not reply.clean and reply.forced
    assert process.stopped and process.exitcode == -15
    assert node.resource_ledger.available == node.resource_ledger.total
    assert request.actor_id not in node._actor_workers


@pytest.mark.heavy
def test_shutdown_waits_for_inflight_actor_creation_then_reclaims(monkeypatch) -> None:
    node_id = _id(NodeID, 8)
    node = _node(node_id)
    request = _request(node_id)
    process = _ActorProcess(6789)
    startup = protocol.ActorWorkerStartup(
        request.actor_id,
        request.generation,
        _id(WorkerID, 9),
        process.pid,
        ("127.0.0.1", 14009),
    )
    constructor_entered = threading.Event()
    allow_constructor = threading.Event()

    def spawn(_request):
        constructor_entered.set()
        assert allow_constructor.wait(1)
        return startup, process

    monkeypatch.setattr(node, "_spawn_actor_worker", spawn)
    stopped = []
    monkeypatch.setattr(
        node,
        "_stop_actor_process_best_effort",
        lambda child, child_startup: stopped.append((child, child_startup)),
    )
    result = []
    creator = threading.Thread(
        target=lambda: result.append(node._handle_reserve_actor_worker(request))
    )
    creator.start()
    assert constructor_entered.wait(1)
    node._stop_event.set()
    shutdown = threading.Thread(target=node._stop_all_actor_workers)
    shutdown.start()
    allow_constructor.set()
    creator.join(1)
    shutdown.join(1)

    assert not creator.is_alive() and not shutdown.is_alive()
    assert result and not result[0].accepted
    assert stopped == [(process, startup)]
    assert node.resource_ledger.available == node.resource_ledger.total
    assert not node._actor_workers
