"""Node Actor restart reducers and a separately classified live supervisor.

Fake process/RPC paths are pure; the supervisor test starts an actual thread,
waits for progress and lacks guaranteed failure teardown, so it remains heavy.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace

import cloudpickle
import pytest

from miniray import protocol
from miniray.ids import (
    ActorGeneration,
    ActorID,
    JobID,
    NodeID,
    TaskID,
    WorkerID,
)
from miniray.node import NodeServer, _ActorWorkerRecord
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector


class _CountingLedger(ResourceLedger):
    def __init__(self, total: ResourceVector) -> None:
        super().__init__(total)
        self.release_calls = 0

    def release(self, token: AllocationToken) -> bool:
        self.release_calls += 1
        return super().release(token)


class _Process:
    def __init__(
        self, pid: int, *, alive: bool = True, exitcode: int | None = None
    ) -> None:
        self.pid = pid
        self.alive = alive
        self.exitcode = exitcode
        self.sentinel = object()
        self.join_calls = 0
        self.closed = False

    def is_alive(self) -> bool:
        return self.alive

    def join(self, _timeout: float = 0) -> None:
        self.join_calls += 1

    def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.alive = False
        self.exitcode = -15


def _request(
    node_id: NodeID,
    *,
    actor_id: ActorID | None = None,
    generation_number: int = 0,
    route_epoch: int = 0,
    restart: protocol.ActorWorkerExitRecord | None = None,
    resources: ResourceVector = ResourceVector({"CPU": 1}),
) -> protocol.ReserveActorWorkerRequest:
    job_id = JobID(bytes([1]) * 16)
    actor_id = actor_id or ActorID.derive(
        job_id, TaskID.for_driver(job_id), 0
    )
    payload = cloudpickle.dumps(type("Counter", (), {}))
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(job_id, __name__, "Counter", "v1"),
        payload,
        hashlib.sha256(payload).hexdigest(),
        ("increment",),
    )
    return protocol.ReserveActorWorkerRequest(
        actor_id,
        ActorGeneration(actor_id, generation_number),
        definition,
        cloudpickle.dumps(((), {})),
        resources,
        WorkerID(bytes([2]) * 16),
        node_id,
        route_epoch,
        restart,
    )


def _node() -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._node_pid = 8001
    node._registration_epoch = 3
    node._state_lock = threading.RLock()
    node._ledger = _CountingLedger(ResourceVector({"CPU": 2}))
    node._cluster_nodes = ()
    node._gcs_address = None
    node._registered_with_gcs = True
    node._gcs_lifecycle_lock = threading.Lock()
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node._actor_workers = {}
    node._actor_creation_locks = {}
    node._actor_generation_outcomes = {}
    node._actor_worker_ids_seen = set()
    node._actor_worker_pids_seen = set()
    node._actor_supervisor_stop = threading.Event()
    node._actor_supervisor_thread = None
    node._actor_lifecycle_lock = threading.Lock()
    node._actor_finalize_request_id = None
    node._actor_finalize_results = {}
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node.event_sink = None
    return node


def _install_generation(
    node: NodeServer, request: protocol.ReserveActorWorkerRequest, process: _Process
) -> _ActorWorkerRecord:
    worker_id = WorkerID.random()
    startup = protocol.ActorWorkerStartup(
        request.actor_id,
        request.generation,
        worker_id,
        process.pid,
        ("127.0.0.1", 14001 + request.generation.generation),
    )
    token = node._ledger.allocate(request.resources)
    reply = protocol.ReserveActorWorkerReply(
        request.actor_id,
        request.generation,
        True,
        node.node_id,
        worker_id,
        startup.worker_address,
        process.pid,
    )
    record = _ActorWorkerRecord(request, token, process, startup, reply)
    node._actor_workers[request.actor_id] = record
    node._actor_worker_ids_seen.add(worker_id)
    node._actor_worker_pids_seen.add(process.pid)
    return record


def _dead_snapshot(
    exit_record: protocol.ActorWorkerExitRecord,
) -> protocol.ActorSnapshot:
    return protocol.ActorSnapshot(
        exit_record.actor_id,
        exit_record.generation,
        protocol.ActorState.DEAD,
        exit_record.route_epoch + 1,
        exit_record.generation.generation,
        exit_record.generation.generation,
        last_exit=exit_record,
        error="restart budget exhausted",
    )


@pytest.mark.unit
def test_exact_process_exit_releases_once_and_replays_stable_gcs_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    request = _request(node.node_id)
    process = _Process(8101, alive=False, exitcode=23)
    record = _install_generation(node, request, process)
    node._gcs_address = ("127.0.0.1", 19001)
    reports: list[protocol.ReportActorWorkerExit] = []

    def report(_address, handler, message, **_kwargs):
        assert handler == "report_actor_worker_exit"
        assert isinstance(message, protocol.ReportActorWorkerExit)
        reports.append(message)
        if len(reports) == 1:
            raise RuntimeError("lost reply")
        return protocol.ReportActorWorkerExitReply(
            message.record,
            protocol.ActorWorkerExitDisposition.ALREADY_APPLIED,
            _dead_snapshot(message.record),
        )

    monkeypatch.setattr("miniray.node.rpc_request", report)

    assert node._handle_unexpected_actor_worker_exit(
        request.actor_id, request.generation, process
    )
    outcome = node._actor_generation_outcomes[request.generation]
    assert outcome.exit_record.worker_id == record.startup.worker_id
    assert outcome.exit_record.worker_pid == process.pid
    assert outcome.exit_record.exit_code == 23
    assert request.actor_id not in node._actor_workers
    assert node._ledger.available == node._ledger.total
    assert node._ledger.release_calls == 1
    assert node._handle_reserve_actor_worker(request) == record.reply

    # A different Process object, even with the same PID, and a duplicate
    # callback for the old exact object cannot release or publish again.
    impostor = _Process(process.pid, alive=False, exitcode=23)
    assert not node._handle_unexpected_actor_worker_exit(
        request.actor_id, request.generation, impostor
    )
    assert not node._handle_unexpected_actor_worker_exit(
        request.actor_id, request.generation, process
    )
    assert node._ledger.release_calls == 1

    assert node._flush_pending_actor_exit_reports()
    assert reports == [outcome.report, outcome.report]
    assert outcome.report_reply is not None


@pytest.mark.unit
def test_restart_reservation_requires_exact_tombstone_and_fresh_incarnation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    first_request = _request(node.node_id)
    first_process = _Process(8201, alive=False, exitcode=17)
    first = _install_generation(node, first_request, first_process)
    assert node._handle_unexpected_actor_worker_exit(
        first_request.actor_id, first_request.generation, first_process
    )
    proof = node._actor_generation_outcomes[
        first_request.generation
    ].exit_record

    changed = replace(
        first_request,
        generation=first_request.generation.next(),
        route_epoch=2,
        restart=proof,
        resources=ResourceVector({"CPU": 2}),
    )
    rejected = node._handle_reserve_actor_worker(changed)
    assert not rejected.accepted
    assert "immutable metadata" in (rejected.error or "")

    wrong_epoch = replace(
        first_request,
        generation=first_request.generation.next(),
        route_epoch=1,
        restart=proof,
    )
    rejected_epoch = node._handle_reserve_actor_worker(wrong_epoch)
    assert not rejected_epoch.accepted
    assert "route epoch" in (rejected_epoch.error or "")

    restart = replace(
        first_request,
        generation=first_request.generation.next(),
        route_epoch=2,
        restart=proof,
    )
    next_process = _Process(8202)
    next_worker = WorkerID.random()
    startup = protocol.ActorWorkerStartup(
        restart.actor_id,
        restart.generation,
        next_worker,
        next_process.pid,
        ("127.0.0.1", 14002),
    )
    spawn_calls = 0

    def spawn(received):
        nonlocal spawn_calls
        spawn_calls += 1
        assert received == restart
        return startup, next_process

    monkeypatch.setattr(node, "_spawn_actor_worker", spawn)
    accepted = node._handle_reserve_actor_worker(restart)
    replay = node._handle_reserve_actor_worker(restart)

    assert accepted.accepted and replay == accepted
    assert spawn_calls == 1
    assert accepted.worker_id != first.startup.worker_id
    assert accepted.worker_pid != first.startup.worker_pid
    assert node._ledger.available == ResourceVector({"CPU": 1})

    # The exact generation-0 operation remains replayable after generation 1
    # is live, but a stale operation with drifted metadata is rejected.
    assert node._handle_reserve_actor_worker(first_request) == first.reply
    stale = node._handle_reserve_actor_worker(
        replace(first_request, resources=ResourceVector({"CPU": 2}))
    )
    assert not stale.accepted
    assert "terminal with different metadata" in (stale.error or "")


@pytest.mark.heavy
def test_actor_supervisor_consumes_only_ready_child_sentinel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    request = _request(node.node_id)
    process = _Process(8301, alive=False, exitcode=9)
    _install_generation(node, request, process)
    observed = threading.Event()

    def wait(sentinels, _timeout):
        assert tuple(sentinels) == (process.sentinel,)
        observed.set()
        return [process.sentinel]

    node._actor_supervisor_wait = wait
    node._start_actor_supervisor()
    assert observed.wait(1)
    for _attempt in range(100):
        if request.generation in node._actor_generation_outcomes:
            break
        threading.Event().wait(0.001)
    assert node._stop_actor_supervisor()
    assert request.generation in node._actor_generation_outcomes
    assert process.join_calls >= 1
    assert node._ledger.release_calls == 1


@pytest.mark.unit
def test_drain_fence_stops_supervisor_sweeps_death_and_rejects_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    request = _request(node.node_id)
    process = _Process(8401, alive=False, exitcode=31)
    _install_generation(node, request, process)
    node._shutdown_request_id = "drain-1"

    assert node._stop_actor_supervisor(sweep=True)
    outcome = node._actor_generation_outcomes[request.generation]
    assert request.actor_id not in node._actor_workers
    assert node._ledger.release_calls == 1

    restart = replace(
        request,
        generation=request.generation.next(),
        route_epoch=2,
        restart=outcome.exit_record,
    )
    monkeypatch.setattr(
        node,
        "_spawn_actor_worker",
        lambda _request: (_ for _ in ()).throw(
            AssertionError("draining Node must not restart an Actor")
        ),
    )
    rejected = node._handle_reserve_actor_worker(restart)
    assert not rejected.accepted
    assert "shutting down" in (rejected.error or "")
