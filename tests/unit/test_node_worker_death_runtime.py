"""Pure Node wiring tests for ordinary Worker membership and death.

No test in this module opens a socket or creates a child process.
"""

from __future__ import annotations

import threading

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import (
    GCS_REGISTER_WORKER_INCARNATION_HANDLER,
    GCS_REPORT_WORKER_DEATH_HANDLER,
    NodeServer,
    _LeaseOutcome,
    _LeaseRecord,
    _WorkerSlot,
)
from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector


pytestmark = pytest.mark.unit


class _Server:
    def __init__(self, events: list[str]) -> None:
        self.address = ("127.0.0.1", 29001)
        self._events = events

    def start(self):
        self._events.append("server")
        return self.address


class _Process:
    def __init__(self, pid: int, *, alive: bool = True, exitcode: int | None = None):
        self.pid = pid
        self.alive = alive
        self.exitcode = exitcode
        self.closed = False
        self.terminated = False

    def is_alive(self) -> bool:
        return self.alive

    def join(self, _timeout: float = 0) -> None:
        return None

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False
        self.exitcode = -15

    def close(self) -> None:
        self.closed = True


def test_start_registers_node_before_starting_or_supervising_workers() -> None:
    events: list[str] = []
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._server = _Server(events)
    node._state_lock = threading.RLock()
    node._cluster_addresses = {}
    node.event_sink = None
    node._register_with_gcs = lambda: events.append("register-node")
    node._start_worker_pool = lambda: events.append("workers")
    node._start_worker_supervisor = lambda: events.append("worker-supervisor")
    node._start_actor_supervisor = lambda: events.append("actor-supervisor")

    assert node.start() == node._server.address
    assert events == [
        "server",
        "register-node",
        "workers",
        "worker-supervisor",
        "actor-supervisor",
    ]


def _startup_node() -> tuple[NodeServer, WorkerID]:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    worker_id = WorkerID.random()
    node.worker_id = worker_id
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(worker_id)}
    node.num_workers_per_node = 1
    node._legacy_worker_compat = False
    node._worker_process = None
    node._worker_address = None
    node._worker_pid = None
    node._worker_exitcode = None
    node._worker_forced = False
    node._active_lease_id = None
    node._state_lock = threading.RLock()
    node._gcs_address = ("127.0.0.1", 29000)
    node._node_pid = 7001
    node._registration_epoch = 4
    node._registered_with_gcs = True
    return node, worker_id


def test_ready_worker_is_published_only_after_exact_registration_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, worker_id = _startup_node()
    process = _Process(7101)
    address = ("127.0.0.1", 29101)
    observed_unpublished: list[bool] = []

    monkeypatch.setattr(node, "_spawn_worker_process", lambda _worker: (process, address))

    def rpc(_address, handler, request, **_kwargs):
        assert handler == GCS_REGISTER_WORKER_INCARNATION_HANDLER
        assert isinstance(request, protocol.RegisterWorkerIncarnation)
        observed_unpublished.append(node._workers[worker_id].process is None)
        return protocol.RegisterWorkerIncarnationReply(request.incarnation, True)

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    node._start_worker_slot(worker_id)

    expected = protocol.WorkerIncarnation(
        node.node_id, 7001, 4, worker_id, process.pid
    )
    slot = node._workers[worker_id]
    assert observed_unpublished == [True]
    assert (slot.process, slot.address, slot.incarnation) == (
        process, address, expected
    )


def test_rejected_registration_stops_child_without_publishing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, worker_id = _startup_node()
    process = _Process(7101)
    address = ("127.0.0.1", 29101)
    stopped: list[tuple[object, object]] = []
    monkeypatch.setattr(node, "_spawn_worker_process", lambda _worker: (process, address))
    monkeypatch.setattr(
        node, "_stop_unpublished_worker",
        lambda candidate, endpoint: stopped.append((candidate, endpoint)),
    )

    def rpc(_address, _handler, request, **_kwargs):
        return protocol.RegisterWorkerIncarnationReply(
            request.incarnation, False, "node incarnation is stale"
        )

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    with pytest.raises(RuntimeError, match="stale"):
        node._start_worker_slot(worker_id)

    assert stopped == [(process, address)]
    slot = node._workers[worker_id]
    assert slot.process is None and slot.address is None and slot.incarnation is None


def test_startup_pool_registration_failure_rolls_back_published_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, first = _startup_node()
    second = WorkerID.random()
    node._worker_order = (first, second)
    node._workers[second] = _WorkerSlot(second)
    started: list[WorkerID] = []
    rolled_back: list[tuple[WorkerID, ...]] = []

    def start(worker_id: WorkerID) -> None:
        if worker_id == second:
            raise RuntimeError("second registration failed")
        started.append(worker_id)
        node._workers[worker_id].process = _Process(7101)

    monkeypatch.setattr(node, "_start_worker_slot", start)
    monkeypatch.setattr(
        node, "_stop_workers",
        lambda *, worker_ids=None: rolled_back.append(worker_ids) or (),
    )

    with pytest.raises(RuntimeError, match="registration failed"):
        node._start_worker_pool()
    assert started == [first]
    assert rolled_back == [(first,)]


def test_replacement_registers_fresh_incarnation_before_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, old_incarnation, process, _lease = _death_node()
    # Preserve the vacant tombstone produced by local death reduction and then
    # exercise one replacement transaction directly.
    monkeypatch.setattr(node, "_flush_pending_worker_death_reports", lambda: False)
    assert node._handle_unexpected_worker_exit(old_incarnation.worker_id, process)
    vacant = node._workers[old_incarnation.worker_id]
    node._shutdown_request_id = None
    fresh = WorkerID.random()
    replacement = _Process(7102)
    replacement_address = ("127.0.0.1", 29102)
    monkeypatch.setattr(node, "_fresh_worker_id_locked", lambda: fresh)
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda _worker: (replacement, replacement_address),
    )
    observed: list[tuple[WorkerID, ...]] = []

    def register(worker_id: WorkerID, candidate: object):
        assert worker_id == fresh and candidate is replacement
        observed.append(node._worker_order)
        return protocol.WorkerIncarnation(
            node.node_id, node._node_pid, node._registration_epoch, fresh,
            replacement.pid,
        )

    monkeypatch.setattr(node, "_register_unpublished_worker", register)
    assert node._retry_vacant_worker_replacement(
        old_incarnation.worker_id, vacant
    )

    assert observed == [(old_incarnation.worker_id,)]
    assert node._worker_order == (fresh,)
    assert node._workers[fresh].incarnation.worker_id == fresh


def test_replacement_registration_failure_leaves_retryable_vacancy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, old_incarnation, process, _lease = _death_node()
    monkeypatch.setattr(node, "_flush_pending_worker_death_reports", lambda: False)
    assert node._handle_unexpected_worker_exit(old_incarnation.worker_id, process)
    vacant = node._workers[old_incarnation.worker_id]
    node._shutdown_request_id = None
    replacement = _Process(7102)
    endpoint = ("127.0.0.1", 29102)
    monkeypatch.setattr(node, "_fresh_worker_id_locked", WorkerID.random)
    monkeypatch.setattr(node, "_spawn_worker_process", lambda _worker: (replacement, endpoint))
    monkeypatch.setattr(
        node, "_register_unpublished_worker",
        lambda _worker, _process: (_ for _ in ()).throw(
            RuntimeError("registration unavailable")
        ),
    )
    stopped: list[tuple[object, object]] = []
    monkeypatch.setattr(
        node, "_stop_unpublished_worker",
        lambda candidate, address: stopped.append((candidate, address)),
    )

    assert not node._retry_vacant_worker_replacement(
        old_incarnation.worker_id, vacant
    )
    assert stopped == [(replacement, endpoint)]
    assert node._worker_order == (old_incarnation.worker_id,)
    assert vacant.process is None
    assert vacant.replacement_error == "RuntimeError: registration unavailable"
    assert node._worker_replacements_inflight == 0


def _death_node() -> tuple[
    NodeServer, protocol.WorkerIncarnation, _Process, protocol.RequestWorkerLease
]:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._node_pid = 7001
    node._registration_epoch = 4
    node._registered_with_gcs = True
    node._gcs_address = ("127.0.0.1", 29000)
    worker_id = WorkerID.random()
    process = _Process(7101, alive=False, exitcode=23)
    incarnation = protocol.WorkerIncarnation(
        node.node_id, node._node_pid, node._registration_epoch, worker_id, process.pid
    )
    lease_id = LeaseID.random()
    address = ("127.0.0.1", 29101)
    node.worker_id = worker_id
    node._worker_order = (worker_id,)
    node._workers = {
        worker_id: _WorkerSlot(
            worker_id, process=process, address=address, pid=process.pid,
            active_lease_id=lease_id, incarnation=incarnation,
        )
    }
    node.num_workers_per_node = 1
    node._legacy_worker_compat = False
    node._worker_process = process
    node._worker_address = address
    node._worker_pid = process.pid
    node._worker_exitcode = None
    node._worker_forced = False
    node._active_lease_id = lease_id
    total = ResourceVector({"CPU": 1})
    node._ledger = ResourceLedger(total)
    token = AllocationToken("worker-death")
    node._ledger.allocate(total, token)
    node._cluster_nodes = (NodeSnapshot(node.node_id, total, ResourceVector()),)
    node._cluster_addresses = {node.node_id: ("127.0.0.1", 29001)}
    owner = WorkerID.random()
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    request = protocol.RequestWorkerLease(
        lease_id, task, attempt, total, node.node_id, owner,
        target_node_id=node.node_id, return_ids=(ObjectID.for_task(task),),
    )
    grant = protocol.GrantWorkerLease(
        lease_id, task, attempt, node.node_id, worker_id, address, token
    )
    node._leases = {lease_id: _LeaseRecord(request, token, grant)}
    node._lease_outcomes = {lease_id: _LeaseOutcome(request, grant)}
    node._lease_cancellations = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._worker_replacements_inflight = 0
    node._worker_death_reports = {}
    node._dead_worker_exitcodes = {}
    node._shutdown_request_id = "draining"
    node._stop_event = threading.Event()
    node._worker_supervisor_stop = threading.Event()
    node._worker_lifecycle_lock = threading.Lock()
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._actor_workers = {}
    node._actor_generation_outcomes = {}
    node._dependency_pin_cleanups = {}
    node._pinned_transfers = {}
    node.event_sink = None
    node._flush_pending_resource_report = lambda: False
    return node, incarnation, process, request


def test_unexpected_exit_reclaims_locally_and_replays_one_frozen_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, incarnation, process, lease = _death_node()
    calls: list[protocol.ReportWorkerDeath] = []
    phase = 0

    def rpc(_address, handler, report, **_kwargs):
        nonlocal phase
        assert handler == GCS_REPORT_WORKER_DEATH_HANDLER
        assert isinstance(report, protocol.ReportWorkerDeath)
        calls.append(report)
        phase += 1
        if phase == 1:
            raise RuntimeError("GCS unavailable")
        if phase == 2:
            drifted = protocol.WorkerIncarnation(
                incarnation.node_id, incarnation.node_pid,
                incarnation.node_registration_epoch, incarnation.worker_id,
                incarnation.worker_pid + 1,
            )
            death = protocol.WorkerDeathRecord(
                report.detection_id, drifted, 1, report.exit_code, report.reason
            )
            return protocol.ReportWorkerDeathReply(
                report.detection_id, report.worker_id,
                protocol.WorkerDeathDisposition.APPLIED, 1, death,
            )
        death = protocol.WorkerDeathRecord(
            report.detection_id, report.incarnation, 1, report.exit_code,
            report.reason,
        )
        return protocol.ReportWorkerDeathReply(
            report.detection_id, report.worker_id,
            protocol.WorkerDeathDisposition.ALREADY_DEAD, 1, death,
        )

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    assert node._handle_unexpected_worker_exit(incarnation.worker_id, process)

    # Local capacity and the lease outcome converge before the first failed GCS
    # report; replacement and owner cleanup need not wait on network recovery.
    assert node._ledger.available == node._ledger.total
    assert node._leases[lease.lease_id].state is protocol.LeaseExecutionState.WORKER_LOST
    outcome = node._worker_death_reports[incarnation.worker_id]
    frozen = outcome.report
    assert frozen.incarnation == incarnation
    assert frozen.exit_code == 23
    assert frozen.reason is protocol.WorkerDeathReason.PROCESS_EXIT
    assert outcome.worker_address == ("127.0.0.1", 29101)
    assert not node._cleanup_plane_quiescent_locked()

    assert not node._flush_pending_worker_death_reports()
    assert node._worker_death_reports[incarnation.worker_id].report is frozen
    assert node._flush_pending_worker_death_reports()
    assert incarnation.worker_id not in node._worker_death_reports
    assert len(calls) == 3 and all(report is frozen for report in calls)


def test_intentional_worker_stop_does_not_publish_process_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, incarnation, process, _lease = _death_node()
    # Make the child live at the intentional-stop cut.  The shutdown helper
    # will terminate this simple fake after its no-op join.
    process.alive = True
    process.exitcode = None
    node._worker_death_reports.clear()
    sent: list[str] = []

    def rpc(_address, handler, _message, **_kwargs):
        sent.append(handler)
        return object()

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    result = node._stop_worker_slot(incarnation.worker_id)

    assert result.forced and process.closed
    assert node._worker_death_reports == {}
    assert GCS_REPORT_WORKER_DEATH_HANDLER not in sent
