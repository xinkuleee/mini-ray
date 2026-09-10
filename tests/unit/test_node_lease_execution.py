"""Pure Node lease reducers, including Worker-loss publication compensation.

Success uses actual Prepare/ARM and Complete. The original Worker-exit case
now makes one real Grant/Start, prepares one tiny INLINE slot, and supplies a
passive process-exit input to the real Node reclaim reducer. Late successful
Complete is rejected for Worker loss, not merely for missing preparation. One
manual output-driver round retires its slot and obtains the real rollback ACK.
No process, thread, socket, wait, user code or worker-stop runtime is exercised.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import node as node_module, output_protocol as wire, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseOutcome, _LeaseRecord, _WorkerSlot
from miniray.output_publication_journal import OutputPublicationJournalState, OutputPublicationStage
from miniray.output_handoff import OutputHandoffPhase
from miniray.resources import AllocationState, AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector
from tests.unit._pure_node_output_current import prepare_ref_free_output


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _lease_fixture() -> tuple[
    NodeServer,
    protocol.RequestWorkerLease,
    protocol.GrantWorkerLease,
]:
    node_id = _id(NodeID, 1)
    worker_id = _id(WorkerID, 2)
    job_id = _id(JobID, 3)
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    attempt_id = AttemptID(task_id, 0)
    lease_id = _id(LeaseID, 4)
    requested = ResourceVector({"CPU": 1})
    allocation = AllocationToken("lease-allocation")

    node = object.__new__(NodeServer)
    node.node_id = node_id
    node._node_pid = 4101
    node._registration_epoch = 1
    node._state_lock = threading.RLock()
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._ledger.allocate(requested, allocation)
    node._cluster_nodes = (
        NodeSnapshot(node_id, node._ledger.total, node._ledger.available),
    )
    node._leases = {}
    node._lease_outcomes = {}

    request = protocol.RequestWorkerLease(
        lease_id=lease_id,
        task_id=task_id,
        attempt_id=attempt_id,
        resources=requested,
        requester_node_id=node_id,
        requester_worker_id=_id(WorkerID, 5),
        target_node_id=node_id,
        return_ids=(ObjectID.for_task(task_id),),
    )
    grant = protocol.GrantWorkerLease(
        lease_id=lease_id,
        task_id=task_id,
        attempt_id=attempt_id,
        node_id=node_id,
        worker_id=worker_id,
        worker_address=("127.0.0.1", 12002),
        allocation_token=allocation,
    )
    record = _LeaseRecord(request, allocation, grant)
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(worker_id, active_lease_id=lease_id)}
    node.num_workers_per_node = 1
    node._leases[lease_id] = record
    node._lease_outcomes[lease_id] = _LeaseOutcome(request, grant)
    return node, request, grant


def _start(request: protocol.RequestWorkerLease, worker_id: WorkerID):
    return protocol.StartWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, worker_id
    )


def _complete(
    request: protocol.RequestWorkerLease,
    worker_id: WorkerID,
    status: protocol.TaskReplyStatus = protocol.TaskReplyStatus.SUCCEEDED,
):
    return protocol.CompleteWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, worker_id, status
    )


@pytest.mark.unit
def test_start_and_complete_are_idempotent_and_release_once() -> None:
    node, request, grant = _lease_fixture()

    start = _start(request, grant.worker_id)
    first_start = node._handle_start_worker_lease(start)
    repeated_start = node._handle_start_worker_lease(start)
    assert first_start == repeated_start
    assert first_start.accepted
    assert first_start.state is protocol.LeaseExecutionState.RUNNING
    assert node.resource_ledger.available.is_zero()

    publication = prepare_ref_free_output(node, request, grant)
    complete = _complete(request, grant.worker_id)
    first_complete = node._handle_complete_worker_lease(complete)
    repeated_complete = node._handle_complete_worker_lease(complete)
    assert first_complete.accepted and first_complete.released
    assert first_complete.output_publication.manifest == publication.manifest
    assert publication.handoffs.query(publication.manifest.publication_id).complete is None
    assert repeated_complete.accepted and not repeated_complete.released
    assert repeated_complete.state is protocol.LeaseExecutionState.COMPLETED
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._workers[node.worker_id].active_lease_id is None

    changed = _complete(
        request, grant.worker_id, protocol.TaskReplyStatus.APPLICATION_ERROR
    )
    rejected = node._handle_complete_worker_lease(changed)
    assert not rejected.accepted and not rejected.released
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.COMPLETED


@pytest.mark.unit
def test_wrong_execution_identity_never_changes_lease_state() -> None:
    node, request, grant = _lease_fixture()
    wrong_worker = _id(WorkerID, 6)

    start_reply = node._handle_start_worker_lease(_start(request, wrong_worker))
    complete_reply = node._handle_complete_worker_lease(
        _complete(request, wrong_worker)
    )

    assert not start_reply.accepted
    assert not complete_reply.accepted and not complete_reply.released
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.GRANTED
    assert node.resource_ledger.available.is_zero()


@pytest.mark.unit
def test_release_only_abandons_a_granted_lease() -> None:
    node, request, grant = _lease_fixture()
    release = protocol.ReleaseWorkerLease(
        request.lease_id, grant.worker_id, grant.allocation_token
    )

    node._handle_start_worker_lease(_start(request, grant.worker_id))
    running = node._handle_release_lease(release)
    assert not running.released
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.RUNNING
    assert node.resource_ledger.available.is_zero()

    prepare_ref_free_output(node, request, grant)
    assert node._handle_complete_worker_lease(_complete(request, grant.worker_id)).accepted
    terminal = node._handle_release_lease(release)
    assert not terminal.released
    assert node.resource_ledger.available == node.resource_ledger.total

    abandoned_node, abandoned_request, abandoned_grant = _lease_fixture()
    abandon = protocol.ReleaseWorkerLease(
        abandoned_request.lease_id,
        abandoned_grant.worker_id,
        abandoned_grant.allocation_token,
    )
    first = abandoned_node._handle_release_lease(abandon)
    replay = abandoned_node._handle_release_lease(abandon)
    assert first.released and not replay.released
    assert (
        abandoned_node._leases[abandoned_request.lease_id].state
        is protocol.LeaseExecutionState.ABANDONED
    )


@pytest.fixture
def _no_worker_exit_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Worker-exit reducer attempted runtime infrastructure")

    for kind, method in (
        (NodeServer, "__init__"),
        (NodeServer, "_stop_workers"), (NodeServer, "_stop_worker_slot"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


class _PassiveExitWorker:
    """Passive exit/liveness data, with no start, join or close implementation."""
    pid = 4242

    def __init__(self):
        self.exitcode = None

    def is_alive(self):
        return self.exitcode is None


def _worker_exit_lease_fixture():
    """The same logical IDs as the old fixture, but a genuinely granted lease."""
    node = object.__new__(NodeServer)
    node.node_id, worker_id = _id(NodeID, 1), _id(WorkerID, 2)
    node._node_pid, node._registration_epoch = 4101, 1
    node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
    node._cluster_addresses = {}
    node._gcs_address, node._registered_with_gcs = None, False
    process = _PassiveExitWorker()
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(
        worker_id, process=process, address=("worker-exit.invalid", 1), pid=process.pid,
    )}
    node.num_workers_per_node = 1
    node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
    node._lease_request_locks, node._inflight_lease_requests = {}, 0
    node.event_sink = None
    job_id = _id(JobID, 3)
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    request = protocol.RequestWorkerLease(
        _id(LeaseID, 4), task_id, AttemptID(task_id, 0), ResourceVector({"CPU": 1}),
        node.node_id, _id(WorkerID, 5), target_node_id=node.node_id,
        return_ids=(ObjectID.for_task(task_id),),
    )
    grant = node._handle_request_lease(request)
    assert type(grant) is protocol.GrantWorkerLease
    assert node._leases[request.lease_id].request == request
    assert node._leases[request.lease_id].grant == grant
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.GRANTED
    assert node._lease_outcomes[request.lease_id].reply == grant
    assert node.resource_ledger.available.is_zero()
    return node, request, grant


@pytest.mark.unit
@pytest.mark.usefixtures("_no_worker_exit_runtime")
def test_worker_exit_reclaims_running_lease_and_fences_late_completion(monkeypatch) -> None:
    node, request, grant = _worker_exit_lease_fixture()
    start = _start(request, grant.worker_id)
    running = node._handle_start_worker_lease(start)
    assert running.accepted and running.state is protocol.LeaseExecutionState.RUNNING
    assert node.resource_ledger.available.is_zero()
    record = node._leases[request.lease_id]
    outcome = node._lease_outcomes[request.lease_id]
    slot = node._workers[grant.worker_id]
    assert slot.active_lease_id == request.lease_id and slot.process.is_alive()

    publication = prepare_ref_free_output(node, request, grant, values=(7,))
    identity = publication.manifest.publication_id
    prepared = publication.journal.snapshot(identity)
    assert prepared.ready_to_complete and prepared.retained_result_slots == (0,)
    assert prepared.complete is None and publication.handoffs.query(identity).manifest == publication.manifest
    assert (publication.manifest.value).size_bytes <= 32
    assert node.object_store.capacity_bytes == 1024 and node.object_store.used_bytes == 0
    release_calls = []
    release = node.resource_ledger.release

    def observe_release(token):
        assert token == grant.allocation_token
        released = release(token)
        release_calls.append((token, released))
        assert len(release_calls) <= 1
        return released

    monkeypatch.setattr(node.resource_ledger, "release", observe_release)
    slot.process.exitcode = -9  # exact passive OS observation; not elapsed time
    assert not slot.process.is_alive()
    with node._state_lock:
        assert node._reclaim_active_lease_after_worker_exit_locked(grant.worker_id)
        assert not node._reclaim_active_lease_after_worker_exit_locked(grant.worker_id)
    released_ledger = node.resource_ledger.snapshot()
    assert release_calls == [(grant.allocation_token, True)]
    assert len(released_ledger.allocations) == 1
    assert released_ledger.allocations[0].state is AllocationState.RELEASED
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST
    assert record.completion is None and record.output_complete_inflight is None
    assert slot.active_lease_id is None
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._lease_outcomes[request.lease_id] == outcome

    completion = _complete(request, grant.worker_id)
    late = node._handle_complete_worker_lease(completion)
    repeated = node._handle_complete_worker_lease(completion)
    assert late == repeated and not late.accepted and not late.released
    assert late.state is protocol.LeaseExecutionState.WORKER_LOST
    assert late.error == "first successful Complete requires a live RUNNING lease"
    assert late.output_publication is None and late.output_completion is None
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST
    assert record.completion is None and record.output_complete_inflight is None
    assert publication.journal.snapshot(identity) == prepared
    assert publication.handoffs.query(identity).complete is None
    assert not node._handle_start_worker_lease(start).accepted
    assert node.resource_ledger.snapshot() == released_ledger

    query = protocol.GetWorkerLeaseOutcome(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
        request.requester_worker_id, request.return_ids,
    )
    before_cleanup = node._handle_get_worker_lease_outcome(query)
    assert before_cleanup.found and not before_cleanup.worker_alive
    assert before_cleanup.state is protocol.LeaseExecutionState.WORKER_LOST
    assert before_cleanup.cleanup_pending and before_cleanup.completion_status is None
    assert before_cleanup.output_publication is None and before_cleanup.output_completion is None
    reports = []
    report_rollback = publication.report_rollback

    def observe_rollback(tombstone, *, manifest):
        assert manifest == publication.manifest and not reports
        reply = report_rollback(tombstone, manifest=manifest)
        assert type(reply) is wire.OutputHandoffReply and reply.accepted
        assert reply.request == wire.ReportOutputHandoffRollback(manifest, tombstone)
        assert reply.snapshot.phase is OutputHandoffPhase.ABORTED
        assert reply.snapshot.abort_reason == tombstone.plan.rollback_id
        reports.append((tombstone, reply))
        return reply

    monkeypatch.setattr(publication.adapter, "_report_rollback", observe_rollback)
    # Exactly one INLINE SLOT_DROP is due. The real supervisor/drain reducer
    # drives its compensation and matching metadata ACK; no stop thread runs.
    assert node._drive_output_publications()
    cleaned = publication.journal.snapshot(identity)
    assert cleaned.state is OutputPublicationJournalState.RETIRED
    assert cleaned.complete is None and cleaned.retained_result_slots == ()
    assert cleaned.rollback.rollback_id == "output-worker-lost:{}".format(request.lease_id)
    assert len(cleaned.rollback.effects) == 1
    assert cleaned.rollback.effects[0].stage is OutputPublicationStage.SLOT_DROP
    assert cleaned.rollback.effects[0].slot_index == 0
    assert len(reports) == 1 and reports[0][0] == cleaned.rollback_tombstone
    assert publication.rollback_reports == reports
    assert publication.handoffs.query(identity).abort_reason == cleaned.rollback.rollback_id
    assert publication.handoffs.query(identity).complete is None
    assert publication.handoffs.query(identity).adoption is None
    assert publication.adapter.rollback_reported(identity)
    assert not publication.adapter.pending_rollbacks()
    assert not publication.adapter.pending_terminal_reports()
    assert not publication.adapter.pending_lease_completions()
    assert node._drive_output_publications()  # replay cannot report or release twice
    assert len(reports) == 1 and release_calls == [(grant.allocation_token, True)]
    after_cleanup = node._handle_get_worker_lease_outcome(query)
    assert after_cleanup.found and not after_cleanup.worker_alive
    assert after_cleanup.state is protocol.LeaseExecutionState.WORKER_LOST
    assert not after_cleanup.cleanup_pending and after_cleanup.completion_status is None
    assert after_cleanup.output_publication is None and after_cleanup.output_completion is None
    assert after_cleanup.descriptors == after_cleanup.orphan_descriptors == ()
    with node._state_lock:
        assert node._output_publications_clean_locked()
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST
    assert record.completion is None and record.output_complete_inflight is None
    assert node.resource_ledger.snapshot() == released_ledger
    assert node.object_store.used_bytes == 0 and not node._sealed_metadata
    assert node._lease_outcomes[request.lease_id] == outcome
