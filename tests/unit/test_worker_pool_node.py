"""Pure state-machine tests for the bounded ordinary-Worker pool.

These tests construct the real Node lease authority around two in-memory Worker
slots. The surviving slot completes through real in-memory output discovery,
Prepare/ARM and Complete; its original allocation is released only by that
Complete transition. They create no listener, background thread, or child
process. A one-slot INLINE journal and at most a 1 KiB empty store stay local.
"""

from __future__ import annotations

from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _WorkerSlot
from miniray.output_publication import OutputPublicationCompleteWitness
from miniray.resources import NodeSnapshot, ResourceLedger, ResourceVector
from tests.unit._pure_node_output import prepare_ref_free_output


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Worker-pool authority attempted runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Event, "wait"), (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _AliveWorker:
    def is_alive(self) -> bool:
        return True


class _ExitedWorker:
    def __init__(self, pid: int, exitcode: int) -> None:
        self.pid = pid
        self.exitcode = exitcode
        self.closed = False

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float) -> None:
        del timeout

    def close(self) -> None:
        self.closed = True


def _two_slot_node() -> tuple[NodeServer, tuple[WorkerID, WorkerID]]:
    node_id = NodeID.random()
    worker_ids = (WorkerID.random(), WorkerID.random())
    total = ResourceVector({"CPU": 3})

    node = object.__new__(NodeServer)
    node.node_id = node_id
    node.worker_id = worker_ids[0]
    node.num_workers_per_node = 2
    node._worker_order = worker_ids
    node._workers = {
        worker_ids[0]: _WorkerSlot(
            worker_ids[0],
            process=_AliveWorker(),
            address=("127.0.0.1", 19101),
            pid=4101,
        ),
        worker_ids[1]: _WorkerSlot(
            worker_ids[1],
            process=_AliveWorker(),
            address=("127.0.0.1", 19102),
            pid=4102,
        ),
    }
    node._legacy_worker_compat = False
    node._worker_process = node._workers[worker_ids[0]].process
    node._worker_address = node._workers[worker_ids[0]].address
    node._worker_pid = node._workers[worker_ids[0]].pid
    node._worker_exitcode = None
    node._worker_forced = False
    node._active_lease_id = None
    node._ledger = ResourceLedger(total)
    node._gcs_address = None
    node._registered_with_gcs = False
    node._cluster_nodes = (NodeSnapshot(node_id, total, total),)
    node._cluster_addresses = {}
    node._shutdown_request_id = None
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_cancellations = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node._stop_event = threading.Event()
    return node, worker_ids


def _request(node: NodeServer, index: int) -> protocol.RequestWorkerLease:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return protocol.RequestWorkerLease(
        lease_id=LeaseID.random(),
        task_id=task_id,
        attempt_id=AttemptID(task_id, 0),
        resources=ResourceVector({"CPU": 1}),
        requester_node_id=NodeID.random(),
        requester_worker_id=WorkerID.random(),
        target_node_id=node.node_id,
    )


def _start(
    request: protocol.RequestWorkerLease, grant: protocol.GrantWorkerLease
) -> protocol.StartWorkerLease:
    return protocol.StartWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id
    )


def _complete(
    request: protocol.RequestWorkerLease, grant: protocol.GrantWorkerLease
) -> protocol.CompleteWorkerLease:
    return protocol.CompleteWorkerLease(
        request.lease_id,
        request.task_id,
        request.attempt_id,
        grant.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED,
    )


def test_two_slots_each_hold_one_active_lease_and_third_waits() -> None:
    node, worker_ids = _two_slot_node()
    first = _request(node, 0)
    second = _request(node, 1)
    waiting = _request(node, 2)

    first_grant = node._handle_request_lease(first)
    second_grant = node._handle_request_lease(second)
    rejected = node._handle_request_lease(waiting)

    assert isinstance(first_grant, protocol.GrantWorkerLease)
    assert isinstance(second_grant, protocol.GrantWorkerLease)
    assert (first_grant.worker_id, second_grant.worker_id) == worker_ids
    assert tuple(
        node._workers[worker_id].active_lease_id for worker_id in worker_ids
    ) == (first.lease_id, second.lease_id)
    assert isinstance(rejected, protocol.RejectWorkerLease)
    assert rejected.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
    # One CPU remains, so this rejection proves the Worker-slot bound rather
    # than merely observing an exhausted resource ledger.
    assert node.resource_ledger.available == ResourceVector({"CPU": 1})

    # Exact replay is bound to the existing slot and cannot consume the other.
    assert node._handle_request_lease(first) == first_grant
    assert tuple(
        node._workers[worker_id].active_lease_id for worker_id in worker_ids
    ) == (first.lease_id, second.lease_id)

    released = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            first.lease_id, first_grant.worker_id, first_grant.allocation_token
        )
    )
    assert released.released
    waiting_grant = node._handle_request_lease(waiting)
    assert isinstance(waiting_grant, protocol.GrantWorkerLease)
    assert waiting_grant.worker_id == worker_ids[0]
    assert tuple(
        node._workers[worker_id].active_lease_id for worker_id in worker_ids
    ) == (waiting.lease_id, second.lease_id)


def test_one_worker_loss_reclaims_only_its_lease_and_preserves_other_slot() -> None:
    node, worker_ids = _two_slot_node()
    node._node_pid, node._registration_epoch = 4000, 1
    first = _request(node, 0)
    second = _request(node, 1)
    second = replace(second, return_ids=(ObjectID.for_task(second.task_id),))
    waiting = _request(node, 2)
    first_grant = node._handle_request_lease(first)
    second_grant = node._handle_request_lease(second)
    assert isinstance(first_grant, protocol.GrantWorkerLease)
    assert isinstance(second_grant, protocol.GrantWorkerLease)
    assert node._handle_start_worker_lease(_start(first, first_grant)).accepted
    assert node._handle_start_worker_lease(_start(second, second_grant)).accepted

    exited = _ExitedWorker(pid=4101, exitcode=-9)
    node._workers[worker_ids[0]].process = exited
    result = node._stop_worker_slot(worker_ids[0])

    assert result.worker_id == worker_ids[0]
    assert result.exitcode == -9 and not result.clean and not result.forced
    assert exited.closed
    assert node._leases[first.lease_id].state is (
        protocol.LeaseExecutionState.WORKER_LOST
    )
    assert node._workers[worker_ids[0]].active_lease_id is None
    assert node._workers[worker_ids[0]].address is None

    # The surviving slot and allocation remain authoritative and untouched.
    assert node._leases[second.lease_id].state is (
        protocol.LeaseExecutionState.RUNNING
    )
    assert node._workers[worker_ids[1]].active_lease_id == second.lease_id
    assert node._workers[worker_ids[1]].address == second_grant.worker_address
    assert node.resource_ledger.available == ResourceVector({"CPU": 2})

    late = node._handle_complete_worker_lease(_complete(first, first_grant))
    assert not late.accepted and not late.released
    assert late.state is protocol.LeaseExecutionState.WORKER_LOST

    # The dead endpoint is not grantable while the surviving Worker is busy.
    pending = node._handle_request_lease(waiting)
    assert isinstance(pending, protocol.RejectWorkerLease)
    assert pending.reason is protocol.LeaseRejectReason.PENDING_CAPACITY

    allocation_before = node.resource_ledger.record(second_grant.allocation_token)
    before_prepare = node.resource_ledger.snapshot()
    publication = prepare_ref_free_output(node, second, second_grant, values=(7,))
    identity = publication.manifest.publication_id
    record = node._leases[second.lease_id]
    assert record.output_publication_id == identity
    assert publication.journal.snapshot(identity).ready_to_complete
    assert publication.recovery.snapshot(identity).armed
    assert publication.journal.snapshot(identity).complete is None
    assert record.state is protocol.LeaseExecutionState.RUNNING
    assert node._workers[worker_ids[1]].active_lease_id == second.lease_id
    assert node.resource_ledger.snapshot() == before_prepare
    assert node.resource_ledger.record(second_grant.allocation_token) == allocation_before

    completed = node._handle_complete_worker_lease(_complete(second, second_grant))
    assert completed.accepted and completed.released
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._workers[worker_ids[1]].active_lease_id is None
    assert completed.output_publication.manifest == publication.manifest
    witness = OutputPublicationCompleteWitness.for_manifest(publication.manifest)
    assert completed.output_publication.complete == publication.journal.snapshot(identity).complete == witness
    after_complete = node.resource_ledger.snapshot()
    replay = node._handle_complete_worker_lease(_complete(second, second_grant))
    assert replay.accepted and not replay.released
    assert replay.output_publication == completed.output_publication
    assert node.resource_ledger.snapshot() == after_complete
    waiting_grant = node._handle_request_lease(waiting)
    assert isinstance(waiting_grant, protocol.GrantWorkerLease)
    assert waiting_grant.worker_id == worker_ids[1]
    assert waiting_grant.allocation_token != second_grant.allocation_token
    assert node._workers[worker_ids[1]].active_lease_id == waiting.lease_id
    assert node.resource_ledger.available == ResourceVector({"CPU": 2})
    assert node._leases[first.lease_id].state is (
        protocol.LeaseExecutionState.WORKER_LOST
    )
