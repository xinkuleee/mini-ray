"""Two real Node lease reducers reclaim active/yielded resource allocations.

An unstarted Node, two fixed ledger tokens and exact passive granted lease
records. Start/Blocked/worker-exit reclaim and late Start/Unblocked call the
production methods. No Task publication, process death detection, Node server,
socket, worker spawning or coordinator runs. This is terminal accounting and
identity fencing, not an assertion that a dead Node locally releases itself.
The existing real Node-crash smoke covers membership and managed process exit.
"""

from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import node as node_module, protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseOutcome, _LeaseRecord, _WorkerSlot
from miniray.resources import AllocationState, AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def no_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("terminal Node resource contract attempted runtime")
    for kind, name in ((NodeServer, "__init__"), (threading.Thread, "start"),
                       (threading.Thread, "join"), (threading.Timer, "start"),
                       (threading.Event, "wait"), (threading.Condition, "wait"),
                       (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


def _two_granted_leases():
    node = object.__new__(NodeServer)
    node.node_id = NodeID(b"n" * 16)
    node._state_lock = threading.RLock()
    node._ledger = ResourceLedger(ResourceVector({"CPU": 2, "GPU": 1, "custom": 1}))
    node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
    node._leases, node._lease_outcomes, node._workers = {}, {}, {}
    node._worker_order = (WorkerID(b"a" * 16), WorkerID(b"b" * 16))
    node._registered_with_gcs = False
    node.event_sink = None
    job = JobID(b"j" * 16)
    requests = []
    for index, worker in enumerate(node._worker_order):
        task = TaskID.derive(job, TaskID.for_driver(job), index)
        lease = LeaseID(bytes((index + 1,)) * 16)
        resources = ResourceVector({"CPU": 1, "GPU" if index == 0 else "custom": 1})
        token = node._ledger.allocate(resources, AllocationToken('terminal-' + str(index)))
        request = protocol.RequestWorkerLease(lease, task, AttemptID(task, 0), resources,
            node.node_id, WorkerID(b"o" * 16), target_node_id=node.node_id, return_ids=(ObjectID.for_task(task),))
        grant = protocol.GrantWorkerLease(lease, task, request.attempt_id, node.node_id, worker,
            ('worker.invalid', index + 1), token)
        node._leases[lease] = _LeaseRecord(request, token, grant)
        node._lease_outcomes[lease] = _LeaseOutcome(request, grant)
        node._workers[worker] = _WorkerSlot(worker, active_lease_id=lease)
        requests.append((request, grant))
    node._refresh_local_cached_availability_locked()
    return node, requests


def test_worker_exit_reclaims_active_and_yielded_leases_once_and_fences_late_notifications(monkeypatch):
    node, pairs = _two_granted_leases()
    for request, grant in pairs:
        started = node._handle_start_worker_lease(protocol.StartWorkerLease(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
    first, grant = pairs[0]
    blocked = protocol.NotifyWorkerBlocked(first.lease_id, first.task_id, first.attempt_id, grant.worker_id, 0)
    assert node._handle_notify_worker_blocked(blocked).changed
    assert node._ledger.available == ResourceVector({"CPU": 1})
    observed = []
    release = node._ledger.release
    def record_release(token):
        reply = release(token)
        observed.append((token, reply))
        assert len(observed) <= 2
        return reply
    monkeypatch.setattr(node._ledger, 'release', record_release)
    for request, grant in pairs:
        # This is the actual post-exit reducer boundary. The surrounding test
        # deliberately does not claim to generate an OS/membership death fact.
        with node._state_lock:
            assert node._reclaim_active_lease_after_worker_exit_locked(grant.worker_id)
            assert not node._reclaim_active_lease_after_worker_exit_locked(grant.worker_id)
        record = node._leases[request.lease_id]
        assert record.state is protocol.LeaseExecutionState.WORKER_LOST
        assert not record.blocking_open and not record.cpu_yielded
        assert node._ledger.record(grant.allocation_token).state is AllocationState.RELEASED
        late_start = protocol.StartWorkerLease(request.lease_id, request.task_id, request.attempt_id, grant.worker_id)
        assert not node._handle_start_worker_lease(late_start).accepted
        late_unblock = protocol.NotifyWorkerUnblocked(request.lease_id, request.task_id, request.attempt_id, grant.worker_id, 0)
        assert not node._handle_notify_worker_unblocked(late_unblock).accepted
        assert not node._handle_notify_worker_blocked(replace(blocked,
            lease_id=request.lease_id, task_id=request.task_id, attempt_id=request.attempt_id, worker_id=grant.worker_id)).accepted
    assert observed == [(grant.allocation_token, True) for _, grant in pairs]
    assert node._ledger.available == node._ledger.total and node._ledger.cpu_debt == 0
    assert all(slot.active_lease_id is None for slot in node._workers.values())
