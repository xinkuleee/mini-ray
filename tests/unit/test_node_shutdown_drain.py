"""L1 Node drain barriers with the original real-thread interleavings.

Run each exact case alone through the 30-second bounded runner. The first
starts one request thread; the second starts two exact replay threads. Each
event/count wait is at most one second, normal joins share two seconds, and
failure-finally joins share one second after releasing the test-owned gate.
Thread errors have a 16-entry cap and are asserted, never lost in a lambda.

The Node is an unstarted in-memory fixture with one logical CPU and an inert
Worker slot: no process, socket, Core, ObjectStore, timer or user Task runs.
The first case pauses the real pre-grant localization boundary, then calls the
original localizer with empty dependencies; it does not exercise byte pulling
or pending replica custody. Its typed cached Worker-clean observation remains
an explicit fixture premise, not evidence of real Worker drain. The second
preserves a real cached grant across BeginDrain and retires it with the real
CancelWorkerLease handler, never by clearing tables or releasing the ledger.
Lock acquisition in production handlers is not cancellable by these waits;
the outer runner bounds the experiment, not the runtime protocol.
"""

from __future__ import annotations

from collections import deque
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.core import CoreWorker
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.node import NodeServer, _WorkerSlot
from miniray.object_store import ObjectStore
from miniray.resources import NodeSnapshot, ResourceLedger, ResourceVector
from miniray.transport import TCPServer
from miniray.worker import WorkerServer


pytestmark = pytest.mark.loopback_smoke
_EVENT_SECONDS = 1.0
_NORMAL_JOIN_SECONDS = 2.0
_CLEANUP_JOIN_SECONDS = 1.0
_ERROR_LIMIT = 16


@pytest.fixture(autouse=True)
def _no_runtime_infrastructure(monkeypatch):
    violations: deque[BaseException] = deque(maxlen=_ERROR_LIMIT)

    def forbidden(*_args, **_kwargs):
        error = AssertionError("Node drain L1 allows only its exact test-owned threads")
        violations.append(error)
        raise error

    for kind, method in (
        (NodeServer, "__init__"), (NodeServer, "start"),
        (CoreWorker, "__init__"), (WorkerServer, "__init__"),
        (ObjectStore, "__init__"), (TCPServer, "__init__"),
        (multiprocessing.process.BaseProcess, "start"),
        (threading.Timer, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr("miniray.node.rpc_request", forbidden)
    yield
    assert not violations, tuple(violations)


class _AliveWorker:
    def is_alive(self) -> bool:
        return True


def _node() -> tuple[NodeServer, WorkerID]:
    node_id = NodeID.random()
    worker_id = WorkerID.random()
    total = ResourceVector({"CPU": 1})
    process = _AliveWorker()

    node = object.__new__(NodeServer)
    node.node_id = node_id
    node.worker_id = worker_id
    node.num_workers_per_node = 1
    node._worker_order = (worker_id,)
    node._workers = {
        worker_id: _WorkerSlot(
            worker_id, process=process, address=("127.0.0.1", 19101), pid=4101
        )
    }
    node._legacy_worker_compat = False
    node._worker_process = process
    node._worker_address = ("127.0.0.1", 19101)
    node._worker_pid = 4101
    node._worker_exitcode = None
    node._worker_forced = False
    node._active_lease_id = None
    node._ledger = ResourceLedger(total)
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_cancellations = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._pinned_transfers = {}
    node._dependency_pin_cleanups = {}
    node._object_localization_locks = {}
    node._actor_workers = {}
    node._actor_creation_locks = {}
    node._worker_drain_statuses = {}
    node._worker_finalize_results = {}
    node._shutdown_request_id = None
    node._stop_event = threading.Event()
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_address = None
    node._registered_with_gcs = False
    node._cluster_nodes = (NodeSnapshot(node_id, total, total),)
    node._cluster_addresses = {node_id: ("127.0.0.1", 19001)}
    return node, worker_id


def _request(node: NodeServer, *, lease_id: LeaseID | None = None):
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return protocol.RequestWorkerLease(
        lease_id or LeaseID.random(),
        task_id,
        AttemptID(task_id, 0),
        ResourceVector({"CPU": 1}),
        node.node_id,
        WorkerID.random(),
        target_node_id=node.node_id,
    )


def _wait_for_count(node: NodeServer, expected: int) -> None:
    deadline = time.monotonic() + _EVENT_SECONDS
    wake = threading.Event()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if node._state_lock.acquire(timeout=min(0.01, remaining)):
            try:
                if node._inflight_lease_requests == expected:
                    return
            finally:
                node._state_lock.release()
        wake.wait(min(0.001, max(0.0, deadline - time.monotonic())))
    raise AssertionError("in-flight lease count did not reach {}".format(expected))


def _request_once(node, request, replies, failures) -> None:
    try:
        replies.append(node._handle_request_lease(request))
    except BaseException as exc:
        failures.append(exc)


def _join_threads(threads, seconds: float) -> None:
    """Join the exact started prefix, including start-then-raise failures."""

    deadline = time.monotonic() + seconds
    for thread in threads:
        if thread.ident is not None:
            thread.join(max(0.0, deadline - time.monotonic()))
    assert all(not thread.is_alive() for thread in threads)


def _cancel(node, request):
    return node._handle_cancel_worker_lease(protocol.CancelWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        request.requester_node_id, request.requester_worker_id,
        scheduling_key=request.scheduling_key, lease_request=request,
    ))


def _assert_idle_node(node, worker_id) -> None:
    # Call only after all exact request threads have stopped. No lock spans a
    # join, and no cleanup assignment manufactures idle/custody observations.
    assert node._inflight_lease_requests == 0
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._workers[worker_id].active_lease_id is None
    assert node._drain_resources_clean_locked()


def test_inflight_localization_blocks_drain_and_cannot_late_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, worker_id = _node()
    request = _request(node)
    entered = threading.Event()
    release = threading.Event()
    replies: list[object] = []
    failures: deque[BaseException] = deque(maxlen=_ERROR_LIMIT)
    original_localize = node._localize_dependencies

    def blocked_localize(dependencies):
        try:
            assert dependencies == ()
            entered.set()
            assert release.wait(_EVENT_SECONDS)
            return original_localize(dependencies)
        except BaseException as exc:
            # The Node converts ordinary localizer errors into typed rejects;
            # retain a test-gate failure independently of that production catch.
            failures.append(exc)
            raise

    monkeypatch.setattr(node, "_localize_dependencies", blocked_localize)
    thread = threading.Thread(
        target=_request_once, args=(node, request, replies, failures),
        name="node-drain-localization-request", daemon=True,
    )
    try:
        thread.start()
        assert entered.wait(_EVENT_SECONDS)
        _wait_for_count(node, 1)

        epoch = "drain-localization"
        begin = protocol.BeginDrain(epoch)
        node._handle_begin_drain(begin)
        # Retain the original typed cached observation: this isolates Node
        # admission/counting, and does not claim to execute Worker drain.
        node._worker_drain_statuses[worker_id] = protocol.DrainStatus(
            epoch, "worker:{}".format(worker_id), True, True
        )
        during = node._node_drain_status(begin, drive=False)
        assert not during.clean and not during.resources_clean

        release.set()
        _join_threads((thread,), _NORMAL_JOIN_SECONDS)
        assert not thread.is_alive()
        assert not failures, tuple(failures)
        assert len(replies) == 1
        assert isinstance(replies[0], protocol.RejectWorkerLease)
        assert replies[0].reason is protocol.LeaseRejectReason.SHUTTING_DOWN
        assert node._inflight_lease_requests == 0
        assert node.resource_ledger.available == node.resource_ledger.total
        assert node._workers[worker_id].active_lease_id is None

        after = node._node_drain_status(begin, drive=False)
        assert after.clean and after.resources_clean
    finally:
        release.set()
        _join_threads((thread,), _CLEANUP_JOIN_SECONDS)
        # Failure before BeginDrain may have allowed a legitimate grant. Keep
        # that rollback separate from the original no-late-grant assertions.
        if request.lease_id in node._leases:
            cleanup = _cancel(node, request)
            assert cleanup.accepted and cleanup.cancelled
        _assert_idle_node(node, worker_id)
        assert not failures, tuple(failures)


def test_exact_cached_replays_are_counted_and_balance_after_begin_drain() -> None:
    node, worker_id = _node()
    request = _request(node)
    replies: list[object] = []
    failures: deque[BaseException] = deque(maxlen=_ERROR_LIMIT)
    threads = tuple(
        threading.Thread(
            target=_request_once, args=(node, request, replies, failures),
            name="node-drain-cached-replay-{}".format(index), daemon=True,
        )
        for index in range(2)
    )
    request_lock = None
    request_lock_held = False
    cancellation = None
    try:
        grant = node._handle_request_lease(request)
        assert isinstance(grant, protocol.GrantWorkerLease)
        assert node._inflight_lease_requests == 0

        node._handle_begin_drain(protocol.BeginDrain("drain-replay"))
        request_lock = node._lease_request_locks[request.lease_id]
        assert request_lock.acquire(timeout=_EVENT_SECONDS)
        request_lock_held = True
        for thread in threads:
            thread.start()
        _wait_for_count(node, 2)
        assert not node._drain_resources_clean_locked()
        request_lock.release()
        request_lock_held = False
        _join_threads(threads, _NORMAL_JOIN_SECONDS)

        assert all(not thread.is_alive() for thread in threads)
        assert not failures, tuple(failures)
        assert replies == [grant, grant]
        assert node._inflight_lease_requests == 0
        assert node._lease_outcomes[request.lease_id].reply == grant

        # BeginDrain preserves the accepted grant; only an exact terminal
        # transition releases its allocation and Worker binding.
        cancellation = _cancel(node, request)
        assert cancellation.accepted and cancellation.cancelled and cancellation.released
        assert cancellation.retired_grant == grant
        assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        _assert_idle_node(node, worker_id)
    finally:
        if request_lock_held:
            request_lock.release()
        _join_threads(threads, _CLEANUP_JOIN_SECONDS)
        if cancellation is None:
            # Preserve the first normal-path receipt; failure cleanup cannot
            # replace its released=True evidence or the original assertions.
            cleanup = _cancel(node, request)
            assert cleanup.accepted and cleanup.cancelled
        _assert_idle_node(node, worker_id)
        assert not failures, tuple(failures)
