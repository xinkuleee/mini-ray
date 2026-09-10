"""PG loss and system-retry admission share one Core linearization point.

Each case composes one threadless Core, two unstarted 1-KiB Nodes, and the
real NodeRegistry/PG coordinator. STRICT_SPREAD really prepares and commits
both bundle ledgers. The initial Task is canonically submitted and obtains a
real Grant/Start/SYSTEM_ERROR Complete, without output publication or user code.

One synchronous callback runs only after the actual outer Core RLock unlocks.
It commits the peer's membership death, reduces PG LOST, installs the complete
survivor snapshot on the surviving Node, then informs Core. The assertion
accepts either legal winner, but forbids consuming retry budget after LOST
already won. A retry which won first still meets the real fresh-dispatch gate.

This is pure authority composition, not a process-death or full GCS-service
gate. At most one logical Task, two physical attempts, three manual lane turns,
and two empty 1-KiB stores exist per case. No transport, thread, wait, timer,
subprocess, output bytes, or user execution is permitted. Dead-node ledgers
remain intact; the fixture never fabricates their physical destruction.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import control, core as core_module, node as node_module, protocol, transport
from miniray.control import NodeRegistry, PlacementGroupControlCoordinator
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, _NodeDeathObserved, _PendingTask, _ReadyTask, _STOP, _WAKE_COORDINATOR
from miniray.errors import PlacementGroupLostError, SystemTaskError
from miniray.ids import LeaseID
from miniray.node import NodeServer
from miniray.ownership import ObjectState
from miniray.placement import ReservationState
from miniray.placement_group_runtime import PlacementGroupPhase
from miniray.recovery import RecoveryAction, TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_node_placement_group_runtime import _node


pytestmark = pytest.mark.unit


def _forbidden(*_args, **_kwargs):
    pytest.fail("pure PG retry atomicity attempted runtime or unmodelled work")


def _never_execute():
    _forbidden("user code must not execute")


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, _forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, _forbidden)
    monkeypatch.setattr(subprocess, "Popen", _forbidden)
    monkeypatch.setattr(time, "sleep", _forbidden)
    for module in (control, core_module, node_module):
        monkeypatch.setattr(module, "rpc_request", _forbidden)
    monkeypatch.setattr(transport, "request", _forbidden)


class _AfterOutermostUnlock:
    """Preserve the real RLock/Condition identity; inject once while unlocked."""

    def __init__(self, lock):
        self.lock = lock
        self.depth = 0
        self.callback = None
        self.fired = 0

    def __enter__(self):
        self.lock.acquire()
        self.depth += 1
        return self

    def __exit__(self, *_args):
        self.depth -= 1
        self.lock.release()
        if self.depth == 0 and self.callback is not None:
            callback, self.callback = self.callback, None
            self.fired += 1
            assert self.fired == 1 and not self.lock._is_owned()
            callback()
        return False

    def __getattr__(self, name):
        return getattr(self.lock, name)


class _Scenario:
    def __init__(self, monkeypatch, *, ordinary=False):
        self.core = core = make_pure_core()
        self.output = None
        self.unlock = _AfterOutermostUnlock(core._state_lock)
        monkeypatch.setattr(core, "_state_lock", self.unlock)
        self.nodes = (_node(), _node())
        self.home, self.peer = self.nodes
        self.registry = NodeRegistry()
        self.participant_calls, self.control_calls, self.leases = [], [], []
        self.events = []
        self.death = None
        for index, node in enumerate(self.nodes):
            node._node_pid = 28101 + index
            # Address is metadata only; no TCPServer is constructed.
            node._server = SimpleNamespace(address=("pg-node-{}.invalid".format(index), 1))
            assert self.registry.register(
                node.node_id, node.address, node.resource_ledger.total, node_pid=node._node_pid,
            )
            node._registration_epoch = self.registry.get(node.node_id).registration_epoch
        self.pg = PlacementGroupControlCoordinator(self.registry, participant_rpc=self.participant_rpc)
        core.node_id, core.node_address = self.home.node_id, self.home.address
        core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
        core.gcs_address = ("pg-control.invalid", 1)
        core._rpc = self.control_rpc
        epoch, infos = self.registry.live_snapshot()
        initial = protocol.InstallClusterSnapshot(epoch, "pg-retry-two-nodes", infos)
        for node in self.nodes:
            assert node._handle_install_cluster_snapshot(initial).installed
        core._membership_epoch = epoch
        core._installed_cluster_snapshot = initial
        self.created = core.create_placement_group(
            (ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1})), "STRICT_SPREAD",
        )
        assert self.created.accepted and self.created.phase is protocol.PlacementGroupPhaseStatus.CREATED
        assert len(self.created.placements) == 2
        assert {key.node_id for key in self.created.placements} == {node.node_id for node in self.nodes}
        self.identity = self.created.placement_group_id, self.created.attempt
        assert core._placement_group_manifests[self.identity] == self.created.placements
        self.key = next(key for key in self.created.placements if key.node_id == self.home.node_id)
        assert len(self.participant_calls) == 4 and len(self.control_calls) == 1
        for node in self.nodes:
            assert node._bundle_reservations.snapshot(*self.identity).state is ReservationState.COMMITTED
            assert node.resource_ledger.available == ResourceVector({"CPU": 1})
        self.pending, self.output = core._register_submission(
            core.define_remote_function(_never_execute), (), {}, ResourceVector({"CPU": 1}),
            max_retries=1, placement_group_scheduling_key=None if ordinary else self.key, _enqueue=True,
        )
        assert core._submissions.get_nowait() is self.pending
        core._submissions.task_done()
        assert core._submissions.empty() and core._accepted_task_count == 1
        self.reply = self.complete_system_attempt(self.pending)
        self.observe_retry_commits(monkeypatch)

    def participant_rpc(self, address, handler, request):
        assert len(self.participant_calls) < 5
        node = next(node for node in self.nodes if node.address == address)
        assert node.node_id == request.node_id
        handlers = {
            control.PREPARE_PLACEMENT_GROUP_HANDLER: node._handle_prepare_placement_group,
            control.COMMIT_PLACEMENT_GROUP_HANDLER: node._handle_commit_placement_group,
            control.ABORT_PLACEMENT_GROUP_HANDLER: node._handle_abort_placement_group,
        }
        assert handler in handlers
        reply = handlers[handler](request)
        assert reply.accepted and reply.applied
        self.participant_calls.append((handler, request, reply))
        return reply

    def control_rpc(self, address, handler, request):
        assert address == self.core.gcs_address
        assert handler == control.CREATE_PLACEMENT_GROUP_HANDLER and not self.control_calls
        assert type(request) is protocol.CreatePlacementGroupRequest
        reply = self.pg.create(request)
        self.control_calls.append((request, reply))
        return reply

    def complete_system_attempt(self, pending):
        assert len(self.leases) < 2
        request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            self.core.node_id, self.core.worker_id, target_node_id=self.home.node_id,
            return_ids=pending.output_ids, scheduling_key=pending.spec.scheduling_key,
        )
        grant = self.home._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease and grant.scheduling_key == pending.spec.scheduling_key
        started = self.home._handle_start_worker_lease(protocol.StartWorkerLease(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id, request.scheduling_key,
        ))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        complete = protocol.CompleteWorkerLease(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
            protocol.TaskReplyStatus.SYSTEM_ERROR, request.scheduling_key,
        )
        completed = self.home._handle_complete_worker_lease(complete)
        assert completed.accepted and completed.released and completed.state is protocol.LeaseExecutionState.COMPLETED
        assert completed.output_publication is None and completed.output_completion is None
        record = self.home._leases[request.lease_id]
        assert record.completion == complete and record.output_publication_id is None
        assert not self.home._workers[grant.worker_id].active_lease_id
        self.leases.append((request, grant, completed))
        return protocol.TaskReply(
            request.task_id, request.attempt_id, grant.worker_id, completed.status,
            error=protocol.RemoteErrorInfo("RuntimeError", "known Worker system failure after real Node Complete"),
        )

    def observe_retry_commits(self, monkeypatch):
        core, pending = self.core, self.pending
        advance = core.owner_table.commit_validated_advance_task_outputs
        commit = core._recovery.commit_validated_transition

        def observe_owner(plan):
            assert core._state_lock._is_owned()
            result = advance(plan)
            assert core.owner_table.snapshot(pending.object_id).current_attempt == pending.spec.attempt_id.next()
            assert core._recovery.task_record(pending.task_id).retries_started == 0
            self.events.append("owner_retry")
            return result

        def observe_recovery(plan):
            result = commit(plan)
            if result.action is RecoveryAction.RETRY_TASK:
                assert core._state_lock._is_owned()
                assert core._recovery.task_record(pending.task_id).retries_started == 1
                assert core._recovery.task_record(pending.task_id).current_attempt == pending.spec.attempt_id.next()
                self.events.append("recovery_retry")
            return result

        monkeypatch.setattr(core.owner_table, "commit_validated_advance_task_outputs", observe_owner)
        monkeypatch.setattr(core._recovery, "commit_validated_transition", observe_recovery)

    def lose_peer(self):
        assert self.death is None and not self.core._state_lock._is_owned()
        reply = self.registry.report_death(protocol.ReportNodeDeath(
            "pg-retry-peer-exit", self.peer.node_id, self.peer._node_pid, self.peer._registration_epoch,
            -9, protocol.NodeDeathReason.PROCESS_EXIT, "peer death reducer input, not an OS process test",
        ))
        assert reply.disposition is protocol.NodeDeathDisposition.APPLIED and reply.death is not None
        self.death = reply.death
        affected = self.pg.fail_node(self.death)
        assert len(affected) == 1 and affected[0].phase is PlacementGroupPhase.LOST
        assert self.pg.visible_placement(self.identity[0]) is None and not self.pg.has_active_operations()
        assert len(self.participant_calls) == 5
        assert self.participant_calls[-1][0] == control.ABORT_PLACEMENT_GROUP_HANDLER
        assert self.participant_calls[-1][1].node_id == self.home.node_id
        assert self.home._bundle_reservations.snapshot(*self.identity).state is ReservationState.ABORTED
        assert self.home.resource_ledger.available == self.home.resource_ledger.total
        assert self.peer._bundle_reservations.snapshot(*self.identity).state is ReservationState.COMMITTED
        installed = protocol.InstallClusterSnapshot(reply.membership_epoch, "pg-retry-survivor", reply.live_nodes)
        assert tuple(node.node_id for node in installed.nodes) == (self.home.node_id,)
        assert self.home._handle_install_cluster_snapshot(installed).installed
        self.core.handle_node_death(self.death, installed)
        assert self.core._dead_nodes == {self.peer.node_id: self.death}
        assert self.core._installed_cluster_snapshot == installed
        assert self.core._placement_group_states[self.identity] is protocol.PlacementGroupPhaseStatus.LOST
        self.events.append("pg_lost")

    def take_retry(self):
        """Consume only the finite submission/Node-observation/wake prefix."""
        retries = []
        for _ in range(5):
            try:
                item = self.core._submissions.get_nowait()
            except queue.Empty:
                break
            try:
                if type(item) is _PendingTask:
                    retries.append(item)
                    assert len(retries) == 1
                elif type(item) is _NodeDeathObserved:
                    assert item.death == self.death
                    self.core._classify_node_death(item)
                else:
                    assert item is _WAKE_COORDINATOR
            finally:
                self.core._submissions.task_done()
        assert self.core._submissions.empty()
        return retries[0] if retries else None

    def dispatch_retry(self, retried):
        self.core._ready_tasks = queue.Queue(maxsize=2)
        self.core._ready_tasks.put_nowait(_ReadyTask(retried, retried.spec))
        self.core._ready_tasks.put_nowait(_STOP)
        self.core._dispatch_loop()
        assert self.core._ready_tasks.empty() and self.core._ready_tasks.unfinished_tasks == 0

    def assert_pg_terminal(self, *, retries):
        record = self.core._recovery.task_record(self.pending.task_id)
        snapshot = self.core.owner_table.snapshot(self.pending.object_id)
        expected = self.pending.spec.attempt_id.next() if retries else self.pending.spec.attempt_id
        assert record.current_attempt == snapshot.current_attempt == expected
        assert record.retries_started == retries and record.retries_remaining == 1 - retries
        assert record.state is TaskState.SYSTEM_FAILED and snapshot.state is ObjectState.ERROR
        assert isinstance(record.last_error, PlacementGroupLostError) and snapshot.error is record.last_error
        assert self.core._objects[self.pending.object_id].event.is_set()
        assert not self.core._protocol_unresolved and not self.core._task_finish_barriers
        assert self.core._accepted_task_count == 0 and self.pending.task_key in self.core._finished_tasks
        assert len(self.leases) == len(self.home._leases) == 1 and not self.peer._leases
        assert len(self.control_calls) == 1

    def close(self):
        self.unlock.callback = None
        if self.output is not None and not self.output.closed:
            # Event.wait is forbidden even for a ready receipt. Invoke the
            # actual local finalizer/mailbox release, without a fabricated ACK.
            done = self.output._release_done
            self.output._closed = True
            self.output._finalizer()
            assert done is not None and done.is_set()
        assert all(node.object_store.used_bytes == 0 for node in self.nodes)
        close_pure_core(self.core)


def test_pg_death_and_system_retry_have_one_atomic_winner(monkeypatch):
    scenario = _Scenario(monkeypatch)
    try:
        scenario.unlock.callback = scenario.lose_peer
        terminal = scenario.core._retry_explicit_system_failure(scenario.pending, scenario.reply)
        assert scenario.unlock.fired == 1
        # Previously, the phase peek unlocked before retry's owner/recovery
        # transaction: pg_lost -> owner_retry -> recovery_retry was observable.
        # Moving the check inside that transaction legitimately changes which
        # lane wins; an atomic retry first is allowed, not asserted away.
        assert scenario.events in (["pg_lost"], ["owner_retry", "recovery_retry", "pg_lost"])
        retried = scenario.take_retry()
        if scenario.events[0] == "pg_lost":
            assert terminal and retried is None
            assert scenario.core._finish_pending_task(scenario.pending)
            retries = 0
        else:
            assert not terminal and retried is not None
            assert retried.task_id == scenario.pending.task_id and retried.output_ids == scenario.pending.output_ids
            assert retried.spec.scheduling_key == scenario.pending.spec.scheduling_key
            assert retried.dependency_hold == scenario.pending.dependency_hold
            scenario.dispatch_retry(retried)
            retries = 1
        scenario.assert_pg_terminal(retries=retries)
    finally:
        scenario.close()


def test_pg_death_before_retry_entry_preserves_attempt_and_budget(monkeypatch):
    scenario = _Scenario(monkeypatch)
    try:
        scenario.lose_peer()
        assert scenario.core._retry_explicit_system_failure(scenario.pending, scenario.reply)
        assert scenario.events == ["pg_lost"] and scenario.take_retry() is None
        assert scenario.core._finish_pending_task(scenario.pending)
        scenario.assert_pg_terminal(retries=0)
    finally:
        scenario.close()


def test_unrelated_pg_loss_does_not_disable_ordinary_system_retry(monkeypatch):
    scenario = _Scenario(monkeypatch, ordinary=True)
    try:
        scenario.lose_peer()
        assert scenario.pending.spec.scheduling_key is None
        assert not scenario.core._retry_explicit_system_failure(scenario.pending, scenario.reply)
        assert scenario.events == ["pg_lost", "owner_retry", "recovery_retry"]
        retried = scenario.take_retry()
        assert retried is not None and retried.spec.scheduling_key is None
        assert retried.task_id == scenario.pending.task_id and retried.output_ids == scenario.pending.output_ids
        assert retried.spec.attempt_id == scenario.pending.spec.attempt_id.next()
        assert retried.dependency_hold == scenario.pending.dependency_hold
        second_reply = scenario.complete_system_attempt(retried)
        assert scenario.core._retry_explicit_system_failure(retried, second_reply)
        assert scenario.core._finish_pending_task(retried)
        snapshot = scenario.core.owner_table.snapshot(scenario.pending.object_id)
        record = scenario.core._recovery.task_record(scenario.pending.task_id)
        assert snapshot.state is ObjectState.ERROR and record.state is TaskState.SYSTEM_FAILED
        assert snapshot.current_attempt == record.current_attempt == retried.spec.attempt_id
        assert record.retries_started == 1 and record.retries_remaining == 0
        assert isinstance(snapshot.error, SystemTaskError) and not isinstance(snapshot.error, PlacementGroupLostError)
        assert snapshot.error is record.last_error
        assert len(scenario.leases) == len(scenario.home._leases) == 2 and not scenario.peer._leases
        assert scenario.home.resource_ledger.available == scenario.home.resource_ledger.total
        assert not scenario.core._task_finish_barriers and not scenario.core._protocol_unresolved
        assert scenario.core._accepted_task_count == 0 and scenario.pending.task_key in scenario.core._finished_tasks
        assert scenario.take_retry() is None
    finally:
        scenario.close()
