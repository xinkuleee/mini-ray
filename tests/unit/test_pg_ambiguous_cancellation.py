"""Pure PG-loss error selection across real cancellation/custody ACKs.

Each case has one threadless Core, one unstarted Node with a 1 KiB empty store,
one logical Task and at most one real GRANTED lease. No user code executes.
PG LOST is the initial Core scheduling fact; the surviving Node has no grant
in the PG cases, so its real Cancel handler freezes an empty inventory. That
inventory proves only this fixture's empty dependencies, not general absence
of pre-grant replicas. The ordinary known-Grant case really holds/releases CPU.

In fault cases, one reply is lost after the real Node handler applies its effect.
Two explicit reducer turns suffice; delayed work is read with get_nowait, never
waited on. The original PG cause becomes sticky before entering custody, while
non-PG ambiguity retains its original wrapper, even for a PG-typed inner cause.
No owner/local error is overwritten by a later PG observation.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, node as node_module, protocol
from miniray.core import (
    CoreWorker, _DelayedReadyTask, _LeaseCancellationState, _LeaseRequestAmbiguous,
    _LeaseRequestState, _LocationReportState, _ObjectWaiter, _PendingTask,
)
from miniray.errors import PlacementGroupLostError
from miniray.ids import AttemptID, LeaseID, ObjectID, PlacementGroupID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.ownership import ObjectState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_cancelled_grant_inventory import _node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure PG cancellation attempted runtime infrastructure or unmodelled RPC")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


class _Scenario:
    def __init__(self, *, pg_lost=False, known_grant=False, lose_cancel=False, lose_custody=False):
        assert not (pg_lost and known_grant)  # no synthetic PG reservation
        assert not (lose_cancel and lose_custody)
        self.core = core = make_pure_core()
        self.node = node = _node(core.node_id, WorkerID.random())
        key = None
        if pg_lost:
            key = protocol.PlacementGroupSchedulingKey(
                PlacementGroupID.random(), 0, 0, node.node_id, "a" * 64,
            )
            core._placement_group_states = {
                (key.placement_group_id, key.attempt): protocol.PlacementGroupPhaseStatus.LOST,
            }
        task = TaskID.derive(core.job_id, core.driver_task_id, 0)
        attempt = AttemptID(task, 0)
        output = ObjectID.for_task(task)
        spec = protocol.TaskSpec(
            core.job_id, task, attempt, protocol.FunctionKey(core.job_id, __name__, "never_run", "v1"),
            (), 1, ResourceVector({"CPU": 1}), core.worker_id, max_retries=3, scheduling_key=key,
        )
        self.pending = _PendingTask(output, spec)
        core.owner_table.register(output, current_attempt=attempt, producer_task_spec=spec)
        core._recovery.register_task(spec, output_ids=(output,), max_retries=3)
        core._objects[output] = _ObjectWaiter(threading.Event())
        self.request = request = protocol.RequestWorkerLease(
            LeaseID.random(), task, attempt, spec.resources, core.node_id, core.worker_id,
            target_node_id=node.node_id, return_ids=(output,), scheduling_key=key,
        )
        self.state = _LeaseRequestState(request, core.node_address, node.node_id, False)
        self.grant = node._handle_request_lease(request) if known_grant else None
        if known_grant:
            assert type(self.grant) is protocol.GrantWorkerLease
            assert node.resource_ledger.available.is_zero()
        else:
            assert not node._leases and node.resource_ledger.available == node.resource_ledger.total
        core._mark_protocol_unresolved(self.pending, "lease_replay_wait", target_node_id=node.node_id)
        self.cancel_calls, self.custody_calls, self.selected_errors = [], [], []
        self.lose_cancel, self.lose_custody = lose_cancel, lose_custody
        core._rpc = self.rpc

    def rpc(self, address, handler, request):
        assert address == self.state.address
        assert len(self.cancel_calls) + len(self.custody_calls) < 4
        self.assert_pending()
        selected = self.core._protocol_unresolved[self.pending.task_key].obligation.terminal_error
        assert selected is not None
        self.selected_errors.append(selected)
        if handler == node_module.CANCEL_LEASE_HANDLER:
            reply = self.node._handle_cancel_worker_lease(request)
            assert reply.accepted and reply.cancelled
            assert reply.state is protocol.LeaseExecutionState.ABANDONED
            assert reply.retired_grant == self.grant
            assert reply.dependency_inventory.lease_request == self.request
            assert reply.dependency_inventory.descriptors == ()
            self.cancel_calls.append((request, reply))
            self.assert_pending()
            if self.lose_cancel:
                self.lose_cancel = False
                raise TransportTimeout("Cancel ACK lost after its real effect")
            return reply
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert self.cancel_calls and request.inventory == self.cancel_calls[-1][1].dependency_inventory
        reply = self.node._handle_ack_lease_dependency_custody(request)
        assert reply.accepted and reply.request == request
        self.custody_calls.append((request, reply))
        assert self.node._dependency_custody_registry_locked()._entries[self.request.lease_id].acknowledged == request.inventory
        self.assert_pending()
        if self.lose_custody:
            self.lose_custody = False
            raise TransportTimeout("Custody ACK lost after its real effect")
        return reply

    def assert_pending(self):
        snapshot = self.core.owner_table.snapshot(self.pending.object_id)
        assert snapshot.state is ObjectState.PENDING and snapshot.error is None
        assert snapshot.current_attempt == self.request.attempt_id
        assert self.core._recovery.task_record(self.pending.task_id).retries_started == 0

    def begin_ambiguous(self, cause):
        failure = _LeaseRequestAmbiguous(self.state, cause)
        terminal = self.core._handle_ambiguous_lease(
            self.pending, self.pending.spec, (), failure, core_module._LEASE_RPC_REPLAY_ATTEMPTS,
        )
        return failure, terminal

    def resume_once(self):
        delayed = self.core._submissions.get_nowait()
        self.core._submissions.task_done()
        assert type(delayed) is _DelayedReadyTask and self.core._submissions.empty()
        ready = delayed.ready
        assert ready.pending is self.pending and ready.spec == self.pending.spec and ready.dependencies == ()
        if ready.cancellation is not None:
            assert ready.location_state is None
            return self.core._resolve_lease_cancellation(self.pending, ready.spec, (), ready.cancellation)
        assert ready.location_state is not None
        return self.core._execute(self.pending, ready.spec, (), location_state=ready.location_state)

    def assert_terminal(self, error):
        snapshot = self.core.owner_table.snapshot(self.pending.object_id)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is error
        assert snapshot.current_attempt == self.request.attempt_id
        assert all(selected is error for selected in self.selected_errors)
        assert not self.core._protocol_unresolved
        recovery = self.core._recovery.task_record(self.pending.task_id)
        assert recovery.retries_started == 0 and recovery.current_attempt == self.request.attempt_id
        assert self.core._recovery.active_recovery(self.pending.task_id) is None
        assert self.node.resource_ledger.available == self.node.resource_ledger.total
        assert self.node.object_store.used_bytes == 0
        assert not self.node._dependency_custody_registry_locked().has_pending()
        assert self.custody_calls

    def close(self):
        # No runtime or actual Python handles exist. Preserve queued GC facts;
        # closing this pure fixture is not a fabricated distributed cleanup.
        close_pure_core(self.core)


def test_pg_loss_waits_for_replayed_cancel_and_custody_before_error():
    scenario = _Scenario(pg_lost=True, lose_cancel=True)
    try:
        original, terminal = scenario.begin_ambiguous(TransportTimeout("unknown Grant reply"))
        assert not terminal
        scenario.assert_pending()
        pending = scenario.core._protocol_unresolved[scenario.pending.task_key].obligation
        assert type(pending) is _LeaseCancellationState and pending.reply is None
        error = pending.terminal_error
        assert isinstance(error, PlacementGroupLostError) and error is not original
        assert len(scenario.cancel_calls) == 1 and not scenario.custody_calls
        assert not scenario.node._leases
        assert scenario.resume_once()
        assert len(scenario.cancel_calls) == 2 and len(scenario.custody_calls) == 1
        assert scenario.cancel_calls[0] == scenario.cancel_calls[1]
        scenario.assert_terminal(error)
    finally:
        scenario.close()


def test_pg_loss_replays_exact_custody_after_effect_then_lost_ack():
    scenario = _Scenario(pg_lost=True, lose_custody=True)
    try:
        _failure, terminal = scenario.begin_ambiguous(TransportTimeout("unknown Grant reply"))
        assert not terminal
        scenario.assert_pending()
        pending = scenario.core._protocol_unresolved[scenario.pending.task_key].obligation
        assert type(pending) is _LocationReportState and pending.grant is None
        assert pending.cancellation_reply is not None and not pending.custody_acknowledged
        error = pending.terminal_error
        assert isinstance(error, PlacementGroupLostError)
        inventory = pending.inventory
        assert inventory == scenario.custody_calls[0][0].inventory
        assert scenario.resume_once()
        assert len(scenario.cancel_calls) == 1 and len(scenario.custody_calls) == 2
        assert scenario.custody_calls[0] == scenario.custody_calls[1]
        assert scenario.custody_calls[1][0].inventory == inventory
        scenario.assert_terminal(error)
    finally:
        scenario.close()


def test_non_pg_ambiguity_preserves_wrapper_even_with_pg_typed_cause():
    scenario = _Scenario()
    try:
        cause = PlacementGroupLostError("not this Task's placement-group state")
        failure, terminal = scenario.begin_ambiguous(cause)
        assert terminal and scenario.pending.spec.scheduling_key is None
        assert failure.cause is cause
        assert len(scenario.cancel_calls) == len(scenario.custody_calls) == 1
        scenario.assert_terminal(failure)
    finally:
        scenario.close()


def test_known_grant_releases_once_and_keeps_first_error_through_custody_replay():
    scenario = _Scenario(known_grant=True, lose_custody=True)
    try:
        error = _LeaseRequestAmbiguous(scenario.state, TransportTimeout("original non-PG ambiguity"))
        assert not scenario.core._begin_known_grant_cancellation(
            scenario.pending, scenario.pending.spec, (), scenario.state.address,
            scenario.grant, scenario.request, error,
        )
        scenario.assert_pending()
        pending = scenario.core._protocol_unresolved[scenario.pending.task_key].obligation
        assert type(pending) is _LocationReportState and pending.grant == scenario.grant
        assert pending.terminal_error is error and not pending.custody_acknowledged
        assert scenario.cancel_calls[0][1].released
        assert scenario.node._leases[scenario.request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        before = scenario.node.resource_ledger.snapshot()
        assert scenario.resume_once()
        assert len(scenario.cancel_calls) == 1 and len(scenario.custody_calls) == 2
        assert scenario.custody_calls[0] == scenario.custody_calls[1]
        assert scenario.node.resource_ledger.snapshot() == before
        scenario.assert_terminal(error)
    finally:
        scenario.close()
