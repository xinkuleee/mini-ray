"""PG LOST cannot discard an already-retained output-loss continuation.

Each case owns one threadless Core and at most two tiny output slots. The
Node-loss pair uses the real in-memory GCS publication reducer: final cleanup
takes effect, its ACK is lost, then the cached PG becomes LOST. The original
saved ReadyTask and STOP enter the actual dispatch loop in a two-item queue.
No child references or ObjectStore are created; this proves the ref-free
metadata cleanup/terminal boundary, not physical replica or child-reference GC.

The deferred-system-failure case is deliberately routing-only. It delegates
to the real retry authority with no live replica cleanup and checks the
existing PG terminal policy; it does not prove late-replica cleanup convergence.
There are no listeners, threads, processes, waits, timers or user executions.
"""

from dataclasses import replace
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, core as core_module, output_protocol as wire, protocol, transport
from miniray.core import (
    CoreWorker, _DeferredSystemFailure, _DelayedReadyTask, _NodeDeathObserved,
    _ObjectWaiter, _OutputNodeLossObligation, _PendingTask, _ReadyTask,
    _STOP, _WAKE_COORDINATOR,
)
from miniray.errors import PlacementGroupLostError, SystemTaskError
from miniray.ids import AttemptID, PlacementGroupID, TaskID
from miniray.object_store import ObjectStore
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import make_pure_core
from tests.unit.test_core_output_node_loss import _close, _consume_queued, _lose_final_cleanup_ack
from tests.unit.test_output_publication_control import _service


pytestmark = pytest.mark.unit


def _forbidden(*_args, **_kwargs):
    pytest.fail("pure PG continuation attempted runtime or unmodelled work")


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    for kind, method in (
        (CoreWorker, "__init__"), (ObjectStore, "__init__"),
        (transport.TCPServer, "__init__"),
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
    monkeypatch.setattr(core_module, "rpc_request", _forbidden)
    monkeypatch.setattr(control, "rpc_request", _forbidden)


def _pg_key(core):
    key = protocol.PlacementGroupSchedulingKey(
        PlacementGroupID(bytes.fromhex("51" * 16)), 0, 0, core.node_id, "a" * 64,
    )
    core._placement_group_states = {(key.placement_group_id, key.attempt):
                                    protocol.PlacementGroupPhaseStatus.CREATED}
    core._placement_group_manifests = {(key.placement_group_id, key.attempt): (key,)}
    return key


def _register_pending(core, spec):
    assert spec.num_returns == 2 and spec.function_definition is None
    assert spec.scheduling_key is not None and spec.max_retries == 3
    assert not spec.args and not spec.kwargs
    core.owner_table.register_task_outputs(spec, local_tokens=("outer0", "outer1"))
    core._recovery.register_task(spec, max_retries=spec.max_retries)
    core._objects = {output: _ObjectWaiter(threading.Event()) for output in spec.return_ids()}
    pending = _PendingTask(spec.return_ids()[0], spec)
    core._accepted_task_count = 1
    core._install_task_finish_barrier_locked(pending)
    return pending


def _dispatch_once(core, ready):
    """Preload both reads; Condition.wait is a failing tripwire."""
    core._ready_tasks = queue.Queue(maxsize=2)
    core._ready_tasks.put_nowait(ready)
    core._ready_tasks.put_nowait(_STOP)
    core._dispatch_loop()
    assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_output_node_loss_replay_finishes_exact_cleanup_before_pg_terminal(monkeypatch, known):
    service, values = _service(monkeypatch, refs=False)
    core = make_pure_core()
    try:
        core.worker_id, core.job_id, core.node_id = values.owner, values.job, values.node
        core.gcs_address = ("gcs.invalid", 1)
        key = _pg_key(core)
        pending = _register_pending(core, protocol.TaskSpec(
            values.job, values.task, values.attempt,
            protocol.FunctionKey(values.job, __name__, "unexecuted_producer", "v1"),
            (), 2, ResourceVector({"CPU": 1}), values.owner,
            max_retries=3, scheduling_key=key,
        ))
        assert pending.execution == values.publication_id.execution
        assert sum(slot.size_bytes for slot in values.manifest.slots) < 1024
        assert not values.manifest.ordered_edges
        registry = service.publications.output_recovery
        assert service.report_output_publication(wire.ReportOutputPublicationIntent(values.manifest)).accepted
        assert service.report_output_publication(wire.ArmOutputPublication(
            values.publication_id, values.manifest.manifest_digest,
        )).accepted
        if known:
            assert service.report_output_publication(wire.ReportOutputPublicationTerminal(values.witness)).accepted
        live_node = service.nodes.get(values.node).to_node_info()
        core.node_address = live_node.address
        core._membership_epoch = service.nodes.membership_epoch
        core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(
            core._membership_epoch, "one-pg-publisher", (live_node,),
        )
        node = values.header.node_incarnation
        death = service.publications.commit_node_death(lambda: service.nodes.report_death(
            protocol.ReportNodeDeath(
                "pg-output-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
                1, protocol.NodeDeathReason.PROCESS_EXIT, "publisher exit committed before Core PG view",
            )
        )).death
        assert death is not None
        obligation = _OutputNodeLossObligation(values.publication_id, death)
        calls = []

        def rpc(address, handler, request):
            assert address == core.gcs_address and len(calls) < 4
            calls.append((handler, request))
            if handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER:
                return service.get_output_node_loss(request)
            if handler == wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER:
                return service.decide_output_node_loss(request)
            if handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER:
                return service.progress_output_node_loss(request)
            _forbidden(handler)

        lost = _lose_final_cleanup_ack(core, calls, rpc)
        before_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        before_record = replace(core._recovery.task_record(pending.task_id))
        assert not core._execute(pending, pending.spec, output_node_loss=obligation)
        assert len(lost) == 1
        resolved = registry.snapshot(values.publication_id)
        assert resolved == lost[0].snapshot and resolved.resolution is not None
        assert resolved.recovery_action is (
            OutputRecoveryAction.POSTCOMPLETE_RESOLVE if known else OutputRecoveryAction.COMPLETION_UNKNOWN
        )
        assert resolved.resolution.kept_slots == ()
        assert resolved.owner_decision.complete == resolved.resolution.complete == (values.witness if known else None)
        assert all(slot.decision is OutputRecoveryOwnerDecision.DROP for slot in resolved.owner_decision.slots)
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert not core._finish_pending_task(pending)
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.phase == "output_node_loss_wait"
        assert marker.obligation == replace(obligation, round=1)
        delayed = _consume_queued(core, _DelayedReadyTask)
        assert delayed.ready.pending is pending and delayed.ready.output_node_loss == marker.obligation
        assert core._submissions.empty()

        # Install the actual GCS death and full survivor view only after the
        # lost ACK has retained this continuation. Observer work cannot finish it.
        installed = protocol.InstallClusterSnapshot(death.death_epoch, "no-pg-survivors", ())
        core.handle_node_death(death, installed)
        observed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert observed == _NodeDeathObserved(death, installed.membership_epoch)
        assert core._submissions.empty()
        assert core._placement_group_phase_for_pending(pending) is protocol.PlacementGroupPhaseStatus.LOST
        assert core._protocol_unresolved[pending.task_key] == marker
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {output: pending for output in pending.output_ids}
        assert not core._finish_pending_task(pending)

        _dispatch_once(core, delayed.ready)
        assert [handler for handler, _request in calls] == [
            wire.GET_OUTPUT_NODE_LOSS_HANDLER, wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER,
            wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER, wire.GET_OUTPUT_NODE_LOSS_HANDLER,
        ]
        assert calls[-1] == calls[0] and len(lost) == 1
        assert registry.snapshot(values.publication_id) == resolved
        assert core.owner_table._output_loss_receipts[values.publication_id] == resolved.resolution
        assert not service.publications.has_active_operations()
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert not core._output_result_custody and not core._output_loss_drivers
        assert values.publication_id in core._output_loss_completed
        assert core._accepted_task_count == 0 and pending.task_key in core._finished_tasks
        record = core._recovery.task_record(pending.task_id)
        assert record.state is (TaskState.SUCCEEDED if known else TaskState.SYSTEM_FAILED)
        assert record.current_attempt == pending.spec.attempt_id
        assert record.retries_started == 0 and record.retries_remaining == 3
        for output in pending.output_ids:
            after = core.owner_table.snapshot(output)
            assert after.state is (ObjectState.LOST if known else ObjectState.ERROR)
            assert after.current_attempt == pending.spec.attempt_id
            if known:
                assert after.error is None
            else:
                assert isinstance(after.error, PlacementGroupLostError)
            assert not after.inline_data and not after.locations and after.output_publication is None
            assert core._objects[output].event.is_set()
        assert tuple(core._submissions.queue) == (_WAKE_COORDINATOR,) * 3
    finally:
        _close(core)


@pytest.mark.parametrize("continuation", (False, True), ids=("fresh-pg-still-rejected", "deferred-system-routing-only"))
def test_lost_pg_gates_fresh_work_but_routes_deferred_failure_to_retry_authority(monkeypatch, continuation):
    core = make_pure_core()
    try:
        key = _pg_key(core)
        task_id = TaskID(bytes.fromhex("52" * 16))
        pending = _register_pending(core, protocol.TaskSpec(
            core.job_id, task_id, AttemptID(task_id, 0),
            protocol.FunctionKey(core.job_id, __name__, "never_dispatched", "v1"),
            (), 2, ResourceVector({"CPU": 1}), core.worker_id,
            max_retries=3, scheduling_key=key,
        ))
        failure = _DeferredSystemFailure(SystemTaskError("already-known failure"), round=1) if continuation else None
        if continuation:
            # This retained value tests dispatch routing only. No pending
            # physical cleanup or fabricated Drop ACK is installed.
            core._mark_protocol_unresolved(pending, "system_failure_cleanup_wait", failure)
        core._placement_group_states[(key.placement_group_id, key.attempt)] = protocol.PlacementGroupPhaseStatus.LOST
        retry_authority = core._retry_system_failure
        routed = []

        def observe_retry(seen_pending, error, *, deferred=None):
            assert continuation and not routed
            assert seen_pending is pending and deferred is failure and error is failure.error
            assert core._protocol_unresolved[pending.task_key].obligation is failure
            assert core._accepted_task_count == 1
            assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
            routed.append(deferred)
            return retry_authority(seen_pending, error, deferred=deferred)

        monkeypatch.setattr(core, "_retry_system_failure", observe_retry)
        assert not core._has_late_replica_cleanup_locked()
        monkeypatch.setattr(core, "_schedule_late_replica_cleanup_locked", _forbidden)
        if not continuation:
            monkeypatch.setattr(core, "_execute", _forbidden)
        _dispatch_once(core, _ReadyTask(pending, pending.spec, system_failure=failure))
        assert routed == ([failure] if continuation else [])
        record = core._recovery.task_record(task_id)
        assert record.state is TaskState.SYSTEM_FAILED
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        assert record.retries_remaining == 3 and isinstance(record.last_error, PlacementGroupLostError)
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert core._accepted_task_count == 0 and pending.task_key in core._finished_tasks
        for output in pending.output_ids:
            after = core.owner_table.snapshot(output)
            assert after.state is ObjectState.ERROR and isinstance(after.error, PlacementGroupLostError)
            assert after.current_attempt == pending.spec.attempt_id and core._objects[output].event.is_set()
        assert tuple(core._submissions.queue) == (_WAKE_COORDINATOR,) * 3
    finally:
        _close(core)
