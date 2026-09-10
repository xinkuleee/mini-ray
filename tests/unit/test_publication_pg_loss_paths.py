"""PG LOST preserves current owner-led cleanup continuations.

One accepted single-output Core task, one child owner and one lost Release
ACK; the exact delayed ReadyTask and STOP run through the real dispatch loop.
Complete/death are explicit input facts, not producer or process execution.
Actual enhanced authority and a local journal bind publication cleanup. No
runtime constructor, wait or background thread runs; routing stays separate.
"""

from dataclasses import replace
import hashlib
import queue

import pytest

from miniray import enhanced_publication as ep, output_protocol as wire, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import (
    _DeferredSystemFailure, _DelayedReadyTask, _NodeDeathObserved,
    _OutputNodeLossObligation, _ReadyTask, _STOP, _WAKE_COORDINATOR,
)
from miniray.errors import PlacementGroupLostError, SystemTaskError
from miniray.ids import LeaseID, NodeID, ObjectID, PlacementGroupID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_enhanced_owner_client import runtime as enhanced_runtime


pytestmark = pytest.mark.unit


def test_enhanced_final_graph_cleanup_ack_loss_precedes_pg_lost_finish(enhanced_runtime):
    from miniray import enhanced_publication as ep
    r = enhanced_runtime
    child = r.leaf(43)
    key = protocol.PlacementGroupSchedulingKey(
        PlacementGroupID(b'R' * 16), 0, 0, r.incarnation.node_id, "b" * 64,
    )
    r.owner._placement_group_states = {(key.placement_group_id, key.attempt): protocol.PlacementGroupPhaseStatus.CREATED}
    r.owner._placement_group_manifests = {(key.placement_group_id, key.attempt): (key,)}
    pending, outer = r.owner._register_submission(
        r.owner.define_remote_function(lambda: None), (), {}, ResourceVector({"CPU": 1}),
        max_retries=1, placement_group_scheduling_key=key, _enqueue=True,
    )
    r.references.append(outer)
    assert r.take() == (pending,)
    envelope = r.prepare(pending, child)
    identity = envelope.publication_id
    publication = ep.TaskPublication(envelope.manifest, r.owner.owner_address)
    death = protocol.NodeDeathRecord(
        "final-graph-ack-publisher-loss", r.incarnation.node_id, r.incarnation.node_pid,
        r.incarnation.registration_epoch, 1, 7, protocol.NodeDeathReason.PROCESS_EXIT,
        "publisher unavailable after actual local Complete",
    )
    before_owner = r.owner.owner_table.snapshot(outer.object_id)
    before_recovery = replace(r.owner._recovery.task_record(pending.task_id))
    r.lose = ep.RetireGraph
    obligation = _OutputNodeLossObligation(identity, death)
    assert not r.owner._execute(pending, pending.spec, output_node_loss=obligation)
    central = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert central.receipt(ep.PublicationStage.RETIRED) is not None and not central.graph_active
    assert central.complete == envelope.complete
    assert r.owner.owner_table.snapshot(outer.object_id) == before_owner
    assert r.owner._recovery.task_record(pending.task_id) == before_recovery
    assert not r.owner._finish_pending_task(pending)
    releases = tuple(request for handler, request in r.calls if handler == "release_contained_reference")
    assert len(releases) == 2
    assert not r.publisher.owner_table.snapshot(child.object_id).contained_holds
    delayed = _take_delayed(r.owner)
    capacity = ResourceVector({"CPU": 1})
    live = protocol.NodeInfo(key.node_id, r.incarnation.node_pid, r.incarnation.registration_epoch,
                             r.owner.node_address, capacity, capacity)
    r.owner._membership_epoch = 0
    r.owner._installed_cluster_snapshot = protocol.InstallClusterSnapshot(0, "before-final-ack-pg-loss", (live,))
    r.owner.handle_node_death(death, protocol.InstallClusterSnapshot(1, "after-final-ack-pg-loss", ()))
    assert r.owner._placement_group_phase_for_pending(pending) is protocol.PlacementGroupPhaseStatus.LOST
    observed = r.owner._submissions.get_nowait()
    r.owner._submissions.task_done()
    assert observed == _NodeDeathObserved(death, 1)
    assert delayed.ready.output_node_loss.publication_id == identity
    _dispatch_once(r.owner, delayed.ready)
    assert tuple(request for handler, request in r.calls if handler == "release_contained_reference") == releases
    after = r.owner.owner_table.snapshot(outer.object_id)
    assert after.state is ObjectState.LOST and after.inline_data is None
    record = r.owner._recovery.task_record(pending.task_id)
    assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
    assert record.current_attempt == pending.spec.attempt_id
    assert not r.owner._protocol_unresolved and not r.owner._task_finish_barriers
    assert r.owner._accepted_task_count == 0 and identity in r.owner._output_loss_completed
    assert r.owner._drive_output_node_loss(pending, obligation)


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    import multiprocessing.process
    import socket
    import subprocess
    import threading
    import time
    from miniray import control, core as core_module, transport
    from miniray.core import CoreWorker
    from miniray.node import NodeServer
    from miniray.worker import WorkerServer

    def forbidden(*args, **kwargs):
        pytest.fail("pure PG continuation attempted runtime or unmodelled work")

    for kind, method in ((CoreWorker, "__init__"), (NodeServer, "__init__"),
                         (WorkerServer, "__init__"), (control.GCSLite, "__init__"),
                         (threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Condition, "wait"),
                         (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    def settled_receipt_only(event, timeout=None):
        # The synchronous mailbox sets the actual release receipt before
        # ObjectRef.close inspects it; an unsettled receipt still fails.
        assert event.is_set(), "pure receipt attempted a blocking wait"
        return True
    monkeypatch.setattr(threading.Event, "wait", settled_receipt_only)


def _id(kind, value):
    return kind(bytes((value,)) * 16)


def _pg_submission():
    core = make_pure_core()
    publisher = _id(NodeID, 71)
    key = protocol.PlacementGroupSchedulingKey(_id(PlacementGroupID, 72), 0, 0, publisher, "a" * 64)
    core._placement_group_states = {(key.placement_group_id, key.attempt): protocol.PlacementGroupPhaseStatus.CREATED}
    core._placement_group_manifests = {(key.placement_group_id, key.attempt): (key,)}
    pending, ref = core._register_submission(
        core.define_remote_function(lambda: None), (), {}, ResourceVector({"CPU": 1}),
        num_returns=1, max_retries=3, placement_group_scheduling_key=key, _enqueue=True,
    )
    assert core._submissions.get_nowait() is pending
    core._submissions.task_done()
    assert core._accepted_task_count == 1 and core._task_finish_barriers == {ref.object_id: pending}
    return core, pending, ref, key


def _dispatch_once(core, ready):
    core._ready_tasks = queue.Queue(maxsize=2)
    core._ready_tasks.put_nowait(ready)
    core._ready_tasks.put_nowait(_STOP)
    core._dispatch_loop()
    assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0


def _take_delayed(core):
    delayed = []
    count = core._submissions.qsize()
    assert count <= 4
    for _ in range(count):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if type(item) is _DelayedReadyTask:
            delayed.append(item)
        else:
            assert item is _WAKE_COORDINATOR
    assert len(delayed) == 1
    return delayed[0]


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_output_node_loss_replay_finishes_exact_cleanup_before_pg_terminal(known):
    core, pending, ref, key = _pg_submission()
    try:
        executor, child_id = _id(WorkerID, 73), ObjectID.for_task(_id(TaskID, 74))
        transfer = PreparedContainedTransfer(
            child_id, executor, ("child.invalid", 1234), OwnedContainedSource(executor),
            ContainedReferenceHold(ref.object_id, executor, "pg-loss-child"),
            ContainedReferenceHold(ref.object_id, core.worker_id, "pg-loss-child"),
        )
        identity = OutputPublicationID(_id(LeaseID, 75), pending.execution)
        manifest = OutputPublicationManifest.create(OutputPublicationHeader(
            identity, core.job_id, executor, core.worker_id,
            OutputPublicationNodeIncarnation(key.node_id, 7101, 1),
        ), (OutputValue(protocol.ResultStorage.INLINE, 5, hashlib.sha256(b'value').hexdigest(), (transfer,))))
        child = ObjectOwnerTable()
        child.register(child_id, local_token="source")
        child.publish_inline(child_id, None, b"child")
        journal = OutputPublicationJournal()
        completions = []

        def register(value):
            result = core.register_output_handoff(wire.RegisterOutputHandoff(value))
            assert result.accepted, result.error

        def report(witness):
            result = core.report_output_handoff_complete(wire.ReportOutputHandoffComplete(witness))
            assert type(result) is wire.OutputHandoffCompleteAck and result.accepted and result.witness == witness

        def abort(publication, scope):
            result = core.abort_owner_publication(ep.AbortOwnerPublication(publication, scope))
            assert result.accepted and result.receipt is not None, result.error
            return result.receipt

        def child_pin(address, request):
            assert address == transfer.contained_owner_address
            method = child.prepare_stored_contained_reference if type(request) is protocol.PrepareStoredContainedPin else child.promote_stored_contained_reference
            return protocol.StoredContainedPinReply(request, method(request.transfer, authority_worker_id=request.authority_worker_id))

        def forbidden(*_args):
            pytest.fail("PG INLINE fixture attempted unrelated effect")

        adapter = OutputPublicationNodeAdapter(journal, register_owner=register, report_complete=report,
            report_rollback=forbidden, publication_value=lambda value: ep.TaskPublication(value, core.owner_address),
            publication_rpc=core._test_publication_authority.apply, abort_owner=abort,
            prepare_child=child_pin, promote_child=child_pin, release_child=forbidden,
            seal_replica=forbidden, drop_replica=forbidden)
        adapter.prepare(manifest, b"value")
        complete = adapter.complete(identity, commit_lease=completions.append).complete
        assert completions == [complete]
        if known:
            assert adapter.report_terminal(identity)
        calls = []
        def release(address, handler, request):
            assert address == transfer.contained_owner_address and handler == "release_contained_reference"
            assert not core._state_lock._is_owned() and len(calls) < 3
            calls.append(request)
            changed = child.release_contained_reference(request.object_id, request.hold)
            if len(calls) == 1:
                raise TimeoutError("actual child release ACK lost before PG loss")
            return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, changed)
        core._borrow_rpc = release
        capacity = ResourceVector({"CPU": 1})
        live = protocol.NodeInfo(key.node_id, 7101, 1, ("publisher.invalid", 1), capacity, capacity)
        core._membership_epoch = 1
        core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(1, "live-pg", (live,))
        death = protocol.NodeDeathRecord("pg-publisher-exit", key.node_id, 7101, 1, 2, 7,
                                         protocol.NodeDeathReason.PROCESS_EXIT, "explicit member fact")
        obligation = _OutputNodeLossObligation(identity, death)
        before_owner = core.owner_table.snapshot(ref.object_id)
        before_record = replace(core._recovery.task_record(pending.task_id))
        assert not core._execute(pending, pending.spec, output_node_loss=obligation)
        assert calls[0].hold == transfer.final_hold
        assert child.contained_release_was_seen(child_id, transfer.final_hold)
        assert core.owner_table.snapshot(ref.object_id) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert not core._finish_pending_task(pending)
        marker = core._protocol_unresolved[pending.task_key]
        delayed = _take_delayed(core)
        assert delayed.ready.output_node_loss == marker.obligation == replace(obligation, round=1)
        installed = protocol.InstallClusterSnapshot(2, "pg-node-gone", ())
        core.handle_node_death(death, installed)
        observed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert observed == _NodeDeathObserved(death, installed.membership_epoch)
        assert core._placement_group_phase_for_pending(pending) is protocol.PlacementGroupPhaseStatus.LOST
        assert core._protocol_unresolved[pending.task_key] == marker
        assert core.owner_table.snapshot(ref.object_id) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        _dispatch_once(core, delayed.ready)
        assert calls[0] == calls[1] and len(calls) == 3
        assert calls[2].hold == transfer.provisional_hold
        assert not child.snapshot(child_id).contained_holds
        assert child.snapshot(child_id).local_tokens == frozenset({"source"})
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert core._accepted_task_count == 0 and pending.task_key in core._finished_tasks
        assert identity in core._output_loss_completed and not core._output_node_cleanup
        record = core._recovery.task_record(pending.task_id)
        assert record.state is (TaskState.SUCCEEDED if known else TaskState.SYSTEM_FAILED)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        assert record.retries_remaining == 3
        after = core.owner_table.snapshot(ref.object_id)
        assert after.state is (ObjectState.LOST if known else ObjectState.ERROR)
        assert after.inline_data is None and not after.locations
        assert after.current_attempt == pending.spec.attempt_id
        if not known:
            assert isinstance(after.error, PlacementGroupLostError)
        assert core._objects[ref.object_id].event.is_set()
        assert core._drive_output_node_loss(pending, obligation) and len(calls) == 3
    finally:
        ref.close()
        close_pure_core(core)


@pytest.mark.parametrize("continuation", (False, True), ids=("fresh-pg-still-rejected", "deferred-system-routing-only"))
def test_lost_pg_gates_fresh_work_but_routes_deferred_failure_to_retry_authority(monkeypatch, continuation):
    core, pending, ref, key = _pg_submission()
    try:
        failure = _DeferredSystemFailure(SystemTaskError("already-known failure"), round=1) if continuation else None
        if continuation:
            core._mark_protocol_unresolved(pending, "system_failure_cleanup_wait", failure)
        core._placement_group_states[(key.placement_group_id, key.attempt)] = protocol.PlacementGroupPhaseStatus.LOST
        retry = core._retry_system_failure
        routed = []
        def observe(seen, error, *, deferred=None):
            assert continuation and not routed and seen is pending and deferred is failure
            assert error is failure.error and core._protocol_unresolved[pending.task_key].obligation is failure
            routed.append(deferred)
            return retry(seen, error, deferred=deferred)
        monkeypatch.setattr(core, "_retry_system_failure", observe)
        if not continuation:
            monkeypatch.setattr(core, "_execute", lambda *_a, **_k: pytest.fail("fresh LOST task executed"))
        assert not core._has_late_replica_cleanup_locked()
        _dispatch_once(core, _ReadyTask(pending, pending.spec, system_failure=failure))
        assert routed == ([failure] if continuation else [])
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.SYSTEM_FAILED and isinstance(record.last_error, PlacementGroupLostError)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0 and record.retries_remaining == 3
        assert not core._protocol_unresolved and not core._task_finish_barriers and core._accepted_task_count == 0
        after = core.owner_table.snapshot(ref.object_id)
        assert after.state is ObjectState.ERROR and isinstance(after.error, PlacementGroupLostError)
        assert after.current_attempt == pending.spec.attempt_id and core._objects[ref.object_id].event.is_set()
    finally:
        ref.close()
        close_pure_core(core)
