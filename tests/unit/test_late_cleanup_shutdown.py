"""Pure owner cutover and reference-consumer shutdown ordering.

One threadless Core, one one-byte STORED publication's retired metadata and one
late replica identity. Typed installed Node-death facts discharge cleanup; no
fake deletion ACK or physical store is used. A stop recorder models only join
completion, never queue emptiness. No thread, process, socket, timer or wait.
"""

from dataclasses import replace
import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.core import CoreWorker, _ObjectWaiter
from miniray.errors import RuntimeShuttingDownError
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.output_recovery import OutputRecoveryResolution
from miniray.ownership import ObjectState, OutputOwnerPublicationPlan
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure late-cleanup shutdown attempted runtime infrastructure")

    for kind, method in (
        (CoreWorker, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "start"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _retired_core():
    core = make_pure_core()
    task = TaskID(bytes((71,)) * 16)
    attempt, output = AttemptID(task, 0), ObjectID.for_task(task)
    spec = protocol.TaskSpec(
        core.job_id, task, attempt,
        protocol.FunctionKey(core.job_id, __name__, "metadata-only-producer", "v1"),
        (), 1, ResourceVector(), core.worker_id,
    )
    core.owner_table.register_task_outputs(spec, local_tokens=("owner-result",))
    core._recovery.register_task(spec)
    core._objects[output] = _ObjectWaiter(threading.Event())
    execution = TaskExecutionKey.from_task_spec(spec)
    identity = OutputPublicationID(LeaseID(bytes((72,)) * 16), execution)
    publisher, secondary = NodeID(bytes((73,)) * 16), NodeID(bytes((74,)) * 16)
    header = OutputPublicationHeader(
        identity, core.job_id, WorkerID(bytes((75,)) * 16), core.worker_id,
        OutputPublicationNodeIncarnation(publisher, 1801, 1),
    )
    checksum = hashlib.sha256(b"x").hexdigest()
    manifest = OutputPublicationManifest.create(header, (
        OutputSlotManifest(output, protocol.ResultStorage.OBJECT_STORE, 1, checksum),
    ))
    complete = OutputPublicationCompleteWitness.for_manifest(manifest)
    envelope = OutputPublicationEnvelope(manifest, complete, (
        protocol.ResultDescriptor(output, protocol.ResultStorage.OBJECT_STORE, 1,
                                  core.worker_id, publisher, checksum),
    ))
    assert core.owner_table.commit_output_publication(OutputOwnerPublicationPlan(execution, envelope)).committed
    core._recovery.record_task_success(task, attempt)
    publisher_death = protocol.NodeDeathRecord(
        "metadata-publisher-exit", publisher, 1801, 1, 1, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "previous publication cleanup",
    )
    resolution = OutputRecoveryResolution(
        identity, manifest.manifest_digest, publisher_death, core.worker_id,
        "metadata-only-drop-resolution", (), complete,
    )
    assert core.owner_table.resolve_output_node_loss(manifest, resolution)
    assert core.owner_table.snapshot(output).state is ObjectState.LOST
    borrower, consumer_task = WorkerID(bytes((76,)) * 16), TaskID(bytes((77,)) * 16)
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, borrower, consumer_task,
        AttemptID(consumer_task, 0),
    )
    token = (borrower, "previous-retained-reader")
    assert core.owner_table.add_borrowed_reference(output, token)
    assert core.owner_table.retain_borrowed_reference_for_task(output, token, hold)
    assert core.owner_table.release_borrowed_reference(output, token)
    assert core.owner_table.release_retained_reference_for_task(output, hold)
    descriptor = protocol.ObjectStoreDescriptor(output, core.worker_id, attempt, secondary, 1, checksum)
    report = protocol.ReportRetainedObjectLocation(output, core.worker_id, borrower, hold, descriptor)
    return core, report


def _close(core):
    for output in core._objects:
        for token in core.owner_table.snapshot(output).local_tokens:
            core.owner_table.release_local_reference(output, token)
    close_pure_core(core)


def _install_replica_death(core, report):
    """Supply an authority fact through the actual Core membership reducer."""
    death = protocol.NodeDeathRecord(
        "metadata-secondary-exit", report.descriptor.node_id, 1802, 2, 2, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "installed physical loss proof",
    )
    core.handle_node_death(death, protocol.InstallClusterSnapshot(2, "no-live-replicas", ()))
    assert core._dead_nodes[death.node_id] == death
    return death


def test_preserved_drain_keeps_cleanup_service_until_late_queue_is_clean(monkeypatch):
    core, report = _retired_core()
    events = []
    original_close = core.event_sink.close

    def stop(deadline):
        assert core.owner_protocol_closed and not core._state_lock._is_owned()
        assert not core._late_replica_cleanup.has_pending()
        assert not core._sink_closed
        events.append("stop")
        return True

    def close_sink():
        events.append("sink")
        original_close()

    monkeypatch.setattr(core, "_stop_reference_events", stop)
    monkeypatch.setattr(core.event_sink, "close", close_sink)
    try:
        assert core.shutdown(timeout=0.25, preserve_owner_protocol=True)
        assert not core._accepting and not core.owner_protocol_closed
        assert not core._sink_closed and events == []
        assert core.can_finalize_shutdown(require_distributed_clean=True)
        before_owner = core.owner_table.snapshot(report.object_id)
        retired = core.report_retained_object_location(report)
        assert retired.status is protocol.RetainedLocationReportStatus.RETIRED
        assert core.owner_table.snapshot(report.object_id) == before_owner
        queue = core._late_replica_cleanup
        (request,) = queue.pending()
        assert request.object_id == report.object_id and request.node_id == report.descriptor.node_id
        assert queue.has_pending() and core._reference_mailbox.pending.qsize() == 1
        assert not core.can_finalize_shutdown(require_distributed_clean=True)
        assert not core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert not core.owner_protocol_closed and events == []
        assert core.report_retained_object_location(report) == retired
        assert queue.pending() == (request,) and core._reference_mailbox.pending.qsize() == 1

        death = _install_replica_death(core, report)
        assert queue.snapshot()[0].proof == death and not queue.has_pending()
        # The retained event consumer still processes the actual queued reducer
        # callbacks; no fake pending-counter reset makes finalization succeed.
        core._reference_mailbox.drain()
        assert core._reference_mailbox.events.unfinished_tasks == 0
        assert core.can_finalize_shutdown(require_distributed_clean=True)
        assert core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert core.owner_protocol_closed and core._sink_closed
        assert events == ["stop", "sink"]
        assert queue.snapshot()[0].proof == death
    finally:
        _close(core)


def test_join_timeout_replays_only_join_after_owner_fence_and_rejects_late_reports(monkeypatch):
    core, report = _retired_core()
    events = []
    sync_calls, stop_deadlines = [], []
    original_sync, original_close = core._sync_worker_deaths, core.event_sink.close
    newer_report = replace(report, descriptor=replace(report.descriptor, node_id=NodeID(bytes((78,)) * 16)))
    try:
        assert core.shutdown(timeout=0.25, preserve_owner_protocol=True)
        assert core.report_retained_object_location(report).status is protocol.RetainedLocationReportStatus.RETIRED
        death = _install_replica_death(core, report)
        core._reference_mailbox.drain()
        queue = core._late_replica_cleanup
        receipts = queue.snapshot()
        assert len(receipts) == 1 and receipts[0].proof == death
        before_owner = core.owner_table.snapshot(report.object_id)

        def sync():
            assert not core.owner_protocol_closed
            sync_calls.append(True)
            events.append("sync")
            return original_sync()

        def stop(deadline):
            assert core.owner_protocol_closed and not core._state_lock._is_owned()
            assert queue.snapshot() == receipts and not queue.has_pending()
            assert not core._sink_closed
            stop_deadlines.append(deadline)
            events.append("stop")
            # A new physical route would enqueue fresh cleanup before owner
            # cutover. The closed owner must reject it even before join ends.
            rejected = core.report_retained_object_location(newer_report)
            assert rejected.status is protocol.RetainedLocationReportStatus.REJECTED
            assert rejected.error == "object owner is stopped"
            assert queue.snapshot() == receipts
            return len(stop_deadlines) > 1

        def close_sink():
            assert core.owner_protocol_closed and len(stop_deadlines) == 2
            events.append("sink")
            original_close()

        monkeypatch.setattr(core, "_sync_worker_deaths", sync)
        monkeypatch.setattr(core, "_stop_reference_events", stop)
        monkeypatch.setattr(core.event_sink, "close", close_sink)
        started = time.monotonic()
        assert not core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert started <= stop_deadlines[0] <= time.monotonic() + 0.25
        assert core.owner_protocol_closed and not core._sink_closed
        assert events == ["sync", "stop"] and sync_calls == [True]
        # can_finalize is permission to finish the already-committed teardown,
        # not a claim that the reference thread has joined.
        assert core.can_finalize_shutdown(require_distributed_clean=True)
        assert core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert sync_calls == [True] and events == ["sync", "stop", "stop", "sink"]
        assert core._sink_closed and core.owner_table.snapshot(report.object_id) == before_owner
        assert queue.snapshot() == receipts and core._reference_mailbox.pending.empty()
    finally:
        _close(core)


def test_completed_death_proof_still_fences_finalize_until_cleanup_rpc_unclaims(monkeypatch):
    core, report = _retired_core()
    stops = []

    def stop(deadline):
        stops.append(deadline)
        assert core.owner_protocol_closed
        return True

    monkeypatch.setattr(core, "_stop_reference_events", stop)
    try:
        assert core.shutdown(timeout=0.25, preserve_owner_protocol=True)
        assert core.report_retained_object_location(report).status is protocol.RetainedLocationReportStatus.RETIRED
        queue = core._late_replica_cleanup
        (request,) = queue.pending()
        assert queue.claim(request)
        death = _install_replica_death(core, report)
        snapshot = queue.snapshot()[0]
        assert snapshot.proof == death and snapshot.in_flight
        assert queue.pending() == () and queue.has_pending()
        assert not core.can_finalize_shutdown(require_distributed_clean=True)
        assert not core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert not core.owner_protocol_closed and stops == []
        # Retiring a local RPC claim is not another physical deletion ACK.
        queue.unclaim(request)
        assert not queue.has_pending() and queue.snapshot()[0].proof == death
        core._reference_mailbox.drain()
        assert core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert len(stops) == 1 and core.owner_protocol_closed
    finally:
        _close(core)


def test_forced_local_stop_preserves_unacknowledged_cleanup_and_fences_transport(monkeypatch):
    core, report = _retired_core()
    stops = []
    try:
        assert core.shutdown(timeout=0.25, preserve_owner_protocol=True)
        assert core.report_retained_object_location(report).status is protocol.RetainedLocationReportStatus.RETIRED
        queue = core._late_replica_cleanup
        pending = queue.snapshot()
        owner = core.owner_table.snapshot(report.object_id)
        recovery = replace(core._recovery.task_record(report.object_id.task_id))
        pending_events = core._reference_mailbox.pending.qsize()
        assert len(pending) == 1 and pending[0].proof is None
        assert not core._shutdown_finalizable_locked(require_distributed_clean=True)

        def stop(deadline):
            assert not core._state_lock._is_owned()
            assert core.owner_protocol_closed and core._reference_transport_closed
            assert not core._gc_retry_timers_open
            assert queue.snapshot() == pending and queue.has_pending()
            assert not core._sink_closed
            stops.append(deadline)
            return True

        def forbidden(*_args, **_kwargs):
            pytest.fail("forced transport shutdown contacted an exited cluster or closed a clean sink")

        monkeypatch.setattr(core, "_stop_reference_events", stop)
        monkeypatch.setattr(core.event_sink, "close", forbidden)
        monkeypatch.setattr("miniray.core.rpc_request", forbidden)
        assert core.stop_after_cluster_exit(timeout=0.25)
        assert len(stops) == 1 and core.owner_protocol_closed
        assert queue.snapshot() == pending and not core._sink_closed
        assert core.owner_table.snapshot(report.object_id) == owner
        assert core._recovery.task_record(report.object_id.task_id) == recovery
        assert not core._shutdown_finalizable_locked(require_distributed_clean=True)
        assert not core.can_finalize_shutdown(require_distributed_clean=True)
        assert not core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert len(stops) == 1 and not core._sink_closed
        assert core._reference_mailbox.pending.qsize() == pending_events
        rejection = core.report_retained_object_location(report)
        assert rejection.status is protocol.RetainedLocationReportStatus.REJECTED
        assert rejection.error == "object owner is stopped"
        request = protocol.GetNodeAddress(report.descriptor.node_id)
        for operation in (
            lambda: CoreWorker._rpc(core, ("127.0.0.1", 31001), "get_node_address", request),
            lambda: CoreWorker._borrow_rpc(core, ("127.0.0.1", 31001), "get_node_address", request),
            lambda: CoreWorker._borrow_rpc_with_deadline(core, ("127.0.0.1", 31001), "get_node_address", request, 0.25),
        ):
            with pytest.raises(RuntimeShuttingDownError, match="managed cluster already exited"):
                operation()
        assert queue.snapshot() == pending
    finally:
        _close(core)


def test_forced_cutover_during_final_death_observation_cannot_become_clean(monkeypatch):
    core, _report = _retired_core()
    stops = []
    try:
        assert core.shutdown(timeout=0.25, preserve_owner_protocol=True)
        monkeypatch.setattr(core, "_stop_reference_events", lambda deadline: (stops.append(deadline), True)[1])

        def sync_then_forced():
            assert not core.owner_protocol_closed
            assert core.stop_after_cluster_exit(timeout=0.25)
            return True

        monkeypatch.setattr(core, "_sync_worker_deaths", sync_then_forced)
        assert not core.finalize_shutdown(require_distributed_clean=True, timeout=0.25)
        assert len(stops) == 1 and core.owner_protocol_closed
        assert not core._sink_closed and not core.can_finalize_shutdown()
    finally:
        _close(core)
