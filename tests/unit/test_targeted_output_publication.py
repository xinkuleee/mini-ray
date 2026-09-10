"""Retained Complete/owner-receipt contracts on ordinary single outputs.

Five threadless cases use actual Core/Node/handoff/journal/child reducers.
Targeted sessions and sibling vectors are retired. Known success is distinct
from payload custody; old publication work cannot disturb a queued successor.
"""

from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, output_protocol as wire, protocol
from miniray.core import CoreWorker, _NodeDeathObserved, _OutputNodeLossObligation, _PendingTask, _WAKE_COORDINATOR
from miniray.ids import LeaseID, NodeID
from miniray.node import NodeServer
from miniray.output_publication import OutputPublicationID
from miniray.ownership import ObjectState, OutputOwnerPublicationDisposition, OutputOwnerPublicationPlan
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import TaskState
from tests.unit.test_core_output_publication import _fixture, _close, _take_adoption


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("ordinary output receipt test attempted runtime work")
    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _take(core):
    size = core._submissions.qsize()
    assert size <= 8
    items = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            items.append(item)
    return tuple(items)


def _publisher_death(fixture, node, core):
    incarnation = fixture.manifest.header.node_incarnation
    registry = control.NodeRegistry()
    survivor = NodeID(bytes(value ^ 1 for value in node.node_id.value))
    resources = node.resource_ledger.total
    assert registry.register(survivor, ("survivor.invalid", 2), resources, node_pid=1702)
    assert registry.register(node.node_id, core.node_address, resources, node_pid=incarnation.node_pid)
    assert registry.get(node.node_id).registration_epoch == incarnation.registration_epoch
    epoch, live = registry.live_snapshot()
    core._membership_epoch = epoch
    core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(epoch, "before-output-loss", live)
    report = registry.report_death(protocol.ReportNodeDeath(
        "known-output-loss", node.node_id, incarnation.node_pid, incarnation.registration_epoch,
        -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
    ))
    assert report.disposition is protocol.NodeDeathDisposition.APPLIED
    epoch, live = registry.live_snapshot()
    core.handle_node_death(report.death, protocol.InstallClusterSnapshot(epoch, "after-output-loss", live))
    observed, = _take(core)
    assert observed == _NodeDeathObserved(report.death, epoch)
    return report.death


@pytest.mark.parametrize("success_recorded", (False, True), ids=("repair-success", "success-recorded"))
def test_known_complete_without_bytes_finishes_without_republishing(monkeypatch, success_recorded):
    fixture, node, core, pending, reply, calls, _rpc = _fixture(refs=True, stored=False)
    try:
        assert fixture.handoffs.query(fixture.id).complete == reply.output_publication.complete
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert not getattr(core, "_output_result_custody", {})
        if success_recorded:
            core._recovery.record_task_success(pending.task_id, pending.spec.attempt_id)
        death = _publisher_death(fixture, node, core)
        def forbidden(*_args):
            pytest.fail("known Complete without bytes cannot republish")
        monkeypatch.setattr(core.owner_table, "commit_output_publication", forbidden)
        obligation = _OutputNodeLossObligation(fixture.id, death)
        assert core._execute(pending, pending.spec, output_node_loss=obligation)
        owner = core.owner_table.snapshot(pending.object_id)
        assert owner.state is ObjectState.LOST and owner.current_attempt == pending.spec.attempt_id
        assert owner.inline_data is None and owner.canonical_stored_result is None
        assert not owner.locations and owner.output_publication is None
        assert owner.local_tokens == frozenset({"outer0"})
        record = replace(core._recovery.task_record(pending.task_id))
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
        assert core._recovery.active_recovery(pending.task_id) is None
        assert core._objects[pending.object_id].event.is_set()
        resolution = core.owner_table._output_loss_receipts[fixture.id]
        assert resolution.complete == reply.output_publication.complete and not resolution.keep
        assert len(resolution.cleanup) == 4
        for transfer in (fixture.manifest.value).transfers:
            child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.final_hold not in child.contained_holds
            assert transfer.provisional_hold not in child.contained_holds
            assert child.local_tokens == frozenset({"source-live"})
        assert all(handler == "release_contained_reference" for handler, _ in calls) and len(calls) == 4
        assert not core._protocol_unresolved
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        before = tuple(calls)
        assert core._execute(pending, pending.spec, output_node_loss=obligation)
        assert tuple(calls) == before and core.owner_table.snapshot(pending.object_id) == owner
        assert core._recovery.task_record(pending.task_id) == record
        assert not core._reconstruction_coordinator()._sessions
    finally:
        _close(core)


@pytest.mark.parametrize("repair", ("same-plan", "revalidated-plan", "receipt-completion"))
def test_owner_cas_effect_then_error_repair_fences_a_later_ordinary_attempt(monkeypatch, repair):
    fixture, node, core, pending, reply, calls, _rpc = _fixture(refs=True, stored=True)
    try:
        original = core.owner_table.commit_output_publication
        commits = []
        def effect_then_error(plan):
            receipt = original(plan)
            commits.append(receipt.disposition)
            raise RuntimeError("owner CAS applied before local callback failed")
        monkeypatch.setattr(core.owner_table, "commit_output_publication", effect_then_error)
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id)
        assert commits == [OutputOwnerPublicationDisposition.APPLIED]
        owner = core.owner_table.snapshot(pending.object_id)
        assert owner.state is ObjectState.READY_STORED
        assert core._recovery.task_record(pending.task_id).state is not TaskState.SUCCEEDED
        assert pending.object_id not in core._stored_descriptors
        assert not core._objects[pending.object_id].event.is_set()
        owner_plan = OutputOwnerPublicationPlan(pending.execution, reply.output_publication)
        receipt = core.owner_table.output_owner_publication_receipt(owner_plan)
        assert receipt.committed and receipt.disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
        saved = _take_adoption(core)
        if repair == "receipt-completion":
            assert core._publish_reply(pending, reply, expected_node_id=node.node_id)
        else:
            obligation = saved if repair == "same-plan" else replace(saved)
            assert core._execute(pending, pending.spec, output_adoption=obligation)
        assert commits == [OutputOwnerPublicationDisposition.APPLIED]
        assert core.owner_table.snapshot(pending.object_id) == owner
        assert core._stored_descriptors[pending.object_id] == reply.results[0]
        assert core._objects[pending.object_id].event.is_set()
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert core._finish_pending_task(pending) and _take(core) == ()
        # Real physical loss and owner route invalidation precede ordinary
        # retirement and accepted reconstruction. No targeted session exists.
        descriptor = reply.results[0]
        dropped = node._handle_drop_object_replica(protocol.DropObjectReplica(
            pending.object_id, pending.spec.attempt_id, core.worker_id, node.node_id, descriptor.checksum,
        ))
        assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
        assert core.owner_table.mark_lost(pending.object_id, pending.spec.attempt_id)
        core._stored_descriptors.pop(pending.object_id)
        outcome = core._start_or_join_reconstruction(
            pending.object_id, core._objects[pending.object_id], return_requested_outcome=True,
        )
        assert outcome.disposition is ReconstructionDisposition.START
        successor, = _take(core)
        assert isinstance(successor, _PendingTask)
        assert successor.spec.attempt_id == pending.spec.attempt_id.next()
        assert core._accepted_task_count == 1 and core._task_finish_barriers == {pending.object_id: successor}
        assert core._recovery.active_recovery(pending.task_id) == successor.spec.attempt_id
        marker_id = OutputPublicationID(LeaseID.random(), successor.execution)
        core._mark_protocol_unresolved(successor, "successor-admitted", output_candidate=marker_id)
        marker = core._protocol_unresolved[successor.task_key]
        before_owner = core.owner_table.snapshot(pending.object_id)
        before_record = replace(core._recovery.task_record(pending.task_id))
        before_calls = tuple(calls)
        assert core._execute(pending, pending.spec, output_adoption=saved)
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id)
        assert not core._finish_pending_task(pending)
        assert core._protocol_unresolved[successor.task_key] is marker
        assert core.owner_table.snapshot(pending.object_id) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert tuple(calls) == before_calls and commits == [OutputOwnerPublicationDisposition.APPLIED]
        assert core._task_finish_barriers[pending.object_id] is successor and _take(core) == ()
        assert before_record.state is TaskState.RETRY_PENDING and before_record.retries_started == 1
        assert fixture.store.used_bytes == 0
    finally:
        _close(core)
