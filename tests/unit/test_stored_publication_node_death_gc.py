"""Pure adopted-output reverse GC after the publishing Node dies.

Two slots, four child transfers, one 1 KiB detached Node store, real owner/graph/
recovery authorities and synchronous callbacks only. The already-finished
handoff uses ordinary per-slot GC, not an obsolete owner-only takeover driver.
Every retry is an explicit call; no server, thread, process or wait is started.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import CoreWorker, _ObjectWaiter, _PendingTask, _RetryInlineGc
from miniray.ids import AttemptID, ObjectID, TaskID
from miniray.node import NodeServer
from miniray.ownership import InvalidObjectTransitionError, ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import make_pure_core, close_pure_core
from tests.unit.test_core_output_publication import _fixture as _output_fixture
from tests.unit.test_output_publication import _assert_metadata


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("adopted-output GC test attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(NodeServer, "__init__", forbidden)
    monkeypatch.setattr("miniray.transport.TCPServer.__init__", forbidden)


def _node_death(node_id, *, node_pid=12345, registration_epoch=1):
    return protocol.NodeDeathRecord(
        "adopted-output-publisher-exit", node_id, node_pid, registration_epoch,
        5, -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publishing Node exit",
    )


def _close(core):
    for object_id in tuple(core._objects):
        if core.owner_table.contains(object_id):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
    close_pure_core(core)


def test_adopted_stored_outer_collects_graph_after_node_death(monkeypatch):
    fixture, node, core, pending, reply, _calls, original_rpc = _output_fixture(refs=True)
    inline_id, stored_id = pending.output_ids
    manifest = reply.output_publication.manifest
    inline_slot, stored_slot = manifest.slots
    identity = manifest.publication_id
    scheduled, releases, graph_requests, reports, committed = [], [], [], [], []
    lost_graph_ack = lost_report_ack = False
    original_release = core._borrow_rpc
    original_commit = core.owner_table.complete_output_publication_collection

    def schedule(mailbox, event, delay):
        assert mailbox is core._reference_mailbox
        assert isinstance(event, _RetryInlineGc) and event.object_id == stored_id
        assert 0 < delay <= 0.25
        scheduled.append(event)

    def release(address, handler, request):
        assert handler == "release_contained_reference"
        result = original_release(address, handler, request)
        assert result.accepted
        releases.append(request)
        return result

    def gc_rpc(address, handler, request):
        nonlocal lost_graph_ack, lost_report_ack
        assert address == core.gcs_address
        _assert_metadata(request)
        assert handler in ("release_contained_graph_container", wire.REPORT_OUTPUT_PUBLICATION_HANDLER)
        obligation = core._object_gc_obligations[
            request.container_object_id if handler == "release_contained_graph_container"
            else request.proof.object_id
        ]
        assert not obligation.pending_edges and not obligation.pending_drops
        result = original_rpc(address, handler, request)
        _assert_metadata(result)
        if handler == "release_contained_graph_container":
            graph_requests.append(request)
            if request.container_object_id == stored_id and not lost_graph_ack:
                lost_graph_ack = True
                raise TransportTimeout("graph release committed before ACK was lost")
        else:
            assert type(request) is wire.ReportOutputPublicationSlotCollected
            reports.append(request)
            if request.proof.object_id == stored_id and not lost_report_ack:
                lost_report_ack = True
                raise TransportTimeout("slot collection report committed before ACK was lost")
        return result

    def commit(plan, receipt):
        obligation = core._object_gc_obligations[plan.membership.object_id]
        assert obligation.graph_release_receipt == receipt
        assert obligation.output_cleanup_reported
        committed.append(plan.membership.object_id)
        return original_commit(plan, receipt)

    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=identity.lease_id)
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert not core._protocol_unresolved and not core._output_result_custody
        assert fixture.recovery.snapshot(identity).adopted is not None
        assert not fixture.journal.snapshot(identity).retained_result_slots
        before = core.owner_table.snapshot(stored_id)
        descriptor = core._stored_descriptors[stored_id]
        assert before.canonical_stored_result == descriptor == reply.output_publication.results[1]
        assert fixture.store.get(stored_id) == fixture.values.payloads[1]

        incarnation = manifest.header.node_incarnation
        death = _node_death(incarnation.node_id, node_pid=incarnation.node_pid,
                            registration_epoch=incarnation.registration_epoch)
        fixture.recovery.freeze_node_death(death)
        removal = core.handle_node_death(death, death.death_epoch, True)
        assert removal.lost == (stored_id,) and removal.collecting == ()
        assert core._dead_nodes[node.node_id] == death
        lost = core.owner_table.snapshot(stored_id)
        assert lost.state is ObjectState.LOST and not lost.locations
        assert lost.canonical_stored_result == descriptor
        assert lost.output_publication == before.output_publication
        assert stored_id not in core._stored_descriptors
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert fixture.recovery.snapshot(identity).resolution is None
        healthy = core.owner_table.snapshot(inline_id)

        monkeypatch.setattr(core, "_schedule_reference_event", schedule)
        monkeypatch.setattr(core, "_borrow_rpc", release)
        monkeypatch.setattr(core, "_rpc", gc_rpc)
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", commit)
        assert core.owner_table.release_local_reference(stored_id, "outer1")
        core._reference_released(stored_id)
        obligation = core._object_gc_obligations[stored_id]
        frozen_plan = obligation.output_plan
        assert frozen_plan.membership == before.output_publication
        assert frozen_plan.metadata_plan.canonical_checksum == descriptor.checksum
        assert frozen_plan.metadata_plan.canonical_size_bytes == descriptor.size_bytes
        assert frozen_plan.metadata_plan.locations == ()
        assert not obligation.pending_drops and not obligation.pending_edges
        assert obligation.graph_release_receipt is None and not obligation.output_cleanup_reported
        assert lost_graph_ack and not lost_report_ack and committed == [] and reports == []
        assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTING
        assert core.owner_table.snapshot(stored_id).canonical_stored_result == descriptor
        assert core._recovery.lineage_for_object(stored_id) is not None
        assert len(releases) == len(stored_slot.transfers) == 2
        for transfer in stored_slot.transfers:
            child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.final_hold not in child.contained_holds
            assert transfer.provisional_hold not in child.contained_holds
        assert set(fixture.graph.snapshot().committed_edges) == set(inline_slot.edges)

        core._reference_released(stored_id)
        assert core._object_gc_obligations[stored_id] is obligation
        assert obligation.output_plan == frozen_plan and obligation.graph_release_receipt is not None
        assert lost_report_ack and not obligation.output_cleanup_reported and committed == []
        assert graph_requests[0] == graph_requests[1] and len(graph_requests) == 2
        assert len(releases) == 2 and len(reports) == 1
        assert fixture.recovery.snapshot(identity).slot_collections == (reports[0].proof,)
        assert core.owner_table.contains(stored_id)
        assert core._recovery.lineage_for_object(stored_id) is not None

        core._reference_released(stored_id)
        assert stored_id not in core._object_gc_obligations
        assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.contains(stored_id) and stored_id not in core._objects
        assert core._recovery.lineage_for_object(stored_id) is None
        assert committed == [stored_id] and len(scheduled) == 2
        assert len(graph_requests) == 2 and len(releases) == 2
        assert len(reports) == 2 and reports[0] == reports[1]
        assert core.owner_table.snapshot(inline_id) == healthy
        assert core._recovery.lineage_for_object(inline_id) is not None

        assert core.owner_table.release_local_reference(inline_id, "outer0")
        core._reference_released(inline_id)
        assert committed == [stored_id, inline_id]
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert not fixture.graph.snapshot().committed_edges and not fixture.graph.snapshot().prepared_edges
        assert len(releases) == 4 and len(set(releases)) == 4
        for slot in manifest.slots:
            assert core.owner_table.collection_state(slot.object_id) is ObjectCollectionState.COLLECTED
            assert core._recovery.lineage_for_object(slot.object_id) is None
            for transfer in slot.transfers:
                child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
                assert transfer.final_hold not in child.contained_holds
                assert transfer.provisional_hold not in child.contained_holds
        terminal = fixture.recovery.snapshot(identity)
        assert terminal.frozen_node_death == death and terminal.adopted is not None
        assert tuple(proof.slot_index for proof in terminal.slot_collections) == (0, 1)
        assert terminal.complete == reply.output_publication.complete and terminal.resolution is None
        _assert_metadata(terminal)
        assert not core._protocol_unresolved and not core._task_finish_barriers
        # Core never consults or mutates the detached Node store after death.
        # A committed death, not a fabricated DROP ACK,
        # discharged its inaccessible replica; every GC RPC above targets GCS.
        assert fixture.store.get(stored_id) == fixture.values.payloads[1]
    finally:
        _close(core)


def test_plain_lost_object_without_descriptor_is_not_collected(monkeypatch):
    core = make_pure_core()
    task = TaskID.derive(core.job_id, core.driver_task_id, 0)
    attempt, object_id = AttemptID(task, 0), ObjectID.for_task(task)
    spec = protocol.TaskSpec(
        core.job_id, task, attempt, protocol.FunctionKey(core.job_id, __name__, "source", "v1"),
        (), 1, ResourceVector(), core.worker_id,
    )
    pending = _PendingTask(object_id, spec)
    core.owner_table.register_task_outputs(spec, local_tokens=("handle",))
    core._recovery.register_task(spec)
    core._objects[object_id] = _ObjectWaiter(threading.Event())

    def forbidden(*_args, **_kwargs):
        pytest.fail("source without integrity metadata attempted collection/RPC")

    monkeypatch.setattr(core, "_rpc", forbidden)
    monkeypatch.setattr(core, "_borrow_rpc", forbidden)
    try:
        # Source/replica metadata without a completed output envelope is not an
        # ordinary successful Task reply and is never routed through _publish_reply.
        assert core.owner_table.publish_stored(object_id, attempt, core.node_id)
        death = _node_death(core.node_id)
        assert core.handle_node_death(death, death.death_epoch, True).lost == (object_id,)
        assert core.owner_table.release_local_reference(object_id, "handle")
        before = core.owner_table.snapshot(object_id)
        assert before.state is ObjectState.LOST and not before.locations
        assert before.canonical_stored_result is None and before.output_publication is None
        assert object_id not in core._stored_descriptors
        # Unknown LOST bytes have no safe deletion identity. Unlike an exactly
        # retired output, this source must reject metadata-only collection. The
        # typed error is intentional; the old no-error return is not restored.
        with pytest.raises(InvalidObjectTransitionError, match="canonical metadata"):
            core._reference_released(object_id)
        assert core.owner_table.snapshot(object_id) == before
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.ACTIVE
        assert object_id not in core._object_gc_obligations
        assert core._recovery.lineage_for_object(pending.object_id) is not None
    finally:
        _close(core)
