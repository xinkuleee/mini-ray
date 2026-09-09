"""Pure adopted-output reverse GC after the publishing Node dies.

One stored output, two child transfers, one 1 KiB detached Node store, actual
owner/handoff/recovery authorities and synchronous callbacks only. The already-finished
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


def test_adopted_stored_outer_collects_exact_child_holds_after_publisher_death(monkeypatch):
    fixture, node, core, pending, reply, _calls, _rpc = _output_fixture(refs=True, stored=True)
    stored_id = pending.object_id
    manifest = reply.output_publication.manifest
    stored_slot = manifest.slots[0]
    identity = manifest.publication_id
    scheduled, releases, commits = [], [], []
    original_release = core._borrow_rpc
    original_commit = core.owner_table.complete_output_publication_collection
    lost_once = False

    def schedule(mailbox, event, delay):
        assert mailbox is core._reference_mailbox
        assert isinstance(event, _RetryInlineGc) and event.object_id == stored_id
        assert 0 < delay <= 0.25
        scheduled.append(event)

    def release(address, handler, request):
        nonlocal lost_once
        assert handler == "release_contained_reference"
        result = original_release(address, handler, request)
        assert result.accepted
        releases.append(request)
        if not lost_once:
            lost_once = True
            raise TransportTimeout("child Release committed before ACK was lost")
        return result

    def commit(plan):
        obligation = core._object_gc_obligations[stored_id]
        assert not obligation.pending_edges and not obligation.pending_drops
        commits.append(plan)
        return original_commit(plan)

    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id,
                                   expected_lease_id=identity.lease_id)
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert not core._protocol_unresolved and not core._output_result_custody
        assert fixture.handoffs.query(identity).adoption is not None
        assert not fixture.journal.snapshot(identity).retained_result_slots
        before = core.owner_table.snapshot(stored_id)
        descriptor = core._stored_descriptors[stored_id]
        assert before.canonical_stored_result == descriptor == reply.output_publication.results[0]
        assert fixture.store.get(stored_id) == fixture.values.payloads[0]
        incarnation = manifest.header.node_incarnation
        death = _node_death(incarnation.node_id, node_pid=incarnation.node_pid,
                            registration_epoch=incarnation.registration_epoch)
        removal = core.handle_node_death(death, protocol.InstallClusterSnapshot(
            death.death_epoch, "installed-publisher-death", ()))
        assert removal.lost == (stored_id,) and removal.collecting == ()
        assert core._dead_nodes[node.node_id] == death
        lost = core.owner_table.snapshot(stored_id)
        assert lost.state is ObjectState.LOST and not lost.locations
        assert lost.canonical_stored_result == descriptor and lost.output_publication == before.output_publication
        assert stored_id not in core._stored_descriptors
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert core._recovery.task_record(pending.task_id).retries_started == 0

        monkeypatch.setattr(core, "_schedule_reference_event", schedule)
        monkeypatch.setattr(core, "_borrow_rpc", release)
        monkeypatch.setattr(core, "_rpc", lambda *_a, **_k: pytest.fail("GC contacted dead publisher or fake GCS"))
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", commit)
        assert core.owner_table.release_local_reference(stored_id, "outer0")
        core._reference_released(stored_id)
        obligation = core._object_gc_obligations[stored_id]
        frozen = obligation.output_plan
        assert frozen.membership == before.output_publication
        assert frozen.metadata_plan.canonical_checksum == descriptor.checksum
        assert frozen.metadata_plan.canonical_size_bytes == descriptor.size_bytes
        assert frozen.metadata_plan.locations == () and not obligation.pending_drops
        assert len(obligation.pending_edges) == 1 and commits == []
        assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTING
        assert core._recovery.lineage_for_object(stored_id) is not None
        assert len(releases) == len(stored_slot.transfers) == 2
        for transfer in stored_slot.transfers:
            child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.final_hold not in child.contained_holds
            assert transfer.provisional_hold not in child.contained_holds

        core._reference_released(stored_id)
        assert commits == [frozen] and len(scheduled) == 1
        assert len(releases) == 3 and releases[2] == releases[0]
        assert stored_id not in core._object_gc_obligations and stored_id not in core._objects
        assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(stored_id) is None
        assert not core._protocol_unresolved and not core._task_finish_barriers
        terminal = fixture.handoffs.query(identity)
        assert terminal.complete == reply.output_publication.complete and terminal.adoption is not None
        _assert_metadata(terminal)
        # Exact Node death discharges its inaccessible private replica; no
        # fabricated Drop ACK and no read/mutation of this detached store.
        assert fixture.store.get(stored_id) == fixture.values.payloads[0]
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
        assert core.handle_node_death(death, protocol.InstallClusterSnapshot(
            death.death_epoch, "installed-source-death", ())).lost == (object_id,)
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
