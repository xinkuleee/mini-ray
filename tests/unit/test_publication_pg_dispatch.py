"""A dispatch replay must finish existing publication after another PG Node dies.

One threadless Core, one tiny input put and one single-slot INLINE publication
use the real discovery/journal/adapter and owner/recovery authorities. A full
two-bundle scheduling manifest and typed survivor snapshot model the other
bundle's Node death; the publication's own Node stays alive. No Node/Core/GCS
constructor, socket, thread, sleep, user execution or physical allocation runs.

An actual terminal/adopted ACK is lost after its effect. Only then does the
existing adoption retry meet PG LOST in the real dispatch loop. Its finite
queue contains that original ReadyTask followed by STOP, never an empty wait.
The two phases distinguish PENDING before owner CAS from already READY with
unretired Node payload. Neither fixture starts from a fabricated terminal error.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, protocol, output_protocol as wire
from miniray.core import CoreWorker, _DelayedReadyTask, _NodeDeathObserved, _STOP, _WAKE_COORDINATOR
from miniray.ids import LeaseID, NodeID, PlacementGroupID, WorkerID
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure publication dispatch attempted runtime or unmodelled RPC")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Barrier, "wait"), (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)


def _close_local(reference):
    if reference is not None and not reference.closed:
        assert reference.borrower_token is None
        done = reference._release_done
        reference._closed = True
        reference._finalizer()  # The pure mailbox applies the real token release.
        assert done is not None and done.is_set()


def _take_adoption_retry(core):
    retry = None
    count = core._submissions.qsize()
    assert count <= 4
    for _ in range(count):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if type(item) is _DelayedReadyTask:
            assert retry is None
            retry = item
        else:
            assert item is _WAKE_COORDINATOR
    assert retry is not None and retry.ready.output_adoption is not None
    assert core._submissions.empty()
    return retry


@pytest.mark.parametrize("lost_ack", ("terminal", "adopted"))
def test_publication_replay_survives_other_bundle_node_loss_before_dispatch(lost_ack):
    core = make_pure_core()
    source = output = None
    try:
        outputs = PureOutputRuntime(core)
        core.gcs_address = outputs.gcs_address
        other_node = NodeID(bytes(value ^ 1 for value in core.node_id.value))
        group_id = PlacementGroupID.random()
        key = protocol.PlacementGroupSchedulingKey(group_id, 0, 0, core.node_id, "a" * 64)
        other_key = protocol.PlacementGroupSchedulingKey(group_id, 0, 1, other_node, "b" * 64)
        core._placement_group_states = {(group_id, 0): protocol.PlacementGroupPhaseStatus.CREATED}
        core._placement_group_manifests = {(group_id, 0): (key, other_key)}
        capacity = ResourceVector({"CPU": 1})
        survivor = protocol.NodeInfo(
            core.node_id, outputs.incarnation.node_pid, outputs.incarnation.registration_epoch,
            core.node_address, capacity, capacity,
        )
        victim = protocol.NodeInfo(other_node, 22001, 4, ("victim.invalid", 2), capacity, capacity)
        core._membership_epoch = 4
        core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(4, "two-pg-bundles", (survivor, victim))

        source = core.put(41)
        assert core._submissions.get_nowait() is _WAKE_COORDINATOR
        core._submissions.task_done()
        pending, output = core._register_submission(
            core.define_remote_function(lambda value: value + 1), (source,), {}, capacity,
            max_retries=1, placement_group_scheduling_key=key, _enqueue=True,
        )
        assert core._submissions.get_nowait() is pending
        core._submissions.task_done()
        assert core._submissions.empty()
        prepared, dependencies, protected = core._prepare_task_dependencies(pending.spec)
        assert dependencies == () and protected == (source.object_id,)
        source_before = core.owner_table.snapshot(source.object_id)
        assert pending.dependency_hold in source_before.submitted_tokens and source_before.lineage_tokens
        push = protocol.PushTask(LeaseID.random(), WorkerID.random(), prepared)
        reply = outputs.complete(push, (42,))
        identity = reply.output_publication.publication_id
        assert reply.output_publication.manifest.header.node_incarnation.node_id == core.node_id
        assert outputs.journal.snapshot(identity).complete == reply.output_publication.complete
        assert outputs.recovery.snapshot(identity).complete is None
        assert outputs.discoveries == len(outputs.completions) == 1
        assert sum(slot.size_bytes for slot in reply.output_publication.manifest.slots) < 1024
        lost = []
        calls = []
        chosen_type = wire.ReportOutputPublicationTerminal if lost_ack == "terminal" else wire.ReportOutputPublicationAdopted

        def lose_one_ack(address, handler, request):
            assert len(calls) < 8
            calls.append((handler, request))
            result = outputs.rpc(address, handler, request)
            if type(request) is chosen_type and not lost:
                assert type(result) is wire.OutputRecoveryReply and result.accepted
                lost.append((request, result))
                raise TransportTimeout("publication fact applied but ACK was lost")
            return result

        core._rpc = lose_one_ack
        assert not core._publish_reply(
            pending, reply, expected_node_id=core.node_id, expected_lease_id=push.lease_id,
        )
        assert len(lost) == 1
        before = core.owner_table.snapshot(output.object_id)
        assert before.state is (ObjectState.PENDING if lost_ack == "terminal" else ObjectState.READY_INLINE)
        assert before.error is None
        assert outputs.recovery.snapshot(identity).complete == reply.output_publication.complete
        assert (outputs.recovery.snapshot(identity).adopted is not None) is (lost_ack == "adopted")
        assert outputs.journal.snapshot(identity).retained_result_slots == (0,)
        assert not core._finish_pending_task(pending)
        assert core._accepted_task_count == 1 and core._task_finish_barriers[output.object_id] == pending
        delayed = _take_adoption_retry(core)
        assert delayed.ready.pending is pending and delayed.ready.output_adoption.envelope == reply.output_publication
        assert delayed.ready.output_adoption.node_id == core.node_id

        death = protocol.NodeDeathRecord(
            "other-pg-bundle-exited", victim.node_id, victim.node_pid, victim.registration_epoch,
            5, -9, protocol.NodeDeathReason.PROCESS_EXIT, "only the other PG bundle Node exited",
        )
        installed = protocol.InstallClusterSnapshot(5, "surviving-publication-node", (survivor,))
        core.handle_node_death(death, installed)
        observed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert observed == _NodeDeathObserved(death, installed.membership_epoch)
        assert core._submissions.empty()
        assert core._dead_nodes == {other_node: death}
        assert core._installed_cluster_snapshot == installed
        assert core._placement_group_states[(group_id, 0)] is protocol.PlacementGroupPhaseStatus.LOST
        assert core.owner_table.snapshot(output.object_id) == before
        assert core.owner_table.snapshot(source.object_id) == source_before
        assert not core._finish_pending_task(pending)

        # Consume the real retained work item, not a direct adoption helper.
        # No ready queue wait occurs: both the replay and STOP are preloaded.
        core._ready_tasks = queue.Queue()
        core._ready_tasks.put_nowait(delayed.ready)
        core._ready_tasks.put_nowait(_STOP)
        core._dispatch_loop()
        assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
        after = core.owner_table.snapshot(output.object_id)
        assert after.state is ObjectState.READY_INLINE and after.error is None
        assert after.inline_data == reply.results[0].inline_data
        assert after.current_attempt == pending.spec.attempt_id
        assert after.output_publication.publication_id == identity
        history = outputs.recovery.snapshot(identity)
        assert history.adopted is not None and history.adopted.complete == reply.output_publication.complete
        assert not outputs.journal.snapshot(identity).retained_result_slots
        assert outputs.discoveries == len(outputs.pushes) == len(outputs.completions) == 1
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert core._accepted_task_count == 0 and pending.task_key in core._finished_tasks
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        source_after = core.owner_table.snapshot(source.object_id)
        assert not source_after.submitted_tokens and source_after.lineage_tokens == source_before.lineage_tokens
        assert sum(handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER for handler, _request in calls) == 1
        assert sum(type(request) is chosen_type for _handler, request in calls) == 2

        _close_local(output)
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(output.object_id) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.snapshot(source.object_id).lineage_tokens
        _close_local(source)
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED
        assert not core._objects and not core._object_gc_obligations
        assert core._reference_mailbox.pending.empty()
        outputs.assert_collected()
    finally:
        _close_local(output)
        _close_local(source)
        close_pure_core(core)
