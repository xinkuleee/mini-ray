"""Pure GCS shutdown barriers for the unified publication control path.

At most two tiny manifests, one registered Node/Worker metadata pair and four
contained edges.  No server start, Thread construction, RPC, real wait or user
code is allowed.  GCS shutdown/membership, output recovery, graph authority and
the child owner tables run unchanged; only transport is synchronous dispatch.
"""

from dataclasses import replace
from types import SimpleNamespace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, output_protocol as wire, protocol
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedGraphTransactionState,
)
from miniray.ids import AttemptID, LeaseID, TaskID
from miniray.output_publication import OutputPublicationManifest
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationSlotCleanupProof,
)
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryDisposition
from miniray.ownership import ObjectOwnerTable
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest
from tests.unit.test_output_publication import _assert_metadata
from tests.unit.test_output_publication_control import _service


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure output shutdown test attempted runtime infrastructure")

    for owner, method in (
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "start"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (socket, "socket"), (socket, "create_connection"),
        (subprocess, "Popen"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)
    monkeypatch.setattr(control, "rpc_request", forbidden)
    # Direct calls take shutdown's real no-joiner branch.  This is not a fake
    # liveness result for a background handler: no handler thread exists here.
    monkeypatch.setattr(control, "current_thread", lambda: SimpleNamespace(daemon=False))


def _peer_releases(service, values):
    """Represent already-promoted child custody in real owner tables.

    This fixture concerns GCS lifetime barriers, not discovery or promotion.
    Those prior facts are installed explicitly and release still executes the
    real owner token/tombstone transition instead of returning constant True.
    """
    owners = {}
    expected = set()
    for slot in values.slots:
        for transfer in slot.transfers:
            owner = owners.setdefault(transfer.contained_owner_worker_id, ObjectOwnerTable())
            child = transfer.contained_object_id
            if not owner.contains(child):
                owner.register(child, local_token="source-stays-live")
            owner.add_contained_reference(child, transfer.final_hold)
            expected.add((
                transfer.contained_owner_address,
                protocol.ReleaseContainedReference(
                    child, transfer.contained_owner_worker_id, transfer.final_hold,
                ),
            ))
    calls = []

    def release(address, handler, request):
        if handler != "release_contained_reference" or (address, request) not in expected:
            pytest.fail("unexpected child cleanup route or identity")
        assert not service.publications._composition_lock._is_owned()
        calls.append(request)
        assert len(calls) <= 4
        changed = owners[request.owner_worker_id].release_contained_reference(
            request.object_id, request.hold,
        )
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, changed,
        )

    service._stored_hold_rpc = release
    return owners, calls


def test_shutdown_fences_new_publications_but_exact_reports_and_cleanup_reach_clean(monkeypatch):
    service, values = _service(monkeypatch, refs=True)
    adapter = service.publications
    graph = values.manifest.to_graph_manifest()
    report = service.report_output_publication
    intent = wire.ReportOutputPublicationIntent(values.manifest)
    prepare = protocol.PrepareContainedGraph(graph)
    assert report(intent).accepted
    assert service.prepare_contained_graph(prepare).accepted
    peers, calls = _peer_releases(service, values)
    shutdown = protocol.Shutdown("output-cleanup-shutdown", "pure barrier contract")

    blocked = service.shutdown(shutdown)
    assert blocked.request_id == shutdown.request_id and not blocked.clean
    assert not service._stop_event.is_set()
    assert not service._shutdown_exit_scheduled
    assert adapter._publication_admission_closed
    assert adapter.graph.snapshot().admission_closed
    assert service._owner_death_progress_thread is None
    assert not service.placement_groups.has_active_operations()
    assert not service.actor_coordinator.has_active_operations()
    assert not service.owner_death_fences.has_active_operations()
    assert adapter.has_active_operations()

    # Closing admission is not permission to strand the already-accepted
    # identity: exact INTENT/PREPARE replay and its completion remain legal.
    replay = report(intent)
    assert replay.accepted and replay.request == intent
    assert replay.ack.disposition is OutputRecoveryDisposition.ALREADY_RECORDED
    prepared = service.prepare_contained_graph(prepare)
    assert prepared.accepted
    assert prepared.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_PREPARED
    other_header = replace(values.header, publication_id=replace(
        values.publication_id, lease_id=type(values.lease).random(),
    ))
    unseen = OutputPublicationManifest.create(other_header, values.slots)
    before = adapter.graph.snapshot()
    assert not report(wire.ReportOutputPublicationIntent(unseen)).accepted
    assert not service.prepare_contained_graph(protocol.PrepareContainedGraph(
        unseen.to_graph_manifest()
    )).accepted
    assert adapter.graph.snapshot() == before
    assert adapter.output_recovery.publication_ids() == (values.publication_id,)
    with pytest.raises(ValueError, match="different request ID"):
        service.shutdown(protocol.Shutdown("other-shutdown", "must not rebind"))

    assert report(wire.ArmOutputPublication(
        values.publication_id, values.manifest.manifest_digest,
    )).accepted
    assert report(wire.ReportOutputPublicationTerminal(values.witness)).accepted
    commit = service.commit_contained_graph(protocol.CommitContainedGraph(graph))
    assert commit.accepted and commit.receipt.state is ContainedGraphTransactionState.COMMITTED
    adoption = wire.ReportOutputPublicationAdopted(OutputPublicationAdoptionProof(
        values.witness, values.owner, "shutdown-owner-received",
    ))
    assert report(adoption).accepted
    assert not service.shutdown(shutdown).clean  # committed graph still owns four edges
    assert not service._stop_event.is_set()

    cleanup_requests = []
    for index, slot in enumerate(values.slots):
        for transfer in slot.transfers:
            request = protocol.ReleaseContainedReference(
                transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold,
            )
            reply = service._stored_hold_rpc(
                transfer.contained_owner_address, "release_contained_reference", request,
            )
            assert reply.accepted and reply.released
        release = protocol.ReleaseContainedGraphContainer(graph, slot.object_id)
        released = service.release_contained_graph_container(release)
        assert released.accepted and released.receipt.released_edges == slot.edges
        cleanup = wire.ReportOutputPublicationSlotCollected(OutputPublicationSlotCleanupProof(
            values.witness, values.owner, index, slot.object_id, "shutdown-slot-{}".format(index),
        ))
        assert report(cleanup).accepted
        cleanup_requests.append((release, cleanup))
        if index == 0:
            assert not service.shutdown(shutdown).clean
            assert not service._stop_event.is_set()
            assert set(adapter.graph.snapshot().committed_edges) == set(values.slots[1].edges)

    assert len(calls) == 4
    for slot in values.slots:
        for transfer in slot.transfers:
            child = peers[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.final_hold not in child.contained_holds
            assert "source-stays-live" in child.local_tokens
    assert not adapter.graph.snapshot().committed_edges
    assert not adapter.graph.snapshot().prepared_edges
    assert not adapter.has_active_operations()
    clean = service.shutdown(shutdown)
    assert clean.clean and clean.request_id == shutdown.request_id
    assert service._stop_event.is_set() and service._shutdown_exit_scheduled
    assert service.shutdown(shutdown).clean
    for release, cleanup in cleanup_requests:
        assert service.release_contained_graph_container(release).receipt.disposition is (
            ContainedGraphManifestDisposition.ALREADY_RELEASED
        )
        assert report(cleanup).ack.disposition is OutputRecoveryDisposition.ALREADY_RECORDED
    assert report(adoption).accepted
    assert service._owner_death_progress_thread is None
    _assert_metadata(adapter.output_recovery.snapshot(values.publication_id))


def test_closed_shutdown_still_commits_and_replays_exact_node_death_under_publication_lock(monkeypatch):
    service, values = _service(monkeypatch, refs=False)
    adapter, registry = service.publications, service.publications.output_recovery
    assert service.report_output_publication(wire.ReportOutputPublicationIntent(values.manifest)).accepted
    shutdown = protocol.Shutdown("output-shutdown-before-node-death", "pure metadata contract")
    assert not service.shutdown(shutdown).clean
    assert adapter._publication_admission_closed and not service._stop_event.is_set()
    node = values.header.node_incarnation
    request = protocol.ReportNodeDeath(
        "node-exit-after-shutdown", node.node_id, node.node_pid, node.registration_epoch,
        1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed process exit",
    )
    membership = service.nodes.report_death
    freeze = registry.freeze_node_death
    events = []

    def commit_membership(message):
        assert adapter._composition_lock._is_owned()
        assert message == request
        result = membership(message)
        events.append(("membership", result.disposition))
        return result

    def freeze_outputs(death):
        assert adapter._composition_lock._is_owned()
        observed = service.nodes.get(node.node_id)
        assert observed.state is protocol.NodeMembershipState.DEAD
        assert observed.death == death
        result = freeze(death)
        assert len(result) == 1
        assert result[0].publication_id == values.publication_id
        events.append(("freeze", death))
        return result

    monkeypatch.setattr(service.nodes, "report_death", commit_membership)
    monkeypatch.setattr(registry, "freeze_node_death", freeze_outputs)
    first = service.report_node_death(request)
    assert first.disposition is protocol.NodeDeathDisposition.APPLIED
    work = registry.frozen_workset(first.death)
    second = service.report_node_death(request)
    assert second.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert second.death == first.death
    assert registry.frozen_workset(second.death) == work
    assert events == [
        ("membership", protocol.NodeDeathDisposition.APPLIED), ("freeze", first.death),
        ("membership", protocol.NodeDeathDisposition.ALREADY_DEAD), ("freeze", first.death),
    ]
    assert work[0].action is OutputRecoveryAction.PRECOMPLETE_ROLLBACK
    assert work[0].snapshot.complete is None
    worker = service.get_worker_state(protocol.GetWorkerState(values.executor))
    assert worker.state is protocol.WorkerMembershipState.DEAD
    assert worker.death.reason is protocol.WorkerDeathReason.NODE_EXIT
    assert not service.shutdown(shutdown).clean
    assert not service._stop_event.is_set()
    assert not service.report_output_publication(
        wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest)
    ).accepted
    assert registry.frozen_workset(first.death) == work

    # No edges, bytes, PGs or actors exist.  One real bounded progress call can
    # resolve the frozen intent without any fabricated peer acknowledgment.
    progress = wire.ProgressOutputNodeLoss(work[0])
    result = service.progress_output_node_loss(progress)
    assert result.snapshot.resolution is not None
    assert result.snapshot.resolution.kept_slots == ()
    assert result.snapshot.complete is None
    assert service.progress_output_node_loss(progress).snapshot == result.snapshot
    assert registry.frozen_workset(first.death) == work
    assert not adapter.has_active_operations()
    assert service.shutdown(shutdown).clean
    assert service._stop_event.is_set()
    assert service._owner_death_progress_thread is None
    _assert_metadata(result)


def test_membership_commit_before_freeze_failure_replays_complete_two_publication_workset(monkeypatch):
    """One failed service composition cannot hide either admitted publication.

    The registry already builds its whole workset before mutation.  This test
    instead interrupts the service between the earlier membership commit and
    that atomic registry operation; it does not recreate a partially mutated
    legacy saga coordinator.  Exactly two publications/four slots, no edges.
    """
    service, values = _service(monkeypatch, refs=False)
    adapter, registry = service.publications, service.publications.output_recovery
    task = TaskID(bytes.fromhex("32" * 16))
    execution = TaskExecutionKey(TaskOutputManifest.for_task(task, 2), AttemptID(task, 0))
    header = replace(values.header, publication_id=replace(
        values.publication_id, execution=execution, lease_id=LeaseID(bytes.fromhex("70" * 16)),
    ))
    second_manifest = OutputPublicationManifest.create(header, tuple(
        replace(slot, object_id=object_id)
        for slot, object_id in zip(values.slots, execution.output_ids)
    ))
    manifests = (values.manifest, second_manifest)
    identities = tuple(manifest.publication_id for manifest in manifests)
    assert len(set(identities)) == 2
    assert len({object_id for identity in identities for object_id in identity.output_ids}) == 4
    for manifest in manifests:
        assert manifest.to_graph_manifest() is None
        assert service.report_output_publication(wire.ReportOutputPublicationIntent(manifest)).accepted
    before = tuple(registry.snapshot(identity) for identity in identities)
    shutdown = protocol.Shutdown("two-publication-freeze-shutdown", "one interrupted freeze")
    assert not service.shutdown(shutdown).clean
    assert adapter._publication_admission_closed and not service._stop_event.is_set()
    node = values.header.node_incarnation
    request = protocol.ReportNodeDeath(
        "node-exit-two-publications", node.node_id, node.node_pid, node.registration_epoch,
        1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed process exit",
    )
    report_membership = service.nodes.report_death
    freeze = registry.freeze_node_death
    membership_dispositions, freeze_attempts = [], []

    def observed_membership(message):
        assert adapter._composition_lock._is_owned()
        assert message == request
        reply = report_membership(message)
        membership_dispositions.append(reply.disposition)
        assert len(membership_dispositions) <= 2
        return reply

    def fail_once_before_atomic_freeze(death):
        assert adapter._composition_lock._is_owned()
        committed_node = service.nodes.get(node.node_id)
        assert committed_node.state is protocol.NodeMembershipState.DEAD
        assert committed_node.death == death
        freeze_attempts.append(death)
        assert len(freeze_attempts) <= 2
        if len(freeze_attempts) == 1:
            raise RuntimeError("publication freeze interrupted after membership commit")
        assert tuple(registry.snapshot(identity) for identity in identities) == before
        workset = freeze(death)
        assert len(workset) == 2
        assert {work.publication_id for work in workset} == set(identities)
        return workset

    monkeypatch.setattr(service.nodes, "report_death", observed_membership)
    monkeypatch.setattr(registry, "freeze_node_death", fail_once_before_atomic_freeze)
    with pytest.raises(RuntimeError, match="after membership commit"):
        service.report_node_death(request)
    committed = service.nodes.get(node.node_id)
    assert committed.state is protocol.NodeMembershipState.DEAD
    assert committed.death is not None and committed.death == freeze_attempts[0]
    assert membership_dispositions == [protocol.NodeDeathDisposition.APPLIED]
    assert tuple(registry.snapshot(identity) for identity in identities) == before
    assert all(snapshot.frozen_node_death is None for snapshot in before)
    assert set(registry.publication_ids()) == set(identities)
    assert adapter.has_active_operations()
    assert not service.shutdown(shutdown).clean
    assert not service._stop_event.is_set()

    # No intervening intent/query handler is called: publisher validation may
    # legitimately perform a lazy freeze.  This replay must itself repair the
    # exposed membership/workset gap and cannot return success after only one
    # of the two publications was admitted to recovery.
    replay = service.report_node_death(request)
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert replay.death == committed.death
    assert membership_dispositions == [
        protocol.NodeDeathDisposition.APPLIED, protocol.NodeDeathDisposition.ALREADY_DEAD,
    ]
    assert freeze_attempts == [committed.death, committed.death]
    workset = registry.frozen_workset(replay.death)
    assert len(workset) == 2 and {work.publication_id for work in workset} == set(identities)
    assert {work.manifest for work in workset} == set(manifests)
    assert all(work.action is OutputRecoveryAction.PRECOMPLETE_ROLLBACK for work in workset)
    assert all(registry.snapshot(identity).frozen_node_death == replay.death for identity in identities)
    assert not service.shutdown(shutdown).clean

    for index, work in enumerate(workset):
        result = service.progress_output_node_loss(wire.ProgressOutputNodeLoss(work))
        assert result.snapshot.resolution is not None
        assert result.snapshot.resolution.kept_slots == () and result.snapshot.complete is None
        assert registry.frozen_workset(replay.death) == workset
        _assert_metadata(result)
        if index == 0:
            assert adapter.has_active_operations()
            assert not service.shutdown(shutdown).clean
            assert not service._stop_event.is_set()
    assert not adapter.has_active_operations()
    assert service.shutdown(shutdown).clean
    assert service._stop_event.is_set()
    assert service._owner_death_progress_thread is None
