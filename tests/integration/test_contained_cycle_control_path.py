"""Bounded real-wire graph cycle rejection and ordinary owner-GC release.

One GCS, one Node, one Worker (three children), a 1 MiB store, one tiny put
and one ordinary task. Two additional publications are explicitly metadata-only
control-test inputs, not public ObjectRefs or claims that their child holds
exist. They name the real registered publisher and exercise unified INTENT and
graph PREPARE/ABORT over TCP. No child/byte/ARM/Complete effect is performed for
them; their rollback reports acknowledge only actual graph ABORT replies.

A separate ordinary task returns a real Driver-owned child, so positive graph
COMMIT/RELEASE evidence comes from actual publication and owner GC, never a
fabricated Complete or adoption proof. No test listener/thread, process patch,
kill, private owner-table mutation or mocked transport/ACK is used. Bounded
handle close reuses the normal finalizer with a deadline instead of an unlimited
wait. Post-init work shares 12 seconds; cleanup gets three seconds. Run only
this exact case through the 30-second process-tree runner after static review.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray import control, output_protocol as wire, protocol
from miniray.api import _get_runtime
from miniray.contained_cycle import ContainedGraphManifestDisposition, ContainedGraphTransactionState
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, LeaseID, ObjectID, TaskID
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationAckDisposition, OutputPublicationEffect,
    OutputPublicationRollbackPlan, OutputPublicationRollbackTombstone, OutputPublicationStage,
)
from miniray.output_recovery import OutputRecoveryDisposition, OutputRecoveryStage
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_sources import BorrowedContainedSource, PreparedContainedTransfer
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest
from miniray.transport import request as rpc_request
from tests.integration.test_multi_contained_output_path import _close_local


pytestmark = pytest.mark.multiprocess_smoke


@ray.remote(max_retries=0)
def _return_live_driver_child(container):
    return {"child": container[0]}


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "contained-cycle control smoke exceeded its shared deadline"
    return remaining


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining),
                       request_timeout=min(1.0, remaining), deadline=deadline)


def _report(address, request, deadline):
    reply = _rpc(address, wire.REPORT_OUTPUT_PUBLICATION_HANDLER, request, deadline)
    assert type(reply) is wire.OutputRecoveryReply and reply.request == request
    return replace(reply)


def _graph(address, handler, request, deadline):
    reply = _rpc(address, handler, request, deadline)
    assert type(reply) is protocol.ContainedGraphReply and reply.request == request
    return replace(reply)


def _graph_query(address, manifest, deadline):
    request = protocol.GetContainedGraph(manifest.transaction_id)
    reply = _rpc(address, control.GET_CONTAINED_GRAPH_HANDLER, request, deadline)
    assert type(reply) is protocol.GetContainedGraphReply and reply.request == request
    return replace(reply)


def _recovery(address, publication_id, deadline, *, allow_missing=False):
    request = wire.GetOutputPublicationRecovery(publication_id)
    reply = _rpc(address, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER, request, deadline)
    assert type(reply) is wire.GetOutputPublicationRecoveryReply and reply.request == request
    assert reply.error_kind is None and reply.error is None
    if allow_missing and not reply.found:
        return None
    assert reply.found and reply.snapshot is not None
    return replace(reply.snapshot)


def _wait(core, predicate, deadline):
    with core._completion:
        while not predicate():
            core._completion.wait(_remaining(deadline))


def _metadata_cycle(core, node, incarnation, owner_address):
    """Declare A -> B and B -> A without installing owner/Node data.

    The source fields are graph-control model values, not acquired borrowing
    permissions. No RPC in this test asks a child owner to trust those values.
    """
    tasks = (TaskID.random(), TaskID.random())
    objects = tuple(ObjectID.for_task(task) for task in tasks)
    manifests = []
    for index, (task, output) in enumerate(zip(tasks, objects)):
        attempt = AttemptID(task, 0)
        identity = OutputPublicationID(LeaseID.random(), TaskExecutionKey(
            TaskOutputManifest.for_task(task, 1), attempt,
        ))
        token = "cycle-control-edge-{}".format(index)
        source = BorrowedContainedSource(node.worker_id, "model-borrow-{}".format(index),
            protocol.TaskHoldSource(protocol.TaskReferenceHold(
                protocol.TaskReferenceHoldKind.RETAINED, node.worker_id, task, attempt,
            )))
        transfer = PreparedContainedTransfer(
            objects[1 - index], core.worker_id, owner_address, source,
            ContainedReferenceHold(output, node.worker_id, token),
            ContainedReferenceHold(output, core.worker_id, token),
        )
        header = OutputPublicationHeader(identity, core.job_id, node.worker_id, core.worker_id, incarnation)
        slot = OutputSlotManifest(output, protocol.ResultStorage.INLINE, 0,
                                  hashlib.sha256(b"").hexdigest(), (transfer,))
        manifests.append(OutputPublicationManifest.create(header, (slot,)))
    return tuple(manifests)


def _abort_metadata(address, manifest, deadline):
    """Close only the graph effect actually attempted by this control client.

    A real Node journal requires real child-prepare ACKs before graph prepare;
    this metadata-only client must not forge those ACKs merely to use that
    convenience API. Its typed rollback proof records only the observed graph
    ABORT. It cannot claim any materialization, child release or Complete.
    """
    snapshot = _recovery(address, manifest.publication_id, deadline, allow_missing=True)
    if snapshot is None:
        return None
    assert snapshot.manifest == manifest and not snapshot.armed
    assert snapshot.complete is snapshot.adopted is None and snapshot.slot_collections == ()
    tombstone = snapshot.rollback
    if tombstone is None:
        request = protocol.AbortContainedGraph(manifest.to_graph_manifest())
        reply = _graph(address, control.ABORT_CONTAINED_GRAPH_HANDLER, request, deadline)
        assert reply.accepted and reply.receipt.state is ContainedGraphTransactionState.ABORTED
        assert reply.receipt.disposition in (ContainedGraphManifestDisposition.APPLIED,
                                             ContainedGraphManifestDisposition.ALREADY_ABORTED)
        assert reply.receipt.released_edges == ()
        effect = OutputPublicationEffect(manifest.publication_id, manifest.manifest_digest,
                                         OutputPublicationStage.GRAPH_ABORT)
        disposition = (OutputPublicationAckDisposition.APPLIED
                       if reply.receipt.disposition is ContainedGraphManifestDisposition.APPLIED
                       else OutputPublicationAckDisposition.ALREADY_APPLIED)
        plan = OutputPublicationRollbackPlan(manifest.publication_id, manifest.manifest_digest,
            "cycle-control-rollback:{}".format(manifest.publication_id.lease_id), (effect,))
        tombstone = OutputPublicationRollbackTombstone(plan, (OutputPublicationAck(effect, disposition),))
    report = wire.ReportOutputPublicationRollback(tombstone, manifest)
    result = _report(address, report, deadline)
    assert result.accepted and result.ack.stage is OutputRecoveryStage.ROLLED_BACK
    assert result.ack.snapshot.rollback == tombstone
    return tombstone


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container():
    context = report = core = source = outer = restored = None
    manifests = ()
    pids, addresses = set(), set()
    cleanup_errors = []
    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False)
        deadline = time.monotonic() + 12.0
        runtime = _get_runtime()
        core, node = runtime.core_worker, context.nodes[0]
        pids.update((context.gcs_pid, node.node_pid, node.worker_pid))
        addresses.update((context.gcs_address, node.node_address, node.worker_address, runtime.owner_service.address))
        assert len(pids) == 3 and os.getpid() not in pids and context.trace_address is None
        registered = _rpc(context.gcs_address, control.GET_NODE_STATE_HANDLER,
                          protocol.GetNodeState(node.node_id), deadline)
        worker = _rpc(context.gcs_address, control.GET_WORKER_STATE_HANDLER,
                      protocol.GetWorkerState(node.worker_id), deadline)
        assert type(registered) is protocol.GetNodeStateReply and registered.found
        assert registered.node_id == node.node_id and registered.node_pid == node.node_pid
        assert registered.state is protocol.NodeMembershipState.ALIVE and registered.death is None
        assert type(worker) is protocol.GetWorkerStateReply and worker.found and worker.death is None
        assert worker.worker_id == node.worker_id and worker.state is protocol.WorkerMembershipState.ALIVE
        incarnation = OutputPublicationNodeIncarnation(node.node_id, node.node_pid, registered.registration_epoch)
        assert worker.incarnation == protocol.WorkerIncarnation(
            node.node_id, node.node_pid, registered.registration_epoch, node.worker_id, node.worker_pid,
        )
        manifests = _metadata_cycle(core, node, incarnation, runtime.owner_service.address)
        first, second = manifests
        graphs = tuple(manifest.to_graph_manifest() for manifest in manifests)
        first_prepare, second_prepare = (protocol.PrepareContainedGraph(graph) for graph in graphs)
        before = []
        for manifest, graph in zip(manifests, graphs):
            assert not core.owner_table.contains(manifest.slots[0].object_id)
            assert _graph_query(context.gcs_address, graph, deadline).disposition is protocol.ContainedGraphQueryDisposition.NOT_FOUND
            intent = _report(context.gcs_address, wire.ReportOutputPublicationIntent(manifest), deadline)
            assert intent.accepted and intent.ack.stage is OutputRecoveryStage.INTENT
            assert not intent.ack.snapshot.armed and intent.ack.snapshot.complete is None
            before.append(intent.ack.snapshot)

        prepared = _graph(context.gcs_address, control.PREPARE_CONTAINED_GRAPH_HANDLER, first_prepare, deadline)
        assert prepared.accepted and prepared.receipt.state is ContainedGraphTransactionState.PREPARED
        assert prepared.receipt.disposition is ContainedGraphManifestDisposition.APPLIED
        replay = _graph(context.gcs_address, control.PREPARE_CONTAINED_GRAPH_HANDLER, first_prepare, deadline)
        assert replay.accepted and replay.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_PREPARED
        for _ in range(2):
            rejected = _graph(context.gcs_address, control.PREPARE_CONTAINED_GRAPH_HANDLER, second_prepare, deadline)
            assert not rejected.accepted and rejected.receipt is None
            assert rejected.error_kind is protocol.ContainedGraphRPCErrorKind.CYCLE
        assert _graph_query(context.gcs_address, graphs[1], deadline).disposition is protocol.ContainedGraphQueryDisposition.NOT_FOUND
        assert tuple(_recovery(context.gcs_address, manifest.publication_id, deadline) for manifest in manifests) == tuple(before)
        for handler, request in (
            (control.COMMIT_CONTAINED_GRAPH_HANDLER, protocol.CommitContainedGraph(graphs[0])),
            (control.RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER, protocol.ReleaseContainedGraphContainer(graphs[0], first.slots[0].object_id)),
        ):
            premature = _graph(context.gcs_address, handler, request, deadline)
            assert not premature.accepted and premature.error_kind is protocol.ContainedGraphRPCErrorKind.INVALID_STATE

        first_rollback = _abort_metadata(context.gcs_address, first, deadline)
        assert first_rollback is not None
        late_first = _graph(context.gcs_address, control.PREPARE_CONTAINED_GRAPH_HANDLER, first_prepare, deadline)
        assert not late_first.accepted and late_first.error_kind is protocol.ContainedGraphRPCErrorKind.INVALID_STATE
        # The rejected inverse edge left no reservation. Once A -> B is really
        # aborted, the exact unchanged B -> A request is admissible.
        second_ok = _graph(context.gcs_address, control.PREPARE_CONTAINED_GRAPH_HANDLER, second_prepare, deadline)
        assert second_ok.accepted and second_ok.receipt.disposition is ContainedGraphManifestDisposition.APPLIED
        second_rollback = _abort_metadata(context.gcs_address, second, deadline)
        assert second_rollback is not None
        for manifest, graph, tombstone in zip(manifests, graphs, (first_rollback, second_rollback)):
            assert _abort_metadata(context.gcs_address, manifest, deadline) == tombstone
            aborted = _graph(context.gcs_address, control.ABORT_CONTAINED_GRAPH_HANDLER, protocol.AbortContainedGraph(graph), deadline)
            assert aborted.accepted and aborted.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_ABORTED
            # FOUND is retained manifest history, not evidence of active edges.
            found = _graph_query(context.gcs_address, graph, deadline)
            assert found.disposition is protocol.ContainedGraphQueryDisposition.FOUND and found.manifest == graph
            for request in (wire.ReportOutputPublicationIntent(manifest),
                            wire.ArmOutputPublication(manifest.publication_id, manifest.manifest_digest)):
                fenced = _report(context.gcs_address, request, deadline)
                assert not fenced.accepted and fenced.ack.disposition is OutputRecoveryDisposition.FENCED
            saved = _recovery(context.gcs_address, manifest.publication_id, deadline)
            assert saved.rollback == tombstone and not saved.armed and saved.complete is saved.adopted is None
            assert not core.owner_table.contains(manifest.slots[0].object_id)

        # Positive RELEASE is never synthesized for the metadata-only graph.
        # Obtain a genuine borrowed-child result through the public runtime.
        source = ray.put(("cycle-control-live-child", 42))
        outer = _return_live_driver_child.remote([source])
        value = ray.get(outer, timeout=_remaining(deadline))
        restored = value["child"]
        assert restored.object_id == source.object_id and restored.owner_worker_id == core.worker_id
        assert ray.get(restored, timeout=_remaining(deadline)) == ("cycle-control-live-child", 42)
        _wait(core, lambda: outer.object_id not in core._task_finish_barriers, deadline)
        snapshot = core.owner_table.snapshot(outer.object_id)
        membership = snapshot.output_publication
        assert snapshot.state is ObjectState.READY_INLINE and membership is not None
        assert len(membership.slot.transfers) == 1
        transfer = membership.slot.transfers[0]
        assert isinstance(transfer.source, BorrowedContainedSource)
        assert transfer.contained_object_id == source.object_id and transfer.contained_owner_worker_id == core.worker_id
        assert transfer.final_hold in core.owner_table.snapshot(source.object_id).contained_holds
        live_graph = membership.manifest.to_graph_manifest()
        assert _graph_query(context.gcs_address, live_graph, deadline).manifest == live_graph
        real_history = _recovery(context.gcs_address, membership.publication_id, deadline)
        assert real_history.complete is not None and real_history.adopted is not None
        assert real_history.slot_collections == ()
        _close_local(restored, deadline)
        _close_local(outer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        child = core.owner_table.snapshot(source.object_id)
        assert transfer.provisional_hold not in child.contained_holds and transfer.final_hold not in child.contained_holds
        assert ray.get(source, timeout=_remaining(deadline)) == ("cycle-control-live-child", 42)
        collected = _recovery(context.gcs_address, membership.publication_id, deadline)
        assert len(collected.slot_collections) == 1
        proof = collected.slot_collections[0]
        assert proof.object_id == outer.object_id and proof.complete == real_history.complete
        release_request = protocol.ReleaseContainedGraphContainer(live_graph, outer.object_id)
        released = _graph(context.gcs_address, control.RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER, release_request, deadline)
        assert released.accepted and released.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_RELEASED
        assert released.receipt.released_edges == membership.slot.edges
        # A late COMMIT is a replay, not a way to restore already-released edges.
        commit = _graph(context.gcs_address, control.COMMIT_CONTAINED_GRAPH_HANDLER, protocol.CommitContainedGraph(live_graph), deadline)
        assert commit.accepted and commit.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_COMMITTED
        again = _graph(context.gcs_address, control.RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER, release_request, deadline)
        assert again.receipt == released.receipt
        assert _recovery(context.gcs_address, membership.publication_id, deadline) == collected
        _close_local(source, deadline)
        _wait(core, lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not getattr(core, "_output_retirement_work", {})
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        try:
            if context is not None:
                for manifest in manifests:
                    try:
                        _abort_metadata(context.gcs_address, manifest, cleanup_deadline)
                    except Exception as exc:
                        cleanup_errors.append("metadata cleanup: {}: {}".format(type(exc).__name__, exc))
            for reference in (restored, outer, source):
                try:
                    _close_local(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append("reference cleanup: {}: {}".format(type(exc).__name__, exc))
        finally:
            report = ray.shutdown()
        assert not cleanup_errors, cleanup_errors
    assert context is not None and report is not None
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean and report.resources_clean
    assert report.finalized and report.shutdown_ack_clean and not report.forced
    assert not ray.is_initialized()
    assert all(not _pid_exists(pid) for pid in pids)
    assert all(child.pid not in pids for child in mp.active_children())
    for address in addresses:
        try:
            with socket.create_connection(address, timeout=0.1):
                pytest.fail("managed endpoint still accepts connections after shutdown")
        except OSError:
            pass
