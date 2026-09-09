"""Real base contained-output lifetime retained from the graph control smoke.

One GCS, one Node, one Worker, one tiny Driver put and one ordinary task. The
task returns a nested borrowed reference to the Driver child; actual owner
handoff adoption and container collection release its exact final hold. The
source handle stays readable until its own close. No phantom manifests or
global graph RPCs are sent. Global cycle rejection belongs to E and is mapped
to current actual enhanced cycle paths in the K3 audit note.

Three children/four endpoints, one 1MiB store, no trace, Actor, test thread or
listener. Gets and <=128 condition polls share 12 seconds after init; cleanup
shares one 3-second epoch. Exact 30-second process-tree runner remains required.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray import control, output_protocol as wire, protocol
from miniray.api import _get_runtime
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_sources import BorrowedContainedSource
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke


@ray.remote(max_retries=0)
def _return_live_driver_child(container):
    return {"child": container[0]}


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("contained owner smoke deadline expired")
    return remaining


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining),
                       request_timeout=min(1.0, remaining), deadline=deadline)


def _wait(core, predicate, deadline):
    with core._completion:
        for _ in range(128):
            if predicate():
                return
            core._completion.wait(min(0.05, _remaining(deadline)))
    raise TimeoutError("contained owner state did not converge")


def _close(reference, deadline):
    if reference is None:
        return
    done = reference._release_done
    assert done is not None and reference._finalizer is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _handoff(address, identity, deadline):
    request = wire.GetOutputHandoff(identity)
    reply = _rpc(address, wire.GET_OUTPUT_HANDOFF_HANDLER, request, deadline)
    assert type(reply) is wire.OutputHandoffReply and reply.request == request and reply.accepted
    assert reply.snapshot is not None
    return reply.snapshot


def test_real_borrowed_driver_child_survives_container_gc_and_exact_handoff_replay():
    context = report = core = source = outer = restored = None
    pids, addresses = set(), set()
    cleanup_errors = []
    cleanup_deadline = None
    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False)
        deadline = time.monotonic() + 12.0
        runtime = _get_runtime()
        core, node = runtime.core_worker, context.nodes[0]
        pids.update((context.gcs_pid, node.node_pid, node.worker_pid))
        addresses.update((context.gcs_address, node.node_address, node.worker_address, runtime.owner_service.address))
        assert len(pids) == 3 and len(addresses) == 4 and os.getpid() not in pids
        assert context.trace_address is None
        registered = _rpc(context.gcs_address, control.GET_NODE_STATE_HANDLER, protocol.GetNodeState(node.node_id), deadline)
        worker = _rpc(context.gcs_address, control.GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(node.worker_id), deadline)
        assert type(registered) is protocol.GetNodeStateReply and registered.found
        assert registered.node_id == node.node_id and registered.node_pid == node.node_pid
        assert registered.state is protocol.NodeMembershipState.ALIVE and registered.death is None
        assert type(worker) is protocol.GetWorkerStateReply and worker.found and worker.death is None
        assert worker.worker_id == node.worker_id and worker.state is protocol.WorkerMembershipState.ALIVE
        assert worker.incarnation == protocol.WorkerIncarnation(
            node.node_id, node.node_pid, registered.registration_epoch, node.worker_id, node.worker_pid,
        )
        source = ray.put(("cycle-control-live-child", 42))
        outer = _return_live_driver_child.remote([source])
        restored = ray.get(outer, timeout=_remaining(deadline))["child"]
        assert isinstance(restored, ray.ObjectRef) and restored.object_id == source.object_id
        assert restored.owner_worker_id == core.worker_id
        assert ray.get(restored, timeout=_remaining(deadline)) == ("cycle-control-live-child", 42)
        _wait(core, lambda: outer.object_id not in core._task_finish_barriers, deadline)
        snapshot = core.owner_table.snapshot(outer.object_id)
        membership = snapshot.output_publication
        assert snapshot.state is ObjectState.READY_INLINE and membership is not None
        assert len(membership.slot.transfers) == 1
        transfer = membership.slot.transfers[0]
        assert isinstance(transfer.source, BorrowedContainedSource)
        assert transfer.contained_object_id == source.object_id and transfer.contained_owner_worker_id == core.worker_id
        child_before = core.owner_table.snapshot(source.object_id)
        assert transfer.final_hold in child_before.contained_holds
        assert transfer.provisional_hold not in child_before.contained_holds
        history = _handoff(runtime.owner_service.address, membership.publication_id, deadline)
        assert history.manifest == membership.manifest and history.phase is OutputHandoffPhase.ADOPTED
        assert history.complete is not None and history.adoption.complete == history.complete
        assert history.adoption.owner_worker_id == core.worker_id
        adoption = wire.AckOutputPublicationAdopted(history.adoption)
        reply = _rpc(node.node_address, wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER, adoption, deadline)
        assert type(reply) is wire.AckOutputPublicationAdoptedReply and reply.request == adoption and reply.accepted

        cleanup_deadline = min(deadline, time.monotonic() + 3.0)
        _close(restored, cleanup_deadline)
        _close(outer, cleanup_deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        child = core.owner_table.snapshot(source.object_id)
        assert transfer.provisional_hold not in child.contained_holds and transfer.final_hold not in child.contained_holds
        assert core.owner_table.contained_release_was_seen(source.object_id, transfer.final_hold)
        assert ray.get(source, timeout=_remaining(cleanup_deadline)) == ("cycle-control-live-child", 42)
        terminal = core.owner_table._output_collection_receipts[outer.object_id]
        assert terminal.publication_id == membership.publication_id
        assert terminal.collection.contained_releases == membership.slot.edges
        assert _handoff(runtime.owner_service.address, membership.publication_id, cleanup_deadline) == history
        replay = _rpc(node.node_address, wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER, adoption, cleanup_deadline)
        assert replay == reply
        assert core.owner_table.snapshot(source.object_id) == child
        _close(source, cleanup_deadline)
        _wait(core, lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        assert not getattr(core, "_output_retirement_work", {})
        assert outer.object_id not in core._objects and source.object_id not in core._objects
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + 3.0
        try:
            for reference in (restored, outer, source):
                try:
                    _close(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(str(exc))
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                assert not any(_pid_exists(pid) for pid in pids)
                assert all(child.pid not in pids for child in mp.active_children())
                for address in addresses:
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            pytest.fail("managed endpoint remains after shutdown")
                    except OSError:
                        pass
                assert not cleanup_errors, cleanup_errors
                assert not ray.is_initialized()
    assert context is not None and report is not None
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean and report.resources_clean
    assert report.finalized and report.shutdown_ack_clean and not report.forced
