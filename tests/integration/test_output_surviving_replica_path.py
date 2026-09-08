"""Bounded publishing-Node loss while an adopted STORED replica survives.

Five children: one GCS, two Nodes and one Worker per Node. One Driver put, one
remote two-return producer and one home-Node consumer; 1 MiB per store and 8 KiB
padding. Both producer slots contain the same live Driver-owned child.

The first real producer Adopted ACK is held for at most eight seconds, after
owner CAS. The other existing dispatch lane consumes the STORED slot through
normal dependency pin/pull/seal/grant/location reporting. Only then does the
Driver crash the exact publisher and release the ACK. No fake response, state
reset, test-owned thread, listener, extra task or producer retry is used.
Run only this exact node ID through the external 30-second bounded runner.
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
import signal
import socket
import threading
import time
from dataclasses import replace

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedGraphTransactionState,
)
from miniray.control import (
    GET_CONTAINED_GRAPH_HANDLER, RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER,
)
from miniray.node import GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.output_recovery import OutputRecoveryOwnerDecision
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState
from miniray.transport import request as rpc_request
from tests.integration.test_multi_contained_output_path import _close_local, _pid_exists


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_RESOURCE = "output_survivor_source"
_TARGET_RESOURCE = "output_survivor_target"
_PADDING = b"S" * (8 * 1024)
_CHILD_VALUE = ("living-driver-child", 42)
_GATE_SECONDS = 8.0
_WORK_SECONDS = 18.0


@ray.remote(num_returns=2, resources={_SOURCE_RESOURCE: 1}, max_retries=0)
def _produce_mixed_contained(container):
    child = container[0]
    return (
        {"child": child, "slot": 0, "producer_pid": os.getpid()},
        {"child": child, "slot": 1, "producer_pid": os.getpid(),
         "padding": _PADDING},
    )


@ray.remote(resources={_TARGET_RESOURCE: 1}, max_retries=0)
def _consume_stored_contained(value):
    # A top-level RefArg forces real target localization before user code. The
    # nested child must also be a live imported handle, not a detached ID.
    return (
        os.getpid(), value["producer_pid"], value["slot"],
        len(value["padding"]), hashlib.sha256(value["padding"]).hexdigest(),
        ray.get(value["child"], timeout=3.0), value["child"].object_id,
    )


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "surviving-output acceptance exceeded its deadline"
    return remaining


def _wait(core, predicate, deadline):
    with core._completion:
        while not predicate():
            core._completion.wait(_remaining(deadline))


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining / 2),
        request_timeout=min(2.0, remaining / 2), deadline=deadline,
    )


def _recovery(context, publication, deadline):
    request = wire.GetOutputPublicationRecovery(publication)
    reply = _query(
        context.gcs_address, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER,
        request, deadline,
    )
    assert type(reply) is wire.GetOutputPublicationRecoveryReply
    assert reply.request == request and reply.found and reply.snapshot is not None
    return reply.snapshot


def test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss():
    context = runtime = core = target = source = child = consumer = None
    original_rpc = original_push = death = report = None
    refs = ()
    restored = []
    pids, addresses = set(), set()
    adopted = threading.Event()
    release_adoption = threading.Event()
    gate_expired = threading.Event()
    observation_overflow = threading.Event()
    observation_lock = threading.Lock()
    held_ack, calls = [], []
    first_pushes, push_counts = {}, {}
    observed_handlers = {
        REQUEST_LEASE_HANDLER, GET_OBJECT_HANDLER, "drop_object_replica",
        wire.REPORT_OUTPUT_PUBLICATION_HANDLER,
        RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER,
    }
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=(
                {"CPU": 1, _TARGET_RESOURCE: 1},
                {"CPU": 1, _SOURCE_RESOURCE: 1},
            ),
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        target, source = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
        assert core._dispatch_lane_count == len(core._dispatchers) == 2
        assert all(lane.is_alive() for lane in core._dispatchers)
        assert core.node_id == target.node_id and runtime.owner_service is not None
        pids.update((context.gcs_pid, target.node_pid, target.worker_pid,
                     source.node_pid, source.worker_pid))
        addresses.update((context.gcs_address, runtime.owner_service.address,
                          target.node_address, target.worker_address,
                          source.node_address, source.worker_address))
        assert len(pids) == 5 and len(addresses) == 6
        assert context.trace_address is None and os.getpid() not in pids
        child = ray.put(_CHILD_VALUE)
        original_rpc, original_push = core._rpc, core._push_task_rpc

        def observe_rpc(address, handler, request):
            reply = original_rpc(address, handler, request)
            should_hold = False
            with observation_lock:
                if handler in observed_handlers:
                    if len(calls) < 96:
                        calls.append((address, handler, request, reply))
                    else:
                        observation_overflow.set()
                if (handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
                        and type(request) is wire.ReportOutputPublicationAdopted
                        and len(request.proof.complete.publication_id.output_ids) == 2
                        and not held_ack):
                    # The only two-return task is the producer, even if its
                    # dispatch races remote() returning handles to the Driver.
                    assert type(reply) is wire.OutputRecoveryReply and reply.accepted
                    assert reply.request == request
                    assert reply.ack.snapshot.adopted == request.proof
                    held_ack.append((request, reply, time.monotonic() + _GATE_SECONDS))
                    should_hold = True
            if should_hold:
                adopted.set()
                if not release_adoption.wait(_GATE_SECONDS):
                    # A synchronization timeout fails the test independently;
                    # it does not manufacture a failed or different RPC reply.
                    gate_expired.set()
            return reply

        def observe_push(address, handler, request):
            with observation_lock:
                task_id = request.spec.task_id
                first_pushes.setdefault(task_id, request)
                push_counts[task_id] = push_counts.get(task_id, 0) + 1
            return original_push(address, handler, request)

        core._rpc, core._push_task_rpc = observe_rpc, observe_push
        refs = _produce_mixed_contained.remote([child])
        assert len(refs) == 2
        assert adopted.wait(min(_GATE_SECONDS, _remaining(deadline)))
        with observation_lock:
            (adopted_request, adopted_reply, gate_deadline), = held_ack
        gate_deadline = min(gate_deadline, deadline)
        publication = adopted_request.proof.complete.publication_id
        assert publication.output_ids == tuple(ref.object_id for ref in refs)
        assert publication.attempt_id.attempt_number == 0
        manifest = adopted_reply.ack.snapshot.manifest
        assert manifest.header.node_incarnation.node_id == source.node_id
        assert manifest.header.executor_worker_id == source.worker_id
        assert tuple(slot.tier for slot in manifest.slots) == (
            protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
        )
        with core._completion:
            assert all(ref.object_id in core._task_finish_barriers for ref in refs)
            assert publication in core._output_result_custody
            assert core.owner_table.snapshot(refs[1].object_id).locations == frozenset({source.node_id})

        consumer = _consume_stored_contained.remote(refs[1])
        assert ray.get(consumer, timeout=_remaining(gate_deadline)) == (
            target.worker_pid, source.worker_pid, 1, len(_PADDING),
            hashlib.sha256(_PADDING).hexdigest(), _CHILD_VALUE, child.object_id,
        )
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, gate_deadline)
        before = tuple(core.owner_table.snapshot(ref.object_id) for ref in refs)
        canonical = before[1].canonical_stored_result
        assert before[0].state is ObjectState.READY_INLINE
        assert before[1].state is ObjectState.READY_STORED
        assert before[1].locations == frozenset({source.node_id, target.node_id})
        assert canonical is not None and canonical.node_id == source.node_id
        assert before[1].current_attempt == publication.attempt_id
        assert all(value.output_publication.manifest == manifest for value in before)
        with observation_lock:
            grants = tuple(reply for _, handler, request, reply in calls
                           if handler == REQUEST_LEASE_HANDLER
                           and request.task_id == consumer.object_id.task_id
                           and type(reply) is protocol.GrantWorkerLease)
        (grant,) = grants
        assert grant.node_id == target.node_id and len(grant.dependencies) == 1
        (localized,) = grant.dependencies
        assert localized == protocol.ObjectStoreDescriptor(
            refs[1].object_id, core.worker_id, publication.attempt_id,
            target.node_id, canonical.size_bytes, canonical.checksum,
        )
        with core._completion:
            assert all(ref.object_id in core._task_finish_barriers for ref in refs)
        assert not gate_expired.is_set()
        death = _test_crash_node(source.node_id, timeout=_remaining(gate_deadline))
        release_adoption.set()
        assert not gate_expired.is_set()
        assert death.node_pid == source.node_pid and death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert death.registration_epoch == runtime.nodes[1].registration_epoch
        _wait(core, lambda: all(ref.object_id not in core._task_finish_barriers for ref in refs), deadline)

        resolved = _recovery(context, publication, deadline)
        assert resolved.frozen_node_death == death
        assert resolved.complete == resolved.resolution.complete == adopted_request.proof.complete
        assert resolved.adopted == adopted_request.proof
        assert resolved.resolution.kept_slots == (0, 1)
        assert tuple(slot.decision for slot in resolved.owner_decision.slots) == (
            OutputRecoveryOwnerDecision.KEEP, OutputRecoveryOwnerDecision.KEEP,
        )
        assert resolved.owner_death is None and resolved.rollback is None
        after = tuple(core.owner_table.snapshot(ref.object_id) for ref in refs)
        assert after[0].state is ObjectState.READY_INLINE and after[0].inline_data == before[0].inline_data
        assert after[1].state is ObjectState.READY_STORED
        assert after[1].locations == frozenset({target.node_id})
        assert after[1].canonical_stored_result == canonical
        assert core._stored_descriptors[refs[1].object_id] == replace(canonical, node_id=target.node_id)
        assert all(value.current_attempt == publication.attempt_id
                   and value.output_publication.manifest == manifest for value in after)
        record = core._recovery.task_record(publication.task_id)
        assert record.state is TaskState.SUCCEEDED and record.current_attempt == publication.attempt_id
        assert record.retries_started == 0 and core._recovery.active_recovery(publication.task_id) is None
        assert all(len(slot.transfers) == 1 for slot in manifest.slots)
        transfers = tuple(slot.transfers[0] for slot in manifest.slots)
        assert transfers[0].contained_object_id == transfers[1].contained_object_id == child.object_id
        assert transfers[0].final_hold != transfers[1].final_hold
        assert all(isinstance(item.source, BorrowedContainedSource)
                   and isinstance(item.source.original_source, protocol.TaskHoldSource) for item in transfers)
        assert core.owner_table.snapshot(child.object_id).contained_holds == frozenset(
            transfer.final_hold for transfer in transfers
        )
        graph_request = protocol.GetContainedGraph(publication.graph_transaction_id)
        graph_reply = _query(context.gcs_address, GET_CONTAINED_GRAPH_HANDLER, graph_request, deadline)
        assert type(graph_reply) is protocol.GetContainedGraphReply and graph_reply.request == graph_request
        assert graph_reply.disposition is protocol.ContainedGraphQueryDisposition.FOUND
        assert graph_reply.manifest == manifest.to_graph_manifest()

        with observation_lock:
            fetch_start = len(calls)
        first = ray.get(refs[0], timeout=_remaining(deadline))
        restored.append(first["child"])
        second = ray.get(refs[1], timeout=_remaining(deadline))
        restored.append(second["child"])
        assert first["producer_pid"] == second["producer_pid"] == source.worker_pid
        assert (first["slot"], second["slot"]) == (0, 1) and second["padding"] == _PADDING
        for reference in restored:
            assert reference.object_id == child.object_id and reference.borrower_token is None
            assert ray.get(reference, timeout=_remaining(deadline)) == _CHILD_VALUE
        with observation_lock:
            fetches = tuple((address, request, reply) for address, handler, request, reply in calls[fetch_start:]
                            if handler == GET_OBJECT_HANDLER and request.object_id == refs[1].object_id)
            producer_push = first_pushes[publication.task_id]
            assert push_counts == {publication.task_id: 1, consumer.object_id.task_id: 1}
        (address, request, reply), = fetches
        assert address == target.node_address and request.expected_attempt_id == publication.attempt_id
        assert request.expected_checksum == canonical.checksum
        assert type(reply) is protocol.GetObjectReply and reply.found and reply.sealed
        assert reply.node_id == target.node_id and reply.producer_attempt_id == publication.attempt_id
        assert reply.owner_worker_id == core.worker_id and reply.size_bytes == canonical.size_bytes
        assert reply.checksum == hashlib.sha256(reply.data).hexdigest() == canonical.checksum
        assert producer_push.lease_id == publication.lease_id
        assert producer_push.spec.attempt_id == publication.attempt_id

        for reference in restored:
            _close_local(reference, deadline)
        # The consumer's lineage retains its input until its own result dies.
        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_local(refs[0], deadline)
        _wait(core, lambda: core.owner_table.collection_state(refs[0].object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert core.owner_table.snapshot(refs[1].object_id).state is ObjectState.READY_STORED
        assert core.owner_table.snapshot(child.object_id).contained_holds == frozenset({transfers[1].final_hold})
        _close_local(refs[1], deadline)
        _wait(core, lambda: core.owner_table.collection_state(refs[1].object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not core.owner_table.snapshot(child.object_id).contained_holds
        _close_local(child, deadline)
        _wait(core, lambda: core.owner_table.collection_state(child.object_id) is ObjectCollectionState.COLLECTED, deadline)
        for object_id in (consumer.object_id, refs[0].object_id, refs[1].object_id, child.object_id):
            assert not core.owner_table.contains(object_id)
            assert object_id not in core._objects and object_id not in core._stored_descriptors
            assert object_id not in core._object_gc_obligations
            assert core._recovery.lineage_for_object(object_id) is None
        retired = _recovery(context, publication, deadline)
        assert tuple(proof.slot_index for proof in retired.slot_collections) == (0, 1)
        assert retired.resolution == resolved.resolution
        with observation_lock:
            releases = tuple((request, reply) for _, handler, request, reply in calls
                             if handler == RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER
                             and request.manifest.publication_id == publication)
        assert {request.container_object_id for request, _ in releases} == set(publication.output_ids)
        for request, reply in releases:
            assert type(reply) is protocol.ContainedGraphReply and reply.request == request and reply.accepted
            assert reply.receipt.state is ContainedGraphTransactionState.COMMITTED
            assert reply.receipt.disposition in (
                ContainedGraphManifestDisposition.RELEASED, ContainedGraphManifestDisposition.ALREADY_RELEASED,
            )
            assert reply.receipt.released_edges == manifest.slots[request.container_object_id.return_index].edges
        absent = _query(
            target.node_address, GET_OBJECT_HANDLER,
            protocol.GetObject(refs[1].object_id, target.node_id), deadline,
        )
        assert type(absent) is protocol.GetObjectReply
        assert not absent.found and not absent.sealed and absent.data is None
        assert publication not in core._output_result_custody
        assert not core._protocol_unresolved and not getattr(core, "_output_retirement_work", {})
        assert not observation_overflow.is_set() and not gate_expired.is_set()
    finally:
        release_adoption.set()
        if core is not None and original_rpc is not None:
            core._rpc = original_rpc
            core._push_task_rpc = original_push
        cleanup_deadline = time.monotonic() + 3.0
        try:
            for reference in (*restored, consumer, *refs, child):
                _close_local(reference, cleanup_deadline)
        finally:
            report = ray.shutdown()

    assert context is not None and report is not None and death is not None
    assert report.core_stopped and report.gcs_clean and not report.forced
    assert report.node_pids == (target.node_pid, source.node_pid)
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == report.node_finalized == (True, False)
    assert report.node_forced == (False, False) and report.worker_forced == (False, False)
    assert report.worker_pids == (target.worker_pid, source.worker_pid)
    assert report.worker_cleans == (True, False) and report.node_deaths[1] == death
    assert not ray.is_initialized() and all(not _pid_exists(pid) for pid in pids)
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            socket.create_connection(address, timeout=0.1)
