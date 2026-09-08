"""One bounded real late-DROP secondary-replica cleanup acceptance.

Five children: GCS, two Nodes, one Worker per Node; two 1 MiB stores. One
Driver-owned child, one mixed two-return producer with 8 KiB padding, and one
consumer that must never reach PushTask. The only failure is an exact managed
publisher-Node crash. No reply, grant, location or cleanup proof is fabricated.

Two existing dispatch lanes hold semantic gates, each at most eight seconds,
inside a shared 18-second work deadline. First hold the real producer Adopted
ACK; then hold the consumer after its real target grant but before recording
the sealed secondary. After the crash, release the producer lane so Node-loss
can lock DROP while the consumer lane remains gated. Release the consumer only
after that choice is observed. Cleanup uses the runtime's existing mailbox.

No test-owned thread/listener, extra task, forced state reset or reconstruction.
Run one exact reviewed node ID through the external 30-second runner only.
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
import signal
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.control import GET_NODES_HANDLER
from miniray.errors import SystemTaskError
from miniray.node import (
    CANCEL_LEASE_HANDLER, DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER,
    GET_WORKER_LEASE_OUTCOME_HANDLER, REQUEST_LEASE_HANDLER,
)
from miniray.output_recovery import OutputRecoveryOwnerDecision
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from miniray.transport import request as rpc_request
from tests.integration.test_multi_contained_output_path import _close_local, _pid_exists


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_RESOURCE = "late_output_source"
_TARGET_RESOURCE = "late_output_target"
_PADDING = b"L" * (8 * 1024)
_CHILD_VALUE = ("late-replica-live-driver-child", 42)
_GATE_SECONDS = 8.0
_WORK_SECONDS = 18.0


@ray.remote(num_returns=2, resources={_SOURCE_RESOURCE: 1}, max_retries=1)
def _produce_mixed_contained(container):
    child = container[0]
    return (
        {"child": child, "slot": 0, "producer_pid": os.getpid()},
        {"child": child, "slot": 1, "producer_pid": os.getpid(), "padding": _PADDING},
    )


@ray.remote(resources={_TARGET_RESOURCE: 1}, max_retries=0)
def _consumer_must_not_execute(_value):
    raise AssertionError("late-location consumer must never execute")


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "late-replica acceptance exceeded its shared deadline"
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


def test_late_sealed_secondary_is_rejected_then_cleaned_after_real_consumer_cancellation():
    context = runtime = core = target = source = child = consumer = None
    original_rpc = original_push = original_record = death = report = None
    publication = expected_drop = None
    refs = ()
    restored = []
    pids, addresses = set(), set()
    adopted = threading.Event()
    release_adoption = threading.Event()
    localized = threading.Event()
    release_late_report = threading.Event()
    drop_latched = threading.Event()
    cleanup_admitted = threading.Event()
    gate_expired = threading.Event()
    observation_overflow = threading.Event()
    observation_lock = threading.Lock()
    held_ack, held_grant, calls, cleanup_observations = [], [], [], []
    push_counts = {}
    observed_handlers = {
        REQUEST_LEASE_HANDLER, CANCEL_LEASE_HANDLER, DROP_OBJECT_REPLICA_HANDLER,
        wire.REPORT_OUTPUT_PUBLICATION_HANDLER, wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER,
        wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER,
    }
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=(
                {"CPU": 1, _TARGET_RESOURCE: 1},
                {"CPU": 1, _SOURCE_RESOURCE: 1},
            ),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        target, source = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
        assert core._dispatch_lane_count == len(core._dispatchers) == 2
        assert all(lane.is_alive() for lane in core._dispatchers)
        assert core.node_id == target.node_id and runtime.owner_service is not None
        pids.update((context.gcs_pid, target.node_pid, target.worker_pid, source.node_pid, source.worker_pid))
        addresses.update((context.gcs_address, runtime.owner_service.address, target.node_address,
                          target.worker_address, source.node_address, source.worker_address))
        assert len(pids) == 5 and len(addresses) == 6 and context.trace_address is None
        assert os.getpid() not in pids
        child = ray.put(_CHILD_VALUE)
        original_rpc, original_push = core._rpc, core._push_task_rpc
        original_record = core._build_location_reports

        def hold(event, gate_deadline):
            if not event.wait(max(0.0, gate_deadline - time.monotonic())):
                # Return the actual outcome on timeout, but independently fail
                # the acceptance. An expired gate is not a fake transport loss.
                gate_expired.set()

        def observe_rpc(address, handler, request):
            if handler == CANCEL_LEASE_HANDLER and held_grant:
                with core._state_lock:
                    pending = core._late_replica_cleanup.pending()
                    assert expected_drop in pending
                    assert target.node_id not in core.owner_table.snapshot(refs[1].object_id).locations
                with observation_lock:
                    cleanup_observations.append(("before-cancel", pending))
                cleanup_admitted.set()
            reply = original_rpc(address, handler, request)
            gate_deadline = None
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
                    assert type(reply) is wire.OutputRecoveryReply and reply.accepted
                    assert reply.request == request and reply.ack.snapshot.adopted == request.proof
                    gate_deadline = min(deadline, time.monotonic() + _GATE_SECONDS)
                    held_ack.append((request, reply, gate_deadline))
                if (handler in (wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER, wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER)
                        and type(reply) is wire.OutputNodeLossReply
                        and publication is not None and request.work.publication_id == publication):
                    decision = reply.snapshot.owner_decision
                    if decision is not None:
                        assert decision.slots[1].decision is OutputRecoveryOwnerDecision.DROP
                        drop_latched.set()
            if gate_deadline is not None:
                adopted.set()
                hold(release_adoption, gate_deadline)
            return reply

        def delay_location_record(requested, grant, foreign_guards=()):
            should_hold = (publication is not None and len(grant.dependencies) == 1
                           and grant.dependencies[0].object_id == publication.output_ids[1])
            if should_hold:
                assert grant.node_id == target.node_id and not foreign_guards
                with observation_lock:
                    assert not held_grant, "consumer location reporting ran a second time"
                    gate_deadline = min(deadline, time.monotonic() + _GATE_SECONDS)
                    held_grant.append((requested, grant, gate_deadline))
                localized.set()
                hold(release_late_report, gate_deadline)
            result = original_record(requested, grant, foreign_guards)
            if should_hold:
                with core._state_lock:
                    assert target.node_id not in core.owner_table.snapshot(refs[1].object_id).locations
                    # Building the complete inventory performs no local
                    # mutation. Cleanup is observed when the driver cancels.
            return result

        def observe_push(address, handler, request):
            with observation_lock:
                task = request.spec.task_id
                push_counts[task] = push_counts.get(task, 0) + 1
                if len(push_counts) > 2 or sum(push_counts.values()) > 8:
                    observation_overflow.set()
            return original_push(address, handler, request)

        core._rpc, core._push_task_rpc = observe_rpc, observe_push
        core._build_location_reports = delay_location_record
        refs = _produce_mixed_contained.remote([child])
        assert len(refs) == 2 and adopted.wait(min(_GATE_SECONDS, _remaining(deadline)))
        with observation_lock:
            (adopted_request, adopted_reply, adoption_deadline), = held_ack
        publication = adopted_request.proof.complete.publication_id
        manifest = adopted_reply.ack.snapshot.manifest
        assert publication.output_ids == tuple(ref.object_id for ref in refs)
        assert manifest.header.node_incarnation.node_id == source.node_id
        assert tuple(slot.tier for slot in manifest.slots) == (
            protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
        )
        with core._completion:
            assert all(ref.object_id in core._task_finish_barriers for ref in refs)
            inline_before = core.owner_table.snapshot(refs[0].object_id)
            canonical = core.owner_table.snapshot(refs[1].object_id).canonical_stored_result
            assert canonical is not None and canonical.node_id == source.node_id

        consumer = _consumer_must_not_execute.remote(refs[1])
        assert localized.wait(min(_GATE_SECONDS, _remaining(adoption_deadline)))
        with observation_lock:
            (requested, grant, report_deadline), = held_grant
        gate_deadline = min(deadline, adoption_deadline, report_deadline)
        assert grant.task_id == consumer.object_id.task_id and grant.worker_id == target.worker_id
        (replica,) = grant.dependencies
        assert replica == protocol.ObjectStoreDescriptor(
            refs[1].object_id, core.worker_id, publication.attempt_id, target.node_id,
            canonical.size_bytes, canonical.checksum,
        )
        assert len(requested) == 1 and requested[0].node_id == source.node_id
        expected_drop = protocol.DropObjectReplica(
            replica.object_id, replica.producer_attempt_id, replica.owner_worker_id, replica.node_id, replica.checksum,
        )
        with core._completion:
            assert core.owner_table.snapshot(refs[1].object_id).locations == frozenset((source.node_id,))
            assert core._objects[consumer.object_id].event.is_set() is False
        # Inspect exact live bytes without unpickling the nested child or
        # registering a location. The real grant owns the target dependency pin.
        ready = _query(target.node_address, GET_OBJECT_HANDLER, protocol.GetObject(
            replica.object_id, target.node_id, replica.producer_attempt_id, replica.owner_worker_id,
            replica.size_bytes, replica.checksum,
        ), gate_deadline)
        assert type(ready) is protocol.GetObjectReply and ready.found and ready.sealed
        assert ready.node_id == target.node_id and ready.producer_attempt_id == publication.attempt_id
        assert ready.size_bytes == len(ready.data) == canonical.size_bytes
        assert ready.checksum == hashlib.sha256(ready.data).hexdigest() == canonical.checksum
        assert not gate_expired.is_set()

        death = _test_crash_node(source.node_id, timeout=_remaining(gate_deadline))
        release_adoption.set()  # Free the producer lane before awaiting DROP.
        assert drop_latched.wait(min(_GATE_SECONDS, _remaining(report_deadline)))
        release_late_report.set()
        assert not gate_expired.is_set()
        assert death.node_pid == source.node_pid and death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert death.registration_epoch == runtime.nodes[1].registration_epoch
        assert cleanup_admitted.wait(_remaining(deadline))
        with pytest.raises(SystemTaskError, match="retired.*replica"):
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: all(ref.object_id not in core._task_finish_barriers for ref in (*refs, consumer))
              and not core._late_replica_cleanup.has_pending(), deadline)

        with core._completion:
            inline, lost = tuple(core.owner_table.snapshot(ref.object_id) for ref in refs)
            assert inline.state is ObjectState.READY_INLINE and inline.inline_data == inline_before.inline_data
            assert inline.output_publication.manifest == manifest
            assert lost.state is ObjectState.LOST and lost.current_attempt == publication.attempt_id
            assert lost.output_publication is None and lost.canonical_stored_result is None and not lost.locations
            assert refs[1].object_id not in core._stored_descriptors
            assert core.owner_table.snapshot(consumer.object_id).state is ObjectState.ERROR
            producer_record = core._recovery.task_record(publication.task_id)
            assert producer_record.state is TaskState.SUCCEEDED and producer_record.retries_started == 0
            assert producer_record.current_attempt == publication.attempt_id
            assert core._recovery.active_recovery(publication.task_id) is None
            (cleanup,) = core._late_replica_cleanup.snapshot()
            assert cleanup.request == expected_drop and not cleanup.in_flight
        proof = cleanup.proof
        assert type(proof) is protocol.DropObjectReplicaReply and proof.accepted
        assert proof.status in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
        assert protocol.DropObjectReplica(proof.object_id, proof.producer_attempt_id, proof.owner_worker_id,
                                          proof.node_id, proof.checksum) == expected_drop
        with observation_lock:
            assert push_counts == {publication.task_id: 1}
            assert {phase for phase, _ in cleanup_observations} == {"before-cancel"}
            assert all(pending == (expected_drop,) for _, pending in cleanup_observations)
            cancellations = tuple((request, reply) for address, handler, request, reply in calls
                                  if handler == CANCEL_LEASE_HANDLER and address == target.node_address)
            drops = tuple((request, reply) for address, handler, request, reply in calls
                          if handler == DROP_OBJECT_REPLICA_HANDLER and address == target.node_address)
            producer_requests = tuple(request for _, handler, request, _ in calls
                                      if handler == REQUEST_LEASE_HANDLER and request.task_id == publication.task_id)
        assert cancellations and any(reply.released for _, reply in cancellations)
        assert all(request.lease_id == grant.lease_id and request.attempt_id == grant.attempt_id
                   and reply.accepted and reply.cancelled and reply.state is protocol.LeaseExecutionState.ABANDONED
                   for request, reply in cancellations)
        assert drops and all(request == expected_drop for request, _ in drops)
        assert any(reply == proof for _, reply in drops)
        assert producer_requests and all(request.attempt_id == publication.attempt_id for request in producer_requests)
        absent = _query(target.node_address, GET_OBJECT_HANDLER,
                        protocol.GetObject(refs[1].object_id, target.node_id), deadline)
        assert type(absent) is protocol.GetObjectReply and not absent.found and not absent.sealed and absent.data is None
        outcome = _query(target.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, protocol.GetWorkerLeaseOutcome(
            grant.lease_id, grant.task_id, grant.attempt_id, grant.worker_id, core.worker_id, (consumer.object_id,),
        ), deadline)
        assert type(outcome) is protocol.GetWorkerLeaseOutcomeReply and outcome.found and outcome.worker_alive
        assert outcome.state is protocol.LeaseExecutionState.ABANDONED and not outcome.cleanup_pending
        nodes = _query(context.gcs_address, GET_NODES_HANDLER, protocol.GetNodes(), deadline)
        assert type(nodes) is protocol.GetNodesReply and len(nodes.nodes) == 1
        (live,) = nodes.nodes
        assert live.node_id == target.node_id
        assert live.available_resources == live.total_resources == ResourceVector({"CPU": 1, _TARGET_RESOURCE: 1})

        request = wire.GetOutputPublicationRecovery(publication)
        recovery = _query(context.gcs_address, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER, request, deadline)
        assert type(recovery) is wire.GetOutputPublicationRecoveryReply and recovery.request == request and recovery.found
        resolved = recovery.snapshot
        assert resolved.frozen_node_death == death and resolved.owner_death is None
        assert resolved.resolution.kept_slots == (0,) and resolved.resolution.complete == adopted_request.proof.complete
        assert tuple(slot.decision for slot in resolved.owner_decision.slots) == (
            OutputRecoveryOwnerDecision.KEEP, OutputRecoveryOwnerDecision.DROP,
        )
        assert core.owner_table.snapshot(child.object_id).contained_holds == frozenset((manifest.slots[0].transfers[0].final_hold,))
        first = ray.get(refs[0], timeout=_remaining(deadline))
        restored.append(first["child"])
        assert first["producer_pid"] == source.worker_pid and first["slot"] == 0
        assert ray.get(first["child"], timeout=_remaining(deadline)) == _CHILD_VALUE
        for reference in restored:
            _close_local(reference, deadline)
        # The consumer's lineage retains its input even on error; collect its
        # output first, then the two producer slots and finally the child.
        for reference in (consumer, refs[0], refs[1]):
            _close_local(reference, deadline)
            _wait(core, lambda reference=reference: core.owner_table.collection_state(reference.object_id)
                  is ObjectCollectionState.COLLECTED, deadline)
        assert not core.owner_table.snapshot(child.object_id).contained_holds
        _close_local(child, deadline)
        _wait(core, lambda: core.owner_table.collection_state(child.object_id) is ObjectCollectionState.COLLECTED, deadline)
        for object_id in (consumer.object_id, refs[0].object_id, refs[1].object_id, child.object_id):
            assert not core.owner_table.contains(object_id) and object_id not in core._objects
            assert object_id not in core._stored_descriptors and object_id not in core._object_gc_obligations
            assert core._recovery.lineage_for_object(object_id) is None
        assert not core._late_replica_cleanup.has_pending() and not core._protocol_unresolved
        assert not gate_expired.is_set() and not observation_overflow.is_set()
    finally:
        release_adoption.set()
        release_late_report.set()
        if core is not None and original_rpc is not None:
            core._rpc, core._push_task_rpc = original_rpc, original_push
            core._build_location_reports = original_record
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
    assert report.node_forced == report.worker_forced == (False, False)
    assert report.worker_pids == (target.worker_pid, source.worker_pid)
    assert report.worker_cleans == (True, False) and report.node_deaths[1] == death
    assert not ray.is_initialized() and all(not _pid_exists(pid) for pid in pids)
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            socket.create_connection(address, timeout=0.1)
