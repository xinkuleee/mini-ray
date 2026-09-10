"""Bounded acceptance path for a stored outer containing an ObjectRef.

One zero-CPU parent task submits one child and returns that Worker-owned handle
beside 64 KiB of padding. The single outer return crosses the unified output
owner-led publication with one OBJECT_STORE output and contained edge. The
test proves the real owner handoff/Node adoption barrier, then closes both
public handles and proves the reverse child-pin/replica/metadata barrier.

Run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Static
bounds are one GCS, one NodeManager, two ordinary Workers, exactly two tasks,
one 1 MiB ObjectStore, no Actor, trace collector, test-owned server, or sleep,
and the runner's 30-second process-tree deadline. All work after init shares
the original ten-second monotonic deadline. Public closes, actual GC and the
original inverse-protocol probes additionally share one final three-second
epoch, reused by failure-finally rather than reset. Borrow RPCs retain their
own finite retry policy; public deadlines do not cancel distributed cleanup.
Four managed PIDs/five endpoints are checked after unconditional shutdown even
if an assertion fails. Normal resource-clean assertions still certify the
successful path. Public close keeps the exact release receipt and deadline.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
import hashlib
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime
from miniray.node import (
    GET_OBJECT_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
)
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import OutputPublicationEnvelope
from miniray.owner_service import RELEASE_CONTAINED_REFERENCE_HANDLER
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_sources import OwnedContainedSource
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_TIMEOUT_SECONDS = 10.0
_INLINE_THRESHOLD = 1024
_OUTER_PADDING_BYTES = 64 * 1024
_OBJECT_STORE_BYTES = 1024 * 1024
_OUTER_PADDING = b"X" * _OUTER_PADDING_BYTES
_CLEANUP_SECONDS = 3.0
_MAX_POLLS = 128
_MAX_METADATA_VALUES = 1024
_MAX_METADATA_DEPTH = 24


@ray.remote(num_cpus=1, max_retries=0)
def _stored_outer_child(value: int) -> tuple[str, int, int]:
    return "stored-outer-child", value + 1, os.getpid()


@ray.remote(num_cpus=0, max_retries=0)
def _return_large_outer_with_child(value: int) -> tuple[object, bytes, int]:
    child = _stored_outer_child.remote(value)
    return child, _OUTER_PADDING, os.getpid()


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("stored outer publication exceeded its work deadline")
    return remaining


def _assert_metadata_only(value):
    # Preserve complete recursive inspection while bounding malformed replies.
    # No truncation may turn a payload-containing suffix into metadata evidence.
    pending = [(value, 0)]
    inspected = 0
    while pending:
        value, depth = pending.pop()
        inspected += 1
        assert inspected <= _MAX_METADATA_VALUES and depth <= _MAX_METADATA_DEPTH
        if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
            continue
        assert not isinstance(value, (
            bytes, bytearray, memoryview, OutputPublicationEnvelope,
            protocol.ResultDescriptor, protocol.ObjectStoreDescriptor,
        ))
        if is_dataclass(value) and not isinstance(value, type):
            pending.extend((getattr(value, field.name), depth + 1) for field in fields(value))
        elif isinstance(value, (tuple, list, set, frozenset)):
            assert len(value) <= _MAX_METADATA_VALUES
            pending.extend((item, depth + 1) for item in value)
        else:
            assert value is None or isinstance(value, (str, int, float, bool, Enum))
        assert len(pending) + inspected <= _MAX_METADATA_VALUES


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining),
        request_timeout=min(2.0, remaining), deadline=deadline,
    )


def _close_current(reference, deadline):
    done = reference._release_done
    assert done is not None and reference._finalizer is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _wait_for_adoption_ack(core: object, object_id: object, deadline: float) -> bool:
    """Wait on the Core condition that the adoption reducer notifies."""

    condition = core._completion
    with condition:
        for _ in range(_MAX_POLLS):
            if (object_id.task_id not in core._protocol_unresolved
                    and object_id not in core._task_finish_barriers):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            condition.wait(min(0.05, remaining))
        return (object_id.task_id not in core._protocol_unresolved
                and object_id not in core._task_finish_barriers)


def _wait_for_borrow_release(core, deadline):
    # This case owns exactly one foreign borrowed handle. Its public close
    # receipt alone need not include the owner ACK; wait for actual drain.
    with core._completion:
        for _ in range(_MAX_POLLS):
            if not core._borrowed_release_obligations:
                return
            core._completion.wait(min(0.05, _remaining(deadline)))
        assert not core._borrowed_release_obligations


def _get_exact_replica(
    node_address: tuple[str, int],
    node_id: object,
    descriptor: protocol.ResultDescriptor,
    producer_attempt_id: object,
    deadline: float,
) -> protocol.GetObjectReply:
    reply = _rpc(
        node_address,
        GET_OBJECT_HANDLER,
        protocol.GetObject(
            object_id=descriptor.object_id,
            requester_node_id=node_id,
            expected_attempt_id=producer_attempt_id,
            expected_owner_worker_id=descriptor.owner_worker_id,
            expected_size_bytes=descriptor.size_bytes,
            expected_checksum=descriptor.checksum,
        ),
        deadline,
    )
    assert isinstance(reply, protocol.GetObjectReply)
    assert reply.object_id == descriptor.object_id
    assert reply.node_id == descriptor.node_id == node_id
    return reply


def test_stored_outer_publication_adopts_owner_handoff_and_collects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = None
    report = None
    outer = child = None
    core = None
    handles = []
    cleanup_deadline = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    collection_completed = threading.Event()
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=2,
            inline_threshold=_INLINE_THRESHOLD,
            object_store_bytes=_OBJECT_STORE_BYTES,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _TIMEOUT_SECONDS
        node = context.nodes[0]
        managed_pids.update((context.gcs_pid, node.node_pid, *node.worker_pids))
        managed_addresses.update((context.gcs_address, node.node_address, *node.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(node.worker_ids) == len(node.worker_pids) == 2
        assert len(node.worker_addresses) == 2
        assert context.trace_address is None

        assert len(managed_pids) == 4
        assert len(managed_addresses) == 5
        assert os.getpid() not in managed_pids

        outer = _return_large_outer_with_child.remote(41)
        handles.append(outer)
        received = ray.get(
            outer, timeout=_remaining(deadline)
        )
        for value in received if isinstance(received, (tuple, list)) else (received,):
            if isinstance(value, ray.ObjectRef):
                handles.append(value)
        assert isinstance(received, tuple) and len(received) == 3
        child, padding, parent_pid = received
        assert isinstance(child, ray.ObjectRef)
        assert len(handles) == 2
        assert padding == _OUTER_PADDING
        assert parent_pid in node.worker_pids
        assert child.owner_worker_id in node.worker_ids
        assert child.owner_address in node.worker_addresses
        assert child.borrower_token is not None

        child_value = ray.get(child, timeout=_remaining(deadline))
        assert child_value[:2] == ("stored-outer-child", 42)
        assert child_value[2] in node.worker_pids

        outer_id = outer.object_id
        snapshot = core.owner_table.snapshot(outer_id)
        assert snapshot.state is ObjectState.READY_STORED
        assert snapshot.inline_data is None
        assert snapshot.locations == frozenset({node.node_id})
        publication = snapshot.output_publication
        assert publication is not None
        assert core.owner_table.output_owner_publication(outer_id) == publication
        assert snapshot.output_retirement_id is None
        assert snapshot.canonical_stored_result is not None
        publication_id = publication.publication_id
        descriptor = snapshot.canonical_stored_result
        assert descriptor == core.owner_table.output_owner_result(outer_id)
        output_manifest = publication.manifest
        assert publication.slot_index == 0
        assert not hasattr(output_manifest, "to_graph_manifest")
        assert descriptor == core._stored_descriptors[outer_id]
        assert descriptor.storage is protocol.ResultStorage.OBJECT_STORE
        assert descriptor.inline_data is None
        assert _OUTER_PADDING_BYTES < descriptor.size_bytes < 128 * 1024
        assert descriptor.object_id == outer_id
        assert descriptor.owner_worker_id == core.worker_id
        assert descriptor.node_id == node.node_id
        assert publication_id.output_ids == publication_id.full_output_ids == (outer_id,)
        assert snapshot.current_attempt == publication_id.attempt_id
        assert output_manifest.header.executor_worker_id == child.owner_worker_id
        assert output_manifest.header.owner_worker_id == core.worker_id
        assert output_manifest.header.node_incarnation.node_id == node.node_id
        assert output_manifest.header.node_incarnation.node_pid == node.node_pid
        assert len(publication.slot.transfers) == 1
        transfer = publication.slot.transfers[0]
        assert isinstance(transfer.source, OwnedContainedSource)
        assert transfer.source.owner_worker_id == child.owner_worker_id

        edges = publication.slot.edges
        assert len(edges) == 1
        edge = edges[0]
        assert snapshot.outgoing_contained_edges == frozenset(edges)
        assert output_manifest.ordered_edges == edges
        assert edge.container_object_id == outer_id
        assert edge.contained_object_id == child.object_id
        assert edge.contained_owner_worker_id == child.owner_worker_id
        assert edge.contained_owner_address == child.owner_address

        # READY is published before the final Node ACK is allowed to unblock
        # the dispatch lane.  Wait on that exact reducer notification, then
        # replay the two terminal operations to prove their durable states.
        assert _wait_for_adoption_ack(core, outer_id, deadline)
        recovery_query = wire.GetOutputHandoff(publication_id)
        recovery_reply = _rpc(
            runtime.owner_service.address, wire.GET_OUTPUT_HANDOFF_HANDLER,
            recovery_query, deadline,
        )
        assert type(recovery_reply) is wire.OutputHandoffReply
        assert recovery_reply.request == recovery_query and recovery_reply.accepted
        _assert_metadata_only(recovery_reply)
        recovery = recovery_reply.snapshot
        assert recovery.manifest == output_manifest and recovery.phase is OutputHandoffPhase.ADOPTED
        assert recovery.complete is not None and recovery.adoption is not None
        assert recovery.complete.publication_id == publication_id
        assert recovery.adoption.complete == recovery.complete
        assert recovery.adoption.owner_worker_id == core.worker_id
        node_adoption_request = wire.AckOutputPublicationAdopted(recovery.adoption)
        node_adoption = _rpc(
            node.node_address,
            wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
            node_adoption_request, deadline,
        )
        assert type(node_adoption) is wire.AckOutputPublicationAdoptedReply
        assert node_adoption.request == node_adoption_request
        assert node_adoption.accepted
        outcome_request = protocol.GetWorkerLeaseOutcome(
            publication_id.lease_id, publication_id.task_id, publication_id.attempt_id,
            output_manifest.header.executor_worker_id, core.worker_id, (outer_id,),
        )
        outcome = _rpc(
            node.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline,
        )
        assert type(outcome) is protocol.GetWorkerLeaseOutcomeReply
        assert outcome.found and outcome.worker_alive
        assert (outcome.lease_id, outcome.task_id, outcome.attempt_id, outcome.executor_worker_id,
                outcome.owner_worker_id, outcome.object_ids) == (
            outcome_request.lease_id, outcome_request.task_id, outcome_request.attempt_id,
            outcome_request.executor_worker_id, outcome_request.owner_worker_id, outcome_request.object_ids,
        )
        assert outcome.state is protocol.LeaseExecutionState.COMPLETED
        assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
        assert outcome.output_publication is None and outcome.descriptors == ()
        assert outcome.output_completion == recovery.complete
        assert not outcome.cleanup_pending

        before_collection = _get_exact_replica(
            node.node_address, node.node_id, descriptor,
            publication_id.attempt_id, deadline,
        )
        assert before_collection.found and before_collection.sealed
        assert before_collection.data is not None
        assert len(before_collection.data) == descriptor.size_bytes
        assert hashlib.sha256(before_collection.data).hexdigest() == (
            descriptor.checksum
        )
        assert before_collection.producer_attempt_id == (
            publication_id.attempt_id
        )
        assert before_collection.owner_worker_id == descriptor.owner_worker_id

        original_emit = core._emit

        def observe_collection(name: str, **attributes: object) -> None:
            original_emit(name, **attributes)
            if (
                name == "object_collection_completed"
                and attributes.get("object_id") == str(outer_id)
            ):
                collection_completed.set()

        monkeypatch.setattr(core, "_emit", observe_collection)

        # Remove the independent borrower first. The owner-bound final hold
        # still keeps the child alive until collecting the outer releases it.
        # Close/GC and every original inverse replay share a single cleanup
        # epoch, additionally capped by the original whole-work deadline.
        cleanup_deadline = min(deadline, time.monotonic() + _CLEANUP_SECONDS)
        _close_current(child, cleanup_deadline)
        assert child.closed
        _wait_for_borrow_release(core, cleanup_deadline)
        assert not core._owner_is_dead(child.owner_worker_id)
        _close_current(outer, cleanup_deadline)
        assert outer.closed
        assert collection_completed.wait(_remaining(cleanup_deadline))

        assert core.owner_table.collection_state(outer_id) is (
            ObjectCollectionState.COLLECTED
        )
        assert not core.owner_table.contains(outer_id)
        assert outer_id not in core._stored_descriptors
        assert outer_id not in core._objects
        assert outer_id not in core._object_gc_obligations
        assert core._recovery.lineage_for_object(outer_id) is None

        child_hold = edge.incoming_hold(core.worker_id)
        child_release_request = protocol.ReleaseContainedReference(
            edge.contained_object_id,
            edge.contained_owner_worker_id,
            child_hold,
        )
        child_release = _rpc(
            edge.contained_owner_address,
            RELEASE_CONTAINED_REFERENCE_HANDLER,
            child_release_request, cleanup_deadline,
        )
        assert isinstance(child_release, protocol.ReleaseContainedReferenceReply)
        assert child_release.object_id == edge.contained_object_id
        assert child_release.owner_worker_id == edge.contained_owner_worker_id
        assert child_release.hold == child_hold
        assert child_release.accepted and not child_release.released

        after_collection = _get_exact_replica(
            node.node_address, node.node_id, descriptor,
            publication_id.attempt_id, cleanup_deadline,
        )
        assert not after_collection.found and not after_collection.sealed
        assert after_collection.data is None
        assert after_collection.checksum is None
        assert after_collection.producer_attempt_id is None
        assert after_collection.owner_worker_id is None
        assert after_collection.size_bytes is None
        retired_reply = _rpc(
            runtime.owner_service.address, wire.GET_OUTPUT_HANDOFF_HANDLER,
            recovery_query, cleanup_deadline,
        )
        assert type(retired_reply) is wire.OutputHandoffReply
        assert retired_reply.request == recovery_query and retired_reply.accepted
        _assert_metadata_only(retired_reply)
        assert retired_reply.snapshot == recovery
        # The collected owner keeps exact local metadata history, not a GCS
        # slot-cleanup success claim. Actual child/byte absence was proved above.
        terminal = core.owner_table._output_collection_receipts[outer_id]
        assert terminal.publication_id == publication_id
        assert terminal.manifest_digest == output_manifest.manifest_digest
        assert terminal.slot_index == 0
        assert terminal.collection.object_id == outer_id and terminal.collection.collected
        assert terminal.collection.contained_releases == edges
        _assert_metadata_only(terminal)
        late_adoption = _rpc(node.node_address, wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
                             node_adoption_request, cleanup_deadline)
        assert late_adoption == node_adoption
        assert not core._borrowed_release_obligations
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for reference in reversed(handles):
                try:
                    _close_current(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                if core is not None and core.owner_address is not None:
                    managed_addresses.add(core.owner_address)
                surviving_pids = tuple(pid for pid in managed_pids if _pid_exists(pid))
                surviving_children = tuple(
                    process.pid for process in mp.active_children() if process.pid in managed_pids
                )
                open_addresses = []
                for address in managed_addresses:
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            open_addresses.append(address)
                    except OSError:
                        pass
                assert not surviving_pids, surviving_pids
                assert not surviving_children, surviving_children
                assert not open_addresses, open_addresses
                assert not close_errors, close_errors
                assert not ray.is_initialized()

    assert not close_errors, "bounded reference cleanup failed: {!r}".format(close_errors)
    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean
    assert len(report.worker_exitcodes) == len(report.worker_cleans) == 2
    assert all(report.worker_cleans)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
    assert not any(report.worker_forced)
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
