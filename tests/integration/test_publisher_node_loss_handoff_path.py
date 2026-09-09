"""Two bounded publisher-Node loss windows with a surviving owner and child.

Each exact case uses one GCS, two Nodes, one ordinary Worker per Node (five
children), one Driver put(42), one node-1-only producer with max_retries=0,
one 8-KiB result and one 1-MiB store per Node. A single existing publication
gate stops after promotions or after exact Complete reporting but before
result delivery. The only fault is _test_crash_node on that managed Node tree.

The Driver owns both the output and its nested child. Thus child Release must
really converge at a surviving owner; child-owner death cannot discharge it.
UNKNOWN ends as a system error without retry budget. Known Complete becomes
LOST/SUCCEEDED without payload adoption; this test never gets that output and
therefore does not confuse explicit reconstruction with initial resolution.

No extra Task, thread, Actor, PG, tracing, fake RPC/death/Complete or Node-state
mutation. Read-only owner handoff and Node bytes queries bind gate metadata.
Work shares 15 seconds after init, the gate lasts at most 10 seconds, and
finally reference cleanup shares 3 seconds before unconditional shutdown.
Root reviews and runs each exact function separately under the 30-second
process-tree runner; neither function is intended for whole-file execution.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
from enum import Enum
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
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import GET_OBJECT_HANDLER
from miniray.output_handoff import NodeLostOutputResolution, OutputHandoffPhase
from miniray.output_publication import OutputPublicationEnvelope
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import (
    OutputPublicationGateConfig, OutputPublicationGatePhase,
    recv_output_publication_gate_arrival,
)
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState
from miniray.transport import request as rpc_request
from tests.integration.test_task_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke
_PUBLISHER_RESOURCE = "publisher_loss_handoff_node"
_PADDING = b"H" * (8 * 1024)
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0


@ray.remote(num_cpus=1, resources={_PUBLISHER_RESOURCE: 1}, max_retries=0)
def _produce_stored_child(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    return {"child": child, "padding": _PADDING, "publisher_pid": os.getpid()}


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("publisher Node-loss handoff exceeded work deadline")
    return remaining


def _poll(predicate, deadline, detail):
    event = threading.Event()
    while True:
        assert deadline > time.monotonic(), detail
        result = predicate()
        if result:
            return result
        event.wait(min(0.01, _remaining(deadline)))


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining),
                       request_timeout=min(2.0, remaining), deadline=deadline)


def _handoff(address, identity, deadline):
    request = wire.GetOutputHandoff(identity)
    reply = _query(address, wire.GET_OUTPUT_HANDOFF_HANDLER, request, deadline)
    assert type(reply) is wire.OutputHandoffReply and reply.request == request and reply.accepted
    assert reply.snapshot is not None and reply.snapshot.publication_id == identity
    _metadata_only(reply)
    return reply.snapshot


def _metadata_only(value):
    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, OutputPublicationEnvelope,
                                  protocol.ResultDescriptor, protocol.ObjectStoreDescriptor))
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            _metadata_only(getattr(value, item.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _metadata_only(item)
    else:
        assert value is None or isinstance(value, (str, int, float, bool, Enum))


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _run_publisher_loss(phase):
    assert phase in (OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK,
                     OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY)
    known_complete = phase is OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY
    listener = connection = context = runtime = core = source = output = report = death = None
    survivor = victim = None
    pids, addresses, close_errors = set(), set(), []
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        gate_address = listener.getsockname()
        addresses.add(gate_address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1}, {"CPU": 1, _PUBLISHER_RESOURCE: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
            _test_output_publication_gate=OutputPublicationGateConfig(1, gate_address, phase, 10.0),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        survivor, victim = context.nodes
        owner_address = runtime.owner_service.address
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, owner_address))
        for node in context.nodes:
            assert len(node.worker_ids) == 1
            addresses.update((node.node_address, node.worker_address))
        assert len(pids) == 5 and len(addresses) == 7 and os.getpid() not in pids
        assert context.trace_address is None
        source = ray.put(42)
        source_before = core.owner_table.snapshot(source.object_id)
        output = _produce_stored_child.remote([source])
        listener.settimeout(_remaining(deadline))
        connection, _ = listener.accept()
        connection.settimeout(min(10.0, _remaining(deadline)))
        arrival = recv_output_publication_gate_arrival(connection)
        assert arrival.phase is phase
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            victim.node_id, victim.node_pid, runtime.nodes[1].registration_epoch,
        )
        identity = arrival.publication_id
        assert identity.output_ids == (output.object_id,)
        assert identity.attempt_id == AttemptID(output.object_id.task_id, 0)
        before = _handoff(owner_address, identity, deadline)
        manifest = before.manifest
        assert manifest is not None and manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == core.worker_id == output.owner_worker_id
        assert manifest.header.executor_worker_id == victim.worker_id
        assert manifest.header.node_incarnation.node_id == victim.node_id
        assert before.phase is OutputHandoffPhase.PENDING and before.adoption is None
        assert (before.complete is not None) is known_complete
        slot, = manifest.slots
        transfer, = slot.transfers
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE
        assert len(_PADDING) < slot.size_bytes < 32 * 1024
        assert transfer.contained_object_id == source.object_id
        assert transfer.contained_owner_worker_id == core.worker_id
        assert transfer.contained_owner_address == owner_address
        assert isinstance(transfer.source, BorrowedContainedSource)
        assert transfer.source.borrower_worker_id == victim.worker_id
        assert type(transfer.source.original_source) is protocol.TaskHoldSource
        with core._state_lock:
            at_gate = core.owner_table.snapshot(source.object_id)
            assert transfer.final_hold in at_gate.contained_holds
            assert transfer.provisional_hold not in at_gate.contained_holds
            assert core.owner_table.contained_release_was_seen(source.object_id, transfer.provisional_hold)
            assert not core.owner_table.contained_release_was_seen(source.object_id, transfer.final_hold)
            assert core.owner_table.snapshot(output.object_id).state is ObjectState.PENDING
            assert output.object_id in core._task_finish_barriers
            assert identity not in getattr(core, "_output_result_custody", {})
        physical = _query(victim.node_address, GET_OBJECT_HANDLER,
                          protocol.GetObject(output.object_id, victim.node_id), deadline)
        assert type(physical) is protocol.GetObjectReply and physical.found and physical.sealed
        assert physical.producer_attempt_id == identity.attempt_id
        assert physical.owner_worker_id == core.worker_id
        assert physical.size_bytes == slot.size_bytes and physical.checksum == slot.checksum
        assert hashlib.sha256(physical.data).hexdigest() == slot.checksum
        # Diagnostic bytes are never supplied to Core as output custody.
        del physical
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        connection.close()
        connection = None
        listener.close()
        listener = None
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert death.node_pid == victim.node_pid and death.exit_code == -signal.SIGKILL
        assert death.registration_epoch == arrival.registration_epoch
        assert _pid_exists(survivor.node_pid) and _pid_exists(survivor.worker_pid)

        def resolved():
            with core._state_lock:
                if (identity not in getattr(core, "_output_loss_completed", set())
                        or output.object_id in core._task_finish_barriers):
                    return None
                return core.owner_table.snapshot(output.object_id)

        result = _poll(resolved, deadline, "registered output Node-loss cleanup did not finish")
        after = _handoff(owner_address, identity, deadline)
        assert after.manifest == manifest and after.complete == before.complete
        assert after.phase is OutputHandoffPhase.ABORTED and after.adoption is None
        with core._state_lock:
            receipt = core.owner_table._output_loss_receipts[identity]
            record = replace(core._recovery.task_record(identity.task_id))
            assert type(receipt) is NodeLostOutputResolution
            assert receipt.publication_id == identity and receipt.node_death == death
            assert receipt.manifest_digest == manifest.manifest_digest
            assert receipt.complete == before.complete and not receipt.keep
            assert all(type(ack) is protocol.ReleaseContainedReferenceReply for ack in receipt.cleanup)
            assert {(ack.object_id, ack.owner_worker_id, ack.hold) for ack in receipt.cleanup} == {
                (source.object_id, core.worker_id, transfer.final_hold),
                (source.object_id, core.worker_id, transfer.provisional_hold),
            }
            assert all(ack.accepted for ack in receipt.cleanup)
            assert record.current_attempt == identity.attempt_id
            assert record.max_retries == record.retries_started == 0
            assert core._recovery.active_recovery(identity.task_id) is None
            assert output.object_id not in core._stored_descriptors
            assert identity not in getattr(core, "_output_node_cleanup", {})
            assert identity not in getattr(core, "_output_result_custody", {})
            assert not core._protocol_unresolved and not core._task_finish_barriers
            assert core._accepted_task_count == 0
        assert result.current_attempt == identity.attempt_id and not result.locations
        assert result.inline_data is None and result.canonical_stored_result is None
        if known_complete:
            assert result.state is ObjectState.LOST and result.error is None
            assert record.state is TaskState.SUCCEEDED
            # No get(output): this receipt must not trigger reconstruction.
        else:
            assert result.state is ObjectState.ERROR and isinstance(result.error, ray.SystemTaskError)
            assert record.state is TaskState.SYSTEM_FAILED
            with pytest.raises(ray.SystemTaskError):
                ray.get(output, timeout=_remaining(deadline))
        child_after = core.owner_table.snapshot(source.object_id)
        assert child_after.state is ObjectState.READY_INLINE
        assert child_after.inline_data == source_before.inline_data
        assert child_after.local_tokens == source_before.local_tokens
        for hold in (transfer.final_hold, transfer.provisional_hold):
            assert hold not in child_after.contained_holds
            assert core.owner_table.contained_release_was_seen(source.object_id, hold)
        assert ray.get(source, timeout=_remaining(deadline)) == 42
        _close_reference(output, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _poll(lambda: core.owner_table.collection_state(output.object_id) is ObjectCollectionState.COLLECTED,
              deadline, "lost/error output did not collect")
        _poll(lambda: not core.owner_table.snapshot(source.object_id).borrowed_tokens,
              deadline, "dead publisher's argument borrower remained live")
        assert ray.get(source, timeout=_remaining(deadline)) == 42
        _close_reference(source, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _poll(lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED,
              deadline, "surviving child did not collect after local close")
        assert _handoff(owner_address, identity, deadline) == after
        for object_id in (output.object_id, source.object_id):
            assert not core.owner_table.contains(object_id)
            assert object_id not in core._objects and object_id not in core._stored_descriptors
            assert object_id not in core._object_gc_obligations
            assert core._recovery.lineage_for_object(object_id) is None
    finally:
        if connection is not None:
            connection.close()
        if listener is not None:
            listener.close()
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for reference in (output, source):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            report = ray.shutdown()
    assert context is not None and death is not None and report is not None and not close_errors
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean
    assert report.gcs_exitcode == 0 and not report.gcs_forced and not report.forced
    assert report.node_pids == (survivor.node_pid, victim.node_pid)
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == report.node_finalized == report.node_shutdown_ack_clean == (True, False)
    assert report.node_resources_clean == (True, False) and report.node_forced == (False, False)
    assert report.node_deaths[1] == death
    assert report.worker_pids == (survivor.worker_pid, victim.worker_pid)
    assert report.worker_cleans == (True, False) and report.worker_forced == (False, False)
    assert report.worker_exitcodes == (0, None)
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0,
          "managed publisher Node-loss process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass


def test_promoted_output_publisher_node_loss_cleans_live_child_then_reports_unknown():
    _run_publisher_loss(OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK)


def test_completed_output_publisher_node_loss_keeps_success_receipt_but_result_lost():
    _run_publisher_loss(OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY)
