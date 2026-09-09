"""W2: real local Complete, with/without surviving GCS terminal knowledge.

Each exact selector starts one GCS, two Nodes and one Worker per Node, creates
one Driver-owned child put and one <=32-KiB STORED Task result, max_retries=0.
The private Node gate observes actual journal/lease C3, then either blocks
before C4 RPC or intercepts the actual accepted GCS reply before its adapter
ACK. Both result/outcome paths share that gate, so owner receives no envelope
or Complete before the managed publisher tree is killed. The latter is an
accepted reply discarded at the Node adapter boundary, not claimed packet loss.

No fake Complete, death, bytes or cleanup receipt. Work has15seconds after init,
gate10seconds, reference cleanup3seconds, then unconditional normal shutdown.
Each function must run alone under the30second process-tree runner.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
import signal
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import enhanced_publication as enhanced, output_protocol as wire, protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.ids import AttemptID
from miniray.node import GET_OBJECT_HANDLER
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import (OutputPublicationGateConfig, OutputPublicationGatePhase,
                                      recv_output_publication_gate_arrival)
from miniray.recovery import TaskState
from miniray.transport import request as rpc_request
from tests.integration.test_task_path import _close_reference

pytestmark = pytest.mark.multiprocess_smoke
_RESOURCE = 'enhanced_terminal_publisher'
_PADDING = b'T' * (8 * 1024)


@ray.remote(num_cpus=1, resources={_RESOURCE: 1}, max_retries=0)
def _produce(container):
    child, = container
    assert child.borrower_token is not None
    return {'child': child, 'padding': _PADDING}


def _remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError('W2 terminal-loss work deadline expired')
    return value


def _poll(predicate, deadline):
    pause = threading.Event()
    while True:
        value = predicate()
        if value:
            return value
        pause.wait(min(0.01, _remaining(deadline)))


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, deadline=deadline,
        connect_timeout=min(0.5, remaining), request_timeout=min(2.0, remaining))


def _publication(context, reference, deadline):
    request = enhanced.GetPublication(reference)
    reply = _query(context.gcs_address, enhanced.PUBLICATION_HANDLER, request, deadline)
    assert type(reply) is enhanced.PublicationReply and reply.request == request and reply.accepted
    assert reply.snapshot is not None and reply.snapshot.reference == reference
    return reply.snapshot


def _handoff(address, identity, deadline):
    request = wire.GetOutputHandoff(identity)
    reply = _query(address, wire.GET_OUTPUT_HANDOFF_HANDLER, request, deadline)
    assert type(reply) is wire.OutputHandoffReply and reply.request == request and reply.accepted
    assert reply.snapshot is not None
    return reply.snapshot


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _run(known):
    phase = (OutputPublicationGatePhase.AFTER_TERMINAL_ACCEPTED_BEFORE_ACK if known
             else OutputPublicationGatePhase.BEFORE_TERMINAL_REPORT)
    listener = connection = context = runtime = source = output = death = report = None
    pids, addresses, close_errors = set(), set(), []
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        gate_address = listener.getsockname()
        context = ray.init(num_nodes=2, num_workers_per_node=1,
            node_resources=({'CPU': 1}, {'CPU': 1, _RESOURCE: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
            _test_output_publication_gate=OutputPublicationGateConfig(1, gate_address, phase, 10.0))
        deadline = time.monotonic() + 15.0
        runtime = _get_runtime()
        core = runtime.core_worker
        survivor, victim = context.nodes
        owner_address = runtime.owner_service.address
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((gate_address, owner_address, context.gcs_address))
        for node in context.nodes:
            addresses.update((node.node_address, node.worker_address))
        assert len(pids) == 5 and context.trace_address is None
        source = ray.put(42)
        output = _produce.remote([source])
        listener.settimeout(_remaining(deadline))
        connection, _ = listener.accept()
        connection.settimeout(min(10.0, _remaining(deadline)))
        arrival = recv_output_publication_gate_arrival(connection)
        assert arrival.phase is phase
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            victim.node_id, victim.node_pid, runtime.nodes[1].registration_epoch)
        identity = arrival.publication_id
        assert identity.output_ids == (output.object_id,)
        assert identity.attempt_id == AttemptID(output.object_id.task_id, 0)
        reference = enhanced.PublicationRef(identity, arrival.manifest_digest)
        central_before = _publication(context, reference, deadline)
        owner_before = _handoff(owner_address, identity, deadline)
        manifest = owner_before.manifest
        assert manifest == central_before.publication.manifest
        assert owner_before.phase is OutputHandoffPhase.PENDING
        assert owner_before.complete is owner_before.adoption is None
        assert central_before.prepared is not None and central_before.graph_active
        assert central_before.receipt(enhanced.PublicationStage.ARMED) is not None
        assert (central_before.complete is not None) is known
        assert (central_before.receipt(enhanced.PublicationStage.TERMINAL) is not None) is known
        assert central_before.adoption is None and central_before.receipt(enhanced.PublicationStage.COMMITTED) is None
        slot, = manifest.slots
        transfer, = slot.transfers
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE and 8192 < slot.size_bytes < 32768
        assert transfer.contained_object_id == source.object_id and transfer.contained_owner_worker_id == core.worker_id
        with core._state_lock:
            assert core.owner_table.snapshot(output.object_id).state is ObjectState.PENDING
            assert identity not in getattr(core, '_output_result_custody', {})
            assert output.object_id in core._task_finish_barriers
            assert transfer.final_hold in core.owner_table.snapshot(source.object_id).contained_holds
        physical = _query(victim.node_address, GET_OBJECT_HANDLER, protocol.GetObject(output.object_id, victim.node_id), deadline)
        assert physical.found and physical.sealed and physical.producer_attempt_id == identity.attempt_id
        assert hashlib.sha256(physical.data).hexdigest() == slot.checksum
        del physical  # Diagnostic bytes never become owner payload custody.
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT and death.exit_code == -signal.SIGKILL
        connection.close()
        connection = None
        listener.close()
        listener = None

        def resolved():
            with core._state_lock:
                if identity not in getattr(core, '_output_loss_completed', set()) or output.object_id in core._task_finish_barriers:
                    return None
                return core.owner_table.snapshot(output.object_id)

        result = _poll(resolved, deadline)
        central_after = _publication(context, reference, deadline)
        owner_after = _handoff(owner_address, identity, deadline)
        assert central_after.complete == central_before.complete
        assert owner_after.complete == central_before.complete and owner_after.adoption is None
        assert owner_after.phase is OutputHandoffPhase.ABORTED
        assert central_after.closed_holds is not None and not central_after.graph_active
        assert central_after.receipt(enhanced.PublicationStage.RETIRED) is not None
        assert central_after.adoption is None
        with core._state_lock:
            record = replace(core._recovery.task_record(identity.task_id))
            resolution = core.owner_table._output_loss_receipts[identity]
            assert resolution.complete == central_before.complete and not resolution.keep
            assert record.current_attempt == identity.attempt_id and record.retries_started == record.max_retries == 0
            assert core._accepted_task_count == 0 and not core._protocol_unresolved and not core._task_finish_barriers
            assert identity not in getattr(core, '_output_result_custody', {})
            assert identity not in getattr(core, '_output_node_cleanup', {})
            assert output.object_id not in core._stored_descriptors
        assert not result.locations and result.inline_data is result.canonical_stored_result is None
        if known:
            assert result.state is ObjectState.LOST and result.error is None and record.state is TaskState.SUCCEEDED
            assert type(central_after.fence) is enhanced.OwnerRetirementReceipt
            assert central_after.fence.reason is enhanced.RetirementReason.PAYLOAD_LOST
        else:
            assert result.state is ObjectState.ERROR and record.state is TaskState.SYSTEM_FAILED
            assert type(central_after.fence) is enhanced.OwnerAbortReceipt
            with pytest.raises(ray.SystemTaskError):
                ray.get(output, timeout=_remaining(deadline))
        # No get of known LOST output: this case observes knowledge, not a
        # separately requested reconstruction. Both versions have lost bytes.
        assert ray.get(source, timeout=_remaining(deadline)) == 42
        for hold in (transfer.final_hold, transfer.provisional_hold):
            assert core.owner_table.contained_release_was_seen(source.object_id, hold)
            assert hold not in core.owner_table.snapshot(source.object_id).contained_holds
        _close_reference(output, min(deadline, time.monotonic() + 3.0))
        _poll(lambda: core.owner_table.collection_state(output.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_reference(source, min(deadline, time.monotonic() + 3.0))
        _poll(lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert _publication(context, reference, deadline) == central_after
        assert _handoff(owner_address, identity, deadline) == owner_after
    finally:
        if connection is not None:
            connection.close()
        if listener is not None:
            listener.close()
        cleanup_deadline = time.monotonic() + 3.0
        try:
            for ref in (output, source):
                try:
                    _close_reference(ref, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            report = ray.shutdown()
    assert context is not None and death is not None and report is not None and not close_errors
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean
    assert report.gcs_exitcode == 0 and not report.gcs_forced and not report.forced
    assert report.node_exitcodes == (0, death.exit_code) and report.node_forced == (False, False)
    assert report.node_cleans == report.node_finalized == report.node_shutdown_ack_clean == (True, False)
    assert report.node_resources_clean == (True, False)
    assert report.worker_exitcodes == (0, None) and report.worker_forced == (False, False)
    assert report.worker_cleans == (True, False)
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0)
    assert all(child.pid not in pids for child in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass


def test_local_complete_without_gcs_terminal_becomes_unknown_after_node_loss():
    _run(False)


def test_gcs_terminal_ack_lost_preserves_success_but_not_bytes_after_node_loss():
    _run(True)
