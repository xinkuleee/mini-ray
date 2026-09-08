"""Two separately bounded real Node transfer-ACK-loss paths.

Each case starts GCS, two Nodes and one Worker per Node: five children,
two 1 MiB stores, one Driver-owned 8 KiB put on A and one never-executed
consumer on B. A test-module spawn wrapper calls the original process
entry point (including setsid); only marked B wraps outgoing transport.
It drops one actual Pin reply or the first three actual Release replies
after validating them. The fourth Release ACK is delivered normally.

No invented reply, production flag/handler, extra process, test thread,
listener, kill, Actor or tracing. Child exit assertions prove the exact
real Pin/Chunk/Release counts without another observation channel. The
Driver checks real rejection, Cancel inventory, custody ACK and complete
GC. Run each exact case separately: 18 seconds post-init work, three
seconds reference cleanup; shutdown has its own bounded drain. The outer
runner starts process-tree termination at 30 seconds plus bounded grace;
internal deadlines do not alone bound startup and shutdown.
"""

from dataclasses import replace
import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray import api as api_module, node as node_module, protocol
from miniray.api import _get_runtime
from miniray.core import _LeaseCancellationState, _LocationReportState
from miniray.errors import LeaseRejectedError
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import TransportTimeout
from tests.integration.test_local_replica_handoff_failure_path import _assert_cancelled, _poll, _remaining, _replica, _rpc, _wait
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_output_owner_death_path import _pid_exists


pytestmark = pytest.mark.multiprocess_smoke
_ACTUAL_NODE_PROCESS_MAIN = api_module._node_process_main
_TARGET = "transfer_ack_loss_target"
_PIN_MODE = "transfer_pin_ack_loss"
_RELEASE_MODE = "transfer_release_ack_loss"
_PIN_ERROR = "actual source Pin ACK deliberately lost"
_PAYLOAD = b"A" * (8 * 1024)


def _node_process_with_transfer_ack_loss(*args):
    """Spawn-safe wrapper; the original entry still owns process setup."""
    node_id, resources = args[:2]
    if not resources.get(_TARGET, 0):
        return _ACTUAL_NODE_PROCESS_MAIN(*args)
    pin_mode = bool(resources.get(_PIN_MODE, 0))
    assert pin_mode != bool(resources.get(_RELEASE_MODE, 0))
    actual_rpc = node_module.rpc_request
    pins, chunks, releases = [], [], []

    def delivery(address, handler, request, **options):
        reply = actual_rpc(address, handler, request, **options)
        if handler == node_module.PIN_OBJECT_HANDLER:
            assert type(request) is protocol.PinObjectForTransfer and request.requester_node_id == node_id
            assert type(reply) is protocol.PinObjectForTransferReply and reply.pinned and reply.error is None
            assert reply.transfer_id == request.transfer_id and reply.descriptor == request.descriptor
            pins.append((address, request))
            assert len(pins) == 1 and request.descriptor.node_id != node_id
            if pin_mode:
                raise TransportTimeout(_PIN_ERROR)
        elif handler == node_module.GET_OBJECT_CHUNK_HANDLER:
            assert len(pins) == 1 and not pin_mode
            source, pin = pins[0]
            assert address == source and request.transfer_id == pin.transfer_id and request.requester_node_id == node_id
            assert request.object_id == pin.descriptor.object_id
            assert type(reply) is protocol.GetObjectChunkReply and reply.ok and reply.error is None
            assert (reply.transfer_id, reply.object_id, reply.node_id, reply.offset) == (
                pin.transfer_id, pin.descriptor.object_id, pin.descriptor.node_id, request.offset,
            )
            assert request.offset == 0 and len(reply.data) == request.size_bytes == pin.descriptor.size_bytes
            chunks.append(request)
            assert len(chunks) == 1
        elif handler == node_module.RELEASE_OBJECT_PIN_HANDLER:
            assert len(pins) == 1
            source, pin = pins[0]
            assert address == source and request == protocol.ReleaseObjectPin(pin.transfer_id, pin.descriptor.object_id, node_id)
            assert type(reply) is protocol.ReleaseObjectPinReply and reply.accepted and reply.error is None
            assert (reply.transfer_id, reply.object_id, reply.node_id) == (pin.transfer_id, pin.descriptor.object_id, pin.descriptor.node_id)
            assert reply.released is (not releases)
            releases.append(reply)
            assert len(releases) <= (1 if pin_mode else 4)
            if not pin_mode and len(releases) <= 3:
                raise TransportTimeout("actual source Release ACK deliberately lost")
        return reply

    node_module.rpc_request = delivery
    try:
        _ACTUAL_NODE_PROCESS_MAIN(*args)
        # A missed injection or extra transfer makes this managed child exit
        # nonzero, which the Driver's exact shutdown report must reject.
        assert len(pins) == 1 and len(chunks) == (0 if pin_mode else 1)
        assert len(releases) == (1 if pin_mode else 4)
    finally:
        node_module.rpc_request = actual_rpc


@ray.remote(resources={_TARGET: 1}, max_retries=0)
def _consumer_must_not_run(value):
    raise AssertionError("transfer ACK loss admitted user code")


def _run_ack_loss(*, pin_mode):
    context = core = reference = consumer = report = None
    original_entry = api_module._node_process_main
    original_rpc = original_push = original_clear = None
    pids, addresses = set(), set()
    calls, cancellations, handoffs, pushes, cleanup_errors = [], [], [], [], []
    try:
        api_module._node_process_main = _node_process_with_transfer_ack_loss
        mode = _PIN_MODE if pin_mode else _RELEASE_MODE
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1}, {"CPU": 1, _TARGET: 1, mode: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + 18.0
        runtime = _get_runtime()
        core = runtime.core_worker
        source, target = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address, source.node_address,
                          source.worker_address, target.node_address, target.worker_address))
        assert len(pids) == 5 and len(addresses) == 6 and os.getpid() not in pids
        assert context.trace_address is None and core.node_id == source.node_id
        reference = ray.put(_PAYLOAD)
        before = core.owner_table.snapshot(reference.object_id)
        assert reference.owner_worker_id == core.worker_id and reference.borrower_token is None
        assert before.state is ObjectState.READY_STORED and before.locations == frozenset((source.node_id,))
        original_rpc, original_push, original_clear = core._rpc, core._push_task_rpc, core._clear_protocol_unresolved

        def observe_rpc(address, handler, request):
            if handler == node_module.CANCEL_LEASE_HANDLER:
                with core._state_lock:
                    pending = core._task_finish_barriers[request.lease_request.return_ids[0]]
                    state = core._protocol_unresolved[pending.task_key].obligation
                    assert type(state) is _LeaseCancellationState
                    cancellations.append(state)
                    assert len(cancellations) == 1
            reply = original_rpc(address, handler, request)
            if handler in (node_module.REQUEST_LEASE_HANDLER, node_module.CANCEL_LEASE_HANDLER,
                           protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER):
                calls.append((address, handler, request, reply))
                assert len(calls) <= 8
            return reply

        def observe_push(address, handler, request):
            pushes.append(request)
            assert len(pushes) <= 1
            return original_push(address, handler, request)

        def observe_clear(pending):
            with core._state_lock:
                marker = core._protocol_unresolved.get(pending.task_key)
                if marker is not None and type(marker.obligation) is _LocationReportState:
                    handoffs.append(marker.obligation)
                    assert len(handoffs) == 1
            return original_clear(pending)

        core._rpc, core._push_task_rpc, core._clear_protocol_unresolved = observe_rpc, observe_push, observe_clear
        consumer = _consumer_must_not_run.remote(reference)
        error_text = _PIN_ERROR if pin_mode else "source pin release failed after 3 attempts; exact obligation retained"
        with pytest.raises(LeaseRejectedError, match=error_text) as failure:
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, deadline)
        assert not pushes
        ((_, _, request, rejection),) = tuple(item for item in calls
            if item[0] == target.node_address and item[1] == node_module.REQUEST_LEASE_HANDLER)
        assert type(rejection) is protocol.RejectWorkerLease and rejection.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
        assert request.task_id == consumer.object_id.task_id and request.attempt_id.attempt_number == 0
        assert len(request.dependencies) == 1 and request.dependencies[0].object_id == reference.object_id
        assert not any(type(reply) is protocol.GrantWorkerLease for _, _, _, reply in calls)
        ((_, _, cancel_request, cancel_reply),) = tuple(item for item in calls if item[1] == node_module.CANCEL_LEASE_HANDLER)
        _assert_cancelled(cancel_reply, cancel_request)
        assert cancel_request.lease_request == request and not cancel_reply.released and cancel_reply.retired_grant is None
        (cancellation,), (handoff,) = tuple(cancellations), tuple(handoffs)
        assert cancellation.terminal_error is handoff.terminal_error is failure.value
        assert cancellation.known_grant is None and cancellation.lease_request == request
        expected = () if pin_mode else (replace(request.dependencies[0], node_id=target.node_id),)
        inventory = protocol.LeaseDependencyInventory(request, target.node_id, expected)
        assert cancel_reply.dependency_inventory == handoff.inventory == inventory
        assert handoff.grant is None and handoff.cancellation_reply == cancel_reply and handoff.execution_outcome is None
        assert handoff.lease_request == request and handoff.custody_acknowledged
        assert handoff.reports == handoff.receipts == () and len(handoff.local_receipts) == len(expected)
        assert all(receipt.accepted and receipt.custody_transferred for receipt in handoff.local_receipts)
        assert tuple(receipt.descriptor for receipt in handoff.local_receipts) == expected
        ((ack_address, _, ack_request, ack_reply),) = tuple(item for item in calls if item[1] == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER)
        assert ack_address == target.node_address and type(ack_request) is protocol.AckLeaseDependencyCustody
        assert ack_request.inventory == inventory and ack_request.requester_worker_id == core.worker_id
        assert type(ack_reply) is protocol.AckLeaseDependencyCustodyReply and ack_reply.accepted and ack_reply.request == ack_request
        failed = core.owner_table.snapshot(consumer.object_id)
        assert failed.state is ObjectState.ERROR and failed.error is failure.value and failed.current_attempt == request.attempt_id
        record = core._recovery.task_record(consumer.object_id.task_id)
        assert record.retries_started == 0 and record.current_attempt == request.attempt_id
        assert core._recovery.active_recovery(consumer.object_id.task_id) is None
        after = core.owner_table.snapshot(reference.object_id)
        expected_locations = (source.node_id,) if pin_mode else (source.node_id, target.node_id)
        assert after.state is ObjectState.READY_STORED and after.locations == frozenset(expected_locations)
        assert after.current_attempt == before.current_attempt and after.canonical_stored_result == before.canonical_stored_result
        assert ray.get(reference, timeout=_remaining(deadline)) == _PAYLOAD
        assert _replica(target, reference.object_id, deadline).found is (not pin_mode)

        def clean(node):
            status = _rpc(node.node_address, node_module.SHUTDOWN_STATUS_HANDLER,
                          protocol.ShutdownStatusRequest("inspect-transfer-ack-loss-only"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
            assert status.child_pids == (node.worker_pid,)
            return status.resources_clean

        _poll(lambda: all(clean(node) for node in (source, target)), deadline, "transfer close outbox did not drain")
        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_local(reference, deadline)
        _wait(core, lambda: core.owner_table.collection_state(reference.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: all(not _replica(node, ref.object_id, deadline).found
                          for node in (source, target) for ref in (reference, consumer)), deadline, "collected bytes survived")
        assert all(clean(node) for node in (source, target))
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        assert all(_pid_exists(pid) for pid in pids)
    finally:
        api_module._node_process_main = original_entry
        if core is not None and original_rpc is not None:
            core._rpc, core._push_task_rpc, core._clear_protocol_unresolved = original_rpc, original_push, original_clear
        cleanup_deadline = time.monotonic() + 3.0
        try:
            for ref in (consumer, reference):
                try:
                    _close_local(ref, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            report = ray.shutdown()
    assert not cleanup_errors and context is not None and report is not None
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean and report.resources_clean and report.finalized and report.shutdown_ack_clean
    assert not report.forced and not report.gcs_forced
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_exitcodes == report.worker_exitcodes == (0, 0)
    assert not ray.is_initialized() and len(pids) == 5
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass


def test_real_pin_ack_loss_closes_unknown_source_session_before_rejection():
    _run_ack_loss(pin_mode=True)


def test_three_real_release_ack_losses_retry_from_the_existing_node_outbox():
    _run_ack_loss(pin_mode=False)
