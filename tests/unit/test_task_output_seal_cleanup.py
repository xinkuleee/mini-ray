"""One Task retains its failed publication until exact physical cleanup ACKs.

One threadless Core, unstarted Node and Worker handlers, one real 1 KiB Store,
one output <=64B, one execution and at most one queued retry. A local Seal
callback applies then fails; while its replica is pinned, failed Complete
releases CPU but cannot authorize retry. After unpin one actual Drop response
is lost, then one actual owner rollback ACK remains hidden until enabled.
All faults reuse the same publication; no GCS, sockets, processes, threads,
timers, waits or another user execution. The Worker drain must stay unclean
while its real prepared-output custody remains. No sibling/selected slot.
"""

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import core as core_module, node as node_module, output_protocol as wire, protocol, worker as worker_module
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, _DelayedReadyTask, _PendingTask, _PushRequestState
from miniray.ids import LeaseID, WorkerID
from miniray.node import NodeServer, _LeaseOutcome, _LeaseRecord, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore, ObjectStoreError
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication_journal import OutputPublicationJournal, OutputPublicationJournalState
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector
from miniray.transport import RemoteCallError, TransportTimeout
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core, make_pure_core

pytestmark = pytest.mark.unit


from tests.support._worker_protocol import initialize_worker_protocol

@pytest.fixture(autouse=True)
def no_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('single output cleanup attempted runtime infrastructure')
    for kind, method in ((CoreWorker, '__init__'), (NodeServer, '__init__'), (WorkerServer, '__init__'),
                         (threading.Thread, 'start'), (threading.Thread, 'join'),
                         (threading.Timer, 'start'), (threading.Event, 'wait'),
                         (threading.Condition, 'wait'), (threading.Condition, 'wait_for'),
                         (multiprocessing.process.BaseProcess, 'start')):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    monkeypatch.setattr(time, 'sleep', forbidden)
    monkeypatch.setattr(node_module, 'rpc_request', forbidden)
    monkeypatch.setattr(core_module, 'rpc_request', forbidden)
    def set_receipt_only(event, timeout=None):
        assert event.is_set(), 'unexpected blocking receipt wait'
        return True
    monkeypatch.setattr(threading.Event, 'wait', set_receipt_only)


class _LiveInput:
    pid = 9101
    exitcode = None
    def is_alive(self):
        return self.exitcode is None


def _composition():
    core = make_pure_core()
    pending, ref = core._register_submission(core.define_remote_function(lambda: b'output'),
        (), {}, ResourceVector({'CPU': 1}), max_retries=1, _enqueue=True)
    assert core._submissions.get_nowait() is pending
    core._submissions.task_done()
    executor, lease = WorkerID(b'w' * 16), LeaseID(b'l' * 16)
    token = AllocationToken('task-seal-cleanup')
    request = protocol.RequestWorkerLease(lease, pending.task_id, pending.spec.attempt_id,
        pending.spec.resources, core.node_id, core.worker_id, target_node_id=core.node_id,
        return_ids=pending.output_ids, requester_owner_address=core.owner_address)
    grant = protocol.GrantWorkerLease(lease, pending.task_id, pending.spec.attempt_id,
        core.node_id, executor, ('worker.invalid', 2), token)
    node = object.__new__(NodeServer)
    node.node_id = core.node_id
    node._node_pid, node._registration_epoch = 21001, 1
    node._registered_with_gcs, node._gcs_address = True, None
    node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
    node._stop_event, node._shutdown_request_id = threading.Event(), None
    node._ledger = ResourceLedger(pending.spec.resources)
    node._ledger.allocate(pending.spec.resources, token)
    node._cluster_nodes = (NodeSnapshot(core.node_id, node._ledger.total, node._ledger.available),)
    node._object_store = ObjectStore(1024)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._sealed_metadata, node._dropped_metadata, node._local_replica_write_claims = {}, {}, {}
    node._object_localization_locks, node._dependency_pin_cleanups = {}, {}
    node._owner_death_fences, node._actor_workers, node._pinned_transfers = {}, {}, {}
    node._worker_order = (executor,)
    node._workers = {executor: _WorkerSlot(executor, _LiveInput(), grant.worker_address, 9101, active_lease_id=lease)}
    node._leases = {lease: _LeaseRecord(request, token, grant)}
    node._lease_outcomes = {lease: _LeaseOutcome(request, grant)}
    node._lease_cancellations, node._lease_request_locks = {}, {}
    node.event_sink = None
    node._output_publication_journal = OutputPublicationJournal()
    def owner_rpc(address, handler, message):
        assert address == core.owner_address
        methods = {wire.REGISTER_OUTPUT_HANDOFF_HANDLER: core.register_output_handoff,
                   wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: core.report_output_handoff_complete,
                   wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: core.report_output_handoff_rollback}
        return methods[handler](message)
    node._background_rpc = owner_rpc
    node._output_publications = node._make_output_publication_adapter()
    worker = object.__new__(WorkerServer)
    initialize_worker_protocol(worker)
    worker.worker_id, worker.node_id, worker.node_address = executor, core.node_id, core.node_address
    worker.inline_threshold, worker._worker_core_enabled = 0, False
    worker._execution_lock = threading.Lock()
    worker._replies, worker._cached_pushes, worker._functions = {}, {}, {}
    worker._completion_acked, worker._crash_after_complete = set(), set()
    worker._lease_bindings, worker._attempt_leases = {}, {}
    worker._failpoint, worker._failpoint_triggers, worker.event_sink = None, 0, None
    push = protocol.PushTask(lease, executor, pending.spec)
    state = _PushRequestState(push, grant, core.node_address, grant.worker_address, 1, True)
    core._mark_protocol_unresolved(pending, 'push_replay_wait', target_node_id=node.node_id)
    core._resolve_node_address = lambda node_id, *, home_route=None: core.node_address
    return core, node, worker, pending, ref, state


def test_unknown_task_materialization_and_drop_ack_gate_retry_and_worker_drain(monkeypatch):
    core, node, worker, pending, ref, state = _composition()
    adapter, journal = node._output_publications, node._output_publication_journal
    executions, seals, drops, completions, reports = [], [], [], [], []
    pin = None
    dropped_reply_lost = False
    owner_ack_allowed = False
    real_loads = cloudpickle.loads
    real_seal, real_drop, real_report = adapter._seal_replica, adapter._drop_replica, adapter._report_rollback
    def execute_once():
        executions.append(True)
        assert len(executions) == 1
        return b'output'
    def load(payload):
        return execute_once if payload == pending.spec.function_definition.payload else real_loads(payload)
    def seal_then_fail(effect, descriptor, payload):
        nonlocal pin
        result = real_seal(effect, descriptor, payload)
        seals.append((effect, descriptor, payload, result))
        assert len(seals) == 1
        pin = node.object_store.pin(pending.object_id, 'retain-unknown-materialization')
        raise ObjectStoreError('injected failure after actual Task Seal')
    def drop_then_lose_reply(effect, request):
        nonlocal dropped_reply_lost
        reply = real_drop(effect, request)
        drops.append((effect, request, reply))
        assert len(drops) <= 24
        if reply.status is protocol.DropObjectReplicaStatus.DROPPED and not dropped_reply_lost:
            dropped_reply_lost = True
            raise TransportTimeout('actual Drop completed; reply lost')
        return reply
    def report_without_reply(tombstone, *, manifest):
        reply = real_report(tombstone, manifest=manifest)
        reports.append(tombstone)
        assert len(reports) <= 16
        if not owner_ack_allowed:
            raise TransportTimeout('actual owner rollback recorded; reply unknown')
        return reply
    def to_node(address, handler, message, **options):
        if handler == 'start_worker_lease': return node._handle_start_worker_lease(message)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER: return node._handle_prepare_output_publication(message)
        if handler == 'get_worker_lease_outcome': return node._handle_get_worker_lease_outcome(message)
        assert handler == 'complete_worker_lease'
        reply = node._handle_complete_worker_lease(message)
        completions.append((message, reply))
        assert len(completions) <= 24
        return reply
    def replay_worker(address, handler, message):
        assert handler == 'push_task' and message == state.push
        try:
            return worker._handle_push_task(message)
        except RuntimeError as exc:
            raise RemoteCallError(handler, type(exc).__name__, str(exc), '') from exc
    monkeypatch.setattr(worker_module.cloudpickle, 'loads', load)
    monkeypatch.setattr(worker_module, 'rpc_request', to_node)
    monkeypatch.setattr(adapter, '_seal_replica', seal_then_fail)
    monkeypatch.setattr(adapter, '_drop_replica', drop_then_lose_reply)
    monkeypatch.setattr(adapter, '_report_rollback', report_without_reply)
    core._rpc = to_node
    core._push_task_rpc = replay_worker
    key = pending.spec.attempt_id, state.push.lease_id
    try:
        with pytest.raises(RuntimeError, match='exact PushTask replay'):
            worker._handle_push_task(state.push)
        prepared = worker._prepared_output_replies[key]
        identity = prepared.outputs.manifest.publication_id
        failure = prepared.failure_reply
        assert failure.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert prepared.complete_envelope is None and not prepared.prepare_acked
        assert executions == [True] and len(seals) == 1
        assert node.object_store.contains(pending.object_id) and pin is not None
        assert journal.snapshot(identity).state is OutputPublicationJournalState.ROLLING_BACK
        assert journal.snapshot(identity).complete is None
        assert node._leases[state.push.lease_id].state is protocol.LeaseExecutionState.COMPLETED
        assert node.resource_ledger.available == node.resource_ledger.total
        assert not core._replay_push(pending, state)
        delayed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert isinstance(delayed, _DelayedReadyTask) and delayed.ready.push_state.push == state.push
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert any(reply.status is protocol.DropObjectReplicaStatus.PINNED for _,_,reply in drops)
        # Actual Worker drain must retain its prepared-output obligation and
        # avoid stopping the embedded Core while physical cleanup is blocked.
        worker._drain_request_id = 'retained-output-drain'
        assert not worker._drain_status('retained-output-drain', timeout=0).clean
        assert worker._prepared_output_replies[key] is prepared
        assert node.object_store.unpin(pending.object_id, pin)
        pin = None
        assert not core._replay_push(pending, delayed.ready.push_state)
        delayed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert isinstance(delayed, _DelayedReadyTask)
        assert dropped_reply_lost and not node.object_store.contains(pending.object_id, sealed_only=False)
        assert not adapter.rollback_reported(identity)
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        # A later exact attempt may reach owner rollback, still without ACK.
        assert not core._replay_push(pending, delayed.ready.push_state)
        delayed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert reports and not adapter.rollback_reported(identity)
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        owner_ack_allowed = True
        assert worker._handle_push_task(state.push) is failure
        assert adapter.rollback_reported(identity) and key not in worker._prepared_output_replies
        assert prepared.discovery.source_references == () and prepared.nested_imports is None
        assert not core._replay_push(pending, delayed.ready.push_state)
        queued = []
        for _ in range(core._submissions.qsize()):
            queued.append(core._submissions.get_nowait()); core._submissions.task_done()
        retries = [item for item in queued if isinstance(item, _PendingTask)]
        assert len(retries) == 1 and retries[0].spec.attempt_id == pending.spec.attempt_id.next()
        assert core._recovery.task_record(pending.task_id).retries_started == 1
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert executions == [True] and len(seals) == 1
        assert not node._local_replica_write_claims and node.object_store.used_bytes == 0
        assert all(message == completions[0][0] for message,_ in completions)
        assert all(tombstone == reports[0] for tombstone in reports)
        assert drops[-1][2].status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    finally:
        if pin is not None:
            node.object_store.unpin(pending.object_id, pin)
        ref.close(timeout=0)
        close_pure_core(core)


def test_worker_drain_retains_real_source_until_local_release_and_same_publication_complete(monkeypatch):
    """One actual child put/source, no generic export-pin compatibility path."""
    core, node, worker, pending, ref, state = _composition()
    child = make_pure_core()
    child.job_id, child.node_id = core.job_id, node.node_id
    child._home_route = _HomeRoute(child.node_id, child.node_address, child._membership_epoch)
    child.worker_id = worker.worker_id
    child.owner_address = ('child.invalid', 3)
    worker._server = SimpleNamespace(address=child.owner_address)
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._active_tasks, worker._accepting_tasks = 0, True
    worker._accepted_pushes, worker._push_obligations = {}, set()
    sources = [child.put(7)]
    child_id = sources[0].object_id
    key = pending.spec.attempt_id, state.push.lease_id
    original_background = node._background_rpc
    original_load = cloudpickle.loads
    original_release = OutputDiscoverySession.release_sources_after_promotions
    calls, executed, cleanup_attempts = [], [], []
    release_allowed = False

    def child_or_owner(address, handler, message):
        if address != child.owner_address:
            return original_background(address, handler, message)
        methods = {'prepare_stored_contained_pin': child.prepare_stored_contained_pin,
                   'promote_stored_contained_pin': child.promote_stored_contained_pin,
                   'release_contained_reference': child.release_contained_reference}
        reply = methods[handler](message)
        calls.append((handler, message, reply))
        assert len(calls) <= 8 and reply.accepted
        return reply

    def execute_once():
        executed.append(True)
        assert len(executed) == len(sources) == 1
        return [sources.pop()]

    def blocked_release(discovery):
        if discovery.header.publication_id != pending_identity():
            return original_release(discovery)
        cleanup_attempts.append(True)
        assert len(cleanup_attempts) <= 3
        if not release_allowed:
            assert len(discovery.source_references) == 1
            assert not discovery.source_references[0].closed
            raise RuntimeError('local source custody release not acknowledged')
        return original_release(discovery)

    def pending_identity():
        from miniray.output_publication import OutputPublicationID
        return OutputPublicationID(state.push.lease_id, pending.execution)

    def to_node(address, handler, message, **options):
        methods = {'start_worker_lease': node._handle_start_worker_lease,
                   wire.PREPARE_OUTPUT_PUBLICATION_HANDLER: node._handle_prepare_output_publication,
                   'complete_worker_lease': node._handle_complete_worker_lease,
                   'get_worker_lease_outcome': node._handle_get_worker_lease_outcome,
                   wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER: node._handle_ack_output_publication_adopted,
                   'drop_object_replica': node._handle_drop_object_replica}
        assert address == core.node_address
        return methods[handler](message)

    monkeypatch.setattr(node, '_background_rpc', child_or_owner)
    monkeypatch.setattr(worker_module.cloudpickle, 'loads', lambda payload:
        execute_once if payload == pending.spec.function_definition.payload else original_load(payload))
    monkeypatch.setattr(worker_module, 'rpc_request', to_node)
    monkeypatch.setattr(OutputDiscoverySession, 'release_sources_after_promotions', blocked_release)
    core._rpc = to_node
    core._borrow_rpc = child_or_owner
    try:
        with pytest.raises(RuntimeError, match='exact PushTask replay'):
            worker._handle_push_task(state.push)
        prepared = worker._prepared_output_replies[key]
        identity = prepared.outputs.manifest.publication_id
        transfer, = (prepared.outputs.manifest.value).transfers
        assert prepared.prepare_acked and prepared.complete_envelope is None
        assert prepared.failure_reply is None
        assert prepared.discovery.source_references[0].object_id == child_id
        assert not sources and executed == [True]
        before = child.owner_table.snapshot(child_id)
        assert before.local_tokens and before.contained_holds == frozenset({transfer.final_hold})
        assert node._output_publication_journal.snapshot(identity).complete is None
        assert node._leases[state.push.lease_id].state is protocol.LeaseExecutionState.RUNNING
        assert node.resource_ledger.available.is_zero()
        worker._drain_request_id = 'source-custody-drain'
        status = worker._drain_status('source-custody-drain', timeout=0.1)
        assert not status.clean and worker._prepared_output_replies[key] is prepared
        assert worker._push_obligations == {key}
        assert child.owner_table.snapshot(child_id) == before
        assert executed == [True] and cleanup_attempts == [True, True]
        release_allowed = True
        reply = worker._handle_push_task(state.push)
        assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
        assert reply.output_publication.manifest == prepared.outputs.manifest
        assert key not in worker._prepared_output_replies and not worker._push_obligations
        assert not prepared.discovery.source_references and prepared.nested_imports is None
        assert executed == [True] and cleanup_attempts == [True, True, True]
        assert node.resource_ledger.available == node.resource_ledger.total
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id,
                                   expected_lease_id=state.push.lease_id)
        assert core._finish_pending_task(pending)
        retained = child.owner_table.snapshot(child_id)
        assert not retained.local_tokens and retained.contained_holds == frozenset({transfer.final_hold})
        assert len([call for call in calls if call[0] == 'prepare_stored_contained_pin']) == 1
        assert len([call for call in calls if call[0] == 'promote_stored_contained_pin']) == 1
        ref.close(timeout=0)
        core._reference_mailbox.drain()
        child._reference_mailbox.drain()
        assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
        assert child.owner_table.collection_state(child_id) is ObjectCollectionState.COLLECTED
        releases = [reply for kind, _, reply in calls if kind == 'release_contained_reference']
        assert releases and all(reply.accepted and reply.hold == transfer.final_hold for reply in releases)
        assert node.object_store.used_bytes == 0 and not node._local_replica_write_claims
    finally:
        release_allowed = True
        for source in sources:
            source.close(timeout=0)
        ref.close(timeout=0)
        close_pure_core(core)
        close_pure_core(child)
