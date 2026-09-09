"""Opt-in L1: two real reconstruction callers, no OS cluster or transport.

One threadless Core and one unstarted Node reducer compose actual Grant/Start,
discovery, Prepare/Complete, owner adoption and physical drops for one
tiny STORED output. A 1-KiB store and real publication/membership authorities
are in memory. No user callable, Worker process or Core runtime thread runs.

Both request threads execute the full internal Core reconstruction path, not
public get (which has a separate readiness/retirement wait). The first
pauses at its existing Drop RPC boundary, holding the real slot-zero retirement
ticket but no Core/Node/journal lock. The second genuinely defers on that
ticket; after the first really enqueues, it retries once and genuinely JOINs.
Retirement is not pre-completed in main and no test mutex encloses Core calls.
There are exactly two daemon request threads and three Core request calls.

The three-party Barrier and each event wait have at most one second. Normal
joins share two seconds and finally joins share one; finally aborts/releases
gates even after Thread.start failure. Audit FIFOs retain <=8 control events,
<=8 drop receipts and <=32 typed bridge calls. No sockets, subprocesses, timers,
sleeps or background runtime constructors are admitted. Normal teardown uses
real terminal error/finish and public close/GC, never a fabricated success.
Run one exact node ID only through the external 30-second bounded runner.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from miniray import control, core as core_module, node as node_module, output_protocol as wire, protocol, transport
from miniray.control import NodeRegistry
from miniray.core import CoreWorker, _PendingTask, _WAKE_COORDINATOR
from miniray.errors import SystemTaskError
from miniray.ids import LeaseID, NodeID, ObjectID, WorkerID
from miniray.lease_dependencies import LeaseDependencyCustody
from miniray.node import NodeServer, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import TaskState, UnknownTaskError
from miniray.resources import NodeSnapshot, ResourceLedger, ResourceVector
from miniray.transfer_pins import TransferPinOutbox
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.loopback_smoke


def _forbidden(*_args, **_kwargs):
    pytest.fail("L1 reconstruction attempted unreviewed runtime or transport")


@pytest.fixture
def allowed_request_threads(monkeypatch):
    allowed = []
    start_thread = threading.Thread.start

    def start(thread):
        assert len(allowed) == 2 and any(thread is item for item in allowed)
        return start_thread(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"), (WorkerServer, "__init__"),
        (threading.Timer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, _forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, _forbidden)
    monkeypatch.setattr(subprocess, "Popen", _forbidden)
    monkeypatch.setattr(time, "sleep", _forbidden)
    for module in (core_module, node_module, control):
        monkeypatch.setattr(module, "rpc_request", _forbidden)
    monkeypatch.setattr(transport, "request", _forbidden)
    return allowed


def _snapshot_queue(fifo):
    with fifo.mutex:
        return tuple(fifo.queue)


def _node():
    """One real local lease/bytes authority, with a passive existing Worker."""
    node = object.__new__(NodeServer)
    node.node_id, node.worker_id = NodeID.random(), WorkerID.random()
    node.num_workers_per_node = 1
    process = SimpleNamespace(pid=7301, exitcode=None, is_alive=lambda: True)
    node._worker_order = (node.worker_id,)
    node._workers = {node.worker_id: _WorkerSlot(node.worker_id, process=process,
        address=("worker.invalid", 7301), pid=process.pid)}
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.available),)
    node._cluster_addresses = {}
    node._gcs_address, node._registered_with_gcs = None, False
    node._node_pid, node._registration_epoch, node._membership_epoch = 7300, 1, 1
    node._resource_report_version = node._resource_reported_version = 0
    node._object_store = ObjectStore(1024)
    node._sealed_metadata, node._dropped_metadata = {}, {}
    node._dependency_pin_cleanups, node._pinned_transfers = {}, {}
    node._object_localization_locks = {}
    node._source_pin_releases = TransferPinOutbox()
    node._lease_dependency_custody = LeaseDependencyCustody(node.node_id)
    node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = node._worker_replacements_inflight = 0
    node._shutdown_request_id = None
    node._stop_event = threading.Event()
    node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node.event_sink = None
    return node


class _Case:
    def __init__(self):
        self.core = core = make_pure_core()
        self.node = node = _node()
        self.refs = ()
        self.pending = self.retried = None
        self.threads = ()
        self.before_drop = None
        self.calls = queue.Queue(maxsize=32)
        self.drops = queue.Queue(maxsize=8)
        self.violations = queue.Queue(maxsize=8)
        self.violation_overflow = False
        self.registry = NodeRegistry()
        self.gcs_address = ("reconstruction-gcs.invalid", 1)
        node._server = SimpleNamespace(address=("reconstruction-node.invalid", 1))
        node._object_manager = ObjectManager(node.node_id, node.object_store)
        node._output_publication_journal = OutputPublicationJournal()
        node._local_replica_write_claims = {}
        node._gcs_address = self.gcs_address
        node._background_rpc = self.control_rpc
        node._register_with_gcs()
        node._output_publications = node._make_output_publication_adapter()
        core.node_id, core.node_address = node.node_id, node.address
        core.gcs_address, core._rpc = self.gcs_address, self.rpc

    def control_rpc(self, address, handler, request, **_options):
        try:
            return self._control_rpc(address, handler, request)
        except BaseException as exc:
            self.record_violation(exc)
            raise

    def record_violation(self, exc):
        try:
            self.violations.put_nowait(exc)
        except queue.Full:
            self.violation_overflow = True

    def assert_no_violations(self):
        assert not self.violation_overflow and self.violations.empty(), _snapshot_queue(self.violations)

    def _control_rpc(self, address, handler, request):
        self.calls.put_nowait((handler, request))
        if address == self.core.owner_address:
            methods = {
                wire.REGISTER_OUTPUT_HANDOFF_HANDLER: self.core.register_output_handoff,
                wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: self.core.report_output_handoff_complete,
                wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: self.core.report_output_handoff_rollback,
            }
            assert handler in methods
            reply = methods[handler](request)
            assert type(reply) is wire.OutputHandoffReply and reply.request == request and reply.accepted
            return reply
        assert address == self.gcs_address
        if handler == node_module.GCS_REGISTER_NODE_HANDLER:
            return self.registry.register_message(request)
        if handler == node_module.GCS_UPDATE_NODE_RESOURCES_HANDLER:
            self.registry.update_resources(
                request.node_id, request.node_pid, request.registration_epoch,
                request.report_seq, request.available_resources,
            )
            # Same acceptance semantics as GCSLite.update_node_resources: a
            # valid exact replay is acknowledged after the real registry call.
            return protocol.UpdateNodeResourcesReply(
                request.node_id, request.node_pid, request.registration_epoch, request.report_seq, True,
            )
        assert handler == "get_worker_deaths" and type(request) is protocol.GetWorkerDeaths
        assert request.after_epoch == 0
        return protocol.GetWorkerDeathsReply(0, 0, ())

    def rpc(self, address, handler, request):
        try:
            return self._rpc(address, handler, request)
        except BaseException as exc:
            self.record_violation(exc)
            raise

    def _rpc(self, address, handler, request):
        if address == self.gcs_address:
            return self.control_rpc(address, handler, request)
        assert address == self.node.address
        self.calls.put_nowait((handler, request))
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            return self.node._handle_ack_output_publication_adopted(request)
        assert handler == node_module.DROP_OBJECT_REPLICA_HANDLER
        if self.before_drop is not None:
            self.before_drop(request)
        reply = self.node._handle_drop_object_replica(request)
        self.drops.put_nowait((threading.current_thread(), request, reply))
        return reply

    def drain_submissions(self):
        for _ in range(16):
            try:
                item = self.core._submissions.get_nowait()
            except queue.Empty:
                assert self.core._submissions.unfinished_tasks == 0
                return
            try:
                assert item is _WAKE_COORDINATOR or item is self.retried
            finally:
                self.core._submissions.task_done()
        pytest.fail("bounded reconstruction submission tail exceeded 16 records")

    def prepare(self):
        core, node = self.core, self.node
        self.prepare_thread = threading.current_thread()
        pending, outputs = core._register_submission(
            core.define_remote_function(lambda: {"original": True}), (), {}, ResourceVector({"CPU": 1}),
            num_returns=1, max_retries=1, _enqueue=True,
        )
        self.pending = pending
        self.refs = (outputs,)
        assert core._submissions.get_nowait() is pending
        core._submissions.task_done()
        assert core._submissions.empty()
        request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, target_node_id=node.node_id, return_ids=pending.output_ids,
            requester_owner_address=core.owner_address,
        )
        grant = node._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease
        started = node._handle_start_worker_lease(protocol.StartWorkerLease(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
        ))
        assert started.accepted and started.node_incarnation is not None
        self.identity = OutputPublicationID(request.lease_id, pending.execution)
        discovery = OutputDiscoverySession(OutputPublicationHeader(
            self.identity, core.job_id, grant.worker_id, core.worker_id, started.node_incarnation,
        ), inline_threshold=0)
        outputs = discovery.discover(({"original": True},))
        assert all(slot.tier is protocol.ResultStorage.OBJECT_STORE and not slot.transfers for slot in outputs.manifest.slots)
        assert sum(slot.size_bytes for slot in outputs.manifest.slots) < 128
        assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            outputs.manifest, outputs.slot_payloads,
        )).accepted
        discovery.release_sources_after_promotions()
        complete = node._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
        ))
        assert complete.accepted and complete.released
        assert complete.state is protocol.LeaseExecutionState.COMPLETED and complete.output_publication is not None
        self.reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, grant.worker_id, complete.status,
            complete.output_publication.results, output_publication=complete.output_publication,
        )
        assert core._publish_reply(pending, self.reply, expected_node_id=node.node_id, expected_lease_id=request.lease_id)
        assert core._finish_pending_task(pending)
        assert node._output_publications.report_terminal(self.identity)
        node._flush_pending_resource_report()
        assert node.resource_ledger.available == node.resource_ledger.total
        assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.COMPLETED
        assert not node._output_publication_journal.snapshot(self.identity).retained_result_slots
        for ref, descriptor in zip(self.refs, self.reply.results):
            ready = core.owner_table.snapshot(ref.object_id)
            assert ready.state is ObjectState.READY_STORED and ready.canonical_stored_result == descriptor
            assert ready.output_publication.publication_id == self.identity
            assert core.drop_object(ref)  # real physical deletion followed by owner location removal
        assert node.object_store.used_bytes == 0
        assert all(core.owner_table.snapshot(object_id).state is ObjectState.LOST
                   and core.owner_table.snapshot(object_id).output_publication is not None
                   for object_id in pending.output_ids)
        handoff = core._output_handoff_table().query(self.identity)
        assert handoff.phase is OutputHandoffPhase.ADOPTED and handoff.complete == self.reply.output_publication.complete
        assert not core.owner_table._output_retirement_receipts
        assert not getattr(core, "_output_retirement_work", {})
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        self.drain_submissions()
        self.assert_no_violations()

    def state(self):
        core, pending = self.core, self.pending
        with core._state_lock:
            return (
                tuple(core.owner_table.snapshot(object_id) for object_id in pending.output_ids),
                replace(core._recovery.task_record(pending.task_id)),
                core._recovery.active_recovery(pending.task_id),
                tuple(core._reconstruction._sessions.items()), dict(core._stored_descriptors),
                dict(core._task_finish_barriers), dict(core._protocol_unresolved),
                core._accepted_task_count, _snapshot_queue(core._submissions),
                core._submissions.unfinished_tasks,
            )

    def finish_current_and_collect(self):
        """Normal post-assertion cleanup of an admitted but unexecuted attempt."""
        assert self.retried is not None and not any(thread.is_alive() for thread in self.threads)
        core = self.core
        error = SystemTaskError("unexecuted reconstruction fixture cleanup")
        assert core._publish_task_error(self.retried, error)
        assert core._finish_pending_task(self.retried)
        assert all(core.owner_table.snapshot(ref.object_id).state is ObjectState.ERROR
                   and core.owner_table.snapshot(ref.object_id).error is error for ref in self.refs)
        assert core._recovery.task_record(self.retried.task_id).state is TaskState.SYSTEM_FAILED
        assert core._recovery.active_recovery(self.retried.task_id) is None
        assert self.retried.task_id not in core._reconstruction._sessions
        for ref in self.refs:
            ref.close(timeout=0)
            assert ref._release_done.is_set()
        core._reference_mailbox.drain()
        self.drain_submissions()
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert not core._object_gc_obligations and not getattr(core, "_output_retirement_work", {})
        assert all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED for ref in self.refs)
        assert all(core._recovery.lineage_for_object(ref.object_id) is None for ref in self.refs)
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(self.retried.task_id)
        self.assert_no_violations()

    def close(self):
        live = tuple(thread for thread in self.threads if thread.is_alive())
        if live:
            # Do not reacquire owner/Node/mailbox locks held by a failed caller.
            # Detaching Python callbacks is not an owner reference release:
            # authority state remains untouched and this test must fail.
            for ref in self.refs:
                if ref._finalizer is not None:
                    ref._finalizer.detach()
            pytest.fail("bounded reconstruction threads still live: " + repr(tuple(t.name for t in live)))
        core = self.core
        self.before_drop = None
        errors = []
        try:
            for ref in self.refs:
                try:
                    ref.close(timeout=0)  # actual synchronous local receipt, never fabricated terminality
                    assert ref._release_done.is_set()
                except BaseException as exc:
                    errors.append(exc)
        finally:
            # Failure cleanup does not pretend a stalled/partial request
            # reached terminal error or GC. Only the normal method above owns
            # those assertions, after all reconstruction checks have passed.
            close_pure_core(core)
        assert not errors, errors
        self.assert_no_violations()


@contextmanager
def _concurrent_reconstruction(case, object_ids, allowed_threads):
    assert len(object_ids) == 2
    core, node = case.core, case.node
    barrier = threading.Barrier(3, timeout=1.0)
    retirement_entered, release_retirement = threading.Event(), threading.Event()
    follower_deferred, first_enqueued = threading.Event(), threading.Event()
    records = queue.Queue(maxsize=8)
    commits = queue.Queue(maxsize=4)
    enqueues = queue.Queue(maxsize=2)
    original_enqueue = core._enqueue_reconstruction_task
    coordinator = core._reconstruction_coordinator()
    original_commit = coordinator.commit_prepared
    gated = False

    def before_drop(request):
        nonlocal gated
        if request.object_id != case.pending.output_ids[0]:
            return
        assert not gated and threading.current_thread() is case.threads[0]
        assert not core._state_lock._is_owned() and not node._state_lock._is_owned()
        assert not node._output_publication_journal._lock._is_owned()
        with core._state_lock:
            assert request.object_id in core._output_retirement_tickets
            snapshot = core.owner_table.snapshot(request.object_id)
            assert snapshot.state is ObjectState.LOST and snapshot.output_retirement_id is not None
            assert snapshot.current_attempt == case.pending.spec.attempt_id
        gated = True
        records.put_nowait(("retirement-held", request.object_id))
        retirement_entered.set()
        assert release_retirement.wait(1.0), "retirement gate was not released"

    def commit(prepared):
        outcome = original_commit(prepared)
        commits.put_nowait(outcome)
        return outcome

    def enqueue(pending):
        original_enqueue(pending)
        case.retried = pending
        enqueues.put_nowait(pending)
        records.put_nowait(("enqueued", pending))
        first_enqueued.set()  # only the real FIFO publication can signal this

    def request(index, object_id):
        try:
            barrier.wait(timeout=1.0)
            if index == 1:
                assert retirement_entered.wait(1.0), "leader did not reach retirement"
            outcome = core._start_or_join_reconstruction(
                object_id, core._objects[object_id], return_requested_outcome=True,
            )
            if index == 0:
                records.put_nowait(("leader", outcome))
            else:
                records.put_nowait(("deferred", outcome))
                assert outcome is None, "contended retirement must genuinely defer"
                follower_deferred.set()
                assert first_enqueued.wait(1.0), "leader did not really enqueue"
                joined = core._start_or_join_reconstruction(
                    object_id, core._objects[object_id], return_requested_outcome=True,
                )
                records.put_nowait(("follower", joined))
        except BaseException as exc:
            records.put_nowait(("error", exc))

    case.threads = tuple(threading.Thread(
        target=request, args=(index, object_id), daemon=True,
        name="miniray-test-reconstruction-{}".format(index),
    ) for index, object_id in enumerate(object_ids))
    allowed_threads.extend(case.threads)
    case.before_drop = before_drop
    core._enqueue_reconstruction_task = enqueue
    coordinator.commit_prepared = commit
    try:
        for thread in case.threads:
            thread.start()
        barrier.wait(timeout=1.0)
        deadline = time.monotonic() + 2.0
        assert retirement_entered.wait(min(1.0, max(0.0, deadline - time.monotonic())))
        assert follower_deferred.wait(min(1.0, max(0.0, deadline - time.monotonic())))
        with core._state_lock:
            assert core._accepted_task_count == 0 and core._submissions.empty()
            assert core._recovery.task_record(case.pending.task_id).retries_started == 0
            assert core._recovery.active_recovery(case.pending.task_id) is None
            assert all(core.owner_table.snapshot(value).state is ObjectState.LOST for value in case.pending.output_ids)
            assert len(_snapshot_queue(case.drops)) == len(case.pending.output_ids)
        release_retirement.set()
        for thread in case.threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in case.threads), "reconstruction exceeded its shared 2 s join bound"
        observed = _snapshot_queue(records)
        assert not any(kind == "error" for kind, _ in observed), observed
        assert len(observed) == 5 and dict(observed)["deferred"] is None
        assert dict(observed)["leader"].disposition is ReconstructionDisposition.START
        assert dict(observed)["follower"].disposition is ReconstructionDisposition.JOIN
        assert [outcome.disposition for outcome in _snapshot_queue(commits)] == [
            ReconstructionDisposition.START, ReconstructionDisposition.JOIN,
        ]
        assert _snapshot_queue(enqueues) == (case.retried,)
        assert _snapshot_queue(core._submissions) == (case.retried,)
        assert core._submissions.unfinished_tasks == 1
        assert coordinator._sessions[case.pending.task_id].attempt_id == case.retried.spec.attempt_id
        assert dict(observed)["follower"].decision.attempt_id == case.retried.spec.attempt_id
        drops = _snapshot_queue(case.drops)
        count = len(case.pending.output_ids)
        assert len(drops) == 2 * count
        for output_id, first, retired_drop in zip(case.pending.output_ids, drops[:count], drops[count:]):
            first_thread, first_request, first_reply = first
            retire_thread, retire_request, retire_reply = retired_drop
            assert first_thread is case.prepare_thread and retire_thread is case.threads[0]
            assert first_request == retire_request and first_request.object_id == output_id
            assert first_reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert retire_reply.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
            for request, reply in ((first_request, first_reply), (retire_request, retire_reply)):
                assert type(reply) is protocol.DropObjectReplicaReply
                assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
                    request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
                )
                assert request.producer_attempt_id == case.pending.spec.attempt_id
                assert request.owner_worker_id == core.worker_id and request.node_id == node.node_id
                assert reply.error is None
        receipts = tuple(core.owner_table._output_retirement_receipts.values())
        assert len(receipts) == 1
        receipt, = receipts
        assert tuple(member.object_id for member in receipt.plan.memberships) == case.pending.output_ids
        assert receipt.released_edges == ()
        assert receipt.dropped_replicas == tuple(reply for _, _, reply in drops[count:])
        assert core.owner_table.output_publication_retirement_receipt(receipt.plan).plan == receipt.plan
        assert core._output_handoff_table().query(case.identity).complete == case.reply.output_publication.complete
        assert not core._output_retirement_work and not core._output_retirement_tickets
        case.assert_no_violations()
        yield core._submissions
    finally:
        barrier.abort()
        release_retirement.set()
        deadline = time.monotonic() + 1.0
        for thread in case.threads:
            if thread.ident is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        live = tuple(thread.name for thread in case.threads if thread.is_alive())
        if not live:
            case.before_drop = None
            core._enqueue_reconstruction_task = original_enqueue
            coordinator.commit_prepared = original_commit
        assert not live, "bounded reconstruction threads did not stop: {!r}".format(live)


def _assert_one_admission_and_stale_fence(case, captured):
    core, original = case.core, case.pending
    queued = case.retried
    assert type(queued) is _PendingTask
    assert queued.task_id == original.task_id and queued.output_ids == original.output_ids
    assert queued.spec.attempt_id == original.spec.attempt_id.next()
    assert len(queued.output_ids) == 1
    assert core._accepted_task_count == 1
    assert core._recovery.task_record(original.task_id).retries_started == 1
    assert core._recovery.active_recovery(original.task_id) == queued.spec.attempt_id
    assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING
               and core.owner_table.snapshot(output).current_attempt == queued.spec.attempt_id
               and core.owner_table.snapshot(output).output_publication is None
               and core.owner_table.snapshot(output).canonical_stored_result is None
               and not core._objects[output].event.is_set()
               for output in original.output_ids)
    assert not core._stored_descriptors
    before = case.state()
    bridge_before = _snapshot_queue(case.calls)
    # This is the actual initial stored Complete envelope, not an obsolete raw
    # success DTO whose shape rejection could masquerade as attempt fencing.
    assert not core._publish_reply(
        original, case.reply, expected_node_id=case.node.node_id, expected_lease_id=case.identity.lease_id,
    )
    assert case.state() == before
    assert _snapshot_queue(case.calls) == bridge_before
    assert captured.get_nowait() is queued
    captured.task_done()
    assert captured.empty() and captured.unfinished_tasks == 0


def test_concurrent_lost_requests_merge_and_old_attempt_is_fenced(allowed_request_threads) -> None:
    case = _Case()
    try:
        case.prepare()
        with _concurrent_reconstruction(case, (case.pending.object_id,) * 2, allowed_request_threads) as captured:
            _assert_one_admission_and_stale_fence(case, captured)
        case.finish_current_and_collect()
    finally:
        case.close()
