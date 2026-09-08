"""Mixed contained-reference contracts with explicit safety classification.

The Worker/Node publication, pending-outer-close, failed-edge-release,
post-finish stale-reply and shutdown-GC-precheck cases are unit. Each of the
latter four uses two threadless Cores, one Node, one canonical Task, one tiny
put and one actual contained edge. The selected INLINE slot is discovered with
a 4 KiB threshold; the separate 1 KiB store stays empty.
Pending close cannot collect until publication installs the edge and the real
Task finish releases its barrier. The release-failure case additionally loses
the first Release before its effect and manually delivers one retry. All four
drive real child release, graph retirement, slot report and owner collection.
The shutdown-GC case invokes only the synchronous cleanup precheck: it finishes
the retained obligation before delivering the old scheduled GC notice as a
no-op. It does not exercise or claim public Core/ray shutdown.
No user Task, process, thread, timer, socket, wait or Queue.join runs there.

The stale-reply case replaces the obsolete
test_stale_reply_edges_are_released_as_orphan_obligations contract. A late
duplicate of an already-adopted, finished publication does not authorize
releasing its still-live child edge. This is not a prior-attempt reconstruction
or an unadopted-publication cleanup test; those use Node/GCS publication proof,
never raw TaskReply edges. No historical orphan-release assertion is claimed.

The obsolete PENDING-or-STORED no-collection case is replaced with a pure
finish-barrier/real-STORED-GC contract. The same-owner wake/recursive release
case is separately L1: one real reference consumer and the main publisher
share the same Core and RLock. It uses no network/Worker process, and does not
claim arbitrary concurrent schedules. Bounded joins do not cancel lock waits;
the exact L1 case needs the external 30-second runner.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import core as core_module, node as node_module, output_protocol as wire, protocol
from miniray.contained_cycle import ContainedReferenceGraphAuthority
from miniray.contained_edges import (
    ContainedReferenceEdge, ContainedReferenceHold,
)
from miniray.core import CoreWorker, _ObjectWaiter, _PendingTask, _RetryInlineGc, _WAKE_COORDINATOR
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan
from miniray.recovery import TaskState
from miniray.resources import NodeSnapshot, ResourceLedger, ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core


def _core(worker_id: WorkerID | None = None) -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = worker_id or WorkerID.random()
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 26001)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core._owner_table = ObjectOwnerTable()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._inline_gc_obligations = {}
    core._owner_protocol_open = True
    core._inflight_borrow_ops = 0
    core._initialize_reference_events()
    return core


def _spec(core: CoreWorker, object_id: ObjectID) -> protocol.TaskSpec:
    return protocol.TaskSpec(
        job_id=core.job_id, task_id=object_id.task_id,
        attempt_id=AttemptID(object_id.task_id, 0),
        function=protocol.FunctionKey(core.job_id, __name__, "f", "v1"),
        args=(), num_returns=1, resources=ResourceVector(),
        owner_worker_id=core.worker_id,
    )


def _pending_outer(core: CoreWorker) -> tuple[_PendingTask, object]:
    task_id = TaskID.derive(core.job_id, core.driver_task_id, 0)
    object_id = ObjectID.for_task(task_id)
    spec = _spec(core, object_id)
    core.owner_table.register(
        object_id, current_attempt=spec.attempt_id, producer_task_spec=spec
    )
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    ref = core._new_object_ref(object_id)
    return _PendingTask(object_id, spec), ref


def _reply(
    pending: _PendingTask, worker: WorkerID, node: NodeID,
    edge: ContainedReferenceEdge,
) -> protocol.TaskReply:
    payload = cloudpickle.dumps({"child": "logical-handle-bytes"})
    descriptor = protocol.ResultDescriptor(
        pending.object_id, protocol.ResultStorage.INLINE, len(payload),
        pending.spec.owner_worker_id, node, hashlib.sha256(payload).hexdigest(),
        payload,
    )
    return protocol.TaskReply(
        pending.spec.task_id, pending.spec.attempt_id, worker,
        protocol.TaskReplyStatus.SUCCEEDED, (descriptor,), None, (edge,),
    )


@pytest.mark.unit
@pytest.mark.usefixtures("_no_edge_retry_runtime")
def test_pending_outer_close_collects_only_after_reply_installs_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _EdgeRetryFixture(monkeypatch, fail_first_release=False)
    core, child_owner = f.core, f.child_owner
    try:
        f.register()
        pending, outer_ref = f.pending, f.ref
        _edge_release_local(outer_ref)  # preserve the original pre-reply close
        _edge_drain_notices(core, pending.object_id)
        before = core.owner_table.snapshot(pending.object_id)
        assert before.state is ObjectState.PENDING and not before.local_tokens
        assert not before.outgoing_contained_edges and before.output_publication is None
        assert not before.collection_pending and not core._object_gc_obligations
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {pending.object_id: pending}

        reply = f.complete()
        identity = reply.output_publication.publication_id
        assert core.owner_table.snapshot(pending.object_id) == before
        assert child_owner.owner_table.snapshot(f.child_id).contained_holds == frozenset({f.transfer.final_hold})
        assert not child_owner.owner_table.snapshot(f.child_id).local_tokens
        assert not f.release_calls and not f.graph_releases and not f.slot_reports
        original_wake = core._wake_object
        observed = []

        def observe_installed_edge(object_id):
            assert object_id == pending.object_id and not observed
            published = core.owner_table.snapshot(object_id)
            assert published.state is ObjectState.READY_INLINE
            assert published.outgoing_contained_edges == frozenset({f.edge})
            assert published.output_publication.manifest == reply.output_publication.manifest
            assert child_owner.owner_table.snapshot(f.child_id).contained_holds == frozenset({f.transfer.final_hold})
            assert f.graph.snapshot().committed_edges == (f.edge,)
            assert not core._objects[object_id].event.is_set()
            assert not f.release_calls
            observed.append(published.outgoing_contained_edges)
            original_wake(object_id)

        monkeypatch.setattr(core, "_wake_object", observe_installed_edge)
        assert core._publish_reply(
            pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        assert observed == [frozenset({f.edge})]
        assert core._objects[pending.object_id].event.is_set()
        assert core.owner_table.snapshot(pending.object_id).current_attempt == pending.spec.attempt_id
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
        assert record.current_attempt == pending.spec.attempt_id
        assert not core._protocol_unresolved
        assert not f.journal.snapshot(identity).retained_result_slots
        # A READY result may be read, but its dispatched execution still owns
        # the finish barrier. Earlier release/READY notices cannot skip it.
        _edge_drain_notices(core, pending.object_id)
        assert core.owner_table.contains(pending.object_id)
        assert child_owner.owner_table.contains(f.child_id)
        assert not f.release_calls and not f.delayed
        assert core._finish_pending_task(pending) and core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert _edge_take_submissions(core) == ()
        _edge_drain_notices(core, pending.object_id)
        assert len(f.release_calls) == len(f.released) == 1
        assert f.release_calls[0].hold == f.transfer.final_hold
        assert len(f.graph_releases) == len(f.slot_reports) == 1
        assert not core.owner_table.contains(pending.object_id)
        assert not child_owner.owner_table.contains(f.child_id)
        assert child_owner.owner_table.contained_release_was_seen(
            f.child_id, f.release_calls[0].hold,
        )
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert child_owner.owner_table.collection_state(f.child_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert not f.graph.has_active_obligations()
        assert not f.graph.snapshot().prepared_edges and not f.graph.snapshot().committed_edges
        assert len(f.recovery.snapshot(identity).slot_collections) == 1
        assert f.adapter.report_terminal(identity)
        assert not f.adapter.pending_terminal_reports() and not f.adapter.pending_lease_completions()
        assert not f.adapter.pending_rollbacks() and not f.delayed
        with f.node._state_lock:
            assert f.node._output_publications_clean_locked()
        assert f.node.resource_ledger.available == f.node.resource_ledger.total
        assert f.node.object_store.used_bytes == 0
        for owner in (core, child_owner):
            assert not owner._objects and not owner._stored_descriptors and not owner._object_gc_obligations
            assert _edge_take_submissions(owner) == ()
            assert owner._reference_mailbox.pending.empty()
            assert owner._reference_mailbox.pending.unfinished_tasks == 0
            assert len(owner._reference_mailbox.releases) == 1
    finally:
        f.close()


@pytest.fixture
def _no_edge_retry_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure edge-release retry attempted runtime work")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
        (queue.Queue, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


def _never_run_edge_task():
    pytest.fail("pure edge-release fixture executed user Task")


def _edge_release_local(ref):
    """Drive the actual close/GC finalizer without an Event.wait call."""
    done, finalizer = ref._release_done, ref._finalizer
    assert done is not None and finalizer is not None
    ref._closed = True
    finalizer()
    assert done.is_set() and not finalizer.alive


def _edge_take_submissions(core):
    size = core._submissions.qsize()
    assert size <= 8
    tasks = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            assert type(item) is _PendingTask
            tasks.append(item)
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    return tuple(tasks)


def _edge_drain_notices(core, object_id):
    fifo = core._reference_mailbox.pending
    size = fifo.qsize()
    assert size <= 8
    for _ in range(size):
        event = fifo.get_nowait()
        try:
            assert type(event) is _RetryInlineGc and event.object_id == object_id
            core._reference_released(event.object_id)
        finally:
            fifo.task_done()
    assert fifo.empty() and fifo.unfinished_tasks == 0


class _EdgeRetryFixture:
    """One canonical outer, one executor-owned put, one selected output."""

    def __init__(self, monkeypatch, *, fail_first_release=True):
        assert type(fail_first_release) is bool
        self.fail_first_release = fail_first_release
        self.core, self.child_owner = make_pure_core(), make_pure_core()
        core, child = self.core, self.child_owner
        child.node_id, child.node_address = core.node_id, core.node_address
        core.owner_address = ("edge-outer-owner.invalid", 1)
        child.owner_address = ("edge-child-owner.invalid", 1)
        core.gcs_address = ("edge-control.invalid", 1)
        self.refs = []
        self.pending = self.ref = self.child_ref = None
        self.session = self.outputs = self.envelope = None
        self.transfer = self.edge = self.grant = self.request = None
        self.release_calls, self.released, self.graph_releases, self.slot_reports = [], [], [], []
        self.prepares, self.promotions, self.controls, self.delayed = [], [], [], []
        self.graph = ContainedReferenceGraphAuthority()
        self.recovery, self.journal = OutputPublicationRecoveryAuthority(), OutputPublicationJournal()
        self.failure = RuntimeError("unreachable")
        self.node = node = object.__new__(NodeServer)
        node.node_id, node.worker_id = core.node_id, child.worker_id
        node._node_pid, node._registration_epoch = 31901, 1
        node._gcs_address, node._registered_with_gcs = None, False
        node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
        node._cluster_addresses = {}
        node._shutdown_request_id, node._active_lease_id = None, None
        node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
        node._lease_request_locks, node._inflight_lease_requests = {}, 0
        node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
        node._stop_event = threading.Event()
        node._worker_process = type("PassiveWorker", (), {"is_alive": lambda _self: True})()
        node._worker_address = ("edge-executor.invalid", 1)
        node._object_store = ObjectStore(1024)
        node._sealed_metadata = {}
        node.event_sink = None
        node._output_publication_journal = self.journal

        def forbidden(*_args, **_kwargs):
            pytest.fail("one INLINE edge attempted physical store or rollback")

        self.adapter = node._output_publications = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent,
            arm_complete=self.recovery.arm_complete, report_terminal=self.recovery.report_terminal,
            report_rollback=self.recovery.report_rollback, prepare_child=self.prepare_child,
            promote_child=self.promote_child, release_child=forbidden,
            prepare_graph=self.prepare_graph, abort_graph=forbidden,
            seal_replica=forbidden, drop_replica=forbidden,
        )
        monkeypatch.setattr(core, "_rpc", self.rpc)
        monkeypatch.setattr(core, "_borrow_rpc", self.release_child)
        monkeypatch.setattr(core, "_schedule_reference_event", self.delay_gc)

    def register(self):
        self.child_ref = self.child_owner.put(7)
        self.refs.append((self.child_owner, self.child_ref))
        self.child_id = self.child_ref.object_id
        assert self.child_owner.get(self.child_ref, timeout=0) == 7
        assert self.child_owner.owner_table.snapshot(self.child_id).state is ObjectState.READY_INLINE
        assert _edge_take_submissions(self.child_owner) == ()
        self.pending, self.ref = self.core._register_submission(
            self.core.define_remote_function(_never_run_edge_task), (), {},
            ResourceVector({"CPU": 1}), _enqueue=True,
        )
        self.refs.append((self.core, self.ref))
        assert _edge_take_submissions(self.core) == (self.pending,)
        assert self.core._accepted_task_count == 1
        assert self.core._task_finish_barriers == {self.pending.object_id: self.pending}

    def prepare_child(self, address, request):
        assert address == self.child_owner.owner_address and not self.prepares
        assert request.transfer == self.transfer
        assert self.session.source_references == (self.child_ref,)
        reply = self.child_owner.prepare_stored_contained_pin(request)
        assert reply.accepted
        self.prepares.append((request, reply))
        assert self.child_owner.owner_table.snapshot(self.child_id).contained_holds == frozenset({self.transfer.provisional_hold})
        return reply

    def promote_child(self, address, request):
        assert address == self.child_owner.owner_address and not self.promotions
        assert request.transfer == self.transfer
        reply = self.child_owner.promote_stored_contained_pin(request)
        assert reply.accepted
        self.promotions.append((request, reply))
        assert self.child_owner.owner_table.snapshot(self.child_id).contained_holds == frozenset({self.transfer.final_hold})
        return reply

    def prepare_graph(self, request):
        assert request.manifest == self.outputs.manifest.to_graph_manifest()
        receipt = self.graph.prepare_manifest(request.manifest)
        assert self.graph.snapshot().prepared_edges == (self.edge,)
        return protocol.ContainedGraphReply(request, receipt)

    def complete(self):
        pending, node = self.pending, self.node
        self.request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            self.core.node_id, self.core.worker_id, target_node_id=node.node_id,
            return_ids=pending.output_ids,
        )
        self.grant = node._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease
        assert node.resource_ledger.available.is_zero()
        started = node._handle_start_worker_lease(protocol.StartWorkerLease(
            self.grant.lease_id, pending.task_id, pending.spec.attempt_id, self.grant.worker_id,
        ))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        identity = OutputPublicationID(self.grant.lease_id, pending.execution)
        self.session = OutputDiscoverySession(OutputPublicationHeader(
            identity, pending.spec.job_id, self.grant.worker_id, self.core.worker_id,
            OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
        ), inline_threshold=4096)
        self.outputs = self.session.discover(({"child": self.child_ref},))
        (slot,) = self.outputs.manifest.slots
        assert slot.tier is protocol.ResultStorage.INLINE and slot.size_bytes <= 4096
        (self.transfer,), (self.edge,) = slot.transfers, slot.edges
        assert self.edge.container_object_id == pending.object_id
        assert self.transfer.contained_object_id == self.child_id
        assert self.transfer.contained_owner_worker_id == self.child_owner.worker_id
        assert self.transfer.final_hold.container_owner_worker_id == self.core.worker_id
        prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            self.outputs.manifest, self.outputs.slot_payloads,
        ))
        assert prepared.accepted and self.recovery.snapshot(identity).armed
        assert self.journal.snapshot(identity).ready_to_complete
        assert len(self.prepares) == len(self.promotions) == 1
        self.session.release_sources_after_promotions()
        _edge_release_local(self.child_ref)
        _edge_drain_notices(self.child_owner, self.child_id)
        child = self.child_owner.owner_table.snapshot(self.child_id)
        assert not child.local_tokens and child.contained_holds == frozenset({self.transfer.final_hold})
        completed = node._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            self.grant.lease_id, pending.task_id, pending.spec.attempt_id, self.grant.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED,
        ))
        assert completed.accepted and completed.released
        self.envelope = completed.output_publication
        assert self.envelope.manifest == self.outputs.manifest
        assert node.resource_ledger.available == node.resource_ledger.total
        assert node.object_store.used_bytes == 0
        return protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, self.grant.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, self.envelope.results, output_publication=self.envelope,
        )

    def rpc(self, address, handler, request):
        self.controls.append((handler, request))
        assert len(self.controls) <= 6
        identity = self.envelope.publication_id
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.core.gcs_address
            if type(request) is wire.ReportOutputPublicationTerminal:
                assert request.witness == self.envelope.complete
                ack = self.recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                receipt = self.core.owner_table.output_owner_publication_receipt(
                    OutputOwnerPublicationPlan(self.pending.execution, self.envelope),
                )
                assert receipt is not None and receipt.committed
                ack = self.recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                assert len(self.released) == len(self.graph_releases) == 1 and not self.slot_reports
                assert self.released[0].accepted and self.released[0].released
                assert not self.child_owner.owner_table.contains(self.child_id)
                assert request.proof.complete == self.envelope.complete
                assert request.proof.object_id == self.pending.object_id and request.proof.slot_index == 0
                self.slot_reports.append(request)
                ack = self.recovery.report_slot_collected(request.proof)
            return wire.OutputRecoveryReply(request, ack)
        if handler == "commit_contained_graph":
            assert address == self.core.gcs_address
            assert request.manifest == self.envelope.manifest.to_graph_manifest()
            return protocol.ContainedGraphReply(request, self.graph.commit_manifest(request.manifest))
        if handler == "release_contained_graph_container":
            assert address == self.core.gcs_address
            assert len(self.release_calls) == 1 + int(self.fail_first_release)
            assert len(self.released) == 1 and not self.graph_releases
            assert request.manifest == self.envelope.manifest.to_graph_manifest()
            assert request.container_object_id == self.pending.object_id
            assert self.child_owner.owner_table.contained_release_was_seen(self.child_id, self.transfer.final_hold)
            receipt = self.graph.release_manifest_container(request.manifest, request.container_object_id)
            self.graph_releases.append(receipt)
            assert not self.graph.snapshot().committed_edges
            return protocol.ContainedGraphReply(request, receipt)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert address == self.core.node_address
        assert self.recovery.snapshot(identity).adopted == request.proof
        return self.node._handle_ack_output_publication_adopted(request)

    def release_child(self, address, handler, request):
        assert address == self.child_owner.owner_address and handler == "release_contained_reference"
        assert type(request) is protocol.ReleaseContainedReference
        assert len(self.release_calls) < 1 + int(self.fail_first_release)
        assert request.object_id == self.child_id and request.owner_worker_id == self.child_owner.worker_id
        assert request.hold == self.transfer.final_hold
        self.release_calls.append(request)
        obligation = self.core._object_gc_obligations[self.pending.object_id]
        assert obligation.pending_edges == {self.edge}
        assert obligation.graph_release_receipt is None and not obligation.output_cleanup_reported
        assert self.core.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTING
        assert self.graph.snapshot().committed_edges == (self.edge,)
        assert not self.graph_releases and not self.slot_reports
        if self.fail_first_release and len(self.release_calls) == 1:
            self.child_before_failure = self.child_owner.owner_table.snapshot(self.child_id)
            assert self.child_before_failure.contained_holds == frozenset({self.transfer.final_hold})
            # Preserve the original pre-effect failure; no child release has
            # happened, and no ACK or release tombstone is fabricated.
            raise self.failure
        assert request == self.release_calls[0]
        if self.fail_first_release:
            assert self.child_owner.owner_table.snapshot(self.child_id) == self.child_before_failure
        else:
            assert self.child_owner.owner_table.snapshot(self.child_id).contained_holds == frozenset({self.transfer.final_hold})
        reply = self.child_owner.release_contained_reference(request)
        assert reply.accepted and reply.released
        self.released.append(reply)
        assert not self.child_owner.owner_table.contains(self.child_id)
        assert self.child_owner.owner_table.contained_release_was_seen(self.child_id, request.hold)
        return reply

    def delay_gc(self, mailbox, event, delay):
        assert self.fail_first_release, "zero-failure collection must not schedule a retry"
        assert mailbox is self.core._reference_mailbox and not self.delayed
        assert type(event) is _RetryInlineGc and event.object_id == self.pending.object_id
        assert 0 < delay <= 0.25 and len(self.release_calls) == 1
        # Record the one timer delivery, not a replacement collection result.
        # The test explicitly enqueues this same event for the next GC turn.
        self.delayed.append((mailbox, event, delay))

    def close(self):
        for _core, ref in self.refs:
            _edge_release_local(ref)
        close_pure_core(self.core)
        close_pure_core(self.child_owner)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_edge_retry_runtime")
def test_failed_edge_release_freezes_outer_metadata_until_retry_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _EdgeRetryFixture(monkeypatch)
    core = f.core
    try:
        f.register()
        pending, ref = f.pending, f.ref
        # The original case closes the outer before _publish_reply; retain
        # that order while using its actual finalizer instead of waiting.
        _edge_release_local(ref)
        _edge_drain_notices(core, pending.object_id)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        reply = f.complete()
        assert core._publish_reply(
            pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        identity = reply.output_publication.publication_id
        published = core.owner_table.snapshot(pending.object_id)
        assert published.state is ObjectState.READY_INLINE and not published.local_tokens
        assert published.outgoing_contained_edges == frozenset({f.edge})
        assert published.output_publication.manifest == reply.output_publication.manifest
        assert f.graph.snapshot().committed_edges == (f.edge,)
        assert not f.journal.snapshot(identity).retained_result_slots
        # Runtime dispatch owns this real finish barrier. Drain earlier READY
        # notices while it is present, then consume its one terminal GC notice.
        _edge_drain_notices(core, pending.object_id)
        assert not f.release_calls
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert _edge_take_submissions(core) == ()
        _edge_drain_notices(core, pending.object_id)
        assert len(f.release_calls) == 1 and not f.released
        assert len(f.delayed) == 1
        obligation = core._object_gc_obligations[pending.object_id]
        frozen_plan = obligation.plan
        frozen = core.owner_table.snapshot(pending.object_id)
        assert frozen.collection_pending and frozen.state is ObjectState.READY_INLINE
        assert frozen.outgoing_contained_edges == frozenset({f.edge})
        assert frozen.output_publication == published.output_publication
        assert frozen.producer_task_spec == pending.spec
        assert frozen_plan.producer_task_spec == pending.spec and frozen_plan.contained_releases == (f.edge,)
        assert obligation.pending_edges == {f.edge} and not obligation.pending_drops
        assert obligation.retry_scheduled and obligation.retry_round == 1
        assert obligation.graph_release_receipt is None and not obligation.output_cleanup_reported
        assert core._recovery.lineage_for_object(pending.object_id) is not None
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert f.child_owner.owner_table.snapshot(f.child_id) == f.child_before_failure
        assert not f.child_owner.owner_table.contained_release_was_seen(f.child_id, f.transfer.final_hold)
        assert f.graph.snapshot().committed_edges == (f.edge,) and not f.graph_releases
        assert f.recovery.snapshot(identity).adopted is not None
        assert not f.recovery.snapshot(identity).slot_collections and not f.slot_reports

        mailbox, event, _delay = f.delayed[0]
        assert mailbox.enqueue_internal(event)
        assert core._object_gc_obligations[pending.object_id] is obligation
        assert obligation.plan == frozen_plan
        _edge_drain_notices(core, pending.object_id)
        assert len(f.release_calls) == 2 and f.release_calls[0] == f.release_calls[1]
        assert len(f.released) == len(f.graph_releases) == len(f.slot_reports) == 1
        assert f.slot_reports[0].proof.cleanup_id == frozen_plan.collection_id
        assert not core.owner_table.contains(pending.object_id)
        assert pending.object_id not in core._inline_gc_obligations
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert f.child_owner.owner_table.collection_state(f.child_id) is ObjectCollectionState.COLLECTED
        assert not f.graph.has_active_obligations()
        assert not f.graph.snapshot().prepared_edges and not f.graph.snapshot().committed_edges
        assert len(f.recovery.snapshot(identity).slot_collections) == 1
        assert f.adapter.report_terminal(identity)
        assert not f.adapter.pending_terminal_reports() and not f.adapter.pending_lease_completions()
        assert f.node.resource_ledger.available == f.node.resource_ledger.total
        assert f.node.object_store.used_bytes == 0
        for owner in (core, f.child_owner):
            assert not owner._objects and not owner._stored_descriptors and not owner._object_gc_obligations
            assert _edge_take_submissions(owner) == ()
            assert owner._reference_mailbox.pending.empty()
            assert owner._reference_mailbox.pending.unfinished_tasks == 0
            assert len(owner._reference_mailbox.releases) == 1
        replay = f.child_owner.release_contained_reference(f.release_calls[0])
        assert replay.accepted and not replay.released
        assert len(f.delayed) == 1
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_edge_retry_runtime")
def test_stale_reply_cannot_release_committed_publication_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-finish duplicate is not an orphan-pin release capability."""
    f = _EdgeRetryFixture(monkeypatch, fail_first_release=False)
    core, child = f.core, f.child_owner
    try:
        f.register()
        pending, ref = f.pending, f.ref
        reply = f.complete()
        assert core._publish_reply(
            pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        identity = reply.output_publication.publication_id
        _edge_drain_notices(core, pending.object_id)
        assert core._finish_pending_task(pending)
        assert _edge_take_submissions(core) == ()
        _edge_drain_notices(core, pending.object_id)
        assert pending.task_key in core._finished_tasks
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert not core._protocol_unresolved
        owned = core.owner_table.snapshot(pending.object_id)
        child_owned = child.owner_table.snapshot(f.child_id)
        assert owned.state is ObjectState.READY_INLINE and not owned.collection_pending
        assert owned.current_attempt == pending.spec.attempt_id
        assert owned.local_tokens == frozenset({ref._local_token})
        assert owned.inline_data == reply.output_publication.results[0].inline_data
        assert owned.outgoing_contained_edges == frozenset({f.edge})
        assert owned.output_publication.manifest == reply.output_publication.manifest
        assert child_owned.state is ObjectState.READY_INLINE and not child_owned.local_tokens
        assert child_owned.contained_holds == frozenset({f.transfer.final_hold})
        assert not child.owner_table.contained_release_was_seen(f.child_id, f.transfer.final_hold)
        assert f.graph.snapshot().committed_edges == (f.edge,)
        assert f.recovery.snapshot(identity).adopted is not None
        assert not f.recovery.snapshot(identity).slot_collections
        assert not f.journal.snapshot(identity).retained_result_slots
        recovery_before = replace(core._recovery.task_record(pending.task_id))
        lineage_before = core._recovery.lineage_for_object(pending.object_id)
        assert recovery_before.state is TaskState.SUCCEEDED and recovery_before.retries_started == 0
        assert recovery_before.current_attempt == pending.spec.attempt_id and lineage_before is not None
        publication_before = (f.graph.snapshot(), f.recovery.snapshot(identity), f.journal.snapshot(identity))
        history_before = (
            tuple(f.prepares), tuple(f.promotions), tuple(f.controls),
            tuple(f.release_calls), tuple(f.released), tuple(f.graph_releases), tuple(f.slot_reports),
        )
        assert not f.release_calls and not f.delayed
        detached_reply = deepcopy(reply)
        assert detached_reply == reply and detached_reply is not reply
        assert detached_reply.output_publication is not reply.output_publication

        # Staleness is the real completed Task finish, not a fabricated next
        # attempt. Both exact and detached deliveries must preserve the same
        # currently needed edge while the outer's local reference is live.
        for duplicate in (reply, detached_reply):
            assert not core._publish_reply(
                pending, duplicate, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
            )
            assert core.owner_table.snapshot(pending.object_id) == owned
            assert child.owner_table.snapshot(f.child_id) == child_owned
            assert core._recovery.task_record(pending.task_id) == recovery_before
            assert core._recovery.lineage_for_object(pending.object_id) == lineage_before
            assert (f.graph.snapshot(), f.recovery.snapshot(identity), f.journal.snapshot(identity)) == publication_before
            assert (
                tuple(f.prepares), tuple(f.promotions), tuple(f.controls),
                tuple(f.release_calls), tuple(f.released), tuple(f.graph_releases), tuple(f.slot_reports),
            ) == history_before
            assert not core._protocol_unresolved and not core._object_gc_obligations
            assert not getattr(core, "_output_result_custody", {})
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
            assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
            assert not f.delayed
        assert not hasattr(core, "_orphan_contained_edges")
        assert not hasattr(core, "_retain_orphan_contained_edges")

        # The last real outer reference, rather than the stale reply, grants
        # normal owner GC its frozen release/graph/slot cleanup obligation.
        _edge_release_local(ref)
        _edge_drain_notices(core, pending.object_id)
        assert len(f.release_calls) == len(f.released) == len(f.graph_releases) == len(f.slot_reports) == 1
        assert f.release_calls[0].hold == f.transfer.final_hold
        assert child.owner_table.contained_release_was_seen(f.child_id, f.transfer.final_hold)
        assert not core.owner_table.contains(pending.object_id) and not child.owner_table.contains(f.child_id)
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert child.owner_table.collection_state(f.child_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert not f.graph.has_active_obligations()
        assert len(f.recovery.snapshot(identity).slot_collections) == 1
        assert f.adapter.report_terminal(identity)
        assert not f.adapter.pending_terminal_reports() and not f.adapter.pending_lease_completions()
        with f.node._state_lock:
            assert f.node._output_publications_clean_locked()
        terminal_history = (
            tuple(f.controls), tuple(f.release_calls), tuple(f.released), tuple(f.graph_releases),
            tuple(f.slot_reports), f.graph.snapshot(), f.recovery.snapshot(identity), f.journal.snapshot(identity),
        )
        assert not core._publish_reply(
            pending, detached_reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        assert (
            tuple(f.controls), tuple(f.release_calls), tuple(f.released), tuple(f.graph_releases),
            tuple(f.slot_reports), f.graph.snapshot(), f.recovery.snapshot(identity), f.journal.snapshot(identity),
        ) == terminal_history
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert child.owner_table.collection_state(f.child_id) is ObjectCollectionState.COLLECTED
        assert not f.delayed
        assert f.node.resource_ledger.available == f.node.resource_ledger.total
        assert f.node.object_store.used_bytes == 0
        for owner in (core, child):
            assert not owner._objects and not owner._stored_descriptors and not owner._object_gc_obligations
            assert not owner._protocol_unresolved and not getattr(owner, "_output_result_custody", {})
            assert _edge_take_submissions(owner) == ()
            assert owner._reference_mailbox.pending.empty() and owner._reference_mailbox.pending.unfinished_tasks == 0
            assert len(owner._reference_mailbox.releases) == 1
    finally:
        f.close()


class _OwnerCollectionFixture:
    """One Core/Node publication, plain STORED or same-owner contained INLINE.

    The real reference thread is installed explicitly by the L1 probe, never
    here. This helper owns one canonical Task, optionally one tiny child put,
    one 1-KiB Node store and the actual output/graph/recovery authorities.
    No user Task executes. This is not the two-Core _EdgeRetryFixture topology.
    """

    def __init__(self, *, contained):
        from miniray.control import NodeRegistry
        from miniray.object_manager import ObjectManager
        from tests.unit.test_node_placement_group_runtime import _node

        self.contained = contained
        self.core, self.node = core, node = make_pure_core(), _node()
        core.worker_id, core.node_id = node.worker_id, node.node_id
        core.node_address, core.owner_address = ("owner-gc-node.invalid", 1), ("same-owner-gc.invalid", 2)
        core.gcs_address = ("owner-gc-control.invalid", 3)
        self.lock = core._state_lock
        registry = NodeRegistry()
        assert registry.register(node.node_id, core.node_address, node.resource_ledger.total, node_pid=node._node_pid)
        node._registration_epoch = registry.get(node.node_id).registration_epoch
        node._registered_with_gcs = True
        node._object_manager = ObjectManager(node.node_id, node.object_store)
        node._local_replica_write_claims = {}
        self.graph, self.recovery, self.journal = ContainedReferenceGraphAuthority(), OutputPublicationRecoveryAuthority(), OutputPublicationJournal()
        self.ref = self.child_ref = self.pending = self.envelope = None
        self.transfer = self.edge = None
        self.controls, self.releases, self.graph_releases, self.drops, self.slot_reports = [], [], [], [], []

        def forbidden(*_args, **_kwargs):
            pytest.fail("successful single publication attempted rollback")

        def prepare_child(address, request):
            assert self.contained and address == core.owner_address and request.transfer == self.transfer
            return core.prepare_stored_contained_pin(request)

        def promote_child(address, request):
            assert self.contained and address == core.owner_address and request.transfer == self.transfer
            return core.promote_stored_contained_pin(request)

        self.adapter = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent, arm_complete=self.recovery.arm_complete,
            report_terminal=self.recovery.report_terminal, report_rollback=self.recovery.report_rollback,
            prepare_child=prepare_child, promote_child=promote_child, release_child=forbidden,
            prepare_graph=lambda request: protocol.ContainedGraphReply(request, self.graph.prepare_manifest(request.manifest)),
            abort_graph=forbidden, seal_replica=node._seal_output_publication_replica,
            drop_replica=node._drop_output_publication_replica,
        )
        node._output_publication_journal, node._output_publications = self.journal, self.adapter
        core._rpc, core._borrow_rpc = self.rpc, self.release_child

    def register(self):
        core = self.core
        if self.contained:
            self.child_ref = core.put(7)
        function = (lambda value: value) if self.contained else (lambda: "stored-result")
        args = ({"child": self.child_ref},) if self.contained else ()
        self.pending, self.ref = core._register_submission(
            core.define_remote_function(function), args, {}, ResourceVector({"CPU": 1}),
            max_retries=1, _enqueue=True,
        )
        assert _edge_take_submissions(core) == (self.pending,)
        assert core._accepted_task_count == 1 and core._task_finish_barriers[self.pending.object_id] is self.pending
        if self.contained:
            snapshot = core.owner_table.snapshot(self.child_ref.object_id)
            assert snapshot.submitted_tokens == frozenset({self.pending.dependency_hold})
            self.lineage = snapshot.lineage_tokens
            assert len(self.lineage) == 1

    def complete(self):
        core, node, pending = self.core, self.node, self.pending
        request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, target_node_id=node.node_id, return_ids=pending.output_ids,
        )
        self.grant = grant = node._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease and grant.worker_id == core.worker_id
        started = node._handle_start_worker_lease(protocol.StartWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id, grant.worker_id,
        ))
        assert started.accepted and started.node_incarnation is not None
        discovery = OutputDiscoverySession(OutputPublicationHeader(
            OutputPublicationID(grant.lease_id, pending.execution), core.job_id, grant.worker_id,
            core.worker_id, started.node_incarnation,
        ), inline_threshold=4096 if self.contained else 0, owner_address=core.owner_address)
        values = ({"child": self.child_ref},) if self.contained else ("stored-result",)
        outputs = discovery.discover(values)
        (slot,) = outputs.manifest.slots
        if self.contained:
            assert slot.tier is protocol.ResultStorage.INLINE and slot.size_bytes <= 4096
            (self.transfer,), (self.edge,) = slot.transfers, slot.edges
            assert self.transfer.contained_owner_worker_id == self.transfer.final_hold.container_owner_worker_id == core.worker_id
            assert not core.owner_table.snapshot(self.child_ref.object_id).contained_holds
        else:
            assert slot.tier is protocol.ResultStorage.OBJECT_STORE and slot.size_bytes <= 128 and not slot.transfers
        assert node.object_store.used_bytes == 0 and not self.journal.publication_ids()
        assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(outputs.manifest, outputs.slot_payloads)).accepted
        assert self.recovery.snapshot(outputs.manifest.publication_id).armed
        discovery.release_sources_after_promotions()
        completed = node._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id, grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
        ))
        assert completed.accepted and completed.released and completed.state is protocol.LeaseExecutionState.COMPLETED
        self.envelope = completed.output_publication
        assert self.envelope is not None and self.envelope.manifest == outputs.manifest
        assert node.resource_ledger.available == node.resource_ledger.total
        return protocol.TaskReply(pending.task_id, pending.spec.attempt_id, grant.worker_id,
                                  protocol.TaskReplyStatus.SUCCEEDED, self.envelope.results,
                                  output_publication=self.envelope)

    def rpc(self, address, handler, request):
        core = self.core
        self.controls.append((handler, request))
        assert len(self.controls) <= 8
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            assert address == core.node_address
            return self.node._handle_ack_output_publication_adopted(request)
        if handler == "drop_object_replica":
            assert not self.contained and address == core.node_address and not self.drops
            obligation = core._object_gc_obligations[self.pending.object_id]
            assert core.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTING
            assert request == next(iter(obligation.pending_drops.values()))
            reply = self.node._handle_drop_object_replica(request)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            self.drops.append((request, reply))
            return reply
        assert address == core.gcs_address
        if handler == "commit_contained_graph":
            assert self.contained
            return protocol.ContainedGraphReply(request, self.graph.commit_manifest(request.manifest))
        if handler == "release_contained_graph_container":
            assert self.contained and len(self.releases) == 1 and not self.graph_releases
            receipt = self.graph.release_manifest_container(request.manifest, request.container_object_id)
            self.graph_releases.append(receipt)
            return protocol.ContainedGraphReply(request, receipt)
        assert handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
        if type(request) is wire.ReportOutputPublicationTerminal:
            ack = self.recovery.report_terminal(request.witness)
        elif type(request) is wire.ReportOutputPublicationAdopted:
            assert core.owner_table.output_owner_publication_receipt(
                OutputOwnerPublicationPlan(self.pending.execution, self.envelope),
            ).committed
            ack = self.recovery.report_adopted(request.proof)
        else:
            assert type(request) is wire.ReportOutputPublicationSlotCollected and not self.slot_reports
            obligation = core._object_gc_obligations[self.pending.object_id]
            assert not obligation.pending_drops and not obligation.pending_edges
            assert request.proof.cleanup_id == obligation.plan.collection_id
            if self.contained:
                assert len(self.releases) == len(self.graph_releases) == 1
                assert core.owner_table.snapshot(self.child_ref.object_id).lineage_tokens == self.lineage
            else:
                assert len(self.drops) == 1 and self.node.object_store.used_bytes == 0
            self.slot_reports.append(request)
            ack = self.recovery.report_slot_collected(request.proof)
        return wire.OutputRecoveryReply(request, ack)

    def release_child(self, address, handler, request):
        core = self.core
        assert self.contained and address == core.owner_address and handler == "release_contained_reference"
        assert threading.current_thread() is core._reference_thread
        assert core._state_lock is self.lock and core._completion._lock is self.lock
        # This checks only this callback thread's lock ownership, not whether
        # another thread may hold the RLock at a different instant.
        assert not self.lock._is_owned()
        assert request.object_id == self.child_ref.object_id and request.owner_worker_id == core.worker_id
        assert request.hold == self.transfer.final_hold and not self.releases
        reply = core.release_contained_reference(request)
        assert reply.accepted and reply.released
        self.releases.append((threading.current_thread(), request, reply))
        child = core.owner_table.snapshot(self.child_ref.object_id)
        assert not child.local_tokens and not child.submitted_tokens and not child.contained_holds
        assert child.lineage_tokens == self.lineage  # outer still owns the canonical input
        return reply

    def assert_collected(self):
        core = self.core
        assert core.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(self.pending.object_id) is None
        if self.contained:
            assert core.owner_table.collection_state(self.child_ref.object_id) is ObjectCollectionState.COLLECTED
            assert not core._recovery.reconstruction_snapshot(self.child_ref.object_id).is_put
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert not core._protocol_unresolved and not core._task_finish_barriers and core._accepted_task_count == 0
        assert _edge_take_submissions(core) == ()
        assert self.node.object_store.used_bytes == 0 and not self.node._sealed_metadata
        assert not self.graph.has_active_obligations() and len(self.slot_reports) == 1
        assert self.adapter.report_terminal(self.envelope.publication_id)
        with self.node._state_lock:
            assert self.node._output_publications_clean_locked()

    def close_pure(self):
        for ref in (self.ref, self.child_ref):
            if ref is not None:
                _edge_release_local(ref)
        close_pure_core(self.core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_edge_retry_runtime")
def test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected() -> None:
    """Replaces obsolete blanket exclusion of STORED results from owner GC."""
    f = _OwnerCollectionFixture(contained=False)
    core = f.core
    try:
        f.register()
        pending, ref = f.pending, f.ref
        _edge_release_local(ref)
        _edge_drain_notices(core, pending.object_id)
        before = core.owner_table.snapshot(pending.object_id)
        assert before.state is ObjectState.PENDING and not before.local_tokens and not before.collection_pending
        assert core._task_finish_barriers == {pending.object_id: pending}
        reply = f.complete()
        assert core.owner_table.snapshot(pending.object_id) == before
        assert f.node.object_store.used_bytes == reply.results[0].size_bytes > 0
        assert core._publish_reply(pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
        stored = core.owner_table.snapshot(pending.object_id)
        assert stored.state is ObjectState.READY_STORED and not stored.local_tokens
        assert not stored.collection_pending and stored.canonical_stored_result == reply.results[0]
        assert core._stored_descriptors[pending.object_id] == reply.results[0]
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        _edge_drain_notices(core, pending.object_id)
        assert core.owner_table.snapshot(pending.object_id) == stored
        assert core._accepted_task_count == 1 and core._task_finish_barriers[pending.object_id] is pending
        assert not f.drops and not f.slot_reports and not core._object_gc_obligations
        assert core._finish_pending_task(pending) and core._finish_pending_task(pending)
        _edge_drain_notices(core, pending.object_id)
        assert len(f.drops) == len(f.slot_reports) == 1
        drop, ack = f.drops[0]
        assert (drop.object_id, drop.producer_attempt_id, drop.owner_worker_id, drop.node_id, drop.checksum) == (
            ack.object_id, ack.producer_attempt_id, ack.owner_worker_id, ack.node_id, ack.checksum,
        )
        f.assert_collected()
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        assert len(core._reference_mailbox.releases) == 1
    finally:
        f.close_pure()


@pytest.mark.unit
def test_task_reply_edge_validation_and_worker_commit_names_outer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pure: one result/child; real discovery, Node journal and child pins."""
    from dataclasses import replace
    from miniray.core import ObjectRef
    from tests.unit.test_worker_unified_output import (
        _ActualNodePublication, _Fixture, _install_no_runtime,
    )

    _install_no_runtime(monkeypatch)
    values = []
    fixture = _Fixture(monkeypatch, lambda: values[0], threshold=65536)
    worker = fixture.worker
    spec = fixture.push.spec
    outer_id = spec.return_ids()[0]
    child_id = ObjectID.for_task(TaskID.derive(spec.job_id, spec.task_id, 0))
    child_ref = ObjectRef(child_id, worker.worker_id, worker.address)
    values.append({"child": child_ref})
    child_owner = ObjectOwnerTable()
    child_owner.register(child_id, local_token="child-local")
    before = child_owner.snapshot(child_id)
    backend = _ActualNodePublication(fixture, child_tables={worker.worker_id: child_owner})
    actual_prepare = fixture.on_prepare

    def observe_prepare(request):
        assert child_owner.snapshot(child_id) == before
        assert fixture.pending.discovery.source_references == (child_ref,)
        return actual_prepare(request)

    fixture.on_prepare = observe_prepare
    reply = worker._handle_push_task(fixture.push)
    assert reply.output_publication == fixture.complete_envelope
    assert not hasattr(reply, "contained_edges")
    (slot,) = reply.output_publication.manifest.slots
    (transfer,) = slot.transfers
    (edge,) = slot.edges
    assert edge.container_object_id == outer_id and edge.contained_object_id == child_id
    assert transfer.final_hold.container_owner_worker_id == spec.owner_worker_id
    holds = child_owner.snapshot(child_id).contained_holds
    assert holds == frozenset((transfer.final_hold,))
    assert transfer.provisional_hold not in holds
    assert backend.completions == [reply.output_publication.complete]
    assert worker._handle_push_task(fixture.push) is reply and fixture.executions == [True]
    with pytest.raises(TypeError, match="unexpected keyword argument.*contained_edges"):
        replace(reply, contained_edges=(edge,))
    with pytest.raises(ValueError, match="output slot"):
        replace(slot, transfers=(replace(
            transfer,
            provisional_hold=replace(transfer.provisional_hold, container_object_id=child_id),
            final_hold=replace(transfer.final_hold, container_object_id=child_id),
        ),))
    assert child_owner.release_contained_reference(child_id, transfer.final_hold)
    assert child_owner.release_local_reference(child_id, "child-local")


class _SameOwnerReferenceProbe:
    """One real reference consumer, exact same-owner callbacks, bounded joins.

    Core initialization and dispatch are not under test. Only the original
    CoreWorker reference runtime is explicitly installed on the single Core,
    and no synchronous fake mailbox is allowed to perform the tested release.
    The main thread publishes/finishes; the real consumer collects both objects.
    """

    def __init__(self, monkeypatch):
        import math
        from miniray import transport, worker

        self.fixture = None
        self.starting = False
        self.started, self.created, self.joins, self.errors, self.violations, self.gc_calls = [], [], [], [], [], []
        self.observation_lock = threading.Lock()
        self.collected = threading.Event()
        self.baseline = self.runtime_threads()
        real_thread = threading.Thread
        probe = self

        class ReferenceThread(real_thread):
            def __init__(thread, *args, **kwargs):
                super().__init__(*args, **kwargs)
                if not probe.starting or thread.name != "miniray-core-reference-events" or probe.created:
                    probe.forbidden("unexpected reference-thread construction")
                probe.created.append(thread)

            def start(thread):
                if thread not in probe.created or probe.started:
                    probe.forbidden("unexpected reference-thread start")
                super().start()
                probe.started.append(thread)

            def join(thread, timeout=None):
                if (thread not in probe.created or type(timeout) not in (int, float)
                        or not math.isfinite(timeout) or not 0 <= timeout <= 1.0):
                    probe.forbidden("unowned or unbounded reference-thread join")
                probe.joins.append((thread, timeout))
                return super().join(timeout)

            def run(thread):
                try:
                    return super().run()
                except BaseException as exc:
                    probe.errors.append(exc)

        monkeypatch.setattr(threading, "Thread", ReferenceThread)
        for kind in (CoreWorker, NodeServer, worker.WorkerServer, transport.TCPServer):
            monkeypatch.setattr(kind, "__init__", self.forbidden)
        for name in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, name, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", self.forbidden)
        monkeypatch.setattr(threading.Timer, "__init__", self.forbidden)
        monkeypatch.setattr(queue.Queue, "join", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        for module in (core_module, node_module, worker):
            monkeypatch.setattr(module, "rpc_request", self.forbidden)
        monkeypatch.setattr(transport, "request", self.forbidden)

    @staticmethod
    def runtime_threads():
        return {thread for thread in threading.enumerate() if thread.name.startswith("miniray-core-")}

    def forbidden(self, *args, **kwargs):
        with self.observation_lock:
            self.violations.append((threading.current_thread(), args, kwargs))
        raise AssertionError("same-owner reference L1 attempted an unmodelled effect")

    def start(self, monkeypatch):
        self.fixture = f = _OwnerCollectionFixture(contained=True)
        core = f.core
        original_collector = core._reference_released

        def observe_collection(object_id):
            try:
                result = original_collector(object_id)
                if len(self.gc_calls) >= 12:
                    self.forbidden("same-owner collector observation limit exceeded")
                self.gc_calls.append((threading.current_thread(), object_id))
                # Recursive child checks can precede outer commit. Signal only
                # after both real owner commits, never just a release ACK.
                if (f.pending is not None and f.child_ref is not None
                        and core.owner_table.collection_state(f.pending.object_id) is ObjectCollectionState.COLLECTED
                        and core.owner_table.collection_state(f.child_ref.object_id) is ObjectCollectionState.COLLECTED):
                    self.collected.set()
                return result
            except BaseException as exc:
                # The production event loop intentionally continues after GC
                # callbacks. Preserve a test failure before it can be swallowed.
                with self.observation_lock:
                    if len(self.errors) < 12:
                        self.errors.append(exc)
                raise

        monkeypatch.setattr(core, "_reference_released", observe_collection)
        monkeypatch.setattr(core, "_execute", self.forbidden)
        monkeypatch.setattr(core, "_push_task_rpc", self.forbidden)
        monkeypatch.setattr(core, "_schedule_reference_event", self.forbidden)
        self.starting = True
        try:
            # Unbound actual initializer, not make_pure_core's tripwire. It
            # replaces only this Core's unused synchronous reference mailbox.
            CoreWorker._initialize_reference_events(core)
        finally:
            self.starting = False
        assert self.started == self.created == [core._reference_thread]
        assert core._reference_thread.is_alive()
        assert core._state_lock is f.lock and core._completion._lock is f.lock
        f.register()
        return f

    def stop_verified(self):
        core = self.fixture.core
        assert self.collected.wait(1.0), "same-owner collection did not finish within the L1 window"
        assert core._stop_reference_events(time.monotonic() + 1.0)
        assert not core._reference_thread.is_alive()
        assert not core._reference_runtime_finalizer.alive
        assert core._reference_mailbox.stopped.is_set()
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        assert not core._gc_retry_timers and not core._gc_retry_timers_open
        assert len(self.gc_calls) <= 12
        assert self.runtime_threads() == self.baseline
        assert not self.errors and not self.violations

    def cleanup(self):
        # If GC is stuck inside the Core lock, do not re-enter it from here
        # through ref.close or _stop_reference_events. The mailbox has a
        # separate short lock; signal its FIFO stop, then join only our thread.
        f = self.fixture
        if f is not None:
            mailbox = getattr(f.core, "_reference_mailbox", None)
            if mailbox is not None and hasattr(mailbox, "stop"):
                mailbox.close_admission()
                mailbox.stop()
        deadline = time.monotonic() + 1.0
        for thread in self.created:
            if thread.ident is not None:
                thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in self.created)
        if f is not None:
            finalizer = getattr(f.core, "_reference_runtime_finalizer", None)
            if finalizer is not None:
                finalizer.detach()
            # Mailbox admission is now closed; local finalizers cannot issue
            # RPCs or wait for a failed GC thread. No owner counts are cleared.
            for ref in (f.ref, f.child_ref):
                if ref is not None and not ref.closed:
                    ref._closed = True
                    ref._finalizer()
        assert self.runtime_threads() == self.baseline
        assert not self.errors and not self.violations


@pytest.mark.loopback_smoke
def test_publish_installs_edge_before_wake_and_same_owner_release_does_not_deadlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real reference-consumer re-entry on one Core, not arbitrary deadlock freedom."""
    probe = _SameOwnerReferenceProbe(monkeypatch)
    try:
        f = probe.start(monkeypatch)
        core, pending, ref = f.core, f.pending, f.ref
        reply = f.complete()
        child_id = f.child_ref.object_id
        f.child_ref.close(timeout=0.5)
        assert not core.owner_table.snapshot(child_id).local_tokens
        observed = []
        original_wake = core._wake_object

        def wake(object_id):
            assert object_id == pending.object_id and not observed
            assert threading.current_thread() is threading.main_thread()
            assert core.owner_table.snapshot(object_id).outgoing_contained_edges == frozenset({f.edge})
            assert core.owner_table.snapshot(child_id).contained_holds == frozenset({f.transfer.final_hold})
            assert f.graph.snapshot().committed_edges == (f.edge,)
            assert not core._objects[object_id].event.is_set()
            observed.append(frozenset({f.edge}))
            original_wake(object_id)

        monkeypatch.setattr(core, "_wake_object", wake)
        assert core._publish_reply(pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
        assert observed == [frozenset({f.edge})]
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert core._finish_pending_task(pending)
        assert core.owner_table.snapshot(child_id).lineage_tokens == f.lineage
        assert not f.releases  # outer local handle still owns the result
        ref.close(timeout=0.5)
        probe.stop_verified()
        assert len(f.releases) == 1 and f.releases[0][0] is core._reference_thread
        assert f.releases[0][1].owner_worker_id == core.worker_id
        assert any(thread is core._reference_thread and object_id == child_id for thread, object_id in probe.gc_calls)
        assert not core.owner_table.contains(pending.object_id)
        assert not core.owner_table.contains(child_id)
        f.assert_collected()
    finally:
        probe.cleanup()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_edge_retry_runtime")
def test_shutdown_retries_retained_edge_obligation_before_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real shutdown GC precheck, not public shutdown or timer liveness."""
    f = _EdgeRetryFixture(monkeypatch)
    core = f.core
    try:
        f.register()
        pending, ref = f.pending, f.ref
        reply = f.complete()
        assert core._publish_reply(
            pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id,
        )
        object_id, identity = pending.object_id, reply.output_publication.publication_id
        published = core.owner_table.snapshot(object_id)
        assert published.state is ObjectState.READY_INLINE
        assert published.local_tokens == frozenset({ref._local_token})
        assert published.outgoing_contained_edges == frozenset({f.edge})
        assert published.output_publication.manifest == reply.output_publication.manifest
        assert not f.release_calls and not f.delayed
        _edge_drain_notices(core, object_id)
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert _edge_take_submissions(core) == ()
        _edge_drain_notices(core, object_id)
        # A real local handle retains the completed outer until this cut.
        # The first release fails before touching the real child, as before.
        assert core.owner_table.contains(object_id) and not f.release_calls
        _edge_release_local(ref)
        _edge_drain_notices(core, object_id)
        assert len(f.release_calls) == 1 and not f.released
        assert object_id in core._inline_gc_obligations
        obligation = core._object_gc_obligations[object_id]
        plan = obligation.plan
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTING
        assert plan.contained_releases == (f.edge,) and plan.producer_task_spec == pending.spec
        assert obligation.pending_edges == {f.edge} and not obligation.pending_drops
        assert obligation.graph_release_receipt is None and not obligation.output_cleanup_reported
        assert obligation.retry_scheduled and obligation.retry_round == 1
        assert f.child_owner.owner_table.snapshot(f.child_id) == f.child_before_failure
        assert not f.child_owner.owner_table.contained_release_was_seen(f.child_id, f.transfer.final_hold)
        assert f.graph.snapshot().committed_edges == (f.edge,)
        assert not f.graph_releases and not f.slot_reports
        assert not f.recovery.snapshot(identity).slot_collections
        assert core._recovery.lineage_for_object(object_id) is not None
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        (scheduled,) = f.delayed
        mailbox, event, _delay = scheduled
        assert mailbox.pending.empty() and event.object_id == object_id
        attempts_before = len(f.release_calls)

        # The original assertion counted this synchronous retry pass. It
        # performs one extra Release (two total), without waiting for a timer.
        assert core._retry_gc_obligations_for_shutdown()
        assert len(f.release_calls) - attempts_before == 1
        assert len(f.release_calls) == 2 and f.release_calls[0] == f.release_calls[1]
        assert len(f.released) == len(f.graph_releases) == len(f.slot_reports) == 1
        assert f.slot_reports[0].proof.cleanup_id == plan.collection_id
        assert not core.owner_table.contains(object_id)
        assert object_id not in core._inline_gc_obligations
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(object_id) is None
        assert f.child_owner.owner_table.collection_state(f.child_id) is ObjectCollectionState.COLLECTED
        assert not f.graph.has_active_obligations()
        assert len(f.recovery.snapshot(identity).slot_collections) == 1
        assert f.delayed == [scheduled]
        assert f.adapter.report_terminal(identity)
        assert not f.adapter.pending_terminal_reports() and not f.adapter.pending_lease_completions()
        with f.node._state_lock:
            assert f.node._output_publications_clean_locked()
        retained_history = (
            tuple(f.release_calls), tuple(f.released), tuple(f.graph_releases),
            tuple(f.slot_reports), tuple(f.controls), f.graph.snapshot(),
            f.recovery.snapshot(identity), f.journal.snapshot(identity),
        )

        # The old scheduling record is still present; deliver that exact event
        # after synchronous cleanup, rather than erasing an undelivered callback.
        assert mailbox.enqueue_internal(event)
        _edge_drain_notices(core, object_id)
        assert core._retry_gc_obligations_for_shutdown()
        assert (
            tuple(f.release_calls), tuple(f.released), tuple(f.graph_releases),
            tuple(f.slot_reports), tuple(f.controls), f.graph.snapshot(),
            f.recovery.snapshot(identity), f.journal.snapshot(identity),
        ) == retained_history
        assert f.delayed == [scheduled]
        assert f.node.resource_ledger.available == f.node.resource_ledger.total
        assert f.node.object_store.used_bytes == 0
        for owner in (core, f.child_owner):
            assert not owner._objects and not owner._stored_descriptors and not owner._object_gc_obligations
            assert _edge_take_submissions(owner) == ()
            assert owner._reference_mailbox.pending.empty()
            assert owner._reference_mailbox.pending.unfinished_tasks == 0
            assert len(owner._reference_mailbox.releases) == 1
            assert owner._accepting  # no public shutdown/admission claim here
    finally:
        f.close()
