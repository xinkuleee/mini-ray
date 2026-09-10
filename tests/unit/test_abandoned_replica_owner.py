"""Pure owner acceptance of replicas abandoned by a dead submitting Worker.

One threadless owner Core, two 1 KiB stores and one tiny stored put. A real
three-call pin/chunk/release transfer creates the target replica; an actual
Grant/Cancel freezes its inventory without reporting it to the input owner.
The input's RETAINED hold and optional live executor borrower use real reducers.

GCS is only an explicit ordered GetWorkerDeaths suffix consumed by the normal
Core death driver. No GCS instance, process, thread, socket, wait or user code
runs. Physical collection uses actual Node Drop handlers; at most two drops
and two explicit cleanup passes. Node-side abandoned-inventory ACK is outside
this owner-boundary test; no fake Node acknowledgement declares it clean.
"""

from copy import deepcopy
from dataclasses import replace
import threading

import pytest

from miniray import control, node as node_module, protocol
from miniray.core import _HomeRoute
from miniray.core import _ObjectWaiter, _worker_death_reference_id
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectState, UnknownObjectError
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_cancelled_grant_inventory import _node
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, monkeypatch, *, live_borrower=False):
        self.core = core = make_pure_core()
        self.source = _node(NodeID(b"a" * 16), WorkerID(b"b" * 16))
        self.target = _node(NodeID(b"c" * 16), WorkerID(b"d" * 16))
        self.source_address, self.target_address = ("source.invalid", 1), ("target.invalid", 2)
        core.node_id, core.node_address = self.source.node_id, self.source_address
        core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
        core.owner_address, core.gcs_address = ("owner.invalid", 3), ("gcs-input.invalid", 4)
        self.target._cluster_addresses = {self.source.node_id: self.source_address}
        self.transfers, self.drops, self.queries = [], [], []
        self.before_source_drop = None
        self.death_published = True
        self.payload = b"owner-alive"
        producer = TaskID.for_put(core.job_id, core.worker_id, 0)
        self.object_id, self.attempt = ObjectID.for_task(producer), AttemptID(producer, 0)
        seal = protocol.SealObject.from_data(self.object_id, self.attempt, core.worker_id, self.payload)
        assert self.source._handle_seal_object(seal).sealed
        self.canonical = protocol.ResultDescriptor(self.object_id, protocol.ResultStorage.OBJECT_STORE,
            len(self.payload), core.worker_id, self.source.node_id, seal.checksum)
        table = core.owner_table
        table.register(self.object_id, current_attempt=self.attempt, local_token="source-live")
        assert table.publish_stored(self.object_id, self.attempt, self.source.node_id, descriptor=self.canonical)
        assert core._recovery.register_put(self.object_id)
        core._stored_descriptors[self.object_id] = self.canonical
        core._objects[self.object_id] = _ObjectWaiter(threading.Event())
        core._objects[self.object_id].event.set()
        self.submitter = WorkerID(b"s" * 16)
        consumer = TaskID.derive(core.job_id, core.driver_task_id, 177)
        consumer_attempt = AttemptID(consumer, 0)
        self.hold = protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.RETAINED,
                                               self.submitter, consumer, consumer_attempt)
        borrower = (self.submitter, "submitter-source")
        assert table.add_borrowed_reference(self.object_id, borrower)
        assert table.retain_borrowed_reference_for_task(self.object_id, borrower, self.hold)
        assert table.release_borrowed_reference(self.object_id, borrower)
        self.live_borrower = (self.target.worker_id, "live-executor-borrow") if live_borrower else None
        if self.live_borrower is not None:
            assert table.acquire_exported_reference(self.object_id, protocol.TaskHoldSource(self.hold), self.live_borrower)
        self.source_descriptor = protocol.ObjectStoreDescriptor(self.object_id, core.worker_id, self.attempt,
            self.source.node_id, len(self.payload), seal.checksum)
        self.route = protocol.DependencyOwnerRoute(self.object_id, core.worker_id, core.owner_address, self.hold)
        self.request = protocol.RequestWorkerLease(
            LeaseID(b"l" * 16), consumer, consumer_attempt, ResourceVector({"CPU": 1}),
            self.source.node_id, self.submitter, target_node_id=self.target.node_id,
            dependencies=(self.source_descriptor,), return_ids=(ObjectID.for_task(consumer),),
            dependency_owner_routes=(self.route,),
        )
        monkeypatch.setattr(node_module, "rpc_request", self.transfer)
        self.grant = self.target._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease and len(self.transfers) == 3
        self.cancel = self.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
            self.request.lease_id, consumer, consumer_attempt, self.source.node_id, self.submitter,
            lease_request=self.request,
        ))
        assert self.cancel.accepted and self.cancel.cancelled and self.cancel.released
        self.inventory = self.cancel.dependency_inventory
        assert self.inventory.lease_request == self.request and self.inventory.descriptors == self.grant.dependencies
        self.descriptor, = self.inventory.descriptors
        assert self.target.object_store.get(self.object_id) == self.payload
        assert self.target.object_store.snapshot(self.object_id).pin_count == 0
        assert table.snapshot(self.object_id).locations == frozenset((self.source.node_id,))
        incarnation = protocol.WorkerIncarnation(self.source.node_id, 7101, 1, self.submitter, 7201)
        self.death = protocol.WorkerDeathRecord(
            "ordered-submitter-exit", incarnation, 1, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        self.report = protocol.ReportAbandonedDependencyReplica(self.inventory, self.descriptor, self.death)
        core._rpc, core._resolve_node_address = self.rpc, self.address

    def address(self, node_id, *, home_route=None):
        assert node_id in (self.source.node_id, self.target.node_id)
        return self.source_address if node_id == self.source.node_id else self.target_address

    def transfer(self, address, handler, request, **kwargs):
        assert address == self.source_address
        self.transfers.append((handler, request))
        assert len(self.transfers) <= 3
        handlers = {node_module.PIN_OBJECT_HANDLER: self.source._handle_pin_object_for_transfer,
                    node_module.GET_OBJECT_CHUNK_HANDLER: self.source._handle_get_object_chunk,
                    node_module.RELEASE_OBJECT_PIN_HANDLER: self.source._handle_release_object_pin}
        assert handler in handlers
        return handlers[handler](request)

    def rpc(self, address, handler, request):
        if address == self.core.gcs_address:
            assert handler == control.GET_WORKER_DEATHS_HANDLER and type(request) is protocol.GetWorkerDeaths
            assert not self.core._state_lock._is_owned()
            self.queries.append(request)
            assert len(self.queries) <= 6
            watermark = 1 if self.death_published else 0
            assert request.after_epoch <= watermark
            suffix = (self.death,) if request.after_epoch == 0 and watermark else ()
            return deepcopy(protocol.GetWorkerDeathsReply(request.after_epoch, watermark, suffix))
        assert handler == node_module.DROP_OBJECT_REPLICA_HANDLER
        node = self.source if address == self.source_address else self.target
        assert address == self.address(node.node_id) and request == self.drop(node.node_id)
        if node is self.source and self.before_source_drop is not None:
            callback, self.before_source_drop = self.before_source_drop, None
            callback()
        reply = node._handle_drop_object_replica(request)
        self.drops.append((request, reply))
        assert len(self.drops) <= 2 and reply.status is protocol.DropObjectReplicaStatus.DROPPED
        return reply

    def drop(self, node_id):
        return protocol.DropObjectReplica(self.object_id, self.attempt, self.core.worker_id, node_id, self.canonical.checksum)

    def sync(self):
        assert self.core._sync_worker_deaths()
        installed = self.core.owner_table.dead_worker_record(self.submitter)
        assert installed is not None and installed.death_id == _worker_death_reference_id(self.death)
        assert not self.core.owner_table.has_retained_reference_for_task(self.object_id, self.hold)

    def offer(self, request=None):
        request = self.report if request is None else request
        reply = self.core.report_abandoned_dependency_replica(request)
        assert type(reply) is protocol.ReportAbandonedDependencyReplicaReply and reply.request == request
        assert self.core._inflight_borrow_ops == 0
        return reply

    def collect_source(self):
        assert self.core.owner_table.release_local_reference(self.object_id, "source-live")
        self.core._reference_released(self.object_id)
        assert self.core.owner_table.collection_state(self.object_id) is ObjectCollectionState.COLLECTED
        assert not self.core._objects and not self.core._stored_descriptors and not self.core._object_gc_obligations

    def drain(self):
        assert self.core._reference_mailbox.pending.qsize() <= 6
        self.core._reference_mailbox.drain()
        assert self.core._reference_mailbox.pending.empty()
        assert not self.core._has_late_replica_cleanup_locked()

    def close(self):
        table = self.core.owner_table
        table.release_retained_reference_for_task(self.object_id, self.hold)
        if table.contains(self.object_id):
            if self.live_borrower is not None:
                table.release_borrowed_reference(self.object_id, self.live_borrower)
            for token in table.snapshot(self.object_id).local_tokens:
                table.release_local_reference(self.object_id, token)
        close_pure_core(self.core)


def test_dead_submitter_handoff_tracks_current_replica_without_releasing_live_executor_borrow(monkeypatch):
    f = _Fixture(monkeypatch, live_borrower=True)
    try:
        assert f.core.owner_table.release_local_reference(f.object_id, "source-live")
        reply = f.offer()
        assert reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY and reply.custody_transferred
        assert len(f.queries) == 1 and f.queries[0].after_epoch == 0
        snapshot = f.core.owner_table.snapshot(f.object_id)
        assert snapshot.retained_tokens == frozenset() and snapshot.borrowed_tokens == frozenset((f.live_borrower,))
        assert snapshot.locations == frozenset((f.source.node_id, f.target.node_id))
        assert snapshot.canonical_stored_result == f.canonical and not snapshot.collection_pending
        assert (f.live_borrower, protocol.TaskHoldSource(f.hold)) in snapshot.borrowed_sources
        f.drain()
        assert not f.drops and f.core.owner_table.snapshot(f.object_id) == snapshot
        assert not f.core.owner_table.dead_worker_record(f.core.worker_id)
        assert f.source.object_store.get(f.object_id) == f.target.object_store.get(f.object_id) == f.payload
    finally:
        f.close()


def test_collected_put_accepts_retired_replica_custody_and_drops_only_exact_late_bytes(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.sync()
        f.collect_source()
        history = f.core.owner_table._stored_collection_history[f.object_id]
        assert not f.source.object_store.contains(f.object_id) and f.target.object_store.contains(f.object_id)
        reply = f.offer()
        assert reply.status is protocol.RetainedLocationReportStatus.RETIRED and reply.custody_transferred
        assert f.core._late_replica_cleanup.pending() == (f.drop(f.target.node_id),)
        assert len(f.drops) == 1 and not f.core.owner_table.contains(f.object_id)
        assert f.core._drive_late_replica_cleanup(schedule_retry=False)
        assert [request.node_id for request, _ in f.drops] == [f.source.node_id, f.target.node_id]
        assert not f.target.object_store.contains(f.object_id)
        assert f.offer().status is protocol.RetainedLocationReportStatus.RETIRED
        f.drain()
        assert len(f.drops) == 2 and not f.core._objects and not f.core._stored_descriptors
        assert f.core.owner_table._stored_collection_history[f.object_id] == history
        with pytest.raises(UnknownObjectError):
            f.core.owner_table.snapshot(f.object_id)
    finally:
        f.close()


def test_collecting_put_keeps_original_gc_plan_while_late_queue_owns_target_cleanup(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.sync()
        seen = []

        def during_source_drop():
            assert not f.core._state_lock._is_owned() and not seen
            obligation = f.core._object_gc_obligations[f.object_id]
            plan = obligation.plan
            assert plan.locations == (f.source.node_id,)
            assert f.core.owner_table.collection_state(f.object_id) is ObjectCollectionState.COLLECTING
            reply = f.offer()
            assert reply.status is protocol.RetainedLocationReportStatus.RETIRED
            assert f.core._object_gc_obligations[f.object_id] is obligation
            assert obligation.plan == plan and tuple(obligation.pending_drops) == (f.source.node_id,)
            assert f.core.owner_table.snapshot(f.object_id).collection_plan == plan
            assert f.core._late_replica_cleanup.pending() == (f.drop(f.target.node_id),)
            assert f.core._drive_late_replica_cleanup(schedule_retry=False)
            assert not f.target.object_store.contains(f.object_id) and f.source.object_store.contains(f.object_id)
            seen.append(plan)

        f.before_source_drop = during_source_drop
        f.collect_source()
        assert len(seen) == 1 and seen[0].locations == (f.source.node_id,)
        assert [request.node_id for request, _ in f.drops] == [f.target.node_id, f.source.node_id]
        f.drain()
        assert len(f.drops) == 2 and not f.core._object_gc_obligations
    finally:
        f.close()


def test_lost_abandoned_report_ack_replays_without_recreating_submitter_hold(monkeypatch):
    f = _Fixture(monkeypatch, live_borrower=True)
    try:
        delivered = []

        def lose_once():
            reply = f.offer()
            delivered.append(reply)
            assert reply.custody_transferred
            if len(delivered) == 1:
                raise TransportTimeout("owner recorded custody before report ACK was lost")
            return reply

        with pytest.raises(TransportTimeout):
            lose_once()
        before = f.core.owner_table.snapshot(f.object_id)
        reply = lose_once()
        assert reply == delivered[0] and reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert f.core.owner_table.snapshot(f.object_id) == before
        assert len(f.queries) == 2 and [query.after_epoch for query in f.queries] == [0, 1]
        assert not before.retained_tokens and before.borrowed_tokens == frozenset((f.live_borrower,))
        f.drain()
        assert not f.drops and not f.core._has_late_replica_cleanup_locked()
    finally:
        f.close()


def test_uninstalled_or_mismatched_death_never_admits_owner_custody(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.death_published = False
        before = f.core.owner_table.snapshot(f.object_id)
        reply = f.offer()
        assert reply.status is protocol.RetainedLocationReportStatus.REJECTED and not reply.custody_transferred
        assert f.core.owner_table.snapshot(f.object_id) == before and not f.drops
        assert not f.core.owner_table.dead_worker_record(f.submitter)
        f.death_published = True
        f.sync()
        before = f.core.owner_table.snapshot(f.object_id)
        mismatched = replace(f.report, submitter_death=replace(f.death, detection_id="another-worker-exit"))
        rejected = f.offer(mismatched)
        assert rejected.status is protocol.RetainedLocationReportStatus.REJECTED and not rejected.custody_transferred
        assert f.core.owner_table.snapshot(f.object_id) == before and not f.drops
        assert not f.core._has_late_replica_cleanup_locked()
    finally:
        f.close()


def test_wrong_canonical_checksum_is_rejected_for_current_and_collected_put(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.sync()
        # The offered wire inventory is internally consistent but lies about
        # physical contents. Only the owner's retained canonical is authority.
        bad_source = replace(f.source_descriptor, checksum="e" * 64)
        bad_target = replace(f.descriptor, checksum="e" * 64)
        bad_inventory = protocol.LeaseDependencyInventory(
            replace(f.request, dependencies=(bad_source,)), f.target.node_id, (bad_target,),
        )
        bad = protocol.ReportAbandonedDependencyReplica(bad_inventory, bad_target, f.death)
        before = f.core.owner_table.snapshot(f.object_id)
        rejected = f.offer(bad)
        assert rejected.status is protocol.RetainedLocationReportStatus.REJECTED and not rejected.custody_transferred
        assert f.core.owner_table.snapshot(f.object_id) == before and not f.drops
        assert not f.core._has_late_replica_cleanup_locked()
        f.collect_source()
        rejected = f.offer(bad)
        assert rejected.status is protocol.RetainedLocationReportStatus.REJECTED and not rejected.custody_transferred
        assert f.core.owner_table.collection_state(f.object_id) is ObjectCollectionState.COLLECTED
        assert len(f.drops) == 1 and not f.core._has_late_replica_cleanup_locked()
        assert f.target.object_store.get(f.object_id) == f.payload
    finally:
        f.close()
