"""Pure Node recovery of an unacknowledged inventory after submitter death.

Two 1 KiB Node stores, one or two tiny inputs, one real lease request and at
most one passive logical-owner Core. Typed GCS replies feed the Node proof
validator and the owner's real ordered death reducer. No GCS service, process,
thread, socket, wait, user callable or second execution runs. At most two
explicit supervisor drives and one actual owner-report delivery failure.

The Node records its own death-authorized completion, not a fabricated
AckLeaseDependencyCustody sent by the dead Worker. Running execution keeps
pins/resources until its actual Node completion reducer is called.
"""

from copy import deepcopy
from dataclasses import replace
import threading

import pytest

from miniray import node as node_module, protocol
from miniray.core import _HomeRoute
from miniray.core import _ObjectWaiter, _worker_death_reference_id
from miniray.ids import WorkerID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_cancelled_grant_inventory import _NodeFixture
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, monkeypatch, *, frontier="granted", lose_owner_ack=False, dead_input_owner=False):
        assert frontier in ("partial", "granted", "running")
        self.nodes = f = _NodeFixture(monkeypatch, commit=False)
        self.source, self.target = f.source, f.target
        self.frontier, self.lose_owner_ack, self.dead_input_owner = frontier, lose_owner_ack, dead_input_owner
        self.submitter = f.request.requester_worker_id if dead_input_owner else WorkerID(b"d" * 16)
        self.owner = None if dead_input_owner else make_pure_core()
        self.owner_address = ("living-owner.invalid", 2401)
        self.gcs_address = ("typed-gcs-input.invalid", 2402)
        descriptors = f.descriptors if frontier == "partial" else f.descriptors[:1]
        self.sources = tuple(descriptors)
        if self.owner is not None:
            self.owner.worker_id = descriptors[0].owner_worker_id
            self.owner.node_id, self.owner.node_address = self.source.node_id, f.source_address
            self.owner._home_route = _HomeRoute(self.owner.node_id, self.owner.node_address, self.owner._membership_epoch)
            self.owner.owner_address = self.owner_address
            self.owner.gcs_address = self.gcs_address
            self.owner._resolve_node_address = self.address
            self.owner._rpc = self.owner_rpc
        hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED if dead_input_owner else protocol.TaskReferenceHoldKind.RETAINED,
            self.submitter, f.request.task_id, f.request.attempt_id,
        )
        self.hold = hold
        routes = []
        for index, descriptor in enumerate(descriptors):
            kind = (protocol.TaskReferenceHoldKind.SUBMITTED if descriptor.owner_worker_id == self.submitter
                    else protocol.TaskReferenceHoldKind.RETAINED)
            route_hold = replace(hold, kind=kind)
            address = self.owner_address if index == 0 else ("second-owner.invalid", 2403)
            routes.append(protocol.DependencyOwnerRoute(descriptor.object_id, descriptor.owner_worker_id, address, route_hold))
        self.request = replace(f.request, requester_worker_id=self.submitter, dependencies=self.sources, dependency_owner_routes=tuple(routes))
        self.death = protocol.WorkerDeathRecord(
            "dependency-submitter-exit", protocol.WorkerIncarnation(self.source.node_id, 2501, 1, self.submitter, 2601),
            1, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        self.proof = protocol.GetWorkerStateReply(
            self.submitter, True, 1, protocol.WorkerMembershipState.DEAD, self.death.incarnation, self.death,
        )
        self.worker_queries, self.owner_queries, self.reports, self.drops, self.owner_sweeps = [], [], [], [], []
        self.owner_ack_lost = False
        self.target._background_rpc = self.background_rpc
        self.target._gcs_address = self.gcs_address
        # Unregistered test Nodes do not emit resource updates to the GCS input
        # endpoint. Only the explicitly named death lookup is allowed there.
        self.target._registered_with_gcs = False
        if self.owner is not None:
            descriptor = descriptors[0]
            self.owner.owner_table.register(descriptor.object_id, current_attempt=descriptor.producer_attempt_id, local_token="owner-live-ref")
            result = protocol.ResultDescriptor(
                descriptor.object_id, protocol.ResultStorage.OBJECT_STORE, descriptor.size_bytes,
                descriptor.owner_worker_id, descriptor.node_id, descriptor.checksum,
            )
            assert self.owner.owner_table.publish_stored(descriptor.object_id, descriptor.producer_attempt_id, descriptor.node_id, descriptor=result)
            self.owner._stored_descriptors[descriptor.object_id] = result
            self.owner._objects[descriptor.object_id] = _ObjectWaiter(threading.Event())
            self.owner._objects[descriptor.object_id].event.set()
            self.owner._recovery.register_put(descriptor.object_id)
            borrower = (self.submitter, "submitter-borrow")
            assert self.owner.owner_table.add_borrowed_reference(descriptor.object_id, borrower)
            assert self.owner.owner_table.retain_borrowed_reference_for_task(descriptor.object_id, borrower, self.hold)
            assert self.owner.owner_table.release_borrowed_reference(descriptor.object_id, borrower)
        if frontier == "partial":
            lost = descriptors[1]
            reply = self.source._handle_drop_object_replica(protocol.DropObjectReplica(
                lost.object_id, lost.producer_attempt_id, lost.owner_worker_id, lost.node_id, lost.checksum,
            ))
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
        self.reply = self.target._handle_request_lease(self.request)
        if frontier == "partial":
            assert type(self.reply) is protocol.RejectWorkerLease
            assert self.reply.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
            assert len(f.transfers) == 5  # rejected second Pin still closes its exact session
        else:
            assert type(self.reply) is protocol.GrantWorkerLease
            assert len(f.transfers) == 3
            if frontier == "running":
                started = self.target._handle_start_worker_lease(protocol.StartWorkerLease(
                    self.reply.lease_id, self.reply.task_id, self.reply.attempt_id, self.reply.worker_id,
                ))
                assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        self.inventory = self.registry().snapshot(self.request.lease_id)
        assert self.inventory.descriptors == (replace(descriptors[0], node_id=self.target.node_id),)
        assert self.registry().has_pending() and not self.worker_queries and not self.reports
        original_sweep = self.target._handle_install_owner_death_fence

        def observe_sweep(request):
            self.owner_sweeps.append(request)
            assert dead_input_owner, "a live input owner was incorrectly treated as dead"
            assert request.owner_worker_id == self.submitter
            return original_sweep(request)

        monkeypatch.setattr(self.target, "_handle_install_owner_death_fence", observe_sweep)

    def registry(self):
        with self.target._state_lock:
            return self.target._dependency_custody_registry_locked()

    def address(self, node_id, *, home_route=None):
        assert node_id in (self.source.node_id, self.target.node_id)
        return self.nodes.source_address if node_id == self.source.node_id else ("target.invalid", 2404)

    def background_rpc(self, address, handler, request, **kwargs):
        assert not self.target._state_lock._is_owned()
        if address == self.gcs_address:
            assert handler == node_module.GCS_GET_WORKER_STATE_HANDLER and type(request) is protocol.GetWorkerState
            self.worker_queries.append(request.worker_id)
            assert len(self.worker_queries) <= 3
            if request.worker_id == self.submitter:
                return deepcopy(self.proof)
            assert self.lose_owner_ack and self.owner is not None and request.worker_id == self.owner.worker_id
            return protocol.GetWorkerStateReply(
                self.owner.worker_id, True, 1, protocol.WorkerMembershipState.ALIVE,
                protocol.WorkerIncarnation(self.source.node_id, 2501, 1, self.owner.worker_id, 2602),
            )
        assert address == self.owner_address
        assert handler == protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER
        assert kwargs["connect_timeout"] == 0.25 and kwargs["request_timeout"] == 0.5
        assert type(kwargs["deadline"]) is float
        assert type(request) is protocol.ReportAbandonedDependencyReplica and self.owner is not None
        assert request.inventory == self.inventory and request.submitter_death == self.death
        assert request.owner_route == self.request.dependency_owner_routes[0]
        assert request.descriptor == self.inventory.descriptors[0]
        record = self.target._leases.get(self.request.lease_id)
        if self.frontier == "running":
            assert record.state is protocol.LeaseExecutionState.RUNNING
            assert self.target.object_store.snapshot(request.descriptor.object_id).pin_count == 1
            assert self.target.resource_ledger.available.is_zero()
        else:
            assert record is None or record.state is protocol.LeaseExecutionState.ABANDONED
            assert self.target.object_store.snapshot(request.descriptor.object_id).pin_count == 0
        reply = self.owner.report_abandoned_dependency_replica(request)
        self.reports.append((request, reply))
        assert len(self.reports) <= 2 and reply.custody_transferred
        assert reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        if self.lose_owner_ack and not self.owner_ack_lost:
            self.owner_ack_lost = True
            raise TransportTimeout("live owner accepted custody before its ACK was lost")
        return reply

    def owner_rpc(self, address, handler, request):
        assert self.owner is not None
        if address == self.gcs_address:
            assert handler == "get_worker_deaths" and type(request) is protocol.GetWorkerDeaths
            self.owner_queries.append(request.after_epoch)
            assert len(self.owner_queries) <= 2 and request.after_epoch in (0, 1)
            return protocol.GetWorkerDeathsReply(request.after_epoch, 1, (self.death,) if request.after_epoch == 0 else ())
        assert handler == node_module.DROP_OBJECT_REPLICA_HANDLER
        node = self.source if address == self.nodes.source_address else self.target
        assert address == self.address(node.node_id)
        assert request.object_id == self.sources[0].object_id and request.owner_worker_id == self.owner.worker_id
        assert request.producer_attempt_id == self.sources[0].producer_attempt_id and request.checksum == self.sources[0].checksum
        reply = node._handle_drop_object_replica(request)
        assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
        assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
            request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
        )
        self.drops.append((request, reply))
        assert len(self.drops) <= 2
        return reply

    def drive(self):
        return self.target._drive_abandoned_dependency_custody(force=True)

    def finish_execution(self):
        record = self.target._leases.get(self.request.lease_id)
        if record is None or record.state is protocol.LeaseExecutionState.ABANDONED:
            return
        if record.state is protocol.LeaseExecutionState.RUNNING:
            reply = self.target._handle_complete_worker_lease(protocol.CompleteWorkerLease(
                self.request.lease_id, self.request.task_id, self.request.attempt_id,
                record.grant.worker_id, protocol.TaskReplyStatus.SYSTEM_ERROR,
            ))
            assert reply.accepted and reply.released
        elif record.state is protocol.LeaseExecutionState.GRANTED:
            reply = self.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
                self.request.lease_id, self.request.task_id, self.request.attempt_id,
                self.request.requester_node_id, self.request.requester_worker_id, lease_request=self.request,
            ))
            assert reply.accepted and reply.cancelled

    def assert_custody_completed(self):
        entry = self.registry()._entries[self.request.lease_id]
        assert entry.abandoned_complete and entry.submitter_death == self.death
        assert entry.acknowledged is None, "Node forged a dead submitter's normal custody ACK"
        assert not self.registry().has_pending() and len(entry.owner_receipts) == 1
        assert self.target._dead_dependency_submitters[self.submitter] == self.death
        assert not self.target._dependency_handoff_drivers
        if self.owner is not None:
            snapshot = self.owner.owner_table.snapshot(self.sources[0].object_id)
            assert snapshot.state is ObjectState.READY_STORED
            assert snapshot.locations == frozenset((self.source.node_id, self.target.node_id))
            assert snapshot.local_tokens == frozenset(("owner-live-ref",))
            assert self.hold not in snapshot.retained_tokens
            installed = self.owner.owner_table.dead_worker_record(self.submitter)
            assert installed.death_id == _worker_death_reference_id(self.death)
            assert self.owner.owner_table.dead_worker_record(self.owner.worker_id) is None
            assert not self.owner_sweeps and not self.drops

    def collect(self):
        self.finish_execution()
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        descriptor = self.sources[0]
        if self.owner is not None:
            assert self.owner.owner_table.release_local_reference(descriptor.object_id, "owner-live-ref")
            self.owner._reference_released(descriptor.object_id)
            assert self.owner._reference_mailbox.pending.qsize() <= 4
            self.owner._reference_mailbox.drain()
            assert self.owner.owner_table.collection_state(descriptor.object_id) is ObjectCollectionState.COLLECTED
            assert len(self.drops) == 2 and not self.owner._object_gc_obligations
        else:
            # The target's owner-wide sweep already removed its dead-owner
            # replica. Apply the same existing fence to the passive source.
            source_reply = self.source._handle_install_owner_death_fence(protocol.InstallOwnerDeathFence(
                "source-dead-input-cleanup", self.death, self.source.node_id,
                scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
            ))
            assert source_reply.complete
        assert not self.target.object_store.contains(descriptor.object_id, sealed_only=False)
        assert not self.source.object_store.contains(descriptor.object_id, sealed_only=False)
        assert not self.registry().has_pending()

    def close(self):
        self.finish_execution()
        if self.owner is not None:
            if self.owner.owner_table.contains(self.sources[0].object_id):
                self.owner.owner_table.release_retained_reference_for_task(self.sources[0].object_id, self.hold)
            for object_id in tuple(self.owner._objects):
                for token in self.owner.owner_table.snapshot(object_id).local_tokens:
                    self.owner.owner_table.release_local_reference(object_id, token)
            close_pure_core(self.owner)


@pytest.mark.parametrize("frontier", ("partial", "granted", "running"))
def test_dead_submitter_inventory_moves_to_live_owner_without_cancelling_running_execution(monkeypatch, frontier):
    f = _Fixture(monkeypatch, frontier=frontier)
    try:
        assert f.drive()
        f.assert_custody_completed()
        assert f.worker_queries == [f.submitter] and f.owner_queries == [0]
        assert len(f.reports) == 1
        if frontier == "running":
            assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.RUNNING
            assert f.target.object_store.snapshot(f.sources[0].object_id).pin_count == 1
            assert f.target.resource_ledger.available.is_zero()
        elif frontier == "granted":
            assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        else:
            assert f.request.lease_id not in f.target._leases
        before = len(f.worker_queries), len(f.owner_queries), len(f.reports)
        assert not f.drive()
        assert (len(f.worker_queries), len(f.owner_queries), len(f.reports)) == before
        f.collect()
    finally:
        f.close()


def test_owner_custody_ack_loss_keeps_node_pending_and_replays_exact_abandonment(monkeypatch):
    f = _Fixture(monkeypatch, lose_owner_ack=True)
    try:
        assert not f.drive()
        assert f.registry().has_pending() and len(f.reports) == 1
        assert f.registry().receipt(f.request.lease_id, f.sources[0].object_id) is None
        assert f.owner.owner_table.snapshot(f.sources[0].object_id).locations == frozenset((f.source.node_id, f.target.node_id))
        assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert f.drive()
        f.assert_custody_completed()
        assert len(f.reports) == 2 and f.reports[0][0] == f.reports[1][0]
        assert f.worker_queries == [f.submitter, f.owner.worker_id] and f.owner_queries == [0, 1]
        f.collect()
    finally:
        f.close()


@pytest.mark.parametrize("invalid", ("unknown", "wrong-worker"))
def test_unknown_or_unrelated_submitter_death_cannot_abandon_lease_or_ack_inventory(monkeypatch, invalid):
    f = _Fixture(monkeypatch)
    try:
        good = f.proof
        if invalid == "unknown":
            f.proof = protocol.GetWorkerStateReply(f.submitter, False, 0, error="unregistered Worker")
        else:
            other = WorkerID(b"x" * 16)
            death = replace(f.death, incarnation=replace(f.death.incarnation, worker_id=other))
            f.proof = protocol.GetWorkerStateReply(other, True, 1, protocol.WorkerMembershipState.DEAD, death.incarnation, death)
        before = f.owner.owner_table.snapshot(f.sources[0].object_id)
        assert not f.drive()
        assert not f.reports and not f.owner_queries and not f.drops
        assert f.registry().has_pending() and f.registry()._entries[f.request.lease_id].submitter_death is None
        assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert f.target.object_store.snapshot(f.sources[0].object_id).pin_count == 1
        assert f.owner.owner_table.snapshot(f.sources[0].object_id) == before
        f.proof = good
        assert f.drive()
        f.assert_custody_completed()
        f.collect()
    finally:
        f.close()


def test_dead_input_owner_uses_existing_exact_owner_fence_instead_of_a_live_owner_report(monkeypatch):
    f = _Fixture(monkeypatch, dead_input_owner=True)
    try:
        assert f.drive()
        f.assert_custody_completed()
        assert not f.reports and len(f.owner_sweeps) == 1
        assert f.owner_sweeps[0].owner_death == f.death
        receipt = f.registry().receipt(f.request.lease_id, f.sources[0].object_id)
        assert type(receipt) is protocol.InstallOwnerDeathFenceReply and receipt.complete
        assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert not f.target.object_store.contains(f.sources[0].object_id, sealed_only=False)
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        f.collect()
    finally:
        f.close()
