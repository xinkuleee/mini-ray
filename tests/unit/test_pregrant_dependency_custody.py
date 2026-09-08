"""Pure custody handoff for replicas sealed before any consumer grant.

Two threadless Cores, two 1 KiB stores, at most two tiny input objects and one
never-executed consumer. Actual Node pull/seal, cancellation, owner handoff,
inventory ACK and physical collection handlers run synchronously. A capacity
case holds one real empty-dependency probe lease without ever pushing it.

Failure is an actually dropped second source, an occupied Worker slot, three
lost source-release ACKs, or one before/after-effect metadata write exception.
Cancel and inventory ACK delivery may each be lost once after actual effects.
At most two manual replay steps; no GCS, process, thread, socket, wait or user code.
"""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import threading

import pytest

from miniray import node as node_module, protocol
from miniray.core import (
    ObjectRef, _DelayedReadyTask, _ForeignDependencyGuard, _LeaseCancellationState,
    _LeaseRequestState, _LocationReportState, _ObjectWaiter, _PendingTask, _ReadyTask,
)
from miniray.errors import LeaseRejectedError, PendingCapacityError
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID
from miniray.lease_dependencies import DependencyCustodyConflict
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_node_dependency_pull import _bare_node


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, monkeypatch, mode, *, cancel_ack_loss=False, custody_ack_loss=False):
        assert mode in ("second-source-lost", "busy-slot", "release-acks-lost", "metadata-write-failure")
        self.mode = mode
        self.cancel_ack_loss = cancel_ack_loss
        self.custody_ack_loss = custody_ack_loss
        self.consumer, self.foreign = make_pure_core(), make_pure_core()
        self.source = _bare_node(NodeID(b"s" * 16), ResourceVector({"CPU": 1}))
        self.target = _bare_node(NodeID(b"t" * 16), ResourceVector({"CPU": 1}))
        self.source_address, self.target_address = ("source.invalid", 1), ("target.invalid", 2)
        for node in (self.source, self.target):
            node._object_store = ObjectStore(1024)
            node._object_manager = ObjectManager(node.node_id, node._object_store)
            node.event_sink = None
        self.target._worker_process = SimpleNamespace(is_alive=lambda: True)
        self.target._worker_address = ("worker.invalid", 3)
        self.target._cluster_addresses = {self.source.node_id: self.source_address}
        self.consumer.node_id, self.consumer.node_address = self.target.node_id, self.target_address
        self.foreign.node_id, self.foreign.node_address = self.source.node_id, self.source_address
        self.foreign.owner_address = ("foreign-owner.invalid", 4)
        self.requests, self.cancels, self.acks, self.reports, self.releases = [], [], [], [], []
        self.transfers, self.source_release_replies, self.drops, self.states = [], [], [], []
        self.error = None
        self.gc_open = False
        self.probe = self.probe_grant = None
        self.probe_cancelled = False
        self.owners = ((self.consumer,) if mode in ("release-acks-lost", "metadata-write-failure")
                       else (self.consumer, self.foreign))
        self.payloads = (b"first-local", b"second-foreign")[:len(self.owners)]
        descriptors, values, refs = [], [], []
        for index, (core, payload) in enumerate(zip(self.owners, self.payloads)):
            task = TaskID.derive(self.consumer.job_id, self.consumer.driver_task_id, 120 + index)
            object_id, attempt = ObjectID.for_task(task), AttemptID(task, 0)
            seal = protocol.SealObject.from_data(object_id, attempt, core.worker_id, payload)
            assert self.source._handle_seal_object(seal).sealed
            result = protocol.ResultDescriptor(object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
                                               core.worker_id, self.source.node_id, seal.checksum)
            core.owner_table.register(object_id, current_attempt=attempt, local_token="source-local")
            assert core.owner_table.publish_stored(object_id, attempt, self.source.node_id, descriptor=result)
            core._recovery.register_put(object_id)
            core._stored_descriptors[object_id] = result
            core._objects[object_id] = _ObjectWaiter(threading.Event())
            core._objects[object_id].event.set()
            descriptors.append(protocol.ObjectStoreDescriptor(
                object_id, core.worker_id, attempt, self.source.node_id, len(payload), seal.checksum,
            ))
            values.append(result)
            refs.append(ObjectRef(object_id, core.worker_id))
        self.sources, self.results, self.refs = tuple(descriptors), tuple(values), tuple(refs)
        self.local_id = self.sources[0].object_id
        self.foreign_id = self.sources[1].object_id if len(self.sources) == 2 else None
        task = TaskID.derive(self.consumer.job_id, self.consumer.driver_task_id, 123)
        attempt = AttemptID(task, 0)
        spec = protocol.TaskSpec(
            self.consumer.job_id, task, attempt,
            protocol.FunctionKey(self.consumer.job_id, __name__, "must-not-execute", "v1"),
            tuple(protocol.RefArg(item.object_id, item.owner_worker_id) for item in self.sources),
            1, ResourceVector({"CPU": 1}), self.consumer.worker_id, max_retries=1,
        )
        self.local_hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED, self.consumer.worker_id, task, attempt,
        )
        assert self.consumer.owner_table.add_submitted_reference(self.local_id, self.local_hold)
        guards = ()
        self.foreign_hold = None
        if self.foreign_id is not None:
            self.foreign_hold = protocol.TaskReferenceHold(
                protocol.TaskReferenceHoldKind.RETAINED, self.consumer.worker_id, task, attempt,
            )
            borrower = (self.consumer.worker_id, "pregrant-borrow")
            assert self.foreign.owner_table.add_borrowed_reference(self.foreign_id, borrower)
            assert self.foreign.owner_table.retain_borrowed_reference_for_task(self.foreign_id, borrower, self.foreign_hold)
            assert self.foreign.owner_table.release_borrowed_reference(self.foreign_id, borrower)
            guards = (_ForeignDependencyGuard(
                self.foreign_id, self.foreign.worker_id, self.foreign.owner_address,
                self.consumer.worker_id, borrower[1], self.foreign_hold,
            ),)
        self.consumer.owner_table.register_task_outputs(spec, local_tokens=("consumer-local",))
        self.consumer._recovery.register_task(spec, max_retries=1)
        self.consumer._objects[spec.return_ids()[0]] = _ObjectWaiter(threading.Event())
        self.pending = _PendingTask(spec.return_ids()[0], spec, protected_dependencies=(self.local_id,),
                                    dependency_hold=self.local_hold, foreign_dependency_guards=guards)
        self.consumer._accepted_task_count += 1
        self.consumer._install_task_finish_barrier_locked(self.pending)
        self.consumer._registered_functions = set()
        self.consumer._rpc, self.consumer._borrow_rpc = self.rpc, self.borrow_rpc
        self.consumer._resolve_node_address = self.address
        self.foreign._rpc, self.foreign._resolve_node_address = self.rpc, self.address
        self.consumer._push_task_rpc = lambda *_a, **_k: pytest.fail("pre-grant rejection reached Push")
        original_mark = self.consumer._mark_protocol_unresolved

        def observe_mark(pending, phase, obligation=None, **kwargs):
            original_mark(pending, phase, obligation, **kwargs)
            if isinstance(obligation, _LocationReportState):
                self.states.append((phase, obligation))
                assert len(self.states) <= 40

        monkeypatch.setattr(self.consumer, "_mark_protocol_unresolved", observe_mark)
        monkeypatch.setattr(node_module, "rpc_request", self.transfer)
        self.request = protocol.RequestWorkerLease(
            LeaseID(b"p" * 16), task, attempt, spec.resources, self.target.node_id,
            self.consumer.worker_id, target_node_id=self.target.node_id, dependencies=self.sources, return_ids=spec.return_ids(),
        )
        self.lease_state = _LeaseRequestState(self.request, self.target_address, self.target.node_id, False)
        if mode == "second-source-lost":
            # Lose the second source after its owner supplied the immutable
            # ready metadata, using the real public debug drop, not table edits.
            assert self.foreign.drop_object(self.refs[1], node_id=self.source.node_id)
            assert not self.source.object_store.contains(self.foreign_id)
            assert self.foreign.owner_table.snapshot(self.foreign_id).state is ObjectState.LOST
        if mode == "busy-slot":
            probe_task = TaskID.derive(self.consumer.job_id, self.consumer.driver_task_id, 124)
            self.probe = protocol.RequestWorkerLease(
                LeaseID(b"b" * 16), probe_task, AttemptID(probe_task, 0), ResourceVector({"CPU": 1}),
                self.target.node_id, self.consumer.worker_id, target_node_id=self.target.node_id,
            )
            self.probe_grant = self.target._handle_request_lease(self.probe)
            assert type(self.probe_grant) is protocol.GrantWorkerLease and self.probe_grant.dependencies == ()
            self.consumer._capacity_retry_rounds = 0

    def address(self, node_id):
        assert node_id in (self.source.node_id, self.target.node_id)
        return self.source_address if node_id == self.source.node_id else self.target_address

    def registry(self):
        with self.target._state_lock:
            return self.target._dependency_custody_registry_locked()

    def expected_descriptors(self):
        selected = self.sources if self.mode == "busy-slot" else self.sources[:1]
        return tuple(replace(item, node_id=self.target.node_id) for item in selected)

    def transfer(self, address, handler, request, **options):
        assert address == self.source_address
        self.transfers.append((handler, request))
        assert len(self.transfers) <= 6
        handlers = {
            node_module.PIN_OBJECT_HANDLER: self.source._handle_pin_object_for_transfer,
            node_module.GET_OBJECT_CHUNK_HANDLER: self.source._handle_get_object_chunk,
            node_module.RELEASE_OBJECT_PIN_HANDLER: self.source._handle_release_object_pin,
        }
        assert handler in handlers
        reply = handlers[handler](request)
        if handler == node_module.RELEASE_OBJECT_PIN_HANDLER:
            self.source_release_replies.append((request, reply))
            assert reply.accepted
            if self.mode == "second-source-lost" and request.object_id == self.foreign_id:
                assert not reply.released and not self.source.object_store.contains(request.object_id)
                assert request.transfer_id not in self.source._pinned_transfers
                assert self.source._closed_transfer_pins[request.transfer_id].closed
            else:
                assert self.source.object_store.snapshot(request.object_id).pin_count == 0
            if self.mode == "release-acks-lost" and len(self.source_release_replies) <= 3:
                assert self.target.object_store.contains(self.local_id)
                inventory = self.registry().snapshot(self.request.lease_id)
                assert inventory.descriptors == self.expected_descriptors()
                raise TransportTimeout("source released its pin but the ACK was lost")
        return reply

    def rpc(self, address, handler, request):
        if handler == node_module.DROP_OBJECT_REPLICA_HANDLER:
            node = self.source if address == self.source_address else self.target
            assert address == self.address(node.node_id) and request.node_id == node.node_id
            assert self.gc_open or (self.mode == "second-source-lost" and request.object_id == self.foreign_id
                                    and node is self.source and not self.requests)
            source = next(item for item in self.sources if item.object_id == request.object_id)
            assert (request.producer_attempt_id, request.owner_worker_id, request.checksum) == (
                source.producer_attempt_id, source.owner_worker_id, source.checksum,
            )
            reply = node._handle_drop_object_replica(request)
            assert type(reply) is protocol.DropObjectReplicaReply and reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
                request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
            )
            self.drops.append((request, reply))
            assert len(self.drops) <= 4
            return reply
        assert address == self.target_address
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert request == self.request
            reply = deepcopy(self.target._handle_request_lease(request))
            self.requests.append((request, reply))
            assert len(self.requests) == 1 and type(reply) is protocol.RejectWorkerLease
            expected = (protocol.LeaseRejectReason.PENDING_CAPACITY if self.mode == "busy-slot"
                        else protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE)
            assert reply.reason is expected and self.request.lease_id not in self.target._leases
            assert (reply.lease_id, reply.task_id, reply.attempt_id) == (request.lease_id, request.task_id, request.attempt_id)
            inventory = self.registry().snapshot(self.request.lease_id)
            assert inventory.lease_request == self.request and inventory.descriptors == self.expected_descriptors()
            assert self.registry().has_pending()
            return reply
        if handler == node_module.CANCEL_LEASE_HANDLER:
            marker = self.consumer._protocol_unresolved[self.pending.task_key]
            assert isinstance(marker.obligation, (_LeaseCancellationState, _LocationReportState))
            error = marker.obligation.terminal_error
            if self.error is None:
                self.error = error
            assert error is self.error
            reply = deepcopy(self.target._handle_cancel_worker_lease(request))
            self.cancels.append((request, reply))
            assert len(self.cancels) <= (2 if self.cancel_ack_loss else 1)
            assert reply.accepted and reply.cancelled and not reply.released
            assert reply.retired_grant is None and reply.state is protocol.LeaseExecutionState.ABANDONED
            assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
                    reply.requester_worker_id, reply.scheduling_key) == (
                request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
                request.requester_worker_id, request.scheduling_key,
            )
            assert reply.dependency_inventory.lease_request == self.request
            assert reply.dependency_inventory.descriptors == self.expected_descriptors()
            assert self.registry().has_pending(), "Cancel cannot stand in for owner custody ACK"
            if self.cancel_ack_loss and len(self.cancels) == 1:
                raise TransportTimeout("pre-grant cancellation applied before ACK loss")
            return reply
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert type(request) is protocol.AckLeaseDependencyCustody
        state = self.consumer._protocol_unresolved[self.pending.task_key].obligation
        assert type(state) is _LocationReportState and state.grant is None
        assert state.terminal_error is self.error and state.inventory == request.inventory
        assert state.descriptors == self.expected_descriptors() and state.node_id == self.target.node_id
        assert request.requester_worker_id == self.consumer.worker_id and request.inventory.lease_request == self.request
        assert len(state.local_receipts) == 1 and state.local_receipts[0].custody_transferred
        assert len(state.receipts) == (1 if self.mode == "busy-slot" else 0)
        assert all(reply.custody_transferred for reply in state.receipts)
        assert self.registry().has_pending() is (not self.acks)
        for item in self.expected_descriptors():
            assert self.target.object_store.snapshot(item.object_id).pin_count == 0
        assert self.pending.dependency_hold in self.consumer.owner_table.snapshot(self.local_id).submitted_tokens
        reply = self.target._handle_ack_lease_dependency_custody(request)
        self.acks.append((request, reply))
        assert len(self.acks) <= (2 if self.custody_ack_loss else 1)
        assert type(reply) is protocol.AckLeaseDependencyCustodyReply
        assert reply.accepted and reply.request == request and not self.registry().has_pending()
        if self.custody_ack_loss and len(self.acks) == 1:
            raise TransportTimeout("Node recorded dependency custody before ACK loss")
        return reply

    def borrow_rpc(self, address, handler, request):
        assert address == self.foreign.owner_address and request.object_id == self.foreign_id
        assert request.hold == self.foreign_hold
        if handler == "report_retained_object_location":
            assert self.mode == "busy-slot" and self.cancels and not self.acks
            reply = self.foreign.report_retained_object_location(request)
            self.reports.append((request, reply))
            assert len(self.reports) == 1 and reply.accepted and reply.custody_transferred
            return reply
        assert handler == "release_owned_object_for_task"
        reply = self.foreign.release_owned_object_for_task(request)
        self.releases.append((request, reply))
        assert len(self.releases) <= 1 and reply.accepted
        return reply

    def replay_once(self):
        selected = []
        count = self.consumer._submissions.qsize()
        assert 1 <= count <= 4
        for _item in range(count):
            item = self.consumer._submissions.get_nowait()
            self.consumer._submissions.task_done()
            if isinstance(item, _DelayedReadyTask):
                selected.append(item.ready)
        assert len(selected) == 1 and type(selected[0]) is _ReadyTask
        ready = selected[0]
        if ready.cancellation is not None:
            return self.consumer._resolve_lease_cancellation(
                ready.pending, ready.spec, ready.dependencies, ready.cancellation,
            )
        assert ready.location_state is not None
        return self.consumer._execute(ready.pending, ready.spec, ready.dependencies, location_state=ready.location_state)

    def execute(self):
        terminal = self.consumer._execute(self.pending, self.pending.spec, self.sources, lease_state=self.lease_state)
        for _ in range(2):
            if terminal:
                return
            terminal = self.replay_once()
        assert terminal, "bounded pre-grant custody did not converge"

    def cancel_probe(self):
        if self.probe is None or self.probe_cancelled:
            return
        reply = self.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
            self.probe.lease_id, self.probe.task_id, self.probe.attempt_id,
            self.probe.requester_node_id, self.probe.requester_worker_id, lease_request=self.probe,
        ))
        assert reply.accepted and reply.cancelled and reply.released
        assert reply.dependency_inventory.descriptors == ()
        self.probe_cancelled = True

    def finish_and_collect(self):
        snapshot = self.consumer.owner_table.snapshot(self.pending.object_id)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is self.error
        assert isinstance(self.error, PendingCapacityError if self.mode == "busy-slot" else LeaseRejectedError)
        assert not self.consumer._protocol_unresolved
        record = self.consumer._recovery.task_record(self.pending.task_id)
        assert record.current_attempt == self.pending.spec.attempt_id and record.retries_started == 0
        assert self.consumer._recovery.active_recovery(self.pending.task_id) is None
        latest = self.states[-1][1]
        assert latest.grant is None and latest.custody_acknowledged and latest.terminal_error is self.error
        assert latest.inventory == self.acks[0][0].inventory
        assert latest.lease_request == self.request and latest.cancellation_reply == self.cancels[-1][1]
        assert latest.execution_outcome is None
        assert len(self.requests) == 1
        assert len(self.cancels) == (2 if self.cancel_ack_loss else 1)
        assert len(self.acks) == (2 if self.custody_ack_loss else 1)
        assert all(request == self.cancels[0][0] for request, _reply in self.cancels)
        assert all(request == self.acks[0][0] for request, _reply in self.acks)
        assert self.request.lease_id not in self.target._leases
        assert self.consumer.owner_table.snapshot(self.local_id).locations == frozenset((self.source.node_id, self.target.node_id))
        for index, item in enumerate(self.expected_descriptors()):
            assert self.target.object_store.get(item.object_id) == self.payloads[index]
            assert self.source.object_store.get(item.object_id) == self.payloads[index]
        if self.mode == "second-source-lost":
            assert not self.target.object_store.contains(self.foreign_id, sealed_only=False)
            assert self.foreign.owner_table.snapshot(self.foreign_id).locations == frozenset()
        self.cancel_probe()
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        with self.target._state_lock:
            assert self.target._cleanup_plane_quiescent_locked()
        assert self.consumer._finish_pending_task(self.pending)
        assert self.local_hold not in self.consumer.owner_table.snapshot(self.local_id).submitted_tokens
        if self.foreign_id is not None:
            assert not self.foreign.owner_table.has_retained_reference_for_task(self.foreign_id, self.foreign_hold)
        self.gc_open = True
        for core in (self.consumer, self.foreign):
            for output in tuple(core._objects):
                for token in core.owner_table.snapshot(output).local_tokens:
                    assert core.owner_table.release_local_reference(output, token)
                core._reference_released(output)
            assert core._reference_mailbox.pending.qsize() <= 8
            core._reference_mailbox.drain()
            assert not core._objects and not core._object_gc_obligations
            assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
        for core, item in zip(self.owners, self.sources):
            assert core.owner_table.collection_state(item.object_id) is ObjectCollectionState.COLLECTED
        assert self.consumer.owner_table.collection_state(self.pending.object_id) is ObjectCollectionState.COLLECTED
        for node in (self.source, self.target):
            assert node.object_store.used_bytes == 0 and node.object_store.object_ids(sealed_only=False) == ()
            assert node._sealed_metadata == {} and not getattr(node, "_dependency_pin_cleanups", {})
        expected_drops = 4 if self.mode == "busy-slot" else 3 if self.mode == "second-source-lost" else 2
        assert len(self.drops) == expected_drops
        assert not self.registry().has_pending()

    def close(self):
        self.cancel_probe()
        if self.consumer.owner_table.contains(self.local_id):
            self.consumer.owner_table.release_submitted_reference(self.local_id, self.local_hold)
        if self.foreign_id is not None:
            self.foreign.owner_table.release_retained_reference_for_task(self.foreign_id, self.foreign_hold)
        for reference in self.refs:
            reference.close()
        for core in (self.consumer, self.foreign):
            for output in tuple(core._objects):
                for token in core.owner_table.snapshot(output).local_tokens:
                    core.owner_table.release_local_reference(output, token)
            close_pure_core(core)


def test_second_source_loss_hands_off_first_sealed_replica_before_terminal_error(monkeypatch):
    f = _Fixture(monkeypatch, "second-source-lost")
    try:
        f.execute()
        assert len(f.transfers) == 5 and len(f.source_release_replies) == 2
        assert f.source_release_replies[0][1].released
        assert not f.source_release_replies[1][1].released
        assert f.cancels[0][1].retired_grant is None
        assert f.cancels[0][1].dependency_inventory.descriptors == f.expected_descriptors()
        f.finish_and_collect()
    finally:
        f.close()


def test_busy_worker_capacity_budget_cancels_and_acknowledges_localized_inventory(monkeypatch):
    f = _Fixture(monkeypatch, "busy-slot")
    try:
        f.execute()
        assert len(f.transfers) == 6 and len(f.reports) == 1
        assert f.target._leases[f.probe.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert not f.target.resource_ledger.available
        assert f.cancels[0][1].retired_grant is None and not f.cancels[0][1].released
        f.finish_and_collect()
    finally:
        f.close()


def test_source_pin_release_ack_loss_retains_already_sealed_pregrant_inventory(monkeypatch):
    f = _Fixture(monkeypatch, "release-acks-lost")
    try:
        f.execute()
        assert len(f.transfers) == 5 and len(f.source_release_replies) == 3
        assert [reply.released for _, reply in f.source_release_replies] == [True, False, False]
        assert all(request == f.source_release_replies[0][0] for request, _ in f.source_release_replies)
        assert "source pin release failed" in f.requests[0][1].detail
        assert f.cancels[0][1].dependency_inventory.descriptors == f.expected_descriptors()
        # Replica custody has completed, but its source-pin Release is an
        # independent obligation until the target receives an exact ACK.
        assert not f.registry().has_pending()
        with f.target._state_lock:
            assert f.target._source_pin_outbox_locked().has_pending()
            assert not f.target._cleanup_plane_quiescent_locked()
        assert f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert len(f.transfers) == 6 and len(f.source_release_replies) == 4
        assert f.source_release_replies[-1][0] == f.source_release_replies[0][0]
        assert not f.source_release_replies[-1][1].released
        f.finish_and_collect()
    finally:
        f.close()


def test_lost_cancel_and_custody_acknowledgements_replay_without_repeating_owner_handoff(monkeypatch):
    f = _Fixture(monkeypatch, "busy-slot", cancel_ack_loss=True, custody_ack_loss=True)
    try:
        original_custody = f.consumer._record_replica_custody_locked
        local_calls = []

        def record_local(descriptor, *, active_hold):
            receipt = original_custody(descriptor, active_hold=active_hold)
            local_calls.append(receipt)
            assert len(local_calls) == 1 and receipt.accepted and receipt.custody_transferred
            return receipt

        monkeypatch.setattr(f.consumer, "_record_replica_custody_locked", record_local)
        assert not f.consumer._execute(f.pending, f.pending.spec, f.sources, lease_state=f.lease_state)
        assert len(f.requests) == len(f.cancels) == 1 and not f.acks and not f.reports
        assert f.registry().has_pending() and not local_calls
        first = f.consumer._protocol_unresolved[f.pending.task_key].obligation
        assert type(first) is _LeaseCancellationState and first.terminal_error is f.error
        assert first.lease_request == f.request and first.reply is None
        assert f.consumer.owner_table.snapshot(f.pending.object_id).state is ObjectState.PENDING
        assert not f.consumer._finish_pending_task(f.pending) and not f.releases
        assert not f.replay_once()
        state = f.consumer._protocol_unresolved[f.pending.task_key].obligation
        assert type(state) is _LocationReportState and state.grant is None
        assert state.terminal_error is first.terminal_error is f.error
        assert state.inventory == f.acks[0][0].inventory and not state.custody_acknowledged
        assert state.cancellation_reply == f.cancels[-1][1]
        assert state.local_receipts == tuple(local_calls) and len(state.receipts) == len(f.reports) == 1
        assert len(f.cancels) == 2 and len(f.acks) == 1
        # The Node genuinely accepted custody; Core cannot infer receipt of the
        # lost reply from Node state and must keep its own obligation pending.
        assert not f.registry().has_pending()
        assert f.consumer.owner_table.snapshot(f.pending.object_id).state is ObjectState.PENDING
        assert not f.consumer._finish_pending_task(f.pending) and not f.releases
        before = tuple(local_calls), tuple(f.reports), tuple(f.cancels), tuple(f.requests), tuple(f.transfers)
        assert f.replay_once()
        assert (tuple(local_calls), tuple(f.reports), tuple(f.cancels), tuple(f.requests), tuple(f.transfers)) == before
        assert len(f.acks) == 2 and f.acks[0] == f.acks[1]
        final = f.states[-1][1]
        assert final.local_receipts == state.local_receipts and final.receipts == state.receipts
        assert final.terminal_error is state.terminal_error and final.custody_acknowledged
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.parametrize("after_effect", (False, True), ids=("before-metadata-write", "after-metadata-write"))
def test_seal_metadata_write_failure_is_reconciled_into_pregrant_custody(monkeypatch, after_effect):
    f = _Fixture(monkeypatch, "metadata-write-failure")
    metadata = None
    try:
        expected = (f.sources[0].producer_attempt_id, f.consumer.worker_id, f.sources[0].size_bytes, f.sources[0].checksum)

        class FailOneMetadataWrite(dict):
            armed = True

            def __init__(self, initial):
                super().__init__(initial)
                self.writes = []
                self.failed_value = None

            def __setitem__(self, key, value):
                if key == f.local_id:
                    self.writes.append((key, value))
                    assert len(self.writes) <= 2 and value == expected
                    assert f.target.object_store.get(key) == f.payloads[0]
                    assert f.target._object_manager.snapshot(key).attempt_id == f.sources[0].producer_attempt_id
                    if self.armed:
                        self.armed = False
                        assert f.registry().has_pending()
                        with pytest.raises(DependencyCustodyConflict, match="physical reconciliation"):
                            f.registry().snapshot(f.request.lease_id)
                        if after_effect:
                            dict.__setitem__(self, key, value)
                        self.failed_value = self.get(key)
                        raise RuntimeError("localized seal metadata write failed once")
                dict.__setitem__(self, key, value)

        metadata = FailOneMetadataWrite(f.target._sealed_metadata)
        assert dict(metadata) == f.target._sealed_metadata
        f.target._sealed_metadata = metadata
        f.execute()
        assert not metadata.armed and len(metadata.writes) == 2 and metadata.writes[0] == metadata.writes[1]
        assert metadata.failed_value == (expected if after_effect else None)
        assert metadata[f.local_id] == expected
        assert len(f.transfers) == 3 and len(f.source_release_replies) == 1
        assert f.source_release_replies[0][1].accepted and f.source_release_replies[0][1].released
        assert "localized seal metadata write failed once" in f.requests[0][1].detail
        assert f.cancels[0][1].dependency_inventory.descriptors == f.expected_descriptors()
        assert f.target.object_store.get(f.local_id) == f.source.object_store.get(f.local_id) == f.payloads[0]
        f.finish_and_collect()
    finally:
        if metadata is not None:
            metadata.armed = False
        f.close()
