"""Pure local/foreign custody handoff after one actual dual-input grant.

Two threadless Cores, two 1 KiB stores, two tiny sealed inputs and one consumer
that never executes. Six real source pin/chunk/release calls create the target
replicas. The public Core drop failpoint then removes the original local input
replica, so repairing its old fetch route is necessary, not an inert injection.

Owner reducers, Node cancellation, foreign reports and exact Node custody ACK
are real. Only one local effect boundary and bounded ACK delivery are faulted.
At most two replay rounds are driven explicitly; no GCS, process, socket,
thread, wait or user code runs.
The fixture admits the consumer directly, without a foreign-lineage registry,
so finish releases its input holds. This does not model normal lineage GC.
"""

from __future__ import annotations

from dataclasses import replace
import threading
from types import SimpleNamespace

import pytest

from miniray import node as node_module, protocol
from miniray.core import (
    ObjectRef, _DelayedReadyTask, _ForeignDependencyGuard, _LeaseRequestState,
    _LocationReportState, _ObjectWaiter, _PendingTask, _ReplicaLocationReceipt, _RetryInlineGc,
)
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_node_dependency_pull import _bare_node


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, monkeypatch, *, consumer_attempt=0):
        self.consumer = core = make_pure_core()
        self.foreign = foreign = make_pure_core()
        self.source = _bare_node(NodeID(bytes((91,)) * 16), ResourceVector({"CPU": 1}))
        self.target = _bare_node(NodeID(bytes((92,)) * 16), ResourceVector({"CPU": 1}))
        self.source_address, self.target_address = ("source.invalid", 1), ("target.invalid", 2)
        for node in (self.source, self.target):
            node._object_store = ObjectStore(1024)
            node._object_manager = ObjectManager(node.node_id, node._object_store)
            node.event_sink = None
        self.target._worker_process = SimpleNamespace(is_alive=lambda: True)
        self.target._worker_address = ("worker.invalid", 3)
        self.target._cluster_addresses = {self.source.node_id: self.source_address}
        core.node_id, core.node_address = self.target.node_id, self.target_address
        foreign.node_id, foreign.node_address = self.source.node_id, self.source_address
        foreign.owner_address = ("foreign-owner.invalid", 4)
        self.calls, self.transfers, self.reports, self.cancels, self.releases = [], [], [], [], []
        self.custody_acks = []
        self.states = []
        self.report_losses = self.cancel_losses = 0
        self.before_report = None
        self.payloads = (b"local-input", b"foreign-input")
        sources, results = [], []
        for index, (owner, payload) in enumerate(zip((core, foreign), self.payloads)):
            task = TaskID.derive(core.job_id, core.driver_task_id, 93 + index)
            output, attempt = ObjectID.for_task(task), AttemptID(task, 0)
            seal = protocol.SealObject.from_data(output, attempt, owner.worker_id, payload)
            assert self.source._handle_seal_object(seal).sealed
            result = protocol.ResultDescriptor(
                output, protocol.ResultStorage.OBJECT_STORE, len(payload),
                owner.worker_id, self.source.node_id, seal.checksum,
            )
            owner.owner_table.register(output, current_attempt=attempt, local_token="source-local")
            assert owner.owner_table.publish_stored(output, attempt, self.source.node_id, descriptor=result)
            owner._stored_descriptors[output] = result
            owner._objects[output] = _ObjectWaiter(threading.Event())
            owner._objects[output].event.set()
            sources.append(protocol.ObjectStoreDescriptor(
                output, owner.worker_id, attempt, self.source.node_id, len(payload), seal.checksum,
            ))
            results.append(result)
        self.sources, self.original_results = tuple(sources), tuple(results)
        self.local_id, self.foreign_id = (item.object_id for item in self.sources)
        self.local_ref = ObjectRef(self.local_id, core.worker_id)

        task = TaskID.derive(core.job_id, core.driver_task_id, 95)
        attempt, origin = AttemptID(task, consumer_attempt), AttemptID(task, 0)
        spec = protocol.TaskSpec(
            core.job_id, task, attempt,
            protocol.FunctionKey(core.job_id, __name__, "never-executed", "v1"),
            tuple(protocol.RefArg(item.object_id, item.owner_worker_id) for item in self.sources),
            1, ResourceVector({"CPU": 1}), core.worker_id, max_retries=1,
        )
        self.local_hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id, task, origin,
        )
        self.foreign_hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, core.worker_id, task, origin,
        )
        assert core.owner_table.add_submitted_reference(self.local_id, self.local_hold)
        borrower = (core.worker_id, "foreign-source-borrow")
        assert foreign.owner_table.add_borrowed_reference(self.foreign_id, borrower)
        assert foreign.owner_table.retain_borrowed_reference_for_task(self.foreign_id, borrower, self.foreign_hold)
        assert foreign.owner_table.release_borrowed_reference(self.foreign_id, borrower)
        self.guard = _ForeignDependencyGuard(
            self.foreign_id, foreign.worker_id, foreign.owner_address, core.worker_id,
            borrower[1], self.foreign_hold,
        )
        core.owner_table.register_task_outputs(spec, local_tokens=("consumer-local",))
        core._recovery.register_task(spec, max_retries=1)
        core._objects[spec.return_ids()[0]] = _ObjectWaiter(threading.Event())
        self.pending = _PendingTask(
            spec.return_ids()[0], spec, protected_dependencies=(self.local_id,),
            dependency_hold=self.local_hold, foreign_dependency_guards=(self.guard,),
        )
        core._accepted_task_count += 1
        core._install_task_finish_barrier_locked(self.pending)
        core._registered_functions = set()
        core._resolve_node_address = self.address
        core._rpc, core._borrow_rpc = self.rpc, self.borrow_rpc

        def no_push(*_args, **_kwargs):
            pytest.fail("a failed local-replica handoff reached consumer execution")

        core._push_task_rpc = no_push
        original_mark = core._mark_protocol_unresolved

        def observe_mark(pending, phase, obligation=None, **kwargs):
            original_mark(pending, phase, obligation, **kwargs)
            if isinstance(obligation, _LocationReportState):
                self.states.append((phase, obligation))
                assert len(self.states) <= 80

        monkeypatch.setattr(core, "_mark_protocol_unresolved", observe_mark)

        def transfer(address, handler, request, **options):
            assert address == self.source_address
            self.transfers.append((handler, request))
            assert len(self.transfers) <= 6
            assert self.target.resource_ledger.available == self.target.resource_ledger.total
            methods = {
                node_module.PIN_OBJECT_HANDLER: self.source._handle_pin_object_for_transfer,
                node_module.GET_OBJECT_CHUNK_HANDLER: self.source._handle_get_object_chunk,
                node_module.RELEASE_OBJECT_PIN_HANDLER: self.source._handle_release_object_pin,
            }
            assert handler in methods
            return methods[handler](request)

        monkeypatch.setattr(node_module, "rpc_request", transfer)
        self.request = protocol.RequestWorkerLease(
            LeaseID(bytes((96,)) * 16), task, attempt, spec.resources,
            self.target.node_id, core.worker_id, target_node_id=self.target.node_id,
            dependencies=self.sources, return_ids=spec.return_ids(),
        )
        self.grant = self.target._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease
        self.inventory = self.target._lease_dependency_custody.snapshot(self.request.lease_id)
        assert self.inventory == protocol.LeaseDependencyInventory(
            self.request, self.target.node_id, self.grant.dependencies,
        )
        assert self.target._lease_dependency_custody.has_pending()
        core._validate_granted_dependencies(self.sources, self.grant)
        self.lease_state = _LeaseRequestState(self.request, self.target_address, self.target.node_id, True)
        assert len(self.transfers) == 6
        for descriptor, payload in zip(self.sources, self.payloads):
            assert self.target.object_store.get(descriptor.object_id) == payload
            assert self.target.object_store.snapshot(descriptor.object_id).pin_count == 1
            assert self.source.object_store.snapshot(descriptor.object_id).pin_count == 0
        # This is a real source deletion after localization, not a fabricated
        # loss flag or a test-side pop of owner/route metadata. The old route
        # remains present but no longer belongs to the owner's location set.
        assert core.drop_object(self.local_ref, node_id=self.source.node_id)
        self.assert_local_unrecorded()
        assert not self.source.object_store.contains(self.local_id)
        assert len(self.calls) == 1 and self.calls[0][1] == node_module.DROP_OBJECT_REPLICA_HANDLER

    def address(self, node_id):
        assert node_id in (self.source.node_id, self.target.node_id)
        return self.source_address if node_id == self.source.node_id else self.target_address

    def rpc(self, address, handler, request):
        self.calls.append((address, handler, request))
        assert len(self.calls) <= 13
        if handler == node_module.DROP_OBJECT_REPLICA_HANDLER:
            assert address == self.source_address and request.object_id == self.local_id
            return self.source._handle_drop_object_replica(request)
        assert address == self.target_address
        if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
            return self.ack_custody(request)
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert request == self.request
            result = self.target._handle_request_lease(request)
            assert result == self.grant and len(self.transfers) == 6
            return result
        assert handler == node_module.CANCEL_LEASE_HANDLER
        assert request.lease_request == self.request
        result = self.target._handle_cancel_worker_lease(request)
        self.cancels.append((request, result))
        assert result.accepted and result.cancelled and result.state is protocol.LeaseExecutionState.ABANDONED
        assert result.dependency_inventory == self.inventory and result.retired_grant == self.grant
        if self.cancel_losses:
            self.cancel_losses -= 1
            raise TransportTimeout("actual cancellation completed before ACK loss")
        return result

    def ack_custody(self, request):
        """Route the exact ACK only after the real owners have receipts."""
        assert type(request) is protocol.AckLeaseDependencyCustody
        assert request.requester_worker_id == self.consumer.worker_id
        assert request.inventory == self.inventory
        registry = self.target._lease_dependency_custody
        assert registry.snapshot(self.request.lease_id) == request.inventory
        assert registry.has_pending() and not self.custody_acks
        state = self.marker().obligation
        assert state.inventory == request.inventory and not state.custody_acknowledged
        assert len(state.local_receipts) == len(state.receipts) == 1
        local, foreign = state.local_receipts[0], state.receipts[0]
        assert type(local) is _ReplicaLocationReceipt and local.custody_transferred
        assert local.descriptor == self.grant.dependencies[0]
        assert type(foreign) is protocol.ReportRetainedObjectLocationReply and foreign.custody_transferred
        assert foreign.descriptor == self.grant.dependencies[1]
        assert (state.reports[0].request, foreign) == self.reports[-1]
        self.assert_local_recorded()
        self.assert_foreign_recorded()
        reply = self.target._handle_ack_lease_dependency_custody(request)
        assert type(reply) is protocol.AckLeaseDependencyCustodyReply
        assert reply.request == request and reply.accepted and reply.error is None
        assert not registry.has_pending() and registry.snapshot(self.request.lease_id) == self.inventory
        self.custody_acks.append((request, reply))
        return reply

    def borrow_rpc(self, address, handler, request):
        assert address == self.foreign.owner_address
        assert request.object_id == self.foreign_id and request.hold == self.foreign_hold
        if handler == "report_retained_object_location":
            if self.before_report is not None:
                self.before_report()
            result = self.foreign.report_retained_object_location(request)
            self.reports.append((request, result))
            assert len(self.reports) <= 3
            if self.report_losses:
                self.report_losses -= 1
                raise TransportTimeout("actual foreign location committed before ACK loss")
            return result
        assert handler == "release_owned_object_for_task"
        result = self.foreign.release_owned_object_for_task(request)
        self.releases.append((request, result))
        assert len(self.releases) <= 1
        return result

    def initial(self):
        reports = self.consumer._build_location_reports(self.sources, self.grant, (self.guard,))
        return _LocationReportState(
            self.grant, self.target_address, reports, lease_request=self.request, inventory=self.inventory,
        )

    def execute(self, state=None):
        options = {"lease_state": self.lease_state} if state is None else {"location_state": state}
        return self.consumer._execute(self.pending, self.pending.spec, self.sources, **options)

    def marker(self):
        marker = self.consumer._protocol_unresolved[self.pending.task_key]
        state = marker.obligation
        assert isinstance(state, _LocationReportState)
        assert state.grant == self.grant and state.lease_request == self.request
        assert state.inventory == self.inventory
        assert tuple(item.owner_worker_id for item in state.grant.dependencies) == (
            self.consumer.worker_id, self.foreign.worker_id,
        )
        assert len(state.reports) == 1 and state.reports[0].guard == self.guard
        assert state.reports[0].request.descriptor == self.grant.dependencies[1]
        return marker

    def delayed(self):
        selected = []
        count = self.consumer._submissions.qsize()
        assert count <= 4
        for _ in range(count):
            item = self.consumer._submissions.get_nowait()
            self.consumer._submissions.task_done()
            if isinstance(item, _DelayedReadyTask):
                selected.append(item)
        assert len(selected) == 1
        state = selected[0].ready.location_state
        assert state == self.marker().obligation and selected[0].ready.cancellation is None
        return state

    def assert_local_unrecorded(self):
        snapshot = self.consumer.owner_table.snapshot(self.local_id)
        assert snapshot.state is ObjectState.LOST and not snapshot.locations
        assert snapshot.canonical_stored_result == self.original_results[0]
        assert self.consumer._stored_descriptors[self.local_id] == self.original_results[0]

    def assert_local_recorded(self):
        snapshot = self.consumer.owner_table.snapshot(self.local_id)
        assert snapshot.state is ObjectState.READY_STORED
        assert snapshot.current_attempt == self.sources[0].producer_attempt_id
        assert snapshot.locations == frozenset((self.target.node_id,))
        assert snapshot.canonical_stored_result == self.original_results[0]
        assert self.consumer._stored_descriptors[self.local_id] == replace(
            self.original_results[0], node_id=self.target.node_id,
        )
        assert self.target.object_store.get(self.local_id) == self.payloads[0]

    def assert_foreign_recorded(self):
        snapshot = self.foreign.owner_table.snapshot(self.foreign_id)
        assert snapshot.locations == frozenset((self.source.node_id, self.target.node_id))
        assert snapshot.canonical_stored_result == self.original_results[1]
        assert self.target.object_store.get(self.foreign_id) == self.payloads[1]

    def assert_cancelled(self):
        assert self.cancels
        assert self.target._leases[self.grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        assert all(self.target.object_store.snapshot(item.object_id).pin_count == 0 for item in self.sources)
        assert all(request == self.cancels[0][0] for request, _ in self.cancels)
        assert self.cancels[0][1].released
        assert all(not reply.released for _, reply in self.cancels[1:])

    def assert_pending(self):
        assert self.consumer.owner_table.snapshot(self.pending.object_id).state is ObjectState.PENDING
        assert not self.consumer._finish_pending_task(self.pending)
        assert not self.releases
        assert self.consumer._recovery.task_record(self.pending.task_id).retries_started == 0

    def assert_terminal(self, error):
        snapshot = self.consumer.owner_table.snapshot(self.pending.object_id)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is error
        assert self.pending.task_key not in self.consumer._protocol_unresolved
        record = self.consumer._recovery.task_record(self.pending.task_id)
        assert record.current_attempt == self.pending.spec.attempt_id and record.retries_started == 0
        assert not self.consumer._location_handoff_drivers
        assert len(self.custody_acks) == 1 and self.states[-1][1].custody_acknowledged
        assert not self.target._lease_dependency_custody.has_pending()
        self.assert_cancelled()
        # The only deletion allowed here was the explicit source failpoint.
        assert sum(handler == node_module.DROP_OBJECT_REPLICA_HANDLER for _, handler, _ in self.calls) == 1

    def finish(self):
        assert self.consumer._finish_pending_task(self.pending)
        assert len(self.releases) == 1
        assert self.local_hold not in self.consumer.owner_table.snapshot(self.local_id).submitted_tokens
        assert not self.foreign.owner_table.has_retained_reference_for_task(self.foreign_id, self.foreign_hold)

    def close(self):
        if self.target._leases[self.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED:
            self.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
                self.request.lease_id, self.request.task_id, self.request.attempt_id,
                self.request.requester_node_id, self.request.requester_worker_id, self.request.scheduling_key,
                lease_request=self.request,
            ))
        self.consumer.owner_table.release_submitted_reference(self.local_id, self.local_hold)
        self.foreign.owner_table.release_retained_reference_for_task(self.foreign_id, self.foreign_hold)
        self.local_ref.close()
        for core in (self.consumer, self.foreign):
            for output in core._objects:
                for token in core.owner_table.snapshot(output).local_tokens:
                    core.owner_table.release_local_reference(output, token)
            close_pure_core(core)


def _route_fault(f, *, after):
    class Routes(dict):
        def __init__(self):
            super().__init__(f.consumer._stored_descriptors)
            self.failed = False
            self.writes = []

        def __setitem__(self, key, value):
            self.writes.append((key, value))
            assert len(self.writes) <= 6
            should_fail = key == f.local_id and value.node_id == f.target.node_id and not self.failed
            if should_fail:
                self.failed = True
                state = f.marker().obligation
                assert state.local_receipts == () and state.receipts == ()
                if after:
                    super().__setitem__(key, value)
                raise RuntimeError("local route failed {} write".format("after" if after else "before"))
            super().__setitem__(key, value)

    routes = Routes()
    f.consumer._stored_descriptors = routes
    return routes


def _owner_fault(f, monkeypatch, *, after):
    actual = f.consumer.owner_table.add_location
    calls = []

    def add(*args, **kwargs):
        assert args[0] == f.local_id
        calls.append((args, kwargs))
        assert len(calls) <= 2
        if len(calls) == 1:
            state = f.marker().obligation
            assert state.local_receipts == () and state.receipts == ()
            if after:
                assert actual(*args, **kwargs)
            raise RuntimeError("local owner failed {} effect".format("after" if after else "before"))
        return actual(*args, **kwargs)

    monkeypatch.setattr(f.consumer.owner_table, "add_location", add)
    return calls


def test_inventory_is_pure_and_full_marker_precedes_local_effect_with_older_logical_hold(monkeypatch):
    # Admit a later physical consumer attempt while retaining its original
    # logical holds. This checks hold permission, not the SYSTEM retry engine.
    f = _Fixture(monkeypatch, consumer_attempt=1)
    try:
        core = f.consumer
        before = (core.owner_table.snapshot(f.local_id), f.foreign.owner_table.snapshot(f.foreign_id),
                  dict(core._stored_descriptors), dict(f.foreign._stored_descriptors),
                  tuple(f.calls), tuple(core._submissions.queue), tuple(core._reference_mailbox.pending.queue))
        initial = f.initial()
        assert initial.local_receipts == initial.receipts == ()
        assert initial.inventory == f.inventory and not initial.custody_acknowledged
        assert initial.terminal_error is initial.cancellation_reply is None
        assert not core._protocol_unresolved
        assert before == (core.owner_table.snapshot(f.local_id), f.foreign.owner_table.snapshot(f.foreign_id),
                          dict(core._stored_descriptors), dict(f.foreign._stored_descriptors),
                          tuple(f.calls), tuple(core._submissions.queue), tuple(core._reference_mailbox.pending.queue))
        assert f.local_hold.origin_attempt_id.attempt_number == 0
        assert f.pending.spec.attempt_id.attempt_number == 1
        assert f.local_hold.origin_attempt_id != f.sources[0].producer_attempt_id
        seen = []
        actual = core._record_replica_custody_locked

        def observe(descriptor, *, active_hold):
            state = f.marker().obligation
            assert state.grant == initial.grant and state.reports == initial.reports
            assert state.local_receipts == state.receipts == () and not seen
            f.assert_local_unrecorded()
            snapshot = core.owner_table.snapshot(f.local_id)
            assert active_hold(snapshot) and f.local_hold in snapshot.submitted_tokens
            reply = actual(descriptor, active_hold=active_hold)
            seen.append(reply)
            return reply

        monkeypatch.setattr(core, "_record_replica_custody_locked", observe)
        # Only the foreign hold is revoked; the local original hold must still
        # authorize adoption for consumer attempt 1 without being fabricated.
        assert f.foreign.owner_table.release_retained_reference_for_task(f.foreign_id, f.foreign_hold)
        assert f.execute()
        state = f.states[-1][1]
        assert len(seen) == 1 and seen[0].status is protocol.RetainedLocationReportStatus.ADDED
        assert state.local_receipts == tuple(seen)
        assert state.receipts[0].status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert "hold is not active" in str(state.terminal_error)
        f.assert_local_recorded()
        f.assert_foreign_recorded()
        f.assert_terminal(state.terminal_error)
        assert len(f.reports) == len(f.cancels) == 1
        f.finish()
    finally:
        f.close()


@pytest.mark.parametrize("after", (False, True), ids=("before-write", "after-write"))
def test_route_failure_retains_full_grant_cancels_then_repairs_only_missing_local_receipt(monkeypatch, after):
    f = _Fixture(monkeypatch)
    try:
        routes = _route_fault(f, after=after)

        def before_foreign():
            f.assert_cancelled()
            state = f.marker().obligation
            assert state.local_receipts == () and isinstance(state.terminal_error, SystemTaskError)
            f.assert_local_unrecorded()

        f.before_report = before_foreign
        assert not f.execute()
        assert routes.failed
        f.assert_pending()
        state = f.delayed()
        assert isinstance(state.terminal_error, SystemTaskError)
        assert "local route failed" in str(state.terminal_error)
        assert state.local_receipts == () and len(state.receipts) == 1
        assert state.cancellation_reply is not None and state.receipts[0].custody_transferred
        assert state.acknowledged_keys == (f.consumer._foreign_guard_key(f.guard),)
        f.assert_local_unrecorded()
        f.assert_foreign_recorded()
        before_calls = tuple(f.calls)
        assert f.execute(state)
        f.assert_terminal(state.terminal_error)
        f.assert_local_recorded()
        assert tuple(f.calls[:-1]) == before_calls and len(f.reports) == len(f.cancels) == 1
        assert f.calls[-1] == (f.target_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER, f.custody_acks[0][0])
        latest = f.states[-1][1]
        assert latest.terminal_error is state.terminal_error
        assert len(latest.local_receipts) == 1 and latest.local_receipts[0].accepted
        assert len([value for key, value in routes.writes if key == f.local_id and value.node_id == f.target.node_id]) == 2
        f.finish()
    finally:
        f.close()


@pytest.mark.parametrize("after", (False, True), ids=("before-owner-effect", "after-owner-effect"))
def test_owner_effect_failure_preserves_true_location_and_replays_without_undoing_committed_route(monkeypatch, after):
    f = _Fixture(monkeypatch)
    try:
        attempts = _owner_fault(f, monkeypatch, after=after)
        f.before_report = f.assert_cancelled
        assert not f.execute()
        state = f.delayed()
        f.assert_pending()
        assert state.local_receipts == () and len(state.receipts) == 1
        assert isinstance(state.terminal_error, SystemTaskError) and "local owner failed" in str(state.terminal_error)
        assert len(attempts) == 1
        if after:
            f.assert_local_recorded()
        else:
            f.assert_local_unrecorded()
        f.assert_foreign_recorded()
        assert f.execute(state)
        f.assert_terminal(state.terminal_error)
        f.assert_local_recorded()
        assert len(attempts) == 2 and attempts[0] == attempts[1]
        latest = f.states[-1][1]
        expected = (protocol.RetainedLocationReportStatus.ALREADY_RECORDED if after
                    else protocol.RetainedLocationReportStatus.ADDED)
        assert latest.local_receipts[0].status is expected
        assert len(f.reports) == len(f.cancels) == 1
        f.finish()
    finally:
        f.close()


def test_old_replay_preserves_local_foreign_and_cancel_progress_across_bounded_ack_losses(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        initial = f.initial()
        attempts = _owner_fault(f, monkeypatch, after=True)
        f.report_losses, f.cancel_losses = 1, 2
        f.before_report = f.assert_cancelled
        assert not f.execute(initial)
        first = f.delayed()
        f.assert_pending()
        assert first.local_receipts == first.receipts == () and first.cancellation_reply is None
        assert isinstance(first.terminal_error, SystemTaskError)
        f.assert_local_recorded()
        f.assert_foreign_recorded()
        # Replay the original progress-free item. The marker, not the queued
        # copy, owns the sticky failure and previously completed cancellation.
        assert not f.execute(initial)
        second = f.delayed()
        f.assert_pending()
        assert second.terminal_error is first.terminal_error and second.cancellation_reply is None
        assert len(second.local_receipts) == len(second.receipts) == 1
        assert second.local_receipts[0].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert second.receipts[0].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert second.acknowledged_keys == (f.consumer._foreign_guard_key(f.guard),)
        assert len(attempts) == len(f.reports) == len(f.cancels) == 2
        assert f.reports[0][0] == f.reports[1][0]
        before = tuple(f.reports), tuple(attempts)
        assert f.execute(initial)
        f.assert_terminal(first.terminal_error)
        assert (tuple(f.reports), tuple(attempts)) == before
        assert len(f.cancels) == 3
        assert not any(handler == node_module.REQUEST_LEASE_HANDLER for _, handler, _ in f.calls)
        latest = f.states[-1][1]
        assert latest.local_receipts == second.local_receipts and latest.receipts == second.receipts
        assert latest.terminal_error is first.terminal_error and latest.cancellation_reply is not None
        assert initial.local_receipts == initial.receipts == () and initial.terminal_error is None
        f.finish()
    finally:
        f.close()


def test_new_local_producer_epoch_before_replay_is_quarantined_without_overwrite_or_deletion(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        routes = _route_fault(f, after=False)
        assert not f.execute()
        first = f.delayed()
        old = f.sources[0]
        newer = old.producer_attempt_id.next()
        # Advance real owner state and seal a new source result. This is a
        # competing owner-reducer admission, not a claim that put lineage was
        # reconstructed or that the old target bytes were authorized to drop.
        assert f.consumer.owner_table.advance_attempt(
            f.local_id, expected_attempt=old.producer_attempt_id, next_attempt=newer,
        )
        payload = b"new-local-epoch"
        seal = protocol.SealObject.from_data(f.local_id, newer, f.consumer.worker_id, payload)
        assert f.source._handle_seal_object(seal).sealed
        result = protocol.ResultDescriptor(
            f.local_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
            f.consumer.worker_id, f.source.node_id, seal.checksum,
        )
        assert f.consumer.owner_table.publish_stored(f.local_id, newer, f.source.node_id, descriptor=result)
        routes[f.local_id] = result
        current = f.consumer.owner_table.snapshot(f.local_id)
        before_calls = tuple(f.calls), tuple(f.reports), tuple(f.cancels)
        assert not f.execute(first)
        marker = f.marker()
        assert marker.phase == "location_quarantined"
        state = marker.obligation
        assert state.terminal_error is first.terminal_error and state.cancellation_reply == first.cancellation_reply
        assert len(state.local_receipts) == 1
        receipt = state.local_receipts[0]
        assert receipt.descriptor == f.grant.dependencies[0]
        assert receipt.status is protocol.RetainedLocationReportStatus.STALE_PRODUCER
        assert not receipt.accepted and not receipt.custody_transferred
        assert state.receipts == first.receipts
        assert f.consumer.owner_table.snapshot(f.local_id) == current
        assert routes[f.local_id] == result
        assert f.source.object_store.get(f.local_id) == payload
        assert f.target.object_store.get(f.local_id) == f.payloads[0]
        assert not f.consumer._has_late_replica_cleanup_locked()
        assert f.consumer._submissions.empty()
        assert not f.execute(first)
        assert (tuple(f.calls), tuple(f.reports), tuple(f.cancels)) == before_calls
        assert f.consumer.owner_table.snapshot(f.local_id) == current and routes[f.local_id] == result
        assert f.consumer._submissions.empty()
        f.assert_pending()
        f.assert_cancelled()
    finally:
        f.close()


def test_local_hold_released_during_foreign_report_keeps_custody_but_revokes_push(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        seen = []

        def release_during_foreign_call():
            state = f.marker().obligation
            assert len(state.local_receipts) == 1 and state.local_receipts[0].accepted
            assert state.terminal_error is None and state.cancellation_reply is None
            assert not f.cancels and not seen
            f.assert_local_recorded()
            assert f.consumer.owner_table.release_submitted_reference(f.local_id, f.local_hold)
            seen.append(state.local_receipts[0])

        f.before_report = release_during_foreign_call
        assert f.execute()
        state = f.states[-1][1]
        assert len(seen) == len(state.local_receipts) == 1
        assert seen[0].status is protocol.RetainedLocationReportStatus.ADDED
        local = state.local_receipts[0]
        assert local.descriptor == seen[0].descriptor
        assert local.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert local.custody_transferred and not local.accepted
        assert len(state.receipts) == 1 and state.receipts[0].accepted
        assert "hold is not active" in str(state.terminal_error)
        f.assert_terminal(state.terminal_error)
        f.assert_local_recorded()
        f.assert_foreign_recorded()
        assert len(f.cancels) == len(f.reports) == 1
        assert any(isinstance(event, _RetryInlineGc) and event.object_id == f.local_id
                   for event in tuple(f.consumer._reference_mailbox.pending.queue))
        f.finish()
    finally:
        f.close()
