"""Pure two-owner handoff through one actual two-dependency Node grant.

Three threadless Cores, two 1 KiB Node stores, two tiny sealed input values,
one unexecuted consumer and one unstarted GCS. One real grant performs exactly
six source pin/chunk/release calls; cancellation, every location report and
exact dependency-inventory ACK use actual reducers/handlers. Only delivery
faults are injected, after real effect.

This isolates the pre-Push retained-hold slice: consumer outputs are registered
directly, with no normal foreign-lineage registry entry. Therefore finish
releases its input holds here; this is not a claim that normal lineage holds
are released before consumer-output GC. Owner death is consumed from the GCS
journal, and its physical cleanup remains delegated to the GCS fence outbox.

No process, thread, socket, listener, timer, wait or user function runs.
"""

from __future__ import annotations

from dataclasses import replace
import queue
import threading
from types import SimpleNamespace

import pytest

from miniray import control, node as node_module, protocol
from miniray.core import _HomeRoute
from miniray.core import (
    _DelayedReadyTask, _ForeignDependencyGuard, _LeaseRequestState,
    _LocationReportState, _NodeDeathObserved, _ObjectWaiter, _PendingTask,
)
from miniray.errors import OwnerDiedError, SystemTaskError
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
    def __init__(self, monkeypatch):
        self.consumer = consumer = make_pure_core()
        self.owners = (make_pure_core(), make_pure_core())
        self.source = _bare_node(NodeID(bytes((51,)) * 16), ResourceVector({"CPU": 1}))
        self.target = _bare_node(NodeID(bytes((52,)) * 16), ResourceVector({"CPU": 1}))
        self.source_address, self.target_address = ("source.invalid", 1), ("target.invalid", 2)
        self.gcs_address = ("gcs.invalid", 3)
        for node in (self.source, self.target):
            node._object_store = ObjectStore(1024)
            node._object_manager = ObjectManager(node.node_id, node._object_store)
            node.event_sink = None
        self.target._workers[self.target.worker_id].process = SimpleNamespace(is_alive=lambda: True)
        self.target._workers[self.target.worker_id].address = ("worker.invalid", 4)
        self.target._cluster_addresses = {self.source.node_id: self.source_address}
        self.service = control.GCSLite()
        for node, pid, address in ((self.source, 1701, self.source_address),
                                   (self.target, 1702, self.target_address)):
            result = self.service.register_node(protocol.RegisterNode(
                node.node_id, pid, address, ResourceVector({"CPU": 1}),
            ))
            assert result.accepted
        self.source_info = self.service.nodes.get(self.source.node_id)
        self.target_info = self.service.nodes.get(self.target.node_id)
        consumer.node_id, consumer.node_address = self.target.node_id, self.target_address
        consumer._home_route = _HomeRoute(consumer.node_id, consumer.node_address, consumer._membership_epoch)
        consumer.gcs_address = self.gcs_address
        consumer._ready_tasks = queue.Queue()  # Explicit death wake FIFO, never a dispatch thread.
        self.calls, self.transfers, self.reports, self.releases, self.cancels = [], [], [], [], []
        self.custody_acks = []
        self.lose_report_once = set()
        self.lost_reports = set()
        self.lose_cancel_ack = False
        self.cancel_ack_lost = False
        self.before_cancel = self.before_report = None
        self.incarnations, sources = [], []
        self.payloads = (b"owner-one", b"owner-two")
        for index, (owner, payload) in enumerate(zip(self.owners, self.payloads)):
            owner.node_id, owner.node_address = self.source.node_id, self.source_address
            owner._home_route = _HomeRoute(owner.node_id, owner.node_address, owner._membership_epoch)
            owner.owner_address = ("owner-{}.invalid".format(index), 10 + index)
            task = TaskID.derive(consumer.job_id, consumer.driver_task_id, 61 + index)
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
            incarnation = protocol.WorkerIncarnation(
                self.source.node_id, self.source_info.node_pid, self.source_info.registration_epoch,
                owner.worker_id, 1801 + index,
            )
            assert self.service.register_worker_incarnation(protocol.RegisterWorkerIncarnation(incarnation)).accepted
            self.incarnations.append(incarnation)
        self.sources = tuple(sources)
        task = TaskID.derive(consumer.job_id, consumer.driver_task_id, 71)
        attempt = AttemptID(task, 0)
        spec = protocol.TaskSpec(
            consumer.job_id, task, attempt,
            protocol.FunctionKey(consumer.job_id, __name__, "never-executed", "v1"),
            tuple(protocol.RefArg(item.object_id, item.owner_worker_id) for item in self.sources),
            1, ResourceVector({"CPU": 1}), consumer.worker_id, max_retries=1,
        )
        self.hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, consumer.worker_id, task, attempt,
        )
        guards = []
        for index, (owner, descriptor) in enumerate(zip(self.owners, self.sources)):
            borrower = (consumer.worker_id, "source-borrow-{}".format(index))
            assert owner.owner_table.add_borrowed_reference(descriptor.object_id, borrower)
            assert owner.owner_table.retain_borrowed_reference_for_task(descriptor.object_id, borrower, self.hold)
            assert owner.owner_table.release_borrowed_reference(descriptor.object_id, borrower)
            guards.append(_ForeignDependencyGuard(
                descriptor.object_id, owner.worker_id, owner.owner_address, consumer.worker_id, borrower[1], self.hold,
            ))
        self.guards = tuple(guards)
        consumer.owner_table.register_task_outputs(spec, local_tokens=("consumer-local",))
        consumer._recovery.register_task(spec, max_retries=1)
        consumer._objects[spec.return_ids()[0]] = _ObjectWaiter(threading.Event())
        self.pending = _PendingTask(spec.return_ids()[0], spec, foreign_dependency_guards=self.guards)
        consumer._accepted_task_count += 1
        consumer._install_task_finish_barrier_locked(self.pending)
        consumer._registered_functions = set()
        consumer._rpc = self.rpc
        consumer._borrow_rpc = self.borrow_rpc

        def no_push(*_args, **_kwargs):
            pytest.fail("failed multi-owner handoff reached user execution")

        consumer._push_task_rpc = no_push

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
            LeaseID(bytes((72,)) * 16), task, attempt, spec.resources,
            self.target.node_id, consumer.worker_id, target_node_id=self.target.node_id,
            dependencies=self.sources, return_ids=spec.return_ids(),
        )
        self.grant = self.target._handle_request_lease(self.request)
        assert type(self.grant) is protocol.GrantWorkerLease
        with self.target._state_lock:
            self.inventory = self.target._dependency_custody_registry_locked().snapshot(self.request.lease_id)
            assert self.target._dependency_custody_registry_locked().has_pending()
        assert self.inventory == protocol.LeaseDependencyInventory(self.request, self.target.node_id, self.grant.dependencies)
        consumer._validate_granted_dependencies(self.sources, self.grant)
        self.lease_state = _LeaseRequestState(self.request, self.target_address, self.target.node_id, True)
        assert len(self.transfers) == 6
        assert all(self.target.object_store.snapshot(item.object_id).pin_count == 1 for item in self.sources)
        assert all(self.source.object_store.snapshot(item.object_id).pin_count == 0 for item in self.sources)
        for descriptor, payload, owner in zip(self.sources, self.payloads, self.owners):
            assert self.target.object_store.get(descriptor.object_id) == payload
            assert owner.owner_table.snapshot(descriptor.object_id).locations == frozenset((self.source.node_id,))

    def rpc(self, address, handler, request):
        self.calls.append((address, handler, request))
        assert len(self.calls) <= 16
        if address == self.gcs_address:
            assert handler == control.GET_WORKER_DEATHS_HANDLER
            return self.service.get_worker_deaths(request)
        assert address == self.target_address
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert request == self.request
            result = self.target._handle_request_lease(request)
            assert result == self.grant and len(self.transfers) == 6
            return result
        if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
            assert type(request) is protocol.AckLeaseDependencyCustody
            assert not self.consumer._node_is_dead(self.target.node_id)
            assert request.requester_worker_id == self.consumer.worker_id and request.inventory == self.inventory
            state = self.marker().obligation
            assert state.inventory == self.inventory and not state.custody_acknowledged
            assert state.cancellation_reply is not None and state.cancellation_reply.cancelled
            for report in state.reports:
                owner = report.guard.owner_worker_id
                custody = any(reply.owner_worker_id == owner and reply.object_id == report.request.object_id
                              and reply.custody_transferred for reply in state.receipts)
                delegated = self.consumer.owner_table.dead_worker_record(owner) is not None
                assert custody or delegated, "Node inventory ACK preceded owner custody or authoritative death"
            with self.target._state_lock:
                assert self.target._dependency_custody_registry_locked().has_pending()
            result = self.target._handle_ack_lease_dependency_custody(request)
            self.custody_acks.append((request, result))
            assert len(self.custody_acks) == 1
            assert type(result) is protocol.AckLeaseDependencyCustodyReply and result.accepted and result.request == request
            with self.target._state_lock:
                assert not self.target._dependency_custody_registry_locked().has_pending()
            return result
        assert handler == node_module.CANCEL_LEASE_HANDLER
        if self.before_cancel is not None:
            self.before_cancel()
        result = self.target._handle_cancel_worker_lease(request)
        self.cancels.append((request, result))
        if self.lose_cancel_ack and not self.cancel_ack_lost:
            assert result.accepted and result.cancelled and result.released
            self.cancel_ack_lost = True
            raise TransportTimeout("actual cancellation committed before ACK loss")
        return result

    def borrow_rpc(self, address, handler, request):
        index = next(index for index, owner in enumerate(self.owners) if owner.owner_address == address)
        owner = self.owners[index]
        assert not self.consumer._owner_is_dead(owner.worker_id), "contacted an authoritatively dead owner"
        assert request.object_id == self.sources[index].object_id and request.hold == self.hold
        if handler == "report_retained_object_location":
            if self.before_report is not None:
                self.before_report(index)
            result = owner.report_retained_object_location(request)
            self.reports.append((index, request, result))
            assert len(self.reports) <= 4
            if index in self.lose_report_once and index not in self.lost_reports:
                self.lost_reports.add(index)
                raise TransportTimeout("owner location mutation committed before ACK loss")
            return result
        assert handler == "release_owned_object_for_task"
        self.releases.append(index)
        return owner.release_owned_object_for_task(request)

    def release_first_hold(self):
        assert self.owners[0].owner_table.release_retained_reference_for_task(self.sources[0].object_id, self.hold)

    def execute(self, state=None):
        options = {"lease_state": self.lease_state} if state is None else {"location_state": state}
        return self.consumer._execute(self.pending, self.pending.spec, self.sources, **options)

    def initial_handoff(self):
        # The grant is actual; preparing its two remote reports has no owner
        # effect. Save this deliberately progress-free queued work for replay.
        reports = self.consumer._build_location_reports(
            self.sources, self.grant, self.guards,
        )
        return _LocationReportState(
            self.grant, self.target_address, reports, lease_request=self.request,
            inventory=self.inventory,
        )

    def marker(self):
        marker = self.consumer._protocol_unresolved[self.pending.task_key]
        assert type(marker.obligation) is _LocationReportState
        assert marker.obligation.grant == self.grant and marker.obligation.lease_request == self.request
        assert marker.obligation.inventory == self.inventory
        return marker

    def delayed(self):
        selected = []
        count = self.consumer._submissions.qsize()
        assert count <= 8
        for _ in range(count):
            item = self.consumer._submissions.get_nowait()
            self.consumer._submissions.task_done()
            if isinstance(item, _DelayedReadyTask):
                selected.append(item)
        assert len(selected) == 1
        assert selected[0].ready.location_state == self.marker().obligation
        assert selected[0].ready.cancellation is None
        return selected[0].ready.location_state

    def assert_cancelled(self):
        assert self.target._leases[self.grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        assert all(self.target.object_store.snapshot(item.object_id).pin_count == 0 for item in self.sources)
        assert all(request == self.cancels[0][0] for request, _ in self.cancels)

    def assert_pending(self):
        assert self.consumer.owner_table.snapshot(self.pending.object_id).state is ObjectState.PENDING
        assert not self.consumer._finish_pending_task(self.pending)
        assert self.releases == []
        assert self.consumer._recovery.task_record(self.pending.task_id).retries_started == 0

    def assert_terminal(self, message):
        snapshot = self.consumer.owner_table.snapshot(self.pending.object_id)
        assert snapshot.state is ObjectState.ERROR and message in str(snapshot.error)
        assert self.pending.task_key not in self.consumer._protocol_unresolved
        record = self.consumer._recovery.task_record(self.pending.task_id)
        assert record.current_attempt == self.pending.spec.attempt_id and record.retries_started == 0
        if not self.consumer._node_is_dead(self.target.node_id):
            assert len(self.custody_acks) == 1
            with self.target._state_lock:
                assert not self.target._dependency_custody_registry_locked().has_pending()

    def install_owner_death(self, index):
        result = self.service.report_worker_death(protocol.ReportWorkerDeath(
            "multi-owner-exit-{}".format(index), self.incarnations[index], -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        ))
        assert result.death is not None
        assert self.consumer._sync_worker_deaths()
        record = self.consumer.owner_table.dead_worker_record(self.owners[index].worker_id)
        assert record is not None
        return result.death, record

    def close(self):
        if not self.consumer._node_is_dead(self.target.node_id):
            record = self.target._leases[self.grant.lease_id]
            if record.state is protocol.LeaseExecutionState.GRANTED:
                self.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
                    self.request.lease_id, self.request.task_id, self.request.attempt_id,
                    self.request.requester_node_id, self.request.requester_worker_id, self.request.scheduling_key,
                ))
        for owner, descriptor in zip(self.owners, self.sources):
            if not self.consumer._owner_is_dead(owner.worker_id):
                owner.owner_table.release_retained_reference_for_task(descriptor.object_id, self.hold)
        for core in (*self.owners, self.consumer):
            for output in core._objects:
                for token in core.owner_table.snapshot(output).local_tokens:
                    core.owner_table.release_local_reference(output, token)
            close_pure_core(core)


def test_custody_only_first_cancels_before_later_owner_and_replays_only_missing_ack(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.release_first_hold()
        f.lose_report_once.add(1)

        def observe(index):
            if index == 1:
                f.assert_cancelled()
                assert f.consumer.owner_table.snapshot(f.pending.object_id).state is ObjectState.PENDING
                assert f.owners[1].owner_table.has_retained_reference_for_task(f.sources[1].object_id, f.hold)

        f.before_report = observe
        assert not f.execute()
        f.assert_pending()
        state = f.delayed()
        assert state.terminal_error is not None and "task hold is not active" in str(state.terminal_error)
        assert state.cancellation_reply is not None and state.cancellation_reply.cancelled
        assert len(state.receipts) == 1 and state.receipts[0].status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert state.receipts[0].custody_transferred and not state.receipts[0].accepted
        assert state.acknowledged_keys == (f.consumer._foreign_guard_key(f.guards[0]),)
        assert f.owners[1].owner_table.snapshot(f.sources[1].object_id).locations == frozenset((f.source.node_id, f.target.node_id))
        assert f.execute(state)
        f.assert_terminal(str(state.terminal_error))
        assert [index for index, _, _ in f.reports] == [0, 1, 1]
        assert f.reports[1][1] == f.reports[2][1]
        assert f.reports[2][2].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert len(f.cancels) == 1 and f.releases == []
        assert f.consumer._finish_pending_task(f.pending) and f.releases == [0, 1]
        assert all(not owner.owner_table.has_retained_reference_for_task(descriptor.object_id, f.hold)
                   for owner, descriptor in zip(f.owners, f.sources))
    finally:
        f.close()


def test_missing_first_ack_still_visits_later_owner_and_freezes_its_rejection(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        # The first owner accepts, but delivery is ambiguous. The second
        # independently returns custody-only and must still revoke this grant.
        assert f.owners[1].owner_table.release_retained_reference_for_task(f.sources[1].object_id, f.hold)
        f.lose_report_once.add(0)
        assert not f.execute()
        f.assert_pending()
        f.assert_cancelled()
        state = f.delayed()
        assert len(state.receipts) == 1
        assert state.receipts[0].owner_worker_id == f.owners[1].worker_id
        assert state.receipts[0].status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert state.terminal_error is not None and state.cancellation_reply is not None
        assert state.acknowledged_keys == (f.consumer._foreign_guard_key(f.guards[1]),)
        assert [index for index, _, _ in f.reports] == [0, 1]
        assert f.execute(state)
        f.assert_terminal(str(state.terminal_error))
        assert [index for index, _, _ in f.reports] == [0, 1, 0]
        assert f.reports[0][1] == f.reports[2][1]
        assert len(f.cancels) == 1 and f.consumer._finish_pending_task(f.pending)
        assert f.releases == [0, 1]
    finally:
        f.close()


def test_cancel_ack_loss_keeps_full_handoff_and_does_not_repeat_owner_receipts(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.release_first_hold()
        f.lose_cancel_ack = True
        f.before_report = lambda index: f.assert_cancelled() if index == 1 else None
        assert not f.execute()
        f.assert_pending()
        f.assert_cancelled()
        state = f.delayed()
        assert state.cancellation_reply is None and state.terminal_error is not None
        assert len(state.receipts) == len(state.acknowledged_keys) == 2
        assert all(reply.custody_transferred for reply in state.receipts)
        assert f.execute(state)
        f.assert_terminal(str(state.terminal_error))
        assert len(f.reports) == 2 and len(f.cancels) == 2
        assert f.cancels[0][1].released and not f.cancels[1][1].released
        assert f.releases == [] and f.consumer._finish_pending_task(f.pending)
        assert f.releases == [0, 1]
    finally:
        f.close()


def test_dead_first_owner_still_hands_healthy_replica_off_after_real_cancel(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        death, record = f.install_owner_death(0)
        seen = []

        def observe(index):
            assert index == 1
            f.assert_cancelled()
            state = f.marker().obligation
            assert state.owner_deaths == (record,)
            assert isinstance(state.terminal_error, OwnerDiedError)
            seen.append(state)

        f.before_report = observe
        assert f.execute()
        f.assert_terminal("confirmed dead")
        assert len(seen) == len(f.cancels) == 1
        assert [index for index, _, _ in f.reports] == [1]
        healthy = f.owners[1].owner_table.snapshot(f.sources[1].object_id)
        assert healthy.locations == frozenset((f.source.node_id, f.target.node_id))
        assert f.service.owner_death_fences.pending_for_owner(death.worker_id)
        # No test-side sweep substitutes for the GCS outbox. The replica is
        # unpinned but remains physically present until that authority cleans it.
        assert f.target.object_store.get(f.sources[0].object_id) == f.payloads[0]
        assert f.consumer._finish_pending_task(f.pending) and f.releases == [1]
    finally:
        f.close()


def test_target_death_preserves_sticky_failure_instead_of_retrying_consumer(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.release_first_hold()
        f.lose_report_once.add(1)
        assert not f.execute()
        state = f.delayed()
        before = (len(f.reports), len(f.cancels))
        result = f.service.publications.commit_node_death(lambda: f.service.nodes.report_death(
            protocol.ReportNodeDeath(
                "multi-owner-target-exit", f.target.node_id, f.target_info.node_pid, f.target_info.registration_epoch,
                2, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed target exit",
            ),
        ))
        epoch, live = f.service.nodes.live_snapshot()
        f.consumer.handle_node_death(result.death, protocol.InstallClusterSnapshot(epoch, "target-lost", live))
        assert f.execute(state)
        f.assert_terminal(str(state.terminal_error))
        assert (len(f.reports), len(f.cancels)) == before
        assert f.consumer._ready_tasks.empty()  # No replacement publication/retry work.
        assert f.consumer._finish_pending_task(f.pending)
        assert f.releases == [0, 1]
    finally:
        f.close()


def test_definitive_noncustody_rejection_is_quarantined_without_skipping_healthy_owner(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.owners[0]._owner_protocol_open = False  # The endpoint truly rejects; no forged reply.
        f.before_report = lambda index: f.assert_cancelled() if index == 1 else None
        assert not f.execute()
        marker = f.marker()
        state = marker.obligation
        assert marker.phase == "location_quarantined"
        assert state.terminal_error is not None and state.cancellation_reply is not None
        assert tuple(reply.status for reply in state.receipts) == (
            protocol.RetainedLocationReportStatus.REJECTED, protocol.RetainedLocationReportStatus.ADDED,
        )
        assert not state.receipts[0].custody_transferred and state.receipts[1].custody_transferred
        assert not f.custody_acks
        with f.target._state_lock:
            assert f.target._dependency_custody_registry_locked().has_pending()
        assert state.acknowledged_keys == (f.consumer._foreign_guard_key(f.guards[1]),)
        assert f.consumer._submissions.empty()
        f.assert_pending()
        before = len(f.calls), len(f.reports), len(f.cancels)
        assert not f.execute(state)
        assert (len(f.calls), len(f.reports), len(f.cancels)) == before
        assert f.consumer._submissions.empty()
        assert f.owners[1].owner_table.snapshot(f.sources[1].object_id).locations == frozenset((f.source.node_id, f.target.node_id))
        assert f.owners[0].owner_table.snapshot(f.sources[0].object_id).locations == frozenset((f.source.node_id,))
        assert all(f.target.object_store.get(item.object_id) == payload for item, payload in zip(f.sources, f.payloads))
        assert not f.consumer._has_late_replica_cleanup_locked()
        assert all(handler != node_module.DROP_OBJECT_REPLICA_HANDLER for _, handler, _ in f.calls)
        # Quarantine leaves exact evidence without network polling. An actual
        # subsequent owner-death journal entry is the external state change.
        death, record = f.install_owner_death(0)
        resumed = f.delayed()
        assert resumed.terminal_error is state.terminal_error
        assert f.execute(resumed)
        f.assert_terminal(str(state.terminal_error))
        assert f.service.owner_death_fences.pending_for_owner(death.worker_id)
        assert f.consumer.owner_table.dead_worker_record(death.worker_id) == record
        assert len(f.reports) == 2 and len(f.cancels) == 1
        assert f.consumer._finish_pending_task(f.pending) and f.releases == [1]
    finally:
        f.close()


def test_old_queued_handoff_uses_latest_marker_receipts_cancel_and_sticky_error(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        initial = f.initial_handoff()
        assert initial.receipts == () and initial.terminal_error is None and initial.cancellation_reply is None
        f.release_first_hold()
        f.lose_report_once.add(1)
        assert not f.execute(initial)
        saved = f.delayed()
        assert saved.receipts[0].status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert len(saved.receipts) == 1 and saved.cancellation_reply is not None
        assert saved.terminal_error is not None
        assert initial.receipts == () and initial.terminal_error is None
        before_calls = tuple(f.calls)

        def observe(index):
            assert index == 1, "old queued work repeated an acknowledged owner report"
            current = f.marker().obligation
            assert current.terminal_error is saved.terminal_error
            assert current.cancellation_reply == saved.cancellation_reply
            assert current.receipts == saved.receipts

        f.before_report = observe
        # Replay the old object, not the newer delayed snapshot. Core must read
        # its canonical marker before any report, cancellation or direct Push.
        assert f.execute(initial)
        f.assert_terminal(str(saved.terminal_error))
        assert tuple(f.calls[:-1]) == before_calls
        assert f.calls[-1] == (f.target_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER, f.custody_acks[0][0])
        assert [index for index, _, _ in f.reports] == [0, 1, 1]
        assert f.reports[1][1] == f.reports[2][1]
        assert len(f.cancels) == 1 and not f.consumer._location_handoff_drivers
        assert f.consumer._finish_pending_task(f.pending) and f.releases == [0, 1]
    finally:
        f.close()


@pytest.mark.parametrize("entry", ("handoff-driver", "whole-execute"))
def test_reentrant_handoff_lane_cannot_repeat_rpc_or_replace_current_marker(monkeypatch, entry):
    f = _Fixture(monkeypatch)
    try:
        initial = f.initial_handoff()
        f.release_first_hold()
        reentries = []

        def enter_other_lane(index):
            if index != 0:
                return
            assert not reentries
            before_marker = f.marker()
            before_io = tuple(f.calls), tuple(f.reports), tuple(f.cancels), tuple(f.releases)
            assert (f.pending.execution, f.grant.lease_id) in f.consumer._location_handoff_drivers
            if entry == "whole-execute":
                assert not f.execute(initial)
            else:
                assert not f.consumer._report_granted_dependency_locations(
                    f.pending, f.pending.spec, f.sources, initial,
                )
            assert f.marker() is before_marker
            assert (tuple(f.calls), tuple(f.reports), tuple(f.cancels), tuple(f.releases)) == before_io
            reentries.append(index)

        f.before_report = enter_other_lane
        assert f.execute(initial)
        f.assert_terminal("task hold is not active")
        assert reentries == [0] and [index for index, _, _ in f.reports] == [0, 1]
        assert len(f.cancels) == 1 and not f.consumer._location_handoff_drivers
        f.assert_cancelled()
        assert f.consumer._finish_pending_task(f.pending)
    finally:
        f.close()


@pytest.mark.parametrize("changed", ("lease-request", "granting-address"))
def test_replay_rejects_changed_handoff_route_or_request_without_any_effect(monkeypatch, changed):
    f = _Fixture(monkeypatch)
    try:
        f.release_first_hold()
        f.lose_report_once.add(1)
        assert not f.execute()
        saved = f.delayed()
        bad = (replace(saved, lease_request=replace(saved.lease_request, lease_id=LeaseID(bytes((99,)) * 16)))
               if changed == "lease-request" else replace(saved, granting_node_address=("wrong.invalid", 99)))
        marker = f.marker()
        before_io = tuple(f.calls), tuple(f.reports), tuple(f.cancels), tuple(f.releases)
        before_owner = tuple(owner.owner_table.snapshot(item.object_id) for owner, item in zip(f.owners, f.sources))
        before_consumer = f.consumer.owner_table.snapshot(f.pending.object_id)
        before_recovery = replace(f.consumer._recovery.task_record(f.pending.task_id))
        # Call the authoritative handoff boundary directly: outer _execute may
        # handle a protocol exception, which is not permission to weaken this
        # zero-effect identity check or replace its canonical progress.
        with pytest.raises(SystemTaskError, match="custody identity"):
            f.consumer._report_granted_dependency_locations(f.pending, f.pending.spec, f.sources, bad)
        assert f.marker() is marker and f.marker().obligation is saved
        assert (tuple(f.calls), tuple(f.reports), tuple(f.cancels), tuple(f.releases)) == before_io
        assert tuple(owner.owner_table.snapshot(item.object_id) for owner, item in zip(f.owners, f.sources)) == before_owner
        assert f.consumer.owner_table.snapshot(f.pending.object_id) == before_consumer
        assert f.consumer._recovery.task_record(f.pending.task_id) == before_recovery
        assert not f.consumer._location_handoff_drivers
        assert f.execute(saved)
        f.assert_terminal(str(saved.terminal_error))
        assert f.consumer._finish_pending_task(f.pending)
    finally:
        f.close()


@pytest.mark.parametrize("lost_authority", ("owner", "target"))
def test_death_observed_before_quarantine_is_consumed_without_lost_wakeup(monkeypatch, lost_authority):
    f = _Fixture(monkeypatch)
    try:
        f.owners[0]._owner_protocol_open = False
        original = f.consumer._mark_protocol_unresolved
        injected, phases = [], []

        def checkpoint_then_death(pending, phase, obligation=None, **kwargs):
            original(pending, phase, obligation, **kwargs)
            phases.append(phase)
            if phase != "location_handoff_pending" or injected:
                return
            before = f.marker()
            state = before.obligation
            assert state.terminal_error is not None and state.cancellation_reply is not None
            assert tuple(reply.status for reply in state.receipts) == (
                protocol.RetainedLocationReportStatus.REJECTED, protocol.RetainedLocationReportStatus.ADDED,
            )
            f.assert_cancelled()
            injected.append(state.terminal_error)
            # Pure same-thread injection deliberately permits reentrant RLock
            # entry. No assertion claims real RPCs run under this lock: it
            # models an authority update at the final decision boundary.
            if lost_authority == "owner":
                death, _record = f.install_owner_death(0)
                assert f.service.owner_death_fences.pending_for_owner(death.worker_id)
            else:
                result = f.service.publications.commit_node_death(lambda: f.service.nodes.report_death(
                    protocol.ReportNodeDeath(
                        "target-before-quarantine", f.target.node_id, f.target_info.node_pid,
                        f.target_info.registration_epoch, 2, protocol.NodeDeathReason.PROCESS_EXIT,
                        "confirmed target exit before parking",
                    ),
                ))
                epoch, live = f.service.nodes.live_snapshot()
                f.consumer.handle_node_death(result.death, protocol.InstallClusterSnapshot(epoch, "target-before-park", live))
                # Consume the classification now, before quarantine exists.
                f.consumer._classify_node_death(_NodeDeathObserved(result.death, epoch))
            assert f.marker() is before and f.marker().phase == "location_handoff_pending"
            assert not any(isinstance(item, _DelayedReadyTask) for item in tuple(f.consumer._submissions.queue))

        monkeypatch.setattr(f.consumer, "_mark_protocol_unresolved", checkpoint_then_death)
        assert f.execute()
        assert len(injected) == 1 and "location_quarantined" not in phases
        f.assert_terminal(str(injected[0]))
        assert f.consumer.owner_table.snapshot(f.pending.object_id).error is injected[0]
        assert [index for index, _, _ in f.reports] == [0, 1] and len(f.cancels) == 1
        assert not f.consumer._location_handoff_drivers and f.consumer._ready_tasks.empty()
        assert not any(isinstance(item, _DelayedReadyTask) for item in tuple(f.consumer._submissions.queue))
        if lost_authority == "owner":
            assert f.consumer._finish_pending_task(f.pending) and f.releases == [1]
        else:
            # The stopped owner's release remains its own unresolved lifetime
            # responsibility. This test checks consumer completion, not a
            # fabricated owner Release ACK or force-clean shutdown.
            assert f.releases == []
    finally:
        f.close()
