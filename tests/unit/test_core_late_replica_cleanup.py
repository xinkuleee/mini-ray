"""Pure late-DROP custody with actual target grant, cancel and replica deletion.

Two 1 KiB stores, one adopted two-output producer and one unexecuted consumer;
the mixed-dependency case adds one seven-byte owner-held value. At most two
threadless Cores, one unstarted GCS, six source transfer calls, and two exact
target grants exist. All interleavings use synchronous RPC hooks.

The target pull/seal/grant happens before publisher death; only owner location
reporting is delayed. No dead source is contacted, no successful grant/drop
is fabricated, and no store or protocol authority is cleared to make progress.
No runtime constructor, process, thread, socket, timer, wait or user task runs.
"""

from __future__ import annotations

from dataclasses import replace
import threading

import pytest

from miniray import node as node_module, output_protocol as wire, protocol
from miniray.core import _HomeRoute
from miniray.core import (
    _DelayedReadyTask, _ForeignDependencyGuard, _LeaseRequestState,
    _ObjectWaiter, _PendingTask,
)
from miniray.ids import AttemptID, LeaseID, ObjectID, TaskID
from miniray.errors import SystemTaskError
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_core_output_surviving_replica import (
    _Fixture, _no_runtime as _no_runtime,
)


pytestmark = pytest.mark.unit


class _Case:
    def __init__(self, monkeypatch, *, foreign=False, healthy=False):
        self.f = f = _Fixture(monkeypatch)
        self.owner = f.core
        self.foreign = foreign
        self.consumer = make_pure_core() if foreign else self.owner
        consumer = self.consumer
        if foreign:
            consumer.job_id = self.owner.job_id
            consumer.driver_task_id = TaskID.for_driver(consumer.job_id)
            consumer.node_id, consumer.node_address = f.target.node_id, f.target_address
        consumer._home_route = _HomeRoute(consumer.node_id, consumer.node_address, consumer._membership_epoch)
        self.calls, self.cancel_replies, self.drop_replies, self.report_replies = [], [], [], []
        self.before_cancel = self.before_progress = None
        self.lose_cancel_ack = self.lose_drop_ack = False
        self.cancel_ack_lost = self.drop_ack_lost = False
        self.requests = []
        self.healthy = None
        dependencies = [f.descriptor]
        if healthy:
            assert not foreign
            object_id = ObjectID.for_task(TaskID.derive(consumer.job_id, f.pending.task_id, 81))
            attempt, payload = AttemptID(object_id.task_id, 0), b"healthy"
            seal = protocol.SealObject.from_data(object_id, attempt, consumer.worker_id, payload)
            assert f.source._handle_seal_object(seal).sealed
            result = protocol.ResultDescriptor(
                object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
                consumer.worker_id, f.source.node_id, seal.checksum,
            )
            consumer.owner_table.register(object_id, current_attempt=attempt, local_token="healthy-local")
            assert consumer.owner_table.publish_stored(object_id, attempt, f.source.node_id, descriptor=result)
            assert consumer._recovery.register_put(object_id)
            consumer._stored_descriptors[object_id] = result
            consumer._objects[object_id] = _ObjectWaiter(threading.Event())
            consumer._objects[object_id].event.set()
            self.healthy = result
            dependencies.append(protocol.ObjectStoreDescriptor(
                object_id, consumer.worker_id, attempt, f.source.node_id, len(payload), seal.checksum,
            ))
        self.dependencies = tuple(dependencies)

        def transfer(address, handler, request, **options):
            assert address == f.source_address
            assert not self.owner._node_is_dead(f.source.node_id)
            f.transfers.append(handler)
            assert len(f.transfers) <= 3 * len(self.dependencies)
            assert f.target.resource_ledger.available == f.target.resource_ledger.total
            operations = {
                node_module.PIN_OBJECT_HANDLER: f.source._handle_pin_object_for_transfer,
                node_module.GET_OBJECT_CHUNK_HANDLER: f.source._handle_get_object_chunk,
                node_module.RELEASE_OBJECT_PIN_HANDLER: f.source._handle_release_object_pin,
            }
            assert handler in operations
            return operations[handler](request)

        monkeypatch.setattr(node_module, "rpc_request", transfer)
        task = TaskID.derive(consumer.job_id, f.pending.task_id, 82)
        attempt = AttemptID(task, 0)
        spec = protocol.TaskSpec(
            consumer.job_id, task, attempt,
            protocol.FunctionKey(consumer.job_id, __name__, "never-executed-consumer", "v1"),
            tuple(protocol.RefArg(item.object_id, item.owner_worker_id) for item in self.dependencies),
            1, ResourceVector({"CPU": 1}), consumer.worker_id,
        )
        self.hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED if foreign else protocol.TaskReferenceHoldKind.SUBMITTED,
            consumer.worker_id, task, attempt,
        )
        guards = ()
        if foreign:
            borrower = (consumer.worker_id, "late-replica-source-borrow")
            assert self.owner.owner_table.add_borrowed_reference(f.output, borrower)
            assert self.owner.owner_table.retain_borrowed_reference_for_task(f.output, borrower, self.hold)
            assert self.owner.owner_table.release_borrowed_reference(f.output, borrower)
            guards = (_ForeignDependencyGuard(
                f.output, self.owner.worker_id, self.owner.owner_address,
                consumer.worker_id, borrower[1], self.hold,
            ),)
        else:
            for item in self.dependencies:
                assert self.owner.owner_table.add_submitted_reference(item.object_id, self.hold)
        consumer.owner_table.register_task_outputs(spec, local_tokens=("consumer-local",))
        consumer._recovery.register_task(spec)
        output = spec.return_ids()[0]
        consumer._objects[output] = _ObjectWaiter(threading.Event())
        self.pending = _PendingTask(
            output, spec, protected_dependencies=() if foreign else tuple(item.object_id for item in self.dependencies),
            dependency_hold=None if foreign else self.hold, foreign_dependency_guards=guards,
        )
        consumer._accepted_task_count += 1
        consumer._install_task_finish_barrier_locked(self.pending)
        consumer._registered_functions = set()
        consumer._resolve_node_address = f.address
        consumer._rpc = self.rpc
        consumer._borrow_rpc = self.borrow_rpc

        def forbidden_push(*_args, **_kwargs):
            pytest.fail("a consumer with a retired dependency reached PushTask")

        consumer._push_task_rpc = forbidden_push
        self.owner._rpc = self.rpc
        request = protocol.RequestWorkerLease(
            LeaseID(bytes((84,)) * 16), task, attempt, spec.resources,
            f.target.node_id, consumer.worker_id, target_node_id=f.target.node_id,
            dependencies=self.dependencies, return_ids=spec.return_ids(),
        )
        self.request = request
        self.requests.append(request)
        self.grant = grant = f.target._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease
        consumer._validate_granted_dependencies(self.dependencies, grant)
        self.lease_state = _LeaseRequestState(request, f.target_address, f.target.node_id, True)
        assert len(f.transfers) == 3 * len(self.dependencies)
        assert f.target.object_store.get(f.output) == f.publication.values.payloads[1]
        assert f.target.object_store.snapshot(f.output).pin_count == 1
        assert f.source.object_store.snapshot(f.output).pin_count == 0
        assert self.owner.owner_table.snapshot(f.output).locations == frozenset((f.source.node_id,))
        self.replica = next(item for item in grant.dependencies if item.object_id == f.output)
        self.drop = protocol.DropObjectReplica(
            self.replica.object_id, self.replica.producer_attempt_id, self.replica.owner_worker_id,
            self.replica.node_id, self.replica.checksum,
        )

    def rpc(self, address, handler, request):
        self.calls.append((address, handler, request))
        assert len(self.calls) <= 32
        f = self.f
        if handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER and self.before_progress is not None:
            callback, self.before_progress = self.before_progress, None
            callback()
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert address == f.target_address and request == self.request
            result = f.target._handle_request_lease(request)
            assert result == self.grant and len(f.transfers) == 3 * len(self.dependencies)
            return result
        if handler == node_module.CANCEL_LEASE_HANDLER:
            assert address == f.target_address
            if self.before_cancel is not None:
                callback, self.before_cancel = self.before_cancel, None
                callback()
            result = f.target._handle_cancel_worker_lease(request)
            self.cancel_replies.append((request, result))
            if self.lose_cancel_ack and not self.cancel_ack_lost:
                assert result.accepted and result.cancelled and result.released
                self.cancel_ack_lost = True
                raise TransportTimeout("target cancellation applied before its ACK was lost")
            return result
        if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
            assert address == f.target_address
            assert request.inventory.lease_request == self.request
            assert request.inventory.descriptors == self.grant.dependencies
            state = self.consumer._protocol_unresolved[self.pending.task_key].obligation
            assert state.inventory == request.inventory
            assert all(receipt.custody_transferred for receipt in state.local_receipts + state.receipts)
            result = f.target._handle_ack_lease_dependency_custody(request)
            assert result.accepted and result.request == request
            return result
        result = f.rpc(address, handler, request)
        if handler == node_module.DROP_OBJECT_REPLICA_HANDLER:
            self.drop_replies.append((request, result))
            if (self.lose_drop_ack and not self.drop_ack_lost
                    and result.status is protocol.DropObjectReplicaStatus.DROPPED):
                self.drop_ack_lost = True
                raise TransportTimeout("exact replica deletion applied before its ACK was lost")
        return result

    def borrow_rpc(self, address, handler, request):
        assert address == self.owner.owner_address
        if handler == "report_retained_object_location":
            result = self.owner.report_retained_object_location(request)
            self.report_replies.append((request, result))
            return result
        assert handler == "release_owned_object_for_task"
        return self.owner.release_owned_object_for_task(request)

    def report(self, descriptor=None):
        assert self.foreign
        return protocol.ReportRetainedObjectLocation(
            self.f.output, self.owner.worker_id, self.consumer.worker_id, self.hold,
            self.replica if descriptor is None else descriptor,
        )

    def execute_during_drop(self, *, expect_terminal=True):
        observed = []

        def execute():
            choice = self.owner._output_loss_choices[self.f.identity]
            assert choice.slots[1].decision.value == "DROP"
            assert self.owner.owner_table.snapshot(self.f.output).locations == frozenset()
            assert self.f.target.object_store.snapshot(self.f.output).pin_count == 1
            result = self.consumer._execute(
                self.pending, self.pending.spec, self.dependencies, lease_state=self.lease_state,
            )
            observed.append(result)
            assert result is expect_terminal

        self.before_progress = execute
        obligation = self.f.lose_publisher()
        assert self.owner._drive_output_node_loss(self.f.pending, obligation)
        assert observed == [expect_terminal]
        resolution = self.f.service.publications.output_recovery.snapshot(self.f.identity).resolution
        assert resolution.kept_slots == (0,) and resolution.complete == self.f.envelope.complete
        assert self.owner._finish_pending_task(self.f.pending)
        current = self.owner.owner_table.snapshot(self.f.output)
        assert current.state is ObjectState.LOST and not current.locations
        assert current.output_publication is None and current.canonical_stored_result is None
        assert self.f.output not in self.owner._stored_descriptors
        assert self.owner._recovery.task_record(self.f.pending.task_id).retries_started == 0

    def assert_pending_cleanup(self):
        cleanup = self.owner._late_replica_cleanup
        assert cleanup.pending() == (self.drop,) and cleanup.has_pending(self.f.output)
        (record,) = cleanup.snapshot()
        assert record.request == self.drop and record.proof is None and not record.in_flight

    def close(self):
        # Exact cancellation is safe even when an assertion interrupted a case.
        # No process exists and no drop/owner authority is force-cleared here.
        for request in self.requests:
            record = self.f.target._leases.get(request.lease_id)
            if (not self.owner._node_is_dead(self.f.target.node_id) and record is not None
                    and record.state is protocol.LeaseExecutionState.GRANTED):
                self.f.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
                    request.lease_id, request.task_id, request.attempt_id,
                    request.requester_node_id, request.requester_worker_id, request.scheduling_key,
                ))
        if self.foreign:
            self.owner.owner_table.release_retained_reference_for_task(self.f.output, self.hold)
            for output in self.consumer._objects:
                for token in self.consumer.owner_table.snapshot(output).local_tokens:
                    self.consumer.owner_table.release_local_reference(output, token)
            close_pure_core(self.consumer)
        else:
            for item in self.dependencies:
                if self.owner.owner_table.contains(item.object_id):
                    self.owner.owner_table.release_submitted_reference(item.object_id, self.hold)
        self.f.close()


@pytest.mark.parametrize("foreign", (False, True), ids=("local-grant", "foreign-report"))
def test_late_drop_report_cancels_real_grant_and_exact_cleanup_survives_both_lost_acks(monkeypatch, foreign):
    case = _Case(monkeypatch, foreign=foreign)
    f, owner = case.f, case.owner
    try:
        case.lose_cancel_ack = case.lose_drop_ack = True

        def prove_pinned_before_cancel():
            case.assert_pending_cleanup()
            assert not owner._drive_late_replica_cleanup(schedule_retry=False)
            case.assert_pending_cleanup()
            assert case.drop_replies[-1][1].status is protocol.DropObjectReplicaStatus.PINNED
            assert f.target._leases[case.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED

        case.before_cancel = prove_pinned_before_cancel
        case.execute_during_drop(expect_terminal=False)
        case.assert_pending_cleanup()
        assert case.cancel_ack_lost and len(case.cancel_replies) == 1
        assert f.target._leases[case.grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        assert f.target.object_store.snapshot(f.output).pin_count == 0
        assert case.consumer.owner_table.snapshot(case.pending.object_id).state is ObjectState.PENDING
        assert not case.consumer._finish_pending_task(case.pending)
        if foreign:
            assert len(case.report_replies) == 1
            assert case.report_replies[0][1].status is protocol.RetainedLocationReportStatus.RETIRED
            assert not case.report_replies[0][1].accepted
            replay = owner.report_retained_object_location(case.report())
            assert replay.status is protocol.RetainedLocationReportStatus.RETIRED
            case.assert_pending_cleanup()

        delayed = tuple(item for item in tuple(case.consumer._submissions.queue)
                        if isinstance(item, _DelayedReadyTask)
                        and item.ready.pending.task_id == case.pending.task_id
                        and item.ready.location_state is not None)
        assert len(delayed) == 1
        state = delayed[0].ready.location_state
        assert state.terminal_error is not None and state.cancellation_reply is None
        assert case.consumer._protocol_unresolved[case.pending.task_key].obligation == state
        assert case.consumer._execute(
            case.pending, case.pending.spec, case.dependencies, location_state=state,
        )
        assert len(case.cancel_replies) == 2
        assert case.cancel_replies[0][0] == case.cancel_replies[1][0]
        assert case.cancel_replies[1][1].cancelled and not case.cancel_replies[1][1].released
        assert case.consumer.owner_table.snapshot(case.pending.object_id).state is ObjectState.ERROR
        assert case.consumer._finish_pending_task(case.pending)
        case.assert_pending_cleanup()

        before = replace(owner._recovery.task_record(f.pending.task_id))
        assert f.output not in owner._task_finish_barriers
        assert not owner._retire_lost_output_memberships(f.output)
        assert owner._start_or_join_reconstruction(f.output, owner._objects[f.output]) is None
        assert owner._recovery.task_record(f.pending.task_id) == before
        assert owner.owner_table.release_local_reference(f.output, "outer1")
        owner._reference_released(f.output)
        assert owner.owner_table.collection_state(f.output) is ObjectCollectionState.ACTIVE

        assert not owner._drive_late_replica_cleanup(schedule_retry=False)
        assert case.drop_ack_lost and not f.target.object_store.contains(f.output, sealed_only=False)
        case.assert_pending_cleanup()
        assert f.target._dropped_metadata[f.output] == (case.drop.producer_attempt_id, case.drop.owner_worker_id, case.drop.checksum)
        owner._reference_released(f.output)
        assert owner.owner_table.collection_state(f.output) is ObjectCollectionState.ACTIVE
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        assert not owner._late_replica_cleanup.has_pending()
        assert [reply.status for _, reply in case.drop_replies] == [
            protocol.DropObjectReplicaStatus.PINNED, protocol.DropObjectReplicaStatus.DROPPED,
            protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        ]
        assert all(request == case.drop for request, _ in case.drop_replies)
        owner._reference_mailbox.drain()
        assert owner.owner_table.collection_state(f.output) is ObjectCollectionState.COLLECTED
        assert f.target.object_store.used_bytes == 0 and f.output not in f.target._sealed_metadata
        assert f.source.object_store.get(f.output) == f.publication.values.payloads[1]
    finally:
        case.close()


def test_first_retired_local_dependency_does_not_skip_a_later_real_healthy_replica(monkeypatch):
    case = _Case(monkeypatch, healthy=True)
    f, owner = case.f, case.owner
    try:
        case.execute_during_drop()
        case.assert_pending_cleanup()
        healthy = case.healthy
        snapshot = owner.owner_table.snapshot(healthy.object_id)
        assert snapshot.state is ObjectState.READY_STORED
        assert snapshot.locations == frozenset((f.target.node_id,))
        assert snapshot.canonical_stored_result == healthy
        assert owner._stored_descriptors[healthy.object_id] == replace(healthy, node_id=f.target.node_id)
        assert f.target.object_store.get(healthy.object_id) == b"healthy"
        assert all(f.target.object_store.snapshot(item.object_id).pin_count == 0 for item in case.dependencies)
        assert case.consumer._finish_pending_task(case.pending)
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        assert [request for request, _ in case.drop_replies] == [case.drop]
        assert f.target.object_store.get(healthy.object_id) == b"healthy"
        assert owner.owner_table.snapshot(healthy.object_id) == replace(snapshot, submitted_tokens=frozenset())
    finally:
        case.close()


def test_completed_old_cleanup_and_late_report_cannot_delete_new_targeted_attempt(monkeypatch):
    case = _Case(monkeypatch, foreign=True)
    f, owner = case.f, case.owner
    try:
        case.execute_during_drop()
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        assert not owner._late_replica_cleanup.has_pending()
        owner._start_or_join_reconstruction(f.output, owner._objects[f.output])
        started = owner._start_open_targeted_reconstruction(f.pending.task_id)
        assert started is not None and started.execution.attempt_id == f.pending.spec.attempt_id.next()
        assert started.target_output_ids == (f.output,)
        newer = started.execution.attempt_id
        payload = b"new-target-attempt"
        assert f.target._handle_seal_object(protocol.SealObject.from_data(
            f.output, newer, owner.worker_id, payload,
        )).sealed
        request = protocol.RequestWorkerLease(
            LeaseID(bytes((85,)) * 16), newer.task_id, newer, ResourceVector({"CPU": 1}),
            f.target.node_id, owner.worker_id, target_node_id=f.target.node_id,
            return_ids=(f.output,), target_execution=started.execution,
        )
        case.requests.append(request)
        grant = f.target._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease
        before = (owner.owner_table.snapshot(f.output), replace(owner._recovery.task_record(f.pending.task_id)),
                  dict(f.target._sealed_metadata), owner._late_replica_cleanup.snapshot())
        count = len(case.drop_replies)
        reply = owner.report_retained_object_location(case.report())
        assert reply.status is protocol.RetainedLocationReportStatus.RETIRED and not reply.accepted
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        assert len(case.drop_replies) == count
        stale = f.target._handle_drop_object_replica(case.drop)
        assert stale.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
        assert stale.accepted and not stale.dropped
        assert f.target.object_store.get(f.output) == payload
        assert f.target._leases[grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert (owner.owner_table.snapshot(f.output), owner._recovery.task_record(f.pending.task_id),
                dict(f.target._sealed_metadata), owner._late_replica_cleanup.snapshot()) == before
    finally:
        case.close()


@pytest.mark.parametrize("field", ("checksum", "size_bytes", "producer_attempt_id"))
def test_corrupt_late_foreign_report_cannot_authorize_historical_replica_deletion(monkeypatch, field):
    case = _Case(monkeypatch, foreign=True)
    f, owner = case.f, case.owner
    try:
        obligation = f.lose_publisher()
        assert owner._drive_output_node_loss(f.pending, obligation)
        assert owner._finish_pending_task(f.pending)
        changes = {
            "checksum": ("1" if case.replica.checksum[0] != "1" else "2") + case.replica.checksum[1:],
            "size_bytes": case.replica.size_bytes + 1,
            "producer_attempt_id": case.replica.producer_attempt_id.next(),
        }
        before = tuple(owner.owner_table.snapshot(output) for output in f.pending.output_ids)
        reply = owner.report_retained_object_location(case.report(replace(case.replica, **{field: changes[field]})))
        assert reply.status in (protocol.RetainedLocationReportStatus.REJECTED, protocol.RetainedLocationReportStatus.STALE_PRODUCER)
        assert not reply.accepted
        assert tuple(owner.owner_table.snapshot(output) for output in f.pending.output_ids) == before
        assert not owner._has_late_replica_cleanup_locked() and case.drop_replies == []
        assert f.target.object_store.get(f.output) == f.publication.values.payloads[1]
        assert f.target.object_store.snapshot(f.output).pin_count == 1
        assert f.target._leases[case.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
        # Finish with the actual unmodified report and actual cancel/drop; no
        # negative-test mutation becomes a successful deletion credential.
        assert case.consumer._execute(case.pending, case.pending.spec, case.dependencies, lease_state=case.lease_state)
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        assert case.consumer._finish_pending_task(case.pending)
        assert [request for request, _ in case.drop_replies] == [case.drop]
    finally:
        case.close()


def test_installed_target_death_discharges_cleanup_without_fabricating_drop_ack(monkeypatch):
    case = _Case(monkeypatch)
    f, owner = case.f, case.owner
    try:
        case.execute_during_drop()
        case.assert_pending_cleanup()
        assert case.consumer._finish_pending_task(case.pending)
        target = f.service.nodes.get(f.target.node_id)
        result = f.service.publications.commit_node_death(lambda: f.service.nodes.report_death(
            protocol.ReportNodeDeath(
                "late-secondary-node-exit", target.node_id, target.node_pid, target.registration_epoch,
                2, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed target Node exit",
            ),
        ))
        epoch, live = f.service.nodes.live_snapshot()
        owner.handle_node_death(result.death, protocol.InstallClusterSnapshot(epoch, "late-target-dead", live))
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        (record,) = owner._late_replica_cleanup.snapshot()
        assert record.request == case.drop and record.proof == result.death
        assert not owner._late_replica_cleanup.has_pending() and case.drop_replies == []
        # The object models inaccessible memory of a dead process, not bytes
        # deleted by an invented response. Every future Node RPC is fenced.
        assert f.target.object_store.get(f.output) == f.publication.values.payloads[1]
        assert owner.owner_table.snapshot(f.output).state is ObjectState.LOST
    finally:
        case.close()


def test_system_retry_waits_for_a_late_old_replica_after_targeted_start(monkeypatch):
    case = _Case(monkeypatch, foreign=True)
    f, owner = case.f, case.owner
    try:
        # The old replica is physically sealed/pinned, but the report has not
        # arrived yet. A legitimate reconstruction may therefore START first.
        obligation = f.lose_publisher()
        assert owner._drive_output_node_loss(f.pending, obligation)
        assert owner._finish_pending_task(f.pending)
        owner._start_or_join_reconstruction(f.output, owner._objects[f.output])
        started = owner._start_open_targeted_reconstruction(f.pending.task_id)
        assert started is not None
        current = next(item for item in tuple(owner._submissions.queue)
                       if isinstance(item, _PendingTask) and item.target_execution == started.execution)
        assert owner.report_retained_object_location(case.report()).status is protocol.RetainedLocationReportStatus.RETIRED
        before = replace(owner._recovery.task_record(f.pending.task_id))
        assert not owner._retry_system_failure(current, SystemTaskError("new attempt failed before result"))
        assert owner._recovery.task_record(f.pending.task_id) == before
        delayed = next(item for item in tuple(owner._submissions.queue)
                       if isinstance(item, _DelayedReadyTask) and item.ready.system_failure is not None)
        assert delayed.ready.pending == current
        assert not owner._finish_pending_task(current)
        # Cancel the original consumer to release its real pin, then consume
        # the exact Node deletion ACK. No user execution is used by replay.
        assert case.consumer._execute(case.pending, case.pending.spec, case.dependencies, lease_state=case.lease_state)
        assert case.consumer._finish_pending_task(case.pending)
        assert owner._drive_late_replica_cleanup(schedule_retry=False)
        terminal = owner._execute(current, current.spec, system_failure=delayed.ready.system_failure)
        assert not terminal
        after = owner._recovery.task_record(f.pending.task_id)
        assert after.current_attempt == before.current_attempt.next()
        assert after.retries_started == before.retries_started + 1
        assert not owner._is_protocol_unresolved(current)
        assert not f.target.object_store.contains(f.output, sealed_only=False)
    finally:
        case.close()
