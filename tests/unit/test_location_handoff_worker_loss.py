"""Pure post-grant cancellation convergence after the executor exits.

Reuse the threadless mixed local/foreign fixture: two 1 KiB stores, two tiny
inputs, one actual grant and six source pin/chunk/release calls. A one-shot
local route exception fixes the consumer's original failure. Before its first
Cancel, the existing fake process reports exited and the real Node worker-loss
reducer releases the granted allocation and pins as WORKER_LOST. No lease or
owner table is overwritten by the test.

The live Node returns real rejected cancellation and exact outcome replies.
Only delivery is faulted, never underlying execution/output authority. At most
three replay rounds and two outcome corruptions run synchronously; no GCS,
process, socket, thread, wait, producer retry or user task executes.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import node as node_module, protocol
from miniray.errors import SystemTaskError
from miniray.ids import WorkerID
from miniray.ownership import ObjectState
from miniray.transport import TransportTimeout
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_local_replica_handoff import _Fixture, _route_fault


pytestmark = pytest.mark.unit


class _WorkerLossTransport:
    def __init__(self, fixture, *, defects=()):
        self.f = fixture
        self.original_rpc = fixture.consumer._rpc
        self.defects = tuple(defects)
        self.reclaims = []
        self.outcomes = []
        self.delivered = []
        self.first_error = None
        self.events = []
        fixture.consumer._rpc = self.rpc
        fixture.before_report = self.before_foreign_report

    def lose_executor(self):
        f = self.f
        with f.target._state_lock:
            slot = f.target._workers[f.grant.worker_id]
            assert slot.active_lease_id == f.grant.lease_id and slot.process.is_alive()
            # The process is the fixture's original SimpleNamespace, not an
            # OS child. All lease/resource/pin changes belong to Node's reducer.
            slot.process.is_alive = lambda: False
            slot.process.exitcode = 1
            assert not slot.process.is_alive()
            released = f.target._reclaim_active_lease_after_worker_exit_locked(f.grant.worker_id)
            assert released
            self.reclaims.append(released)
            assert not f.target._reclaim_active_lease_after_worker_exit_locked(f.grant.worker_id)
        self.assert_execution_fenced()

    def assert_execution_fenced(self):
        f = self.f
        assert self.reclaims == [True]
        record = f.target._leases[f.grant.lease_id]
        assert record.state is protocol.LeaseExecutionState.WORKER_LOST and record.completion is None
        assert record.dependency_pins == ()
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        assert f.target._workers[f.grant.worker_id].active_lease_id is None
        assert not f.target._workers[f.grant.worker_id].process.is_alive()
        assert not f.consumer._node_is_dead(f.target.node_id)
        for descriptor, payload in zip(f.sources, f.payloads):
            assert f.target.object_store.snapshot(descriptor.object_id).pin_count == 0
            assert f.target.object_store.get(descriptor.object_id) == payload
        assert not f.target.object_store.contains(f.pending.object_id, sealed_only=False)

    def before_foreign_report(self):
        f = self.f
        self.assert_execution_fenced()
        state = f.marker().obligation
        assert state.terminal_error is self.first_error
        assert state.cancellation_reply is None and not state.local_receipts
        assert self.outcomes and len(f.cancels) == 1 and not f.reports
        self.events.append("foreign-report")

    def rpc(self, address, handler, request):
        f = self.f
        if handler not in (node_module.CANCEL_LEASE_HANDLER, node_module.GET_WORKER_LEASE_OUTCOME_HANDLER):
            return self.original_rpc(address, handler, request)
        assert address == f.target_address
        f.calls.append((address, handler, request))
        assert len(f.calls) <= 16
        if handler == node_module.CANCEL_LEASE_HANDLER:
            state = f.marker().obligation
            assert isinstance(state.terminal_error, SystemTaskError)
            if self.first_error is None:
                self.first_error = state.terminal_error
            assert state.terminal_error is self.first_error
            if not self.reclaims:
                self.lose_executor()
            reply = f.target._handle_cancel_worker_lease(request)
            f.cancels.append((request, reply))
            assert len(f.cancels) <= 3
            assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
                    reply.requester_worker_id, reply.scheduling_key) == (
                request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
                request.requester_worker_id, request.scheduling_key,
            )
            assert reply.state is protocol.LeaseExecutionState.WORKER_LOST
            assert not reply.accepted and not reply.cancelled and not reply.released
            self.events.append("cancel-rejected")
            return reply

        assert type(request) is protocol.GetWorkerLeaseOutcome
        assert (request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
                request.owner_worker_id, request.object_ids, request.scheduling_key, request.target_execution) == (
            f.grant.lease_id, f.grant.task_id, f.grant.attempt_id, f.grant.worker_id,
            f.consumer.worker_id, f.pending.output_ids, f.grant.scheduling_key, f.grant.target_execution,
        )
        # A real wire round-trip detaches response IDs from the query and Node
        # records. Preserve that boundary before deliberate delivery corruption.
        reply = deepcopy(f.target._handle_get_worker_lease_outcome(request))
        assert type(reply) is protocol.GetWorkerLeaseOutcomeReply and reply.found and not reply.worker_alive
        assert reply.state is protocol.LeaseExecutionState.WORKER_LOST and reply.completion_status is None
        assert not reply.descriptors and not reply.orphan_descriptors and not reply.cleanup_pending
        assert reply.output_publication is None and reply.output_completion is None
        self.outcomes.append((request, reply))
        assert len(self.outcomes) <= 3
        self.events.append("worker-lost-outcome")
        index = len(self.outcomes) - 1
        defect = self.defects[index] if index < len(self.defects) else None
        if defect == "ack-loss":
            raise TransportTimeout("live Node outcome reply was lost")
        if defect == "wrong-executor":
            other = WorkerID(bytes(value ^ 1 for value in reply.executor_worker_id.value))
            delivered = replace(reply, executor_worker_id=other)
        elif defect == "wrong-state":
            delivered = replace(reply, state=protocol.LeaseExecutionState.ABANDONED)
        elif defect == "dirty-orphan":
            # A byte-free forged delivery is not a user result: no SealObject
            # or publication occurs. Corrupt its nested checksum after normal
            # construction to model wire DTOs that bypass __post_init__.
            orphan = protocol.ObjectStoreDescriptor(
                f.pending.object_id, f.consumer.worker_id, f.pending.spec.attempt_id,
                f.target.node_id, 1, "a" * 64,
            )
            delivered = replace(reply, orphan_descriptors=(orphan,))
            object.__setattr__(delivered.orphan_descriptors[0], "checksum", "invalid-checksum")
        else:
            assert defect is None
            delivered = reply
        self.delivered.append(delivered)
        return delivered

    def assert_terminal(self):
        f = self.f
        self.assert_execution_fenced()
        snapshot = f.consumer.owner_table.snapshot(f.pending.object_id)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is self.first_error
        assert type(snapshot.error) is SystemTaskError and "local route failed" in str(snapshot.error)
        assert snapshot.current_attempt == f.pending.spec.attempt_id
        assert f.pending.task_key not in f.consumer._protocol_unresolved
        assert not f.consumer._location_handoff_drivers
        record = f.consumer._recovery.task_record(f.pending.task_id)
        assert record.current_attempt == f.pending.spec.attempt_id and record.retries_started == 0
        assert f.consumer._recovery.active_recovery(f.pending.task_id) is None
        latest = f.states[-1][1]
        assert latest.terminal_error is self.first_error and latest.cancellation_reply is None
        assert latest.execution_outcome == self.outcomes[-1][1]
        assert len(latest.local_receipts) == len(latest.receipts) == 1
        assert latest.local_receipts[0].accepted and latest.local_receipts[0].custody_transferred
        assert latest.receipts[0].accepted and latest.receipts[0].custody_transferred
        f.assert_local_recorded()
        f.assert_foreign_recorded()
        assert len(f.reports) == 1 and len(self.reclaims) == 1
        assert all(not reply.cancelled and not reply.released for _, reply in f.cancels)
        assert all(request == f.cancels[0][0] for request, _ in f.cancels)
        assert all(request == self.outcomes[0][0] for request, _ in self.outcomes)
        assert self.events.index("cancel-rejected") < self.events.index("worker-lost-outcome") < self.events.index("foreign-report")
        assert sum(handler == node_module.REQUEST_LEASE_HANDLER for _, handler, _ in f.calls) == 1
        assert sum(handler == node_module.DROP_OBJECT_REPLICA_HANDLER for _, handler, _ in f.calls) == 1
        assert len(f.transfers) == 6
        return latest


def test_worker_lost_outcome_finishes_handoff_without_fabricating_cancel_ack(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        _route_fault(f, after=False)
        transport = _WorkerLossTransport(f)
        assert not f.execute()
        first = f.delayed()
        f.assert_pending()
        assert first.execution_outcome == transport.outcomes[0][1]
        assert first.cancellation_reply is None and first.local_receipts == ()
        assert len(first.receipts) == 1 and first.receipts[0].custody_transferred
        before = tuple(f.calls)
        assert f.execute(first)
        latest = transport.assert_terminal()
        assert tuple(f.calls[:-1]) == before
        assert f.calls[-1][1] == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert f.calls[-1][2].inventory == f.inventory
        assert len(f.cancels) == len(transport.outcomes) == 1
        saved = deepcopy(latest.execution_outcome)
        # The durable proof is detached from a response object that transport
        # code still holds, including nested executor identity.
        object.__setattr__(transport.outcomes[0][1].executor_worker_id, "value", b"z" * 16)
        assert latest.execution_outcome == saved
        f.finish()
    finally:
        f.close()


def test_missing_worker_lost_ack_keeps_pending_and_replays_exact_outcome(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        _route_fault(f, after=False)
        transport = _WorkerLossTransport(f, defects=("ack-loss",))
        assert not f.execute()
        first = f.delayed()
        f.assert_pending()
        assert first.execution_outcome is None and first.cancellation_reply is None
        assert first.terminal_error is transport.first_error and len(first.receipts) == 1
        f.assert_foreign_recorded()
        assert f.execute(first)
        transport.assert_terminal()
        assert len(f.cancels) == len(transport.outcomes) == 2 and len(f.reports) == 1
        f.finish()
    finally:
        f.close()


def test_wrong_executor_and_wrong_terminal_state_never_become_worker_loss_proof(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        _route_fault(f, after=False)
        transport = _WorkerLossTransport(f, defects=("wrong-executor", "wrong-state"))
        assert not f.execute()
        first = f.delayed()
        f.assert_pending()
        assert first.execution_outcome is None and first.cancellation_reply is None
        assert transport.delivered[0].executor_worker_id != f.grant.worker_id
        assert not f.execute(first)
        second = f.delayed()
        f.assert_pending()
        assert second.execution_outcome is None and second.cancellation_reply is None
        assert second.terminal_error is first.terminal_error is transport.first_error
        assert len(second.local_receipts) == len(second.receipts) == 1
        assert transport.delivered[1].state is protocol.LeaseExecutionState.ABANDONED
        assert f.execute(second)
        transport.assert_terminal()
        assert len(f.cancels) == len(transport.outcomes) == 3 and len(f.reports) == 1
        f.finish()
    finally:
        f.close()


def test_orphan_descriptor_with_corrupt_checksum_cannot_acknowledge_clean_worker_loss(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        _route_fault(f, after=False)
        transport = _WorkerLossTransport(f, defects=("dirty-orphan",))
        assert not f.execute()
        first = f.delayed()
        f.assert_pending()
        assert first.execution_outcome is None and first.cancellation_reply is None
        assert first.terminal_error is transport.first_error and len(first.receipts) == 1
        assert transport.delivered[0].orphan_descriptors[0].checksum == "invalid-checksum"
        transport.assert_execution_fenced()
        assert f.execute(first)
        transport.assert_terminal()
        assert len(f.cancels) == len(transport.outcomes) == 2 and len(f.reports) == 1
        f.finish()
    finally:
        f.close()
