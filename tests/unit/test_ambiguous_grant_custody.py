"""Pure custody recovery when every reply to an actual grant is lost.

Two threadless Cores and two 1 KiB stores reuse the real mixed-input fixture.
Its Node seals two small inputs via six pin/chunk/release calls and grants one
consumer; the public Core drop failpoint deletes the original local source.
Four explicitly driven lease rounds lose twelve real Grant replies, then
normal cancellation discloses the historical grant's complete inventory.

All owner reducers, Node handlers, exact custody ACK, lease/pin release and
final physical GC are real. Only response delivery or a bounded pure-builder/
route effect is faulted; an existing fake process may report exited before
Node's worker-loss reducer runs. No new process, thread, socket, wait, GCS, producer retry or user task
executes. Additional manual replay is bounded to three steps; one case delivers
an exact queued cancellation intent reentrantly on the current driver thread.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import node as node_module, protocol
from miniray.core import (
    _DelayedReadyTask, _LeaseCancellationState, _LeaseRequestAmbiguous,
    _LocationReportState, _ReadyTask,
)
from miniray.ids import NodeID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_local_replica_handoff import _Fixture, _route_fault


pytestmark = pytest.mark.unit


class _GrantDelivery:
    def __init__(self, fixture, *, cancel_defects=(), worker_lost=False, grant_subset_once=False,
                 grant_success=False, expected_reports=1, grant_defect="partial"):
        self.f = fixture
        self.cancel_defects = tuple(cancel_defects)
        self.worker_lost = worker_lost
        self.grant_subset_once = grant_subset_once
        self.grant_defect = grant_defect
        self.grant_success = grant_success
        self.expected_reports = expected_reports
        self.lease_calls, self.cancel_calls, self.outcome_calls, self.drops = [], [], [], []
        self.delivered_cancels = []
        self.delivered_bad_grants = []
        self.death_reclaims = []
        self.first_error = None
        self.events = []
        self.gc_open = False
        fixture.consumer._rpc = self.rpc
        fixture.foreign._rpc = self.rpc
        fixture.foreign._resolve_node_address = fixture.address
        fixture.before_report = self.before_foreign_report

    def assert_fenced(self):
        f = self.f
        expected = (protocol.LeaseExecutionState.WORKER_LOST if self.worker_lost
                    else protocol.LeaseExecutionState.ABANDONED)
        record = f.target._leases[f.grant.lease_id]
        assert record.state is expected and record.completion is None and record.dependency_pins == ()
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        assert f.target._workers[f.grant.worker_id].active_lease_id is None
        assert not f.consumer._node_is_dead(f.target.node_id)
        for descriptor, payload in zip(f.sources, f.payloads):
            assert f.target.object_store.snapshot(descriptor.object_id).pin_count == 0
            assert f.target.object_store.get(descriptor.object_id) == payload
        assert not f.target.object_store.contains(f.pending.object_id, sealed_only=False)

    def lose_worker(self):
        f = self.f
        with f.target._state_lock:
            slot = f.target._workers[f.grant.worker_id]
            assert slot.process.is_alive() and slot.active_lease_id == f.grant.lease_id
            slot.process.is_alive = lambda: False
            slot.process.exitcode = 1
            changed = f.target._reclaim_active_lease_after_worker_exit_locked(f.grant.worker_id)
            assert changed
            self.death_reclaims.append(changed)
            assert not f.target._reclaim_active_lease_after_worker_exit_locked(f.grant.worker_id)
        self.assert_fenced()

    def before_foreign_report(self):
        f = self.f
        self.assert_fenced()
        state = f.marker().obligation
        assert state.grant == f.grant and state.lease_request == f.request
        assert state.terminal_error is self.first_error
        if self.grant_subset_once:
            assert state.local_receipts == ()
        else:
            assert len(state.local_receipts) == 1
            assert state.local_receipts[0].accepted and state.local_receipts[0].custody_transferred
        assert f.local_hold in f.consumer.owner_table.snapshot(f.local_id).submitted_tokens
        assert f.foreign.owner_table.has_retained_reference_for_task(f.foreign_id, f.foreign_hold)
        assert not f.releases and not f.consumer._finish_pending_task(f.pending)
        assert len(f.reports) < self.expected_reports
        self.events.append("foreign-custody")

    def rpc(self, address, handler, request):
        f = self.f
        f.calls.append((address, handler, request))
        assert len(f.calls) <= 25
        if handler == node_module.DROP_OBJECT_REPLICA_HANDLER:
            assert self.gc_open, "handoff attempted deletion before complete custody"
            node = f.source if address == f.source_address else f.target
            assert address == f.address(node.node_id) and request.node_id == node.node_id
            source = next(item for item in f.sources if item.object_id == request.object_id)
            assert request.producer_attempt_id == source.producer_attempt_id
            assert request.owner_worker_id == source.owner_worker_id and request.checksum == source.checksum
            reply = node._handle_drop_object_replica(request)
            assert type(reply) is protocol.DropObjectReplicaReply and reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
                request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
            )
            self.drops.append((request, reply))
            assert len(self.drops) <= 3
            return reply

        assert address == f.target_address
        if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
            reply = f.ack_custody(request)
            self.events.append("node-custody-ack")
            return reply
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert request == f.request
            reply = deepcopy(f.target._handle_request_lease(request))
            assert type(reply) is protocol.GrantWorkerLease and reply == f.grant
            self.lease_calls.append((request, reply))
            assert len(self.lease_calls) <= 12 and len(f.transfers) == 6
            if self.grant_success:
                assert not self.grant_subset_once and len(self.lease_calls) == 1
                return reply
            if self.grant_subset_once:
                if len(self.lease_calls) == 1:
                    # The raw in-process delivery imitates a decoded malformed
                    # DTO. Never mutate the Node's actual cached grant or the
                    # separately retained observation of that valid reply.
                    delivered = deepcopy(reply)
                    if self.grant_defect == "partial":
                        delivered = replace(delivered, dependencies=delivered.dependencies[:1])
                    elif self.grant_defect == "missing-lease-id":
                        object.__delattr__(delivered, "lease_id")
                    elif self.grant_defect == "missing-worker-value":
                        object.__delattr__(delivered.worker_id, "value")
                    elif self.grant_defect == "duplicate-dependency":
                        object.__setattr__(delivered, "dependencies", delivered.dependencies + delivered.dependencies[:1])
                    else:
                        pytest.fail("unexpected bounded grant-delivery defect")
                    self.delivered_bad_grants.append(delivered)
                    return delivered
                assert len(self.lease_calls) == 2
                assert len(self.delivered_bad_grants) == 1
                assert not self.cancel_calls and not f.reports
                f.assert_local_unrecorded()
                return reply
            raise TransportTimeout("actual complete grant reply was lost")

        if handler == node_module.CANCEL_LEASE_HANDLER:
            marker = f.consumer._protocol_unresolved[f.pending.task_key]
            state = marker.obligation
            assert isinstance(state, (_LeaseCancellationState, _LocationReportState))
            assert state.lease_request == f.request
            assert request.lease_request == f.request
            if self.first_error is None:
                self.first_error = state.terminal_error
            assert state.terminal_error is self.first_error
            if self.worker_lost and not self.death_reclaims:
                self.lose_worker()
            reply = deepcopy(f.target._handle_cancel_worker_lease(request))
            self.cancel_calls.append((request, reply))
            f.cancels.append((request, reply))
            assert len(self.cancel_calls) <= 3
            assert type(reply) is protocol.CancelWorkerLeaseReply
            assert reply.retired_grant == f.grant
            assert reply.dependency_inventory == f.inventory
            assert f.target._lease_dependency_custody.snapshot(f.request.lease_id) == f.inventory
            assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
                    reply.requester_worker_id, reply.scheduling_key) == (
                request.lease_id, request.task_id, request.attempt_id, request.requester_node_id,
                request.requester_worker_id, request.scheduling_key,
            )
            if self.worker_lost:
                assert reply.state is protocol.LeaseExecutionState.WORKER_LOST
                assert not reply.accepted and not reply.cancelled and not reply.released
            else:
                assert reply.state is protocol.LeaseExecutionState.ABANDONED and reply.accepted and reply.cancelled
                assert reply.released is (len(self.cancel_calls) == 1)
            self.assert_fenced()
            self.events.append("cancel-inventory")
            index = len(self.cancel_calls) - 1
            defect = self.cancel_defects[index] if index < len(self.cancel_defects) else None
            if defect == "ack-loss":
                raise TransportTimeout("actual cancellation inventory reply was lost")
            if defect == "wrong-node":
                other = NodeID(bytes(value ^ 1 for value in f.target.node_id.value))
                inventory = replace(reply.retired_grant, node_id=other, dependencies=tuple(
                    replace(item, node_id=other) for item in reply.retired_grant.dependencies
                ))
                # Corrupt only the delivered decoded DTO, not the Node's
                # actual cached proof or this observation of its real reply.
                # Its new request-scoped inventory must also reject a grant
                # that no longer agrees with the independently retained proof.
                delivered = deepcopy(reply)
                object.__setattr__(delivered, "retired_grant", inventory)
            elif defect == "partial":
                delivered = deepcopy(reply)
                object.__setattr__(delivered, "retired_grant", replace(
                    reply.retired_grant, dependencies=reply.retired_grant.dependencies[:1],
                ))
            else:
                assert defect is None
                delivered = reply
            self.delivered_cancels.append(delivered)
            return delivered

        assert handler == node_module.GET_WORKER_LEASE_OUTCOME_HANDLER and self.worker_lost
        assert type(request) is protocol.GetWorkerLeaseOutcome
        assert (request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
                request.owner_worker_id, request.object_ids, request.scheduling_key, request.target_execution) == (
            f.grant.lease_id, f.grant.task_id, f.grant.attempt_id, f.grant.worker_id,
            f.consumer.worker_id, f.pending.output_ids, f.grant.scheduling_key, f.grant.target_execution,
        )
        reply = deepcopy(f.target._handle_get_worker_lease_outcome(request))
        assert type(reply) is protocol.GetWorkerLeaseOutcomeReply and reply.found and not reply.worker_alive
        assert reply.state is protocol.LeaseExecutionState.WORKER_LOST and reply.completion_status is None
        assert not reply.descriptors and not reply.orphan_descriptors and not reply.cleanup_pending
        self.outcome_calls.append((request, reply))
        assert len(self.outcome_calls) <= 1
        self.events.append("worker-loss-proof")
        return reply


def _next_ready(f):
    count = f.consumer._submissions.qsize()
    assert 1 <= count <= 6
    selected = []
    for _ in range(count):
        item = f.consumer._submissions.get_nowait()
        f.consumer._submissions.task_done()
        if isinstance(item, _DelayedReadyTask):
            assert type(item.ready) is _ReadyTask
            selected.append(item.ready)
    assert len(selected) == 1
    return selected[0]


def _drive_ready(f, ready):
    assert ready.pending == f.pending and ready.dependencies == f.sources
    if ready.cancellation is not None:
        assert ready.cancellation.lease_request == f.request
        return f.consumer._resolve_lease_cancellation(
            ready.pending, ready.spec, ready.dependencies, ready.cancellation,
        )
    return f.consumer._execute(
        ready.pending, ready.spec, ready.dependencies, lease_state=ready.lease_state,
        ambiguity_round=ready.ambiguity_round, location_state=ready.location_state,
    )


def _lose_four_lease_rounds(f, transport):
    ready = _ReadyTask(f.pending, f.pending.spec, f.sources, lease_state=f.lease_state)
    for round_number in range(4):
        assert ready.cancellation is None and ready.location_state is None
        assert ready.lease_state.request == f.request and ready.ambiguity_round == round_number
        terminal = _drive_ready(f, ready)
        assert len(transport.lease_calls) == 3 * (round_number + 1)
        if round_number < 3:
            assert not terminal and not transport.cancel_calls and not f.reports
            f.assert_pending()
            f.assert_local_unrecorded()
            assert f.foreign.owner_table.snapshot(f.foreign_id).locations == frozenset((f.source.node_id,))
            assert all(f.target.object_store.snapshot(item.object_id).pin_count == 1 for item in f.sources)
            ready = _next_ready(f)
    return terminal


def _finish_handoff(f, terminal):
    for _ in range(3):
        if terminal:
            return
        terminal = _drive_ready(f, _next_ready(f))
    assert terminal, "bounded cancellation/custody replay failed to converge"


def _assert_terminal(f, transport, *, ambiguous=True, cancel_before_foreign=True):
    transport.assert_fenced()
    snapshot = f.consumer.owner_table.snapshot(f.pending.object_id)
    assert snapshot.state is ObjectState.ERROR and snapshot.error is transport.first_error
    if ambiguous:
        assert type(snapshot.error) is _LeaseRequestAmbiguous
        assert isinstance(snapshot.error.cause, TransportTimeout)
        assert snapshot.error.state.request == f.request
    assert f.pending.task_key not in f.consumer._protocol_unresolved
    assert not f.consumer._location_handoff_drivers
    record = f.consumer._recovery.task_record(f.pending.task_id)
    assert record.current_attempt == f.pending.spec.attempt_id and record.retries_started == 0
    assert f.consumer._recovery.active_recovery(f.pending.task_id) is None
    f.assert_local_recorded()
    f.assert_foreign_recorded()
    assert len(f.reports) == transport.expected_reports and not f.releases and not transport.drops
    latest = f.states[-1][1]
    assert latest.grant == f.grant and latest.lease_request == f.request
    assert latest.inventory == f.inventory and latest.custody_acknowledged
    assert len(f.custody_acks) == 1 and not f.target._lease_dependency_custody.has_pending()
    assert latest.terminal_error is transport.first_error
    assert len(latest.local_receipts) == len(latest.receipts) == 1
    assert latest.local_receipts[0].accepted and latest.receipts[0].accepted
    if transport.worker_lost:
        assert latest.cancellation_reply is None and latest.execution_outcome == transport.outcome_calls[0][1]
        assert len(transport.death_reclaims) == 1 and 1 <= len(transport.cancel_calls) <= 2
    else:
        assert latest.cancellation_reply == transport.cancel_calls[-1][1]
        assert latest.execution_outcome is None
    assert all(request == f.request for request, _ in transport.lease_calls)
    assert all(request == transport.cancel_calls[0][0] for request, _ in transport.cancel_calls)
    cancelled_at = transport.events.index("cancel-inventory")
    foreign_at = transport.events.index("foreign-custody")
    assert foreign_at < transport.events.index("node-custody-ack")
    if cancel_before_foreign:
        assert cancelled_at < foreign_at
    else:
        assert foreign_at < cancelled_at
    assert len(f.transfers) == 6
    return latest


def _collect_all(f, transport):
    f.finish()
    transport.gc_open = True
    f.local_ref.close()
    for core in (f.consumer, f.foreign):
        for output in tuple(core._objects):
            for token in core.owner_table.snapshot(output).local_tokens:
                assert core.owner_table.release_local_reference(output, token)
            core._reference_released(output)
        assert core._reference_mailbox.pending.qsize() <= 8
        core._reference_mailbox.drain()
        assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
    assert f.consumer.owner_table.collection_state(f.local_id) is ObjectCollectionState.COLLECTED
    assert f.foreign.owner_table.collection_state(f.foreign_id) is ObjectCollectionState.COLLECTED
    assert f.consumer.owner_table.collection_state(f.pending.object_id) is ObjectCollectionState.COLLECTED
    assert not f.consumer._objects and not f.foreign._objects
    assert not f.consumer._object_gc_obligations and not f.foreign._object_gc_obligations
    assert not f.consumer._task_finish_barriers and not f.consumer._protocol_unresolved
    assert {(request.object_id, request.node_id) for request, _ in transport.drops} == {
        (f.local_id, f.target.node_id), (f.foreign_id, f.source.node_id), (f.foreign_id, f.target.node_id),
    }
    assert len(transport.drops) == 3
    for node in (f.source, f.target):
        assert node.object_store.used_bytes == 0 and node._sealed_metadata == {}
        assert node.object_store.object_ids(sealed_only=False) == ()
        assert not getattr(node, "_dependency_pin_cleanups", {})
        assert not node._dependency_custody_registry_locked().has_pending()
    assert all(not f.target._object_manager.is_ready(item.object_id) for item in f.sources)


def _close(f):
    # Unlike the reused fixture.close, this also accepts actual completed GC.
    # Existing authorities are released through their methods, never cleared.
    if f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED:
        f.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
            f.request.lease_id, f.request.task_id, f.request.attempt_id,
            f.request.requester_node_id, f.request.requester_worker_id, f.request.scheduling_key,
            lease_request=f.request,
        ))
    if f.consumer.owner_table.contains(f.local_id):
        f.consumer.owner_table.release_submitted_reference(f.local_id, f.local_hold)
    f.foreign.owner_table.release_retained_reference_for_task(f.foreign_id, f.foreign_hold)
    f.local_ref.close()
    for core in (f.consumer, f.foreign):
        for output in tuple(core._objects):
            for token in core.owner_table.snapshot(output).local_tokens:
                core.owner_table.release_local_reference(output, token)
        close_pure_core(core)


def test_all_grant_acknowledgements_lost_recovers_cancel_inventory_and_collects_real_replicas(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f)
        terminal = _lose_four_lease_rounds(f, transport)
        _finish_handoff(f, terminal)
        latest = _assert_terminal(f, transport)
        assert len(transport.lease_calls) == 12 and len(transport.cancel_calls) == 1
        assert latest.cancellation_reply.retired_grant == f.grant
        _collect_all(f, transport)
    finally:
        _close(f)


def test_lost_cancel_inventory_ack_replays_only_cancellation_then_hands_off(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, cancel_defects=("ack-loss",))
        assert not _lose_four_lease_rounds(f, transport)
        f.assert_pending()
        f.assert_local_unrecorded()
        transport.assert_fenced()
        assert not f.reports and len(transport.cancel_calls) == 1
        replay = _next_ready(f)
        assert replay.cancellation is not None and replay.cancellation.lease_request == f.request
        _finish_handoff(f, _drive_ready(f, replay))
        _assert_terminal(f, transport)
        assert len(transport.lease_calls) == 12 and len(transport.cancel_calls) == 2
        assert transport.cancel_calls[0][1].released and not transport.cancel_calls[1][1].released
        assert transport.cancel_calls[0][1].retired_grant == transport.cancel_calls[1][1].retired_grant == f.grant
        _collect_all(f, transport)
    finally:
        _close(f)


@pytest.mark.parametrize("defect", ("wrong-node", "partial"))
def test_untrusted_retired_inventory_keeps_holds_and_bytes_until_exact_cancel_replay(monkeypatch, defect):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, cancel_defects=(defect,))
        assert not _lose_four_lease_rounds(f, transport)
        f.assert_pending()
        f.assert_local_unrecorded()
        transport.assert_fenced()
        assert not f.reports and not transport.drops and len(transport.cancel_calls) == 1
        assert transport.delivered_cancels[0].retired_grant != f.grant
        assert f.foreign.owner_table.snapshot(f.foreign_id).locations == frozenset((f.source.node_id,))
        pending_error = transport.first_error
        replay = _next_ready(f)
        _finish_handoff(f, _drive_ready(f, replay))
        _assert_terminal(f, transport)
        assert transport.first_error is pending_error and len(transport.cancel_calls) == 2
        assert len(transport.lease_calls) == 12
        _collect_all(f, transport)
    finally:
        _close(f)


def test_unknown_grant_then_executor_loss_uses_retired_inventory_and_distinct_execution_proof(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, worker_lost=True)
        terminal = _lose_four_lease_rounds(f, transport)
        _finish_handoff(f, terminal)
        latest = _assert_terminal(f, transport)
        assert len(transport.lease_calls) == 12 and len(transport.outcome_calls) == 1
        assert latest.cancellation_reply is None and latest.execution_outcome.state is protocol.LeaseExecutionState.WORKER_LOST
        assert all(reply.retired_grant == f.grant and not reply.cancelled for _, reply in transport.cancel_calls)
        _collect_all(f, transport)
    finally:
        _close(f)


@pytest.mark.parametrize("grant_defect", (
    "partial", "missing-lease-id", "missing-worker-value", "duplicate-dependency",
))
def test_partial_live_grant_reply_replays_exact_hop_before_any_custody_or_push(monkeypatch, grant_defect):
    f = _Fixture(monkeypatch)
    try:
        # The route fault happens only after the complete valid grant returns.
        # It keeps this regression nonexecuting while proving the first malformed
        # reply did not escape the lease-hop validator into terminal teardown.
        routes = _route_fault(f, after=False)
        transport = _GrantDelivery(f, grant_subset_once=True, grant_defect=grant_defect)
        assert not f.execute()
        assert len(transport.lease_calls) == 2 and routes.failed
        f.assert_pending()
        # The valid replay retained the complete manifest before the local
        # fault; its missing local receipt repairs after foreign custody.
        _finish_handoff(f, _drive_ready(f, _next_ready(f)))
        _assert_terminal(f, transport, ambiguous=False)
        assert len(transport.lease_calls) == 2 and len(transport.cancel_calls) == 1
        assert len(transport.delivered_bad_grants) == 1
        assert all(reply == f.grant for _, reply in transport.lease_calls)
        assert f.target._leases[f.grant.lease_id].grant == f.grant
        assert "local route failed" in str(transport.first_error)
        _collect_all(f, transport)
    finally:
        _close(f)


def test_postgrant_builder_failure_reuses_validated_cancel_inventory_without_repeating_rpc(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, grant_success=True)
        original_build = f.consumer._build_location_reports
        errors = (RuntimeError("first complete-grant inventory build failed"),
                  RuntimeError("second cancelled-grant inventory build failed"))
        builds = []

        def build(requested, grant, guards=()):
            builds.append((requested, grant, guards))
            assert len(builds) <= 3
            assert requested == f.sources and grant == f.grant and guards == (f.guard,)
            f.assert_local_unrecorded()
            assert not f.reports and not f.releases
            assert f.local_hold in f.consumer.owner_table.snapshot(f.local_id).submitted_tokens
            assert f.foreign.owner_table.has_retained_reference_for_task(f.foreign_id, f.foreign_hold)
            if len(builds) == 1:
                assert not transport.cancel_calls
                assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
                assert all(f.target.object_store.snapshot(item.object_id).pin_count == 1 for item in f.sources)
            else:
                transport.assert_fenced()
                marker = f.consumer._protocol_unresolved[f.pending.task_key]
                assert type(marker.obligation) is _LeaseCancellationState
                state = marker.obligation
                assert state.known_grant == f.grant and state.lease_request == f.request
                assert state.reply == transport.cancel_calls[0][1]
                assert state.terminal_error is errors[0]
            if len(builds) <= 2:
                raise errors[len(builds) - 1]
            return original_build(requested, grant, guards)

        monkeypatch.setattr(f.consumer, "_build_location_reports", build)
        assert not f.execute()
        assert len(builds) == 2 and len(transport.lease_calls) == len(transport.cancel_calls) == 1
        f.assert_pending()
        replay = _next_ready(f)
        assert replay.cancellation is not None
        saved_reply = replay.cancellation.reply
        assert saved_reply == transport.cancel_calls[0][1] and saved_reply.retired_grant == f.grant
        assert replay.cancellation.terminal_error is transport.first_error is errors[0]
        before = tuple(f.calls)
        _finish_handoff(f, _drive_ready(f, replay))
        latest = _assert_terminal(f, transport, ambiguous=False)
        assert len(builds) == 3 and builds[0] == builds[1] == builds[2]
        assert tuple(f.calls[:-1]) == before and len(transport.lease_calls) == len(transport.cancel_calls) == 1
        assert f.calls[-1] == (f.target_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER, f.custody_acks[0][0])
        assert latest.cancellation_reply == saved_reply and latest.terminal_error is errors[0]
        assert type(latest.terminal_error) is RuntimeError
        _collect_all(f, transport)
    finally:
        _close(f)


def test_old_queued_cancel_resumes_canonical_handoff_without_losing_local_receipt(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, cancel_defects=("ack-loss",), expected_reports=2)
        f.report_losses = 1
        original_custody = f.consumer._record_replica_custody_locked
        local_calls = []

        def record_local(descriptor, *, active_hold):
            assert descriptor == f.grant.dependencies[0]
            receipt = original_custody(descriptor, active_hold=active_hold)
            local_calls.append(receipt)
            assert len(local_calls) == 1 and receipt.accepted and receipt.custody_transferred
            return receipt

        monkeypatch.setattr(f.consumer, "_record_replica_custody_locked", record_local)
        assert not _lose_four_lease_rounds(f, transport)
        stale_ready = _next_ready(f)
        assert stale_ready.cancellation is not None and stale_ready.cancellation.reply is None
        assert stale_ready.cancellation.terminal_error is transport.first_error
        assert not _drive_ready(f, stale_ready)
        location_ready = _next_ready(f)
        assert location_ready.location_state is not None and location_ready.cancellation is None
        current = f.marker().obligation
        assert current == location_ready.location_state and current.terminal_error is transport.first_error
        assert len(current.local_receipts) == len(local_calls) == 1 and current.receipts == ()
        assert current.cancellation_reply == transport.cancel_calls[-1][1]
        assert current.local_receipts == tuple(local_calls)
        assert len(f.reports) == 1 and f.reports[0][1].accepted
        f.assert_pending()
        f.assert_local_recorded()
        f.assert_foreign_recorded()
        before = tuple(f.calls)
        # The old actual queued cancel is deliberately delivered after the
        # newer Location state has committed local custody and lost foreign ACK.
        assert _drive_ready(f, stale_ready)
        latest = _assert_terminal(f, transport)
        assert tuple(f.calls[:-1]) == before and len(transport.lease_calls) == 12 and len(transport.cancel_calls) == 2
        assert f.calls[-1] == (f.target_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER, f.custody_acks[0][0])
        assert len(local_calls) == 1 and latest.local_receipts == current.local_receipts
        assert latest.cancellation_reply == current.cancellation_reply and latest.terminal_error is current.terminal_error
        assert len(f.reports) == 2 and f.reports[0][0] == f.reports[1][0]
        assert f.reports[1][1].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert latest.receipts == (f.reports[1][1],)
        # Frozen historical/queued values stay progress-free instead of being
        # mutated into a second, competing source of acknowledgements.
        assert stale_ready.cancellation.reply is None
        assert location_ready.location_state.local_receipts == current.local_receipts
        assert location_ready.location_state.receipts == ()
        _collect_all(f, transport)
    finally:
        _close(f)


def test_queued_cancel_latches_failure_into_an_active_success_only_handoff(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, grant_success=True)
        error = RuntimeError("older cancellation revoked this active handoff")
        old_cancel = _LeaseCancellationState(
            protocol.CancelWorkerLease(
                f.request.lease_id, f.request.task_id, f.request.attempt_id,
                f.request.requester_node_id, f.request.requester_worker_id, f.request.scheduling_key,
                lease_request=f.request,
            ),
            f.target_address, error, target_node_id=f.target.node_id,
            lease_request=f.request, known_grant=f.grant,
        )
        # Queue an exact cancellation intent, not a fabricated successful ACK.
        # The test controls delivery order; the real driver owns every effect.
        queued = _ReadyTask(f.pending, f.pending.spec, f.sources, cancellation=old_cancel)
        f.consumer._submissions.put(_DelayedReadyTask(queued, 0.0))
        original_custody = f.consumer._record_replica_custody_locked
        local_calls, observed = [], []

        def record_local(descriptor, *, active_hold):
            receipt = original_custody(descriptor, active_hold=active_hold)
            local_calls.append(receipt)
            assert len(local_calls) == 1 and receipt.accepted
            return receipt

        def deliver_old_cancel_during_foreign_call():
            assert not observed and not transport.cancel_calls and not f.reports
            state = f.marker().obligation
            assert state.terminal_error is None and state.cancellation_reply is None
            assert state.local_receipts == tuple(local_calls) and len(local_calls) == 1
            assert state.receipts == ()
            assert (f.pending.execution, f.grant.lease_id) in f.consumer._location_handoff_drivers
            assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
            assert all(f.target.object_store.snapshot(item.object_id).pin_count == 1 for item in f.sources)
            delayed = _next_ready(f)
            assert delayed is queued
            before_calls = tuple(f.calls)
            assert not _drive_ready(f, delayed)
            latched = f.marker().obligation
            assert latched.terminal_error is error and state.terminal_error is None
            assert latched.grant == state.grant and latched.reports == state.reports
            assert latched.local_receipts == state.local_receipts and latched.receipts == ()
            assert latched.cancellation_reply is None and latched.execution_outcome is None
            assert tuple(f.calls) == before_calls and not f.releases
            observed.append((state, latched))
            transport.events.append("foreign-custody")

        monkeypatch.setattr(f.consumer, "_record_replica_custody_locked", record_local)
        f.before_report = deliver_old_cancel_during_foreign_call
        assert f.execute()
        latest = _assert_terminal(f, transport, ambiguous=False, cancel_before_foreign=False)
        assert len(observed) == len(local_calls) == len(f.reports) == 1
        assert latest.terminal_error is error is transport.first_error
        assert latest.local_receipts == observed[0][0].local_receipts
        assert observed[0][0].terminal_error is None and observed[0][1].terminal_error is error
        assert len(transport.lease_calls) == len(transport.cancel_calls) == 1
        assert old_cancel.reply is None
        _collect_all(f, transport)
    finally:
        _close(f)
