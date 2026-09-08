"""Pure recovery of pending pre-grant witnesses and a late hold release.

Each case reuses two threadless Cores, two 1 KiB stores, one tiny local input
and one nonexecuting consumer. Three real pin/chunk/release calls localize it.
The first two cases fail the original and immediate-repair metadata/snapshot
operations, then let an already queued exact cancellation invoke the Node's
normal physical reconciliation. No test-side owner, registry or store repair.

The third case admits a real grant and releases the actual SUBMITTED hold after
the Node's custody ACK effect, proving the final permission check revokes Push.
At most one manual replay per case; no runtime thread, socket, process, wait,
GCS, additional task or fabricated protocol reply. Physical GC uses real drops.
"""

from copy import deepcopy

import pytest

from miniray import node as node_module, protocol
from miniray.core import _LeaseCancellationState, _LocationReportState, _RetryInlineGc
from miniray.errors import LeaseRejectedError
from miniray.lease_dependencies import DependencyCustodyConflict
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import TransportTimeout
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_pregrant_dependency_custody import _Fixture


pytestmark = pytest.mark.unit


class _TwiceFaultedLocalization:
    """Expose the true unresolved Request result without fixture assumptions."""

    def __init__(self, fixture):
        self.f = fixture
        self.actual_rpc = fixture.rpc
        self.cancel_attempts = []
        fixture.consumer._rpc = self.rpc

    def rpc(self, address, handler, request):
        f = self.f
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert address == f.target_address and request == f.request
            reply = deepcopy(f.target._handle_request_lease(request))
            f.requests.append((request, reply))
            assert len(f.requests) == 1 and type(reply) is protocol.RejectWorkerLease
            assert reply.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
            assert f.request.lease_id not in f.target._leases
            assert f.registry().candidates(f.request.lease_id) == f.expected_descriptors()
            assert f.registry().has_pending()
            with pytest.raises(DependencyCustodyConflict, match="physical reconciliation"):
                f.registry().snapshot(f.request.lease_id)
            assert f.target.object_store.get(f.local_id) == f.payloads[0]
            return reply
        if handler == node_module.CANCEL_LEASE_HANDLER:
            assert address == f.target_address and request.lease_request == f.request
            self.cancel_attempts.append(request)
            assert len(self.cancel_attempts) <= 2
            if len(self.cancel_attempts) == 1:
                # Retain a real Core delayed cancellation so the next drive is
                # its public protocol path, not a direct reconciliation call.
                state = f.consumer._protocol_unresolved[f.pending.task_key].obligation
                assert type(state) is _LeaseCancellationState and state.reply is None
                assert isinstance(state.terminal_error, LeaseRejectedError)
                f.error = state.terminal_error
                raise TransportTimeout("first cancellation delivery unavailable")
        return self.actual_rpc(address, handler, request)


def _assert_unresolved_then_replay(f, delivery):
    core = f.consumer
    assert not core._execute(f.pending, f.pending.spec, f.sources, lease_state=f.lease_state)
    assert len(delivery.cancel_attempts) == 1 and not f.cancels and not f.acks
    assert not f.states and not f.reports and not f.drops
    assert len(f.requests) == 1 and len(f.transfers) == 3
    assert len(f.source_release_replies) == 1 and f.source_release_replies[0][1].released
    assert f.registry().has_pending() and f.registry().candidates(f.request.lease_id) == f.expected_descriptors()
    state = core._protocol_unresolved[f.pending.task_key].obligation
    assert type(state) is _LeaseCancellationState and state.reply is None
    assert state.lease_request == f.request and state.terminal_error is f.error
    assert core.owner_table.snapshot(f.pending.object_id).state is ObjectState.PENDING
    assert f.local_hold in core.owner_table.snapshot(f.local_id).submitted_tokens
    assert not core._finish_pending_task(f.pending)
    assert f.target.object_store.snapshot(f.local_id).pin_count == 0
    # No authority is changed to heal the fault. The two failures are exhausted;
    # existing Cancel's object/state-lock reconciliation owns the next effect.
    assert f.replay_once()
    assert len(delivery.cancel_attempts) == 2 and delivery.cancel_attempts[0] == delivery.cancel_attempts[1]
    assert len(f.cancels) == len(f.acks) == 1 and len(f.requests) == 1
    assert f.cancels[0][1].retired_grant is None
    assert f.cancels[0][1].dependency_inventory.descriptors == f.expected_descriptors()
    assert f.registry().candidates(f.request.lease_id) == () and not f.registry().has_pending()
    assert not getattr(f.target, "_localization_seal_witnesses", {})
    assert f.states[-1][1].terminal_error is f.error and f.states[-1][1].custody_acknowledged
    assert f.states[-1][1].grant is None and len(f.states[-1][1].local_receipts) == 1
    assert f.consumer.owner_table.snapshot(f.local_id).canonical_stored_result == f.results[0]
    assert f.target.object_store.get(f.local_id) == f.source.object_store.get(f.local_id) == f.payloads[0]
    f.finish_and_collect()


def test_two_metadata_write_failures_recover_through_exact_cancel_inventory_and_gc(monkeypatch):
    f = _Fixture(monkeypatch, "metadata-write-failure")
    metadata = None
    try:
        expected = (f.sources[0].producer_attempt_id, f.consumer.worker_id, f.sources[0].size_bytes, f.sources[0].checksum)

        class FailTwice(dict):
            def __init__(self, initial):
                super().__init__(initial)
                self.remaining = 2
                self.writes = []

            def __setitem__(self, key, value):
                if key == f.local_id:
                    self.writes.append((key, value))
                    assert value == expected and len(self.writes) <= 3
                    assert f.target.object_store.get(key) == f.payloads[0]
                    assert f.registry().candidates(f.request.lease_id) == f.expected_descriptors()
                    assert f.target._localization_seal_witnesses[f.request.lease_id, key] == f.expected_descriptors()[0]
                    if self.remaining:
                        self.remaining -= 1
                        assert key not in self
                        raise RuntimeError("sealed metadata temporarily unavailable")
                dict.__setitem__(self, key, value)

        metadata = FailTwice(f.target._sealed_metadata)
        f.target._sealed_metadata = metadata
        delivery = _TwiceFaultedLocalization(f)
        _assert_unresolved_then_replay(f, delivery)
        assert metadata.remaining == 0 and len(metadata.writes) == 3
        assert metadata.writes[0] == metadata.writes[1] == metadata.writes[2]
        assert "sealed metadata temporarily unavailable" in f.requests[0][1].detail
    finally:
        if metadata is not None:
            metadata.remaining = 0
        f.close()


def test_two_postseal_snapshot_failures_recover_without_repull_or_new_lease(monkeypatch):
    f = _Fixture(monkeypatch, "metadata-write-failure")
    try:
        actual = f.target.object_store.snapshot
        failures = []

        def snapshot(object_id):
            if object_id == f.local_id and len(failures) < 2:
                assert f.target.object_store.contains(object_id, sealed_only=True)
                assert f.target._sealed_metadata[object_id] == (
                    f.sources[0].producer_attempt_id, f.consumer.worker_id,
                    f.sources[0].size_bytes, f.sources[0].checksum,
                )
                assert f.registry().candidates(f.request.lease_id) == f.expected_descriptors()
                failures.append(object_id)
                raise RuntimeError("sealed snapshot temporarily unavailable")
            return actual(object_id)

        monkeypatch.setattr(f.target.object_store, "snapshot", snapshot)
        delivery = _TwiceFaultedLocalization(f)
        _assert_unresolved_then_replay(f, delivery)
        assert failures == [f.local_id, f.local_id]
        assert "sealed snapshot temporarily unavailable" in f.requests[0][1].detail
    finally:
        f.close()


def test_submitted_hold_released_after_real_inventory_ack_prevents_push_and_cancels_grant(monkeypatch):
    f = _Fixture(monkeypatch, "metadata-write-failure")
    try:
        core = f.consumer
        actual_rpc = f.rpc
        grants, events, at_ack = [], [], []
        local_calls = []
        actual_custody = core._record_replica_custody_locked

        def record_local(descriptor, *, active_hold):
            receipt = actual_custody(descriptor, active_hold=active_hold)
            local_calls.append(receipt)
            assert len(local_calls) == 1 and receipt.accepted
            return receipt

        def rpc(address, handler, request):
            if handler == node_module.REQUEST_LEASE_HANDLER:
                assert address == f.target_address and request == f.request
                grant = deepcopy(f.target._handle_request_lease(request))
                grants.append(grant)
                assert len(grants) == 1 and type(grant) is protocol.GrantWorkerLease
                assert grant.dependencies == f.expected_descriptors()
                assert f.target.object_store.snapshot(f.local_id).pin_count == 1
                events.append("granted")
                return grant
            if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
                assert address == f.target_address and not at_ack
                state = core._protocol_unresolved[f.pending.task_key].obligation
                assert type(state) is _LocationReportState and state.grant == grants[0]
                assert state.terminal_error is None and state.cancellation_reply is None
                assert state.local_receipts == tuple(local_calls) and len(local_calls) == 1
                assert not state.receipts and not state.custody_acknowledged
                assert f.local_hold in core.owner_table.snapshot(f.local_id).submitted_tokens
                reply = f.target._handle_ack_lease_dependency_custody(request)
                f.acks.append((request, reply))
                assert reply.accepted and reply.request == request
                assert not f.registry().has_pending()
                assert f.target._leases[grants[0].lease_id].state is protocol.LeaseExecutionState.GRANTED
                events.append("custody-ack")
                assert core.owner_table.release_submitted_reference(f.local_id, f.local_hold)
                events.append("hold-released")
                at_ack.append(state)
                return reply
            if handler == node_module.CANCEL_LEASE_HANDLER:
                assert address == f.target_address and request.lease_request == f.request
                state = core._protocol_unresolved[f.pending.task_key].obligation
                assert type(state) is _LocationReportState and state.custody_acknowledged
                assert state.local_receipts[0].status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
                assert state.terminal_error is not None and "hold is not active" in str(state.terminal_error)
                reply = f.target._handle_cancel_worker_lease(request)
                f.cancels.append((request, reply))
                assert len(f.cancels) == 1 and reply.accepted and reply.cancelled and reply.released
                assert reply.retired_grant == grants[0]
                assert reply.dependency_inventory == f.acks[0][0].inventory
                assert reply.state is protocol.LeaseExecutionState.ABANDONED
                events.append("cancelled")
                return reply
            return actual_rpc(address, handler, request)

        monkeypatch.setattr(core, "_record_replica_custody_locked", record_local)
        monkeypatch.setattr(core, "_rpc", rpc)
        terminal = core._execute(f.pending, f.pending.spec, f.sources, lease_state=f.lease_state)
        if not terminal:
            state = core._protocol_unresolved[f.pending.task_key].obligation
            assert type(state) is _LocationReportState and state.custody_acknowledged
            assert state.local_receipts[0].status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
            assert state.terminal_error is not None
            assert core.owner_table.snapshot(f.pending.object_id).state is ObjectState.PENDING
            assert not core._finish_pending_task(f.pending)
            assert f.replay_once()
        assert events == ["granted", "custody-ack", "hold-released", "cancelled"]
        assert len(grants) == len(f.acks) == len(f.cancels) == len(at_ack) == len(local_calls) == 1
        latest = f.states[-1][1]
        result = core.owner_table.snapshot(f.pending.object_id)
        assert result.state is ObjectState.ERROR and result.error is latest.terminal_error
        assert "hold is not active" in str(result.error)
        assert latest.custody_acknowledged and latest.cancellation_reply == f.cancels[0][1]
        assert latest.local_receipts[0].custody_transferred and not latest.local_receipts[0].accepted
        assert at_ack[0].local_receipts[0].accepted and at_ack[0].terminal_error is None
        assert not core._protocol_unresolved and not f.registry().has_pending()
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        assert f.target.object_store.snapshot(f.local_id).pin_count == 0
        assert len(f.transfers) == 3 and not f.drops
        assert any(isinstance(event, _RetryInlineGc) and event.object_id == f.local_id
                   for event in tuple(core._reference_mailbox.pending.queue))
        assert core._recovery.task_record(f.pending.task_id).retries_started == 0
        assert core._finish_pending_task(f.pending)
        f.gc_open = True
        for participant in (core, f.foreign):
            for output in tuple(participant._objects):
                for token in participant.owner_table.snapshot(output).local_tokens:
                    assert participant.owner_table.release_local_reference(output, token)
                participant._reference_released(output)
            assert participant._reference_mailbox.pending.qsize() <= 8
            participant._reference_mailbox.drain()
            assert not participant._objects and not participant._object_gc_obligations
        assert core.owner_table.collection_state(f.local_id) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(f.pending.object_id) is ObjectCollectionState.COLLECTED
        assert len(f.drops) == 2 and not f.registry().has_pending()
        for node in (f.source, f.target):
            assert node.object_store.used_bytes == 0 and node._sealed_metadata == {}
    finally:
        f.close()
