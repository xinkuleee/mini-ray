"""Pure linearization at the completed-handoff to direct-Push boundary.

Reuse two threadless Cores, two 1 KiB stores and one real dual-input grant.
Hooks run synchronously after the real handoff returns success and releases
its ticket, or after the final push_send admission. No process, thread, socket,
wait, user callable, extra lease or producer retry runs. At most one exact
cancel ACK is lost and at most two manual custody replay steps are permitted.

Before-admission cancellation must prevent even a Push transport invocation.
After-admission cancellation cannot retract that invocation; Node Start remains
the execution authority. All cases end in real input/reference collection.
"""

import pytest

from miniray import protocol
from miniray.core import _LeaseCancellationState, _LocationReportState
from miniray.ownership import ObjectState
from tests.unit.test_ambiguous_grant_custody import (
    _GrantDelivery, _assert_terminal, _close, _collect_all,
)
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_local_replica_handoff import _Fixture


pytestmark = pytest.mark.unit


class _AfterHandoff:
    def __init__(self, f, monkeypatch, *, lose_ack=False):
        self.f = f
        self.transport = _GrantDelivery(
            f, grant_success=True, cancel_defects=("ack-loss",) if lose_ack else (),
        )
        self.error = RuntimeError("cancellation selected after custody, before Push admission")
        self.cancellation = _LeaseCancellationState(
            protocol.CancelWorkerLease(
                f.request.lease_id, f.request.task_id, f.request.attempt_id,
                f.request.requester_node_id, f.request.requester_worker_id, f.request.scheduling_key,
                lease_request=f.request,
            ),
            f.target_address, self.error, target_node_id=f.target.node_id,
            lease_request=f.request, known_grant=f.grant,
        )
        self.lose_ack = lose_ack
        self.original_handoff = f.consumer._report_granted_dependency_locations
        self.initial = None
        self.injected = False
        self.cancel_returns = []
        self.push_phases = []
        self.pushes = []
        self.local_receipts = []
        f.before_report = self.before_foreign
        original_custody = f.consumer._record_replica_custody_locked

        def rpc_without_core_lock(address, handler, request):
            assert not f.consumer._state_lock._is_owned(), "Push admission held the Core lock across RPC"
            return self.transport.rpc(address, handler, request)

        f.consumer._rpc = rpc_without_core_lock

        def record_local(descriptor, *, active_hold):
            receipt = original_custody(descriptor, active_hold=active_hold)
            self.local_receipts.append(receipt)
            assert len(self.local_receipts) == 1 and receipt.accepted
            return receipt

        original_mark = f.consumer._mark_protocol_unresolved

        def observe_mark(pending, phase, obligation=None, **kwargs):
            original_mark(pending, phase, obligation, **kwargs)
            if phase == "push_send":
                self.push_phases.append((pending, obligation))
                assert len(self.push_phases) <= 1

        def no_push(*args, **kwargs):
            self.pushes.append((args, kwargs))
            pytest.fail("cancellation won before admission but stale lane invoked Push")

        monkeypatch.setattr(f.consumer, "_record_replica_custody_locked", record_local)
        monkeypatch.setattr(f.consumer, "_mark_protocol_unresolved", observe_mark)
        monkeypatch.setattr(f.consumer, "_report_granted_dependency_locations", self.after_handoff)
        f.consumer._push_task_rpc = no_push

    def before_foreign(self):
        f = self.f
        state = f.marker().obligation
        assert state.terminal_error is None and state.cancellation_reply is None
        assert state.local_receipts == tuple(self.local_receipts) and len(self.local_receipts) == 1
        assert not self.transport.cancel_calls and not self.injected and not f.reports
        assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
        self.transport.events.append("foreign-custody")

    def after_handoff(self, pending, prepared, dependencies, state):
        result = self.original_handoff(pending, prepared, dependencies, state)
        if result and not self.injected:
            self.injected = True
            f = self.f
            marker = f.marker()
            self.initial = marker.obligation
            assert marker.phase == "locations_reported"
            assert self.initial.terminal_error is None
            assert len(self.initial.local_receipts) == len(self.initial.receipts) == 1
            assert not f.consumer._location_handoff_drivers and not self.push_phases
            assert self.initial.local_receipts[0].accepted and self.initial.receipts[0].accepted
            selected = f.consumer._resolve_lease_cancellation(
                pending, prepared, dependencies, self.cancellation,
            )
            self.cancel_returns.append(selected)
            assert len(self.cancel_returns) == 1 and selected is (not self.lose_ack)
            self.transport.assert_fenced()
            if self.lose_ack:
                f.assert_pending()
                retained = f.marker().obligation
                assert retained.terminal_error is self.error and retained.cancellation_reply is None
                assert retained.local_receipts == self.initial.local_receipts
                assert retained.receipts == self.initial.receipts
            else:
                snapshot = f.consumer.owner_table.snapshot(f.pending.object_id)
                assert snapshot.state is ObjectState.ERROR and snapshot.error is self.error
                assert not f.consumer._protocol_unresolved
                # The other lane has not finalized yet: checking only the
                # _finished_tasks tombstone would miss this terminal object.
                assert f.pending.task_key not in f.consumer._finished_tasks
                assert f.consumer._task_finish_barriers[f.pending.object_id] == f.pending
        return result

    def assert_terminal(self):
        f = self.f
        latest = _assert_terminal(f, self.transport, ambiguous=False, cancel_before_foreign=False)
        assert self.injected and latest.terminal_error is self.error
        assert not self.push_phases and not self.pushes
        assert len(self.local_receipts) == len(f.reports) == len(self.transport.lease_calls) == 1
        assert latest.local_receipts == self.initial.local_receipts and latest.receipts == self.initial.receipts
        assert len(self.transport.cancel_calls) == (2 if self.lose_ack else 1)


@pytest.mark.parametrize("lose_ack", (True, False), ids=("cancel-ack-unknown", "error-not-yet-finalized"))
def test_cancellation_after_handoff_success_prevents_stale_push_admission(monkeypatch, lose_ack):
    f = _Fixture(monkeypatch)
    try:
        case = _AfterHandoff(f, monkeypatch, lose_ack=lose_ack)
        assert f.execute()
        case.assert_terminal()
        _collect_all(f, case.transport)
    finally:
        _close(f)


def test_cancellation_after_push_admission_is_arbitrated_by_node_start(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        transport = _GrantDelivery(f, grant_success=True)
        error = RuntimeError("cancellation selected after Push admission")
        cancellation = _LeaseCancellationState(
            protocol.CancelWorkerLease(
                f.request.lease_id, f.request.task_id, f.request.attempt_id,
                f.request.requester_node_id, f.request.requester_worker_id, f.request.scheduling_key,
                lease_request=f.request,
            ),
            f.target_address, error, target_node_id=f.target.node_id,
            lease_request=f.request, known_grant=f.grant,
        )
        admitted, pushes, starts = [], [], []
        original_mark = f.consumer._mark_protocol_unresolved

        def before_foreign():
            state = f.marker().obligation
            assert state.terminal_error is None and state.cancellation_reply is None
            assert not transport.cancel_calls and not f.reports
            transport.events.append("foreign-custody")

        def observe_mark(pending, phase, obligation=None, **kwargs):
            original_mark(pending, phase, obligation, **kwargs)
            if phase == "push_send":
                assert f.consumer._state_lock._is_owned()
                marker = f.marker()
                assert marker.phase == phase and type(marker.obligation) is _LocationReportState
                assert marker.obligation.terminal_error is None
                assert marker.obligation.cancellation_reply is None and marker.obligation.execution_outcome is None
                assert len(marker.obligation.local_receipts) == len(marker.obligation.receipts) == 1
                admitted.append(marker.obligation)
                assert len(admitted) == 1

        class StopAtPushBoundary(KeyboardInterrupt):
            pass

        def observe_push(address, handler, request):
            # No Worker or user code is run. This spy is the admitted transport
            # boundary; a later exact Cancel cannot retroactively unsend it.
            pushes.append((address, handler, request))
            assert len(pushes) == len(admitted) == 1
            assert not f.consumer._state_lock._is_owned()
            assert address == f.grant.worker_address and handler == "push_task"
            assert request.lease_id == f.grant.lease_id and request.spec == f.pending.spec
            assert request.dependencies == f.grant.dependencies
            assert f.consumer._resolve_lease_cancellation(f.pending, f.pending.spec, f.sources, cancellation)
            start = f.target._handle_start_worker_lease(protocol.StartWorkerLease(
                f.grant.lease_id, f.grant.task_id, f.grant.attempt_id, f.grant.worker_id,
                f.grant.scheduling_key, f.grant.target_execution,
            ))
            starts.append(start)
            assert type(start) is protocol.StartWorkerLeaseReply
            assert not start.accepted and start.state is protocol.LeaseExecutionState.ABANDONED
            raise StopAtPushBoundary()

        def stop_outcome_replay(*_args, **_kwargs):
            raise StopAtPushBoundary()

        f.before_report = before_foreign
        monkeypatch.setattr(f.consumer, "_mark_protocol_unresolved", observe_mark)
        f.consumer._push_task_rpc = observe_push
        # Core catches transport-boundary exceptions normally. Terminate this
        # inspection at its existing replay seam without inventing a TaskReply.
        monkeypatch.setattr(f.consumer, "_schedule_ambiguous_push", stop_outcome_replay)
        with pytest.raises(StopAtPushBoundary):
            f.execute()
        latest = _assert_terminal(f, transport, ambiguous=False, cancel_before_foreign=False)
        assert latest.terminal_error is error and len(admitted) == len(pushes) == len(starts) == 1
        assert len(transport.cancel_calls) == len(transport.lease_calls) == 1
        _collect_all(f, transport)
    finally:
        _close(f)
