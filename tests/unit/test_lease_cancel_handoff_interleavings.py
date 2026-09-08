"""Pure interleavings between old cancellation work and newer custody.

Two threadless Cores, two 1 KiB stores and one actual two-input Node grant.
The first two cases reuse at most twelve lost Grant replies and one lost Cancel
ACK, then reenter once after a real Core RLock is fully released. The inner
lane records actual local custody and loses one actual foreign-report ACK. No
receipt or owner state is fabricated to stand in for that progress.

The third case supplies a typed target-death reducer input after a real Cancel
inventory was saved but its pure report builder failed. It explicitly does not
claim a process died or physical bytes disappeared in these in-memory stores.
No process, thread, socket, GCS, timer, wait or user function runs.
"""

import queue

import pytest

from miniray import protocol
from miniray.core import _DelayedReadyTask, _LeaseCancellationState
from miniray.ownership import ObjectState
from tests.unit.test_ambiguous_grant_custody import (
    _GrantDelivery, _assert_terminal, _close, _collect_all, _drive_ready,
    _lose_four_lease_rounds, _next_ready,
)
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_local_replica_handoff import _Fixture


pytestmark = pytest.mark.unit


class _AfterOutermostUnlock:
    """Schedule one synchronous lane only after the actual RLock is free.

    This preserves the real Core lock and Condition lock identity. It observes
    composition boundaries, not source line numbers, and never permits the
    injected lane to run under the first lane's lock. The runtime tripwires
    separately forbid replacing this interleaving with real threads or waits.
    """

    def __init__(self, lock):
        self.lock = lock
        self.depth = 0
        self.callback = None
        self.skip = 0
        self.fired = 0

    def arm(self, callback, *, skip=0):
        assert self.callback is None and self.fired == 0
        self.callback, self.skip = callback, skip

    def acquire(self, *args, **kwargs):
        acquired = self.lock.acquire(*args, **kwargs)
        if acquired:
            self.depth += 1
        return acquired

    def release(self):
        assert self.depth > 0
        self.depth -= 1
        self.lock.release()
        if self.depth or self.callback is None:
            return
        if self.skip:
            self.skip -= 1
            return
        callback, self.callback = self.callback, None
        self.fired += 1
        assert self.fired == 1 and not self.lock._is_owned()
        callback()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_args):
        self.release()
        return False

    def __getattr__(self, name):
        return getattr(self.lock, name)

    def disarm(self):
        self.callback = None


def _record_local_calls(f, monkeypatch):
    actual = f.consumer._record_replica_custody_locked
    calls = []

    def record(descriptor, *, active_hold):
        assert descriptor == f.grant.dependencies[0]
        receipt = actual(descriptor, active_hold=active_hold)
        calls.append(receipt)
        assert len(calls) <= 2
        return receipt

    monkeypatch.setattr(f.consumer, "_record_replica_custody_locked", record)
    return calls


def _inner_handoff(f, stale_ready, seen):
    assert not seen
    assert not _drive_ready(f, stale_ready)
    state = f.marker().obligation
    assert len(state.local_receipts) == 1 and state.local_receipts[0].accepted
    assert state.receipts == () and state.cancellation_reply is not None
    assert state.cancellation_reply.retired_grant == f.grant
    assert state.terminal_error is stale_ready.cancellation.terminal_error
    assert len(f.reports) == 1 and f.reports[0][1].accepted
    f.assert_pending()
    f.assert_local_recorded()
    f.assert_foreign_recorded()
    seen.append(state)


def _take_two_queued_replays(f):
    count = f.consumer._submissions.qsize()
    assert 2 <= count <= 4
    replays = []
    for _ in range(count):
        item = f.consumer._submissions.get_nowait()
        f.consumer._submissions.task_done()
        if isinstance(item, _DelayedReadyTask):
            replays.append(item.ready)
    assert len(replays) == 2
    return replays


def test_validation_unlock_reentry_cannot_replace_new_handoff_with_old_cancel(monkeypatch):
    f = _Fixture(monkeypatch)
    unlock = None
    try:
        transport = _GrantDelivery(f, cancel_defects=("ack-loss",), expected_reports=2)
        assert not _lose_four_lease_rounds(f, transport)
        stale_ready = _next_ready(f)
        assert stale_ready.cancellation is not None and stale_ready.cancellation.reply is None
        assert len(transport.cancel_calls) == 1
        f.report_losses = 1
        local_calls = _record_local_calls(f, monkeypatch)
        unlock = _AfterOutermostUnlock(f.consumer._state_lock)
        monkeypatch.setattr(f.consumer, "_state_lock", unlock)
        actual_validate = f.consumer._validate_granted_dependencies
        armed, inner = [], []

        def validate(requested, grant):
            result = actual_validate(requested, grant)
            if not armed:
                assert requested == f.sources and grant == f.grant
                assert len(transport.cancel_calls) == 2
                armed.append(True)
                # Before the fix this unlock followed resume_handoff's read
                # but preceded its unconditional cancellation mark. The inner
                # real Location receipts were then overwritten by that mark.
                unlock.arm(lambda: _inner_handoff(f, stale_ready, inner))
            return result

        monkeypatch.setattr(f.consumer, "_validate_granted_dependencies", validate)
        assert _drive_ready(f, stale_ready)
        assert unlock.fired == 1 and len(inner) == 1
        latest = _assert_terminal(f, transport)
        assert len(local_calls) == 1 and latest.local_receipts == inner[0].local_receipts
        assert latest.cancellation_reply == inner[0].cancellation_reply
        assert latest.terminal_error is inner[0].terminal_error
        assert len(transport.lease_calls) == 12 and len(transport.cancel_calls) == 2
        assert len(f.reports) == 2 and f.reports[0][0] == f.reports[1][0]
        assert f.reports[1][1].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        queued = _next_ready(f)
        assert queued.location_state == inner[0] and queued.cancellation is None
        assert stale_ready.cancellation.reply is None
        _collect_all(f, transport)
    finally:
        if unlock is not None:
            unlock.disarm()
        _close(f)


def test_builder_failure_replay_unlock_preserves_saved_inventory_and_new_receipts(monkeypatch):
    f = _Fixture(monkeypatch)
    unlock = None
    try:
        transport = _GrantDelivery(f, cancel_defects=("ack-loss",), expected_reports=2)
        assert not _lose_four_lease_rounds(f, transport)
        stale_ready = _next_ready(f)
        assert stale_ready.cancellation is not None and stale_ready.cancellation.reply is None
        f.report_losses = 1
        local_calls = _record_local_calls(f, monkeypatch)
        unlock = _AfterOutermostUnlock(f.consumer._state_lock)
        monkeypatch.setattr(f.consumer, "_state_lock", unlock)
        actual_build = f.consumer._build_location_reports
        builds, inner = [], []

        def build(requested, grant, guards=()):
            reports = actual_build(requested, grant, guards)
            builds.append((requested, grant, guards))
            assert len(builds) <= 2
            if len(builds) == 1:
                marker = f.consumer._protocol_unresolved[f.pending.task_key]
                saved = marker.obligation
                assert type(saved) is _LeaseCancellationState
                assert saved.reply == transport.cancel_calls[-1][1]
                assert saved.reply.retired_grant == f.grant
                assert saved.terminal_error is transport.first_error
                # The exception path first performs one Node-death lookup.
                # Reenter after the following replay-selection lock releases.
                # Before the fix this was resume_handoff's check/mark gap.
                unlock.arm(lambda: _inner_handoff(f, stale_ready, inner), skip=1)
                raise RuntimeError("pure builder failed after inventory was retained")
            return reports

        monkeypatch.setattr(f.consumer, "_build_location_reports", build)
        assert not _drive_ready(f, stale_ready)
        assert unlock.fired == 1 and len(inner) == 1 and len(builds) == 2
        assert f.marker().obligation is inner[0]
        f.assert_pending()
        location_ready, cancellation_ready = sorted(
            _take_two_queued_replays(f), key=lambda ready: ready.cancellation is not None,
        )
        assert location_ready.location_state == inner[0] and location_ready.cancellation is None
        assert cancellation_ready.location_state is None and cancellation_ready.cancellation is not None
        saved = cancellation_ready.cancellation
        assert saved.reply == inner[0].cancellation_reply and saved.reply.retired_grant == f.grant
        assert saved.terminal_error is transport.first_error
        before = tuple(f.calls)
        assert _drive_ready(f, cancellation_ready)
        latest = _assert_terminal(f, transport)
        assert tuple(f.calls[:-1]) == before
        assert f.calls[-1][1] == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert f.calls[-1][2].inventory == f.inventory and len(transport.cancel_calls) == 2
        assert len(local_calls) == 1 and latest.local_receipts == inner[0].local_receipts
        assert latest.cancellation_reply == inner[0].cancellation_reply
        assert latest.terminal_error is inner[0].terminal_error
        assert len(f.reports) == 2 and f.reports[0][0] == f.reports[1][0]
        assert f.reports[1][1].status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        _collect_all(f, transport)
    finally:
        if unlock is not None:
            unlock.disarm()
        _close(f)


def test_target_death_after_saved_cancel_inventory_preserves_original_builder_error(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        core = f.consumer
        core._ready_tasks = queue.Queue()
        transport = _GrantDelivery(f, grant_success=True)
        actual_build = core._build_location_reports
        errors = (RuntimeError("original granted-inventory builder failure"),
                  RuntimeError("cancelled-inventory builder still unavailable"))
        builds = []

        def fail_build(requested, grant, guards=()):
            actual_build(requested, grant, guards)
            builds.append((requested, grant, guards))
            assert len(builds) <= 2
            raise errors[len(builds) - 1]

        monkeypatch.setattr(core, "_build_location_reports", fail_build)
        assert not f.execute()
        ready = _next_ready(f)
        saved = ready.cancellation
        assert type(saved) is _LeaseCancellationState
        assert saved.lease_request == f.request and saved.known_grant == f.grant
        assert saved.reply == transport.cancel_calls[0][1] and saved.reply.retired_grant == f.grant
        assert saved.terminal_error is errors[0] is transport.first_error
        marker = core._protocol_unresolved[f.pending.task_key]
        assert marker.target_node_id == saved.target_node_id == f.target.node_id
        assert marker.output_candidate.execution == f.pending.execution
        assert marker.output_candidate.lease_id == saved.request.lease_id == f.grant.lease_id
        assert len(builds) == 2 and len(transport.lease_calls) == len(transport.cancel_calls) == 1
        transport.assert_fenced()
        f.assert_pending()
        assert not f.reports and not f.states
        before = tuple(f.calls), tuple(builds), tuple(f.reports)
        death = protocol.NodeDeathRecord(
            "cancel-inventory-target-exit", f.target.node_id, 2902, 2, 3, -9,
            protocol.NodeDeathReason.PROCESS_EXIT, "typed target-death reducer input",
        )
        source = protocol.NodeInfo(
            f.source.node_id, 2901, 1, f.source_address,
            f.source.resource_ledger.total, f.source.resource_ledger.available,
        )
        core.handle_node_death(death, protocol.InstallClusterSnapshot(3, "cancel-target-removed", (source,)))
        assert core._dead_nodes[f.target.node_id] == death
        assert _drive_ready(f, ready)
        assert (tuple(f.calls), tuple(builds), tuple(f.reports)) == before
        result = core.owner_table.snapshot(f.pending.object_id)
        assert result.state is ObjectState.ERROR and result.error is errors[0]
        assert result.current_attempt == f.pending.spec.attempt_id
        assert f.pending.task_key not in core._protocol_unresolved
        assert core._ready_tasks.empty() and not f.states
        assert not getattr(core, "_location_handoff_drivers", set())
        record = core._recovery.task_record(f.pending.task_id)
        assert record.current_attempt == f.pending.spec.attempt_id and record.retries_started == 0
        assert core._recovery.active_recovery(f.pending.task_id) is None
        assert f.local_hold in core.owner_table.snapshot(f.local_id).submitted_tokens
        assert f.foreign.owner_table.has_retained_reference_for_task(f.foreign_id, f.foreign_hold)
        f.assert_local_unrecorded()
        assert f.foreign.owner_table.snapshot(f.foreign_id).locations == frozenset((f.source.node_id,))
        # A reducer input is not an actual process exit: preserve and inspect
        # these real fixture bytes rather than clearing the store as proof.
        assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        for descriptor, payload in zip(f.sources, f.payloads):
            assert f.target.object_store.get(descriptor.object_id) == payload
            assert f.target.object_store.snapshot(descriptor.object_id).pin_count == 0
        assert not transport.drops and not f.reports
        f.finish()
    finally:
        f.close()
