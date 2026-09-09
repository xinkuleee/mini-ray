"""Single-output admission preflight and exact old-effect retirement.

One threadless Core/Node, one 1 KiB store, two real child transfers and at
most 16 synchronous cleanup calls. Foreign renewal results are explicit
prerequisite doubles; publication, owner/recovery, retirement and GC are real.
No process, socket, producer execution, sleep or blocking wait is allowed.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import protocol
from miniray.core import CoreWorker, ObjectRef, _PendingTask, _WAKE_COORDINATOR
from miniray.errors import SystemTaskError
from miniray.foreign_lineage_runtime import ForeignLineageRenewalDisposition, ForeignLineageRenewalResult
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from tests.unit.test_core_output_publication import _fixture, _close


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("retirement contract attempted real runtime work")
    for owner, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)


def _drain_queue(core):
    records = []
    for _ in range(8):
        try:
            item = core._submissions.get_nowait()
        except queue.Empty:
            return tuple(records)
        records.append(item)
        core._submissions.task_done()
    pytest.fail("retirement fixture exceeded eight queue records")


class _Case:
    def __init__(self, *, refs=True):
        self.fixture, self.node, self.core, self.pending, reply, _calls, rpc = _fixture(refs=refs)
        core, pending = self.core, self.pending
        assert core._publish_reply(pending, reply, expected_node_id=self.node.node_id,
                                   expected_lease_id=self.fixture.id.lease_id)
        assert core._finish_pending_task(pending)
        assert all(item is _WAKE_COORDINATOR for item in _drain_queue(core))
        self.lost = pending.object_id
        assert core.owner_table.mark_lost(self.lost, pending.spec.attempt_id)
        # Core's real LOST transitions invalidate the readable route together
        # with owner locations (notify_node_dead and output Node-loss drive).
        # Keep canonical owner metadata and Node bytes for exact retirement.
        core._stored_descriptors.pop(self.lost)
        assert core.owner_table.snapshot(self.lost).canonical_stored_result is not None
        # This is an inert view of the existing real outer0 owner token.
        self.ref = ObjectRef(self.lost, core.worker_id)
        self.events, self.rpc_calls = [], []
        borrow = core._borrow_rpc

        def rpc_unlocked(address, handler, request):
            assert not core._state_lock._is_owned(), handler
            self.rpc_calls.append((handler, request))
            assert len(self.rpc_calls) <= 16
            return rpc(address, handler, request)

        def borrow_unlocked(address, handler, request):
            assert not core._state_lock._is_owned(), handler
            assert handler == "release_contained_reference"
            self.rpc_calls.append((handler, request))
            assert len(self.rpc_calls) <= 16
            return borrow(address, handler, request)

        core._rpc, core._borrow_rpc = rpc_unlocked, borrow_unlocked

    def start(self):
        return self.core._start_or_join_reconstruction(
            self.lost, self.core._objects[self.lost], return_requested_outcome=True,
        )

    def metadata(self):
        core, fixture = self.core, self.fixture
        return (
            core.owner_table.snapshot(self.lost), dict(core._stored_descriptors),
            tuple((transfer, fixture.child_owners[transfer.contained_owner_worker_id]
                   .snapshot(transfer.contained_object_id)) for transfer in fixture.manifest.slots[0].transfers),
            fixture.handoffs.query(fixture.id), fixture.journal.snapshot(fixture.id),
            fixture.store.used_bytes,
        )

    def record(self):
        return replace(self.core._recovery.task_record(self.pending.task_id))

    def assert_not_started(self, record):
        core = self.core
        assert self.record() == record and record.state is TaskState.SUCCEEDED
        assert core._recovery.active_recovery(self.pending.task_id) is None
        assert core._accepted_task_count == 0
        assert not core._reconstruction_coordinator()._sessions
        assert not any(isinstance(item, _PendingTask) for item in tuple(core._submissions.queue))

    @contextmanager
    def foreign(self, monkeypatch, drive, *, validate=None, complete=None):
        # Only prerequisite replies are doubled. Preserve real registry/runtime
        # identity and restore them before real owner/recovery/child GC.
        registry, runtime = self.core._foreign_lineage_registry, self.core._foreign_lineage_runtime
        def forbidden(*_args):
            pytest.fail("deferred admission reached a commit-only foreign operation")
        with monkeypatch.context() as patch:
            patch.setattr(registry, "snapshot", lambda task: SimpleNamespace(edges=())
                          if task == self.pending.task_id else None)
            patch.setattr(runtime, "drive_renewal", drive)
            patch.setattr(runtime, "validate_renewal_ready", forbidden if validate is None else validate)
            patch.setattr(runtime, "complete_renewal", forbidden if complete is None else complete)
            yield
        assert self.core._foreign_lineage_registry is registry
        assert self.core._foreign_lineage_runtime is runtime

    def result(self, task_id, attempt_id, disposition):
        assert task_id == self.pending.task_id and attempt_id == self.pending.spec.attempt_id.next()
        return ForeignLineageRenewalResult(task_id, attempt_id, disposition, (), 0)

    def no_retirement(self, monkeypatch):
        def unexpected(_object_id):
            pytest.fail("inadmissible START retired old output membership")
        monkeypatch.setattr(self.core, "_retire_lost_output_memberships", unexpected)

    def assert_collected(self):
        core, fixture = self.core, self.fixture
        assert core.owner_table.release_local_reference(self.lost, "outer0")
        core._reference_released(self.lost)
        assert core.owner_table.collection_state(self.lost) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.contains(self.lost)
        assert core._recovery.lineage_for_object(self.lost) is None
        assert not core._objects and not core._stored_descriptors
        assert not core._object_gc_obligations and not getattr(core, "_output_retirement_work", {})
        assert not core.owner_table.has_active_output_retirements()
        fixture.assert_no_pins_or_bytes()
        for transfer in fixture.manifest.slots[0].transfers:
            assert fixture.child_owners[transfer.contained_owner_worker_id].snapshot(
                transfer.contained_object_id).local_tokens == frozenset(("source-live",))
        assert not core._foreign_lineage_runtime.has_pending_obligations()

    def close(self):
        _close(self.core)


def test_waiting_foreign_renewal_keeps_old_membership_children_and_bytes(monkeypatch):
    case = _Case()
    before, record = case.metadata(), case.record()
    case.no_retirement(monkeypatch)
    def drive(task, attempt):
        assert not case.core._state_lock._is_owned()
        case.events.append("renew")
        return case.result(task, attempt, ForeignLineageRenewalDisposition.WAITING)
    try:
        with case.foreign(monkeypatch, drive):
            assert case.start() is None
        assert case.events == ["renew"]
        assert case.metadata() == before and case.rpc_calls == []
        case.assert_not_started(record)
        assert _drain_queue(case.core) == ()
    finally:
        case.close()


def test_finish_barrier_arriving_during_renewal_precedes_any_retirement(monkeypatch):
    case = _Case()
    core = case.core
    before, record = case.metadata(), case.record()
    case.no_retirement(monkeypatch)
    def drive(task, attempt):
        assert not core._state_lock._is_owned()
        with core._state_lock:
            core._install_task_finish_barrier_locked(case.pending)
        case.events.append("finish-arrived")
        return case.result(task, attempt, ForeignLineageRenewalDisposition.READY)
    try:
        with case.foreign(monkeypatch, drive):
            assert case.start() is None
        assert case.events == ["finish-arrived"]
        assert core._task_finish_barriers[case.lost] is case.pending
        assert case.metadata() == before and case.rpc_calls == []
        case.assert_not_started(record)
    finally:
        case.close()


def test_exhausted_budget_is_rejected_before_old_output_or_foreign_effects(monkeypatch):
    case = _Case()
    current = case.core._recovery.task_record(case.pending.task_id)
    current.retries_started = current.max_retries
    before, record = case.metadata(), case.record()
    case.no_retirement(monkeypatch)
    def never_renew(*_args):
        pytest.fail("exhausted START began a foreign hold exchange")
    try:
        with case.foreign(monkeypatch, never_renew):
            with pytest.raises(SystemTaskError, match="budget is exhausted"):
                case.start()
        assert case.metadata() == before and case.rpc_calls == []
        case.assert_not_started(record)
    finally:
        case.close()


def test_renewal_revoked_during_real_retirement_prevents_owner_attempt_commit(monkeypatch):
    case = _Case()
    core, record = case.core, case.record()
    retire = core._retire_lost_output_memberships
    revoked = False
    def drive(task, attempt):
        assert not core._state_lock._is_owned()
        case.events.append("renew")
        return case.result(task, attempt, ForeignLineageRenewalDisposition.READY)
    def validate(task, attempt):
        case.result(task, attempt, ForeignLineageRenewalDisposition.READY)
        case.events.append("validate-revoked" if revoked else "validate-ready")
        if revoked:
            raise SystemTaskError("foreign input hold revoked during retirement")
    def retire_then_revoke(object_id):
        nonlocal revoked
        assert not core._state_lock._is_owned()
        assert case.events[-1] == "validate-ready"
        result = retire(object_id)
        assert result and object_id == case.lost
        case.events.append("retired")
        revoked = True
        return result
    monkeypatch.setattr(core, "_retire_lost_output_memberships", retire_then_revoke)
    try:
        with case.foreign(monkeypatch, drive, validate=validate):
            with pytest.raises(SystemTaskError, match="revoked during retirement"):
                case.start()
        assert case.events == ["renew", "validate-ready", "retired", "validate-revoked"]
        lost = core.owner_table.snapshot(case.lost)
        assert lost.state is ObjectState.LOST and lost.current_attempt == case.pending.spec.attempt_id
        assert lost.output_publication is None and lost.output_retirement_id is None
        assert len(case.rpc_calls) == 3
        case.fixture.assert_no_pins_or_bytes()
        case.assert_not_started(record)
        case.assert_collected()
    finally:
        case.close()


def test_finish_barrier_arriving_during_real_retirement_prevents_attempt_commit(monkeypatch):
    case = _Case()
    core, record = case.core, case.record()
    rpc = core._rpc
    injected = []
    def finish_during_drop(address, handler, request):
        result = rpc(address, handler, request)
        if handler == "drop_object_replica":
            assert not injected
            with core._state_lock:
                core._install_task_finish_barrier_locked(case.pending)
            injected.append(request)
        return result
    monkeypatch.setattr(core, "_rpc", finish_during_drop)
    try:
        assert case.start() is None
        assert len(injected) == 1
        assert core._task_finish_barriers == {case.lost: case.pending}
        assert core.owner_table.snapshot(case.lost).output_publication is None
        case.fixture.assert_no_pins_or_bytes()
        case.assert_not_started(record)
        calls = tuple(case.rpc_calls)
        assert case.start() is None and tuple(case.rpc_calls) == calls
        case.assert_not_started(record)
    finally:
        case.close()


@pytest.mark.parametrize("prerequisite", ("budget", "foreign-failed"))
def test_failed_admission_keeps_live_output_until_real_retirement_and_gc(monkeypatch, prerequisite):
    case = _Case()
    core = case.core
    if prerequisite == "budget":
        current = core._recovery.task_record(case.pending.task_id)
        current.retries_started = current.max_retries
    before, record = case.metadata(), case.record()
    renewal_calls = []
    def failed_renewal(task, attempt):
        assert not core._state_lock._is_owned()
        renewal_calls.append((task, attempt))
        return replace(case.result(task, attempt, ForeignLineageRenewalDisposition.FAILED),
                       failure="foreign prerequisite definitively failed")
    try:
        with case.foreign(monkeypatch, failed_renewal):
            with pytest.raises(SystemTaskError):
                case.start()
        assert len(renewal_calls) == int(prerequisite == "foreign-failed")
        assert case.metadata() == before and case.rpc_calls == []
        case.assert_not_started(record)
        # Ordinary admission reports the failure and retains the live LOST
        # object. It has no targeted OPEN failure/error-publication latch.
        # Explicit old-effect retirement uses the same real cleanup authority
        # as a subsequent reconstruction; final reference release runs GC.
        assert core._retire_lost_output_memberships(case.lost)
        retired = core.owner_table.snapshot(case.lost)
        assert retired.state is ObjectState.LOST and retired.error is None
        assert retired.current_attempt == case.pending.spec.attempt_id
        assert retired.local_tokens == frozenset(("outer0",))
        assert retired.output_publication is None and retired.output_retirement_id is None
        assert not retired.outgoing_contained_edges and retired.canonical_stored_result is None
        assert not retired.locations
        case.assert_not_started(record)
        assert len(case.rpc_calls) == 3
        case.assert_collected()
    finally:
        case.close()


def test_lost_retirement_ack_keeps_admission_fenced_until_exact_cleanup_replays(monkeypatch):
    case = _Case()
    core, record = case.core, case.record()
    original = core._rpc
    drops, statuses = [], []
    original_member = core.owner_table.snapshot(case.lost).output_publication
    def lose_drop_ack(address, handler, request):
        result = original(address, handler, request)
        if handler == "drop_object_replica":
            drops.append(request)
            statuses.append(result.status)
            if len(drops) == 1:
                raise TimeoutError("physical retirement applied; ACK lost")
        return result
    monkeypatch.setattr(core, "_rpc", lose_drop_ack)
    try:
        assert case.start() is None
        unresolved = core.owner_table.snapshot(case.lost)
        assert unresolved.state is ObjectState.LOST and unresolved.error is None
        assert unresolved.output_publication == original_member
        assert unresolved.output_retirement_id is not None
        plan = core._output_retirement_work[case.lost]["plan"]
        assert plan.retirement_id == unresolved.output_retirement_id
        assert len(core._output_retirement_work[case.lost]["child"]) == 2
        assert not core._output_retirement_work[case.lost]["replica"]
        case.fixture.assert_no_pins_or_bytes()
        case.assert_not_started(record)
        calls = tuple(case.rpc_calls)
        with pytest.raises(TimeoutError, match="retirement did not finish"):
            core.get(case.ref, timeout=0)
        assert tuple(case.rpc_calls) == calls
        case.assert_not_started(record)
        assert core._retire_lost_output_memberships(case.lost)
        receipt = core.owner_table.output_publication_retirement_receipt(plan)
        assert receipt.plan == plan
        assert drops == [drops[0]] * 2
        assert statuses == [protocol.DropObjectReplicaStatus.DROPPED,
                            protocol.DropObjectReplicaStatus.ALREADY_DROPPED]
        assert sum(handler == "release_contained_reference" for handler, _ in case.rpc_calls) == 2
        assert len(case.rpc_calls) == 4
        case.assert_not_started(record)
        case.assert_collected()
    finally:
        case.close()
