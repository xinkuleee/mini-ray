"""Pure targeted START admission before irreversible old-output retirement.

One in-memory Node/store (at most 4 KiB), two logical tasks and three output
slots total (at most two selected per publication), four contained transfers,
bounded synchronous renewal callbacks and at most 32 RPC reducer calls per
case.  Real unified publication/ownership/graph/retirement methods
run underneath fake transport.  No process, thread, socket, user function,
sleep, blocking wait or background queue consumption is permitted.
"""

from __future__ import annotations

import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import (
    CoreWorker, _DelayedTargetedReconstruction, _PendingTask,
    _StartTargetedReconstruction,
)
from miniray.errors import SystemTaskError
from miniray.foreign_lineage_runtime import (
    ForeignLineageRenewalDisposition, ForeignLineageRenewalResult,
)
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.targeted_reconstruction import TargetedSessionPhase
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture
from tests.unit.test_task_finish_barrier import _Fixture as _StoredFixture


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("targeted-retirement contract attempted real runtime work")

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
    for _ in range(64):
        try:
            item = core._submissions.get_nowait()
        except queue.Empty:
            return tuple(records)
        records.append(item)
        core._submissions.task_done()
    pytest.fail("targeted fixture exceeded 64 in-memory queue records")


class _Case:
    def __init__(self, *, refs=True):
        self.fixture, self.node, self.core, self.pending, reply, _calls, rpc = _fixture(refs=refs)
        core, pending = self.core, self.pending
        assert core._publish_reply(
            pending, reply, expected_node_id=self.node.node_id,
            expected_lease_id=self.fixture.id.lease_id,
        )
        assert core._finish_pending_task(pending)
        assert not any(isinstance(item, _PendingTask) for item in _drain_queue(core))
        self.healthy, self.lost = pending.output_ids
        assert core.owner_table.mark_lost(self.lost, pending.spec.attempt_id)
        self.targeted = core._targeted_reconstruction_coordinator()
        with core._state_lock:
            opened = self.targeted.request(self.lost, pending.spec.attempt_id)
        assert opened.session.phase is TargetedSessionPhase.OPEN
        assert opened.session.target_output_ids == (self.lost,)
        self.events = []
        self.rpc_calls = []
        borrow = core._borrow_rpc

        def rpc_unlocked(address, handler, request):
            assert not core._state_lock._is_owned(), handler
            self.rpc_calls.append((handler, request))
            assert len(self.rpc_calls) <= 32
            return rpc(address, handler, request)

        def borrow_unlocked(address, handler, request):
            assert not core._state_lock._is_owned(), handler
            if handler != "release_contained_reference":
                pytest.fail("unexpected targeted retirement owner RPC")
            assert isinstance(request, protocol.ReleaseContainedReference)
            self.rpc_calls.append((handler, request))
            assert len(self.rpc_calls) <= 32
            return borrow(address, handler, request)

        core._rpc = rpc_unlocked
        core._borrow_rpc = borrow_unlocked

    def metadata(self):
        core, fixture = self.core, self.fixture
        child_keys = {
            (transfer.contained_owner_worker_id, transfer.contained_object_id)
            for slot in fixture.manifest.slots for transfer in slot.transfers
        }
        return (
            tuple(core.owner_table.snapshot(value) for value in self.pending.output_ids),
            dict(core._stored_descriptors),
            fixture.graph.snapshot(),
            tuple((owner, child, fixture.child_owners[owner].snapshot(child))
                  for owner, child in sorted(child_keys)),
            fixture.recovery.snapshot(fixture.id),
            fixture.journal.snapshot(fixture.id),
            fixture.store.used_bytes,
        )

    def record(self):
        return replace(self.core._recovery.task_record(self.pending.task_id))

    def assert_not_started(self, record):
        core = self.core
        assert core._recovery.task_record(self.pending.task_id) == record
        assert record.state is TaskState.SUCCEEDED
        assert core._recovery.active_recovery(self.pending.task_id) is None
        assert core._accepted_task_count == 0
        session = self.targeted.current_session(self.pending.task_id)
        assert session is not None and session.phase is TargetedSessionPhase.OPEN
        assert not any(isinstance(item, _PendingTask) for item in tuple(core._submissions.queue))

    def foreign(self, drive, *, validate=None, complete=None):
        task_id = self.pending.task_id
        self.core._foreign_lineage_registry = SimpleNamespace(
            snapshot=lambda task: SimpleNamespace(edges=()) if task == task_id else None
        )

        def forbidden(*_args):
            pytest.fail("deferred targeted admission reached a commit-only foreign operation")

        self.core._foreign_lineage_runtime = SimpleNamespace(
            drive_renewal=drive,
            validate_renewal_ready=forbidden if validate is None else validate,
            complete_renewal=forbidden if complete is None else complete,
        )

    def result(self, task_id, attempt_id, disposition):
        assert task_id == self.pending.task_id
        assert attempt_id == self.pending.spec.attempt_id.next()
        return ForeignLineageRenewalResult(task_id, attempt_id, disposition, (), 0)

    def no_retirement(self, monkeypatch):
        def unexpected(_object_id):
            pytest.fail("inadmissible targeted START retired old output membership")
        monkeypatch.setattr(self.core, "_retire_lost_output_memberships", unexpected)

    def close(self):
        # No local ObjectRef is created by this fixture: release its two exact
        # fixture tokens without executing queued GC or changing retry state.
        for object_id in tuple(self.core._objects):
            for token in tuple(self.core.owner_table.snapshot(object_id).local_tokens):
                self.core.owner_table.release_local_reference(object_id, token)
        close_pure_core(self.core)


def test_waiting_foreign_renewal_keeps_old_membership_graph_and_healthy_sibling(monkeypatch):
    case = _Case(refs=True)
    core = case.core
    before, record = case.metadata(), case.record()
    case.no_retirement(monkeypatch)

    def drive(task, attempt):
        assert not core._state_lock._is_owned()
        case.events.append("renew")
        return case.result(task, attempt, ForeignLineageRenewalDisposition.WAITING)

    case.foreign(drive)
    try:
        core._start_open_targeted_reconstruction(case.pending.task_id)
        assert case.events == ["renew"]
        assert case.metadata() == before
        assert case.rpc_calls == []
        case.assert_not_started(record)
        records = _drain_queue(core)
        assert len(records) == 1 and isinstance(records[0], _DelayedTargetedReconstruction)
        assert records[0].event == _StartTargetedReconstruction(case.pending.task_id, 1)
    finally:
        case.close()


def test_finish_barrier_arriving_during_renewal_precedes_any_retirement(monkeypatch):
    case = _Case(refs=True)
    core = case.core
    before, record = case.metadata(), case.record()
    case.no_retirement(monkeypatch)

    def drive(task, attempt):
        assert not core._state_lock._is_owned()
        # One deterministic concurrent-history hook, not a fake owner result:
        # old logical execution has not yet released its last finish hold.
        with core._state_lock:
            core._task_finish_barriers[case.lost] = case.pending
        case.events.append("finish-arrived")
        return case.result(task, attempt, ForeignLineageRenewalDisposition.READY)

    case.foreign(drive)
    try:
        core._start_open_targeted_reconstruction(case.pending.task_id)
        assert case.events == ["finish-arrived"]
        assert core._task_finish_barriers[case.lost] is case.pending
        assert case.metadata() == before
        assert case.rpc_calls == []
        case.assert_not_started(record)
    finally:
        case.close()


def test_exhausted_budget_is_rejected_before_old_output_or_foreign_effects(monkeypatch):
    case = _Case(refs=True)
    core = case.core
    current = core._recovery.task_record(case.pending.task_id)
    # This fixture uses attempt #3; an exhausted 3-retry history is valid.
    current.retries_started = current.max_retries
    before, record = case.metadata(), case.record()
    case.no_retirement(monkeypatch)

    def never_renew(*_args):
        pytest.fail("exhausted targeted START began a foreign hold exchange")

    case.foreign(never_renew)
    try:
        with pytest.raises(SystemTaskError):
            core._start_open_targeted_reconstruction(case.pending.task_id)
        assert case.metadata() == before
        assert case.rpc_calls == []
        case.assert_not_started(record)
    finally:
        case.close()


def test_renewal_revoked_during_real_retirement_prevents_owner_attempt_commit(monkeypatch):
    case = _Case(refs=True)
    core = case.core
    record = case.record()
    healthy = core.owner_table.snapshot(case.healthy)
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
        assert case.events and case.events[-1] == "validate-ready"
        result = retire(object_id)
        assert result and object_id == case.lost
        case.events.append("retired")
        revoked = True
        return result

    case.foreign(drive, validate=validate)
    monkeypatch.setattr(core, "_retire_lost_output_memberships", retire_then_revoke)
    try:
        with pytest.raises(SystemTaskError, match="revoked during retirement"):
            core._start_open_targeted_reconstruction(case.pending.task_id)
        assert case.events[0] == "renew"
        assert case.events[-2:] == ["retired", "validate-revoked"]
        assert case.events[1:-2] and set(case.events[1:-2]) == {"validate-ready"}
        assert core.owner_table.snapshot(case.healthy) == healthy
        lost = core.owner_table.snapshot(case.lost)
        assert lost.state is ObjectState.LOST
        assert lost.current_attempt == case.pending.spec.attempt_id
        assert lost.output_publication is None
        assert case.rpc_calls, "the revocation hook must follow real retirement effects"
        case.assert_not_started(record)
    finally:
        case.close()


def _lose_inline_history_with_finish_gate(case):
    """Inject a missing INLINE payload without pretending mark_lost supports it.

    This adversarial history leaves the old membership alive behind a logical
    finish gate.  It specifically checks that another slot's helper cannot
    expand its cleanup authority to all LOST siblings.
    """
    core = case.core
    with core._state_lock:
        entry = core.owner_table._entries[case.healthy]
        assert entry.state is ObjectState.READY_INLINE
        entry.state = ObjectState.LOST
        entry.inline_data = None
        core._task_finish_barriers[case.healthy] = case.pending
    return core.owner_table.snapshot(case.healthy)


def test_retiring_stored_target_does_not_touch_lost_inline_sibling_behind_finish_gate():
    case = _Case(refs=True)
    core = case.core
    sibling = _lose_inline_history_with_finish_gate(case)
    sibling_edges = frozenset(case.fixture.manifest.slots[0].edges)
    child_holds = tuple(
        (transfer, case.fixture.child_owners[transfer.contained_owner_worker_id]
         .snapshot(transfer.contained_object_id).contained_holds)
        for transfer in case.fixture.manifest.slots[0].transfers
    )
    record = case.record()
    try:
        assert core._retire_lost_output_memberships(case.lost)
        assert core.owner_table.snapshot(case.healthy) == sibling
        assert frozenset(case.fixture.graph.snapshot().committed_edges) == sibling_edges
        for transfer, prior in child_holds:
            current = case.fixture.child_owners[transfer.contained_owner_worker_id].snapshot(
                transfer.contained_object_id
            )
            assert transfer.final_hold in prior
            assert transfer.final_hold in current.contained_holds
        drops = [request for handler, request in case.rpc_calls if handler == "drop_object_replica"]
        assert len(drops) == 1 and drops[0].object_id == case.lost
        reports = [request for handler, request in case.rpc_calls
                   if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER]
        assert len(reports) == 1 and reports[0].proof.object_id == case.lost
        assert core.owner_table.snapshot(case.lost).output_publication is None
        assert core._task_finish_barriers[case.healthy] is case.pending
        case.assert_not_started(record)
    finally:
        case.close()


def test_target_merged_during_retirement_is_repreflighted_before_any_attempt_advance(monkeypatch):
    case = _Case(refs=True)
    core = case.core
    record = case.record()
    rpc = core._rpc
    injected = []
    previews = []
    preview = case.targeted.preview_start

    def observed_preview(task_id):
        current = case.targeted.current_session(task_id)
        previews.append(current.target_output_ids)
        return preview(task_id)

    def merge_during_rpc(address, handler, request):
        assert not core._state_lock._is_owned()
        if not injected and handler == "release_contained_graph_container":
            sibling = _lose_inline_history_with_finish_gate(case)
            with core._state_lock:
                merged = case.targeted.request(case.healthy, case.pending.spec.attempt_id)
            assert merged.session.target_output_ids == case.pending.output_ids
            injected.append(sibling)
        return rpc(address, handler, request)

    monkeypatch.setattr(case.targeted, "preview_start", observed_preview)
    monkeypatch.setattr(core, "_rpc", merge_during_rpc)
    try:
        core._start_open_targeted_reconstruction(case.pending.task_id)
        assert len(injected) == 1
        assert previews and previews[0] == (case.lost,)
        # A changed target set must either be deferred explicitly or re-enter
        # preflight.  A delayed event is the same semantic wake with backoff,
        # so a finish gate need not make the coordinator spin.  Neither path
        # may silently commit the old one-slot proposal.
        queued = tuple(core._submissions.queue)
        events = tuple(
            item.event if isinstance(item, _DelayedTargetedReconstruction) else item
            for item in queued
        )
        assert any(isinstance(event, _StartTargetedReconstruction)
                   and event.task_id == case.pending.task_id for event in events) or (
            case.pending.output_ids in previews
        )
        assert case.targeted.current_session(case.pending.task_id).target_output_ids == case.pending.output_ids
        assert core.owner_table.snapshot(case.healthy) == injected[0]
        assert core.owner_table.snapshot(case.lost).current_attempt == case.pending.spec.attempt_id
        assert core.owner_table.snapshot(case.lost).output_publication is None
        case.assert_not_started(record)
        # Finish still blocks the newly merged slot on the next explicit turn.
        core._start_open_targeted_reconstruction(case.pending.task_id)
        assert core.owner_table.snapshot(case.healthy) == injected[0]
        case.assert_not_started(record)
    finally:
        case.close()


def _assert_failed_without_start(case, error, record, healthy):
    core = case.core
    failed = core.owner_table.snapshot(case.lost)
    assert failed.state is ObjectState.ERROR
    # Retired-output snapshots are isolated deep copies, not aliases of the
    # owner authority. The published choice stays exact inside that authority.
    assert type(failed.error) is type(error) and failed.error.args == error.args
    assert core.owner_table._entries[case.lost].error is error
    assert failed.current_attempt == case.pending.spec.attempt_id
    assert failed.output_publication is None
    assert failed.output_retirement_id is None
    assert failed.canonical_stored_result is None and not failed.locations
    assert not failed.outgoing_contained_edges
    assert case.lost not in core._stored_descriptors
    assert core._objects[case.lost].event.is_set()
    assert core.owner_table.snapshot(case.healthy) == healthy
    assert core._recovery.task_record(case.pending.task_id) == record
    assert core._recovery.active_recovery(case.pending.task_id) is None
    assert core._accepted_task_count == 0
    assert case.targeted.current_session(case.pending.task_id) is None
    assert case.targeted.open_failure(case.pending.task_id) is None
    assert not any(isinstance(item, _PendingTask) for item in tuple(core._submissions.queue))
    assert not core.owner_table.has_active_output_retirements()
    assert not getattr(core, "_output_retirement_work", {})
    assert not case.fixture.store.contains(case.lost, sealed_only=False)


def _collect_failed_target_then_healthy_sibling(case, healthy):
    """Close exact fixture handles and execute real per-slot owner GC.

    Child sources are intentionally still live.  Only this publication's
    provisional/final holds and bytes must vanish; collecting child owners'
    unrelated source tokens would weaken the test.
    """
    core, fixture = case.core, case.fixture
    lost_slot, healthy_slot = fixture.manifest.slots[1], fixture.manifest.slots[0]
    assert fixture.store.used_bytes == 0
    assert frozenset(fixture.graph.snapshot().committed_edges) == frozenset(healthy_slot.edges)
    for transfer in lost_slot.transfers:
        child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(
            transfer.contained_object_id
        )
        assert transfer.final_hold not in child.contained_holds
        assert transfer.provisional_hold not in child.contained_holds
    for transfer in healthy_slot.transfers:
        assert transfer.final_hold in (
            fixture.child_owners[transfer.contained_owner_worker_id]
            .snapshot(transfer.contained_object_id).contained_holds
        )

    # These are the actual owner tokens installed by _fixture, not inert refs.
    # Their synchronous release is the same owner authority that close uses.
    assert core.owner_table.release_local_reference(case.lost, "outer1")
    core._reference_released(case.lost)
    assert core.owner_table.collection_state(case.lost) is ObjectCollectionState.COLLECTED
    assert not core.owner_table.contains(case.lost)
    assert case.lost not in core._objects
    assert core._recovery.lineage_for_object(case.lost) is None
    assert core._recovery.lineage_for_object(case.healthy) is not None
    assert core.owner_table.snapshot(case.healthy) == healthy
    assert frozenset(fixture.graph.snapshot().committed_edges) == frozenset(healthy_slot.edges)
    assert not core._object_gc_obligations

    assert core.owner_table.release_local_reference(case.healthy, "outer0")
    core._reference_released(case.healthy)
    assert core.owner_table.collection_state(case.healthy) is ObjectCollectionState.COLLECTED
    assert not core.owner_table.contains(case.healthy)
    assert core._recovery.lineage_for_object(case.healthy) is None
    assert not core._objects and not core._stored_descriptors
    assert not core._object_gc_obligations
    assert not getattr(core, "_output_retirement_work", {})
    assert not core.owner_table.has_active_output_retirements()
    assert not fixture.graph.snapshot().committed_edges
    assert not fixture.graph.snapshot().prepared_edges
    assert fixture.store.used_bytes == 0
    assert not fixture.journal.snapshot(fixture.id).retained_result_slots
    for slot in fixture.manifest.slots:
        for transfer in slot.transfers:
            child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(
                transfer.contained_object_id
            )
            assert transfer.final_hold not in child.contained_holds
            assert transfer.provisional_hold not in child.contained_holds
    assert tuple(proof.slot_index for proof in fixture.recovery.snapshot(fixture.id).slot_collections) == (0, 1)
    assert not core._foreign_lineage_runtime.has_pending_obligations()


@pytest.mark.parametrize("prerequisite", ("budget", "foreign-failed"))
def test_terminal_open_failure_retires_selected_membership_before_error_and_gc(monkeypatch, prerequisite):
    case = _Case(refs=True)
    core = case.core
    healthy = core.owner_table.snapshot(case.healthy)
    if prerequisite == "budget":
        current = core._recovery.task_record(case.pending.task_id)
        current.retries_started = current.max_retries
    before, record = case.metadata(), case.record()
    original_registry = core._foreign_lineage_registry
    original_runtime = core._foreign_lineage_runtime
    assert original_registry.snapshot(case.pending.task_id) is None
    renewal_calls = []

    def failed_renewal(task_id, attempt):
        assert not core._state_lock._is_owned()
        renewal_calls.append((task_id, attempt))
        return replace(case.result(task_id, attempt, ForeignLineageRenewalDisposition.FAILED),
                       failure="foreign prerequisite definitively failed")

    try:
        with monkeypatch.context() as patch:
            if prerequisite == "foreign-failed":
                # This is a prerequisite-result double, not a claim to cover
                # a real foreign input hold.  Restore the real empty registry
                # before retirement and GC so collection uses real authorities.
                patch.setattr(original_registry, "snapshot",
                              lambda task: SimpleNamespace(edges=())
                              if task == case.pending.task_id else None)
                patch.setattr(original_runtime, "drive_renewal", failed_renewal)
            with pytest.raises(SystemTaskError) as raised:
                core._start_open_targeted_reconstruction(case.pending.task_id)
        error = raised.value
        assert len(renewal_calls) == (1 if prerequisite == "foreign-failed" else 0)
        assert case.metadata() == before
        assert case.rpc_calls == []
        case.assert_not_started(record)
        assert core._foreign_lineage_registry is original_registry
        assert original_registry.snapshot(case.pending.task_id) is None

        core._fail_open_targeted_reconstruction(case.pending.task_id, error)
        _assert_failed_without_start(case, error, record, healthy)
        drops = [request for handler, request in case.rpc_calls if handler == "drop_object_replica"]
        assert len(drops) == 1 and drops[0].object_id == case.lost
        _collect_failed_target_then_healthy_sibling(case, healthy)
    finally:
        case.close()


def test_lost_terminal_retirement_ack_keeps_failure_latched_until_same_cleanup_replays(monkeypatch):
    case = _Case(refs=True)
    core = case.core
    healthy = core.owner_table.snapshot(case.healthy)
    current = core._recovery.task_record(case.pending.task_id)
    current.retries_started = current.max_retries
    record = case.record()
    original_member = core.owner_table.snapshot(case.lost).output_publication
    original_rpc = core._rpc
    report_requests = []
    lost_ack = False

    def report_then_lose_ack(address, handler, request):
        nonlocal lost_ack
        result = original_rpc(address, handler, request)
        if (handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
                and type(request) is wire.ReportOutputPublicationSlotCollected
                and request.proof.object_id == case.lost):
            report_requests.append(request)
            assert len(report_requests) <= 2
            if not lost_ack:
                lost_ack = True
                raise TimeoutError("slot retirement report applied; ACK lost")
        return result

    def no_forward_work(*_args, **_kwargs):
        pytest.fail("latched OPEN failure re-entered START/renewal/execute")

    monkeypatch.setattr(core, "_rpc", report_then_lose_ack)
    try:
        with pytest.raises(SystemTaskError) as raised:
            core._start_open_targeted_reconstruction(case.pending.task_id)
        error = raised.value
        core._fail_open_targeted_reconstruction(case.pending.task_id, error)
        assert lost_ack and len(report_requests) == 1
        assert case.targeted.open_failure(case.pending.task_id) is error
        unresolved = core.owner_table.snapshot(case.lost)
        assert unresolved.state is ObjectState.LOST
        assert unresolved.output_publication == original_member
        assert unresolved.canonical_stored_result is not None
        assert unresolved.output_retirement_id is not None
        assert case.lost in core._output_retirement_work
        plan = core._output_retirement_work[case.lost]["plan"]
        assert plan.retirement_id == unresolved.output_retirement_id
        assert report_requests[0].proof.cleanup_id == plan.retirement_id
        assert core.owner_table.has_active_output_retirements()
        assert core.owner_table.snapshot(case.healthy) == healthy
        case.assert_not_started(record)
        acked_proofs = case.fixture.recovery.snapshot(case.fixture.id).slot_collections
        assert acked_proofs == (report_requests[0].proof,)
        assert case.fixture.store.used_bytes == 0

        # Do not fake a renewed prerequisite into success: replay of the public
        # admission entry must follow the latched failure before preview/renew.
        monkeypatch.setattr(case.targeted, "preview_start", no_forward_work)
        monkeypatch.setattr(core._foreign_lineage_runtime, "drive_renewal", no_forward_work)
        monkeypatch.setattr(core._foreign_lineage_runtime, "complete_renewal", no_forward_work)
        monkeypatch.setattr(core, "_execute", no_forward_work)
        assert core._start_open_targeted_reconstruction(case.pending.task_id) is None
        assert len(report_requests) == 2 and report_requests[1] == report_requests[0]
        assert report_requests[1].proof.cleanup_id == plan.retirement_id
        _assert_failed_without_start(case, error, record, healthy)
        # Child/graph/replica ACKs from the first pass are never repeated; only
        # the uncertain, exact GCS report must be replayed on the second pass.
        drops = [request for handler, request in case.rpc_calls if handler == "drop_object_replica"]
        releases = [request for handler, request in case.rpc_calls
                    if handler == "release_contained_reference"]
        assert len(drops) == 1
        assert len(releases) == len(case.fixture.manifest.slots[1].transfers)
        _collect_failed_target_then_healthy_sibling(case, healthy)
    finally:
        case.close()


def _drop_one_stored_slot(fixture, pending, object_id):
    """Lose one real stored replica, preserving sibling bytes and lineage."""
    core = fixture.core
    snapshot = core.owner_table.snapshot(object_id)
    descriptor = snapshot.canonical_stored_result
    assert snapshot.state is ObjectState.READY_STORED and descriptor is not None
    assert snapshot.output_publication.publication_id.execution == pending.execution
    reply = fixture.backend.node._handle_drop_object_replica(protocol.DropObjectReplica(
        object_id, pending.spec.attempt_id, core.worker_id, core.node_id, descriptor.checksum,
    ))
    assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
    assert not fixture.backend.store.contains(object_id, sealed_only=False)
    assert core.owner_table.mark_lost(object_id, pending.spec.attempt_id)
    core._stored_descriptors.pop(object_id, None)


def _release_stored_fixture_refs(fixture):
    # _StoredFixture refs are deliberately inert; their owner tokens are real.
    # Release only those exact tokens without driving unrelated pending work.
    core = fixture.core
    for object_id in tuple(core._objects):
        for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
            assert core.owner_table.release_local_reference(object_id, token)
    assert core._state_lock.depth == 0
    assert core._state_lock.waits == 0
    assert not hasattr(core, "_coordinator") and not hasattr(core, "_reference_thread")


def test_all_lost_sibling_request_cannot_bypass_existing_targeted_failure_latch(monkeypatch):
    """A live retry budget must not turn terminal cleanup into whole START."""
    fixture = _StoredFixture()
    core = fixture.core
    producer, refs = fixture.submit(num_returns=2)
    fixture.succeed(producer, stored=True)
    assert core._finish_pending_task(producer) and not fixture.queued()
    first, later = producer.output_ids
    _drop_one_stored_slot(fixture, producer, first)
    targeted = core._targeted_reconstruction_coordinator()
    with core._state_lock:
        targeted.request(first, producer.spec.attempt_id)
    record = replace(core._recovery.task_record(producer.task_id))
    assert record.retries_remaining > 0 and record.retries_started == 0
    failure = SystemTaskError("targeted prerequisite terminal despite available budget")
    rpc = core._rpc
    reports = []
    lost_ack = False

    def lose_one_report_ack(address, handler, request):
        nonlocal lost_ack
        assert core._state_lock.depth == 0, handler
        result = rpc(address, handler, request)
        if (handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
                and type(request) is wire.ReportOutputPublicationSlotCollected
                and request.proof.object_id == first):
            reports.append(request)
            assert len(reports) <= 2
            if not lost_ack:
                lost_ack = True
                raise TimeoutError("first target retirement ACK lost")
        return result

    def forbidden_start(*_args, **_kwargs):
        pytest.fail("active targeted failure was bypassed through whole-task START")

    monkeypatch.setattr(core, "_rpc", lose_one_report_ack)
    try:
        core._fail_open_targeted_reconstruction(producer.task_id, failure)
        assert lost_ack and len(reports) == 1
        assert targeted.open_failure(producer.task_id) is failure
        assert targeted.current_session(producer.task_id).phase is TargetedSessionPhase.OPEN
        original_membership = core.owner_table.snapshot(first).output_publication
        assert original_membership is not None
        assert core.owner_table.snapshot(first).output_retirement_id is not None
        _drop_one_stored_slot(fixture, producer, later)
        assert all(core.owner_table.snapshot(value).state is ObjectState.LOST
                   for value in producer.output_ids)

        # The old target's get must wait on its unresolved cleanup, not restart
        # even though both siblings are now LOST and budget remains available.
        whole = core._reconstruction_coordinator()
        for method in ("preflight_graph", "preview", "prepare", "commit_prepared"):
            monkeypatch.setattr(whole, method, forbidden_start)
        monkeypatch.setattr(targeted, "preview_start", forbidden_start)
        monkeypatch.setattr(core, "_execute", forbidden_start)
        with pytest.raises(TimeoutError, match="retirement did not finish"):
            core.get(refs[0], timeout=0)
        assert len(reports) == 1
        assert core._recovery.task_record(producer.task_id) == record

        outcome = core._start_or_join_reconstruction(
            later, core._objects[later], return_requested_outcome=True,
        )
        assert outcome is None
        assert len(reports) == 2 and reports[1] == reports[0]
        assert core._recovery.task_record(producer.task_id) == record
        assert core._recovery.active_recovery(producer.task_id) is None
        assert core._accepted_task_count == 0
        assert not any(isinstance(item, _PendingTask) for item in tuple(core._submissions.queue))
        assert not whole._sessions
        for object_id in producer.output_ids:
            result = core.owner_table.snapshot(object_id)
            assert result.current_attempt == producer.spec.attempt_id
            assert result.state is ObjectState.ERROR
            assert type(result.error) is SystemTaskError and result.error.args == failure.args
        with pytest.raises(SystemTaskError, match="targeted prerequisite terminal"):
            core.get(refs[0], timeout=0)
        assert targeted.current_session(producer.task_id) is None
        assert targeted.open_failure(producer.task_id) is None
        assert fixture.backend.store.used_bytes == 0
    finally:
        _release_stored_fixture_refs(fixture)


def test_parent_lineage_cannot_reconstruct_producer_with_active_targeted_session(monkeypatch):
    """Ordinary DFS cannot acquire a second authority over targeted outputs."""
    fixture = _StoredFixture()
    core = fixture.core
    producer, refs = fixture.submit(num_returns=2)
    fixture.succeed(producer, stored=True)
    assert core._finish_pending_task(producer) and not fixture.queued()
    parent, parent_ref = fixture.submit(refs[0])
    fixture.succeed(parent, stored=True)
    assert core._finish_pending_task(parent) and not fixture.queued()
    first, later = producer.output_ids
    _drop_one_stored_slot(fixture, producer, first)
    targeted = core._targeted_reconstruction_coordinator()
    with core._state_lock:
        opened = targeted.request(first, producer.spec.attempt_id).session
    # Force whole-producer DFS to otherwise be admissible: every output is
    # LOST at the same epoch, but the earlier targeted OPEN remains authority.
    _drop_one_stored_slot(fixture, producer, later)
    _drop_one_stored_slot(fixture, parent, parent.object_id)
    ids = producer.output_ids + parent.output_ids
    before = tuple(core.owner_table.snapshot(object_id) for object_id in ids)
    records = {pending.task_id: replace(core._recovery.task_record(pending.task_id))
               for pending in (producer, parent)}
    publications = fixture.backend.recovery.publication_ids()
    remote_before = tuple(fixture.backend.recovery.snapshot(value) for value in publications)
    calls_before = tuple(fixture.backend.calls)

    def no_retirement_or_start(*_args, **_kwargs):
        pytest.fail("parent recovery crossed a producer's active targeted session")

    whole = core._reconstruction_coordinator()
    monkeypatch.setattr(core, "_retire_lost_output_memberships", no_retirement_or_start)
    monkeypatch.setattr(whole, "commit_prepared", no_retirement_or_start)
    monkeypatch.setattr(core, "_execute", no_retirement_or_start)
    try:
        assert core._start_or_join_reconstruction(
            parent.object_id, core._objects[parent.object_id], return_requested_outcome=True,
        ) is None
        with pytest.raises(TimeoutError, match="reconstruction admission did not finish"):
            core.get(parent_ref, timeout=0)
        assert tuple(core.owner_table.snapshot(object_id) for object_id in ids) == before
        assert targeted.current_session(producer.task_id) == opened
        assert targeted.current_session(parent.task_id) is None
        assert not whole._sessions
        assert core._accepted_task_count == 0
        assert not getattr(core, "_output_retirement_work", {})
        assert not core.owner_table.has_active_output_retirements()
        assert not any(isinstance(item, _PendingTask) for item in tuple(core._submissions.queue))
        assert tuple(fixture.backend.calls) == calls_before
        assert tuple(fixture.backend.recovery.snapshot(value) for value in publications) == remote_before
        for pending in (producer, parent):
            assert core._recovery.task_record(pending.task_id) == records[pending.task_id]
            assert core._recovery.active_recovery(pending.task_id) is None
    finally:
        _release_stored_fixture_refs(fixture)
