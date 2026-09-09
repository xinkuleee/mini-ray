"""Unified Node publication contracts migrated from the former INLINE path.

Historical source is recoverable through docs/history-index.md. Pure cases
use one tiny INLINE result and two child-owner tables with real local
journal/owner reducers; no Node constructor or I/O. One separately marked
loopback case preserves the original bounded cross-thread outbox race.
"""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
import queue
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.output_publication import OutputPublicationManifest
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationJournalState,
)
from miniray.resources import ResourceVector
from tests.unit.test_output_publication_node_server import _node as _single_node


def _node(*, refs=True):
    return _single_node(refs=refs, stored=False)


@pytest.fixture(autouse=True)
def _guard_runtime(request, monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("unified Node contract attempted unmodelled runtime work")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    if request.node.get_closest_marker("loopback_smoke") is None:
        for kind, name in ((threading.Thread, "start"), (threading.Thread, "join"),
                           (threading.Event, "wait"), (threading.Condition, "wait")):
            monkeypatch.setattr(kind, name, forbidden)


def _prepare(fixture, node):
    return node._handle_prepare_output_publication(wire.PrepareOutputPublication(
        fixture.manifest, fixture.values.payloads,
    ))


def _query(fixture, record):
    identity = fixture.id
    return protocol.GetWorkerLeaseOutcome(
        identity.lease_id, identity.task_id, identity.attempt_id,
        fixture.values.executor, fixture.values.owner, identity.output_ids,
    )


def _lose_worker(fixture, node):
    node._workers[fixture.values.executor].process = SimpleNamespace(is_alive=lambda: False)
    with node._state_lock:
        return node._reclaim_active_lease_after_worker_exit_locked(fixture.values.executor)


def _drive_until_done(node):
    for _ in range(16):
        if node._drive_output_publications():
            return
    pytest.fail("unified publication exceeded its sixteen bounded cleanup rounds")


def _adopt(fixture, node):
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "node-contract-owner-cas")
    assert node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(proof)).accepted


def _clean(node):
    with node._state_lock:
        return node._output_publications_clean_locked()


@pytest.mark.unit
def test_open_prepare_bind_order_and_authority_exclusion():
    fixture, node, record, _complete = _node()
    assert _prepare(fixture, node).accepted
    assert record.output_publication_id == fixture.id
    assert fixture.events == [
        "owner-register", "prepare", "prepare", "promote", "promote",
    ]
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.ready_to_complete and snapshot.complete is None
    assert snapshot.materialized_slots == (0,)
    assert fixture.handoffs.query(fixture.id).manifest == fixture.manifest
    assert fixture.handoffs.query(fixture.id).complete is None
    assert fixture.store.used_bytes == 0
    assert fixture.ledger.available == ResourceVector()


@pytest.mark.unit
def test_owner_registration_ack_loss_replays_exact_manifest_before_any_effect():
    fixture, node, record, _complete = _node()
    fixture.fault = "owner-register"
    with pytest.raises(TimeoutError, match="owner-register"):
        _prepare(fixture, node)
    assert record.output_publication_id == fixture.id
    assert fixture.events == ["owner-register"]
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()
    assert _prepare(fixture, node).accepted
    assert fixture.events[:2] == ["owner-register", "owner-register"]
    assert fixture.events.count("prepare") == 2


@pytest.mark.unit
def test_promotion_ack_loss_keeps_effects_and_replays_only_missing_promotions():
    fixture, node, _record, _complete = _node()
    fixture.fault = "promote"
    with pytest.raises(TimeoutError, match="promote"):
        _prepare(fixture, node)
    before = tuple(fixture.events)
    first = fixture.manifest.slots[0].transfers[0]
    assert first.final_hold in fixture.child_owners[first.contained_owner_worker_id].snapshot(first.contained_object_id).contained_holds
    assert not fixture.journal.snapshot(fixture.id).ready_to_complete
    assert _prepare(fixture, node).accepted
    assert tuple(fixture.events) == before + ("promote", "promote")
    assert fixture.journal.snapshot(fixture.id).ready_to_complete


@pytest.mark.unit
def test_complete_still_requires_the_exact_prepared_ack():
    fixture, node, record, complete = _node()
    fixture.fault = "promote"
    with pytest.raises(TimeoutError):
        _prepare(fixture, node)
    rejected = node._handle_complete_worker_lease_inner(complete)
    assert not rejected.accepted and not rejected.released
    assert "effect ACK" in rejected.error
    assert record.state is protocol.LeaseExecutionState.RUNNING
    assert record.output_complete_inflight is None
    assert fixture.journal.snapshot(fixture.id).complete is None
    assert fixture.ledger.available == ResourceVector()
    assert _prepare(fixture, node).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted


@pytest.mark.unit
def test_complete_replay_and_outcome_return_without_any_gcs_io(monkeypatch):
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    before = tuple(fixture.events)

    def forbidden(*_args, **_kwargs):
        pytest.fail("Complete/outcome attempted GCS or resource-report I/O")

    monkeypatch.setattr(fixture.adapter, "_report_complete", forbidden)
    monkeypatch.setattr(node, "_background_rpc", forbidden)
    monkeypatch.setattr(node, "_flush_pending_resource_report", forbidden)
    first = node._handle_complete_worker_lease_inner(complete)
    replay = node._handle_complete_worker_lease_inner(complete)
    outcome = node._handle_get_worker_lease_outcome(_query(fixture, record))
    assert first.accepted and first.released and replay.accepted and not replay.released
    assert first.output_publication == replay.output_publication == outcome.output_publication == fixture.values.envelope
    assert outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.descriptors == () and outcome.orphan_descriptors == ()
    assert fixture.handoffs.query(fixture.id).complete is None
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert node._workers[fixture.values.executor].active_lease_id is None
    assert record.completion == complete and record.output_complete_inflight is None
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert tuple(fixture.events) == before
    assert not _clean(node)


@pytest.mark.unit
def test_prepare_rejects_rebound_single_manifest_before_any_effect():
    fixture, node, record, _complete = _node(refs=False)
    assert _prepare(fixture, node).accepted
    before = fixture.journal.snapshot(fixture.id)
    events = tuple(fixture.events)
    wrong = OutputPublicationManifest.create(
        replace(fixture.manifest.header, job_id=type(fixture.values.job)(b"z" * 16)),
        fixture.manifest.slots,
    )
    reply = node._handle_prepare_output_publication(wire.PrepareOutputPublication(wrong, fixture.values.payloads))
    assert not reply.accepted
    assert record.output_publication_id == fixture.id
    assert fixture.journal.snapshot(fixture.id) == before and tuple(fixture.events) == events


@pytest.mark.unit
def test_prepare_failure_is_trusted_only_after_explicit_rollback():
    fixture, node, record, complete = _node()
    fixture.fault = "promote"
    with pytest.raises(TimeoutError, match="promote"):
        _prepare(fixture, node)
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    first = node._handle_complete_worker_lease_inner(failed)
    assert not first.accepted
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert node._handle_get_worker_lease_outcome(_query(fixture, record)).cleanup_pending
    _drive_until_done(node)
    reply = node._handle_complete_worker_lease_inner(failed)
    assert reply.accepted and reply.output_publication is None
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.rollback_tombstone is not None and snapshot.complete is None
    assert fixture.adapter.rollback_reported(fixture.id)
    fixture.assert_no_pins_or_bytes()
    assert fixture.events.count("rollback-report") == 1
    assert _clean(node)


@pytest.mark.unit
def test_aborted_ack_loss_keeps_shutdown_dirty_until_exact_replay():
    fixture, node, record, complete = _node(refs=False)
    assert _prepare(fixture, node).accepted
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    fixture.fault = "rollback-report"
    saw_loss = False
    for _ in range(4):
        try:
            node._handle_complete_worker_lease_inner(failed)
        except TimeoutError as exc:
            assert "rollback-report" in str(exc)
            saw_loss = True
            break
    assert saw_loss and fixture.store.used_bytes == 0
    assert not fixture.adapter.rollback_reported(fixture.id)
    assert not _clean(node)
    assert node._handle_get_worker_lease_outcome(_query(fixture, record)).cleanup_pending
    before = tuple(fixture.events)
    assert node._handle_complete_worker_lease_inner(failed).accepted
    assert tuple(fixture.events) == before + ("rollback-report",)
    assert _clean(node)


@pytest.mark.unit
def test_worker_loss_resumes_an_ambiguous_explicit_abort():
    fixture, node, record, _complete = _node()
    assert _prepare(fixture, node).accepted
    fixture.fault = "release"
    # First bounded turn discards the INLINE result; the next turn sends the
    # first exact child Release and loses its reply after the effect.
    assert fixture.adapter.rollback(fixture.id, "explicit-abort-before-worker-loss", max_effects=1) is None
    with pytest.raises(TimeoutError, match="release"):
        fixture.adapter.rollback(fixture.id, "explicit-abort-before-worker-loss", max_effects=1)
    before = fixture.journal.snapshot(fixture.id).rollback
    assert before is not None
    assert _lose_worker(fixture, node)
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST
    _drive_until_done(node)
    assert fixture.journal.snapshot(fixture.id).rollback == before
    assert fixture.events.count("release") == 5
    fixture.assert_no_pins_or_bytes()
    assert _clean(node)


@pytest.mark.unit
def test_shutdown_cleanliness_blocks_unresolved_publication_and_unretired_payload():
    fixture, node, _record, complete = _node()
    assert _prepare(fixture, node).accepted
    assert not _clean(node)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    assert not _clean(node)
    _drive_until_done(node)
    # A terminal-report ACK is not a payload-custody ACK.
    assert not _clean(node)
    _adopt(fixture, node)
    assert _clean(node)


@pytest.mark.unit
def test_terminal_ack_loss_keeps_only_background_report_pending():
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted
    fixture.fault = "complete-report"
    assert not node._drive_output_publications()
    assert fixture.handoffs.query(fixture.id).complete == fixture.values.witness
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert record.output_complete_inflight is None
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and not replay.released and replay.output_publication == fixture.values.envelope
    assert fixture.events.count("complete-report") == 1
    assert node._drive_output_publications()
    assert fixture.events.count("complete-report") == 2
    assert not fixture.adapter.pending_terminal_reports()
    _adopt(fixture, node)
    assert _clean(node)


@pytest.mark.unit
def test_outcome_keeps_success_after_terminal_ack_loss_and_worker_loss():
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted
    fixture.fault = "complete-report"
    assert not node._drive_output_publications()
    assert not _lose_worker(fixture, node)
    outcome = node._handle_get_worker_lease_outcome(_query(fixture, record))
    assert outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert outcome.output_publication == fixture.values.envelope and not outcome.worker_alive
    assert fixture.events.count("complete-report") == 1
    assert record.completion == complete


@pytest.mark.unit
@pytest.mark.parametrize("observer", ("outcome", "supervisor"))
def test_worker_loss_after_complete_boundary_preserves_success(observer):
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    record.output_complete_inflight = complete
    assert fixture.journal.complete(fixture.id, fixture.values.witness) == fixture.values.envelope
    assert _lose_worker(fixture, node)
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST
    if observer == "outcome":
        recovered = node._handle_get_worker_lease_outcome(_query(fixture, record))
        assert recovered.output_publication == fixture.values.envelope
        assert recovered.state is protocol.LeaseExecutionState.COMPLETED
    else:
        _drive_until_done(node)
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert record.completion == complete and record.output_complete_inflight is None
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert fixture.journal.snapshot(fixture.id).rollback is None
    assert fixture.handoffs.query(fixture.id).manifest == fixture.manifest


@pytest.mark.unit
def test_background_terminal_failure_releases_interrupted_local_completion(monkeypatch):
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    record.output_complete_inflight = complete
    fixture.journal.complete(fixture.id, fixture.values.witness)
    calls = []

    def unavailable(witness):
        calls.append(witness)
        assert record.state is protocol.LeaseExecutionState.COMPLETED
        assert fixture.ledger.available == ResourceVector({"CPU": 1})
        assert node._workers[fixture.values.executor].active_lease_id is None
        raise TimeoutError("terminal metadata unavailable")

    monkeypatch.setattr(fixture.adapter, "_report_complete", unavailable)
    assert not node._drive_output_publications()
    assert calls == [fixture.values.witness]
    assert record.completion == complete and record.output_complete_inflight is None
    assert node._handle_get_worker_lease_outcome(_query(fixture, record)).output_publication == fixture.values.envelope
    assert calls == [fixture.values.witness]


@pytest.mark.unit
def test_complete_intent_alone_cannot_win_after_worker_loss():
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    record.output_complete_inflight = complete
    assert _lose_worker(fixture, node)
    rejected = node._handle_complete_worker_lease_inner(complete)
    assert not rejected.accepted and not rejected.released
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST
    assert record.output_complete_inflight is None
    assert fixture.journal.snapshot(fixture.id).complete is None
    _drive_until_done(node)
    fixture.assert_no_pins_or_bytes()
    assert _clean(node)
    outcome = node._handle_get_worker_lease_outcome(_query(fixture, record))
    assert outcome.state is protocol.LeaseExecutionState.WORKER_LOST
    assert outcome.output_publication is None and not outcome.cleanup_pending


@pytest.mark.unit
def test_complete_replay_recovers_local_fault_after_resource_release(monkeypatch):
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    original = node._release_record_locked
    releases = []

    def fail_after_release(current, state):
        releases.append(original(current, state))
        raise RuntimeError("after local lease release")

    monkeypatch.setattr(node, "_release_record_locked", fail_after_release)
    with pytest.raises(RuntimeError, match="after local lease release"):
        node._handle_complete_worker_lease_inner(complete)
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert record.state is protocol.LeaseExecutionState.COMPLETED and record.completion is None
    assert fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
    monkeypatch.setattr(node, "_release_record_locked", original)
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and not replay.released
    assert replay.output_publication == fixture.values.envelope
    assert releases == [True]
    assert record.completion == complete and record.output_complete_inflight is None


@pytest.mark.unit
def test_terminal_callback_can_read_exact_local_complete_without_another_rpc(monkeypatch):
    fixture, node, record, complete = _node()
    assert _prepare(fixture, node).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted
    original = fixture.adapter._report_complete
    calls = []

    def read_before_reply(witness):
        calls.append(witness)
        assert len(calls) == 1
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned()
        replay = node._handle_complete_worker_lease_inner(complete)
        outcome = node._handle_get_worker_lease_outcome(_query(fixture, record))
        assert replay.output_publication == outcome.output_publication == fixture.values.envelope
        return original(witness)

    monkeypatch.setattr(fixture.adapter, "_report_complete", read_before_reply)
    assert node._drive_output_publications()
    assert calls == [fixture.values.witness]


@pytest.mark.unit
@pytest.mark.parametrize("finish_cleanup", (False, True))
def test_owner_fence_during_terminal_reply_preserves_fact_but_never_restores_custody(
    monkeypatch, finish_cleanup,
):
    fixture, node, record, complete = _node(refs=False)
    assert _prepare(fixture, node).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted
    incarnation = fixture.manifest.header.node_incarnation
    death = protocol.WorkerDeathRecord(
        "owner-exit-during-terminal", protocol.WorkerIncarnation(
            incarnation.node_id, incarnation.node_pid, incarnation.registration_epoch,
            fixture.values.owner, 1901,
        ), 1, 7, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    cleanup = wire.FinalizeOutputOwnerDeath(fixture.manifest, death)
    original = fixture.adapter._report_complete
    reports = []
    worker_acks = []

    def worker_rpc(address, handler, request):
        assert address == record.grant.worker_address
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER and request == cleanup
        worker_acks.append(request)
        return wire.FinalizeOutputOwnerDeathReply(request, finish_cleanup)

    def fence_after_apply(witness):
        reports.append(witness)
        acknowledgement = original(witness)
        assert node._handle_install_owner_death_fence(protocol.InstallOwnerDeathFence(
            "owner-terminal-fence", death, node.node_id,
        )).accepted
        if finish_cleanup:
            # This no-ref INLINE value has no physical replica or child cleanup.
            for slot in fixture.manifest.slots:
                if slot.object_id in node._sealed_metadata:
                    reply = node._handle_drop_object_replica(protocol.DropObjectReplica(
                        slot.object_id, fixture.id.attempt_id, fixture.values.owner, node.node_id, slot.checksum,
                    ))
                    assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert node._handle_finalize_output_owner_death(cleanup).cleaned
        return acknowledgement

    monkeypatch.setattr(node, "_background_rpc", worker_rpc)
    monkeypatch.setattr(fixture.adapter, "_report_complete", fence_after_apply)
    node._drive_output_publications()
    assert reports == [fixture.values.witness]
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.complete == fixture.values.witness
    assert snapshot.rollback_tombstone is None
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert not _prepare(fixture, node).accepted
    with pytest.raises(ValueError, match="death-fenced"):
        node._handle_complete_worker_lease_inner(complete)
    with pytest.raises(ValueError, match="death-fenced"):
        node._handle_get_output_worker_lease_outcome(_query(fixture, record), fixture.id)
    if finish_cleanup:
        assert snapshot.state is OutputPublicationJournalState.RETIRED
        assert not snapshot.retained_result_slots
        assert worker_acks == [cleanup]
        assert node._handle_finalize_output_owner_death(cleanup).cleaned
        assert _clean(node)
        assert node._drive_output_publications()
    else:
        assert snapshot.retained_result_slots == (0,)
        assert not fixture.adapter.owner_death_finished(fixture.id)
        assert not _clean(node)
        # Progress completion is not clean-shutdown/custody completion.  The
        # already reported true Complete stays true while owner cleanup waits.
        node._drive_output_publications()
        after = fixture.journal.snapshot(fixture.id)
        assert after.complete == snapshot.complete and after.rollback_tombstone is None
        assert not fixture.adapter.owner_death_finished(fixture.id)
        assert worker_acks == [cleanup]
        assert not _clean(node)
    assert reports == [fixture.values.witness]


@pytest.mark.unit
def test_supervisor_flushes_completion_metadata_without_starting_threads():
    fixture, node, _record, complete = _node()
    assert _prepare(fixture, node).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted
    waits = []

    class OnePollStop:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, timeout):
            waits.append(timeout)
            self.stopped = True

    node._worker_supervisor_stop = OnePollStop()
    node._flush_pending_worker_death_reports = lambda: None
    node._registered_with_gcs = True
    reports = []

    def report(_address, _handler, request):
        assert type(request) is protocol.UpdateNodeResources
        reports.append(request)
        return protocol.UpdateNodeResourcesReply(
            request.node_id, request.node_pid, request.registration_epoch, request.report_seq, True,
        )

    node._background_rpc = report
    node._worker_supervisor_loop()
    assert len(waits) == len(reports) == 1
    assert reports[0].available_resources == ResourceVector({"CPU": 1})
    assert not fixture.adapter.pending_terminal_reports()
    assert fixture.handoffs.query(fixture.id).complete == fixture.values.witness


@pytest.mark.loopback_smoke
def test_pending_terminal_rpc_does_not_lock_out_complete_or_outcome():
    """Original L1 invariant: two threads, fixed gates and finally joins."""
    fixture, node, record, complete = _node()
    release = threading.Event()
    entered = threading.Event()
    threads = []
    failures = queue.Queue()
    replies = []
    outcomes = []
    driver_results = []
    original = fixture.adapter._report_complete

    def block(witness):
        entered.set()
        assert release.wait(1.0), "terminal gate exceeded one second"
        return original(witness)

    def drive():
        try:
            driver_results.append(node._drive_output_publications())
        except BaseException as exc:
            failures.put_nowait(exc)

    def read():
        try:
            replies.append(node._handle_complete_worker_lease_inner(complete))
            outcomes.append(node._handle_get_worker_lease_outcome(_query(fixture, record)))
        except BaseException as exc:
            failures.put_nowait(exc)

    try:
        assert _prepare(fixture, node).accepted
        assert node._handle_complete_worker_lease_inner(complete).accepted
        fixture.adapter._report_complete = block
        driver = threading.Thread(target=drive, daemon=True, name="output-terminal-report")
        reader = threading.Thread(target=read, daemon=True, name="output-terminal-reader")
        threads.extend((driver, reader))
        driver.start()
        assert entered.wait(1.0), "terminal report did not enter callback"
        assert not _lose_worker(fixture, node)
        reader.start()
        reader.join(0.25)
        assert not reader.is_alive()
        assert driver.is_alive()
        assert fixture.adapter.pending_terminal_reports()
    finally:
        release.set()
        deadline = time.monotonic() + 1.0
        for thread in threads:
            if thread.ident is not None:
                thread.join(max(0.0, deadline - time.monotonic()))
    assert all(not thread.is_alive() for thread in threads)
    assert failures.empty(), failures.get_nowait() if not failures.empty() else None
    assert replies[0].accepted and not replies[0].released
    assert replies[0].output_publication == outcomes[0].output_publication == fixture.values.envelope
    assert outcomes[0].state is protocol.LeaseExecutionState.COMPLETED and not outcomes[0].worker_alive
    assert driver_results == [True]
    assert record.completion == complete and record.output_complete_inflight is None
