"""Unified Node publication contracts with three opt-in thread lock probes.

One stored output and two child owners use actual Node journal, Store,
resource ledger and owner handoff reducers without runtime constructors.
Normal Complete is local; its owner report remains a separate outbox.
Unknown registration is compensated by exact rollback, never a new forward
request. Three original bounded thread lock probes remain opt-in L1.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol, publication_gate as gates
from miniray.ids import NodeID, WorkerID
from miniray.node import NodeServer
from miniray.output_publication import OutputPublicationManifest
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationJournalState, OutputPublicationStage,
)
from miniray.output_publication_node import OutputPublicationRemoteError
from miniray.output_handoff import OutputHandoffPhase
from tests.unit.test_output_publication_node_server import _node


# Do not add a module-level unit marker: the three original L1 node IDs remain
# explicitly opt-in. Each probe starts only one bounded, joined helper thread.
@pytest.fixture(autouse=True)
def _no_unreviewed_runtime(request, monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Node publication contract attempted unreviewed infrastructure")

    if request.node.get_closest_marker("loopback_smoke") is None:
        for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                             (threading.Event, "wait"), (threading.Condition, "wait")):
            monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "socketpair", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for target in (
        "miniray.node.NodeServer.__init__", "miniray.worker.WorkerServer.__init__",
        "miniray.core.CoreWorker.__init__", "miniray.transport.TCPServer.__init__",
        "miniray.node.rpc_request", "miniray.worker.rpc_request",
    ):
        monkeypatch.setattr(target, forbidden)


def _prepare(fixture, node):
    request = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    reply = node._handle_prepare_output_publication(request)
    assert reply.accepted and reply.request_identity == request.request_identity
    return request, reply


def _proof(fixture):
    return OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "node-owner-CAS")


def _outcome_request(fixture, record):
    values = fixture.values
    return protocol.GetWorkerLeaseOutcome(
        values.lease, values.task, values.attempt, values.executor, values.owner,
        ((fixture.id.object_id,)),
        scheduling_key=record.request.scheduling_key,
    )


def _track_releases(node, monkeypatch):
    released = []
    original = node._release_record_locked

    def release(record, state):
        result = original(record, state)
        released.append((record.request.lease_id, state, result))
        return result

    monkeypatch.setattr(node, "_release_record_locked", release)
    return released


def _interrupt_local_release(fixture, node, complete, monkeypatch):
    """A real journal Complete precedes one local ledger callback failure."""
    original = node._commit_output_lease_locked
    attempts = []

    def commit(manifest, request):
        attempts.append(request)
        assert manifest == fixture.manifest and request == complete
        if len(attempts) == 1:
            raise RuntimeError("local release interrupted after Complete")
        return original(manifest, request)

    monkeypatch.setattr(node, "_commit_output_lease_locked", commit)
    with pytest.raises(RuntimeError, match="local release interrupted"):
        node._handle_complete_worker_lease_inner(complete)
    assert fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
    assert fixture.adapter.pending_lease_completions() == (fixture.values.witness,)
    return attempts


def _probe_state_lock_from_thread(node: NodeServer) -> bool:
    """One bounded real-thread probe; never a listener or process."""
    acquired, failures = [], []

    def probe():
        try:
            locked = node._state_lock.acquire(timeout=0.1)
            try:
                acquired.append(locked)
            finally:
                if locked:
                    node._state_lock.release()
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=probe, daemon=True)
    try:
        thread.start()
        thread.join(1.0)
        assert not thread.is_alive()
        if failures:
            raise failures[0]
        assert len(acquired) == 1
        return acquired[0]
    finally:
        if thread.ident is not None:
            thread.join(1.0)
        assert not thread.is_alive()


@pytest.mark.unit
@pytest.mark.parametrize("refs", (False, True))
def test_prepare_requires_exact_running_single_output_lease_and_replays(refs):
    fixture, node, record, complete = _node(refs=refs, stored=True)
    request, first = _prepare(fixture, node)
    effects = tuple(fixture.events)
    replay = node._handle_prepare_output_publication(request)
    assert replay == first and tuple(fixture.events) == effects
    assert record.output_publication_id == fixture.id
    assert fixture.id.execution == fixture.manifest.execution
    assert node._handle_complete_worker_lease_inner(complete).accepted
    completed = fixture.journal.snapshot(fixture.id)
    rejected = node._handle_prepare_output_publication(request)
    assert not rejected.accepted and "running lease" in rejected.error
    assert fixture.journal.snapshot(fixture.id) == completed


@pytest.mark.unit
@pytest.mark.parametrize("field", ("executor", "owner", "node", "epoch", "attempt", "return-manifest"))
def test_prepare_fences_executor_owner_node_attempt_and_return_manifest(field):
    fixture, node, record, _complete = _node(refs=False)
    header = fixture.manifest.header
    if field == "executor":
        header = replace(header, executor_worker_id=WorkerID.random())
    elif field == "owner":
        header = replace(header, owner_worker_id=WorkerID.random())
    elif field == "node":
        header = replace(header, node_incarnation=replace(header.node_incarnation, node_id=NodeID.random()))
    elif field == "epoch":
        header = replace(header, node_incarnation=replace(header.node_incarnation, registration_epoch=8))
    elif field == "attempt":
        header = replace(header, publication_id=replace(fixture.id, execution=fixture.id.execution.for_attempt(fixture.values.attempt.next())))
    else:
        record.request = replace(record.request, return_ids=())
    manifest = OutputPublicationManifest.create(header, (fixture.manifest.value))
    request = wire.PrepareOutputPublication(manifest, (fixture.values.payload))
    reply = node._handle_prepare_output_publication(request)
    assert not reply.accepted and reply.request_identity == request.request_identity
    assert fixture.journal.publication_ids() == () and fixture.events == []
    assert record.output_publication_id is None and record.state is protocol.LeaseExecutionState.RUNNING
    assert fixture.store.used_bytes == 0


@pytest.mark.unit
def test_one_prepare_orders_owner_child_materialize_and_promotion_effects(monkeypatch):
    fixture, node, record, _complete = _node()
    seal = fixture.adapter._seal_replica

    def observe_seal(effect, descriptor, payload):
        snapshot = fixture.journal.snapshot(fixture.id)
        assert len([ack for ack in snapshot.acknowledgements if ack.effect.stage is OutputPublicationStage.PREPARE]) == 2
        assert any(ack.effect.stage is OutputPublicationStage.OWNER_REGISTER for ack in snapshot.acknowledgements)
        assert fixture.handoffs.query(fixture.id).manifest == fixture.manifest
        fixture.events.append("seal")
        return seal(effect, descriptor, payload)

    monkeypatch.setattr(fixture.adapter, "_seal_replica", observe_seal)
    request, reply = _prepare(fixture, node)
    assert fixture.events == ["owner-register", "prepare", "prepare", "seal", "promote", "promote"]
    assert reply.request_identity.publication_id == fixture.id
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.ready_to_complete and snapshot.complete is None
    assert (snapshot.manifest.value.edges) == (fixture.manifest.value).edges
    assert (fixture.journal.materialized_result(fixture.id, 0)) == (fixture.values.result)
    assert fixture.store.get((fixture.id.object_id)) == (fixture.values.payload)
    assert record.completion is None
    before = tuple(fixture.events)
    assert node._handle_prepare_output_publication(request).accepted
    assert tuple(fixture.events) == before


@pytest.mark.unit
def test_success_complete_and_adoption_require_bound_prepared_publication():
    fixture, node, record, complete = _node(refs=False)
    early = node._handle_complete_worker_lease_inner(complete)
    assert not early.accepted and not early.released
    assert record.state is protocol.LeaseExecutionState.RUNNING and record.completion is None
    assert fixture.journal.publication_ids() == ()
    assert fixture.ledger.available != fixture.ledger.total
    with pytest.raises((ValueError, LookupError)):
        node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(_proof(fixture)))
    _prepare(fixture, node)
    assert node._handle_complete_worker_lease_inner(complete).accepted


@pytest.mark.unit
def test_failed_complete_records_rollback_when_owner_registration_was_not_applied(monkeypatch):
    fixture, node, record, complete = _node()
    reports, intent_calls = [], []
    real_report = fixture.adapter._report_rollback

    def unavailable(manifest):
        intent_calls.append(manifest)
        raise OutputPublicationRemoteError("intent authority unavailable")

    def report(tombstone, *, manifest):
        reports.append((tombstone, manifest))
        if len(reports) == 1:
            raise TimeoutError("rollback report unavailable")
        return real_report(tombstone, manifest=manifest)

    monkeypatch.setattr(fixture.adapter, "_register_owner", unavailable)
    monkeypatch.setattr(fixture.adapter, "_report_rollback", report)
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload)))
    assert not prepared.accepted and record.output_publication_id == fixture.id
    assert fixture.handoffs.query(fixture.id) is None
    releases = _track_releases(node, monkeypatch)
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    with pytest.raises(TimeoutError, match="rollback report unavailable"):
        node._handle_complete_worker_lease_inner(failed)
    assert record.state is protocol.LeaseExecutionState.COMPLETED and record.completion == failed
    assert len(releases) == 1 and fixture.ledger.available == fixture.ledger.total
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.rollback.effects == () and snapshot.rollback_tombstone is not None
    assert node._handle_get_worker_lease_outcome(_outcome_request(fixture, record)).cleanup_pending
    assert not node._output_publications_clean_locked()
    reply = node._handle_complete_worker_lease_inner(failed)
    assert reply.accepted and not reply.released
    assert reports[0] == reports[1] and intent_calls == [fixture.manifest]
    assert fixture.handoffs.query(fixture.id).phase is OutputHandoffPhase.ABORTED
    assert fixture.handoffs.query(fixture.id).complete is None
    assert not node._handle_get_worker_lease_outcome(_outcome_request(fixture, record)).cleanup_pending
    assert node._handle_complete_worker_lease_inner(failed).accepted
    assert len(releases) == 1 and len(reports) == 2
    assert "prepare" not in fixture.events and "release" not in fixture.events
    fixture.assert_no_pins_or_bytes()


@pytest.mark.unit
def test_failed_complete_after_lost_owner_registration_ack_retains_cleanup_history(monkeypatch):
    fixture, node, record, complete = _node()
    manifests = []
    original = fixture.adapter._register_owner

    def applied_without_ack(manifest):
        manifests.append(manifest)
        original(manifest)
        raise TimeoutError("intent ACK lost after apply")

    monkeypatch.setattr(fixture.adapter, "_register_owner", applied_without_ack)
    with pytest.raises(TimeoutError, match="intent ACK lost"):
        node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload)))
    assert fixture.handoffs.query(fixture.id).manifest == fixture.manifest
    assert fixture.events == ["owner-register"]
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    reply = node._handle_complete_worker_lease_inner(failed)
    assert reply.accepted and reply.released
    assert manifests == [fixture.manifest]  # rollback does not authorize new forward work
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.rollback.effects == () and snapshot.complete is None
    assert fixture.handoffs.query(fixture.id).phase is OutputHandoffPhase.ABORTED
    assert node._handle_complete_worker_lease_inner(failed).accepted
    assert fixture.events == ["owner-register", "rollback-report"]
    fixture.assert_no_pins_or_bytes()


@pytest.mark.unit
def test_prepare_exactly_replays_owner_registration_after_applied_ack_was_lost(monkeypatch):
    fixture, node, record, _complete = _node()
    reports, dispositions = [], []
    original = fixture.adapter._register_owner

    def report(manifest):
        reports.append(manifest)
        acknowledgement = original(manifest)
        dispositions.append(fixture.handoffs.query(fixture.id))
        if len(reports) == 1:
            raise TimeoutError("intent ACK lost after apply")
        return acknowledgement

    monkeypatch.setattr(fixture.adapter, "_register_owner", report)
    request = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    with pytest.raises(TimeoutError, match="intent ACK lost"):
        node._handle_prepare_output_publication(request)
    assert fixture.events == ["owner-register"] and fixture.store.used_bytes == 0
    assert record.output_publication_id == fixture.id
    assert node._handle_prepare_output_publication(request).accepted
    assert reports == [fixture.manifest, fixture.manifest]
    assert len(dispositions) == 2 and dispositions[0] == dispositions[1]
    assert dispositions[0].manifest == fixture.manifest and dispositions[0].complete is None
    assert fixture.journal.snapshot(fixture.id).ready_to_complete


@pytest.mark.loopback_smoke
def test_stored_complete_releases_state_lock_around_external_terminal_ack():
    """Stable L1 ID: external terminal ACK now belongs to the outbox."""
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    first = node._handle_complete_worker_lease_inner(complete)
    assert first.accepted and first.released and record.completion == complete
    assert "complete-report" not in fixture.events
    observed = []
    original = fixture.adapter._report_complete

    def report(witness):
        assert record.state is protocol.LeaseExecutionState.COMPLETED
        assert record.output_complete_inflight is None
        assert fixture.ledger.available == fixture.ledger.total
        assert not fixture.journal._lock._is_owned()
        observed.append(_probe_state_lock_from_thread(node))
        return original(witness)

    fixture.adapter._report_complete = report
    node._drive_output_publications()
    assert observed == [True]
    assert fixture.adapter.pending_terminal_reports() == ()
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and not replay.released and replay.output_publication == first.output_publication
    assert record.completion == complete and observed == [True]


@pytest.mark.unit
def test_complete_gate_runs_after_node_commit_before_reply(monkeypatch):
    fixture, node, record, request = _node(refs=False)
    _prepare(fixture, node)
    node._output_publication_gate = gates.OutputPublicationGate(gates.OutputPublicationGateConfig(0, ("127.0.0.1", 32112)))
    sent = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def settimeout(self, value):
            assert value > 0

        def sendall(self, payload):
            assert record.state is protocol.LeaseExecutionState.COMPLETED
            assert record.completion == request and record.output_complete_inflight is None
            assert fixture.ledger.available == fixture.ledger.total
            assert not node._state_lock._is_owned() and not fixture.journal._lock._is_owned()
            assert fixture.handoffs.query(fixture.id).complete == fixture.values.witness
            sent.append(payload)

        def recv(self, size):
            assert size == 1
            return gates.OUTPUT_PUBLICATION_GATE_RELEASE

    monkeypatch.setattr(gates.socket, "create_connection", lambda *_args, **_opts: Connection())
    first = node._handle_complete_worker_lease(request)
    replay = node._handle_complete_worker_lease(request)
    assert first.accepted and first.released and replay.accepted and not replay.released
    assert first.output_publication == replay.output_publication == fixture.values.envelope
    assert (first.output_publication.result) == (fixture.values.result)
    assert first.output_publication.result.inline_data is None
    assert len(sent) == 1
    arrival = gates.OutputPublicationGateArrival.from_bytes(sent[0])
    assert arrival.publication_id == fixture.id
    assert arrival.phase is gates.OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY


@pytest.mark.unit
def test_complete_gate_failure_is_sticky_and_never_rolls_back(monkeypatch):
    fixture, node, record, request = _node(refs=False)
    _prepare(fixture, node)
    node._output_publication_gate = gates.OutputPublicationGate(gates.OutputPublicationGateConfig(0, ("127.0.0.1", 32113)))
    calls = []

    def fail_connect(*_args, **_kwargs):
        calls.append(True)
        raise TimeoutError("gate unavailable")

    monkeypatch.setattr(gates.socket, "create_connection", fail_connect)
    with pytest.raises(TimeoutError, match="gate unavailable"):
        node._handle_complete_worker_lease(request)
    with pytest.raises(RuntimeError, match="gate unavailable"):
        node._handle_complete_worker_lease(request)
    assert calls == [True] and record.state is protocol.LeaseExecutionState.COMPLETED
    assert record.completion == request and record.output_complete_inflight is None
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.complete == fixture.values.witness and snapshot.rollback is None
    assert snapshot.retained_result_slots == (0,) and fixture.ledger.available == fixture.ledger.total


@pytest.mark.unit
def test_local_complete_error_retains_exact_marker_until_release_replay(monkeypatch):
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    releases = _track_releases(node, monkeypatch)
    attempts = _interrupt_local_release(fixture, node, complete, monkeypatch)
    assert record.state is protocol.LeaseExecutionState.RUNNING
    assert record.output_complete_inflight == complete and record.completion is None
    assert fixture.journal.snapshot(fixture.id).rollback is None
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and replay.released and replay.output_publication == fixture.values.envelope
    assert attempts == [complete, complete] and len(releases) == 1
    assert record.output_complete_inflight is None and record.completion == complete
    assert fixture.adapter.pending_lease_completions() == ()
    assert node._handle_complete_worker_lease_inner(complete).accepted and len(releases) == 1


@pytest.mark.unit
def test_local_complete_witness_wins_after_worker_loss_without_second_release(monkeypatch):
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    releases = _track_releases(node, monkeypatch)
    _interrupt_local_release(fixture, node, complete, monkeypatch)
    assert node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    node._workers[fixture.values.executor].process.is_alive = lambda: False
    assert fixture.handoffs.query(fixture.id).complete is None
    reply = node._handle_complete_worker_lease_inner(complete)
    assert reply.accepted and not reply.released
    assert reply.output_publication == fixture.values.envelope
    assert record.state is protocol.LeaseExecutionState.COMPLETED and record.completion == complete
    assert len(releases) == 1 and releases[0][1] is protocol.LeaseExecutionState.WORKER_LOST


@pytest.mark.loopback_smoke
def test_stored_outcome_validates_under_lock_then_queries_adapter_lock_free():
    """Stable L1 ID: data-plane delivery follows local exact outcome checks."""
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    observed = []

    class Probe:
        def checkpoint(self, arrival, ensure_terminal=None):
            assert arrival.publication_id == fixture.id
            assert record.completion == complete
            assert not fixture.journal._lock._is_owned()
            observed.append(_probe_state_lock_from_thread(node))

    node._output_publication_gate = Probe()
    request = _outcome_request(fixture, record)
    with pytest.raises(ValueError, match="execution"):
        node._handle_get_worker_lease_outcome(replace(request, executor_worker_id=WorkerID.random()))
    assert observed == []
    reply = node._handle_get_worker_lease_outcome(request)
    assert observed == [True] and reply.found and reply.worker_alive
    assert reply.state is protocol.LeaseExecutionState.COMPLETED
    assert reply.output_publication == fixture.values.envelope


@pytest.mark.unit
@pytest.mark.parametrize("worker_lost", (False, True))
def test_local_completed_outcome_reconciles_lease_before_adoption(monkeypatch, worker_lost):
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    releases = _track_releases(node, monkeypatch)
    _interrupt_local_release(fixture, node, complete, monkeypatch)
    if worker_lost:
        assert node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
        node._workers[fixture.values.executor].process.is_alive = lambda: False
    before = len(releases)
    reply = node._handle_get_worker_lease_outcome(_outcome_request(fixture, record))
    assert reply.output_publication == fixture.values.envelope and reply.worker_alive is (not worker_lost)
    assert reply.state is protocol.LeaseExecutionState.COMPLETED
    assert reply.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert record.completion == complete and record.output_complete_inflight is None
    assert len(releases) - before == int(not worker_lost)
    assert fixture.ledger.available == fixture.ledger.total
    proof = _proof(fixture)
    assert node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(proof)).accepted
    node._drive_output_publications()
    assert fixture.adapter.pending_lease_completions() == () and len(releases) == 1
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()


@pytest.mark.loopback_smoke
def test_publication_step_external_effect_does_not_hold_node_state_lock():
    fixture, node, _record, _complete = _node()
    observed = []
    prepare_child = fixture.adapter._prepare_child

    def prepare(address, request):
        if not observed:
            assert not fixture.journal._lock._is_owned()
            observed.append(_probe_state_lock_from_thread(node))
        return prepare_child(address, request)

    fixture.adapter._prepare_child = prepare
    _prepare(fixture, node)
    assert observed == [True] and fixture.events.count("prepare") == 2


@pytest.mark.unit
def test_death_driver_never_rolls_back_a_known_complete_with_pending_release(monkeypatch):
    fixture, node, record, complete = _node()
    _prepare(fixture, node)
    _interrupt_local_release(fixture, node, complete, monkeypatch)
    assert node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    node._workers[fixture.values.executor].process.is_alive = lambda: False
    monkeypatch.setattr(fixture.adapter, "rollback", lambda *_a, **_k: pytest.fail("known Complete rolled back"))
    node._drive_output_publications()
    assert record.state is protocol.LeaseExecutionState.COMPLETED and record.completion == complete
    assert record.output_complete_inflight is None
    assert fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
    assert fixture.journal.snapshot(fixture.id).rollback is None
    assert fixture.adapter.pending_lease_completions() == fixture.adapter.pending_terminal_reports() == ()
    assert not node._output_publications_clean_locked()  # retained handoff still needs adoption


@pytest.mark.unit
def test_ambiguous_rollback_ack_is_dirty_then_exact_retry_converges(monkeypatch):
    fixture, node, record, _complete = _node()
    _prepare(fixture, node)
    assert node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    node._workers[fixture.values.executor].process.is_alive = lambda: False
    reports = []
    report = fixture.adapter._report_rollback

    def lost_ack(tombstone, *, manifest):
        reports.append((tombstone, manifest))
        result = report(tombstone, manifest=manifest)
        if len(reports) == 1:
            raise TimeoutError("rollback ACK lost")
        return result

    monkeypatch.setattr(fixture.adapter, "_report_rollback", lost_ack)
    for _ in range(16):
        clean = node._drive_output_publications()
        if reports:
            break
    else:
        pytest.fail("fixed rollback effects did not reach final report")
    assert not clean and len(reports) == 1
    assert not node._output_publications_clean_locked()
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    assert not fixture.adapter.rollback_reported(fixture.id)
    releases = fixture.events.count("release")
    fixture.assert_no_pins_or_bytes()
    assert node._drive_output_publications()
    assert reports[0] == reports[1] and fixture.events.count("release") == releases
    assert node._output_publications_clean_locked() and fixture.adapter.pending_rollbacks() == ()


@pytest.mark.unit
def test_successful_unclaimed_publication_is_retained_but_blocks_finalize(monkeypatch):
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    node._drive_output_publications()
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.COMPLETED
    assert snapshot.retained_result_slots == (0,) and snapshot.rollback is None
    assert fixture.adapter.pending_terminal_reports() == ()
    assert not node._output_publications_clean_locked()
    request = protocol.BeginDrain("retained-output-drain", "pure publication boundary")
    node._shutdown_request_id = request.request_id
    node._worker_drain_statuses = {fixture.values.executor: protocol.DrainStatus(
        request.request_id, "worker:{}".format(fixture.values.executor), True, True,
    )}
    # Only unrelated background reporters are inert; actual Node publication
    # and ledger cleanliness predicates remain responsible for the result.
    for method in ("_flush_pending_actor_exit_reports", "_flush_pending_worker_death_reports",
                   "_retry_dependency_pin_cleanups", "_flush_pending_resource_report"):
        monkeypatch.setattr(node, method, lambda *_a, **_k: None)
    status = node._node_drain_status(request, drive=False)
    assert status.drain_started and not status.clean and not status.resources_clean
    assert fixture.journal.snapshot(fixture.id) == snapshot
    assert node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(_proof(fixture))).accepted
    assert node._output_publications_clean_locked()
    status = node._node_drain_status(request, drive=False)
    assert status.clean and status.resources_clean
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert fixture.store.used_bytes > 0  # adoption retires reply cache, not live owner replica


@pytest.mark.unit
def test_adopted_ack_is_exact_idempotent_and_does_not_repeat_child_effects():
    fixture, node, record, complete = _node()
    _prepare(fixture, node)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    before = tuple(fixture.events)
    request = wire.AckOutputPublicationAdopted(_proof(fixture))
    first = node._handle_ack_output_publication_adopted(request)
    replay = node._handle_ack_output_publication_adopted(request)
    assert first == replay and first.request == request and first.accepted
    assert tuple(fixture.events) == before
    assert record.completion == complete and fixture.journal.snapshot(fixture.id).retained_result_slots == ()
    assert fixture.store.used_bytes > 0
    assert not node._output_publications_clean_locked()  # terminal outbox is independent
    assert node._drive_output_publications() and node._output_publications_clean_locked()


def _clean_publication_fixture():
    fixture, node, record, complete = _node(refs=False)
    _prepare(fixture, node)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    assert node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(_proof(fixture))).accepted
    assert node._drive_output_publications()
    with node._state_lock:
        assert node._output_publications_clean_locked()
    return fixture, node, record


def _cleanliness_snapshot(fixture, record):
    return (
        fixture.journal.snapshot(fixture.id), fixture.handoffs.query(fixture.id),
        fixture.ledger.snapshot(), replace(record), tuple(fixture.events),
        fixture.adapter.pending_terminal_reports(), fixture.adapter.pending_lease_completions(),
        fixture.adapter.pending_rollbacks(), frozenset(fixture.adapter._tickets),
        fixture.store.get((fixture.id.object_id)),
    )


class _BusyPublicationLock:
    """Simulate contention without a thread, timeout or actual wait."""

    def __init__(self, node):
        self.node = node
        self.acquisitions = []

    def acquire(self, *, blocking):
        assert self.node._state_lock._is_owned()
        self.acquisitions.append(blocking)
        assert blocking is False
        return False

    def release(self):
        pytest.fail("cleanliness released a publication lock it never acquired")


@pytest.mark.unit
def test_cleanliness_journal_contention_returns_unclean_without_wait_or_mutation(monkeypatch):
    fixture, node, record = _clean_publication_fixture()
    before = _cleanliness_snapshot(fixture, record)
    journal_lock, adapter_lock = fixture.journal._lock, fixture.adapter._lock
    busy = _BusyPublicationLock(node)

    def forbidden(*_args, **_kwargs):
        pytest.fail("contended cleanliness predicate performed RPC or progress")

    with monkeypatch.context() as patch:
        patch.setattr(fixture.journal, "_lock", busy)
        patch.setattr(node, "_background_rpc", forbidden)
        patch.setattr(node, "_drive_output_publications", forbidden)
        with node._state_lock:
            assert not node._output_publications_clean_locked()
            assert busy.acquisitions == [False]
            assert not adapter_lock._is_owned()
    assert fixture.journal._lock is journal_lock
    assert not journal_lock._is_owned() and not adapter_lock._is_owned()
    assert _cleanliness_snapshot(fixture, record) == before
    with node._state_lock:
        assert node._output_publications_clean_locked()


@pytest.mark.unit
def test_cleanliness_adapter_contention_releases_journal_without_wait_or_mutation(monkeypatch):
    fixture, node, record = _clean_publication_fixture()
    before = _cleanliness_snapshot(fixture, record)
    journal_lock, adapter_lock = fixture.journal._lock, fixture.adapter._lock
    busy = _BusyPublicationLock(node)
    acquired = busy.acquire

    def fail_after_journal(*, blocking):
        assert journal_lock._is_owned()
        return acquired(blocking=blocking)

    def forbidden(*_args, **_kwargs):
        pytest.fail("contended cleanliness predicate performed RPC or progress")

    busy.acquire = fail_after_journal
    with monkeypatch.context() as patch:
        patch.setattr(fixture.adapter, "_lock", busy)
        patch.setattr(node, "_background_rpc", forbidden)
        patch.setattr(node, "_drive_output_publications", forbidden)
        with node._state_lock:
            assert not node._output_publications_clean_locked()
            assert busy.acquisitions == [False]
            assert not journal_lock._is_owned()
    assert fixture.adapter._lock is adapter_lock
    assert not journal_lock._is_owned() and not adapter_lock._is_owned()
    assert _cleanliness_snapshot(fixture, record) == before
    with node._state_lock:
        assert node._output_publications_clean_locked()


@pytest.mark.unit
@pytest.mark.parametrize("field", ("attempt", "lease", "owner", "digest"))
def test_adoption_fences_complete_execution_owner_and_digest(field):
    fixture, node, _record, complete = _node(refs=False)
    _prepare(fixture, node)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    proof = _proof(fixture)
    if field == "owner":
        proof = replace(proof, owner_worker_id=WorkerID.random())
    elif field == "digest":
        proof = replace(proof, complete=replace(proof.complete, manifest_digest="0" * 64))
    else:
        identity = fixture.id
        identity = (replace(identity, execution=identity.execution.for_attempt(fixture.values.attempt.next()))
                    if field == "attempt" else replace(identity, lease_id=type(identity.lease_id).random()))
        proof = replace(proof, complete=replace(proof.complete, publication_id=identity))
    before = fixture.journal.snapshot(fixture.id)
    effects = tuple(fixture.events)
    with pytest.raises((ValueError, LookupError)):
        node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(proof))
    assert fixture.journal.snapshot(fixture.id) == before and tuple(fixture.events) == effects


@pytest.mark.unit
def test_promotion_transport_ambiguity_propagates_and_exact_prepare_resumes():
    fixture, node, record, _complete = _node()
    fixture.fault = "promote"
    request = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    with pytest.raises(TimeoutError, match="lost-ACK"):
        node._handle_prepare_output_publication(request)
    snapshot = fixture.journal.snapshot(fixture.id)
    assert not snapshot.ready_to_complete and snapshot.complete is None
    assert record.state is protocol.LeaseExecutionState.RUNNING and record.completion is None
    assert not any(ack.effect.stage is OutputPublicationStage.PROMOTE for ack in snapshot.acknowledgements)
    stored_bytes = fixture.store.get((fixture.id.object_id))
    assert node._handle_prepare_output_publication(request).accepted
    assert fixture.journal.snapshot(fixture.id).ready_to_complete
    assert fixture.events.count("prepare") == 2 and fixture.events.count("promote") == 3
    assert fixture.store.get((fixture.id.object_id)) == stored_bytes


@pytest.mark.unit
@pytest.mark.parametrize("handler", (
    "_handle_prepare_output_publication", "_handle_complete_worker_lease_inner",
    "_handle_get_worker_lease_outcome", "_handle_ack_output_publication_adopted",
    "_handle_finalize_output_owner_death",
))
def test_handlers_reject_wrong_wire_types(handler):
    _fixture, node, *_rest = _node(refs=False)
    with pytest.raises(TypeError):
        getattr(node, handler)(object())
