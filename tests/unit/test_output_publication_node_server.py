"""Bounded synchronous Node/GCS/child-owner unified publication handlers.

No Node constructor, sockets, worker process, background thread or waits.
One real resource ledger, two tiny results and at most four child transfers.
"""

from dataclasses import replace
from types import SimpleNamespace
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.node import NodeServer, _LeaseRecord, _WorkerSlot
from miniray.output_publication_journal import OutputPublicationAdoptionProof, OutputPublicationJournalState
from miniray.publication_gate import OutputPublicationGatePhase
from miniray.resources import NodeSnapshot, ResourceVector
from tests.unit.test_output_publication_node import _Fixture, _bind_real_node_storage


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Node handler attempted runtime work")
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _node(*, refs=True, target=False):
    fixture = _Fixture(refs=refs, target=target)
    node = _bind_real_node_storage(fixture)
    values = fixture.values
    node._ledger = fixture.ledger
    node._cluster_nodes = (NodeSnapshot(values.node, ResourceVector({"CPU": 1}), ResourceVector()),)
    node._cluster_addresses = {}
    node._gcs_address = ("gcs.invalid", 1)
    node._registered_with_gcs = False
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node._dependency_pin_cleanups = {}
    node._pinned_transfers = {}
    node._actor_workers = {}
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node.event_sink = None
    request = protocol.RequestWorkerLease(
        values.lease, values.task, values.attempt, ResourceVector({"CPU": 1}),
        values.node, values.owner, return_ids=values.publication_id.output_ids,
        target_execution=values.execution if target else None,
    )
    grant = protocol.GrantWorkerLease(
        values.lease, values.task, values.attempt, values.node, values.executor,
        ("worker.invalid", 1), fixture.token, target_execution=request.target_execution,
    )
    record = _LeaseRecord(request, fixture.token, grant, state=protocol.LeaseExecutionState.RUNNING)
    node._leases = {values.lease: record}
    node._workers = {values.executor: _WorkerSlot(
        values.executor, process=SimpleNamespace(is_alive=lambda: True),
        address=grant.worker_address, pid=1801, active_lease_id=values.lease,
    )}
    node._worker_order = (values.executor,)
    node._legacy_worker_compat = False
    node._output_publications = fixture.adapter
    complete = protocol.CompleteWorkerLease(
        values.lease, values.task, values.attempt, values.executor,
        protocol.TaskReplyStatus.SUCCEEDED, target_execution=request.target_execution,
    )
    return fixture, node, record, complete


@pytest.mark.parametrize("refs,target", ((True, False), (False, False), (True, True)))
def test_handlers_complete_mixed_selected_outputs_locally_without_terminal_rpc(refs, target):
    fixture, node, record, complete = _node(refs=refs, target=target)
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads))
    assert prepared.accepted
    assert record.output_publication_id == fixture.id
    reply = node._handle_complete_worker_lease_inner(complete)
    assert reply.accepted and reply.released
    assert reply.output_publication == fixture.values.envelope
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert fixture.recovery.snapshot(fixture.id).complete is None
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and not replay.released
    assert replay.output_publication == reply.output_publication
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, fixture.id.output_ids, target_execution=record.request.target_execution,
    )
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.output_publication == reply.output_publication
    assert len(outcome.descriptors) == 1
    assert node._drive_output_publications()
    assert fixture.recovery.snapshot(fixture.id).complete == fixture.values.witness


def test_failure_complete_releases_cpu_but_withholds_ack_until_all_compensation():
    fixture, node, record, complete = _node()
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)).accepted
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    first = node._handle_complete_worker_lease_inner(failed)
    assert not first.accepted
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, fixture.id.output_ids,
    )
    unavailable = node._handle_get_worker_lease_outcome(query)
    assert unavailable.found and unavailable.state is protocol.LeaseExecutionState.COMPLETED
    assert unavailable.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR and unavailable.cleanup_pending
    for _ in range(12):
        reply = node._handle_complete_worker_lease_inner(failed)
        if reply.accepted:
            break
    else:
        pytest.fail("bounded rollback did not consume its effects")
    fixture.assert_no_pins_or_bytes()
    assert fixture.recovery.snapshot(fixture.id).rollback is not None
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.found and not outcome.cleanup_pending and outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert node._handle_complete_worker_lease_inner(failed).accepted
    success = node._handle_complete_worker_lease_inner(complete)
    assert not success.accepted


def test_lost_rollback_report_ack_withholds_failed_outcome_after_local_cleanup():
    fixture, node, record, complete = _node(refs=False)
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)).accepted
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    fixture.fault = "rollback-report"
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, fixture.id.output_ids,
    )
    observed = False
    for _ in range(4):
        try:
            node._handle_complete_worker_lease_inner(failed)
        except TimeoutError as exc:
            assert "rollback-report" in str(exc)
            observed = True
            break
    assert observed
    assert fixture.store.used_bytes == 0
    assert fixture.journal.snapshot(fixture.id).rollback_tombstone is not None
    assert fixture.recovery.snapshot(fixture.id).rollback is not None
    assert not fixture.adapter.rollback_reported(fixture.id)
    assert node._handle_get_worker_lease_outcome(query).cleanup_pending
    assert node._handle_complete_worker_lease_inner(failed).accepted
    assert fixture.adapter.rollback_reported(fixture.id)
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR and not outcome.cleanup_pending


def test_wrong_lease_manifest_has_no_journal_record_or_child_effect():
    fixture, node, record, _complete = _node()
    changed = replace(fixture.manifest.header, executor_worker_id=fixture.values.owner)
    # Preserve no-child metadata to isolate physical executor binding.
    from miniray.output_publication import OutputPublicationManifest
    wrong = OutputPublicationManifest.create(changed, tuple(replace(slot, transfers=()) for slot in fixture.manifest.slots))
    reply = node._handle_prepare_output_publication(wire.PrepareOutputPublication(wrong, fixture.values.payloads))
    assert not reply.accepted
    assert record.output_publication_id is None
    assert fixture.journal.publication_ids() == ()
    assert fixture.events == []


def test_early_success_complete_cannot_fence_a_later_valid_prepare():
    fixture, node, record, complete = _node()
    fixture.fault = "intent"
    request = wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)
    with pytest.raises(TimeoutError):
        node._handle_prepare_output_publication(request)
    reply = node._handle_complete_worker_lease_inner(complete)
    assert not reply.accepted and record.output_complete_inflight is None
    assert node._handle_prepare_output_publication(request).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted


def test_payload_retirement_is_independent_of_terminal_outbox_and_physical_replica():
    fixture, node, record, complete = _node()
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads))
    assert node._handle_complete_worker_lease_inner(complete).accepted
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "owner-cas")
    assert node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(proof)).accepted
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    assert fixture.store.used_bytes > 0
    assert node._drive_output_publications()
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, fixture.id.output_ids,
    )
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert outcome.output_publication is None and outcome.descriptors == ()
    assert outcome.output_completion == fixture.values.witness
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and replay.output_publication is None
    assert replay.output_completion == fixture.values.witness


def test_first_success_complete_after_worker_loss_cannot_change_terminal_history():
    fixture, node, record, complete = _node()
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)).accepted
    node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, fixture.id.output_ids,
    )
    node._workers[fixture.values.executor].process = SimpleNamespace(is_alive=lambda: False)
    assert node._handle_get_worker_lease_outcome(query).state is protocol.LeaseExecutionState.WORKER_LOST
    reply = node._handle_complete_worker_lease_inner(complete)
    assert not reply.accepted
    assert fixture.journal.snapshot(fixture.id).complete is None
    assert record.output_complete_inflight is None
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST


def test_complete_keeps_both_local_locks_through_witness_and_ledger(monkeypatch):
    fixture, node, record, complete = _node()
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads))
    original = fixture.journal.complete
    observed = []

    def checked(publication_id, witness):
        # Synchronous lock ownership probe; no helper thread or blocking wait.
        assert node._state_lock._is_owned()
        assert fixture.journal._lock._is_owned()
        assert record.state is protocol.LeaseExecutionState.RUNNING
        observed.append(True)
        return original(publication_id, witness)

    monkeypatch.setattr(fixture.journal, "complete", checked)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    assert observed == [True]
    assert record.state is protocol.LeaseExecutionState.COMPLETED


def test_preboundary_complete_error_does_not_block_worker_loss_rollback(monkeypatch):
    fixture, node, record, complete = _node()
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads))
    original = fixture.journal.complete
    monkeypatch.setattr(fixture.journal, "complete", lambda *_: (_ for _ in ()).throw(ValueError("before witness")))
    with pytest.raises(ValueError, match="before witness"):
        node._handle_complete_worker_lease_inner(complete)
    assert record.output_complete_inflight is None
    assert fixture.journal.snapshot(fixture.id).complete is None
    monkeypatch.setattr(fixture.journal, "complete", original)
    node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    node._workers[fixture.values.executor].process = SimpleNamespace(is_alive=lambda: False)
    for _ in range(16):
        if node._drive_output_publications():
            break
    else:
        pytest.fail("uncrossed Complete blocked bounded worker-loss rollback")
    fixture.assert_no_pins_or_bytes()


def test_configured_checkpoints_follow_exact_intent_promotions_and_arm_acknowledgements():
    fixture, node, record, complete = _node()
    observed = []

    class Gate:
        def checkpoint(self, arrival):
            assert not node._state_lock._is_owned()
            assert not fixture.journal._lock._is_owned()
            assert not fixture.adapter._lock._is_owned()
            assert fixture.id in fixture.adapter._tickets
            assert arrival.publication_id == fixture.id
            assert arrival.manifest_digest == fixture.manifest.manifest_digest
            saved = fixture.recovery.snapshot(fixture.id)
            local = fixture.journal.snapshot(fixture.id)
            assert saved.complete is None and local.complete is None
            assert record.state is protocol.LeaseExecutionState.RUNNING
            if arrival.phase is OutputPublicationGatePhase.AFTER_INTENT_ACK:
                assert not saved.armed and local.materialized_slots == ()
                assert fixture.store.used_bytes == 0
                assert not fixture.graph.snapshot().prepared_edges
            elif arrival.phase is OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK:
                assert not saved.armed and local.ready_to_arm
                assert fixture.store.used_bytes > 0
                for slot in fixture.manifest.slots:
                    for transfer in slot.transfers:
                        holds = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id).contained_holds
                        assert transfer.final_hold in holds and transfer.provisional_hold not in holds
            else:
                assert arrival.phase is OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE
                assert saved.armed and local.ready_to_complete
            observed.append(arrival.phase)

    node._output_publication_gate = Gate()
    fixture.adapter._test_checkpoint = node._test_output_publication_checkpoint
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(
        fixture.manifest, fixture.values.payloads,
    )).accepted
    assert observed == [
        OutputPublicationGatePhase.AFTER_INTENT_ACK,
        OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK,
        OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE,
    ]
    assert node._handle_complete_worker_lease_inner(complete).accepted


def test_armed_prepare_replay_cannot_emit_an_earlier_precomplete_checkpoint():
    fixture, node, _record, complete = _node(refs=False)
    calls = []

    class Gate:
        def checkpoint(self, arrival):
            calls.append(arrival.phase)

    node._output_publication_gate = Gate()
    fixture.adapter._test_checkpoint = node._test_output_publication_checkpoint
    request = wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)
    fixture.fault = "arm"
    with pytest.raises(TimeoutError, match="arm"):
        node._handle_prepare_output_publication(request)
    assert calls == [OutputPublicationGatePhase.AFTER_INTENT_ACK, OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK]
    assert fixture.recovery.snapshot(fixture.id).armed
    assert not fixture.journal.snapshot(fixture.id).ready_to_complete
    for phase in (OutputPublicationGatePhase.AFTER_INTENT_ACK, OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK):
        with pytest.raises(RuntimeError, match="acknowledged phase"):
            node._test_output_publication_checkpoint(fixture.manifest, phase)
    assert node._handle_prepare_output_publication(request).accepted
    assert calls[-1] is OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE
    assert len(calls) == 3
    assert node._handle_complete_worker_lease_inner(complete).accepted
