"""Unified publisher terminal after exact owner-death cleanup.

Legacy INLINE/STORED full source and the explicit migration mapping live in
``docs/history/retired-node-publication``.  These cases use current output
manifest/finalize proofs; no retired publication journal is instantiated.
"""
from __future__ import annotations

from dataclasses import replace
import pickle

import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import ObjectID, TaskID
from miniray.output_publication import OutputPublicationConflictError, OutputPublicationManifest
from miniray.output_publication_journal import OutputPublicationAdoptionProof, OutputPublicationJournalState
from miniray.resources import AllocationState, ResourceVector
from tests.unit.test_output_owner_death_node import _fixture
from tests.unit.test_output_publication_node_server import _no_runtime


pytestmark = pytest.mark.unit


def _ack_worker(node, expected, calls):
    def rpc(address, handler, request):
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        assert request == expected
        assert address == node._workers[request.manifest.header.executor_worker_id].address
        assert not node._state_lock._is_owned()
        calls.append(request)
        return wire.FinalizeOutputOwnerDeathReply(request, True)
    node._background_rpc = rpc


def _assert_no_owner_delivery(fixture, node, record, complete):
    assert not node._handle_prepare_output_publication(wire.PrepareOutputPublication(
        fixture.manifest, (fixture.values.payload),
    )).accepted
    with pytest.raises(ValueError, match="death-fenced"):
        node._handle_complete_worker_lease_inner(complete)
    query = protocol.GetWorkerLeaseOutcome(
        fixture.id.lease_id, fixture.id.task_id, fixture.id.attempt_id,
        fixture.values.executor, fixture.values.owner, ((fixture.id.object_id,)),
    )
    # Fenced Node handlers reject rather than turning a known Complete into
    # an ordinary descriptor-only success or supplying retired bytes.
    with pytest.raises(ValueError, match="death-fenced"):
        node._handle_get_output_worker_lease_outcome(query, fixture.id)


@pytest.mark.parametrize("phase", ("running", "complete"))
def test_node_finalize_installs_independent_terminal_and_exact_replay(monkeypatch, phase):
    fixture, node, record, complete, request = _fixture(monkeypatch, phase=phase)
    calls = []
    _ack_worker(node, request, calls)
    assert pickle.loads(pickle.dumps(request)) == request
    first = node._handle_finalize_output_owner_death(request)
    replay = node._handle_finalize_output_owner_death(request)
    assert first.cleaned and replay == first
    assert calls == [request]
    assert record.state is (protocol.LeaseExecutionState.COMPLETED if phase == "complete"
                            else protocol.LeaseExecutionState.ABANDONED)
    assert (record.completion == complete) is (phase == "complete")
    assert node._ledger.record(record.allocation_token).state is AllocationState.RELEASED
    assert node._workers[record.grant.worker_id].active_lease_id is None
    terminal = fixture.journal.snapshot(fixture.id)
    assert terminal.state is OutputPublicationJournalState.RETIRED
    assert not terminal.retained_result_slots
    assert (terminal.complete == fixture.values.witness) is (phase == "complete")
    assert terminal.rollback_tombstone is None
    assert fixture.adapter.owner_death_finished(fixture.id)
    assert node._drive_output_publications()
    assert node._output_publications_clean_locked()
    _assert_no_owner_delivery(fixture, node, record, complete)


def test_node_finalize_rejects_rebound_death_manifest_and_node_identity(monkeypatch):
    fixture, node, record, complete, request = _fixture(monkeypatch, phase="complete")
    calls = []
    _ack_worker(node, request, calls)
    assert node._handle_finalize_output_owner_death(request).cleaned
    before = fixture.journal.snapshot(fixture.id)
    changed_death = replace(request, owner_death=replace(request.owner_death, detection_id="different-death-proof"))
    with pytest.raises(OutputPublicationConflictError, match="installed owner fence"):
        node._handle_finalize_output_owner_death(changed_death)
    changed_header = replace(fixture.manifest.header, node_incarnation=replace(
        fixture.manifest.header.node_incarnation, registration_epoch=fixture.manifest.header.node_incarnation.registration_epoch + 1,
    ))
    changed_manifest = OutputPublicationManifest.create(changed_header, (fixture.manifest.value))
    with pytest.raises(OutputPublicationConflictError, match="Node incarnation"):
        node._handle_finalize_output_owner_death(wire.FinalizeOutputOwnerDeath(changed_manifest, request.owner_death))
    slots = (replace((fixture.manifest.value), checksum="ab" * 32),)
    rebound = OutputPublicationManifest.create(fixture.manifest.header, slots)
    with pytest.raises(OutputPublicationConflictError, match="publication identity"):
        node._handle_finalize_output_owner_death(wire.FinalizeOutputOwnerDeath(rebound, request.owner_death))
    assert fixture.journal.snapshot(fixture.id) == before and calls == [request]
    _assert_no_owner_delivery(fixture, node, record, complete)


def test_finalize_wire_revalidates_corrupted_execution_key(monkeypatch):
    fixture, node, _record, _complete, request = _fixture(monkeypatch)
    before = fixture.journal.snapshot(fixture.id)
    object.__setattr__(request.manifest.header.publication_id.execution.attempt_id, "attempt_number", True)
    with pytest.raises((ProtocolError, TypeError, ValueError)):
        pickle.loads(pickle.dumps(request))
    with pytest.raises((ProtocolError, TypeError, ValueError)):
        node._handle_finalize_output_owner_death(request)
    assert fixture.journal.snapshot(fixture.id) == before


def test_finalize_releases_dependency_pins_and_fences_late_output_steps(monkeypatch):
    fixture, node, record, complete, request = _fixture(monkeypatch)
    dependency = ObjectID.for_task(TaskID(bytes.fromhex("91" * 16)))
    node._object_store.put(dependency, b"dependency")
    pin = ("lease-dependency", fixture.id.lease_id, dependency)
    node._object_store.pin(dependency, pin)
    record.dependency_pins = ((dependency, pin),)
    record.output_complete_inflight = complete
    calls = []
    _ack_worker(node, request, calls)
    assert node._handle_finalize_output_owner_death(request).cleaned
    assert node._object_store.snapshot(dependency).pin_count == 0
    assert node._object_store.get(dependency) == b"dependency"
    assert record.output_complete_inflight is None and record.completion is None
    assert record.state is protocol.LeaseExecutionState.ABANDONED
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    _assert_no_owner_delivery(fixture, node, record, complete)


def test_finalize_fences_late_adoption_without_rewriting_success_history(monkeypatch):
    fixture, node, record, complete, request = _fixture(monkeypatch, phase="complete")
    calls = []
    _ack_worker(node, request, calls)
    assert node._handle_finalize_output_owner_death(request).cleaned
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "late-owner-cas")
    with pytest.raises(ValueError, match="death-fenced"):
        node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(proof))
    assert fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()
    assert record.completion == complete and record.state is protocol.LeaseExecutionState.COMPLETED
    assert node._handle_finalize_output_owner_death(request).cleaned and calls == [request]


@pytest.mark.parametrize("phase", ("partial", "running", "complete"))
def test_busy_worker_finalize_does_not_start_an_ordinary_supervisor_rollback(monkeypatch, phase):
    fixture, node, record, complete, request = _fixture(monkeypatch, phase=phase)
    calls = []
    worker_ready = False

    def rpc(address, handler, message):
        assert address == record.grant.worker_address
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER and message == request
        assert not node._state_lock._is_owned()
        calls.append(message)
        assert len(calls) <= 3
        return wire.FinalizeOutputOwnerDeathReply(message, worker_ready)

    node._background_rpc = rpc
    first = node._handle_finalize_output_owner_death(request)
    assert not first.cleaned and calls == [request]
    assert record.state is (protocol.LeaseExecutionState.COMPLETED if phase == "complete"
                            else protocol.LeaseExecutionState.ABANDONED)
    before = fixture.journal.snapshot(fixture.id)
    assert before.rollback is None and before.rollback_tombstone is None
    assert not fixture.adapter.owner_death_finished(fixture.id)
    # Owner cleanup may already have released CPU, but the live Worker's
    # pending ACK belongs to that same terminal, not a new rollback saga.
    # The current supervisor retries that exact owner-death request itself;
    # keep its ACK negative for this round rather than assuming no RPC occurs.
    def ordinary_rollback(*_args, **_kwargs):
        pytest.fail("owner-death pending ACK must not start an ordinary rollback")
    monkeypatch.setattr(fixture.adapter, "rollback", ordinary_rollback)
    assert not node._drive_output_publications()
    assert fixture.journal.snapshot(fixture.id) == before and calls == [request, request]
    assert not fixture.adapter.owner_death_finished(fixture.id)
    worker_ready = True
    assert node._handle_finalize_output_owner_death(request).cleaned
    assert calls == [request, request, request]
    assert fixture.adapter.owner_death_finished(fixture.id)
    after = fixture.journal.snapshot(fixture.id)
    assert after.state is OutputPublicationJournalState.RETIRED
    assert after.rollback is None and after.rollback_tombstone is None
    assert (after.complete == fixture.values.witness) is (phase == "complete")
    assert node._drive_output_publications()
