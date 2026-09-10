"""Small, synchronous Node composition tests: no threads, sockets or waits.

Each case has one tiny output, at most two child transfers, one in-memory
ObjectStore and one CPU ledger.  Faults are single callback exceptions; retries
are explicit calls with fixed bounds, not a background runtime.
"""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.output_handoff import OutputHandoffPhase, OutputHandoffStateError, OutputHandoffTable
from miniray.object_store import ObjectStore
from miniray.object_manager import ObjectManager
from miniray.node import NodeServer
from miniray.output_publication import OutputPublicationConflictError
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationJournal,
    OutputPublicationJournalState, OutputPublicationJournalStateError,
    OutputPublicationStage as Stage,
)
from miniray.output_publication_node import (
    OutputPublicationBusy, OutputPublicationNodeAdapter, OutputPublicationRemoteError,
)
from miniray.ownership import ObjectOwnerTable, OutputOwnerPublicationPlan
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector
from miniray.publication_sources import BorrowedContainedSource
from tests.unit.test_output_publication import _Fixture as _Values, _assert_metadata


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("output Node contract attempted real runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Fixture:
    def __init__(self, *, refs=True, stored=True):
        self.values = _Values(refs=refs, stored=stored)
        self.manifest = self.values.manifest
        self.id = self.values.publication_id
        self.journal = OutputPublicationJournal()
        self.handoffs = OutputHandoffTable()
        self.store = ObjectStore(1024)
        self.child_owners = {}
        self.events = []
        self.fault = None
        self.drop_history = set()
        self.ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        self.token = AllocationToken("test-output-lease")
        self.ledger.allocate(ResourceVector({"CPU": 1}), self.token)
        self.release_calls = 0
        self.releases = 0
        for transfer in (self.manifest.value.transfers):
            table = self.child_owners.setdefault(transfer.contained_owner_worker_id, ObjectOwnerTable())
            table.register(transfer.contained_object_id, local_token='source-live')
            if isinstance(transfer.source, BorrowedContainedSource):
                source = transfer.source.original_source
                root_borrower = (self.values.executor, 'upstream-root')
                table.add_borrowed_reference(transfer.contained_object_id, root_borrower)
                table.retain_borrowed_reference_for_task(transfer.contained_object_id, root_borrower, source.hold)
                table.acquire_exported_reference(transfer.contained_object_id, source, transfer.source.owner_table_token)
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register_owner,
            report_complete=self.terminal, report_rollback=self.rollback_report,
            prepare_child=self.prepare_child, promote_child=self.promote_child,
            release_child=self.release_child, seal_replica=self.seal, drop_replica=self.drop,
        )

    def hit(self, stage):
        self.events.append(stage)
        if self.fault == stage:
            self.fault = None
            raise TimeoutError("injected effect-then-lost-ACK: " + stage)

    def register_owner(self, manifest):
        ack = self.handoffs.register(manifest, self.id.attempt_id)
        self.hit("owner-register")
        assert ack.manifest == manifest
        return ack

    def terminal(self, witness):
        ack = self.handoffs.record_complete(witness)
        self.hit("terminal")
        return ack

    def rollback_report(self, tombstone, *, manifest):
        assert tombstone == self.journal.snapshot(self.id).rollback_tombstone
        ack = self.handoffs.abort_manifest(manifest, tombstone.plan.rollback_id)
        self.hit("rollback-report")
        return ack

    def prepare_child(self, address, request):
        assert address == request.transfer.contained_owner_address
        disposition = self.child_owners[request.authority_worker_id].prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id
        )
        self.hit("prepare")
        return protocol.StoredContainedPinReply(request, disposition)

    def promote_child(self, address, request):
        assert address == request.transfer.contained_owner_address
        disposition = self.child_owners[request.authority_worker_id].promote_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id
        )
        self.hit("promote")
        return protocol.StoredContainedPinReply(request, disposition)

    def release_child(self, address, request):
        assert address in (("127.0.0.1", 30101), ("127.0.0.1", 30102))
        released = self.child_owners[request.owner_worker_id].release_contained_reference(request.object_id, request.hold)
        self.hit("release")
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released
        )

    def seal(self, effect, descriptor, payload):
        assert effect.stage is Stage.MATERIALIZE
        if self.store.contains(descriptor.object_id):
            assert self.store.get(descriptor.object_id) == payload
        else:
            self.store.put(descriptor.object_id, payload)
        self.hit("seal")
        return descriptor

    def drop(self, effect, request):
        assert effect.stage is Stage.SLOT_DROP
        if request in self.drop_history:
            status = protocol.DropObjectReplicaStatus.ALREADY_DROPPED
        else:
            if self.store.contains(request.object_id, sealed_only=False):
                self.store.delete(request.object_id)
            self.drop_history.add(request)
            status = protocol.DropObjectReplicaStatus.DROPPED
        self.hit("drop")
        return protocol.DropObjectReplicaReply(
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum, status,
        )

    def commit(self, witness):
        assert witness == self.values.witness
        self.release_calls += 1
        self.releases += self.ledger.release(self.token)
        self.hit("commit")

    def prepare(self):
        self.adapter.prepare(self.manifest, (self.values.payload))

    def complete(self):
        return self.adapter.complete(self.id, commit_lease=self.commit)

    def rollback_all(self):
        # One result plus two pairs of child holds is the maximum here.
        result = self.adapter.rollback(self.id, "test-rollback", max_effects=5)
        assert result is not None
        return result

    def assert_no_pins_or_bytes(self):
        assert self.store.used_bytes == 0
        assert self.journal.snapshot(self.id).retained_result_slots == ()
        for transfer in (self.manifest.value.transfers):
            snapshot = self.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.provisional_hold not in snapshot.contained_holds
            assert transfer.final_hold not in snapshot.contained_holds


def _bind_real_node_storage(fixture):
    """Use actual Node storage handlers, with no server or child process."""
    node = object.__new__(NodeServer)
    incarnation = fixture.manifest.header.node_incarnation
    node.node_id = incarnation.node_id
    node._node_pid = incarnation.node_pid
    node._registration_epoch = incarnation.registration_epoch
    node._state_lock = threading.RLock()
    node._object_store = fixture.store
    node._object_manager = ObjectManager(node.node_id, fixture.store)
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._local_replica_write_claims = {}
    node._object_localization_locks = {}
    node._owner_death_fences = {}
    node._output_publication_journal = fixture.journal
    fixture.adapter._seal_replica = node._seal_output_publication_replica
    fixture.adapter._drop_replica = node._drop_output_publication_replica
    return node


@pytest.mark.parametrize("refs,stored", ((True, True), (False, True), (True, False)))
def test_single_output_uses_owner_registration_and_local_complete(refs, stored):
    fixture = _Fixture(refs=refs, stored=stored)
    fixture.prepare()
    assert fixture.events[0] == "owner-register"
    assert fixture.events.count("prepare") == fixture.events.count("promote") == (2 if refs else 0)
    assert fixture.events.count("seal") == int(stored)
    assert fixture.ledger.available == ResourceVector({"CPU": 0})
    envelope = fixture.complete()
    assert envelope == fixture.values.envelope
    assert fixture.releases == 1
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert "terminal" not in fixture.events
    assert fixture.handoffs.query(fixture.id).complete is None
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert fixture.journal.materialized_result(fixture.id, 0).inline_data == (None if stored else (fixture.values.payload))
    if stored:
        assert fixture.store.get((fixture.manifest.publication_id).object_id) == (fixture.values.payload)
    else:
        assert fixture.store.used_bytes == 0
    _assert_metadata(fixture.handoffs.query(fixture.id))
    _assert_metadata(fixture.journal.snapshot(fixture.id))
    assert (envelope.result.object_id.return_index) == (0)
    assert envelope.result == fixture.values.result
    assert not hasattr(envelope, 'results')


@pytest.mark.parametrize("stage", ("owner-register", "prepare", "seal", "promote"))
def test_effect_then_lost_ack_replays_the_exact_frozen_publication(stage):
    fixture = _Fixture()
    fixture.fault = stage
    with pytest.raises(TimeoutError, match=stage):
        fixture.prepare()
    assert fixture.journal.snapshot(fixture.id).complete is None
    fixture.prepare()
    fixture.prepare()
    assert fixture.complete() == fixture.values.envelope
    assert fixture.releases == 1
    for transfer in (fixture.manifest.value.transfers):
        snapshot = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
        assert transfer.final_hold in snapshot.contained_holds
        assert transfer.provisional_hold not in snapshot.contained_holds


def test_terminal_failure_does_not_hold_cpu_or_repeat_complete():
    fixture = _Fixture()
    fixture.prepare()
    fixture.complete()
    fixture.fault = "terminal"
    with pytest.raises(TimeoutError):
        fixture.adapter.report_terminal(fixture.id)
    assert fixture.releases == 1
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert fixture.complete() == fixture.values.envelope
    assert fixture.releases == 1
    assert fixture.adapter.report_terminal(fixture.id)
    assert fixture.adapter.pending_terminal_reports() == ()
    assert not fixture.adapter.report_terminal(fixture.id)
    fixture.complete()
    assert fixture.adapter.pending_terminal_reports() == ()


def test_owner_adoption_can_precede_terminal_outbox_without_reopening_forward_work():
    fixture = _Fixture()
    fixture.prepare()
    fixture.complete()
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "owner-cas")
    # Owner can receive Complete via the exact envelope, independently of the
    # Node's reporting outbox; this table records that existing fact only.
    fixture.handoffs.record_complete(fixture.values.witness)
    fixture.handoffs.adopt(proof)
    assert fixture.adapter.report_terminal(fixture.id)
    assert fixture.adapter.pending_terminal_reports() == ()
    before = tuple(fixture.events)
    fixture.prepare()
    assert tuple(fixture.events) == before


def test_callback_failure_after_local_complete_never_authorizes_rollback():
    fixture = _Fixture()
    fixture.prepare()
    fixture.fault = "commit"
    with pytest.raises(TimeoutError):
        fixture.complete()
    assert fixture.releases == 1
    assert fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert fixture.adapter.pending_lease_completions() == (fixture.values.witness,)
    with pytest.raises(OutputPublicationJournalStateError, match="forbidden"):
        fixture.rollback_all()
    assert fixture.complete() == fixture.values.envelope
    assert fixture.releases == 1
    assert fixture.adapter.pending_lease_completions() == ()


def test_journal_complete_effect_then_error_still_records_outbox_and_releases_cpu(monkeypatch):
    fixture = _Fixture()
    fixture.prepare()
    real = fixture.journal.complete

    def effect_then_error(publication_id, witness):
        real(publication_id, witness)
        raise TimeoutError("journal Complete reply lost")

    monkeypatch.setattr(fixture.journal, "complete", effect_then_error)
    with pytest.raises(TimeoutError, match="journal Complete"):
        fixture.complete()
    assert fixture.releases == 1
    assert fixture.adapter.pending_lease_completions() == ()
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert fixture.adapter.report_terminal(fixture.id)


def test_supervisor_converges_lease_after_payload_retirement_and_terminal_report():
    fixture = _Fixture()
    fixture.prepare()
    fixture.fault = "commit"
    with pytest.raises(TimeoutError):
        fixture.complete()
    assert fixture.adapter.report_terminal(fixture.id)
    fixture.journal.retire_completed(OutputPublicationAdoptionProof(
        fixture.values.witness, fixture.values.owner, "adopted",
    ))
    assert fixture.adapter.pending_terminal_reports() == ()
    assert fixture.adapter.pending_lease_completions() == (fixture.values.witness,)
    fixture.adapter.converge_completed(fixture.id, commit_lease=fixture.commit)
    assert fixture.adapter.pending_lease_completions() == ()
    assert fixture.releases == 1
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()


@pytest.mark.parametrize("stage", ("owner-register", "prepare", "seal", "promote"))
def test_precomplete_rollback_compensates_intended_effects_without_ack(stage):
    fixture = _Fixture()
    fixture.fault = stage
    with pytest.raises(TimeoutError):
        fixture.prepare()
    tombstone = fixture.rollback_all()
    assert fixture.handoffs.query(fixture.id).phase is OutputHandoffPhase.ABORTED
    assert fixture.journal.snapshot(fixture.id).rollback_tombstone == tombstone
    assert fixture.rollback_all() == tombstone
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    fixture.assert_no_pins_or_bytes()
    with pytest.raises(OutputPublicationJournalStateError):
        fixture.prepare()


@pytest.mark.parametrize("stage", ("drop", "release", "rollback-report"))
def test_rollback_ack_loss_retains_same_effect_and_resumes(stage):
    fixture = _Fixture()
    fixture.prepare()
    fixture.fault = stage
    with pytest.raises(TimeoutError):
        fixture.rollback_all()
    assert len(fixture.adapter.pending_rollbacks()) == 1
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()
    assert fixture.adapter.pending_terminal_reports() == ()
    assert fixture.adapter.pending_rollbacks() == ()


def test_partial_unsealed_local_write_is_dropped_by_intent_rollback():
    fixture = _Fixture()

    def partial_write(_effect, descriptor, payload):
        fixture.store.create(descriptor.object_id, descriptor.size_bytes)
        fixture.store.write(descriptor.object_id, payload[:3])
        raise TimeoutError("local write interrupted before seal")

    fixture.adapter._seal_replica = partial_write
    with pytest.raises(TimeoutError, match="before seal"):
        fixture.prepare()
    slot_id = (fixture.manifest.publication_id).object_id
    assert fixture.store.contains(slot_id, sealed_only=False)
    assert not fixture.store.contains(slot_id)
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()


@pytest.mark.parametrize('wrong', ('length', 'checksum', 'empty-tuple', 'tuple', 'list', 'bytearray', 'memoryview', 'none'))
def test_bad_single_output_bytes_has_zero_journal_or_external_effects(wrong):
    fixture = _Fixture()
    exact = fixture.values.payload
    payload = {
        'length': b'wrong-output',
        'checksum': b'x' * len(exact),
        'empty-tuple': (),
        'tuple': (exact,),
        'list': [exact],
        'bytearray': bytearray(exact),
        'memoryview': memoryview(exact),
        'none': None,
    }[wrong]
    error = OutputPublicationConflictError if wrong in ('length', 'checksum') else TypeError
    with pytest.raises(error, match='payload'):
        fixture.adapter.prepare(fixture.manifest, payload)
    assert fixture.events == []
    assert fixture.journal.publication_ids() == ()
    assert fixture.handoffs.snapshots() == ()
    assert fixture.store.used_bytes == 0


def test_wrong_child_echo_is_not_acknowledged_and_is_compensated():
    fixture = _Fixture()
    real = fixture.adapter._prepare_child

    def wrong_echo(address, request):
        actual = real(address, request)
        other = (fixture.manifest.value).transfers[1]
        return protocol.StoredContainedPinReply(
            protocol.PrepareStoredContainedPin(other, other.contained_owner_worker_id),
            actual.disposition,
        )

    fixture.adapter._prepare_child = wrong_echo
    with pytest.raises(OutputPublicationConflictError, match="identity"):
        fixture.prepare()
    snapshot = fixture.journal.snapshot(fixture.id)
    assert not any(ack.effect.stage is Stage.PREPARE for ack in snapshot.acknowledgements)
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()


def test_nonblocking_ticket_prevents_reentrant_rollback_before_forward_ack():
    fixture = _Fixture()
    real = fixture.adapter._prepare_child
    observed = []

    def reentrant(address, request):
        with pytest.raises(OutputPublicationBusy):
            fixture.adapter.rollback(fixture.id, "conflicting-driver")
        observed.append(True)
        return real(address, request)

    fixture.adapter._prepare_child = reentrant
    fixture.prepare()
    assert len(observed) == 2
    assert fixture.complete() == fixture.values.envelope


def test_aborted_owner_handoff_does_not_authorize_any_new_effect():
    fixture = _Fixture()
    fixture.fault = "owner-register"
    with pytest.raises(TimeoutError):
        fixture.prepare()
    assert fixture.handoffs.abort(fixture.id, "owner-fenced")
    with pytest.raises(OutputHandoffStateError, match="aborted"):
        fixture.prepare()
    assert set(fixture.events) == {"owner-register"}
    assert fixture.store.used_bytes == 0
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()


def test_drop_callback_cannot_mutate_the_expected_slot_identity():
    fixture = _Fixture()
    fixture.prepare()
    real = fixture.adapter._drop_replica

    def drift(_effect, request):
        # Simulate a callback mutating its arguments and echoing that drift.
        # The local manifest snapshot must not become the ACK authority.
        object.__setattr__(request.object_id, "return_index", 9)
        return protocol.DropObjectReplicaReply(
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum, protocol.DropObjectReplicaStatus.DROPPED,
        )

    fixture.adapter._drop_replica = drift
    with pytest.raises(OutputPublicationConflictError, match="replica identity"):
        fixture.rollback_all()
    assert fixture.journal.next_rollback_effect(fixture.id).stage is Stage.SLOT_DROP
    assert fixture.store.used_bytes > 0
    fixture.adapter._drop_replica = real
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()


def test_owner_adoption_then_gc_releases_single_output_and_each_child_hold():
    """Actual owner CAS and child releases compose with physical Node Drop.

    Callback transport is synchronous; no Worker execution or GCS operation
    is claimed. Both final child holds must remain live until explicit GC.
    """
    fixture = _Fixture()
    node = _bind_real_node_storage(fixture)
    values = fixture.values
    output_owner = ObjectOwnerTable()
    spec = protocol.TaskSpec(
        values.job, values.task, values.attempt,
        protocol.FunctionKey(values.job, __name__, "producer", "v1"),
        (), 1, ResourceVector(), values.owner,
    )
    output_owner.register_task_outputs(spec, local_tokens=("outer-0",))
    fixture.prepare()
    envelope = fixture.complete()
    plan = OutputOwnerPublicationPlan(values.execution, envelope)
    assert output_owner.commit_output_publication(plan).committed
    proof = OutputPublicationAdoptionProof(values.witness, values.owner, "one-batch-cas")
    fixture.handoffs.record_complete(values.witness)
    fixture.handoffs.adopt(proof)
    fixture.journal.retire_completed(proof)
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()
    assert fixture.store.used_bytes > 0  # reply retirement is not physical GC
    assert fixture.adapter.report_terminal(fixture.id)
    for transfer in (fixture.manifest.value).transfers:
        assert transfer.final_hold in fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id).contained_holds

    object_id, value = fixture.id.object_id, fixture.manifest.value
    assert output_owner.release_local_reference(object_id, 'outer-0')
    collection = output_owner.begin_output_publication_collection(object_id, collection_id='collect-slot-0')
    assert collection is not None
    for transfer in (value.transfers):
        request = protocol.ReleaseContainedReference(transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold)
        assert (fixture.release_child(transfer.contained_owner_address, request).accepted)
    if value.tier is protocol.ResultStorage.OBJECT_STORE:
        drop = protocol.DropObjectReplica(object_id, fixture.id.attempt_id, values.owner, values.node, value.checksum)
        assert node._handle_drop_object_replica(drop).status is protocol.DropObjectReplicaStatus.DROPPED
        assert node._handle_drop_object_replica(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    done = output_owner.complete_output_publication_collection(collection)
    assert done.collection.collected
    assert done.collection.contained_releases == tuple(sorted(value.edges))
    assert output_owner.complete_output_publication_collection(collection).collection == done.collection
    assert not output_owner._entries
    fixture.assert_no_pins_or_bytes()


def test_node_effect_adapter_uses_existing_node_storage_authority_for_single_complete():
    fixture = _Fixture()
    node = _bind_real_node_storage(fixture)
    fixture.prepare()
    (object_id, value) = (fixture.id.object_id, fixture.manifest.value)
    assert node._sealed_metadata == {object_id: (
        fixture.id.attempt_id, fixture.values.owner, value.size_bytes, value.checksum,
    )}
    assert node._local_replica_write_claims == {}
    assert fixture.complete() == fixture.values.envelope
    assert fixture.releases == 1
    assert fixture.store.get(object_id) == (fixture.values.payload)


def test_real_node_partial_write_claim_composes_with_journal_reverse_rollback(monkeypatch):
    fixture = _Fixture()
    node = _bind_real_node_storage(fixture)
    real_write = fixture.store.write

    def partial_write(object_id, payload):
        real_write(object_id, payload[:2])
        raise TimeoutError("partial Node write before seal")

    monkeypatch.setattr(fixture.store, "write", partial_write)
    with pytest.raises(TimeoutError, match="partial Node"):
        fixture.prepare()
    (object_id, value) = (fixture.id.object_id, fixture.manifest.value)
    assert object_id in node._local_replica_write_claims
    assert not node._sealed_metadata
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()
    assert node._local_replica_write_claims == {}
    assert node._dropped_metadata[object_id] == (
        fixture.id.attempt_id, fixture.values.owner, value.checksum,
    )
