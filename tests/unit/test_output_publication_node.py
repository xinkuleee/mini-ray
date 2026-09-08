"""Small, synchronous Node composition tests: no threads, sockets or waits.

Each case has two tiny output slots, at most four child transfers, one in-memory
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
from miniray.contained_cycle import ContainedReferenceGraphAuthority
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
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.ownership import ObjectOwnerTable, OutputOwnerPublicationPlan
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector
from miniray.stored_publication import BorrowedContainedSource
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
    def __init__(self, *, refs=True, target=False):
        self.values = _Values(refs=refs, target=target)
        self.manifest = self.values.manifest
        self.id = self.values.publication_id
        self.journal = OutputPublicationJournal()
        self.recovery = OutputPublicationRecoveryAuthority()
        self.graph = ContainedReferenceGraphAuthority()
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
        for slot in self.manifest.slots:
            for transfer in slot.transfers:
                table = self.child_owners.setdefault(transfer.contained_owner_worker_id, ObjectOwnerTable())
                table.register(transfer.contained_object_id, local_token="source-live")
                if isinstance(transfer.source, BorrowedContainedSource):
                    source = transfer.source.original_source
                    root_borrower = (self.values.executor, "upstream-root")
                    table.add_borrowed_reference(transfer.contained_object_id, root_borrower)
                    table.retain_borrowed_reference_for_task(transfer.contained_object_id, root_borrower, source.hold)
                    table.acquire_exported_reference(
                        transfer.contained_object_id, source, transfer.source.owner_table_token
                    )
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.intent, arm_complete=self.arm,
            report_terminal=self.terminal, report_rollback=self.rollback_report,
            prepare_child=self.prepare_child, promote_child=self.promote_child,
            release_child=self.release_child, prepare_graph=self.prepare_graph,
            abort_graph=self.abort_graph, seal_replica=self.seal, drop_replica=self.drop,
        )

    def hit(self, stage):
        self.events.append(stage)
        if self.fault == stage:
            self.fault = None
            raise TimeoutError("injected effect-then-lost-ACK: " + stage)

    def intent(self, manifest):
        ack = self.recovery.report_intent(manifest)
        self.hit("intent")
        return ack

    def arm(self, publication_id, digest):
        ack = self.recovery.arm_complete(publication_id, digest)
        self.hit("arm")
        return ack

    def terminal(self, witness):
        ack = self.recovery.report_terminal(witness)
        self.hit("terminal")
        return ack

    def rollback_report(self, tombstone, *, manifest):
        ack = self.recovery.report_rollback(tombstone, manifest=manifest)
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

    def prepare_graph(self, request):
        receipt = self.graph.prepare_manifest(request.manifest)
        self.hit("graph")
        return protocol.ContainedGraphReply(request, receipt)

    def abort_graph(self, request):
        receipt = self.graph.abort_manifest(request.manifest)
        self.hit("abort")
        return protocol.ContainedGraphReply(request, receipt)

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
        self.adapter.prepare(self.manifest, self.values.payloads)

    def complete(self):
        return self.adapter.complete(self.id, commit_lease=self.commit)

    def rollback_all(self):
        # One graph + two slots + eight hold inverses is the maximum here.
        result = self.adapter.rollback(self.id, "test-rollback", max_effects=12)
        assert result is not None
        return result

    def assert_no_pins_or_bytes(self):
        assert self.store.used_bytes == 0
        assert self.journal.snapshot(self.id).retained_result_slots == ()
        for slot in self.manifest.slots:
            for transfer in slot.transfers:
                snapshot = self.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
                assert transfer.provisional_hold not in snapshot.contained_holds
                assert transfer.final_hold not in snapshot.contained_holds
        assert not self.graph.snapshot().prepared_edges
        assert not self.graph.snapshot().committed_edges


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


@pytest.mark.parametrize("refs,target", ((True, False), (False, False), (True, True)))
def test_mixed_batch_uses_one_graph_one_arm_and_local_complete(refs, target):
    fixture = _Fixture(refs=refs, target=target)
    fixture.prepare()
    assert fixture.events[0] == "intent" and fixture.events[-1] == "arm"
    assert fixture.events.count("graph") == int(refs)
    assert fixture.events.count("seal") == 1
    assert fixture.ledger.available == ResourceVector({"CPU": 0})
    envelope = fixture.complete()
    assert envelope == fixture.values.envelope
    assert fixture.releases == 1
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert "terminal" not in fixture.events
    assert fixture.recovery.snapshot(fixture.id).complete is None
    assert fixture.adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert fixture.journal.materialized_result(fixture.id, 0).inline_data == fixture.values.payloads[0]
    assert fixture.store.get(fixture.manifest.slots[1].object_id) == fixture.values.payloads[1]
    _assert_metadata(fixture.recovery.snapshot(fixture.id))
    _assert_metadata(fixture.journal.snapshot(fixture.id))
    if target:
        assert tuple(result.object_id.return_index for result in envelope.results) == (1, 3)


@pytest.mark.parametrize("stage", ("intent", "prepare", "graph", "seal", "promote", "arm"))
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
    for slot in fixture.manifest.slots:
        for transfer in slot.transfers:
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
    fixture.recovery.report_adopted(proof)
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


@pytest.mark.parametrize("stage", ("intent", "prepare", "graph", "seal", "promote", "arm"))
def test_precomplete_rollback_compensates_intended_effects_without_ack(stage):
    fixture = _Fixture()
    fixture.fault = stage
    with pytest.raises(TimeoutError):
        fixture.prepare()
    tombstone = fixture.rollback_all()
    assert fixture.recovery.snapshot(fixture.id).rollback == tombstone
    assert fixture.rollback_all() == tombstone
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    fixture.assert_no_pins_or_bytes()
    with pytest.raises(OutputPublicationJournalStateError):
        fixture.prepare()


@pytest.mark.parametrize("stage", ("abort", "drop", "release", "rollback-report"))
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
    slot_id = fixture.manifest.slots[1].object_id
    assert fixture.store.contains(slot_id, sealed_only=False)
    assert not fixture.store.contains(slot_id)
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()


def test_later_slot_bad_bytes_has_zero_journal_or_external_effects():
    fixture = _Fixture()
    with pytest.raises(OutputPublicationConflictError, match="payload"):
        fixture.adapter.prepare(fixture.manifest, (fixture.values.payloads[0], b"wrong-later-slot"))
    assert fixture.events == []
    assert fixture.journal.publication_ids() == ()
    assert fixture.recovery.publication_ids() == ()


def test_wrong_child_echo_is_not_acknowledged_and_is_compensated():
    fixture = _Fixture()
    real = fixture.adapter._prepare_child

    def wrong_echo(address, request):
        actual = real(address, request)
        other = fixture.manifest.slots[1].transfers[0]
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
    assert len(observed) == 4
    assert fixture.complete() == fixture.values.envelope


def test_death_fenced_intent_does_not_authorize_any_new_effect():
    fixture = _Fixture()
    fixture.fault = "intent"
    with pytest.raises(TimeoutError):
        fixture.prepare()
    node = fixture.manifest.header.node_incarnation
    fixture.recovery.freeze_node_death(protocol.NodeDeathRecord(
        "publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
        8, 1, protocol.NodeDeathReason.PROCESS_EXIT, "observed exit",
    ))
    with pytest.raises(OutputPublicationRemoteError, match="fenced"):
        fixture.prepare()
    assert set(fixture.events) == {"intent"}
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


def test_mixed_batch_owner_adoption_then_per_slot_gc_preserves_sibling_child_holds():
    """Actual pure authorities compose without N independent publications.

    This deliberately does not claim the Worker/Node/GCS/Core RPC wiring is
    enabled.  It checks the new journal, graph, child owner and output owner
    contracts agree on one whole manifest and per-container cleanup.
    """
    fixture = _Fixture()
    values = fixture.values
    output_owner = ObjectOwnerTable()
    spec = protocol.TaskSpec(
        values.job, values.task, values.attempt,
        protocol.FunctionKey(values.job, __name__, "producer", "v1"),
        (), 2, ResourceVector(), values.owner,
    )
    output_owner.register_task_outputs(spec, local_tokens=("outer-0", "outer-1"))
    fixture.prepare()
    envelope = fixture.complete()
    graph = envelope.manifest.to_graph_manifest()
    assert fixture.graph.commit_manifest(graph).manifest == graph
    plan = OutputOwnerPublicationPlan(values.execution, envelope)
    assert output_owner.commit_output_publication(plan).committed
    proof = OutputPublicationAdoptionProof(values.witness, values.owner, "one-batch-cas")
    fixture.recovery.report_adopted(proof)
    fixture.journal.retire_completed(proof)
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()
    assert fixture.store.used_bytes > 0  # reply retirement is not physical GC
    assert fixture.adapter.report_terminal(fixture.id)
    second_before = output_owner.snapshot(values.publication_id.output_ids[1])

    for index, slot in enumerate(fixture.manifest.slots):
        assert output_owner.release_local_reference(slot.object_id, "outer-{}".format(index))
        collection = output_owner.begin_output_publication_collection(
            slot.object_id, collection_id="collect-slot-{}".format(index)
        )
        assert collection is not None
        for transfer in slot.transfers:
            request = protocol.ReleaseContainedReference(
                transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold
            )
            assert fixture.release_child(transfer.contained_owner_address, request).accepted
        request = protocol.ReleaseContainedGraphContainer(graph, slot.object_id)
        graph_reply = protocol.ContainedGraphReply(
            request, fixture.graph.release_manifest_container(graph, slot.object_id)
        )
        assert graph_reply.receipt.released_edges == slot.edges
        if slot.tier is protocol.ResultStorage.OBJECT_STORE:
            assert fixture.store.delete(slot.object_id)
        done = output_owner.complete_output_publication_collection(collection, graph_reply.receipt)
        assert done.collection.collected
        if index == 0:
            assert output_owner.snapshot(values.publication_id.output_ids[1]) == second_before
            for transfer in fixture.manifest.slots[1].transfers:
                assert transfer.final_hold in fixture.child_owners[transfer.contained_owner_worker_id].snapshot(
                    transfer.contained_object_id
                ).contained_holds
    assert not output_owner._entries
    fixture.assert_no_pins_or_bytes()


def test_node_effect_adapter_uses_existing_node_storage_authority_for_mixed_complete():
    fixture = _Fixture()
    node = _bind_real_node_storage(fixture)
    fixture.prepare()
    slot = fixture.manifest.slots[1]
    assert node._sealed_metadata == {slot.object_id: (
        fixture.id.attempt_id, fixture.values.owner, slot.size_bytes, slot.checksum,
    )}
    assert node._local_replica_write_claims == {}
    assert fixture.complete() == fixture.values.envelope
    assert fixture.releases == 1
    assert fixture.store.get(slot.object_id) == fixture.values.payloads[1]


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
    slot = fixture.manifest.slots[1]
    assert slot.object_id in node._local_replica_write_claims
    assert not node._sealed_metadata
    fixture.rollback_all()
    fixture.assert_no_pins_or_bytes()
    assert node._local_replica_write_claims == {}
    assert node._dropped_metadata[slot.object_id] == (
        fixture.id.attempt_id, fixture.values.owner, slot.checksum,
    )
