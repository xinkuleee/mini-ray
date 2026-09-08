"""Small synchronous contracts for the unified Node publication journal."""

from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
from enum import Enum

import pytest

from miniray import protocol
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedGraphManifestReceipt,
    ContainedGraphTransactionState,
)
from miniray.errors import ProtocolError
from miniray.ids import JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationID, OutputPublicationManifest,
)
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationAckDisposition,
    OutputPublicationAdoptionProof, OutputPublicationEffect, OutputPublicationJournal,
    OutputPublicationJournalState, OutputPublicationJournalStateError,
    OutputPublicationPayloadRetired, OutputPublicationSlotCleanupProof,
    OutputPublicationStage as Stage, UnknownOutputPublicationError,
)
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest
from tests.unit.test_output_publication import _Fixture as _Values, _id


pytestmark = pytest.mark.unit


def _metadata(value):
    if type(value) in (JobID, TaskID, LeaseID, NodeID, WorkerID):
        assert type(value.value) is bytes and len(value.value) == 16
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, protocol.ResultDescriptor))
    if value is None or isinstance(value, (str, int, Enum)):
        return
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            _metadata(getattr(value, item.name))
    elif isinstance(value, dict):
        for key, item in value.items():
            _metadata(key)
            _metadata(item)
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _metadata(item)
    else:
        raise AssertionError(type(value).__name__)


class _Fixture:
    def __init__(self, *, refs=True, target=False):
        self.values = _Values(refs=refs, target=target)
        self.journal = OutputPublicationJournal()
        self.manifest = self.values.manifest
        self.id = self.manifest.publication_id
        assert self.journal.open(self.manifest)

    def effect(self, stage, slot=None, transfer=None):
        return OutputPublicationEffect(self.id, self.manifest.manifest_digest, stage, slot, transfer)

    def graph_receipt(self, *, abort=False, replay=False):
        return ContainedGraphManifestReceipt(
            self.manifest.to_graph_manifest(),
            ContainedGraphTransactionState.ABORTED if abort else ContainedGraphTransactionState.PREPARED,
            (ContainedGraphManifestDisposition.ALREADY_ABORTED if abort
             else ContainedGraphManifestDisposition.ALREADY_PREPARED) if replay
            else ContainedGraphManifestDisposition.APPLIED,
        )

    def prepare(self):
        journal = self.journal
        effect = journal.begin_intent(self.id)
        journal.ack_intent(OutputPublicationAck(effect))
        for slot_index, slot in enumerate(self.manifest.slots):
            for transfer_index in range(len(slot.transfers)):
                effect = journal.begin_prepare(self.id, slot_index, transfer_index)
                journal.ack_prepared(OutputPublicationAck(effect))

    def graph(self):
        self.prepare()
        effect = self.journal.begin_graph_prepare(self.id)
        if effect is not None:
            self.journal.ack_graph_prepared(OutputPublicationAck(effect), self.graph_receipt())

    def materialize(self):
        self.graph()
        for index, descriptor in enumerate(self.values.results):
            effect = self.journal.begin_materialize(self.id, index)
            self.journal.ack_materialized(OutputPublicationAck(effect), descriptor)

    def promote(self):
        self.materialize()
        for slot_index, slot in enumerate(self.manifest.slots):
            for transfer_index in range(len(slot.transfers)):
                effect = self.journal.begin_promote(self.id, slot_index, transfer_index)
                self.journal.ack_promoted(OutputPublicationAck(effect))

    def complete(self):
        self.promote()
        effect = self.journal.begin_arm_complete(self.id)
        self.journal.ack_arm_complete(OutputPublicationAck(effect))
        return self.journal.complete(self.id, self.values.witness)

    def rollback(self):
        effects = []
        # Exact tiny manifests have a finite compensation list.  A reducer
        # regression cannot turn this pure test into an unbounded wait/loop.
        limit = len(self.journal.snapshot(self.id).rollback.effects)
        for _ in range(limit + 1):
            effect = self.journal.next_rollback_effect(self.id)
            if effect is None:
                return tuple(effects)
            effects.append(effect)
            self.journal.ack_rollback(
                OutputPublicationAck(effect),
                self.graph_receipt(abort=True) if effect.stage is Stage.GRAPH_ABORT else None,
            )
        pytest.fail("rollback failed to consume its bounded effect list")


def test_one_journal_completes_mixed_slots_with_exact_data_plane_replay():
    fixture = _Fixture()
    journal = fixture.journal
    assert not journal.open(fixture.manifest)
    opened = journal.snapshot(fixture.id)
    assert opened.state is OutputPublicationJournalState.ACTIVE
    assert not opened.ready_to_arm and not opened.ready_to_complete
    envelope = fixture.complete()
    assert envelope == fixture.values.envelope
    completed = journal.snapshot(fixture.id)
    assert completed.state is OutputPublicationJournalState.COMPLETED
    assert completed.complete == fixture.values.witness
    assert completed.materialized_slots == completed.retained_result_slots == (0, 1)
    assert not completed.ready_to_complete
    assert journal.complete(fixture.id, fixture.values.witness) == envelope
    assert journal.materialized_result(fixture.id, 0) == fixture.values.results[0]
    _metadata(completed)
    with journal.linearize(fixture.id):
        assert journal.complete(fixture.id, fixture.values.witness) == envelope
    assert journal.publication_ids() == (fixture.id,)


@pytest.mark.parametrize("refs,target", ((False, False), (False, True), (True, True)))
def test_no_refs_and_targeted_indices_reuse_same_lifecycle(refs, target):
    fixture = _Fixture(refs=refs, target=target)
    envelope = fixture.complete()
    if target:
        assert tuple(slot.object_id.return_index for slot in envelope.manifest.slots) == (1, 3)
    graph_effects = tuple(value for value in fixture.journal.snapshot(fixture.id).intents if value.stage is Stage.GRAPH_PREPARE)
    assert len(graph_effects) == (1 if refs else 0)
    assert envelope == fixture.values.envelope


def test_single_output_uses_same_journal_and_arm_after_no_graph():
    fixture = _Fixture(refs=False)

    execution = TaskExecutionKey(
        TaskOutputManifest.for_task(fixture.id.task_id, 1), fixture.id.attempt_id
    )
    manifest = OutputPublicationManifest.create(
        replace(fixture.manifest.header, publication_id=OutputPublicationID(fixture.id.lease_id, execution)),
        fixture.manifest.slots[:1],
    )
    journal = OutputPublicationJournal()
    journal.open(manifest)
    publication_id = manifest.publication_id
    journal.ack_intent(OutputPublicationAck(journal.begin_intent(publication_id)))
    assert journal.begin_graph_prepare(publication_id) is None
    journal.ack_materialized(
        OutputPublicationAck(journal.begin_materialize(publication_id, 0)), fixture.values.results[0]
    )
    journal.ack_arm_complete(OutputPublicationAck(journal.begin_arm_complete(publication_id)))
    envelope = journal.complete(publication_id, OutputPublicationCompleteWitness.for_manifest(manifest))
    assert envelope.results == fixture.values.results[:1]


def test_intent_prepare_graph_materialize_promote_and_arm_are_ordered_gates():
    fixture = _Fixture()
    journal = fixture.journal
    initial = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError, match="intent ACK"):
        journal.begin_prepare(fixture.id, 0, 0)
    with pytest.raises(OutputPublicationJournalStateError, match="before its intent"):
        journal.ack_intent(OutputPublicationAck(fixture.effect(Stage.INTENT)))
    assert journal.snapshot(fixture.id) == initial
    journal.ack_intent(OutputPublicationAck(journal.begin_intent(fixture.id)))
    with pytest.raises(OutputPublicationJournalStateError, match="provisional"):
        journal.begin_graph_prepare(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError):
        journal.begin_materialize(fixture.id, 0)
    fixture.prepare()
    with pytest.raises(OutputPublicationJournalStateError, match="graph prepare ACK"):
        journal.begin_materialize(fixture.id, 0)
    graph = journal.begin_graph_prepare(fixture.id)
    journal.ack_graph_prepared(OutputPublicationAck(graph), fixture.graph_receipt())
    first = journal.begin_materialize(fixture.id, 0)
    journal.ack_materialized(OutputPublicationAck(first), fixture.values.results[0])
    with pytest.raises(OutputPublicationJournalStateError, match="every materialization"):
        journal.begin_promote(fixture.id, 0, 0)
    second = journal.begin_materialize(fixture.id, 1)
    journal.ack_materialized(OutputPublicationAck(second), fixture.values.results[1])
    with pytest.raises(OutputPublicationJournalStateError, match="every promotion"):
        journal.begin_arm_complete(fixture.id)
    fixture.promote()
    assert journal.snapshot(fixture.id).ready_to_arm
    arm = journal.begin_arm_complete(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError, match="arm ACK"):
        journal.complete(fixture.id, fixture.values.witness)
    journal.ack_arm_complete(OutputPublicationAck(arm))
    assert journal.snapshot(fixture.id).ready_to_complete
    journal.complete(fixture.id, fixture.values.witness)


def test_exact_forward_replay_acks_do_not_mutate_or_duplicate_intents():
    fixture = _Fixture()
    journal = fixture.journal
    effect = journal.begin_intent(fixture.id)
    assert journal.begin_intent(fixture.id) == effect
    assert journal.ack_intent(OutputPublicationAck(effect))
    before = journal.snapshot(fixture.id)
    assert not journal.ack_intent(OutputPublicationAck(effect, OutputPublicationAckDisposition.ALREADY_APPLIED))
    assert journal.snapshot(fixture.id) == before
    fixture.graph()
    effect = journal.begin_materialize(fixture.id, 0)
    assert journal.ack_materialized(OutputPublicationAck(effect), fixture.values.results[0])
    before = journal.snapshot(fixture.id)
    assert not journal.ack_materialized(OutputPublicationAck(effect), replace(fixture.values.results[0]))
    assert journal.snapshot(fixture.id) == before


def test_valid_stage_ack_cannot_implicitly_create_prepare_or_arm_intent():
    fixture = _Fixture()
    journal = fixture.journal
    journal.ack_intent(OutputPublicationAck(journal.begin_intent(fixture.id)))
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError, match="before its intent"):
        journal.ack_prepared(OutputPublicationAck(fixture.effect(Stage.PREPARE, 0, 0)))
    assert journal.snapshot(fixture.id) == before
    fixture.promote()
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError, match="before its intent"):
        journal.ack_arm_complete(OutputPublicationAck(fixture.effect(Stage.ARM_COMPLETE)))
    assert journal.snapshot(fixture.id) == before


def test_wrong_manifest_stage_slot_and_graph_ack_have_zero_mutation():
    fixture = _Fixture(target=True)
    journal = fixture.journal
    fixture.prepare()
    effect = journal.begin_graph_prepare(fixture.id)
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationConflictError, match="exact publication"):
        journal.ack_graph_prepared(OutputPublicationAck(replace(effect, manifest_digest="ab" * 32)), fixture.graph_receipt())
    with pytest.raises(OutputPublicationConflictError, match="another stage"):
        journal.ack_promoted(OutputPublicationAck(effect))
    receipt = fixture.graph_receipt()
    for changed in (
        replace(receipt, manifest=replace(receipt.manifest, manifest_digest="ab" * 32)),
        replace(receipt, state=ContainedGraphTransactionState.COMMITTED),
    ):
        with pytest.raises(OutputPublicationConflictError):
            journal.ack_graph_prepared(OutputPublicationAck(effect), changed)
    assert journal.snapshot(fixture.id) == before
    for slot, transfer in ((2, 0), (0, 2), (True, 0), (-1, 0)):
        with pytest.raises((OutputPublicationConflictError, ValueError)):
            journal.begin_prepare(fixture.id, slot, transfer)
    journal.ack_graph_prepared(OutputPublicationAck(effect), receipt)
    materialize = journal.begin_materialize(fixture.id, 0)
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationConflictError, match="slot identity"):
        journal.ack_materialized(OutputPublicationAck(materialize), fixture.values.results[1])
    assert journal.snapshot(fixture.id) == before


def test_partial_materialization_rollback_covers_unacknowledged_effects_reverse():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.graph()
    first = journal.begin_materialize(fixture.id, 0)
    journal.ack_materialized(OutputPublicationAck(first), fixture.values.results[0])
    journal.begin_materialize(fixture.id, 1)  # seal may succeed; response was lost
    plan = journal.begin_rollback(fixture.id, "partial-seal")
    assert tuple((effect.stage, effect.slot_index, effect.transfer_index) for effect in plan.effects) == (
        (Stage.GRAPH_ABORT, None, None), (Stage.SLOT_DROP, 1, None),
        (Stage.SLOT_DROP, 0, None),
        (Stage.PROVISIONAL_RELEASE, 1, 1), (Stage.PROVISIONAL_RELEASE, 1, 0),
        (Stage.PROVISIONAL_RELEASE, 0, 1), (Stage.PROVISIONAL_RELEASE, 0, 0),
    )
    before = journal.snapshot(fixture.id)
    assert journal.begin_rollback(fixture.id, "partial-seal") == plan
    with pytest.raises(OutputPublicationJournalStateError):
        journal.ack_materialized(OutputPublicationAck(first), fixture.values.results[0])
    with pytest.raises(OutputPublicationJournalStateError, match="out of order"):
        journal.ack_rollback(OutputPublicationAck(plan.effects[1]))
    with pytest.raises(OutputPublicationConflictError, match="rollback identity"):
        journal.begin_rollback(fixture.id, "another-rollback")
    assert journal.snapshot(fixture.id) == before
    assert fixture.rollback() == plan.effects
    terminal = journal.snapshot(fixture.id)
    assert terminal.state is OutputPublicationJournalState.RETIRED
    assert terminal.retained_result_slots == ()
    assert terminal.rollback_tombstone.plan == plan
    assert not journal.open(fixture.manifest)
    _metadata(journal._records)
    assert journal.next_rollback_effect(fixture.id) is None
    assert not journal.ack_rollback(OutputPublicationAck(plan.effects[0]), fixture.graph_receipt(abort=True, replay=True))
    assert journal.snapshot(fixture.id) == terminal


def test_partial_promotion_rollback_releases_possible_final_before_provisional():
    fixture = _Fixture()
    fixture.materialize()
    journal = fixture.journal
    first = journal.begin_promote(fixture.id, 0, 0)
    journal.ack_promoted(OutputPublicationAck(first))
    journal.begin_promote(fixture.id, 1, 1)  # applied promotion can lack ACK
    plan = journal.begin_rollback(fixture.id, "partial-promotion")
    assert tuple((item.stage, item.slot_index, item.transfer_index) for item in plan.effects[3:5]) == (
        (Stage.FINAL_RELEASE, 1, 1), (Stage.FINAL_RELEASE, 0, 0),
    )
    assert plan.effects[5].stage is Stage.PROVISIONAL_RELEASE
    fixture.rollback()
    _metadata(journal._records)


def test_prepare_or_graph_effect_without_ack_is_still_compensated():
    fixture = _Fixture()
    journal = fixture.journal
    journal.ack_intent(OutputPublicationAck(journal.begin_intent(fixture.id)))
    journal.begin_prepare(fixture.id, 0, 1)
    plan = journal.begin_rollback(fixture.id, "prepare-reply-lost")
    assert tuple((item.stage, item.slot_index, item.transfer_index) for item in plan.effects) == (
        (Stage.PROVISIONAL_RELEASE, 0, 1),
    )
    fixture.rollback()
    second = _Fixture()
    second.prepare()
    second.journal.begin_graph_prepare(second.id)
    assert second.journal.begin_rollback(second.id, "graph-reply-lost").effects[0].stage is Stage.GRAPH_ABORT
    second.rollback()


def test_before_first_effect_can_retire_without_rpc_but_armed_still_can_rollback():
    unopened_effects = _Fixture()
    plan = unopened_effects.journal.begin_rollback(unopened_effects.id, "cancelled-before-intent")
    assert plan.effects == ()
    snapshot = unopened_effects.journal.snapshot(unopened_effects.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.rollback_tombstone.acknowledgements == ()
    fixture = _Fixture(refs=False)
    fixture.promote()
    arm = fixture.journal.begin_arm_complete(fixture.id)
    fixture.journal.ack_arm_complete(OutputPublicationAck(arm))
    fixture.journal.begin_rollback(fixture.id, "worker-lost-before-local-complete")
    fixture.rollback()
    with pytest.raises(OutputPublicationJournalStateError):
        fixture.journal.complete(fixture.id, fixture.values.witness)


def test_late_arm_ack_cannot_resurrect_rollback_or_authorize_complete():
    fixture = _Fixture(refs=False)
    fixture.promote()
    journal = fixture.journal
    arm = journal.begin_arm_complete(fixture.id)
    journal.begin_rollback(fixture.id, "arm-response-lost-worker-died")
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError):
        journal.ack_arm_complete(OutputPublicationAck(arm))
    with pytest.raises(OutputPublicationJournalStateError):
        journal.complete(fixture.id, fixture.values.witness)
    assert journal.snapshot(fixture.id) == before
    fixture.rollback()
    assert journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    _metadata(journal._records)


def test_complete_fences_rollback_and_every_forward_effect():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.complete()
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationJournalStateError, match="forbidden after Complete"):
        journal.begin_rollback(fixture.id, "too-late")
    for call in (
        lambda: journal.begin_intent(fixture.id),
        lambda: journal.begin_prepare(fixture.id, 0, 0),
        lambda: journal.begin_materialize(fixture.id, 0),
        lambda: journal.begin_promote(fixture.id, 0, 0),
        lambda: journal.begin_arm_complete(fixture.id),
    ):
        with pytest.raises(OutputPublicationJournalStateError):
            call()
    with pytest.raises(OutputPublicationConflictError, match="Complete witness"):
        journal.complete(fixture.id, replace(fixture.values.witness, manifest_digest="ab" * 32))
    assert journal.snapshot(fixture.id) == before


def test_slot_cleanup_retires_only_one_payload_and_cannot_rebuild_partial_envelope():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.complete()
    proof = OutputPublicationSlotCleanupProof(
        fixture.values.witness, fixture.values.owner, 0, fixture.manifest.slots[0].object_id, "slot-zero-gc"
    )
    tombstone = journal.retire_slot(fixture.id, 0, proof)
    assert journal.retire_slot(fixture.id, 0, proof) == tombstone
    assert journal.materialized_result(fixture.id, 0) is None
    assert journal.materialized_result(fixture.id, 1) == fixture.values.results[1]
    snapshot = journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.COMPLETED
    assert snapshot.retained_result_slots == (1,)
    with pytest.raises(OutputPublicationPayloadRetired) as exc:
        journal.complete(fixture.id, fixture.values.witness)
    assert exc.value.tombstones == (tombstone,)
    _metadata(exc.value.tombstones)
    with pytest.raises(OutputPublicationConflictError, match="rebound"):
        journal.retire_slot(fixture.id, 0, replace(proof, cleanup_id="another-proof"))
    assert journal.snapshot(fixture.id) == snapshot
    other = OutputPublicationSlotCleanupProof(
        fixture.values.witness, fixture.values.owner, 1, fixture.manifest.slots[1].object_id, "slot-one-gc"
    )
    journal.retire_slot(fixture.id, 1, other)
    assert journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    _metadata(journal._records)


def test_all_inline_siblings_keep_independent_payload_custody():
    fixture = _Fixture(refs=False)
    values = fixture.values
    second = replace(values.slots[1], tier=protocol.ResultStorage.INLINE)
    manifest = OutputPublicationManifest.create(values.header, (values.slots[0], second))
    witness = OutputPublicationCompleteWitness.for_manifest(manifest)
    second_result = replace(values.results[1], storage=protocol.ResultStorage.INLINE, inline_data=values.payloads[1])
    expected = OutputPublicationEnvelope(manifest, witness, (values.results[0], second_result))
    journal = OutputPublicationJournal()
    journal.open(manifest)
    journal.ack_intent(OutputPublicationAck(journal.begin_intent(fixture.id)))
    for index, result in enumerate(expected.results):
        journal.ack_materialized(OutputPublicationAck(journal.begin_materialize(fixture.id, index)), result)
    journal.ack_arm_complete(OutputPublicationAck(journal.begin_arm_complete(fixture.id)))
    assert journal.complete(fixture.id, witness) == expected
    proof = OutputPublicationAdoptionProof(witness, values.owner, "all-inline-adopted")
    journal.retire_slot(fixture.id, 0, proof)
    assert journal.materialized_result(fixture.id, 0) is None
    assert journal.materialized_result(fixture.id, 1).inline_data == values.payloads[1]
    journal.retire_slot(fixture.id, 1, proof)
    _metadata(journal._records)


def test_whole_owner_adoption_clears_all_slots_and_retains_only_exact_metadata():
    fixture = _Fixture(target=True)
    journal = fixture.journal
    fixture.complete()
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "whole-owner-cas")
    tombstones = journal.retire_completed(proof)
    assert tuple(item.object_id.return_index for item in tombstones) == (1, 3)
    assert journal.retire_completed(proof) == tombstones
    assert journal.snapshot(fixture.id).retained_result_slots == ()
    assert journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    _metadata(journal._records)
    with pytest.raises(OutputPublicationPayloadRetired):
        journal.complete(fixture.id, fixture.values.witness)
    with pytest.raises(OutputPublicationConflictError, match="owner commit"):
        journal.retire_completed(replace(proof, owner_commit_id="changed-cas"))


def test_adoption_of_one_slot_never_discards_unretired_sibling_payload():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.complete()
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "adopted-batch")
    # Retire the STORED slot first: the INLINE sibling must retain its real bytes.
    journal.retire_slot(fixture.id, 1, proof)
    assert journal.materialized_result(fixture.id, 0).inline_data == fixture.values.payloads[0]
    assert journal.materialized_result(fixture.id, 1) is None
    before = journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationConflictError, match="owner commit"):
        journal.retire_slot(fixture.id, 0, replace(proof, owner_commit_id="wrong-cas"))
    assert journal.snapshot(fixture.id) == before
    journal.retire_completed(proof)
    _metadata(journal._records)


def test_payload_retirement_is_not_physical_replica_gc_or_a_replaceable_proof():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.complete()
    proof = OutputPublicationAdoptionProof(
        fixture.values.witness, fixture.values.owner, "owner-adopted-before-gc"
    )
    # Physical store state is owned by a different component.  No store,
    # callback, or Drop is passed to the journal's retirement boundary.
    stored_id = fixture.manifest.slots[1].object_id
    physical_replicas = {stored_id: fixture.values.payloads[1]}
    tombstone = journal.retire_slot(fixture.id, 1, proof)
    assert physical_replicas == {stored_id: fixture.values.payloads[1]}
    assert journal.materialized_result(fixture.id, 1) is None
    assert journal.materialized_result(fixture.id, 0).inline_data == fixture.values.payloads[0]
    before = journal.snapshot(fixture.id)
    later_gc = OutputPublicationSlotCleanupProof(
        fixture.values.witness, fixture.values.owner, 1, stored_id, "later-physical-gc"
    )
    with pytest.raises(OutputPublicationConflictError, match="slot retirement proof was rebound"):
        journal.retire_slot(fixture.id, 1, later_gc)
    assert journal.retire_slot(fixture.id, 1, proof) == tombstone
    assert journal.snapshot(fixture.id) == before
    assert not any(effect.stage is Stage.SLOT_DROP for effect in before.intents)


def test_conflicting_last_slot_prevents_partial_bulk_payload_retirement():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.complete()
    cleanup = OutputPublicationSlotCleanupProof(
        fixture.values.witness, fixture.values.owner, 1,
        fixture.manifest.slots[1].object_id, "slot-one-retired-first",
    )
    first_tombstone = journal.retire_slot(fixture.id, 1, cleanup)
    proof = OutputPublicationAdoptionProof(
        fixture.values.witness, fixture.values.owner, "late-whole-owner-adoption"
    )
    before = journal.snapshot(fixture.id)
    assert before.retained_result_slots == (0,)
    with pytest.raises(OutputPublicationConflictError, match="slot retirement proof was rebound"):
        journal.retire_completed(proof)
    assert journal.snapshot(fixture.id) == before
    assert journal.materialized_result(fixture.id, 0).inline_data == fixture.values.payloads[0]
    assert journal._records[fixture.id].adoption_proof is None
    assert journal.retire_slot(fixture.id, 1, cleanup) == first_tombstone


def test_payload_tombstones_and_diagnostics_do_not_alias_terminal_authority():
    fixture = _Fixture()
    journal = fixture.journal
    fixture.complete()
    proof = OutputPublicationAdoptionProof(
        fixture.values.witness, fixture.values.owner, "frozen-owner-cas"
    )
    returned = journal.retire_slot(fixture.id, 0, proof)
    before = journal.snapshot(fixture.id)
    object.__setattr__(returned.proof, "owner_commit_id", "tampered-caller-copy")
    object.__setattr__(proof, "owner_commit_id", "tampered-original-proof")
    assert journal.snapshot(fixture.id) == before
    exact = OutputPublicationAdoptionProof(
        fixture.values.witness, fixture.values.owner, "frozen-owner-cas"
    )
    journal.retire_completed(exact)
    with pytest.raises(OutputPublicationPayloadRetired) as exc:
        journal.complete(fixture.id, fixture.values.witness)
    assert tuple(value.slot_index for value in exc.value.tombstones) == (0, 1)
    object.__setattr__(exc.value.tombstones[0].proof, "owner_commit_id", "tampered-exception")
    _metadata(journal._records)
    assert all(
        value.proof.owner_commit_id == "frozen-owner-cas"
        for value in journal.snapshot(fixture.id).retired_slots
    )


def test_retirement_requires_local_complete_and_exact_owner_scope():
    fixture = _Fixture()
    journal = fixture.journal
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "cas")
    with pytest.raises(OutputPublicationJournalStateError, match="local Complete"):
        journal.retire_slot(fixture.id, 0, proof)
    fixture.complete()
    before = journal.snapshot(fixture.id)
    for wrong in (
        replace(proof, owner_worker_id=_id(WorkerID, 50)),
        replace(proof, complete=replace(proof.complete, manifest_digest="ab" * 32)),
    ):
        with pytest.raises(OutputPublicationConflictError, match="Complete or its owner"):
            journal.retire_slot(fixture.id, 0, wrong)
    with pytest.raises(TypeError, match="proof"):
        journal.retire_slot(fixture.id, 0, fixture.id)
    cleanup = OutputPublicationSlotCleanupProof(
        fixture.values.witness, fixture.values.owner, 1, fixture.manifest.slots[1].object_id, "slot-one"
    )
    with pytest.raises(OutputPublicationConflictError):
        journal.retire_slot(fixture.id, 0, cleanup)
    assert journal.snapshot(fixture.id) == before


def test_open_conflict_unknown_identity_and_mutated_inputs_fail_closed():
    fixture = _Fixture()
    journal = fixture.journal
    changed = OutputPublicationManifest.create(
        fixture.manifest.header, (replace(fixture.manifest.slots[0], checksum="ab" * 32), fixture.manifest.slots[1])
    )
    with pytest.raises(OutputPublicationConflictError, match="manifest was rebound"):
        journal.open(changed)
    with pytest.raises(UnknownOutputPublicationError):
        journal.snapshot(replace(fixture.id, lease_id=_id(LeaseID, 40)))
    effect = journal.begin_intent(fixture.id)
    altered = replace(effect)
    object.__setattr__(altered, "stage", "INTENT")
    with pytest.raises(TypeError):
        journal.ack_intent(OutputPublicationAck(altered))
    assert not journal.acknowledged(effect)
    fixture.graph()
    materialize = journal.begin_materialize(fixture.id, 0)
    descriptor = replace(fixture.values.results[0])
    object.__setattr__(descriptor, "inline_data", b"changed")
    with pytest.raises(ProtocolError):
        journal.ack_materialized(OutputPublicationAck(materialize), descriptor)
    assert journal.materialized_result(fixture.id, 0) is None


def test_graph_and_retirement_ack_revalidate_tampered_nested_ids():
    fixture = _Fixture()
    fixture.prepare()
    journal = fixture.journal
    graph_effect = journal.begin_graph_prepare(fixture.id)
    receipt = fixture.graph_receipt()
    object.__setattr__(receipt.manifest.ordered_edges[0].container_object_id, "return_index", False)
    with pytest.raises((ValueError, TypeError)):
        journal.ack_graph_prepared(OutputPublicationAck(graph_effect), receipt)
    assert not journal.acknowledged(graph_effect)
    empty_payload_receipt = fixture.graph_receipt()
    object.__setattr__(empty_payload_receipt, "released_edges", b"")
    with pytest.raises(TypeError, match="released_edges"):
        journal.ack_graph_prepared(OutputPublicationAck(graph_effect), empty_payload_receipt)
    journal.ack_graph_prepared(OutputPublicationAck(graph_effect), fixture.graph_receipt())
    fixture.complete()
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "cas")
    object.__setattr__(proof.owner_worker_id, "value", b"short")
    before = journal.snapshot(fixture.id)
    with pytest.raises(ValueError):
        journal.retire_slot(fixture.id, 0, proof)
    assert journal.snapshot(fixture.id) == before


def test_snapshots_and_data_plane_returns_do_not_alias_journal_authority():
    fixture = _Fixture()
    journal = fixture.journal
    envelope = fixture.complete()
    snapshot = journal.snapshot(fixture.id)
    object.__setattr__(snapshot.manifest.slots[0], "checksum", "ab" * 32)
    object.__setattr__(envelope.results[0], "inline_data", b"changed")
    cached = journal.materialized_result(fixture.id, 0)
    object.__setattr__(cached, "inline_data", b"changed-again")
    assert journal.complete(fixture.id, fixture.values.witness) == fixture.values.envelope
    assert journal.snapshot(fixture.id).manifest == fixture.manifest
