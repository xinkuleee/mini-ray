"""Pure contracts for the current owner-led, single-output Node journal.

Each case uses at most two journal records, one <= 32-byte output per record,
and at most two explicit child-transfer identities. ACKs and owner adoption
are validated pure inputs, not real RPC, child holds, lease release or owner
CAS evidence. Rollback consumes at most five effects in a bounded loop. No
Core/Node/Worker constructor, thread, socket, process, timer or wait is used.
"""

from __future__ import annotations

import hashlib
from dataclasses import fields, is_dataclass, replace
from enum import Enum

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationAckDisposition,
    OutputPublicationAdoptionProof, OutputPublicationEffect, OutputPublicationJournal,
    OutputPublicationJournalState, OutputPublicationJournalStateError,
    OutputPublicationPayloadRetired, OutputPublicationStage as Stage,
    UnknownOutputPublicationError,
)
from miniray.publication_sources import (
    BorrowedContainedSource, OwnedContainedSource, PreparedContainedTransfer,
)
from miniray.task_outputs import TaskExecution

pytestmark = pytest.mark.unit


def _id(kind, byte):
    return kind(bytes((byte,)) * 16)


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
    """An independent single-output fixture; no legacy graph/target adapter."""

    def __init__(self, *, refs=True, stored=False):
        self.owner, executor = _id(WorkerID, 6), _id(WorkerID, 5)
        task = _id(TaskID, 2)
        self.id = OutputPublicationID(
            _id(LeaseID, 4), (TaskExecution(AttemptID(task, 3))),
        )
        header = OutputPublicationHeader(
            self.id, _id(JobID, 1), executor, self.owner,
            OutputPublicationNodeIncarnation(_id(NodeID, 8), 1701, 2),
        )
        self.object_id = (self.id.object_id)
        source_task = _id(TaskID, 11)
        original_source = protocol.TaskHoldSource(protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, executor, source_task,
            AttemptID(source_task, 2),
        ))
        transfers = ()
        if refs:
            transfers = tuple(
                PreparedContainedTransfer(
                    ObjectID.for_task(_id(TaskID, 9 + index)), child_owner,
                    ("child.invalid", 30101 + index), source,
                    ContainedReferenceHold(self.object_id, executor, token),
                    ContainedReferenceHold(self.object_id, self.owner, token),
                )
                for index, (child_owner, source, token) in enumerate((
                    (executor, OwnedContainedSource(executor), "owned-child"),
                    (_id(WorkerID, 7), BorrowedContainedSource(
                        executor, "borrow-token", original_source,
                    ), "borrowed-child"),
                ))
            )
        self.payload = b"single-result"
        tier = protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE
        value = (OutputValue(tier, len(self.payload), hashlib.sha256(self.payload).hexdigest(), transfers))
        self.manifest = OutputPublicationManifest.create(header, value)
        self.witness = OutputPublicationCompleteWitness.for_manifest(self.manifest)
        self.result = protocol.ResultDescriptor(
            self.object_id, tier, len(self.payload), self.owner,
            header.node_incarnation.node_id, value.checksum,
            None if stored else self.payload,
        )
        self.envelope = OutputPublicationEnvelope(self.manifest, self.witness, (self.result))
        self.journal = OutputPublicationJournal()
        assert self.journal.open(self.manifest)

    def effect(self, stage, transfer=None):
        return OutputPublicationEffect(self.id, self.manifest.manifest_digest, stage, transfer)

    def register(self):
        self.journal.ack_owner_registered(OutputPublicationAck(self.journal.begin_owner_register(self.id)))

    def prepare(self):
        self.register()
        for index in range(len((self.manifest.value).transfers)):
            self.journal.ack_prepared(OutputPublicationAck(self.journal.begin_prepare(self.id, index)))

    def materialize(self):
        self.prepare()
        self.journal.ack_materialized(
            OutputPublicationAck(self.journal.begin_materialize(self.id)), self.result,
        )

    def promote(self):
        self.materialize()
        for index in range(len((self.manifest.value).transfers)):
            self.journal.ack_promoted(OutputPublicationAck(self.journal.begin_promote(self.id, index)))

    def complete(self):
        self.promote()
        return self.journal.complete(self.id, self.witness)

    def adoption(self, commit="owner-cas"):
        return OutputPublicationAdoptionProof(self.witness, self.owner, commit)

    def rollback(self):
        effects = []
        limit = len(self.journal.snapshot(self.id).rollback.effects)
        assert limit <= 5
        for _ in range(limit + 1):
            effect = self.journal.next_rollback_effect(self.id)
            if effect is None:
                return tuple(effects)
            effects.append(effect)
            self.journal.ack_rollback(OutputPublicationAck(effect))
        pytest.fail("rollback did not consume its bounded effect list")


@pytest.mark.parametrize("refs,stored", ((False, False), (False, True), (True, False), (True, True)), ids=("plain-inline", "plain-stored", "children-inline", "children-stored"))
def test_single_output_journal_replays_inline_and_stored_with_or_without_children(refs, stored):
    f = _Fixture(refs=refs, stored=stored)
    journal = f.journal
    assert not journal.open(f.manifest)
    assert not journal.snapshot(f.id).ready_to_complete
    assert f.complete() == f.envelope
    snapshot = journal.snapshot(f.id)
    assert snapshot.state is OutputPublicationJournalState.COMPLETED
    assert snapshot.complete == f.witness
    assert snapshot.materialized is True and snapshot.result_retained is True
    assert not snapshot.ready_to_complete
    assert journal.materialized_result(f.id) == f.result
    with journal.linearize(f.id):
        replay = journal.complete(f.id, f.witness)
        assert replay == f.envelope
        assert replay.result == f.result
        assert not hasattr(replay, 'results')
        assert not hasattr(replay.manifest, 'slots')
        assert not hasattr(replay.publication_id, 'output_ids')
    assert journal.publication_ids() == (f.id,)
    assert {effect.stage for effect in snapshot.intents} == (
        {Stage.OWNER_REGISTER, Stage.PREPARE, Stage.MATERIALIZE, Stage.PROMOTE}
        if refs else {Stage.OWNER_REGISTER, Stage.MATERIALIZE}
    )
    _metadata(snapshot)


def test_owner_registration_prepare_materialize_and_promote_are_ordered_gates():
    f = _Fixture()
    journal = f.journal
    before = journal.snapshot(f.id)
    for call in (lambda: journal.begin_prepare(f.id, 0), lambda: journal.begin_materialize(f.id)):
        with pytest.raises(OutputPublicationJournalStateError, match="owner registration"):
            call()
    assert journal.snapshot(f.id) == before
    f.register()
    first = journal.begin_prepare(f.id, 0)
    journal.ack_prepared(OutputPublicationAck(first))
    with pytest.raises(OutputPublicationJournalStateError, match="every provisional"):
        journal.begin_materialize(f.id)
    journal.ack_prepared(OutputPublicationAck(journal.begin_prepare(f.id, 1)))
    materialize = journal.begin_materialize(f.id)
    with pytest.raises(OutputPublicationJournalStateError, match="every materialization"):
        journal.begin_promote(f.id, 0)
    journal.ack_materialized(OutputPublicationAck(materialize), f.result)
    journal.ack_promoted(OutputPublicationAck(journal.begin_promote(f.id, 0)))
    assert not journal.snapshot(f.id).ready_to_complete
    with pytest.raises(OutputPublicationJournalStateError, match="promotion ACKs"):
        journal.complete(f.id, f.witness)
    journal.ack_promoted(OutputPublicationAck(journal.begin_promote(f.id, 1)))
    assert journal.snapshot(f.id).ready_to_complete
    assert journal.complete(f.id, f.witness) == f.envelope


def test_exact_forward_replay_acks_do_not_mutate_or_duplicate_intents():
    f = _Fixture()
    journal = f.journal
    steps = [(lambda: journal.begin_owner_register(f.id), journal.ack_owner_registered)]
    steps += [(lambda index=index: journal.begin_prepare(f.id, index), journal.ack_prepared) for index in range(2)]
    steps += [(lambda: journal.begin_materialize(f.id), lambda ack: journal.ack_materialized(ack, replace(f.result)))]
    steps += [(lambda index=index: journal.begin_promote(f.id, index), journal.ack_promoted) for index in range(2)]
    for begin, acknowledge in steps:
        effect = begin()
        assert begin() == effect
        assert acknowledge(OutputPublicationAck(effect))
        before = journal.snapshot(f.id)
        assert not acknowledge(OutputPublicationAck(effect, OutputPublicationAckDisposition.ALREADY_APPLIED))
        assert journal.snapshot(f.id) == before
    assert len(journal.snapshot(f.id).intents) == 6


def test_each_forward_ack_requires_its_own_recorded_intent():
    f = _Fixture()
    journal = f.journal
    stages = (
        (lambda: None, f.effect(Stage.OWNER_REGISTER), journal.ack_owner_registered),
        (f.register, f.effect(Stage.PREPARE, 0), journal.ack_prepared),
        (f.prepare, f.effect(Stage.MATERIALIZE), lambda ack: journal.ack_materialized(ack, f.result)),
        (f.materialize, f.effect(Stage.PROMOTE, 0), journal.ack_promoted),
    )
    for prepare, effect, acknowledge in stages:
        prepare()
        before = journal.snapshot(f.id)
        with pytest.raises(OutputPublicationJournalStateError, match="before its intent"):
            acknowledge(OutputPublicationAck(effect))
        assert journal.snapshot(f.id) == before


def test_wrong_manifest_stage_indices_and_descriptor_have_zero_mutation():
    f = _Fixture()
    journal = f.journal
    f.register()
    effect = journal.begin_prepare(f.id, 0)
    before = journal.snapshot(f.id)
    with pytest.raises(OutputPublicationConflictError, match="exact publication"):
        journal.ack_prepared(OutputPublicationAck(replace(effect, manifest_digest="ab" * 32)))
    with pytest.raises(OutputPublicationConflictError, match="another stage"):
        journal.ack_promoted(OutputPublicationAck(effect))
    for child in (2, True, -1, None, 1 << 64):
        for operation in (journal.begin_prepare, journal.begin_promote):
            with pytest.raises((OutputPublicationConflictError, ValueError)):
                operation(f.id, child)
    for stage in (Stage.OWNER_REGISTER, Stage.MATERIALIZE, Stage.SLOT_DROP):
        with pytest.raises(OutputPublicationConflictError, match="non-transfer"):
            f.effect(stage, 0)
    malformed = f.effect(Stage.MATERIALIZE)
    object.__setattr__(malformed, "transfer_index", 0)
    with pytest.raises(OutputPublicationConflictError, match="non-transfer"):
        journal.ack_materialized(OutputPublicationAck(malformed), f.result)
    assert journal.snapshot(f.id) == before
    f.prepare()
    materialize = journal.begin_materialize(f.id)
    before = journal.snapshot(f.id)
    wrong = replace(f.result, object_id=ObjectID.for_task(_id(TaskID, 50)))
    with pytest.raises(OutputPublicationConflictError, match="slot identity"):
        journal.ack_materialized(OutputPublicationAck(materialize), wrong)
    assert journal.snapshot(f.id) == before
    assert journal.materialized_result(f.id) is None


def test_materialize_intent_without_ack_is_compensated_before_reverse_child_releases():
    f = _Fixture(stored=True)
    journal = f.journal
    f.prepare()
    materialize = journal.begin_materialize(f.id)
    plan = journal.begin_rollback(f.id, "seal-ack-lost")
    assert tuple((e.stage, e.transfer_index) for e in plan.effects) == (
        (Stage.SLOT_DROP, None),
        (Stage.PROVISIONAL_RELEASE, 1), (Stage.PROVISIONAL_RELEASE, 0),
    )
    before = journal.snapshot(f.id)
    assert journal.begin_rollback(f.id, "seal-ack-lost") == plan
    with pytest.raises(OutputPublicationJournalStateError):
        journal.ack_materialized(OutputPublicationAck(materialize), f.result)
    with pytest.raises(OutputPublicationJournalStateError, match="out of order"):
        journal.ack_rollback(OutputPublicationAck(plan.effects[1]))
    with pytest.raises(OutputPublicationConflictError, match="rollback identity"):
        journal.begin_rollback(f.id, "different-rollback")
    assert journal.snapshot(f.id) == before
    assert f.rollback() == plan.effects
    terminal = journal.snapshot(f.id)
    assert terminal.state is OutputPublicationJournalState.RETIRED
    assert terminal.result_retained is False
    assert terminal.rollback_tombstone.plan == plan
    assert not journal.open(f.manifest)
    assert journal.next_rollback_effect(f.id) is None
    assert not journal.ack_rollback(OutputPublicationAck(plan.effects[0], OutputPublicationAckDisposition.ALREADY_APPLIED))
    assert journal.snapshot(f.id) == terminal
    _metadata(journal._records)


def test_partial_promotion_rollback_releases_possible_final_before_provisional():
    f = _Fixture()
    f.materialize()
    journal = f.journal
    journal.ack_promoted(OutputPublicationAck(journal.begin_promote(f.id, 0)))
    journal.begin_promote(f.id, 1)  # Effect may have happened before its ACK was lost.
    plan = journal.begin_rollback(f.id, "promotion-ack-lost")
    assert tuple((e.stage, e.transfer_index) for e in plan.effects) == (
        (Stage.SLOT_DROP, None), (Stage.FINAL_RELEASE, 1), (Stage.FINAL_RELEASE, 0),
        (Stage.PROVISIONAL_RELEASE, 1), (Stage.PROVISIONAL_RELEASE, 0),
    )
    assert journal.materialized_result(f.id) == f.result
    assert f.rollback() == plan.effects
    assert journal.materialized_result(f.id) is None
    _metadata(journal._records)


def test_prepare_effect_without_ack_still_requires_exact_compensation():
    f = _Fixture()
    f.register()
    f.journal.begin_prepare(f.id, 1)
    plan = f.journal.begin_rollback(f.id, "prepare-ack-lost")
    assert tuple((e.stage, e.transfer_index) for e in plan.effects) == (
        (Stage.PROVISIONAL_RELEASE, 1),
    )
    assert f.rollback() == plan.effects


def test_no_effect_rollback_is_local_but_fully_prepared_output_still_can_rollback():
    unopened = _Fixture()
    plan = unopened.journal.begin_rollback(unopened.id, "before-first-effect")
    assert plan.effects == ()
    snapshot = unopened.journal.snapshot(unopened.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.rollback_tombstone.acknowledgements == ()
    f = _Fixture(refs=False)
    f.promote()
    assert f.journal.snapshot(f.id).ready_to_complete
    f.journal.begin_rollback(f.id, "before-local-complete")
    f.rollback()
    with pytest.raises(OutputPublicationJournalStateError):
        f.journal.complete(f.id, f.witness)


@pytest.mark.parametrize("stage", (Stage.MATERIALIZE, Stage.PROMOTE), ids=("materialize", "promote"))
def test_late_forward_ack_cannot_resurrect_rollback_or_authorize_complete(stage):
    f = _Fixture()
    journal = f.journal
    if stage is Stage.MATERIALIZE:
        f.prepare()
        effect = journal.begin_materialize(f.id)
        acknowledge = lambda: journal.ack_materialized(OutputPublicationAck(effect), f.result)
    else:
        f.materialize()
        effect = journal.begin_promote(f.id, 0)
        acknowledge = lambda: journal.ack_promoted(OutputPublicationAck(effect))
    journal.begin_rollback(f.id, "late-forward-ack")
    before = journal.snapshot(f.id)
    with pytest.raises(OutputPublicationJournalStateError):
        acknowledge()
    with pytest.raises(OutputPublicationJournalStateError):
        journal.complete(f.id, f.witness)
    assert journal.snapshot(f.id) == before
    f.rollback()
    assert journal.snapshot(f.id).state is OutputPublicationJournalState.RETIRED
    _metadata(journal._records)


def test_complete_fences_rollback_and_every_forward_effect():
    f = _Fixture()
    journal = f.journal
    f.complete()
    before = journal.snapshot(f.id)
    with pytest.raises(OutputPublicationJournalStateError, match="forbidden after Complete"):
        journal.begin_rollback(f.id, "too-late")
    for call in (
        lambda: journal.begin_owner_register(f.id),
        lambda: journal.begin_prepare(f.id, 0),
        lambda: journal.begin_materialize(f.id),
        lambda: journal.begin_promote(f.id, 0),
        lambda: journal.ack_owner_registered(OutputPublicationAck(f.effect(Stage.OWNER_REGISTER))),
        lambda: journal.ack_prepared(OutputPublicationAck(f.effect(Stage.PREPARE, 0))),
        lambda: journal.ack_materialized(OutputPublicationAck(f.effect(Stage.MATERIALIZE)), f.result),
        lambda: journal.ack_promoted(OutputPublicationAck(f.effect(Stage.PROMOTE, 0))),
    ):
        with pytest.raises(OutputPublicationJournalStateError):
            call()
    with pytest.raises(OutputPublicationConflictError, match="Complete witness"):
        journal.complete(f.id, replace(f.witness, manifest_digest="ab" * 32))
    assert journal.snapshot(f.id) == before


@pytest.mark.parametrize("stored", (False, True), ids=("inline", "stored"))
def test_owner_adoption_retires_one_payload_and_preserves_exact_complete_metadata(stored):
    f = _Fixture(stored=stored)
    journal = f.journal
    f.complete()
    proof = f.adoption()
    tombstone = journal.retire_completed(proof)
    assert tombstone.object_id == f.object_id and tombstone.publication_id == f.id
    assert journal.retire_completed(proof) == tombstone
    snapshot = journal.snapshot(f.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.complete == f.witness and snapshot.result_retained is False
    assert snapshot.materialized is True and snapshot.retirement == tombstone
    assert journal.materialized_result(f.id) is None
    with pytest.raises(OutputPublicationPayloadRetired) as caught:
        journal.complete(f.id, f.witness)
    assert caught.value.tombstone == tombstone
    with pytest.raises(OutputPublicationConflictError, match="owner commit"):
        journal.retire_completed(replace(proof, owner_commit_id="different-cas"))
    assert journal.snapshot(f.id) == snapshot
    _metadata(journal._records)


def test_payload_retirement_does_not_invent_physical_drop_or_rollback_receipts():
    f = _Fixture(stored=True)
    f.complete()
    before = f.journal.snapshot(f.id)
    f.journal.retire_completed(f.adoption())
    after = f.journal.snapshot(f.id)
    assert after.intents == before.intents
    assert after.acknowledgements == before.acknowledgements
    assert after.rollback is after.rollback_tombstone is None
    assert not any(e.stage is Stage.SLOT_DROP for e in after.intents)
    # This proves the journal's metadata boundary, not a physical ObjectStore GC.
    assert f.journal.materialized_result(f.id) is None


def test_payload_tombstones_and_diagnostics_do_not_alias_terminal_authority():
    f = _Fixture()
    journal = f.journal
    f.complete()
    proof = f.adoption("frozen-owner-cas")
    returned = journal.retire_completed(proof)
    before = journal.snapshot(f.id)
    object.__setattr__(returned.proof, "owner_commit_id", "tampered-return")
    object.__setattr__(proof, "owner_commit_id", "tampered-input")
    assert journal.snapshot(f.id) == before
    exact = f.adoption("frozen-owner-cas")
    assert journal.retire_completed(exact) == before.retirement
    with pytest.raises(OutputPublicationPayloadRetired) as caught:
        journal.complete(f.id, f.witness)
    assert caught.value.tombstone.object_id == f.object_id
    assert caught.value.tombstone.publication_id == f.id
    assert caught.value.tombstone.manifest_digest == f.manifest.manifest_digest
    object.__setattr__(caught.value.tombstone.proof, "owner_commit_id", "tampered-exception")
    assert journal.snapshot(f.id) == before
    _metadata(journal._records)


def test_retirement_requires_local_complete_and_exact_owner_scope_without_mutation():
    f = _Fixture()
    journal = f.journal
    proof = f.adoption()
    f.materialize()
    before = journal.snapshot(f.id)
    with pytest.raises(OutputPublicationJournalStateError, match="local Complete"):
        journal.retire_completed(proof)
    assert journal.snapshot(f.id) == before and journal.materialized_result(f.id) == f.result
    f.complete()
    before = journal.snapshot(f.id)
    for wrong in (
        replace(proof, owner_worker_id=_id(WorkerID, 50)),
        replace(proof, complete=replace(proof.complete, manifest_digest="ab" * 32)),
    ):
        with pytest.raises(OutputPublicationConflictError, match="Complete or its owner"):
            journal.retire_completed(wrong)
        assert journal.snapshot(f.id) == before
        assert journal.materialized_result(f.id) == f.result
    with pytest.raises(TypeError, match="proof"):
        journal.retire_completed(f.id)
    assert journal.snapshot(f.id) == before


def test_open_conflict_unknown_identity_and_mutated_inputs_fail_closed():
    f = _Fixture()
    journal = f.journal
    before = journal.snapshot(f.id)
    changed = OutputPublicationManifest.create(
        f.manifest.header, (replace(f.manifest.value, checksum='ab' * 32)),
    )
    with pytest.raises(OutputPublicationConflictError, match="manifest was rebound"):
        journal.open(changed)
    with pytest.raises(UnknownOutputPublicationError):
        journal.snapshot(replace(f.id, lease_id=_id(LeaseID, 40)))
    assert journal.snapshot(f.id) == before
    effect = journal.begin_owner_register(f.id)
    altered = replace(effect)
    object.__setattr__(altered, "stage", "OWNER_REGISTER")
    with pytest.raises(TypeError):
        journal.ack_owner_registered(OutputPublicationAck(altered))
    assert not journal.acknowledged(effect)
    f.prepare()
    materialize = journal.begin_materialize(f.id)
    before = journal.snapshot(f.id)
    descriptor = replace(f.result)
    object.__setattr__(descriptor, "inline_data", b"changed")
    with pytest.raises(ProtocolError):
        journal.ack_materialized(OutputPublicationAck(materialize), descriptor)
    assert journal.snapshot(f.id) == before
    assert journal.materialized_result(f.id) is None


def test_forward_and_retirement_acks_revalidate_tampered_nested_ids():
    f = _Fixture()
    f.register()
    effect = f.journal.begin_prepare(f.id, 0)
    ack = OutputPublicationAck(effect)
    object.__setattr__(ack.effect.publication_id.lease_id, "value", b"short")
    before = f.journal.snapshot(f.id)
    with pytest.raises(ValueError):
        f.journal.ack_prepared(ack)
    assert f.journal.snapshot(f.id) == before
    assert not f.journal.acknowledged(effect)
    f.complete()
    proof = f.adoption()
    object.__setattr__(proof.owner_worker_id, "value", b"short")
    before = f.journal.snapshot(f.id)
    with pytest.raises(ValueError):
        f.journal.retire_completed(proof)
    assert f.journal.snapshot(f.id) == before


def test_snapshots_and_data_plane_returns_do_not_alias_journal_authority():
    f = _Fixture()
    journal = f.journal
    envelope = f.complete()
    snapshot = journal.snapshot(f.id)
    object.__setattr__((snapshot.manifest.value), "checksum", "ab" * 32)
    object.__setattr__((envelope.result), "inline_data", b"changed")
    cached = journal.materialized_result(f.id)
    object.__setattr__(cached, "inline_data", b"changed-again")
    assert journal.complete(f.id, f.witness) == f.envelope
    assert journal.snapshot(f.id).manifest == f.manifest
