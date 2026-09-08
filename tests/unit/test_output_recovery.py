"""Pure metadata registry contracts; no runtime, sockets or real waits.

Every fixture has at most two selected output slots and four child holds.
Registration/death interleavings are ordered method calls, not thread tests.
The Node journal fixture produces real metadata rollback proofs in memory.
"""

from __future__ import annotations

import pickle
import socket
import threading
import time
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import NodeID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationManifest,
)
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationAdoptionProof,
    OutputPublicationEffect, OutputPublicationJournal,
    OutputPublicationRollbackPlan, OutputPublicationRollbackTombstone,
    OutputPublicationSlotCleanupProof, OutputPublicationStage,
)
from miniray.output_recovery import (
    OutputPublicationRecoveryAuthority, OutputRecoveryAck, OutputRecoveryAction,
    OutputRecoveryConflictError, OutputRecoveryDisposition as Disposition,
    OutputRecoveryOwnerDecision as Decision,
    OutputRecoverySnapshot, OutputRecoveryStage as Stage, OutputRecoveryStateError,
    OutputRecoveryWork, OutputSlotDecision, UnknownOutputRecoveryError,
)
from tests.unit.test_output_publication import (
    _Fixture as _Values, _assert_metadata, _id,
)
from tests.unit.test_output_publication_journal import _Fixture as _Journal


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("metadata recovery contract attempted runtime work")

    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Thread, "join", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _node_death(values, *, epoch=7, detection="node-exit"):
    incarnation = values.manifest.header.node_incarnation
    return protocol.NodeDeathRecord(
        detection, incarnation.node_id, incarnation.node_pid,
        incarnation.registration_epoch, epoch, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "observed process exit",
    )


def _owner_death(values, *, epoch=13, detection="owner-exit"):
    return protocol.WorkerDeathRecord(
        detection, protocol.WorkerIncarnation(
            _id(NodeID, 30), 7301, 3, values.owner, 7302,
        ), epoch, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )


def _register(values=None, *, arm=False, complete=False):
    values = _Values() if values is None else values
    authority = OutputPublicationRecoveryAuthority()
    authority.report_intent(values.manifest)
    if arm or complete:
        authority.arm_complete(values.publication_id, values.manifest.manifest_digest)
    if complete:
        authority.report_terminal(values.witness)
    return authority, values


def _decisions(values, *decisions):
    return tuple(
        OutputSlotDecision(index, object_id, decision)
        for index, (object_id, decision) in enumerate(zip(
            values.publication_id.output_ids, decisions
        ))
    )


def _adoption(values, commit="owner-commit"):
    return OutputPublicationAdoptionProof(values.witness, values.owner, commit)


def _collection(values, index, cleanup="slot-cleanup"):
    return OutputPublicationSlotCleanupProof(
        values.witness, values.owner, index,
        values.publication_id.output_ids[index], cleanup,
    )


def _empty_rollback(values, rollback_id="rollback-before-intent"):
    journal = OutputPublicationJournal()
    journal.open(values.manifest)
    journal.begin_rollback(values.publication_id, rollback_id)
    return journal.snapshot(values.publication_id).rollback_tombstone


def test_intent_arm_terminal_are_exact_metadata_only_gates():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    intent = authority.report_intent(values.manifest)
    assert intent.stage is Stage.INTENT
    assert intent.disposition is Disposition.APPLIED
    assert intent.snapshot.manifest == values.manifest
    assert intent.snapshot.forward_allowed
    assert not intent.snapshot.armed and not intent.snapshot.terminal_report_allowed
    assert authority.report_intent(values.manifest).disposition is Disposition.ALREADY_RECORDED
    armed = authority.arm_complete(values.publication_id, values.manifest.manifest_digest)
    assert armed.stage is Stage.ARM_COMPLETE and armed.snapshot.armed
    assert armed.snapshot.forward_allowed and armed.snapshot.terminal_report_allowed
    assert armed.snapshot.complete is None
    assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is Disposition.ALREADY_RECORDED
    terminal = authority.report_terminal(values.witness)
    assert terminal.stage is Stage.TERMINAL
    assert terminal.snapshot.complete == values.witness
    assert not terminal.snapshot.forward_allowed
    assert terminal.snapshot.terminal_report_allowed
    assert authority.report_terminal(values.witness).disposition is Disposition.ALREADY_RECORDED
    assert authority.report_intent(values.manifest).disposition is Disposition.FENCED
    assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is Disposition.FENCED
    assert authority.publication_ids() == (values.publication_id,)
    for metadata in (intent, armed, terminal, authority.snapshot(values.publication_id)):
        _assert_metadata(metadata)
        assert pickle.loads(pickle.dumps(metadata)) == metadata


def test_terminal_requires_registered_intent_then_arm_without_partial_mutation():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    with pytest.raises(UnknownOutputRecoveryError):
        authority.arm_complete(values.publication_id, values.manifest.manifest_digest)
    with pytest.raises(UnknownOutputRecoveryError):
        authority.report_terminal(values.witness)
    assert authority.publication_ids() == ()
    before = authority.report_intent(values.manifest).snapshot
    with pytest.raises(OutputRecoveryStateError, match="ARM"):
        authority.report_terminal(values.witness)
    with pytest.raises(OutputRecoveryConflictError, match="digest"):
        authority.arm_complete(values.publication_id, "0" * 64)
    with pytest.raises(OutputRecoveryConflictError, match="witness"):
        authority.report_terminal(replace(values.witness, manifest_digest="0" * 64))
    assert authority.snapshot(values.publication_id) == before


@pytest.mark.parametrize("refs,target", ((False, False), (False, True), (True, True)))
def test_mixed_no_refs_and_targeted_slots_share_the_same_registry(refs, target):
    authority, values = _register(_Values(refs=refs, target=target), complete=True)
    snapshot = authority.snapshot(values.publication_id)
    assert snapshot.manifest == values.manifest
    assert snapshot.complete == values.witness
    if target:
        assert tuple(item.return_index for item in snapshot.publication_id.output_ids) == (1, 3)
    assert tuple(slot.tier for slot in snapshot.manifest.slots) == (
        protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
    )
    assert bool(snapshot.manifest.ordered_edges) is refs
    _assert_metadata(snapshot)


def test_rebinding_manifest_or_node_incarnation_is_atomic():
    authority, values = _register()
    before = authority.snapshot(values.publication_id)
    changed = OutputPublicationManifest.create(
        replace(values.header, owner_worker_id=_id(WorkerID, 60)),
        tuple(replace(slot, transfers=()) for slot in values.slots),
    )
    with pytest.raises(OutputRecoveryConflictError, match="manifest"):
        authority.report_intent(changed)
    alternate = _Values(refs=False)
    changed_header = replace(
        alternate.header,
        publication_id=replace(alternate.publication_id, lease_id=_id(type(alternate.lease), 61)),
        node_incarnation=replace(alternate.header.node_incarnation, node_pid=999),
    )
    wrong_process = OutputPublicationManifest.create(changed_header, alternate.slots)
    with pytest.raises(OutputRecoveryConflictError, match="incarnation"):
        authority.report_intent(wrong_process)
    assert authority.publication_ids() == (values.publication_id,)
    assert authority.snapshot(values.publication_id) == before


@pytest.mark.parametrize("phase,action", (
    ("intent", OutputRecoveryAction.PRECOMPLETE_ROLLBACK),
    ("armed", OutputRecoveryAction.COMPLETION_UNKNOWN),
    ("complete", OutputRecoveryAction.POSTCOMPLETE_RESOLVE),
))
def test_node_death_freezes_exact_phase_and_late_forward_reports_never_upgrade_it(phase, action):
    authority, values = _register(arm=phase == "armed", complete=phase == "complete")
    death = _node_death(values)
    (work,) = authority.freeze_node_death(death)
    assert work.action is action
    assert work.snapshot.frozen_node_death == death
    assert not work.snapshot.forward_allowed and not work.snapshot.terminal_report_allowed
    before = authority.snapshot(values.publication_id)
    assert authority.report_intent(values.manifest).disposition is Disposition.FENCED
    assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is Disposition.FENCED
    assert authority.report_terminal(values.witness).disposition is Disposition.FENCED
    assert authority.snapshot(values.publication_id) == before
    assert authority.freeze_node_death(death) == (work,)
    assert authority.frozen_workset(death) == (work,)
    _assert_metadata(work)


def test_registration_and_node_death_have_both_serial_orders_without_lost_work():
    values = _Values()
    first = OutputPublicationRecoveryAuthority()
    first.report_intent(values.manifest)
    assert len(first.freeze_node_death(_node_death(values))) == 1
    second = OutputPublicationRecoveryAuthority()
    assert second.freeze_node_death(_node_death(values)) == ()
    with pytest.raises(OutputRecoveryStateError, match="death-frozen"):
        second.report_intent(values.manifest)
    assert second.publication_ids() == ()
    assert second.freeze_node_death(_node_death(values)) == ()


@pytest.mark.parametrize("field,value", (("node_pid", 999), ("registration_epoch", 999)))
def test_wrong_node_incarnation_cannot_freeze_or_poison_registered_work(field, value):
    authority, values = _register(arm=True)
    before = authority.snapshot(values.publication_id)
    with pytest.raises(OutputRecoveryConflictError, match="incarnation"):
        authority.freeze_node_death(replace(_node_death(values), **{field: value}))
    assert authority.snapshot(values.publication_id) == before
    assert len(authority.freeze_node_death(_node_death(values))) == 1


@pytest.mark.parametrize("field,value", (
    ("detection_id", "different-death"), ("death_epoch", 8),
    ("exit_code", 2), ("detail", "different evidence"),
))
def test_frozen_node_death_cannot_be_rebound_even_when_process_identity_matches(field, value):
    authority, values = _register()
    death = _node_death(values)
    work = authority.freeze_node_death(death)
    with pytest.raises(OutputRecoveryConflictError):
        authority.freeze_node_death(replace(death, **{field: value}))
    assert authority.frozen_workset(death) == work


def test_global_death_detection_and_epoch_cannot_be_reused_for_other_nodes():
    authority, values = _register()
    death = _node_death(values)
    authority.freeze_node_death(death)
    other = replace(death, node_id=_id(NodeID, 99), node_pid=901)
    for candidate in (replace(other, death_epoch=70), replace(other, detection_id="other")):
        with pytest.raises(OutputRecoveryConflictError, match="detection or epoch"):
            authority.freeze_node_death(candidate)
    assert authority.freeze_node_death(replace(other, death_epoch=70, detection_id="other")) == ()


def test_expected_node_and_owner_exit_do_not_manufacture_failure_fences():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    assert authority.freeze_node_death(replace(
        _node_death(values), reason=protocol.NodeDeathReason.EXPECTED
    )) == ()
    assert authority.freeze_owner_death(replace(
        _owner_death(values), reason=protocol.WorkerDeathReason.EXPECTED
    )) == ()
    assert authority.report_intent(values.manifest).disposition is Disposition.APPLIED
    snapshot = authority.arm_complete(values.publication_id, values.manifest.manifest_digest).snapshot
    assert snapshot.forward_allowed and snapshot.owner_death is None


def test_local_rollback_before_intent_creates_metadata_tombstone_against_late_sends():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    proof = _empty_rollback(values)
    ack = authority.report_rollback(proof, manifest=values.manifest)
    assert ack.stage is Stage.ROLLED_BACK
    assert ack.snapshot.rollback == proof
    assert ack.snapshot.complete is None and not ack.snapshot.armed
    assert not ack.snapshot.forward_allowed
    assert authority.report_rollback(proof, manifest=values.manifest).disposition is Disposition.ALREADY_RECORDED
    assert authority.report_intent(values.manifest).disposition is Disposition.FENCED
    assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is Disposition.FENCED
    with pytest.raises(OutputRecoveryStateError, match="rolled-back"):
        authority.report_terminal(values.witness)
    with pytest.raises(OutputRecoveryStateError, match="rolled-back"):
        authority.report_adopted(_adoption(values))
    (work,) = authority.freeze_node_death(_node_death(values))
    assert work.action is OutputRecoveryAction.PRECOMPLETE_ROLLBACK
    assert work.snapshot.rollback == proof
    assert authority.report_rollback(proof, manifest=values.manifest).disposition is Disposition.ALREADY_RECORDED
    _assert_metadata(ack)


def test_armed_rollback_needs_every_inverse_and_success_can_never_be_rolled_back():
    fixture = _Journal()
    fixture.promote()
    fixture.journal.ack_arm_complete(OutputPublicationAck(
        fixture.journal.begin_arm_complete(fixture.id)
    ))
    fixture.journal.begin_rollback(fixture.id, "armed-node-rollback")
    fixture.rollback()
    proof = fixture.journal.snapshot(fixture.id).rollback_tombstone
    authority, values = _register(fixture.values, arm=True)
    before = authority.snapshot(values.publication_id)
    partial = OutputPublicationRollbackTombstone(
        replace(proof.plan, effects=proof.plan.effects[:-1]), proof.acknowledgements[:-1],
    )
    with pytest.raises(OutputRecoveryStateError, match="every possible effect"):
        authority.report_rollback(partial, manifest=values.manifest)
    assert authority.snapshot(values.publication_id) == before
    assert authority.report_rollback(proof, manifest=values.manifest).snapshot.rollback == proof
    (work,) = authority.freeze_node_death(_node_death(values))
    assert work.action is OutputRecoveryAction.PRECOMPLETE_ROLLBACK
    assert work.snapshot.armed and work.snapshot.complete is None

    succeeded, _ = _register(values, complete=True)
    succeeded.report_adopted(_adoption(values))
    before = succeeded.snapshot(values.publication_id)
    with pytest.raises(OutputRecoveryStateError, match="after successful Complete"):
        succeeded.report_rollback(proof, manifest=values.manifest)
    assert succeeded.snapshot(values.publication_id) == before


def test_rollback_rejects_other_manifest_and_out_of_range_transfer_without_state_change():
    authority, values = _register()
    before = authority.snapshot(values.publication_id)
    effect = OutputPublicationEffect(
        values.publication_id, values.manifest.manifest_digest,
        OutputPublicationStage.PROVISIONAL_RELEASE, 0, 99,
    )
    proof = OutputPublicationRollbackTombstone(
        OutputPublicationRollbackPlan(values.publication_id, values.manifest.manifest_digest,
                                      "bad-rollback", (effect,)),
        (OutputPublicationAck(effect),),
    )
    with pytest.raises(OutputRecoveryConflictError, match="invalid or unordered"):
        authority.report_rollback(proof, manifest=values.manifest)
    mismatched = _empty_rollback(values)
    mismatched = replace(mismatched, plan=replace(mismatched.plan, manifest_digest="0" * 64))
    with pytest.raises(OutputRecoveryConflictError, match="manifest"):
        authority.report_rollback(mismatched, manifest=values.manifest)
    assert authority.snapshot(values.publication_id) == before


def test_late_unreported_rollback_is_fenced_by_node_loss_not_installed_as_history():
    authority, values = _register()
    (work,) = authority.freeze_node_death(_node_death(values))
    ack = authority.report_rollback(_empty_rollback(values), manifest=values.manifest)
    assert ack.disposition is Disposition.FENCED
    assert ack.snapshot.rollback is None
    assert authority.frozen_workset(work.death) == (work,)


def test_adoption_before_terminal_outbox_records_complete_without_reopening_effects():
    authority, values = _register(arm=True)
    proof = _adoption(values)
    ack = authority.report_adopted(proof)
    assert ack.stage is Stage.ADOPTED and ack.disposition is Disposition.APPLIED
    assert ack.snapshot.adopted == proof and ack.snapshot.complete == values.witness
    assert not ack.snapshot.forward_allowed and ack.snapshot.terminal_report_allowed
    assert authority.report_terminal(values.witness).disposition is Disposition.ALREADY_RECORDED
    assert authority.report_adopted(proof).disposition is Disposition.ALREADY_RECORDED
    assert authority.report_intent(values.manifest).disposition is Disposition.FENCED
    assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is Disposition.FENCED
    with pytest.raises(OutputRecoveryConflictError, match="adoption proof"):
        authority.report_adopted(replace(proof, owner_commit_id="other-commit"))
    assert authority.snapshot(values.publication_id) == ack.snapshot


@pytest.mark.parametrize("report", ("adopted", "collected"))
def test_owner_proof_requires_exact_owner_witness_and_arm(report):
    authority, values = _register()
    before = authority.snapshot(values.publication_id)
    proof = _adoption(values) if report == "adopted" else _collection(values, 0)
    method = authority.report_adopted if report == "adopted" else authority.report_slot_collected
    with pytest.raises(OutputRecoveryStateError, match="ARM"):
        method(proof)
    with pytest.raises(OutputRecoveryConflictError, match="another publication owner"):
        method(replace(proof, owner_worker_id=_id(WorkerID, 88)))
    with pytest.raises(OutputRecoveryConflictError, match="witness"):
        method(replace(proof, complete=replace(values.witness, manifest_digest="0" * 64)))
    assert authority.snapshot(values.publication_id) == before


def test_slot_collection_is_independent_and_later_adoption_never_erases_its_proof():
    authority, values = _register(arm=True)
    first = _collection(values, 0)
    ack = authority.report_slot_collected(first)
    assert ack.snapshot.slot_collections == (first,)
    assert ack.snapshot.complete == values.witness
    assert ack.snapshot.adopted is None
    assert authority.report_slot_collected(first).disposition is Disposition.ALREADY_RECORDED
    with pytest.raises(OutputRecoveryConflictError, match="collection proof"):
        authority.report_slot_collected(replace(first, cleanup_id="other-cleanup"))
    adoption = _adoption(values)
    authority.report_adopted(adoption)
    second = _collection(values, 1)
    snapshot = authority.report_slot_collected(second).snapshot
    assert snapshot.adopted == adoption
    assert snapshot.slot_collections == (first, second)
    assert authority.report_terminal(values.witness).disposition is Disposition.ALREADY_RECORDED
    (work,) = authority.freeze_node_death(_node_death(values))
    assert work.snapshot.slot_collections == (first, second)
    assert work.snapshot.adopted == adoption
    assert authority.report_slot_collected(first).disposition is Disposition.ALREADY_RECORDED
    assert authority.frozen_workset(work.death) == (work,)
    _assert_metadata(snapshot)


def test_unknown_complete_owner_decision_is_per_slot_and_does_not_upgrade_frozen_history():
    authority, values = _register(arm=True)
    (work,) = authority.freeze_node_death(_node_death(values))
    vector = _decisions(values, Decision.KEEP, Decision.DROP)
    ack = authority.decide_owner(work, values.owner, vector,
                                 decision_id="owner-decision", complete=values.witness)
    decision = ack.snapshot.owner_decision
    assert decision.slots == vector and decision.complete == values.witness
    assert ack.snapshot.complete is None
    assert ack.snapshot.recovery_action is OutputRecoveryAction.COMPLETION_UNKNOWN
    assert authority.frozen_workset(work.death) == (work,)
    assert work.snapshot.owner_decision is None
    assert authority.decide_owner(work, values.owner, vector,
                                  decision_id="owner-decision", complete=values.witness).disposition is Disposition.ALREADY_RECORDED
    with pytest.raises(OutputRecoveryConflictError, match="vector was rebound"):
        authority.decide_owner(work, values.owner, _decisions(values, Decision.DROP, Decision.KEEP),
                               decision_id="owner-decision", complete=values.witness)
    assert authority.snapshot(values.publication_id).owner_decision == decision
    _assert_metadata(ack)


@pytest.mark.parametrize("target", (False, True))
def test_owner_decision_requires_full_selected_vector_exact_owner_and_keep_witness(target):
    authority, values = _register(_Values(target=target), arm=True)
    (work,) = authority.freeze_node_death(_node_death(values))
    vector = _decisions(values, Decision.KEEP, Decision.DROP)
    before = authority.snapshot(values.publication_id)
    for invalid in (vector[:1], vector[::-1], (vector[0], vector[0])):
        with pytest.raises(OutputRecoveryConflictError, match="every ordered selected slot"):
            authority.decide_owner(work, values.owner, invalid,
                                   decision_id="bad-vector", complete=values.witness)
    with pytest.raises(OutputRecoveryStateError, match="KEEP requires"):
        authority.decide_owner(work, values.owner, vector, decision_id="no-witness")
    with pytest.raises(OutputRecoveryConflictError, match="exact output owner"):
        authority.decide_owner(work, _id(WorkerID, 70), vector,
                               decision_id="wrong-owner", complete=values.witness)
    with pytest.raises(OutputRecoveryConflictError, match="Complete identity"):
        authority.decide_owner(work, values.owner, vector, decision_id="wrong-witness",
                               complete=replace(values.witness, manifest_digest="0" * 64))
    assert authority.snapshot(values.publication_id) == before


def test_drop_vector_needs_no_invented_complete_and_collected_slot_cannot_be_kept():
    authority, values = _register(arm=True)
    (work,) = authority.freeze_node_death(_node_death(values))
    ack = authority.decide_owner(work, values.owner, _decisions(values, Decision.DROP, Decision.DROP),
                                 decision_id="all-missing")
    assert ack.snapshot.owner_decision.complete is None
    assert ack.snapshot.complete is None

    collected, _ = _register(values, complete=True)
    collected.report_slot_collected(_collection(values, 1))
    (work,) = collected.freeze_node_death(_node_death(values))
    with pytest.raises(OutputRecoveryConflictError, match="already collected"):
        collected.decide_owner(work, values.owner, _decisions(values, Decision.KEEP, Decision.KEEP),
                               decision_id="stale-custody", complete=values.witness)
    accepted = collected.decide_owner(work, values.owner, _decisions(values, Decision.KEEP, Decision.DROP),
                                      decision_id="current-custody", complete=values.witness)
    assert accepted.snapshot.owner_decision.slots[1].decision is Decision.DROP


def test_decision_cannot_rewrite_frozen_work_or_choose_keep_for_precomplete():
    authority, values = _register(arm=True)
    (work,) = authority.freeze_node_death(_node_death(values))
    forged = OutputRecoveryWork(
        work.death, replace(work.snapshot, complete=values.witness),
        OutputRecoveryAction.POSTCOMPLETE_RESOLVE,
    )
    with pytest.raises(OutputRecoveryConflictError, match="original frozen work"):
        authority.decide_owner(forged, values.owner, _decisions(values, Decision.KEEP, Decision.KEEP),
                               decision_id="forged-history", complete=values.witness)
    assert authority.frozen_workset(work.death) == (work,)
    precomplete, _ = _register(values)
    (early,) = precomplete.freeze_node_death(_node_death(values))
    with pytest.raises(OutputRecoveryStateError, match="pre-Complete"):
        precomplete.decide_owner(early, values.owner, _decisions(values, Decision.DROP, Decision.DROP),
                                 decision_id="not-a-custody-decision")


@pytest.mark.parametrize("owner_first", (False, True))
def test_owner_death_is_orthogonal_and_never_rewrites_node_or_owner_work(owner_first):
    authority, values = _register(arm=True)
    node_death, owner_death = _node_death(values), _owner_death(values)
    if owner_first:
        (owner_work,) = authority.freeze_owner_death(owner_death)
        assert owner_work.snapshot.frozen_node_death is None
        (node_work,) = authority.freeze_node_death(node_death)
    else:
        (node_work,) = authority.freeze_node_death(node_death)
        assert node_work.snapshot.owner_death is None
        (owner_work,) = authority.freeze_owner_death(owner_death)
    current = authority.snapshot(values.publication_id)
    assert current.owner_death == owner_death and current.frozen_node_death == node_death
    assert current.complete is None
    assert authority.freeze_node_death(node_death) == (node_work,)
    assert authority.freeze_owner_death(owner_death) == (owner_work,)
    assert authority.frozen_owner_workset(owner_death) == (owner_work,)
    assert authority.decide_owner(node_work, values.owner, _decisions(values, Decision.DROP, Decision.DROP),
                                  decision_id="late-owner").disposition is Disposition.FENCED
    assert authority.report_intent(values.manifest).disposition is Disposition.FENCED
    assert authority.report_terminal(values.witness).disposition is Disposition.FENCED
    assert authority.snapshot(values.publication_id) == current
    _assert_metadata(owner_work)


def test_owner_death_after_mixed_decision_preserves_vector_and_fences_replay_authorization():
    authority, values = _register(arm=True)
    (work,) = authority.freeze_node_death(_node_death(values))
    vector = _decisions(values, Decision.KEEP, Decision.DROP)
    prior = authority.decide_owner(work, values.owner, vector, decision_id="mixed", complete=values.witness).snapshot
    (owner_work,) = authority.freeze_owner_death(_owner_death(values))
    assert owner_work.snapshot.owner_decision == prior.owner_decision
    assert authority.frozen_workset(work.death) == (work,)
    assert authority.decide_owner(work, values.owner, vector, decision_id="mixed",
                                  complete=values.witness).disposition is Disposition.FENCED
    assert authority.snapshot(values.publication_id).owner_decision == prior.owner_decision


def test_dead_owner_rejects_new_intent_and_exact_death_identity_cannot_be_rebound():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    death = _owner_death(values)
    assert authority.freeze_owner_death(death) == ()
    with pytest.raises(OutputRecoveryStateError, match="owner is death-frozen"):
        authority.report_intent(values.manifest)
    with pytest.raises(OutputRecoveryConflictError):
        authority.freeze_owner_death(replace(death, incarnation=replace(
            death.incarnation, worker_pid=9999
        )))
    assert authority.publication_ids() == ()
    assert authority.freeze_owner_death(death) == ()


def test_all_registry_observations_detach_nested_mutable_dataclasses_from_authority():
    authority, values = _register(arm=True)
    expected = replace(authority.snapshot(values.publication_id))
    object.__setattr__(values.manifest.header.owner_worker_id, "value", bytes((77,)) * 16)
    assert authority.snapshot(expected.publication_id) == expected
    returned = authority.snapshot(expected.publication_id)
    object.__setattr__(returned.manifest.header.node_incarnation, "node_pid", 9999)
    assert authority.snapshot(expected.publication_id) == expected
    witness = OutputPublicationCompleteWitness.for_manifest(expected.manifest)
    ack = authority.report_terminal(witness)
    object.__setattr__(witness.publication_id.execution.attempt_id, "attempt_number", 999)
    object.__setattr__(ack.snapshot.manifest.slots[0], "size_bytes", 999)
    fresh = authority.snapshot(expected.publication_id)
    assert fresh.manifest == expected.manifest
    assert fresh.complete == OutputPublicationCompleteWitness.for_manifest(expected.manifest)
    death = protocol.NodeDeathRecord(
        "detach-death", expected.manifest.header.node_incarnation.node_id,
        expected.manifest.header.node_incarnation.node_pid,
        expected.manifest.header.node_incarnation.registration_epoch, 7, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "exit",
    )
    (work,) = authority.freeze_node_death(death)
    canonical = replace(work)
    object.__setattr__(work.snapshot.complete.publication_id.execution.attempt_id, "attempt_number", 901)
    object.__setattr__(death.node_id, "value", bytes((33,)) * 16)
    assert authority.frozen_workset(canonical.death) == (canonical,)


def test_malformed_mutated_values_fail_before_any_registration_or_death_mutation():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    malformed = replace(values.manifest)
    object.__setattr__(malformed.header.executor_worker_id, "value", bytearray(b"x" * 16))
    with pytest.raises(TypeError):
        authority.report_intent(malformed)
    assert authority.publication_ids() == ()
    authority.report_intent(values.manifest)
    before = authority.snapshot(values.publication_id)
    malformed_death = _node_death(values)
    object.__setattr__(malformed_death, "node_pid", True)
    with pytest.raises(ValueError):
        authority.freeze_node_death(malformed_death)
    assert authority.snapshot(values.publication_id) == before
    assert len(authority.freeze_node_death(_node_death(values))) == 1


def test_payload_envelopes_and_descriptor_objects_never_enter_metadata_registry():
    values = _Values()
    authority = OutputPublicationRecoveryAuthority()
    for payload in (values.envelope, values.results[0], b"payload"):
        with pytest.raises(TypeError):
            authority.report_intent(payload)
    authority.report_intent(values.manifest)
    with pytest.raises(TypeError):
        authority.report_terminal(values.envelope)
    with pytest.raises(TypeError):
        OutputRecoverySnapshot(values.envelope)
    with pytest.raises(OutputRecoveryStateError, match="acknowledged fact"):
        OutputRecoveryAck(Stage.TERMINAL, Disposition.APPLIED, authority.snapshot(values.publication_id))
    assert authority.snapshot(values.publication_id).complete is None
    _assert_metadata(authority.snapshot(values.publication_id))
