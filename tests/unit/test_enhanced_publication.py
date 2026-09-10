"""Pure GCS reducer contracts; no runtime, sockets, processes or waits.

Constructed preparation/Complete/adoption values are reducer input facts only.
They do not claim a Worker executed, a Node journal observed an ACK, or a live
application formed a cycle. Runtime tests establish those separate boundaries.
Every test uses at most four graph objects and two children per publication.
"""
from dataclasses import fields, is_dataclass, replace
import hashlib
import pickle

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.enhanced_publication import *
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.output_publication_journal import OutputPublicationAdoptionProof
from miniray.ownership import StoredContainedReferenceDisposition as PinDisposition
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.put_handoff import PutManifest
from miniray.task_outputs import TaskExecution

pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes((number,)) * 16)


OWNER = _id(WorkerID, 1)
JOB = _id(JobID, 2)
NODE = OutputPublicationNodeIncarnation(_id(NodeID, 3), 1001, 1)
ROUTE = ("owner.invalid", 1234)
CHECKSUM = hashlib.sha256(b"value").hexdigest()


def _object(number):
    return ObjectID.for_task(_id(TaskID, number))


def _transfers(outer, children):
    return tuple(PreparedContainedTransfer(
        child, OWNER, ROUTE, OwnedContainedSource(OWNER),
        ContainedReferenceHold(outer, OWNER, f"provisional:{outer.hex}:{index}"),
        ContainedReferenceHold(outer, OWNER, f"{outer.hex}:{index}"),
    ) for index, child in enumerate(children))


def _task(number=10, children=(), attempt=0, tier=protocol.ResultStorage.INLINE):
    outer = _object(number)
    identity = OutputPublicationID(_id(LeaseID, number + attempt + 30), TaskExecution(AttemptID(outer.task_id, attempt)))
    header = OutputPublicationHeader(identity, JOB, OWNER, OWNER, NODE)
    value = OutputValue(tier, 5, CHECKSUM, _transfers(outer, children))
    return TaskPublication(OutputPublicationManifest.create(header, value), ROUTE)


def _put(number=20, children=(), tier=protocol.ResultStorage.INLINE):
    return PutPublication(JOB, ROUTE, PutManifest(
        _object(number), OWNER, tier, 5, CHECKSUM, _transfers(_object(number), children),
    ))


def _prepared(publication):
    prepares = tuple(protocol.StoredContainedPinReply(
        protocol.PrepareStoredContainedPin(transfer, transfer.contained_owner_worker_id),
        PinDisposition.PREPARED,
    ) for transfer in publication.transfers)
    promotions = tuple(protocol.StoredContainedPinReply(
        protocol.PromoteStoredContainedPin(transfer, transfer.contained_owner_worker_id),
        PinDisposition.PROMOTED,
    ) for transfer in publication.transfers)
    if type(publication) is TaskPublication:
        return TaskPreparedReceipt(publication.reference, prepares, promotions,
                                   MaterializationReceipt(publication.reference, NODE))
    stored = publication.manifest.tier is protocol.ResultStorage.OBJECT_STORE
    return PutPreparedReceipt(publication.reference, prepares, promotions,
        MaterializationReceipt(publication.reference, NODE if stored else None),
        protocol.SealObjectReply(publication.object_id, True, NODE.node_id, 5, CHECKSUM) if stored else None)


def _ok(authority, request):
    reply = authority.apply(request)
    assert reply.accepted, reply.error
    return reply


def _begin(authority, publication):
    _ok(authority, BeginPublication(publication))
    return _ok(authority, PrepareGraph(publication.reference))


def _commit(authority, publication):
    _begin(authority, publication)
    if type(publication) is TaskPublication:
        _ok(authority, ArmTask(_prepared(publication)))
        _ok(authority, RecordTerminal(OutputPublicationCompleteWitness.for_manifest(publication.manifest)))
        return _ok(authority, CommitGraph(publication.reference))
    return _ok(authority, CommitGraph(publication.reference, _prepared(publication)))


def _releases(publication, *, provisional=True):
    return tuple(protocol.ReleaseContainedReferenceReply(
        transfer.contained_object_id, transfer.contained_owner_worker_id, hold, True, False,
    ) for transfer in publication.transfers
      for hold in ((transfer.final_hold, transfer.provisional_hold) if provisional else (transfer.final_hold,)))


def _abort(authority, publication, scope=None):
    _ok(authority, FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "abort-1")))
    closed = ClosedContainedHolds(publication.reference, _releases(publication), rollback_scope=scope)
    return _ok(authority, RetireGraph(closed))


def test_plain_task_has_all_metadata_stages_but_no_adoption_before_owner_fact():
    authority, publication = PublicationAuthority(), _task()
    ref = publication.reference
    assert authority.query(GetPublication(ref)).snapshot is None
    intent = _ok(authority, BeginPublication(publication))
    assert intent.snapshot.complete is None and not intent.snapshot.graph_active
    assert not authority.apply(RecordTerminal(OutputPublicationCompleteWitness.for_manifest(publication.manifest))).accepted
    _ok(authority, PrepareGraph(ref))
    arm = _ok(authority, ArmTask(_prepared(publication)))
    assert arm.snapshot.complete is None
    complete = OutputPublicationCompleteWitness.for_manifest(publication.manifest)
    _ok(authority, RecordTerminal(complete))
    commit = _ok(authority, CommitGraph(ref))
    assert commit.snapshot.adoption is None
    adoption = OutputPublicationAdoptionProof(complete, OWNER, "actual-owner-cas")
    adopted = _ok(authority, RecordAdoption(adoption))
    assert adopted.snapshot.adoption == adoption
    assert [item.stage for item in adopted.snapshot.receipts] == list(PublicationStage)[:6]
    assert authority.apply(RecordAdoption(adoption)).receipt == adopted.receipt


@pytest.mark.parametrize("change", ("digest", "route", "owner", "job"))
def test_begin_binds_complete_identity_and_rejects_rebinding(change):
    authority, publication = PublicationAuthority(), _task()
    before = _ok(authority, BeginPublication(publication)).snapshot
    if change == "digest":
        request = PrepareGraph(replace(publication.reference, digest="f" * 64))
    elif change == "route":
        request = BeginPublication(replace(publication, owner_address=("another.invalid", 4321)))
    else:
        header = replace(publication.manifest.header, **{
            "owner_worker_id" if change == "owner" else "job_id":
            _id(WorkerID if change == "owner" else JobID, 9),
        })
        request = BeginPublication(TaskPublication(OutputPublicationManifest.create(header, publication.manifest.value), ROUTE))
    rejected = authority.apply(request)
    assert not rejected.accepted and rejected.error_kind is PublicationErrorKind.CONFLICT
    assert authority.query(GetPublication(publication.reference)).snapshot == before


def test_prebegin_fence_prevents_late_begin_without_claiming_cleanup():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    fence = FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "early-abort"))
    fenced = _ok(authority, fence)
    assert fenced.snapshot.receipt(PublicationStage.INTENT) is None
    assert not fenced.snapshot.forward_open and fenced.snapshot.closed_holds is None
    assert not authority.begin(BeginPublication(publication)).accepted
    assert not authority.prepare(PrepareGraph(publication.reference)).accepted
    assert not authority.retire(RetireGraph(ClosedContainedHolds(publication.reference))).accepted
    scope = TaskRollbackScope(publication.reference, "node-observed-no-effects", (), (), False)
    retired = _ok(authority, RetireGraph(ClosedContainedHolds(publication.reference, rollback_scope=scope)))
    assert not retired.snapshot.graph_active
    assert authority.apply(fence).receipt == fenced.receipt


def test_prepared_union_rejects_inverse_edge_until_genuine_cleanup():
    authority = PublicationAuthority()
    first, second = _task(10, (_object(11),)), _task(11, (_object(10),))
    first_reply = _begin(authority, first)
    _ok(authority, BeginPublication(second))
    rejected = authority.prepare(PrepareGraph(second.reference))
    assert not rejected.accepted and rejected.error_kind is PublicationErrorKind.CYCLE
    assert rejected.snapshot.receipt(PublicationStage.PREPARED) is None
    _ok(authority, FencePublication(first, OwnerAbortReceipt(first.reference, OWNER, "abort-1")))
    assert authority.query(GetPublication(first.reference)).snapshot.graph_active
    assert authority.prepare(PrepareGraph(second.reference)).error_kind is PublicationErrorKind.CYCLE
    scope = TaskRollbackScope(first.reference, "no-child-effects", (), (), False)
    retired = _ok(authority, RetireGraph(ClosedContainedHolds(first.reference, rollback_scope=scope)))
    assert not retired.snapshot.graph_active
    assert retired.snapshot.receipt(PublicationStage.PREPARED) == first_reply.receipt
    assert _ok(authority, PrepareGraph(second.reference)).snapshot.graph_active


def test_self_cycle_rejected_and_two_alias_holds_do_not_hide_cycle():
    authority, publication = PublicationAuthority(), _task(10, (_object(10), _object(10)))
    _ok(authority, BeginPublication(publication))
    reply = authority.apply(PrepareGraph(publication.reference))
    assert not reply.accepted and reply.error_kind is PublicationErrorKind.CYCLE


@pytest.mark.parametrize("bad", ("missing-prepare", "missing-promote", "wrong-node", "reordered"))
def test_arm_requires_complete_exact_observed_preparation(bad):
    authority, publication = PublicationAuthority(), _task(children=(_object(11), _object(12)))
    _begin(authority, publication)
    prepared = _prepared(publication)
    if bad == "missing-prepare":
        prepared = replace(prepared, prepare_replies=prepared.prepare_replies[:-1])
    elif bad == "missing-promote":
        prepared = replace(prepared, promote_replies=prepared.promote_replies[:-1])
    elif bad == "wrong-node":
        prepared = replace(prepared, materialization=MaterializationReceipt(publication.reference, replace(NODE, registration_epoch=2)))
    else:
        prepared = replace(prepared, prepare_replies=prepared.prepare_replies[::-1])
    rejected = authority.apply(ArmTask(prepared))
    assert not rejected.accepted and rejected.error_kind is PublicationErrorKind.CONFLICT
    assert rejected.snapshot.prepared is None


def test_w1_scope_settles_unknown_effects_and_does_not_claim_unattempted_effects():
    authority, publication = PublicationAuthority(), _task(children=(_object(11), _object(12)))
    _begin(authority, publication)
    _ok(authority, FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "abort-1")))
    scope = TaskRollbackScope(publication.reference, "node-rollback-1", (0,), (0,), False)
    release = _releases(publication)
    assert not authority.retire(RetireGraph(ClosedContainedHolds(publication.reference, release[:1], rollback_scope=scope))).accepted
    retired = _ok(authority, RetireGraph(ClosedContainedHolds(publication.reference, release[:2], rollback_scope=scope)))
    assert not retired.snapshot.graph_active
    assert retired.snapshot.closed_holds.releases == release[:2]
    changed = replace(scope, prepare_intents=(0, 1))
    assert not authority.retire(RetireGraph(ClosedContainedHolds(publication.reference, release, rollback_scope=changed))).accepted


def test_recorded_promotion_proves_provisional_retirement_but_final_release_still_required():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    _commit(authority, publication)
    _ok(authority, FencePublication(publication, OwnerRetirementReceipt(
        publication.reference, OWNER, "gc-1", RetirementReason.GC)))
    assert not authority.retire(RetireGraph(ClosedContainedHolds(publication.reference))).accepted
    closed = ClosedContainedHolds(publication.reference, _releases(publication, provisional=False))
    retired = _ok(authority, RetireGraph(closed))
    assert not retired.snapshot.graph_active
    assert retired.snapshot.receipt(PublicationStage.COMMITTED) is not None


def test_w2_late_terminal_is_history_never_reopens_fenced_progress():
    authority, publication = PublicationAuthority(), _task()
    _begin(authority, publication)
    _ok(authority, ArmTask(_prepared(publication)))
    _ok(authority, FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "owner-abort")))
    complete = OutputPublicationCompleteWitness.for_manifest(publication.manifest)
    terminal = _ok(authority, RecordTerminal(complete))
    assert terminal.snapshot.complete == complete and not terminal.snapshot.forward_open
    assert not authority.commit(CommitGraph(publication.reference)).accepted
    assert authority.terminal(RecordTerminal(complete)).receipt == terminal.receipt


def test_complete_cannot_contradict_an_actual_frozen_rollback_scope():
    authority, publication = PublicationAuthority(), _task()
    _begin(authority, publication)
    _ok(authority, ArmTask(_prepared(publication)))
    scope = TaskRollbackScope(publication.reference, "rollback", (), (), True)
    _abort(authority, publication, scope)
    reply = authority.terminal(RecordTerminal(OutputPublicationCompleteWitness.for_manifest(publication.manifest)))
    assert not reply.accepted and reply.error_kind is PublicationErrorKind.CONFLICT


def test_w4_old_commits_releases_and_adoption_do_not_touch_successor():
    authority = PublicationAuthority()
    old = _task(10, (_object(11),))
    _commit(authority, old)
    proof = OutputPublicationAdoptionProof(OutputPublicationCompleteWitness.for_manifest(old.manifest), OWNER, "cas-old")
    adopted = _ok(authority, RecordAdoption(proof))
    _ok(authority, FencePublication(old, OwnerRetirementReceipt(old.reference, OWNER, "reconstruct-old", RetirementReason.RECONSTRUCTION)))
    closed = ClosedContainedHolds(old.reference, _releases(old, provisional=False))
    retired = _ok(authority, RetireGraph(closed))
    new = _task(10, (_object(12),), attempt=1)
    current = _commit(authority, new).snapshot
    for request in (CommitGraph(old.reference), RecordAdoption(proof), RetireGraph(closed)):
        assert _ok(authority, request).snapshot.reference == old.reference
        assert authority.query(GetPublication(new.reference)).snapshot == current
    assert not authority.query(GetPublication(old.reference)).snapshot.graph_active
    assert _ok(authority, RecordAdoption(proof)).receipt == adopted.receipt
    assert _ok(authority, RetireGraph(closed)).receipt == retired.receipt


def test_old_membership_blocks_new_attempt_until_retired():
    authority, old = PublicationAuthority(), _task()
    _commit(authority, old)
    new = _task(attempt=1)
    assert not authority.begin(BeginPublication(new)).accepted
    _ok(authority, FencePublication(old, OwnerRetirementReceipt(old.reference, OWNER, "r", RetirementReason.RECONSTRUCTION)))
    assert not authority.begin(BeginPublication(new)).accepted
    _ok(authority, RetireGraph(ClosedContainedHolds(old.reference)))
    assert _ok(authority, BeginPublication(new)).snapshot.forward_open


@pytest.mark.parametrize("tier", tuple(protocol.ResultStorage))
def test_put_shares_graph_but_has_no_task_execution_history(tier):
    authority, publication = PublicationAuthority(), _put(children=(_object(11),), tier=tier)
    committed = _commit(authority, publication).snapshot
    assert committed.prepared == _prepared(publication)
    assert committed.complete is None and committed.adoption is None
    assert committed.receipt(PublicationStage.ARMED) is None
    assert committed.receipt(PublicationStage.TERMINAL) is None
    _ok(authority, FencePublication(publication, OwnerRetirementReceipt(publication.reference, OWNER, "put-gc", RetirementReason.GC)))
    closed = ClosedContainedHolds(publication.reference, _releases(publication, provisional=False))
    assert not _ok(authority, RetireGraph(closed)).snapshot.graph_active


def test_put_preparation_validates_actual_seal_identity_and_digest_covers_route():
    publication = _put(tier=protocol.ResultStorage.OBJECT_STORE)
    assert replace(publication, owner_address=("other.invalid", 4321)).reference.digest != publication.reference.digest
    authority = PublicationAuthority()
    _begin(authority, publication)
    proof = _prepared(publication)
    changed = replace(proof, seal_reply=replace(proof.seal_reply, checksum="f" * 64))
    rejected = authority.commit(CommitGraph(publication.reference, changed))
    assert not rejected.accepted and rejected.error_kind is PublicationErrorKind.CONFLICT


def test_mutated_frozen_nested_wire_is_revalidated_and_returned_copy_cannot_mutate_authority():
    authority, publication = PublicationAuthority(), _task()
    request = BeginPublication(publication)
    original = _ok(authority, request).snapshot
    object.__setattr__(request.publication.manifest.header.node_incarnation, "registration_epoch", 0)
    with pytest.raises((TypeError, ValueError)):
        authority.apply(request)
    queried = authority.query(GetPublication(publication.reference)).snapshot
    assert queried == original
    object.__setattr__(queried.publication.manifest.header, "owner_worker_id", _id(WorkerID, 9))
    assert authority.query(GetPublication(publication.reference)).snapshot == original


def test_metadata_roundtrip_has_no_payload_and_preserves_exact_request():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    snapshot = _commit(authority, publication).snapshot
    assert pickle.loads(pickle.dumps(snapshot)) == snapshot
    def inspect(value):
        if isinstance(value, (JobID, LeaseID, NodeID, TaskID, WorkerID)):
            return
        assert not isinstance(value, (bytes, bytearray, memoryview))
        if is_dataclass(value):
            for item in fields(value):
                inspect(getattr(value, item.name))
        elif type(value) is tuple:
            for item in value:
                inspect(item)
    inspect(snapshot)


def test_unrelated_cleanup_and_conflicting_owner_do_not_remove_edges():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    _begin(authority, publication)
    _ok(authority, FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "abort")))
    releases = _releases(publication)
    changed = replace(releases[0], object_id=_object(99))
    reply = authority.retire(RetireGraph(ClosedContainedHolds(publication.reference, (changed,) + releases[1:])))
    assert not reply.accepted and reply.error_kind is PublicationErrorKind.CONFLICT
    assert reply.snapshot.graph_active
    other = _task(11)
    header = replace(other.manifest.header, owner_worker_id=_id(WorkerID, 9))
    other = TaskPublication(OutputPublicationManifest.create(header, other.manifest.value), ROUTE)
    assert authority.begin(BeginPublication(other)).error_kind is PublicationErrorKind.CONFLICT


def test_adoption_cannot_replace_successful_owner_abort():
    authority, publication = PublicationAuthority(), _task()
    _commit(authority, publication)
    _ok(authority, FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "abort")))
    proof = OutputPublicationAdoptionProof(OutputPublicationCompleteWitness.for_manifest(publication.manifest), OWNER, "impossible-cas")
    assert not authority.adopt(RecordAdoption(proof)).accepted


def test_owner_abort_request_and_reply_bind_real_rollback_identity():
    publication = _task()
    scope = TaskRollbackScope(publication.reference, "rollback", (), (), False)
    request = AbortOwnerPublication(publication, scope)
    receipt = OwnerAbortReceipt(publication.reference, OWNER, "abort")
    reply = AbortOwnerPublicationReply(request, True, receipt)
    assert pickle.loads(pickle.dumps(reply)) == reply
    with pytest.raises(ValueError):
        AbortOwnerPublicationReply(request, True)


def _death(owner=OWNER):
    return protocol.WorkerDeathRecord(
        "confirmed-process-exit", protocol.WorkerIncarnation(
            NODE.node_id, NODE.node_pid, NODE.registration_epoch, owner, 1002),
        1, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )


def test_owner_death_fences_without_erasing_success_and_real_child_death_settles_holds():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    committed = _commit(authority, publication)
    _ok(authority, FencePublication(publication, _death()))
    closed = ClosedContainedHolds(publication.reference, child_deaths=(_death(),))
    retired = _ok(authority, RetireGraph(closed))
    assert retired.snapshot.complete == committed.snapshot.complete
    assert not retired.snapshot.graph_active
    proof = OutputPublicationAdoptionProof(committed.snapshot.complete, OWNER, "cas-before-death-report")
    late = _ok(authority, RecordAdoption(proof))
    assert late.snapshot.adoption == proof and not late.snapshot.forward_open
    assert not late.snapshot.graph_active


def test_expected_exit_and_unrelated_child_death_cannot_fake_clean():
    with pytest.raises(ValueError):
        ClosedContainedHolds(_task().reference, child_deaths=(replace(_death(), reason=protocol.WorkerDeathReason.EXPECTED),))
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    _begin(authority, publication)
    _ok(authority, FencePublication(publication, _death()))
    rejected = authority.retire(RetireGraph(ClosedContainedHolds(
        publication.reference, child_deaths=(_death(_id(WorkerID, 9)),))))
    assert not rejected.accepted and rejected.error_kind is PublicationErrorKind.CONFLICT
    assert rejected.snapshot.graph_active


def test_armed_preparation_cannot_be_cleaned_with_empty_rollback_scope():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    _begin(authority, publication)
    _ok(authority, ArmTask(_prepared(publication)))
    _ok(authority, FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "abort")))
    scope = TaskRollbackScope(publication.reference, "false-empty-scope", (), (), False)
    rejected = authority.retire(RetireGraph(ClosedContainedHolds(publication.reference, rollback_scope=scope)))
    assert not rejected.accepted and rejected.error_kind is PublicationErrorKind.CONFLICT
    assert rejected.snapshot.graph_active


def test_returned_preparation_is_deeply_detached_from_record_authority():
    authority, publication = PublicationAuthority(), _task(children=(_object(11),))
    snapshot = _commit(authority, publication).snapshot
    queried = authority.query(GetPublication(publication.reference)).snapshot
    object.__setattr__(queried.prepared.prepare_replies[0].request.transfer.final_hold, "transfer_token", "mutated")
    assert authority.query(GetPublication(publication.reference)).snapshot == snapshot
    with pytest.raises((ValueError, TypeError)):
        PublicationSnapshot(queried.publication, queried.receipts, prepared=queried.prepared, complete=queried.complete)


def test_unregistered_fences_beside_live_successor_never_claim_epoch_or_forward_permission():
    authority, current = PublicationAuthority(), _task(attempt=1)
    active = _commit(authority, current).snapshot
    stale = _task(attempt=0)
    next_publication = _task(attempt=2)
    header = next_publication.manifest.header
    denied_header = replace(header, publication_id=replace(header.publication_id, lease_id=_id(LeaseID, 99)))
    denied = TaskPublication(OutputPublicationManifest.create(denied_header, next_publication.manifest.value), ROUTE)
    for publication in (stale, denied):
        assert not authority.begin(BeginPublication(publication)).accepted
        fence = FencePublication(publication, OwnerAbortReceipt(publication.reference, OWNER, "never-admitted"))
        fenced = _ok(authority, fence).snapshot
        assert fenced.receipt(PublicationStage.INTENT) is None
        assert not fenced.forward_open and not fenced.graph_active
        assert not authority.begin(BeginPublication(publication)).accepted
        assert not authority.prepare(PrepareGraph(publication.reference)).accepted
        assert not authority.commit(CommitGraph(publication.reference)).accepted
        scope = TaskRollbackScope(publication.reference, "actual-no-effects", (), (), False)
        _ok(authority, RetireGraph(ClosedContainedHolds(publication.reference, rollback_scope=scope)))
        assert authority.query(GetPublication(current.reference)).snapshot == active
        # Cleanup admission still validates complete immutable owner routes.
        changed = replace(publication, owner_address=("wrong-owner.invalid", 4567))
        assert authority.fence(FencePublication(changed, OwnerAbortReceipt(changed.reference, OWNER, "never-admitted"))).error_kind is PublicationErrorKind.CONFLICT
    _ok(authority, FencePublication(current, OwnerRetirementReceipt(
        current.reference, OWNER, "retire-current", RetirementReason.RECONSTRUCTION)))
    _ok(authority, RetireGraph(ClosedContainedHolds(current.reference)))
    # A denied alternate lease never established attempt 2. Its fence cannot
    # prevent the genuinely admitted next attempt with a different exact key.
    assert _ok(authority, BeginPublication(next_publication)).snapshot.forward_open
