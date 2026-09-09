"""Pure metadata contracts for one owner handoff, one output and one child.

No runtime constructor, user code, transport, thread or Store is used.
The rollback checks compose real Core owner handlers and the Node journal with
a synchronous owner table; no Node execution or live RPC is simulated.
Constructed Complete/adoption values are input facts for this local reducer;
these tests do not claim that a Node executed or that Core performed owner CAS.
"""

from dataclasses import fields, is_dataclass, replace
import hashlib

import pytest

from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_handoff import (
    OutputHandoffConflictError, OutputHandoffPhase, OutputHandoffStateError,
    OutputHandoffTable,
)
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.output_publication_journal import OutputPublicationAdoptionProof
from miniray.protocol import ResultStorage
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest


pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes((number,)) * 16)


class _Fixture:
    def __init__(self):
        self.owner = _id(WorkerID, 1)
        self.executor = _id(WorkerID, 2)
        self.task = _id(TaskID, 3)
        self.attempt = AttemptID(self.task, 0)
        self.output = ObjectID.for_task(self.task)
        self.identity = OutputPublicationID(
            _id(LeaseID, 4), TaskExecutionKey(
                TaskOutputManifest.for_task(self.task, 1), self.attempt,
            ),
        )
        header = OutputPublicationHeader(
            self.identity, _id(JobID, 5), self.executor, self.owner,
            OutputPublicationNodeIncarnation(_id(NodeID, 6), 1001, 1),
        )
        self.child = ObjectID.for_task(_id(TaskID, 7))
        transfer = PreparedContainedTransfer(
            self.child, self.executor, ("owner.invalid", 1234),
            OwnedContainedSource(self.executor),
            ContainedReferenceHold(self.output, self.executor, "child-transfer"),
            ContainedReferenceHold(self.output, self.owner, "child-transfer"),
        )
        slot = OutputSlotManifest(
            self.output, ResultStorage.INLINE, 5,
            hashlib.sha256(b"value").hexdigest(), (transfer,),
        )
        self.manifest = OutputPublicationManifest.create(header, (slot,))
        self.complete = OutputPublicationCompleteWitness.for_manifest(self.manifest)
        self.adoption = OutputPublicationAdoptionProof(self.complete, self.owner, "owner-cas-1")
        self.table = OutputHandoffTable()


def _metadata_only(value):
    if type(value) in (JobID, LeaseID, NodeID, TaskID, WorkerID):
        return
    assert not isinstance(value, (bytes, bytearray, memoryview))
    if is_dataclass(value):
        for item in fields(value):
            _metadata_only(getattr(value, item.name))
    elif isinstance(value, tuple):
        for item in value:
            _metadata_only(item)


def test_registration_keeps_exact_single_manifest_with_child_and_no_payload():
    f = _Fixture()
    assert f.table.query(f.identity) is None
    registered = f.table.register(f.manifest, f.attempt)
    assert registered.phase is OutputHandoffPhase.PENDING
    assert registered.manifest == f.manifest and registered.complete is None
    assert registered.adoption is None and registered.abort_reason is None
    assert registered.manifest.slots[0].transfers[0].contained_object_id == f.child
    assert f.table.register(f.manifest, f.attempt) == registered
    assert f.table.snapshots() == (registered,)
    _metadata_only(registered)


@pytest.mark.parametrize("change", ("checksum", "child-address", "executor-incarnation"))
def test_registration_binds_all_manifest_fields_not_only_identity(change):
    f = _Fixture()
    before = f.table.register(f.manifest, f.attempt)
    header, slot = f.manifest.header, f.manifest.slots[0]
    if change == "checksum":
        slot = replace(slot, checksum=hashlib.sha256(b"other").hexdigest())
    elif change == "child-address":
        slot = replace(slot, transfers=(replace(
            slot.transfers[0], contained_owner_address=("another.invalid", 1234),
        ),))
    else:
        header = replace(header, node_incarnation=replace(header.node_incarnation, registration_epoch=2))
    changed = OutputPublicationManifest.create(header, (slot,))
    assert changed.publication_id == f.identity
    with pytest.raises(OutputHandoffConflictError, match="different manifest"):
        f.table.register(changed, f.attempt)
    assert f.table.query(f.identity) == before


def test_registration_checks_current_attempt_but_history_query_does_not():
    f = _Fixture()
    with pytest.raises(OutputHandoffStateError, match="current single-output"):
        f.table.register(f.manifest, f.attempt.next())
    assert f.table.query(f.identity) is None
    f.table.register(f.manifest, f.attempt)
    f.table.record_complete(f.complete)
    adopted = f.table.adopt(f.adoption)
    with pytest.raises(OutputHandoffStateError, match="current single-output"):
        f.table.register(f.manifest, f.attempt.next())
    assert f.table.query(f.identity) == adopted


def test_abort_before_registration_fences_late_manifest_and_complete():
    f = _Fixture()
    assert f.table.abort(f.identity, "cancelled before registration")
    aborted = f.table.query(f.identity)
    assert aborted.phase is OutputHandoffPhase.ABORTED and aborted.manifest is None
    assert not f.table.abort(f.identity, "cancelled before registration")
    with pytest.raises(OutputHandoffStateError, match="aborted"):
        f.table.register(f.manifest, f.attempt)
    with pytest.raises(OutputHandoffConflictError):
        f.table.record_complete(f.complete)
    with pytest.raises(OutputHandoffConflictError):
        f.table.adopt(f.adoption)
    assert f.table.query(f.identity) == aborted


def test_abort_registered_pending_blocks_new_complete_and_reason_rebinding():
    f = _Fixture()
    f.table.register(f.manifest, f.attempt)
    assert f.table.abort(f.identity, "owner cancelled")
    before = f.table.query(f.identity)
    with pytest.raises(OutputHandoffStateError, match="aborted"):
        f.table.record_complete(f.complete)
    with pytest.raises(OutputHandoffConflictError, match="reason"):
        f.table.abort(f.identity, "different cancellation")
    assert f.table.query(f.identity) == before
    assert before.manifest == f.manifest and before.complete is None


def test_abort_preserves_known_complete_and_exact_replay_without_forward_progress():
    f = _Fixture()
    f.table.register(f.manifest, f.attempt)
    f.table.record_complete(f.complete)
    assert f.table.abort(f.identity, "known success cannot be delivered")
    aborted = f.table.query(f.identity)
    assert aborted.complete == f.complete and aborted.phase is OutputHandoffPhase.ABORTED
    assert f.table.record_complete(f.complete) == aborted
    with pytest.raises(OutputHandoffStateError, match="aborted"):
        f.table.adopt(f.adoption)
    assert f.table.query(f.identity) == aborted


def test_adoption_requires_registered_matching_complete_and_owner():
    f = _Fixture()
    with pytest.raises(OutputHandoffStateError, match="not been registered"):
        f.table.record_complete(f.complete)
    f.table.register(f.manifest, f.attempt)
    with pytest.raises(OutputHandoffConflictError, match="registered owner and Complete"):
        f.table.adopt(f.adoption)
    with pytest.raises(OutputHandoffConflictError, match="registered manifest"):
        f.table.record_complete(replace(f.complete, manifest_digest="f" * 64))
    f.table.record_complete(f.complete)
    with pytest.raises(OutputHandoffConflictError, match="registered owner and Complete"):
        f.table.adopt(replace(f.adoption, owner_worker_id=_id(WorkerID, 8)))
    assert f.table.query(f.identity).phase is OutputHandoffPhase.PENDING


def test_adopted_history_survives_complete_replay_and_cannot_abort_or_rebind():
    f = _Fixture()
    f.table.register(f.manifest, f.attempt)
    f.table.record_complete(f.complete)
    adopted = f.table.adopt(f.adoption)
    assert adopted.phase is OutputHandoffPhase.ADOPTED
    assert adopted.complete == f.complete and adopted.adoption == f.adoption
    assert f.table.record_complete(f.complete) == adopted
    assert f.table.adopt(f.adoption) == adopted
    assert not f.table.abort(f.identity, "late cancellation")
    with pytest.raises(OutputHandoffConflictError, match="historical receipt"):
        f.table.adopt(replace(f.adoption, owner_commit_id="different-cas"))
    assert f.table.query(f.identity) == adopted
    _metadata_only(adopted)


def test_caller_mutation_cannot_change_saved_manifest_or_query_history():
    f = _Fixture()
    saved = f.table.register(f.manifest, f.attempt)
    object.__setattr__(f.manifest.slots[0].transfers[0], "contained_owner_address", ("changed.invalid", 9))
    assert f.table.query(f.identity) == saved
    queried = f.table.query(f.identity)
    object.__setattr__(queried.manifest.slots[0], "checksum", "f" * 64)
    assert f.table.query(f.identity) == saved


def test_abort_manifest_binds_unknown_cleanup_without_forward_registration():
    f = _Fixture()
    f.table.abort(f.identity, "earlier cancellation")
    snapshot = f.table.abort_manifest(f.manifest, "node rollback:late")
    assert snapshot.manifest == f.manifest and snapshot.phase is OutputHandoffPhase.ABORTED
    assert snapshot.abort_reason == "earlier cancellation"
    assert f.table.abort_manifest(f.manifest, "node rollback:late") == snapshot
    with pytest.raises(OutputHandoffStateError, match="aborted"):
        f.table.register(f.manifest, f.attempt)
    changed = OutputPublicationManifest.create(f.manifest.header, (replace(
        f.manifest.slots[0], checksum="f" * 64,
    ),))
    with pytest.raises(OutputHandoffConflictError):
        f.table.abort_manifest(changed, "node rollback:late")
    assert f.table.query(f.identity) == snapshot


def _rollback_core(f):
    # Real Core methods with no runtime constructor or background work.
    from threading import RLock, Condition
    from miniray.core import CoreWorker
    from miniray.ownership import ObjectOwnerTable
    core = object.__new__(CoreWorker)
    core.worker_id = f.owner
    core._owner_protocol_open = True
    core._state_lock = RLock()
    core._completion = Condition(core._state_lock)
    core._owner_table = ObjectOwnerTable()
    core._output_handoffs = f.table
    return core


def _journal(f):
    from miniray.output_publication_journal import OutputPublicationJournal
    journal = OutputPublicationJournal()
    journal.open(f.manifest)
    journal.begin_owner_register(f.identity)
    return journal


def test_real_registration_rejection_can_ack_empty_node_rollback_and_fence_late_register():
    from miniray import output_protocol as wire
    f = _Fixture()
    core = _rollback_core(f)
    rejected = core.register_output_handoff(wire.RegisterOutputHandoff(f.manifest))
    assert not rejected.accepted and f.table.query(f.identity) is None
    journal = _journal(f)
    plan = journal.begin_rollback(f.identity, "registration-rejected")
    assert not plan.effects
    tombstone = journal.snapshot(f.identity).rollback_tombstone
    request = wire.ReportOutputHandoffRollback(f.manifest, tombstone)
    acknowledged = core.report_output_handoff_rollback(request)
    assert acknowledged.accepted and acknowledged.request == request
    assert acknowledged.snapshot.phase is OutputHandoffPhase.ABORTED
    assert acknowledged.snapshot.manifest == f.manifest
    assert core.report_output_handoff_rollback(request) == acknowledged
    with pytest.raises(OutputHandoffStateError, match="aborted"):
        f.table.register(f.manifest, f.attempt)


def test_unknown_registration_and_child_ack_keep_full_compensation_obligation():
    from miniray import output_protocol as wire
    from miniray.output_publication_journal import (
        OutputPublicationRollbackTombstone, OutputPublicationStage,
    )
    from miniray.output_publication_node import OutputPublicationNodeAdapter
    from miniray.ownership import ObjectOwnerTable
    from miniray import protocol
    from miniray import enhanced_publication as enhanced
    from miniray.transport import TransportTimeout
    f = _Fixture()
    core = _rollback_core(f)
    core.owner_address = ("owner.invalid", 1234)
    authority = enhanced.PublicationAuthority()
    publication = enhanced.TaskPublication(f.manifest, core.owner_address)
    child = ObjectOwnerTable()
    child.register(f.child, current_attempt=AttemptID(f.child.task_id, 0))
    child.publish_inline(f.child, AttemptID(f.child.task_id, 0), b"child")
    registers, releases = [], []

    def register(manifest):
        # Register really commits, but its first response is lost.
        f.table.register(manifest, f.attempt)
        registers.append(manifest)
        if len(registers) == 1:
            raise TransportTimeout("owner registration ACK lost")

    def prepare(address, request):
        assert address == f.manifest.slots[0].transfers[0].contained_owner_address
        child.prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        raise TransportTimeout("child prepare ACK lost after actual hold")

    def release(address, request):
        assert address == f.manifest.slots[0].transfers[0].contained_owner_address
        released = child.release_contained_reference(request.object_id, request.hold)
        releases.append(request)
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def report(tombstone, *, manifest):
        reply = core.report_output_handoff_rollback(wire.ReportOutputHandoffRollback(manifest, tombstone))
        assert reply.accepted, reply.error

    def forbidden(*args, **kwargs):
        pytest.fail("unreached publication effect")

    def abort_owner(value, scope):
        reply = core.abort_owner_publication(enhanced.AbortOwnerPublication(value, scope))
        assert reply.accepted, reply.error
        return reply.receipt

    journal = _journal(f)
    adapter = OutputPublicationNodeAdapter(
        journal, register_owner=register, report_complete=forbidden, report_rollback=report,
        prepare_child=prepare, promote_child=forbidden, release_child=release,
        seal_replica=forbidden, drop_replica=forbidden,
        publication_value=lambda manifest: enhanced.TaskPublication(manifest, core.owner_address),
        publication_rpc=authority.apply, abort_owner=abort_owner,
    )
    with pytest.raises(TransportTimeout, match="registration"):
        adapter.prepare(f.manifest, (b"value",))
    assert f.table.query(f.identity).phase is OutputHandoffPhase.PENDING
    assert not child.snapshot(f.child).contained_holds and not releases
    with pytest.raises(TransportTimeout, match="child prepare"):
        adapter.prepare(f.manifest, (b"value",))
    hold = f.manifest.slots[0].transfers[0].provisional_hold
    assert hold in child.snapshot(f.child).contained_holds
    plan = journal.begin_rollback(f.identity, "partial-child")
    assert len(plan.effects) == 1 and plan.effects[0].stage is OutputPublicationStage.PROVISIONAL_RELEASE
    with pytest.raises(ValueError, match="every ordered effect ACK"):
        OutputPublicationRollbackTombstone(plan, ())
    assert journal.snapshot(f.identity).rollback_tombstone is None
    assert f.table.query(f.identity).phase is OutputHandoffPhase.PENDING
    completed = adapter.rollback(f.identity, "partial-child")
    assert completed is not None and len(releases) == 1
    assert not child.snapshot(f.child).contained_holds
    assert f.table.query(f.identity).phase is OutputHandoffPhase.ABORTED
    assert adapter.rollback(f.identity, "partial-child") == completed
    assert len(releases) == 1
    assert not authority.query(enhanced.GetPublication(publication.reference)).snapshot.graph_active


@pytest.mark.parametrize("invalid", ("wrong-owner", "absent-child-index", "unregistered-effect"))
def test_rollback_rejects_wrong_owner_or_out_of_manifest_effect_without_binding(invalid):
    from miniray import output_protocol as wire
    from miniray.output_publication_journal import (
        OutputPublicationAck, OutputPublicationEffect, OutputPublicationRollbackPlan,
        OutputPublicationRollbackTombstone, OutputPublicationStage,
    )
    f = _Fixture()
    core = _rollback_core(f)
    effects = ()
    if invalid == "wrong-owner":
        core.worker_id = _id(WorkerID, 9)
    else:
        effects = (OutputPublicationEffect(
            f.identity, f.manifest.manifest_digest, OutputPublicationStage.PROVISIONAL_RELEASE,
            0, 1 if invalid == "absent-child-index" else 0,
        ),)
    plan = OutputPublicationRollbackPlan(f.identity, f.manifest.manifest_digest, "invalid", effects)
    tombstone = OutputPublicationRollbackTombstone(plan, tuple(OutputPublicationAck(effect) for effect in effects))
    reply = core.report_output_handoff_rollback(wire.ReportOutputHandoffRollback(f.manifest, tombstone))
    assert not reply.accepted and f.table.query(f.identity) is None
    assert not getattr(core, "_output_rollback_receipts", {})


def test_core_rollback_replay_binding_cannot_be_replaced_or_override_complete():
    from miniray import output_protocol as wire
    from miniray.output_publication_journal import OutputPublicationRollbackTombstone
    f = _Fixture()
    core = _rollback_core(f)
    journal = _journal(f)
    plan = journal.begin_rollback(f.identity, "first-rollback")
    original = wire.ReportOutputHandoffRollback(f.manifest, journal.snapshot(f.identity).rollback_tombstone)
    acknowledged = core.report_output_handoff_rollback(original)
    assert acknowledged.accepted
    rebound = wire.ReportOutputHandoffRollback(
        f.manifest, OutputPublicationRollbackTombstone(replace(plan, rollback_id="rebound"), ()),
    )
    assert not core.report_output_handoff_rollback(rebound).accepted
    assert core.report_output_handoff_rollback(original) == acknowledged
    assert core._output_rollback_receipts[f.identity] == original.tombstone

    successful = _Fixture()
    successful_core = _rollback_core(successful)
    successful.table.register(successful.manifest, successful.attempt)
    complete_snapshot = successful.table.record_complete(successful.complete)
    rejected = successful_core.report_output_handoff_rollback(original)
    assert not rejected.accepted and successful.table.query(successful.identity) == complete_snapshot
    assert not getattr(successful_core, "_output_rollback_receipts", {})
