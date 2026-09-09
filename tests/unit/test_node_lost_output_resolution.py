"""Pure owner transitions after a publisher's confirmed exit.

Complete and death records are inputs, not runtime observations. Child release
replies are backed by a second owner table; no Node, Core, Store, transport,
thread, process, or user function is started by these tests.
"""

from dataclasses import replace
import hashlib

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_handoff import (
    NodeLostOutputResolution, OutputHandoffConflictError, OutputHandoffTable,
)
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.ownership import (
    ObjectOwnerTable, ObjectState, OutputOwnerPublicationConflictError,
    OutputOwnerPublicationPlan,
)
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey


pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes((number,)) * 16)


class _Fixture:
    def __init__(self, *, stored=False):
        self.job = _id(JobID, 1)
        self.task = _id(TaskID, 2)
        self.owner = _id(WorkerID, 3)
        self.executor = _id(WorkerID, 4)
        self.node = _id(NodeID, 5)
        self.attempt = AttemptID(self.task, 0)
        self.output = ObjectID.for_task(self.task)
        self.spec = protocol.TaskSpec(
            self.job, self.task, self.attempt,
            protocol.FunctionKey(self.job, __name__, "fixture", "1"),
            (), 1, ResourceVector(), self.owner,
        )
        self.execution = TaskExecutionKey.from_task_spec(self.spec)
        self.identity = OutputPublicationID(_id(LeaseID, 6), self.execution)
        self.child = ObjectID.for_task(_id(TaskID, 7))
        self.transfer = PreparedContainedTransfer(
            self.child, self.executor, ("child.invalid", 1234),
            OwnedContainedSource(self.executor),
            ContainedReferenceHold(self.output, self.executor, "child-handoff"),
            ContainedReferenceHold(self.output, self.owner, "child-handoff"),
        )
        self.payload = b"retained result"
        tier = protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE
        self.slot = OutputSlotManifest(
            self.output, tier, len(self.payload), hashlib.sha256(self.payload).hexdigest(),
            (self.transfer,),
        )
        self.manifest = OutputPublicationManifest.create(
            OutputPublicationHeader(
                self.identity, self.job, self.executor, self.owner,
                OutputPublicationNodeIncarnation(self.node, 1001, 2),
            ),
            (self.slot,),
        )
        self.complete = OutputPublicationCompleteWitness.for_manifest(self.manifest)
        self.descriptor = protocol.ResultDescriptor(
            self.output, tier, self.slot.size_bytes, self.owner, self.node,
            self.slot.checksum, None if stored else self.payload,
        )
        self.envelope = OutputPublicationEnvelope(self.manifest, self.complete, (self.descriptor,))
        self.plan = OutputOwnerPublicationPlan(self.execution, self.envelope)
        self.death = protocol.NodeDeathRecord(
            "publisher-exit", self.node, 1001, 2, 3, 1,
            protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
        )
        self.table = ObjectOwnerTable()
        self.table.register_task_outputs(self.spec, local_tokens=("live-handle",))
        self.handoff = OutputHandoffTable()
        self.handoff.register(self.manifest, self.attempt)
        self.child_table = ObjectOwnerTable()
        self.child_table.register(self.child, local_token="child-handle")
        self.child_table.publish_inline(self.child, None, b"child value")
        self.child_table.prepare_stored_contained_reference(
            self.transfer, authority_worker_id=self.executor,
        )
        self.child_table.promote_stored_contained_reference(
            self.transfer, authority_worker_id=self.executor,
        )

    def cleanup(self):
        return tuple(
            protocol.ReleaseContainedReferenceReply(
                self.child, self.executor, hold, True,
                self.child_table.release_contained_reference(self.child, hold),
            )
            for hold in (self.transfer.final_hold, self.transfer.provisional_hold)
        )

    def resolution(self, *, known=False, keep=False, cleanup=()):
        if known:
            self.handoff.record_complete(self.complete)
        return NodeLostOutputResolution(
            self.identity, self.manifest.manifest_digest, self.owner, self.death,
            self.handoff.query(self.identity).complete, keep, cleanup,
        )


def test_node_loss_rejects_other_publisher_incarnation_without_owner_mutation():
    f = _Fixture()
    resolution = replace(f.resolution(cleanup=f.cleanup()),
                         node_death=replace(f.death, registration_epoch=3))
    before = f.table.snapshot(f.output)
    with pytest.raises(OutputHandoffConflictError, match="publishing incarnation"):
        f.table.resolve_output_node_loss(f.manifest, resolution)
    assert f.table.snapshot(f.output) == before
    assert f.table.output_owner_publication_receipt(f.plan) is None


def test_discard_requires_both_exact_child_hold_receipts_before_owner_mutation():
    f = _Fixture()
    before = f.table.snapshot(f.output)
    with pytest.raises(OutputHandoffConflictError, match="every final and provisional"):
        f.table.resolve_output_node_loss(f.manifest, f.resolution(known=True))
    assert f.table.snapshot(f.output) == before
    cleanup = f.cleanup()
    for partial in (cleanup[:1], cleanup[1:]):
        with pytest.raises(OutputHandoffConflictError, match="every final and provisional"):
            f.table.resolve_output_node_loss(f.manifest, f.resolution(known=True, cleanup=partial))
        assert f.table.snapshot(f.output) == before
    wrong = replace(cleanup[0], hold=replace(cleanup[0].hold, transfer_token="another-handoff"))
    with pytest.raises(OutputHandoffConflictError, match="manifest hold"):
        f.table.resolve_output_node_loss(
            f.manifest, f.resolution(known=True, cleanup=(wrong, cleanup[1])),
        )
    assert f.table.snapshot(f.output) == before


def test_known_complete_without_bytes_becomes_lost_and_keeps_live_references():
    f = _Fixture()
    outer_borrower = (_id(WorkerID, 8), "outer-reader")
    child_borrower = (_id(WorkerID, 9), "child-reader")
    f.table.add_borrowed_reference(f.output, outer_borrower)
    f.child_table.acquire_exported_reference(
        f.child, protocol.ContainedTransferSource(f.transfer.final_hold), child_borrower,
    )
    resolution = f.resolution(known=True, cleanup=f.cleanup())
    assert f.table.resolve_output_node_loss(f.manifest, resolution)
    snapshot = f.table.snapshot(f.output)
    assert snapshot.state is ObjectState.LOST
    assert snapshot.inline_data is None and snapshot.canonical_stored_result is None
    assert not snapshot.locations and snapshot.output_publication is None
    assert snapshot.local_tokens == frozenset({"live-handle"})
    assert snapshot.borrowed_tokens == frozenset({outer_borrower})
    assert f.table.begin_collection(f.output) is None
    child = f.child_table.snapshot(f.child)
    assert not child.contained_holds and child.borrowed_tokens == frozenset({child_borrower})
    assert child.inline_data == b"child value" and f.child_table.is_live(f.child)
    assert not f.table.resolve_output_node_loss(f.manifest, resolution)
    assert f.table.snapshot(f.output) == snapshot


def test_unknown_complete_stays_pending_without_success_and_fences_late_publish():
    f = _Fixture()
    resolution = f.resolution(cleanup=f.cleanup())
    assert f.table.resolve_output_node_loss(f.manifest, resolution)
    snapshot = f.table.snapshot(f.output)
    assert snapshot.state is ObjectState.PENDING
    assert snapshot.current_attempt == f.attempt
    assert snapshot.inline_data is None and snapshot.output_publication is None
    assert f.table.output_owner_publication_receipt(f.plan) is None
    assert f.handoff.query(f.identity).complete is None
    assert not f.table.commit_output_publication(f.plan).committed
    assert f.table.snapshot(f.output) == snapshot


def test_stored_survivor_cannot_be_discarded_and_keep_preserves_descriptor_and_holds():
    f = _Fixture(stored=True)
    f.handoff.record_complete(f.complete)
    assert f.table.commit_output_publication(f.plan).committed
    survivor = _id(NodeID, 10)
    f.table.publish_stored(f.output, f.attempt, survivor,
                           descriptor=replace(f.descriptor, node_id=survivor))
    before = f.table.snapshot(f.output)
    # Even real child cleanup cannot authorize erasing a live replica.
    with pytest.raises(OutputOwnerPublicationConflictError, match="surviving replicas"):
        f.table.resolve_output_node_loss(f.manifest, f.resolution(known=True, cleanup=f.cleanup()))
    assert f.table.snapshot(f.output) == before
    # The keep path starts independently with final child holds still live.
    f = _Fixture(stored=True)
    f.handoff.record_complete(f.complete)
    assert f.table.commit_output_publication(f.plan).committed
    f.table.publish_stored(f.output, f.attempt, survivor,
                           descriptor=replace(f.descriptor, node_id=survivor))
    child_before = f.child_table.snapshot(f.child)
    assert f.table.resolve_output_node_loss(f.manifest, f.resolution(known=True, keep=True))
    snapshot = f.table.snapshot(f.output)
    assert snapshot.state is ObjectState.READY_STORED
    assert snapshot.locations == frozenset({survivor})
    assert snapshot.canonical_stored_result == f.descriptor
    assert f.table.output_owner_result(f.output) == f.descriptor
    assert snapshot.outgoing_contained_edges == frozenset(f.slot.edges)
    assert f.child_table.snapshot(f.child) == child_before


def test_late_old_resolution_cannot_modify_new_attempt_ready_value():
    f = _Fixture()
    resolution = f.resolution(known=True, cleanup=f.cleanup())
    next_attempt = f.attempt.next()
    assert f.table.advance_task_outputs(f.execution, next_attempt)
    assert f.table.publish_inline(f.output, next_attempt, b"new attempt value")
    before = f.table.snapshot(f.output)
    with pytest.raises(OutputOwnerPublicationConflictError, match="fenced"):
        f.table.resolve_output_node_loss(f.manifest, resolution)
    assert f.table.snapshot(f.output) == before
    assert before.state is ObjectState.READY_INLINE
    assert before.inline_data == b"new attempt value"
    assert f.table.output_owner_publication_receipt(f.plan) is None
