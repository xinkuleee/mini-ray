"""Finite owner reducer carriers for API-014..017 and API-019..020.

One result per task; at most two distinct child references. These tests inspect
local CAS and frozen plans, and perform real child-table releases. They do not
claim RPC or physical deletion evidence.
"""

from dataclasses import replace
import hashlib

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceEdge, ContainedReferenceHold, LineageReferenceEdge
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationHeader,
    OutputPublicationID, OutputPublicationManifest, OutputPublicationNodeIncarnation,
    OutputValue,
)
from miniray.ownership import (
    ConflictingObjectResultError, InvalidObjectTransitionError,
    ObjectCollectionInProgressError, ObjectOwnerTable, ObjectState,
    OutputOwnerPublicationCollectionRequiredError, OutputOwnerPublicationConflictError,
    OutputOwnerPublicationPlan,
)
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecution

pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes([number]) * 16)


class _Fixture:
    def __init__(self, *, stored=False, task=False, live=False):
        job, task_id = _id(JobID, 1), _id(TaskID, 2)
        self.owner, self.child_owner, self.node = _id(WorkerID, 3), _id(WorkerID, 4), _id(NodeID, 5)
        self.output, self.attempt = ObjectID.for_task(task_id), AttemptID(task_id, 0)
        self.spec = protocol.TaskSpec(job, task_id, self.attempt,
            protocol.FunctionKey(job, __name__, 'fixture', '1'),
            (), 1, ResourceVector(), self.owner)
        self.execution = TaskExecution.from_task_spec(self.spec)
        payload = b'one whole result'
        self.descriptor = protocol.ResultDescriptor(
            self.output, protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE,
            len(payload), self.owner, self.node, hashlib.sha256(payload).hexdigest(),
            None if stored else payload)
        self.transfers = tuple(PreparedContainedTransfer(
            ObjectID.for_task(_id(TaskID, number)), self.child_owner, ('child.invalid', 1234),
            OwnedContainedSource(self.child_owner),
            ContainedReferenceHold(self.output, self.child_owner, 'hold:' + str(number)),
            ContainedReferenceHold(self.output, self.owner, 'hold:' + str(number)),
        ) for number in (6, 7))
        self.edges = tuple(ContainedReferenceEdge(
            self.output, value.contained_object_id, self.child_owner,
            value.contained_owner_address, value.final_hold.transfer_token) for value in self.transfers)
        self.table = ObjectOwnerTable()
        if task:
            self.table.register_task_outputs(self.spec, local_tokens=('live',) if live else None)
        else:
            self.table.register(self.output, current_attempt=self.attempt, local_token='live' if live else None)

    def put(self, edges=None):
        return self.table.publish_put_value(self.output, self.attempt, self.descriptor,
            self.edges if edges is None else edges)

    def unified_plan(self, transfers=None):
        identity = OutputPublicationID(_id(LeaseID, 8), self.execution)
        manifest = OutputPublicationManifest.create(
            OutputPublicationHeader(identity, self.spec.job_id, self.child_owner, self.owner,
                OutputPublicationNodeIncarnation(self.node, 1001, 1)),
            (OutputValue(self.descriptor.storage, self.descriptor.size_bytes, self.descriptor.checksum, self.transfers if transfers is None else transfers)))
        return OutputOwnerPublicationPlan(self.execution, OutputPublicationEnvelope(
            manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (self.descriptor)))


def test_atomic_put_edges_freeze_until_exact_plan_completes_after_child_release():
    f = _Fixture(live=True)
    children = ObjectOwnerTable()
    for transfer in f.transfers:
        children.register(transfer.contained_object_id)
        assert children.add_contained_reference(transfer.contained_object_id, transfer.final_hold)
    assert f.put()
    assert f.put(edges=f.edges + (f.edges[0],))
    assert f.table.snapshot(f.output).outgoing_contained_edges == frozenset(f.edges)
    assert f.table.begin_collection(f.output) is None
    assert f.table.release_local_reference(f.output, 'live')
    plan = f.table.begin_collection(f.output, collection_id='exact-gc')
    assert plan is not None and plan.contained_releases == tuple(sorted(f.edges))
    assert f.table.contains(f.output)
    assert f.table.begin_collection(f.output, collection_id='ignored-retry') == plan
    frozen = f.table.snapshot(f.output)
    with pytest.raises(InvalidObjectTransitionError, match='identity changed'):
        f.table.complete_collection(replace(plan, collection_id='wrong'))
    with pytest.raises(ObjectCollectionInProgressError):
        f.put()
    assert f.table.snapshot(f.output) == frozen
    for edge in plan.contained_releases:
        hold = ContainedReferenceHold(edge.container_object_id, f.owner, edge.transfer_token)
        assert children.release_contained_reference(edge.contained_object_id, hold)
        assert not children.release_contained_reference(edge.contained_object_id, hold)
        assert children.contained_release_was_seen(edge.contained_object_id, hold)
    collected = f.table.complete_collection(plan)
    assert collected.collected and collected.contained_releases == plan.contained_releases
    assert not f.table.contains(f.output)


def test_all_reference_reasons_block_collection_until_each_exact_release():
    f = _Fixture(live=True)
    assert f.put(edges=())
    borrower = _id(WorkerID, 9)
    submitted = protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.SUBMITTED,
        borrower, f.spec.task_id, f.attempt)
    contained = ContainedReferenceHold(ObjectID.for_task(_id(TaskID, 10)), borrower, 'incoming')
    assert f.table.add_submitted_reference(f.output, submitted)
    assert f.table.add_borrowed_reference(f.output, (borrower, 'borrowed'))
    assert f.table.add_contained_reference(f.output, contained)
    assert f.table.release_local_reference(f.output, 'live')
    assert f.table.begin_collection(f.output) is None
    assert f.table.release_submitted_reference(f.output, submitted)
    assert f.table.begin_collection(f.output) is None
    assert f.table.release_borrowed_reference(f.output, (borrower, 'borrowed'))
    assert f.table.begin_collection(f.output) is None
    assert f.table.release_contained_reference(f.output, contained)
    plan = f.table.begin_collection(f.output)
    assert plan is not None and f.table.complete_collection(plan).collected


def test_stored_collection_freezes_integrity_and_all_locations_without_deleting():
    f = _Fixture(stored=True)
    assert f.put()
    other_node = _id(NodeID, 11)
    assert f.table.publish_stored(f.output, f.attempt, other_node,
        descriptor=replace(f.descriptor, node_id=other_node))
    plan = f.table.begin_collection(f.output, collection_id='stored-gc',
        canonical_size_bytes=f.descriptor.size_bytes, canonical_checksum=f.descriptor.checksum)
    assert plan is not None
    assert plan.locations == tuple(sorted((f.node, other_node)))
    assert plan.canonical_checksum == f.descriptor.checksum
    assert plan.canonical_size_bytes == f.descriptor.size_bytes
    assert plan.contained_releases == tuple(sorted(f.edges))
    assert f.table.contains(f.output)
    before = f.table.snapshot(f.output)
    with pytest.raises(ObjectCollectionInProgressError):
        f.table.add_local_reference(f.output, 'late')
    assert f.table.snapshot(f.output) == before
    # Actual DropObjectReplica ACK ordering is covered by the owned-drop
    # boundary and exact stored-GC smoke selector, never inferred from this plan.


def test_unified_result_cannot_change_child_edges_or_use_plain_collection_commit():
    f = _Fixture(task=True, live=True)
    plan = f.unified_plan()
    assert f.table.commit_output_publication(plan).committed
    before = f.table.snapshot(f.output)
    changed = replace(f.transfers[0],
        provisional_hold=replace(f.transfers[0].provisional_hold, transfer_token='changed'),
        final_hold=replace(f.transfers[0].final_hold, transfer_token='changed'))
    with pytest.raises(OutputOwnerPublicationConflictError):
        f.table.commit_output_publication(f.unified_plan((changed, f.transfers[1])))
    assert f.table.snapshot(f.output) == before
    assert f.table.begin_output_publication_collection(f.output, collection_id='output-gc') is None
    assert f.table.release_local_reference(f.output, 'live')
    claim = f.table.begin_output_publication_collection(f.output, collection_id='output-gc')
    assert claim is not None and claim.metadata_plan.contained_releases == tuple(sorted(f.edges))
    frozen = f.table.snapshot(f.output)
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError):
        f.table.complete_collection(claim.metadata_plan)
    assert f.table.snapshot(f.output) == frozen


def test_single_output_lineage_release_is_frozen_until_exact_metadata_commit():
    f = _Fixture(task=True)
    dependency = ObjectID.for_task(_id(TaskID, 15))
    edge = LineageReferenceEdge(f.output, dependency, 'lineage:input')
    assert f.table.add_outgoing_lineage_edge(f.output, edge)
    assert not f.table.add_outgoing_lineage_edge(f.output, edge)
    assert f.table.publish_inline(f.output, f.attempt, b'plain result')
    plan = f.table.begin_collection(f.output, collection_id='lineage-gc')
    assert plan is not None and plan.lineage_releases == (edge,)
    assert f.table.contains(f.output)
    assert f.table.begin_collection(f.output) == plan
    collected = f.table.complete_collection(plan)
    assert collected.collected and collected.lineage_releases == (edge,)
    assert not f.table.contains(f.output)


@pytest.mark.parametrize('stored', (False, True), ids=('inline', 'stored'))
def test_plain_result_validates_and_commits_under_one_composition_lock(stored):
    f = _Fixture(stored=stored, task=True)
    before = f.table.snapshot(f.output)
    # Pure fixture owns this table exclusively. Production Actor publication
    # holds Core._state_lock continuously across the same validated pair.
    with f.table._lock:
        plan = f.table.validate_publish_task_outputs(f.execution, (f.descriptor,))
        assert plan is not None and f.table.snapshot(f.output) == before
        f.table.commit_validated_publish_task_outputs(plan)
    after = f.table.snapshot(f.output)
    assert after.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
    with f.table._lock:
        replay = f.table.validate_publish_task_outputs(f.execution, (f.descriptor,))
        assert replay is not None
        f.table.commit_validated_publish_task_outputs(replay)
    assert f.table.snapshot(f.output) == after


def test_plain_stored_replay_rejects_changed_descriptor_and_attempt_without_mutation():
    f = _Fixture(stored=True, task=True)
    with f.table._lock:
        plan = f.table.validate_publish_task_outputs(f.execution, (f.descriptor,))
        f.table.commit_validated_publish_task_outputs(plan)
    before = f.table.snapshot(f.output)
    changed = (replace(f.descriptor, node_id=_id(NodeID, 12)),
        replace(f.descriptor, size_bytes=f.descriptor.size_bytes + 1),
        replace(f.descriptor, checksum='f' * 64))
    for descriptor in changed:
        with f.table._lock, pytest.raises(ConflictingObjectResultError):
            f.table.validate_publish_task_outputs(f.execution, (descriptor,))
        assert f.table.snapshot(f.output) == before
    with pytest.raises(ValueError):
        f.table.validate_publish_task_outputs(f.execution, (replace(f.descriptor, owner_worker_id=_id(WorkerID, 13)),))
    with pytest.raises(ValueError):
        f.table.validate_publish_task_outputs(f.execution, ())
    with pytest.raises(ValueError):
        f.table.validate_publish_task_outputs(f.execution, (replace(f.descriptor, object_id=ObjectID.for_task(_id(TaskID, 14))),))
    assert f.table.validate_publish_task_outputs(f.execution.for_attempt(f.attempt.next()), (f.descriptor,)) is None
    assert f.table.snapshot(f.output) == before


def test_stale_preflight_is_revalidated_before_plain_commit():
    f = _Fixture(task=True)
    old_plan = f.table.validate_publish_task_outputs(f.execution, (f.descriptor,))
    assert old_plan is not None
    assert f.table.publish_inline(f.output, f.attempt, b'a conflicting result')
    before = f.table.snapshot(f.output)
    # A plan kept across a composition boundary is not a validated commit
    # capability. Re-run validation; never feed old_plan to unchecked commit.
    with f.table._lock, pytest.raises(ConflictingObjectResultError):
        f.table.validate_publish_task_outputs(old_plan.execution, old_plan.results)
    assert f.table.snapshot(f.output) == before


def test_plain_result_cannot_revive_an_exact_retired_output_attempt():
    f = _Fixture(stored=True, task=True)
    assert f.table.commit_output_publication(f.unified_plan()).committed
    children = ObjectOwnerTable()
    for transfer in f.transfers:
        children.register(transfer.contained_object_id)
        children.prepare_stored_contained_reference(transfer, authority_worker_id=f.child_owner)
        children.promote_stored_contained_reference(transfer, authority_worker_id=f.child_owner)
    assert f.table.mark_lost(f.output, f.attempt)
    membership = f.table.output_owner_publication(f.output)
    retirement = f.table.begin_output_publication_retirement(
        (membership,), retirement_id='old-result', replica_locations={f.output: (f.node,)})
    releases = []
    for request in retirement.contained_releases:
        released = children.release_contained_reference(request.object_id, request.hold)
        assert released and children.contained_release_was_seen(request.object_id, request.hold)
        releases.append(protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released))
    request, = retirement.replica_drops
    # The byte-drop receipt is an explicit pure-model input, not evidence of
    # deletion. The existing exact stored-GC smoke validates physical deletion.
    drop = protocol.DropObjectReplicaReply(request.object_id, request.producer_attempt_id,
        request.owner_worker_id, request.node_id, request.checksum, protocol.DropObjectReplicaStatus.DROPPED)
    f.table.complete_output_publication_retirement(
        retirement, released_edges=tuple(releases), dropped_replicas=(drop,))
    before = f.table.snapshot(f.output)
    with f.table._lock, pytest.raises(InvalidObjectTransitionError):
        f.table.validate_publish_task_outputs(f.execution, (f.descriptor,))
    assert f.table.snapshot(f.output) == before
