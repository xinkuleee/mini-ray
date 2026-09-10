"""Pure single-output owner CAS and frozen metadata collection.

At most one publication and one unrelated control object per table. Explicit
Complete values are reducer inputs; these tests do not prove remote child
Release, physical Drop, Node execution or runtime GC. No runtime constructor,
network, thread or process is used.
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from enum import Enum

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold, LineageReferenceEdge
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.ownership import (
    InvalidObjectTransitionError, ObjectCollectionInProgressError, ObjectCollectionState, ObjectOwnerTable,
    ObjectState, OutputOwnerPublicationCollectionPlan,
    OutputOwnerPublicationCollectionRequiredError, OutputOwnerPublicationConflictError,
    OutputOwnerPublicationDisposition, OutputOwnerPublicationMembership,
    OutputOwnerPublicationPlan,
)
from miniray.resources import ResourceVector
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.task_outputs import TaskExecution


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, *, edges=True, all_stored=False):
        self.job = JobID.random()
        self.task = TaskID.derive(self.job, TaskID.for_driver(self.job), 31)
        self.attempt = AttemptID(self.task, 0)
        self.owner = WorkerID.random()
        self.executor = WorkerID.random()
        self.node = NodeID.random()
        self.spec = protocol.TaskSpec(
            self.job, self.task, self.attempt,
            protocol.FunctionKey(self.job, __name__, "producer", "v1"),
            (protocol.InlineArg(b"producer-lineage-argument"),),
            1, ResourceVector(), self.owner, max_retries=2,
        )
        self.execution = (TaskExecution.from_task_spec(self.spec))
        self.publication_id = OutputPublicationID(LeaseID.random(), self.execution)
        self.header = OutputPublicationHeader(
            self.publication_id, self.job, self.executor, self.owner,
            OutputPublicationNodeIncarnation(self.node, 301, 1),
        )
        self.child = ObjectID.for_task(TaskID.derive(self.job, self.task, 3))
        object_id = self.publication_id.object_id
        self.payload = b'return-value'
        tier = protocol.ResultStorage.OBJECT_STORE if all_stored else protocol.ResultStorage.INLINE
        transfers = ()
        if edges:
            transfers = (PreparedContainedTransfer(self.child, self.executor, ('127.0.0.1', 31800), OwnedContainedSource(self.executor), ContainedReferenceHold(object_id, self.executor, 'shared-token'), ContainedReferenceHold(object_id, self.owner, 'shared-token')),)
        checksum = hashlib.sha256(self.payload).hexdigest()
        self.value = OutputValue(tier, len(self.payload), checksum, transfers)
        self.result = protocol.ResultDescriptor(object_id, tier, len(self.payload), self.owner, self.node, checksum, self.payload if tier is protocol.ResultStorage.INLINE else None)
        self.manifest = OutputPublicationManifest.create(self.header, (self.value))
        self.envelope = OutputPublicationEnvelope(
            self.manifest, OutputPublicationCompleteWitness.for_manifest(self.manifest),
            (self.result),
        )
        self.plan = OutputOwnerPublicationPlan(self.execution, self.envelope)

    def table(self):
        table = ObjectOwnerTable()
        table.register_task_outputs(
            self.spec, local_tokens=tuple(f"handle-{i}" for i in range(self.spec.num_returns))
        )
        return table

    def release_handle(self, table, object_id):
        assert table.release_local_reference(object_id, f"handle-{object_id.return_index}")


def _snapshots(table, fixture):
    return tuple(table.snapshot(object_id) for object_id in fixture.spec.return_ids())


def _changed_result_plan(fixture):
    payload = b"changed-inline-result"
    first = replace(
        (fixture.envelope.result), size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(), inline_data=payload,
    )
    value = (replace(fixture.manifest.value, size_bytes=first.size_bytes, checksum=first.checksum))
    manifest = OutputPublicationManifest.create(fixture.header, value)
    envelope = OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
        first,
    )
    return OutputOwnerPublicationPlan(fixture.execution, envelope)


def _assert_metadata_only(value):
    if type(value) in (JobID, TaskID, LeaseID, NodeID, WorkerID):
        assert len(value.value) == 16
        return
    assert not isinstance(value, (
        bytes, bytearray, memoryview, protocol.TaskSpec, protocol.ResultDescriptor,
        OutputPublicationEnvelope, OutputOwnerPublicationPlan,
    ))
    if value is None or isinstance(value, (str, int, Enum)):
        return
    if is_dataclass(value) and not isinstance(value, type):
        for member in fields(value):
            _assert_metadata_only(getattr(value, member.name))
    elif isinstance(value, dict):
        for key, item in value.items():
            _assert_metadata_only(key)
            _assert_metadata_only(item)
    elif isinstance(value, (tuple, set, frozenset)):
        for item in value:
            _assert_metadata_only(item)
    else:
        raise AssertionError(type(value).__name__)


@pytest.mark.parametrize("stored", (False, True))
def test_single_output_publishes_once_with_metadata_only_owner_history(stored):
    fixture = _Fixture(all_stored=stored)
    table = fixture.table()
    before = _snapshots(table, fixture)
    assert table.validate_output_publication(fixture.plan) is OutputOwnerPublicationDisposition.APPLIED
    assert _snapshots(table, fixture) == before
    receipt = table.commit_output_publication(fixture.plan)
    assert receipt.committed and receipt.plan == fixture.plan
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    memberships = []
    for index, descriptor in enumerate(((fixture.envelope.result,))):
        snapshot = table.snapshot(descriptor.object_id)
        membership = snapshot.output_publication
        memberships.append(membership)
        assert membership == OutputOwnerPublicationMembership(fixture.manifest)
        assert table.output_owner_publication(descriptor.object_id) == membership
        assert table.output_owner_result(descriptor.object_id) == descriptor
        assert snapshot.outgoing_contained_edges == frozenset((fixture.manifest.value).edges)
        assert snapshot.producer_task_spec == fixture.spec
        assert snapshot.output_retirement_id is None
        assert all(not hasattr(snapshot, name) for name in (
            "inline_publication", "stored_publication", "stored_retirement_id",
        ))
        if descriptor.storage is protocol.ResultStorage.INLINE:
            assert snapshot.state is ObjectState.READY_INLINE
            assert snapshot.inline_data is descriptor.inline_data
            assert snapshot.canonical_stored_result is None and not snapshot.locations
        else:
            assert snapshot.state is ObjectState.READY_STORED
            assert snapshot.inline_data is None
            assert snapshot.canonical_stored_result == descriptor
            assert snapshot.locations == frozenset({fixture.node})
        _assert_metadata_only(membership)
    assert len({id(item.manifest) for item in memberships}) == len(memberships)
    internal = tuple(table._entries[object_id].output_publication
                     for object_id in ((fixture.publication_id.object_id,)))
    assert len({id(item.manifest) for item in internal}) == 1
    assert all(public.manifest is not private.manifest
               for public, private in zip(memberships, internal))
    _assert_metadata_only(table._output_publication_receipts)
    ready = _snapshots(table, fixture)
    replay = table.commit_output_publication(fixture.plan)
    assert replay.disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert table.output_owner_publication_receipt(fixture.plan).committed
    assert _snapshots(table, fixture) == ready


def test_conflicting_same_publication_replay_never_changes_ready_slots():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerPublicationConflictError, match="rebound"):
        table.commit_output_publication(_changed_result_plan(fixture))
    assert _snapshots(table, fixture) == before


@pytest.mark.parametrize("stored", (False, True))
def test_public_snapshot_and_membership_queries_cannot_poison_owner_metadata(stored):
    fixture = _Fixture(all_stored=stored)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    before = _snapshots(table, fixture)
    first = ((fixture.publication_id.object_id,))[0]
    snapshot = table.snapshot(first)
    object.__setattr__(snapshot.output_publication.manifest.header.owner_worker_id, "value", b"x" * 16)
    object.__setattr__(next(iter(snapshot.outgoing_contained_edges)).contained_object_id, "return_index", 9)
    object.__setattr__(snapshot.current_attempt, "attempt_number", 9)
    object.__setattr__(snapshot.object_id.task_id, "value", b"y" * 16)
    object.__setattr__(snapshot.producer_task_spec.args[0], "data", b"poisoned")
    membership = table.output_owner_publication(first)
    object.__setattr__((membership.manifest.value), "checksum", "0" * 64)
    object.__setattr__(membership.manifest.header.publication_id.lease_id, "value", b"z" * 16)
    if stored:
        stored_snapshot = table.snapshot(first)
        object.__setattr__(stored_snapshot.canonical_stored_result.node_id, "value", b"q" * 16)
        object.__setattr__(next(iter(stored_snapshot.locations)), "value", b"r" * 16)
    result = table.output_owner_result(first)
    object.__setattr__(result.object_id, "return_index", 99)
    assert _snapshots(table, fixture) == before
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED


@pytest.mark.parametrize("source", ["commit", "replay", "query"])
def test_public_commit_receipt_never_shares_the_authoritative_manifest(source):
    fixture = _Fixture()
    table = fixture.table()
    receipt = table.commit_output_publication(fixture.plan)
    if source == "replay":
        receipt = table.commit_output_publication(fixture.plan)
    elif source == "query":
        receipt = table.output_owner_publication_receipt(fixture.plan)
    before = _snapshots(table, fixture)
    object.__setattr__(receipt.plan.envelope.manifest, "manifest_digest", "f" * 64)
    object.__setattr__(receipt.plan.envelope.manifest.header.publication_id.lease_id, "value", b"c" * 16)
    object.__setattr__((receipt.plan.envelope.result), "checksum", "e" * 64)
    object.__setattr__(receipt.plan.execution.attempt_id, "attempt_number", 100)
    assert _snapshots(table, fixture) == before
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert table._output_publication_receipts[fixture.publication_id] == fixture.manifest


@pytest.mark.parametrize("bad_field", ["owner_worker_id", "job_id"])
def test_wrong_owner_or_job_has_no_partial_result_mutation(bad_field):
    fixture = _Fixture()
    table = fixture.table()
    different = WorkerID.random() if bad_field == "owner_worker_id" else JobID.random()
    header = replace(fixture.header, **{bad_field: different})
    # No child transfers are needed to isolate the owner/lineage identity.
    value = (replace(fixture.manifest.value, transfers=()))
    manifest = OutputPublicationManifest.create(header, value)
    result = (replace(fixture.envelope.result, owner_worker_id=header.owner_worker_id))
    plan = OutputOwnerPublicationPlan(fixture.execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), result
    ))
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerPublicationConflictError, match="owner and job"):
        table.commit_output_publication(plan)
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_stale_output_attempt_fences_publication_without_mutation():
    fixture = _Fixture()
    table = fixture.table()
    assert table.advance_attempt(
        ((fixture.publication_id.object_id,))[-1], expected_attempt=fixture.attempt,
        next_attempt=fixture.attempt.next(),
    )
    before = _snapshots(table, fixture)
    receipt = table.commit_output_publication(fixture.plan)
    assert receipt.disposition is OutputOwnerPublicationDisposition.FENCED
    assert not receipt.committed
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_existing_incompatible_result_cannot_be_replaced_by_publication():
    fixture = _Fixture()
    table = fixture.table()
    last = ((fixture.publication_id.object_id,))[-1]
    assert table.publish_inline(last, fixture.attempt, b"legacy-result")
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerPublicationConflictError, match="partial or incompatible"):
        table.commit_output_publication(fixture.plan)
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_plan_execution_and_nested_payload_revalidation_precede_owner_mutation():
    fixture = _Fixture()
    with pytest.raises(OutputOwnerPublicationConflictError, match="equal the envelope"):
        OutputOwnerPublicationPlan(fixture.execution.for_attempt(fixture.attempt.next()), fixture.envelope)
    table = fixture.table()
    forged = replace(fixture.plan)
    object.__setattr__((forged.envelope.result), "inline_data", b"wrong-bytes")
    before = _snapshots(table, fixture)
    with pytest.raises((TypeError, ValueError, ProtocolError)):
        table.commit_output_publication(forged)
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_tampered_manifest_and_membership_cannot_bypass_single_output_identity():
    fixture = _Fixture()
    table = fixture.table()
    forged = replace(fixture.plan)
    object.__setattr__(forged.envelope.manifest, "manifest_digest", "0" * 64)
    before = _snapshots(table, fixture)
    with pytest.raises(ValueError, match="manifest_digest"):
        table.commit_output_publication(forged)
    assert _snapshots(table, fixture) == before
    for invalid in (-1, True, 1, None, (), (fixture.manifest,), (fixture.manifest, fixture.manifest)):
        with pytest.raises(TypeError, match="OutputPublicationManifest"):
            OutputOwnerPublicationMembership(invalid)
    assert tuple(item.name for item in fields(OutputOwnerPublicationMembership)) == ("manifest",)


def test_publication_does_not_mutate_an_unrelated_object():
    fixture = _Fixture()
    table = fixture.table()
    unrelated = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 80))
    table.register(unrelated, current_attempt=AttemptID(unrelated.task_id, 0), local_token="unrelated")
    assert table.publish_inline(unrelated, AttemptID(unrelated.task_id, 0), b"unrelated")
    before = table.snapshot(unrelated)
    receipt = table.commit_output_publication(fixture.plan)
    assert receipt.committed
    assert table.snapshot(unrelated) == before
    assert table.snapshot(fixture.publication_id.object_id).output_publication == (
        OutputOwnerPublicationMembership(fixture.manifest)
    )
    assert tuple(object_id.return_index for object_id in ((fixture.publication_id.object_id,))) == (0,)
    for object_id in ((fixture.publication_id.object_id,)):
        assert table.snapshot(object_id).current_attempt == fixture.execution.attempt_id
        assert table.snapshot(object_id).producer_task_spec == fixture.spec


def test_publication_does_not_block_on_unrelated_collection():
    fixture = _Fixture()
    table = fixture.table()
    healthy = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 81))
    table.register(healthy, current_attempt=AttemptID(healthy.task_id, 0), local_token="unrelated")
    table.publish_inline(healthy, AttemptID(healthy.task_id, 0), b"healthy")
    assert table.release_local_reference(healthy, "unrelated")
    frozen = table.begin_collection(healthy, collection_id="healthy-gc")
    before = table.snapshot(healthy)
    assert frozen is not None
    assert table.commit_output_publication(fixture.plan).committed
    assert table.snapshot(healthy) == before


def test_output_collection_preserves_unrelated_object_and_its_lineage():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    other_task = TaskID.derive(fixture.job, fixture.task, 82)
    other_spec = replace(fixture.spec, task_id=other_task, attempt_id=AttemptID(other_task, 0))
    table.register_task_outputs(other_spec, local_tokens=("unrelated",))
    healthy = other_spec.return_ids()[0]
    table.publish_inline(healthy, other_spec.attempt_id, b"healthy-value")
    dependency = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 64))
    table.add_outgoing_lineage_edge(
        healthy, LineageReferenceEdge(healthy, dependency, "unrelated-lineage")
    )
    before = table.snapshot(healthy)
    table.commit_output_publication(fixture.plan)
    for object_id in ((fixture.publication_id.object_id,)):
        fixture.release_handle(table, object_id)
        plan = table.begin_output_publication_collection(
            object_id, collection_id=f"target-gc-{object_id.return_index}"
        )
        result = table.complete_output_publication_collection(plan)
        assert not result.collection.lineage_releases
    assert other_task in table._task_lineage and fixture.task not in table._task_lineage
    assert table.snapshot(healthy) == before


def test_current_output_collection_prevents_republication():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = ((fixture.publication_id.object_id,))[0]
    fixture.release_handle(table, first)
    assert table.begin_output_publication_collection(first, collection_id="pending-gc") is not None
    before = _snapshots(table, fixture)
    with pytest.raises(ObjectCollectionInProgressError, match="output collection"):
        table.commit_output_publication(fixture.plan)
    assert _snapshots(table, fixture) == before


def test_new_lease_publication_cannot_replace_unretired_membership():
    fixture = _Fixture(all_stored=True)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    target = ((fixture.publication_id.object_id,))[0]
    assert table.mark_lost(target, fixture.attempt)
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError, match="retirement"):
        table.advance_attempt(
            target, expected_attempt=fixture.attempt, next_attempt=fixture.attempt.next()
        )
    execution = fixture.execution
    header = replace(fixture.header, publication_id=OutputPublicationID(LeaseID.random(), execution))
    manifest = OutputPublicationManifest.create(header, (fixture.manifest.value))
    plan = OutputOwnerPublicationPlan(execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
        (fixture.envelope.result),
    ))
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError, match="unretired"):
        table.commit_output_publication(plan)
    assert _snapshots(table, fixture) == before


def test_lost_stored_slot_replay_never_restores_a_dead_replica():
    fixture = _Fixture(all_stored=True)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    target = ((fixture.publication_id.object_id,))[0]
    assert table.mark_lost(target, fixture.attempt)
    before = _snapshots(table, fixture)
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert _snapshots(table, fixture) == before
    assert table.output_owner_result(target) == (fixture.envelope.result)
    assert not table.snapshot(target).locations


def test_no_reference_output_uses_the_same_publication_and_collection():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    assert (fixture.manifest.value.edges) == ()
    assert table.commit_output_publication(fixture.plan).committed
    for object_id in ((fixture.publication_id.object_id,)):
        fixture.release_handle(table, object_id)
        plan = table.begin_output_publication_collection(
            object_id, collection_id=f"plain-gc-{object_id.return_index}"
        )
        receipt = table.complete_output_publication_collection(plan)
        assert receipt.collection.collected and not receipt.collection.contained_releases
    assert not table._entries
    _assert_metadata_only(table._output_publication_receipts)
    _assert_metadata_only(table._output_collection_receipts)


@pytest.mark.parametrize("stored", (False, True))
def test_output_collection_preserves_exact_edges_and_replay_without_payloads(stored):
    fixture = _Fixture(all_stored=stored)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    object_id = ((fixture.publication_id.object_id,))[0]
    slot = (fixture.manifest.value)
    assert table.begin_output_publication_collection(object_id, collection_id="slot-gc") is None
    fixture.release_handle(table, object_id)
    plan = table.begin_output_publication_collection(object_id, collection_id="slot-gc")
    assert plan is not None
    assert plan.metadata_plan.contained_releases == tuple(sorted(slot.edges))
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError, match="per-slot"):
        table.complete_collection(plan.metadata_plan)
    # Core is responsible for actual Release/Drop before this local metadata
    # CAS. This pure test checks its frozen plan, not fabricated remote ACKs.
    receipt = table.complete_output_publication_collection(plan)
    assert receipt.collection.contained_releases == tuple(sorted(slot.edges))
    assert not receipt.collection.lineage_releases
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    assert table.collection_state(object_id) is ObjectCollectionState.COLLECTED
    assert table.output_owner_result(object_id) is None
    assert table.output_owner_publication(object_id) is None
    _assert_metadata_only(table._output_collection_receipts)
    _assert_metadata_only(table._output_publication_receipts)
    replay = table.complete_output_publication_collection(plan)
    assert replay.disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert table.output_publication_collection_receipt(plan).collection == receipt.collection
    assert table.begin_output_publication_collection(object_id, collection_id="slot-gc") is None
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED


def test_collection_rejects_changed_manifest_membership_and_frozen_metadata():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = ((fixture.publication_id.object_id,))[0]
    fixture.release_handle(table, first)
    plan = table.begin_output_publication_collection(first, collection_id="first-gc")
    before = table.snapshot(first)
    changed_header = replace(fixture.header, publication_id=replace(fixture.publication_id, lease_id=LeaseID.random()))
    changed_manifest = OutputPublicationManifest.create(changed_header, (fixture.manifest.value))
    changed_member = OutputOwnerPublicationMembership(changed_manifest)
    with pytest.raises(OutputOwnerPublicationConflictError):
        table.complete_output_publication_collection(replace(plan, membership=changed_member))
    altered = replace(plan, metadata_plan=replace(plan.metadata_plan, collection_id="other-gc"))
    with pytest.raises(InvalidObjectTransitionError, match="identity changed"):
        table.complete_output_publication_collection(altered)
    assert table.snapshot(first) == before
    assert table.complete_output_publication_collection(plan).collection.collected


def test_empty_edge_collection_requires_no_invented_remote_receipt():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    empty = ((fixture.publication_id.object_id,))[0]
    fixture.release_handle(table, empty)
    plan = table.begin_output_publication_collection(empty, collection_id="empty-gc")
    assert plan.metadata_plan.contained_releases == ()
    assert plan.metadata_plan.locations == ()
    assert table.complete_output_publication_collection(plan).collection.collected


def test_single_output_collection_releases_its_child_and_final_task_lineage_once():
    fixture = _Fixture()
    table = fixture.table()
    first = ((fixture.publication_id.object_id,))[0]
    dependency = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 72))
    table.add_outgoing_lineage_edge(first, LineageReferenceEdge(first, dependency, "producer-lineage"))
    table.commit_output_publication(fixture.plan)
    fixture.release_handle(table, first)
    plan = table.begin_output_publication_collection(first, collection_id="single-gc")
    assert plan.metadata_plan.lineage_releases == (LineageReferenceEdge(first, dependency, "producer-lineage"),)
    assert fixture.task in table._task_lineage
    final = table.complete_output_publication_collection(plan)
    assert final.collection.contained_releases == tuple(sorted((fixture.manifest.value).edges))
    assert final.collection.lineage_releases == (
        LineageReferenceEdge(first, dependency, "producer-lineage"),
    )
    assert not table._task_lineage
    assert not table._entries
    _assert_metadata_only(table._output_collection_receipts)
    _assert_metadata_only(table._output_publication_receipts)
    assert table.complete_output_publication_collection(plan).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED


def test_terminal_collection_replay_binds_metadata_hash_without_storing_taskspec():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    object_id = ((fixture.publication_id.object_id,))[0]
    fixture.release_handle(table, object_id)
    plan = table.begin_output_publication_collection(object_id, collection_id="hash-gc")
    table.complete_output_publication_collection(plan)
    changed_spec = replace(fixture.spec, args=(protocol.InlineArg(b"different-lineage-arg"),))
    changed = OutputOwnerPublicationCollectionPlan(
        plan.membership, replace(plan.metadata_plan, producer_task_spec=changed_spec)
    )
    with pytest.raises(OutputOwnerPublicationConflictError, match="terminal identity"):
        table.complete_output_publication_collection(changed)
    with pytest.raises(OutputOwnerPublicationConflictError, match="terminal identity"):
        table.output_publication_collection_receipt(changed)
    with pytest.raises(OutputOwnerPublicationConflictError, match="identity changed"):
        table.begin_output_publication_collection(object_id, collection_id="another-gc")
    _assert_metadata_only(table._output_collection_receipts)


def test_public_collection_plans_are_detached_from_live_collection_claim():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = ((fixture.publication_id.object_id,))[0]
    fixture.release_handle(table, first)
    plan = table.begin_output_publication_collection(first, collection_id="detached-gc")
    stable_plan = deepcopy(plan)
    before = _snapshots(table, fixture)
    object.__setattr__(plan.membership.manifest.header.owner_worker_id, "value", b"a" * 16)
    object.__setattr__(plan.metadata_plan.contained_releases[0].contained_object_id, "return_index", 45)
    object.__setattr__(plan.metadata_plan.producer_task_spec.args[0], "data", b"poisoned")
    generic = table.begin_collection(first, collection_id="detached-gc")
    object.__setattr__(generic.object_id.task_id, "value", b"b" * 16)
    object.__setattr__(generic.producer_attempt_id, "attempt_number", 71)
    public_snapshot = table.snapshot(first)
    object.__setattr__(public_snapshot.collection_plan.contained_releases[0], "transfer_token", "poisoned")
    assert _snapshots(table, fixture) == before
    assert table.begin_output_publication_collection(first, collection_id="detached-gc") == stable_plan
    assert table.complete_output_publication_collection(stable_plan).collection.collected


@pytest.mark.parametrize("source", ["complete", "replay", "query"])
def test_public_collection_receipt_ids_cannot_poison_tombstone_history(source):
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = ((fixture.publication_id.object_id,))[0]
    fixture.release_handle(table, first)
    plan = table.begin_output_publication_collection(first, collection_id="receipt-gc")
    receipt = table.complete_output_publication_collection(plan)
    if source == "replay":
        receipt = table.complete_output_publication_collection(plan)
    elif source == "query":
        receipt = table.output_publication_collection_receipt(plan)
    terminal = deepcopy(table._output_collection_receipts[first])
    object.__setattr__(receipt.collection.contained_releases[0].contained_object_id, "return_index", 88)
    object.__setattr__(receipt.collection.object_id.task_id, "value", b"d" * 16)
    object.__setattr__(receipt.plan.metadata_plan.object_id.task_id, "value", b"e" * 16)
    object.__setattr__(receipt.plan.membership.manifest.header.publication_id.lease_id, "value", b"f" * 16)
    assert table._output_collection_receipts[first] == terminal
    assert table.collection_state(first) is ObjectCollectionState.COLLECTED
    assert table.output_publication_collection_receipt(plan).collection == terminal.collection
    assert table.complete_output_publication_collection(plan).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED


def test_legacy_objects_still_use_their_original_owner_and_gc_apis():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    object_id = ((fixture.publication_id.object_id,))[0]
    assert table.publish_inline(object_id, fixture.attempt, b"legacy")
    assert table.output_owner_publication(object_id) is None
    fixture.release_handle(table, object_id)
    metadata = table.begin_collection(object_id, collection_id="legacy-gc")
    assert table.complete_collection(metadata).collected


def test_published_output_edges_cannot_be_extended_by_rebound_atomic_plan():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = ((fixture.publication_id.object_id,))[0]
    edge = (fixture.manifest.value).edges[0]
    before = table.snapshot(first)
    transfer = (fixture.manifest.value).transfers[0]
    changed = replace(transfer,
        provisional_hold=replace(transfer.provisional_hold, transfer_token="not-the-committed-hold"),
        final_hold=replace(transfer.final_hold, transfer_token="not-the-committed-hold"))
    manifest = OutputPublicationManifest.create(fixture.header, (replace(fixture.manifest.value, transfers=(transfer, changed))))
    rebound = OutputOwnerPublicationPlan(fixture.execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (fixture.envelope.result)))
    with pytest.raises(OutputOwnerPublicationConflictError, match="rebound"):
        table.commit_output_publication(rebound)
    assert table.snapshot(first) == before
