"""Pure selected-output owner CAS and per-slot GC; no runtime or threads."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from enum import Enum

import pytest

from miniray import protocol
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedReferenceGraphAuthority,
)
from miniray.contained_edges import ContainedReferenceHold, LineageReferenceEdge
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.ownership import (
    ObjectCollectionInProgressError, ObjectCollectionState, ObjectOwnerTable,
    ObjectState, OutputOwnerPublicationCollectionPlan,
    OutputOwnerPublicationCollectionRequiredError, OutputOwnerPublicationConflictError,
    OutputOwnerPublicationDisposition, OutputOwnerPublicationMembership,
    OutputOwnerPublicationPlan,
)
from miniray.resources import ResourceVector
from miniray.stored_publication import OwnedContainedSource, PreparedContainedTransfer
from miniray.task_outputs import TargetExecutionKey, TargetOutputManifest, TaskExecutionKey


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, *, targeted=False, edges=True, all_stored=False):
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
            4 if targeted else 3, ResourceVector(), self.owner, max_retries=2,
        )
        self.full_execution = TaskExecutionKey.from_task_spec(self.spec)
        self.execution = (
            TargetExecutionKey(
                TargetOutputManifest(
                    self.full_execution.manifest,
                    (self.spec.return_ids()[1], self.spec.return_ids()[3]),
                ), self.attempt.next(),
            ) if targeted else self.full_execution
        )
        self.publication_id = OutputPublicationID(LeaseID.random(), self.execution)
        self.header = OutputPublicationHeader(
            self.publication_id, self.job, self.executor, self.owner,
            OutputPublicationNodeIncarnation(self.node, 301, 1),
        )
        self.child = ObjectID.for_task(TaskID.derive(self.job, self.task, 3))
        self.payloads = tuple(
            f"return-slot-{object_id.return_index}".encode()
            for object_id in self.publication_id.output_ids
        )
        slots = []
        results = []
        for index, (object_id, payload) in enumerate(zip(
            self.publication_id.output_ids, self.payloads
        )):
            tier = (protocol.ResultStorage.OBJECT_STORE
                    if all_stored or index == 1 else protocol.ResultStorage.INLINE)
            transfers = ()
            if edges and index < 2:
                transfers = (PreparedContainedTransfer(
                    self.child, self.executor, ("127.0.0.1", 31800),
                    OwnedContainedSource(self.executor),
                    ContainedReferenceHold(object_id, self.executor, "shared-token"),
                    ContainedReferenceHold(object_id, self.owner, "shared-token"),
                ),)
            checksum = hashlib.sha256(payload).hexdigest()
            slots.append(OutputSlotManifest(
                object_id, tier, len(payload), checksum, transfers
            ))
            results.append(protocol.ResultDescriptor(
                object_id, tier, len(payload), self.owner, self.node, checksum,
                payload if tier is protocol.ResultStorage.INLINE else None,
            ))
        self.manifest = OutputPublicationManifest.create(self.header, tuple(slots))
        self.envelope = OutputPublicationEnvelope(
            self.manifest, OutputPublicationCompleteWitness.for_manifest(self.manifest),
            tuple(results),
        )
        self.plan = OutputOwnerPublicationPlan(self.execution, self.envelope)

    def table(self):
        table = ObjectOwnerTable()
        table.register_task_outputs(
            self.spec, local_tokens=tuple(f"handle-{i}" for i in range(self.spec.num_returns))
        )
        if isinstance(self.execution, TargetExecutionKey):
            for object_id in self.publication_id.output_ids:
                assert table.advance_attempt(
                    object_id, expected_attempt=self.attempt,
                    next_attempt=self.execution.attempt_id,
                )
        return table

    def graph(self):
        graph = ContainedReferenceGraphAuthority()
        manifest = self.manifest.to_graph_manifest()
        if manifest is not None:
            graph.prepare_manifest(manifest)
            graph.commit_manifest(manifest)
        return graph

    def release_handle(self, table, object_id):
        assert table.release_local_reference(object_id, f"handle-{object_id.return_index}")


def _snapshots(table, fixture):
    return tuple(table.snapshot(object_id) for object_id in fixture.spec.return_ids())


def _changed_result_plan(fixture):
    payload = b"changed-inline-result"
    first = replace(
        fixture.envelope.results[0], size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(), inline_data=payload,
    )
    slots = (replace(
        fixture.manifest.slots[0], size_bytes=first.size_bytes, checksum=first.checksum
    ),) + fixture.manifest.slots[1:]
    manifest = OutputPublicationManifest.create(fixture.header, slots)
    envelope = OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
        (first,) + fixture.envelope.results[1:],
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


def test_mixed_batch_publishes_once_without_retaining_sibling_payloads():
    fixture = _Fixture()
    table = fixture.table()
    before = _snapshots(table, fixture)
    assert table.validate_output_publication(fixture.plan) is OutputOwnerPublicationDisposition.APPLIED
    assert _snapshots(table, fixture) == before
    receipt = table.commit_output_publication(fixture.plan)
    assert receipt.committed and receipt.plan == fixture.plan
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    memberships = []
    for index, descriptor in enumerate(fixture.envelope.results):
        snapshot = table.snapshot(descriptor.object_id)
        membership = snapshot.output_publication
        memberships.append(membership)
        assert membership == OutputOwnerPublicationMembership(fixture.manifest, index)
        assert table.output_owner_publication(descriptor.object_id) == membership
        assert table.output_owner_result(descriptor.object_id) == descriptor
        assert snapshot.outgoing_contained_edges == frozenset(fixture.manifest.slots[index].edges)
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
                     for object_id in fixture.publication_id.output_ids)
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


def test_public_snapshot_and_membership_queries_cannot_poison_owner_metadata():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    before = _snapshots(table, fixture)
    first, stored, _ = fixture.publication_id.output_ids
    snapshot = table.snapshot(first)
    object.__setattr__(snapshot.output_publication.manifest.header.owner_worker_id, "value", b"x" * 16)
    object.__setattr__(next(iter(snapshot.outgoing_contained_edges)).contained_object_id, "return_index", 9)
    object.__setattr__(snapshot.current_attempt, "attempt_number", 9)
    object.__setattr__(snapshot.object_id.task_id, "value", b"y" * 16)
    object.__setattr__(snapshot.producer_task_spec.args[0], "data", b"poisoned")
    membership = table.output_owner_publication(first)
    object.__setattr__(membership.manifest.slots[0], "checksum", "0" * 64)
    object.__setattr__(membership.manifest.header.publication_id.lease_id, "value", b"z" * 16)
    stored_snapshot = table.snapshot(stored)
    object.__setattr__(stored_snapshot.canonical_stored_result.node_id, "value", b"q" * 16)
    object.__setattr__(next(iter(stored_snapshot.locations)), "value", b"r" * 16)
    result = table.output_owner_result(stored)
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
    object.__setattr__(receipt.plan.envelope.results[1], "checksum", "e" * 64)
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
    slots = tuple(replace(slot, transfers=()) for slot in fixture.manifest.slots)
    manifest = OutputPublicationManifest.create(header, slots)
    results = tuple(replace(
        descriptor, owner_worker_id=header.owner_worker_id
    ) for descriptor in fixture.envelope.results)
    plan = OutputOwnerPublicationPlan(fixture.execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), results
    ))
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerPublicationConflictError, match="owner and job"):
        table.commit_output_publication(plan)
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_one_stale_selected_slot_fences_entire_batch():
    fixture = _Fixture()
    table = fixture.table()
    assert table.advance_attempt(
        fixture.publication_id.output_ids[-1], expected_attempt=fixture.attempt,
        next_attempt=fixture.attempt.next(),
    )
    before = _snapshots(table, fixture)
    receipt = table.commit_output_publication(fixture.plan)
    assert receipt.disposition is OutputOwnerPublicationDisposition.FENCED
    assert not receipt.committed
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_late_slot_conflict_cannot_publish_earlier_slots():
    fixture = _Fixture()
    table = fixture.table()
    last = fixture.publication_id.output_ids[-1]
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
    object.__setattr__(forged.envelope.results[0], "inline_data", b"wrong-bytes")
    before = _snapshots(table, fixture)
    with pytest.raises((TypeError, ValueError, ProtocolError)):
        table.commit_output_publication(forged)
    assert _snapshots(table, fixture) == before
    assert not table._output_publication_receipts


def test_tampered_manifest_and_membership_cannot_bypass_selected_slot_identity():
    fixture = _Fixture()
    table = fixture.table()
    forged = replace(fixture.plan)
    object.__setattr__(forged.envelope.manifest, "manifest_digest", "0" * 64)
    before = _snapshots(table, fixture)
    with pytest.raises(ValueError, match="manifest_digest"):
        table.commit_output_publication(forged)
    assert _snapshots(table, fixture) == before
    for invalid in (-1, True, len(fixture.manifest.slots)):
        with pytest.raises(ValueError, match="slot_index"):
            OutputOwnerPublicationMembership(fixture.manifest, invalid)


def test_targeted_batch_changes_only_selected_slots_with_original_indices():
    fixture = _Fixture(targeted=True)
    table = fixture.table()
    healthy = (fixture.spec.return_ids()[0], fixture.spec.return_ids()[2])
    for index, object_id in enumerate(healthy):
        assert table.publish_inline(object_id, fixture.attempt, f"healthy-{index}".encode())
    before = tuple(table.snapshot(object_id) for object_id in healthy)
    receipt = table.commit_output_publication(fixture.plan)
    assert receipt.committed
    assert tuple(table.snapshot(object_id) for object_id in healthy) == before
    assert tuple(table.snapshot(object_id).output_publication.slot_index
                 for object_id in fixture.publication_id.output_ids) == (0, 1)
    assert tuple(object_id.return_index for object_id in fixture.publication_id.output_ids) == (1, 3)
    for object_id in fixture.publication_id.output_ids:
        assert table.snapshot(object_id).current_attempt == fixture.execution.attempt_id
        assert table.snapshot(object_id).producer_task_spec == fixture.spec


def test_targeted_commit_does_not_block_on_healthy_sibling_collection():
    fixture = _Fixture(targeted=True)
    table = fixture.table()
    healthy = fixture.spec.return_ids()[0]
    table.publish_inline(healthy, fixture.attempt, b"healthy")
    fixture.release_handle(table, healthy)
    frozen = table.begin_collection(healthy, collection_id="healthy-gc")
    before = table.snapshot(healthy)
    assert frozen is not None
    assert table.commit_output_publication(fixture.plan).committed
    assert table.snapshot(healthy) == before


def test_targeted_slot_collection_keeps_healthy_siblings_and_task_lineage():
    fixture = _Fixture(targeted=True)
    table = fixture.table()
    healthy = (fixture.spec.return_ids()[0], fixture.spec.return_ids()[2])
    for object_id in healthy:
        table.publish_inline(object_id, fixture.attempt, b"healthy-value")
    dependency = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 64))
    table.add_outgoing_lineage_edge(
        healthy[0], LineageReferenceEdge(healthy[0], dependency, "shared-lineage")
    )
    before = tuple(table.snapshot(object_id) for object_id in healthy)
    graph = fixture.graph()
    table.commit_output_publication(fixture.plan)
    for object_id in fixture.publication_id.output_ids:
        fixture.release_handle(table, object_id)
        plan = table.begin_output_publication_collection(
            object_id, collection_id=f"target-gc-{object_id.return_index}"
        )
        ack = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), object_id)
        result = table.complete_output_publication_collection(plan, ack)
        assert not result.collection.lineage_releases
    assert fixture.task in table._task_lineage
    assert tuple(table.snapshot(object_id) for object_id in healthy) == before


def test_current_selected_slot_collection_prevents_batch_republication():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = fixture.publication_id.output_ids[0]
    fixture.release_handle(table, first)
    assert table.begin_output_publication_collection(first, collection_id="pending-gc") is not None
    before = _snapshots(table, fixture)
    with pytest.raises(ObjectCollectionInProgressError, match="selected output"):
        table.commit_output_publication(fixture.plan)
    assert _snapshots(table, fixture) == before


def test_new_targeted_publication_cannot_replace_unretired_membership():
    fixture = _Fixture(all_stored=True)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    target = fixture.publication_id.output_ids[1]
    assert table.mark_lost(target, fixture.attempt)
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError, match="retirement"):
        table.advance_attempt(
            target, expected_attempt=fixture.attempt, next_attempt=fixture.attempt.next()
        )
    execution = TargetExecutionKey(
        TargetOutputManifest(fixture.execution.manifest, (target,)), fixture.attempt
    )
    header = replace(fixture.header, publication_id=OutputPublicationID(LeaseID.random(), execution))
    manifest = OutputPublicationManifest.create(header, (fixture.manifest.slots[1],))
    plan = OutputOwnerPublicationPlan(execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
        (fixture.envelope.results[1],),
    ))
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError, match="unretired"):
        table.commit_output_publication(plan)
    assert _snapshots(table, fixture) == before


def test_lost_stored_slot_replay_never_restores_a_dead_replica():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    target = fixture.publication_id.output_ids[1]
    assert table.mark_lost(target, fixture.attempt)
    before = _snapshots(table, fixture)
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert _snapshots(table, fixture) == before
    assert table.output_owner_result(target) == fixture.envelope.results[1]
    assert not table.snapshot(target).locations


def test_no_reference_batch_uses_the_same_publication_and_per_slot_collection():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    assert fixture.manifest.to_graph_manifest() is None
    assert table.commit_output_publication(fixture.plan).committed
    for object_id in fixture.publication_id.output_ids:
        fixture.release_handle(table, object_id)
        plan = table.begin_output_publication_collection(
            object_id, collection_id=f"plain-gc-{object_id.return_index}"
        )
        receipt = table.complete_output_publication_collection(plan)
        assert receipt.collection.collected and not receipt.collection.contained_releases
    assert not table._entries
    _assert_metadata_only(table._output_publication_receipts)
    _assert_metadata_only(table._output_collection_receipts)


@pytest.mark.parametrize("index", [0, 1, 2])
def test_each_slot_collects_only_its_own_edges_and_never_keeps_sibling_bytes(index):
    fixture = _Fixture()
    table = fixture.table()
    graph = fixture.graph()
    table.commit_output_publication(fixture.plan)
    object_id = fixture.publication_id.output_ids[index]
    slot = fixture.manifest.slots[index]
    assert table.begin_output_publication_collection(object_id, collection_id="slot-gc") is None
    fixture.release_handle(table, object_id)
    assert not table.collect_if_unused(object_id)
    assert not table.collect_unused_with_edges(object_id).collected
    plan = table.begin_output_publication_collection(object_id, collection_id="slot-gc")
    assert plan is not None
    assert plan.metadata_plan.contained_releases == tuple(sorted(slot.edges))
    before = tuple(table.snapshot(other) for other in fixture.publication_id.output_ids if other != object_id)
    with pytest.raises(OutputOwnerPublicationCollectionRequiredError, match="per-slot"):
        table.complete_collection(plan.metadata_plan)
    graph_receipt = (graph.release_manifest_container(fixture.manifest.to_graph_manifest(), object_id)
                     if slot.edges else None)
    receipt = table.complete_output_publication_collection(plan, graph_receipt)
    assert receipt.collection.contained_releases == tuple(sorted(slot.edges))
    assert not receipt.collection.lineage_releases
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    assert table.collection_state(object_id) is ObjectCollectionState.COLLECTED
    assert table.output_owner_result(object_id) is None
    assert table.output_owner_publication(object_id) is None
    assert tuple(table.snapshot(other) for other in fixture.publication_id.output_ids if other != object_id) == before
    _assert_metadata_only(table._output_collection_receipts)
    _assert_metadata_only(table._output_publication_receipts)
    replay = table.complete_output_publication_collection(plan, graph_receipt)
    assert replay.disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert table.output_publication_collection_receipt(plan).collection == receipt.collection
    assert table.begin_output_publication_collection(object_id, collection_id="slot-gc") is None
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert tuple(table.snapshot(other) for other in fixture.publication_id.output_ids if other != object_id) == before


def test_collection_rejects_another_slots_graph_ack_and_altered_full_graph():
    fixture = _Fixture()
    table = fixture.table()
    graph = fixture.graph()
    table.commit_output_publication(fixture.plan)
    first, second, _ = fixture.publication_id.output_ids
    fixture.release_handle(table, first)
    plan = table.begin_output_publication_collection(first, collection_id="first-gc")
    other_ack = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), second)
    before = table.snapshot(first)
    with pytest.raises(OutputOwnerPublicationConflictError, match="exact batch"):
        table.complete_output_publication_collection(plan, other_ack)
    correct = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), first)
    wrong_graph = replace(correct, manifest=replace(
        correct.manifest, transaction_id="another-graph"
    ))
    with pytest.raises(OutputOwnerPublicationConflictError, match="exact batch"):
        table.complete_output_publication_collection(plan, wrong_graph)
    with pytest.raises(TypeError, match="GraphManifestReceipt"):
        table.complete_output_publication_collection(plan)
    assert table.snapshot(first) == before
    assert table.complete_output_publication_collection(plan, correct).collection.collected


def test_empty_edge_slot_needs_no_graph_ack_even_when_siblings_have_refs():
    fixture = _Fixture()
    table = fixture.table()
    graph = fixture.graph()
    table.commit_output_publication(fixture.plan)
    empty = fixture.publication_id.output_ids[2]
    fixture.release_handle(table, empty)
    plan = table.begin_output_publication_collection(empty, collection_id="empty-gc")
    sibling_ack = graph.release_manifest_container(
        fixture.manifest.to_graph_manifest(), fixture.publication_id.output_ids[0]
    )
    with pytest.raises(OutputOwnerPublicationConflictError, match="empty-edge"):
        table.complete_output_publication_collection(plan, sibling_ack)
    assert table.complete_output_publication_collection(plan).collection.collected


def test_shared_child_holds_and_task_lineage_survive_until_their_own_last_slot():
    fixture = _Fixture()
    table = fixture.table()
    graph = fixture.graph()
    first, second, last = fixture.publication_id.output_ids
    dependency = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 72))
    table.add_outgoing_lineage_edge(first, LineageReferenceEdge(first, dependency, "producer-lineage"))
    table.commit_output_publication(fixture.plan)
    plans = []
    for object_id in (first, second, last):
        fixture.release_handle(table, object_id)
        plans.append(table.begin_output_publication_collection(
            object_id, collection_id=f"gc-{object_id.return_index}"
        ))
    assert all(not plan.metadata_plan.lineage_releases for plan in plans)
    first_ack = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), first)
    first_done = table.complete_output_publication_collection(plans[0], first_ack)
    assert not first_done.collection.lineage_releases
    (graph_state,) = graph.snapshot().manifests
    assert graph_state.active_edges == fixture.manifest.slots[1].edges
    assert table.snapshot(second).outgoing_contained_edges == frozenset(fixture.manifest.slots[1].edges)
    second_ack = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), second)
    second_done = table.complete_output_publication_collection(plans[1], second_ack)
    assert not second_done.collection.lineage_releases
    final = table.complete_output_publication_collection(plans[2])
    assert final.collection.lineage_releases == (
        LineageReferenceEdge(last, dependency, "producer-lineage"),
    )
    assert not table._task_lineage
    assert not table._entries
    _assert_metadata_only(table._output_collection_receipts)
    _assert_metadata_only(table._output_publication_receipts)


def test_terminal_collection_replay_binds_metadata_hash_without_storing_taskspec():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    object_id = fixture.publication_id.output_ids[0]
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
    first = fixture.publication_id.output_ids[0]
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
    graph = fixture.graph()
    ack = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), first)
    assert table.complete_output_publication_collection(stable_plan, ack).collection.collected


@pytest.mark.parametrize("source", ["complete", "replay", "query"])
def test_public_collection_receipt_ids_cannot_poison_tombstones_or_siblings(source):
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    graph = fixture.graph()
    first, second, _ = fixture.publication_id.output_ids
    fixture.release_handle(table, first)
    plan = table.begin_output_publication_collection(first, collection_id="receipt-gc")
    ack = graph.release_manifest_container(fixture.manifest.to_graph_manifest(), first)
    receipt = table.complete_output_publication_collection(plan, ack)
    if source == "replay":
        receipt = table.complete_output_publication_collection(plan, ack)
    elif source == "query":
        receipt = table.output_publication_collection_receipt(plan)
    before = table.snapshot(second)
    terminal = deepcopy(table._output_collection_receipts[first])
    object.__setattr__(receipt.collection.contained_releases[0].contained_object_id, "return_index", 88)
    object.__setattr__(receipt.collection.object_id.task_id, "value", b"d" * 16)
    object.__setattr__(receipt.plan.metadata_plan.object_id.task_id, "value", b"e" * 16)
    object.__setattr__(receipt.plan.membership.manifest.header.publication_id.lease_id, "value", b"f" * 16)
    assert table.snapshot(second) == before
    assert table._output_collection_receipts[first] == terminal
    assert table.collection_state(first) is ObjectCollectionState.COLLECTED
    assert table.output_publication_collection_receipt(plan).collection == terminal.collection
    assert table.complete_output_publication_collection(plan, ack).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED


def test_legacy_objects_still_use_their_original_owner_and_gc_apis():
    fixture = _Fixture(edges=False)
    table = fixture.table()
    object_id = fixture.publication_id.output_ids[0]
    assert table.publish_inline(object_id, fixture.attempt, b"legacy")
    assert table.output_owner_publication(object_id) is None
    fixture.release_handle(table, object_id)
    metadata = table.begin_collection(object_id, collection_id="legacy-gc")
    assert table.complete_collection(metadata).collected


def test_published_slot_edges_cannot_be_extended_by_legacy_helpers():
    fixture = _Fixture()
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    first = fixture.publication_id.output_ids[0]
    edge = fixture.manifest.slots[0].edges[0]
    assert not table.add_outgoing_contained_edge(first, edge)
    assert table.add_outgoing_contained_edges(first, (edge,)) == 0
    before = table.snapshot(first)
    changed = replace(edge, transfer_token="not-the-committed-hold")
    with pytest.raises(OutputOwnerPublicationConflictError, match="cannot change edges"):
        table.add_outgoing_contained_edge(first, changed)
    with pytest.raises(OutputOwnerPublicationConflictError, match="cannot change edges"):
        table.add_outgoing_contained_edges(first, (edge, changed))
    assert table.snapshot(first) == before
