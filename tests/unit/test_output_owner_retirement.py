"""Pure selected LOST-slot retirement; no runtime, processes or threads."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold, LineageReferenceEdge
from miniray.ids import LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationID, OutputPublicationManifest,
)
from miniray.ownership import (
    InvalidObjectTransitionError, ObjectCollectionInProgressError,
    ObjectCollectionState, ObjectState, OutputOwnerPublicationDisposition,
    OutputOwnerPublicationPlan, OutputOwnerRetirementConflictError,
    OutputOwnerRetirementInProgressError, TargetOutputAttemptAdvancePlan,
    TaskOutputAttemptAdvancePlan,
)
from miniray.task_outputs import TargetExecutionKey, TargetOutputManifest

from test_output_owner_publication import _Fixture, _assert_metadata_only, _snapshots


pytestmark = pytest.mark.unit


def _begin(table, fixture, indices=(1,), *, identity="retire:outputs", extra_nodes=()):
    members = tuple(table.output_owner_publication(fixture.spec.return_ids()[index])
                    for index in indices)
    return table.begin_output_publication_retirement(
        members, retirement_id=identity,
        replica_locations={member.object_id: (member.manifest.header.node_incarnation.node_id,)
                           + extra_nodes for member in members},
    )


def _proofs(plan, graph):
    return dict(
        released_edges=tuple(protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, True,
        ) for request in plan.contained_releases),
        graph_receipts=tuple(graph.release_manifest_container(
            member.manifest.to_graph_manifest(), member.object_id,
        ) for member in plan.memberships if member.slot.edges),
        dropped_replicas=tuple(protocol.DropObjectReplicaReply(
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum, protocol.DropObjectReplicaStatus.DROPPED,
        ) for request in plan.replica_drops),
    )


def _target_publication(fixture, execution, source_indices):
    header = replace(fixture.header, publication_id=OutputPublicationID(LeaseID.random(), execution))
    manifest = OutputPublicationManifest.create(
        header, tuple(fixture.manifest.slots[index] for index in source_indices),
    )
    envelope = OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
        tuple(fixture.envelope.results[index] for index in source_indices),
    )
    return OutputOwnerPublicationPlan(execution, envelope)


def _lost_fixture(*, all_stored=False, edges=True, indices=(1,)):
    fixture = _Fixture(all_stored=all_stored, edges=edges)
    table = fixture.table()
    table.commit_output_publication(fixture.plan)
    for index in indices:
        assert table.mark_lost(fixture.spec.return_ids()[index], fixture.attempt)
    return fixture, table, fixture.graph()


def test_retirement_preserves_stable_id_incoming_holds_and_task_lineage():
    fixture, table, graph = _lost_fixture()
    target = fixture.spec.return_ids()[1]
    incoming = ContainedReferenceHold(
        ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 41)),
        WorkerID.random(), "incoming-live-hold",
    )
    assert table.add_contained_reference(target, incoming)
    assert table.add_lineage_reference(target, "incoming-lineage")
    lineage = LineageReferenceEdge(target, fixture.child, "producer-lineage")
    assert table.add_outgoing_lineage_edge(target, lineage)
    before = table.snapshot(target)
    siblings = {oid: table.snapshot(oid) for oid in fixture.spec.return_ids() if oid != target}
    plan = _begin(table, fixture)
    assert table.has_active_output_retirements()
    assert table.collection_state(target) is ObjectCollectionState.ACTIVE
    assert table.snapshot(target).output_retirement_id == plan.retirement_id
    assert _begin(table, fixture) == plan
    assert table.add_local_reference(target, "new-live-handle")
    assert table.release_local_reference(target, "new-live-handle")
    proofs = _proofs(plan, graph)
    receipt = table.complete_output_publication_retirement(plan, **proofs)
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    assert not table.has_active_output_retirements()
    after = table.snapshot(target)
    assert after == replace(
        before, canonical_stored_result=None, output_publication=None,
        outgoing_contained_edges=frozenset(),
    )
    assert after.is_live and after.is_reconstructible and after.state is ObjectState.LOST
    assert table.output_owner_result(target) is None
    assert {oid: table.snapshot(oid) for oid in siblings} == siblings
    assert table.task_lineage_edges(fixture.task)
    _assert_metadata_only(plan)
    _assert_metadata_only(receipt)
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is (
        OutputOwnerPublicationDisposition.ALREADY_APPLIED
    )


def test_retirement_gates_publish_advance_and_gc_but_not_reference_release():
    fixture, table, _ = _lost_fixture()
    target = fixture.spec.return_ids()[1]
    _begin(table, fixture)
    execution = TargetExecutionKey(
        TargetOutputManifest(fixture.full_execution.manifest, (target,)), fixture.attempt.next(),
    )
    target_plan = TargetOutputAttemptAdvancePlan(execution, ((target, fixture.attempt),))
    batch_plan = TaskOutputAttemptAdvancePlan(
        fixture.full_execution, fixture.full_execution.for_attempt(fixture.attempt.next()),
    )
    operations = (
        lambda: table.publish_stored(target, fixture.attempt, fixture.node),
        lambda: table.publish_inline(target, fixture.attempt, b"late"),
        lambda: table.publish_error(target, fixture.attempt, RuntimeError("late")),
        lambda: table.advance_attempt(target, expected_attempt=fixture.attempt, next_attempt=fixture.attempt.next()),
        lambda: table.validate_advance_target_outputs(execution, {target: fixture.attempt}),
        lambda: table.commit_validated_advance_target_outputs(target_plan),
        lambda: table.advance_task_outputs(fixture.full_execution, fixture.attempt.next()),
        lambda: table.commit_validated_advance_task_outputs(batch_plan),
        lambda: table.begin_collection(target),
        lambda: table.commit_output_publication(fixture.plan),
    )
    before = _snapshots(table, fixture)
    for operation in operations:
        with pytest.raises(OutputOwnerRetirementInProgressError):
            operation()
        assert _snapshots(table, fixture) == before
    assert table.release_local_reference(target, "handle-1")


def test_retirement_rejects_ready_slot_or_missing_replica_inventory_atomically():
    fixture, table, _ = _lost_fixture(all_stored=True, indices=(0,))
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerRetirementConflictError, match="LOST"):
        _begin(table, fixture, (0, 1))
    assert _snapshots(table, fixture) == before
    assert not table.has_active_output_retirements()
    member = table.output_owner_publication(fixture.spec.return_ids()[0])
    with pytest.raises(OutputOwnerRetirementConflictError, match="publishing Node"):
        table.begin_output_publication_retirement(
            (member,), retirement_id="missing-node", replica_locations={member.object_id: ()},
        )
    assert _snapshots(table, fixture) == before
    with pytest.raises(OutputOwnerRetirementConflictError, match="exactly"):
        table.begin_output_publication_retirement((member,), retirement_id="missing-slot", replica_locations={})


def test_retirement_rejects_overlapping_claim_and_rebound_identity():
    fixture, table, _ = _lost_fixture()
    plan = _begin(table, fixture)
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerRetirementInProgressError):
        _begin(table, fixture, identity="overlap")
    with pytest.raises(OutputOwnerRetirementConflictError, match="retirement_id"):
        _begin(table, fixture, extra_nodes=(NodeID.random(),))
    assert _snapshots(table, fixture) == before
    assert table.output_publication_retirement_receipt(plan) is None


@pytest.mark.parametrize("field", ["released_edges", "graph_receipts", "dropped_replicas"])
def test_completion_requires_every_cleanup_proof_and_never_partially_clears(field):
    fixture, table, graph = _lost_fixture(all_stored=True, indices=(0, 1))
    plan = _begin(table, fixture, (0, 1))
    proofs = _proofs(plan, graph)
    before = _snapshots(table, fixture)
    changed = dict(proofs)
    changed[field] = proofs[field][:-1]
    with pytest.raises(OutputOwnerRetirementConflictError, match="every"):
        table.complete_output_publication_retirement(plan, **changed)
    assert _snapshots(table, fixture) == before
    assert table.has_active_output_retirements()
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is (
        OutputOwnerPublicationDisposition.APPLIED
    )


@pytest.mark.parametrize("status", [
    protocol.DropObjectReplicaStatus.PINNED, protocol.DropObjectReplicaStatus.STALE_EPOCH,
    protocol.DropObjectReplicaStatus.NODE_DRAINING, protocol.DropObjectReplicaStatus.INCONSISTENT,
])
def test_replica_nonterminal_status_is_not_cleanup_proof(status):
    fixture, table, graph = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(plan, graph)
    proofs["dropped_replicas"] = (replace(proofs["dropped_replicas"][0], status=status, error="not cleaned"),)
    before = _snapshots(table, fixture)
    with pytest.raises(OutputOwnerRetirementConflictError, match="replica"):
        table.complete_output_publication_retirement(plan, **proofs)
    assert _snapshots(table, fixture) == before


@pytest.mark.parametrize("kind", ["child-owner", "child-hold", "child-rejected", "graph-sibling", "replica-epoch", "replica-node", "bool"])
def test_cleanup_proofs_cannot_substitute_another_hold_slot_or_epoch(kind):
    fixture, table, graph = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(plan, graph)
    if kind.startswith("child"):
        reply = proofs["released_edges"][0]
        if kind == "child-owner":
            reply = replace(reply, owner_worker_id=WorkerID.random())
        elif kind == "child-hold":
            reply = replace(reply, hold=replace(reply.hold, transfer_token="wrong-hold"))
        else:
            reply = replace(reply, accepted=False, released=False, error="not released")
        proofs["released_edges"] = (reply,)
    elif kind == "graph-sibling":
        proofs["graph_receipts"] = (graph.release_manifest_container(
            fixture.manifest.to_graph_manifest(), fixture.spec.return_ids()[0],
        ),)
    elif kind == "bool":
        proofs["dropped_replicas"] = (True,)
    else:
        reply = proofs["dropped_replicas"][0]
        reply = (replace(reply, producer_attempt_id=fixture.attempt.next())
                 if kind == "replica-epoch" else replace(reply, node_id=NodeID.random()))
        proofs["dropped_replicas"] = (reply,)
    before = _snapshots(table, fixture)
    with pytest.raises((OutputOwnerRetirementConflictError, TypeError)):
        table.complete_output_publication_retirement(plan, **proofs)
    assert _snapshots(table, fixture) == before


def test_dead_publishing_node_requires_its_exact_frozen_incarnation():
    fixture, table, graph = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(plan, graph)
    node = fixture.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "retire-node-death", node.node_id, node.node_pid, node.registration_epoch,
        1, -9, protocol.NodeDeathReason.PROCESS_EXIT, "Node exited",
    )
    for changed in (replace(death, node_pid=node.node_pid + 1),
                    replace(death, registration_epoch=node.registration_epoch + 1)):
        with pytest.raises(OutputOwnerRetirementConflictError, match="incarnation"):
            table.complete_output_publication_retirement(plan, **dict(proofs, dropped_replicas=(changed,)))
    assert table.complete_output_publication_retirement(
        plan, **dict(proofs, dropped_replicas=(death,)),
    ).disposition is OutputOwnerPublicationDisposition.APPLIED


def test_additional_replica_obligations_require_every_exact_ack():
    fixture, table, graph = _lost_fixture()
    replica = NodeID.random()
    plan = _begin(table, fixture, extra_nodes=(replica,))
    assert {drop.node_id for drop in plan.replica_drops} == {fixture.node, replica}
    proofs = _proofs(plan, graph)
    with pytest.raises(OutputOwnerRetirementConflictError, match="every replica"):
        table.complete_output_publication_retirement(
            plan, **dict(proofs, dropped_replicas=proofs["dropped_replicas"][:1]),
        )
    table.complete_output_publication_retirement(plan, **proofs)


def test_retired_slot_can_reconstruct_with_original_index_and_old_replays_are_fenced():
    fixture, table, graph = _lost_fixture()
    target = fixture.spec.return_ids()[1]
    plan = _begin(table, fixture)
    proofs = _proofs(plan, graph)
    table.complete_output_publication_retirement(plan, **proofs)
    retired = _snapshots(table, fixture)
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    with pytest.raises(InvalidObjectTransitionError, match="retired"):
        table.publish_stored(target, fixture.attempt, fixture.node)
    assert _snapshots(table, fixture) == retired
    execution = TargetExecutionKey(
        TargetOutputManifest(fixture.full_execution.manifest, (target,)), fixture.attempt.next(),
    )
    advance = table.validate_advance_target_outputs(execution, {target: fixture.attempt})
    assert table.commit_advance_target_outputs(advance)
    replacement = _target_publication(fixture, execution, (1,))
    assert table.commit_output_publication(replacement).disposition is OutputOwnerPublicationDisposition.APPLIED
    assert table.snapshot(target).output_publication.slot.object_id.return_index == 1
    ready = _snapshots(table, fixture)
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is (
        OutputOwnerPublicationDisposition.ALREADY_APPLIED
    )
    assert _snapshots(table, fixture) == ready
    changed = dict(proofs, dropped_replicas=(replace(
        proofs["dropped_replicas"][0], status=protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
    ),))
    with pytest.raises(OutputOwnerRetirementConflictError, match="proofs"):
        table.complete_output_publication_retirement(plan, **changed)
    assert _snapshots(table, fixture) == ready


def test_one_retirement_vector_can_span_old_publications_and_attempts():
    fixture, table, graph = _lost_fixture(all_stored=True, indices=(0,))
    first = _begin(table, fixture, (0,), identity="first-retirement")
    table.complete_output_publication_retirement(first, **_proofs(first, graph))
    first_id, second_id, healthy_id = fixture.spec.return_ids()
    execution = TargetExecutionKey(
        TargetOutputManifest(fixture.full_execution.manifest, (first_id,)), fixture.attempt.next(),
    )
    table.commit_advance_target_outputs(table.validate_advance_target_outputs(
        execution, {first_id: fixture.attempt},
    ))
    replacement = _target_publication(fixture, execution, (0,))
    table.commit_output_publication(replacement)
    replacement_graph = replacement.envelope.manifest.to_graph_manifest()
    graph.prepare_manifest(replacement_graph)
    graph.commit_manifest(replacement_graph)
    table.mark_lost(first_id, fixture.attempt.next())
    table.mark_lost(second_id, fixture.attempt)
    healthy = table.snapshot(healthy_id)
    plan = _begin(table, fixture, (0, 1), identity="mixed-old-publications")
    assert tuple(member.publication_id.attempt_id for member in plan.memberships) == (
        fixture.attempt.next(), fixture.attempt,
    )
    assert len({member.publication_id for member in plan.memberships}) == 2
    table.complete_output_publication_retirement(plan, **_proofs(plan, graph))
    assert table.snapshot(healthy_id) == healthy
    assert table.snapshot(first_id).state is table.snapshot(second_id).state is ObjectState.LOST


def test_empty_edge_slot_retires_without_graph_or_child_operations():
    fixture, table, graph = _lost_fixture(edges=False)
    plan = _begin(table, fixture)
    proofs = _proofs(plan, graph)
    assert proofs["released_edges"] == proofs["graph_receipts"] == ()
    table.complete_output_publication_retirement(plan, **proofs)


def test_post_retirement_gc_collects_metadata_only_and_preserves_sibling_lineage():
    fixture, table, graph = _lost_fixture()
    target = fixture.spec.return_ids()[1]
    assert table.add_outgoing_lineage_edge(
        target, LineageReferenceEdge(target, fixture.child, "keep-until-last-sibling"),
    )
    plan = _begin(table, fixture)
    table.complete_output_publication_retirement(plan, **_proofs(plan, graph))
    assert table.release_local_reference(target, "handle-1")
    metadata = table.begin_collection(target, collection_id="retired-slot-metadata")
    assert metadata is not None
    assert metadata.locations == metadata.contained_releases == metadata.lineage_releases == ()
    assert metadata.canonical_size_bytes is metadata.canonical_checksum is None
    result = table.complete_collection(metadata)
    assert result.collected and result.lineage_releases == ()
    assert table.collection_state(target) is ObjectCollectionState.COLLECTED
    assert table.task_lineage_edges(fixture.task)
    assert table.output_publication_retirement_receipt(plan).disposition is (
        OutputOwnerPublicationDisposition.ALREADY_APPLIED
    )


def test_normal_collection_and_reconstruction_retirement_cannot_overlap():
    fixture, table, _ = _lost_fixture()
    target = fixture.spec.return_ids()[1]
    fixture.release_handle(table, target)
    assert table.begin_output_publication_collection(target, collection_id="normal-gc") is not None
    before = _snapshots(table, fixture)
    with pytest.raises(ObjectCollectionInProgressError):
        _begin(table, fixture)
    assert _snapshots(table, fixture) == before


def test_public_plans_proofs_and_terminal_getters_cannot_mutate_owner_history():
    fixture, table, graph = _lost_fixture()
    plan = _begin(table, fixture)
    saved_plan = deepcopy(plan)
    object.__setattr__(plan.memberships[0].manifest.header.owner_worker_id, "value", b"x" * 16)
    plan = _begin(table, fixture)
    assert plan == saved_plan
    proofs = _proofs(plan, graph)
    saved_proofs = deepcopy(proofs)
    receipt = table.complete_output_publication_retirement(plan, **proofs)
    object.__setattr__(receipt.plan.memberships[0].manifest, "manifest_digest", "f" * 64)
    object.__setattr__(proofs["dropped_replicas"][0].node_id, "value", b"y" * 16)
    readback = table.output_publication_retirement_receipt(saved_plan)
    assert readback.plan == saved_plan
    object.__setattr__(readback.plan.replica_drops[0].object_id, "return_index", 99)
    assert table.output_publication_retirement_receipt(saved_plan).plan == saved_plan
    assert table.complete_output_publication_retirement(saved_plan, **saved_proofs).disposition is (
        OutputOwnerPublicationDisposition.ALREADY_APPLIED
    )
    for value in (table._output_retirement_plans, table._output_retirement_receipts,
                  table._retired_output_slots, table._retired_output_attempts,
                  table._output_publication_receipts):
        _assert_metadata_only(value)
