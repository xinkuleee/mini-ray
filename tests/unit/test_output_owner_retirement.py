"""Current single-output LOST retirement and exact cleanup-proof contracts.

One output, at most two children and two replica obligations per case. Child
release ACKs come from a real local owner table; Drop/Node-death values are
explicit boundary facts, not claims of physical deletion or detected exits.
No runtime constructor, transport, process, thread, wait or user code runs.
"""

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
    ObjectCollectionState, ObjectOwnerTable, ObjectState,
    OutputOwnerPublicationDisposition, OutputOwnerPublicationPlan,
    OutputOwnerRetirementConflictError, OutputOwnerRetirementInProgressError,
    TaskOutputAttemptAdvancePlan,
)
from tests.unit.test_output_owner_publication import _Fixture as _OwnerValues, _assert_metadata_only


pytestmark = pytest.mark.unit


class _Fixture(_OwnerValues):
    def __init__(self, *, edges=True, stored=True):
        super().__init__(edges=edges, all_stored=stored)
        self.output = (self.publication_id.object_id)
        if edges:
            first = (self.manifest.value).transfers[0]
            second = replace(first, contained_object_id=ObjectID.for_task(TaskID.derive(self.job, self.task, 4)),
                provisional_hold=replace(first.provisional_hold, transfer_token="second-child"),
                final_hold=replace(first.final_hold, transfer_token="second-child"))
            self.value = replace(self.manifest.value, transfers=(first, second))
            self.manifest = OutputPublicationManifest.create(
                self.header, (self.value),
            )
            self.envelope = OutputPublicationEnvelope(
                self.manifest, OutputPublicationCompleteWitness.for_manifest(self.manifest), (self.envelope.result),
            )
            self.plan = OutputOwnerPublicationPlan(self.execution, self.envelope)
        self.child_owner = ObjectOwnerTable()
        for transfer in (self.manifest.value).transfers:
            self.child_owner.register(transfer.contained_object_id, local_token="child-source")
            self.child_owner.prepare_stored_contained_reference(transfer, authority_worker_id=self.executor)
            self.child_owner.promote_stored_contained_reference(transfer, authority_worker_id=self.executor)


def _state(table):
    return deepcopy({name: value for name, value in vars(table).items() if name != "_lock"})


def _begin(table, fixture, *, identity="retire:output", extra_nodes=()):
    member = table.output_owner_publication(fixture.output)
    locations = ((fixture.node,) + extra_nodes if (member.manifest.value).tier is protocol.ResultStorage.OBJECT_STORE else ())
    return table.begin_output_publication_retirement(
        (member,), retirement_id=identity, replica_locations={member.object_id: locations},
    )


def _proofs(fixture, plan):
    releases = tuple(protocol.ReleaseContainedReferenceReply(
        request.object_id, request.owner_worker_id, request.hold, True,
        fixture.child_owner.release_contained_reference(request.object_id, request.hold),
    ) for request in plan.contained_releases)
    return dict(released_edges=releases, dropped_replicas=tuple(protocol.DropObjectReplicaReply(
        request.object_id, request.producer_attempt_id, request.owner_worker_id,
        request.node_id, request.checksum, protocol.DropObjectReplicaStatus.DROPPED,
    ) for request in plan.replica_drops))


def _lost_fixture(*, edges=True, stored=True):
    fixture = _Fixture(edges=edges, stored=stored)
    table = fixture.table()
    assert table.commit_output_publication(fixture.plan).committed
    assert table.mark_lost(fixture.output, fixture.attempt)
    return fixture, table


def _replacement_publication(fixture, attempt):
    execution = fixture.execution.for_attempt(attempt)
    header = replace(fixture.header, publication_id=OutputPublicationID(LeaseID.random(), execution))
    transfers = tuple(replace(transfer,
        provisional_hold=replace(transfer.provisional_hold, transfer_token="new:" + transfer.provisional_hold.transfer_token),
        final_hold=replace(transfer.final_hold, transfer_token="new:" + transfer.final_hold.transfer_token),
    ) for transfer in (fixture.manifest.value).transfers)
    manifest = OutputPublicationManifest.create(header, (replace(fixture.manifest.value, transfers=transfers)))
    for transfer in transfers:
        fixture.child_owner.prepare_stored_contained_reference(transfer, authority_worker_id=fixture.executor)
        fixture.child_owner.promote_stored_contained_reference(transfer, authority_worker_id=fixture.executor)
    return OutputOwnerPublicationPlan(execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (fixture.envelope.result),
    ))


def test_retirement_preserves_stable_id_incoming_holds_and_task_lineage():
    fixture, table = _lost_fixture()
    target = fixture.output
    incoming = ContainedReferenceHold(ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 41)),
                                      WorkerID.random(), "incoming-live-hold")
    assert table.add_contained_reference(target, incoming)
    assert table.add_lineage_reference(target, "incoming-lineage")
    lineage = LineageReferenceEdge(target, fixture.child, "producer-lineage")
    assert table.add_outgoing_lineage_edge(target, lineage)
    unrelated = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 42))
    table.register(unrelated, local_token="unrelated-live")
    assert table.publish_inline(unrelated, None, b"unrelated value")
    untouched = table.snapshot(unrelated)
    before = table.snapshot(target)
    plan = _begin(table, fixture)
    assert table.has_active_output_retirements() and table.collection_state(target) is ObjectCollectionState.ACTIVE
    assert table.snapshot(target).output_retirement_id == plan.retirement_id
    assert _begin(table, fixture) == plan
    assert table.add_local_reference(target, "new-live-handle")
    assert table.release_local_reference(target, "new-live-handle")
    proofs = _proofs(fixture, plan)
    receipt = table.complete_output_publication_retirement(plan, **proofs)
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    assert not table.has_active_output_retirements()
    after = table.snapshot(target)
    assert after == replace(before, canonical_stored_result=None, output_publication=None,
                            outgoing_contained_edges=frozenset())
    assert after.is_live and after.is_reconstructible and after.state is ObjectState.LOST
    assert table.output_owner_result(target) is None and table.snapshot(unrelated) == untouched
    assert table.task_lineage_edges(fixture.task)
    _assert_metadata_only(plan)
    _assert_metadata_only(receipt)
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED


def test_retirement_gates_publish_advance_and_gc_but_not_reference_release():
    fixture, table = _lost_fixture()
    target = fixture.output
    _begin(table, fixture)
    advance = TaskOutputAttemptAdvancePlan(fixture.execution, fixture.execution.for_attempt(fixture.attempt.next()))
    operations = (
        lambda: table.publish_stored(target, fixture.attempt, fixture.node),
        lambda: table.publish_inline(target, fixture.attempt, b"late"),
        lambda: table.publish_error(target, fixture.attempt, RuntimeError("late")),
        lambda: table.advance_attempt(target, expected_attempt=fixture.attempt, next_attempt=fixture.attempt.next()),
        lambda: table.validate_advance_task_outputs(fixture.execution, fixture.attempt.next()),
        lambda: table.commit_advance_task_outputs(advance),
        lambda: table.commit_validated_advance_task_outputs(advance),
        lambda: table.begin_collection(target),
        lambda: table.commit_output_publication(fixture.plan),
    )
    before = _state(table)
    for operation in operations:
        with table._lock:
            with pytest.raises(OutputOwnerRetirementInProgressError):
                operation()
        assert _state(table) == before
    assert table.release_local_reference(target, "handle-0")


def test_retirement_rejects_ready_slot_or_missing_replica_inventory_atomically():
    fixture = _Fixture()
    table = fixture.table()
    assert table.commit_output_publication(fixture.plan).committed
    before = _state(table)
    with pytest.raises(OutputOwnerRetirementConflictError, match="LOST"):
        _begin(table, fixture)
    assert _state(table) == before and not table.has_active_output_retirements()
    assert table.mark_lost(fixture.output, fixture.attempt)
    member = table.output_owner_publication(fixture.output)
    before = _state(table)
    for inventory, message in (({fixture.output: ()}, "publishing Node"), ({}, "exactly")):
        with pytest.raises(OutputOwnerRetirementConflictError, match=message):
            table.begin_output_publication_retirement((member,), retirement_id="missing-inventory", replica_locations=inventory)
        assert _state(table) == before


def test_retirement_rejects_overlapping_claim_and_rebound_identity():
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture)
    before = _state(table)
    with pytest.raises(OutputOwnerRetirementInProgressError):
        _begin(table, fixture, identity="overlap")
    with pytest.raises(OutputOwnerRetirementConflictError, match="retirement_id"):
        _begin(table, fixture, extra_nodes=(NodeID.random(),))
    assert _state(table) == before and table.output_publication_retirement_receipt(plan) is None


@pytest.mark.parametrize("field", ["released_edges", "dropped_replicas"])
def test_completion_requires_every_cleanup_proof_and_never_partially_clears(field):
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture, extra_nodes=(NodeID.random(),))
    proofs = _proofs(fixture, plan)
    assert len(proofs["released_edges"]) == len(proofs["dropped_replicas"]) == 2
    before = _state(table)
    changed = dict(proofs)
    changed[field] = proofs[field][:-1]
    with pytest.raises(OutputOwnerRetirementConflictError, match="every"):
        table.complete_output_publication_retirement(plan, **changed)
    assert _state(table) == before and table.has_active_output_retirements()
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is OutputOwnerPublicationDisposition.APPLIED


@pytest.mark.parametrize("status", [protocol.DropObjectReplicaStatus.PINNED,
    protocol.DropObjectReplicaStatus.STALE_EPOCH, protocol.DropObjectReplicaStatus.NODE_DRAINING,
    protocol.DropObjectReplicaStatus.INCONSISTENT])
def test_replica_nonterminal_status_is_not_cleanup_proof(status):
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(fixture, plan)
    proofs["dropped_replicas"] = (replace(proofs["dropped_replicas"][0], status=status, error="not cleaned"),)
    before = _state(table)
    with pytest.raises(OutputOwnerRetirementConflictError, match="replica"):
        table.complete_output_publication_retirement(plan, **proofs)
    assert _state(table) == before


@pytest.mark.parametrize("kind", ["child-owner", "child-hold", "child-rejected", "replica-epoch", "replica-node", "replica-checksum", "bool"])
def test_cleanup_proofs_cannot_substitute_another_hold_slot_or_epoch(kind):
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(fixture, plan)
    if kind.startswith("child"):
        reply = proofs["released_edges"][0]
        if kind == "child-owner":
            reply = replace(reply, owner_worker_id=WorkerID.random())
        elif kind == "child-hold":
            reply = replace(reply, hold=replace(reply.hold, transfer_token="wrong-hold"))
        else:
            reply = replace(reply, accepted=False, released=False, error="not released")
        proofs["released_edges"] = (reply,) + proofs["released_edges"][1:]
    elif kind == "bool":
        proofs["dropped_replicas"] = (True,)
    else:
        reply = proofs["dropped_replicas"][0]
        if kind == "replica-epoch":
            reply = replace(reply, producer_attempt_id=fixture.attempt.next())
        elif kind == "replica-node":
            reply = replace(reply, node_id=NodeID.random())
        else:
            reply = replace(reply, checksum="f" * 64)
        proofs["dropped_replicas"] = (reply,)
    before = _state(table)
    with pytest.raises((OutputOwnerRetirementConflictError, TypeError)):
        table.complete_output_publication_retirement(plan, **proofs)
    assert _state(table) == before


def test_dead_publishing_node_requires_its_exact_frozen_incarnation():
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(fixture, plan)
    node = fixture.header.node_incarnation
    death = protocol.NodeDeathRecord("retire-node-death", node.node_id, node.node_pid, node.registration_epoch,
                                    1, -9, protocol.NodeDeathReason.PROCESS_EXIT, "Node exited")
    before = _state(table)
    for changed in (replace(death, node_pid=node.node_pid + 1), replace(death, registration_epoch=node.registration_epoch + 1)):
        with pytest.raises(OutputOwnerRetirementConflictError, match="incarnation"):
            table.complete_output_publication_retirement(plan, **dict(proofs, dropped_replicas=(changed,)))
        assert _state(table) == before
    assert table.complete_output_publication_retirement(plan, **dict(proofs, dropped_replicas=(death,))).disposition is OutputOwnerPublicationDisposition.APPLIED


def test_additional_replica_obligations_require_every_exact_ack():
    fixture, table = _lost_fixture()
    replica = NodeID.random()
    plan = _begin(table, fixture, extra_nodes=(replica,))
    assert {drop.node_id for drop in plan.replica_drops} == {fixture.node, replica}
    proofs = _proofs(fixture, plan)
    before = _state(table)
    with pytest.raises(OutputOwnerRetirementConflictError, match="every replica"):
        table.complete_output_publication_retirement(plan, **dict(proofs, dropped_replicas=proofs["dropped_replicas"][:1]))
    assert _state(table) == before
    table.complete_output_publication_retirement(plan, **proofs)


def test_retired_slot_can_reconstruct_with_original_index_and_old_replays_are_fenced():
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture)
    proofs = _proofs(fixture, plan)
    table.complete_output_publication_retirement(plan, **proofs)
    retired = _state(table)
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    with pytest.raises(InvalidObjectTransitionError, match="retired"):
        table.publish_stored(fixture.output, fixture.attempt, fixture.node)
    assert _state(table) == retired
    with table._lock:
        advance = table.validate_advance_task_outputs(fixture.execution, fixture.attempt.next())
        table.commit_validated_advance_task_outputs(advance)
    replacement = _replacement_publication(fixture, fixture.attempt.next())
    assert table.commit_output_publication(replacement).disposition is OutputOwnerPublicationDisposition.APPLIED
    assert (table.snapshot(fixture.output).output_publication).object_id.return_index == 0
    ready = _state(table)
    children = _state(fixture.child_owner)
    assert table.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    changed = dict(proofs, dropped_replicas=(replace(proofs["dropped_replicas"][0], status=protocol.DropObjectReplicaStatus.ALREADY_DROPPED),))
    with pytest.raises(OutputOwnerRetirementConflictError, match="proofs"):
        table.complete_output_publication_retirement(plan, **changed)
    assert _state(table) == ready and _state(fixture.child_owner) == children


def test_empty_edge_slot_retires_without_graph_or_child_operations():
    fixture, table = _lost_fixture(edges=False)
    plan = _begin(table, fixture)
    proofs = _proofs(fixture, plan)
    assert proofs["released_edges"] == () and not fixture.child_owner._entries
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is OutputOwnerPublicationDisposition.APPLIED


def test_post_retirement_gc_collects_metadata_only_and_releases_final_task_lineage():
    fixture, table = _lost_fixture()
    lineage = LineageReferenceEdge(fixture.output, fixture.child, "keep-until-output-gc")
    assert table.add_outgoing_lineage_edge(fixture.output, lineage)
    unrelated = ObjectID.for_task(TaskID.derive(fixture.job, fixture.task, 44))
    table.register(unrelated, local_token="unrelated-live")
    other_lineage = LineageReferenceEdge(unrelated, fixture.child, "unrelated-lineage")
    assert table.add_outgoing_lineage_edge(unrelated, other_lineage)
    untouched = table.snapshot(unrelated)
    plan = _begin(table, fixture)
    table.complete_output_publication_retirement(plan, **_proofs(fixture, plan))
    assert table.task_lineage_edges(fixture.task)
    fixture.release_handle(table, fixture.output)
    metadata = table.begin_collection(fixture.output, collection_id="retired-output-metadata")
    assert metadata is not None and metadata.locations == metadata.contained_releases == ()
    assert metadata.lineage_releases == (lineage,)
    assert metadata.canonical_size_bytes is metadata.canonical_checksum is None
    result = table.complete_collection(metadata)
    assert result.collected and result.lineage_releases == (lineage,)
    assert table.collection_state(fixture.output) is ObjectCollectionState.COLLECTED
    assert not table.task_lineage_edges(fixture.task)
    assert table.snapshot(unrelated) == untouched and table.task_lineage_edges(unrelated.task_id)
    assert table.output_publication_retirement_receipt(plan).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED


def test_normal_collection_and_reconstruction_retirement_cannot_overlap():
    fixture, table = _lost_fixture()
    fixture.release_handle(table, fixture.output)
    assert table.begin_output_publication_collection(fixture.output, collection_id="normal-gc") is not None
    before = _state(table)
    with pytest.raises(ObjectCollectionInProgressError):
        _begin(table, fixture)
    assert _state(table) == before


def test_public_plans_proofs_and_terminal_getters_cannot_mutate_owner_history():
    fixture, table = _lost_fixture()
    plan = _begin(table, fixture)
    saved_plan = deepcopy(plan)
    object.__setattr__(plan.memberships[0].manifest.header.owner_worker_id, "value", b"x" * 16)
    plan = _begin(table, fixture)
    assert plan == saved_plan
    proofs = _proofs(fixture, plan)
    saved_proofs = deepcopy(proofs)
    receipt = table.complete_output_publication_retirement(plan, **proofs)
    object.__setattr__(receipt.plan.memberships[0].manifest, "manifest_digest", "f" * 64)
    object.__setattr__(proofs["dropped_replicas"][0].node_id, "value", b"y" * 16)
    readback = table.output_publication_retirement_receipt(saved_plan)
    assert readback.plan == saved_plan
    object.__setattr__(readback.plan.replica_drops[0].object_id, "return_index", 99)
    assert table.output_publication_retirement_receipt(saved_plan).plan == saved_plan
    assert table.complete_output_publication_retirement(saved_plan, **saved_proofs).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    for value in (table._output_retirement_plans, table._output_retirement_receipts, table._retired_output_slots,
                  table._retired_output_attempts, table._output_publication_receipts):
        _assert_metadata_only(value)


@pytest.mark.parametrize("field", ["size_bytes", "checksum"])
def test_canonical_result_is_revalidated_before_retirement_claim_and_completion(field):
    fixture, table = _lost_fixture()
    entry = table._entries[fixture.output]
    original = entry.canonical_stored_result
    changed = replace(original, **{field: original.size_bytes + 1 if field == "size_bytes" else "f" * 64})
    entry.canonical_stored_result = changed
    before = _state(table)
    with pytest.raises(OutputOwnerRetirementConflictError, match="result identity"):
        _begin(table, fixture)
    assert _state(table) == before and not table.has_active_output_retirements()
    entry.canonical_stored_result = original
    plan = _begin(table, fixture)
    proofs = _proofs(fixture, plan)
    entry.canonical_stored_result = changed
    before = _state(table)
    with pytest.raises(OutputOwnerRetirementConflictError, match="result identity"):
        table.complete_output_publication_retirement(plan, **proofs)
    assert _state(table) == before and table.has_active_output_retirements()
    entry.canonical_stored_result = original
    assert table.complete_output_publication_retirement(plan, **proofs).disposition is OutputOwnerPublicationDisposition.APPLIED
