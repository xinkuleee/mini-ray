"""Pure retirement fences shared by plain and unified owner publication.

Two tiny selected slots and at most two publication attempts.  Real metadata
recovery supplies an exact known-Complete all-DROP resolution; the owner CAS
must preserve its retirement across every publishing entry point and must
not use an old epoch to waive newer replica-GC integrity requirements.  No
Node/Core runtime, network, worker, thread, user code or real wait is used.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time
from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceEdge
from miniray.ids import LeaseID, NodeID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationID,
    OutputPublicationManifest,
)
from miniray.output_recovery import (
    OutputPublicationRecoveryAuthority, OutputRecoveryOwnerDecision,
    OutputRecoveryResolution, OutputSlotDecision,
)
from miniray.ownership import (
    InvalidObjectTransitionError, ObjectCollectionState, ObjectState,
    OutputOwnerPublicationConflictError, OutputOwnerPublicationDisposition,
    OutputOwnerPublicationPlan, OutputOwnerRetirementConflictError,
)
from miniray.task_outputs import TaskExecutionKey
from tests.unit.test_output_owner_publication import _Fixture as _OwnerValues
from tests.unit.test_output_publication import _assert_metadata


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure owner fencing test attempted runtime work")

    for owner, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start"),
        (socket, "socket"), (socket, "create_connection"),
        (subprocess, "Popen"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)


def _retired_owner():
    values = _OwnerValues(edges=False)
    # Reuse the canonical value builder but reduce its fixture to two outputs
    # before creating an owner table.  No third slot is ever registered.
    values.spec = replace(values.spec, num_returns=2)
    values.execution = TaskExecutionKey.from_task_spec(values.spec)
    values.full_execution = values.execution
    values.publication_id = OutputPublicationID(values.publication_id.lease_id, values.execution)
    values.header = replace(values.header, publication_id=values.publication_id)
    values.manifest = OutputPublicationManifest.create(values.header, values.manifest.slots[:2])
    complete = OutputPublicationCompleteWitness.for_manifest(values.manifest)
    values.envelope = OutputPublicationEnvelope(values.manifest, complete, values.envelope.results[:2])
    values.plan = OutputOwnerPublicationPlan(values.execution, values.envelope)
    values.payloads = values.payloads[:2]
    table = values.table()
    registry = OutputPublicationRecoveryAuthority()
    registry.report_intent(values.manifest)
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    registry.report_terminal(complete)
    node = values.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "retired-owner-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
        5, 1, protocol.NodeDeathReason.PROCESS_EXIT, "known completed payload lost",
    )
    (work,) = registry.freeze_node_death(death)
    decision = tuple(
        OutputSlotDecision(index, slot.object_id, OutputRecoveryOwnerDecision.DROP)
        for index, slot in enumerate(values.manifest.slots)
    )
    registry.decide_owner(work, values.owner, decision,
                          decision_id="no-owner-payload", complete=complete)
    resolution = OutputRecoveryResolution(
        values.publication_id, values.manifest.manifest_digest, death, values.owner,
        "all-output-effects-gone", (), complete,
    )
    assert registry.resolve_node_loss(work, resolution).snapshot.resolution == resolution
    assert table.resolve_output_node_loss(values.manifest, resolution)
    for object_id in values.execution.output_ids:
        snapshot = table.snapshot(object_id)
        assert snapshot.state is ObjectState.LOST and snapshot.current_attempt == values.attempt
        assert snapshot.producer_task_spec == values.spec
        assert snapshot.inline_data is None and snapshot.canonical_stored_result is None
        assert not snapshot.locations and not snapshot.outgoing_contained_edges
        assert snapshot.output_publication is None and snapshot.output_retirement_id is None
        assert snapshot.local_tokens == frozenset({"handle-{}".format(object_id.return_index)})
    assert table._retired_output_attempts == {
        (object_id, values.attempt) for object_id in values.execution.output_ids
    }
    _assert_metadata(registry.snapshot(values.publication_id))
    return values, table, registry, resolution


def _whole_owner_state(table):
    # Everything except the synchronization primitive is authoritative state,
    # including receipts, released-source bindings, retirement and GC tombstones.
    return deepcopy({name: value for name, value in vars(table).items() if name != "_lock"})


def _replacement_plan(values, *, attempt, tiers, fresh_lease=True):
    execution = values.execution.for_attempt(attempt)
    identity = OutputPublicationID(LeaseID.random(), execution) if fresh_lease else values.publication_id
    # The next attempt may execute on a surviving Node.  Same old-attempt
    # requests keep their old Node so only the retirement fence rejects them.
    node = values.header.node_incarnation
    if attempt != values.attempt:
        node = replace(node, node_id=NodeID.random(), node_pid=node.node_pid + 1)
    header = replace(values.header, publication_id=identity, node_incarnation=node)
    slots = tuple(replace(slot, tier=tier) for slot, tier in zip(values.manifest.slots, tiers))
    manifest = OutputPublicationManifest.create(header, slots)
    results = tuple(
        protocol.ResultDescriptor(
            slot.object_id, slot.tier, slot.size_bytes, values.owner, node.node_id, slot.checksum,
            values.payloads[index] if slot.tier is protocol.ResultStorage.INLINE else None,
        ) for index, slot in enumerate(slots)
    )
    return OutputOwnerPublicationPlan(execution, OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), results,
    ))


def _assert_rejected_without_mutation(table, call):
    before = _whole_owner_state(table)
    try:
        result = call()
    except (InvalidObjectTransitionError, OutputOwnerPublicationConflictError):
        pass
    else:
        if type(result) is bool:
            assert result is False, "retired epoch was silently published"
        else:
            assert result.disposition is OutputOwnerPublicationDisposition.FENCED
            assert not result.committed
    assert _whole_owner_state(table) == before


@pytest.mark.parametrize("entrypoint", (
    "stored-replica", "location-only", "plain-task-outputs", "same-unified-publication",
    "fresh-lease-mixed", "fresh-lease-all-stored", "fresh-lease-all-inline",
))
def test_retired_attempt_is_fenced_across_plain_and_cross_tier_unified_publication(entrypoint):
    values, table, _registry, _resolution = _retired_owner()
    stored = _replacement_plan(values, attempt=values.attempt, tiers=(
        protocol.ResultStorage.OBJECT_STORE, protocol.ResultStorage.OBJECT_STORE,
    ))
    first = values.execution.output_ids[0]
    descriptor = stored.envelope.results[0]
    if entrypoint == "stored-replica":
        call = lambda: table.publish_stored(first, values.attempt, values.node, descriptor=descriptor)
    elif entrypoint == "location-only":
        call = lambda: table.publish_stored(first, values.attempt, values.node)
    elif entrypoint == "plain-task-outputs":
        call = lambda: table.publish_task_outputs(values.execution, stored.envelope.results)
    else:
        if entrypoint == "same-unified-publication":
            plan = values.plan
        elif entrypoint == "fresh-lease-mixed":
            plan = _replacement_plan(values, attempt=values.attempt, tiers=(
                protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
            ))
        elif entrypoint == "fresh-lease-all-stored":
            plan = stored
        else:
            plan = _replacement_plan(values, attempt=values.attempt, tiers=(
                protocol.ResultStorage.INLINE, protocol.ResultStorage.INLINE,
            ))
        call = lambda: table.commit_output_publication(plan)
    _assert_rejected_without_mutation(table, call)
    assert table._output_publication_receipts == {values.publication_id: values.manifest}


def test_next_attempt_accepts_fresh_cross_tier_publication_without_replaying_old_retirement():
    values, table, registry, resolution = _retired_owner()
    old_tombstones = frozenset(table._retired_output_attempts)
    next_attempt = values.attempt.next()
    assert table.advance_task_outputs(values.execution, next_attempt)
    pending = _whole_owner_state(table)
    assert not table.resolve_output_node_loss(values.manifest, resolution)
    assert _whole_owner_state(table) == pending
    assert table.commit_output_publication(values.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert _whole_owner_state(table) == pending
    fresh = _replacement_plan(values, attempt=next_attempt, tiers=(
        protocol.ResultStorage.OBJECT_STORE, protocol.ResultStorage.INLINE,
    ))
    assert table.validate_output_publication(fresh) is OutputOwnerPublicationDisposition.APPLIED
    assert _whole_owner_state(table) == pending
    receipt = table.commit_output_publication(fresh)
    assert receipt.committed and receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    ready = _whole_owner_state(table)
    assert table.commit_output_publication(fresh).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert not table.resolve_output_node_loss(values.manifest, resolution)
    assert _whole_owner_state(table) == ready
    assert frozenset(table._retired_output_attempts) == old_tombstones
    for index, object_id in enumerate(values.execution.output_ids):
        snapshot = table.snapshot(object_id)
        assert snapshot.current_attempt == next_attempt and snapshot.producer_task_spec == values.spec
        assert snapshot.output_publication.publication_id == fresh.publication_id
        assert snapshot.state is (ObjectState.READY_STORED if index == 0 else ObjectState.READY_INLINE)
        assert (object_id, next_attempt) not in table._retired_output_attempts
    assert registry.snapshot(values.publication_id).resolution == resolution


def test_old_epoch_tombstone_does_not_waive_new_lost_replica_canonical_gc_identity():
    values, table, _registry, resolution = _retired_owner()
    next_attempt = values.attempt.next()
    assert table.advance_task_outputs(values.execution, next_attempt)
    target, sibling = values.execution.output_ids
    new_node = NodeID.random()
    # The public location-only path is legitimate for narrow owner callers.
    # It is deliberately missing canonical bytes/integrity metadata.
    assert table.publish_stored(target, next_attempt, new_node)
    assert table.mark_lost(target, next_attempt)
    values.release_handle(table, target)
    before = _whole_owner_state(table)
    assert table.snapshot(target).canonical_stored_result is None
    assert (target, values.attempt) in table._retired_output_attempts
    assert (target, next_attempt) not in table._retired_output_attempts
    with pytest.raises(InvalidObjectTransitionError, match="canonical metadata"):
        table.begin_collection(target, collection_id="must-not-use-old-retirement")
    assert _whole_owner_state(table) == before
    assert not table.resolve_output_node_loss(values.manifest, resolution)
    assert _whole_owner_state(table) == before
    assert table.collection_state(target) is ObjectCollectionState.ACTIVE
    assert table.snapshot(sibling).state is ObjectState.PENDING
    assert table.snapshot(sibling).current_attempt == next_attempt


@pytest.mark.parametrize("contamination", ("payload", "canonical", "locations", "edges"))
def test_contaminated_retired_metadata_cannot_be_collected_or_silently_repaired(contamination):
    values, table, _registry, resolution = _retired_owner()
    target, _sibling = values.execution.output_ids
    values.release_handle(table, target)
    entry = table._entries[target]
    # Fault-inject exactly one stale field after a genuine retirement.  The
    # collector must reject this inconsistent history without discarding it.
    descriptor = replace(values.envelope.results[0],
                         storage=protocol.ResultStorage.OBJECT_STORE, inline_data=None)
    if contamination == "payload":
        entry.inline_data = b"late-result-bytes"
    elif contamination == "canonical":
        entry.canonical_stored_result = descriptor
    elif contamination == "locations":
        entry.location_attempts[values.node] = values.attempt
    else:
        entry.outgoing_contained_edges.add(ContainedReferenceEdge(
            target, values.child, values.executor, ("child.invalid", 1), "late-edge",
        ))
    before = _whole_owner_state(table)
    with pytest.raises(OutputOwnerRetirementConflictError, match="regained result metadata"):
        table.begin_collection(target, collection_id="reject-contaminated-retirement")
    assert _whole_owner_state(table) == before
    assert table.collection_state(target) is ObjectCollectionState.ACTIVE
    assert table.snapshot(target).collection_plan is None
    assert not table.resolve_output_node_loss(values.manifest, resolution)
    assert _whole_owner_state(table) == before


def test_exact_retired_epoch_without_contamination_remains_metadata_only_collectible():
    values, table, _registry, _resolution = _retired_owner()
    for object_id in values.execution.output_ids:
        values.release_handle(table, object_id)
        plan = table.begin_collection(object_id, collection_id="clean-retired-{}".format(object_id.return_index))
        assert plan is not None
        assert plan.producer_attempt_id == values.attempt
        assert plan.producer_task_spec == values.spec
        assert plan.canonical_size_bytes is None and plan.canonical_checksum is None
        assert plan.locations == () and plan.contained_releases == ()
        assert table.complete_collection(plan).collected
        assert table.collection_state(object_id) is ObjectCollectionState.COLLECTED
    assert not table._entries
    assert table.commit_output_publication(values.plan).disposition is OutputOwnerPublicationDisposition.FENCED
