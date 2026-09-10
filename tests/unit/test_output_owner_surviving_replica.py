"""Pure owner reductions for STORED KEEP after publishing-Node loss.

Each case has one tiny result, one outgoing child identity,
one incoming hold and one task-lineage edge. Locations are *already
advertised metadata* installed through the real owner reducer; these tests
do not prove that any physical replica exists. Grant/Node-store validation
belongs to Core composition tests, not this transport-free owner reducer.

No Core, Node, Worker, server, process, thread, timer or wait is constructed
or started. Interleavings are a fixed sequence of synchronous reductions.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold, LineageReferenceEdge
from miniray.ids import JobID, LeaseID, NodeID, ObjectID, TaskID
from miniray.output_handoff import NodeLostOutputResolution, OutputHandoffConflictError
from miniray.output_publication import OutputPublicationManifest
from miniray.ownership import ObjectState, OutputOwnerPublicationConflictError
from tests.unit.test_output_owner_publication import _Fixture, _assert_metadata_only


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure surviving-replica owner test attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "socketpair", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _case(*, all_stored=True, adopted=True):
    values = _Fixture(all_stored=all_stored)
    owner = values.table()
    output = values.publication_id.object_id
    container = ObjectID.for_task(TaskID.derive(values.job, values.task, 80))
    incoming = ContainedReferenceHold(container, values.executor, 'incoming-0')
    assert owner.add_contained_reference(output, incoming)
    assert owner.add_lineage_reference(output, 'incoming-lineage-0')
    assert owner.add_outgoing_lineage_edge(
        output, LineageReferenceEdge(output, values.child, "producer-input"),
    )
    if adopted:
        assert owner.commit_output_publication(values.plan).committed
    # Deterministically distinct identities without a cluster or a listener.
    secondary = NodeID(bytes(value ^ 1 for value in values.node.value))
    return values, owner, secondary


def _advertise(owner, values, node):
    """Model an already-validated location report, not a physical health check."""
    result = values.envelope.result
    assert result.object_id == values.publication_id.object_id
    assert result.storage is protocol.ResultStorage.OBJECT_STORE
    assert owner.add_location(result.object_id, values.execution.attempt_id, node, descriptor=replace(result, node_id=node))


def _resolution(values):
    """Call only after the test's decision-time replica metadata is installed."""
    publisher = values.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "stored-keep-publisher-exit", publisher.node_id, publisher.node_pid,
        publisher.registration_epoch, 1, -9, protocol.NodeDeathReason.PROCESS_EXIT,
        "confirmed publishing Node exit",
    )
    return NodeLostOutputResolution(
        values.publication_id, values.manifest.manifest_digest, values.owner, death,
        complete=values.envelope.complete, keep=True, cleanup=(),
    )


def _snapshots(owner, values):
    return ((owner.snapshot(values.publication_id.object_id),))


def _owner_state(owner, values):
    # Snapshots omit per-location attempt values, so capture that map too.
    output = values.publication_id.object_id
    return deepcopy((
        _snapshots(owner, values),
        ({output: dict(owner._entries[output].location_attempts)}),
        owner._task_lineage, owner._output_publication_receipts,
        owner._output_loss_receipts, owner._retired_output_slots,
        owner._retired_output_attempts,
    ))


def _assert_kept_identity(owner, values, lineage):
    assert owner._task_lineage == lineage
    assert not owner._retired_output_slots and not owner._retired_output_attempts
    result = values.envelope.result
    snapshot = owner.snapshot(result.object_id)
    assert snapshot.current_attempt == values.execution.attempt_id
    assert snapshot.local_tokens and snapshot.contained_holds and snapshot.lineage_tokens
    assert snapshot.outgoing_lineage_edges
    assert snapshot.output_publication is not None
    assert owner.output_owner_result(result.object_id) == result
    if result.storage is protocol.ResultStorage.OBJECT_STORE:
        assert snapshot.canonical_stored_result == result
        assert snapshot.canonical_stored_result.node_id == values.node
    _assert_metadata_only(owner._output_loss_receipts)
    _assert_metadata_only(owner._output_publication_receipts)


def test_survivor_query_filters_publisher_and_unavailable_nodes_without_mutation():
    values, owner, secondary = _case()
    other = NodeID(bytes(value ^ 2 for value in values.node.value))
    _advertise(owner, values, secondary)
    _advertise(owner, values, other)
    before = _owner_state(owner, values)
    queries = (
        ((), tuple(sorted((secondary, other)))),
        ((other,), (secondary,)),
        ((secondary, other), ()),
    )
    for unavailable, expected in queries:
        assert owner.surviving_output_locations(values.manifest, 0, unavailable_nodes=unavailable) == expected
        assert _owner_state(owner, values) == before
    with pytest.raises(OutputOwnerPublicationConflictError, match="outside the manifest"):
        owner.surviving_output_locations(values.manifest, 1)
    inline_values, inline_owner, _ = _case(all_stored=False)
    inline_before = _owner_state(inline_owner, inline_values)
    assert inline_owner.surviving_output_locations(inline_values.manifest, 0) == ()
    assert _owner_state(inline_owner, inline_values) == inline_before



@pytest.mark.parametrize("fence", (
    "missing-receipt", "missing-membership", "replica-epoch",
    "excluded-publisher-epoch", "canonical-checksum", "retirement-pending",
    "retired-slot", "retired-attempt", "collection-pending",
))
def test_survivor_query_never_authorizes_keep_from_fenced_metadata(fence):
    values, owner, secondary = _case()
    _advertise(owner, values, secondary)
    output = (values.publication_id.object_id)
    entry = owner._entries[output]
    # Deliberate negative metadata injection: a usable-looking secondary must
    # not bypass the exact commit receipt, any replica epoch, or lifecycle fence.
    if fence == "missing-receipt":
        owner._output_publication_receipts.pop(values.publication_id)
    elif fence == "missing-membership":
        entry.output_publication = None
    elif fence == "replica-epoch":
        entry.location_attempts[secondary] = values.attempt.next()
    elif fence == "excluded-publisher-epoch":
        # The publisher is excluded from returned locations, but its stale
        # epoch still invalidates the full authoritative location map.
        entry.location_attempts[values.node] = values.attempt.next()
    elif fence == "canonical-checksum":
        entry.canonical_stored_result = replace(entry.canonical_stored_result, checksum="0" * 64)
    elif fence == "retirement-pending":
        entry.output_retirement_id = "pending-exact-slot-retirement"
    elif fence == "retired-slot":
        owner._retired_output_slots.add((values.publication_id, output))
    elif fence == "retired-attempt":
        owner._retired_output_attempts.add((output, values.attempt))
    else:
        entry.collection_pending = True
    before = _owner_state(owner, values)

    for _ in range(2):
        assert owner.surviving_output_locations(values.manifest, 0) == ()
        assert _owner_state(owner, values) == before


def test_stored_keep_preserves_survivor_metadata_and_all_reference_lifetimes():
    values, owner, secondary = _case()
    unavailable = NodeID(bytes(value ^ 2 for value in values.node.value))
    _advertise(owner, values, secondary)
    _advertise(owner, values, unavailable)
    resolution = _resolution(values)
    before = _snapshots(owner, values)
    lineage = deepcopy(owner._task_lineage)
    assert before[0].locations == frozenset((values.node, secondary, unavailable))

    assert owner.resolve_output_node_loss(
        values.manifest, resolution, values.envelope, unavailable_nodes=(unavailable,),
    )

    assert _snapshots(owner, values) == (
        replace(before[0], locations=frozenset((secondary,))),
    )
    assert owner._output_loss_receipts == {values.publication_id: resolution}
    _assert_kept_identity(owner, values, lineage)


def test_stored_keep_needs_adopted_identity_but_no_retained_envelope():
    values, owner, secondary = _case(all_stored=True)
    _advertise(owner, values, secondary)
    resolution = _resolution(values)
    before = _snapshots(owner, values)
    lineage = deepcopy(owner._task_lineage)

    assert owner.resolve_output_node_loss(values.manifest, resolution)

    assert _snapshots(owner, values) == tuple(
        replace(snapshot, locations=frozenset((secondary,))) for snapshot in before
    )
    assert all(snapshot.state is ObjectState.READY_STORED
               and snapshot.inline_data is None for snapshot in _snapshots(owner, values))
    _assert_kept_identity(owner, values, lineage)


@pytest.mark.parametrize("loss", ("remove-location", "remove-node", "unavailable-node"))
def test_stored_keep_loses_last_secondary_after_decision_without_retiring_membership(loss):
    values, owner, secondary = _case(all_stored=True)
    _advertise(owner, values, secondary)
    resolution = _resolution(values)
    lineage = deepcopy(owner._task_lineage)
    # The decision was KEEP while a secondary was advertised. These are later
    # owner/membership reductions, not alterations to that immutable decision.
    owner.remove_node_locations(values.node)
    unavailable = ()
    if loss == "remove-location":
        output = values.publication_id.object_id
        assert owner.remove_location(output, values.attempt, secondary)
    elif loss == "remove-node":
        removal = owner.remove_node_locations(secondary)
        assert set(removal.lost) == ({values.publication_id.object_id})
    else:
        unavailable = (secondary,)
    before = _snapshots(owner, values)

    assert owner.resolve_output_node_loss(
        values.manifest, resolution, unavailable_nodes=unavailable,
    )

    assert _snapshots(owner, values) == tuple(
        replace(snapshot, state=ObjectState.LOST, locations=frozenset())
        for snapshot in before
    )
    assert owner._output_loss_receipts[values.publication_id].keep is True
    _assert_kept_identity(owner, values, lineage)


def test_stored_keep_uses_new_current_replica_not_the_decision_time_location():
    values, owner, secondary = _case(all_stored=True)
    replacement = NodeID(bytes(value ^ 2 for value in values.node.value))
    _advertise(owner, values, secondary)
    resolution = _resolution(values)
    owner.remove_node_locations(values.node)
    owner.remove_node_locations(secondary)
    assert all(snapshot.state is ObjectState.LOST for snapshot in _snapshots(owner, values))
    _advertise(owner, values, replacement)
    before = _snapshots(owner, values)
    lineage = deepcopy(owner._task_lineage)

    assert owner.resolve_output_node_loss(
        values.manifest, resolution, unavailable_nodes=(secondary,),
    )

    assert _snapshots(owner, values) == before
    assert all(snapshot.locations == frozenset((replacement,)) for snapshot in before)
    _assert_kept_identity(owner, values, lineage)


@pytest.mark.parametrize("loss", (('one-output'), "entire-node"))
def test_resolution_replay_cannot_resurrect_a_subsequently_lost_secondary(loss):
    values, owner, secondary = _case(all_stored=True)
    _advertise(owner, values, secondary)
    resolution = _resolution(values)
    assert owner.resolve_output_node_loss(values.manifest, resolution)
    target = (values.publication_id.object_id)
    if loss == ('one-output'):
        assert owner.remove_location(target, values.attempt, secondary)
    else:
        owner.remove_node_locations(secondary)
    assert owner.snapshot(target).state is ObjectState.LOST
    before = _owner_state(owner, values)

    for _ in range(2):
        assert not owner.resolve_output_node_loss(values.manifest, resolution)
        assert _owner_state(owner, values) == before
    assert not owner.snapshot(target).locations
    assert owner.output_owner_result(target) == (values.envelope.result)
    assert owner.snapshot(target).output_publication is not None


def test_unadopted_stored_keep_rejects_even_an_exact_complete_envelope_atomically():
    values, owner, _secondary = _case(adopted=False)
    # This deliberately invalid KEEP has a valid descriptor/witness but no
    # adopted STORED membership; it cannot create owner publication state.
    resolution = _resolution(values)
    before = _owner_state(owner, values)
    assert all(snapshot.state is ObjectState.PENDING for snapshot in _snapshots(owner, values))

    with pytest.raises(OutputOwnerPublicationConflictError):
        owner.resolve_output_node_loss(values.manifest, resolution, values.envelope)

    assert _owner_state(owner, values) == before
    assert owner._output_publication_receipts == owner._output_loss_receipts == {}


def test_inline_keep_still_requires_bytes_with_adopted_owner_membership():
    values, owner, _secondary = _case(all_stored=False)
    resolution = _resolution(values)
    before = _owner_state(owner, values)
    # The owner reducer requires its caller to supply a separately rebuilt
    # INLINE envelope; a metadata-only KEEP receipt is never itself bytes.
    with pytest.raises(OutputOwnerPublicationConflictError, match="locally retained output bytes"):
        owner.resolve_output_node_loss(values.manifest, resolution)
    assert _owner_state(owner, values) == before



@pytest.mark.parametrize("corruption", (
    "canonical-checksum", "canonical-publisher", "producer-epoch",
    "replica-epoch", "different-membership", "missing-membership",
    "collection-pending", "different-lineage-job", "lost-with-locations",
    "ready-without-locations",
))
def test_bad_stored_metadata_cannot_apply_keep_or_mutate_receipts(corruption):
    values, owner, secondary = _case(all_stored=True)
    _advertise(owner, values, secondary)
    resolution = _resolution(values)
    output = (values.publication_id.object_id)
    entry = owner._entries[output]
    # Direct corruption is confined to this negative preflight test. Successful
    # histories above use commit/add/remove reducers rather than fake readiness.
    if corruption == "canonical-checksum":
        entry.canonical_stored_result = replace(entry.canonical_stored_result, checksum="0" * 64)
    elif corruption == "canonical-publisher":
        entry.canonical_stored_result = replace(entry.canonical_stored_result, node_id=secondary)
    elif corruption == "producer-epoch":
        entry.current_attempt = values.attempt.next()
    elif corruption == "replica-epoch":
        entry.location_attempts[secondary] = values.attempt.next()
    elif corruption == "different-membership":
        header = replace(values.header, publication_id=replace(values.publication_id, lease_id=LeaseID.random()))
        manifest = OutputPublicationManifest.create(header, (values.manifest.value))
        entry.output_publication = replace(entry.output_publication, manifest=manifest)
    elif corruption == "missing-membership":
        entry.output_publication = None
    elif corruption == "collection-pending":
        entry.collection_pending = True
    elif corruption == "different-lineage-job":
        # One output has one canonical TaskSpec. Its job is independently
        # bound by the admitted publication header; cross-slot args equality
        # has no supported single-output counterpart.
        job = JobID.random()
        entry.producer_task_spec = replace(
            values.spec, job_id=job, function=replace(values.spec.function, job_id=job),
        )
    elif corruption == "lost-with-locations":
        entry.state = ObjectState.LOST
    else:
        entry.location_attempts.clear()
    before = _owner_state(owner, values)

    with pytest.raises(OutputOwnerPublicationConflictError):
        owner.resolve_output_node_loss(
            values.manifest, resolution, unavailable_nodes=(secondary,),
        )

    assert _owner_state(owner, values) == before
    assert not owner._output_loss_receipts
    assert not owner._retired_output_slots and not owner._retired_output_attempts


@pytest.mark.parametrize("field", ("node_id", "node_pid", "registration_epoch", "reason"))
def test_wrong_publishing_node_death_identity_cannot_apply_stored_keep(field):
    values, owner, secondary = _case(all_stored=True)
    _advertise(owner, values, secondary)
    resolution = _resolution(values)
    death = resolution.node_death
    if field == "node_id":
        changed = secondary
    elif field == "reason":
        changed = protocol.NodeDeathReason.EXPECTED
    else:
        changed = getattr(death, field) + 1
    before = _owner_state(owner, values)

    with pytest.raises((OutputOwnerPublicationConflictError, OutputHandoffConflictError)):
        wrong = replace(resolution, node_death=replace(death, **{field: changed}))
        owner.resolve_output_node_loss(values.manifest, wrong)

    assert _owner_state(owner, values) == before
    assert not owner._output_loss_receipts
