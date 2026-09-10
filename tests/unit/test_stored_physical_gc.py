"""Bounded stored-object collection on actual Node replicas and owner receipts.

One threadless Core, one accepted output and at most two 1-KiB Node stores.
The existing publication fixture produces real owner registration and Node
Complete before owner adoption. Replica Drop callbacks invoke actual Node
handlers; only ACK delivery or one physical pin is faulted. Retry notifications
are recorded locally and every replay is an explicit bounded call. No process,
thread, socket, timer, sleep or blocking queue join runs.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceEdge, ContainedReferenceHold
from miniray.core import CoreWorker
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ObjectCollectionInProgressError, ObjectCollectionState, ObjectOwnerTable, InvalidObjectTransitionError,
)
from miniray.reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionRuntimeError,
)
from miniray.resources import ResourceVector
from miniray.object_store import ObjectStore
from miniray.object_manager import ObjectManager
from miniray.recovery import TaskState
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture as _publication
from tests.unit.test_node_dependency_pull import _bare_node
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from miniray.output_publication import (OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
    OutputPublicationCompleteWitness, OutputPublicationEnvelope)
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.ownership import OutputOwnerPublicationPlan
from miniray.task_outputs import TaskExecution
from miniray.ids import LeaseID


def _identity(index: int = 0) -> tuple[JobID, ObjectID, AttemptID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return job_id, ObjectID.for_task(task_id), AttemptID(task_id, 0)


def _spec(
    job_id: JobID, object_id: ObjectID, attempt: AttemptID, owner: WorkerID
) -> protocol.TaskSpec:
    return protocol.TaskSpec(
        job_id, object_id.task_id, attempt,
        protocol.FunctionKey(job_id, __name__, "producer", "v1"),
        (), 1, ResourceVector(), owner, max_retries=1,
    )


def _stored_core(
    *, locations: int = 2, local_token: object | None = None,
) -> tuple[CoreWorker, ObjectID, AttemptID, protocol.TaskSpec, tuple[NodeID, ...]]:
    assert locations in (1, 2)
    fixture, source, core, pending, reply, _calls, _rpc = _publication(refs=False, stored=True)
    object_id, attempt, spec = pending.object_id, pending.spec.attempt_id, pending.spec
    assert core._publish_reply(pending, reply, expected_node_id=source.node_id,
                               expected_lease_id=fixture.id.lease_id)
    assert core._finish_pending_task(pending)
    nodes = {source.node_id: source}
    payload = fixture.values.payload
    assert len(payload) <= 64
    for _ in range(locations - 1):
        node_id = NodeID.random()
        replica = _bare_node(node_id, ResourceVector())
        replica._object_store = ObjectStore(1024)
        replica._object_manager = ObjectManager(node_id, replica._object_store)
        assert replica._handle_seal_object(protocol.SealObject.from_data(
            object_id, attempt, core.worker_id, payload,
        )).sealed
        assert core.owner_table.publish_stored(object_id, attempt, node_id)
        nodes[node_id] = replica
    if local_token is not None:
        assert core.owner_table.add_local_reference(object_id, local_token)
    assert core.owner_table.release_local_reference(object_id, 'outer0')
    core._physical_gc_nodes = nodes
    core._physical_gc_retry_notifications = []

    def schedule(mailbox, event, delay):
        assert mailbox is core._reference_mailbox and 0 < delay <= 0.25
        assert event.object_id == object_id
        core._physical_gc_retry_notifications.append(event)
        assert len(core._physical_gc_retry_notifications) <= 3

    core._schedule_reference_event = schedule
    return core, object_id, attempt, spec, tuple(sorted(nodes))


def _drop_reply(core: CoreWorker, request: protocol.DropObjectReplica, *, pinned=False):
    node = core._physical_gc_nodes[request.node_id]
    pin = node.object_store.pin(request.object_id, 'physical-gc-pin') if pinned else None
    try:
        reply = node._handle_drop_object_replica(request)
    finally:
        if pin is not None:
            node.object_store.unpin(request.object_id, pin)
    if pinned:
        assert reply.status is protocol.DropObjectReplicaStatus.PINNED
    else:
        assert reply.status in (protocol.DropObjectReplicaStatus.DROPPED,
                                protocol.DropObjectReplicaStatus.ALREADY_DROPPED)
    return reply


def _stop_core(core: CoreWorker) -> None:
    # No background execution exists and teardown does not claim GC completion.
    for object_id in tuple(core._objects):
        for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
            assert core.owner_table.release_local_reference(object_id, token)
    close_pure_core(core)


@pytest.mark.unit
def test_owner_freezes_complete_stored_plan_and_fences_every_mutator() -> None:
    job_id, object_id, attempt = _identity()
    owner_id = WorkerID.random()
    nodes = tuple(sorted((NodeID.random(), NodeID.random())))
    spec = _spec(job_id, object_id, attempt, owner_id)
    table = ObjectOwnerTable()
    table.register(object_id, current_attempt=attempt, producer_task_spec=spec)
    child = ObjectID.for_task(TaskID.random())
    edge = ContainedReferenceEdge(
        object_id, child, WorkerID.random(), ("127.0.0.1", 27001), "edge"
    )
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, 17, owner_id, nodes[0], "a" * 64,
    )
    execution = TaskExecution.from_task_spec(spec)
    transfer = PreparedContainedTransfer(child, edge.contained_owner_worker_id,
        edge.contained_owner_address, OwnedContainedSource(edge.contained_owner_worker_id),
        ContainedReferenceHold(object_id, edge.contained_owner_worker_id, edge.transfer_token),
        ContainedReferenceHold(object_id, owner_id, edge.transfer_token))
    manifest = OutputPublicationManifest.create(OutputPublicationHeader(
        OutputPublicationID(LeaseID.random(), execution), job_id, edge.contained_owner_worker_id,
        owner_id, OutputPublicationNodeIncarnation(nodes[0], 101, 1)),
        (OutputValue(descriptor.storage, 17, descriptor.checksum, (transfer,))))
    assert table.commit_output_publication(OutputOwnerPublicationPlan(execution,
        OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
                                  descriptor))).committed
    for node in nodes[1:]:
        table.publish_stored(object_id, attempt, node)

    plan = table.begin_collection(
        object_id, collection_id="collection-1",
        canonical_size_bytes=17, canonical_checksum="a" * 64,
    )

    assert plan is not None
    assert plan.producer_attempt_id == attempt
    assert plan.locations == nodes
    assert plan.producer_task_spec == spec
    assert plan.canonical_size_bytes == 17
    assert plan.canonical_checksum == "a" * 64
    assert plan.contained_releases == (edge,)
    assert table.collection_state(object_id) is ObjectCollectionState.COLLECTING
    mutators = (
        lambda: table.add_local_reference(object_id, "late-local"),
        lambda: table.add_borrowed_reference(object_id, "late-borrow"),
        lambda: table.add_contained_reference(object_id, ContainedReferenceHold(
            ObjectID.for_task(TaskID.random()), WorkerID.random(), "late-contained")),
        lambda: table.publish_stored(object_id, attempt, NodeID.random()),
        lambda: table.mark_lost(object_id, attempt),
        lambda: table.advance_attempt(
            object_id, expected_attempt=attempt, next_attempt=attempt.next()
        ),
    )
    for mutate in mutators:
        with pytest.raises(ObjectCollectionInProgressError):
            mutate()


@pytest.mark.unit
def test_two_node_partial_ack_replays_only_missing_drop() -> None:
    core, object_id, _attempt, _spec_value, nodes = _stored_core()
    calls: list[NodeID] = []
    rounds = {node: 0 for node in nodes}

    def rpc(_address: object, _handler: str, request: object) -> object:
        assert isinstance(request, protocol.DropObjectReplica)
        calls.append(request.node_id)
        rounds[request.node_id] += 1
        if request.node_id == nodes[1] and rounds[request.node_id] == 1:
            return _drop_reply(core, request, pinned=True)
        return _drop_reply(core, request)

    core._rpc = rpc
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27100)
    try:
        core._reference_released(object_id)
        obligation = core._object_gc_obligations[object_id]
        assert set(obligation.pending_drops) == {nodes[1]}
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTING
        assert not core._physical_gc_nodes[nodes[0]].object_store.contains(object_id)
        assert core._physical_gc_nodes[nodes[1]].object_store.contains(object_id)

        core._reference_released(object_id)
        assert calls.count(nodes[0]) == 1
        assert calls.count(nodes[1]) == 2
        assert object_id not in core._object_gc_obligations
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert object_id not in core._objects
        assert object_id not in core._stored_descriptors
        assert core._recovery.lineage_for_object(object_id) is None
    finally:
        _stop_core(core)


@pytest.mark.unit
def test_wrong_drop_ack_identity_is_ignored_until_exact_replay() -> None:
    core, object_id, _attempt, _spec_value, nodes = _stored_core(locations=1)
    calls = 0

    def rpc(_address: object, _handler: str, request: object) -> object:
        nonlocal calls
        calls += 1
        assert isinstance(request, protocol.DropObjectReplica)
        reply = _drop_reply(core, request)
        return replace(reply, owner_worker_id=WorkerID.random()) if calls == 1 else reply

    core._rpc = rpc
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27101)
    try:
        core._reference_released(object_id)
        first = core._object_gc_obligations[object_id]
        frozen_id = first.plan.collection_id
        assert set(first.pending_drops) == set(nodes)
        assert core._recovery.lineage_for_object(object_id) is not None

        core._reference_released(object_id)
        assert calls == 2
        assert not core._physical_gc_nodes[nodes[0]].object_store.contains(object_id, sealed_only=False)
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert object_id not in core._object_gc_obligations
        assert frozen_id
    finally:
        _stop_core(core)


@pytest.mark.unit
def test_shutdown_reports_unclean_then_second_convergence_is_clean() -> None:
    core, object_id, _attempt, _spec_value, _nodes = _stored_core(locations=1)
    calls = 0

    def rpc(_address: object, _handler: str, request: object) -> object:
        nonlocal calls
        calls += 1
        assert isinstance(request, protocol.DropObjectReplica)
        if calls < 3:
            raise TimeoutError("ambiguous drop ACK")
        return _drop_reply(core, request)

    core._rpc = rpc
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27102)
    try:
        core._reference_released(object_id)
        assert not core._retry_gc_obligations_for_shutdown()
        assert object_id in core._object_gc_obligations
        assert core._retry_gc_obligations_for_shutdown()
        assert object_id not in core._object_gc_obligations
    finally:
        _stop_core(core)


@pytest.mark.unit
def test_collection_freeze_rejects_reconstruction_before_budget_mutation() -> None:
    core, object_id, attempt, spec, _nodes = _stored_core(locations=1)
    assert core.owner_table.mark_lost(object_id, attempt)
    descriptor = core._stored_descriptors[object_id]
    plan = core.owner_table.begin_collection(
        object_id, collection_id="lost-collection",
        canonical_size_bytes=descriptor.size_bytes, canonical_checksum=descriptor.checksum,
    )
    assert plan is not None
    coordinator = ReconstructionCoordinator(core._recovery, core.owner_table)
    before = replace(core._recovery.task_record(spec.task_id))
    owner_before = core.owner_table.snapshot(object_id)
    try:
        with pytest.raises(ReconstructionRuntimeError, match="collection"):
            coordinator.request(object_id)
        assert core._recovery.task_record(spec.task_id) == before
        assert core._recovery.active_recovery(spec.task_id) is None
        assert core.owner_table.snapshot(object_id) == owner_before
    finally:
        _stop_core(core)


@pytest.mark.unit
def test_close_before_stored_success_records_lineage_before_gc() -> None:
    fixture, node, core, pending, reply, _calls, original_rpc = _publication(refs=False, stored=True)
    object_id = pending.object_id
    observed = []
    assert core._recovery.task_record(pending.task_id).state is TaskState.PENDING
    assert core.owner_table.release_local_reference(object_id, 'outer0')
    assert not core.owner_table.snapshot(object_id).local_tokens

    def rpc(address, handler, request):
        if handler == 'drop_object_replica':
            assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
            assert core._recovery.lineage_for_object(object_id) is not None
            observed.append(request)
        return original_rpc(address, handler, request)

    core._rpc = rpc
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id,
                                   expected_lease_id=fixture.id.lease_id)
        assert not observed and node.object_store.contains(object_id)
        assert core._finish_pending_task(pending)
        core._reference_mailbox.drain()
        assert len(observed) == 1
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(object_id) is None
        assert not node.object_store.contains(object_id, sealed_only=False)
        assert not core._object_gc_obligations and not core._task_finish_barriers
    finally:
        _stop_core(core)


@pytest.mark.unit
def test_owner_completion_failure_never_deletes_recovery_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, object_id, _attempt, _spec_value, _nodes = _stored_core(locations=1)
    core._rpc = lambda _address, _handler, request: _drop_reply(core, request)
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27104)
    original = core.owner_table.complete_output_publication_collection

    def fail_once(_plan: object) -> object:
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", original)
        raise RuntimeError("injected owner commit failure")

    monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", fail_once)
    try:
        with pytest.raises(RuntimeError, match="owner commit"):
            core._reference_released(object_id)
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTING
        assert core._recovery.lineage_for_object(object_id) is not None
        core._reference_released(object_id)
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(object_id) is None
    finally:
        _stop_core(core)


@pytest.mark.unit
def test_stored_collection_requires_canonical_metadata_before_freezing_drop_plan() -> None:
    _job, object_id, attempt = _identity()
    table = ObjectOwnerTable()
    table.register(object_id, current_attempt=attempt)
    table.publish_stored(object_id, attempt, NodeID.random())

    before = table.snapshot(object_id)
    with pytest.raises(InvalidObjectTransitionError, match="canonical metadata"):
        table.begin_collection(object_id)
    assert table.snapshot(object_id) == before
    assert table.contains(object_id)
