"""Mixed owner metadata and real-thread stored-object collection contracts.

Only the two ObjectOwnerTable-only cases are pure. Every _stored_core fixture
starts a real reference-event thread; retries may also start Timer threads.
Their original cleanup cancels timers without a full bounded join, and the
close-before-success case uses an unbounded Queue.join plus a stale pre-unified
success reply. Keep those six tests heavy without hiding or rewriting either
their runtime contract or the separate publication-fixture migration debt.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceEdge, ContainedReferenceHold
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, _ObjectWaiter, _PendingTask
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ObjectCollectionInProgressError, ObjectCollectionState, ObjectOwnerTable, InvalidObjectTransitionError,
)
from miniray.reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionRuntimeError,
)
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector
from miniray.output_publication import (OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputSlotManifest,
    OutputPublicationCompleteWitness, OutputPublicationEnvelope)
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.ownership import OutputOwnerPublicationPlan
from miniray.task_outputs import TaskExecutionKey
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
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 26021)
    core.gcs_address = ("127.0.0.1", 26022)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core._owner_table = ObjectOwnerTable()
    core._recovery = RecoveryManager()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._membership_epoch = 0
    core._installed_cluster_snapshot = None
    core._home_route = _HomeRoute(core.node_id, core.node_address, 0)
    core._dead_nodes = {}
    core._completion = threading.Condition(core._state_lock)
    core._object_gc_obligations = {}
    core._gc_retry_timers = set()
    core._owner_protocol_open = True
    core._inflight_borrow_ops = 0
    core._initialize_reference_events()

    task_id = TaskID.derive(core.job_id, core.driver_task_id, 0)
    object_id = ObjectID.for_task(task_id)
    attempt = AttemptID(task_id, 0)
    spec = _spec(core.job_id, object_id, attempt, core.worker_id)
    node_ids = (core.node_id,) + tuple(NodeID.random() for _ in range(locations - 1))
    core.owner_table.register(
        object_id, current_attempt=attempt, producer_task_spec=spec,
        local_token=local_token,
    )
    for node_id in node_ids:
        core.owner_table.publish_stored(object_id, attempt, node_id)
    payload = b"stored-physical-gc"
    core._stored_descriptors[object_id] = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
        core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(),
    )
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    core._recovery.register_task(spec, max_retries=1)
    core._recovery.record_task_success(spec.task_id, attempt)
    return core, object_id, attempt, spec, tuple(sorted(node_ids))


def _drop_reply(
    request: protocol.DropObjectReplica,
    status: protocol.DropObjectReplicaStatus,
) -> protocol.DropObjectReplicaReply:
    return protocol.DropObjectReplicaReply(
        request.object_id, request.producer_attempt_id,
        request.owner_worker_id, request.node_id, request.checksum, status,
        None if status in (
            protocol.DropObjectReplicaStatus.DROPPED,
            protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        ) else status.value.lower(),
    )


def _stop_core(core: CoreWorker) -> None:
    core._stop_reference_events(__import__("time").monotonic() + 1.0)
    for timer in tuple(core._gc_retry_timers):
        timer.cancel()


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
    execution = TaskExecutionKey.from_task_spec(spec)
    transfer = PreparedContainedTransfer(child, edge.contained_owner_worker_id,
        edge.contained_owner_address, OwnedContainedSource(edge.contained_owner_worker_id),
        ContainedReferenceHold(object_id, edge.contained_owner_worker_id, edge.transfer_token),
        ContainedReferenceHold(object_id, owner_id, edge.transfer_token))
    manifest = OutputPublicationManifest.create(OutputPublicationHeader(
        OutputPublicationID(LeaseID.random(), execution), job_id, edge.contained_owner_worker_id,
        owner_id, OutputPublicationNodeIncarnation(nodes[0], 101, 1)),
        (OutputSlotManifest(object_id, descriptor.storage, 17, descriptor.checksum, (transfer,)),))
    assert table.commit_output_publication(OutputOwnerPublicationPlan(execution,
        OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest),
                                  (descriptor,)))).committed
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


@pytest.mark.heavy
def test_two_node_partial_ack_replays_only_missing_drop() -> None:
    core, object_id, _attempt, _spec_value, nodes = _stored_core()
    calls: list[NodeID] = []
    rounds = {node: 0 for node in nodes}

    def rpc(_address: object, _handler: str, request: object) -> object:
        assert isinstance(request, protocol.DropObjectReplica)
        calls.append(request.node_id)
        rounds[request.node_id] += 1
        if request.node_id == nodes[1] and rounds[request.node_id] == 1:
            return _drop_reply(request, protocol.DropObjectReplicaStatus.PINNED)
        return _drop_reply(
            request, protocol.DropObjectReplicaStatus.DROPPED
            if rounds[request.node_id] == 1
            else protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        )

    core._rpc = rpc
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27100)
    try:
        core._reference_released(object_id)
        obligation = core._object_gc_obligations[object_id]
        assert set(obligation.pending_drops) == {nodes[1]}
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTING

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


@pytest.mark.heavy
def test_wrong_drop_ack_identity_is_ignored_until_exact_replay() -> None:
    core, object_id, _attempt, _spec_value, nodes = _stored_core(locations=1)
    calls = 0

    def rpc(_address: object, _handler: str, request: object) -> object:
        nonlocal calls
        calls += 1
        assert isinstance(request, protocol.DropObjectReplica)
        reply = _drop_reply(request, protocol.DropObjectReplicaStatus.DROPPED)
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
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert object_id not in core._object_gc_obligations
        assert frozen_id
    finally:
        _stop_core(core)


@pytest.mark.heavy
def test_shutdown_reports_unclean_then_second_convergence_is_clean() -> None:
    core, object_id, _attempt, _spec_value, _nodes = _stored_core(locations=1)
    calls = 0

    def rpc(_address: object, _handler: str, request: object) -> object:
        nonlocal calls
        calls += 1
        assert isinstance(request, protocol.DropObjectReplica)
        if calls < 3:
            raise TimeoutError("ambiguous drop ACK")
        return _drop_reply(request, protocol.DropObjectReplicaStatus.ALREADY_DROPPED)

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


@pytest.mark.heavy
def test_collection_freeze_rejects_reconstruction_before_budget_mutation() -> None:
    core, object_id, attempt, spec, _nodes = _stored_core(locations=1)
    core.owner_table.mark_lost(object_id, attempt)
    core.owner_table.begin_collection(
        object_id, collection_id="lost-collection",
        canonical_size_bytes=18, canonical_checksum="b" * 64,
    )
    coordinator = ReconstructionCoordinator(core._recovery, core.owner_table)
    before = core._recovery.task_record(spec.task_id).retries_started

    with pytest.raises(ReconstructionRuntimeError, match="collection"):
        coordinator.request(object_id)

    assert core._recovery.task_record(spec.task_id).retries_started == before
    assert core._recovery.active_recovery(spec.task_id) is None
    _stop_core(core)


@pytest.mark.heavy
def test_close_before_stored_success_records_lineage_before_gc() -> None:
    core, object_id, attempt, spec, _nodes = _stored_core(
        locations=1, local_token="handle"
    )
    # Reset the helper's completed result into the pre-reply state while
    # keeping the same registered logical producer and local handle.
    core.owner_table.remove_location(object_id, attempt, core.node_id)
    core.owner_table.advance_attempt(
        object_id, expected_attempt=attempt, next_attempt=attempt.next()
    )
    next_attempt = attempt.next()
    next_spec = replace(spec, attempt_id=next_attempt)
    # Rebuild a coherent fixture instead of mutating RecoveryManager's
    # historical record across attempts.
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(
        object_id, current_attempt=next_attempt, producer_task_spec=next_spec,
        local_token="handle",
    )
    core._recovery = RecoveryManager()
    core._recovery.register_task(next_spec, max_retries=1)
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    core.owner_table.release_local_reference(object_id, "handle")
    payload = b"closed-before-ready"
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
        core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(),
    )
    reply = protocol.TaskReply(
        next_spec.task_id, next_attempt, WorkerID.random(),
        protocol.TaskReplyStatus.SUCCEEDED, (descriptor,),
    )
    pending = _PendingTask(object_id, next_spec)
    core._rpc = lambda _address, _handler, request: _drop_reply(
        request, protocol.DropObjectReplicaStatus.DROPPED
    )
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27103)
    try:
        assert core._publish_reply(
            pending, reply, expected_node_id=core.node_id
        )
        core._reference_mailbox.events.join()
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(object_id) is None
    finally:
        _stop_core(core)


@pytest.mark.heavy
def test_owner_completion_failure_never_deletes_recovery_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, object_id, _attempt, _spec_value, _nodes = _stored_core(locations=1)
    core._rpc = lambda _address, _handler, request: _drop_reply(
        request, protocol.DropObjectReplicaStatus.DROPPED
    )
    core._resolve_node_address = lambda _node, *, home_route=None: ("127.0.0.1", 27104)
    original = core.owner_table.complete_collection

    def fail_once(_plan: object) -> object:
        monkeypatch.setattr(core.owner_table, "complete_collection", original)
        raise RuntimeError("injected owner commit failure")

    monkeypatch.setattr(core.owner_table, "complete_collection", fail_once)
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
