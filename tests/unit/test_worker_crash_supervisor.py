"""Mixed in-memory Worker-death reducers and real supervisor/race tests.

The bare Node fixture owns only a 1 KiB in-memory store and fake processes.
Eleven synchronous functions remain unit; their successful completion paths
use the actual journal/adapter, output envelope and resource ledger.
The live supervisor retry and concurrent detector/Complete functions start real
threads (the race also uses an unbounded Barrier), so remain heavy pending an
independent bounded review. Original IDs and semantic contracts are retained.
The old success fixtures use the required publication path; stale reseal checks
also preserve the newer attempt across the original deletion watermark.
"""

from __future__ import annotations

import hashlib
import socket
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseOutcome, _LeaseRecord, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_handoff import OutputHandoffTable, OutputHandoffPhase
from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector
from miniray.task_outputs import TaskExecution
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerFailpointConfig,
    WorkerFailpointMode,
    WorkerServer,
)


from tests.support._worker_protocol import initialize_worker_protocol

@pytest.fixture(autouse=True)
def _pure_cases_have_no_runtime(request, monkeypatch):
    if request.node.get_closest_marker("heavy") is not None:
        return

    def forbidden(*_args, **_kwargs):
        pytest.fail("pure crash-supervisor test attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Event, "wait"), (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Process:
    def __init__(self, pid: int, *, alive: bool, exitcode: int | None = None) -> None:
        self.pid = pid
        self.alive = alive
        self.exitcode = exitcode
        self.closed = False
        self.terminated = False

    def is_alive(self) -> bool:
        return self.alive

    def join(self, _timeout: float = 0) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False
        self.exitcode = -15


class _CountingLedger(ResourceLedger):
    def __init__(self, total: ResourceVector) -> None:
        super().__init__(total)
        self.release_calls = 0

    def release(self, token: AllocationToken) -> bool:
        self.release_calls += 1
        return super().release(token)


def _node_fixture() -> tuple[
    NodeServer, protocol.RequestWorkerLease, protocol.GrantWorkerLease, ObjectID
]:
    node_id = NodeID.random()
    executor = WorkerID.random()
    owner = WorkerID.random()
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    object_id = ObjectID.for_task(task)
    lease = LeaseID.random()
    allocation = AllocationToken("crash-supervisor")
    total = ResourceVector({"CPU": 1})
    process = _Process(7101, alive=True)

    node = object.__new__(NodeServer)
    node.node_id = node_id
    node.num_workers_per_node = 1
    node._worker_order = (executor,)
    node._workers = {
        executor: _WorkerSlot(
            executor, process=process, address=("127.0.0.1", 27101),
            pid=process.pid, active_lease_id=lease,
        )
    }
    node._ledger = _CountingLedger(total)
    node._ledger.allocate(total, allocation)
    node._cluster_nodes = (NodeSnapshot(node_id, total, ResourceVector()),)
    node._cluster_addresses = {node_id: ("127.0.0.1", 27001)}
    node._gcs_address = None
    node._registered_with_gcs = False
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._object_store = ObjectStore(1024)
    node._dependency_pin_cleanups = {}
    node._pinned_transfers = {}
    node._actor_workers = {}
    node._shutdown_request_id = None
    node._stop_event = threading.Event()
    node._worker_supervisor_stop = threading.Event()
    node._worker_supervisor_thread = None
    node._worker_replacements_inflight = 0
    node._worker_lifecycle_lock = threading.Lock()
    node._dead_worker_exitcodes = {}
    node._worker_finalize_results = {}
    node._worker_drain_statuses = {}
    node._inflight_lease_requests = 0
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node.event_sink = None

    request = protocol.RequestWorkerLease(
        lease, task, attempt, total, node_id, owner, target_node_id=node_id,
        return_ids=(object_id,),
    )
    grant = protocol.GrantWorkerLease(
        lease, task, attempt, node_id, executor, ("127.0.0.1", 27101), allocation
    )
    record = _LeaseRecord(request, allocation, grant)
    node._leases = {lease: record}
    node._lease_outcomes = {lease: _LeaseOutcome(request, grant)}
    node._lease_cancellations = {}
    node._lease_request_locks = {}
    return node, request, grant, object_id


def _query(
    request: protocol.RequestWorkerLease,
    grant: protocol.GrantWorkerLease,
    object_id: ObjectID,
) -> protocol.GetWorkerLeaseOutcome:
    return protocol.GetWorkerLeaseOutcome(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
        request.requester_worker_id, (object_id,),
    )


def _start(request, grant):
    return protocol.StartWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id
    )


def _complete(request, grant, status=protocol.TaskReplyStatus.SUCCEEDED):
    return protocol.CompleteWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id, status
    )


def _attach_output_publication(node):
    """Add real publication/storage callbacks without changing lease truth."""
    assert not hasattr(node, "_output_publication_journal")
    node._node_pid, node._registration_epoch = 7001, 1
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._local_replica_write_claims = {}
    node._object_localization_locks = {}
    journal = node._output_publication_journal = OutputPublicationJournal()
    handoffs = OutputHandoffTable()

    def forbidden(*_args, **_kwargs):
        pytest.fail("ref-free publication attempted child/graph/RPC effects")

    def register_owner(manifest):
        snapshot = handoffs.register(manifest, manifest.publication_id.attempt_id)
        reply = wire.OutputHandoffReply(wire.RegisterOutputHandoff(manifest), True, snapshot)
        assert reply.snapshot.manifest == manifest

    def report_complete(witness):
        snapshot = handoffs.record_complete(witness)
        reply = wire.OutputHandoffReply(wire.ReportOutputHandoffComplete(witness), True, snapshot)
        assert reply.snapshot.complete == witness

    def report_rollback(tombstone, *, manifest):
        assert journal.snapshot(manifest.publication_id).rollback_tombstone == tombstone
        snapshot = handoffs.abort_manifest(manifest, tombstone.plan.rollback_id)
        reply = wire.OutputHandoffReply(wire.ReportOutputHandoffRollback(manifest, tombstone), True, snapshot)
        assert reply.snapshot.phase is OutputHandoffPhase.ABORTED

    adapter = node._output_publications = OutputPublicationNodeAdapter(
        journal, register_owner=register_owner, report_complete=report_complete,
        report_rollback=report_rollback, prepare_child=forbidden, promote_child=forbidden,
        release_child=forbidden, seal_replica=node._seal_output_publication_replica,
        drop_replica=node._drop_output_publication_replica,
    )
    return SimpleNamespace(journal=journal, handoffs=handoffs, adapter=adapter)


def _prepare_one_output(node, request, grant, publication, payload, *, stored):
    assert len(request.return_ids) == 1 and len(payload) <= 64
    identity = OutputPublicationID(request.lease_id, (TaskExecution(request.attempt_id)))
    header = OutputPublicationHeader(
        identity, JobID.random(), grant.worker_id, request.requester_worker_id,
        OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
    )
    manifest = OutputPublicationManifest.create(header, (OutputValue(protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE, len(payload), hashlib.sha256(payload).hexdigest())))
    before = node.resource_ledger.snapshot()
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(manifest, (payload,)))
    assert prepared.accepted and node._leases[request.lease_id].output_publication_id == identity
    assert publication.journal.snapshot(identity).ready_to_complete
    assert publication.handoffs.query(identity).manifest == manifest
    assert publication.journal.snapshot(identity).complete is None
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.RUNNING
    assert node.resource_ledger.snapshot() == before and node._ledger.release_calls == 0
    return manifest


@pytest.mark.unit
def test_outcome_protocol_and_handler_require_full_identity() -> None:
    node, request, grant, object_id = _node_fixture()
    exact = _query(request, grant, object_id)
    reply = node._handle_get_worker_lease_outcome(exact)
    assert reply.found and reply.state is protocol.LeaseExecutionState.GRANTED
    assert reply.worker_alive and reply.descriptors == ()
    assert (reply.lease_id, reply.task_id, reply.attempt_id) == (
        request.lease_id, request.task_id, request.attempt_id
    )

    wrong = protocol.GetWorkerLeaseOutcome(
        request.lease_id, request.task_id, request.attempt_id, WorkerID.random(),
        request.requester_worker_id, (object_id,),
    )
    rejected = node._handle_get_worker_lease_outcome(wrong)
    assert not rejected.found and rejected.error and rejected.state is None
    other_job = JobID.random()
    other_task = TaskID.derive(
        other_job, TaskID.for_driver(other_job), 9
    )
    mismatches = (
        protocol.GetWorkerLeaseOutcome(
            LeaseID.random(), request.task_id, request.attempt_id, grant.worker_id,
            request.requester_worker_id, (object_id,),
        ),
        protocol.GetWorkerLeaseOutcome(
            request.lease_id, other_task, AttemptID(other_task, 0), grant.worker_id,
            request.requester_worker_id, (ObjectID.for_task(other_task),),
        ),
        protocol.GetWorkerLeaseOutcome(
            request.lease_id, request.task_id, request.attempt_id.next(),
            grant.worker_id, request.requester_worker_id, (object_id,),
        ),
        protocol.GetWorkerLeaseOutcome(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
            WorkerID.random(), (object_id,),
        ),
    )
    for mismatch in mismatches:
        candidate = node._handle_get_worker_lease_outcome(mismatch)
        assert not candidate.found and candidate.state is None
    empty_manifest = protocol.GetWorkerLeaseOutcome(
        request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
        request.requester_worker_id, (),
    )
    rejected_manifest = node._handle_get_worker_lease_outcome(empty_manifest)
    assert not rejected_manifest.found
    assert "return manifest" in (rejected_manifest.error or "")
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.GRANTED
    assert node._ledger.release_calls == 0

    with pytest.raises(ProtocolError, match="unique"):
        protocol.GetWorkerLeaseOutcome(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
            request.requester_worker_id, (object_id, object_id),
        )
    with pytest.raises(ProtocolError, match="successful COMPLETED"):
        protocol.GetWorkerLeaseOutcomeReply(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
            request.requester_worker_id, (object_id,), node.node_id, True, True,
            state=protocol.LeaseExecutionState.RUNNING,
            descriptors=(protocol.ObjectStoreDescriptor(
                object_id, request.requester_worker_id, request.attempt_id,
                node.node_id, 1, hashlib.sha256(b"x").hexdigest(),
            ),),
        )


@pytest.mark.unit
def test_completed_outcome_survives_death_and_returns_only_matching_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, object_id = _node_fixture()
    payload = b"stored"
    checksum = hashlib.sha256(payload).hexdigest()
    publication = _attach_output_publication(node)
    assert node._handle_start_worker_lease(_start(request, grant)).accepted
    manifest = _prepare_one_output(node, request, grant, publication, payload, stored=True)
    identity = manifest.publication_id
    assert node._object_store.get(object_id) == payload
    assert node._sealed_metadata[object_id] == (
        request.attempt_id, request.requester_worker_id, len(payload), checksum,
    )
    running = node._handle_get_worker_lease_outcome(_query(request, grant, object_id))
    assert running.descriptors == ()
    assert running.output_publication is running.output_completion is None
    completed = node._handle_complete_worker_lease(_complete(request, grant))
    assert completed.accepted and completed.released
    assert completed.output_publication.manifest == manifest
    assert completed.output_publication.complete == OutputPublicationCompleteWitness.for_manifest(manifest)
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._ledger.release_calls == 1

    old_process = node._workers[grant.worker_id].process
    assert old_process is not None
    old_process.alive = False
    old_process.exitcode = 23
    replacement_id = WorkerID.random()
    replacement = _Process(7102, alive=True)
    monkeypatch.setattr(node, "_fresh_worker_id_locked", lambda: replacement_id)
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda worker_id: (replacement, ("127.0.0.1", 27102)),
    )
    assert node._handle_unexpected_worker_exit(grant.worker_id, old_process)

    reply = node._handle_get_worker_lease_outcome(_query(request, grant, object_id))
    assert reply.found and not reply.worker_alive
    assert reply.state is protocol.LeaseExecutionState.COMPLETED
    assert reply.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.output_publication == completed.output_publication
    assert reply.output_completion is None and not reply.cleanup_pending
    assert publication.journal.snapshot(identity).complete == completed.output_publication.complete
    assert publication.handoffs.query(identity).complete == completed.output_publication.complete
    assert reply.descriptors == (protocol.ObjectStoreDescriptor(
        object_id, request.requester_worker_id, request.attempt_id, node.node_id,
        len(payload), checksum,
    ),)
    assert node._worker_order == (replacement_id,)
    assert grant.worker_id not in node._workers
    assert node._idle_worker_slot_locked().worker_id == replacement_id
    before_replay = node.resource_ledger.snapshot()
    assert node._handle_get_worker_lease_outcome(_query(request, grant, object_id)) == reply
    assert node.resource_ledger.snapshot() == before_replay and node._ledger.release_calls == 1
    assert node._object_store.get(object_id) == payload


@pytest.mark.unit
def test_sealed_output_before_worker_loss_is_reported_only_as_orphan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, object_id = _node_fixture()
    payload = b"stored"
    checksum = hashlib.sha256(payload).hexdigest()
    sealed = node._handle_seal_object(
        protocol.SealObject.from_data(
            object_id, request.attempt_id, request.requester_worker_id, payload
        )
    )
    assert sealed.sealed
    node._handle_start_worker_lease(_start(request, grant))
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    node._shutdown_request_id = "draining"
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda _worker: (_ for _ in ()).throw(
            AssertionError("replacement is fenced while draining")
        ),
    )
    node._handle_unexpected_worker_exit(grant.worker_id, process)

    reply = node._handle_get_worker_lease_outcome(
        _query(request, grant, object_id)
    )
    descriptor = protocol.ObjectStoreDescriptor(
        object_id, request.requester_worker_id, request.attempt_id,
        node.node_id, len(payload), checksum,
    )
    assert reply.state is protocol.LeaseExecutionState.WORKER_LOST
    assert reply.descriptors == ()
    assert reply.orphan_descriptors == (descriptor,)

    drop = protocol.DropObjectReplica(
        object_id, request.attempt_id, request.requester_worker_id,
        node.node_id, checksum,
    )
    dropped = node._handle_drop_object_replica(drop)
    assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
    old_after_drop = node._handle_seal_object(
        protocol.SealObject.from_data(
            object_id, request.attempt_id, request.requester_worker_id, payload
        )
    )
    assert not old_after_drop.sealed
    assert "fenced by replica deletion" in (old_after_drop.error or "")
    next_seal = node._handle_seal_object(
        protocol.SealObject.from_data(
            object_id, request.attempt_id.next(),
            request.requester_worker_id, b"retry",
        )
    )
    assert next_seal.sealed
    stale = node._handle_seal_object(
        protocol.SealObject.from_data(
            object_id, request.attempt_id, request.requester_worker_id, payload
        )
    )
    assert not stale.sealed
    # The exact deletion watermark remains authoritative even after a newer
    # attempt has sealed. Reject the old producer before comparing new bytes.
    assert "fenced by replica deletion" in (stale.error or "")
    assert node._object_store.get(object_id) == b"retry"
    assert node._sealed_metadata[object_id] == (
        request.attempt_id.next(), request.requester_worker_id, len(b"retry"),
        hashlib.sha256(b"retry").hexdigest(),
    )
    assert node._dropped_metadata[object_id] == (
        request.attempt_id, request.requester_worker_id, checksum,
    )


@pytest.mark.unit
def test_outcome_reply_rejects_orphan_overlap_extra_and_reordering() -> None:
    node, request, grant, object_id = _node_fixture()
    first = protocol.ObjectStoreDescriptor(object_id, request.requester_worker_id,
        request.attempt_id, node.node_id, 1, hashlib.sha256(b"a").hexdigest())
    values = dict(lease_id=request.lease_id, task_id=request.task_id,
        attempt_id=request.attempt_id, executor_worker_id=grant.worker_id,
        owner_worker_id=request.requester_worker_id, object_ids=(object_id,),
        node_id=node.node_id, found=True, worker_alive=False,
        state=protocol.LeaseExecutionState.WORKER_LOST)
    with pytest.raises(ProtocolError, match="disjoint"):
        protocol.GetWorkerLeaseOutcomeReply(**values, descriptors=(first,), orphan_descriptors=(first,))
    other_task = TaskID.random()
    extra = protocol.ObjectStoreDescriptor(ObjectID.for_task(other_task), request.requester_worker_id,
        AttemptID(other_task, 0), node.node_id, 1, hashlib.sha256(b"c").hexdigest())
    with pytest.raises(ProtocolError, match="was not requested"):
        protocol.GetWorkerLeaseOutcomeReply(**values, orphan_descriptors=(extra,))
    # Two-output ordering is retired. Duplicate entries for the one output
    # must still fail; neither an empty observation nor one orphan is success.
    with pytest.raises(ProtocolError, match="unique objects"):
        protocol.GetWorkerLeaseOutcomeReply(**values, orphan_descriptors=(first, first))
    empty = protocol.GetWorkerLeaseOutcomeReply(**values)
    assert empty.orphan_descriptors == () and empty.completion_status is None
    present = protocol.GetWorkerLeaseOutcomeReply(**values, orphan_descriptors=(first,))
    assert present.orphan_descriptors == (first,) and present.completion_status is None


@pytest.mark.unit
def test_node_rejects_outcome_manifest_subset_extra_and_reordering() -> None:
    node, request, grant, first_id = _node_fixture()
    # The supported empty probe is structurally valid but cannot match the
    # actual lease's one-output manifest at the Node authority.
    empty = protocol.GetWorkerLeaseOutcome(request.lease_id, request.task_id,
        request.attempt_id, grant.worker_id, request.requester_worker_id, ())
    assert not node._handle_get_worker_lease_outcome(empty).found
    # Multi-output/reordered manifests are rejected by the wire boundary,
    # before they can reach a Node whose lease is still unchanged.
    for manifest in ((first_id, first_id), (ObjectID(request.task_id, 1),),
                     (first_id, ObjectID(request.task_id, 1))):
        with pytest.raises(ProtocolError):
            protocol.GetWorkerLeaseOutcome(request.lease_id, request.task_id,
                request.attempt_id, grant.worker_id, request.requester_worker_id, manifest)
    assert node._leases[request.lease_id].request == request


@pytest.mark.unit
@pytest.mark.parametrize("complete_first", [True, False])
def test_death_and_complete_have_one_terminal_winner(
    complete_first: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, object_id = _node_fixture()
    publication = _attach_output_publication(node)
    assert node._handle_start_worker_lease(_start(request, grant)).accepted
    manifest = _prepare_one_output(node, request, grant, publication, b"winner", stored=False)
    identity = manifest.publication_id
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    monkeypatch.setattr(node, "_spawn_worker_process", lambda _worker: (_ for _ in ()).throw(AssertionError("no replacement after fence")))
    node._shutdown_request_id = "draining"
    if complete_first:
        completed = node._handle_complete_worker_lease(_complete(request, grant))
        assert completed.accepted and completed.released
        assert completed.output_publication.manifest == manifest
        node._handle_unexpected_worker_exit(grant.worker_id, process)
        expected = protocol.LeaseExecutionState.COMPLETED
        assert publication.journal.snapshot(identity).complete == completed.output_publication.complete
    else:
        node._handle_unexpected_worker_exit(grant.worker_id, process)
        late = node._handle_complete_worker_lease(_complete(request, grant))
        assert not late.accepted and not late.released
        expected = protocol.LeaseExecutionState.WORKER_LOST
        assert publication.journal.snapshot(identity).complete is None
        assert publication.adapter.rollback_reported(identity)
        assert publication.handoffs.query(identity).phase is OutputHandoffPhase.ABORTED
    assert node._leases[request.lease_id].state is expected
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._ledger.release_calls == 1
    assert node._handle_get_worker_lease_outcome(
        _query(request, grant, object_id)
    ).state is expected


@pytest.mark.unit
def test_replacement_is_fresh_and_begin_drain_prevents_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, _object_id = _node_fixture()
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    fresh = WorkerID.random()
    replacement = _Process(7102, alive=True)
    monkeypatch.setattr(node, "_fresh_worker_id_locked", lambda: fresh)
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda worker_id: (replacement, ("127.0.0.1", 27102)),
    )
    assert node._handle_unexpected_worker_exit(grant.worker_id, process)
    assert fresh != grant.worker_id and node._worker_order == (fresh,)
    assert not node._handle_unexpected_worker_exit(grant.worker_id, process)

    second, request2, grant2, _ = _node_fixture()
    process2 = second._workers[grant2.worker_id].process
    assert process2 is not None
    process2.alive = False
    process2.exitcode = 23
    second._install_worker_drain_fence("epoch")
    monkeypatch.setattr(
        second, "_spawn_worker_process",
        lambda _worker: (_ for _ in ()).throw(AssertionError("replacement forbidden")),
    )
    # BeginDrain synchronously reaps a child the supervisor had not observed.
    assert not second._handle_unexpected_worker_exit(grant2.worker_id, process2)
    assert second._worker_order == (grant2.worker_id,)
    assert second._workers[grant2.worker_id].process is None
    assert second._leases[request2.lease_id].state is (
        protocol.LeaseExecutionState.WORKER_LOST
    )
    drained_query = second._handle_get_worker_lease_outcome(
        _query(request2, grant2, ObjectID.for_task(request2.task_id))
    )
    assert drained_query.found and not drained_query.worker_alive


@pytest.mark.heavy
def test_replacement_failures_balance_counter_and_supervisor_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, _request, grant, _object_id = _node_fixture()
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23

    # Fresh-ID selection is part of the replacement transaction.  Its failure
    # must leave a retryable vacant slot without leaking the shutdown-visible
    # inflight count or escaping the death handler.
    monkeypatch.setattr(
        node,
        "_fresh_worker_id_locked",
        lambda: (_ for _ in ()).throw(RuntimeError("ID source unavailable")),
    )
    assert node._handle_unexpected_worker_exit(grant.worker_id, process)
    vacant = node._workers[grant.worker_id]
    assert vacant.process is None
    assert vacant.replacement_error == "RuntimeError: ID source unavailable"
    assert node._worker_replacements_inflight == 0

    replacement_id = WorkerID.random()
    replacement = _Process(7102, alive=True)
    spawn_calls = 0

    def spawn(worker_id: WorkerID):
        nonlocal spawn_calls
        assert worker_id == replacement_id
        spawn_calls += 1
        if spawn_calls == 1:
            raise RuntimeError("transient spawn failure")
        return replacement, ("127.0.0.1", 27102)

    published = threading.Event()

    def emit(name: str, **_attributes: object) -> None:
        if name == "worker_replaced":
            published.set()

    vacant.replacement_retry_after = 0.0
    monkeypatch.setattr(node, "_fresh_worker_id_locked", lambda: replacement_id)
    monkeypatch.setattr(node, "_spawn_worker_process", spawn)
    monkeypatch.setattr(node, "_emit", emit)

    node._start_worker_supervisor()
    try:
        assert published.wait(1.0), "supervisor did not retry the vacant slot"
    finally:
        node._stop_worker_supervisor()

    assert spawn_calls == 2
    assert node._worker_supervisor_thread is None
    assert node._worker_replacements_inflight == 0
    assert node._worker_order == (replacement_id,)
    assert node._workers[replacement_id].process is replacement
    assert grant.worker_id not in node._workers


@pytest.mark.unit
def test_begin_drain_fences_vacant_replacement_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, _object_id = _node_fixture()
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    monkeypatch.setattr(
        node,
        "_fresh_worker_id_locked",
        lambda: (_ for _ in ()).throw(RuntimeError("ID source unavailable")),
    )
    assert node._handle_unexpected_worker_exit(grant.worker_id, process)
    assert node._worker_replacements_inflight == 0

    monkeypatch.setattr(
        node,
        "_spawn_worker_process",
        lambda _worker: (_ for _ in ()).throw(
            AssertionError("BeginDrain must not retry a vacant slot")
        ),
    )
    node._install_worker_drain_fence("vacant-before-drain")

    assert node._worker_supervisor_stop.is_set()
    assert node._worker_replacements_inflight == 0
    assert node._worker_order == (grant.worker_id,)
    assert node._workers[grant.worker_id].process is None
    assert node._leases[request.lease_id].state is (
        protocol.LeaseExecutionState.WORKER_LOST
    )


@pytest.mark.unit
def test_begin_drain_reaps_child_that_died_before_supervisor_observed_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, object_id = _node_fixture()
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda _worker: (_ for _ in ()).throw(
            AssertionError("BeginDrain must not replace a dead Worker")
        ),
    )

    node._install_worker_drain_fence("dead-before-drain")

    assert process.closed
    assert node._workers[grant.worker_id].process is None
    assert node._leases[request.lease_id].state is (
        protocol.LeaseExecutionState.WORKER_LOST
    )
    outcome = node._handle_get_worker_lease_outcome(
        _query(request, grant, object_id)
    )
    assert outcome.found and not outcome.worker_alive
    assert outcome.state is protocol.LeaseExecutionState.WORKER_LOST
    assert node.resource_ledger.available == node.resource_ledger.total


@pytest.mark.unit
@pytest.mark.parametrize("started", [False, True])
def test_granted_or_running_child_death_reclaims_once(
    started: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, object_id = _node_fixture()
    if started:
        assert node._handle_start_worker_lease(_start(request, grant)).accepted
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    node._shutdown_request_id = "fence"
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda _worker: (_ for _ in ()).throw(AssertionError("no replacement")),
    )
    assert node._handle_unexpected_worker_exit(grant.worker_id, process)
    assert not node._handle_unexpected_worker_exit(grant.worker_id, process)
    assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.WORKER_LOST
    assert node._ledger.release_calls == 1
    assert node._handle_get_worker_lease_outcome(
        _query(request, grant, object_id)
    ).state is protocol.LeaseExecutionState.WORKER_LOST


@pytest.mark.heavy
def test_concurrent_complete_and_detector_release_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, _ = _node_fixture()
    node._handle_start_worker_lease(_start(request, grant))
    process = node._workers[grant.worker_id].process
    assert process is not None
    process.alive = False
    process.exitcode = 23
    node._shutdown_request_id = "fence"
    monkeypatch.setattr(
        node, "_spawn_worker_process",
        lambda _worker: (_ for _ in ()).throw(AssertionError("no replacement")),
    )
    gate = threading.Barrier(3)
    complete_replies: list[protocol.CompleteWorkerLeaseReply] = []

    def complete() -> None:
        gate.wait()
        complete_replies.append(
            node._handle_complete_worker_lease(_complete(request, grant))
        )

    def detect() -> None:
        gate.wait()
        node._handle_unexpected_worker_exit(grant.worker_id, process)

    threads = (threading.Thread(target=complete), threading.Thread(target=detect))
    for thread in threads:
        thread.start()
    gate.wait()
    for thread in threads:
        thread.join(1.0)
        assert not thread.is_alive()
    state = node._leases[request.lease_id].state
    assert state in (
        protocol.LeaseExecutionState.COMPLETED,
        protocol.LeaseExecutionState.WORKER_LOST,
    )
    assert node._ledger.release_calls == 1
    assert complete_replies[0].accepted is (
        state is protocol.LeaseExecutionState.COMPLETED
    )


class _PatchedExit(BaseException):
    pass


@pytest.mark.unit
def test_crash_failpoint_exits_after_complete_before_task_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, object_id = _node_fixture()
    publication = _attach_output_publication(node)
    # The fake process is still inert. Registration only makes the real Start
    # handler echo this fixture's declared Node incarnation.
    node._registered_with_gcs = True
    node_id, worker_id, owner = node.node_id, grant.worker_id, request.requester_worker_id
    job = JobID.random()
    task, attempt = request.task_id, request.attempt_id
    executions = []

    def produce():
        executions.append(True)
        return b"stored"

    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job, __name__, "crash_value", "v1"),
        cloudpickle.dumps(produce),
    )
    spec = protocol.TaskSpec(
        job, task, attempt, definition.key, (), 1, request.resources, owner,
        function_definition=definition, max_retries=1,
    )
    push = protocol.PushTask(request.lease_id, worker_id, spec)
    worker = object.__new__(WorkerServer)
    initialize_worker_protocol(worker)
    worker.worker_id = worker_id
    worker.node_id = node_id
    worker.node_address = ("127.0.0.1", 27001)
    worker.inline_threshold = 0
    worker._execution_lock = threading.Lock()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._failpoint = WorkerFailpointConfig(mode=WorkerFailpointMode.CRASH)
    worker._failpoint_triggers = 0
    worker._crash_after_complete = set()
    worker._worker_core_enabled = False
    worker.event_sink = None
    events: list[str] = []
    prepared_manifests, completion_replies = [], []
    real_loads = cloudpickle.loads

    def rpc(address, handler, message):
        assert address == worker.node_address and len(events) < 3
        events.append(handler)
        if handler == START_WORKER_LEASE_HANDLER:
            started = node._handle_start_worker_lease(message)
            assert started.accepted and started.node_incarnation == OutputPublicationNodeIncarnation(
                node_id, node._node_pid, node._registration_epoch,
            )
            return started
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            assert not prepared_manifests
            prepared_manifests.append(message.manifest)
            before = node.resource_ledger.snapshot()
            prepared = node._handle_prepare_output_publication(message)
            identity = message.manifest.publication_id
            assert prepared.accepted and publication.journal.snapshot(identity).ready_to_complete
            assert publication.handoffs.query(identity).manifest == message.manifest
            assert publication.journal.snapshot(identity).complete is None
            assert node.resource_ledger.snapshot() == before and node._ledger.release_calls == 0
            return prepared
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        assert not completion_replies and message == _complete(request, grant)
        completed = node._handle_complete_worker_lease(message)
        assert completed.accepted and completed.released
        assert completed.output_publication.manifest == prepared_manifests[0]
        assert node._ledger.release_calls == 1 and node.resource_ledger.available == node.resource_ledger.total
        completion_replies.append(completed)
        return completed

    exits: list[int] = []

    def exit_after_complete(code):
        assert completion_replies and not exits
        envelope = completion_replies[0].output_publication
        key = (attempt, push.lease_id)
        assert node._leases[request.lease_id].state is protocol.LeaseExecutionState.COMPLETED
        assert publication.journal.snapshot(envelope.publication_id).complete == envelope.complete
        assert worker._replies[key].output_publication == envelope and key in worker._completion_acked
        assert worker._cached_pushes[key] == push and key not in worker._prepared_output_replies
        assert node._ledger.release_calls == 1 and executions == [True]
        events.append("exit")
        exits.append(code)
        raise _PatchedExit()

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    monkeypatch.setattr("miniray.worker.cloudpickle.loads", lambda _payload: produce)
    monkeypatch.setattr("miniray.worker.os._exit", exit_after_complete)
    # A real os._exit cannot unwind Python. This original BaseException stub
    # can, and the publication boundary preserves it as the chained cause.
    with pytest.raises(RuntimeError, match="discovered output custody") as captured:
        worker._handle_push_task(push)
    assert isinstance(captured.value.__cause__, _PatchedExit)
    assert events == [
        START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
        COMPLETE_WORKER_LEASE_HANDLER, "exit",
    ]
    assert exits == [23] and worker._failpoint_triggers == 1
    key = (attempt, push.lease_id)
    assert worker._replies[key].status is protocol.TaskReplyStatus.SUCCEEDED
    assert key in worker._completion_acked
    envelope = worker._replies[key].output_publication
    assert envelope == completion_replies[0].output_publication
    assert (envelope.manifest.value).tier is protocol.ResultStorage.OBJECT_STORE
    assert (envelope.manifest.publication_id).object_id == object_id
    assert envelope.complete == OutputPublicationCompleteWitness.for_manifest(envelope.manifest)
    assert real_loads(node._object_store.get(object_id)) == b"stored"
    assert node._object_store.used_bytes < 64
    replay = worker._handle_push_task(push)
    assert replay is worker._replies[key] and replay.output_publication == envelope
    assert executions == [True] and exits == [23] and len(events) == 4
    assert node._ledger.release_calls == 1

    assert WorkerFailpointConfig().mode is WorkerFailpointMode.SYSTEM_ERROR
    with pytest.raises(ValueError, match="mode"):
        WorkerFailpointConfig(mode="unknown")
    probe = object.__new__(WorkerServer)
    probe._failpoint = WorkerFailpointConfig(mode=WorkerFailpointMode.CRASH)
    probe._failpoint_triggers = 0
    assert probe._claim_failpoint(AttemptID(task, 1)) is None
    assert probe._failpoint_triggers == 0
