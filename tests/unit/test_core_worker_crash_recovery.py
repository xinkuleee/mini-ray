"""Worker crash contracts with explicitly guarded unified-output cases.

Function-level unit marks identify threadless reducers and fake RPCs. The
original dependency-hold case now uses canonical submission on a pure Core,
one unstarted Node with two passive Worker slots, and one empty 1 KiB store.
Two tiny single-output publications are real. A lost Worker is a declared
process-state input to the real Node reclaim reducer, not a real process exit.
One delayed Push and one retry are driven explicitly; no user code, runtime
thread, process, socket, timer, wait or public shutdown runs in that case.
All cases use accepted single-output Core state. Legacy graph/GCS publication
transactions and the two-result partial-delete dimension are retired.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import fields, is_dataclass, replace
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import core as core_module, node as node_module, output_protocol as wire, protocol
from miniray.core import (
    CoreWorker, _DelayedReadyTask, _ObjectWaiter, _PendingTask, _PushRequestState,
    _ReadyTask, _RetryInlineGc, _WAKE_COORDINATOR, _lineage_hold_token,
)
from miniray.errors import TaskError, WorkerDiedError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseRecord, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import (
    OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.resources import AllocationToken, HybridPolicy, NodeSnapshot, ResourceLedger, ResourceVector
from miniray.transport import RemoteCallError, TransportConnectionError, TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core


@pytest.fixture(autouse=True)
def _no_output_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("unified crash-recovery case attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start"),
                         (NodeServer, "__init__")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr("miniray.transport.TCPServer.__init__", forbidden)


def _fixture(max_retries: int = 1):
    core = make_pure_core()
    core._registered_functions = set()
    task = TaskID.derive(core.job_id, core.driver_task_id, 0)
    attempt = AttemptID(task, 0)
    object_id = ObjectID.for_task(task)
    key = protocol.FunctionKey(core.job_id, "tests", "crash", "v1")
    definition = protocol.FunctionDefinition.from_payload(key, b"function")
    spec = protocol.TaskSpec(
        core.job_id, task, attempt, key, (), 1, ResourceVector({"CPU": 1}),
        core.worker_id, function_definition=definition, max_retries=max_retries,
    )
    pending = _PendingTask(object_id, spec)
    core._owner_table.register_task_outputs(spec)
    core._recovery.register_task(spec, output_ids=(object_id,), max_retries=max_retries)
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    with core._state_lock:
        core._install_task_finish_barrier_locked(pending)
        core._accepted_task_count += 1
        core._enqueue_reconstruction_task(pending)
    assert core._submissions.get_nowait() is pending
    core._submissions.task_done()
    core._resolve_node_address = lambda node_id, *, home_route=None: core.node_address if node_id == core.node_id else pytest.fail("unknown Node")
    worker = WorkerID.random()
    grant = protocol.GrantWorkerLease(
        LeaseID.random(), task, attempt, core.node_id, worker,
        ("127.0.0.1", 28002), AllocationToken("crash-grant"),
    )
    push = protocol.PushTask(grant.lease_id, worker, spec)
    state = _PushRequestState(
        push, grant, core.node_address, grant.worker_address, 1, True
    )
    core._mark_protocol_unresolved(pending, "push_replay_wait", output_candidate=OutputPublicationID(grant.lease_id, pending.execution))
    return core, pending, state


def _outcome(
    core, pending, state, lease_state, *, alive=False, status=None,
    descriptors=(), orphan_descriptors=(), output_publication=None,
    output_completion=None, cleanup_pending=False,
):
    return protocol.GetWorkerLeaseOutcomeReply(
        state.push.lease_id, pending.spec.task_id, pending.spec.attempt_id,
        state.push.worker_id, core.worker_id, pending.spec.return_ids(),
        state.grant.node_id, True, alive, state=lease_state,
        completion_status=status, descriptors=descriptors,
        orphan_descriptors=orphan_descriptors,
        output_publication=output_publication, output_completion=output_completion,
        cleanup_pending=cleanup_pending,
    )


def _assert_output_metadata(value):
    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (
        bytes, bytearray, memoryview, protocol.ResultDescriptor,
        protocol.ObjectStoreDescriptor, OutputPublicationEnvelope,
    )), "recovery metadata cannot supply result bytes"
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            assert item.name not in ("inline_data", "payload", "slot_payloads", "envelope", "descriptor")
            _assert_output_metadata(getattr(value, item.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_output_metadata(item)


def _completed_output(core, pending, state, *, inline=False):
    """One real discovery/prepare/Complete, not a descriptor-to-envelope shim."""
    reductions, seals, completions, calls = [], [], [], []

    class OnceResult:
        def __reduce__(self):
            reductions.append(True)
            return bytes, (b"stored",)

    identity = OutputPublicationID(state.push.lease_id, pending.execution)
    header = OutputPublicationHeader(
        identity, core.job_id, state.push.worker_id, core.worker_id,
        OutputPublicationNodeIncarnation(core.node_id, 21001, 3),
    )
    discovery = OutputDiscoverySession(header, inline_threshold=1024 if inline else 0)
    outputs = discovery.discover((OnceResult()))
    journal, store = OutputPublicationJournal(), ObjectStore(1024)
    ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    ledger.allocate(pending.spec.resources, state.grant.allocation_token)
    node = object.__new__(NodeServer)
    node.node_id, node._node_pid, node._registration_epoch = core.node_id, 21001, 3
    node._state_lock = threading.RLock()
    node._object_store, node._object_manager = store, ObjectManager(node.node_id, store)
    node._sealed_metadata, node._dropped_metadata = {}, {}
    node._local_replica_write_claims, node._object_localization_locks = {}, {}
    node._owner_death_fences, node._actor_workers = {}, {}
    node._output_publication_journal, node._ledger = journal, ledger
    node._cluster_nodes = (NodeSnapshot(node.node_id, ledger.total, ResourceVector()),)
    node._cluster_addresses = {}
    node._gcs_address, node._registered_with_gcs = None, False
    node._resource_report_version = node._resource_reported_version = 0
    node._dependency_pin_cleanups, node._pinned_transfers = {}, {}
    node._stop_event, node._shutdown_request_id, node.event_sink = threading.Event(), None, None
    request = protocol.RequestWorkerLease(
        state.grant.lease_id, pending.task_id, pending.spec.attempt_id, pending.spec.resources,
        node.node_id, core.worker_id, return_ids=pending.output_ids, requester_owner_address=core.owner_address,
    )
    node._leases = {state.grant.lease_id: _LeaseRecord(
        request, state.grant.allocation_token, state.grant, state=protocol.LeaseExecutionState.RUNNING,
    )}
    node._workers = {state.push.worker_id: _WorkerSlot(
        state.push.worker_id, process=SimpleNamespace(is_alive=lambda: True),
        address=state.grant.worker_address, pid=21002, active_lease_id=state.grant.lease_id,
    )}
    node._worker_order = (state.push.worker_id,)
    def background_rpc(address, handler, request):
        assert address == core.owner_address
        _assert_output_metadata(request)
        methods = {
            wire.REGISTER_OUTPUT_HANDOFF_HANDLER: core.register_output_handoff,
            wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: core.report_output_handoff_complete,
            wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: core.report_output_handoff_rollback,
        }
        assert handler in methods
        reply = methods[handler](request)
        assert reply.accepted and reply.request == request
        _assert_output_metadata(reply)
        return reply
    node._background_rpc = background_rpc
    adapter = node._output_publications = node._make_output_publication_adapter()
    original_seal = adapter._seal_replica
    def seal(effect, descriptor, payload):
        result = original_seal(effect, descriptor, payload)
        seals.append(effect)
        return result
    adapter._seal_replica = seal
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(outputs.manifest, (outputs.payload))).accepted
    discovery.release_sources_after_promotions()
    completion = protocol.CompleteWorkerLease(state.grant.lease_id, pending.task_id, pending.spec.attempt_id,
                                               state.push.worker_id, protocol.TaskReplyStatus.SUCCEEDED)
    complete = node._handle_complete_worker_lease_inner(completion)
    assert complete.accepted and complete.released
    envelope = complete.output_publication
    completions.append(envelope.complete)
    assert envelope.manifest == outputs.manifest
    assert len(((envelope.result,))) == 1 and reductions == [True]
    assert core._output_handoff_table().query(identity).complete is None
    descriptors = tuple(protocol.ObjectStoreDescriptor(
        result.object_id, result.owner_worker_id, pending.spec.attempt_id,
        result.node_id, result.size_bytes, result.checksum,
    ) for result in ((envelope.result,)) if result.storage is protocol.ResultStorage.OBJECT_STORE)
    def rpc(address, handler, request):
        calls.append((handler, request))
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert address == core.node_address and request.proof.complete == envelope.complete
        return node._handle_ack_output_publication_adopted(request)
    return SimpleNamespace(
        outputs=outputs, envelope=envelope, descriptors=descriptors, journal=journal,
        handoffs=core._output_handoff_table(), store=store, ledger=ledger, reductions=reductions,
        seals=seals, completions=completions, calls=calls, rpc=rpc, node=node,
    )


def _orphan_descriptor(core, pending):
    return protocol.ObjectStoreDescriptor(
        pending.object_id, core.worker_id, pending.spec.attempt_id, core.node_id,
        6, hashlib.sha256(b"stored").hexdigest(),
    )


def _orphan_store(core, descriptor):
    node = object.__new__(NodeServer)
    node.node_id = core.node_id
    node._state_lock = threading.RLock()
    node._object_store = ObjectStore(1024)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._sealed_metadata = {descriptor.object_id: (
        descriptor.producer_attempt_id, descriptor.owner_worker_id, descriptor.size_bytes, descriptor.checksum,
    )}
    node._dropped_metadata, node._object_localization_locks = {}, {}
    node._local_replica_write_claims, node._owner_death_fences = {}, {}
    node._pinned_transfers = {}
    node._object_store.put(descriptor.object_id, b"stored")
    return node


def _connect_fails(*_args):
    raise TransportConnectionError("old worker is gone")


def _next_pending(core: CoreWorker) -> _PendingTask:
    values = _dependency_queue(core)
    assert len(values) == 1 and isinstance(values[0], _PendingTask)
    return values[0]


@pytest.mark.unit
def test_worker_lost_outcome_retries_with_new_attempt_and_stable_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture(max_retries=1)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    monkeypatch.setattr(
        core, "_rpc",
        lambda *_args: _outcome(
            core, pending, state, protocol.LeaseExecutionState.WORKER_LOST
        ),
    )
    assert not core._replay_push(pending, state)
    retried = _next_pending(core)
    assert retried.object_id == pending.object_id
    assert retried.spec.task_id == pending.spec.task_id
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
    assert pending.task_id not in core._protocol_unresolved
    stale = protocol.TaskReply(
        pending.spec.task_id, pending.spec.attempt_id, state.push.worker_id,
        protocol.TaskReplyStatus.SYSTEM_ERROR, error=protocol.RemoteErrorInfo(
            "WorkerDiedError", "late"
        ),
    )
    assert not core._publish_reply(pending, stale)


@pytest.mark.unit
def test_worker_lost_orphan_is_persisted_dropped_then_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture(max_retries=1)
    descriptor = _orphan_descriptor(core, pending)
    orphan = _orphan_store(core, descriptor)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    calls = []

    def rpc(_address, handler, message):
        calls.append((handler, message, core._protocol_unresolved[pending.task_id]))
        if handler == "get_worker_lease_outcome":
            return _outcome(
                core, pending, state,
                protocol.LeaseExecutionState.WORKER_LOST,
                orphan_descriptors=(descriptor,),
            )
        assert handler == "drop_object_replica"
        assert calls[-1][2].phase == "orphan_cleanup_send"
        assert calls[-1][2].obligation.drops == (message,)
        assert core._recovery.task_record(
            pending.spec.task_id
        ).current_attempt == pending.spec.attempt_id
        return orphan._handle_drop_object_replica(message)

    monkeypatch.setattr(core, "_rpc", rpc)
    assert not core._replay_push(pending, state)
    retried = _next_pending(core)
    assert orphan._object_store.used_bytes == 0
    drop = calls[1][1]
    assert isinstance(drop, protocol.DropObjectReplica)
    assert (
        drop.object_id, drop.producer_attempt_id, drop.owner_worker_id,
        drop.node_id, drop.checksum,
    ) == (
        descriptor.object_id, descriptor.producer_attempt_id,
        descriptor.owner_worker_id, descriptor.node_id, descriptor.checksum,
    )
    assert retried.object_id == pending.object_id
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert pending.task_id not in core._protocol_unresolved


@pytest.mark.parametrize("fault", ("wrong", "pinned"))
@pytest.mark.unit
def test_wrong_or_retryable_drop_ack_keeps_attempt_and_cleanup_obligation(
    monkeypatch: pytest.MonkeyPatch, fault,
) -> None:
    core, pending, state = _fixture(max_retries=1)
    descriptor = _orphan_descriptor(core, pending)
    orphan = _orphan_store(core, descriptor)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    outcomes = []

    def rpc(_address, handler, message):
        if handler == "get_worker_lease_outcome":
            return _outcome(
                core, pending, state,
                protocol.LeaseExecutionState.WORKER_LOST,
                orphan_descriptors=(descriptor,),
            )
        outcomes.append(message)
        if fault == "pinned":
            return protocol.DropObjectReplicaReply(
                message.object_id, message.producer_attempt_id, message.owner_worker_id,
                message.node_id, message.checksum, protocol.DropObjectReplicaStatus.PINNED, "pinned",
            )
        return protocol.DropObjectReplicaReply(
            message.object_id, message.producer_attempt_id,
            message.owner_worker_id, message.node_id,
            hashlib.sha256(b"wrong").hexdigest(),
            protocol.DropObjectReplicaStatus.DROPPED,
        )

    monkeypatch.setattr(core, "_rpc", rpc)
    assert not core._replay_push(pending, state)
    (delayed,) = _dependency_queue(core)
    assert isinstance(delayed, _DelayedReadyTask)
    cleanup = delayed.ready.push_state.orphan_cleanup
    assert cleanup is not None
    assert cleanup.drops == (outcomes[0],)
    assert cleanup.acknowledged == ()
    assert orphan._object_store.get(pending.object_id) == b"stored"
    unresolved = core._protocol_unresolved[pending.task_id]
    assert unresolved.phase == "orphan_cleanup_wait"
    assert unresolved.obligation == cleanup
    assert core._recovery.task_record(
        pending.spec.task_id
    ).current_attempt == pending.spec.attempt_id
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING

    monkeypatch.setattr(core, "_rpc", lambda _a, _h, request: orphan._handle_drop_object_replica(request))
    assert not core._replay_push(pending, delayed.ready.push_state)
    assert _next_pending(core).spec.attempt_id == pending.spec.attempt_id.next()
    assert orphan._object_store.used_bytes == 0


@pytest.mark.unit
def test_application_error_orphan_cleanup_is_terminal_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture(max_retries=3)
    descriptor = _orphan_descriptor(core, pending)
    orphan = _orphan_store(core, descriptor)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)

    def rpc(_address, handler, message):
        if handler == "get_worker_lease_outcome":
            return _outcome(
                core, pending, state,
                protocol.LeaseExecutionState.COMPLETED,
                status=protocol.TaskReplyStatus.APPLICATION_ERROR,
                orphan_descriptors=(descriptor,),
            )
        return orphan._handle_drop_object_replica(message)

    monkeypatch.setattr(core, "_rpc", rpc)
    assert core._replay_push(pending, state)
    assert orphan._object_store.used_bytes == 0
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.ERROR
    assert isinstance(snapshot.error, TaskError)
    assert core._recovery.task_record(
        pending.spec.task_id
    ).current_attempt == pending.spec.attempt_id
    assert _dependency_queue(core) == ()
    assert pending.task_id not in core._protocol_unresolved


@pytest.mark.unit
def test_replacement_rejection_queries_node_instead_of_replaying_old_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new Worker may reuse the dead Worker's former TCP port."""

    core, pending, state = _fixture(max_retries=1)
    monkeypatch.setattr(
        core, "_push_task_rpc",
        lambda *_args: (_ for _ in ()).throw(
            RemoteCallError(
                "push_task", "RuntimeError",
                "PushTask targets a different worker", "",
            )
        ),
    )
    queries = []

    def outcome(_address, handler, request):
        queries.append((handler, request))
        return _outcome(
            core, pending, state, protocol.LeaseExecutionState.WORKER_LOST
        )

    monkeypatch.setattr(core, "_rpc", outcome)

    assert not core._replay_push(pending, state)
    retried = _next_pending(core)
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert len(queries) == 1
    assert queries[0][0] == "get_worker_lease_outcome"


@pytest.mark.unit
def test_worker_lost_budget_zero_publishes_worker_died(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture(max_retries=0)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    monkeypatch.setattr(
        core, "_rpc",
        lambda *_args: _outcome(
            core, pending, state, protocol.LeaseExecutionState.WORKER_LOST
        ),
    )
    assert core._replay_push(pending, state)
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.ERROR
    assert isinstance(snapshot.error, WorkerDiedError)


@pytest.mark.parametrize(
    ("lease_state", "alive"),
    [
        (protocol.LeaseExecutionState.RUNNING, False),
        (protocol.LeaseExecutionState.GRANTED, True),
    ],
)
@pytest.mark.unit
def test_nonterminal_outcome_preserves_exact_push(
    lease_state, alive, monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture()
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    monkeypatch.setattr(
        core, "_rpc",
        lambda *_args: _outcome(
            core, pending, state, lease_state, alive=alive
        ),
    )
    assert not core._replay_push(pending, state)
    (delayed,) = _dependency_queue(core)
    assert isinstance(delayed, _DelayedReadyTask)
    assert delayed.ready.push_state.push is state.push
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING


@pytest.mark.usefixtures("_no_output_runtime")
@pytest.mark.unit
def test_completed_stored_result_publishes_while_worker_remains_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture()
    publication = _completed_output(core, pending, state)
    descriptor = publication.descriptors[0]
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    seen = []

    def query(address, handler, message):
        seen.append((handler, message))
        if handler == "get_worker_lease_outcome":
            assert address == core.node_address
            result = publication.node._handle_get_worker_lease_outcome(message)
            assert result.worker_alive and result.completion_status is protocol.TaskReplyStatus.SUCCEEDED
            assert result.output_publication == publication.envelope
            return result
        return publication.rpc(address, handler, message)

    monkeypatch.setattr(core, "_rpc", query)
    assert core._replay_push(pending, state)
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.READY_STORED
    assert snapshot.current_attempt == pending.spec.attempt_id
    assert snapshot.locations == frozenset({core.node_id})
    assert snapshot.output_publication is not None
    assert snapshot.output_publication.manifest == publication.outputs.manifest
    assert snapshot.output_publication.slot_index == 0
    assert snapshot.output_retirement_id is None
    assert snapshot.canonical_stored_result == (publication.envelope.result)
    assert not snapshot.outgoing_contained_edges
    result = core._stored_descriptors[pending.object_id]
    assert result.inline_data is None and result.checksum == descriptor.checksum
    assert result == (publication.envelope.result)
    assert publication.store.get(pending.object_id) == (publication.outputs.payload)
    assert cloudpickle.loads(publication.store.get(pending.object_id)) == b"stored"
    assert publication.reductions == [True] and len(publication.seals) == 1
    assert publication.completions == [publication.envelope.complete]
    assert publication.ledger.available == ResourceVector({"CPU": 1})
    assert publication.journal.snapshot(publication.envelope.publication_id).retained_result_slots == ()
    assert publication.handoffs.query(publication.envelope.publication_id).adoption is not None
    assert core._recovery.task_record(pending.task_id).retries_started == 0
    assert not core._protocol_unresolved
    assert _dependency_queue(core) == ()
    assert isinstance(seen[0][1], protocol.GetWorkerLeaseOutcome)
    assert [handler for handler, _request in seen] == [
        "get_worker_lease_outcome", wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
    ]


@pytest.mark.usefixtures("_no_output_runtime")
@pytest.mark.parametrize("inline", (False, True))
@pytest.mark.unit
def test_completed_without_local_bytes_and_malformed_query_keep_exact_replay(
    monkeypatch: pytest.MonkeyPatch,
    inline: bool,
) -> None:
    core, pending, state = _fixture()
    publication = _completed_output(core, pending, state, inline=inline)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    monkeypatch.setattr(core, "_retry_system_failure",
                        lambda *_args: pytest.fail("known Complete became ordinary retry"))
    witness_reply = _outcome(
        core, pending, state, protocol.LeaseExecutionState.COMPLETED, alive=True,
        status=protocol.TaskReplyStatus.SUCCEEDED, output_completion=publication.envelope.complete,
    )
    deliver_envelope = [False]
    queries = []

    def rpc(address, handler, message):
        if handler != "get_worker_lease_outcome":
            return publication.rpc(address, handler, message)
        queries.append(message)
        if len(queries) == 1:
            return replace(witness_reply, node_id=NodeID.random())
        if not deliver_envelope[0]:
            return witness_reply
        return _outcome(
            core, pending, state, protocol.LeaseExecutionState.COMPLETED, alive=True,
            status=protocol.TaskReplyStatus.SUCCEEDED, descriptors=publication.descriptors,
            output_publication=publication.envelope,
        )

    monkeypatch.setattr(core, "_rpc", rpc)
    assert not core._replay_push(pending, state)
    (delayed,) = _dependency_queue(core)
    assert isinstance(delayed, _DelayedReadyTask)
    assert pending.task_id in core._protocol_unresolved
    # A matching metadata witness cannot manufacture either INLINE bytes or a
    # STORED owner membership. The live Node, not elapsed time, owns delivery.
    for _ in range(2):
        assert not core._replay_push(pending, delayed.ready.push_state)
        (delayed,) = _dependency_queue(core)
        assert isinstance(delayed, _DelayedReadyTask)
        assert delayed.ready.push_state.push is state.push
        assert delayed.ready.push_state.grant == state.grant
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.PENDING
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert snapshot.inline_data is None and snapshot.output_publication is None
        assert pending.object_id not in core._stored_descriptors
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert pending.task_id in core._protocol_unresolved
        assert not getattr(core, "_output_result_custody", {})
        assert publication.calls == []
    deliver_envelope[0] = True
    assert core._replay_push(pending, delayed.ready.push_state)
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is (ObjectState.READY_INLINE if inline else ObjectState.READY_STORED)
    assert snapshot.current_attempt == pending.spec.attempt_id
    assert snapshot.output_publication.manifest == publication.outputs.manifest
    if inline:
        assert snapshot.inline_data == (publication.envelope.result).inline_data
        assert cloudpickle.loads(snapshot.inline_data) == b"stored"
    else:
        assert core._stored_descriptors[pending.object_id] == (publication.envelope.result)
        assert publication.store.get(pending.object_id) == (publication.outputs.payload)
    assert core._recovery.task_record(pending.task_id).retries_started == 0
    assert not core._protocol_unresolved and _dependency_queue(core) == ()
    assert all(request == queries[0] for request in queries)
    assert publication.reductions == [True] and len(publication.completions) == 1


@pytest.mark.usefixtures("_no_output_runtime")
@pytest.mark.parametrize("lease_state,status,alive", (
    (protocol.LeaseExecutionState.COMPLETED, protocol.TaskReplyStatus.SYSTEM_ERROR, True),
    (protocol.LeaseExecutionState.WORKER_LOST, None, False),
))
@pytest.mark.unit
def test_cleanup_pending_outcome_keeps_original_attempt_until_exact_ack(
    monkeypatch: pytest.MonkeyPatch, lease_state, status, alive,
) -> None:
    core, pending, state = _fixture(max_retries=1)
    outcome = _outcome(
        core, pending, state, lease_state, alive=alive, status=status, cleanup_pending=True,
    )
    ready = [False]
    queries = []
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)

    def rpc(address, handler, request):
        assert address == core.node_address and handler == "get_worker_lease_outcome"
        queries.append(request)
        return replace(outcome, cleanup_pending=not ready[0])

    monkeypatch.setattr(core, "_rpc", rpc)
    replay_state = state
    for _ in range(2):
        assert not core._replay_push(pending, replay_state)
        (delayed,) = _dependency_queue(core)
        assert isinstance(delayed, _DelayedReadyTask)
        replay_state = delayed.ready.push_state
        assert replay_state.push is state.push and replay_state.grant == state.grant
        assert replay_state.orphan_cleanup is None
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        assert pending.task_id in core._protocol_unresolved
    ready[0] = True
    assert not core._replay_push(pending, replay_state)
    (retried,) = _dependency_queue(core)
    assert isinstance(retried, _PendingTask) and core._submissions.empty()
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert retried.object_id == pending.object_id
    assert core._recovery.task_record(pending.task_id).retries_started == 1
    assert all(request == queries[0] for request in queries)


@pytest.mark.unit
def test_completed_application_error_detail_loss_is_not_system_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture(max_retries=1)
    monkeypatch.setattr(core, "_push_task_rpc", _connect_fails)
    monkeypatch.setattr(
        core, "_rpc",
        lambda *_args: _outcome(
            core, pending, state, protocol.LeaseExecutionState.COMPLETED,
            status=protocol.TaskReplyStatus.APPLICATION_ERROR,
        ),
    )

    assert core._replay_push(pending, state)
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.ERROR
    assert isinstance(snapshot.error, TaskError)
    assert core._recovery.task_record(pending.spec.task_id).retries_started == 0
    assert _dependency_queue(core) == ()


@pytest.fixture
def _no_dependency_runtime(monkeypatch, _no_output_runtime):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure dependency retry attempted runtime or unmodelled RPC")

    for kind, method in ((NodeServer, "__init__"),
                         (threading.Condition, "wait_for"),
                         (threading.Barrier, "wait"),
                         (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socketpair", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


def _never_execute_dependency(*_args):
    pytest.fail("dependency retry fixture executed user code")


def _dependency_queue(core):
    """One finite snapshot of the real FIFO, with real task_done accounting."""
    size = core._submissions.qsize()
    assert size <= 16
    pending = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            assert isinstance(item, (_PendingTask, _DelayedReadyTask))
            pending.append(item)
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    return tuple(pending)


def _release_dependency_handle(ref):
    """Apply the real local finalizer without close's Event.wait call."""
    finalizer, done = ref._finalizer, ref._release_done
    assert finalizer is not None and done is not None
    ref._closed = True
    finalizer()
    assert done.is_set() and not finalizer.alive


class _PassiveDependencyWorker:
    def __init__(self):
        self.alive = True

    def is_alive(self):
        return self.alive


class _DependencyRetryFixture:
    """Two logical Tasks, three actual leases and two real INLINE outputs."""

    def __init__(self, monkeypatch):
        self.core = core = make_pure_core()
        core._registered_functions = set()
        core.gcs_address = None
        self.node = node = object.__new__(NodeServer)
        node.node_id = core.node_id
        node._node_pid, node._registration_epoch = 31801, 1
        node._gcs_address, node._registered_with_gcs = None, False
        node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
        node._cluster_addresses = {}
        node._scheduling_policy = HybridPolicy(seed=0)
        node._shutdown_request_id = None
        node._leases, node._lease_outcomes, node._lease_cancellations = {}, {}, {}
        node._lease_request_locks, node._inflight_lease_requests = {}, 0
        node._state_lock, node._scheduling_lock = threading.RLock(), threading.Lock()
        node._stop_event = threading.Event()
        node._object_store = ObjectStore(1024)
        node._sealed_metadata = {}
        node.event_sink = None
        node.num_workers_per_node = 2
        self.workers = (WorkerID.random(), WorkerID.random())
        node._worker_order = self.workers
        node._workers = {worker: _WorkerSlot(
            worker, process=_PassiveDependencyWorker(),
            address=("dependency-worker-{}.invalid".format(index), 1), pid=31802 + index,
        ) for index, worker in enumerate(self.workers)}
        self.journal = node._output_publication_journal = OutputPublicationJournal()
        node._dropped_metadata, node._local_replica_write_claims = {}, {}
        node._object_localization_locks, node._owner_death_fences = {}, {}
        node._pinned_transfers, node._dependency_pin_cleanups, node._actor_workers = {}, {}, {}
        node._resource_report_version = node._resource_reported_version = 0
        node._object_manager = ObjectManager(node.node_id, node._object_store)
        node._background_rpc = self.owner_rpc
        self.adapter = node._output_publications = node._make_output_publication_adapter()
        self.handoffs = core._output_handoff_table()
        self.refs, self.submissions, self.leases, self.pushes, self.queries = [], [], [], [], []
        self.replies, self.completions, self.gc_order, self.controls = {}, [], [], []
        self.active, self.prepared, self.crash_push = None, None, None
        self.dependency_id = self.hold = self.lineage = None
        monkeypatch.setattr(core, "_rpc", self.rpc)
        monkeypatch.setattr(core, "_push_task_rpc", self.push)
        original_collect = core.owner_table.complete_output_publication_collection
        def collect(plan):
            result = original_collect(plan)
            self.gc_order.append(plan.object_id)
            assert len(self.gc_order) <= 2
            return result
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", collect)

    def owner_rpc(self, address, handler, request):
        assert address == self.core.owner_address
        _assert_output_metadata(request)
        methods = {
            wire.REGISTER_OUTPUT_HANDOFF_HANDLER: self.core.register_output_handoff,
            wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: self.core.report_output_handoff_complete,
            wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: self.core.report_output_handoff_rollback,
        }
        assert handler in methods
        reply = methods[handler](request)
        assert reply.accepted and reply.request == request, reply.error
        _assert_output_metadata(reply)
        return reply

    def submit(self, *args, max_retries):
        assert len(self.submissions) < 2
        pending, ref = self.core._register_submission(
            self.core.define_remote_function(_never_execute_dependency), args, {},
            ResourceVector({"CPU": 1}), max_retries=max_retries, _enqueue=True,
        )
        self.submissions.append(pending)
        self.refs.append(ref)
        assert _dependency_queue(self.core) == (pending,)
        return pending, ref

    def select(self, pending):
        self.active = pending
        prepared, dependencies, protected = self.core._prepare_task_dependencies(pending.spec)
        assert protected == pending.protected_dependencies
        assert dependencies == ()  # this real producer returned tiny INLINE 1
        self.prepared = prepared
        if pending.protected_dependencies:
            assert len(prepared.args) == 1 and type(prepared.args[0]) is protocol.InlineArg
            assert cloudpickle.loads(prepared.args[0].data) == 1
            assert type(pending.spec.args[0]) is protocol.RefArg
        return prepared

    def execute(self, pending):
        return self.core._execute(pending, self.select(pending))

    def assert_holds(self):
        if self.dependency_id is None:
            return
        snapshot = self.core.owner_table.snapshot(self.dependency_id)
        assert snapshot.state is ObjectState.READY_INLINE
        assert snapshot.submitted_tokens == frozenset({self.hold})
        assert snapshot.lineage_tokens == frozenset({self.lineage})

    def rpc(self, address, handler, request):
        self.controls.append((handler, request))
        assert len(self.controls) <= 16
        if handler == "request_worker_lease":
            assert address == self.core.node_address and len(self.leases) < 3
            assert request.task_id == self.active.task_id and request.attempt_id == self.active.spec.attempt_id
            assert request.return_ids == self.active.output_ids and request.dependencies == ()
            self.assert_holds()
            reply = self.node._handle_request_lease(request)
            assert type(reply) is protocol.GrantWorkerLease
            assert self.node.resource_ledger.available.is_zero()
            self.leases.append((request, reply))
            return reply
        if handler == "get_worker_lease_outcome":
            assert address == self.core.node_address and not self.queries
            assert request.lease_id == self.crash_push.lease_id
            assert request.executor_worker_id == self.workers[0]
            self.assert_holds()
            reply = self.node._handle_get_worker_lease_outcome(request)
            assert reply.found and not reply.worker_alive
            assert reply.state is protocol.LeaseExecutionState.WORKER_LOST
            assert reply.completion_status is None and not reply.cleanup_pending
            assert reply.descriptors == reply.orphan_descriptors == ()
            assert reply.output_publication is None and reply.output_completion is None
            self.queries.append((request, reply))
            return reply
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert address == self.core.node_address
        assert self.handoffs.query(request.proof.complete.publication_id).adoption == request.proof
        return self.node._handle_ack_output_publication_adopted(request)

    def push(self, address, handler, push):
        assert handler == "push_task" and len(self.pushes) < 4
        assert type(push) is protocol.PushTask
        if self.crash_push is not None and push.lease_id == self.crash_push.lease_id:
            assert push == self.crash_push and push.worker_id == self.workers[0]
            assert address == self.node._workers[push.worker_id].address
            assert not self.node._workers[push.worker_id].process.is_alive()
            assert self.node._leases[push.lease_id].state is protocol.LeaseExecutionState.WORKER_LOST
            self.assert_holds()
            self.pushes.append(push)
            return _connect_fails()
        request, grant = self.leases[-1]
        assert push.lease_id == grant.lease_id and push.worker_id == grant.worker_id
        assert address == grant.worker_address and push.dependencies == ()
        # An already-exported function may omit its definition on later Tasks.
        assert replace(push.spec, function_definition=self.prepared.function_definition) == self.prepared
        self.pushes.append(push)
        self.assert_holds()
        started = self.node._handle_start_worker_lease(protocol.StartWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
        ))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        assert self.node.resource_ledger.available.is_zero()
        if self.active.protected_dependencies and self.active.spec.attempt_id.attempt_number == 0:
            assert self.crash_push is None and push.worker_id == self.workers[0]
            self.crash_push = push
            # Real RUNNING allocation; no output exists when this reply is lost.
            raise TransportTimeout("consumer reply lost before Worker exit")
        identity = OutputPublicationID(push.lease_id, self.active.execution)
        assert identity not in self.replies and len(self.replies) < 2
        session = OutputDiscoverySession(OutputPublicationHeader(
            identity, self.core.job_id, push.worker_id, self.core.worker_id,
            OutputPublicationNodeIncarnation(self.node.node_id, self.node._node_pid, self.node._registration_epoch),
        ), inline_threshold=1024)
        outputs = session.discover((1))
        assert len(((outputs.manifest.value,))) == 1
        assert (outputs.manifest.value).tier is protocol.ResultStorage.INLINE
        assert (outputs.manifest.value).size_bytes <= 32 and not (outputs.manifest.value).transfers
        prepared = self.node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            outputs.manifest, (outputs.payload),
        ))
        assert prepared.accepted and self.handoffs.query(identity).manifest == outputs.manifest
        assert self.node.resource_ledger.available.is_zero()
        session.release_sources_after_promotions()
        completion = protocol.CompleteWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED,
        )
        reply = self.node._handle_complete_worker_lease(completion)
        assert reply.accepted and reply.released
        assert reply.output_publication.manifest == outputs.manifest
        assert self.node.resource_ledger.available == self.node.resource_ledger.total
        self.completions.append((completion, reply))
        result = protocol.TaskReply(
            push.spec.task_id, push.spec.attempt_id, push.worker_id, protocol.TaskReplyStatus.SUCCEEDED,
            ((reply.output_publication.result,)), output_publication=reply.output_publication,
        )
        self.replies[identity] = result
        return result

    def lose_worker(self):
        slot = self.node._workers[self.workers[0]]
        assert self.crash_push is not None and slot.active_lease_id == self.crash_push.lease_id
        slot.process.alive = False  # passive OS-exit input; no process is started
        with self.node._state_lock:
            assert self.node._reclaim_active_lease_after_worker_exit_locked(slot.worker_id)
            after = self.node.resource_ledger.snapshot()
            assert not self.node._reclaim_active_lease_after_worker_exit_locked(slot.worker_id)
            assert self.node.resource_ledger.snapshot() == after
        assert after.available == self.node.resource_ledger.total
        assert slot.active_lease_id is None
        assert self.node._workers[self.workers[1]].process.is_alive()

    def gc_notices(self, *, stop_after=None):
        """Drive at most 16 real notices, optionally pause at one collection."""
        fifo = self.core._reference_mailbox.pending
        assert fifo.qsize() <= 16
        known = {ref.object_id for ref in self.refs}
        for _ in range(16):
            try:
                event = fifo.get_nowait()
            except queue.Empty:
                return
            try:
                assert type(event) is _RetryInlineGc and event.object_id in known
                self.core._reference_released(event.object_id)
            finally:
                fifo.task_done()
            if stop_after is not None and self.core.owner_table.collection_state(stop_after) is ObjectCollectionState.COLLECTED:
                return
        assert fifo.empty(), "dependency GC exceeded 16 explicit notices"

    def close(self):
        for ref in self.refs:
            _release_dependency_handle(ref)
        close_pure_core(self.core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_dependency_runtime")
def test_dependency_hold_survives_worker_loss_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    f = _DependencyRetryFixture(monkeypatch)
    core = f.core
    try:
        producer, dependency = f.submit(max_retries=0)
        assert f.execute(producer)
        assert core.get(dependency, timeout=0) == 1
        assert core._finish_pending_task(producer)
        assert _dependency_queue(core) == ()
        f.gc_notices()
        consumer, result = f.submit(dependency, max_retries=1)
        hold = consumer.dependency_hold
        lineage = _lineage_hold_token(consumer.task_id, producer.object_id)
        f.dependency_id, f.hold, f.lineage = producer.object_id, hold, lineage
        assert hold == protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id,
            consumer.spec.task_id, consumer.spec.attempt_id,
        )
        assert consumer.protected_dependencies == (producer.object_id,)
        f.assert_holds()
        edges = core.owner_table.task_lineage_edges(consumer.task_id)
        assert len(edges) == 1 and next(iter(edges)).token == lineage
        _release_dependency_handle(dependency)
        f.gc_notices()
        assert not core.owner_table.snapshot(producer.object_id).local_tokens
        f.assert_holds()

        assert not f.execute(consumer)
        (delayed,) = _dependency_queue(core)
        assert type(delayed) is _DelayedReadyTask and type(delayed.ready) is _ReadyTask
        state = delayed.ready.push_state
        assert state is not None and state.ambiguous and state.round == 1
        assert state.push == f.crash_push and state.lease_request == f.leases[-1][0]
        assert state.grant == f.leases[-1][1]
        assert not core._finish_pending_task(consumer)
        assert core._recovery.task_record(consumer.task_id).retries_started == 0
        f.assert_holds()
        f.lose_worker()
        # The original failure path now consumes a real Node WORKER_LOST
        # outcome. Transport failure itself is not its death evidence.
        assert not core._replay_push(consumer, state)
        (retried,) = _dependency_queue(core)
        assert type(retried) is _PendingTask
        assert retried.dependency_hold == hold
        assert retried.dependency_hold.origin_attempt_id == consumer.spec.attempt_id
        assert retried.protected_dependencies == consumer.protected_dependencies
        assert retried.object_id == consumer.object_id == result.object_id
        assert retried.task_id == consumer.task_id
        assert retried.spec.attempt_id == consumer.spec.attempt_id.next()
        assert core.owner_table.snapshot(producer.object_id).submitted_tokens == frozenset({hold})
        assert core.owner_table.task_lineage_edges(consumer.task_id) == edges
        assert core.owner_table.snapshot(result.object_id).current_attempt == retried.spec.attempt_id
        assert core.owner_table.snapshot(result.object_id).state is ObjectState.PENDING
        retry = core._recovery.task_record(consumer.task_id)
        assert retry.state is TaskState.RETRY_PENDING and retry.retries_started == 1
        assert retry.current_attempt == retried.spec.attempt_id and isinstance(retry.last_error, WorkerDiedError)
        assert core._accepted_task_count == 1 and core._task_finish_barriers == {result.object_id: retried}
        assert not core._protocol_unresolved and len(f.queries) == 1
        assert not core._finish_pending_task(consumer)
        f.gc_notices()
        f.assert_holds()

        assert f.execute(retried)
        assert f.leases[-1][1].worker_id == f.workers[1]
        assert f.leases[-1][1].lease_id != state.grant.lease_id
        assert f.leases[-1][1].attempt_id == retried.spec.attempt_id
        assert core.get(result, timeout=0) == 1
        assert retry.state is TaskState.SUCCEEDED and retry.retries_started == 1
        assert not core._protocol_unresolved
        f.assert_holds()
        assert core._finish_pending_task(retried) and core._finish_pending_task(retried)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        retained = core.owner_table.snapshot(producer.object_id)
        assert not retained.submitted_tokens and not retained.local_tokens
        assert retained.lineage_tokens == frozenset({lineage})
        assert core.owner_table.task_lineage_edges(consumer.task_id) == edges
        assert _dependency_queue(core) == ()
        f.gc_notices()
        assert core.owner_table.collection_state(producer.object_id) is ObjectCollectionState.ACTIVE
        assert core._recovery.lineage_for_object(producer.object_id) is not None

        _release_dependency_handle(result)
        _release_dependency_handle(result)
        f.gc_notices(stop_after=result.object_id)
        assert core.owner_table.collection_state(result.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(result.object_id) is None
        assert not core.owner_table.task_lineage_edges(consumer.task_id)
        released = core.owner_table.snapshot(producer.object_id)
        assert not released.lineage_tokens and not released.submitted_tokens and not released.local_tokens
        assert core.owner_table.collection_state(producer.object_id) is ObjectCollectionState.ACTIVE
        assert tuple(core._reference_mailbox.pending.queue) == (_RetryInlineGc(producer.object_id),)
        f.gc_notices()
        assert f.gc_order == [result.object_id, producer.object_id]
        assert core.owner_table.collection_state(producer.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(producer.object_id) is None
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert core._reference_mailbox.pending.unfinished_tasks == 0
        assert _dependency_queue(core) == ()
        assert len(core._reference_mailbox.releases) == 2
        assert len(f.leases) == 3 and len(f.pushes) == 4 and len(f.completions) == 2
        assert f.node.resource_ledger.available == f.node.resource_ledger.total
        assert f.node.object_store.used_bytes == 0 and not f.node._sealed_metadata
        assert not f.adapter.pending_lease_completions()
        for identity, reply in f.replies.items():
            record = f.handoffs.query(identity)
            assert record.complete == reply.output_publication.complete and record.adoption is not None
            assert not f.journal.snapshot(identity).retained_result_slots
            assert f.adapter.report_terminal(identity)
        assert not f.adapter.pending_terminal_reports()
    finally:
        f.close()
