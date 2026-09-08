"""Bounded synchronous composition for partial unified multi-return sealing.

No transport server or child process is started.  Calls are routed directly to
the real WorkerServer and NodeServer handlers so the test exercises production
batch prepare, once-serialized bytes, local lease release, exact publication
rollback ACKs and Core retry gating.  Two tiny STORED slots share one manifest;
no legacy per-slot SealObject or descriptor-only success is synthesized.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import (
    CoreWorker, _DelayedReadyTask, _ObjectWaiter, _PendingTask,
    _PushRequestState,
)
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseOutcome, _LeaseRecord, _WorkerSlot
from miniray.object_store import ObjectStore
from miniray.output_publication_journal import (
    OutputPublicationJournal, OutputPublicationJournalState,
    OutputPublicationStage,
)
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.recovery import RecoveryManager
from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector
from miniray.trace import MemoryEventSink
from miniray.transport import RemoteCallError, TransportError, TransportTimeout
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
    START_WORKER_LEASE_HANDLER, WorkerServer,
)


pytestmark = pytest.mark.unit


@pytest.fixture
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("partial batch cleanup attempted real runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(NodeServer, "__init__", forbidden)
    monkeypatch.setattr(WorkerServer, "__init__", forbidden)
    monkeypatch.setattr("miniray.worker.TCPServer.__init__", forbidden)


class _LiveProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.exitcode = None
        self.alive = True

    def is_alive(self) -> bool:
        return self.alive


def _fixture() -> tuple[
    NodeServer, WorkerServer, CoreWorker, _PendingTask, _PushRequestState
]:
    node_id = NodeID.random()
    executor = WorkerID.random()
    owner = WorkerID.random()
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 0)
    outputs = tuple(ObjectID.for_task(task, index) for index in range(2))
    lease_id = LeaseID.random()
    resources = ResourceVector({"CPU": 1})
    allocation = AllocationToken("partial-seal")

    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job, __name__, "partial_seal", "v1"),
        cloudpickle.dumps(lambda: (b"first", b"second-is-too-large")),
    )
    spec = protocol.TaskSpec(
        job, task, attempt, definition.key, (), 2, resources, owner,
        function_definition=definition, max_retries=1,
    )
    request = protocol.RequestWorkerLease(
        lease_id, task, attempt, resources, node_id, owner,
        target_node_id=node_id, return_ids=outputs,
    )
    grant = protocol.GrantWorkerLease(
        lease_id, task, attempt, node_id, executor,
        ("127.0.0.1", 29102), allocation,
    )

    node = object.__new__(NodeServer)
    process = _LiveProcess(9101)
    node.node_id = node_id
    node.worker_id = executor
    node.num_workers_per_node = 1
    node._worker_order = (executor,)
    node._workers = {
        executor: _WorkerSlot(
            executor, process=process, address=grant.worker_address,
            pid=process.pid, active_lease_id=lease_id,
        )
    }
    node._legacy_worker_compat = False
    node._worker_process = process
    node._worker_address = grant.worker_address
    node._worker_pid = process.pid
    node._worker_exitcode = None
    node._worker_forced = False
    node._active_lease_id = lease_id
    node._ledger = ResourceLedger(resources)
    node._ledger.allocate(resources, allocation)
    node._cluster_nodes = (NodeSnapshot(node_id, resources, ResourceVector()),)
    node._cluster_addresses = {node_id: ("127.0.0.1", 29101)}
    node._gcs_address = None
    node._registered_with_gcs = True
    node._node_pid = 21001
    node._registration_epoch = 3
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._local_replica_write_claims = {}
    node._owner_death_fences = {}
    # The first serialized result fits; the second deterministically exceeds
    # capacity after slot zero has committed a real sealed replica.
    first_payload = cloudpickle.dumps(b"first")
    node._object_store = ObjectStore(len(first_payload))
    node._object_manager = None
    node._object_localization_locks = {}
    node._dependency_pin_cleanups = {}
    node._pinned_transfers = {}
    node._actor_workers = {}
    node._shutdown_request_id = None
    node._stop_event = threading.Event()
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node.event_sink = None
    record = _LeaseRecord(request, allocation, grant)
    node._leases = {lease_id: record}
    node._lease_outcomes = {lease_id: _LeaseOutcome(request, grant)}
    node._lease_cancellations = {}
    node._lease_request_locks = {}
    node._output_publication_journal = OutputPublicationJournal()
    recovery = OutputPublicationRecoveryAuthority()

    def report_output(handler, message):
        assert handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
        if type(message) is wire.ReportOutputPublicationIntent:
            ack = recovery.report_intent(message.manifest)
        elif type(message) is wire.ArmOutputPublication:
            ack = recovery.arm_complete(message.publication_id, message.manifest_digest)
        elif type(message) is wire.ReportOutputPublicationTerminal:
            ack = recovery.report_terminal(message.witness)
        else:
            assert type(message) is wire.ReportOutputPublicationRollback
            ack = recovery.report_rollback(message.tombstone, manifest=message.manifest)
        return wire.OutputRecoveryReply(message, ack)

    def forbidden_effect(*_args, **_kwargs):
        pytest.fail("ref-free batch attempted a child/graph/network effect")

    node._stored_gcs_rpc = report_output
    node._background_rpc = forbidden_effect
    node._output_publications = node._make_output_publication_adapter()

    worker = object.__new__(WorkerServer)
    worker.worker_id = executor
    worker.node_id = node_id
    worker.node_address = ("127.0.0.1", 29101)
    worker.inline_threshold = 0
    worker._execution_lock = threading.Lock()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._failpoint = None
    worker._failpoint_triggers = 0
    worker._crash_after_complete = set()
    worker._worker_core_enabled = False
    worker.event_sink = None

    core = object.__new__(CoreWorker)
    core.node_id = node_id
    core.node_address = ("127.0.0.1", 29101)
    core.gcs_address = None
    core.job_id = job
    core.worker_id = owner
    core.driver_task_id = TaskID.for_driver(job)
    core.event_sink = MemoryEventSink()
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register_task_outputs(spec)
    core._recovery = RecoveryManager()
    core._recovery.register_task(spec, max_retries=1)
    core._objects = {value: _ObjectWaiter(threading.Event()) for value in outputs}
    core._stored_descriptors = {}
    core._registered_functions = set()
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._submissions = queue.Queue()
    core._protocol_unresolved = {}
    core._dead_nodes = {}
    core._node_death_attempts = {}
    pending = _PendingTask(outputs[0], spec)
    push = protocol.PushTask(lease_id, executor, spec)
    state = _PushRequestState(
        push, grant, core.node_address, grant.worker_address, 1, True
    )
    core._mark_protocol_unresolved(
        pending, "push_replay_wait", target_node_id=node_id
    )
    return node, worker, core, pending, state


@pytest.mark.usefixtures("_no_runtime")
def test_partial_seal_orphan_requires_exact_drop_ack_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, worker, core, pending, state = _fixture()
    first_id, second_id = pending.output_ids
    key = pending.spec.attempt_id, state.push.lease_id
    prepares, prepare_replies, completions, completion_replies = [], [], [], []
    seal_attempts, drop_attempts, rollback_reports = [], [], []
    executions, serialized_values = [], []
    lost_drop_ack = False
    rollback_report_allowed = False
    first_pin = None
    real_loads, real_dumps = cloudpickle.loads, cloudpickle.dumps
    seal = node._seal_output_publication_replica
    drop = node._drop_output_publication_replica
    report_rollback = node._output_publications._report_rollback

    def counting_callable():
        executions.append(True)
        return b"first", b"second-is-too-large"

    def loads(payload):
        return counting_callable if payload == pending.spec.function_definition.payload else real_loads(payload)

    def dumps(value, *args, **kwargs):
        if type(value) is bytes and value in (b"first", b"second-is-too-large"):
            serialized_values.append(value)
        return real_dumps(value, *args, **kwargs)

    def seal_and_pin(effect, descriptor, payload):
        nonlocal first_pin
        seal_attempts.append((effect, descriptor, payload))
        result = seal(effect, descriptor, payload)
        if descriptor.object_id == first_id and first_pin is None:
            first_pin = node.object_store.pin(first_id, "partial-seal-pin")
        return result

    def drop_and_lose_ack(effect, request):
        nonlocal lost_drop_ack
        result = drop(effect, request)
        drop_attempts.append((effect, request, result))
        if request.object_id == first_id and result.status is protocol.DropObjectReplicaStatus.DROPPED and not lost_drop_ack:
            lost_drop_ack = True
            raise RuntimeError("drop took effect before its callback ACK")
        return result

    def uncertain_rollback_report(tombstone, *, manifest):
        result = report_rollback(tombstone, manifest=manifest)
        rollback_reports.append((tombstone, result))
        if not rollback_report_allowed:
            raise TransportTimeout("GCS rollback report committed but ACK was lost")
        return result

    def worker_to_node(_address, handler, message):
        if handler == START_WORKER_LEASE_HANDLER:
            return node._handle_start_worker_lease(message)
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            assert type(message) is wire.PrepareOutputPublication
            prepares.append(message)
            result = node._handle_prepare_output_publication(message)
            prepare_replies.append(result)
            return result
        if handler == GET_WORKER_LEASE_OUTCOME_HANDLER:
            return node._handle_get_worker_lease_outcome(message)
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        completions.append(message)
        result = node._handle_complete_worker_lease(message)
        completion_replies.append(result)
        return result

    monkeypatch.setattr("miniray.worker.cloudpickle.loads", loads)
    monkeypatch.setattr("miniray.output_discovery.cloudpickle.dumps", dumps)
    monkeypatch.setattr(node._output_publications, "_seal_replica", seal_and_pin)
    monkeypatch.setattr(node._output_publications, "_drop_replica", drop_and_lose_ack)
    monkeypatch.setattr(node._output_publications, "_report_rollback", uncertain_rollback_report)
    monkeypatch.setattr("miniray.worker.rpc_request", worker_to_node)

    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        worker._handle_push_task(state.push)
    retained = worker._prepared_output_replies[key]
    publication = retained.outputs.manifest.publication_id
    journal = node._output_publication_journal
    adapter = node._output_publications
    assert len(prepares) == len(prepare_replies) == 1
    assert not prepare_replies[0].accepted
    assert prepare_replies[0].request_identity == prepares[0].request_identity
    assert prepare_replies[0].error_kind is wire.OutputPublicationRPCErrorKind.INVALID_STATE
    assert retained.outputs.manifest == prepares[0].manifest
    assert retained.outputs.slot_payloads == prepares[0].slot_payloads
    assert len(retained.outputs.slot_payloads) == 2
    assert all(slot.tier is protocol.ResultStorage.OBJECT_STORE for slot in retained.outputs.manifest.slots)
    assert tuple(effect.slot_index for effect, _descriptor, _payload in seal_attempts) == (0, 1)
    assert tuple(payload for _effect, _descriptor, payload in seal_attempts) == retained.outputs.slot_payloads
    assert executions == [True] and serialized_values == [b"first", b"second-is-too-large"]
    failure = retained.failure_reply
    assert failure is not None and failure.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert failure.results == () and failure.output_publication is None
    assert retained.complete_envelope is None and not retained.prepare_acked
    assert key not in worker._replies and key not in worker._completion_acked
    record = node._leases[state.push.lease_id]
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert record.completion is not None
    assert record.completion.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert node.resource_ledger.available == ResourceVector({"CPU": 1})
    assert node.object_store.contains(first_id)
    assert not node.object_store.contains(second_id, sealed_only=False)
    assert first_pin is not None
    assert journal.snapshot(publication).complete is None
    assert journal.snapshot(publication).state is OutputPublicationJournalState.ROLLING_BACK
    assert not adapter.rollback_reported(publication)

    outcome_request = protocol.GetWorkerLeaseOutcome(
        state.push.lease_id, pending.task_id, pending.spec.attempt_id,
        state.push.worker_id, core.worker_id, pending.output_ids,
    )

    def assert_outcome_pending():
        outcome = node._handle_get_worker_lease_outcome(outcome_request)
        assert outcome.found and outcome.worker_alive and outcome.cleanup_pending
        assert outcome.state is protocol.LeaseExecutionState.COMPLETED
        assert outcome.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert outcome.descriptors == outcome.orphan_descriptors == ()
        assert outcome.output_publication is None and outcome.output_completion is None
        assert core._recovery.task_record(pending.task_id).current_attempt == pending.spec.attempt_id
        assert all(core.owner_table.snapshot(value).state is ObjectState.PENDING
                   for value in pending.output_ids)

    assert_outcome_pending()

    core_calls = []

    def core_to_node(_address, handler, message):
        assert handler == GET_WORKER_LEASE_OUTCOME_HANDLER
        core_calls.append(message)
        return node._handle_get_worker_lease_outcome(message)

    def replay_worker(_address, handler, message):
        assert handler == "push_task" and message == state.push
        try:
            return worker._handle_push_task(message)
        except RuntimeError as error:
            if isinstance(error, TransportError):
                raise
            raise RemoteCallError(handler, type(error).__name__, str(error), "") from error

    monkeypatch.setattr(core, "_push_task_rpc", replay_worker)
    monkeypatch.setattr(core, "_rpc", core_to_node)
    assert not core._replay_push(pending, state)
    delayed = core._submissions.get_nowait()
    assert isinstance(delayed, _DelayedReadyTask)
    assert delayed.ready.push_state.orphan_cleanup is None
    assert delayed.ready.push_state.push == state.push
    assert node.object_store.contains(first_id)
    assert any(request.object_id == first_id and reply.status is protocol.DropObjectReplicaStatus.PINNED
               for _effect, request, reply in drop_attempts)
    assert_outcome_pending()

    assert node.object_store.unpin(first_id, first_pin)
    # The first unpinned drop commits the Node tombstone but loses its ACK, so
    # the same old attempt still cannot advance.
    assert not core._replay_push(pending, delayed.ready.push_state)
    delayed = core._submissions.get_nowait()
    assert isinstance(delayed, _DelayedReadyTask)
    assert lost_drop_ack
    assert not node.object_store.contains(first_id, sealed_only=False)
    assert not adapter.rollback_reported(publication)
    assert journal.snapshot(publication).state is OutputPublicationJournalState.ROLLING_BACK
    assert_outcome_pending()

    # Exact ALREADY_DROPPED retires local bytes, but a lost GCS rollback-report
    # ACK is still an obligation.  Neither Complete nor outcome may permit retry.
    assert not core._replay_push(pending, delayed.ready.push_state)
    delayed = core._submissions.get_nowait()
    assert isinstance(delayed, _DelayedReadyTask)
    assert journal.snapshot(publication).state is OutputPublicationJournalState.RETIRED
    assert journal.snapshot(publication).rollback_tombstone is not None
    assert not adapter.rollback_reported(publication) and rollback_reports
    assert_outcome_pending()

    rollback_report_allowed = True
    assert worker._handle_push_task(state.push) is failure
    assert adapter.rollback_reported(publication)
    assert core._recovery.task_record(pending.task_id).current_attempt == pending.spec.attempt_id
    # Preserve the original dead-route contract: only after the real failed
    # Complete and cleanup ACK does this fake managed child report exit.  Core
    # must recover the terminal from Node, never infer it from a transport loss.
    process = node._workers[state.push.worker_id].process
    assert isinstance(process, _LiveProcess)
    process.alive = False
    process.exitcode = 23

    def lost_task_reply(*_args, **_kwargs):
        raise TransportTimeout("completed Worker task reply route was lost")

    monkeypatch.setattr(core, "_push_task_rpc", lost_task_reply)
    assert not core._replay_push(pending, delayed.ready.push_state)
    queued_count = core._submissions.qsize()
    assert queued_count <= 4
    queued = tuple(core._submissions.get_nowait() for _ in range(queued_count))
    retries = tuple(value for value in queued if isinstance(value, _PendingTask))
    assert len(retries) == 1
    retried = retries[0]
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert all(
        core.owner_table.snapshot(value).current_attempt
        == retried.spec.attempt_id
        and core.owner_table.snapshot(value).state is ObjectState.PENDING
        for value in pending.output_ids
    )
    assert core._recovery.task_record(pending.task_id).retries_started == 1
    assert adapter.rollback_reported(publication)
    assert key in worker._completion_acked and worker._replies[key] is failure
    assert key not in worker._prepared_output_replies
    assert retained.discovery.source_references == () and retained.nested_imports is None
    assert node.object_store.used_bytes == 0 and not node._sealed_metadata
    assert not node._local_replica_write_claims
    outcome = node._handle_get_worker_lease_outcome(outcome_request)
    assert outcome.found and not outcome.worker_alive and not outcome.cleanup_pending
    assert outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert outcome.descriptors == outcome.orphan_descriptors == ()
    assert outcome.output_publication is None and outcome.output_completion is None
    assert all(request == completions[0] for request in completions)
    assert not completion_replies[0].accepted and completion_replies[-1].accepted
    assert all(tombstone == rollback_reports[0][0] for tombstone, _ack in rollback_reports)
    assert all(request == outcome_request for request in core_calls)
    first_drops = tuple(item for item in drop_attempts if item[1].object_id == first_id)
    assert first_drops[-1][2].status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert all((effect, request) == first_drops[0][:2] for effect, request, _reply in first_drops)
    effect, drop_request, _ack = first_drops[-1]
    assert effect.stage is OutputPublicationStage.SLOT_DROP
    replay = node._drop_output_publication_replica(effect, drop_request)
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert len(prepares) == 1 and len(seal_attempts) == 2
    assert executions == [True] and serialized_values == [b"first", b"second-is-too-large"]
    assert node.resource_ledger.available == ResourceVector({"CPU": 1})
