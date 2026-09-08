"""Spillback contracts classified by real helper effects.

The three original Core submission cases now compose one threadless Core,
one or two real Node reducers, a NodeRegistry, and one selected INLINE output.
Hybrid spillback, Grant/Start/Complete, publication INTENT/ARM/adoption and owner
GC are real; the transport supplies one tiny scalar without running user code.
Timeout and explicit Worker rejection retain the same Push for one manual replay.
At most one 1 KiB store is allocated and no pure case waits or starts runtime
infrastructure. This does not replace live Worker or shutdown coverage.

Other unit bodies are unchanged. The duplicate-spillback case is opt-in L1:
two real request threads overlap at the original local-snapshot boundary,
with bounded Barrier/events/joins and the actual Node locks, policy and cache.
It creates no cluster, Core, ObjectStore or socket, but must still run one
exact node ID through the external 30-second runner after review.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import pickle
import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace
from types import SimpleNamespace
from typing import Optional

import pytest

from miniray import core as core_module, node as node_module, output_protocol as wire, protocol
from miniray.control import NodeRegistry
from miniray.core import CoreWorker, _DelayedReadyTask, _ReadyTask, _RetryInlineGc, _WAKE_COORDINATOR
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.node import NodeServer, RELEASE_LEASE_HANDLER, REQUEST_LEASE_HANDLER
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan
from miniray.recovery import TaskState
from miniray.resources import (
    AllocationToken,
    HybridPolicy,
    NodeSnapshot,
    ResourceLedger,
    ResourceVector,
)
from miniray.trace import MemoryEventSink
from miniray.transport import RemoteCallError, TransportConnectionError
from miniray.worker import PUSH_TASK_HANDLER
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_node_output import prepare_ref_free_output


_REMOTE_ONLY_RESOURCE = "node2_only"


class _AliveWorker:
    def is_alive(self) -> bool:
        return True


def _task_identity(index: int = 0) -> tuple[TaskID, AttemptID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return task_id, AttemptID(task_id, 0)


def _lease_request(
    *,
    requester_node_id: NodeID,
    requester_worker_id: WorkerID,
    resources: ResourceVector,
    target_node_id: Optional[NodeID] = None,
    lease_id: Optional[LeaseID] = None,
) -> protocol.RequestWorkerLease:
    task_id, attempt_id = _task_identity()
    return protocol.RequestWorkerLease(
        lease_id=lease_id or LeaseID.random(),
        task_id=task_id,
        attempt_id=attempt_id,
        resources=resources,
        requester_node_id=requester_node_id,
        requester_worker_id=requester_worker_id,
        preferred_node_id=requester_node_id,
        target_node_id=target_node_id,
    )


def _node_without_transport(
    node_id: NodeID, total: ResourceVector, *, worker_id: WorkerID
) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = node_id
    node.worker_id = worker_id
    node._ledger = ResourceLedger(total)
    # A non-None address selects the cluster scheduling path for first-hop
    # requests.  Tests replace _get_cluster_nodes, so no RPC is performed.
    node._gcs_address = ("127.0.0.1", 19000)
    node._scheduling_policy = HybridPolicy(seed=0)
    node._registered_with_gcs = False
    node._cluster_nodes = (NodeSnapshot(node_id, total, total),)
    node._cluster_addresses = {}
    node._worker_process = _AliveWorker()
    node._worker_address = ("127.0.0.1", 19001)
    node._shutdown_request_id = None
    node._leases = {}
    node._lease_outcomes = {}
    node._active_lease_id = None
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._gcs_lifecycle_lock = threading.Lock()
    node._stop_event = threading.Event()
    return node


def _core_without_dispatcher(
    home_node_id: NodeID, home_address: tuple[str, int]
) -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.node_id = home_node_id
    core.node_address = home_address
    core.gcs_address = ("127.0.0.1", 18000)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.event_sink = MemoryEventSink()
    core._submission_index = 0
    core._owner_table = ObjectOwnerTable()
    core._objects = {}
    core._stored_descriptors = {}
    core._owner_protocol_open = True
    core._registered_functions = set()
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._submissions = queue.Queue()
    return core


@pytest.fixture
def _no_submission_runtime(monkeypatch):
    """Only the three migrated cases get these runtime tripwires."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure spillback submission attempted runtime infrastructure")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)


def _never_run_spillback_function():
    pytest.fail("pure spillback fixture executed a user function")


def _submission_items(core):
    size = core._submissions.qsize()
    assert size <= 8
    items = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            items.append(item)
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    return tuple(items)


def _release_submission_ref(ref):
    finalizer, done = ref._finalizer, ref._release_done
    assert finalizer is not None and done is not None
    ref._closed = True
    finalizer()
    assert not finalizer.alive and done.is_set()


class _SubmissionFixture:
    """A finite real routing/publication composition; only Push is delivery."""

    def __init__(self, monkeypatch, *, spillback=False, first_failure=None):
        assert first_failure in (None, "timeout", "rejected")
        assert not spillback or first_failure is None
        self.core = core = make_pure_core()
        self.registry = NodeRegistry()
        self.calls, self.node_control_calls = [], []
        self.lease_requests, self.pushes, self.start_replies = [], [], []
        self.grant = self.publication = self.task_reply = None
        self.complete_request = self.completed = None
        self.push_bytes = None
        self.first_failure = first_failure
        self.value = ("remote result" if spillback else
                      "unobserved result" if first_failure == "timeout" else None)
        self.home = _node_without_transport(
            core.node_id, ResourceVector({"CPU": 1}), worker_id=WorkerID.random(),
        )
        self.target = (_node_without_transport(
            NodeID.random(), ResourceVector({"CPU": 1, _REMOTE_ONLY_RESOURCE: 1}),
            worker_id=WorkerID.random(),
        ) if spillback else self.home)
        self.nodes = (self.home, self.target) if spillback else (self.home,)
        core.gcs_address = ("spillback-control.invalid", 1)
        for index, node in enumerate(self.nodes):
            # PIDs are declared reducer identities, not spawned processes.
            # The real registration handler fills the corresponding epoch.
            node._node_pid = 31601 + index
            node._membership_epoch = 0
            node._server = SimpleNamespace(address=("spillback-{}.invalid".format(index), 1))
            node._worker_address = ("spillback-worker-{}.invalid".format(index), 1)
            node._gcs_address = core.gcs_address
            monkeypatch.setattr(node, "_background_rpc", self.node_control)
            node._register_with_gcs()
            assert node._registered_with_gcs
            assert node._registration_epoch == self.registry.get(node.node_id).registration_epoch
        epoch, infos = self.registry.live_snapshot()
        snapshot = protocol.InstallClusterSnapshot(epoch, "pure-spillback-snapshot", infos)
        for node in self.nodes:
            assert node._handle_install_cluster_snapshot(snapshot).installed
        core.node_address = self.home.address
        core._membership_epoch, core._installed_cluster_snapshot = epoch, snapshot
        core._registered_functions = set()
        monkeypatch.setattr(core, "_rpc", self.rpc)
        monkeypatch.setattr(core, "_push_task_rpc", self.push)
        resources = self.target.resource_ledger.total
        self.pending, self.ref = core._register_submission(
            core.define_remote_function(_never_run_spillback_function), (), {},
            resources, max_retries=3, _enqueue=True,
        )
        assert _submission_items(core) == (self.pending,)
        self.assert_pending()

    def node_control(self, address, handler, request, **_options):
        assert address == self.core.gcs_address and len(self.node_control_calls) < 4
        self.node_control_calls.append((handler, request))
        if handler == node_module.GCS_REGISTER_NODE_HANDLER:
            return self.registry.register_message(request)
        assert handler == node_module.GCS_UPDATE_NODE_RESOURCES_HANDLER
        assert type(request) is protocol.UpdateNodeResources
        assert self.registry.update_resources(
            request.node_id, request.node_pid, request.registration_epoch,
            request.report_seq, request.available_resources,
        )
        return protocol.UpdateNodeResourcesReply(
            request.node_id, request.node_pid, request.registration_epoch,
            request.report_seq, True,
        )

    def assert_pending(self):
        core, pending = self.core, self.pending
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.PENDING and snapshot.error is None
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert snapshot.local_tokens == frozenset({self.ref._local_token})
        assert snapshot.producer_task_spec == pending.spec
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.PENDING and record.retries_started == 0
        assert record.current_attempt == pending.spec.attempt_id and record.max_retries == 3
        assert core._recovery.lineage_for_object(pending.object_id) is not None
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {pending.object_id: pending}
        assert not core._finished_tasks and not self.ref._release_done.is_set()

    def rpc(self, address, handler, request):
        assert len(self.calls) < 12
        self.calls.append((address, handler, request))
        if handler == REQUEST_LEASE_HANDLER:
            self.assert_pending()
            node = next(item for item in self.nodes if item.address == address)
            assert type(request) is protocol.RequestWorkerLease
            assert request.return_ids == self.pending.output_ids and request.dependencies == ()
            assert request.task_id == self.pending.task_id and request.attempt_id == self.pending.spec.attempt_id
            self.lease_requests.append(request)
            assert len(self.lease_requests) <= len(self.nodes)
            reply = node._handle_request_lease(request)
            if node is self.home and self.home is not self.target:
                assert type(reply) is protocol.SpillbackWorkerLease
                assert reply.target_node_id == self.target.node_id and reply.target_address == self.target.address
                assert not node._leases and node.resource_ledger.available == node.resource_ledger.total
            else:
                assert type(reply) is protocol.GrantWorkerLease and self.grant is None
                self.grant = reply
                assert self.target._leases[reply.lease_id].grant == reply
                assert self.target.resource_ledger.available.is_zero()
            self.assert_pending()
            return reply
        if handler == "get_node_address":
            assert address == self.core.gcs_address
            assert type(request) is protocol.GetNodeAddress and request.node_id == self.target.node_id
            return protocol.GetNodeAddressReply(request.node_id, True, self.registry.address(request.node_id))
        assert self.publication is not None and self.task_reply is not None
        envelope = self.task_reply.output_publication
        plan = OutputOwnerPublicationPlan(self.pending.execution, envelope)
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.core.gcs_address
            recovery = self.publication.recovery
            if type(request) is wire.ReportOutputPublicationTerminal:
                self.assert_pending()
                assert request.witness == envelope.complete
                ack = recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                assert self.core.owner_table.output_owner_publication_receipt(plan).committed
                assert request.proof.complete == envelope.complete
                ack = recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                assert request.proof.complete == envelope.complete and request.proof.slot_index == 0
                assert self.core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                ack = recovery.report_slot_collected(request.proof)
            return wire.OutputRecoveryReply(request, ack)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert address == self.target.address
        assert self.publication.recovery.snapshot(envelope.publication_id).adopted == request.proof
        assert self.core.owner_table.output_owner_publication_receipt(plan).committed
        return self.target._handle_ack_output_publication_adopted(request)

    def push(self, address, handler, push):
        assert self.grant is not None and len(self.pushes) < 2
        assert address == self.grant.worker_address and handler == PUSH_TASK_HANDLER
        self.calls.append((address, handler, push))
        assert type(push) is protocol.PushTask
        assert push.lease_id == self.grant.lease_id and push.worker_id == self.grant.worker_id
        assert push.spec == self.pending.spec and push.dependencies == ()
        frozen = pickle.dumps(push, protocol=pickle.HIGHEST_PROTOCOL)
        assert len(frozen) <= 8192
        if self.push_bytes is None:
            self.push_bytes = frozen
        else:
            assert frozen == self.push_bytes and push == self.pushes[0]
        self.pushes.append(push)
        self.assert_pending()
        node = self.target
        record = node._leases[push.lease_id]
        assert record.grant == self.grant and node.resource_ledger.available.is_zero()
        assert record.completion is None and record.output_publication_id is None
        if len(self.pushes) == 1 and self.first_failure == "rejected":
            assert record.state is protocol.LeaseExecutionState.GRANTED
            raise RemoteCallError(handler, "RuntimeError", "start rejected", "")
        start = node._handle_start_worker_lease(protocol.StartWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
        ))
        self.start_replies.append(start)
        assert start.accepted and start.state is protocol.LeaseExecutionState.RUNNING
        assert start.node_incarnation.node_id == node.node_id
        assert start.node_incarnation.node_pid == node._node_pid
        assert start.node_incarnation.registration_epoch == node._registration_epoch
        assert node.resource_ledger.available.is_zero()
        if len(self.pushes) == 1 and self.first_failure == "timeout":
            # The original timeout is ambiguous. Keep a real RUNNING lease
            # and its CPU, with neither a success reply nor a completion.
            raise TimeoutError("reply was lost after an ambiguous send")
        assert self.publication is None
        self.publication = prepare_ref_free_output(
            node, self.lease_requests[-1], self.grant,
            job_id=self.core.job_id, values=(self.value,),
        )
        assert self.publication.manifest.header.node_incarnation == start.node_incarnation
        assert node.resource_ledger.available.is_zero()
        self.complete_request = protocol.CompleteWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED,
        )
        self.completed = completed = node._handle_complete_worker_lease(self.complete_request)
        assert completed.accepted and completed.released
        assert completed.state is protocol.LeaseExecutionState.COMPLETED
        assert node.resource_ledger.available == node.resource_ledger.total
        envelope = completed.output_publication
        assert envelope.manifest == self.publication.manifest
        assert len(envelope.results) == 1 and envelope.results[0].storage is protocol.ResultStorage.INLINE
        assert envelope.complete == self.publication.journal.snapshot(envelope.publication_id).complete
        assert node.object_store.used_bytes == 0
        self.assert_pending()
        self.task_reply = protocol.TaskReply(
            push.spec.task_id, push.spec.attempt_id, push.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, envelope.results, output_publication=envelope,
        )
        return self.task_reply

    def execute(self):
        return self.core._execute(self.pending, self.pending.spec)

    def ready(self):
        self.assert_pending()
        assert not self.core._finish_pending_task(self.pending)
        (item,) = _submission_items(self.core)
        assert type(item) is _DelayedReadyTask and type(item.ready) is _ReadyTask
        ready = item.ready
        assert ready.pending is self.pending and ready.spec == self.pending.spec
        assert ready.dependencies == () and ready.lease_state is ready.cancellation is None
        assert ready.output_adoption is ready.location_state is None
        state = ready.push_state
        assert state is not None and state.ambiguous and state.round == 1
        assert state.push == self.pushes[0] and state.grant == self.grant
        assert state.lease_request == self.lease_requests[-1]
        assert state.granting_node_address == self.target.address
        assert state.worker_address == self.grant.worker_address
        assert pickle.dumps(state.push, protocol=pickle.HIGHEST_PROTOCOL) == self.push_bytes
        marker = self.core._protocol_unresolved[self.pending.task_key]
        assert marker.pending is self.pending and marker.target_node_id == self.target.node_id
        assert marker.output_candidate.execution == self.pending.execution
        assert marker.output_candidate.lease_id == self.grant.lease_id
        assert self.publication is self.task_reply is None
        assert self.target.resource_ledger.available.is_zero()
        lease = self.target._leases[self.grant.lease_id]
        assert lease.state is (protocol.LeaseExecutionState.RUNNING if self.first_failure == "timeout"
                               else protocol.LeaseExecutionState.GRANTED)
        assert lease.completion is None and lease.output_publication_id is None
        assert all(call[1] != RELEASE_LEASE_HANDLER for call in self.calls)
        return ready

    def replay(self, ready):
        return self.core._execute(
            self.pending, ready.spec, ready.dependencies, push_state=ready.push_state,
        )

    def assert_success(self):
        core, pending = self.core, self.pending
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.READY_INLINE and snapshot.error is None
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert snapshot.local_tokens == frozenset({self.ref._local_token})
        assert core.get(self.ref, timeout=0) == self.value
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
        assert record.current_attempt == pending.spec.attempt_id
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {pending.object_id: pending}
        assert not core._protocol_unresolved and not core._finished_tasks
        assert core._registered_functions == {(self.grant.worker_id, pending.spec.function)}
        assert all(call[1] != RELEASE_LEASE_HANDLER for call in self.calls)
        assert len(self.lease_requests) == len(self.nodes)
        assert len(self.target._leases) == 1
        lease = self.target._leases[self.grant.lease_id]
        assert lease.state is protocol.LeaseExecutionState.COMPLETED
        assert lease.completion == self.complete_request
        assert self.target._workers[self.grant.worker_id].active_lease_id is None
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        before = self.target.resource_ledger.snapshot()
        version = self.target._resource_report_version
        # A real duplicate Complete after adoption is metadata-only. It must
        # preserve the successful witness without releasing the allocation twice.
        replay = self.target._handle_complete_worker_lease(self.complete_request)
        assert replay.accepted and not replay.released
        assert replay.output_publication is None
        assert replay.output_completion == self.completed.output_publication.complete
        assert self.target.resource_ledger.snapshot() == before
        assert self.target._resource_report_version == version == 2
        identity = self.completed.output_publication.publication_id
        assert not self.publication.journal.snapshot(identity).retained_result_slots
        assert self.publication.recovery.snapshot(identity).adopted is not None
        if self.home is not self.target:
            assert not self.home._leases
            assert self.home.resource_ledger.available == self.home.resource_ledger.total
        if self.first_failure == "timeout":
            assert len(self.start_replies) == 2 and self.start_replies[0] == self.start_replies[1]
        else:
            assert len(self.start_replies) == 1

    def finish_and_collect(self):
        core, pending = self.core, self.pending
        assert core._finish_pending_task(pending) and core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert _submission_items(core) == ()
        _release_submission_ref(self.ref)
        _release_submission_ref(self.ref)
        assert len(core._reference_mailbox.releases) == 1
        fifo = core._reference_mailbox.pending
        assert fifo.qsize() <= 8
        for _ in range(8):
            try:
                event = fifo.get_nowait()
            except queue.Empty:
                break
            try:
                assert type(event) is _RetryInlineGc and event.object_id == pending.object_id
                core._reference_released(event.object_id)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0
        assert _submission_items(core) == ()
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        identity = self.completed.output_publication.publication_id
        assert len(self.publication.recovery.snapshot(identity).slot_collections) == 1
        assert self.publication.adapter.report_terminal(identity)
        assert not self.publication.adapter.pending_terminal_reports()
        assert self.target.object_store.used_bytes == 0
        # Resource reporting is independent of Complete delivery; reduce its
        # already-pending notice explicitly rather than starting a supervisor.
        assert self.target._flush_pending_resource_report()
        assert self.registry.resources(self.target.node_id) == self.target.resource_ledger.total

    def close(self):
        # Failure teardown releases the actual local handle but cannot invent
        # Complete, clear protocol fences or claim full-runtime shutdown.
        _release_submission_ref(self.ref)
        close_pure_core(self.core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_submission_runtime")
def test_core_preserves_identity_and_does_not_release_from_submitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _SubmissionFixture(monkeypatch, spillback=True)
    try:
        assert f.execute()
        assert f.core.get(f.ref, timeout=0) == "remote result"
        lease_calls = [call for call in f.calls if call[1] == REQUEST_LEASE_HANDLER]
        assert [call[0] for call in lease_calls] == [f.home.address, f.target.address]
        first_request, targeted_request = (call[2] for call in lease_calls)
        assert isinstance(first_request, protocol.RequestWorkerLease)
        assert isinstance(targeted_request, protocol.RequestWorkerLease)
        assert first_request.target_node_id is None
        assert targeted_request.target_node_id == f.target.node_id
        assert (targeted_request.lease_id, targeted_request.task_id, targeted_request.attempt_id) == (
            first_request.lease_id, first_request.task_id, first_request.attempt_id,
        )
        assert targeted_request == replace(first_request, target_node_id=f.target.node_id)
        push_calls = [call for call in f.calls if call[1] == PUSH_TASK_HANDLER]
        assert len(push_calls) == 1 and push_calls[0][0] == f.target._worker_address
        push = push_calls[0][2]
        assert isinstance(push, protocol.PushTask)
        assert push.lease_id == first_request.lease_id
        assert f.home._lease_outcomes[first_request.lease_id].request == first_request
        assert f.target._lease_outcomes[first_request.lease_id].request == targeted_request
        # Actual Node Complete, not a fabricated TaskReply, released the
        # granting Node's allocation before delivering the selected output.
        assert [call for call in f.calls if call[1] == RELEASE_LEASE_HANDLER] == []
        f.assert_success()
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_submission_runtime")
def test_core_does_not_release_after_ambiguous_push_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _SubmissionFixture(monkeypatch, first_failure="timeout")
    try:
        assert not f.execute()
        assert [call[1] for call in f.calls] == [REQUEST_LEASE_HANDLER, PUSH_TASK_HANDLER]
        assert all(call[1] != RELEASE_LEASE_HANDLER for call in f.calls)
        ready = f.ready()
        before = f.target.resource_ledger.snapshot()
        assert before.available.is_zero()
        assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.RUNNING
        assert f.replay(ready)
        assert len(f.pushes) == 2 and f.pushes[0] == f.pushes[1]
        assert len(f.lease_requests) == 1
        assert all(call[1] != RELEASE_LEASE_HANDLER for call in f.calls)
        f.assert_success()
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
def test_push_task_rpc_has_no_user_task_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    core = _core_without_dispatcher(NodeID.random(), ("127.0.0.1", 18201))
    captured: dict[str, object] = {}

    def fake_request(address, handler, message, **options):
        captured.update(options)
        return "reply"

    monkeypatch.setattr("miniray.core.rpc_request", fake_request)
    assert core._push_task_rpc(("127.0.0.1", 18202), PUSH_TASK_HANDLER, object()) == "reply"
    assert captured == {
        "connect_timeout": 0.5,
        "request_timeout": None,
        "event_sink": core.event_sink,
        "trace_component": "core_worker",
    }


@pytest.mark.unit
@pytest.mark.usefixtures("_no_submission_runtime")
def test_explicit_worker_rejection_requeues_exact_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = _SubmissionFixture(monkeypatch, first_failure="rejected")
    try:
        assert not f.execute()
        assert [call for call in f.calls if call[1] == RELEASE_LEASE_HANDLER] == []
        ready = f.ready()
        assert ready.push_state is not None and ready.push_state.ambiguous
        assert ready.push_state.push.lease_id == f.calls[0][2].lease_id
        assert not f.start_replies
        assert f.target._leases[f.grant.lease_id].state is protocol.LeaseExecutionState.GRANTED
        assert f.replay(ready)
        assert len(f.lease_requests) == 1 and len(f.pushes) == 2
        assert f.pushes[0] == f.pushes[1]
        assert [call for call in f.calls if call[1] == RELEASE_LEASE_HANDLER] == []
        f.assert_success()
        f.finish_and_collect()
    finally:
        f.close()


@pytest.mark.unit
def test_connection_failure_abandons_but_receive_timeout_does_not() -> None:
    assert CoreWorker._push_definitely_did_not_start(
        TransportConnectionError("connect failed")
    )
    assert not CoreWorker._push_definitely_did_not_start(
        TimeoutError("ambiguous receive timeout")
    )


@pytest.mark.unit
def test_stored_dependency_preparation_keeps_bytes_out_of_driver_path(monkeypatch) -> None:
    """One pure Core, two registered tasks and descriptor-only grant metadata."""
    from tests.unit._pure_core import close_pure_core, make_pure_core
    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    core = make_pure_core()
    refs = []
    try:
        source_node_id = NodeID.random()
        producer, producer_ref = core._register_submission(
            core.define_remote_function(lambda: b"large"),
            (), {}, ResourceVector({"CPU": 1}),
        )
        refs.append(producer_ref)
        payload = b"stored payload must not be fetched by Core"
        checksum = hashlib.sha256(payload).hexdigest()
        source_result = protocol.ResultDescriptor(
            object_id=producer.object_id,
            storage=protocol.ResultStorage.OBJECT_STORE,
            size_bytes=len(payload),
            owner_worker_id=core.worker_id,
            node_id=source_node_id,
            checksum=checksum,
        )
        assert core.owner_table.publish_stored(
            producer.object_id, producer.spec.attempt_id, source_node_id,
            descriptor=source_result,
        )
        core._stored_descriptors[producer.object_id] = source_result
        consumer, consumer_ref = core._register_submission(
            core.define_remote_function(lambda value: value),
            (producer_ref,), {}, ResourceVector({"CPU": 1}),
        )
        refs.append(consumer_ref)
        assert consumer.protected_dependencies == (producer.object_id,)
        assert core.owner_table.snapshot(producer.object_id).submitted_tokens == (
            frozenset({consumer.dependency_hold})
        )
        assert consumer.dependency_hold == protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id,
            consumer.spec.task_id, consumer.spec.attempt_id,
        )

        def forbidden_fetch(*_args, **_kwargs):
            pytest.fail("Core must not fetch stored dependency bytes")

        monkeypatch.setattr(core, "_fetch_stored_object", forbidden_fetch)
        prepared, dependencies, protected = core._prepare_task_dependencies(consumer.spec)
        assert prepared.args == consumer.spec.args
        assert isinstance(prepared.args[0], protocol.RefArg)
        assert protected == (producer.object_id,)
        assert dependencies == (
            protocol.ObjectStoreDescriptor(
                object_id=producer.object_id, owner_worker_id=core.worker_id,
                producer_attempt_id=producer.spec.attempt_id, node_id=source_node_id,
                size_bytes=len(payload), checksum=checksum,
            ),
        )

        target_node_id = NodeID.random()
        # This boundary validates immutable metadata, not Node pull/physical
        # sealing. The bounded real-store handoff cases cover that separately.
        grant = protocol.GrantWorkerLease(
            lease_id=LeaseID.random(), task_id=consumer.spec.task_id,
            attempt_id=consumer.spec.attempt_id, node_id=target_node_id,
            worker_id=WorkerID.random(), worker_address=("worker.invalid", 18402),
            allocation_token=AllocationToken("dependency-location"),
            dependencies=(replace(dependencies[0], node_id=target_node_id),),
        )
        core._validate_granted_dependencies(dependencies, grant)
        before = core.owner_table.snapshot(producer.object_id)
        assert core._build_location_reports(dependencies, grant) == ()
        assert core.owner_table.snapshot(producer.object_id) == before
        assert core._stored_descriptors[producer.object_id] == source_result
        with core._state_lock:
            receipt = core._record_replica_custody_locked(
                grant.dependencies[0],
                active_hold=lambda snapshot: consumer.dependency_hold in snapshot.submitted_tokens,
            )
        assert receipt.accepted
        assert core.owner_table.snapshot(producer.object_id).locations == frozenset(
            {source_node_id, target_node_id}
        )
        assert core.owner_table.snapshot(producer.object_id).canonical_stored_result == source_result
        assert core._stored_descriptors[producer.object_id] == source_result
    finally:
        # Invoke each real finalizer; the pure mailbox applies releases
        # synchronously. Do not call close(), whose unconditional wait is
        # deliberately forbidden here, or erase admitted tasks/lineage/holds.
        for ref in reversed(refs):
            ref._closed = True
            assert ref._finalizer is not None
            ref._finalizer()
            assert ref._release_done is not None and ref._release_done.is_set()
        close_pure_core(core)


@pytest.mark.unit
def test_death_before_local_replica_report_restores_ready_route_atomically(monkeypatch) -> None:
    """Pure typed death/location reducers, not physical Node-loss evidence."""
    from miniray.core import _NodeDeathObserved, _RetryInlineGc
    from tests.unit._pure_core import close_pure_core, make_pure_core
    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    core = make_pure_core()
    producer_ref = None
    try:
        source_node_id, target_node_id = NodeID.random(), core.node_id
        producer, producer_ref = core._register_submission(
            core.define_remote_function(lambda: b"large"),
            (), {}, ResourceVector({"CPU": 1}),
        )
        payload = b"late sealed replica"
        checksum = hashlib.sha256(payload).hexdigest()
        source_result = protocol.ResultDescriptor(
            producer.object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
            core.worker_id, source_node_id, checksum,
        )
        assert core.owner_table.publish_stored(
            producer.object_id, producer.spec.attempt_id, source_node_id,
            descriptor=source_result,
        )
        core._stored_descriptors[producer.object_id] = source_result
        # Supply a typed control-plane input and complete survivor view. The
        # real Core reducer removes location/route; no object() death placeholder
        # or direct metadata pop stands in for that transition.
        death = protocol.NodeDeathRecord(
            "pure-local-source-exit", source_node_id, 18410, 1, 2, -9,
            protocol.NodeDeathReason.PROCESS_EXIT, "source death reducer input",
        )
        resources = ResourceVector({"CPU": 1})
        live = protocol.NodeInfo(
            target_node_id, 18411, 1, core.node_address, resources, resources,
        )
        removal = core.handle_node_death(
            death, protocol.InstallClusterSnapshot(2, "pure-survivor-view", (live,)),
        )
        assert removal.lost == (producer.object_id,)
        assert core._dead_nodes[source_node_id] == death
        assert producer.object_id not in core._stored_descriptors
        lost = core.owner_table.snapshot(producer.object_id)
        assert lost.state.value == "LOST" and not lost.locations
        assert lost.canonical_stored_result == source_result
        assert tuple(core._submissions.queue) == (_NodeDeathObserved(death, 2),)

        target_descriptor = protocol.ObjectStoreDescriptor(
            producer.object_id, core.worker_id, producer.spec.attempt_id,
            target_node_id, len(payload), checksum,
        )
        consumer_task, consumer_attempt = _task_identity(7)
        grant = protocol.GrantWorkerLease(
            LeaseID.random(), consumer_task, consumer_attempt, target_node_id,
            WorkerID.random(), ("worker.invalid", 18412),
            AllocationToken("late-replica-location"), dependencies=(target_descriptor,),
        )
        assert core._build_location_reports((target_descriptor,), grant) == ()
        assert core.owner_table.snapshot(producer.object_id) == lost
        assert producer.object_id not in core._stored_descriptors
        with core._state_lock:
            receipt = core._record_replica_custody_locked(
                target_descriptor, active_hold=lambda _snapshot: False,
            )
        assert receipt.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert receipt.custody_transferred and not receipt.accepted
        restored = core.owner_table.snapshot(producer.object_id)
        assert restored.state.value == "READY_STORED"
        assert restored.current_attempt == producer.spec.attempt_id
        assert restored.locations == frozenset({target_node_id})
        assert restored.canonical_stored_result == source_result
        assert core._stored_descriptors[producer.object_id] == replace(
            source_result, node_id=target_node_id,
        )
        assert tuple(core._reference_mailbox.pending.queue) == (_RetryInlineGc(producer.object_id),)
    finally:
        if producer_ref is not None:
            producer_ref._closed = True
            assert producer_ref._finalizer is not None
            producer_ref._finalizer()
            assert producer_ref._release_done is not None and producer_ref._release_done.is_set()
        # Leave the actual death/GC work queued, with its normal counts. There
        # is no runtime lane, and this reducer test does not claim shutdown.
        close_pure_core(core)


class _ConcurrentSpillbackProbe:
    """Two real request threads; only observation gates delay Node progress.

    The actual per-LeaseID and scheduling locks, local snapshot copy, policy
    selection and outcome cache are unchanged. Both handlers enter before the
    first snapshot gate opens. This observes overlap, not a fabricated waiter
    state or a claim that a particular OS thread is already blocked in acquire.
    Explicit waits are at most one second; normal joins share two seconds and
    failure cleanup shares one. Thread.start and ordinary locks still rely on
    the separately reviewed outer runner's deadline. No TCP or process runs.
    """

    def __init__(self, monkeypatch):
        import math
        from miniray import transport, worker
        from miniray.object_store import ObjectStore

        self.observation_lock = threading.Lock()
        self.barrier = threading.Barrier(3, timeout=1.0)
        self.snapshot_entered = threading.Event()
        self.duplicate_entered = threading.Event()
        self.allow_snapshot = threading.Event()
        self.created, self.started, self.joins = [], [], []
        self.errors, self.violations, self.arrivals, self.snapshots, self.decisions = [], [], [], [], []
        self.replies = []
        self.constructing = False
        self.main_thread = threading.current_thread()
        self.node = self.request = None
        real_thread = threading.Thread
        probe = self

        class RequestThread(real_thread):
            def __init__(thread, *args, **kwargs):
                if not probe.constructing or len(probe.created) >= 2:
                    probe.forbidden("unexpected spillback thread construction")
                super().__init__(*args, **kwargs)
                if not thread.daemon or thread.name != "miniray-spillback-request-{}".format(len(probe.created)):
                    probe.forbidden("spillback thread identity changed")
                probe.created.append(thread)

            def start(thread):
                if thread not in probe.created or thread in probe.started or len(probe.started) >= 2:
                    probe.forbidden("unexpected spillback thread start")
                probe.started.append(thread)
                return super().start()

            def join(thread, timeout=None):
                if (thread not in probe.created or type(timeout) not in (int, float)
                        or not math.isfinite(timeout) or not 0 <= timeout <= 2.0):
                    probe.forbidden("unowned or unbounded spillback thread join")
                probe.joins.append((thread, timeout))
                return super().join(timeout)

        monkeypatch.setattr(threading, "Thread", RequestThread)
        for kind in (CoreWorker, NodeServer, worker.WorkerServer, transport.TCPServer, ObjectStore):
            monkeypatch.setattr(kind, "__init__", self.forbidden)
        for method in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, method, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", self.forbidden)
        monkeypatch.setattr(threading.Timer, "__init__", self.forbidden)
        monkeypatch.setattr(queue.Queue, "join", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        for module in (core_module, node_module, worker):
            monkeypatch.setattr(module, "rpc_request", self.forbidden)
        monkeypatch.setattr(transport, "request", self.forbidden)

    def forbidden(self, *args, **kwargs):
        with self.observation_lock:
            self.violations.append((threading.current_thread(), args, kwargs))
        raise AssertionError("spillback L1 attempted an unmodelled runtime effect")

    def bind(self, monkeypatch, node, request):
        self.node, self.request = node, request
        self.lease_locks = node._lease_request_locks
        self.state_lock, self.scheduling_lock = node._state_lock, node._scheduling_lock
        self.policy = node._scheduling_policy
        self.installed_nodes, self.installed_addresses = node._cluster_nodes, dict(node._cluster_addresses)
        real_snapshot, real_emit = node._get_cluster_nodes, node._emit
        real_schedule = self.policy.schedule

        def snapshot():
            # The real accessor copies the installed local view first. The
            # wait below owns no Node state or test observation lock.
            result = real_snapshot()
            with self.observation_lock:
                self.snapshots.append((threading.current_thread(), result))
            self.snapshot_entered.set()
            if not self.allow_snapshot.wait(1.0):
                self.forbidden("spillback snapshot gate exceeded one second")
            return result

        def emit(name, **attributes):
            real_emit(name, **attributes)
            if name != "lease_requested":
                self.forbidden("unexpected spillback event")
            # This production call is after the real inflight increment and
            # before the LeaseID lock; it is outside Node._state_lock.
            if node._state_lock._is_owned():
                self.forbidden("lease entry observation unexpectedly owns Node lock")
            with node._state_lock:
                with self.observation_lock:
                    self.arrivals.append((threading.current_thread(), node._inflight_lease_requests, attributes))
                    duplicated = len(self.arrivals) == 2
            if duplicated:
                self.duplicate_entered.set()

        def schedule(resources, nodes, **options):
            decision = real_schedule(resources, nodes, **options)
            with self.observation_lock:
                self.decisions.append((threading.current_thread(), resources, tuple(nodes), dict(options), decision))
            return decision

        monkeypatch.setattr(node, "_get_cluster_nodes", snapshot)
        monkeypatch.setattr(node, "_emit", emit)
        monkeypatch.setattr(self.policy, "schedule", schedule)
        monkeypatch.setattr(node, "_background_rpc", self.forbidden)
        monkeypatch.setattr(node, "_localize_dependencies", self.forbidden)

    def start(self):
        def invoke():
            try:
                self.barrier.wait(timeout=1.0)
                reply = self.node._handle_request_lease(self.request)
                with self.observation_lock:
                    self.replies.append((threading.current_thread(), reply))
            except BaseException as exc:
                with self.observation_lock:
                    self.errors.append(exc)

        self.constructing = True
        try:
            threads = tuple(threading.Thread(
                target=invoke, name="miniray-spillback-request-{}".format(index), daemon=True,
            ) for index in range(2))
        finally:
            self.constructing = False
        for thread in threads:
            thread.start()
        self.barrier.wait(timeout=1.0)
        assert self.snapshot_entered.wait(1.0)
        assert self.duplicate_entered.wait(1.0)
        node = self.node
        with node._state_lock:
            assert node._inflight_lease_requests == 2
            assert node._lease_outcomes == {} and node._leases == {}
            assert set(node._lease_request_locks) == {self.request.lease_id}
            assert node._lease_request_locks[self.request.lease_id].locked()
            assert node._scheduling_lock.locked()
            with self.observation_lock:
                assert len(self.snapshots) == 1 and not self.decisions and not self.replies
                assert len(self.arrivals) == 2 and self.arrivals[1][1] == 2
                assert {thread for thread, _, _ in self.arrivals} == set(self.created)
        self.allow_snapshot.set()
        deadline = time.monotonic() + 2.0
        for thread in self.created:
            thread.join(max(0.0, deadline - time.monotonic()))
        assert self.created == self.started and len(self.created) == 2
        assert all(not thread.is_alive() for thread in self.created)
        assert not self.errors and not self.violations

    def close(self):
        # Failure cleanup never re-enters a Node lock potentially held by a
        # failed worker. Only our gates and exact owned threads are touched.
        self.allow_snapshot.set()
        self.barrier.abort()
        deadline = time.monotonic() + 1.0
        for thread in self.created:
            if thread.ident is not None:
                thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in self.created)
        assert not any(thread in self.created for thread in threading.enumerate())
        assert not self.errors and not self.violations


@pytest.mark.loopback_smoke
def test_concurrent_duplicate_spillback_uses_one_cached_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two real overlapping handlers share one installed-view policy decision."""
    home_node_id = NodeID.random()
    target_node_id = NodeID.random()
    requester_worker_id = WorkerID.random()
    local_total = ResourceVector({"CPU": 1})
    remote_total = ResourceVector({"CPU": 1, _REMOTE_ONLY_RESOURCE: 1})
    probe = _ConcurrentSpillbackProbe(monkeypatch)
    try:
        node = _node_without_transport(home_node_id, local_total, worker_id=WorkerID.random())
        home_address, target_address = ("spillback-home.invalid", 1), ("spillback-target.invalid", 2)
        node._node_pid = 31701
        node._server = SimpleNamespace(address=home_address)
        registry = NodeRegistry()
        for node_id, pid, address, total in (
            (home_node_id, node._node_pid, home_address, local_total),
            (target_node_id, 31702, target_address, remote_total),
        ):
            assert registry.register_message(protocol.RegisterNode(node_id, pid, address, total)).accepted
        node._registration_epoch = registry.get(home_node_id).registration_epoch
        epoch, infos = registry.live_snapshot()
        installed = protocol.InstallClusterSnapshot(epoch, "concurrent-spillback-hints", infos)
        assert node._handle_install_cluster_snapshot(installed).installed
        # The remote entry is a scheduling hint, not an instantiated target
        # Node or an authoritative remote allocation. GCS is not queried here.
        request = _lease_request(
            requester_node_id=home_node_id, requester_worker_id=requester_worker_id, resources=remote_total,
        )
        assert request.target_node_id is None and request.dependencies == ()
        probe.bind(monkeypatch, node, request)
        probe.start()
        assert {thread for thread, _ in probe.replies} == set(probe.created)
        replies = tuple(reply for _, reply in probe.replies)
        assert len(replies) == 2 and all(type(reply) is protocol.SpillbackWorkerLease for reply in replies)
        assert replies[0] == replies[1]
        reply = replies[0]
        assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.target_node_id, reply.target_address) == (
            request.lease_id, request.task_id, request.attempt_id, target_node_id, target_address,
        )
        assert reply.scheduling_key == request.scheduling_key and reply.target_execution == request.target_execution
        assert len(probe.snapshots) == len(probe.decisions) == 1
        snapshot_thread, (snapshots, addresses) = probe.snapshots[0]
        decision_thread, resources, policy_nodes, options, decision = probe.decisions[0]
        assert snapshot_thread is decision_thread and snapshot_thread in probe.created
        assert snapshots == policy_nodes == probe.installed_nodes
        assert addresses == probe.installed_addresses and addresses is not node._cluster_addresses
        assert resources == request.resources
        assert options == {"preferred_node_id": home_node_id, "require_available": True}
        assert decision.node_id == target_node_id
        assert set(node._lease_outcomes) == {request.lease_id}
        cached = node._lease_outcomes[request.lease_id]
        assert cached.request == request and cached.reply == reply
        assert node._lease_request_locks is probe.lease_locks
        assert node._state_lock is probe.state_lock and node._scheduling_lock is probe.scheduling_lock
        assert node._scheduling_policy is probe.policy
        assert node._inflight_lease_requests == 0 and node._ledger.available == local_total
        assert not node._leases and not hasattr(node, "_object_store")

        changed_request = replace(request, resources=ResourceVector({"CPU": 0.5}))
        rejected = node._handle_request_lease(changed_request)
        assert type(rejected) is protocol.RejectWorkerLease
        assert rejected.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
        assert (rejected.lease_id, rejected.task_id, rejected.attempt_id) == (request.lease_id, request.task_id, request.attempt_id)
        assert node._lease_outcomes[request.lease_id] is cached
        assert len(probe.snapshots) == len(probe.decisions) == len(node._lease_outcomes) == 1
        assert len(probe.arrivals) == 3 and probe.arrivals[2][0] is probe.main_thread
        assert all(attributes == {
            "lease_id": str(request.lease_id), "task_id": str(request.task_id),
            "attempt_id": str(request.attempt_id), "node_id": str(home_node_id),
        } for _, _, attributes in probe.arrivals)
        assert node._inflight_lease_requests == 0 and node._ledger.available == local_total
        assert node._lease_dependency_custody.request(request.lease_id) == request
        assert not node._lease_dependency_custody.has_pending()
        assert not node._source_pin_releases.has_pending()
        assert not node._leases and not node._lease_cancellations
        assert node._cluster_nodes == probe.installed_nodes and node._cluster_addresses == probe.installed_addresses
        assert not probe.errors and not probe.violations
    finally:
        probe.close()


@pytest.mark.unit
def test_target_node_rechecks_local_capacity_instead_of_stale_gcs_hint() -> None:
    home_node_id = NodeID.random()
    target_node_id = NodeID.random()
    worker_id = WorkerID.random()
    capacity = ResourceVector({"CPU": 1, _REMOTE_ONLY_RESOURCE: 1})
    node = _node_without_transport(target_node_id, capacity, worker_id=worker_id)
    snapshot_calls = 0

    def stale_available_snapshot():
        nonlocal snapshot_calls
        snapshot_calls += 1
        return (
            (NodeSnapshot(target_node_id, capacity, capacity),),
            {target_node_id: ("127.0.0.1", 19001)},
        )

    node._get_cluster_nodes = stale_available_snapshot
    occupied = _lease_request(
        requester_node_id=home_node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=target_node_id,
    )
    occupied_reply = node._handle_request_lease(occupied)
    assert isinstance(occupied_reply, protocol.GrantWorkerLease)
    assert node._ledger.available.is_zero()

    # The head may have spilled this request back from an older "available"
    # snapshot.  A targeted retry skips global selection and consults the
    # target Node's authoritative worker/ledger state.
    contender = _lease_request(
        requester_node_id=home_node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=target_node_id,
    )
    rejected = node._handle_request_lease(contender)
    assert isinstance(rejected, protocol.RejectWorkerLease)
    assert rejected.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
    assert snapshot_calls == 0
    assert contender.lease_id not in node._leases

    released = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            lease_id=occupied.lease_id,
            worker_id=worker_id,
            allocation_token=occupied_reply.allocation_token,
        )
    )
    assert released.released
    assert node._ledger.available == capacity


@pytest.mark.unit
def test_pending_capacity_reuses_one_node_record_until_eventual_grant() -> None:
    node_id = NodeID.random()
    worker_id = WorkerID.random()
    capacity = ResourceVector({"CPU": 1})
    node = _node_without_transport(node_id, capacity, worker_id=worker_id)
    occupied = _lease_request(
        requester_node_id=node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=node_id,
    )
    occupied_reply = node._handle_request_lease(occupied)
    assert isinstance(occupied_reply, protocol.GrantWorkerLease)

    waiting = _lease_request(
        requester_node_id=node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=node_id,
    )
    for _ in range(256):
        reply = node._handle_request_lease(waiting)
        assert isinstance(reply, protocol.RejectWorkerLease)
        assert reply.reason is protocol.LeaseRejectReason.PENDING_CAPACITY

    # One busy lease plus one waiting decision: polling capacity does not
    # manufacture IDs, cache entries, or locks.
    assert set(node._lease_outcomes) == {
        occupied.lease_id, waiting.lease_id,
    }
    assert set(node._lease_request_locks) == {
        occupied.lease_id, waiting.lease_id,
    }
    assert waiting.lease_id not in node._leases

    released = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            occupied.lease_id, worker_id, occupied_reply.allocation_token
        )
    )
    assert released.released
    grant = node._handle_request_lease(waiting)
    assert isinstance(grant, protocol.GrantWorkerLease)
    assert grant.lease_id == waiting.lease_id
    assert node._handle_request_lease(waiting) == grant
    assert len(node._lease_outcomes) == 2
    assert len(node._lease_request_locks) == 2


@pytest.mark.unit
def test_cancelled_pending_capacity_lease_can_never_grant_later() -> None:
    node_id = NodeID.random()
    worker_id = WorkerID.random()
    capacity = ResourceVector({"CPU": 1})
    node = _node_without_transport(node_id, capacity, worker_id=worker_id)
    occupied = _lease_request(
        requester_node_id=node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=node_id,
    )
    occupied_reply = node._handle_request_lease(occupied)
    assert isinstance(occupied_reply, protocol.GrantWorkerLease)
    waiting = _lease_request(
        requester_node_id=node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=node_id,
    )
    pending = node._handle_request_lease(waiting)
    assert isinstance(pending, protocol.RejectWorkerLease)
    assert pending.reason is protocol.LeaseRejectReason.PENDING_CAPACITY

    cancelled = node._handle_cancel_worker_lease(
        protocol.CancelWorkerLease(
            waiting.lease_id, waiting.task_id, waiting.attempt_id,
            waiting.requester_node_id, waiting.requester_worker_id,
        )
    )
    assert cancelled.accepted and cancelled.cancelled
    assert not cancelled.released
    assert node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            occupied.lease_id, worker_id, occupied_reply.allocation_token
        )
    ).released

    late = node._handle_request_lease(waiting)
    assert isinstance(late, protocol.RejectWorkerLease)
    assert late.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
    assert waiting.lease_id not in node._leases
    assert node._active_lease_id is None
    assert node._ledger.available == capacity


@pytest.mark.unit
def test_shutdown_freezes_replayed_pending_lease_without_a_late_grant() -> None:
    node_id = NodeID.random()
    capacity = ResourceVector({"CPU": 1})
    node = _node_without_transport(
        node_id, capacity, worker_id=WorkerID.random()
    )
    # Model an earlier capacity observation without retaining a second live
    # lease: the retry must re-evaluate Node lifecycle before allocating.
    node._active_lease_id = LeaseID.random()
    waiting = _lease_request(
        requester_node_id=node_id,
        requester_worker_id=WorkerID.random(),
        resources=capacity,
        target_node_id=node_id,
    )
    pending = node._handle_request_lease(waiting)
    assert isinstance(pending, protocol.RejectWorkerLease)
    assert pending.reason is protocol.LeaseRejectReason.PENDING_CAPACITY

    node._active_lease_id = None
    node._shutdown_request_id = "shutdown-capacity-test"
    stopped = node._handle_request_lease(waiting)
    replay = node._handle_request_lease(waiting)

    assert isinstance(stopped, protocol.RejectWorkerLease)
    assert stopped.reason is protocol.LeaseRejectReason.SHUTTING_DOWN
    assert replay == stopped
    assert waiting.lease_id not in node._leases
    assert node._ledger.available == capacity
    assert len(node._lease_outcomes) == 1
    assert len(node._lease_request_locks) == 1
