"""Placement-group control reducers and separately classified Core runtime tests.

Unit cases use unstarted fixtures, synchronous RPC boundaries and finite replay
sequences. Four admission cases use real Node/bundle reducers, explicit
FIFO/finish and public close on a synchronous mailbox; the three execution
cases also publish real selected outputs. Three remaining live functions
(four expanded cases) still start runtime work and remain heavy.
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

import miniray.core as core_module
from miniray import (
    node as node_module, output_protocol as output_wire, protocol,
    transport as transport_module, worker as worker_module,
)
from miniray.core import CoreWorker, RemoteFunctionDefinition
from miniray.control import NodeRegistry, PlacementGroupControlCoordinator
from miniray.errors import (
    PlacementGroupLostError, RuntimeShuttingDownError, SystemTaskError,
)
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, PlacementGroupID, TaskID, WorkerID,
)
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.ownership import (
    ObjectCollectionState, ObjectOwnerTable, ObjectState, OutputOwnerPublicationPlan,
)
from miniray.placement import PlacementStrategy
from miniray.reconstruction_runtime import ReconstructionCoordinator
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey
from miniray.trace import EventSink
from miniray.transport import TransportTimeout
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, START_WORKER_LEASE_HANDLER, WorkerServer,
)
from tests.unit._core_test_utils import add_core_thread_finalizer
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_node_placement_group_runtime import _node as _pure_pg_node


def _scheduling_key(node_id: NodeID) -> protocol.PlacementGroupSchedulingKey:
    return protocol.PlacementGroupSchedulingKey(
        PlacementGroupID.random(), 3, 0, node_id, "a" * 64
    )


def _core() -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.node_address = ("127.0.0.1", 21001)
    core.gcs_address = ("127.0.0.1", 21000)
    core.owner_address = ("127.0.0.1", 21004)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.event_sink = EventSink()
    core._submission_index = 0
    core._owner_table = ObjectOwnerTable()
    core._recovery = RecoveryManager()
    core._reconstruction = ReconstructionCoordinator(
        core._recovery, core._owner_table
    )
    core._objects = {}
    core._stored_descriptors = {}
    core._registered_functions = set()
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._submissions = queue.Queue()
    core._accepting = True
    core._accepted_task_count = 0
    core._inflight_pg_control_ops = 0
    core._placement_group_states = {}
    core._placement_group_manifests = {}
    core._dead_nodes = {}
    core._membership_epoch = 0
    core._object_gc_obligations = {}
    core._blocked_tasks = {}
    return core


def _register_pg_task(
    core: CoreWorker, key: protocol.PlacementGroupSchedulingKey, *, max_retries: int = 1
):
    core._placement_group_states[(
        key.placement_group_id, key.attempt
    )] = protocol.PlacementGroupPhaseStatus.CREATED
    return core._register_submission(
        RemoteFunctionDefinition.from_callable(lambda: "pg", core.job_id),
        (),
        {},
        ResourceVector({"CPU": 1}),
        max_retries=max_retries,
        placement_group_scheduling_key=key,
    )


@pytest.fixture
def _no_pg_admission_runtime(monkeypatch):
    """Guard only the four explicit, threadless admission scenarios below."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure PG admission attempted runtime infrastructure")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure close attempted a blocking receipt wait"
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (WorkerServer, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "start"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, worker_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport_module, "request", forbidden)


class _PurePgAdmission:
    """One real committed bundle and tiny selected-output/owner authorities.

    Addresses and the Node's existing inert Worker slot are metadata only.
    At most two logical Tasks, three leases, two publications and one 1-KiB
    store exist. The test explicitly consumes the actual submission FIFO;
    no dispatcher, scheduler clock, user code or transport is substituted.
    """

    def __init__(self):
        self.core, self.node = core, node = make_pure_core(), _pure_pg_node()
        self.target_address = ("pg-target.invalid", 1)
        core.gcs_address = node._gcs_address = ("pg-control.invalid", 1)
        core._registered_functions = set()
        self.registry = NodeRegistry()
        assert self.registry.register(
            node.node_id, self.target_address, node.resource_ledger.total,
            node_pid=node._node_pid,
        )
        node._registration_epoch = self.registry.get(node.node_id).registration_epoch
        node._registered_with_gcs = True
        node._background_rpc = self.resource_rpc
        self.participant_calls, self.calls, self.pushes, self.refs = [], [], [], []
        self.publications, self.drops = {}, []
        self.mode = "inline"
        self.pg = PlacementGroupControlCoordinator(
            self.registry, participant_rpc=self.participant_rpc,
        )
        # The original contract distinguishes PG attempt 3 from task attempts
        # 0/1/2. A real control reducer creates that exact committed capability;
        # only Core's cached control reply is installed as a direct fixture.
        created = self.pg.create(protocol.CreatePlacementGroupRequest(
            PlacementGroupID.random(), 3,
            (protocol.PlacementGroupBundle(0, ResourceVector({"CPU": 1})),),
            PlacementStrategy.PACK.value,
        ))
        assert created.accepted and created.phase is protocol.PlacementGroupPhaseStatus.CREATED
        assert len(created.placements) == 1 and len(self.participant_calls) == 2
        self.key = created.placements[0]
        self.identity = created.placement_group_id, created.attempt
        core._placement_group_states = {self.identity: created.phase}
        core._placement_group_manifests = {self.identity: created.placements}
        self.child = node._bundle_reservations.ledger_for(*self.identity, 0)
        assert self.child.total == self.child.available == ResourceVector({"CPU": 1})
        assert node.resource_ledger.available == ResourceVector({"CPU": 1})
        self.journal = OutputPublicationJournal()
        self.output_recovery = OutputPublicationRecoveryAuthority()

        def no_reference_effect(*_args, **_kwargs):
            pytest.fail("ref-free PG result attempted a child or graph effect")

        self.adapter = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.output_recovery.report_intent,
            arm_complete=self.output_recovery.arm_complete,
            report_terminal=self.output_recovery.report_terminal,
            report_rollback=self.output_recovery.report_rollback,
            prepare_child=no_reference_effect, promote_child=no_reference_effect,
            release_child=no_reference_effect, prepare_graph=no_reference_effect,
            abort_graph=no_reference_effect,
            seal_replica=node._seal_output_publication_replica,
            drop_replica=node._drop_output_publication_replica,
        )
        node._object_manager = ObjectManager(node.node_id, node.object_store)
        node._local_replica_write_claims = {}
        node._output_publication_journal, node._output_publications = self.journal, self.adapter
        core._rpc, core._push_task_rpc = self.rpc, self.push_rpc

    def resource_rpc(self, address, handler, request):
        assert address == self.core.gcs_address and handler == "update_node_resources"
        assert type(request) is protocol.UpdateNodeResources
        updated = self.registry.update_resources(
            request.node_id, request.node_pid, request.registration_epoch,
            request.report_seq, request.available_resources,
        )
        return protocol.UpdateNodeResourcesReply(
            request.node_id, request.node_pid, request.registration_epoch,
            request.report_seq, updated,
        )

    def participant_rpc(self, address, handler, request):
        assert address == self.target_address and len(self.participant_calls) < 2
        handlers = {
            "prepare_placement_group": self.node._handle_prepare_placement_group,
            "commit_placement_group": self.node._handle_commit_placement_group,
        }
        assert handler in handlers
        reply = handlers[handler](request)
        assert reply.accepted and reply.applied
        self.participant_calls.append((request, reply))
        return reply

    def submit(self, *, max_retries=1):
        assert len(self.refs) < 2
        pending, ref = self.core._register_submission(
            RemoteFunctionDefinition.from_callable(lambda: "pg", self.core.job_id),
            (), {}, ResourceVector({"CPU": 1}), max_retries=max_retries,
            placement_group_scheduling_key=self.key, _enqueue=True,
        )
        self.refs.append(ref)
        assert self.take() == (pending,)
        assert pending.spec.scheduling_key is self.key
        assert self.core._task_finish_barriers[pending.object_id] is pending
        return pending, ref

    def take(self):
        from miniray.core import _DelayedReadyTask, _PendingTask, _WAKE_COORDINATOR
        size = self.core._submissions.qsize()
        assert size <= 16
        work = []
        for _ in range(size):
            item = self.core._submissions.get_nowait()
            try:
                if item is not _WAKE_COORDINATOR:
                    assert type(item) in (_PendingTask, _DelayedReadyTask)
                    work.append(item)
            finally:
                self.core._submissions.task_done()
        assert self.core._submissions.empty() and self.core._submissions.unfinished_tasks == 0
        return tuple(work)

    def rpc(self, address, handler, request):
        core, node = self.core, self.node
        if handler == "get_node_address":
            assert address == core.gcs_address and request.node_id == node.node_id
            return protocol.GetNodeAddressReply(request.node_id, True, self.registry.get(request.node_id).address)
        if handler == "request_worker_lease":
            assert address == self.target_address and len(self.calls) < 8
            assert request.target_node_id == request.preferred_node_id == node.node_id
            assert request.scheduling_key == self.key
            self.calls.append((address, handler, request))
            reply = node._handle_request_lease(request)
            assert type(reply) in (protocol.GrantWorkerLease, protocol.RejectWorkerLease)
            return reply
        if handler == output_wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == core.gcs_address
            if type(request) is output_wire.ReportOutputPublicationTerminal:
                envelope = self.publications[request.witness.publication_id]
                assert request.witness == envelope.complete
                ack = self.output_recovery.report_terminal(request.witness)
            elif type(request) is output_wire.ReportOutputPublicationAdopted:
                envelope = self.publications[request.proof.complete.publication_id]
                assert core.owner_table.output_owner_publication_receipt(
                    OutputOwnerPublicationPlan(envelope.manifest.execution, envelope),
                ).committed
                ack = self.output_recovery.report_adopted(request.proof)
            else:
                assert type(request) is output_wire.ReportOutputPublicationSlotCollected
                snapshot = core.owner_table.snapshot(request.proof.object_id)
                if snapshot.output_retirement_id is not None:
                    assert snapshot.state is ObjectState.LOST
                    assert request.proof.cleanup_id == snapshot.output_retirement_id
                else:
                    assert core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                assert not node.object_store.contains(request.proof.object_id, sealed_only=False)
                ack = self.output_recovery.report_slot_collected(request.proof)
            return output_wire.OutputRecoveryReply(request, ack)
        if handler == output_wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            assert address == self.target_address
            assert self.output_recovery.snapshot(request.proof.complete.publication_id).adopted == request.proof
            return node._handle_ack_output_publication_adopted(request)
        if handler == "drop_object_replica":
            assert address == self.target_address and len(self.drops) < 4
            reply = node._handle_drop_object_replica(request)
            self.drops.append((request, reply))
            return reply
        if handler == "get_worker_deaths":
            assert address == core.gcs_address and request.after_epoch == 0
            return protocol.GetWorkerDeathsReply(0, 0, ())
        pytest.fail("unexpected pure PG RPC: " + handler)

    def push_rpc(self, address, handler, push):
        assert address == self.node._workers[push.worker_id].address and handler == "push_task"
        self.calls.append((address, handler, push))
        return self.complete_worker(push)

    def complete_worker(self, push, *, mode=None):
        node = self.node
        assert type(push) is protocol.PushTask and len(self.pushes) < 3
        assert push.spec.scheduling_key == self.key
        self.pushes.append(push)
        started = node._handle_start_worker_lease(protocol.StartWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id, self.key,
        ))
        assert started.accepted and started.state is protocol.LeaseExecutionState.RUNNING
        assert started.node_incarnation is not None and self.child.available == ResourceVector()
        mode = self.mode if mode is None else mode
        assert mode in ("inline", "stored", "system")
        if mode != "system":
            assert len(self.publications) < 2
            identity = OutputPublicationID(push.lease_id, TaskExecutionKey.from_task_spec(push.spec))
            discovery = OutputDiscoverySession(OutputPublicationHeader(
                identity, push.spec.job_id, push.worker_id, self.core.worker_id, started.node_incarnation,
            ), inline_threshold=1024 if mode == "inline" else 0)
            outputs = discovery.discover(("pg" if mode == "inline" else "stored",))
            assert len(outputs.manifest.slots) == 1
            assert all(not slot.transfers and slot.size_bytes <= 1024 for slot in outputs.manifest.slots)
            prepared = node._handle_prepare_output_publication(output_wire.PrepareOutputPublication(
                outputs.manifest, outputs.slot_payloads,
            ))
            assert prepared.accepted and self.output_recovery.snapshot(identity).armed
            discovery.release_sources_after_promotions()
        status = protocol.TaskReplyStatus.SYSTEM_ERROR if mode == "system" else protocol.TaskReplyStatus.SUCCEEDED
        complete = node._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id, status, self.key,
        ))
        assert complete.accepted and complete.released and complete.state is protocol.LeaseExecutionState.COMPLETED
        assert self.child.available == self.child.total and node._workers[push.worker_id].active_lease_id is None
        if mode == "system":
            assert complete.output_publication is None and complete.output_completion is None
            return protocol.TaskReply(
                push.spec.task_id, push.spec.attempt_id, push.worker_id, status,
                error=protocol.RemoteErrorInfo("RuntimeError", "retry"),
            )
        envelope = complete.output_publication
        assert envelope is not None and envelope.manifest == outputs.manifest
        self.publications[identity] = envelope
        return protocol.TaskReply(push.spec.task_id, push.spec.attempt_id, push.worker_id, status,
                                  envelope.results, output_publication=envelope)

    def close(self):
        core = self.core
        try:
            assert not core._protocol_unresolved
            # Each successful body finishes its actual terminal attempts.
            # An earlier assertion failure must not manufacture an ERROR or
            # retire a pending Task whose remote lease is still authoritative.
            assert not core._task_finish_barriers and core._accepted_task_count == 0
            for ref in self.refs:
                ref.close(timeout=0)
            assert core._reference_mailbox.pending.qsize() <= 16
            core._reference_mailbox.drain()
            assert self.take() == ()
            assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
            assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
            assert core._accepted_task_count == 0 and not core._task_finish_barriers
            for ref in self.refs:
                assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                assert core._recovery.lineage_for_object(ref.object_id) is None
            for identity in self.publications:
                snapshot = self.output_recovery.snapshot(identity)
                assert snapshot.adopted is not None and len(snapshot.slot_collections) == 1
                assert not self.journal.snapshot(identity).retained_result_slots
                assert self.adapter.report_terminal(identity)
            assert not self.adapter.pending_terminal_reports()
            assert self.node.object_store.used_bytes == 0 and not self.node._sealed_metadata
            assert not self.node._local_replica_write_claims
            assert self.child.available == self.child.total
            # This fixture does not claim distributed shutdown/removal: the
            # existing committed PG root reservation remains authoritative.
            assert self.node.resource_ledger.available == ResourceVector({"CPU": 1})
            assert core._placement_group_states[self.identity] is protocol.PlacementGroupPhaseStatus.CREATED
        finally:
            for ref in self.refs:
                ref.close(timeout=0)
            close_pure_core(core)


def _death(
    node_id: NodeID, *,
    reason: protocol.NodeDeathReason = protocol.NodeDeathReason.PROCESS_EXIT,
) -> protocol.NodeDeathRecord:
    return protocol.NodeDeathRecord(
        "pg-death", node_id, 4101, 1, 2,
        0 if reason is protocol.NodeDeathReason.EXPECTED else -9,
        reason, "placement participant exited",
    )


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pg_admission_runtime")
def test_committed_participant_death_fences_complete_manifest_and_queued_task(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
) -> None:
    """One queued Task, two empty 1-KiB Nodes and a typed death barrier.

    The death is a reducer input, not an OS process claim. All PG participant
    operations and owner/recovery/finish transitions are real; the peer's
    inert local ledger is deliberately not erased to simulate destruction.
    """
    from types import SimpleNamespace

    from miniray.core import _NodeDeathObserved, _WAKE_COORDINATOR
    from miniray.placement import ReservationState
    from miniray.placement_group_runtime import PlacementGroupPhase

    core = make_pure_core()
    nodes = (_pure_pg_node(), _pure_pg_node())
    registry = NodeRegistry()
    core.gcs_address = ("queued-pg-control.invalid", 1)
    core._ready_tasks = queue.Queue(maxsize=1)
    participant_calls, creates, leases, pushes = [], [], [], []
    ref = None

    def resource_rpc(address, handler, message):
        assert address == core.gcs_address and handler == "update_node_resources"
        assert type(message) is protocol.UpdateNodeResources
        updated = registry.update_resources(
            message.node_id, message.node_pid, message.registration_epoch,
            message.report_seq, message.available_resources,
        )
        return protocol.UpdateNodeResourcesReply(
            message.node_id, message.node_pid, message.registration_epoch,
            message.report_seq, updated,
        )

    for index, node in enumerate(nodes):
        node._node_pid = 28301 + index
        node._server = SimpleNamespace(address=("queued-pg-{}.invalid".format(index), 1))
        assert registry.register(node.node_id, node.address, node.resource_ledger.total, node_pid=node._node_pid)
        node._registration_epoch = registry.get(node.node_id).registration_epoch
        node._gcs_address, node._registered_with_gcs = core.gcs_address, True
        node._background_rpc = resource_rpc

    def participant_rpc(address, handler, message):
        assert len(participant_calls) < 5
        node = next(node for node in nodes if node.address == address)
        assert message.node_id == node.node_id
        handlers = {
            "prepare_placement_group": node._handle_prepare_placement_group,
            "commit_placement_group": node._handle_commit_placement_group,
            "abort_placement_group": node._handle_abort_placement_group,
        }
        assert handler in handlers
        reply = handlers[handler](message)
        assert reply.accepted and reply.applied
        participant_calls.append((handler, message, reply))
        return reply

    pg = PlacementGroupControlCoordinator(registry, participant_rpc=participant_rpc)

    def control_rpc(address, handler, message):
        assert address == core.gcs_address and handler == "create_placement_group"
        assert type(message) is protocol.CreatePlacementGroupRequest and not creates
        reply = pg.create(message)
        creates.append((message, reply))
        return reply

    def no_lease(state):
        leases.append(state.request)
        pytest.fail("LOST PG queued Task requested a Worker lease")

    def no_push(*args):
        pushes.append(args)
        pytest.fail("LOST PG queued Task reached user execution")

    core._rpc = control_rpc
    monkeypatch.setattr(core, "_request_lease_hop", no_lease)
    monkeypatch.setattr(core, "_push_task_rpc", no_push)
    epoch, live_nodes = registry.live_snapshot()
    initial = protocol.InstallClusterSnapshot(epoch, "queued-pg-two-nodes", live_nodes)
    for node in nodes:
        assert node._handle_install_cluster_snapshot(initial).installed
    core._membership_epoch, core._installed_cluster_snapshot = epoch, initial

    try:
        created = core.create_placement_group(
            (ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1})), PlacementStrategy.STRICT_SPREAD,
        )
        assert created.accepted and created.phase is protocol.PlacementGroupPhaseStatus.CREATED
        dead_key, survivor_key = created.placements
        assert (dead_key.bundle_index, survivor_key.bundle_index) == (0, 1)
        assert {dead_key.node_id, survivor_key.node_id} == {node.node_id for node in nodes}
        dead = next(node for node in nodes if node.node_id == dead_key.node_id)
        survivor = next(node for node in nodes if node.node_id == survivor_key.node_id)
        core.node_id, core.node_address = survivor.node_id, survivor.address
        identity = created.placement_group_id, created.attempt
        assert created.attempt == 0 and len(creates) == 1 and len(participant_calls) == 4
        assert core._placement_group_manifests[identity] == (dead_key, survivor_key)
        for node in nodes:
            assert node._bundle_reservations.snapshot(*identity).state is ReservationState.COMMITTED
            assert node.resource_ledger.available == ResourceVector({"CPU": 1})
        pending, ref = core._register_submission(
            RemoteFunctionDefinition.from_callable(lambda: "pg", core.job_id),
            (), {}, ResourceVector({"CPU": 1}), max_retries=3,
            placement_group_scheduling_key=survivor_key, _enqueue=True,
        )
        before = core.owner_table.snapshot(pending.object_id)
        assert before.state is ObjectState.PENDING and len(before.local_tokens) == 1
        assert pending.spec.scheduling_key == survivor_key
        assert core._accepted_task_count == 1 and core._task_finish_barriers[pending.object_id] is pending
        assert core._submissions.qsize() == core._submissions.unfinished_tasks == 1
        assert core._placement_group_states[identity] is protocol.PlacementGroupPhaseStatus.CREATED

        # No timeout or uncommitted observation becomes a death. The Node
        # registry first commits the exact physical incarnation, then the PG
        # authority invalidates both keys and aborts only the surviving Node.
        reported = registry.report_death(protocol.ReportNodeDeath(
            "queued-pg-peer-exit", dead.node_id, dead._node_pid, dead._registration_epoch,
            -9, protocol.NodeDeathReason.PROCESS_EXIT, "pure committed participant death input",
        ))
        assert reported.disposition is protocol.NodeDeathDisposition.APPLIED and reported.death is not None
        (lost,) = pg.fail_node(reported.death)
        assert lost.phase is PlacementGroupPhase.LOST
        assert pg.visible_placement(identity[0]) is None and not pg.has_active_operations()
        assert len(participant_calls) == 5
        assert participant_calls[-1][0] == "abort_placement_group"
        assert participant_calls[-1][1].node_id == survivor.node_id
        assert survivor._bundle_reservations.snapshot(*identity).state is ReservationState.ABORTED
        assert survivor.resource_ledger.available == survivor.resource_ledger.total
        assert dead._bundle_reservations.snapshot(*identity).state is ReservationState.COMMITTED
        assert core._placement_group_states[identity] is protocol.PlacementGroupPhaseStatus.CREATED
        epoch, live_nodes = registry.live_snapshot()
        installed = protocol.InstallClusterSnapshot(epoch, "queued-pg-survivor", live_nodes)
        assert tuple(info.node_id for info in installed.nodes) == (survivor.node_id,)
        assert installed.membership_epoch == reported.membership_epoch
        assert survivor._handle_install_cluster_snapshot(installed).installed
        core.handle_node_death(reported.death, installed)
        assert core._dead_nodes == {dead.node_id: reported.death}
        assert core._installed_cluster_snapshot == installed
        assert core._placement_group_states[identity] is protocol.PlacementGroupPhaseStatus.LOST
        assert core._placement_group_manifests[identity] == created.placements
        assert core.owner_table.snapshot(pending.object_id) == before

        # The originally enqueued Task is still first in the real FIFO. Its
        # target survived, but the complete immutable PG attempt did not.
        item = core._submissions.get_nowait()
        try:
            assert item is pending
            core._admit_or_block(item)
        finally:
            core._submissions.task_done()
        snapshot = core.owner_table.snapshot(pending.object_id)
        record = core._recovery.task_record(pending.task_id)
        assert snapshot.state is ObjectState.ERROR and isinstance(snapshot.error, PlacementGroupLostError)
        assert snapshot.local_tokens == before.local_tokens
        assert snapshot.current_attempt == record.current_attempt == pending.spec.attempt_id
        assert record.state is TaskState.SYSTEM_FAILED and record.last_error is snapshot.error
        assert record.retries_started == 0 and record.retries_remaining == 3
        assert pending.spec.attempt_id.attempt_number == 0
        assert core._objects[pending.object_id].event.is_set()
        assert core._accepted_task_count == 0 and pending.task_key in core._finished_tasks
        assert not core._task_finish_barriers and not core._protocol_unresolved and not core._blocked_tasks
        assert core._finish_pending_task(pending)
        assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
        assert leases == pushes == [] and all(not node._leases for node in nodes)
        with pytest.raises(PlacementGroupLostError):
            core.assert_placement_group_task_admissible(*identity)

        observed = []
        for _ in range(8):
            if core._submissions.empty():
                break
            item = core._submissions.get_nowait()
            try:
                if type(item) is _NodeDeathObserved:
                    assert item.death == reported.death and not observed
                    core._classify_node_death(item)
                    observed.append(item)
                else:
                    assert item is _WAKE_COORDINATOR
            finally:
                core._submissions.task_done()
        assert len(observed) == 1 and core._submissions.empty() and core._submissions.unfinished_tasks == 0
        ref.close(timeout=0)
        assert core._reference_mailbox.pending.qsize() <= 4
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert not core._objects and not core._object_gc_obligations
        assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert len(core._reference_mailbox.releases) == 1
        assert all(node.object_store.used_bytes == 0 for node in nodes)
    finally:
        if ref is not None:
            ref.close(timeout=0)
        close_pure_core(core)


@pytest.mark.unit
def test_unrelated_or_expected_node_death_does_not_mark_pg_lost() -> None:
    core = _core()
    participant = NodeID.random()
    pg_id = PlacementGroupID.random()
    key = protocol.PlacementGroupSchedulingKey(
        pg_id, 0, 0, participant, "a" * 64
    )
    identity = pg_id, 0
    core._placement_group_states[identity] = (
        protocol.PlacementGroupPhaseStatus.CREATED
    )
    core._placement_group_manifests[identity] = (key,)

    core.handle_node_death(_death(NodeID.random()), 2, True)
    assert core._placement_group_states[identity] is (
        protocol.PlacementGroupPhaseStatus.CREATED
    )
    expected = protocol.NodeDeathRecord(
        "expected-pg-node", participant, 4102, 1, 3, 0,
        protocol.NodeDeathReason.EXPECTED, "clean shutdown",
    )
    core.handle_node_death(expected, 3, True)
    assert core._placement_group_states[identity] is (
        protocol.PlacementGroupPhaseStatus.CREATED
    )


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pg_admission_runtime")
def test_pg_task_first_lease_hop_targets_plan_and_never_spills_back(
    request: pytest.FixtureRequest,
) -> None:
    scenario = _PurePgAdmission()
    core, node, key = scenario.core, scenario.node, scenario.key
    try:
        pending, ref = scenario.submit()
        assert core.node_id != node.node_id and core.node_address != scenario.target_address
        assert core._execute(pending, pending.spec)
        assert [call[0] for call in scenario.calls] == [
            scenario.target_address, node._workers[node.worker_id].address,
        ]
        lease, push = (call[2] for call in scenario.calls)
        assert type(lease) is protocol.RequestWorkerLease
        assert lease.target_node_id == lease.preferred_node_id == node.node_id
        assert lease.scheduling_key is key
        assert isinstance(push, protocol.PushTask) and push.spec.scheduling_key is key
        assert push.lease_id == lease.lease_id and len(node._leases) == 1
        snapshot = core.owner_table.snapshot(ref.object_id)
        assert snapshot.state is ObjectState.READY_INLINE
        assert cloudpickle.loads(snapshot.inline_data) == "pg"
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert not core._protocol_unresolved and core._accepted_task_count == 1
        assert core._finish_pending_task(pending) and core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert scenario.take() == ()
    finally:
        scenario.close()


@pytest.mark.unit
def test_pg_lease_reply_must_echo_exact_scheduling_key() -> None:
    target_node = NodeID.random()
    key = _scheduling_key(target_node)
    other_key = protocol.PlacementGroupSchedulingKey(
        key.placement_group_id, key.attempt, key.bundle_index, key.node_id, "b" * 64
    )
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    request = protocol.RequestWorkerLease(
        LeaseID.random(), task_id, AttemptID(task_id, 0), ResourceVector(),
        NodeID.random(), WorkerID.random(), target_node_id=target_node,
        scheduling_key=key,
    )
    reply = protocol.RejectWorkerLease(
        request.lease_id, request.task_id, request.attempt_id,
        protocol.LeaseRejectReason.PENDING_CAPACITY, scheduling_key=other_key,
    )
    with pytest.raises(SystemTaskError, match="scheduling key"):
        CoreWorker._validate_lease_reply_identity(request, reply)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pg_admission_runtime")
def test_pg_pending_capacity_retry_preserves_exact_targeted_lease() -> None:
    from miniray.core import _DelayedReadyTask, _LeaseRequestState, _ReadyTask

    scenario = _PurePgAdmission()
    core, node, key = scenario.core, scenario.node, scenario.key
    try:
        busy, _busy_ref = scenario.submit(max_retries=0)
        busy_request = protocol.RequestWorkerLease(
            LeaseID.random(), busy.task_id, busy.spec.attempt_id, busy.spec.resources,
            core.node_id, core.worker_id, preferred_node_id=node.node_id,
            target_node_id=node.node_id, return_ids=busy.output_ids, scheduling_key=key,
        )
        busy_grant = scenario.rpc(scenario.target_address, "request_worker_lease", busy_request)
        assert type(busy_grant) is protocol.GrantWorkerLease
        pending, _ref = scenario.submit()
        request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.spec.task_id, pending.spec.attempt_id,
            pending.spec.resources, core.node_id, core.worker_id,
            preferred_node_id=node.node_id, target_node_id=node.node_id,
            return_ids=pending.spec.return_ids(), scheduling_key=key,
        )
        state = _LeaseRequestState(request, scenario.target_address, node.node_id, False)
        rejection = scenario.rpc(scenario.target_address, "request_worker_lease", request)
        assert type(rejection) is protocol.RejectWorkerLease
        assert rejection.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
        assert not core._handle_lease_rejection(
            pending, pending.spec, (), rejection, lease_state=state,
        )
        (delayed,) = scenario.take()
        assert type(delayed) is _DelayedReadyTask and isinstance(delayed.ready, _ReadyTask)
        assert delayed.ready.lease_state == state
        assert delayed.ready.lease_state.request is request
        assert delayed.ready.pending.capacity_round == pending.capacity_round + 1
        assert delayed.ready.pending.dependency_hold == pending.dependency_hold
        assert delayed.ready.pending.spec.attempt_id == pending.spec.attempt_id
        assert not delayed.ready.lease_state.allow_spillback
        assert core._accepted_task_count == 2 and not core._protocol_unresolved

        # A real failed Complete releases the occupied Worker/bundle. The test
        # invokes the captured ready continuation explicitly; it never waits
        # on a wall clock or pretends a coordinator processed the deadline.
        busy_reply = scenario.complete_worker(
            protocol.PushTask(busy_grant.lease_id, busy_grant.worker_id, busy.spec), mode="system",
        )
        assert core._retry_explicit_system_failure(busy, busy_reply)
        assert core._finish_pending_task(busy)
        assert core.owner_table.snapshot(busy.object_id).state is ObjectState.ERROR
        assert isinstance(core.owner_table.snapshot(busy.object_id).error, SystemTaskError)
        ready = delayed.ready
        assert core._execute(ready.pending, ready.spec, lease_state=ready.lease_state)
        consumer_calls = [call for call in scenario.calls
                          if call[1] == "request_worker_lease" and call[2].task_id == pending.task_id]
        assert len(consumer_calls) == 2 and all(call[0] == scenario.target_address for call in consumer_calls)
        assert all(call[2] is request for call in consumer_calls)
        assert scenario.pushes[-1].lease_id == request.lease_id
        assert len(node._lease_outcomes) == len(node._leases) == 2
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.READY_INLINE
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._finish_pending_task(ready.pending) and core._finish_pending_task(ready.pending)
        assert core._accepted_task_count == 0 and not core._protocol_unresolved
        assert scenario.take() == ()
    finally:
        scenario.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pg_admission_runtime")
def test_pg_key_survives_system_retry_and_reconstruction() -> None:
    scenario = _PurePgAdmission()
    core, node, key = scenario.core, scenario.node, scenario.key
    try:
        pending, ref = scenario.submit(max_retries=2)
        old_attempt = pending.spec.attempt_id
        scenario.mode = "system"
        assert not core._execute(pending, pending.spec)
        (retried,) = scenario.take()
        assert retried.spec.attempt_id == old_attempt.next()
        assert retried.spec.scheduling_key is key and key.attempt == 3
        assert retried.task_id == pending.task_id and retried.output_ids == pending.output_ids
        assert retried.dependency_hold == pending.dependency_hold
        assert not core._finish_pending_task(pending)
        assert core._accepted_task_count == 1 and core._task_finish_barriers[pending.object_id] is retried
        assert core.owner_table.snapshot(pending.object_id).current_attempt == retried.spec.attempt_id
        assert core._recovery.task_record(pending.task_id).retries_started == 1

        scenario.mode = "stored"
        assert core._execute(retried, retried.spec)
        old_envelope = next(iter(scenario.publications.values()))
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.READY_STORED
        assert core._stored_descriptors[ref.object_id] == old_envelope.results[0]
        assert cloudpickle.loads(node.object_store.get(ref.object_id)) == "stored"
        assert core._finish_pending_task(retried)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert scenario.take() == ()

        # Lose only this physical replica through the public teaching failpoint.
        # A dead PG participant would make reconstruction illegal; it remains
        # alive and its exact committed attempt-3 reservation stays in place.
        assert core.drop_object(ref, node.node_id)
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.LOST
        assert not core._dead_nodes and node.object_store.used_bytes == 0
        assert core._placement_group_states[scenario.identity] is protocol.PlacementGroupPhaseStatus.CREATED
        core._start_or_join_reconstruction(retried.object_id, core._objects[retried.object_id])
        (reconstructed,) = scenario.take()
        assert reconstructed.spec.attempt_id == retried.spec.attempt_id.next()
        # Real publication snapshots detach the lineage spec from mutable
        # aliases. Preserve every capability field, not Python object identity.
        assert reconstructed.spec.scheduling_key == key
        assert reconstructed.spec.scheduling_key.attempt == 3
        assert reconstructed.task_id == pending.task_id and reconstructed.output_ids == pending.output_ids
        assert reconstructed.dependency_hold != retried.dependency_hold
        assert reconstructed.dependency_hold.origin_attempt_id == reconstructed.spec.attempt_id
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers[ref.object_id] is reconstructed
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.PENDING
        assert core.owner_table.snapshot(ref.object_id).output_publication is None
        assert core.owner_table.snapshot(ref.object_id).current_attempt == reconstructed.spec.attempt_id
        assert ref.object_id not in core._stored_descriptors and not core._objects[ref.object_id].event.is_set()
        assert core._recovery.task_record(pending.task_id).retries_started == 2
        assert core._recovery.task_record(pending.task_id).retries_remaining == 0
        assert core._recovery.active_recovery(pending.task_id) == reconstructed.spec.attempt_id
        assert len(scenario.output_recovery.snapshot(old_envelope.publication_id).slot_collections) == 1
        assert [reply.status for _, reply in scenario.drops] == [
            protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        ]
        assert core._execute(reconstructed, reconstructed.spec)
        assert core._finish_pending_task(reconstructed) and core._finish_pending_task(reconstructed)
        assert core._accepted_task_count == 0 and not core._protocol_unresolved and not core._task_finish_barriers
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert core._recovery.active_recovery(pending.task_id) is None
        assert cloudpickle.loads(node.object_store.get(ref.object_id)) == "stored"
        assert [push.spec.attempt_id.attempt_number for push in scenario.pushes] == [0, 1, 2]
        assert len({push.lease_id for push in scenario.pushes}) == 3
        assert all(push.spec.scheduling_key == key for push in scenario.pushes)
        assert scenario.take() == ()
    finally:
        scenario.close()


@pytest.mark.unit
def test_worker_start_and_complete_echo_task_scheduling_key(monkeypatch) -> None:
    from miniray.output_publication import OutputPublicationNodeIncarnation

    node_id = NodeID.random()
    worker_id = WorkerID.random()
    key = _scheduling_key(node_id)
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    spec = protocol.TaskSpec(
        job_id, task_id, AttemptID(task_id, 0),
        protocol.FunctionKey(job_id, __name__, "pg_worker", "v1"), (), 1,
        ResourceVector(), WorkerID.random(), scheduling_key=key,
    )
    push = protocol.PushTask(LeaseID.random(), worker_id, spec)
    worker = object.__new__(WorkerServer)
    worker.worker_id = worker_id
    worker.node_id = node_id
    worker.node_address = ("127.0.0.1", 22001)
    worker._completion_acked = set()
    worker._push_obligations = set()
    seen = []

    def rpc(_address, handler, request):
        seen.append(request)
        if handler == START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                request.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                scheduling_key=request.scheduling_key,
                node_incarnation=OutputPublicationNodeIncarnation(node_id, 21001, 3),
            )
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        return protocol.CompleteWorkerLeaseReply(
            request.lease_id, request.task_id, request.attempt_id,
            request.worker_id, request.status,
            protocol.LeaseExecutionState.COMPLETED, True, True,
            scheduling_key=request.scheduling_key,
        )

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    worker._start_worker_lease(push)
    reply = protocol.TaskReply(
        task_id, spec.attempt_id, worker_id, protocol.TaskReplyStatus.SUCCEEDED, ()
    )
    worker._ensure_completion_acked(push, reply, (spec.attempt_id, push.lease_id))
    assert len(seen) == 2
    assert all(request.scheduling_key is key for request in seen)


@pytest.mark.unit
def test_core_pg_create_and_remove_are_typed_gcs_rpcs() -> None:
    core = _core()
    calls = []

    def rpc(address, handler, request):
        calls.append((address, handler, request))
        assert address == core.gcs_address
        if handler == "create_placement_group":
            placements = tuple(
                protocol.PlacementGroupSchedulingKey(
                    request.placement_group_id, request.attempt,
                    bundle.bundle_index, NodeID.random(),
                    format(bundle.bundle_index + 1, "064x"),
                )
                for bundle in request.bundles
            )
            return protocol.CreatePlacementGroupReply(
                request.placement_group_id, request.attempt, True,
                protocol.PlacementGroupPhaseStatus.CREATED, placements,
            )
        assert handler == "remove_placement_group"
        return protocol.RemovePlacementGroupReply(
            request.placement_group_id, request.attempt, True, True,
            protocol.PlacementGroupPhaseStatus.REMOVED,
        )

    core._rpc = rpc
    created = core.create_placement_group(
        (ResourceVector({"CPU": 1}), ResourceVector({"CPU": 2})),
        PlacementStrategy.STRICT_SPREAD,
    )
    removed = core.remove_placement_group(
        created.placement_group_id, created.attempt
    )

    assert tuple(key.bundle_index for key in created.placements) == (0, 1)
    assert removed.accepted and removed.removed
    assert [handler for _, handler, _ in calls] == [
        "create_placement_group", "remove_placement_group"
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "first_phase",
    (
        protocol.PlacementGroupPhaseStatus.PREPARING,
        protocol.PlacementGroupPhaseStatus.COMMITTING,
    ),
)
def test_core_create_replays_one_pg_transaction_until_created(
    monkeypatch, first_phase
) -> None:
    core = _core()
    requests = []
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)

    def rpc(_address, handler, request):
        if handler == "get_worker_deaths":
            assert isinstance(request, protocol.GetWorkerDeaths)
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, request.after_epoch, ()
            )
        assert handler == "create_placement_group"
        requests.append(request)
        if len(requests) == 1:
            return protocol.CreatePlacementGroupReply(
                request.placement_group_id, request.attempt, False, first_phase,
                error="participant acknowledgement is unresolved",
            )
        key = protocol.PlacementGroupSchedulingKey(
            request.placement_group_id, request.attempt, 0, NodeID.random(),
            "c" * 64,
        )
        return protocol.CreatePlacementGroupReply(
            request.placement_group_id, request.attempt, True,
            protocol.PlacementGroupPhaseStatus.CREATED, (key,),
        )

    core._rpc = rpc
    reply = core.create_placement_group(
        (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
    )

    assert reply.accepted
    assert len(requests) == 2
    assert requests[0] is requests[1]
    assert requests[0].placement_group_id == reply.placement_group_id


@pytest.mark.unit
def test_core_create_waits_for_removed_before_raising_rejection(
    monkeypatch,
) -> None:
    core = _core()
    requests = []
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)

    def rpc(_address, handler, request):
        if handler == "get_worker_deaths":
            assert isinstance(request, protocol.GetWorkerDeaths)
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, request.after_epoch, ()
            )
        assert handler == "create_placement_group"
        requests.append(request)
        phase = (
            protocol.PlacementGroupPhaseStatus.ABORTING
            if len(requests) == 1
            else protocol.PlacementGroupPhaseStatus.REMOVED
        )
        return protocol.CreatePlacementGroupReply(
            request.placement_group_id, request.attempt, False, phase,
            error="participant rejected reservation",
        )

    core._rpc = rpc
    with pytest.raises(SystemTaskError, match="participant rejected"):
        core.create_placement_group(
            (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
        )

    assert len(requests) == 2
    assert requests[0] is requests[1]


@pytest.mark.unit
def test_core_and_gcs_adapter_converge_ambiguous_prepare_with_one_pg_id(
    monkeypatch,
) -> None:
    core = _core()
    nodes = NodeRegistry()
    target_node = NodeID.random()
    nodes.register(
        target_node, ("127.0.0.1", 23001), ResourceVector({"CPU": 1}),
        node_pid=4301,
    )
    first_prepare = None
    create_requests = []

    def participant_rpc(_address, _handler, request):
        nonlocal first_prepare
        if first_prepare is None:
            first_prepare = request
            raise TimeoutError("prepare acknowledgement lost")
        if request.phase is protocol.PlacementGroupParticipantPhase.PREPARE:
            assert request == first_prepare
        reply_type = (
            protocol.PreparePlacementGroupReply
            if request.phase is protocol.PlacementGroupParticipantPhase.PREPARE
            else protocol.CommitPlacementGroupReply
        )
        return reply_type(
            request.placement_group_id, request.attempt, request.node_id,
            request.plan_digest, request.phase, True, True,
        )

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)

    def gcs_rpc(_address, handler, request):
        assert handler == "create_placement_group"
        create_requests.append(request)
        return adapter.create(request)

    core._rpc = gcs_rpc
    reply = core.create_placement_group(
        (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
    )

    assert reply.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert len(create_requests) == 2
    assert create_requests[0] is create_requests[1]
    assert adapter.snapshot(reply.placement_group_id).phase.value == "CREATED"


@pytest.mark.unit
def test_core_and_gcs_adapter_raise_only_after_reject_abort_is_removed(
    monkeypatch,
) -> None:
    core = _core()
    nodes = NodeRegistry()
    target_node = NodeID.random()
    nodes.register(
        target_node, ("127.0.0.1", 23002), ResourceVector({"CPU": 1}),
        node_pid=4302,
    )
    create_requests = []

    def participant_rpc(_address, _handler, request):
        reply_type = (
            protocol.PreparePlacementGroupReply
            if request.phase is protocol.PlacementGroupParticipantPhase.PREPARE
            else protocol.AbortPlacementGroupReply
        )
        if request.phase is protocol.PlacementGroupParticipantPhase.PREPARE:
            return reply_type(
                request.placement_group_id, request.attempt, request.node_id,
                request.plan_digest, request.phase, False, False, "busy",
            )
        return reply_type(
            request.placement_group_id, request.attempt, request.node_id,
            request.plan_digest, request.phase, True, True,
        )

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)
    core._rpc = lambda _address, handler, request: (
        create_requests.append(request) or adapter.create(request)
        if handler == "create_placement_group"
        else None
    )

    with pytest.raises(SystemTaskError, match="busy"):
        core.create_placement_group(
            (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
        )

    pg_id = create_requests[0].placement_group_id
    snapshot = adapter.snapshot(pg_id)
    assert snapshot.phase.value == "REMOVED"
    assert adapter._coordinator.next_operations(pg_id) == ()
    assert len({request.placement_group_id for request in create_requests}) == 1


@pytest.mark.heavy
def test_pg_create_is_a_shutdown_visible_inflight_operation(monkeypatch) -> None:
    core = CoreWorker(
        ("127.0.0.1", 21001), NodeID.random(),
        gcs_address=("127.0.0.1", 21000), event_sink=EventSink(),
        dispatch_lanes=1,
    )
    entered = threading.Event()
    release = threading.Event()
    created = []
    errors = []
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)

    def rpc(_address, handler, request):
        if handler == "get_worker_deaths":
            assert isinstance(request, protocol.GetWorkerDeaths)
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, request.after_epoch, ()
            )
        assert handler == "create_placement_group"
        if not entered.is_set():
            entered.set()
            assert release.wait(1.0)
            return protocol.CreatePlacementGroupReply(
                request.placement_group_id, request.attempt, False,
                protocol.PlacementGroupPhaseStatus.PREPARING,
                error="prepare pending",
            )
        key = protocol.PlacementGroupSchedulingKey(
            request.placement_group_id, request.attempt, 0, NodeID.random(),
            "d" * 64,
        )
        return protocol.CreatePlacementGroupReply(
            request.placement_group_id, request.attempt, True,
            protocol.PlacementGroupPhaseStatus.CREATED, (key,),
        )

    core._rpc = rpc

    def create():
        try:
            created.append(core.create_placement_group(
                (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
            ))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=create)
    thread.start()
    try:
        assert entered.wait(1.0)
        assert core._inflight_pg_control_ops == 1
        assert not core.can_finalize_shutdown()
        # The short drain closes admission but cannot finalize while the PG
        # transaction still needs GCS to finish prepare/commit.
        assert not core.shutdown(timeout=0.01, preserve_owner_protocol=True)
        assert not core.can_finalize_shutdown()
        release.set()
        thread.join(1.0)
        assert not thread.is_alive()
        assert created == []
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeShuttingDownError)
        assert core._inflight_pg_control_ops == 0
        assert core.shutdown(timeout=1.0, preserve_owner_protocol=True)
        assert core.can_finalize_shutdown()
    finally:
        release.set()
        thread.join(1.0)


@pytest.mark.unit
def test_pg_control_admission_rejects_after_shutdown_fence() -> None:
    core = _core()
    core._accepting = False

    with pytest.raises(RuntimeShuttingDownError, match="shutting down"):
        core.create_placement_group(
            (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
        )
    with pytest.raises(RuntimeShuttingDownError, match="shutting down"):
        core.remove_placement_group(PlacementGroupID.random(), 0)
    assert core._inflight_pg_control_ops == 0


@pytest.mark.unit
def test_core_remove_fences_tasks_and_replays_until_removed(monkeypatch) -> None:
    core = _core()
    pg_id = PlacementGroupID.random()
    attempt = 0
    state_key = pg_id, attempt
    core._placement_group_states[state_key] = (
        protocol.PlacementGroupPhaseStatus.CREATED
    )
    requests = []
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)

    def rpc(_address, handler, request):
        assert handler == "remove_placement_group"
        requests.append(request)
        phase = (
            protocol.PlacementGroupPhaseStatus.REMOVING
            if len(requests) == 1
            else protocol.PlacementGroupPhaseStatus.REMOVED
        )
        return protocol.RemovePlacementGroupReply(
            request.placement_group_id, request.attempt,
            True,
            phase is protocol.PlacementGroupPhaseStatus.REMOVED,
            phase,
        )

    core._rpc = rpc
    removed = core.remove_placement_group(pg_id, attempt)

    assert removed.accepted and removed.removed
    assert len(requests) == 2 and requests[0] is requests[1]
    assert core._placement_group_states[state_key] is (
        protocol.PlacementGroupPhaseStatus.REMOVED
    )
    with pytest.raises(ValueError, match="not active"):
        core.assert_placement_group_task_admissible(pg_id, attempt)


@pytest.mark.unit
def test_core_pg_task_admission_accepts_only_created_state() -> None:
    core = _core()
    pg_id = PlacementGroupID.random()
    state_key = pg_id, 0
    core._placement_group_states[state_key] = (
        protocol.PlacementGroupPhaseStatus.CREATED
    )
    core.assert_placement_group_task_admissible(pg_id, 0)

    core._placement_group_states[state_key] = (
        protocol.PlacementGroupPhaseStatus.REMOVING
    )
    with pytest.raises(ValueError, match="REMOVING"):
        core.assert_placement_group_task_admissible(pg_id, 0)


@pytest.mark.unit
def test_rejected_removing_reply_is_terminal_not_an_in_progress_replay(
    monkeypatch,
) -> None:
    core = _core()
    pg_id = PlacementGroupID.random()
    core._placement_group_states[(pg_id, 0)] = (
        protocol.PlacementGroupPhaseStatus.CREATED
    )
    calls = 0
    monkeypatch.setattr(
        core, "_wait_for_placement_group_retry",
        lambda _: (_ for _ in ()).throw(
            AssertionError("terminal rejection must not retry")
        ),
    )

    def rpc(_address, _handler, request):
        nonlocal calls
        calls += 1
        return protocol.RemovePlacementGroupReply(
            request.placement_group_id, request.attempt, False, False,
            protocol.PlacementGroupPhaseStatus.REMOVING,
            "stale removal identity",
        )

    core._rpc = rpc
    with pytest.raises(SystemTaskError, match="stale removal"):
        core.remove_placement_group(pg_id, 0)
    assert calls == 1
    assert core._placement_group_states[(pg_id, 0)] is (
        protocol.PlacementGroupPhaseStatus.REMOVING
    )


@pytest.mark.unit
def test_core_and_adapter_replay_pending_against_fresh_capacity(
    monkeypatch,
) -> None:
    core = _core()
    nodes = NodeRegistry()
    target_node = NodeID.random()
    total = ResourceVector({"CPU": 1})
    nodes.register(
        target_node, ("127.0.0.1", 24001), total,
        node_pid=4401,
        available_resources=ResourceVector(),
    )
    registration = nodes.get(target_node)
    requests = []

    def participant_rpc(_address, _handler, request):
        reply_type = (
            protocol.PreparePlacementGroupReply
            if request.phase is protocol.PlacementGroupParticipantPhase.PREPARE
            else protocol.CommitPlacementGroupReply
        )
        return reply_type(
            request.placement_group_id, request.attempt, request.node_id,
            request.plan_digest, request.phase, True, True,
        )

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )

    def make_capacity_available(round_number):
        assert round_number == 1
        nodes.update_resources(
            target_node, registration.node_pid, registration.registration_epoch,
            1, total,
        )

    monkeypatch.setattr(
        core, "_wait_for_placement_group_retry", make_capacity_available
    )

    def rpc(_address, handler, request):
        assert handler == "create_placement_group"
        requests.append(request)
        return adapter.create(request)

    core._rpc = rpc
    reply = core.create_placement_group(
        (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
    )

    assert reply.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert len(requests) == 2 and requests[0] is requests[1]
    assert requests[0].placement_group_id == reply.placement_group_id
    assert requests[0].attempt == reply.attempt == 0


@pytest.mark.unit
def test_core_replays_accepted_created_with_incomplete_manifest(
    monkeypatch,
) -> None:
    core = _core()
    requests = []
    monkeypatch.setattr(core, "_wait_for_placement_group_retry", lambda _: None)

    def rpc(_address, handler, request):
        assert handler == "create_placement_group"
        requests.append(request)
        keys = tuple(
            protocol.PlacementGroupSchedulingKey(
                request.placement_group_id, request.attempt, index,
                NodeID.random(), format(index + 1, "064x"),
            )
            for index in range(2)
        )
        manifest = keys[:1] if len(requests) == 1 else keys
        return protocol.CreatePlacementGroupReply(
            request.placement_group_id, request.attempt, True,
            protocol.PlacementGroupPhaseStatus.CREATED, manifest,
        )

    core._rpc = rpc
    reply = core.create_placement_group(
        (ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1})),
        PlacementStrategy.STRICT_SPREAD,
    )

    assert tuple(key.bundle_index for key in reply.placements) == (0, 1)
    assert len(requests) == 2 and requests[0] is requests[1]


@pytest.mark.heavy
def test_pg_submission_final_publication_fences_removal_and_rolls_back_hold(
    monkeypatch, request: pytest.FixtureRequest,
) -> None:
    core = _core()
    add_core_thread_finalizer(request, core)
    source_pending, source = core._register_submission(
        RemoteFunctionDefinition.from_callable(lambda: 1, core.job_id),
        (), {}, ResourceVector(),
    )
    key = _scheduling_key(NodeID.random())
    state_key = key.placement_group_id, key.attempt
    core._placement_group_states[state_key] = (
        protocol.PlacementGroupPhaseStatus.CREATED
    )
    encoded = threading.Event()
    release = threading.Event()
    errors = []
    real_encode = core_module.encode_task_argument

    def pause_after_encode(*args, **kwargs):
        result = real_encode(*args, **kwargs)
        encoded.set()
        assert release.wait(1.0)
        return result

    monkeypatch.setattr(core_module, "encode_task_argument", pause_after_encode)

    def submit():
        try:
            core.submit(
                RemoteFunctionDefinition.from_callable(lambda value: value, core.job_id),
                ([source],), {}, ResourceVector(),
                placement_group_scheduling_key=key,
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=submit)
    thread.start()
    try:
        assert encoded.wait(1.0)
        submitted_holds = core.owner_table.snapshot(
            source_pending.object_id
        ).submitted_tokens
        assert len(submitted_holds) == 1
        hold = next(iter(submitted_holds))
        assert hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert hold.submitting_worker_id == core.worker_id
        assert hold.origin_attempt_id == AttemptID(hold.task_id, 0)
        with core._state_lock:
            core._placement_group_states[state_key] = (
                protocol.PlacementGroupPhaseStatus.REMOVING
            )
        release.set()
        thread.join(1.0)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        assert "not active at task publication" in str(errors[0])
        assert not core.owner_table.snapshot(
            source_pending.object_id
        ).submitted_tokens
        assert core._submissions.empty()
        assert core._accepted_task_count == 0
        assert len(core._objects) == 1
    finally:
        release.set()
        thread.join(1.0)
        source.close()


@pytest.mark.heavy
@pytest.mark.parametrize("operation", ("create", "remove"))
def test_pg_control_timeout_hands_off_at_shutdown_fence(
    monkeypatch, operation
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 25001), NodeID.random(),
        gcs_address=("127.0.0.1", 25000), event_sink=EventSink(),
        dispatch_lanes=1,
    )
    entered_retry = threading.Event()
    release_retry = threading.Event()
    errors = []
    pg_id = PlacementGroupID.random()
    if operation == "remove":
        core._placement_group_states[(pg_id, 0)] = (
            protocol.PlacementGroupPhaseStatus.CREATED
        )

    def rpc(_address, handler, request):
        if handler == "get_worker_deaths":
            assert isinstance(request, protocol.GetWorkerDeaths)
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, request.after_epoch, ()
            )
        raise TransportTimeout("GCS reply remains ambiguous")

    core._rpc = rpc

    def block_retry(_round):
        entered_retry.set()
        assert release_retry.wait(1.0)

    monkeypatch.setattr(core, "_wait_for_placement_group_retry", block_retry)

    def invoke():
        try:
            if operation == "create":
                core.create_placement_group(
                    (ResourceVector({"CPU": 1}),), PlacementStrategy.PACK
                )
            else:
                core.remove_placement_group(pg_id, 0)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=invoke)
    thread.start()
    try:
        assert entered_retry.wait(1.0)
        assert core._inflight_pg_control_ops == 1
        assert not core.shutdown(timeout=0.01, preserve_owner_protocol=True)
        release_retry.set()
        thread.join(1.0)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeShuttingDownError)
        assert core._inflight_pg_control_ops == 0
        assert core.shutdown(timeout=1.0, preserve_owner_protocol=True)
    finally:
        release_retry.set()
        thread.join(1.0)
