"""Pure Core locality hints and their boundary with real Node lease decisions.

Each case owns two unstarted 1-KiB Nodes and one threadless Core. Local stored
values are physically sealed before the real owner table advertises them; no
Task success, output publication, or reconstruction completion is fabricated.
Foreign descriptors in route-only cases are metadata inputs, not ownership
transfers or evidence of foreign task execution.

Execution cases admit one consumer, use real Hybrid/Grant/custody/Start and
SYSTEM_ERROR Complete reducers, and finish through the real Core authority.
At most one extra unstarted lease occupies a Worker, four exact lease sends
occur, and one <=64-byte dependency uses three direct transfer calls. Delayed
replays are consumed explicitly once; no timer, wait, thread, socket, process,
user function, output-publication or full-runtime shutdown runs.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import control, core as core_module, node as node_module, protocol, transport
from miniray.control import NodeRegistry, PlacementGroupControlCoordinator
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, _DelayedReadyTask, _ObjectWaiter, _RPC_CALL_DEADLINE
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.ownership import ObjectState
from miniray.resources import HybridPolicy, ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_node_placement_group_runtime import _node


pytestmark = pytest.mark.unit


def _forbidden(*_args, **_kwargs):
    pytest.fail("pure Core locality test attempted runtime or unmodelled work")


def _never_execute(*_args):
    _forbidden("no user code is executed by this fixture")


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, _forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, _forbidden)
    monkeypatch.setattr(subprocess, "Popen", _forbidden)
    monkeypatch.setattr(time, "sleep", _forbidden)
    for module in (control, core_module, node_module):
        monkeypatch.setattr(module, "rpc_request", _forbidden)
    monkeypatch.setattr(transport, "request", _forbidden)


class _Case:
    def __init__(self, monkeypatch, *, snapshot=True):
        self.core = core = make_pure_core()
        self.home, self.peer = self.nodes = (_node(), _node())
        self.registry = NodeRegistry()
        self.refs, self.leases, self.pushes, self.transfers = [], [], [], []
        self.participant_calls = []
        self.lose_lease_acks = 0
        self.object_index = 0
        self.pg = None
        for index, node in enumerate(self.nodes):
            node._node_pid = 31401 + index
            node._server = SimpleNamespace(address=("locality-{}.invalid".format(index), 1))
            node._workers[node.worker_id].address = ("locality-worker-{}.invalid".format(index), 1)
            node._object_manager = ObjectManager(node.node_id, node.object_store)
            node._scheduling_policy = HybridPolicy()
            node._gcs_address = ("locality-control.invalid", 1)
            # No resource-report transport or Worker registration service is
            # started. Registry and Node handlers below are direct reducers.
            assert not node._registered_with_gcs
            assert self.registry.register(
                node.node_id, node.address, node.resource_ledger.total, node_pid=node._node_pid,
            )
            node._registration_epoch = self.registry.get(node.node_id).registration_epoch
        epoch, infos = self.registry.live_snapshot()
        self.snapshot = protocol.InstallClusterSnapshot(epoch, "locality-two-nodes", infos)
        for node in self.nodes:
            assert node._handle_install_cluster_snapshot(self.snapshot).installed
        core.node_id, core.node_address = self.home.node_id, self.home.address
        core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
        core.gcs_address = ("locality-control.invalid", 1)
        core._membership_epoch = epoch
        core._installed_cluster_snapshot = self.snapshot if snapshot else None
        core._registered_functions = set()
        core._rpc, core._push_task_rpc = self.rpc, self.push
        monkeypatch.setattr(node_module, "rpc_request", self.transfer)

    @property
    def route(self):
        route = self.core._home_route_snapshot()
        assert route is not None
        return route

    def stored(self, *nodes, payload=b"tiny-locality-input"):
        """Owner-held put identity, backed by the exact real sealed bytes."""
        assert 1 <= len(nodes) <= 2 and len(payload) <= 64
        core = self.core
        # This physical put fixture and public Core.put share the same logical
        # identity namespace; reserve its real sequence before registration.
        task = TaskID.for_put(core.job_id, core.worker_id, core._put_index)
        core._put_index += 1
        object_id, attempt = ObjectID.for_task(task), AttemptID(task, 0)
        seal = protocol.SealObject.from_data(object_id, attempt, core.worker_id, payload)
        core.owner_table.register(object_id, current_attempt=attempt)
        assert core._recovery.register_put(object_id)
        core._objects[object_id] = _ObjectWaiter(threading.Event())
        for node in nodes:
            assert node._handle_seal_object(seal).sealed
            result = protocol.ResultDescriptor(
                object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
                core.worker_id, node.node_id, seal.checksum,
            )
            assert core.owner_table.publish_stored(object_id, attempt, node.node_id, descriptor=result)
        canonical = core.owner_table.snapshot(object_id).canonical_stored_result
        assert canonical is not None and canonical.node_id == nodes[0].node_id
        core._stored_descriptors[object_id] = canonical
        core._objects[object_id].event.set()
        ref = core._new_object_ref(object_id)
        self.refs.append(ref)
        descriptor = protocol.ObjectStoreDescriptor(
            object_id, core.worker_id, attempt, nodes[0].node_id, len(payload), seal.checksum,
        )
        return ref, descriptor

    def add_replica(self, descriptor, node):
        source = next(item for item in self.nodes if item.node_id == descriptor.node_id)
        payload = source.object_store.get(descriptor.object_id)
        seal = protocol.SealObject.from_data(
            descriptor.object_id, descriptor.producer_attempt_id, descriptor.owner_worker_id, payload,
        )
        assert node._handle_seal_object(seal).sealed
        result = protocol.ResultDescriptor(
            descriptor.object_id, protocol.ResultStorage.OBJECT_STORE, descriptor.size_bytes,
            descriptor.owner_worker_id, node.node_id, descriptor.checksum,
        )
        assert self.core.owner_table.add_location(
            descriptor.object_id, descriptor.producer_attempt_id, node.node_id, descriptor=result,
        )

    def foreign(self, node_id, *, size=7):
        task = TaskID.derive(self.core.job_id, self.core.driver_task_id, 100 + self.object_index)
        self.object_index += 1
        return protocol.ObjectStoreDescriptor(
            ObjectID.for_task(task), WorkerID.random(), AttemptID(task, 0), node_id,
            size, hashlib.sha256(b"f" * size).hexdigest(),
        )

    def lose(self, node):
        reply = self.registry.report_death(protocol.ReportNodeDeath(
            "locality-death", node.node_id, node._node_pid, node._registration_epoch,
            -9, protocol.NodeDeathReason.PROCESS_EXIT, "pure membership reducer input",
        ))
        assert reply.disposition is protocol.NodeDeathDisposition.APPLIED
        assert reply.death is not None
        snapshot = protocol.InstallClusterSnapshot(
            reply.membership_epoch, "locality-survivors", reply.live_nodes,
        )
        for survivor in self.nodes:
            if survivor is not node:
                assert survivor._handle_install_cluster_snapshot(snapshot).installed
        self.core.handle_node_death(reply.death, snapshot)
        return reply.death

    def submit(self, ref, *, key=None):
        pending, output = self.core._register_submission(
            self.core.define_remote_function(_never_execute), (ref,), {}, ResourceVector({"CPU": 1}),
            max_retries=0, placement_group_scheduling_key=key, _enqueue=True,
        )
        self.refs.append(output)
        assert self.core._submissions.get_nowait() is pending
        self.core._submissions.task_done()
        prepared, dependencies, protected = self.core._prepare_task_dependencies(pending.spec)
        assert protected == pending.protected_dependencies and len(dependencies) == 1
        return pending, prepared, dependencies

    def occupy(self, node, *, cpu):
        task = TaskID.derive(self.core.job_id, self.core.driver_task_id, 501)
        request = protocol.RequestWorkerLease(
            LeaseID.random(), task, AttemptID(task, 0), ResourceVector({"CPU": cpu}),
            self.core.node_id, self.core.worker_id, target_node_id=node.node_id,
            return_ids=(ObjectID.for_task(task),),
        )
        grant = node._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease
        return grant

    def release(self, node, grant):
        reply = node._handle_release_lease(protocol.ReleaseWorkerLease(
            grant.lease_id, grant.worker_id, grant.allocation_token,
        ))
        assert reply.released
        assert node._leases[grant.lease_id].state is protocol.LeaseExecutionState.ABANDONED

    def rpc(self, address, handler, request):
        if handler == control.CREATE_PLACEMENT_GROUP_HANDLER:
            assert address == self.core.gcs_address and self.pg is not None
            return self.pg.create(request)
        node = next(item for item in self.nodes if item.address == address)
        if handler == node_module.REQUEST_LEASE_HANDLER:
            assert len(self.leases) < 4
            result = node._handle_request_lease(request)
            self.leases.append((node.node_id, request, result))
            if self.lose_lease_acks:
                assert type(result) is protocol.GrantWorkerLease
                self.lose_lease_acks -= 1
                raise TransportTimeout("real grant committed before its ACK was lost")
            return result
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        reply = node._handle_ack_lease_dependency_custody(request)
        assert reply.accepted
        return reply

    def transfer(self, address, handler, request, **_options):
        assert len(self.transfers) < 3
        node = next(item for item in self.nodes if item.address == address)
        operations = {
            node_module.PIN_OBJECT_HANDLER: node._handle_pin_object_for_transfer,
            node_module.GET_OBJECT_CHUNK_HANDLER: node._handle_get_object_chunk,
            node_module.RELEASE_OBJECT_PIN_HANDLER: node._handle_release_object_pin,
        }
        assert handler in operations
        self.transfers.append((node.node_id, handler))
        return operations[handler](request)

    def push(self, address, handler, push):
        assert not self.pushes and handler == "push_task"
        node = next(item for item in self.nodes if item._workers[item.worker_id].address == address)
        record = node._leases[push.lease_id]
        assert record.grant.worker_id == push.worker_id
        assert push.dependencies == record.grant.dependencies
        start = node._handle_start_worker_lease(protocol.StartWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
            push.spec.scheduling_key,
        ))
        assert start.accepted and start.state is protocol.LeaseExecutionState.RUNNING
        complete = protocol.CompleteWorkerLease(
            push.lease_id, push.spec.task_id, push.spec.attempt_id, push.worker_id,
            protocol.TaskReplyStatus.SYSTEM_ERROR, push.spec.scheduling_key,
        )
        completed = node._handle_complete_worker_lease(complete)
        assert completed.accepted and completed.released
        assert completed.state is protocol.LeaseExecutionState.COMPLETED
        assert completed.output_publication is None and completed.output_completion is None
        self.pushes.append((node.node_id, push, completed))
        return protocol.TaskReply(
            push.spec.task_id, push.spec.attempt_id, push.worker_id, completed.status,
            error=protocol.RemoteErrorInfo("SystemTaskError", "explicit failure after real Node Complete"),
        )

    def take_delayed(self):
        item = self.core._submissions.get_nowait()
        self.core._submissions.task_done()
        assert type(item) is _DelayedReadyTask and self.core._submissions.empty()
        return item.ready

    def finish(self, pending):
        snapshot = self.core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.ERROR and isinstance(snapshot.error, SystemTaskError)
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert self.core._recovery.task_record(pending.task_id).retries_started == 0
        assert not self.core._protocol_unresolved
        assert self.core._finish_pending_task(pending)
        assert self.core._accepted_task_count == 0 and not self.core._task_finish_barriers
        assert pending.task_key in self.core._finished_tasks

    def placement_group(self):
        def participant(address, handler, request):
            assert len(self.participant_calls) < 4
            node = next(item for item in self.nodes if item.address == address)
            operations = {
                control.PREPARE_PLACEMENT_GROUP_HANDLER: node._handle_prepare_placement_group,
                control.COMMIT_PLACEMENT_GROUP_HANDLER: node._handle_commit_placement_group,
            }
            assert handler in operations
            result = operations[handler](request)
            assert result.accepted and result.applied
            self.participant_calls.append((handler, request))
            return result

        self.pg = PlacementGroupControlCoordinator(self.registry, participant_rpc=participant)
        created = self.core.create_placement_group(
            (ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1})), "STRICT_SPREAD",
        )
        assert created.accepted and created.phase is protocol.PlacementGroupPhaseStatus.CREATED
        assert len(self.participant_calls) == 4
        return next(key for key in created.placements if key.node_id == self.peer.node_id)

    def close(self):
        for ref in self.refs:
            if not ref.closed:
                # Run the actual finalizer/mailbox release, without Event.wait
                # or substituting a success receipt. Stored GC stays explicit.
                done = ref._release_done
                ref._closed = True
                ref._finalizer()
                assert not ref._finalizer.alive and done is not None and done.is_set()
        close_pure_core(self.core)


def test_local_owner_replicas_score_once_and_keep_canonical_source(monkeypatch):
    case = _Case(monkeypatch)
    try:
        _, shared = case.stored(case.home, case.peer, payload=b"shared-by-both")
        _, peer_only = case.stored(case.peer, payload=b"extra")
        before = case.core.owner_table.snapshot(shared.object_id)
        descriptors = (replace(shared, node_id=case.peer.node_id), peer_only)
        assert before.canonical_stored_result.node_id == case.home.node_id
        assert case.core._first_lease_route(descriptors, home_route=case.route) == (case.peer.node_id, case.peer.address)
        assert case.core._first_lease_route((shared, shared), home_route=case.route) == (case.home.node_id, case.home.address)
        assert case.core.owner_table.snapshot(shared.object_id) == before
        assert case.core._stored_descriptors[shared.object_id] == before.canonical_stored_result
        # A real physical retirement of only the original replica leaves its
        # immutable publisher identity intact. That identity need not still
        # occur in the owner location set for a secondary to be a useful hint.
        dropped = case.home._handle_drop_object_replica(protocol.DropObjectReplica(
            shared.object_id, shared.producer_attempt_id, shared.owner_worker_id,
            case.home.node_id, shared.checksum,
        ))
        assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
        assert case.core.owner_table.remove_location(
            shared.object_id, shared.producer_attempt_id, case.home.node_id,
        )
        secondary = replace(shared, node_id=case.peer.node_id)
        assert case.core._first_lease_route((secondary,), home_route=case.route) == (case.peer.node_id, case.peer.address)
        after = case.core.owner_table.snapshot(shared.object_id)
        assert after.locations == frozenset((case.peer.node_id,))
        assert after.canonical_stored_result == before.canonical_stored_result
        assert not case.leases and not case.transfers
    finally:
        case.close()


def test_local_descriptor_epoch_content_and_owner_state_must_match(monkeypatch):
    case = _Case(monkeypatch)
    try:
        _, descriptor = case.stored(case.peer)
        before = case.core.owner_table.snapshot(descriptor.object_id)
        assert case.core._first_lease_route((descriptor,), home_route=case.route)[0] == case.peer.node_id
        other_task = TaskID.derive(case.core.job_id, case.core.driver_task_id, 701)
        inline = case.core.put(7)
        case.refs.append(inline)
        variants = (
            replace(descriptor, producer_attempt_id=descriptor.producer_attempt_id.next()),
            replace(descriptor, size_bytes=descriptor.size_bytes + 1),
            replace(descriptor, checksum="00" * 32),
            replace(descriptor, object_id=ObjectID.for_task(other_task), producer_attempt_id=AttemptID(other_task, 0)),
            replace(descriptor, object_id=inline.object_id,
                    producer_attempt_id=case.core.owner_table.snapshot(inline.object_id).current_attempt),
        )
        for candidate in variants:
            # A missing hint is not a dependency failure or permission to use
            # the stale source. No owner state or passed descriptor is changed.
            assert case.core._first_lease_route((candidate,), home_route=case.route) == (case.home.node_id, case.home.address)
        assert case.core.owner_table.snapshot(descriptor.object_id) == before
        assert not case.leases and not case.transfers
    finally:
        case.close()


def test_foreign_hint_uses_only_its_descriptor_source_without_owner_query(monkeypatch):
    case = _Case(monkeypatch)
    try:
        descriptor = case.foreign(case.peer.node_id)
        monkeypatch.setattr(case.core.owner_table, "snapshot", _forbidden)
        monkeypatch.setattr(case.core, "_resolve_node_address", _forbidden)
        assert case.core._first_lease_route((descriptor,), home_route=case.route) == (case.peer.node_id, case.peer.address)
        assert not case.leases and not case.transfers
    finally:
        case.close()


def test_installed_snapshot_filters_unknown_dead_and_zero_byte_hints(monkeypatch):
    case = _Case(monkeypatch)
    try:
        monkeypatch.setattr(case.core, "_resolve_node_address", _forbidden)
        unknown = case.foreign(NodeID.random(), size=64)
        zero = case.foreign(case.peer.node_id, size=0)
        assert case.core._first_lease_route((unknown, zero), home_route=case.route) == (case.home.node_id, case.home.address)
        descriptor = case.foreign(case.peer.node_id)
        case.core._lease_locality_addresses = {case.peer.node_id: case.peer.address}
        death = case.lose(case.peer)
        assert case.core._first_lease_route((descriptor,), home_route=case.route) == (case.home.node_id, case.home.address)
        assert case.core._dead_nodes == {case.peer.node_id: death}
        assert case.core._first_lease_route((), home_route=case.route) == (case.home.node_id, case.home.address)
    finally:
        case.close()


def test_cold_lookup_has_short_deadline_positive_cache_and_restores_context(monkeypatch):
    case = _Case(monkeypatch, snapshot=False)
    calls = []
    parent = time.monotonic() + 0.25
    token = _RPC_CALL_DEADLINE.set(parent)
    try:
        descriptor = case.foreign(case.peer.node_id)

        def resolve(node_id, *, home_route=None):
            assert not case.core._state_lock._is_owned()
            deadline = _RPC_CALL_DEADLINE.get()
            assert time.monotonic() <= deadline <= parent
            calls.append(node_id)
            return case.peer.address

        monkeypatch.setattr(case.core, "_resolve_node_address", resolve)
        for _ in range(2):
            assert case.core._first_lease_route((descriptor,), home_route=case.route) == (case.peer.node_id, case.peer.address)
            assert _RPC_CALL_DEADLINE.get() == parent
        assert calls == [case.peer.node_id]
        assert case.core._lease_locality_addresses == {case.peer.node_id: case.peer.address}
        assert not case.core._dead_nodes and case.core.worker_id != descriptor.owner_worker_id
    finally:
        _RPC_CALL_DEADLINE.reset(token)
        case.close()


def test_cold_lookup_failure_or_malformed_endpoint_is_only_a_missing_hint(monkeypatch):
    case = _Case(monkeypatch, snapshot=False)
    try:
        descriptor = case.foreign(case.peer.node_id)
        before_context = _RPC_CALL_DEADLINE.get()
        calls = []
        # Three fixed observations, no retry loop or negative death cache.
        for answer in (SystemTaskError("address unavailable"), ("peer.invalid", 0), ("", 1)):
            def resolve(node_id, *, home_route=None):
                deadline = _RPC_CALL_DEADLINE.get()
                assert deadline is not None and deadline <= time.monotonic() + 0.75
                calls.append(node_id)
                if isinstance(answer, Exception):
                    raise answer
                return answer

            monkeypatch.setattr(case.core, "_resolve_node_address", resolve)
            assert case.core._first_lease_route((descriptor,), home_route=case.route) == (case.home.node_id, case.home.address)
            assert _RPC_CALL_DEADLINE.get() == before_context
            assert not getattr(case.core, "_lease_locality_addresses", {})
            assert not case.core._dead_nodes
        assert calls == [case.peer.node_id] * 3
    finally:
        case.close()


def test_death_during_cold_lookup_fences_its_successful_address(monkeypatch):
    case = _Case(monkeypatch, snapshot=False)
    try:
        descriptor = case.foreign(case.peer.node_id)
        calls = []
        owner_identity = case.core.worker_id

        def resolve(node_id, *, home_route=None):
            assert not case.core._state_lock._is_owned()
            calls.append(node_id)
            case.lose(case.peer)
            return case.peer.address

        monkeypatch.setattr(case.core, "_resolve_node_address", resolve)
        assert case.core._first_lease_route((descriptor,), home_route=case.route) == (case.home.node_id, case.home.address)
        assert calls == [case.peer.node_id] and case.core.worker_id == owner_identity
        assert case.peer.node_id in case.core._dead_nodes
        assert not getattr(case.core, "_lease_locality_addresses", {})
    finally:
        case.close()


def test_snapshot_installed_during_lookup_wins_over_optional_address(monkeypatch):
    case = _Case(monkeypatch, snapshot=False)
    try:
        descriptor = case.foreign(case.peer.node_id)

        def resolve(node_id, *, home_route=None):
            assert node_id == case.peer.node_id and not case.core._state_lock._is_owned()
            # The Node copies were already installed by the real handlers at
            # construction. Deliver their same authoritative view to Core now.
            with case.core._state_lock:
                case.core._installed_cluster_snapshot = case.snapshot
            return ("stale-optional-address.invalid", 1)

        monkeypatch.setattr(case.core, "_resolve_node_address", resolve)
        assert case.core._first_lease_route((descriptor,), home_route=case.route) == (case.peer.node_id, case.peer.address)
        assert not getattr(case.core, "_lease_locality_addresses", {})
        assert not case.core._dead_nodes
    finally:
        case.close()


def test_locality_first_hop_can_really_spill_back_to_requester_home(monkeypatch):
    case = _Case(monkeypatch)
    try:
        ref, descriptor = case.stored(case.peer)
        occupied = case.occupy(case.peer, cpu=1)
        pending, prepared, dependencies = case.submit(ref)
        assert case.core._execute(pending, prepared, dependencies)
        assert len(case.leases) == 2
        first_node, first, spill = case.leases[0]
        final_node, targeted, grant = case.leases[1]
        assert first_node == case.peer.node_id and first.requester_node_id == case.home.node_id
        assert first.preferred_node_id == case.peer.node_id and first.target_node_id is None
        assert type(spill) is protocol.SpillbackWorkerLease and spill.target_node_id == case.home.node_id
        assert final_node == case.home.node_id and targeted == replace(first, target_node_id=case.home.node_id)
        assert type(grant) is protocol.GrantWorkerLease and case.pushes[0][0] == case.home.node_id
        assert first.dependencies == targeted.dependencies == (descriptor,)
        assert len(case.transfers) == 3
        assert case.core.owner_table.snapshot(ref.object_id).locations == frozenset(node.node_id for node in case.nodes)
        assert case.core.node_id == case.home.node_id and case.core._home_route_snapshot().node_id == case.home.node_id
        case.finish(pending)
        case.release(case.peer, occupied)
        assert all(node.resource_ledger.available == node.resource_ledger.total for node in case.nodes)
    finally:
        case.close()


@pytest.mark.parametrize("reason", ("ack_loss", "capacity"))
def test_existing_first_hop_replay_ignores_new_locality_hints(monkeypatch, reason):
    case = _Case(monkeypatch)
    try:
        ref, descriptor = case.stored(case.peer)
        occupied = None
        if reason == "ack_loss":
            case.lose_lease_acks = 3
        else:
            # An unstarted zero-CPU lease occupies the only Worker. Resource
            # scores still tie, so Hybrid selects preferred B and its actual
            # slot check returns PENDING_CAPACITY (not a fabricated rejection).
            occupied = case.occupy(case.peer, cpu=0)
        pending, prepared, dependencies = case.submit(ref)
        assert not case.core._execute(pending, prepared, dependencies)
        ready = case.take_delayed()
        state = ready.lease_state
        assert state is not None and state.expected_node_id == case.peer.node_id
        assert state.address == case.peer.address and state.allow_spillback
        assert state.request.target_node_id is None and state.request.preferred_node_id == case.peer.node_id
        assert state.request.requester_node_id == case.home.node_id
        assert all(request == state.request for _, request, _ in case.leases)
        if reason == "ack_loss":
            assert len(case.leases) == 3 and ready.ambiguity_round == 1
            assert case.core._protocol_unresolved[pending.task_key].target_node_id == case.peer.node_id
            assert len(case.peer._leases) == 1
        else:
            assert len(case.leases) == 1 and ready.pending.capacity_round == 1
            assert case.leases[0][2].reason is protocol.LeaseRejectReason.PENDING_CAPACITY
            assert not case.core._protocol_unresolved and occupied is not None
            case.release(case.peer, occupied)
        case.add_replica(descriptor, case.home)
        # A genuinely fresh selector would now prefer home on equal bytes.
        assert case.core._first_lease_route(dependencies, home_route=case.route)[0] == case.home.node_id
        monkeypatch.setattr(case.core, "_first_lease_route", _forbidden)
        assert case.core._execute(
            ready.pending, ready.spec, ready.dependencies, lease_state=state, ambiguity_round=ready.ambiguity_round,
        )
        assert all(node == case.peer.node_id and request == state.request for node, request, _ in case.leases)
        assert len(case.pushes) == 1 and case.pushes[0][0] == case.peer.node_id
        assert not case.transfers
        case.finish(ready.pending)
        assert all(node.resource_ledger.available == node.resource_ledger.total for node in case.nodes)
    finally:
        case.close()


def test_committed_pg_capability_bypasses_locality_and_hybrid(monkeypatch):
    case = _Case(monkeypatch)
    try:
        ref, _ = case.stored(case.home)
        key = case.placement_group()
        pending, prepared, dependencies = case.submit(ref, key=key)
        assert case.core._first_lease_route(dependencies, home_route=case.route)[0] == case.home.node_id
        monkeypatch.setattr(case.core, "_first_lease_route", _forbidden)
        monkeypatch.setattr(case.peer._scheduling_policy, "schedule", _forbidden)
        monkeypatch.setattr(case.core, "_resolve_node_address", lambda node_id, **_kwargs: (
            case.peer.address if node_id == case.peer.node_id else _forbidden(node_id)
        ))
        assert case.core._execute(pending, prepared, dependencies)
        assert len(case.leases) == 1 and case.leases[0][0] == case.peer.node_id
        request = case.leases[0][1]
        assert request.scheduling_key == key and request.target_node_id == request.preferred_node_id == key.node_id
        assert request.requester_node_id == case.home.node_id
        assert len(case.transfers) == 3 and case.pushes[0][0] == case.peer.node_id
        case.finish(pending)
        child = case.peer._bundle_reservations.ledger_for(key.placement_group_id, key.attempt, key.bundle_index)
        assert child.available == child.total == ResourceVector({"CPU": 1})
        # Committed bundles still own their root resources. This test is not a
        # PG removal or full cluster shutdown gate and does not erase ledgers.
        assert case.peer.resource_ledger.available == ResourceVector({"CPU": 1})
    finally:
        case.close()
