"""Bounded put/home contracts with real local storage and death reducers.

One threadless Core, two 1-KiB Nodes and one <=128-byte put per case. A simulated
process-exit observation enters the real NodeRegistry; the survivor installs
its real snapshot before Core.handle_node_death installs the fence and route.
This proves the protocol boundary, not an OS process exit. A dead Node's store
remains an inaccessible fixture snapshot; no fake deletion models its death.
The actual graph authority adds at most 32 metadata callbacks, separately
from the original five physical calls; no service process is started.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import control, core as core_module, node as node_module, protocol, transport, worker
from miniray import enhanced_publication as ep
from miniray.control import NodeRegistry
from miniray.core import CoreWorker, _HomeRoute, _NodeDeathObserved, _WAKE_COORDINATOR
from miniray.errors import NodeDiedError
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.put_work import PutChoice
from miniray.resources import ResourceLedger, ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.unit


class _Homes:
    def __init__(self):
        self.core = core = make_pure_core()
        self.registry = NodeRegistry()
        self.authority = core._test_publication_authority
        core._test_publication_nodes = self.registry
        self.gcs_calls = []
        core.gcs_address = ("put-home-gcs.invalid", 23000)
        self.nodes, self.references, self.calls = [], [], []
        self.deaths, self.mode, self.failed_seals = [], None, 0
        for index in range(2):
            node = object.__new__(NodeServer)
            node.node_id = core.node_id if index == 0 else NodeID.random()
            node._server = SimpleNamespace(address=("home-{}.invalid".format(index), 23001 + index))
            node._node_pid = 7001 + index
            node._state_lock = threading.RLock()
            node._stop_event = threading.Event()
            node._shutdown_request_id = None
            node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
            node._object_store = ObjectStore(1024)
            node._object_manager = ObjectManager(node.node_id, node._object_store)
            node._sealed_metadata, node._dropped_metadata = {}, {}
            node._object_localization_locks = {}
            self.nodes.append(node)
            registered = self.registry.register_message(protocol.RegisterNode(
                node.node_id, node._node_pid, node.address, node._ledger.total,
            ))
            assert registered.accepted
            node._registration_epoch = registered.registration_epoch
        epoch, infos = self.registry.live_snapshot()
        initial = protocol.InstallClusterSnapshot(epoch, "initial-homes", infos)
        for node in self.nodes:
            self.install(node, initial)
        core.node_address = self.nodes[0].address
        core._home_route = _HomeRoute(core.node_id, core.node_address, epoch)
        core._membership_epoch, core._installed_cluster_snapshot = epoch, initial
        core._rpc = self.rpc
        core.inline_threshold = 1

    @staticmethod
    def install(node, snapshot):
        reply = node._handle_install_cluster_snapshot(snapshot)
        assert reply == protocol.InstallClusterSnapshotReply(
            snapshot.membership_epoch, snapshot.snapshot_id, node.node_id, True,
        )

    def lose_first(self):
        assert not self.deaths
        original, survivor = self.nodes
        result = self.registry.report_death(protocol.ReportNodeDeath(
            "put-home-exit", original.node_id, original._node_pid,
            original._registration_epoch, 17, protocol.NodeDeathReason.PROCESS_EXIT,
            "bounded simulated process-exit observation",
        ))
        assert result.disposition is protocol.NodeDeathDisposition.APPLIED
        snapshot = protocol.InstallClusterSnapshot(
            result.membership_epoch, "surviving-home", result.live_nodes,
        )
        self.install(survivor, snapshot)
        removed = self.core.handle_node_death(result.death, snapshot)
        assert not removed.lost and not removed.surviving
        assert self.core._node_is_dead(original.node_id)
        assert self.core._dead_nodes[original.node_id] == result.death
        assert self.core._home_route == _HomeRoute(
            survivor.node_id, survivor.address, snapshot.membership_epoch,
        )
        self.deaths.append(result.death)

    def rpc(self, address, handler, request):
        if address == self.core.gcs_address:
            assert len(self.gcs_calls) < 32, "put home graph metadata exceeded finite budget"
            self.gcs_calls.append((handler, request))
            if handler == ep.PUBLICATION_HANDLER:
                return self.authority.apply(request)
            assert handler == "get_node_state"
            return self.registry.get_state_reply(request)
        assert len(self.calls) < 5
        target, = [node for node in self.nodes if node.address == address]
        assert not self.core._node_is_dead(target.node_id), "never access the dead Node fixture"
        assert handler in ("seal_object", "drop_object_replica")
        self.calls.append((target.node_id, handler, request))
        if handler == "seal_object":
            assert len(request.data) <= 128
            assert request.object_id in self.core._put_handoffs
            if self.mode == "timeout" and self.failed_seals == 1:
                self.failed_seals += 1
                raise TimeoutError("cleanup Seal ACK still unknown")
            reply = target._handle_seal_object(request)
            assert reply.sealed and target.object_store.get(request.object_id) == request.data
            if self.mode == "death-after-seal":
                self.mode = None
                self.lose_first()
                raise TimeoutError("old home exited after Seal")
            if self.mode == "timeout":
                self.failed_seals += 1
                raise TimeoutError("Seal reply lost without death proof")
            return reply
        reply = target._handle_drop_object_replica(request)
        assert reply.accepted and reply.dropped
        return reply

    def collect(self):
        for ref in self.references:
            if not ref.closed:
                ref.close(timeout=0)
        assert self.core._reference_mailbox.pending.qsize() <= 8
        self.core._reference_mailbox.drain()
        # Explicitly classify the real finite death observation; no coordinator.
        for _ in range(8):
            try:
                event = self.core._submissions.get_nowait()
            except queue.Empty:
                break
            try:
                if type(event) is _NodeDeathObserved:
                    assert event.death in self.deaths
                    self.core._classify_node_death(event)
                else:
                    assert event is _WAKE_COORDINATOR
            finally:
                self.core._submissions.task_done()
        assert self.core._submissions.empty() and self.core._submissions.unfinished_tasks == 0
        assert self.core._reference_mailbox.pending.empty()
        assert self.core._reference_mailbox.pending.unfinished_tasks == 0
        assert not self.core._objects and not self.core._stored_descriptors
        assert not self.core._put_handoffs and not self.core._object_gc_obligations
        assert self.core._accepted_task_count == self.core._inflight_puts == 0
        assert not self.core._recovery._tasks and not self.core._task_finish_barriers
        for node in self.nodes:
            if not self.core._node_is_dead(node.node_id):
                assert node.object_store.used_bytes == 0 and not node._sealed_metadata
        close_pure_core(self.core)
        assert all(snapshot.receipt(ep.PublicationStage.RETIRED) is not None
                   for snapshot in self.authority.snapshots())


@pytest.fixture
def homes(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure put failover attempted runtime or blocking work")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure close must not block"
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (NodeServer, "__init__"), (worker.WorkerServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "__init__"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (control, core_module, node_module, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    fixture = _Homes()
    try:
        yield fixture
    finally:
        fixture.collect()


def test_put_reseals_exact_bytes_on_installed_survivor_after_original_seal(homes):
    homes.mode = "death-after-seal"
    value = b"one-logical-put"
    ref = homes.core.put(value)
    homes.references.append(ref)
    old, survivor = homes.nodes
    assert len(homes.calls) == 2
    assert [item[:2] for item in homes.calls] == [
        (old.node_id, "seal_object"), (survivor.node_id, "seal_object"),
    ]
    first, second = [item[2] for item in homes.calls]
    assert first == second and first.object_id == ref.object_id
    assert first.data == cloudpickle.dumps(value)
    assert first.attempt_id == AttemptID(ref.object_id.task_id, 0)
    snapshot = homes.core.owner_table.snapshot(ref.object_id)
    assert snapshot.state is ObjectState.READY_STORED
    assert snapshot.locations == frozenset({survivor.node_id})
    assert snapshot.canonical_stored_result == homes.core._stored_descriptors[ref.object_id]
    assert snapshot.canonical_stored_result.node_id == survivor.node_id
    assert survivor.object_store.get(ref.object_id) == first.data
    assert snapshot.producer_task_spec is None
    assert homes.core._recovery.reconstruction_snapshot(ref.object_id).is_put
    assert homes.core._put_index == 1 and not homes.core._put_handoffs


def test_put_home_death_after_graph_commit_never_rebinds_materialization_before_owner_install(homes, monkeypatch):
    core = homes.core
    old, survivor = homes.nodes
    original_route = core._home_route_snapshot()
    client = core._publication_client()
    real_rpc = homes.rpc
    observed = {}

    def death_after_commit(address, handler, request):
        reply = real_rpc(address, handler, request)
        if handler == ep.PUBLICATION_HANDLER and type(request) is ep.CommitGraph:
            assert not observed, "one logical put must never recommit on a survivor"
            assert reply.accepted and reply.receipt.stage is ep.PublicationStage.COMMITTED
            publication = reply.snapshot.publication
            identity = publication.object_id
            work = core._put_handoffs[identity]
            # C5 is an actual authority fact while Core still owns the open
            # put handoff and has not installed a public owner value.
            assert work.choice is PutChoice.OPEN and work.driving
            assert core.owner_table.snapshot(identity).state is ObjectState.PENDING
            assert identity not in core._stored_descriptors
            assert work.materialization.route == original_route
            committed = client.query(publication)
            assert committed == reply.snapshot and committed.graph_active
            assert committed.prepared == request.put_prepared
            assert committed.prepared.materialization.node_incarnation.node_id == old.node_id
            assert committed.prepared.seal_reply == work.materialization.seal_receipt
            assert not committed.prepared.prepare_replies and not committed.prepared.promote_replies
            observed.update(work=work, publication=publication, committed=committed, request=request)
            homes.lose_first()
            assert core.owner_table.snapshot(identity).state is ObjectState.PENDING
            assert core._put_handoffs[identity] is work
            assert client.query(publication) == committed
        return reply

    monkeypatch.setattr(core, "_rpc", death_after_commit)
    with pytest.raises(NodeDiedError, match="materialization was lost before owner installation"):
        core.put(b"fixed-on-old-home")
    work, publication, committed = observed["work"], observed["publication"], observed["committed"]
    identity = publication.object_id
    assert len(homes.calls) == 1 and homes.calls[0][:2] == (old.node_id, "seal_object")
    assert homes.calls[0][2] == work.materialization.seal_request
    assert homes.calls[0][2].data == cloudpickle.dumps(b"fixed-on-old-home")
    assert work.materialization.route == original_route
    assert work.materialization.seal_receipt == committed.prepared.seal_reply
    assert work.materialization.drop_receipt == homes.deaths[0]
    assert work.materialization.drop_request is None
    assert work.choice is PutChoice.ABORTED and not work.driving and not work.children
    owner = core.owner_table.snapshot(identity)
    assert owner.state is ObjectState.ERROR and isinstance(owner.error, NodeDiedError)
    assert owner.inline_data is None and owner.canonical_stored_result is None and not owner.locations
    assert identity not in core._stored_descriptors and identity not in core._put_handoffs
    assert survivor.object_store.used_bytes == 0 and not survivor._sealed_metadata
    assert core._home_route_snapshot().node_id == survivor.node_id
    assert core._put_index == 1 and core._inflight_puts == 0 and not homes.references
    retired = client.query(publication)
    assert retired.prepared == committed.prepared
    assert retired.receipt(ep.PublicationStage.COMMITTED) == committed.receipt(ep.PublicationStage.COMMITTED)
    assert retired.fence == work.abort_receipt
    assert retired.closed_holds == ep.ClosedContainedHolds(publication.reference)
    assert retired.receipt(ep.PublicationStage.RETIRED) is not None
    assert not retired.forward_open and not retired.graph_active
    assert retired.complete is None and retired.adoption is None
    assert [request for handler, request in homes.gcs_calls if type(request) is ep.CommitGraph] == [observed["request"]]
    assert core._drive_put_handoff_cleanup(identity)
    assert client.query(publication) == retired and len(homes.calls) == 1


def test_inline_put_uses_owner_bytes_after_home_death_without_node_access(homes):
    homes.lose_first()
    homes.core.inline_threshold = 128
    ref = homes.core.put({"local": 7})
    homes.references.append(ref)
    snapshot = homes.core.owner_table.snapshot(ref.object_id)
    assert snapshot.state is ObjectState.READY_INLINE and not snapshot.locations
    assert homes.core._node_is_dead(homes.core.node_id)
    assert homes.core.get(ref) == {"local": 7}
    assert homes.calls == [] and not homes.core._stored_descriptors


def test_unknown_seal_does_not_change_home_or_discard_cleanup_identity(homes):
    homes.mode = "timeout"
    core = homes.core
    original_route = core._home_route_snapshot()
    with pytest.raises(TimeoutError, match="without death proof"):
        core.put(b"ambiguous-value")
    identity = ObjectID.for_task(TaskID.for_put(core.job_id, core.worker_id, 0))
    assert core._home_route_snapshot() == original_route and not core._dead_nodes
    assert len(homes.calls) == 2 and homes.failed_seals == 2
    assert all(item[:2] == (homes.nodes[0].node_id, "seal_object") for item in homes.calls)
    assert homes.calls[0][2] == homes.calls[1][2]
    work = core._put_handoffs[identity]
    assert work.choice is PutChoice.ABORTED and not work.driving
    assert work.materialization.route == original_route and work.materialization.seal_request == homes.calls[0][2]
    assert core.owner_table.snapshot(identity).state is ObjectState.ERROR
    assert core.owner_table.collection_state(identity) is ObjectCollectionState.ACTIVE
    assert homes.nodes[0].object_store.get(identity) == work.prepared.payload
    assert homes.nodes[1].object_store.used_bytes == 0
    # Resolve the existing intent explicitly; no new put or alternate Node.
    homes.mode = None
    assert core._drive_put_handoff_cleanup(identity)
    assert identity not in core._put_handoffs
    assert [item[1] for item in homes.calls] == [
        "seal_object", "seal_object", "seal_object", "drop_object_replica",
    ]
    assert all(item[0] == homes.nodes[0].node_id for item in homes.calls)
    assert core._put_index == 1 and not core._stored_descriptors
