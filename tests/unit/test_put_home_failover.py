"""Bounded put/home contracts with real local storage and death reducers.

One threadless Core, two 1-KiB Nodes and one <=128-byte put per case. A simulated
process-exit observation enters the real NodeRegistry; the survivor installs
its real snapshot before Core.handle_node_death installs the fence and route.
This proves the protocol boundary, not an OS process exit. A dead Node's store
remains an inaccessible fixture snapshot; no fake deletion models its death.
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
from miniray.control import NodeRegistry
from miniray.core import CoreWorker, _HomeRoute, _NodeDeathObserved, _WAKE_COORDINATOR
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
