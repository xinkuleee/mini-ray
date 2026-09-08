"""Shared GCS owner-death fences with one explicitly opt-in live driver.

Pure cases use a captured handler map and in-memory authorities. The single
progress-thread case keeps its bounded real race and must run as L1, not unit.
"""

from __future__ import annotations

import threading
import hashlib
import multiprocessing.process
import socket
import subprocess
import time
from dataclasses import replace

import pytest

from miniray import control, protocol
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_publication import OutputPublicationManifest, OutputPublicationNodeIncarnation
from miniray.resources import ResourceVector
from tests.unit.test_output_publication import _Fixture as _OutputValues


# No module-level unit marker: the live progress driver is deliberately L1.
@pytest.fixture(autouse=True)
def _no_unreviewed_runtime(request, monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("owner-fence contract attempted unreviewed runtime work")

    if request.node.get_closest_marker("loopback_smoke") is None:
        for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                             (threading.Event, "wait"), (threading.Condition, "wait")):
            monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(control, "rpc_request", forbidden)
    monkeypatch.setattr(NodeServer, "__init__", forbidden)


class _Server:
    def __init__(self, handlers, **_configuration) -> None:
        self.handlers = handlers
        self.address = ("127.0.0.1", 39999)
        self.is_running = False

    def start(self):
        self.is_running = True
        return self.address

    def stop(self):
        self.is_running = False


def _id(kind: type, byte: int):
    return kind(bytes([byte]) * 16)


def _register(service: control.GCSLite, byte: int):
    return service.register_node(protocol.RegisterNode(
        _id(NodeID, byte), 4100 + byte, ("127.0.0.1", 14000 + byte),
        ResourceVector({"CPU": 1}),
    ))


def _worker(service: control.GCSLite, node, byte: int):
    incarnation = protocol.WorkerIncarnation(
        node.node_id, node.node_pid, node.registration_epoch,
        _id(WorkerID, byte), 5100 + byte,
    )
    assert service.register_worker_incarnation(
        protocol.RegisterWorkerIncarnation(incarnation)
    ).accepted
    return incarnation


def _fenced(request):
    return protocol.InstallOwnerDeathFenceReply(
        request, protocol.OwnerDeathFenceDisposition.FENCED, ()
    )


def _service(monkeypatch, rpc):
    monkeypatch.setattr(control, "TCPServer", _Server)
    publications = control.PublicationControlAdapter()
    service = control.GCSLite(publications=publications, stored_hold_rpc=rpc)
    assert service.publications is publications
    assert not hasattr(service, "stored_publications")
    return service


def _node_handler(node_id: NodeID) -> NodeServer:
    """Small real Node composition without a process or socket server."""

    node = object.__new__(NodeServer)
    node.node_id = node_id
    node._state_lock = threading.RLock()
    node._object_store = ObjectStore(128 * 1024)
    node._object_manager = ObjectManager(node_id, node._object_store)
    node._sealed_metadata = {}
    node._dropped_metadata = {}
    node._object_localization_locks = {}
    node._pinned_transfers = {}
    node._owner_death_fences = {}
    node._owner_death_fence_outcomes = {}
    node._cluster_addresses = {}
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    return node


@pytest.mark.unit
def test_zero_publication_owner_death_hides_node_until_fence_ack(
    monkeypatch,
) -> None:
    calls = []

    def rpc(_address, handler, request):
        calls.append((handler, request))
        return _fenced(request)

    service = _service(monkeypatch, rpc)
    node = _register(service, 1)
    owner = _worker(service, node, 11)

    death = service.report_worker_death(protocol.ReportWorkerDeath(
        "owner-exit", owner, -9, protocol.WorkerDeathReason.PROCESS_EXIT
    ))

    assert death.death is not None
    registry = service.publications.output_recovery
    assert registry.publication_ids() == ()
    assert registry.frozen_owner_workset(death.death) == ()
    assert service.get_nodes(protocol.GetNodes()).nodes == ()
    assert len(service.owner_death_fences.pending()) == 1

    drained = service.drain_publication_owner_deaths(
        protocol.DrainPublicationOwnerDeaths("drain")
    )

    assert drained.clean and drained.active_publications == 0
    assert tuple(item.node_id for item in service.get_nodes().nodes) == (
        node.node_id,
    )
    assert [handler for handler, _request in calls] == [
        control.INSTALL_OWNER_DEATH_FENCE_HANDLER
    ]
    assert calls[0][1].scope is (
        protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
    )
    assert registry.publication_ids() == ()
    assert registry.frozen_owner_workset(death.death) == ()
    # Even without prior publications, the exact dead-owner identity remains
    # fenced against a later intent; absence is not permission to resurrect it.
    values = _OutputValues(refs=False)
    manifest = OutputPublicationManifest.create(replace(
        values.header, owner_worker_id=owner.worker_id,
        node_incarnation=OutputPublicationNodeIncarnation(
            node.node_id, node.node_pid, node.registration_epoch,
        ),
    ), values.slots)
    with pytest.raises(ValueError, match="death-frozen"):
        registry.report_intent(manifest)
    assert registry.publication_ids() == ()


@pytest.mark.unit
def test_late_node_is_hidden_until_every_historical_fence_is_acked(
    monkeypatch,
) -> None:
    calls = []

    def rpc(_address, _handler, request):
        calls.append(request)
        return _fenced(request)

    service = _service(monkeypatch, rpc)
    first = _register(service, 1)
    owner = _worker(service, first, 12)
    service.report_worker_death(protocol.ReportWorkerDeath(
        "historical-owner-exit", owner, -9,
        protocol.WorkerDeathReason.PROCESS_EXIT,
    ))
    assert service.drain_publication_owner_deaths(
        protocol.DrainPublicationOwnerDeaths("drain-first")
    ).clean

    late = _register(service, 2)
    assert tuple(item.node_id for item in service.get_nodes().nodes) == (
        first.node_id,
    )
    # Actor and placement-group coordinators consume NodeRegistry.snapshot(),
    # so the same bootstrap barrier fences internal GCS scheduling as well.
    assert tuple(item.node_id for item in service.nodes.snapshot()) == (
        first.node_id,
    )

    assert service.drain_publication_owner_deaths(
        protocol.DrainPublicationOwnerDeaths("drain-late")
    ).clean
    assert tuple(item.node_id for item in service.get_nodes().nodes) == (
        first.node_id, late.node_id,
    )
    assert tuple(item.node_id for item in service.nodes.snapshot()) == (
        first.node_id, late.node_id,
    )
    assert tuple(request.node_id for request in calls) == (
        first.node_id, late.node_id,
    )


@pytest.mark.unit
def test_process_exit_node_death_discharges_unreachable_fence(
    monkeypatch,
) -> None:
    service = _service(
        monkeypatch, lambda *_args: (_ for _ in ()).throw(
            ConnectionError("unreachable")
        )
    )
    victim = _register(service, 1)
    survivor = _register(service, 2)
    owner = _worker(service, victim, 13)
    service.report_worker_death(protocol.ReportWorkerDeath(
        "owner-exit-before-node", owner, -9,
        protocol.WorkerDeathReason.PROCESS_EXIT,
    ))

    reply = service.report_node_death(protocol.ReportNodeDeath(
        "victim-node-exit", victim.node_id, victim.node_pid,
        victim.registration_epoch, -9, protocol.NodeDeathReason.PROCESS_EXIT,
        "test Node exited",
    ))

    assert reply.disposition is protocol.NodeDeathDisposition.APPLIED
    pending = service.owner_death_fences.pending()
    assert tuple(effect.key.target.node_id for effect in pending) == (
        survivor.node_id,
    )
    terminals = service.owner_death_fences.snapshot().completed
    assert any(
        completion.effect.key.target.node_id == victim.node_id
        and completion.terminal.value == "NODE_DEAD"
        for completion in terminals
    )


@pytest.mark.loopback_smoke
def test_live_progress_thread_retries_nonterminal_owner_sweep(
    monkeypatch,
) -> None:
    calls = []
    completed = threading.Event()

    def rpc(_address, handler, request):
        assert handler == control.INSTALL_OWNER_DEATH_FENCE_HANDLER
        calls.append(request)
        if len(calls) == 1:
            task = _id(TaskID, 99)
            descriptor = protocol.ObjectStoreDescriptor(
                ObjectID.for_task(task), request.owner_worker_id,
                AttemptID(task, 0), request.node_id, 1, "00" * 32,
            )
            return protocol.InstallOwnerDeathFenceReply(
                request, protocol.OwnerDeathFenceDisposition.FENCED,
                (protocol.OwnerDeathReplicaObservation(
                    descriptor, protocol.OwnerDeathReplicaStatus.PINNED, 1
                ),),
            )
        completed.set()
        return _fenced(request)

    service = _service(monkeypatch, rpc)
    try:
        node = _register(service, 3)
        owner = _worker(service, node, 13)
        service.start()
        service.report_worker_death(protocol.ReportWorkerDeath(
            "live-progress-owner-exit", owner, -9,
            protocol.WorkerDeathReason.PROCESS_EXIT,
        ))
        assert completed.wait(2.0)
        deadline = threading.Event()
        for _ in range(100):
            if not service.owner_death_fences.has_active_operations():
                break
            deadline.wait(0.01)
        assert not service.owner_death_fences.has_active_operations()
        assert len(calls) >= 2
        assert all(call == calls[0] for call in calls)
    finally:
        worker = service._owner_death_progress_thread
        service.stop()
        assert worker is not None and not worker.is_alive()
        assert service._owner_death_progress_thread is None


@pytest.mark.unit
def test_gcs_owner_wide_effect_drives_real_node_handler_and_deletes_bytes(
    monkeypatch,
) -> None:
    """Compose Worker death -> GCS outbox -> Node sealed-store cleanup."""

    service = _service(monkeypatch, lambda *_args: None)
    registration = _register(service, 4)
    node = _node_handler(registration.node_id)
    owner = _worker(service, registration, 14)
    task = _id(TaskID, 100)
    attempt = AttemptID(task, 0)
    object_id = ObjectID.for_task(task)
    payload = b"ordinary stored owner-wide composition"
    checksum = hashlib.sha256(payload).hexdigest()
    node._object_store.put(object_id, payload)
    node._sealed_metadata[object_id] = (
        attempt, owner.worker_id, len(payload), checksum,
    )
    calls = []

    def control_to_node(address, handler, request):
        assert address == ("127.0.0.1", 14004)
        assert handler == control.INSTALL_OWNER_DEATH_FENCE_HANDLER
        calls.append(request)
        return node._handle_install_owner_death_fence(request)

    service._stored_hold_rpc = control_to_node
    death = service.report_worker_death(protocol.ReportWorkerDeath(
        "ordinary-owner-composition-exit", owner, -9,
        protocol.WorkerDeathReason.PROCESS_EXIT,
    ))
    assert death.death is not None
    assert node._object_store.contains(object_id)

    progress = service.progress_publication_owner_death(
        protocol.ProgressPublicationOwnerDeath(owner.worker_id)
    )

    assert progress.progressed and progress.clean
    assert len(calls) == 1
    assert calls[0].scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
    assert calls[0].expected_replicas == ()
    assert node._owner_death_fences[owner.worker_id] == death.death
    assert not node._object_store.contains(object_id, sealed_only=False)
    assert object_id not in node._sealed_metadata
    assert node._dropped_metadata[object_id] == (
        attempt, owner.worker_id, checksum,
    )
    completion = service.owner_death_fences.snapshot().completed
    assert len(completion) == 1
    assert completion[0].reply is not None
    assert completion[0].reply.complete
    assert completion[0].reply.observations[0].status is (
        protocol.OwnerDeathReplicaStatus.ABSENT
    )
