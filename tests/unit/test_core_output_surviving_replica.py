"""Pure real-store composition for a publisher loss with an adopted secondary.

One tiny stored output, two 1 KiB in-memory stores, one threadless Core and
one actual member registry. Actual Node pin/chunk/seal/grant handlers create the secondary
before the Core records its location. Fake transport calls those handlers
synchronously; fault hooks run once, without a thread, wait or user execution.
The publisher's committed death is metadata, not a real process crash.
"""

from dataclasses import replace
from types import SimpleNamespace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, node as node_module, output_protocol as wire, protocol
from miniray.core import CoreWorker, _HomeRoute, _OutputNodeLossObligation
from miniray.ids import AttemptID, LeaseID, NodeID, TaskID
from miniray.node import NodeServer, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture as _publication
from tests.unit.test_node_dependency_pull import _bare_node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure surviving-replica composition attempted runtime infrastructure")

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Fixture:
    def __init__(self, monkeypatch):
        publication, source, core, pending, reply, _calls, _rpc = _publication(refs=False)
        self.publication, self.source, self.core = publication, source, core
        self.pending, self.envelope = pending, reply.output_publication
        self.manifest, self.identity = self.envelope.manifest, publication.id
        self.output = pending.output_ids[0]
        self.calls, self.transfers = [], []
        self.source_address, self.target_address = ("source.invalid", 1), ("target.invalid", 2)
        target_id = NodeID(bytes(value ^ 1 for value in source.node_id.value))
        target = _bare_node(target_id, ResourceVector({"CPU": 1}))
        target._object_store = ObjectStore(1024)
        target._object_manager = ObjectManager(target_id, target._object_store)
        target._cluster_addresses = {source.node_id: self.source_address}
        target.event_sink = None
        self.target = target
        self.registry = control.NodeRegistry()
        for node_id, pid, address in ((target_id, 1702, self.target_address),
                                     (source.node_id, source._node_pid, self.source_address)):
            assert self.registry.register(node_id, address, ResourceVector({"CPU": 1}), node_pid=pid)
        assert self.registry.get(source.node_id).registration_epoch == source._registration_epoch
        target._node_pid, target._registration_epoch = 1702, self.registry.get(target_id).registration_epoch
        target._workers = {target.worker_id: _WorkerSlot(target.worker_id,
            process=SimpleNamespace(is_alive=lambda: True), address=("worker.invalid", 3), pid=1703)}
        target._worker_order = (target.worker_id,)
        core.node_id, core.node_address = target_id, self.target_address
        epoch, live = self.registry.live_snapshot()
        core._home_route = _HomeRoute(target_id, self.target_address, epoch)
        core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(epoch, "pure-start", live)
        core._membership_epoch = epoch
        core._resolve_node_address = self.address
        core._rpc = self.rpc

        def transfer(address, handler, request, **options):
            assert address == self.source_address
            self.transfers.append(handler)
            assert len(self.transfers) <= 3  # one sub-chunk-sized object
            assert target.resource_ledger.available == target.resource_ledger.total
            handlers = {
                node_module.PIN_OBJECT_HANDLER: source._handle_pin_object_for_transfer,
                node_module.GET_OBJECT_CHUNK_HANDLER: source._handle_get_object_chunk,
                node_module.RELEASE_OBJECT_PIN_HANDLER: source._handle_release_object_pin,
            }
            assert handler in handlers
            return handlers[handler](request)

        monkeypatch.setattr(node_module, "rpc_request", transfer)
        self.adopted_acks = 0

        def lose_adopted_ack(address, handler, request):
            result = self.rpc(address, handler, request)
            if isinstance(request, wire.AckOutputPublicationAdopted):
                self.adopted_acks += 1
                assert self.adopted_acks == 1
                raise TimeoutError("actual Node adoption receipt retired before ACK loss")
            return result

        core._rpc = lose_adopted_ack
        assert not core._publish_reply(pending, reply, expected_node_id=source.node_id,
                                      expected_lease_id=self.identity.lease_id)
        core._rpc = self.rpc
        assert self.adopted_acks == 1 and not core._finish_pending_task(pending)
        assert core.owner_table.snapshot(self.output).state is ObjectState.READY_STORED
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        result = self.envelope.results[0]
        self.descriptor = protocol.ObjectStoreDescriptor(
            self.output, core.worker_id, pending.spec.attempt_id, source.node_id,
            result.size_bytes, result.checksum,
        )

    def address(self, node_id, *, home_route=None):
        assert node_id in (self.source.node_id, self.target.node_id)
        return self.source_address if node_id == self.source.node_id else self.target_address

    def rpc(self, address, handler, request):
        self.calls.append((address, handler, request))
        assert len(self.calls) <= 32
        node = self.target if address == self.target_address else self.source
        assert address == self.address(node.node_id)
        assert not self.core._node_is_dead(node.node_id), "contacted a committed dead publisher"
        handlers = {
            wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER: node._handle_ack_output_publication_adopted,
            node_module.GET_OBJECT_HANDLER: node._handle_get_object,
            node_module.DROP_OBJECT_REPLICA_HANDLER: node._handle_drop_object_replica,
        }
        assert handler in handlers
        return handlers[handler](request)

    def add_secondary(self):
        task = TaskID(bytes((73,)) * 16)
        request = protocol.RequestWorkerLease(
            LeaseID(bytes((74,)) * 16), task, AttemptID(task, 0), ResourceVector({"CPU": 1}),
            self.target.node_id, self.core.worker_id, target_node_id=self.target.node_id,
            dependencies=(self.descriptor,),
        )
        grant = self.target._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease
        assert self.target.object_store.get(self.output) == self.publication.values.payloads[0]
        self.core._validate_granted_dependencies((self.descriptor,), grant)
        assert self.core._build_location_reports((self.descriptor,), grant) == ()
        with self.core._state_lock:
            # This helper is a custody-only probe, not a submitted consumer.
            # Actual executing consumers prove their SUBMITTED hold in driver.
            receipt = self.core._record_replica_custody_locked(grant.dependencies[0], active_hold=lambda _snapshot: False)
        assert receipt.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert self.target._handle_release_lease(protocol.ReleaseWorkerLease(
            grant.lease_id, grant.worker_id, grant.allocation_token,
        )).released
        assert self.target.resource_ledger.available == self.target.resource_ledger.total
        assert self.target.object_store.snapshot(self.output).pin_count == 0
        assert self.source.object_store.snapshot(self.output).pin_count == 0
        assert self.core.owner_table.snapshot(self.output).locations == frozenset((self.source.node_id, self.target.node_id))

    def lose_publisher(self):
        source = self.source
        death_reply = self.registry.report_death(protocol.ReportNodeDeath(
            "surviving-publication-source-exit", source.node_id, source._node_pid,
            source._registration_epoch, 1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed"))
        epoch, live = self.registry.live_snapshot()
        self.core.handle_node_death(death_reply.death, protocol.InstallClusterSnapshot(epoch, "pure-after-loss", live))
        self.death = death_reply.death
        return _OutputNodeLossObligation(self.identity, self.death)

    def drop_secondary(self):
        result = self.envelope.results[0]
        request = protocol.DropObjectReplica(
            self.output, self.pending.spec.attempt_id, self.core.worker_id,
            self.target.node_id, result.checksum,
        )
        acknowledgement = self.target._handle_drop_object_replica(request)
        assert acknowledgement.status is protocol.DropObjectReplicaStatus.DROPPED
        assert self.core.owner_table.remove_location(self.output, self.pending.spec.attempt_id, self.target.node_id)
        self.core._stored_descriptors.pop(self.output, None)

    def assert_kept(self, *, ready=True):
        snapshot = self.core.owner_table.snapshot(self.output)
        resolution = self.core.owner_table._output_loss_receipts[self.identity]
        assert resolution.keep
        assert resolution.complete == self.envelope.complete
        assert snapshot.state is (ObjectState.READY_STORED if ready else ObjectState.LOST)
        assert snapshot.current_attempt == self.pending.spec.attempt_id
        assert snapshot.canonical_stored_result == self.envelope.results[0]
        assert snapshot.output_publication.manifest == self.manifest
        assert snapshot.locations == (frozenset((self.target.node_id,)) if ready else frozenset())
        record = self.core._recovery.task_record(self.pending.task_id)
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
        if ready:
            assert self.core._stored_descriptors[self.output] == replace(self.envelope.results[0], node_id=self.target.node_id)
            assert self.core._fetch_stored_object(self.output, snapshot) == self.publication.values.payloads[0]
        else:
            assert self.output not in self.core._stored_descriptors
        assert self.core._finish_pending_task(self.pending)

    def close(self):
        for output in self.core._objects:
            for token in self.core.owner_table.snapshot(output).local_tokens:
                self.core.owner_table.release_local_reference(output, token)
        close_pure_core(self.core)


def test_adopted_output_keeps_grant_proven_secondary_and_collects_its_real_bytes(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        f.add_secondary()
        obligation = f.lose_publisher()
        assert f.core._drive_output_node_loss(f.pending, obligation)
        f.assert_kept()
        for index, output in enumerate(f.pending.output_ids):
            f.core.owner_table.release_local_reference(output, "outer{}".format(index))
            f.core._reference_released(output)
            assert f.core.owner_table.collection_state(output) is ObjectCollectionState.COLLECTED
        assert f.target.object_store.used_bytes == 0
        assert f.target._sealed_metadata == {}
        assert not f.core._object_gc_obligations
    finally:
        f.close()


@pytest.mark.parametrize("phase", ("before-owner-cas", "owner-cas"))
def test_exact_keep_replay_does_not_restore_a_secondary_lost_after_decision(monkeypatch, phase):
    f = _Fixture(monkeypatch)
    try:
        f.add_secondary()
        obligation = f.lose_publisher()
        effects = []
        original = f.core.owner_table.resolve_output_node_loss

        def lose_secondary_at_cas(manifest, resolution, envelope, **kwargs):
            assert resolution.keep and resolution.complete == f.envelope.complete
            if not effects and phase == "before-owner-cas":
                effects.append(resolution)
                f.drop_secondary()
                raise TimeoutError("local KEEP selected before owner CAS")
            changed = original(manifest, resolution, envelope, **kwargs)
            if not effects:
                effects.append(resolution)
                f.drop_secondary()
                raise RuntimeError("owner CAS committed before local error")
            return changed

        monkeypatch.setattr(f.core.owner_table, "resolve_output_node_loss", lose_secondary_at_cas)
        assert not f.core._drive_output_node_loss(f.pending, obligation)
        assert len(effects) == 1
        decision = f.core._output_loss_choices[f.identity]
        assert decision is True
        retained = f.core._protocol_unresolved[f.pending.task_key].obligation
        assert f.core._drive_output_node_loss(f.pending, retained)
        assert f.core._output_loss_choices[f.identity] is decision
        f.assert_kept(ready=False)
        assert not f.target.object_store.contains(f.output, sealed_only=False)
        assert f.target._sealed_metadata == {}
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 0
    finally:
        f.close()


def test_descriptor_without_a_secondary_remains_drop_after_publisher_loss(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        obligation = f.lose_publisher()
        assert f.core.owner_table.output_owner_result(f.output) == f.envelope.results[0]
        assert f.core._drive_output_node_loss(f.pending, obligation)
        resolved = f.core.owner_table._output_loss_receipts[f.identity]
        assert not resolved.keep and resolved.complete == f.envelope.complete
        current = f.core.owner_table.snapshot(f.output)
        assert current.state is ObjectState.LOST and current.output_publication is None
        assert current.canonical_stored_result is None and not current.locations
        assert f.output not in f.core._stored_descriptors
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 0
        assert f.core._finish_pending_task(f.pending)
    finally:
        f.close()
