from __future__ import annotations

from threading import RLock

import pytest

from miniray import protocol
from miniray.control import GCSLite, NodeRegistry, WorkerRegistry
from miniray.ids import NodeID, WorkerID
from miniray.resources import ResourceVector
from miniray.trace import EventSink


pytestmark = pytest.mark.unit


def _node(byte: int) -> NodeID:
    return NodeID(bytes([byte]) * 16)


def _worker(byte: int) -> WorkerID:
    return WorkerID(bytes([byte]) * 16)


def _gcs() -> GCSLite:
    gcs = object.__new__(GCSLite)
    gcs.nodes = NodeRegistry()
    gcs.workers = WorkerRegistry(gcs.nodes)
    gcs.event_sink = EventSink()
    gcs._snapshot_lock = RLock()
    gcs._on_node_dead = None
    gcs.actor_coordinator = None
    return gcs


def _register_node(gcs: GCSLite, byte: int, pid: int) -> protocol.RegisterNodeReply:
    return gcs.register_node(
        protocol.RegisterNode(
            _node(byte), pid, ("127.0.0.1", 15000 + byte),
            ResourceVector({"CPU": 2}),
        )
    )


def _register_worker(
    gcs: GCSLite, node: protocol.RegisterNodeReply, byte: int, pid: int
) -> protocol.WorkerIncarnation:
    incarnation = protocol.WorkerIncarnation(
        node.node_id, node.node_pid, node.registration_epoch,
        _worker(byte), pid,
    )
    assert gcs.workers.register(
        protocol.RegisterWorkerIncarnation(incarnation)
    ).accepted
    return incarnation


def _report_node_exit(
    gcs: GCSLite, node: protocol.RegisterNodeReply, detection: str,
    *, expected: bool = False,
) -> protocol.ReportNodeDeathReply:
    return gcs.report_node_death(
        protocol.ReportNodeDeath(
            detection, node.node_id, node.node_pid, node.registration_epoch,
            0 if expected else -9,
            (
                protocol.NodeDeathReason.EXPECTED
                if expected
                else protocol.NodeDeathReason.PROCESS_EXIT
            ),
            "expected stop" if expected else "managed Node exited",
        )
    )


def test_process_exit_fans_out_live_workers_in_stable_order_once() -> None:
    gcs = _gcs()
    node = _register_node(gcs, 1, 4101)
    second = _register_worker(gcs, node, 12, 5102)
    first = _register_worker(gcs, node, 11, 5101)

    reply = _report_node_exit(gcs, node, "node-death")
    replay = _report_node_exit(gcs, node, "node-death")
    journal = gcs.workers.deaths_after(protocol.GetWorkerDeaths(0))

    assert reply.disposition is protocol.NodeDeathDisposition.APPLIED
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert tuple(item.worker_id for item in journal.deaths) == (
        first.worker_id, second.worker_id
    )
    assert tuple(item.reason for item in journal.deaths) == (
        protocol.WorkerDeathReason.NODE_EXIT,
        protocol.WorkerDeathReason.NODE_EXIT,
    )
    assert journal.watermark == 2
    # The exact Node-death replay exposes the same committed workset so later
    # composition can repair a crash after Worker tombstones but before saga
    # admission.
    assert gcs.workers.fail_node(reply.death) == (
        journal.deaths[0], journal.deaths[1]
    )


def test_fanout_skips_already_dead_worker_and_other_node() -> None:
    gcs = _gcs()
    victim = _register_node(gcs, 1, 4101)
    survivor = _register_node(gcs, 2, 4102)
    dead = _register_worker(gcs, victim, 11, 5101)
    live = _register_worker(gcs, victim, 12, 5102)
    other = _register_worker(gcs, survivor, 13, 5103)
    first = gcs.workers.report_death(
        protocol.ReportWorkerDeath(
            "direct-exit", dead, -7, protocol.WorkerDeathReason.PROCESS_EXIT
        )
    )
    assert first.disposition is protocol.WorkerDeathDisposition.APPLIED

    _report_node_exit(gcs, victim, "node-death")
    journal = gcs.workers.deaths_after(protocol.GetWorkerDeaths(0))

    assert tuple(item.worker_id for item in journal.deaths) == (
        dead.worker_id, live.worker_id
    )
    assert gcs.workers.get(other.worker_id).state is (
        protocol.WorkerMembershipState.ALIVE
    )


def test_expected_node_exit_does_not_create_reference_cleanup_facts() -> None:
    gcs = _gcs()
    node = _register_node(gcs, 1, 4101)
    worker = _register_worker(gcs, node, 11, 5101)

    reply = _report_node_exit(gcs, node, "expected-node", expected=True)

    assert reply.disposition is protocol.NodeDeathDisposition.APPLIED
    assert gcs.workers.death_watermark == 0
    assert gcs.workers.get(worker.worker_id).state is (
        protocol.WorkerMembershipState.ALIVE
    )
