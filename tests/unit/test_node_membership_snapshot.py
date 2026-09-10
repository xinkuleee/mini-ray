"""Pure NodeManager membership-epoch and live scheduling-view contracts."""

from __future__ import annotations

import os
import threading

import pytest

from miniray import protocol
from miniray.ids import NodeID, WorkerID
from miniray.node import NodeServer, _WorkerSlot
from miniray.resources import HybridPolicy, ResourceLedger, ResourceVector


pytestmark = pytest.mark.unit


class _Server:
    address = ("127.0.0.1", 28101)


def _rv(**values: int) -> ResourceVector:
    return ResourceVector(values)


def _node() -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    worker_id = WorkerID.random()
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(worker_id)}
    node.num_workers_per_node = 1
    node._node_pid = os.getpid()
    node._registration_epoch = 7
    node._membership_epoch = 1
    node._server = _Server()
    node._ledger = ResourceLedger(_rv(CPU=1))
    node._state_lock = threading.RLock()
    node._gcs_lifecycle_lock = threading.Lock()
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._cluster_snapshot_id = "epoch-1"
    local = protocol.NodeInfo(
        node.node_id, node._node_pid, node._registration_epoch,
        node.address, node._ledger.total, node._ledger.available,
    )
    node._cluster_nodes = (node._as_scheduling_snapshot(local),)
    node._cluster_addresses = {node.node_id: node.address}
    node._installed_snapshot_nodes = (local,)
    node._scheduling_policy = HybridPolicy(seed=0)
    return node


def _info(
    node_id: NodeID, pid: int, epoch: int, address: tuple[str, int],
    resources: ResourceVector,
) -> protocol.NodeInfo:
    return protocol.NodeInfo(
        node_id, pid, epoch, address, resources, resources,
    )


def test_higher_membership_epoch_removes_dead_remote_from_scheduling_view() -> None:
    node = _node()
    remote = _info(
        NodeID.random(), os.getpid() + 1000, 3,
        ("127.0.0.1", 28102), _rv(CPU=1, victim=1),
    )
    local = node._installed_snapshot_nodes[0]
    first = protocol.InstallClusterSnapshot(2, "epoch-2", (local, remote))
    assert node._handle_install_cluster_snapshot(first).installed
    assert remote.node_id in node._cluster_addresses

    refreshed = protocol.InstallClusterSnapshot(3, "epoch-3", (local,))
    reply = node._handle_install_cluster_snapshot(refreshed)

    assert reply.installed and reply.membership_epoch == 3
    assert tuple(item.node_id for item in node._cluster_nodes) == (node.node_id,)
    assert set(node._cluster_addresses) == {node.node_id}
    decision = node._scheduling_policy.schedule(
        _rv(CPU=1, victim=1), node._cluster_nodes
    )
    assert not decision.selected


def test_node_registers_and_reports_resources_with_physical_incarnation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    node._gcs_address = ("127.0.0.1", 28000)
    node._registered_with_gcs = False
    seen: list[object] = []

    def rpc(_address, handler, message):
        seen.append(message)
        if handler == "register_node":
            assert isinstance(message, protocol.RegisterNode)
            return protocol.RegisterNodeReply(
                node.node_id, node._node_pid, True, 9, 4
            )
        assert handler == "update_node_resources"
        assert isinstance(message, protocol.UpdateNodeResources)
        return protocol.UpdateNodeResourcesReply(
            node.node_id, node._node_pid, 9, message.report_seq, True
        )

    monkeypatch.setattr("miniray.node.rpc_request", rpc)
    node._register_with_gcs()
    with node._state_lock:
        node._mark_resource_report_pending_locked()
    assert node._flush_pending_resource_report()

    register, update = seen
    assert register.node_pid == os.getpid()
    assert node._registration_epoch == 9 and node._membership_epoch == 4
    assert (update.node_pid, update.registration_epoch, update.report_seq) == (
        node._node_pid, 9, node._resource_report_version,
    )


def test_first_snapshot_can_install_at_already_observed_registration_epoch() -> None:
    node = _node()
    node._membership_epoch = 2
    node._installed_membership_epoch = 0
    node._cluster_snapshot_id = None
    node._installed_snapshot_nodes = None
    local = protocol.NodeInfo(
        node.node_id, node._node_pid, node._registration_epoch, node.address,
        node._ledger.total, node._ledger.available,
    )
    stale = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(1, "pre-bootstrap-stale", (local,))
    )
    assert not stale.installed and "stale" in stale.error

    reply = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(2, "bootstrap-epoch-2", (local,))
    )

    assert reply.installed
    assert node._membership_epoch == 2
    assert node._installed_membership_epoch == 2
    assert node._cluster_snapshot_id == "bootstrap-epoch-2"


def test_stale_and_equal_conflicting_epochs_reject_without_mutation() -> None:
    node = _node()
    local = node._installed_snapshot_nodes[0]
    before = (node._cluster_nodes, dict(node._cluster_addresses))

    stale = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(0, "stale", (local,))
    )
    assert not stale.installed and "stale" in stale.error

    replay = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(1, "epoch-1", (local,))
    )
    assert replay.installed
    conflict_info = _info(
        NodeID.random(), os.getpid() + 1001, 1,
        ("127.0.0.1", 28103), _rv(CPU=1),
    )
    conflict = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(1, "other", (local, conflict_info))
    )
    assert not conflict.installed and "reused" in conflict.error
    assert (node._cluster_nodes, node._cluster_addresses) == before


def test_higher_epoch_requires_exact_live_self_incarnation_and_shutdown_fences() -> None:
    node = _node()
    wrong_self = protocol.NodeInfo(
        node.node_id, node._node_pid + 1, node._registration_epoch,
        node.address, node._ledger.total, node._ledger.available,
    )
    rejected = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(2, "wrong-self", (wrong_self,))
    )
    assert not rejected.installed
    assert node._membership_epoch == 1

    local = node._installed_snapshot_nodes[0]
    remote = _info(
        NodeID.random(), os.getpid() + 1002, 2,
        ("127.0.0.1", 28104), _rv(CPU=1),
    )
    assert node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(2, "with-remote", (local, remote))
    ).installed
    node._shutdown_request_id = "draining"
    local_busy = protocol.NodeInfo(
        local.node_id, local.node_pid, local.registration_epoch, local.address,
        local.total_resources, ResourceVector(),
    )
    contracted = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(3, "remote-dead", (local_busy,))
    )
    assert contracted.installed
    assert node._installed_membership_epoch == 3
    assert node._cluster_nodes[0].available == node._ledger.available
    expanded = node._handle_install_cluster_snapshot(
        protocol.InstallClusterSnapshot(4, "re-expanded", (local, remote))
    )
    assert not expanded.installed and "contraction" in expanded.error


def test_generic_stop_does_not_publish_expected_membership_death(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a committed graceful Finalize may create EXPECTED tombstone."""

    node = _node()
    calls: list[str] = []
    node._emit = lambda *_args, **_kwargs: None
    node._begin_remove_all_placement_groups_locked = lambda: None
    node._flush_pending_resource_report = lambda: True
    node._stop_worker_supervisor = lambda: None
    # This metadata-only Node has no transfer readers. The real stop path now
    # inspects their registry before the existing stubbed release hook.
    node._pinned_transfers = {}
    # Observe the real transport boundary; an unused helper spy cannot prove
    # that generic stop refrained from publishing a membership death.
    node._background_rpc = lambda *args, **kwargs: calls.append((args, kwargs))
    node._release_all_transfer_pins = lambda: None
    node._server.stop = lambda: None
    node._stop_all_actor_workers = lambda *_args: ()
    node._stop_workers = lambda: ()
    node._retry_dependency_pin_cleanups = lambda **_kwargs: None
    node._resources_clean_locked = lambda: True

    reply = node.stop()

    assert reply.clean
    assert calls == []
