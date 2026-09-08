"""Pure contracts for the cached cluster-snapshot bootstrap barrier."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from miniray import api, protocol
from miniray.ids import NodeID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _node_info(byte: int, port: int, **resources: int) -> protocol.NodeInfo:
    total = ResourceVector(resources)
    return protocol.NodeInfo(
        node_id=_id(NodeID, byte),
        node_pid=4000 + byte,
        registration_epoch=byte,
        address=("127.0.0.1", port),
        total_resources=total,
        available_resources=total,
    )


def test_snapshot_contract_is_immutable_unique_and_content_addressed() -> None:
    first = _node_info(1, 12001, CPU=1)
    second = _node_info(2, 12002, CPU=2, custom=1)

    forward_id = api._cluster_snapshot_id(2, (first, second))
    reverse_id = api._cluster_snapshot_id(2, (second, first))
    snapshot = protocol.InstallClusterSnapshot(2, forward_id, (first, second))
    empty = protocol.InstallClusterSnapshot(3, "empty-epoch-3", ())

    assert forward_id == reverse_id
    assert snapshot.nodes == (first, second)
    assert empty.nodes == ()
    with pytest.raises(Exception, match="unique"):
        protocol.InstallClusterSnapshot(2, forward_id, (first, first))


def test_snapshot_content_id_changes_with_address_or_resources() -> None:
    baseline = _node_info(1, 12001, CPU=1)
    changed_address = _node_info(1, 12002, CPU=1)
    changed_resources = _node_info(1, 12001, CPU=2)

    assert api._cluster_snapshot_id(1, (baseline,)) != api._cluster_snapshot_id(
        1, (changed_address,)
    )
    assert api._cluster_snapshot_id(1, (baseline,)) != api._cluster_snapshot_id(
        1, (changed_resources,)
    )
    assert api._cluster_snapshot_id(1, (baseline,)) != api._cluster_snapshot_id(
        2, (baseline,)
    )


@dataclass(frozen=True)
class _Startup:
    node_address: tuple[str, int]
    node_pid: int


@dataclass(frozen=True)
class _NodeRuntime:
    node_id: NodeID
    resources: ResourceVector
    startup: _Startup
    registration_epoch: int = 0


def test_bootstrap_fetches_gcs_once_and_installs_identical_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _node_info(1, 12001, CPU=1)
    second = _node_info(2, 12002, CPU=1, node2=1)
    nodes = (
        _NodeRuntime(first.node_id, first.total_resources, _Startup(first.address, first.node_pid)),
        _NodeRuntime(second.node_id, second.total_resources, _Startup(second.address, second.node_pid)),
    )
    calls: list[tuple[tuple[str, int], str, object]] = []

    def fake_request(address, handler, payload, **_kwargs):
        calls.append((address, handler, payload))
        if handler == api.GET_NODES_HANDLER:
            return protocol.GetNodesReply(2, (second, first))
        assert isinstance(payload, protocol.InstallClusterSnapshot)
        installed_id = first.node_id if address == first.address else second.node_id
        return protocol.InstallClusterSnapshotReply(
            payload.membership_epoch, payload.snapshot_id, installed_id, installed=True
        )

    monkeypatch.setattr(api, "rpc_request", fake_request)
    snapshot = api._fetch_and_validate_cluster_snapshot(("127.0.0.1", 11999), nodes)
    api._install_cluster_snapshot(nodes, snapshot)

    gcs_calls = [call for call in calls if call[1] == api.GET_NODES_HANDLER]
    installs = [
        call for call in calls if call[1] == api.INSTALL_CLUSTER_SNAPSHOT_HANDLER
    ]
    assert len(gcs_calls) == 1
    assert len(installs) == 2
    assert installs[0][2] is snapshot and installs[1][2] is snapshot
    assert tuple(node.node_id for node in snapshot.nodes) == (
        first.node_id,
        second.node_id,
    )
    assert snapshot.membership_epoch == 2
    assert tuple(node.registration_epoch for node in nodes) == (1, 2)


def test_bootstrap_rejects_missing_or_mismatched_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _node_info(1, 12001, CPU=1)
    node = _NodeRuntime(
        expected.node_id, expected.total_resources,
        _Startup(expected.address, expected.node_pid)
    )

    monkeypatch.setattr(
        api,
        "rpc_request",
        lambda *_args, **_kwargs: protocol.GetNodesReply(1, ()),
    )
    with pytest.raises(RuntimeError, match="does not match"):
        api._fetch_and_validate_cluster_snapshot(("127.0.0.1", 11999), (node,))

    wrong = protocol.NodeInfo(
        expected.node_id,
        expected.node_pid,
        expected.registration_epoch,
        ("127.0.0.1", 12999),
        expected.total_resources,
        expected.available_resources,
    )
    monkeypatch.setattr(
        api,
        "rpc_request",
        lambda *_args, **_kwargs: protocol.GetNodesReply(1, (wrong,)),
    )
    with pytest.raises(RuntimeError, match="wrong address"):
        api._fetch_and_validate_cluster_snapshot(("127.0.0.1", 11999), (node,))
