from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from miniray.control import (
    ACTOR_STATUS,
    PLACEMENT_GROUP_STATUS,
    ControlSnapshot,
    GCSLite,
    NodeRegistry,
)
from miniray.ids import NodeID
from miniray.placement import PlacementPlanner, PlacementStatus, PlacementStrategy
from miniray.resources import (
    HybridPolicy,
    NodeSnapshot as SchedulingNodeSnapshot,
    ResourceVector,
    SchedulingStatus,
)


def node_id(value: int) -> NodeID:
    return NodeID(bytes([value]) * 16)


def resources(**values: int) -> ResourceVector:
    return ResourceVector(values)


def test_registry_exports_the_shared_scheduling_snapshot_model() -> None:
    registry = NodeRegistry()
    first = node_id(1)
    registry.register(
        first,
        ("127.0.0.1", 10001),
        resources(CPU=4, GPU=1),
        node_pid=1001,
        available_resources=resources(CPU=2, GPU=1),
    )

    control_node = registry.get(first)
    scheduling_node = control_node.to_scheduling_snapshot()

    assert control_node.address == ("127.0.0.1", 10001)
    assert isinstance(scheduling_node, SchedulingNodeSnapshot)
    assert scheduling_node.node_id == first
    assert scheduling_node.total == resources(CPU=4, GPU=1)
    assert scheduling_node.available == resources(CPU=2, GPU=1)
    assert scheduling_node.alive
    assert registry.scheduling_snapshot() == (scheduling_node,)


def test_one_registry_snapshot_feeds_hybrid_and_placement_schedulers() -> None:
    registry = NodeRegistry()
    first = node_id(1)
    second = node_id(2)
    registry.register(
        first, ("127.0.0.1", 10001), resources(CPU=2), node_pid=1001
    )
    registry.register(
        second, ("127.0.0.1", 10002), resources(CPU=2), node_pid=1002
    )
    nodes = registry.scheduling_snapshot()

    decision = HybridPolicy(seed=0).schedule(resources(CPU=1), nodes)
    plan = PlacementPlanner().plan(
        [resources(CPU=1), resources(CPU=1)],
        nodes,
        PlacementStrategy.STRICT_SPREAD,
    )

    assert decision.status is SchedulingStatus.SELECTED
    assert decision.node_id in {first, second}
    assert plan.status is PlacementStatus.SUCCESS
    assert {plan.node_for(0), plan.node_for(1)} == {first, second}


def test_control_snapshot_exposes_scheduling_nodes_without_losing_addresses() -> None:
    registry = NodeRegistry()
    registered = node_id(3)
    registry.register(
        registered, ("127.0.0.1", 10003), resources(CPU=1), node_pid=1003
    )
    snapshot = ControlSnapshot(
        nodes=registry.snapshot(),
        functions=(),
        actor_support=ACTOR_STATUS,
        placement_group_support=PLACEMENT_GROUP_STATUS,
    )

    assert snapshot.nodes[0].address == ("127.0.0.1", 10003)
    assert snapshot.scheduling_nodes == registry.scheduling_snapshot()


def test_gcs_rpc_surface_has_no_ordinary_task_data_path() -> None:
    # ``handlers`` and unsupported dispatch are pure class behavior.  Avoid
    # constructing TCPServer here so this stays an in-memory unit test.
    gcs = object.__new__(GCSLite)

    assert all("task" not in handler_name for handler_name in gcs.handlers)

    class PushTask:
        pass

    with pytest.raises(TypeError, match="unsupported GCS-lite message"):
        gcs.handle(PushTask())
