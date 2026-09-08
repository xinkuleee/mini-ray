"""Pure public API contracts for bounded per-node Worker pools."""

from __future__ import annotations

from dataclasses import dataclass
import inspect

import pytest

import miniray as ray
from miniray import api, protocol
from miniray.ids import NodeID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _node_context(index: int, workers: int) -> api.NodeRuntimeContext:
    base = 20000 + index * 100
    return api.NodeRuntimeContext(
        node_id=NodeID.random(),
        node_address=("127.0.0.1", base),
        node_pid=5000 + index,
        worker_ids=tuple(WorkerID.random() for _ in range(workers)),
        worker_pids=tuple(5100 + index * 10 + slot for slot in range(workers)),
        worker_addresses=tuple(
            ("127.0.0.1", base + slot + 1) for slot in range(workers)
        ),
    )


def test_runtime_context_flattens_workers_node_major_and_keeps_first_views() -> None:
    first = _node_context(0, 2)
    second = _node_context(1, 1)
    context = api.RuntimeContext(
        gcs_pid=4900,
        gcs_address=("127.0.0.1", 19999),
        nodes=(first, second),
    )

    assert context.nodes == (first, second)
    assert context.node_ids == (first.node_id, second.node_id)
    assert context.node_addresses == (first.node_address, second.node_address)
    assert context.node_pids == (first.node_pid, second.node_pid)
    assert context.worker_ids == first.worker_ids + second.worker_ids
    assert context.worker_pids == first.worker_pids + second.worker_pids
    assert context.worker_addresses == (
        first.worker_addresses + second.worker_addresses
    )
    assert context.node_id == first.node_id
    assert context.node_address == first.node_address
    assert context.node_pid == first.node_pid
    assert context.worker_id == first.worker_ids[0] == first.worker_id
    assert context.worker_pid == first.worker_pids[0] == first.worker_pid
    assert context.worker_address == first.worker_addresses[0] == first.worker_address


def test_node_runtime_context_requires_one_or_two_aligned_worker_slots() -> None:
    valid = _node_context(0, 2)
    common = dict(
        node_id=valid.node_id,
        node_address=valid.node_address,
        node_pid=valid.node_pid,
    )
    with pytest.raises(ValueError, match="non-empty|align"):
        api.NodeRuntimeContext(
            **common, worker_ids=(), worker_pids=(), worker_addresses=()
        )
    with pytest.raises(ValueError, match="non-empty|align"):
        api.NodeRuntimeContext(
            **common,
            worker_ids=valid.worker_ids,
            worker_pids=valid.worker_pids[:1],
            worker_addresses=valid.worker_addresses,
        )
    with pytest.raises(ValueError, match="unique"):
        api.NodeRuntimeContext(
            **common,
            worker_ids=(valid.worker_ids[0], valid.worker_ids[0]),
            worker_pids=valid.worker_pids,
            worker_addresses=valid.worker_addresses,
        )


@pytest.mark.parametrize("invalid", [True, False, 1.0, "2", None])
def test_init_rejects_non_integer_worker_pool_size_before_spawning(
    invalid: object,
) -> None:
    assert "num_workers_per_node" in inspect.signature(ray.init).parameters
    with pytest.raises(TypeError, match="num_workers_per_node"):
        ray.init(num_workers_per_node=invalid)  # type: ignore[arg-type]
    assert not ray.is_initialized()


@pytest.mark.parametrize("invalid", [0, -1, 3])
def test_init_rejects_worker_pool_size_outside_teaching_bound(invalid: int) -> None:
    with pytest.raises(ValueError, match="one or two Workers per node"):
        ray.init(num_workers_per_node=invalid)
    assert not ray.is_initialized()


@dataclass(frozen=True)
class _NodeRuntime:
    startup: protocol.NodeStartup


def _runtime(workers: int, index: int = 0) -> _NodeRuntime:
    context = _node_context(index, workers)
    return _NodeRuntime(
        protocol.NodeStartup(
            context.node_id,
            context.node_pid,
            context.node_address,
            context.worker_ids,
            context.worker_pids,
            context.worker_addresses,
        )
    )


def test_dispatch_lanes_follow_total_workers_with_a_teaching_cap_of_two() -> None:
    assert api._dispatch_lanes_for((_runtime(1),)) == 1
    assert api._dispatch_lanes_for((_runtime(2),)) == 2
    assert api._dispatch_lanes_for((_runtime(1, 0), _runtime(1, 1))) == 2
    assert api._dispatch_lanes_for((_runtime(2, 0), _runtime(2, 1))) == 2


def test_node_constructor_has_the_same_pool_bound_as_public_init() -> None:
    from miniray.node import NodeServer

    for invalid in (True, False, 0, 3, 1.0, "2"):
        with pytest.raises(ValueError, match="num_workers_per_node"):
            NodeServer(
                NodeID.random(),
                ResourceVector({"CPU": 1}),
                num_workers_per_node=invalid,  # type: ignore[arg-type]
            )
