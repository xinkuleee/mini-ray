"""Pure contracts for the two-node Hybrid/spillback runtime slice.

These tests isolate the public API and scheduling contracts.  They perform no
socket I/O and start no child processes.
"""

from __future__ import annotations

import inspect

import pytest

import miniray as ray
from miniray import protocol
from miniray.control import NodeRegistry
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.resources import (
    AllocationToken,
    HybridPolicy,
    ResourceVector,
    SchedulingStatus,
)


pytestmark = pytest.mark.unit

_REMOTE_ONLY_RESOURCE = "node2_only"


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _return_process_id() -> int:
    # The multiprocess smoke uses the same shape, but this unit test never
    # submits the function.
    return 0


def test_public_init_accepts_per_node_resources() -> None:
    """Each logical node must be able to advertise a distinct capacity."""

    assert "node_resources" in inspect.signature(ray.init).parameters


def test_remote_options_accept_custom_resources_without_starting_runtime() -> None:
    """Keep task resource declarations close to Ray's public API."""

    remote_function = ray.remote(
        num_cpus=1, resources={_REMOTE_ONLY_RESOURCE: 1}
    )(_return_process_id)

    assert isinstance(remote_function, ray.RemoteFunction)


def test_gcs_snapshot_feeds_hybrid_policy_and_selects_only_feasible_node() -> None:
    """GCS supplies a stale-able view; Hybrid selection stays outside GCS."""

    local_node = _id(NodeID, 1)
    remote_node = _id(NodeID, 2)
    registry = NodeRegistry()
    registry.register(
        local_node,
        ("127.0.0.1", 12001),
        ResourceVector({"CPU": 1}),
        node_pid=4201,
    )
    registry.register(
        remote_node,
        ("127.0.0.1", 12002),
        ResourceVector({"CPU": 1, _REMOTE_ONLY_RESOURCE: 1}),
        node_pid=4202,
    )

    request = ResourceVector({"CPU": 1, _REMOTE_ONLY_RESOURCE: 1})
    decision = HybridPolicy(seed=0).schedule(
        request,
        registry.scheduling_snapshot(),
        preferred_node_id=local_node,
    )

    assert decision.status is SchedulingStatus.SELECTED
    assert decision.node_id == remote_node
    assert decision.selected_is_available
    assert decision.candidates == (remote_node,)
    assert registry.address(decision.node_id) == ("127.0.0.1", 12002)


def test_spillback_preserves_lease_and_attempt_identity() -> None:
    """A redirect changes placement, never the logical or physical attempt."""

    job_id = _id(JobID, 3)
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    attempt_id = AttemptID(task_id, 0)
    lease_id = _id(LeaseID, 4)
    local_node = _id(NodeID, 5)
    remote_node = _id(NodeID, 6)
    requester = _id(WorkerID, 7)
    remote_worker = _id(WorkerID, 8)
    request = protocol.RequestWorkerLease(
        lease_id=lease_id,
        task_id=task_id,
        attempt_id=attempt_id,
        resources=ResourceVector({"CPU": 1, _REMOTE_ONLY_RESOURCE: 1}),
        requester_node_id=local_node,
        requester_worker_id=requester,
        preferred_node_id=local_node,
    )

    spillback = protocol.SpillbackWorkerLease(
        lease_id=request.lease_id,
        task_id=request.task_id,
        attempt_id=request.attempt_id,
        target_node_id=remote_node,
        target_address=("127.0.0.1", 12002),
        reason="only feasible node",
    )

    assert spillback.lease_id == request.lease_id
    assert spillback.task_id == request.task_id
    assert spillback.attempt_id == request.attempt_id
    assert spillback.target_node_id != request.requester_node_id
    assert spillback.target_node_id == remote_node
    assert spillback.target_address == ("127.0.0.1", 12002)

    grant = protocol.GrantWorkerLease(
        lease_id=spillback.lease_id,
        task_id=spillback.task_id,
        attempt_id=spillback.attempt_id,
        node_id=spillback.target_node_id,
        worker_id=remote_worker,
        worker_address=("127.0.0.1", 13002),
        allocation_token=AllocationToken("node2-allocation"),
    )
    assert grant.lease_id == request.lease_id
    assert grant.task_id == request.task_id
    assert grant.attempt_id == request.attempt_id
    assert grant.node_id == remote_node
