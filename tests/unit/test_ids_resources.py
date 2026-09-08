from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

pytestmark = pytest.mark.unit

from miniray.errors import (
    AllocationAlreadyReleasedError,
    AllocationTokenError,
    InvalidResourceError,
)
from miniray.ids import (
    ActorGeneration,
    ActorID,
    AttemptID,
    JobID,
    NodeID,
    ObjectID,
    TaskID,
)
from miniray.resources import (
    AllocationToken,
    HybridPolicy,
    NodeSnapshot,
    ResourceLedger,
    ResourceVector,
    SchedulingStatus,
)


def opaque_id(id_type, byte: int = 1):
    return id_type(bytes([byte]) * 16)


def test_ids_are_strong_and_immutable():
    raw = bytes.fromhex("01" * 16)
    job_id = JobID(raw)
    node_id = NodeID(raw)

    assert job_id != node_id
    assert JobID.from_hex(job_id.hex) == job_id
    with pytest.raises(FrozenInstanceError):
        job_id.value = bytes(16)


def test_task_and_object_ids_are_deterministic_across_attempts():
    job_id = opaque_id(JobID)
    driver_task_id = TaskID.for_driver(job_id)

    task_a = TaskID.derive(job_id, driver_task_id, 7)
    task_b = TaskID.derive(job_id, driver_task_id, 7)
    other_task = TaskID.derive(job_id, driver_task_id, 8)

    assert task_a == task_b
    assert task_a != other_task
    assert AttemptID(task_a, 0).next() == AttemptID(task_a, 1)
    assert ObjectID.for_task(task_a, 0) == ObjectID.for_task(task_b, 0)
    assert ObjectID.for_task(task_a, 0) != ObjectID.for_task(task_a, 1)


def test_actor_generation_changes_without_changing_logical_actor():
    job_id = opaque_id(JobID)
    parent = TaskID.for_driver(job_id)
    actor_id = ActorID.derive(job_id, parent, 0)

    generation = ActorGeneration(actor_id)
    assert generation.next() == ActorGeneration(actor_id, 1)
    assert generation.next().actor_id == generation.actor_id


def test_resource_vector_uses_exact_fixed_point_arithmetic():
    first = ResourceVector({"CPU": 0.1, "custom": "1.125"})
    second = ResourceVector({"CPU": 0.2, "custom": "0.875"})

    total = first + second
    assert total.quantity("CPU") == Decimal("0.3")
    assert total.quantity("custom") == Decimal("2")
    assert total - first == second

    with pytest.raises(InvalidResourceError):
        ResourceVector({"CPU": "0.0001"})


def test_resource_ledger_tokens_make_allocate_and_release_idempotent():
    total = ResourceVector({"CPU": 2, "GPU": 1})
    request = ResourceVector({"CPU": 0.5})
    token = AllocationToken("lease-1")
    ledger = ResourceLedger(total)

    assert ledger.allocate(request, token) == token
    after_first_allocate = ledger.available
    assert ledger.allocate(request, token) == token
    assert ledger.available == after_first_allocate

    assert ledger.release(token) is True
    assert ledger.release(token) is False
    assert ledger.available == total
    with pytest.raises(AllocationAlreadyReleasedError):
        ledger.allocate(request, token)


def test_resource_ledger_rejects_token_reuse_for_another_request():
    ledger = ResourceLedger(ResourceVector({"CPU": 2}))
    token = AllocationToken("lease-1")
    ledger.allocate(ResourceVector({"CPU": 1}), token)

    with pytest.raises(AllocationTokenError):
        ledger.allocate(ResourceVector({"CPU": 2}), token)


def test_hybrid_policy_distinguishes_infeasible_from_temporarily_busy():
    node = opaque_id(NodeID)
    busy = NodeSnapshot(
        node, ResourceVector({"CPU": 2}), ResourceVector({"CPU": 0})
    )
    policy = HybridPolicy(seed=1)

    pending = policy.schedule(ResourceVector({"CPU": 1}), [busy])
    infeasible = policy.schedule(ResourceVector({"CPU": 3}), [busy])

    assert pending.status is SchedulingStatus.PENDING_CAPACITY
    assert infeasible.status is SchedulingStatus.INFEASIBLE


def test_hybrid_policy_preserves_gpu_nodes_for_gpu_work_when_possible():
    cpu_node_id = opaque_id(NodeID, 1)
    gpu_node_id = opaque_id(NodeID, 2)
    cpu_node = NodeSnapshot(
        cpu_node_id, ResourceVector({"CPU": 4}), ResourceVector({"CPU": 4})
    )
    gpu_node = NodeSnapshot(
        gpu_node_id,
        ResourceVector({"CPU": 4, "GPU": 1}),
        ResourceVector({"CPU": 4, "GPU": 1}),
    )

    decision = HybridPolicy(top_k=2, seed=7).schedule(
        ResourceVector({"CPU": 1}), [gpu_node, cpu_node]
    )
    assert decision.node_id == cpu_node_id


def test_hybrid_policy_critical_utilization_threshold_preserves_locality_below_it():
    remote_node_id = opaque_id(NodeID, 1)
    local_node_id = opaque_id(NodeID, 2)
    remote_node = NodeSnapshot(
        remote_node_id,
        ResourceVector({"CPU": 4, "memory": 10}),
        ResourceVector({"CPU": 4, "memory": 9}),
    )
    local_node_below_threshold = NodeSnapshot(
        local_node_id,
        ResourceVector({"CPU": 4, "memory": 10}),
        ResourceVector({"CPU": 3, "memory": 6}),
    )
    local_node_at_threshold = NodeSnapshot(
        local_node_id,
        ResourceVector({"CPU": 4, "memory": 10}),
        ResourceVector({"CPU": 3, "memory": 5}),
    )
    request = ResourceVector({"CPU": 1})
    policy = HybridPolicy(spread_threshold="0.5", seed=0)

    below_threshold = policy.schedule(
        request,
        [remote_node, local_node_below_threshold],
        preferred_node_id=local_node_id,
    )
    at_threshold = policy.schedule(
        request,
        [remote_node, local_node_at_threshold],
        preferred_node_id=local_node_id,
    )

    assert local_node_below_threshold.critical_resource_utilization() == Decimal(
        "0.4"
    )
    assert local_node_at_threshold.critical_resource_utilization() == Decimal(
        "0.5"
    )
    assert below_threshold.node_id == local_node_id
    assert at_threshold.node_id == remote_node_id


def test_hybrid_policy_prefers_locality_only_within_the_minimum_score_tier():
    best_node_id = opaque_id(NodeID, 1)
    preferred_node_id = opaque_id(NodeID, 3)
    equally_idle = NodeSnapshot(
        best_node_id, ResourceVector({"CPU": 4}), ResourceVector({"CPU": 4})
    )
    preferred_idle = NodeSnapshot(
        preferred_node_id,
        ResourceVector({"CPU": 4}),
        ResourceVector({"CPU": 4}),
    )
    preferred_busy = NodeSnapshot(
        preferred_node_id,
        ResourceVector({"CPU": 4}),
        ResourceVector({"CPU": 2}),
    )
    request = ResourceVector({"CPU": 1})
    policy = HybridPolicy(spread_threshold=0, top_k=1, seed=0)

    tied = policy.schedule(
        request, [equally_idle, preferred_idle], preferred_node_id=preferred_node_id
    )
    busier = policy.schedule(
        request, [equally_idle, preferred_busy], preferred_node_id=preferred_node_id
    )

    assert tied.node_id == preferred_node_id
    assert tied.candidates == (best_node_id, preferred_node_id)
    assert busier.node_id == best_node_id
    assert busier.candidates == (best_node_id, preferred_node_id)


def test_hybrid_policy_seeded_choice_is_reproducible_and_limited_to_top_k():
    nodes = [
        NodeSnapshot(
            opaque_id(NodeID, index),
            ResourceVector({"CPU": 10}),
            ResourceVector({"CPU": 11 - index}),
        )
        for index in (1, 2, 3, 4)
    ]
    request = ResourceVector({"CPU": 1})

    first_policy = HybridPolicy(spread_threshold=0, top_k=2, seed=42)
    second_policy = HybridPolicy(spread_threshold=0, top_k=2, seed=42)
    first = [first_policy.schedule(request, nodes) for _ in range(12)]
    second = [second_policy.schedule(request, list(reversed(nodes))) for _ in range(12)]

    first_ids = [decision.node_id for decision in first]
    second_ids = [decision.node_id for decision in second]
    top_two = {nodes[0].node_id, nodes[1].node_id}
    ranked = tuple(node.node_id for node in nodes)

    assert first_ids == second_ids
    assert set(first_ids) == top_two
    assert all(decision.candidates == ranked for decision in first)


def test_hybrid_policy_top_k_fraction_uses_the_cluster_snapshot_size():
    candidates = [
        NodeSnapshot(
            opaque_id(NodeID, index),
            ResourceVector({"CPU": 10}),
            ResourceVector({"CPU": 11 - index}),
        )
        for index in (1, 2, 3)
    ]
    infeasible = [
        NodeSnapshot(
            opaque_id(NodeID, index), ResourceVector(), ResourceVector()
        )
        for index in range(4, 11)
    ]
    policy = HybridPolicy(
        spread_threshold=0, top_k=1, top_k_fraction="0.5", seed=42
    )

    decisions = [
        policy.schedule(ResourceVector({"CPU": 1}), candidates + infeasible)
        for _ in range(12)
    ]

    assert {decision.node_id for decision in decisions} == {
        node.node_id for node in candidates
    }
    assert all(
        decision.candidates == tuple(node.node_id for node in candidates)
        for decision in decisions
    )
