from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from miniray.ids import LeaseID, NodeID
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector
from miniray.runtime_state import (
    Allocation,
    AllocationState,
    DeadRuntimeNodeError,
    LeaseRuntimeStatus,
    NodeRuntimeStatus,
    RuntimeState,
)


def make_id(identifier_type, byte: int):
    return identifier_type(bytes([byte]) * 16)


def rv(**values) -> ResourceVector:
    return ResourceVector(values)


def test_blocking_get_yields_cpu_once_but_retains_gpu_and_custom_resources():
    total = rv(CPU=2, GPU=1, accelerator=2)
    ledger = ResourceLedger(total)
    allocation = Allocation.acquire(
        ledger,
        rv(CPU=1, GPU=1, accelerator=1),
        AllocationToken("outer-task"),
    )

    assert ledger.available == rv(CPU=1, accelerator=1)
    assert allocation.yield_cpu() is True
    assert allocation.yield_cpu() is False
    assert allocation.state is AllocationState.CPU_YIELDED
    assert allocation.held_resources == rv(GPU=1, accelerator=1)
    assert ledger.available == rv(CPU=2, accelerator=1)


def test_reacquire_is_immediate_and_records_debt_until_child_finishes():
    ledger = ResourceLedger(rv(CPU=1, GPU=1))
    allocation = Allocation.acquire(
        ledger, rv(CPU=1, GPU=1), AllocationToken("outer-task")
    )
    assert allocation.yield_cpu()

    contender = ledger.allocate(rv(CPU=1), AllocationToken("inner-task"))
    assert allocation.reacquire() is True
    assert ledger.available == ResourceVector.empty()
    assert ledger.cpu_debt == 1
    assert not ledger.can_allocate(rv(CPU=0.001))
    available_after_reacquire = ledger.available
    assert allocation.reacquire() is True
    assert ledger.available == available_after_reacquire
    assert allocation.state is AllocationState.CPU_REACQUIRED
    # The accounting reducer deliberately supports another episode.  A future
    # Node lease state machine uses the protocol sequence to distinguish this
    # from a delayed duplicate of the preceding Blocked message.
    assert allocation.yield_cpu() is True
    assert allocation.reacquire() is True
    assert ledger.available == ResourceVector.empty()

    assert ledger.release(contender)
    assert ledger.cpu_debt == 0
    assert ledger.available == ResourceVector.empty()


@pytest.mark.parametrize("reacquire", [False, True])
def test_final_release_never_double_frees_after_cpu_yield(reacquire: bool):
    total = rv(CPU=1, GPU=1, custom=1)
    ledger = ResourceLedger(total)
    allocation = Allocation.acquire(
        ledger, total, AllocationToken("task-allocation")
    )
    assert allocation.yield_cpu()
    if reacquire:
        assert allocation.reacquire()

    assert allocation.release() is True
    assert allocation.release() is False
    assert allocation.reacquire() is False
    assert allocation.state is AllocationState.RELEASED
    assert allocation.held_resources == ResourceVector.empty()
    assert ledger.available == total


def test_node_death_releases_each_active_lease_and_fences_it_once():
    node_id = make_id(NodeID, 1)
    first_lease = make_id(LeaseID, 1)
    second_lease = make_id(LeaseID, 2)
    total = rv(CPU=2, GPU=1, custom=1)
    ledger = ResourceLedger(total)
    state = RuntimeState()
    assert state.register_node(node_id, ledger)

    state.grant_lease(first_lease, node_id, rv(CPU=1, GPU=1))
    state.grant_lease(second_lease, node_id, rv(CPU=1, custom=1))
    assert state.yield_cpu(first_lease)

    assert state.mark_node_dead(node_id) == (first_lease, second_lease)
    assert state.mark_node_dead(node_id) == ()
    assert ledger.available == total
    assert state.node_snapshot(node_id).status is NodeRuntimeStatus.DEAD
    assert (
        state.lease_snapshot(first_lease).status
        is LeaseRuntimeStatus.NODE_LOST
    )
    assert (
        state.lease_snapshot(second_lease).status
        is LeaseRuntimeStatus.NODE_LOST
    )
    assert state.release_lease(first_lease) is False
    assert state.reacquire_cpu(first_lease) is False

    with pytest.raises(DeadRuntimeNodeError):
        state.grant_lease(make_id(LeaseID, 3), node_id, rv(CPU=1))


def test_normal_lease_release_is_idempotent_and_restores_the_ledger():
    node_id = make_id(NodeID, 1)
    lease_id = make_id(LeaseID, 1)
    total = rv(CPU=1, GPU=1)
    ledger = ResourceLedger(total)
    state = RuntimeState()
    state.register_node(node_id, ledger)
    state.grant_lease(lease_id, node_id, total)

    assert state.release_lease(lease_id) is True
    assert state.release_lease(lease_id) is False
    assert ledger.available == total
    assert state.lease_snapshot(lease_id).status is LeaseRuntimeStatus.RELEASED
