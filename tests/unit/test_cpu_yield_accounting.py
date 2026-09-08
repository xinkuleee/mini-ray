"""Pure contracts for Node-authoritative blocking-get CPU accounting."""

from __future__ import annotations

from decimal import Decimal

import pytest

from miniray.errors import AllocationTokenError
from miniray.resources import (
    AllocationState,
    AllocationToken,
    ResourceLedger,
    ResourceVector,
)


pytestmark = pytest.mark.unit


def rv(**values: object) -> ResourceVector:
    return ResourceVector(values)


def test_yield_releases_only_cpu_and_is_transition_idempotent() -> None:
    total = rv(CPU=2, GPU=1, memory=4, accelerator=1)
    ledger = ResourceLedger(total)
    token = ledger.allocate(
        rv(CPU=1, GPU=1, memory=2, accelerator=1),
        AllocationToken("parent"),
    )

    assert ledger.yield_cpu(token)
    assert not ledger.yield_cpu(token)
    assert ledger.available == rv(CPU=2, memory=2)
    record = ledger.record(token)
    assert record is not None
    assert record.state is AllocationState.CPU_YIELDED
    assert record.held_resources == rv(GPU=1, memory=2, accelerator=1)
    assert ledger.cpu_debt == 0


@pytest.mark.parametrize("finish_parent_first", [False, True])
def test_unblock_debt_is_repaid_for_either_completion_order(
    finish_parent_first: bool,
) -> None:
    ledger = ResourceLedger(rv(CPU=1, GPU=1))
    parent = ledger.allocate(
        rv(CPU=1, GPU=1), AllocationToken("parent")
    )
    assert ledger.yield_cpu(parent)
    child = ledger.allocate(rv(CPU=1), AllocationToken("child"))

    # Unblock never waits behind the child that consumed yielded capacity.
    assert ledger.reacquire_cpu(parent)
    assert ledger.signed_cpu_available == Decimal("-1")
    assert ledger.cpu_debt == Decimal("1")
    assert ledger.available == ResourceVector.empty()
    assert not ledger.can_allocate(rv(CPU=0.001))

    first, second = (parent, child) if finish_parent_first else (child, parent)
    assert ledger.release(first)
    assert ledger.release(second)
    assert ledger.available == rv(CPU=1, GPU=1)
    assert ledger.signed_cpu_available == Decimal("1")
    assert ledger.cpu_debt == 0


def test_one_allocation_supports_multiple_blocking_episodes() -> None:
    ledger = ResourceLedger(rv(CPU=1, custom=1))
    parent = ledger.allocate(
        rv(CPU=1, custom=1), AllocationToken("parent")
    )

    for index in range(3):
        assert ledger.yield_cpu(parent)
        assert ledger.available == rv(CPU=1)
        assert ledger.reacquire_cpu(parent)
        assert ledger.available == ResourceVector.empty()
        assert ledger.cpu_debt == 0
        record = ledger.record(parent)
        assert record is not None and record.state is AllocationState.ACTIVE

    assert ledger.release(parent)
    assert ledger.available == rv(CPU=1, custom=1)


def test_final_release_from_yielded_state_does_not_double_credit_cpu() -> None:
    ledger = ResourceLedger(rv(CPU=1, GPU=1, custom=1))
    parent = ledger.allocate(
        rv(CPU=1, GPU=1, custom=1), AllocationToken("parent")
    )
    assert ledger.yield_cpu(parent)
    assert ledger.release(parent)
    assert not ledger.release(parent)
    assert ledger.available == rv(CPU=1, GPU=1, custom=1)
    assert ledger.cpu_debt == 0


def test_zero_cpu_allocation_cannot_enter_yielded_state() -> None:
    ledger = ResourceLedger(rv(CPU=1, GPU=1))
    token = ledger.allocate(rv(GPU=1), AllocationToken("gpu-only"))

    assert not ledger.yield_cpu(token)
    assert not ledger.reacquire_cpu(token)
    assert ledger.record(token).state is AllocationState.ACTIVE  # type: ignore[union-attr]
    assert ledger.available == rv(CPU=1)


def test_unknown_tokens_cannot_change_signed_accounting() -> None:
    ledger = ResourceLedger(rv(CPU=1))
    missing = AllocationToken("missing")

    with pytest.raises(AllocationTokenError):
        ledger.yield_cpu(missing)
    with pytest.raises(AllocationTokenError):
        ledger.reacquire_cpu(missing)
    assert ledger.available == rv(CPU=1)
    assert ledger.cpu_debt == 0
