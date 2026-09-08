"""Pure committed-bundle sub-ledger and removal contracts."""

from __future__ import annotations

import pytest

from miniray.ids import PlacementGroupID
from miniray.placement import (
    Bundle, BundleReservationLedger, ReservationState,
)
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector


pytestmark = pytest.mark.unit


def _rv(cpu: int) -> ResourceVector:
    return ResourceVector({"CPU": cpu})


def test_prepare_charges_root_once_and_commit_creates_independent_children() -> None:
    root = ResourceLedger(_rv(3))
    reservations = BundleReservationLedger(root)
    pg = PlacementGroupID.random()
    bundles = (Bundle(0, _rv(1)), Bundle(1, _rv(2)))

    assert reservations.prepare(pg, 0, bundles)
    assert reservations.prepare(pg, 0, bundles)
    assert root.available == ResourceVector.empty()
    assert len(root.snapshot().allocations) == 1
    assert reservations.commit(pg, 0)
    assert reservations.commit(pg, 0)

    first = reservations.ledger_for(pg, 0, 0)
    second = reservations.ledger_for(pg, 0, 1)
    assert first is reservations.ledger_for(pg, 0, 0)
    assert first.total == _rv(1) and second.total == _rv(2)
    first_token = first.allocate(_rv(1), AllocationToken.random())
    second_token = second.allocate(_rv(2), AllocationToken.random())
    # Child allocations consume already-reserved capacity, never root twice.
    assert len(root.snapshot().allocations) == 1
    assert root.available == ResourceVector.empty()
    assert first.release(first_token) and second.release(second_token)


def test_busy_committed_abort_fences_new_leases_until_finalize() -> None:
    root = ResourceLedger(_rv(1))
    reservations = BundleReservationLedger(root)
    pg = PlacementGroupID.random()
    assert reservations.prepare(pg, 4, (Bundle(0, _rv(1)),))
    assert reservations.commit(pg, 4)
    child = reservations.ledger_for(pg, 4, 0)
    task = child.allocate(_rv(1), AllocationToken.random())

    assert not reservations.abort(pg, 4)
    assert reservations.snapshot(pg, 4).state is ReservationState.REMOVING
    with pytest.raises(RuntimeError, match="committed"):
        reservations.ledger_for(pg, 4, 0)
    assert root.available == ResourceVector.empty()
    assert not reservations.finalize_remove(pg, 4)

    assert child.release(task)
    assert reservations.finalize_remove(pg, 4)
    assert reservations.finalize_remove(pg, 4)
    assert reservations.snapshot(pg, 4).state is ReservationState.ABORTED
    assert root.available == _rv(1)


def test_cpu_yielded_child_remains_live_until_terminal_release() -> None:
    root = ResourceLedger(_rv(1))
    reservations = BundleReservationLedger(root)
    pg = PlacementGroupID.random()
    assert reservations.prepare(pg, 5, (Bundle(0, _rv(1)),))
    assert reservations.commit(pg, 5)
    child = reservations.ledger_for(pg, 5, 0)
    task = child.allocate(_rv(1), AllocationToken.random())
    assert child.yield_cpu(task)

    assert not reservations.abort(pg, 5)
    assert reservations.snapshot(pg, 5).state is ReservationState.REMOVING
    assert not reservations.finalize_remove(pg, 5)
    # Yield returns only the child CPU; the root reservation must remain held.
    assert root.available == ResourceVector.empty()

    assert child.release(task)
    assert reservations.finalize_remove(pg, 5)
    assert reservations.snapshot(pg, 5).state is ReservationState.ABORTED
    assert root.available == _rv(1)


def test_zero_resource_active_child_remains_live_until_terminal_release() -> None:
    root = ResourceLedger(_rv(1))
    reservations = BundleReservationLedger(root)
    pg = PlacementGroupID.random()
    assert reservations.prepare(pg, 6, (Bundle(0, _rv(1)),))
    assert reservations.commit(pg, 6)
    child = reservations.ledger_for(pg, 6, 0)
    task = child.allocate(ResourceVector.empty(), AllocationToken.random())

    assert not reservations.abort(pg, 6)
    assert reservations.snapshot(pg, 6).state is ReservationState.REMOVING
    assert not reservations.finalize_remove(pg, 6)
    assert root.available == ResourceVector.empty()

    assert child.release(task)
    assert reservations.finalize_remove(pg, 6)
    assert reservations.snapshot(pg, 6).state is ReservationState.ABORTED
    assert root.available == _rv(1)


def test_remove_and_abort_tombstones_are_idempotent() -> None:
    root = ResourceLedger(_rv(2))
    reservations = BundleReservationLedger(root)
    pg = PlacementGroupID.random()

    assert reservations.begin_remove(pg, 7)
    assert reservations.begin_remove(pg, 7)
    assert not reservations.prepare(pg, 7, (Bundle(0, _rv(1)),))
    assert not reservations.commit(pg, 7)
    assert root.available == _rv(2)

    other = PlacementGroupID.random()
    assert reservations.prepare(other, 0, (Bundle(0, _rv(1)),))
    assert reservations.begin_remove(other, 0)
    assert reservations.snapshot(other, 0).state is ReservationState.ABORTED
    assert root.available == _rv(2)


def test_ledger_lookup_requires_exact_committed_identity_and_bundle() -> None:
    root = ResourceLedger(_rv(1))
    reservations = BundleReservationLedger(root)
    pg = PlacementGroupID.random()
    assert reservations.prepare(pg, 0, (Bundle(3, _rv(1)),))

    with pytest.raises(RuntimeError, match="committed"):
        reservations.ledger_for(pg, 0, 3)
    assert reservations.commit(pg, 0)
    with pytest.raises(KeyError):
        reservations.ledger_for(pg, 1, 3)
    with pytest.raises(KeyError):
        reservations.ledger_for(pg, 0, 2)
    with pytest.raises(TypeError):
        reservations.ledger_for(pg, 0, True)
