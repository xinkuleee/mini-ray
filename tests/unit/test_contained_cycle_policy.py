"""Explicitly classified Python-cycle and contained-ObjectID-cycle contracts.

Synchronous cases are pure. The original concurrent prepare race uses real
ThreadPoolExecutor workers and an unbounded Barrier; it remains heavy until
its complete lifecycle has an independently reviewed bounded entry.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from miniray.contained_cycle import (
    ContainedContainerBusyError,
    ContainedGraphTransaction,
    ContainedGraphTransactionConflictError,
    ContainedGraphTransactionState,
    ContainedGraphTransactionStateError,
    ContainedReferenceCycleError,
    ContainedReferenceGraphAuthority,
)
from miniray.contained_edges import ContainedReferenceEdge
from miniray.dependency import decode_inline_argument, encode_task_argument
from miniray.ids import JobID, ObjectID, TaskID, WorkerID
from miniray.protocol import InlineArg


_JOB = JobID.from_hex("10" * 16)
_WORKER = WorkerID.from_hex("20" * 16)


def _object(index: int) -> ObjectID:
    return ObjectID.for_task(
        TaskID.derive(_JOB, TaskID.for_driver(_JOB), index), 0
    )


def _edge(
    source: ObjectID, target: ObjectID, token: str
) -> ContainedReferenceEdge:
    return ContainedReferenceEdge(
        source, target, _WORKER, ("127.0.0.1", 23001), token
    )


def _transaction(
    transaction_id: str, *edges: ContainedReferenceEdge
) -> ContainedGraphTransaction:
    return ContainedGraphTransaction(transaction_id, edges)


@pytest.mark.unit
def test_python_container_cycle_is_serialized_without_an_object_id_edge() -> None:
    value: list[object] = []
    value.append(value)

    encoded = encode_task_argument(value)

    assert isinstance(encoded, InlineArg)
    assert encoded.nested_refs == ()
    decoded = decode_inline_argument(encoded)
    assert isinstance(decoded, list)
    assert decoded[0] is decoded
    assert ContainedReferenceGraphAuthority().snapshot().committed_edges == ()


@pytest.mark.unit
def test_self_contained_object_id_is_rejected_without_mutation() -> None:
    authority = ContainedReferenceGraphAuthority()
    object_id = _object(1)
    transaction = _transaction(
        "self", _edge(object_id, object_id, "pin:self")
    )

    with pytest.raises(ContainedReferenceCycleError) as raised:
        authority.prepare(transaction)

    assert raised.value.cycle_path == (object_id, object_id)
    assert authority.snapshot().transactions == ()


@pytest.mark.unit
def test_whole_candidate_batch_is_rejected_atomically() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second, third = (_object(index) for index in range(2, 5))
    transaction = _transaction(
        "internal-cycle",
        _edge(first, second, "pin:first-second"),
        _edge(second, third, "pin:second-third"),
        _edge(third, first, "pin:third-first"),
    )

    with pytest.raises(ContainedReferenceCycleError) as raised:
        authority.publish(transaction)

    assert raised.value.cycle_path[0] == raised.value.cycle_path[-1]
    snapshot = authority.snapshot()
    assert snapshot.committed_edges == ()
    assert snapshot.prepared_edges == ()
    assert snapshot.transactions == ()


@pytest.mark.heavy
def test_prepared_edges_close_the_concurrent_preflight_race() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second = _object(5), _object(6)
    forward = _transaction(
        "forward", _edge(first, second, "pin:forward")
    )
    reverse = _transaction(
        "reverse", _edge(second, first, "pin:reverse")
    )
    barrier = Barrier(2)

    def prepare(
        transaction: ContainedGraphTransaction,
    ) -> tuple[str, object]:
        barrier.wait()
        try:
            return "prepared", authority.prepare(transaction)
        except ContainedReferenceCycleError as exc:
            return "cycle", exc.cycle_path

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(prepare, (forward, reverse)))

    assert sorted(kind for kind, _ in outcomes) == ["cycle", "prepared"]
    snapshot = authority.snapshot()
    assert len(snapshot.prepared_edges) == 1
    assert len(snapshot.transactions) == 1
    winner_id = snapshot.transactions[0][0]
    winner = forward if winner_id == "forward" else reverse
    loser = reverse if winner is forward else forward
    assert authority.commit(winner)
    with pytest.raises(ContainedGraphTransactionStateError):
        authority.commit(loser)
    with pytest.raises(ContainedReferenceCycleError):
        authority.publish(loser)
    assert authority.snapshot().transactions == (
        (winner.transaction_id, ContainedGraphTransactionState.COMMITTED),
    )


@pytest.mark.unit
def test_abort_removes_reservation_but_tombstones_its_identity() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second = _object(7), _object(8)
    forward = _transaction(
        "forward-abort", _edge(first, second, "pin:forward-abort")
    )
    reverse = _transaction(
        "reverse-after-abort",
        _edge(second, first, "pin:reverse-after-abort"),
    )

    assert authority.prepare(forward)
    assert authority.abort(forward)
    assert authority.publish(reverse)
    assert not authority.publish(reverse)
    with pytest.raises(ContainedGraphTransactionStateError):
        authority.prepare(forward)


@pytest.mark.unit
def test_abort_of_unseen_transaction_is_a_non_mutating_replay() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second = _object(70), _object(71)
    transaction = _transaction(
        "unseen-abort", _edge(first, second, "pin:unseen-abort")
    )

    assert not authority.abort(transaction)
    assert authority.snapshot().transactions == ()
    assert authority.prepare(transaction)


@pytest.mark.unit
def test_release_returns_full_obligations_and_opens_the_dag_again() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second = _object(9), _object(10)
    edge = _edge(first, second, "pin:release")
    forward = _transaction("forward-release", edge)
    reverse = _transaction(
        "reverse-after-release",
        _edge(second, first, "pin:reverse-release"),
    )

    assert authority.publish(forward)
    with pytest.raises(ContainedReferenceCycleError):
        authority.prepare(reverse)
    assert authority.release_container(first) == (edge,)
    assert authority.release_container(first) == ()
    assert not authority.commit(forward)
    assert authority.snapshot().committed_edges == ()
    assert authority.publish(reverse)


@pytest.mark.unit
def test_prepared_container_cannot_be_released_then_committed() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second = _object(11), _object(12)
    transaction = _transaction(
        "busy", _edge(first, second, "pin:busy")
    )

    assert authority.prepare(transaction)
    with pytest.raises(ContainedContainerBusyError):
        authority.release_container(first)
    assert authority.commit(transaction)


@pytest.mark.unit
def test_transaction_replay_is_exact_and_conflicts_are_non_mutating() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second, third = (_object(index) for index in range(13, 16))
    original = _transaction(
        "stable-id", _edge(first, second, "pin:stable")
    )
    conflict = _transaction(
        "stable-id", _edge(first, third, "pin:conflict")
    )

    assert authority.prepare(original)
    assert not authority.prepare(original)
    before = authority.snapshot()
    with pytest.raises(ContainedGraphTransactionConflictError):
        authority.prepare(conflict)
    assert authority.snapshot() == before
    assert authority.commit(original)
    assert not authority.commit(original)


@pytest.mark.unit
def test_multi_container_batch_reserves_one_manifest_atomically() -> None:
    authority = ContainedReferenceGraphAuthority()
    first, second, third = (_object(index) for index in range(16, 19))
    manifest = _transaction(
        "multi-container",
        _edge(first, second, "pin:multi-1"),
        _edge(second, third, "pin:multi-2"),
    )

    assert authority.prepare(manifest)
    assert authority.commit(manifest)
    assert authority.release_container(first) == (manifest.edges[0],)
    assert authority.snapshot().committed_edges == (manifest.edges[1],)
