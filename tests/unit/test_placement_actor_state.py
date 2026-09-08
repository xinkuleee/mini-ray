from __future__ import annotations

from dataclasses import dataclass

import pytest

pytestmark = pytest.mark.unit

from miniray.actor_state import (
    ActorCallConflictError,
    ActorCallKey,
    ActorCallState,
    ActorMailbox,
    ActorMailboxStateError,
    ActorSubmitStatus,
)
from miniray.ids import ActorGeneration, ActorID, NodeID, PlacementGroupID, WorkerID
from miniray.placement import (
    Bundle,
    BundleReservationLedger,
    PlacementPlanner,
    PlacementStatus,
    PlacementStrategy,
    ReservationConflictError,
    ReservationState,
)
from miniray.resources import NodeSnapshot, ResourceVector


def rv(**values: int) -> ResourceVector:
    return ResourceVector(values)


def make_id(cls, value: str):
    # Opaque IDs are fixed-width bytes.  A deterministic repeated byte keeps
    # expected placements readable without depending on random values.
    return cls(bytes([sum(value.encode("utf-8")) % 251]) * 16)


def node(name: str, total: ResourceVector, available: ResourceVector) -> NodeSnapshot:
    return NodeSnapshot(make_id(NodeID, name), total, available, alive=True)


def test_pack_reuses_nodes_while_spread_uses_distinct_nodes() -> None:
    nodes = [node("n1", rv(CPU=4), rv(CPU=4)), node("n2", rv(CPU=4), rv(CPU=4))]
    bundles = [Bundle(0, rv(CPU=1)), Bundle(1, rv(CPU=1))]
    planner = PlacementPlanner()

    packed = planner.plan(bundles, nodes, PlacementStrategy.PACK)
    spread = planner.plan(bundles, nodes, PlacementStrategy.SPREAD)

    assert packed.status is PlacementStatus.SUCCESS
    assert packed.node_for(0) == packed.node_for(1)
    assert spread.status is PlacementStatus.SUCCESS
    assert spread.node_for(0) != spread.node_for(1)


def test_strict_pack_requires_one_node_to_hold_the_entire_group() -> None:
    nodes = [node("n1", rv(CPU=2), rv(CPU=2)), node("n2", rv(CPU=2), rv(CPU=2))]
    bundles = [Bundle(0, rv(CPU=2)), Bundle(1, rv(CPU=2))]

    plan = PlacementPlanner().plan(bundles, nodes, PlacementStrategy.STRICT_PACK)

    assert plan.status is PlacementStatus.INFEASIBLE


def test_strict_spread_uses_matching_instead_of_greedy_first_fit() -> None:
    # The GPU bundle can run only on n1.  A naive CPU-first greedy assignment
    # to n1 would fail, while bipartite matching moves CPU to n2.
    nodes = [
        node("n1", rv(CPU=1, GPU=1), rv(CPU=1, GPU=1)),
        node("n2", rv(CPU=1), rv(CPU=1)),
    ]
    bundles = [Bundle(0, rv(CPU=1)), Bundle(1, rv(GPU=1))]

    plan = PlacementPlanner().plan(bundles, nodes, PlacementStrategy.STRICT_SPREAD)

    assert plan.status is PlacementStatus.SUCCESS
    assert plan.node_for(0) == make_id(NodeID, "n2")
    assert plan.node_for(1) == make_id(NodeID, "n1")


def test_planner_distinguishes_temporarily_unavailable_from_infeasible() -> None:
    busy = [node("n1", rv(CPU=4), rv(CPU=0))]
    planner = PlacementPlanner()

    pending = planner.plan([rv(CPU=2)], busy)
    infeasible = planner.plan([rv(CPU=8)], busy)

    assert pending.status is PlacementStatus.PENDING
    assert infeasible.status is PlacementStatus.INFEASIBLE


@dataclass(frozen=True)
class FakeToken:
    number: int
    resources: ResourceVector


class FakeResourceLedger:
    def __init__(self, available: ResourceVector) -> None:
        self.available = available
        self.next_token = 0
        self.live: dict[int, FakeToken] = {}
        self.release_count = 0

    def allocate(self, resources: ResourceVector) -> FakeToken | None:
        if not resources.fits_in(self.available):
            return None
        self.available = self.available - resources
        token = FakeToken(self.next_token, resources)
        self.next_token += 1
        self.live[token.number] = token
        return token

    def release(self, token: FakeToken) -> None:
        assert self.live.pop(token.number) == token
        self.available = self.available + token.resources
        self.release_count += 1


def test_prepare_failure_rolls_back_every_bundle_allocated_on_the_node() -> None:
    resources = FakeResourceLedger(rv(CPU=2))
    ledger = BundleReservationLedger(resources)  # type: ignore[arg-type]
    pg = make_id(PlacementGroupID, "pg")

    prepared = ledger.prepare(
        pg, 0, [Bundle(0, rv(CPU=1)), Bundle(1, rv(CPU=2))]
    )

    assert not prepared
    assert resources.available == rv(CPU=2)
    assert resources.live == {}
    # Atomic prepare checks the aggregate before allocating any bundle.
    assert resources.next_token == 0
    assert ledger.snapshot(pg, 0).state is ReservationState.ABORTED  # type: ignore[union-attr]


def test_reservation_protocol_is_idempotent_and_abort_rolls_back_commit() -> None:
    resources = FakeResourceLedger(rv(CPU=2))
    ledger = BundleReservationLedger(resources)  # type: ignore[arg-type]
    pg = make_id(PlacementGroupID, "pg")
    bundles = [Bundle(0, rv(CPU=1))]

    assert ledger.prepare(pg, 3, bundles)
    assert ledger.prepare(pg, 3, bundles)
    assert len(resources.live) == 1
    assert ledger.commit(pg, 3)
    assert ledger.commit(pg, 3)
    assert ledger.snapshot(pg, 3).state is ReservationState.COMMITTED  # type: ignore[union-attr]

    assert ledger.abort(pg, 3)
    assert ledger.abort(pg, 3)
    assert resources.available == rv(CPU=2)
    assert resources.release_count == 1
    assert not ledger.commit(pg, 3)
    assert not ledger.prepare(pg, 3, bundles)


def test_reusing_transaction_key_with_different_payload_is_rejected() -> None:
    resources = FakeResourceLedger(rv(CPU=4))
    ledger = BundleReservationLedger(resources)  # type: ignore[arg-type]
    pg = make_id(PlacementGroupID, "pg")
    assert ledger.prepare(pg, 0, [rv(CPU=1)])

    with pytest.raises(ReservationConflictError):
        ledger.prepare(pg, 0, [rv(CPU=2)])


def test_abort_before_prepare_fences_late_prepare() -> None:
    resources = FakeResourceLedger(rv(CPU=2))
    ledger = BundleReservationLedger(resources)  # type: ignore[arg-type]
    pg = make_id(PlacementGroupID, "pg")

    assert ledger.abort(pg, 7)
    assert not ledger.prepare(pg, 7, [rv(CPU=1)])
    assert resources.available == rv(CPU=2)


def actor_id(value: str = "actor") -> ActorID:
    return make_id(ActorID, value)


def generation(actor: ActorID, value: int) -> ActorGeneration:
    return ActorGeneration(actor, value)


def worker(value: str) -> WorkerID:
    return make_id(WorkerID, value)


def test_actor_mailbox_buffers_gaps_and_admits_each_caller_in_fifo_order() -> None:
    aid = actor_id()
    gen = generation(aid, 0)
    caller_a = worker("a")
    caller_b = worker("b")
    mailbox = ActorMailbox(aid, gen)

    assert mailbox.submit(
        generation=gen, caller_id=caller_a, sequence=1, payload="a1"
    ).status is ActorSubmitStatus.BUFFERED
    mailbox.submit(generation=gen, caller_id=caller_b, sequence=0, payload="b0")
    admission = mailbox.submit(
        generation=gen, caller_id=caller_a, sequence=0, payload="a0"
    )

    assert [call.payload for call in admission.admitted] == ["a0", "a1"]
    assert [mailbox.take_next().payload] == ["b0"]  # type: ignore[union-attr]
    assert mailbox.take_next() is None  # Serial actor: current call must finish.
    mailbox.complete_current("done-b")
    assert mailbox.take_next().payload == "a0"  # type: ignore[union-attr]
    mailbox.complete_current("done-a0")
    assert mailbox.take_next().payload == "a1"  # type: ignore[union-attr]


def test_actor_mailbox_deduplicates_calls_and_returns_cached_result() -> None:
    aid = actor_id()
    gen = generation(aid, 0)
    caller = worker("caller")
    mailbox = ActorMailbox(aid, gen)

    mailbox.submit(generation=gen, caller_id=caller, sequence=0, payload="inc")
    mailbox.take_next()
    mailbox.complete_current(42)
    duplicate = mailbox.submit(
        generation=gen, caller_id=caller, sequence=0, payload="inc"
    )

    assert duplicate.status is ActorSubmitStatus.DUPLICATE
    assert duplicate.has_cached_result
    assert duplicate.cached_result == 42
    with pytest.raises(ActorCallConflictError):
        mailbox.submit(
            generation=gen, caller_id=caller, sequence=0, payload="different"
        )


def test_actor_generation_fences_old_calls_and_resets_caller_sequences() -> None:
    aid = actor_id()
    old = generation(aid, 0)
    new = generation(aid, 1)
    caller = worker("caller")
    mailbox = ActorMailbox(aid, old)
    mailbox.submit(generation=old, caller_id=caller, sequence=0, payload="old")

    abandoned = mailbox.advance_generation(new)

    assert [call.payload for call in abandoned] == ["old"]
    stale = mailbox.submit(
        generation=old, caller_id=caller, sequence=1, payload="stale"
    )
    fresh = mailbox.submit(
        generation=new, caller_id=caller, sequence=0, payload="fresh"
    )
    assert stale.status is ActorSubmitStatus.FENCED
    assert fresh.status is ActorSubmitStatus.ACCEPTED
    assert mailbox.call_state(ActorCallKey(caller, 0)) is ActorCallState.QUEUED


def test_actor_mailbox_rejects_generation_for_another_actor() -> None:
    aid = actor_id("actor-a")
    other = actor_id("actor-b")

    with pytest.raises(ValueError):
        ActorMailbox(aid, ActorGeneration(other, 0))


def test_actor_mailbox_transition_helper_rejects_skipped_state() -> None:
    aid = actor_id()
    gen = generation(aid, 0)
    caller = worker("caller")
    mailbox = ActorMailbox(aid, gen)
    mailbox.submit(generation=gen, caller_id=caller, sequence=0, payload="call")

    record = mailbox._records[ActorCallKey(caller, 0)]
    record.state = ActorCallState.BUFFERED
    with pytest.raises(ActorMailboxStateError, match="BUFFERED -> RUNNING"):
        mailbox.take_next()
