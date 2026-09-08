from __future__ import annotations

import pytest

from miniray import protocol
from miniray.actor_client import ActorClientTable
from miniray.ids import ActorGeneration, ActorID, NodeID, ObjectID, TaskID, WorkerID


pytestmark = pytest.mark.unit


def _alive(actor, generation, epoch, worker, last_exit=None):
    return protocol.ActorSnapshot(
        actor, generation, protocol.ActorState.ALIVE, epoch,
        generation.generation, 1, last_exit=last_exit,
        node_id=NodeID.random(), worker_id=worker,
        worker_address=("127.0.0.1", 12001), worker_pid=1201,
    )


def test_route_invalidation_fences_old_call_and_resets_new_generation_sequence():
    actor = ActorID.random()
    generation0 = ActorGeneration(actor, 0)
    worker0 = WorkerID.random()
    table = ActorClientTable()
    table.register(_alive(actor, generation0, 1, worker0), ("inc",))
    object0 = ObjectID.for_task(TaskID.random())
    _snapshot, sequence, fence = table.begin_call(actor, object0)
    assert sequence == 0 and table.can_publish(object0, fence)

    exit_record = protocol.ActorWorkerExitRecord(
        "exit", actor, generation0, 1, NodeID.random(), 123, 1, worker0, 456, -9
    )
    generation1 = generation0.next()
    restarting = protocol.ActorSnapshot(
        actor, generation1, protocol.ActorState.RESTARTING, 2, 1, 1, exit_record
    )
    changed, fenced = table.install(restarting)
    assert changed and fenced == (object0,)
    assert not table.can_publish(object0, fence)
    with pytest.raises(RuntimeError, match="not callable"):
        table.begin_call(actor, ObjectID.for_task(TaskID.random()))

    worker1 = WorkerID.random()
    table.install(_alive(actor, generation1, 3, worker1, exit_record))
    _snapshot, sequence, _fence = table.begin_call(
        actor, ObjectID.for_task(TaskID.random())
    )
    assert sequence == 0


def test_route_epoch_exact_replay_and_conflict():
    actor = ActorID.random()
    generation = ActorGeneration(actor, 0)
    table = ActorClientTable()
    snapshot = _alive(actor, generation, 1, WorkerID.random())
    table.register(snapshot, ("inc",))
    assert table.install(snapshot) == (False, ())
    conflicting = _alive(actor, generation, 1, WorkerID.random())
    with pytest.raises(ValueError, match="reused"):
        table.install(conflicting)


def test_provisional_route_is_not_callable_and_rollback_is_snapshot_cas():
    actor = ActorID.random()
    generation0 = ActorGeneration(actor, 0)
    provisional = protocol.ActorSnapshot(
        actor, generation0, protocol.ActorState.CREATING, 0, 0, 1
    )
    table = ActorClientTable()
    table.register(provisional, ("inc",))

    with pytest.raises(RuntimeError, match="CREATING"):
        table.begin_call(actor, ObjectID.for_task(TaskID.random()))

    alive = _alive(actor, generation0, 1, WorkerID.random())
    assert table.install(alive)[0]
    assert not table.remove(actor, provisional)
    with pytest.raises(ValueError, match="CREATING"):
        table.remove(actor, alive)
    assert table.snapshot(actor) == alive


def test_only_provisional_entries_can_be_removed_exactly_once():
    actor = ActorID.random()
    generation0 = ActorGeneration(actor, 0)
    provisional = protocol.ActorSnapshot(
        actor, generation0, protocol.ActorState.CREATING, 0, 0, 0
    )
    table = ActorClientTable()
    table.register(provisional, ("inc",))
    assert table.remove(actor, provisional)
    assert not table.remove(actor, provisional)

    table.register(provisional, ("inc",))
    dead = protocol.ActorSnapshot(
        actor, generation0, protocol.ActorState.DEAD, 1, 0, 0,
        error="constructor failed",
    )
    assert table.install(dead)[0]
    with pytest.raises(ValueError, match="CREATING"):
        table.remove(actor, dead)
    assert table.snapshot(actor) == dead
