"""Owner-fence outbox contracts with explicit execution classification.

Nine cases are synchronous pure registry transitions. The original registration
race remains heavy: it starts two real threads and has unbounded Barrier waits,
without a reviewed failure/teardown deadline. Its body is deliberately retained.
No module-level unit marker: pytest adds it even to a heavy-decorated case.
"""

from __future__ import annotations

from dataclasses import replace
from threading import Barrier, Thread

import pytest

from miniray import protocol
from miniray.ids import NodeID, WorkerID
from miniray.owner_death_fence_registry import (
    OwnerDeathFenceEffect, OwnerDeathFenceRegistry,
    OwnerDeathFenceRegistryConflictError,
    OwnerDeathFenceRegistryStateError, OwnerDeathFenceTerminal,
    OwnerFenceNodeIncarnation, UnknownOwnerDeathFenceEffectError,
)


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _node(byte: int, *, epoch: int | None = None) -> OwnerFenceNodeIncarnation:
    return OwnerFenceNodeIncarnation(
        _id(NodeID, byte), 4000 + byte, byte if epoch is None else epoch
    )


def _death(
    byte: int, *, death_epoch: int | None = None,
    detection_id: str | None = None,
    reason: protocol.WorkerDeathReason = protocol.WorkerDeathReason.PROCESS_EXIT,
) -> protocol.WorkerDeathRecord:
    return protocol.WorkerDeathRecord(
        detection_id or "owner-death-{}".format(byte),
        protocol.WorkerIncarnation(
            _id(NodeID, 100 + byte), 5000 + byte, 10 + byte,
            _id(WorkerID, byte), 6000 + byte,
        ),
        byte if death_epoch is None else death_epoch, -9, reason,
    )


def _reply(
    effect: OwnerDeathFenceEffect,
) -> protocol.InstallOwnerDeathFenceReply:
    return protocol.InstallOwnerDeathFenceReply(
        effect.request, protocol.OwnerDeathFenceDisposition.FENCED, ()
    )


def _node_death(
    target: OwnerFenceNodeIncarnation, *, death_epoch: int = 50,
) -> protocol.NodeDeathRecord:
    return protocol.NodeDeathRecord(
        "node-death-{}".format(target.node_id.hex), target.node_id,
        target.node_pid, target.registration_epoch, death_epoch, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "test Node exit",
    )


@pytest.mark.unit
def test_owner_death_fans_out_to_every_live_node_without_publications() -> None:
    registry = OwnerDeathFenceRegistry()
    first, second = _node(1), _node(2)
    registry.register_node(second)
    registry.register_node(first)

    pending = registry.commit_owner_death(_death(10))

    assert tuple(effect.key.target for effect in pending) == (first, second)
    assert all(effect.request.expected_replicas == () for effect in pending)
    assert all(
        effect.request.scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
        for effect in pending
    )
    assert len({effect.request.request_id for effect in pending}) == 2
    assert registry.has_active_operations()
    assert registry.cleanup_safe_node_ids() == ()


@pytest.mark.unit
def test_late_registration_gets_every_historical_owner_fence_before_safe() -> None:
    registry = OwnerDeathFenceRegistry()
    later = _node(3)
    first = _death(11, death_epoch=1)
    second = _death(12, death_epoch=2)
    assert registry.commit_owner_death(second) == ()
    assert registry.commit_owner_death(first) == ()
    assert not registry.has_active_operations()

    bootstrap = registry.register_node(later)

    assert tuple(effect.owner_death for effect in bootstrap) == (first, second)
    assert not registry.node_bootstrap_complete(later)
    assert registry.has_active_operations()
    registry.acknowledge(bootstrap[0], _reply(bootstrap[0]))
    assert not registry.node_bootstrap_complete(later)
    registry.acknowledge(bootstrap[1], _reply(bootstrap[1]))
    assert registry.node_bootstrap_complete(later)
    assert registry.cleanup_safe_node_ids() == (later.node_id,)
    assert not registry.has_active_operations()


@pytest.mark.unit
def test_registration_and_death_orders_produce_the_same_exact_effect() -> None:
    target = _node(4)
    death = _death(13)
    before = OwnerDeathFenceRegistry()
    before.register_node(target)
    effect_before = before.commit_owner_death(death)[0]
    after = OwnerDeathFenceRegistry()
    after.commit_owner_death(death)
    effect_after = after.register_node(target)[0]

    assert effect_before == effect_after
    assert effect_before.request.request_id.startswith(
        "owner-death-global-fence:"
    )
    assert effect_before.request.owner_death == death
    assert effect_before.request.node_id == target.node_id


@pytest.mark.unit
def test_exact_replays_do_not_duplicate_and_conflicting_proofs_fail() -> None:
    registry = OwnerDeathFenceRegistry()
    target = _node(5)
    death = _death(14)
    assert registry.register_node(target) == ()
    original = registry.commit_owner_death(death)

    assert registry.commit_owner_death(death) == original
    assert registry.register_node(target) == original
    assert registry.snapshot().pending == original
    with pytest.raises(
        OwnerDeathFenceRegistryConflictError, match="another death proof"
    ):
        registry.commit_owner_death(replace(
            death, detection_id="changed-proof", death_epoch=99
        ))
    with pytest.raises(
        OwnerDeathFenceRegistryConflictError, match="another incarnation"
    ):
        registry.register_node(OwnerFenceNodeIncarnation(
            target.node_id, target.node_pid + 1, target.registration_epoch + 1
        ))


@pytest.mark.unit
def test_acknowledgement_is_exact_and_lost_ack_replay_is_idempotent() -> None:
    registry = OwnerDeathFenceRegistry()
    first, second = _node(6), _node(7)
    registry.register_node(first)
    registry.register_node(second)
    effects = registry.commit_owner_death(_death(15))
    first_effect, second_effect = effects
    reply = _reply(first_effect)

    completion = registry.acknowledge(first_effect, reply)
    replay = registry.acknowledge(first_effect, reply)

    assert replay is completion
    assert completion.terminal is OwnerDeathFenceTerminal.ACKNOWLEDGED
    assert registry.pending() == (second_effect,)
    with pytest.raises(
        OwnerDeathFenceRegistryStateError, match="exact effect"
    ):
        registry.acknowledge(second_effect, reply)
    forged = OwnerDeathFenceEffect.create(_death(16), first)
    with pytest.raises(UnknownOwnerDeathFenceEffectError):
        registry.acknowledge(forged, _reply(forged))


@pytest.mark.unit
def test_pending_queries_are_owner_and_exact_node_scoped() -> None:
    registry = OwnerDeathFenceRegistry()
    first, second = _node(8), _node(9)
    registry.register_node(first)
    registry.register_node(second)
    first_death, second_death = _death(17), _death(18)
    registry.commit_owner_death(first_death)
    registry.commit_owner_death(second_death)

    assert len(registry.pending_for_node(first)) == 2
    assert len(registry.pending_for_owner(first_death.worker_id)) == 2
    first_effect = registry.pending_for_owner(first_death.worker_id)[0]
    registry.acknowledge(first_effect, _reply(first_effect))
    assert len(registry.pending_for_owner(first_death.worker_id)) == 1
    assert len(registry.pending_for_node(first_effect.key.target)) == 1
    with pytest.raises(OwnerDeathFenceRegistryStateError):
        registry.pending_for_owner(_id(WorkerID, 99))


@pytest.mark.unit
def test_exact_node_death_discharges_only_that_incarnation_pending_work() -> None:
    registry = OwnerDeathFenceRegistry()
    first, second = _node(20), _node(21)
    registry.register_node(first)
    registry.register_node(second)
    registry.commit_owner_death(_death(19))

    completions = registry.mark_node_dead(_node_death(first))

    assert len(completions) == 1
    assert completions[0].terminal is OwnerDeathFenceTerminal.NODE_DEAD
    assert completions[0].node_death.node_id == first.node_id
    assert tuple(effect.key.target for effect in registry.pending()) == (second,)
    assert registry.node_bootstrap_complete(first) is False
    assert registry.node_bootstrap_complete(second) is False
    with pytest.raises(
        OwnerDeathFenceRegistryConflictError, match="another registered"
    ):
        registry.mark_node_dead(_node_death(OwnerFenceNodeIncarnation(
            second.node_id, second.node_pid + 1, second.registration_epoch
        ), death_epoch=51))


@pytest.mark.unit
def test_fence_ack_and_node_death_race_has_one_monotonic_terminal() -> None:
    target = _node(25)
    death = _death(25)

    death_first = OwnerDeathFenceRegistry()
    death_first.register_node(target)
    death_effect = death_first.commit_owner_death(death)[0]
    death_terminal = death_first.mark_node_dead(_node_death(target))[0]
    late_ack = death_first.acknowledge(death_effect, _reply(death_effect))
    assert late_ack is death_terminal
    assert late_ack.terminal is OwnerDeathFenceTerminal.NODE_DEAD
    assert not death_first.has_active_operations()

    ack_first = OwnerDeathFenceRegistry()
    ack_first.register_node(target)
    ack_effect = ack_first.commit_owner_death(death)[0]
    ack_terminal = ack_first.acknowledge(ack_effect, _reply(ack_effect))
    assert ack_first.mark_node_dead(_node_death(target)) == ()
    assert ack_first.snapshot().completed == (ack_terminal,)
    assert ack_terminal.terminal is OwnerDeathFenceTerminal.ACKNOWLEDGED
    assert not ack_first.has_active_operations()


@pytest.mark.heavy
def test_registration_owner_death_race_never_misses_cross_product_entry() -> None:
    registry = OwnerDeathFenceRegistry()
    target = _node(22)
    death = _death(23)
    barrier = Barrier(3)
    failures: list[BaseException] = []

    def register() -> None:
        try:
            barrier.wait()
            registry.register_node(target)
        except BaseException as exc:
            failures.append(exc)

    def die() -> None:
        try:
            barrier.wait()
            registry.commit_owner_death(death)
        except BaseException as exc:
            failures.append(exc)

    threads = (Thread(target=register), Thread(target=die))
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(1.0)

    assert failures == []
    assert all(not thread.is_alive() for thread in threads)
    pending = registry.pending()
    assert len(pending) == 1
    assert pending[0] == OwnerDeathFenceEffect.create(death, target)
    assert not registry.node_bootstrap_complete(target)


@pytest.mark.unit
def test_expected_worker_exit_is_not_an_owner_death_fence() -> None:
    registry = OwnerDeathFenceRegistry()
    registry.register_node(_node(24))

    with pytest.raises(
        OwnerDeathFenceRegistryStateError, match="PROCESS_EXIT or NODE_EXIT"
    ):
        registry.commit_owner_death(_death(24, reason=protocol.WorkerDeathReason.EXPECTED))

    assert registry.pending() == ()
    assert not registry.has_active_operations()
