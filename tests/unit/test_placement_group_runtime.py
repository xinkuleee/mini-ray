from __future__ import annotations

from dataclasses import replace

import pytest

pytestmark = pytest.mark.unit

from miniray import protocol
from miniray.ids import NodeID, PlacementGroupID
from miniray.placement import Bundle, PlacementStrategy
from miniray.placement_group_runtime import (
    AbortReservation, CommitReservation, ParticipantReplyStatus,
    ParticipantProgress, PlacementGroupAttempt, PlacementGroupConflictError,
    PlacementGroupCoordinator, PlacementGroupPhase, PlacementGroupSpec,
    PrepareReservation, ReservationReply,
)
from miniray.resources import NodeSnapshot, ResourceVector


def _id(cls, seed: int):
    return cls(bytes([seed]) * 16)


def _node(seed: int, *, total: int = 1, available: int = 1) -> NodeSnapshot:
    return NodeSnapshot(
        _id(NodeID, seed), ResourceVector({"CPU": total}),
        ResourceVector({"CPU": available}), True,
    )


def _spec(
    *, strategy=PlacementStrategy.STRICT_SPREAD, pg_seed: int = 9,
) -> PlacementGroupSpec:
    return PlacementGroupSpec(
        _id(PlacementGroupID, pg_seed),
        (Bundle(1, ResourceVector({"CPU": 1})),
         Bundle(0, ResourceVector({"CPU": 1}))),
        strategy,
    )


def _reply(operation, status, error=None):
    participant = operation.participant
    return ReservationReply(
        participant.attempt, participant.node_id, participant.digest, status, error
    )


def _death(
    node_id: NodeID, *, detection_id: str = "node-death-1",
    node_pid: int = 101, registration_epoch: int = 1, death_epoch: int = 2,
    detail: str = "participant process exited",
) -> protocol.NodeDeathRecord:
    return protocol.NodeDeathRecord(
        detection_id, node_id, node_pid, registration_epoch, death_epoch, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, detail,
    )


def _advance_to(
    coordinator: PlacementGroupCoordinator, phase: PlacementGroupPhase,
    *, pg_seed: int = 9,
):
    snapshot = coordinator.create(
        _spec(pg_seed=pg_seed), (_node(1), _node(2))
    )
    if phase is PlacementGroupPhase.PREPARING:
        return snapshot
    for operation in coordinator.next_operations(snapshot.spec.placement_group_id):
        snapshot = coordinator.apply_reply(
            _reply(operation, ParticipantReplyStatus.PREPARED)
        )
    if phase is PlacementGroupPhase.COMMITTING:
        return snapshot
    for operation in coordinator.next_operations(snapshot.spec.placement_group_id):
        snapshot = coordinator.apply_reply(
            _reply(operation, ParticipantReplyStatus.COMMITTED)
        )
    assert phase is PlacementGroupPhase.CREATED
    return snapshot


def test_spec_rejects_empty_bundle_set_before_planning() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        PlacementGroupSpec(
            _id(PlacementGroupID, 9), (), PlacementStrategy.STRICT_PACK
        )


@pytest.mark.parametrize("count", (3, 4))
def test_spec_rejects_more_than_two_bundles_before_planning(count) -> None:
    with pytest.raises(ValueError, match="at most two"):
        PlacementGroupSpec(
            _id(PlacementGroupID, 9),
            tuple(Bundle(index, ResourceVector({"CPU": 1})) for index in range(count)),
        )


def test_spec_defaults_to_strict_pack_and_rejects_soft_strategies() -> None:
    bundles = (Bundle(0, ResourceVector({"CPU": 1})),)
    assert PlacementGroupSpec(_id(PlacementGroupID, 9), bundles).strategy is PlacementStrategy.STRICT_PACK
    for strategy in ("PACK", "SPREAD"):
        with pytest.raises(ValueError):
            PlacementGroupSpec(_id(PlacementGroupID, 9), bundles, strategy)

def test_plan_freezes_canonical_participants_and_is_invisible_until_all_commits() -> None:
    coordinator = PlacementGroupCoordinator()
    snapshot = coordinator.create(_spec(), (_node(1), _node(2)))
    assert snapshot.phase is PlacementGroupPhase.PREPARING
    assert snapshot.plan is not None
    assert tuple(bundle.index for bundle in snapshot.plan.spec.bundles) == (0, 1)
    assert tuple(p.node_id for p in snapshot.plan.participants) == (_id(NodeID, 1), _id(NodeID, 2))
    assert coordinator.visible_placement(snapshot.spec.placement_group_id) is None

    prepares = coordinator.next_operations(snapshot.spec.placement_group_id)
    assert all(isinstance(item, PrepareReservation) for item in prepares)
    coordinator.apply_reply(_reply(prepares[0], ParticipantReplyStatus.PREPARED))
    assert coordinator.next_operations(snapshot.spec.placement_group_id) == (prepares[1],)
    after_prepare = coordinator.apply_reply(
        _reply(prepares[1], ParticipantReplyStatus.PREPARED)
    )
    assert after_prepare.phase is PlacementGroupPhase.COMMITTING
    commits = coordinator.next_operations(snapshot.spec.placement_group_id)
    coordinator.apply_reply(_reply(commits[0], ParticipantReplyStatus.COMMITTED))
    assert coordinator.visible_placement(snapshot.spec.placement_group_id) is None
    created = coordinator.apply_reply(_reply(commits[1], ParticipantReplyStatus.COMMITTED))
    assert created.phase is PlacementGroupPhase.CREATED
    assert coordinator.visible_placement(snapshot.spec.placement_group_id) == snapshot.plan


def test_reject_aborts_every_participant_and_exact_replies_are_idempotent() -> None:
    coordinator = PlacementGroupCoordinator()
    snapshot = coordinator.create(_spec(), (_node(1), _node(2)))
    prepares = coordinator.next_operations(snapshot.spec.placement_group_id)
    accepted = _reply(prepares[0], ParticipantReplyStatus.PREPARED)
    assert coordinator.apply_reply(accepted).phase is PlacementGroupPhase.PREPARING
    assert coordinator.apply_reply(accepted).phase is PlacementGroupPhase.PREPARING
    rejected = _reply(prepares[1], ParticipantReplyStatus.REJECTED, "busy")
    aborting = coordinator.apply_reply(rejected)
    assert aborting.phase is PlacementGroupPhase.ABORTING
    aborts = coordinator.next_operations(snapshot.spec.placement_group_id)
    assert len(aborts) == 2 and all(isinstance(item, AbortReservation) for item in aborts)
    coordinator.apply_reply(_reply(aborts[0], ParticipantReplyStatus.ABORTED))
    removed = coordinator.apply_reply(_reply(aborts[1], ParticipantReplyStatus.ABORTED))
    assert removed.phase is PlacementGroupPhase.REMOVED
    assert coordinator.visible_placement(snapshot.spec.placement_group_id) is None


def test_commit_ambiguity_reissues_only_missing_operation() -> None:
    coordinator = PlacementGroupCoordinator()
    snapshot = coordinator.create(_spec(), (_node(1), _node(2)))
    for operation in coordinator.next_operations(snapshot.spec.placement_group_id):
        coordinator.apply_reply(_reply(operation, ParticipantReplyStatus.PREPARED))
    commits = coordinator.next_operations(snapshot.spec.placement_group_id)
    coordinator.apply_reply(_reply(commits[0], ParticipantReplyStatus.COMMITTED))
    assert coordinator.next_operations(snapshot.spec.placement_group_id) == (commits[1],)
    assert coordinator.snapshot(snapshot.spec.placement_group_id).phase is PlacementGroupPhase.COMMITTING


def test_reply_validates_attempt_node_digest_and_phase() -> None:
    coordinator = PlacementGroupCoordinator()
    snapshot = coordinator.create(_spec(), (_node(1), _node(2)))
    prepare = coordinator.next_operations(snapshot.spec.placement_group_id)[0]
    valid = _reply(prepare, ParticipantReplyStatus.PREPARED)
    with pytest.raises(PlacementGroupConflictError, match="stale attempt"):
        coordinator.apply_reply(replace(
            valid, attempt=PlacementGroupAttempt(valid.attempt.placement_group_id, 1)
        ))
    with pytest.raises(PlacementGroupConflictError, match="non-participant"):
        coordinator.apply_reply(replace(valid, node_id=_id(NodeID, 8)))
    with pytest.raises(PlacementGroupConflictError, match="digest"):
        coordinator.apply_reply(replace(valid, digest="0" * 64))
    with pytest.raises(PlacementGroupConflictError, match="phase"):
        coordinator.apply_reply(replace(valid, status=ParticipantReplyStatus.COMMITTED))


def test_pending_and_infeasible_have_no_participant_operations() -> None:
    pending = PlacementGroupCoordinator()
    pending_snapshot = pending.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK), (_node(1, total=2, available=0),)
    )
    assert pending_snapshot.phase is PlacementGroupPhase.PENDING
    assert pending.next_operations(pending_snapshot.spec.placement_group_id) == ()

    infeasible = PlacementGroupCoordinator()
    infeasible_snapshot = infeasible.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK), (_node(1),)
    )
    assert infeasible_snapshot.phase is PlacementGroupPhase.INFEASIBLE
    assert infeasible.next_operations(infeasible_snapshot.spec.placement_group_id) == ()


def test_retry_pending_uses_fresh_capacity_without_changing_attempt() -> None:
    coordinator = PlacementGroupCoordinator()
    initial = coordinator.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK),
        (_node(1, total=2, available=0),),
    )

    still_pending = coordinator.retry_pending(
        initial.spec.placement_group_id,
        (_node(1, total=2, available=0),),
    )
    assert still_pending.phase is PlacementGroupPhase.PENDING
    assert still_pending.attempt == initial.attempt
    assert still_pending.plan is None

    preparing = coordinator.retry_pending(
        initial.spec.placement_group_id,
        (_node(1, total=2, available=2),),
    )
    assert preparing.phase is PlacementGroupPhase.PREPARING
    assert preparing.attempt == initial.attempt
    assert preparing.plan is not None
    assert coordinator.next_operations(initial.spec.placement_group_id)

    with pytest.raises(PlacementGroupConflictError, match="only a pending"):
        coordinator.retry_pending(
            initial.spec.placement_group_id, (_node(1, total=2, available=2),)
        )


def test_retry_pending_can_converge_to_terminal_infeasible() -> None:
    coordinator = PlacementGroupCoordinator()
    initial = coordinator.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK),
        (_node(1, total=2, available=0),),
    )

    infeasible = coordinator.retry_pending(
        initial.spec.placement_group_id, (_node(1, total=0, available=0),)
    )

    assert infeasible.phase is PlacementGroupPhase.INFEASIBLE
    assert infeasible.attempt == initial.attempt
    assert infeasible.plan is None
    assert coordinator.next_operations(initial.spec.placement_group_id) == ()


def test_created_remove_uses_abort_operations_and_visibility_closes_immediately() -> None:
    coordinator = PlacementGroupCoordinator()
    snapshot = coordinator.create(_spec(), (_node(1), _node(2)))
    for operation in coordinator.next_operations(snapshot.spec.placement_group_id):
        coordinator.apply_reply(_reply(operation, ParticipantReplyStatus.PREPARED))
    for operation in coordinator.next_operations(snapshot.spec.placement_group_id):
        coordinator.apply_reply(_reply(operation, ParticipantReplyStatus.COMMITTED))
    removing = coordinator.remove(snapshot.spec.placement_group_id)
    assert removing.phase is PlacementGroupPhase.REMOVING
    assert coordinator.visible_placement(snapshot.spec.placement_group_id) is None
    operations = coordinator.next_operations(snapshot.spec.placement_group_id)
    for operation in operations:
        assert isinstance(operation, AbortReservation)
        final = coordinator.apply_reply(_reply(operation, ParticipantReplyStatus.ABORTED))
    assert final.phase is PlacementGroupPhase.REMOVED


def test_shutdown_cancel_maps_each_phase_to_cleanup_or_terminal() -> None:
    pending = PlacementGroupCoordinator()
    pending_snapshot = pending.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK),
        (_node(1, total=2, available=0),),
    )
    cancelled_pending = pending.cancel_for_shutdown(
        pending_snapshot.spec.placement_group_id
    )
    assert cancelled_pending.phase is PlacementGroupPhase.REMOVED
    assert pending.next_operations(pending_snapshot.spec.placement_group_id) == ()

    preparing = PlacementGroupCoordinator()
    preparing_snapshot = preparing.create(_spec(), (_node(1), _node(2)))
    cancelled_preparing = preparing.cancel_for_shutdown(
        preparing_snapshot.spec.placement_group_id
    )
    assert cancelled_preparing.phase is PlacementGroupPhase.ABORTING
    assert len(preparing.next_operations(preparing_snapshot.spec.placement_group_id)) == 2

    created = PlacementGroupCoordinator()
    created_snapshot = created.create(_spec(), (_node(1), _node(2)))
    for operation in created.next_operations(created_snapshot.spec.placement_group_id):
        created.apply_reply(_reply(operation, ParticipantReplyStatus.PREPARED))
    for operation in created.next_operations(created_snapshot.spec.placement_group_id):
        created.apply_reply(_reply(operation, ParticipantReplyStatus.COMMITTED))
    removing = created.cancel_for_shutdown(created_snapshot.spec.placement_group_id)
    assert removing.phase is PlacementGroupPhase.REMOVING
    assert created.visible_placement(created_snapshot.spec.placement_group_id) is None
    assert len(created.next_operations(created_snapshot.spec.placement_group_id)) == 2


def test_participant_progress_binds_complete_node_death_proof() -> None:
    node_id = _id(NodeID, 1)
    death = _death(node_id)

    progress = ParticipantProgress(
        node_id, prepared=True, aborted=True, death=death
    )

    assert progress.death == death
    with pytest.raises(ValueError, match="another NodeID"):
        ParticipantProgress(
            node_id, aborted=True, death=_death(_id(NodeID, 2))
        )
    with pytest.raises(ValueError, match="cleanup satisfied"):
        ParticipantProgress(node_id, death=death)
    with pytest.raises(ValueError, match="also be prepared"):
        ParticipantProgress(node_id, committed=True)


@pytest.mark.parametrize(
    "starting_phase",
    (
        PlacementGroupPhase.PREPARING,
        PlacementGroupPhase.COMMITTING,
        PlacementGroupPhase.CREATED,
    ),
)
def test_participant_death_makes_active_or_visible_attempt_terminal_lost(
    starting_phase: PlacementGroupPhase,
) -> None:
    coordinator = PlacementGroupCoordinator()
    before = _advance_to(coordinator, starting_phase)
    placement_group_id = before.spec.placement_group_id
    death = _death(_id(NodeID, 1))

    affected = coordinator.fail_node(death)

    assert len(affected) == 1
    lost = affected[0]
    assert lost.phase is PlacementGroupPhase.LOST
    assert lost.plan == before.plan
    assert coordinator.visible_placement(placement_group_id) is None
    dead, survivor = lost.participants
    assert dead.node_id == death.node_id
    assert dead.aborted and dead.death == death
    assert survivor.node_id == _id(NodeID, 2) and survivor.death is None
    operations = coordinator.next_operations(placement_group_id)
    assert len(operations) == 1
    assert isinstance(operations[0], AbortReservation)
    assert operations[0].participant.node_id == survivor.node_id

    # The complete proof is an idempotency identity, not merely a NodeID hint.
    assert coordinator.fail_node(death) == affected
    assert coordinator.next_operations(placement_group_id) == operations


def test_nonparticipant_death_is_a_strict_no_op() -> None:
    coordinator = PlacementGroupCoordinator()
    before = coordinator.create(_spec(), (_node(1), _node(2)))
    operations = coordinator.next_operations(before.spec.placement_group_id)

    assert coordinator.fail_node(_death(_id(NodeID, 8))) == ()
    assert coordinator.snapshot(before.spec.placement_group_id) == before
    assert coordinator.next_operations(before.spec.placement_group_id) == operations


def test_conflicting_node_or_detection_proof_mutates_no_pg_state() -> None:
    coordinator = PlacementGroupCoordinator()
    first_pg = coordinator.create(
        _spec(pg_seed=9), (_node(1), _node(2))
    )
    second_pg = coordinator.create(
        _spec(pg_seed=10), (_node(1), _node(2))
    )
    death = _death(_id(NodeID, 1), detection_id="shared-proof")
    first_reduction = coordinator.fail_node(death)
    assert len(first_reduction) == 2
    before = (
        coordinator.snapshot(first_pg.spec.placement_group_id),
        coordinator.snapshot(second_pg.spec.placement_group_id),
    )
    obligations = (
        coordinator.next_operations(first_pg.spec.placement_group_id),
        coordinator.next_operations(second_pg.spec.placement_group_id),
    )
    proof_indexes = (
        dict(coordinator._node_deaths),
        dict(coordinator._death_detections),
    )

    with pytest.raises(PlacementGroupConflictError, match="NodeID"):
        coordinator.fail_node(
            _death(
                death.node_id, detection_id="another-proof",
                death_epoch=3, detail="conflicting node tombstone",
            )
        )
    assert (
        coordinator.snapshot(first_pg.spec.placement_group_id),
        coordinator.snapshot(second_pg.spec.placement_group_id),
    ) == before
    assert (
        coordinator.next_operations(first_pg.spec.placement_group_id),
        coordinator.next_operations(second_pg.spec.placement_group_id),
    ) == obligations
    assert (
        coordinator._node_deaths, coordinator._death_detections
    ) == proof_indexes

    with pytest.raises(PlacementGroupConflictError, match="detection_id"):
        coordinator.fail_node(
            _death(
                _id(NodeID, 2), detection_id=death.detection_id,
                death_epoch=4, detail="rebound detection identity",
            )
        )
    assert (
        coordinator.snapshot(first_pg.spec.placement_group_id),
        coordinator.snapshot(second_pg.spec.placement_group_id),
    ) == before
    assert (
        coordinator.next_operations(first_pg.spec.placement_group_id),
        coordinator.next_operations(second_pg.spec.placement_group_id),
    ) == obligations
    assert (
        coordinator._node_deaths, coordinator._death_detections
    ) == proof_indexes


def test_malformed_death_proof_is_rejected_before_any_mutation() -> None:
    coordinator = PlacementGroupCoordinator()
    before = coordinator.create(_spec(), (_node(1), _node(2)))
    placement_group_id = before.spec.placement_group_id
    operations = coordinator.next_operations(placement_group_id)
    malformed = object.__new__(protocol.NodeDeathRecord)
    object.__setattr__(malformed, "detection_id", "malformed")
    object.__setattr__(malformed, "node_id", _id(NodeID, 1))
    object.__setattr__(malformed, "node_pid", 0)
    object.__setattr__(malformed, "registration_epoch", 1)
    object.__setattr__(malformed, "death_epoch", 2)
    object.__setattr__(malformed, "exit_code", -9)
    object.__setattr__(
        malformed, "reason", protocol.NodeDeathReason.PROCESS_EXIT
    )
    object.__setattr__(malformed, "detail", "invalid pid")

    with pytest.raises(Exception):
        coordinator.fail_node(malformed)

    assert coordinator.snapshot(placement_group_id) == before
    assert coordinator.next_operations(placement_group_id) == operations
    assert coordinator._node_deaths == {}
    assert coordinator._death_detections == {}


def test_late_prepare_and_commit_replies_cannot_resurrect_lost_visibility() -> None:
    coordinator = PlacementGroupCoordinator()
    preparing = coordinator.create(_spec(), (_node(1), _node(2)))
    placement_group_id = preparing.spec.placement_group_id
    prepare_operations = coordinator.next_operations(placement_group_id)
    accepted_before_loss = _reply(
        prepare_operations[0], ParticipantReplyStatus.PREPARED
    )
    coordinator.apply_reply(accepted_before_loss)
    lost = coordinator.fail_node(_death(prepare_operations[0].participant.node_id))[0]
    assert lost.phase is PlacementGroupPhase.LOST

    # An ACK accepted before death remains an exact replay, but it cannot move
    # the terminal phase.  Previously unseen late prepare/commit ACKs conflict.
    assert coordinator.apply_reply(accepted_before_loss).phase is PlacementGroupPhase.LOST
    late_prepare = _reply(prepare_operations[1], ParticipantReplyStatus.PREPARED)
    late_commit = _reply(prepare_operations[1], ParticipantReplyStatus.COMMITTED)
    with pytest.raises(PlacementGroupConflictError, match="phase"):
        coordinator.apply_reply(late_prepare)
    with pytest.raises(PlacementGroupConflictError, match="phase"):
        coordinator.apply_reply(late_commit)
    assert coordinator.snapshot(placement_group_id).phase is PlacementGroupPhase.LOST
    assert coordinator.visible_placement(placement_group_id) is None

    abort = coordinator.next_operations(placement_group_id)[0]
    final = coordinator.apply_reply(_reply(abort, ParticipantReplyStatus.ABORTED))
    assert final.phase is PlacementGroupPhase.LOST
    assert coordinator.next_operations(placement_group_id) == ()
    assert coordinator.visible_placement(placement_group_id) is None


def test_exact_late_committed_replay_cannot_mutate_lost_attempt() -> None:
    coordinator = PlacementGroupCoordinator()
    committing = _advance_to(coordinator, PlacementGroupPhase.COMMITTING)
    placement_group_id = committing.spec.placement_group_id
    commits = coordinator.next_operations(placement_group_id)
    accepted_before_loss = _reply(
        commits[0], ParticipantReplyStatus.COMMITTED
    )
    coordinator.apply_reply(accepted_before_loss)
    lost = coordinator.fail_node(_death(commits[0].participant.node_id))[0]
    before = coordinator.snapshot(placement_group_id)
    obligations = coordinator.next_operations(placement_group_id)
    assert lost == before and lost.phase is PlacementGroupPhase.LOST

    assert coordinator.apply_reply(accepted_before_loss) == before
    assert coordinator.snapshot(placement_group_id) == before
    assert coordinator.next_operations(placement_group_id) == obligations
    with pytest.raises(PlacementGroupConflictError, match="phase"):
        coordinator.apply_reply(
            _reply(commits[1], ParticipantReplyStatus.COMMITTED)
        )
    assert coordinator.snapshot(placement_group_id) == before
    assert coordinator.next_operations(placement_group_id) == obligations
    assert coordinator.visible_placement(placement_group_id) is None


def test_survivor_abort_ack_leaves_terminal_phase_lost() -> None:
    coordinator = PlacementGroupCoordinator()
    snapshot = coordinator.create(
        _spec(), (_node(1), _node(2))
    )
    placement_group_id = snapshot.spec.placement_group_id

    lost = coordinator.fail_node(_death(_id(NodeID, 1)))[0]
    aborts = coordinator.next_operations(placement_group_id)

    assert lost.phase is PlacementGroupPhase.LOST
    assert tuple(
        operation.participant.node_id for operation in aborts
    ) == (_id(NodeID, 2),)
    for operation in aborts:
        reduced = coordinator.apply_reply(
            _reply(operation, ParticipantReplyStatus.ABORTED)
        )
        assert reduced.phase is PlacementGroupPhase.LOST
    assert coordinator.next_operations(placement_group_id) == ()
    assert coordinator.visible_placement(placement_group_id) is None


def test_pending_infeasible_and_removed_are_unaffected_by_node_death() -> None:
    pending = PlacementGroupCoordinator()
    pending_before = pending.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK),
        (_node(1, total=2, available=0),),
    )
    assert pending_before.phase is PlacementGroupPhase.PENDING
    assert pending.fail_node(_death(_id(NodeID, 1))) == ()
    assert pending.snapshot(pending_before.spec.placement_group_id) == pending_before

    infeasible = PlacementGroupCoordinator()
    infeasible_before = infeasible.create(
        _spec(strategy=PlacementStrategy.STRICT_PACK), (_node(1),)
    )
    assert infeasible_before.phase is PlacementGroupPhase.INFEASIBLE
    assert infeasible.fail_node(_death(_id(NodeID, 1))) == ()
    assert (
        infeasible.snapshot(infeasible_before.spec.placement_group_id)
        == infeasible_before
    )

    removed = PlacementGroupCoordinator()
    created = _advance_to(removed, PlacementGroupPhase.CREATED)
    placement_group_id = created.spec.placement_group_id
    removed.remove(placement_group_id)
    for operation in removed.next_operations(placement_group_id):
        removed_snapshot = removed.apply_reply(
            _reply(operation, ParticipantReplyStatus.ABORTED)
        )
    assert removed_snapshot.phase is PlacementGroupPhase.REMOVED
    assert removed.fail_node(_death(_id(NodeID, 1))) == ()
    assert removed.snapshot(placement_group_id) == removed_snapshot


@pytest.mark.parametrize(
    "cleanup_phase",
    (PlacementGroupPhase.ABORTING, PlacementGroupPhase.REMOVING),
)
def test_participant_death_satisfies_cleanup_without_rewriting_cleanup_intent(
    cleanup_phase: PlacementGroupPhase,
) -> None:
    coordinator = PlacementGroupCoordinator()
    if cleanup_phase is PlacementGroupPhase.ABORTING:
        snapshot = coordinator.create(_spec(), (_node(1), _node(2)))
        operations = coordinator.next_operations(snapshot.spec.placement_group_id)
        snapshot = coordinator.apply_reply(
            _reply(operations[0], ParticipantReplyStatus.REJECTED, "busy")
        )
    else:
        snapshot = _advance_to(coordinator, PlacementGroupPhase.CREATED)
        snapshot = coordinator.remove(snapshot.spec.placement_group_id)
    assert snapshot.phase is cleanup_phase
    placement_group_id = snapshot.spec.placement_group_id
    death = _death(_id(NodeID, 1))

    after_death = coordinator.fail_node(death)[0]

    assert after_death.phase is cleanup_phase
    dead = next(item for item in after_death.participants if item.node_id == death.node_id)
    assert dead.aborted and dead.death == death
    obligations = coordinator.next_operations(placement_group_id)
    assert len(obligations) == 1
    assert isinstance(obligations[0], AbortReservation)
    assert obligations[0].participant.node_id == _id(NodeID, 2)

    removed = coordinator.apply_reply(
        _reply(obligations[0], ParticipantReplyStatus.ABORTED)
    )
    assert removed.phase is PlacementGroupPhase.REMOVED
    # Exact death replay remains valid after survivor cleanup completed.
    assert coordinator.fail_node(death) == (removed,)
