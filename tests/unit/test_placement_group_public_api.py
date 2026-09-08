"""Pure contracts for the deliberately synchronous public PG surface."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

import miniray as ray
from miniray import api, protocol
from miniray.core import CoreWorker
from miniray.ids import NodeID, PlacementGroupID


pytestmark = pytest.mark.unit


def _reply(bundle_count: int = 2) -> protocol.CreatePlacementGroupReply:
    placement_group_id = PlacementGroupID.random()
    return protocol.CreatePlacementGroupReply(
        placement_group_id,
        0,
        True,
        protocol.PlacementGroupPhaseStatus.CREATED,
        tuple(
            protocol.PlacementGroupSchedulingKey(
                placement_group_id, 0, index, NodeID.random(),
                format(index + 1, "064x"),
            )
            for index in range(bundle_count)
        ),
    )


def _core(monkeypatch: pytest.MonkeyPatch, reply: protocol.CreatePlacementGroupReply) -> CoreWorker:
    core = object.__new__(CoreWorker)
    monkeypatch.setattr(api, "_active_core_worker", lambda: core)
    monkeypatch.setattr(
        core, "create_placement_group", lambda bundles, strategy: reply
    )
    monkeypatch.setattr(
        core,
        "assert_placement_group_task_admissible",
        lambda placement_group_id, attempt: None,
    )
    return core


def test_create_returns_immutable_runtime_bound_committed_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reply = _reply()
    core = _core(monkeypatch, reply)
    group = ray.placement_group(
        [{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD"
    )

    assert isinstance(group, ray.PlacementGroup)
    assert group.bundle_count == 2
    assert group.placements == reply.placements
    assert group._scheduling_key_for(core, 1) == reply.placements[1]
    with pytest.raises(FrozenInstanceError):
        group.attempt = 2  # type: ignore[misc]


def test_protocol_never_constructs_an_accepted_but_uncommitted_group() -> None:
    created = _reply(1)
    with pytest.raises(protocol.ProtocolError, match="must report CREATED"):
        protocol.CreatePlacementGroupReply(
            created.placement_group_id,
            created.attempt,
            True,
            protocol.PlacementGroupPhaseStatus.COMMITTING,
            created.placements,
        )


def test_remote_options_forward_exact_bundle_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reply = _reply()
    core = _core(monkeypatch, reply)
    submitted: dict[str, object] = {}
    monkeypatch.setattr(
        core, "define_remote_function", lambda function: object()
    )

    def submit(*args: object, **kwargs: object) -> object:
        submitted.update(kwargs)
        return "ref"

    monkeypatch.setattr(core, "submit", submit)
    group = ray.placement_group([{"CPU": 1}, {"CPU": 1}])
    task = ray.remote(lambda: 1).options(
        placement_group=group, bundle_index=1
    )

    assert task.remote() == "ref"
    assert submitted["placement_group_scheduling_key"] == reply.placements[1]


def test_options_require_a_valid_group_and_bundle_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reply = _reply(1)
    _core(monkeypatch, reply)
    task = ray.remote(lambda: None)

    with pytest.raises(ValueError, match="specified together"):
        task.options(placement_group=ray.placement_group([{"CPU": 1}]))
    with pytest.raises(ValueError, match="specified together"):
        task.options(bundle_index=0)
    with pytest.raises(TypeError, match="must be a PlacementGroup"):
        task.options(placement_group=object(), bundle_index=0)
    group = ray.placement_group([{"CPU": 1}])
    with pytest.raises(ValueError, match="identify a bundle"):
        task.options(placement_group=group, bundle_index=1).remote()


def test_handle_cannot_cross_runtime_and_remove_validates_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reply = _reply(1)
    owner = _core(monkeypatch, reply)
    group = ray.placement_group([{"CPU": 1}])
    other = object.__new__(CoreWorker)
    monkeypatch.setattr(api, "_active_core_worker", lambda: other)

    with pytest.raises(ValueError, match="different mini-Ray runtime"):
        ray.remote(lambda: None).options(
            placement_group=group, bundle_index=0
        ).remote()
    with pytest.raises(ValueError, match="different mini-Ray runtime"):
        ray.remove_placement_group(group)

    monkeypatch.setattr(api, "_active_core_worker", lambda: owner)
    monkeypatch.setattr(
        owner,
        "remove_placement_group",
        lambda placement_group_id, attempt: protocol.RemovePlacementGroupReply(
            placement_group_id, attempt, True, True,
            protocol.PlacementGroupPhaseStatus.REMOVED,
        ),
    )
    assert ray.remove_placement_group(group)


def test_removed_or_removing_handle_is_fenced_before_function_export_or_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reply = _reply(1)
    core = _core(monkeypatch, reply)
    group = ray.placement_group([{"CPU": 1}])
    task = ray.remote(lambda: None).options(
        placement_group=group, bundle_index=0
    )
    exported = False
    submitted = False

    def reject(_placement_group_id: object, _attempt: int) -> None:
        raise ValueError("placement group is not active for task submission: REMOVING")

    def export(_function: object) -> object:
        nonlocal exported
        exported = True
        return object()

    def submit(*_args: object, **_kwargs: object) -> object:
        nonlocal submitted
        submitted = True
        return object()

    monkeypatch.setattr(core, "assert_placement_group_task_admissible", reject)
    monkeypatch.setattr(core, "define_remote_function", export)
    monkeypatch.setattr(core, "submit", submit)

    with pytest.raises(ValueError, match="REMOVING"):
        task.remote()
    assert not exported
    assert not submitted


def test_actor_pg_options_are_explicitly_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reply = _reply(1)
    _core(monkeypatch, reply)
    group = ray.placement_group([{"CPU": 1}])

    class Actor:
        pass

    with pytest.raises(NotImplementedError, match="Actor creation"):
        ray.remote(placement_group=group, bundle_index=0)(Actor)
    with pytest.raises(NotImplementedError, match="Actor creation"):
        ray.remote(Actor).options(placement_group=group, bundle_index=0)
