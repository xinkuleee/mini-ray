from __future__ import annotations

import pytest
from threading import RLock

from miniray import protocol
from miniray.control import DeadNodeError, GCSLite, NodeRegistry
from miniray.ids import NodeID
from miniray.resources import ResourceVector
from miniray.trace import EventSink


pytestmark = pytest.mark.unit


def _node(byte: int = 1) -> NodeID:
    return NodeID(bytes([byte]) * 16)


def _register(
    node_id: NodeID | None = None, *, pid: int = 4101, port: int = 14101
) -> protocol.RegisterNode:
    return protocol.RegisterNode(
        node_id or _node(), pid, ("127.0.0.1", port),
        ResourceVector({"CPU": 2}), ResourceVector({"CPU": 2}),
    )


def _death(
    registration: protocol.RegisterNodeReply, *, detection_id: str = "death-1",
    detail: str = "node process exited",
) -> protocol.ReportNodeDeath:
    return protocol.ReportNodeDeath(
        detection_id, registration.node_id, registration.node_pid,
        registration.registration_epoch, 23,
        protocol.NodeDeathReason.PROCESS_EXIT, detail,
    )


def _gcs(callback=None) -> GCSLite:
    gcs = object.__new__(GCSLite)
    gcs.nodes = NodeRegistry()
    gcs._on_node_dead = callback
    gcs.event_sink = EventSink()
    gcs._snapshot_lock = RLock()
    return gcs


def test_registration_replay_keeps_epochs_and_dead_node_cannot_resurrect() -> None:
    registry = NodeRegistry()
    request = _register()

    first = registry.register_message(request)
    replay = registry.register_message(request)

    assert first == replay
    assert first.accepted
    assert first.registration_epoch == 1
    assert first.membership_epoch == 1

    death = registry.report_death(_death(first))
    assert death.disposition is protocol.NodeDeathDisposition.APPLIED
    assert death.membership_epoch == 2
    rejected = registry.register_message(request)
    assert not rejected.accepted
    assert rejected.registration_epoch == first.registration_epoch
    assert rejected.membership_epoch == death.membership_epoch
    assert "cannot be registered again" in (rejected.error or "")


def test_resource_sequence_exact_replay_stale_conflict_and_dead_fencing() -> None:
    registry = NodeRegistry()
    registration = registry.register_message(_register())
    available = ResourceVector({"CPU": 1})

    assert registry.update_resources(
        registration.node_id, registration.node_pid,
        registration.registration_epoch, 1, available,
    )
    assert not registry.update_resources(
        registration.node_id, registration.node_pid,
        registration.registration_epoch, 1, available,
    )
    replayed_registration = registry.register_message(_register())
    assert replayed_registration == registration
    assert registry.get(registration.node_id).available_resources == available
    assert registry.get(registration.node_id).resource_report_seq == 1
    with pytest.raises(Exception, match="reused"):
        registry.update_resources(
            registration.node_id, registration.node_pid,
            registration.registration_epoch, 1, ResourceVector(),
        )
    with pytest.raises(Exception, match="stale"):
        registry.update_resources(
            registration.node_id, registration.node_pid,
            registration.registration_epoch, 0, available,
        )
    with pytest.raises(Exception, match="incarnation"):
        registry.update_resources(
            registration.node_id, registration.node_pid + 1,
            registration.registration_epoch, 2, available,
        )

    registry.report_death(_death(registration))
    with pytest.raises(DeadNodeError):
        registry.update_resources(
            registration.node_id, registration.node_pid,
            registration.registration_epoch, 2, available,
        )


def test_death_is_atomic_idempotent_and_live_views_exclude_tombstone() -> None:
    registry = NodeRegistry()
    first = registry.register_message(_register(_node(1), pid=4101, port=14101))
    second = registry.register_message(_register(_node(2), pid=4102, port=14102))
    request = _death(first)

    applied = registry.report_death(request)
    registry.update_resources(
        second.node_id, second.node_pid, second.registration_epoch, 1,
        ResourceVector({"CPU": 1}),
    )
    replay = registry.report_death(request)

    assert applied.disposition is protocol.NodeDeathDisposition.APPLIED
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert replay.death == applied.death
    assert replay.membership_epoch == applied.membership_epoch == 3
    assert replay.live_nodes == applied.live_nodes
    assert tuple(node.node_id for node in applied.live_nodes) == (second.node_id,)
    assert tuple(node.node_id for node in registry.snapshot()) == (second.node_id,)
    assert {node.node_id for node in registry.all_snapshot()} == {
        first.node_id, second.node_id
    }
    assert registry.get(first.node_id).state is protocol.NodeMembershipState.DEAD
    with pytest.raises(DeadNodeError):
        registry.address(first.node_id)

    conflict = registry.report_death(_death(first, detail="different payload"))
    assert conflict.disposition is protocol.NodeDeathDisposition.CONFLICT
    assert conflict.membership_epoch == applied.membership_epoch
    unknown = registry.report_death(
        protocol.ReportNodeDeath(
            "unknown", _node(9), 4909, 9, 1,
            protocol.NodeDeathReason.PROCESS_EXIT, "unknown node",
        )
    )
    assert unknown.disposition is protocol.NodeDeathDisposition.UNKNOWN
    assert unknown.membership_epoch == applied.membership_epoch


def test_detection_id_is_globally_bound_to_one_death_proof() -> None:
    registry = NodeRegistry()
    first = registry.register_message(_register(_node(1), pid=4101, port=14101))
    second = registry.register_message(_register(_node(2), pid=4102, port=14102))
    applied = registry.report_death(_death(first, detection_id="shared-proof"))

    reused = registry.report_death(
        protocol.ReportNodeDeath(
            "shared-proof", second.node_id, second.node_pid,
            second.registration_epoch, 24,
            protocol.NodeDeathReason.PROCESS_EXIT, "another process exited",
        )
    )

    assert applied.disposition is protocol.NodeDeathDisposition.APPLIED
    assert reused.disposition is protocol.NodeDeathDisposition.CONFLICT
    assert registry.get(second.node_id).state is protocol.NodeMembershipState.ALIVE


def test_gcs_typed_queries_and_death_callback_fire_only_on_first_transition() -> None:
    callbacks: list[protocol.NodeDeathRecord] = []
    gcs = _gcs(callbacks.append)
    assert "report_node_death" in gcs.handlers
    assert "get_node_state" in gcs.handlers
    request = _register()
    registration = gcs.register_node(request)
    assert registration.accepted
    alive = gcs.get_nodes(protocol.GetNodes())
    assert alive.membership_epoch == registration.membership_epoch
    assert len(alive.nodes) == 1
    assert alive.nodes[0].state is protocol.NodeMembershipState.ALIVE

    death_request = _death(registration)
    applied = gcs.report_node_death(death_request)
    replay = gcs.report_node_death(death_request)
    assert applied.disposition is protocol.NodeDeathDisposition.APPLIED
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert callbacks == [applied.death]
    assert gcs.get_nodes(protocol.GetNodes()).nodes == ()

    state = gcs.get_node_state(protocol.GetNodeState(request.node_id))
    assert state.found and state.state is protocol.NodeMembershipState.DEAD
    assert state.death == applied.death
    address = gcs.get_node_address(protocol.GetNodeAddress(request.node_id))
    assert not address.found and "DEAD" in (address.error or "")


def test_expected_unregister_creates_a_queryable_dead_tombstone() -> None:
    gcs = _gcs()
    registration = gcs.register_node(_register())
    request = protocol.UnregisterNode(
        registration.node_id, registration.node_pid,
        registration.registration_epoch, "expected-stop",
    )

    removed = gcs.unregister_node(request)
    replay = gcs.unregister_node(request)

    assert removed.removed and replay.removed
    assert removed.death == replay.death
    assert removed.death is not None
    assert removed.death.reason is protocol.NodeDeathReason.EXPECTED
    state = gcs.get_node_state(protocol.GetNodeState(registration.node_id))
    assert state.state is protocol.NodeMembershipState.DEAD


def test_expected_unregister_retires_actor_without_owner_ack_obligation() -> None:
    gcs = _gcs()
    failures = []
    gcs.actor_coordinator = type("ActorCoordinatorProbe", (), {
        "fail_node": lambda self, node_id, error, **options: failures.append(
            (node_id, error, options)
        )
    })()
    registration = gcs.register_node(_register())
    request = protocol.UnregisterNode(
        registration.node_id, registration.node_pid,
        registration.registration_epoch, "expected-actor-stop",
    )

    removed = gcs.unregister_node(request)
    replay = gcs.unregister_node(request)

    assert removed.removed and replay.removed
    assert removed.death is not None
    assert removed.death.reason is protocol.NodeDeathReason.EXPECTED
    assert len(failures) == 2
    assert all(
        failure[0] == registration.node_id
        and failure[2] == {"require_owner_ack": False}
        for failure in failures
    )


def test_expected_report_node_death_uses_post_barrier_actor_retirement() -> None:
    gcs = _gcs()
    failures = []
    gcs.actor_coordinator = type("ActorCoordinatorProbe", (), {
        "fail_node": lambda self, node_id, error, **options: failures.append(
            (node_id, error, options)
        )
    })()
    registration = gcs.register_node(_register())
    report = protocol.ReportNodeDeath(
        "expected-report", registration.node_id, registration.node_pid,
        registration.registration_epoch, 0, protocol.NodeDeathReason.EXPECTED,
        "finalized normally",
    )

    applied = gcs.report_node_death(report)
    replay = gcs.report_node_death(report)

    assert applied.disposition is protocol.NodeDeathDisposition.APPLIED
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert len(failures) == 2
    assert all(
        failure[0] == registration.node_id
        and failure[2] == {"require_owner_ack": False}
        for failure in failures
    )


def test_membership_protocol_rejects_unbound_or_inconsistent_identity() -> None:
    node_id = _node()
    with pytest.raises(Exception, match="positive"):
        protocol.RegisterNode(
            node_id, 0, ("127.0.0.1", 14001), ResourceVector({"CPU": 1})
        )
    with pytest.raises(Exception, match="positive"):
        protocol.NodeInfo(
            node_id, 4101, 0, ("127.0.0.1", 14001),
            ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1}),
        )
    with pytest.raises(Exception, match="live scheduling"):
        protocol.NodeInfo(
            node_id, 4101, 1, ("127.0.0.1", 14001),
            ResourceVector({"CPU": 1}), ResourceVector(),
            protocol.NodeMembershipState.DEAD,
        )
    with pytest.raises(Exception, match="death record"):
        protocol.GetNodeStateReply(
            node_id, True, 1, protocol.NodeMembershipState.DEAD, 4101, 1
        )
    with pytest.raises(Exception, match="requires an error"):
        protocol.ReportNodeDeathReply(
            "detect", node_id, 4101, protocol.NodeDeathDisposition.UNKNOWN,
            0, (),
        )
