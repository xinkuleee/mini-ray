from __future__ import annotations

from threading import Event, RLock

import pytest

from miniray import protocol
from miniray.control import (
    ABORT_PLACEMENT_GROUP_HANDLER,
    COMMIT_PLACEMENT_GROUP_HANDLER,
    PREPARE_PLACEMENT_GROUP_HANDLER,
    GCSLite,
    NodeRegistry,
    PlacementGroupControlCoordinator,
    PlacementGroupPrepareFailureConfig,
    _PlacementGroupPrepareFailureRPC,
)
from miniray.ids import NodeID, PlacementGroupID
from miniray.node import NodeServer
from miniray import api as ray_api
from miniray.object_store import ObjectStore
from miniray.placement import BundleReservationLedger, ReservationState
from miniray.resources import ResourceLedger, ResourceVector
from miniray.trace import EventSink, MemoryEventSink


pytestmark = pytest.mark.unit


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _nodes() -> tuple[NodeRegistry, NodeID, NodeID]:
    nodes = NodeRegistry()
    first = _id(NodeID, 1)
    second = _id(NodeID, 2)
    nodes.register(
        first, ("127.0.0.1", 12001), ResourceVector({"CPU": 1}),
        node_pid=4101,
    )
    nodes.register(
        second, ("127.0.0.1", 12002), ResourceVector({"CPU": 1}),
        node_pid=4102,
    )
    return nodes, first, second


def _request() -> protocol.CreatePlacementGroupRequest:
    return protocol.CreatePlacementGroupRequest(
        _id(PlacementGroupID, 9),
        0,
        (
            protocol.PlacementGroupBundle(
                0, ResourceVector({"CPU": 1})
            ),
            protocol.PlacementGroupBundle(
                1, ResourceVector({"CPU": 1})
            ),
        ),
        "STRICT_SPREAD",
    )


def _accepted_reply(request: object) -> object:
    values = (
        request.placement_group_id, request.attempt, request.node_id,
        request.plan_digest, request.phase, True, True,
    )
    if isinstance(request, protocol.PreparePlacementGroupRequest):
        return protocol.PreparePlacementGroupReply(*values)
    if isinstance(request, protocol.CommitPlacementGroupRequest):
        return protocol.CommitPlacementGroupReply(*values)
    if isinstance(request, protocol.AbortPlacementGroupRequest):
        return protocol.AbortPlacementGroupReply(*values)
    raise AssertionError(type(request).__name__)


def _ledger_participant(node_id: NodeID) -> NodeServer:
    """Build the real Node PG participant seam without sockets/processes."""

    node = object.__new__(NodeServer)
    node.node_id = node_id
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._bundle_reservations = BundleReservationLedger(node._ledger)
    node._placement_group_digests = {}
    node._cluster_nodes = ()
    node._gcs_address = None
    node._registered_with_gcs = False
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node._shutdown_request_id = None
    node._stop_event = Event()
    node._state_lock = RLock()
    node._gcs_lifecycle_lock = RLock()
    node._object_store = ObjectStore(1024)
    node.event_sink = None
    return node


def test_real_participants_rollback_partial_prepare_and_fence_exact_replay(
) -> None:
    """Bridge the GCS reducer to two real Node reservation ledgers."""

    nodes, first_id, second_id = _nodes()
    first = _ledger_participant(first_id)
    second = _ledger_participant(second_id)
    # GCS plans from its immutable summary while the second Node's local ledger
    # is the final authority.  This deterministic pre-existing allocation makes
    # the second real PREPARE return a typed rejection after the first succeeded.
    second_blocker = second.resource_ledger.allocate(
        ResourceVector({"CPU": 1})
    )
    baseline = {
        first_id: first.resource_ledger.snapshot(),
        second_id: second.resource_ledger.snapshot(),
    }
    by_address = {
        nodes.address(first_id): first,
        nodes.address(second_id): second,
    }
    prepares: list[protocol.PreparePlacementGroupRequest] = []
    aborts: list[protocol.AbortPlacementGroupRequest] = []
    first_prepared = None
    first_abort_ack_lost = True

    def participant_rpc(address, handler, request):
        nonlocal first_prepared, first_abort_ack_lost
        node = by_address[address]
        if handler == PREPARE_PLACEMENT_GROUP_HANDLER:
            prepares.append(request)
            reply = node._handle_prepare_placement_group(request)
            if request.node_id == first_id:
                first_prepared = (
                    node._bundle_reservations.snapshot(
                        request.placement_group_id, request.attempt
                    ),
                    node.resource_ledger.snapshot(),
                )
            return reply
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            aborts.append(request)
            reply = node._handle_abort_placement_group(request)
            if request.node_id == first_id and first_abort_ack_lost:
                first_abort_ack_lost = False
                # The Node committed ABORT, but GCS did not observe its ACK.
                raise TimeoutError("injected lost first abort ACK")
            return reply
        raise AssertionError(handler)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    unresolved = adapter.create(request)

    assert not unresolved.accepted
    assert unresolved.phase is protocol.PlacementGroupPhaseStatus.ABORTING
    assert tuple(item.node_id for item in prepares) == (first_id, second_id)
    assert first_prepared is not None
    prepared_reservation, prepared_root = first_prepared
    assert prepared_reservation.state is ReservationState.PREPARED
    assert prepared_root.available == ResourceVector()
    assert len(prepared_root.allocations) == 1
    assert next(
        item for item in prepares if item.node_id == second_id
    ) != prepares[0]
    assert tuple(item.node_id for item in aborts) == (first_id, second_id)

    for node_id, node in ((first_id, first), (second_id, second)):
        reservation = node._bundle_reservations.snapshot(
            request.placement_group_id, request.attempt
        )
        assert reservation is not None
        assert reservation.state is ReservationState.ABORTED
        assert node.resource_ledger.available == baseline[node_id].available
    after_ambiguous_abort = {
        first_id: first.resource_ledger.snapshot(),
        second_id: second.resource_ledger.snapshot(),
    }

    removed = adapter.create(request)

    assert not removed.accepted
    assert removed.phase is protocol.PlacementGroupPhaseStatus.REMOVED
    assert tuple(item.node_id for item in aborts) == (
        first_id, second_id, first_id
    )
    assert aborts[0] == aborts[2]
    assert adapter.visible_placement(request.placement_group_id) is None
    assert adapter._coordinator.next_operations(
        request.placement_group_id
    ) == ()
    assert first.resource_ledger.snapshot() == after_ambiguous_abort[first_id]
    assert second.resource_ledger.snapshot() == after_ambiguous_abort[second_id]

    # ABORT tombstones fence delayed PREPARE on both participants.  Replaying
    # the old messages cannot allocate again or resurrect a child ledger.
    for prepare, node in zip(prepares, (first, second)):
        late = node._handle_prepare_placement_group(prepare)
        assert not late.accepted and not late.applied
        assert node._bundle_reservations.snapshot(
            request.placement_group_id, request.attempt
        ).state is ReservationState.ABORTED
    assert first.resource_ledger.snapshot() == after_ambiguous_abort[first_id]
    assert second.resource_ledger.snapshot() == after_ambiguous_abort[second_id]
    assert second.resource_ledger.release(second_blocker)
    assert second.resource_ledger.available == second.resource_ledger.total


def test_prepare_failure_checkpoint_waits_for_real_prefix_and_binds_identity(
) -> None:
    first = _id(NodeID, 1)
    second = _id(NodeID, 2)
    first_request = protocol.PreparePlacementGroupRequest(
        _id(PlacementGroupID, 7), 0, first, "a" * 64,
        protocol.PlacementGroupParticipantPhase.PREPARE,
        (protocol.PlacementGroupBundle(0, ResourceVector({"CPU": 1})),),
    )
    second_request = protocol.PreparePlacementGroupRequest(
        first_request.placement_group_id, 0, second, "b" * 64,
        protocol.PlacementGroupParticipantPhase.PREPARE,
        (protocol.PlacementGroupBundle(1, ResourceVector({"CPU": 1})),),
    )
    real_calls: list[object] = []
    observations = MemoryEventSink()

    def real_rpc(_address, _handler, request):
        real_calls.append(request)
        return _accepted_reply(request)

    checkpoint = _PlacementGroupPrepareFailureRPC(
        PlacementGroupPrepareFailureConfig(2), real_rpc, observations
    )

    first_reply = checkpoint(
        ("127.0.0.1", 12001),
        PREPARE_PLACEMENT_GROUP_HANDLER,
        first_request,
    )
    rejected = checkpoint(
        ("127.0.0.1", 12002),
        PREPARE_PLACEMENT_GROUP_HANDLER,
        second_request,
    )
    replay = checkpoint(
        ("127.0.0.1", 12002),
        PREPARE_PLACEMENT_GROUP_HANDLER,
        second_request,
    )

    assert first_reply.accepted and first_reply.applied
    assert not rejected.accepted and not rejected.applied
    assert replay == rejected
    assert real_calls == [first_request]
    checkpoint.observe_rejection(second_request)
    checkpoint.observe_rejection(second_request)
    observed = observations.events
    assert len(observed) == 1
    assert observed[0].name == (
        "placement_group_prepare_rejected_by_failpoint"
    )
    assert observed[0].attributes["plan_digest"] == second_request.plan_digest
    assert observed[0].attributes["prepared_prefix"] == (str(first),)

    other = protocol.PreparePlacementGroupRequest(
        _id(PlacementGroupID, 8), 0, second, "c" * 64,
        protocol.PlacementGroupParticipantPhase.PREPARE,
        (protocol.PlacementGroupBundle(0, ResourceVector({"CPU": 1})),),
    )
    assert checkpoint(
        ("127.0.0.1", 12002), PREPARE_PLACEMENT_GROUP_HANDLER, other
    ).accepted
    assert real_calls == [first_request, other]


@pytest.mark.parametrize(
    "value,error",
    (
        (object(), TypeError),
        (PlacementGroupPrepareFailureConfig(3), ValueError),
    ),
)
def test_init_rejects_invalid_prepare_failure_config_before_spawning(
    value: object, error: type[Exception], monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ray_api.mp, "get_context",
        lambda _method: (_ for _ in ()).throw(
            AssertionError("validation must precede process creation")
        ),
    )
    with pytest.raises(error, match="placement.group.*prepare.failure"):
        ray_api.init(
            num_nodes=2,
            enable_tracing=False,
            _test_placement_group_prepare_failure=value,
        )


def _gcs_for(adapter: PlacementGroupControlCoordinator) -> GCSLite:
    """Construct only the pure shutdown state; never bind a test socket."""

    gcs = object.__new__(GCSLite)
    gcs.nodes = adapter._nodes
    gcs.placement_groups = adapter
    gcs.actor_coordinator = None
    gcs._on_node_dead = None
    gcs.event_sink = EventSink()
    gcs._snapshot_lock = RLock()
    gcs._stop_event = Event()
    gcs._shutdown_request_id = None
    gcs._shutdown_exit_scheduled = False
    gcs._placement_group_drain_request_id = None
    return gcs


def test_committed_node_death_marks_pg_lost_and_replay_redrives_survivor_abort(
) -> None:
    nodes, first, second = _nodes()
    fail_survivor_abort = False
    aborts: list[protocol.AbortPlacementGroupRequest] = []

    def participant_rpc(_address, handler, request):
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            aborts.append(request)
            if request.node_id == second and fail_survivor_abort:
                raise TimeoutError("survivor abort ACK is ambiguous")
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()
    assert adapter.create(request).accepted
    gcs = _gcs_for(adapter)
    victim = nodes.get(first)
    death = protocol.ReportNodeDeath(
        "pg-node-death", first, victim.node_pid,
        victim.registration_epoch, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "participant exited",
    )

    fail_survivor_abort = True
    applied = gcs.report_node_death(death)
    lost = adapter.snapshot(request.placement_group_id)
    assert applied.disposition is protocol.NodeDeathDisposition.APPLIED
    assert lost.phase.value == "LOST"
    assert adapter.visible_placement(request.placement_group_id) is None
    assert [item.node_id for item in aborts] == [second]
    assert adapter.has_active_operations()
    assert not adapter.begin_shutdown_cleanup()
    assert [item.node_id for item in aborts] == [second, second]

    fail_survivor_abort = False
    replay = gcs.report_node_death(death)
    final = adapter.snapshot(request.placement_group_id)
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert final.phase.value == "LOST"
    assert [item.node_id for item in aborts] == [second, second, second]
    assert aborts[0] == aborts[1] == aborts[2]
    assert not adapter.has_active_operations()


def test_expected_unregister_never_invokes_pg_loss_seam() -> None:
    nodes, first, _second = _nodes()
    node = nodes.get(first)
    calls: list[object] = []
    gcs = object.__new__(GCSLite)
    gcs.nodes = nodes
    gcs.placement_groups = type(
        "NarrowPGFixture", (),
        {"fail_node": lambda self, death: calls.append(death)},
    )()
    gcs.actor_coordinator = None
    gcs._on_node_dead = None
    gcs.event_sink = EventSink()

    request = protocol.UnregisterNode(
        first, node.node_pid, node.registration_epoch, "expected-pg-exit"
    )
    first_reply = gcs.unregister_node(request)
    replay = gcs.unregister_node(request)

    assert first_reply.removed and replay.removed
    assert first_reply.death is not None
    assert first_reply.death.reason is protocol.NodeDeathReason.EXPECTED
    assert calls == []


def test_happy_create_converges_prepare_then_commit_once() -> None:
    nodes, first, second = _nodes()
    calls: list[tuple[tuple[str, int], str, object]] = []

    def participant_rpc(address, handler, request):
        calls.append((address, handler, request))
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    created = adapter.create(request)
    replay = adapter.create(request)

    assert created.accepted and replay == created
    assert created.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert [handler for _address, handler, _request in calls] == [
        PREPARE_PLACEMENT_GROUP_HANDLER,
        PREPARE_PLACEMENT_GROUP_HANDLER,
        COMMIT_PLACEMENT_GROUP_HANDLER,
        COMMIT_PLACEMENT_GROUP_HANDLER,
    ]
    assert tuple(key.bundle_index for key in created.placements) == (0, 1)
    assert {key.node_id for key in created.placements} == {first, second}
    assert all(
        key.plan_digest
        == next(
            call.plan_digest
            for _address, handler, call in calls
            if handler == PREPARE_PLACEMENT_GROUP_HANDLER
            and call.node_id == key.node_id
        )
        for key in created.placements
    )


def test_create_publishes_bundle_keys_only_after_every_commit() -> None:
    nodes, first, second = _nodes()
    node_snapshot = nodes.snapshot()
    calls: list[tuple[tuple[str, int], str, object]] = []
    busy_once = {second}

    def participant_rpc(address, handler, request):
        calls.append((address, handler, request))
        if (
            handler == COMMIT_PLACEMENT_GROUP_HANDLER
            and request.node_id in busy_once
        ):
            busy_once.remove(request.node_id)
            return protocol.CommitPlacementGroupReply(
                request.placement_group_id, request.attempt, request.node_id,
                request.plan_digest, request.phase, True, False,
            )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    first_reply = adapter.create(request)

    assert not first_reply.accepted
    assert first_reply.phase is protocol.PlacementGroupPhaseStatus.COMMITTING
    assert "busy" in (first_reply.error or "")
    pending = adapter.get(
        protocol.GetPlacementGroupRequest(request.placement_group_id)
    )
    assert (
        pending.found
        and pending.phase is protocol.PlacementGroupPhaseStatus.COMMITTING
    )
    assert pending.placements == ()
    assert adapter.visible_placement(request.placement_group_id) is None

    created = adapter.create(request)

    assert created.accepted
    assert tuple(key.bundle_index for key in created.placements) == (0, 1)
    assert {key.node_id for key in created.placements} == {first, second}
    assert all(
        key.plan_digest
        == next(
            call[2].plan_digest
            for call in calls
            if call[1] == PREPARE_PLACEMENT_GROUP_HANDLER
            and call[2].node_id == key.node_id
        )
        for key in created.placements
    )
    visible = adapter.get(
        protocol.GetPlacementGroupRequest(request.placement_group_id)
    )
    assert visible.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert visible.placements == created.placements
    assert nodes.snapshot() == node_snapshot


def test_exact_pending_create_replay_replans_with_fresh_node_snapshot() -> None:
    nodes = NodeRegistry()
    first = _id(NodeID, 1)
    second = _id(NodeID, 2)
    nodes.register(
        first, ("127.0.0.1", 12001),
        ResourceVector({"CPU": 1}),
        node_pid=4101,
        available_resources=ResourceVector(),
    )
    nodes.register(
        second, ("127.0.0.1", 12002),
        ResourceVector({"CPU": 1}),
        node_pid=4102,
        available_resources=ResourceVector(),
    )
    calls = []

    def participant_rpc(_address, _handler, request):
        calls.append(request)
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    pending = adapter.create(request)
    assert not pending.accepted
    assert pending.phase is protocol.PlacementGroupPhaseStatus.PENDING
    assert calls == []

    nodes.update_resources(first, 4101, 1, 1, ResourceVector({"CPU": 1}))
    nodes.update_resources(second, 4102, 2, 1, ResourceVector({"CPU": 1}))
    created = adapter.create(request)

    assert created.accepted
    assert created.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert tuple(key.bundle_index for key in created.placements) == (0, 1)
    assert len(calls) == 4


def test_ambiguous_prepare_is_replayed_without_fabricating_rejection() -> None:
    nodes, _first, _second = _nodes()
    first_wire_request = None
    failed = False
    handlers: list[str] = []

    def participant_rpc(_address, handler, request):
        nonlocal failed, first_wire_request
        handlers.append(handler)
        if not failed:
            failed = True
            first_wire_request = request
            raise TimeoutError("ambiguous prepare")
        if first_wire_request is not None:
            assert request == first_wire_request
            first_wire_request = None
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    unresolved = adapter.create(request)
    assert not unresolved.accepted
    assert unresolved.phase is protocol.PlacementGroupPhaseStatus.PREPARING
    assert "unresolved" in (unresolved.error or "")
    assert adapter.snapshot(request.placement_group_id).phase.value == "PREPARING"

    created = adapter.create(request)
    assert created.accepted
    assert ABORT_PLACEMENT_GROUP_HANDLER not in handlers


def test_prepare_rejection_converges_abort_on_every_participant() -> None:
    nodes, _first, second = _nodes()
    calls: list[tuple[str, object]] = []

    def participant_rpc(_address, handler, request):
        calls.append((handler, request))
        if (
            handler == PREPARE_PLACEMENT_GROUP_HANDLER
            and request.node_id == second
        ):
            return protocol.PreparePlacementGroupReply(
                request.placement_group_id, request.attempt, request.node_id,
                request.plan_digest, request.phase, False, False, "busy",
            )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    rejected = adapter.create(request)

    assert not rejected.accepted and "busy" in (rejected.error or "")
    assert rejected.phase is protocol.PlacementGroupPhaseStatus.REMOVED
    assert adapter.snapshot(request.placement_group_id).phase.value == "REMOVED"
    aborts = [request for handler, request in calls
              if handler == ABORT_PLACEMENT_GROUP_HANDLER]
    assert len(aborts) == 2
    assert {request.node_id for request in aborts} == {
        call.node_id for handler, call in calls
        if handler == PREPARE_PLACEMENT_GROUP_HANDLER
    }
    assert adapter.visible_placement(request.placement_group_id) is None


def test_remove_closes_visibility_before_busy_participant_and_replay_converges() -> None:
    nodes, _first, second = _nodes()
    adapter = None
    saw_closed_visibility = False
    busy_once = {second}

    def participant_rpc(_address, handler, request):
        nonlocal saw_closed_visibility
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            assert adapter is not None
            saw_closed_visibility = (
                adapter.visible_placement(request.placement_group_id) is None
            )
            if request.node_id in busy_once:
                busy_once.remove(request.node_id)
                return protocol.AbortPlacementGroupReply(
                    request.placement_group_id, request.attempt, request.node_id,
                    request.plan_digest, request.phase, True, False,
                )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()
    assert adapter.create(request).accepted
    remove = protocol.RemovePlacementGroupRequest(
        request.placement_group_id, request.attempt
    )

    busy = adapter.remove(remove)

    assert saw_closed_visibility
    assert busy.accepted and not busy.removed
    assert busy.error is None
    state = adapter.get(
        protocol.GetPlacementGroupRequest(request.placement_group_id)
    )
    assert (
        state.phase is protocol.PlacementGroupPhaseStatus.REMOVING
        and state.placements == ()
    )

    removed = adapter.remove(remove)
    assert removed.accepted and removed.removed
    replay = adapter.remove(remove)
    assert replay == removed


@pytest.mark.parametrize("first_failure", ["busy", "timeout"])
def test_remove_first_participant_failure_still_aborts_later_participant(
    first_failure: str,
) -> None:
    nodes, first, second = _nodes()
    first_failed = False
    abort_calls: list[NodeID] = []

    def participant_rpc(_address, handler, request):
        nonlocal first_failed
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            abort_calls.append(request.node_id)
            if request.node_id == first and not first_failed:
                first_failed = True
                if first_failure == "timeout":
                    raise TimeoutError("ambiguous first abort")
                return protocol.AbortPlacementGroupReply(
                    request.placement_group_id, request.attempt, request.node_id,
                    request.plan_digest, request.phase, True, False,
                )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()
    assert adapter.create(request).accepted
    remove = protocol.RemovePlacementGroupRequest(
        request.placement_group_id, request.attempt
    )

    pending = adapter.remove(remove)

    assert pending.accepted and not pending.removed
    assert pending.error is None
    assert pending.phase is protocol.PlacementGroupPhaseStatus.REMOVING
    assert abort_calls == [first, second]
    assert adapter.snapshot(request.placement_group_id).phase.value == "REMOVING"
    assert adapter.visible_placement(request.placement_group_id) is None

    removed = adapter.remove(remove)

    assert removed.accepted and removed.removed
    assert removed.phase is protocol.PlacementGroupPhaseStatus.REMOVED
    assert abort_calls == [first, second, first]


def test_prepare_rejection_starts_complete_best_effort_abort_round() -> None:
    nodes, first, second = _nodes()
    first_abort_busy = True
    abort_calls: list[NodeID] = []

    def participant_rpc(_address, handler, request):
        nonlocal first_abort_busy
        if (
            handler == PREPARE_PLACEMENT_GROUP_HANDLER
            and request.node_id == second
        ):
            return protocol.PreparePlacementGroupReply(
                request.placement_group_id, request.attempt, request.node_id,
                request.plan_digest, request.phase, False, False, "busy",
            )
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            abort_calls.append(request.node_id)
            if request.node_id == first and first_abort_busy:
                first_abort_busy = False
                return protocol.AbortPlacementGroupReply(
                    request.placement_group_id, request.attempt, request.node_id,
                    request.plan_digest, request.phase, True, False,
                )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    rejected = adapter.create(request)

    assert not rejected.accepted
    assert abort_calls == [first, second]
    assert adapter.snapshot(request.placement_group_id).phase.value == "ABORTING"

    replay = adapter.create(request)

    assert not replay.accepted
    assert abort_calls == [first, second, first]
    assert adapter.snapshot(request.placement_group_id).phase.value == "REMOVED"


def test_wrong_typed_reply_identity_remains_an_outstanding_obligation() -> None:
    nodes, _first, _second = _nodes()
    malformed_once = True

    def participant_rpc(_address, _handler, request):
        nonlocal malformed_once
        if malformed_once:
            malformed_once = False
            return protocol.PreparePlacementGroupReply(
                request.placement_group_id, request.attempt, NodeID.random(),
                request.plan_digest, request.phase, True, True,
            )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()

    first = adapter.create(request)
    assert not first.accepted
    assert "identity" in (first.error or "")
    assert adapter.snapshot(request.placement_group_id).phase.value == "PREPARING"
    assert adapter.create(request).accepted


def test_conflicting_create_reports_existing_typed_phase() -> None:
    nodes, _first, _second = _nodes()
    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=lambda _address, _handler, request: (
            _accepted_reply(request)
        )
    )
    original = _request()
    assert adapter.create(original).accepted
    conflict = protocol.CreatePlacementGroupRequest(
        original.placement_group_id,
        original.attempt,
        (
            protocol.PlacementGroupBundle(
                0, ResourceVector({"CPU": 1})
            ),
        ),
        "PACK",
    )

    rejected = adapter.create(conflict)

    assert not rejected.accepted
    assert rejected.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert rejected.placements == ()
    assert "another spec" in (rejected.error or "")


def test_remove_unknown_and_stale_report_typed_actual_phase() -> None:
    nodes, _first, _second = _nodes()
    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=lambda _address, _handler, request: (
            _accepted_reply(request)
        )
    )
    request = _request()

    unknown = adapter.remove(
        protocol.RemovePlacementGroupRequest(PlacementGroupID.random(), 0)
    )
    assert not unknown.accepted and not unknown.removed
    assert unknown.phase is protocol.PlacementGroupPhaseStatus.REMOVED

    assert adapter.create(request).accepted
    stale = adapter.remove(
        protocol.RemovePlacementGroupRequest(request.placement_group_id, 1)
    )
    assert not stale.accepted and not stale.removed
    assert stale.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert "stale" in (stale.error or "")


def test_independent_drain_cancels_pg_and_final_shutdown_exits_after_clean() -> None:
    nodes, first, second = _nodes()
    first_abort_timeout = True
    abort_calls: list[NodeID] = []

    def participant_rpc(_address, handler, request):
        nonlocal first_abort_timeout
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            abort_calls.append(request.node_id)
            if request.node_id == first and first_abort_timeout:
                first_abort_timeout = False
                raise TimeoutError("ambiguous first shutdown abort")
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    existing = _request()
    assert adapter.create(existing).accepted
    gcs = _gcs_for(adapter)
    drain = protocol.DrainPlacementGroupsRequest.create()
    shutdown = protocol.Shutdown.create("after PG cleanup barrier")

    before_drain = gcs.shutdown(shutdown)

    assert not before_drain.clean
    assert not gcs._stop_event.is_set()
    assert abort_calls == []

    first_drain = gcs.drain_placement_groups(drain)

    assert first_drain.accepted and not first_drain.clean
    assert not gcs._stop_event.is_set()
    assert abort_calls == [first, second]
    assert adapter.snapshot(existing.placement_group_id).phase.value == "REMOVING"
    assert adapter.visible_placement(existing.placement_group_id) is None
    new_request = protocol.CreatePlacementGroupRequest(
        PlacementGroupID.random(), existing.attempt, existing.bundles,
        existing.strategy,
    )
    fenced = adapter.create(new_request)
    assert not fenced.accepted
    assert fenced.phase is protocol.PlacementGroupPhaseStatus.REMOVED
    assert "admission is closed" in (fenced.error or "")

    second_drain = gcs.drain_placement_groups(drain)

    assert second_drain.accepted and second_drain.clean
    assert not gcs._stop_event.is_set()
    assert abort_calls == [first, second, first]
    assert adapter.snapshot(existing.placement_group_id).phase.value == "REMOVED"

    final_shutdown = gcs.shutdown(shutdown)

    assert final_shutdown.clean
    assert gcs._stop_event.is_set()


def test_drain_cancels_committing_pg_without_external_create_replay() -> None:
    nodes, first, second = _nodes()
    second_commit_busy = True
    first_abort_timeout = True
    abort_calls: list[NodeID] = []

    def participant_rpc(_address, handler, request):
        nonlocal second_commit_busy, first_abort_timeout
        if (
            handler == COMMIT_PLACEMENT_GROUP_HANDLER
            and request.node_id == second
            and second_commit_busy
        ):
            second_commit_busy = False
            return protocol.CommitPlacementGroupReply(
                request.placement_group_id, request.attempt, request.node_id,
                request.plan_digest, request.phase, True, False,
            )
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            abort_calls.append(request.node_id)
            if request.node_id == first and first_abort_timeout:
                first_abort_timeout = False
                raise TimeoutError("ambiguous committing-PG abort")
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    existing = _request()
    pending = adapter.create(existing)
    assert pending.phase is protocol.PlacementGroupPhaseStatus.COMMITTING
    gcs = _gcs_for(adapter)
    drain = protocol.DrainPlacementGroupsRequest.create()

    first_drain = gcs.drain_placement_groups(drain)

    assert first_drain.accepted and not first_drain.clean
    assert not gcs._stop_event.is_set()
    assert abort_calls == [first, second]
    assert adapter.snapshot(existing.placement_group_id).phase.value == "ABORTING"

    second_drain = gcs.drain_placement_groups(drain)

    assert second_drain.accepted and second_drain.clean
    assert not gcs._stop_event.is_set()
    assert abort_calls == [first, second, first]
    assert adapter.snapshot(existing.placement_group_id).phase.value == "REMOVED"


def test_different_drain_epoch_is_rejected_without_pg_side_effect() -> None:
    nodes, first, _second = _nodes()
    first_abort_busy = True
    abort_calls: list[NodeID] = []

    def participant_rpc(_address, handler, request):
        nonlocal first_abort_busy
        if handler == ABORT_PLACEMENT_GROUP_HANDLER:
            abort_calls.append(request.node_id)
            if request.node_id == first and first_abort_busy:
                first_abort_busy = False
                return protocol.AbortPlacementGroupReply(
                    request.placement_group_id, request.attempt, request.node_id,
                    request.plan_digest, request.phase, True, False,
                )
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    existing = _request()
    assert adapter.create(existing).accepted
    gcs = _gcs_for(adapter)
    original = protocol.DrainPlacementGroupsRequest.create()
    first = gcs.drain_placement_groups(original)
    assert first.accepted and not first.clean
    calls_after_first_round = tuple(abort_calls)

    wrong = gcs.drain_placement_groups(
        protocol.DrainPlacementGroupsRequest.create()
    )

    assert not wrong.accepted and not wrong.clean
    assert "different request ID" in (wrong.error or "")
    assert tuple(abort_calls) == calls_after_first_round
    assert adapter.snapshot(existing.placement_group_id).phase.value == "REMOVING"
    replay = gcs.drain_placement_groups(original)
    assert replay.accepted and replay.clean


def test_shutdown_fence_rejects_conflicting_replay_with_existing_phase() -> None:
    nodes, _first, _second = _nodes()
    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=lambda _address, _handler, request: (
            _accepted_reply(request)
        )
    )
    existing = _request()
    assert adapter.create(existing).accepted
    adapter.close_admission()
    conflict = protocol.CreatePlacementGroupRequest(
        existing.placement_group_id, existing.attempt,
        (existing.bundles[0],), "PACK",
    )

    rejected = adapter.create(conflict)

    assert not rejected.accepted
    assert rejected.phase is protocol.PlacementGroupPhaseStatus.CREATED
    assert "exact replay" in (rejected.error or "")


def test_shutdown_exact_pending_replay_does_not_start_new_obligations() -> None:
    nodes = NodeRegistry()
    first = _id(NodeID, 1)
    second = _id(NodeID, 2)
    nodes.register(
        first, ("127.0.0.1", 12001),
        ResourceVector({"CPU": 1}),
        node_pid=4101,
        available_resources=ResourceVector(),
    )
    nodes.register(
        second, ("127.0.0.1", 12002),
        ResourceVector({"CPU": 1}),
        node_pid=4102,
        available_resources=ResourceVector(),
    )
    calls = []

    def participant_rpc(_address, _handler, request):
        calls.append(request)
        return _accepted_reply(request)

    adapter = PlacementGroupControlCoordinator(
        nodes, participant_rpc=participant_rpc
    )
    request = _request()
    assert adapter.create(request).phase is protocol.PlacementGroupPhaseStatus.PENDING
    assert adapter.begin_shutdown_cleanup()
    nodes.update_resources(first, 4101, 1, 1, ResourceVector({"CPU": 1}))
    nodes.update_resources(second, 4102, 2, 1, ResourceVector({"CPU": 1}))

    replay = adapter.create(request)

    assert not replay.accepted
    assert replay.phase is protocol.PlacementGroupPhaseStatus.REMOVED
    assert calls == []
