from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from miniray import protocol
from miniray.errors import ProtocolError
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, PlacementGroupID, TaskID, WorkerID,
)
from miniray.resources import AllocationToken, ResourceVector


pytestmark = pytest.mark.unit


def _values():
    pg = PlacementGroupID.random()
    nodes = (NodeID.random(), NodeID.random())
    digests = ("a" * 64, "b" * 64)
    keys = tuple(
        protocol.PlacementGroupSchedulingKey(pg, 2, index, node, digest)
        for index, (node, digest) in enumerate(zip(nodes, digests))
    )
    bundles = tuple(
        protocol.PlacementGroupBundle(index, ResourceVector({"CPU": 1}))
        for index in range(2)
    )
    return pg, nodes, digests, keys, bundles


def _task(key=None):
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    spec = protocol.TaskSpec(
        job, task, AttemptID(task, 0),
        protocol.FunctionKey(job, __name__, "f", "v1"), (), 1,
        ResourceVector({"CPU": 1}), WorkerID.random(),
        scheduling_key=key,
    )
    return spec


def test_control_messages_are_immutable_and_keep_participant_digests_distinct() -> None:
    pg, _nodes, _digests, keys, bundles = _values()
    request = protocol.CreatePlacementGroupRequest(
        pg, 2, bundles, "STRICT_SPREAD"
    )
    reply = protocol.CreatePlacementGroupReply(
        pg, 2, True, protocol.PlacementGroupPhaseStatus.CREATED, keys
    )
    assert reply.placements == keys
    assert keys[0].plan_digest != keys[1].plan_digest
    with pytest.raises(FrozenInstanceError):
        request.attempt = 3  # type: ignore[misc]

    found = protocol.GetPlacementGroupReply(
        pg, True, attempt=2,
        phase=protocol.PlacementGroupPhaseStatus.CREATED, placements=keys
    )
    lost = protocol.GetPlacementGroupReply(
        pg, True, attempt=2, phase=protocol.PlacementGroupPhaseStatus.LOST
    )
    missing = protocol.GetPlacementGroupReply(pg, False, error="unknown")
    removed = protocol.RemovePlacementGroupReply(
        pg, 2, True, True, protocol.PlacementGroupPhaseStatus.REMOVED
    )
    assert found.placements == keys and not missing.found and removed.removed
    assert lost.found and lost.phase is protocol.PlacementGroupPhaseStatus.LOST
    with pytest.raises(ProtocolError, match="PlacementGroupPhaseStatus"):
        protocol.CreatePlacementGroupReply(
            pg, 2, True, "CREATED", keys  # type: ignore[arg-type]
        )
    with pytest.raises(ProtocolError, match="PlacementGroupPhaseStatus"):
        protocol.GetPlacementGroupReply(
            pg, True, attempt=2, phase="CREATED", placements=keys  # type: ignore[arg-type]
        )
    assert (
        protocol.PlacementGroupPhaseStatus("LOST")
        is protocol.PlacementGroupPhaseStatus.LOST
    )


def test_create_reply_enforces_terminal_success_and_failure_matrix() -> None:
    pg, _nodes, _digests, keys, _bundles = _values()
    conflict = protocol.CreatePlacementGroupReply(
        pg, 2, False, protocol.PlacementGroupPhaseStatus.CREATED,
        error="same PG ID has another definition",
    )
    assert not conflict.accepted and conflict.placements == ()

    invalid = (
        lambda: protocol.CreatePlacementGroupReply(
            pg, 2, True, protocol.PlacementGroupPhaseStatus.PENDING, keys,
        ),
        lambda: protocol.CreatePlacementGroupReply(
            pg, 2, False, protocol.PlacementGroupPhaseStatus.CREATED, keys,
            "conflict",
        ),
        lambda: protocol.CreatePlacementGroupReply(
            pg, 2, True, protocol.PlacementGroupPhaseStatus.CREATED, (),
        ),
        lambda: protocol.CreatePlacementGroupReply(
            pg, 2, False, protocol.PlacementGroupPhaseStatus.INFEASIBLE,
        ),
    )
    for construct in invalid:
        with pytest.raises(ProtocolError):
            construct()


def test_remove_reply_enforces_removing_or_removed_phase_matrix() -> None:
    pg, _nodes, _digests, _keys, _bundles = _values()
    success = protocol.RemovePlacementGroupReply(
        pg, 2, True, True, protocol.PlacementGroupPhaseStatus.REMOVED
    )
    pending = protocol.RemovePlacementGroupReply(
        pg, 2, True, False, protocol.PlacementGroupPhaseStatus.REMOVING
    )
    rejected = protocol.RemovePlacementGroupReply(
        pg, 2, False, False, protocol.PlacementGroupPhaseStatus.CREATED,
        "placement group still owns active bundles",
    )
    assert success.removed
    assert pending.accepted and not pending.removed and pending.error is None
    assert not rejected.accepted and rejected.phase is protocol.PlacementGroupPhaseStatus.CREATED
    invalid = (
        lambda: protocol.RemovePlacementGroupReply(
            pg, 2, True, True, protocol.PlacementGroupPhaseStatus.REMOVING
        ),
        lambda: protocol.RemovePlacementGroupReply(
            pg, 2, True, False, protocol.PlacementGroupPhaseStatus.REMOVED
        ),
        lambda: protocol.RemovePlacementGroupReply(
            pg, 2, True, False, protocol.PlacementGroupPhaseStatus.REMOVING,
            "accepted cannot contain error",
        ),
        lambda: protocol.RemovePlacementGroupReply(
            pg, 2, False, False, protocol.PlacementGroupPhaseStatus.REMOVING
        ),
        lambda: protocol.RemovePlacementGroupReply(
            pg, 2, False, True, protocol.PlacementGroupPhaseStatus.REMOVED,
            "rejected",
        ),
    )
    for construct in invalid:
        with pytest.raises(ProtocolError):
            construct()


def test_placement_group_drain_protocol_has_stable_epoch_and_clean_matrix() -> None:
    request = protocol.DrainPlacementGroupsRequest.create()
    pending = protocol.DrainPlacementGroupsReply(
        request.request_id, True, False
    )
    clean = protocol.DrainPlacementGroupsReply(
        request.request_id, True, True
    )
    rejected = protocol.DrainPlacementGroupsReply(
        request.request_id, False, False, "different drain epoch"
    )

    assert pending.accepted and not pending.clean
    assert clean.accepted and clean.clean
    assert not rejected.accepted and not rejected.clean
    with pytest.raises(ProtocolError):
        protocol.DrainPlacementGroupsRequest("")
    with pytest.raises(ProtocolError):
        protocol.DrainPlacementGroupsReply("epoch", False, True, "rejected")
    with pytest.raises(ProtocolError):
        protocol.DrainPlacementGroupsReply("epoch", True, False, "error")


@pytest.mark.parametrize(
    ("request_type", "reply_type", "phase"),
    (
        (protocol.PreparePlacementGroupRequest, protocol.PreparePlacementGroupReply, protocol.PlacementGroupParticipantPhase.PREPARE),
        (protocol.CommitPlacementGroupRequest, protocol.CommitPlacementGroupReply, protocol.PlacementGroupParticipantPhase.COMMIT),
        (protocol.AbortPlacementGroupRequest, protocol.AbortPlacementGroupReply, protocol.PlacementGroupParticipantPhase.ABORT),
    ),
)
def test_participant_messages_echo_complete_identity(request_type, reply_type, phase) -> None:
    pg, nodes, digests, _keys, bundles = _values()
    request = request_type(pg, 2, nodes[0], digests[0], phase, (bundles[0],))
    reply = reply_type(pg, 2, nodes[0], digests[0], phase, True, True)
    assert (reply.placement_group_id, reply.attempt, reply.node_id) == (
        request.placement_group_id, request.attempt, request.node_id
    )
    assert reply.plan_digest == request.plan_digest and reply.phase is phase


def test_task_and_lease_lifecycle_preserve_one_scheduling_key() -> None:
    _pg, nodes, _digests, keys, _bundles = _values()
    key = keys[0]
    spec = _task(key)
    lease = LeaseID.random()
    request = protocol.RequestWorkerLease(
        lease, spec.task_id, spec.attempt_id, spec.resources, NodeID.random(),
        spec.owner_worker_id, target_node_id=nodes[0], scheduling_key=key,
    )
    worker = WorkerID.random()
    grant = protocol.GrantWorkerLease(
        lease, spec.task_id, spec.attempt_id, nodes[0], worker,
        ("127.0.0.1", 21000), AllocationToken.random(),
        scheduling_key=key,
    )
    rejected = protocol.RejectWorkerLease(
        lease, spec.task_id, spec.attempt_id,
        protocol.LeaseRejectReason.PENDING_CAPACITY, scheduling_key=key,
    )
    start = protocol.StartWorkerLease(
        lease, spec.task_id, spec.attempt_id, worker, key
    )
    complete = protocol.CompleteWorkerLease(
        lease, spec.task_id, spec.attempt_id, worker,
        protocol.TaskReplyStatus.SUCCEEDED, key,
    )
    assert all(
        item.scheduling_key == key
        for item in (spec, request, grant, rejected, start, complete)
    )


def test_pg_lease_must_target_planned_node_and_cannot_spill_back() -> None:
    _pg, nodes, _digests, keys, _bundles = _values()
    spec = _task(keys[0])
    with pytest.raises(ProtocolError, match="planned node"):
        protocol.RequestWorkerLease(
            LeaseID.random(), spec.task_id, spec.attempt_id, spec.resources,
            NodeID.random(), spec.owner_worker_id, target_node_id=nodes[1],
            scheduling_key=keys[0],
        )
    with pytest.raises(ProtocolError, match="cannot spill back"):
        protocol.SpillbackWorkerLease(
            LeaseID.random(), spec.task_id, spec.attempt_id, nodes[1],
            scheduling_key=keys[0],
        )
    with pytest.raises(ProtocolError, match="planned node"):
        protocol.GrantWorkerLease(
            LeaseID.random(), spec.task_id, spec.attempt_id, nodes[1],
            WorkerID.random(), ("127.0.0.1", 21001),
            AllocationToken.random(), scheduling_key=keys[0],
        )


def test_default_none_remains_backward_compatible() -> None:
    spec = _task()
    request = protocol.RequestWorkerLease(
        LeaseID.random(), spec.task_id, spec.attempt_id, spec.resources,
        NodeID.random(), spec.owner_worker_id,
    )
    assert spec.scheduling_key is None and request.scheduling_key is None
