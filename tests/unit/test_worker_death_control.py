from __future__ import annotations

from threading import RLock

import pytest

from miniray import protocol
from miniray.control import GCSLite, NodeRegistry, WorkerRegistry
from miniray.ids import NodeID, WorkerID
from miniray.resources import ResourceVector
from miniray.trace import EventSink
from miniray.errors import ProtocolError


pytestmark = pytest.mark.unit


def _node(byte: int = 1) -> NodeID:
    return NodeID(bytes([byte]) * 16)


def _worker(byte: int) -> WorkerID:
    return WorkerID(bytes([byte]) * 16)


def _node_registration(
    nodes: NodeRegistry, *, byte: int = 1, pid: int = 4101
) -> protocol.RegisterNodeReply:
    return nodes.register_message(
        protocol.RegisterNode(
            _node(byte), pid, ("127.0.0.1", 14000 + byte),
            ResourceVector({"CPU": 2}),
        )
    )


def _incarnation(
    registration: protocol.RegisterNodeReply, *,
    worker_byte: int = 11, worker_pid: int = 5101,
) -> protocol.WorkerIncarnation:
    return protocol.WorkerIncarnation(
        registration.node_id, registration.node_pid,
        registration.registration_epoch, _worker(worker_byte), worker_pid,
    )


def _death(
    incarnation: protocol.WorkerIncarnation, *,
    detection_id: str = "worker-death-1", exit_code: int = 23,
    reason: protocol.WorkerDeathReason = (
        protocol.WorkerDeathReason.PROCESS_EXIT
    ),
) -> protocol.ReportWorkerDeath:
    return protocol.ReportWorkerDeath(
        detection_id, incarnation, exit_code, reason
    )


def _gcs() -> GCSLite:
    gcs = object.__new__(GCSLite)
    gcs.nodes = NodeRegistry()
    gcs.workers = WorkerRegistry(gcs.nodes)
    gcs.event_sink = EventSink()
    gcs._snapshot_lock = RLock()
    return gcs


def test_registration_is_exactly_idempotent_and_node_incarnation_fenced() -> None:
    nodes = NodeRegistry()
    registration = _node_registration(nodes)
    workers = WorkerRegistry(nodes)
    incarnation = _incarnation(registration)
    request = protocol.RegisterWorkerIncarnation(incarnation)

    assert workers.register(request).accepted
    assert workers.register(request).accepted

    changed_pid = protocol.WorkerIncarnation(
        incarnation.node_id, incarnation.node_pid,
        incarnation.node_registration_epoch, incarnation.worker_id,
        incarnation.worker_pid + 1,
    )
    conflict = workers.register(
        protocol.RegisterWorkerIncarnation(changed_pid)
    )
    assert not conflict.accepted
    assert "another incarnation" in (conflict.error or "")

    wrong_node_lifetime = protocol.WorkerIncarnation(
        incarnation.node_id, incarnation.node_pid + 1,
        incarnation.node_registration_epoch, _worker(12), 5102,
    )
    fenced = workers.register(
        protocol.RegisterWorkerIncarnation(wrong_node_lifetime)
    )
    assert not fenced.accepted
    assert "Node incarnation" in (fenced.error or "")


def test_unknown_worker_and_wrong_incarnation_death_are_rejected() -> None:
    nodes = NodeRegistry()
    registration = _node_registration(nodes)
    workers = WorkerRegistry(nodes)
    incarnation = _incarnation(registration)

    unknown = workers.report_death(_death(incarnation))
    assert unknown.disposition is protocol.WorkerDeathDisposition.UNKNOWN
    assert unknown.watermark == 0

    assert workers.register(
        protocol.RegisterWorkerIncarnation(incarnation)
    ).accepted
    wrong_pid = protocol.WorkerIncarnation(
        incarnation.node_id, incarnation.node_pid,
        incarnation.node_registration_epoch, incarnation.worker_id,
        incarnation.worker_pid + 1,
    )
    conflict = workers.report_death(_death(wrong_pid))
    assert conflict.disposition is protocol.WorkerDeathDisposition.CONFLICT
    assert conflict.watermark == 0
    assert workers.get(incarnation.worker_id).state is (
        protocol.WorkerMembershipState.ALIVE
    )


def test_exact_death_replay_is_already_dead_and_field_drift_conflicts() -> None:
    nodes = NodeRegistry()
    registration = _node_registration(nodes)
    workers = WorkerRegistry(nodes)
    incarnation = _incarnation(registration)
    workers.register(protocol.RegisterWorkerIncarnation(incarnation))
    request = _death(incarnation)

    applied = workers.report_death(request)
    replay = workers.report_death(request)
    drift = workers.report_death(_death(incarnation, exit_code=24))
    reason_drift = workers.report_death(
        _death(incarnation, reason=protocol.WorkerDeathReason.EXPECTED)
    )

    assert applied.disposition is protocol.WorkerDeathDisposition.APPLIED
    assert replay.disposition is protocol.WorkerDeathDisposition.ALREADY_DEAD
    assert replay.death == applied.death
    assert replay.watermark == applied.watermark == 1
    assert drift.disposition is protocol.WorkerDeathDisposition.CONFLICT
    assert reason_drift.disposition is protocol.WorkerDeathDisposition.CONFLICT
    assert drift.watermark == 1
    state = workers.get_state_reply(
        protocol.GetWorkerState(incarnation.worker_id)
    )
    assert state.found
    assert state.state is protocol.WorkerMembershipState.DEAD
    assert state.death == applied.death


def test_detection_id_is_globally_bound_to_one_worker_death_proof() -> None:
    nodes = NodeRegistry()
    registration = _node_registration(nodes)
    workers = WorkerRegistry(nodes)
    first = _incarnation(registration, worker_byte=11, worker_pid=5101)
    second = _incarnation(registration, worker_byte=12, worker_pid=5102)
    for incarnation in (first, second):
        workers.register(protocol.RegisterWorkerIncarnation(incarnation))

    assert workers.report_death(
        _death(first, detection_id="shared-proof")
    ).disposition is protocol.WorkerDeathDisposition.APPLIED
    reused = workers.report_death(
        _death(second, detection_id="shared-proof")
    )

    assert reused.disposition is protocol.WorkerDeathDisposition.CONFLICT
    assert workers.get(second.worker_id).state is (
        protocol.WorkerMembershipState.ALIVE
    )


def test_fresh_replacement_worker_id_registers_after_old_worker_dies() -> None:
    nodes = NodeRegistry()
    registration = _node_registration(nodes)
    workers = WorkerRegistry(nodes)
    old = _incarnation(registration, worker_byte=11, worker_pid=5101)
    fresh = _incarnation(registration, worker_byte=12, worker_pid=5102)
    workers.register(protocol.RegisterWorkerIncarnation(old))
    workers.report_death(_death(old))

    accepted = workers.register(protocol.RegisterWorkerIncarnation(fresh))
    resurrect = workers.register(protocol.RegisterWorkerIncarnation(old))

    assert accepted.accepted
    assert workers.get(fresh.worker_id).state is (
        protocol.WorkerMembershipState.ALIVE
    )
    assert not resurrect.accepted
    assert "dead WorkerID" in (resurrect.error or "")


def test_global_death_journal_is_ordered_and_cursor_is_a_watermark() -> None:
    nodes = NodeRegistry()
    first_node = _node_registration(nodes, byte=1, pid=4101)
    second_node = _node_registration(nodes, byte=2, pid=4102)
    workers = WorkerRegistry(nodes)
    incarnations = (
        _incarnation(first_node, worker_byte=11, worker_pid=5101),
        _incarnation(second_node, worker_byte=12, worker_pid=5102),
    )
    for incarnation in incarnations:
        workers.register(protocol.RegisterWorkerIncarnation(incarnation))
    first = workers.report_death(
        _death(incarnations[0], detection_id="death-a")
    )
    second = workers.report_death(
        _death(incarnations[1], detection_id="death-b")
    )

    all_deaths = workers.deaths_after(protocol.GetWorkerDeaths(0))
    suffix = workers.deaths_after(protocol.GetWorkerDeaths(1))
    caught_up = workers.deaths_after(protocol.GetWorkerDeaths(2))

    assert all_deaths.watermark == 2
    assert all_deaths.deaths == (first.death, second.death)
    assert tuple(death.death_epoch for death in all_deaths.deaths) == (1, 2)
    assert suffix.deaths == (second.death,)
    assert caught_up.deaths == ()
    with pytest.raises(ValueError, match="cursor exceeds"):
        workers.deaths_after(protocol.GetWorkerDeaths(3))


def test_journal_preserves_process_node_and_expected_exit_reasons() -> None:
    nodes = NodeRegistry()
    registration = _node_registration(nodes)
    workers = WorkerRegistry(nodes)
    reasons = (
        protocol.WorkerDeathReason.PROCESS_EXIT,
        protocol.WorkerDeathReason.NODE_EXIT,
        protocol.WorkerDeathReason.EXPECTED,
    )
    for index, reason in enumerate(reasons, start=11):
        incarnation = _incarnation(
            registration, worker_byte=index, worker_pid=5100 + index
        )
        workers.register(protocol.RegisterWorkerIncarnation(incarnation))
        reply = workers.report_death(
            _death(
                incarnation, detection_id="death-{}".format(index),
                reason=reason,
            )
        )
        assert reply.death is not None and reply.death.reason is reason

    journal = workers.deaths_after(protocol.GetWorkerDeaths(0))
    assert tuple(death.reason for death in journal.deaths) == reasons
    with pytest.raises(ProtocolError, match="WorkerDeathReason"):
        protocol.ReportWorkerDeath(
            "bad-reason", incarnations[0] if False else _incarnation(
                registration, worker_byte=21, worker_pid=5121
            ), 0, "PROCESS_EXIT",  # type: ignore[arg-type]
        )


def test_gcs_exposes_only_typed_worker_control_handlers() -> None:
    gcs = _gcs()
    registration = gcs.register_node(
        protocol.RegisterNode(
            _node(), 4101, ("127.0.0.1", 14101),
            ResourceVector({"CPU": 1}),
        )
    )
    incarnation = _incarnation(registration)

    assert {
        "register_worker_incarnation",
        "report_worker_death",
        "get_worker_state",
        "get_worker_deaths",
    }.issubset(gcs.handlers)
    registered = gcs.handle(protocol.RegisterWorkerIncarnation(incarnation))
    died = gcs.handle(_death(incarnation))
    state = gcs.handle(protocol.GetWorkerState(incarnation.worker_id))
    journal = gcs.handle(protocol.GetWorkerDeaths(0))

    assert registered.accepted
    assert died.disposition is protocol.WorkerDeathDisposition.APPLIED
    assert state.death == died.death
    assert journal.deaths == (died.death,)
