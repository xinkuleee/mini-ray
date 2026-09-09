from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.control import ActorCoordinator, ActorRegistry, NodeRegistry
from miniray.ids import ActorGeneration, ActorID, JobID, NodeID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(cls: type, byte: int):
    return cls(bytes([byte]) * 16)


def _setup(*, max_restarts: int = 1):
    node_id = _id(NodeID, 1)
    node_pid = 4101
    nodes = NodeRegistry()
    nodes.register(
        node_id, ("127.0.0.1", 14101), ResourceVector({"CPU": 2}),
        node_pid=node_pid,
    )
    node = nodes.get(node_id)
    actor_id = _id(ActorID, 2)
    generation = ActorGeneration(actor_id, 0)
    owner = _id(WorkerID, 3)
    payload = b"actor-class"
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(_id(JobID, 4), "demo", "Counter", "v1"),
        payload, hashlib.sha256(payload).hexdigest(), ("inc",),
    )
    request = protocol.CreateActorRequest(
        actor_id, generation, definition, b"constructor",
        ResourceVector({"CPU": 1}), owner, max_restarts,
        ("127.0.0.1", 14103) if max_restarts else None,
    )
    old_worker = _id(WorkerID, 5)
    created = protocol.ReserveActorWorkerReply(
        actor_id, generation, True, node_id, old_worker,
        ("127.0.0.1", 14105), 5105,
    )
    actors = ActorRegistry()
    actors.begin(request)
    actors.install_initial_reservation(
        request, protocol.ReserveActorWorkerRequest(
            actor_id, generation, definition, request.constructor_payload,
            request.resources, owner, node_id, 1,
        ),
    )
    actors.finish(
        request, protocol.CreateActorReply(
            actor_id, generation, True, node_id, old_worker,
            created.worker_address, created.worker_pid, route_epoch=1,
        ),
    )
    exit_record = protocol.ActorWorkerExitRecord(
        "actor-exit-1", actor_id, generation, 1, node_id, node_pid,
        node.registration_epoch, old_worker, 5105, 17,
    )
    return nodes, actors, request, exit_record


def _accepted_restart(request: protocol.ReserveActorWorkerRequest):
    return protocol.ReserveActorWorkerReply(
        request.actor_id, request.generation, True, request.target_node_id,
        _id(WorkerID, 6), ("127.0.0.1", 14106), 5106,
    )


def test_exit_fences_owner_before_same_node_restart_and_publishes_new_route() -> None:
    nodes, actors, _request, exit_record = _setup()
    events = []

    def install(_address, request):
        events.append(("install", request.snapshot))
        return protocol.InstallActorStateReply(
            request.owner_worker_id, request.snapshot, True
        )

    def reserve(_address, request):
        events.append(("reserve", request))
        return _accepted_restart(request)

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve, install_actor_state=install
    )
    reply = coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(exit_record)
    )

    assert reply.disposition is protocol.ActorWorkerExitDisposition.APPLIED
    assert reply.snapshot is not None
    assert reply.snapshot.state is protocol.ActorState.ALIVE
    assert reply.snapshot.generation == exit_record.generation.next()
    assert reply.snapshot.route_epoch == 3
    assert [event[0] for event in events] == ["install", "reserve", "install"]
    restarting = events[0][1]
    assert restarting.state is protocol.ActorState.RESTARTING
    assert restarting.node_id is None and restarting.worker_id is None
    reservation = events[1][1]
    assert reservation.target_node_id == exit_record.node_id
    assert reservation.route_epoch == 3
    assert reservation.restart == exit_record


def test_owner_fence_failure_keeps_frozen_restart_and_never_calls_node() -> None:
    nodes, actors, _request, exit_record = _setup()
    reserve_calls = []

    def reject_install(_address, request):
        return protocol.InstallActorStateReply(
            request.owner_worker_id, request.snapshot, False, "owner busy"
        )

    coordinator = ActorCoordinator(
        nodes, actors,
        reserve_actor_worker=lambda *_args: reserve_calls.append(_args),
        install_actor_state=reject_install,
    )
    reply = coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(exit_record)
    )

    assert reply.disposition is protocol.ActorWorkerExitDisposition.RETRYABLE
    assert reply.snapshot is not None
    assert reply.snapshot.state is protocol.ActorState.RESTARTING
    assert reserve_calls == []
    frozen = actors.restart_reservation_for(exit_record)
    assert frozen is not None and frozen.restart == exit_record


def test_exact_exit_replay_does_not_consume_another_restart() -> None:
    nodes, actors, _request, exit_record = _setup()
    reservations = []

    def reserve(_address, request):
        reservations.append(request)
        return _accepted_restart(request)

    def install(_address, request):
        return protocol.InstallActorStateReply(
            request.owner_worker_id, request.snapshot, True
        )

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve, install_actor_state=install
    )
    first = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exit_record))
    replay = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exit_record))

    assert first.disposition is protocol.ActorWorkerExitDisposition.APPLIED
    assert replay.disposition is protocol.ActorWorkerExitDisposition.ALREADY_APPLIED
    assert replay.snapshot == first.snapshot
    assert actors.get(exit_record.actor_id).restarts_used == 1
    assert len(reservations) == 1


def test_stale_conflicting_and_reused_detection_proofs_do_not_mutate_state() -> None:
    nodes, actors, _request, exit_record = _setup()
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *_: _accepted_restart(_[1]),
        install_actor_state=lambda _address, request: protocol.InstallActorStateReply(
            request.owner_worker_id, request.snapshot, True
        ),
    )
    assert coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(exit_record)
    ).disposition is protocol.ActorWorkerExitDisposition.APPLIED
    before = actors.get(exit_record.actor_id)

    stale = protocol.ActorWorkerExitRecord(
        "another-detection", exit_record.actor_id, exit_record.generation,
        exit_record.route_epoch, exit_record.node_id, exit_record.node_pid,
        exit_record.registration_epoch, exit_record.worker_id,
        exit_record.worker_pid, exit_record.exit_code,
    )
    assert coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(stale)
    ).disposition is protocol.ActorWorkerExitDisposition.STALE
    conflicting = protocol.ActorWorkerExitRecord(
        exit_record.detection_id, exit_record.actor_id,
        exit_record.generation.next(), 3, exit_record.node_id,
        exit_record.node_pid, exit_record.registration_epoch, _id(WorkerID, 6),
        5106, 9,
    )
    assert coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(conflicting)
    ).disposition is protocol.ActorWorkerExitDisposition.CONFLICT
    assert actors.get(exit_record.actor_id) == before


def test_exhausted_restart_budget_commits_dead_without_reserving() -> None:
    nodes, actors, _request, exit_record = _setup(max_restarts=0)
    reservations = []
    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *_: reservations.append(_),
    )

    reply = coordinator.report_worker_exit(
        protocol.ReportActorWorkerExit(exit_record)
    )

    assert reply.disposition is protocol.ActorWorkerExitDisposition.APPLIED
    assert reply.snapshot is not None
    assert reply.snapshot.state is protocol.ActorState.DEAD
    assert reply.snapshot.node_id is None and reply.snapshot.worker_id is None
    assert reservations == []


def test_get_actor_state_is_typed_and_unknown_is_not_found() -> None:
    _nodes, actors, request, _exit_record = _setup()
    found = actors.get_state_reply(protocol.GetActorState(request.actor_id))
    missing_id = _id(ActorID, 99)
    missing = actors.get_state_reply(protocol.GetActorState(missing_id))

    assert found.found and found.snapshot == actors.get(request.actor_id)
    assert not missing.found and missing.snapshot is None and missing.error


def test_old_exact_exit_replay_after_later_restart_keeps_its_own_ack_proof() -> None:
    nodes, actors, request, exit0 = _setup(max_restarts=2)

    def reserve(_address, reservation):
        worker = _id(WorkerID, 6 + reservation.generation.generation)
        return protocol.ReserveActorWorkerReply(
            reservation.actor_id, reservation.generation, True,
            reservation.target_node_id, worker,
            ("127.0.0.1", 14106 + reservation.generation.generation),
            5106 + reservation.generation.generation,
        )

    def install(_address, install_request):
        return protocol.InstallActorStateReply(
            install_request.owner_worker_id, install_request.snapshot, True
        )

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve, install_actor_state=install
    )
    first = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exit0))
    assert first.snapshot is not None and first.snapshot.state is protocol.ActorState.ALIVE
    live1 = first.snapshot
    node = nodes.get(exit0.node_id)
    exit1 = protocol.ActorWorkerExitRecord(
        "actor-exit-2", request.actor_id, live1.generation, live1.route_epoch,
        exit0.node_id, exit0.node_pid, node.registration_epoch,
        live1.worker_id, live1.worker_pid, 19,
    )
    second = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exit1))
    assert second.snapshot is not None
    assert second.snapshot.generation.generation == 2

    replay = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exit0))
    assert replay.disposition is protocol.ActorWorkerExitDisposition.ALREADY_APPLIED
    assert replay.snapshot is not None
    assert replay.snapshot.last_exit == exit0
    assert replay.snapshot.route_epoch > exit0.route_epoch
    assert replay.snapshot.generation == exit0.generation.next()
    assert actors.get(request.actor_id) == second.snapshot


def test_closed_admission_makes_worker_exit_terminal_without_new_generation() -> None:
    nodes, actors, _request, exit_record = _setup()
    reservations = []
    installed = []

    def install(_address, request):
        installed.append(request.snapshot)
        return protocol.InstallActorStateReply(
            request.owner_worker_id, request.snapshot, True
        )

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=lambda *_: reservations.append(_),
        install_actor_state=install,
    )
    coordinator.close_admission()
    reply = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exit_record))

    assert reply.disposition is protocol.ActorWorkerExitDisposition.APPLIED
    assert reply.snapshot is not None
    assert reply.snapshot.state is protocol.ActorState.DEAD
    assert reply.snapshot.generation == exit_record.generation
    assert reply.snapshot.restarts_used == 0
    assert "admission is closed" in (reply.snapshot.error or "")
    assert reservations == []
    assert installed == [reply.snapshot]


def test_typed_restart_rejection_commits_dead_and_exact_replay_does_not_reserve() -> None:
    nodes, actors, _request, exit_record = _setup()
    reservations = []
    publications = []

    def reserve(_address, request):
        reservations.append(request)
        return protocol.ReserveActorWorkerReply(
            request.actor_id, request.generation, False,
            error="constructor raised ValueError: resource file missing",
            failure=protocol.ActorWorkerFailure.CONSTRUCTOR_FAILED,
        )

    def install(_address, request):
        publications.append(request.snapshot)
        return protocol.InstallActorStateReply(
            request.owner_worker_id, request.snapshot, True
        )

    coordinator = ActorCoordinator(
        nodes, actors, reserve_actor_worker=reserve, install_actor_state=install
    )
    report = protocol.ReportActorWorkerExit(exit_record)

    first = coordinator.report_worker_exit(report)
    replay = coordinator.report_worker_exit(report)

    assert first.disposition is protocol.ActorWorkerExitDisposition.APPLIED
    assert replay.disposition is protocol.ActorWorkerExitDisposition.ALREADY_APPLIED
    assert first.snapshot == replay.snapshot == actors.get(exit_record.actor_id)
    assert first.snapshot is not None
    assert first.snapshot.state is protocol.ActorState.DEAD
    assert first.snapshot.generation == exit_record.generation.next()
    assert first.snapshot.restarts_used == 1
    assert first.snapshot.route_epoch == exit_record.route_epoch + 2
    assert first.snapshot.last_exit == exit_record
    assert "constructor raised ValueError" in (first.snapshot.error or "")
    assert first.snapshot.node_id is None and first.snapshot.worker_id is None
    assert len(reservations) == 1
    assert reservations[0].restart == exit_record
    assert [snapshot.state for snapshot in publications] == [
        protocol.ActorState.RESTARTING,
        protocol.ActorState.DEAD,
        protocol.ActorState.DEAD,
    ]
    assert actors.restart_reservation_for(exit_record) is None


@pytest.mark.parametrize("kind", tuple(protocol.ActorWorkerFailure))
def test_matching_typed_rejection_is_terminal_without_capacity_retry(kind) -> None:
    nodes, actors, _request, exited = _setup()
    calls = []
    def reserve(_address, request):
        calls.append(request)
        return protocol.ReserveActorWorkerReply(
            request.actor_id, request.generation, False, error="resource rejection",
            failure=kind,
        )
    coordinator = ActorCoordinator(nodes, actors, reserve_actor_worker=reserve,
        install_actor_state=lambda _address, message: protocol.InstallActorStateReply(
            message.owner_worker_id, message.snapshot, True))
    reply = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exited))
    assert reply.snapshot.state is protocol.ActorState.DEAD
    assert len(calls) == 1
    replay = coordinator.report_worker_exit(protocol.ReportActorWorkerExit(exited))
    assert replay.snapshot == reply.snapshot and len(calls) == 1


def test_reservation_rejection_requires_typed_failure_and_success_forbids_it() -> None:
    _nodes, _actors, request, exited = _setup()
    with pytest.raises(protocol.ProtocolError, match="typed failure"):
        protocol.ReserveActorWorkerReply(request.actor_id, request.generation, False, error="resource")
    with pytest.raises(protocol.ProtocolError, match="cannot have a failure"):
        protocol.ReserveActorWorkerReply(
            request.actor_id, request.generation, True, exited.node_id, exited.worker_id,
            ("127.0.0.1", 14105), exited.worker_pid,
            failure=protocol.ActorWorkerFailure.CONSTRUCTOR_FAILED,
        )


@pytest.mark.parametrize("constructor_fails", (True, False))
def test_actor_startup_reports_typed_phase_without_parsing_error_text(monkeypatch, constructor_fails) -> None:
    import cloudpickle
    from miniray import actor_worker

    class ConstructorFails:
        def __init__(self):
            raise RuntimeError("resource file missing in constructor")
        def inc(self):
            return 1

    class Constructed:
        def inc(self):
            return 1

    class ReadyCapture:
        def __init__(self):
            self.messages = []
            self.closed = False
        def send(self, message):
            self.messages.append(message)
        def close(self):
            self.closed = True

    class NoTrace:
        def emit(self, *args, **kwargs):
            pass
        def close(self):
            pass

    def fail_server_startup(*args, **kwargs):
        raise RuntimeError("resource file missing in server startup")

    monkeypatch.setattr(actor_worker, "sink_from_config", lambda _config: NoTrace())
    monkeypatch.setattr(actor_worker, "ActorWorkerServer", fail_server_startup)
    _nodes, _actors, request, exited = _setup()
    payload = cloudpickle.dumps(ConstructorFails if constructor_fails else Constructed)
    definition = protocol.ActorClassDefinition(
        request.class_definition.key, payload, hashlib.sha256(payload).hexdigest(), ("inc",)
    )
    ready = ReadyCapture()
    with pytest.raises(RuntimeError, match="resource file missing"):
        actor_worker.actor_worker_main(
            request.actor_id, request.generation, exited.worker_id, definition,
            cloudpickle.dumps(((), {})), exited.node_id, ("127.0.0.1", 14101), ready,
        )
    assert ready.closed and len(ready.messages) == 1
    ok, failure = ready.messages[0]
    assert ok is False and isinstance(failure, protocol.ActorWorkerStartupFailure)
    expected = (protocol.ActorWorkerFailure.CONSTRUCTOR_FAILED if constructor_fails
                else protocol.ActorWorkerFailure.STARTUP_FAILED)
    assert failure.failure is expected and "resource file missing" in failure.error
