"""Public and CoreWorker contracts for the K0 Actor execution path.

Only the decorator/API-shape case is pure. The other cases construct a real
CoreWorker, start its background threads and exercise shutdown; fake Actor RPCs
do not necessarily intercept periodic Worker-death queries. Those cases remain
heavy pending separate exact bounded-runtime and teardown review.
"""

from __future__ import annotations

import hashlib

import cloudpickle
import pytest

import miniray as ray
from miniray import debug as ray_debug
from miniray import protocol
from miniray.core import ActorEndpoint, CoreWorker
from miniray.errors import RuntimeShuttingDownError, SystemTaskError
from miniray.ids import JobID, NodeID, ObjectID, WorkerID
from miniray.resources import ResourceVector
from miniray.transport import TransportConnectionError, TransportTimeout


_ACTOR_ONLY_RESOURCE = "actor_only"


class _Counter:
    def __init__(self) -> None:
        self.value = 0

    def inc(self) -> int:
        self.value += 1
        return self.value


def _actor_definition(job_id: JobID) -> protocol.ActorClassDefinition:
    payload = cloudpickle.dumps(_Counter)
    key = protocol.FunctionKey(job_id, __name__, "_Counter", "contract-v1")
    return protocol.ActorClassDefinition(
        key=key,
        payload=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        method_names=("inc",),
    )


def _accepted_create(
    request: protocol.CreateActorRequest,
    *,
    node_id: NodeID | None = None,
    worker_id: WorkerID | None = None,
) -> protocol.CreateActorReply:
    return protocol.CreateActorReply(
        actor_id=request.actor_id,
        generation=request.generation,
        accepted=True,
        node_id=node_id or NodeID.random(),
        worker_id=worker_id or WorkerID.random(),
        worker_address=("127.0.0.1", 12201),
        worker_pid=12201,
        route_epoch=1,
    )


@pytest.mark.unit
def test_remote_class_exposes_ray_style_actor_public_api() -> None:
    actor_class = ray.remote(
        num_cpus=1, resources={_ACTOR_ONLY_RESOURCE: 1}
    )(_Counter)

    assert isinstance(actor_class, ray.ActorClass)
    assert callable(actor_class.remote)
    assert ray.ActorHandle is not None


@pytest.mark.heavy
def test_actor_handle_debug_snapshot_queries_current_route_without_caching_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actor_id = protocol.ActorID.random()
    generation = protocol.ActorGeneration(actor_id, 0)
    node_id = NodeID.random()
    worker_id = WorkerID.random()
    endpoint = ActorEndpoint(
        actor_id, generation, node_id, worker_id,
        ("127.0.0.1", 12101), ("inc",), route_epoch=1, worker_pid=12101,
    )
    core = CoreWorker(
        ("127.0.0.1", 12102), NodeID.random(),
        gcs_address=("127.0.0.1", 12103),
    )
    snapshot = protocol.ActorSnapshot(
        actor_id, generation, protocol.ActorState.ALIVE, 1, 0, 0,
        node_id=node_id, worker_id=worker_id,
        worker_address=endpoint.worker_address, worker_pid=endpoint.worker_pid,
    )
    queried = []

    def query(received_actor_id):
        queried.append(received_actor_id)
        return snapshot

    monkeypatch.setattr(core, "_query_actor_state", query)
    try:
        handle = ray.ActorHandle(core, endpoint)
        assert handle.actor_id == actor_id
        assert ray_debug.snapshot(handle) == snapshot
        assert queried == [actor_id]
        # Endpoint is construction-time input only.  The handle cannot expose a
        # stale physical route after a future generation replaces it.
        assert "_endpoint" not in handle.__dict__
    finally:
        core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_initial_actor_create_replays_exact_request_after_retryable_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12301), NodeID.random(),
        gcs_address=("127.0.0.1", 12302),
    )
    definition = _actor_definition(core.job_id)
    requests: list[protocol.CreateActorRequest] = []

    def create(_address: object, handler: str, request: object) -> object:
        assert handler == "create_actor"
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        if len(requests) == 1:
            # Models a Node reservation whose acknowledgement did not let GCS
            # publish the final route to this caller.
            return protocol.CreateActorReply(
                request.actor_id, request.generation, False,
                error="Actor reservation outcome is unresolved",
                retryable=True,
            )
        return _accepted_create(request)

    monkeypatch.setattr(core, "_rpc", create)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        endpoint = core.create_actor(
            definition, (object(),), {"value": object()}, ResourceVector()
        )
    finally:
        core.shutdown(timeout=1.0)

    assert len(requests) == 2
    assert requests[0] is requests[1]
    assert requests[0].actor_id == endpoint.actor_id
    assert requests[0].constructor_payload == requests[1].constructor_payload


@pytest.mark.heavy
def test_reply_loss_then_early_restart_installs_into_provisional_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12311), NodeID.random(),
        gcs_address=("127.0.0.1", 12312),
        owner_address=("127.0.0.1", 12313),
        restartable_actor_owner=True,
    )
    definition = _actor_definition(core.job_id)
    requests = []
    installed = []

    def create(_address: object, handler: str, request: object) -> object:
        assert handler == "create_actor"
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        if len(requests) == 1:
            # Simulate reply loss after generation 0 became ALIVE, followed by
            # an immediate Actor-worker crash.  GCS must be able to push both
            # state transitions before create_actor returns a public handle.
            initial = _accepted_create(request)
            generation0 = protocol.ActorSnapshot(
                request.actor_id, request.generation, protocol.ActorState.ALIVE,
                1, 0, request.max_restarts, node_id=initial.node_id,
                worker_id=initial.worker_id,
                worker_address=initial.worker_address,
                worker_pid=initial.worker_pid,
            )
            assert core.install_actor_state(
                protocol.InstallActorState(core.worker_id, generation0)
            ).installed
            exit_record = protocol.ActorWorkerExitRecord(
                "early-actor-crash", request.actor_id, request.generation, 1,
                initial.node_id, 12314, 1, initial.worker_id,
                initial.worker_pid, -9,
            )
            restarting = protocol.ActorSnapshot(
                request.actor_id, request.generation.next(),
                protocol.ActorState.RESTARTING, 2, 1, request.max_restarts,
                exit_record,
            )
            assert core.install_actor_state(
                protocol.InstallActorState(core.worker_id, restarting)
            ).installed
            current = protocol.ActorSnapshot(
                request.actor_id, request.generation.next(),
                protocol.ActorState.ALIVE, 3, 1, request.max_restarts,
                exit_record, initial.node_id, WorkerID.random(),
                ("127.0.0.1", 12315), 12315,
            )
            assert core.install_actor_state(
                protocol.InstallActorState(core.worker_id, current)
            ).installed
            installed.append(current)
            raise TransportTimeout("initial create reply was lost")
        current = installed[0]
        return protocol.CreateActorReply(
            request.actor_id, current.generation, True, current.node_id,
            current.worker_id, current.worker_address, current.worker_pid,
            route_epoch=current.route_epoch,
        )

    monkeypatch.setattr(core, "_rpc", create)
    monkeypatch.setattr(core, "_query_actor_state", lambda actor_id: (
        installed[0] if actor_id == requests[0].actor_id else None
    ))
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        endpoint = core.create_actor(
            definition, (), {}, ResourceVector(), max_restarts=1
        )
    finally:
        core.shutdown(timeout=1.0)

    assert len(requests) == 2 and requests[0] is requests[1]
    assert endpoint.actor_id == requests[0].actor_id
    assert endpoint.generation.generation == 1
    assert endpoint.worker_id == installed[0].worker_id
    assert core._actor_clients.snapshot(endpoint.actor_id) == installed[0]


@pytest.mark.heavy
def test_initial_actor_create_nonretryable_rejection_is_not_replayed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12401), NodeID.random(),
        gcs_address=("127.0.0.1", 12402),
    )
    definition = _actor_definition(core.job_id)
    requests = []
    state_queries = []
    dead = []

    def reject(_address: object, handler: str, request: object) -> object:
        if handler == "get_actor_state":
            state_queries.append(request)
            if len(state_queries) == 1:
                raise TransportTimeout(
                    "terminal Actor state reply was temporarily lost"
                )
            return protocol.GetActorStateReply(
                request.actor_id, True, dead[0]
            )
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        if not dead:
            dead.append(protocol.ActorSnapshot(
                request.actor_id, request.generation, protocol.ActorState.DEAD,
                1, 0, request.max_restarts,
                error="Actor resources are infeasible",
            ))
        return protocol.CreateActorReply(
            request.actor_id, request.generation, False,
            error="Actor resources are infeasible", retryable=False,
            route_epoch=1,
        )

    monkeypatch.setattr(core, "_rpc", reject)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        with pytest.raises(SystemTaskError, match="infeasible"):
            core.create_actor(definition, (), {}, ResourceVector())
        assert len(requests) == 2
        assert requests[0] is requests[1]
        assert len(state_queries) == 2
        assert core._actor_clients.snapshot(requests[0].actor_id) == dead[0]
        # A delayed exact GCS state-install ACK remains valid while the owner
        # protocol is open; retaining DEAD makes the replay idempotent.
        exact = core.install_actor_state(
            protocol.InstallActorState(core.worker_id, dead[0])
        )
        assert exact.installed and exact.snapshot == dead[0]
    finally:
        core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_nonretryable_unknown_actor_rolls_back_exact_provisional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12411), NodeID.random(),
        gcs_address=("127.0.0.1", 12412),
    )
    definition = _actor_definition(core.job_id)
    create_requests = []

    def rpc(_address: object, handler: str, request: object) -> object:
        if handler == "create_actor":
            assert isinstance(request, protocol.CreateActorRequest)
            create_requests.append(request)
            return protocol.CreateActorReply(
                request.actor_id, request.generation, False,
                error="Actor admission is closed", retryable=False,
            )
        assert handler == "get_actor_state"
        return protocol.GetActorStateReply(
            request.actor_id, False, error="unknown Actor"
        )

    monkeypatch.setattr(core, "_rpc", rpc)
    try:
        with pytest.raises(SystemTaskError, match="admission is closed"):
            core.create_actor(definition, (), {}, ResourceVector())
    finally:
        core.shutdown(timeout=1.0)

    assert len(create_requests) == 1
    with pytest.raises(KeyError):
        core._actor_clients.snapshot(create_requests[0].actor_id)


@pytest.mark.heavy
def test_unknown_state_cannot_delete_concurrent_late_alive_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12421), NodeID.random(),
        gcs_address=("127.0.0.1", 12422),
    )
    definition = _actor_definition(core.job_id)
    create_requests = []
    alive = []
    state_queries = 0

    def rpc(_address: object, handler: str, request: object) -> object:
        assert handler == "create_actor"
        assert isinstance(request, protocol.CreateActorRequest)
        create_requests.append(request)
        if len(create_requests) == 1:
            return protocol.CreateActorReply(
                request.actor_id, request.generation, False,
                error="Actor admission is closed", retryable=False,
            )
        current = alive[0]
        return protocol.CreateActorReply(
            request.actor_id, current.generation, True, current.node_id,
            current.worker_id, current.worker_address, current.worker_pid,
            route_epoch=current.route_epoch,
        )

    def query_state(actor_id):
        nonlocal state_queries
        state_queries += 1
        if state_queries == 1:
            current = protocol.ActorSnapshot(
                actor_id, protocol.ActorGeneration(actor_id, 0),
                protocol.ActorState.ALIVE, 1, 0, 0, node_id=NodeID.random(),
                worker_id=WorkerID.random(),
                worker_address=("127.0.0.1", 12423), worker_pid=12423,
            )
            alive.append(current)
            assert core.install_actor_state(
                protocol.InstallActorState(core.worker_id, current)
            ).installed
            # The reply was captured before the concurrent install and is now
            # stale.  The provisional CAS must fail rather than delete ALIVE.
            return protocol.GetActorStateReply(
                actor_id, False, error="stale unknown Actor"
            )
        return protocol.GetActorStateReply(actor_id, True, alive[0])

    monkeypatch.setattr(core, "_rpc", rpc)
    original_resolve = core._resolve_terminal_actor_create

    def resolve(request, reply, provisional):
        state_reply = query_state(request.actor_id)
        assert not state_reply.found
        # Exercise the real CAS reducer after injecting the stale unknown.
        return original_resolve(request, reply, provisional)

    monkeypatch.setattr(core, "_resolve_terminal_actor_create", resolve)
    monkeypatch.setattr(
        core, "_query_actor_state_reply",
        lambda actor_id: protocol.GetActorStateReply(
            actor_id, True, alive[0]
        ),
    )
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        endpoint = core.create_actor(definition, (), {}, ResourceVector())
    finally:
        core.shutdown(timeout=1.0)

    assert len(create_requests) == 2
    assert create_requests[0] is create_requests[1]
    assert endpoint.worker_id == alive[0].worker_id
    assert core._actor_clients.snapshot(endpoint.actor_id) == alive[0]


@pytest.mark.heavy
def test_initial_actor_create_replays_invalid_reply_without_new_actor_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12501), NodeID.random(),
        gcs_address=("127.0.0.1", 12502),
    )
    definition = _actor_definition(core.job_id)
    requests = []

    def create(_address: object, _handler: str, request: object) -> object:
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        if len(requests) == 1:
            return object()
        return _accepted_create(request)

    monkeypatch.setattr(core, "_rpc", create)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        endpoint = core.create_actor(definition, (), {}, ResourceVector())
    finally:
        core.shutdown(timeout=1.0)

    assert len(requests) == 2
    assert requests[0] is requests[1]
    assert endpoint.actor_id == requests[0].actor_id


@pytest.mark.heavy
def test_initial_create_replay_adopts_full_snapshot_if_actor_already_restarted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12511), NodeID.random(),
        gcs_address=("127.0.0.1", 12512),
        owner_address=("127.0.0.1", 12516),
        restartable_actor_owner=True,
    )
    definition = _actor_definition(core.job_id)
    requests = []
    state_queries = []

    def create(_address: object, handler: str, request: object) -> object:
        assert handler == "create_actor"
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        if len(requests) == 1:
            return protocol.CreateActorReply(
                request.actor_id, request.generation, False,
                error="initial reply was lost", retryable=True,
            )
        generation1 = request.generation.next()
        return protocol.CreateActorReply(
            request.actor_id, generation1, True, NodeID.random(),
            WorkerID.random(), ("127.0.0.1", 12513), 12513, route_epoch=3,
        )

    def state(actor_id):
        state_queries.append(actor_id)
        if len(state_queries) == 1:
            raise TransportConnectionError("state query temporarily unavailable")
        reply = requests[-1]
        generation1 = reply.generation.next()
        exit_record = protocol.ActorWorkerExitRecord(
            "initial-reply-lost", actor_id, reply.generation, 1,
            NodeID.random(), 12514, 1, WorkerID.random(), 12515, 9,
        )
        # Match the endpoint returned by the second create reply.
        latest_reply = create_replies[-1]
        return protocol.ActorSnapshot(
            actor_id, generation1, protocol.ActorState.ALIVE, 3, 1, 1,
            exit_record, latest_reply.node_id, latest_reply.worker_id,
            latest_reply.worker_address, latest_reply.worker_pid,
        )

    create_replies = []

    def recording_create(address, handler, request):
        reply = create(address, handler, request)
        create_replies.append(reply)
        return reply

    monkeypatch.setattr(core, "_rpc", recording_create)
    monkeypatch.setattr(core, "_query_actor_state", state)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        endpoint = core.create_actor(
            definition, (), {}, ResourceVector(), max_restarts=1
        )
    finally:
        core.shutdown(timeout=1.0)

    assert requests[0] is requests[1] is requests[2]
    assert endpoint.generation.generation == 1
    assert endpoint.route_epoch == 3
    assert state_queries == [requests[0].actor_id, requests[0].actor_id]


@pytest.mark.heavy
def test_shutdown_takeover_after_clean_connect_failure_prevents_another_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12601), NodeID.random(),
        gcs_address=("127.0.0.1", 12602),
    )
    definition = _actor_definition(core.job_id)
    requests = []

    def unavailable(_address: object, _handler: str, request: object) -> object:
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        raise TransportConnectionError("GCS listener unavailable")

    def close_admission(_round: int) -> None:
        with core._state_lock:
            core._accepting = False

    monkeypatch.setattr(core, "_rpc", unavailable)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", close_admission)
    try:
        with pytest.raises(RuntimeShuttingDownError, match="cluster shutdown"):
            core.create_actor(definition, (), {}, ResourceVector())
        assert core._actor_control_ops == 0
        assert len(requests) == 1
    finally:
        # Restore ordinary Core teardown after this narrow admission-race test.
        with core._state_lock:
            core._accepting = True
        core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_observed_retryable_create_ignores_shutdown_takeover_until_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12611), NodeID.random(),
        gcs_address=("127.0.0.1", 12612),
    )
    definition = _actor_definition(core.job_id)
    requests = []

    def create(_address: object, _handler: str, request: object) -> object:
        assert isinstance(request, protocol.CreateActorRequest)
        requests.append(request)
        if len(requests) <= 4:
            return protocol.CreateActorReply(
                request.actor_id, request.generation, False,
                error="reservation acknowledgement missing", retryable=True,
            )
        return _accepted_create(request)

    def close_admission(_round: int) -> None:
        with core._state_lock:
            core._accepting = False

    monkeypatch.setattr(core, "_rpc", create)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", close_admission)
    try:
        endpoint = core.create_actor(definition, (), {}, ResourceVector())
        assert endpoint.actor_id == requests[0].actor_id
        assert len(requests) == 5
        assert all(request is requests[0] for request in requests)
    finally:
        with core._state_lock:
            core._accepting = True
        core.shutdown(timeout=1.0)


@pytest.mark.heavy
def test_three_clean_connection_failures_can_return_without_orphan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = CoreWorker(
        ("127.0.0.1", 12621), NodeID.random(),
        gcs_address=("127.0.0.1", 12622),
    )
    definition = _actor_definition(core.job_id)
    requests = []

    death_barriers: list[protocol.GetWorkerDeaths] = []

    def unavailable(_address: object, handler: str, request: object) -> object:
        if handler == "get_worker_deaths":
            assert isinstance(request, protocol.GetWorkerDeaths)
            death_barriers.append(request)
            return protocol.GetWorkerDeathsReply(
                request.after_epoch, request.after_epoch, ()
            )
        requests.append(request)
        raise TransportConnectionError("GCS listener unavailable")

    monkeypatch.setattr(core, "_rpc", unavailable)
    monkeypatch.setattr(core, "_wait_for_actor_create_retry", lambda _round: None)
    try:
        with pytest.raises(SystemTaskError, match="after 3 attempts"):
            core.create_actor(definition, (), {}, ResourceVector())
    finally:
        core.shutdown(timeout=1.0)

    assert len(requests) == 3
    assert all(request is requests[0] for request in requests)
    assert death_barriers
    with pytest.raises(KeyError):
        core._actor_clients.snapshot(requests[0].actor_id)


@pytest.mark.heavy
def test_actor_creation_uses_gcs_once_then_methods_push_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Actor creation is control-plane work; method calls are not."""

    local_node = NodeID.random()
    actor_node = NodeID.random()
    actor_worker = WorkerID.random()
    local_node_address = ("127.0.0.1", 12001)
    gcs_address = ("127.0.0.1", 12002)
    actor_address = ("127.0.0.1", 12003)
    core = CoreWorker(
        local_node_address, local_node, gcs_address=gcs_address
    )
    definition = _actor_definition(core.job_id)
    control_calls: list[tuple[object, str, object]] = []
    death_barriers: list[protocol.GetWorkerDeaths] = []
    direct_calls: list[tuple[object, str, protocol.ActorCallRequest]] = []

    def control_rpc(address: object, handler: str, message: object) -> object:
        if handler == "get_worker_deaths":
            assert address == gcs_address
            assert isinstance(message, protocol.GetWorkerDeaths)
            death_barriers.append(message)
            return protocol.GetWorkerDeathsReply(
                message.after_epoch, message.after_epoch, ()
            )
        control_calls.append((address, handler, message))
        assert address == gcs_address
        assert handler == "create_actor"
        assert isinstance(message, protocol.CreateActorRequest)
        assert message.resources == ResourceVector(
            {"CPU": 1, _ACTOR_ONLY_RESOURCE: 1}
        )
        return protocol.CreateActorReply(
            actor_id=message.actor_id,
            generation=message.generation,
            accepted=True,
            node_id=actor_node,
            worker_id=actor_worker,
            worker_address=actor_address,
            worker_pid=43210,
            route_epoch=1,
        )

    def direct_rpc(address: object, handler: str, message: object) -> object:
        assert address == actor_address
        assert handler == "actor_call"
        assert isinstance(message, protocol.ActorCallRequest)
        direct_calls.append((address, handler, message))
        value = message.sequence + 1
        payload = cloudpickle.dumps(value)
        result = protocol.ResultDescriptor(
            object_id=ObjectID.for_task(message.task_id),
            storage=protocol.ResultStorage.INLINE,
            size_bytes=len(payload),
            owner_worker_id=message.owner_worker_id,
            node_id=actor_node,
            checksum=hashlib.sha256(payload).hexdigest(),
            inline_data=payload,
        )
        task_reply = protocol.TaskReply(
            task_id=message.task_id,
            attempt_id=message.attempt_id,
            worker_id=actor_worker,
            status=protocol.TaskReplyStatus.SUCCEEDED,
            results=(result,),
        )
        return protocol.ActorCallReply(
            actor_id=message.actor_id,
            generation=message.generation,
            caller_worker_id=message.caller_worker_id,
                sequence=message.sequence,
                task_reply=task_reply,
                route_epoch=message.route_epoch,
            )

    monkeypatch.setattr(core, "_rpc", control_rpc)
    monkeypatch.setattr(core, "_push_task_rpc", direct_rpc)
    try:
        endpoint = core.create_actor(
            definition,
            (),
            {},
            ResourceVector({"CPU": 1, _ACTOR_ONLY_RESOURCE: 1}),
        )
        assert isinstance(endpoint, ActorEndpoint)
        handle = ray.ActorHandle(core, endpoint)

        values = [
            core.get(handle.inc.remote(), timeout=1.0),
            core.get(handle.inc.remote(), timeout=1.0),
            core.get(handle.inc.remote(), timeout=1.0),
        ]
    finally:
        core.shutdown(timeout=1.0)

    assert values == [1, 2, 3]
    assert len(control_calls) == 1
    assert death_barriers
    assert control_calls[0][0] == gcs_address
    assert isinstance(control_calls[0][2], protocol.CreateActorRequest)
    assert len(direct_calls) == 3
    assert [call[2].sequence for call in direct_calls] == [0, 1, 2]
    assert all(call[0] == actor_address for call in direct_calls)
    assert all(call[1] == "actor_call" for call in direct_calls)
    assert all(
        call[1] not in {"request_worker_lease", "release_worker_lease"}
        for call in direct_calls
    )
