"""Owner-side Actor restart contracts, mostly exercising a real Core runtime.

Only the options-validation case is pure. The other cases start real Core
threads, and some also start Actor-call threads or use control-plane RPCs. They
remain heavy pending exact bounded review; synthetic teardown is not evidence
of successful semantic drain or of a pure fixture.
"""

from __future__ import annotations

import hashlib
import threading

import cloudpickle
import pytest

import miniray as ray
from miniray import protocol
from miniray.core import CoreWorker
from miniray.errors import ActorDiedError, ActorUnavailableError
from miniray.ids import ActorGeneration, ActorID, NodeID, ObjectID, WorkerID
from miniray.resources import ResourceVector
from miniray.transport import TransportError, TransportTimeout
from tests.unit._core_test_utils import (
    assert_core_threads_stopped, force_stop_core_threads,
)


class _Counter:
    def inc(self):
        return 1


def _alive(
    actor_id, generation, epoch, worker_id, *, max_restarts=1, last_exit=None
):
    return protocol.ActorSnapshot(
        actor_id, generation, protocol.ActorState.ALIVE, epoch,
        generation.generation, max_restarts, last_exit=last_exit,
        node_id=NodeID.random() if last_exit is None else last_exit.node_id, worker_id=worker_id,
        worker_address=("127.0.0.1", 23000 + epoch),
        worker_pid=23000 + epoch,
    )


def _exit(snapshot):
    return protocol.ActorWorkerExitRecord(
        "exit-{}".format(snapshot.route_epoch), snapshot.actor_id,
        snapshot.generation, snapshot.route_epoch, snapshot.node_id, 31001, 1,
        snapshot.worker_id, snapshot.worker_pid, -9,
    )


def _core():
    return CoreWorker(
        ("127.0.0.1", 21001), NodeID.random(),
        gcs_address=("127.0.0.1", 21002),
        owner_address=("127.0.0.1", 21003),
        restartable_actor_owner=True,
    )


def _stop(core: CoreWorker) -> None:
    if not core.shutdown(timeout=1.0):
        force_stop_core_threads(core)
    assert_core_threads_stopped(core)


def _register(core, *, max_restarts=1):
    actor_id = ActorID.random()
    generation = ActorGeneration(actor_id, 0)
    snapshot = _alive(
        actor_id, generation, 1, WorkerID.random(),
        max_restarts=max_restarts,
    )
    core._actor_clients.register(snapshot, ("inc",))
    return snapshot


def _success(request, snapshot):
    payload = cloudpickle.dumps(7)
    descriptor = protocol.ResultDescriptor(
        ObjectID.for_task(request.task_id), protocol.ResultStorage.INLINE,
        len(payload), request.owner_worker_id, snapshot.node_id,
        hashlib.sha256(payload).hexdigest(), payload,
    )
    task_reply = protocol.TaskReply(
        request.task_id, request.attempt_id, snapshot.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED, (descriptor,),
    )
    return protocol.ActorCallReply(
        request.actor_id, request.generation, request.caller_worker_id,
        request.sequence, task_reply, request.route_epoch,
    )


@pytest.mark.heavy
def test_install_restarting_fails_old_inflight_and_new_generation_starts_at_zero(
    monkeypatch,
):
    core = _core()
    first = _register(core)
    entered = threading.Event()
    release = threading.Event()
    seen = []
    new_sequences = []

    def direct(_address, _handler, request):
        seen.append(request)
        entered.set()
        assert release.wait(1.0)
        return _success(request, first)

    monkeypatch.setattr(core, "_push_task_rpc", direct)
    try:
        ref = core.submit_actor_call(first.actor_id, "inc", (), {})
        assert entered.wait(1.0)
        generation1 = first.generation.next()
        restarting = protocol.ActorSnapshot(
            first.actor_id, generation1, protocol.ActorState.RESTARTING, 2, 1,
            1, _exit(first),
        )
        installed = core.install_actor_state(
            protocol.InstallActorState(core.worker_id, restarting)
        )
        assert installed.installed
        with pytest.raises(ActorDiedError):
            core.get(ref, timeout=1.0)

        second = _alive(
            first.actor_id, generation1, 3, WorkerID.random(),
            last_exit=restarting.last_exit,
        )
        assert core.install_actor_state(
            protocol.InstallActorState(core.worker_id, second)
        ).installed
        release.set()

        def new_direct(_address, _handler, request):
            new_sequences.append(request.sequence)
            return _success(request, second)

        monkeypatch.setattr(core, "_push_task_rpc", new_direct)
        assert core.get(
            core.submit_actor_call(first.actor_id, "inc", (), {}), timeout=1.0
        ) == 7
    finally:
        release.set()
        _stop(core)

    assert seen[0].generation == first.generation
    assert seen[0].target_worker_id == first.worker_id
    assert seen[0].route_epoch == 1
    # The new generation owns a fresh mailbox sequence namespace.
    assert new_sequences == [0]
    assert core._actor_clients.snapshot(first.actor_id) == second




@pytest.mark.heavy
def test_same_alive_route_transport_failure_is_typed_actor_unavailable(
    monkeypatch,
):
    core = _core()
    first = _register(core)
    now = [0.0]
    waits = []
    pushes = []
    state_queries = []

    def unavailable(address, _handler, request):
        pushes.append((address, request))
        raise TransportTimeout("actor socket timeout")

    def wait(delay):
        waits.append(delay)
        now[0] += delay

    def query(address, handler, request):
        assert address == core.gcs_address
        assert handler == "get_actor_state"
        state_queries.append(request)
        return protocol.GetActorStateReply(first.actor_id, True, first)

    monkeypatch.setattr(core, "_push_task_rpc", unavailable)
    monkeypatch.setattr(core, "_rpc", query)
    monkeypatch.setattr(core, "_actor_call_now", lambda: now[0])
    monkeypatch.setattr(core, "_actor_call_retry_wait", wait)
    try:
        ref = core.submit_actor_call(first.actor_id, "inc", (), {})
        with pytest.raises(
            ActorUnavailableError, match="same ALIVE route.*unreachable"
        ) as caught:
            core.get(ref, timeout=1.0)
        assert not isinstance(caught.value, TransportError)
        assert len(pushes) == len(state_queries) == len(waits)
        assert len(pushes) > 3
        assert all(address == first.worker_address for address, _ in pushes)
        assert all(request == pushes[0][1] for _, request in pushes)
        assert all(
            request == protocol.GetActorState(first.actor_id)
            for request in state_queries
        )
        assert waits[:4] == pytest.approx([0.01, 0.02, 0.04, 0.08])
        assert max(waits) <= 0.1
        assert now[0] == pytest.approx(1.0)
        assert core._actor_clients.snapshot(first.actor_id) == first
        assert ray.ActorUnavailableError is ActorUnavailableError
    finally:
        _stop(core)


@pytest.mark.heavy
def test_transport_failure_with_new_gcs_route_fences_old_generation(
    monkeypatch,
):
    core = _core()
    first = _register(core)
    restarting = protocol.ActorSnapshot(
        first.actor_id, first.generation.next(), protocol.ActorState.RESTARTING,
        first.route_epoch + 1, 1, 1, _exit(first),
    )
    now = [0.0]
    waits = []
    pushes = []
    state_queries = []

    def unavailable(address, _handler, request):
        pushes.append((address, request))
        raise TransportTimeout("old actor route is unreachable")

    def wait(delay):
        waits.append(delay)
        now[0] += delay

    def query(_address, handler, request):
        assert handler == "get_actor_state"
        state_queries.append(request)
        snapshot = first if len(state_queries) <= 3 else restarting
        return protocol.GetActorStateReply(first.actor_id, True, snapshot)

    monkeypatch.setattr(core, "_push_task_rpc", unavailable)
    monkeypatch.setattr(core, "_rpc", query)
    monkeypatch.setattr(core, "_actor_call_now", lambda: now[0])
    monkeypatch.setattr(core, "_actor_call_retry_wait", wait)
    try:
        ref = core.submit_actor_call(first.actor_id, "inc", (), {})
        with pytest.raises(ActorDiedError):
            core.get(ref, timeout=1.0)
        assert len(pushes) == len(state_queries) == len(waits) == 4
        assert all(address == first.worker_address for address, _ in pushes)
        assert all(request == pushes[0][1] for _, request in pushes)
        assert now[0] == pytest.approx(0.15)
        assert core._actor_clients.snapshot(first.actor_id) == restarting
    finally:
        _stop(core)


@pytest.mark.heavy
def test_install_state_exact_replay_stale_noop_and_equal_epoch_conflict():
    core = _core()
    first = _register(core)
    try:
        exact = protocol.InstallActorState(core.worker_id, first)
        assert core.install_actor_state(exact).installed

        conflicting = _alive(
            first.actor_id, first.generation, first.route_epoch, WorkerID.random()
        )
        rejected = core.install_actor_state(
            protocol.InstallActorState(core.worker_id, conflicting)
        )
        assert not rejected.installed and "reused" in (rejected.error or "")

        generation1 = first.generation.next()
        restarting = protocol.ActorSnapshot(
            first.actor_id, generation1, protocol.ActorState.RESTARTING, 2, 1,
            1, _exit(first),
        )
        assert core.install_actor_state(
            protocol.InstallActorState(core.worker_id, restarting)
        ).installed
        # A valid stale publication is acknowledged as a no-op.
        assert core.install_actor_state(exact).installed
        assert core._actor_clients.snapshot(first.actor_id) == restarting
    finally:
        _stop(core)


@pytest.mark.heavy
def test_dead_snapshot_rejects_new_calls_immediately():
    core = _core()
    first = _register(core, max_restarts=0)
    dead = protocol.ActorSnapshot(
        first.actor_id, first.generation, protocol.ActorState.DEAD, 2, 0, 0,
        _exit(first), error="restart budget exhausted",
    )
    try:
        assert core.install_actor_state(
            protocol.InstallActorState(core.worker_id, dead)
        ).installed
        with pytest.raises(ActorDiedError, match="not callable"):
            core.submit_actor_call(first.actor_id, "inc", (), {})
    finally:
        _stop(core)


@pytest.mark.unit
def test_max_restarts_is_actor_only_and_options_copy_is_independent():
    actor_class = ray.remote(max_restarts=2)(_Counter)
    overridden = actor_class.options(max_restarts=1)

    assert actor_class._max_restarts == 2
    assert overridden._max_restarts == 1
    with pytest.raises(TypeError, match="max_restarts.*Actor"):
        ray.remote(max_restarts=1)(lambda: None)
    with pytest.raises(TypeError, match="max_retries.*Actor"):
        actor_class.options(max_retries=1)
    with pytest.raises(ValueError, match="max_restarts"):
        ray.remote(max_restarts=-1)(_Counter)


@pytest.mark.heavy
def test_create_actor_binds_restart_policy_to_driver_owner_endpoint(monkeypatch):
    core = _core()
    payload = cloudpickle.dumps(_Counter)
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(core.job_id, __name__, "_Counter", "v1"),
        payload, hashlib.sha256(payload).hexdigest(), ("inc",),
    )
    observed = []

    def create(_address, handler, request):
        observed.append(request)
        assert handler == "create_actor"
        return protocol.CreateActorReply(
            request.actor_id, request.generation, True, NodeID.random(),
            WorkerID.random(), ("127.0.0.1", 24001), 24001,
            route_epoch=1,
        )

    monkeypatch.setattr(core, "_rpc", create)
    try:
        endpoint = core.create_actor(
            definition, (), {}, ResourceVector(), max_restarts=2
        )
    finally:
        _stop(core)

    assert endpoint.actor_id == observed[0].actor_id
    assert observed[0].max_restarts == 2
    assert observed[0].owner_address == core.owner_address
