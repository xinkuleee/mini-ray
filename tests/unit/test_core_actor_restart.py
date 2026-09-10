"""Owner-side Actor restart contracts with explicit runtime ownership.

Three L1 cases use the public Actor submission and real dispatch/publication
path, with at most two Actor-call threads and one-second gates/joins. The
remaining state/options/create cases are threadless. All cases use a real
owner table and ActorClientTable; synchronous transport replies and the local
reference FIFO replace only infrastructure. No Core startup, listener,
process, timer, ordinary-task execution or unbounded teardown runs.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

import miniray as ray
import miniray.core as core_module
import miniray.transport as transport_module
from miniray import protocol
from miniray.actor_client import ActorClientTable
from miniray.core import CoreWorker
from miniray.errors import ActorDiedError, ActorUnavailableError
from miniray.ids import ActorGeneration, ActorID, NodeID, ObjectID, WorkerID
from miniray.resources import ResourceVector
from miniray.transport import TransportError, TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core


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


@pytest.fixture(autouse=True)
def _runtime_bounds(monkeypatch, request):
    threads = []
    violations = []
    real_start = threading.Thread.start
    real_join = threading.Thread.join
    real_wait = threading.Event.wait
    concurrent = request.node.get_closest_marker("loopback_smoke") is not None

    def forbidden(*_args, **_kwargs):
        violations.append("unexpected runtime boundary")
        pytest.fail("Actor restart fixture attempted unmodelled runtime work")

    def start(thread):
        if (not concurrent or not thread.name.startswith("miniray-actor-call-")
                or len(threads) >= 2):
            forbidden()
        # Record ownership before a partially successful Thread.start.
        threads.append(thread)
        return real_start(thread)

    def join(thread, timeout=None):
        if thread not in threads or timeout is None or not 0 <= timeout <= 1.0:
            forbidden()
        return real_join(thread, timeout)

    def wait(event, timeout=None):
        if event.is_set():
            return True
        # CPython Thread.start waits for its own bootstrap receipt. All
        # scenario waits have explicit deadlines; no arbitrary infinite wait.
        bootstrap = any(event is thread._started for thread in threads)
        if not concurrent or (not bootstrap and (timeout is None or not 0 <= timeout <= 1.0)):
            forbidden()
        return real_wait(event, timeout)

    for owner, name in (
        (CoreWorker, "__init__"), (threading.Timer, "start"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
        (subprocess, "Popen"), (time, "sleep"),
        (core_module, "rpc_request"), (transport_module, "request"),
    ):
        monkeypatch.setattr(owner, name, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(threading.Thread, "join", join)
    monkeypatch.setattr(threading.Event, "wait", wait)
    if not concurrent:
        monkeypatch.setattr(threading.Condition, "wait", forbidden)
        monkeypatch.setattr(threading.Barrier, "wait", forbidden)
    yield threads
    assert not any(thread.is_alive() for thread in threads)
    assert not violations


@pytest.fixture
def actor_core(monkeypatch, _runtime_bounds):
    core = make_pure_core()
    core.gcs_address = ("gcs.invalid", 1)
    core._restartable_actor_owner = True
    core._actor_clients = ActorClientTable()
    core._actor_control_ops = 0
    # make_pure_core fences this boundary by default. Restore the real Actor
    # route/retry protocol; each case replaces only its two RPC boundaries.
    core._actor_call_rpc = CoreWorker._actor_call_rpc.__get__(core)
    refs = []
    new_ref = core._new_object_ref

    def remember_ref(object_id):
        ref = new_ref(object_id)
        refs.append(ref)
        return ref

    monkeypatch.setattr(core, "_new_object_ref", remember_ref)
    core._test_actor_threads = _runtime_bounds
    core._test_actor_refs = refs
    try:
        yield core
    finally:
        _stop(core)


def _stop(core: CoreWorker) -> None:
    # No drain counters or authority tables are reset to force success.
    deadline = time.monotonic() + 1.0
    for thread in core._test_actor_threads:
        if thread.ident is not None:
            thread.join(max(0.0, deadline - time.monotonic()))
    assert not any(thread.is_alive() for thread in core._test_actor_threads)
    assert not core._actor_call_threads and core._actor_control_ops == 0
    for ref in core._test_actor_refs:
        ref.close(timeout=0)
        assert ref._release_done.is_set()
    core._reference_mailbox.drain()
    assert core._reference_mailbox.pending.empty()
    assert not core._object_gc_obligations
    close_pure_core(core)


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


@pytest.mark.loopback_smoke
def test_install_restarting_fails_old_inflight_and_new_generation_starts_at_zero(
    monkeypatch, actor_core,
):
    core = actor_core
    first = _register(core)
    entered = threading.Event()
    release = threading.Event()
    seen = []
    new_sequences = []

    def direct(_address, _handler, request):
        assert len(seen) < 1
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
            assert len(new_sequences) < 1
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




@pytest.mark.loopback_smoke
def test_same_alive_route_transport_failure_is_typed_actor_unavailable(
    monkeypatch, actor_core,
):
    core = actor_core
    first = _register(core)
    now = [0.0]
    waits = []
    pushes = []
    state_queries = []

    def unavailable(address, _handler, request):
        assert len(pushes) < 16, "same-route retry exceeded its finite budget"
        pushes.append((address, request))
        raise TransportTimeout("actor socket timeout")

    def wait(delay):
        assert 0 < delay <= 0.1
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


@pytest.mark.loopback_smoke
def test_transport_failure_with_new_gcs_route_fences_old_generation(
    monkeypatch, actor_core,
):
    core = actor_core
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
        assert len(pushes) < 4, "old generation replay crossed its new route"
        pushes.append((address, request))
        raise TransportTimeout("old actor route is unreachable")

    def wait(delay):
        assert 0 < delay <= 0.1
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


@pytest.mark.unit
def test_install_state_exact_replay_stale_noop_and_equal_epoch_conflict(actor_core):
    core = actor_core
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
        assert core._actor_clients.snapshot(first.actor_id) == first

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


@pytest.mark.unit
def test_dead_snapshot_rejects_new_calls_immediately(actor_core):
    core = actor_core
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
        assert core._actor_clients.snapshot(first.actor_id) == dead
        assert not core._objects and not core._actor_call_threads
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


@pytest.mark.unit
def test_create_actor_binds_restart_policy_to_driver_owner_endpoint(monkeypatch, actor_core):
    core = actor_core
    payload = cloudpickle.dumps(_Counter)
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(core.job_id, __name__, "_Counter", "v1"),
        payload, hashlib.sha256(payload).hexdigest(), ("inc",),
    )
    observed = []

    def create(_address, handler, request):
        assert not observed, "accepted Actor creation must finish in one RPC"
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
