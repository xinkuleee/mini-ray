"""Pure contracts for the cluster-wide drain/finalize barrier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from miniray import api, protocol
from miniray.errors import ProtocolError
from miniray.ids import NodeID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


class _FakeProcess:
    """Already-exited process handle used only for shutdown bookkeeping."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.exitcode: Optional[int] = 0
        self.closed = False

    def is_alive(self) -> bool:
        return False

    def join(self, _timeout: Optional[float] = None) -> None:
        return None

    def terminate(self) -> None:
        raise AssertionError("a pure clean-barrier test must not force a process")

    def close(self) -> None:
        self.closed = True


class _FakeCore:
    def __init__(
        self,
        events: list[tuple[str, object]],
        *,
        preflight_result: bool = True,
        finalize_result: bool = True,
    ) -> None:
        self._events = events
        self._preflight_result = preflight_result
        self._finalize_result = finalize_result
        self.finalize_calls = 0

    def shutdown(
        self, timeout: float, *, preserve_owner_protocol: bool = False
    ) -> bool:
        assert timeout > 0
        assert preserve_owner_protocol
        self._events.append(("core_drain", preserve_owner_protocol))
        return True

    def can_finalize_shutdown(
        self, *, require_distributed_clean: bool = True
    ) -> bool:
        assert require_distributed_clean
        self._events.append(("core_preflight", require_distributed_clean))
        return self._preflight_result

    def finalize_shutdown(
        self, *, require_distributed_clean: bool = True, timeout: float = 1.0
    ) -> bool:
        assert require_distributed_clean
        assert 0 <= timeout <= 1.0
        self.finalize_calls += 1
        self._events.append(("core_finalize", require_distributed_clean))
        return self._finalize_result


class _FakeOwnerService:
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self._events = events
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1
        self._events.append(("owner_service_stop", self.stop_calls))


@dataclass
class _Harness:
    events: list[tuple[str, object]]
    core: _FakeCore
    nodes: tuple[api._NodeRuntime, ...]
    epochs: list[str]
    owner_service: _FakeOwnerService
    runtime: api._Runtime


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    preflight_result: bool = True,
    finalize_result: bool = True,
    actor_drain_clean: bool = True,
    actor_drain_pending_rounds: int = 0,
    pg_drain_clean: bool = True,
    owner_death_drain_clean: bool = True,
    owner_death_drain_pending_rounds: int = 0,
) -> _Harness:
    events: list[tuple[str, object]] = []
    epochs: list[str] = []
    core = _FakeCore(
        events,
        preflight_result=preflight_result,
        finalize_result=finalize_result,
    )
    owner_service = _FakeOwnerService(events)
    nodes = []
    for index in range(2):
        node_id = _id(NodeID, index + 1)
        worker_id = _id(WorkerID, index + 11)
        startup = protocol.NodeStartup(
            node_id=node_id,
            node_pid=4100 + index,
            node_address=("127.0.0.1", 14100 + index),
            worker_ids=(worker_id,),
            worker_pids=(4200 + index,),
            worker_addresses=(("127.0.0.1", 14200 + index),),
        )
        nodes.append(
            api._NodeRuntime(
                node_id,
                ResourceVector({"CPU": 1}),
                _FakeProcess(startup.node_pid),  # type: ignore[arg-type]
                startup,
                registration_epoch=index + 1,
            )
        )
    node_tuple = tuple(nodes)
    runtime = api._Runtime(
        core_worker=core,  # type: ignore[arg-type]
        gcs_process=_FakeProcess(4000),  # type: ignore[arg-type]
        gcs_startup=protocol.GCSStartup(4000, ("127.0.0.1", 14000)),
        nodes=node_tuple,
        owner_service=owner_service,
    )
    # This is a pure shutdown harness: no real sentinel-monitor thread exists.
    monkeypatch.setattr(api, "_stop_node_monitor", lambda _runtime: None)
    class _InlineThread:
        """Execute the existing bounded callbacks, without starting a thread."""
        def __init__(self, *, target, args=(), **_kwargs):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

        def join(self, timeout=None):
            return None

        def is_alive(self):
            return False

    def immediate_wait(_event, timeout=None):
        assert timeout is not None and 0 <= timeout <= 0.1
        return False

    monkeypatch.setattr(api.threading, "Thread", _InlineThread)
    monkeypatch.setattr(api.threading.Event, "wait", immediate_wait)
    monkeypatch.setattr(api, "_process_group_exists", lambda _pid: False)
    drain_round = 0
    actor_drain_round = 0
    owner_death_drain_round = 0

    def fake_parallel_node_rpc(nodes, handler, message, *, deadline):
        nonlocal drain_round
        assert deadline > 0
        requested_nodes = tuple(nodes)
        assert all(node in node_tuple for node in requested_nodes)
        if handler == api.NODE_BEGIN_DRAIN_HANDLER:
            assert isinstance(message, protocol.BeginDrain)
            epochs.append(message.request_id)
            replies = {}
            for node in requested_nodes:
                events.append(("node_begin", node.node_id))
                replies[node.node_id] = protocol.DrainStatus(
                    message.request_id,
                    "node:{}".format(node.node_id),
                    drain_started=True,
                    clean=False,
                    resources_clean=False,
                    child_pids=node.startup.worker_pids,
                    child_cleans=(False,),
                )
            return replies
        if handler == api.NODE_DRAIN_STATUS_HANDLER:
            assert isinstance(message, protocol.BeginDrain)
            epochs.append(message.request_id)
            drain_round += 1
            replies = {}
            for node in requested_nodes:
                events.append(("node_clean", (drain_round, node.node_id)))
                replies[node.node_id] = protocol.DrainStatus(
                    message.request_id,
                    "node:{}".format(node.node_id),
                    drain_started=True,
                    clean=True,
                    resources_clean=True,
                    child_pids=node.startup.worker_pids,
                    child_cleans=(True,),
                )
            return replies
        if handler == api.NODE_FINALIZE_SHUTDOWN_HANDLER:
            assert isinstance(message, protocol.FinalizeShutdown)
            epochs.append(message.request_id)
            replies = {}
            for node in requested_nodes:
                events.append(("node_finalize", node.node_id))
                replies[node.node_id] = protocol.ShutdownAck(
                    request_id=message.request_id,
                    component="node:{}".format(node.node_id),
                    clean=True,
                    resources_clean=True,
                    child_pids=node.startup.worker_pids,
                    child_exitcodes=(0,),
                    child_cleans=(True,),
                    child_forced=(False,),
                )
            return replies
        raise AssertionError("unexpected shutdown RPC handler: {}".format(handler))

    def fake_shutdown_gcs(process, startup):
        assert process is runtime.gcs_process
        assert startup is runtime.gcs_startup
        events.append(("gcs_shutdown", startup.gcs_pid))
        return 0, True, False

    def fake_rpc_request(address, handler, message, *, request_timeout=None):
        nonlocal actor_drain_round, owner_death_drain_round
        assert address == runtime.gcs_startup.gcs_address
        assert request_timeout is not None and request_timeout > 0
        epochs.append(message.request_id)
        if handler == api.DRAIN_ACTORS_HANDLER:
            assert isinstance(message, protocol.DrainActorsRequest)
            actor_drain_round += 1
            events.append(("gcs_actor_drain", message.request_id))
            actor_clean_now = (
                actor_drain_clean
                and actor_drain_round > actor_drain_pending_rounds
            )
            return protocol.DrainActorsReply(
                message.request_id, accepted=True, clean=actor_clean_now,
                active_actor_ids=(
                    () if actor_clean_now else (protocol.ActorID.random(),)
                ),
            )
        if handler == api.DRAIN_PLACEMENT_GROUPS_HANDLER:
            assert isinstance(message, protocol.DrainPlacementGroupsRequest)
            events.append(("gcs_pg_drain", message.request_id))
            return protocol.DrainPlacementGroupsReply(
                message.request_id, accepted=True, clean=pg_drain_clean
            )
        assert handler == api.DRAIN_OWNER_DEATH_FENCES_HANDLER
        assert isinstance(message, protocol.DrainOwnerDeathFences)
        owner_death_drain_round += 1
        events.append(("gcs_owner_death_drain", message.request_id))
        owner_death_clean_now = (
            owner_death_drain_clean
            and owner_death_drain_round > owner_death_drain_pending_rounds
        )
        return protocol.DrainOwnerDeathFencesReply(
            message.request_id, owner_death_clean_now,
            0 if owner_death_clean_now else 1,
        )

    monkeypatch.setattr(api, "_runtime", runtime)
    monkeypatch.setattr(api, "_parallel_node_rpc", fake_parallel_node_rpc)
    monkeypatch.setattr(api, "_shutdown_gcs", fake_shutdown_gcs)
    monkeypatch.setattr(api, "rpc_request", fake_rpc_request)
    return _Harness(events, core, node_tuple, epochs, owner_service, runtime)


def _positions(events: list[tuple[str, object]], name: str) -> list[int]:
    return [index for index, event in enumerate(events) if event[0] == name]


def test_clean_cluster_commits_core_between_barrier_and_node_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(monkeypatch)

    report = api.shutdown()

    assert report is not None
    assert report.core_stopped
    assert report.finalized
    assert harness.core.finalize_calls == 1
    assert len(set(harness.epochs)) == 1

    begins = _positions(harness.events, "node_begin")
    clean_observations = _positions(harness.events, "node_clean")
    core_finalize = _positions(harness.events, "core_finalize")
    owner_service_stop = _positions(harness.events, "owner_service_stop")
    node_finalize = _positions(harness.events, "node_finalize")
    gcs_shutdown = _positions(harness.events, "gcs_shutdown")
    pg_drain = _positions(harness.events, "gcs_pg_drain")
    actor_drain = _positions(harness.events, "gcs_actor_drain")
    owner_death_drain = _positions(
        harness.events, "gcs_owner_death_drain"
    )

    assert len(begins) == len(harness.nodes)
    assert len(clean_observations) == 2 * len(harness.nodes)
    assert len(core_finalize) == 1
    assert len(owner_service_stop) == 1
    assert len(node_finalize) == len(harness.nodes)
    assert len(gcs_shutdown) == 1
    assert len(pg_drain) == 1
    assert len(actor_drain) == 1
    assert len(owner_death_drain) == 1
    control_drains = actor_drain + pg_drain + owner_death_drain
    assert max(begins) < min(control_drains)
    assert max(control_drains) < min(clean_observations)
    assert (
        max(clean_observations)
        < core_finalize[0]
        < owner_service_stop[0]
        < min(node_finalize)
    )
    assert max(node_finalize) < gcs_shutdown[0]
    assert harness.events[-1][0] == "gcs_shutdown"


def test_owner_cutover_precedes_retry_of_reference_thread_join(monkeypatch):
    harness = _install_runtime(monkeypatch)
    attempts = []

    def finalize(*, require_distributed_clean, timeout):
        assert require_distributed_clean and 0 <= timeout <= 0.1
        attempts.append(timeout)
        if len(attempts) == 1:
            harness.core.owner_protocol_closed = True
            return False  # cutover committed; bounded local join remains
        assert harness.runtime.core_finalized
        return True

    monkeypatch.setattr(harness.core, "finalize_shutdown", finalize)
    report = api.shutdown()
    assert report is not None and report.core_stopped and report.finalized
    assert len(attempts) == 2 and harness.runtime.core_finalized
    assert harness.owner_service.stop_calls == 1


def test_unclean_teardown_stops_local_transport_without_faking_core_commit(monkeypatch):
    harness = _install_runtime(monkeypatch, finalize_result=False)
    stops = []

    def stop_local(*, timeout):
        assert all(node.process.closed for node in harness.nodes)
        assert harness.owner_service.stop_calls == 1
        assert not harness.runtime.core_finalized
        stops.append(timeout)
        return True

    monkeypatch.setattr(harness.core, "stop_after_cluster_exit", stop_local, raising=False)
    report = api.shutdown()
    assert report is not None and not report.core_stopped and not report.finalized
    assert stops == [1.0]


def test_unclean_pg_drain_never_starts_core_drain_or_node_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(monkeypatch, pg_drain_clean=False)
    # Keep this pure failure test instantaneous while preserving the ordering
    # branch: Node BeginDrain succeeds, then the first accepted in-progress PG
    # reply exhausts the synthetic deadline.
    ticks = iter((0.0, 0.0, 0.0, 100.0, 100.0, 100.0, 100.0))
    monkeypatch.setattr(api.time, "monotonic", lambda: next(ticks, 100.0))

    report = api.shutdown()

    assert report is not None
    begins = _positions(harness.events, "node_begin")
    pg_drain = _positions(harness.events, "gcs_pg_drain")
    assert begins and pg_drain and max(begins) < min(pg_drain)
    assert not _positions(harness.events, "core_drain")
    assert not _positions(harness.events, "core_finalize")
    assert not _positions(harness.events, "node_finalize")
    assert not report.finalized


def test_unclean_actor_drain_never_starts_core_drain_or_node_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(monkeypatch, actor_drain_clean=False)
    # Node BeginDrain succeeds, one pending Actor reply is observed, then the
    # synthetic deadline expires without invoking Core/Node drain.
    ticks = iter((0.0, 0.0, 0.0, 100.0, 100.0, 100.0, 100.0))
    monkeypatch.setattr(api.time, "monotonic", lambda: next(ticks, 100.0))

    report = api.shutdown()

    assert report is not None
    begins = _positions(harness.events, "node_begin")
    actor_drain = _positions(harness.events, "gcs_actor_drain")
    assert begins and actor_drain and max(begins) < min(actor_drain)
    assert not _positions(harness.events, "core_drain")
    assert not _positions(harness.events, "core_finalize")
    assert not _positions(harness.events, "node_finalize")
    assert not report.finalized


def test_unclean_owner_death_drain_never_starts_core_or_node_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(monkeypatch, owner_death_drain_clean=False)
    # BeginDrain succeeds and one accepted in-progress owner-death round is
    # observed before the synthetic deadline expires.
    ticks = iter((0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 100.0, 100.0))
    monkeypatch.setattr(api.time, "monotonic", lambda: next(ticks, 100.0))

    report = api.shutdown()

    assert report is not None
    begins = _positions(harness.events, "node_begin")
    owner_death_drain = _positions(
        harness.events, "gcs_owner_death_drain"
    )
    assert begins and owner_death_drain
    assert max(begins) < min(owner_death_drain)
    assert not _positions(harness.events, "core_drain")
    assert not _positions(harness.events, "node_clean")
    assert not _positions(harness.events, "core_finalize")
    assert not _positions(harness.events, "node_finalize")
    assert not report.finalized


def test_actor_drain_replays_to_clean_before_core_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(
        monkeypatch, actor_drain_pending_rounds=1
    )

    report = api.shutdown()

    assert report is not None and report.finalized
    actor_drain = _positions(harness.events, "gcs_actor_drain")
    pg_drain = _positions(harness.events, "gcs_pg_drain")
    core_drain = _positions(harness.events, "core_drain")
    assert len(actor_drain) == 2
    assert len(pg_drain) == 1
    assert max(actor_drain + pg_drain) < min(core_drain)
    assert len(set(harness.epochs)) == 1


def test_owner_death_drain_replays_to_clean_before_core_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(
        monkeypatch, owner_death_drain_pending_rounds=1
    )

    report = api.shutdown()

    assert report is not None and report.finalized
    actor_drain = _positions(harness.events, "gcs_actor_drain")
    pg_drain = _positions(harness.events, "gcs_pg_drain")
    owner_death_drain = _positions(
        harness.events, "gcs_owner_death_drain"
    )
    core_drain = _positions(harness.events, "core_drain")
    assert len(actor_drain) == len(pg_drain) == 1
    assert len(owner_death_drain) == 2
    assert max(actor_drain + pg_drain + owner_death_drain) < min(core_drain)
    assert len(set(harness.epochs)) == 1


def test_membership_change_invalidates_all_control_drain_acks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(monkeypatch)
    original_rpc = api.rpc_request
    injected = False

    def change_membership(address, handler, message, *, request_timeout=None):
        nonlocal injected
        reply = original_rpc(
            address, handler, message, request_timeout=request_timeout
        )
        if (
            handler == api.DRAIN_OWNER_DEATH_FENCES_HANDLER
            and not injected
        ):
            injected = True
            victim = harness.nodes[-1]
            with harness.runtime.node_death_lock:
                harness.runtime.node_deaths[victim.node_id] = (
                    protocol.NodeDeathRecord(
                        "shutdown-membership-change", victim.node_id,
                        victim.startup.node_pid, victim.registration_epoch, 1, -9,
                        protocol.NodeDeathReason.PROCESS_EXIT,
                        "injected membership change after control ACKs",
                    )
                )
        return reply

    monkeypatch.setattr(api, "rpc_request", change_membership)

    report = api.shutdown()

    assert report is not None and report.core_stopped
    actor_drain = _positions(harness.events, "gcs_actor_drain")
    pg_drain = _positions(harness.events, "gcs_pg_drain")
    owner_death_drain = _positions(
        harness.events, "gcs_owner_death_drain"
    )
    core_drain = _positions(harness.events, "core_drain")
    assert injected
    assert len(actor_drain) == len(pg_drain) == len(owner_death_drain) == 2
    assert max(actor_drain + pg_drain + owner_death_drain) < min(core_drain)
    assert len(set(harness.epochs)) == 1


def test_shutdown_uses_final_worker_incarnation_from_clean_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement changes physical identity, not the Node's slot count."""

    harness = _install_runtime(monkeypatch)
    replacement_pids = {
        node.node_id: node.startup.worker_pids[0] + 1000
        for node in harness.nodes
    }
    original_parallel = api._parallel_node_rpc

    def with_replacements(nodes, handler, message, *, deadline):
        replies = original_parallel(nodes, handler, message, deadline=deadline)
        if handler == api.NODE_DRAIN_STATUS_HANDLER:
            return {
                node_id: protocol.DrainStatus(
                    reply.request_id, reply.component, reply.drain_started,
                    reply.clean, reply.resources_clean, reply.detail,
                    (replacement_pids[node_id],), reply.child_cleans,
                )
                for node_id, reply in replies.items()
            }
        if handler == api.NODE_FINALIZE_SHUTDOWN_HANDLER:
            return {
                node_id: protocol.ShutdownAck(
                    reply.request_id, reply.component, reply.clean, reply.detail,
                    resources_clean=reply.resources_clean,
                    child_pids=(replacement_pids[node_id],),
                    child_exitcodes=reply.child_exitcodes,
                    child_cleans=reply.child_cleans,
                    child_forced=reply.child_forced,
                )
                for node_id, reply in replies.items()
            }
        return replies

    monkeypatch.setattr(api, "_parallel_node_rpc", with_replacements)

    report = api.shutdown()

    assert report is not None and report.finalized and report.worker_clean
    assert report.worker_pids == tuple(
        replacement_pids[node.node_id] for node in harness.nodes
    )
    assert report.worker_pids != tuple(
        pid for node in harness.nodes for pid in node.startup.worker_pids
    )


@pytest.mark.parametrize(
    ("preflight_result", "finalize_result", "expected_finalize_calls"),
    ((False, True, 0), (True, False, 1)),
)
def test_core_commit_failure_never_sends_node_finalize(
    monkeypatch: pytest.MonkeyPatch,
    preflight_result: bool,
    finalize_result: bool,
    expected_finalize_calls: int,
) -> None:
    harness = _install_runtime(
        monkeypatch,
        preflight_result=preflight_result,
        finalize_result=finalize_result,
    )

    report = api.shutdown()

    assert report is not None
    assert not report.core_stopped
    assert not _positions(harness.events, "node_finalize")
    owner_stops = _positions(harness.events, "owner_service_stop")
    assert len(owner_stops) == 1
    assert not _positions(harness.events, "node_finalize")
    node_forces = _positions(harness.events, "node_force")
    if node_forces:
        assert max(node_forces) < owner_stops[0]
    assert harness.core.finalize_calls == expected_finalize_calls
    assert harness.events[-1][0] == "gcs_shutdown"


def test_monitor_cutover_follows_core_and_node_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_runtime(monkeypatch)

    def cutover(runtime):
        assert runtime.core_worker is harness.core
        harness.events.append(("monitor_cutover", True))

    monkeypatch.setattr(api, "_stop_node_monitor", cutover)

    report = api.shutdown()

    assert report is not None and report.core_stopped
    cutover_at = _positions(harness.events, "monitor_cutover")
    finalize_at = _positions(harness.events, "core_finalize")
    assert len(cutover_at) == len(finalize_at) == 1
    node_finalize = _positions(harness.events, "node_finalize")
    assert finalize_at[0] < min(node_finalize) < cutover_at[0]


@pytest.mark.parametrize(
    "factory",
    (
        lambda: protocol.BeginDrain(""),
        lambda: protocol.FinalizeShutdown(""),
        lambda: protocol.DrainStatus(
            "epoch", "node:n1", drain_started=False, clean=True
        ),
        lambda: protocol.DrainStatus(
            "epoch", "node:n1", drain_started=True, clean=True,
            resources_clean=False,
        ),
        lambda: protocol.DrainStatus(
            "epoch", "node:n1", drain_started=True, clean=True,
            child_pids=(123,), child_cleans=(False,),
        ),
        lambda: protocol.DrainStatus(
            "epoch", "node:n1", drain_started=True, clean=False,
            child_pids=(123,), child_cleans=(),
        ),
    ),
)
def test_shutdown_protocol_rejects_invalid_epoch_or_clean_claim(factory) -> None:
    with pytest.raises(ProtocolError):
        factory()
