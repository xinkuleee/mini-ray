"""Driver-owned Node death orchestration with explicit test classification.

Ten unit-marked cases use fake Process/Core objects and synchronous RPC replies.
They never start a listener, process or thread; the Actor-migration retry case
replaces only its one expected backoff wait. The two actual thread/event races
remain heavy pending independent bounded-lifecycle review. The survivor path
retains the real ACK vector and tests certificate publication before Core notice.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time
from dataclasses import replace

import pytest

from miniray import api, protocol
from miniray.ids import NodeID, WorkerID
from miniray.node_death_view import (
    PUBLISH_NODE_DEATH_VIEW, PublishInstalledNodeDeaths, PublishInstalledNodeDeathsReply,
)
from miniray.resources import ResourceVector


@pytest.fixture(autouse=True)
def _unit_has_no_runtime(request, monkeypatch):
    if request.node.get_closest_marker("heavy") is not None:
        return

    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Node-death orchestration attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Process:
    def __init__(self, pid: int, *, exitcode: int | None = None) -> None:
        self.pid = pid
        self.exitcode = exitcode
        self.sentinel = object()
        self.joined: list[float] = []

    def join(self, timeout: float = 0) -> None:
        self.joined.append(timeout)

    def is_alive(self) -> bool:
        return self.exitcode is None


class _DelayedExitcodeProcess(_Process):
    def __init__(self, pid: int, terminal_exitcode: int) -> None:
        super().__init__(pid)
        self._terminal_exitcode = terminal_exitcode

    def join(self, timeout: float = 0) -> None:
        super().join(timeout)
        if len(self.joined) == 2:
            self.exitcode = self._terminal_exitcode


class _Core:
    def __init__(self) -> None:
        self.observed: list[tuple[object, protocol.InstallClusterSnapshot]] = []

    def handle_node_death(self, death, snapshot):
        self.observed.append((death, snapshot))


def _node(byte: int, pid: int, port: int) -> api._NodeRuntime:
    node_id = NodeID(bytes([byte]) * 16)
    startup = protocol.NodeStartup(
        node_id, pid, ("127.0.0.1", port),
        (WorkerID(bytes([byte + 10]) * 16),),
        (pid + 100,), (("127.0.0.1", port + 100),),
    )
    return api._NodeRuntime(
        node_id, ResourceVector({"CPU": 1}), _Process(pid), startup,
        registration_epoch=byte,
    )


def _runtime() -> api._Runtime:
    first = _node(1, 4101, 14101)
    second = _node(2, 4102, 14102)
    return api._Runtime(
        core_worker=_Core(),  # type: ignore[arg-type]
        gcs_process=_Process(4000),  # type: ignore[arg-type]
        gcs_startup=protocol.GCSStartup(4000, ("127.0.0.1", 14000)),
        nodes=(first, second),
    )


def _live_info(node: api._NodeRuntime) -> protocol.NodeInfo:
    return protocol.NodeInfo(
        node.node_id, node.startup.node_pid, node.registration_epoch,
        node.startup.node_address, node.resources, node.resources,
    )


class _SurvivorBarrier:
    """Synchronous peer for exactly one snapshot and its real returned ACK.

    This double records only transport interactions. Production API builds and
    publishes the certificate; it is not replaced with a constant no-op.
    """

    def __init__(self, runtime, survivor, order):
        self.runtime, self.survivor, self.order = runtime, survivor, order
        self.snapshot = self.ack = self.published = None

    def rpc(self, address, handler, message, **options):
        assert address == self.survivor.startup.node_address
        assert options["request_timeout"] > 0
        if handler == api.INSTALL_CLUSTER_SNAPSHOT_HANDLER:
            self.order.append("snapshot")
            assert self.snapshot is self.ack is None
            assert type(message) is protocol.InstallClusterSnapshot
            assert message.nodes == (_live_info(self.survivor),)
            assert message.membership_epoch == self.runtime.latest_membership_epoch
            assert message.snapshot_id == api._cluster_snapshot_id(message.membership_epoch, message.nodes)
            self.snapshot = replace(message)
            self.ack = protocol.InstallClusterSnapshotReply(
                message.membership_epoch, message.snapshot_id, self.survivor.node_id, True,
            )
            return self.ack
        assert handler == PUBLISH_NODE_DEATH_VIEW
        self.order.append("certificate")
        assert self.snapshot is not None and self.ack is not None and self.published is None
        assert type(message) is PublishInstalledNodeDeaths and message.node_id == self.survivor.node_id
        assert message.view.snapshot == self.snapshot
        assert message.view.survivor_acks == (self.ack,) == self.runtime.latest_snapshot_acks
        assert message.view.deaths == tuple(sorted(self.runtime.node_gcs_deaths.values(), key=lambda death: death.death_epoch))
        assert not self.runtime.core_worker.observed, "Core must not precede certificate retention"
        assert 0 < options["connect_timeout"] <= 0.25
        assert 0 < options["request_timeout"] <= api._NODE_DEATH_RPC_TIMEOUT_SECONDS
        assert options["deadline"] > api.time.monotonic()
        self.published = replace(message)
        return PublishInstalledNodeDeathsReply(message, True)


@pytest.mark.unit
def test_sentinel_ready_exitcode_race_rejoins_only_the_managed_child() -> None:
    process = _DelayedExitcodeProcess(4199, -9)
    process.join(0)

    observed = api._await_managed_process_exitcode(
        process, api.time.monotonic() + 1.0
    )

    assert observed == -9
    assert process.pid == 4199
    assert len(process.joined) == 2
    assert all(timeout >= 0 for timeout in process.joined)


@pytest.mark.unit
def test_snapshot_digest_binds_physical_incarnation() -> None:
    node = _runtime().nodes[0]
    info = _live_info(node)
    other_pid = protocol.NodeInfo(
        info.node_id, info.node_pid + 1, info.registration_epoch, info.address,
        info.total_resources, info.available_resources,
    )
    other_epoch = protocol.NodeInfo(
        info.node_id, info.node_pid, info.registration_epoch + 1, info.address,
        info.total_resources, info.available_resources,
    )

    baseline = api._cluster_snapshot_id(2, (info,))
    assert baseline != api._cluster_snapshot_id(2, (other_pid,))
    assert baseline != api._cluster_snapshot_id(2, (other_epoch,))


@pytest.mark.unit
def test_observer_commits_death_installs_survivor_then_notifies_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    survivor, victim = runtime.nodes
    victim.process.exitcode = -9
    order: list[str] = []
    request_holder: list[protocol.ReportNodeDeath] = []
    barrier = _SurvivorBarrier(runtime, survivor, order)

    def rpc(address, handler, message, *, request_timeout, **options):
        assert request_timeout > 0
        if handler == api.REPORT_NODE_DEATH_HANDLER:
            order.append("gcs")
            assert address == runtime.gcs_startup.gcs_address
            assert isinstance(message, protocol.ReportNodeDeath)
            request_holder.append(message)
            death = protocol.NodeDeathRecord(
                message.detection_id, message.node_id, message.node_pid,
                message.expected_registration_epoch, 3, message.exit_code,
                message.reason, message.detail,
            )
            return protocol.ReportNodeDeathReply(
                message.detection_id, message.node_id, message.node_pid,
                protocol.NodeDeathDisposition.APPLIED, 3,
                (_live_info(survivor),), death,
            )
        return barrier.rpc(address, handler, message, request_timeout=request_timeout, **options)

    original = runtime.core_worker.handle_node_death

    def observe(*args, **kwargs):
        assert barrier.published is not None
        order.append("core")
        original(*args, **kwargs)

    runtime.core_worker.handle_node_death = observe
    monkeypatch.setattr(api, "rpc_request", rpc)

    runtime.latest_membership_epoch = 2
    runtime.latest_live_nodes = tuple(_live_info(node) for node in runtime.nodes)
    api._observe_managed_node_exit(runtime, victim.process)

    assert runtime.node_death_errors == {}
    assert order == ["gcs", "snapshot", "certificate", "core"]
    assert victim.process.joined == [0]
    assert len(request_holder) == 1
    assert runtime.node_deaths[victim.node_id].node_id == victim.node_id
    assert runtime.latest_snapshot is not None
    assert runtime.node_death_events[victim.node_id].is_set()
    assert runtime.core_worker.observed == [
        (runtime.node_deaths[victim.node_id], runtime.latest_snapshot)
    ]
    assert barrier.published.view.snapshot == runtime.latest_snapshot
    assert barrier.published.view.deaths == (runtime.node_deaths[victim.node_id],)
    assert runtime.latest_snapshot_acks == (barrier.ack,)


@pytest.mark.unit
def test_observer_replays_committed_death_until_actor_migration_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    survivor, victim = runtime.nodes
    victim.process.exitcode = -9
    runtime.latest_membership_epoch = 2
    runtime.latest_live_nodes = tuple(_live_info(node) for node in runtime.nodes)
    reports = 0
    order, delays, requests = [], [], []
    barrier = _SurvivorBarrier(runtime, survivor, order)

    def rpc(address, handler, message, *, request_timeout, **options):
        nonlocal reports
        if handler == api.REPORT_NODE_DEATH_HANDLER:
            reports += 1
            assert reports <= 2 and address == runtime.gcs_startup.gcs_address
            order.append("gcs")
            requests.append(message)
            death = protocol.NodeDeathRecord(
                message.detection_id, message.node_id, message.node_pid,
                message.expected_registration_epoch, 3, message.exit_code,
                message.reason, message.detail,
            )
            return protocol.ReportNodeDeathReply(
                message.detection_id, message.node_id, message.node_pid,
                (protocol.NodeDeathDisposition.APPLIED if reports == 1
                 else protocol.NodeDeathDisposition.ALREADY_DEAD),
                3, (_live_info(survivor),), death,
                actor_migration_converged=reports > 1,
            )
        assert reports == 2, "the Actor migration barrier must precede survivor installation"
        return barrier.rpc(address, handler, message, request_timeout=request_timeout, **options)

    def backoff(_event, timeout=None):
        assert timeout == api._NODE_DEATH_RETRY_INTERVAL_SECONDS
        assert reports == 1 and not delays
        assert barrier.snapshot is None and not runtime.core_worker.observed
        delays.append(timeout)
        return False

    monkeypatch.setattr(api, "rpc_request", rpc)
    monkeypatch.setattr(threading.Event, "wait", backoff)
    api._observe_managed_node_exit(runtime, victim.process)

    assert reports == 2
    assert requests[0] == requests[1]
    assert delays == [api._NODE_DEATH_RETRY_INTERVAL_SECONDS]
    assert order == ["gcs", "gcs", "snapshot", "certificate"]
    assert runtime.node_death_errors == {}
    assert victim.node_id in runtime.node_deaths
    assert runtime.core_worker.observed == [(runtime.node_deaths[victim.node_id], barrier.snapshot)]
    assert barrier.published.view.deaths == (runtime.node_deaths[victim.node_id],)


@pytest.mark.unit
def test_conflicting_death_reply_never_reaches_snapshot_or_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    victim = runtime.nodes[1]
    victim.process.exitcode = 23

    def rpc(_address, handler, message, *, request_timeout):
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.CONFLICT, 2,
            tuple(_live_info(node) for node in runtime.nodes),
            error="conflicting proof",
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, victim.process)

    assert victim.node_id not in runtime.node_deaths
    assert victim.node_id in runtime.node_death_errors
    assert runtime.node_death_events[victim.node_id].is_set()
    assert runtime.core_worker.observed == []


@pytest.mark.heavy
def test_monitor_dispatch_does_not_block_on_another_death_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    first_started = threading.Event()
    release_first = threading.Event()
    second_done = threading.Event()
    calls: list[object] = []

    def observe(_runtime, process):
        calls.append(process)
        if process is runtime.nodes[0].process:
            first_started.set()
            assert release_first.wait(1)
        else:
            second_done.set()

    monkeypatch.setattr(api, "_observe_managed_node_exit", observe)

    api._dispatch_managed_node_exit(runtime, runtime.nodes[0].process)
    assert first_started.wait(1)
    api._dispatch_managed_node_exit(runtime, runtime.nodes[1].process)
    assert second_done.wait(1)
    release_first.set()
    for thread in tuple(runtime.node_death_threads.values()):
        thread.join(1)
    assert calls == [runtime.nodes[0].process, runtime.nodes[1].process]


@pytest.mark.unit
def test_expected_gcs_tombstone_is_not_reported_as_process_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = runtime.nodes[1]
    node.process.exitcode = 0
    runtime.core_finalized = True
    runtime.node_finalize_acks[node.node_id] = protocol.ShutdownAck(
        "finalize", "node:{}".format(node.node_id), True
    )
    handlers: list[str] = []

    def rpc(_address, handler, message, *, request_timeout):
        handlers.append(handler)
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        assert message.reason is protocol.NodeDeathReason.EXPECTED
        expected = protocol.NodeDeathRecord(
            message.detection_id, message.node_id, message.node_pid,
            message.expected_registration_epoch, 3, 0, message.reason,
            message.detail,
        )
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.APPLIED, 3,
            (_live_info(runtime.nodes[0]),), expected,
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, node.process)

    assert handlers == [api.REPORT_NODE_DEATH_HANDLER]
    expected = runtime.node_expected_exits[node.node_id]
    assert expected.reason is protocol.NodeDeathReason.EXPECTED
    assert runtime.node_deaths == {node.node_id: expected}
    assert runtime.core_worker.observed == []


@pytest.mark.heavy
def test_zero_exit_waits_for_finalize_ack_decision_before_expected_fact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = runtime.nodes[1]
    node.process.exitcode = 0
    runtime.core_finalized = True
    decision = threading.Event()
    runtime.node_finalize_decisions[node.node_id] = decision
    sent = threading.Event()

    def rpc(_address, handler, message, *, request_timeout):
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        assert message.reason is protocol.NodeDeathReason.EXPECTED
        sent.set()
        death = protocol.NodeDeathRecord(
            message.detection_id, message.node_id, message.node_pid,
            message.expected_registration_epoch, 3, 0, message.reason,
            message.detail,
        )
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.APPLIED, 3,
            (_live_info(runtime.nodes[0]),), death,
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    observer = threading.Thread(
        target=api._observe_managed_node_exit, args=(runtime, node.process)
    )
    observer.start()
    assert not sent.wait(0.05)
    runtime.node_finalize_acks[node.node_id] = protocol.ShutdownAck(
        "finalize", "node:{}".format(node.node_id), True
    )
    decision.set()
    observer.join(1)

    assert not observer.is_alive()
    assert runtime.node_deaths[node.node_id].reason is (
        protocol.NodeDeathReason.EXPECTED
    )


@pytest.mark.unit
def test_expected_tombstone_cannot_hide_nonzero_managed_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    survivor, node = runtime.nodes
    node.process.exitcode = -9
    runtime.latest_membership_epoch = 2
    runtime.latest_live_nodes = tuple(_live_info(item) for item in runtime.nodes)
    runtime.core_finalized = True
    runtime.node_finalize_acks[node.node_id] = protocol.ShutdownAck(
        "finalize", "node:{}".format(node.node_id), True
    )
    handlers: list[str] = []

    def rpc(_address, handler, message, *, request_timeout):
        handlers.append(handler)
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        assert message.reason is protocol.NodeDeathReason.PROCESS_EXIT
        death = protocol.NodeDeathRecord(
            message.detection_id, message.node_id, message.node_pid,
            message.expected_registration_epoch, 3, message.exit_code,
            message.reason, message.detail,
        )
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.APPLIED, 3, (_live_info(survivor),),
            death,
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, node.process)

    assert handlers == [api.REPORT_NODE_DEATH_HANDLER]
    assert node.node_id not in runtime.node_expected_exits
    assert runtime.node_deaths[node.node_id].reason is (
        protocol.NodeDeathReason.PROCESS_EXIT
    )


@pytest.mark.unit
def test_post_core_finalize_crash_records_fact_without_snapshot_or_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    node = runtime.nodes[1]
    node.process.exitcode = -9
    runtime.core_finalized = True
    runtime.latest_membership_epoch = 2
    runtime.latest_live_nodes = tuple(_live_info(item) for item in runtime.nodes)
    handlers: list[str] = []

    def rpc(_address, handler, message, *, request_timeout):
        handlers.append(handler)
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        death = protocol.NodeDeathRecord(
            message.detection_id, message.node_id, message.node_pid,
            message.expected_registration_epoch, 3, message.exit_code,
            message.reason, message.detail,
        )
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.APPLIED, 3,
            (_live_info(runtime.nodes[0]),), death,
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, node.process)

    assert handlers == [api.REPORT_NODE_DEATH_HANDLER]
    assert runtime.node_deaths[node.node_id].exit_code == -9
    assert runtime.core_worker.observed == []


@pytest.mark.unit
def test_same_epoch_conflict_never_enters_committed_death_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    survivor, victim = runtime.nodes
    victim.process.exitcode = -9
    runtime.latest_membership_epoch = 3
    runtime.latest_live_nodes = (_live_info(survivor),)

    def rpc(_address, handler, message, *, request_timeout):
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        death = protocol.NodeDeathRecord(
            message.detection_id, message.node_id, message.node_pid,
            message.expected_registration_epoch, 3, message.exit_code,
            message.reason, message.detail,
        )
        # Reuse epoch 3 with a different live view: invalid even though the
        # death record itself has a plausible identity.
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.APPLIED, 3, (), death,
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, victim.process)

    assert victim.node_id in runtime.node_death_errors
    assert runtime.node_gcs_deaths == {}
    assert runtime.node_deaths == {}
    assert runtime.core_worker.observed == []


@pytest.mark.unit
def test_older_valid_death_reply_is_kept_after_newer_membership_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    older = runtime.nodes[1]
    older.process.exitcode = -9
    newer_death = protocol.NodeDeathRecord(
        "newer", runtime.nodes[0].node_id, runtime.nodes[0].startup.node_pid,
        runtime.nodes[0].registration_epoch, 4, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "newer death",
    )
    runtime.latest_membership_epoch = 4
    runtime.latest_live_nodes = ()
    runtime.node_gcs_deaths[newer_death.node_id] = newer_death
    runtime.node_death_notified.add(newer_death.node_id)
    runtime.node_deaths[newer_death.node_id] = newer_death

    def rpc(_address, handler, message, *, request_timeout):
        assert handler == api.REPORT_NODE_DEATH_HANDLER
        death = protocol.NodeDeathRecord(
            message.detection_id, message.node_id, message.node_pid,
            message.expected_registration_epoch, 3, message.exit_code,
            message.reason, message.detail,
        )
        return protocol.ReportNodeDeathReply(
            message.detection_id, message.node_id, message.node_pid,
            protocol.NodeDeathDisposition.APPLIED, 3, (), death,
        )

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, older.process)

    assert runtime.node_death_errors == {}
    assert runtime.latest_membership_epoch == 4
    assert set(runtime.node_gcs_deaths) == {
        runtime.nodes[0].node_id, older.node_id
    }
    assert set(runtime.node_deaths) == {runtime.nodes[0].node_id, older.node_id}
