"""Pure Driver certificate ordering and observer retry contracts.

Two fake managed processes and one real bare Node metadata service; all RPC,
thread starts, clock and delay are synchronous doubles. No process/sockets.
"""
from dataclasses import replace
import threading
from types import SimpleNamespace

import pytest

from miniray import api, protocol
from miniray.node_death_view import PUBLISH_NODE_DEATH_VIEW, PublishInstalledNodeDeathsReply
from tests.unit.test_node_death_api_runtime import _runtime, _live_info
from tests.unit.test_node_death_view import _no_runtime as _no_runtime
from tests.unit.test_worker_node_death_view import _node


pytestmark = pytest.mark.unit


def _fixture():
    runtime = _runtime()
    survivor, victim = runtime.nodes
    victim.process.exitcode = -9
    request = api._node_death_request(victim, -9)
    death = protocol.NodeDeathRecord(
        request.detection_id, victim.node_id, victim.startup.node_pid, victim.registration_epoch,
        3, -9, request.reason, request.detail,
    )
    live = (_live_info(survivor),)
    snapshot = protocol.InstallClusterSnapshot(3, api._cluster_snapshot_id(3, live), live)
    from miniray.node_death_view import InstalledNodeDeathView
    ack = protocol.InstallClusterSnapshotReply(3, snapshot.snapshot_id, survivor.node_id, True)
    node = _node(InstalledNodeDeathView(snapshot, (ack,), (death,)))
    runtime.latest_membership_epoch, runtime.latest_live_nodes = 3, live
    return runtime, node, survivor, victim, death, snapshot


def test_actual_survivor_ack_precedes_certified_retention_and_driver_notification(monkeypatch):
    runtime, node, survivor, victim, death, snapshot = _fixture()
    calls = []

    def rpc(address, handler, request, **options):
        calls.append(handler)
        if handler == api.REPORT_NODE_DEATH_HANDLER:
            assert address == runtime.gcs_startup.gcs_address
            return protocol.ReportNodeDeathReply(
                request.detection_id, victim.node_id, victim.startup.node_pid,
                protocol.NodeDeathDisposition.APPLIED, 3, (_live_info(survivor),), death,
            )
        assert address == survivor.startup.node_address
        if handler == api.INSTALL_CLUSTER_SNAPSHOT_HANDLER:
            assert not runtime.latest_snapshot_acks
            return node._handle_install_cluster_snapshot(request)
        assert handler == PUBLISH_NODE_DEATH_VIEW
        assert request.view.survivor_acks == runtime.latest_snapshot_acks
        assert request.view.snapshot == snapshot and request.view.deaths == (death,)
        assert not runtime.core_worker.observed
        return node._handle_publish_installed_node_deaths(request)

    monkeypatch.setattr(api, "rpc_request", rpc)
    api._observe_managed_node_exit(runtime, victim.process)
    assert not runtime.node_death_errors
    assert calls == [api.REPORT_NODE_DEATH_HANDLER, api.INSTALL_CLUSTER_SNAPSHOT_HANDLER, PUBLISH_NODE_DEATH_VIEW]
    assert runtime.core_worker.observed == [(death, snapshot)]
    assert runtime.node_deaths[victim.node_id] == death
    assert runtime.node_death_events[victim.node_id].is_set()
    assert node._certified_node_deaths.deaths == (death,)


def test_lost_retention_ack_retries_identical_certificate_not_new_snapshot(monkeypatch):
    runtime, node, survivor, _victim, death, snapshot = _fixture()
    runtime.latest_snapshot_acks = (node._handle_install_cluster_snapshot(snapshot),)
    requests, delays = [], []

    def rpc(address, handler, request, **options):
        assert address == survivor.startup.node_address and handler == PUBLISH_NODE_DEATH_VIEW
        assert options["connect_timeout"] <= 0.25 and options["request_timeout"] <= 1.0
        requests.append(request)
        reply = node._handle_publish_installed_node_deaths(request)
        if len(requests) == 1:
            raise TimeoutError("retained but ACK lost")
        return reply

    monkeypatch.setattr(api, "rpc_request", rpc)
    monkeypatch.setattr(threading.Event, "wait", lambda self, delay: delays.append(delay))
    assert api._publish_installed_node_deaths(runtime, snapshot, (death,))
    assert len(requests) == 2 and requests[0] == requests[1]
    assert node._certified_node_deaths == requests[0].view
    assert len(delays) == 1 and 0 <= delays[0] <= api._NODE_DEATH_RETRY_INTERVAL_SECONDS


def test_superseded_snapshot_never_publishes_old_certificate(monkeypatch):
    runtime, node, _survivor, _victim, death, snapshot = _fixture()
    runtime.latest_snapshot_acks = (node._handle_install_cluster_snapshot(snapshot),)
    runtime.latest_membership_epoch = 4
    monkeypatch.setattr(api, "rpc_request", lambda *_a, **_k: pytest.fail("superseded certificate sent"))
    assert not api._publish_installed_node_deaths(runtime, snapshot, (death,))
    assert node._certified_node_deaths is None


@pytest.mark.parametrize("shutdown", (False, True))
def test_existing_observer_retries_failed_control_round_until_terminal_or_shutdown(monkeypatch, shutdown):
    runtime = _runtime()
    node = runtime.nodes[1]
    turns, delays = [], []

    def observe(candidate, process):
        assert candidate is runtime and process is node.process
        turns.append(process)
        if len(turns) == 1:
            runtime.node_death_errors[node.node_id] = TimeoutError("control round unavailable")
            runtime.shutting_down = shutdown
        else:
            assert len(turns) == 2
            runtime.node_death_errors.pop(node.node_id)
            runtime.node_deaths[node.node_id] = "exact terminal test sentinel"

    class Thread:
        def __init__(self, target, **kwargs):
            self.target = target
            assert kwargs["daemon"] is True

        def start(self):
            assert node.node_id in runtime.node_death_threads
            self.target()

    monkeypatch.setattr(api, "_observe_managed_node_exit", observe)
    monkeypatch.setattr(api.threading, "Thread", Thread)
    monkeypatch.setattr(threading.Event, "wait", lambda self, delay: delays.append(delay))
    api._dispatch_managed_node_exit(runtime, node.process)
    assert len(turns) == (1 if shutdown else 2)
    assert delays == ([] if shutdown else [0.1])
    assert not runtime.node_death_threads


def test_retention_round_timeout_keeps_actual_gcs_death_for_the_same_observer_retry(monkeypatch):
    runtime, node, survivor, victim, death, snapshot = _fixture()
    attempts, reports, delays = [], [], []
    clock = [10.0]

    def rpc(address, handler, request, **options):
        if handler == api.REPORT_NODE_DEATH_HANDLER:
            reports.append(request)
            assert len(reports) <= 2
            return protocol.ReportNodeDeathReply(
                request.detection_id, victim.node_id, victim.startup.node_pid,
                protocol.NodeDeathDisposition.APPLIED if len(reports) == 1 else protocol.NodeDeathDisposition.ALREADY_DEAD,
                3, (_live_info(survivor),), death,
            )
        assert address == survivor.startup.node_address
        if handler == api.INSTALL_CLUSTER_SNAPSHOT_HANDLER:
            return node._handle_install_cluster_snapshot(request)
        assert handler == PUBLISH_NODE_DEATH_VIEW
        attempts.append(request)
        reply = node._handle_publish_installed_node_deaths(request)
        if len(attempts) == 1:
            clock[0] += api._STOP_TIMEOUT_SECONDS + 1.0
            raise TimeoutError("certificate retained; entire control round lost its reply")
        assert len(attempts) == 2
        assert runtime.node_gcs_deaths[victim.node_id] == death
        assert not runtime.core_worker.observed
        return reply

    class Thread:
        def __init__(self, target, **_kwargs):
            self.target = target
        def start(self):
            self.target()

    def backoff(_event, timeout):
        assert timeout == 0.1 and not delays
        assert runtime.node_death_errors and victim.node_id not in runtime.node_deaths
        assert node._certified_node_deaths is not None
        delays.append(timeout)

    monkeypatch.setattr(api, "rpc_request", rpc)
    monkeypatch.setattr(api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(api.threading, "Thread", Thread)
    monkeypatch.setattr(threading.Event, "wait", backoff)
    api._dispatch_managed_node_exit(runtime, victim.process)
    assert len(reports) == len(attempts) == 2
    assert reports[0] == reports[1] and attempts[0] == attempts[1]
    assert delays == [0.1] and not runtime.node_death_errors
    assert runtime.node_deaths[victim.node_id] == death
    assert runtime.core_worker.observed == [(death, snapshot)]
    assert not runtime.node_death_threads
