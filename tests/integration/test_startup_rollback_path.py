"""Bounded real-process proof for partial ``init`` rollback.

Run only the exact node ID through ``scripts/run_baseline.py --smoke EXACT``.  Static
bounds are one GCS, two Nodes, one ordinary Worker per Node, no tasks, no Actor,
two 1-MiB stores and no trace collector. The failpoint fires after Node 1 has
returned its validated Node/Worker startup descriptor but before that Node is
published into ``init``'s committed prefix.

At most eight passive callback records capture actual Process PIDs and startup
endpoints. Normal assertions prove init's own rollback before finally calls
shutdown; that unconditional fallback also cleans an unexpectedly successful
init. Every known PID/socket is checked again after the fallback. No test-owned
thread, listener, sleep or new failure is introduced. Startup, rollback and
shutdown retain the external runner's bound, not a fabricated local timeout.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket

import pytest

import miniray as ray
from miniray import api, protocol


pytestmark = pytest.mark.multiprocess_smoke
_MAX_OBSERVATIONS = 8


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_second_node_ready_failure_rolls_back_every_started_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_checkpoint = api._startup_node_ready_checkpoint
    original_shutdown_node = api._shutdown_node
    original_shutdown_gcs = api._shutdown_gcs
    original_force_stop = api._force_stop_process
    observations: list[tuple[str, object]] = []
    observation_failed = overflow = False
    context = report = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()

    def observe(kind, reader):
        nonlocal observation_failed, overflow
        try:
            if len(observations) < _MAX_OBSERVATIONS:
                observations.append((kind, reader()))
            else:
                overflow = True
        except Exception:
            # A diagnostic failure must not interrupt the real rollback or
            # replace its original startup exception. Assert flags in main.
            observation_failed = True

    def observe_checkpoint(
        index: int,
        startup: protocol.NodeStartup,
        fail_after_node_ready: int | None,
    ) -> None:
        observe("ready", lambda: (index, startup, ray.is_initialized()))
        original_checkpoint(index, startup, fail_after_node_ready)

    def observe_shutdown_node(node: object):
        observe("node_rollback", lambda: (node.process.pid, node.startup))
        return original_shutdown_node(node)

    def observe_shutdown_gcs(process: object, startup: protocol.GCSStartup):
        observe("gcs_rollback", lambda: (process.pid, startup))
        return original_shutdown_gcs(process, startup)

    def observe_force_stop(process: object, *, process_group: bool = False):
        observe("force_stop", lambda: (process.pid, process_group))
        return original_force_stop(process, process_group=process_group)

    def remember_pid(pid):
        if type(pid) is int and pid > 0:
            managed_pids.add(pid)

    def remember_startup(startup):
        if isinstance(startup, protocol.NodeStartup):
            remember_pid(startup.node_pid)
            for pid in startup.worker_pids:
                remember_pid(pid)
            managed_addresses.update((startup.node_address, *startup.worker_addresses))
        elif isinstance(startup, protocol.GCSStartup):
            remember_pid(startup.gcs_pid)
            managed_addresses.add(startup.gcs_address)

    def remember_observations():
        for kind, record in observations:
            if kind == "ready":
                remember_startup(record[1])
            elif kind in ("node_rollback", "gcs_rollback"):
                remember_pid(record[0])
                remember_startup(record[1])
            elif kind == "force_stop":
                remember_pid(record[0])

    def assert_no_known_survivors():
        # Evaluate every tracked PID and endpoint before asserting, so one
        # surviving child does not suppress checks for its siblings. Successful
        # socket probes are always closed, including on an assertion failure.
        surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
        surviving_children = tuple(
            child.pid for child in mp.active_children() if child.pid in managed_pids
        )
        open_addresses = []
        for address in sorted(managed_addresses):
            try:
                with socket.create_connection(address, timeout=0.1):
                    open_addresses.append(address)
            except OSError:
                pass
        assert not surviving_pids, surviving_pids
        assert not surviving_children, surviving_children
        assert not open_addresses, open_addresses

    try:
        monkeypatch.setattr(api, "_startup_node_ready_checkpoint", observe_checkpoint)
        monkeypatch.setattr(api, "_shutdown_node", observe_shutdown_node)
        monkeypatch.setattr(api, "_shutdown_gcs", observe_shutdown_gcs)
        monkeypatch.setattr(api, "_force_stop_process", observe_force_stop)
        with pytest.raises(
            RuntimeError,
            match="injected startup failure after node 1 reported readiness",
        ):
            context = ray.init(
                num_nodes=2,
                num_cpus=1,
                num_workers_per_node=1,
                object_store_bytes=1024 * 1024,
                enable_tracing=False,
                _test_fail_after_node_ready=1,
            )

        assert not observation_failed and not overflow
        checkpoints = [record for kind, record in observations if kind == "ready"]
        startups = [startup for _, startup, _ in checkpoints]
        visible_at_checkpoint = [visible for _, _, visible in checkpoints]
        node_rollbacks = [record for kind, record in observations if kind == "node_rollback"]
        orderly_node_rollbacks = [pid for pid, _ in node_rollbacks]
        gcs_records = [record for kind, record in observations if kind == "gcs_rollback"]
        gcs_rollbacks = [startup for _, startup in gcs_records]
        force_stops = [record for kind, record in observations if kind == "force_stop"]
        assert [index for index, _, _ in checkpoints] == [0, 1]
        assert len(startups) == 2
        assert visible_at_checkpoint == [False, False]
        assert not ray.is_initialized()
        first, unpublished_second = startups
        assert orderly_node_rollbacks == [first.node_pid]
        assert node_rollbacks == [(first.node_pid, first)]
        assert len(gcs_rollbacks) == 1
        gcs = gcs_rollbacks[0]
        assert gcs_records == [(gcs.gcs_pid, gcs)]
        assert first.node_id != unpublished_second.node_id
        assert all(len(startup.worker_ids) == len(startup.worker_pids) == len(startup.worker_addresses) == 1
                   for startup in startups)

        remember_observations()
        assert managed_pids == {gcs.gcs_pid, first.node_pid, first.worker_pid,
                                unpublished_second.node_pid, unpublished_second.worker_pid}
        assert len(managed_pids) == 5 and os.getpid() not in managed_pids
        assert (first.node_pid, True) in force_stops
        assert (unpublished_second.node_pid, True) in force_stops
        assert (gcs.gcs_pid, False) in force_stops
        assert len(managed_addresses) == 5
        # This assertion precedes the safety fallback: init's own rollback,
        # not a later successful shutdown, must have cleaned every known child.
        assert_no_known_survivors()
    finally:
        try:
            remember_observations()
            if context is not None:
                # pytest.raises also fails if the checkpoint stops raising.
                # The returned live runtime still requires all its endpoints,
                # including the otherwise never-started Driver owner service.
                managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
                managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
            if ray.is_initialized():
                runtime = api._get_runtime()
                if runtime.owner_service is not None:
                    managed_addresses.add(runtime.owner_service.address)
        finally:
            try:
                report = ray.shutdown()
            finally:
                remember_observations()
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                assert_no_known_survivors()
                assert not ray.is_initialized()
                assert not observation_failed and not overflow

    assert context is None and report is None
