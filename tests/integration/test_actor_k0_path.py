"""Strictly bounded K0 Actor creation and direct-call acceptance test.

The maximum topology is one GCS, two NodeManagers, two ordinary Workers, and
one dedicated Actor Worker.  Run only by its allowlisted node ID through the
30-second bounded runner; the test submits exactly one Actor and three tiny
method calls. Each Node store is 1 MiB. Work shares ten seconds after init;
public ObjectRef closes share three seconds and always fall through to shutdown.
Synchronous Actor creation/debug calls and runtime startup/shutdown retain their
existing contracts under the outer bound, not deadline-triggered cancellation.
All observed child PIDs and endpoints (six/eight on success, including
owner/trace/Actor) are checked on failure too. A create that never installs a
route still relies on Node shutdown and the outer process-tree bound. The test
creates no listener or helper thread.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray import debug as ray_debug
from miniray import protocol
from miniray.api import _get_runtime
from tests.integration.test_task_path import _close_reference, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_ACTOR_ONLY_RESOURCE = "actor_only"
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


@ray.remote(num_cpus=1, resources={_ACTOR_ONLY_RESOURCE: 1})
class Counter:
    def __init__(self) -> None:
        self.value = 0

    def inc(self) -> tuple[int, int]:
        self.value += 1
        return self.value, os.getpid()


def _pid_exists(pid: int) -> bool:
    """Return whether a POSIX process still exists, including a zombie."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker() -> None:
    context = None
    report = None
    runtime = None
    counter = None
    refs = []
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    actor_pid = None
    actor_address = None
    try:
        context = ray.init(
            num_nodes=2,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=True,
            node_resources=(
                {"CPU": 1},
                {"CPU": 1, _ACTOR_ONLY_RESOURCE: 1},
            ),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        local_node, actor_node = context.nodes
        managed_pids.update(
            {
                context.gcs_pid,
                local_node.node_pid,
                local_node.worker_pid,
                actor_node.node_pid,
                actor_node.worker_pid,
            }
        )
        managed_addresses.update(
            {
                context.gcs_address,
                local_node.node_address,
                local_node.worker_address,
                actor_node.node_address,
                actor_node.worker_address,
            }
        )
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5 and len(managed_addresses) == 7
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        counter = Counter.remote()
        _remaining(deadline)
        # ActorHandle retains only stable logical identity.  Read the current
        # physical route through the GCS-authoritative debug snapshot instead
        # of reintroducing a stale endpoint cache on the public handle.
        actor = ray_debug.snapshot(counter)
        assert isinstance(actor, protocol.ActorSnapshot)
        if actor.worker_pid is not None:
            managed_pids.add(actor.worker_pid)
        if actor.worker_address is not None:
            managed_addresses.add(actor.worker_address)
        _remaining(deadline)
        assert actor.actor_id == counter.actor_id
        assert actor.state is protocol.ActorState.ALIVE
        assert actor.node_id == actor_node.node_id
        assert actor.worker_address is not None
        assert actor.worker_pid is not None
        actor_address = actor.worker_address
        actor_pid = actor.worker_pid
        for _ in range(3):
            _remaining(deadline)
            refs.append(counter.inc.remote())
        results = ray.get(refs, timeout=_remaining(deadline))
        assert [value for value, _pid in results] == [1, 2, 3]
        assert {_pid for _value, _pid in results} == {actor_pid}

        managed_pids.add(actor_pid)
        managed_addresses.add(actor_address)
        assert actor_pid not in context.worker_pids
        assert len(managed_pids) == 6
        assert len(managed_addresses) == 8
        assert os.getpid() not in managed_pids
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if runtime is not None:
                try:
                    # Read the already-installed route only; cleanup must not
                    # issue a GCS query if create/debug failed after a route
                    # was installed but before a handle/snapshot returned. A
                    # still-CREATING route has no physical endpoint to inspect.
                    # This test admitted at most one Actor.
                    clients = runtime.core_worker._actor_clients
                    if not clients._lock.acquire(timeout=max(0.0, cleanup_deadline - time.monotonic())):
                        raise TimeoutError("Actor route inspection exceeded cleanup deadline")
                    try:
                        known_actors = tuple(entry.snapshot for entry in clients._entries.values())
                    finally:
                        clients._lock.release()
                    assert len(known_actors) <= 1
                    for known_actor in known_actors:
                        if known_actor.worker_pid is not None:
                            managed_pids.add(known_actor.worker_pid)
                        if known_actor.worker_address is not None:
                            managed_addresses.add(known_actor.worker_address)
                except Exception as exc:
                    close_errors.append(exc)
            for ref in refs:
                try:
                    _close_reference(ref, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                surviving_pids = tuple(
                    pid for pid in sorted(managed_pids) if _pid_exists(pid)
                )
                surviving_children = tuple(
                    child.pid for child in mp.active_children()
                    if child.pid in managed_pids
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
                assert not close_errors, close_errors

    assert context is not None
    assert report is not None
    assert actor_pid is not None
    assert actor_address is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert report.node_clean
    assert report.worker_clean
    assert report.resources_clean
    assert report.finalized
    assert report.shutdown_ack_clean
    assert not report.forced
