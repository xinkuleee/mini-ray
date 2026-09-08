"""Strictly bounded two-node GCS/Hybrid spillback acceptance test.

It starts one GCS, two NodeManagers, and one Worker per node, with two 1 MiB
stores and one tiny Task. Work shares ten seconds; public close has a separate
three-second receipt bound and always falls through to shutdown. Run only its
exact ID through the 30-second process-tree runner.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime
from tests.integration.test_task_path import _close_reference, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_REMOTE_ONLY_RESOURCE = "node2_only"


def _execution_process_id() -> int:
    return os.getpid()


def _pid_exists(pid: int) -> bool:
    """Return whether a POSIX process still exists, including a zombie."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_custom_resource_spills_task_to_second_node_and_cleans_cluster() -> None:
    """Exercise one deterministic spillback with five bounded child processes."""

    remote_only_task = ray.remote(
        num_cpus=1, resources={_REMOTE_ONLY_RESOURCE: 1}
    )(_execution_process_id)
    context = ref = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    report = None
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1},
                {"CPU": 1, _REMOTE_ONLY_RESOURCE: 1},
            ),
            object_store_bytes=1024 * 1024,
        )
        deadline = time.monotonic() + 10.0
        runtime = _get_runtime()
        nodes = tuple(context.nodes)
        assert len(nodes) == 2
        local_node, remote_node = nodes
        assert context.node_id == local_node.node_id

        managed_pids.update({
            context.gcs_pid,
            local_node.node_pid,
            local_node.worker_pid,
            remote_node.node_pid,
            remote_node.worker_pid,
        })
        managed_addresses.update({
            context.gcs_address,
            local_node.node_address,
            local_node.worker_address,
            remote_node.node_address,
            remote_node.worker_address,
        })
        assert context.trace_address is not None and runtime.owner_service is not None
        managed_addresses.update((context.trace_address, runtime.owner_service.address))
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7
        assert os.getpid() not in managed_pids

        ref = remote_only_task.remote()
        assert ray.get(ref, timeout=_remaining(deadline)) == remote_node.worker_pid
    finally:
        try:
            _close_reference(ref, time.monotonic() + 3.0)
        except Exception as exc:
            close_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not ray.is_initialized()
    assert report is not None
    assert context is not None
    assert not close_errors
    assert len(managed_pids) == 5
    assert len(managed_addresses) == 7
    assert report.core_stopped
    assert report.resources_clean
    assert report.finalized
    assert not report.forced
    assert report.gcs_pid == context.gcs_pid and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean and report.shutdown_ack_clean
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)

    assert all(not _pid_exists(pid) for pid in managed_pids)
    assert all(
        child.pid not in managed_pids
        for child in mp.active_children()
    )
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
