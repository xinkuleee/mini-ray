"""Bounded real-process smoke coverage for the first task path.

One GCS, one Node and one Worker, three tiny Tasks, one 1 MiB store, no
fault/retry or test-owned thread. Work shares ten seconds after init; actual
reference finalizers share three seconds and always fall through to shutdown.
Run this exact node ID only through the 30-second process-tree runner.
"""

from __future__ import annotations

import os
import multiprocessing as mp
import socket
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("teaching task work exceeded its deadline")
    return remaining


def _close_reference(reference, deadline: float) -> None:
    """Exercise public close while observing its original local receipt."""

    if reference is None:
        return
    assert reference.borrower_token is None
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    # A previous timed-out close is still an outstanding receipt: do not skip
    # this wait merely because the Python handle is already marked closed.
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _pid_exists(pid: int) -> bool:
    """Return whether a POSIX process still exists, including a zombie."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@ray.remote
def identify_and_add(left: int, right: int) -> tuple[int, int]:
    return os.getpid(), left + right


def test_one_node_one_worker_task_path() -> None:
    with pytest.raises(TypeError, match="unsupported remote option"):
        ray.remote(unknown_option=True)(lambda: None)

    context = report = None
    refs = []
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    driver_pid = os.getpid()
    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           object_store_bytes=1024 * 1024, enable_tracing=True)
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        managed_pids.update((context.gcs_pid, context.node_pid, context.worker_pid))
        assert context.trace_address is not None and runtime.owner_service is not None
        managed_addresses.update((context.gcs_address, context.node_address, context.worker_address,
                                  runtime.owner_service.address, context.trace_address))
        assert len(managed_pids) == 3 and driver_pid not in managed_pids
        assert len(managed_addresses) == 5
        for value in range(3):
            refs.append(identify_and_add.remote(value, 7))

        assert all(isinstance(ref, ray.ObjectRef) for ref in refs)
        ready, remaining = ray.wait(refs, num_returns=1, timeout=_remaining(deadline))
        assert len(ready) == 1
        assert len(remaining) == 2

        results = ray.get(refs, timeout=_remaining(deadline))
        worker_pids = {worker_pid for worker_pid, _ in results}
        assert worker_pids == {context.worker_pid}
        assert len({driver_pid, context.node_pid, context.worker_pid}) == 3
        assert [value for _, value in results] == [7, 8, 9]
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for ref in refs:
                try:
                    _close_reference(ref, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not ray.is_initialized()
    assert not close_errors and context is not None and report is not None
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.worker_pid == context.worker_pid
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.node_pid == context.node_pid
    assert report.node_clean and report.node_exitcode == 0
    assert report.finalized
    assert report.shutdown_ack_clean
    assert report.resources_clean
    assert not report.forced
    assert all(not _pid_exists(pid) for pid in managed_pids)
    assert all(
        child.pid not in managed_pids
        for child in mp.active_children()
    )
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
