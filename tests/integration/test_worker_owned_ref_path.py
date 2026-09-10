"""Bounded smoke for a Worker-owned child ObjectRef escaping to Driver.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Bounds:
one GCS, two NodeManagers, one ordinary Worker per node, exactly two tiny tasks,
one escaping inline ObjectRef, two outer deserializations, two owner gets, and
the runner's 30-second process-tree deadline. Each Node has a 1 MiB store. All
gets share one ten-second work deadline; real public close waits share three
seconds in finally before unconditional shutdown and five-PID/seven-endpoint
hygiene checks, including failure paths. Worker owners reuse Worker endpoints.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime


pytestmark = pytest.mark.multiprocess_smoke

_PARENT_RESOURCE = "borrow_parent"
_CHILD_RESOURCE = "borrow_child"
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


@ray.remote(num_cpus=1, resources={_CHILD_RESOURCE: 1}, max_retries=0)
def borrowed_child(value: int) -> tuple[str, int]:
    return "worker-owned", value + 1


@ray.remote(num_cpus=0, resources={_PARENT_RESOURCE: 1}, max_retries=0)
def return_child_ref(value: int) -> object:
    return borrowed_child.remote(value)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Worker-owned reference exceeded its work deadline")
    return remaining


def _wait_for_borrower_release(core, reference, deadline: float) -> None:
    # Public close only bounds its local receipt. The original repeated-get
    # contract needs this particular remote release to have really converged.
    key = (reference.owner_worker_id, reference.object_id, core.worker_id, reference.borrower_token)
    with core._completion:
        while key in core._borrowed_release_obligations:
            core._completion.wait(_remaining(deadline))


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _assert_tracked_hygiene(pids, addresses) -> None:
    deadline = time.monotonic() + 2.0
    wake = threading.Event()
    alive = tuple(pid for pid in pids if _pid_exists(pid))
    while alive:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wake.wait(min(0.01, remaining))
        alive = tuple(pid for pid in pids if _pid_exists(pid))
    children = tuple(child.pid for child in mp.active_children() if child.pid in pids)
    open_endpoints = []
    for address in addresses:
        try:
            with socket.create_connection(address, timeout=0.1):
                open_endpoints.append(address)
        except OSError:
            pass
    assert not (alive or children or open_endpoints), {
        "live_pids": alive, "active_children": children, "open_endpoints": open_endpoints,
    }
    assert not ray.is_initialized()


def test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get() -> None:
    context = report = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    close_errors = []
    outer = None
    first = None
    second = None
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _PARENT_RESOURCE: 1},
                {"CPU": 1, _CHILD_RESOURCE: 1},
            ),
            num_workers_per_node=1, inline_threshold=1024,
            object_store_bytes=1024 * 1024, enable_tracing=True,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        if context.trace_address is not None:
            managed_addresses.add(context.trace_address)
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        assert context.trace_address is not None and runtime.owner_service is not None
        assert len(managed_pids) == 5 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 7
        outer = return_child_ref.remote(41)
        first = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(first, ray.ObjectRef)
        assert first.owner_worker_id == context.worker_ids[0]
        assert first.owner_address == context.worker_addresses[0]
        assert first.owner_worker_id != core.worker_id and first.borrower_token is not None
        assert first.owner_address in managed_addresses
        assert ray.get(first, timeout=_remaining(deadline)) == ("worker-owned", 42)
        first_token = first.borrower_token
        first.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        _wait_for_borrower_release(core, first, deadline)

        # A second deserialization occurs after the first borrower released.
        # The durable transfer/contained pin must still authorize a fresh token.
        second = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(second, ray.ObjectRef)
        assert first.object_id == second.object_id
        assert second.owner_worker_id == context.worker_ids[0]
        assert second.owner_address == context.worker_addresses[0]
        assert first_token != second.borrower_token
        assert second.borrower_token is not None
        assert ray.get(second, timeout=_remaining(deadline)) == ("worker-owned", 42)
        second.close(timeout=min(_CLEANUP_SECONDS, _remaining(deadline)))
        _wait_for_borrower_release(core, second, deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for reference in (first, second, outer):
                if reference is not None:
                    try:
                        reference.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
                    except Exception as exc:
                        close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                _assert_tracked_hygiene(managed_pids, managed_addresses)

    assert not close_errors and context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.node_clean and report.worker_clean
    assert report.gcs_pid == context.gcs_pid and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
