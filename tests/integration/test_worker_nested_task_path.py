"""Bounded Worker-side nested submission across two logical nodes.

The parent holds a ``parent_only`` resource on node 0 but requests no CPU.  From
inside that ordinary Worker it submits a child requiring ``child_only`` and one
CPU, then resolves the child with the public ``ray.get`` API.  The parent returns
only a plain value and the two execution PIDs; no ObjectRef escapes to the Driver.

The trace assertion distinguishes the Driver Core from the Core embedded in
the parent Worker and proves that child placement bypasses GCS. Publication
still uses the same real GCS-backed output protocol; trace names do not deny
that dependency. After review and registration, run this exact node ID only through
``scripts/run_baseline.py --case EXACT``. The hard bounds are one
GCS, two NodeManagers, one ordinary Worker per node, exactly two tiny tasks, one
nested ``get``, one bounded trace poll, and a 1 MiB store per Node. Driver and
Worker waits share a ten-second post-init work deadline; public reference
close waits use at most three seconds, and finally always shuts the cluster
down and checks all five PIDs and seven endpoints. Run under the 30-second
process-tree deadline; no additional test thread or listener is created.

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

_PARENT_RESOURCE = "parent_only"
_CHILD_RESOURCE = "child_only"
_NESTED_GET_TIMEOUT_SECONDS = 5.0
_DRIVER_GET_TIMEOUT_SECONDS = 10.0
_TRACE_DELIVERY_TIMEOUT_SECONDS = 2.0
_TRACE_POLL_SECONDS = 0.01
_CLEANUP_SECONDS = 3.0
_CHILD_CORE_EVENTS = frozenset(
    {
        "task_submitted",
        "lease_requested",
        "lease_granted",
        "task_pushed",
        "task_finished",
    }
)
_TASK_EVENT_PREFIXES = ("task_", "lease_")


@ray.remote(num_cpus=1, resources={_CHILD_RESOURCE: 1}, max_retries=0)
def nested_child(value: int) -> tuple[int, int]:
    return value + 1, os.getpid()


@ray.remote(num_cpus=0, resources={_PARENT_RESOURCE: 1}, max_retries=0)
def nested_parent(value: int, deadline: float) -> tuple[int, int, int]:
    child_ref = None
    try:
        _remaining(deadline)
        child_ref = nested_child.remote(value)
        child_value, child_pid = ray.get(
            child_ref, timeout=min(_NESTED_GET_TIMEOUT_SECONDS, _remaining(deadline))
        )
        return child_value, os.getpid(), child_pid
    finally:
        if child_ref is not None:
            child_ref.close(timeout=_CLEANUP_SECONDS)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Worker nested task exceeded its work deadline")
    return remaining


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _child_trace(
    parent_worker_pid: int, work_deadline: float,
) -> tuple[tuple[object, ...], str | None]:
    """Wait only for the one child task's complete Worker-Core story."""

    _remaining(work_deadline)
    deadline = min(work_deadline, time.monotonic() + _TRACE_DELIVERY_TIMEOUT_SECONDS)
    wake = threading.Event()
    while True:
        records = tuple(ray.trace())
        submitted = [
            record
            for record in records
            if record.component == "worker_core"
            and record.process_id == str(parent_worker_pid)
            and record.event == "task_submitted"
        ]
        if len(submitted) == 1:
            child_task_id = dict(submitted[0].fields).get("task_id")
            child_events = {
                record.event
                for record in records
                if record.component == "worker_core"
                and record.process_id == str(parent_worker_pid)
                and dict(record.fields).get("task_id") == child_task_id
            }
            if child_task_id and _CHILD_CORE_EVENTS <= child_events:
                return records, child_task_id
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return records, None
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))


def _assert_tracked_hygiene(pids, addresses) -> None:
    deadline = time.monotonic() + 2.0
    wake = threading.Event()
    alive = tuple(pid for pid in pids if _pid_exists(pid))
    while alive:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))
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


def test_worker_submits_child_task_and_gets_plain_result() -> None:
    context = None
    report = None
    result_ref = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _PARENT_RESOURCE: 1},
                {"CPU": 1, _CHILD_RESOURCE: 1},
            ),
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=True,
        )
        deadline = time.monotonic() + _DRIVER_GET_TIMEOUT_SECONDS
        parent_node, child_node = context.nodes
        managed_pids.update(
            {
                context.gcs_pid,
                parent_node.node_pid,
                parent_node.worker_pid,
                child_node.node_pid,
                child_node.worker_pid,
            }
        )
        managed_addresses.update(
            {
                context.gcs_address,
                parent_node.node_address,
                parent_node.worker_address,
                child_node.node_address,
                child_node.worker_address,
            }
        )
        runtime = _get_runtime()
        if context.trace_address is not None:
            managed_addresses.add(context.trace_address)
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        assert context.trace_address is not None and runtime.owner_service is not None
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7
        assert os.getpid() not in managed_pids

        result_ref = nested_parent.remote(41, deadline)
        value, parent_pid, child_pid = ray.get(
            result_ref, timeout=_remaining(deadline)
        )

        assert value == 42
        assert parent_pid == parent_node.worker_pid
        assert child_pid == child_node.worker_pid
        assert parent_pid != child_pid
        assert (parent_pid, child_pid) == (
            context.worker_pids[0],
            context.worker_pids[1],
        )

        records, child_task_id = _child_trace(parent_pid, deadline)
        assert child_task_id is not None, [
            (record.process_id, record.component, record.event, record.fields)
            for record in records
        ]
        child_records = [
            record
            for record in records
            if record.component == "worker_core"
            and record.process_id == str(parent_pid)
            and dict(record.fields).get("task_id") == child_task_id
        ]
        assert _CHILD_CORE_EVENTS <= {
            record.event for record in child_records
        }
        assert all(record.process_id == str(parent_pid) for record in child_records)
        # The placement path is CoreWorker -> Node -> Worker. GCS never receives
        # child TaskSpecs; its separate publication metadata is still real.
        assert not any(
            record.component == "gcs"
            and record.event.startswith(_TASK_EVENT_PREFIXES)
            for record in records
        )
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if result_ref is not None:
                try:
                    result_ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                _assert_tracked_hygiene(managed_pids, managed_addresses)

    assert not close_errors and context is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
