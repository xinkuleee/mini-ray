"""Bounded real-process acceptance test for one committed placement group.

Each exact case starts five children: one GCS, two one-CPU Nodes, and one
ordinary Worker per Node, with 1 MiB per store and tracing disabled. They cover
explicit removal after two tiny bundle-bound Tasks and cluster-owned cleanup
of one committed group without application Tasks or explicit removal.

Work shares ten seconds after init; public reference receipt waits use the
remaining work budget and at most three seconds. Finally retries outstanding
receipts within one shared three-second cleanup budget and always attempts
shutdown. Startup, synchronous PG create/remove, and shutdown still require
the outer 30-second process-tree runner: a work deadline is not transaction
cancellation. Run one exact node ID at a time. No test-owned thread, listener,
sleep or fault is added. All five PIDs and six endpoints, including the Driver
owner, are checked even after a work, remove or close failure.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime
from tests.integration.test_task_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


@ray.remote(num_cpus=1)
def identify_pg_executor(bundle_index: int) -> tuple[int, int]:
    return bundle_index, os.getpid()


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("placement-group work exceeded its deadline")
    return remaining


def _assert_managed_cleanup(pids, addresses) -> None:
    surviving_pids = tuple(pid for pid in sorted(pids) if _pid_exists(pid))
    surviving_children = tuple(
        child.pid for child in mp.active_children() if child.pid in pids
    )
    open_addresses = []
    for address in sorted(addresses):
        try:
            with socket.create_connection(address, timeout=0.1):
                open_addresses.append(address)
        except OSError:
            pass
    assert not surviving_pids, surviving_pids
    assert not surviving_children, surviving_children
    assert not open_addresses, open_addresses


def test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources() -> None:
    context = None
    group = None
    refs: list[ray.ObjectRef] = []
    removed = False
    report = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=2,
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update(
            {context.gcs_pid, *context.node_pids, *context.worker_pids}
        )
        managed_addresses.update(
            {context.gcs_address, *context.node_addresses, *context.worker_addresses}
        )
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 6
        assert context.trace_address is None
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        group = ray.placement_group(
            [{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD"
        )
        _remaining(deadline)
        assert group.bundle_count == 2
        assert tuple(key.bundle_index for key in group.placements) == (0, 1)
        assert len({key.node_id for key in group.placements}) == 2

        for index in range(group.bundle_count):
            _remaining(deadline)
            # Retain each returned handle immediately if the next submission
            # fails; a partially built list comprehension loses that cleanup.
            refs.append(identify_pg_executor.options(
                placement_group=group, bundle_index=index
            ).remote(index))
        results = ray.get(refs, timeout=_remaining(deadline))

        worker_pid_by_node = {
            node.node_id: node.worker_pid for node in context.nodes
        }
        expected = [
            (key.bundle_index, worker_pid_by_node[key.node_id])
            for key in group.placements
        ]
        assert results == expected
        assert len({pid for _index, pid in results}) == 2

        close_deadline = min(deadline, time.monotonic() + _CLEANUP_SECONDS)
        for ref in refs:
            _close_reference(ref, close_deadline)
        refs.clear()
        _remaining(deadline)
        removed = ray.remove_placement_group(group)
        _remaining(deadline)
        assert removed
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for ref in refs:
                try:
                    _close_reference(ref, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                # Keep the original public cleanup attempt. This synchronous
                # transaction has no timeout/cancel API; the outer runner, not
                # cleanup_deadline, bounds a stuck create/remove operation.
                if group is not None and not removed:
                    try:
                        ray.remove_placement_group(group)
                    except Exception:
                        pass
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    _assert_managed_cleanup(managed_pids, managed_addresses)
                    assert not close_errors, close_errors

    assert context is not None
    assert report is not None
    assert removed
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)


def test_shutdown_removes_committed_group_without_explicit_remove() -> None:
    """Cluster PG-drain converges while both Node endpoints are still alive."""

    context = None
    group = None
    report = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=2,
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update(
            {context.gcs_pid, *context.node_pids, *context.worker_pids}
        )
        managed_addresses.update(
            {context.gcs_address, *context.node_addresses, *context.worker_addresses}
        )
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5 and len(managed_addresses) == 6
        assert os.getpid() not in managed_pids and context.trace_address is None
        _remaining(deadline)
        group = ray.placement_group(
            [{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD"
        )
        _remaining(deadline)
        assert group.bundle_count == 2
        assert len({key.node_id for key in group.placements}) == 2
        # Deliberately do not call remove_placement_group().  shutdown() first
        # fences both Nodes, then asks the still-live GCS to converge every
        # participant abort before Node finalization.
    finally:
        try:
            report = ray.shutdown()
        finally:
            _assert_managed_cleanup(managed_pids, managed_addresses)

    assert context is not None and group is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
