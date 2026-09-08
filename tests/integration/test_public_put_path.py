"""Bounded single-node acceptance for inline and stored ``ray.put`` values.

The topology is fixed at one GCS, one NodeManager, and one ordinary Worker.
No task is submitted: both puts must be owned and published by the Driver-side
CoreWorker, with the large value using only the local Node object store.
The store is 1 MiB and the stored value is 64 KiB. Work shares ten seconds
after init; both real public reference closes share three seconds and always
fall through to shutdown. Synchronous put/startup/shutdown retain their existing
contracts under the 30-second exact-node runner, not deadline cancellation.
All observed child PIDs and owner/trace/runtime ports are checked on failure too.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime
from miniray.ownership import ObjectState
from tests.integration.test_task_path import _close_reference, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_STORED_BYTES = 64 * 1024
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_public_put_inline_and_stored_values_without_worker_execution() -> None:
    context = None
    report = None
    inline_ref = stored_ref = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=1,
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=True,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update(
            {context.gcs_pid, context.node_pid, context.worker_pid}
        )
        managed_addresses.update(
            {context.gcs_address, context.node_address, context.worker_address}
        )
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3
        assert len(managed_addresses) == 5
        assert os.getpid() not in managed_pids
        core = runtime.core_worker
        submission_index = core._submission_index

        inline_value = {"kind": "inline", "value": 7}
        stored_value = b"p" * _STORED_BYTES
        _remaining(deadline)
        inline_ref = ray.put(inline_value)
        _remaining(deadline)
        stored_ref = ray.put(stored_value)
        _remaining(deadline)
        assert inline_ref.object_id != stored_ref.object_id

        ready, remaining = ray.wait(
            [inline_ref, stored_ref], num_returns=2, timeout=0
        )
        assert ready == [inline_ref, stored_ref]
        assert remaining == []
        assert ray.get(inline_ref, timeout=_remaining(deadline)) == inline_value
        assert ray.get(stored_ref, timeout=_remaining(deadline)) == stored_value

        inline_snapshot = core.owner_table.snapshot(inline_ref.object_id)
        stored_snapshot = core.owner_table.snapshot(stored_ref.object_id)
        assert inline_snapshot.state is ObjectState.READY_INLINE
        assert inline_snapshot.producer_task_spec is None
        assert stored_snapshot.state is ObjectState.READY_STORED
        assert stored_snapshot.producer_task_spec is None
        assert stored_snapshot.locations == frozenset({context.node_id})

        # ``put`` is a Driver/ObjectStore operation.  The ordinary Worker is
        # still the original idle process; no additional executor was created.
        assert context.worker_pid in managed_pids
        assert len(mp.active_children()) == 2  # GCS + Node; Worker is Node's child.
        assert core._submission_index == submission_index
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for ref in (stored_ref, inline_ref):
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
                assert not close_errors, close_errors

    assert context is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
