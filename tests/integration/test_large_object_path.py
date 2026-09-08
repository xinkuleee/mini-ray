"""One bounded task proving the original single-node stored result path.

One GCS, Node and ordinary Worker (three children), one 1 MiB store, one
64 KiB result and one trace collector. Work shares ten seconds after init;
public reference close gets three seconds and always falls through to shutdown.
Startup/shutdown keep their runtime contracts under the 30-second exact-node
runner. All observed PIDs and five owner/trace/runtime ports are checked even
if setup, work, close or shutdown raises. No extra task or listener is added.
"""

from __future__ import annotations

import os
import multiprocessing as mp
import socket
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime
from miniray.ownership import ObjectState
from miniray import protocol
from tests.integration.test_task_path import _close_reference, _pid_exists, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_PAYLOAD_BYTES = 64 * 1024
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


@ray.remote
def make_bounded_payload() -> tuple[int, bytes]:
    return os.getpid(), b"L" * _PAYLOAD_BYTES


def test_one_node_large_result_uses_object_store() -> None:
    context = report = ref = None
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
        managed_pids.update((context.gcs_pid, context.node_pid, context.worker_pid))
        managed_addresses.update((context.gcs_address, context.node_address, context.worker_address))
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 5
        _remaining(deadline)
        ref = make_bounded_payload.remote()
        ready, remaining = ray.wait([ref], num_returns=1, timeout=_remaining(deadline))
        assert ready == [ref]
        assert remaining == []

        core = runtime.core_worker
        snapshot = core.owner_table.snapshot(ref.object_id)
        descriptor = core._stored_descriptors[ref.object_id]
        assert snapshot.state is ObjectState.READY_STORED
        assert snapshot.inline_data is None
        assert snapshot.locations == frozenset({context.node_id})
        assert descriptor.storage is protocol.ResultStorage.OBJECT_STORE
        assert descriptor.inline_data is None

        worker_pid, value = ray.get(ref, timeout=_remaining(deadline))
        assert worker_pid == context.worker_pid
        assert value == b"L" * _PAYLOAD_BYTES
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
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

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert len({os.getpid(), context.node_pid, context.worker_pid}) == 3
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.finalized and report.shutdown_ack_clean
    assert report.resources_clean and not report.forced
