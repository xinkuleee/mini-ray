"""One bounded real-process explicit SYSTEM_ERROR retry smoke.

Three children: one GCS, one one-CPU Node and one Worker, with one 1 MiB store.
The original Worker fail-once hook completes attempt 0 with SYSTEM_ERROR before
user-code decode; the same one submitted Task executes its tiny callable only
on attempt 1. There is no test-owned thread/listener, sleep or extra failure.

Work shares ten seconds after init and public reference close has three
seconds in finally. At most sixteen lease/Push observations are retained; an
overflow remains passive and fails only in the main test thread. Startup and
shutdown still require this exact node ID's external 30-second process-tree
runner. Every tracked PID and all five endpoints, including Driver owner and
trace, are checked even after work or reference-close failure.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.node import REQUEST_LEASE_HANDLER
from miniray.worker import PUSH_TASK_HANDLER, WorkerFailpointConfig
from tests.integration.test_task_path import _close_reference, _pid_exists

pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 16


@ray.remote(max_retries=1)
def run_after_retry() -> int:
    return os.getpid()


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("explicit SYSTEM_ERROR retry exceeded its deadline")
    return remaining


def test_explicit_worker_system_error_retries_once() -> None:
    context = core = ref = report = None
    original_rpc = original_push = None
    calls = []
    overflow = False
    close_errors = []
    lock = threading.Lock()
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=1, num_cpus=1, num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            _test_worker_failpoint=WorkerFailpointConfig(),
            enable_tracing=True,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, context.node_pid, context.worker_pid))
        managed_addresses.update((context.gcs_address, context.node_address, context.worker_address))
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3 and len(managed_addresses) == 5
        assert os.getpid() not in managed_pids
        original_rpc, original_push = core._rpc, core._push_task_rpc

        def inspect_rpc(address, handler, message):
            nonlocal overflow
            reply = original_rpc(address, handler, message)
            if handler == REQUEST_LEASE_HANDLER:
                with lock:
                    if len(calls) < _MAX_OBSERVATIONS:
                        calls.append((handler, message, reply))
                    else:
                        overflow = True
            return reply

        def inspect_push(address, handler, message):
            nonlocal overflow
            reply = original_push(address, handler, message)
            if handler == PUSH_TASK_HANDLER:
                with lock:
                    if len(calls) < _MAX_OBSERVATIONS:
                        calls.append((handler, message, reply))
                    else:
                        overflow = True
            # Observation never changes real RPC replies or retry deadlines.
            return reply

        core._rpc = inspect_rpc
        core._push_task_rpc = inspect_push
        _remaining(deadline)
        ref = run_after_retry.remote()
        assert ray.get(ref, timeout=_remaining(deadline)) == context.worker_pid
        # READY precedes publication retirement. Wait for the existing Core
        # finish notification, not shutdown repairing a still-open attempt.
        with core._completion:
            while (ref.object_id in core._task_finish_barriers
                   or core._protocol_unresolved or core._accepted_task_count):
                core._completion.wait(_remaining(deadline))
            assert ref.object_id.task_id in core._finished_tasks
        snapshot = core.owner_table.snapshot(ref.object_id)
        assert snapshot.current_attempt.attempt_number == 1
        with lock:
            assert not overflow
            observed = tuple(calls)
        leases = [m for h, m, _ in observed if h == REQUEST_LEASE_HANDLER]
        pushes = [(m, r) for h, m, r in observed if h == PUSH_TASK_HANDLER]
        assert [m.attempt_id.attempt_number for m in leases] == [0, 1]
        assert leases[0].task_id == leases[1].task_id == ref.object_id.task_id
        assert leases[0].lease_id != leases[1].lease_id
        assert [r.status for _, r in pushes] == [
            protocol.TaskReplyStatus.SYSTEM_ERROR,
            protocol.TaskReplyStatus.SUCCEEDED,
        ]
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
                if core is not None and original_rpc is not None:
                    core._rpc, core._push_task_rpc = original_rpc, original_push
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

    assert context is not None and report is not None
    assert not ray.is_initialized() and not overflow
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
