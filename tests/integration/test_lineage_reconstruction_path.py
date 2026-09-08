"""Bounded smoke for the first lineage reconstruction slice.

Three children, one 1 MiB store and one tiny task executed twice. All gets
share ten seconds; the public physical drop retains its finite RPC deadline.
Reference close shares three seconds and always falls through to shutdown.
Run only its exact node ID through the 30-second process-tree runner.
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


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_stored_task_output_reconstructs_with_same_object_id() -> None:
    context = ref = None
    report = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    @ray.remote(max_retries=1)
    def produce() -> dict[str, str]:
        return {"payload": "large-enough-for-store"}

    try:
        context = ray.init(num_nodes=1, inline_threshold=1, object_store_bytes=1024 * 1024)
        deadline = time.monotonic() + 10.0
        runtime = _get_runtime()
        managed_pids.update((context.gcs_pid, context.node_pid, context.worker_pid))
        assert context.trace_address is not None and runtime.owner_service is not None
        managed_addresses.update((context.gcs_address, context.node_address, context.worker_address,
                                  context.trace_address, runtime.owner_service.address))
        assert len(managed_pids) == 3 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 5
        ref = produce.remote()
        first_id = ref.object_id
        assert ray.get(ref, timeout=_remaining(deadline)) == {
            "payload": "large-enough-for-store"
        }
        _remaining(deadline)
        assert ray.drop_object(ref)
        assert ray.get(ref, timeout=_remaining(deadline)) == {
            "payload": "large-enough-for-store"
        }
        assert ref.object_id == first_id
    finally:
        try:
            _close_reference(ref, time.monotonic() + 3.0)
        except Exception as exc:
            close_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert context is not None
    assert report is not None
    assert not close_errors
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
    assert all(not _pid_exists(pid) for pid in managed_pids)
    assert all(child.pid not in managed_pids for child in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
