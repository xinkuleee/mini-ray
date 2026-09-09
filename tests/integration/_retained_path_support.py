"""Finite lifecycle helpers for the two retained Stage 1 evidence cases."""

from contextlib import contextmanager
from dataclasses import dataclass, field
import multiprocessing as mp
import os
import socket
import time

import miniray as ray
from miniray.api import _get_runtime


def remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("retained path work or cleanup deadline expired")
    return value


def wait_local(core, predicate, deadline):
    with core._completion:
        for index in range(128):
            if predicate():
                return
            if index < 127:
                core._completion.wait(min(0.1, remaining(deadline)))
    raise TimeoutError("retained path state did not converge")


def close(reference, deadline):
    if reference is None:
        return
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@dataclass
class Case:
    context: object
    core: object
    deadline: float
    refs: list = field(default_factory=list)

    def keep(self, reference):
        self.refs.append(reference)
        return reference


@contextmanager
def cluster(*, workers):
    """One Node, bounded public closes, unconditional shutdown and hygiene."""
    context = case = report = None
    pids, addresses, errors = set(), set(), []
    try:
        context = ray.init(num_nodes=1, num_cpus=workers, num_workers_per_node=workers,
                           inline_threshold=1024, object_store_bytes=1024 * 1024,
                           enable_tracing=False)
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        if runtime.owner_service is not None:
            addresses.add(runtime.owner_service.address)
        case = Case(context, runtime.core_worker, time.monotonic() + 15.0)
        assert runtime.owner_service is not None and context.trace_address is None
        assert len(pids) == workers + 2 and len(addresses) == workers + 3
        assert os.getpid() not in pids
        yield case
    finally:
        deadline = time.monotonic() + 3.0
        try:
            if case is not None:
                keys = []
                for ref in reversed(case.refs):
                    if ref.borrower_token is not None:
                        keys.append((ref.owner_worker_id, ref.object_id, case.core.worker_id, ref.borrower_token))
                    try:
                        close(ref, deadline)
                    except Exception as exc:
                        errors.append(("close", repr(exc)))
                try:
                    wait_local(case.core, lambda: all(key not in case.core._borrowed_release_obligations
                                                      for key in keys), deadline)
                except Exception as exc:
                    errors.append(("borrower release", repr(exc)))
        finally:
            try:
                report = ray.shutdown()
            except Exception as exc:
                errors.append(("shutdown", repr(exc)))
        if report is not None:
            pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
        survivors = tuple(pid for pid in pids if _pid_exists(pid))
        active = tuple(child.pid for child in mp.active_children() if child.pid in pids)
        listening = []
        for address in addresses:
            try:
                with socket.create_connection(address, timeout=0.1):
                    listening.append(address)
            except OSError:
                pass
        assert not survivors and not active and not listening, (survivors, active, listening)
        assert not errors, errors
        assert not ray.is_initialized()
        if context is not None:
            assert report is not None and report.core_stopped
            assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
            assert report.worker_pids == context.worker_pids
            assert report.gcs_clean and report.gcs_exitcode == 0
            assert report.node_clean and report.worker_clean and report.resources_clean
            assert report.finalized and report.shutdown_ack_clean and not report.forced
            assert report.node_exitcodes == (0,) and report.worker_exitcodes == (0,) * workers
