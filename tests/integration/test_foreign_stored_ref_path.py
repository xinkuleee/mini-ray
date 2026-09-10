"""Bounded smoke for a foreign stored ObjectRef data path.

After review and registration, run only this exact node ID through
``scripts/run_baseline.py --case EXACT``.  Its fixed bounds are one GCS, one NodeManager,
two ordinary Workers, exactly two tasks, one INLINE outer result containing one
stored child ref, and no Actor/trace or test-owned server.
One 1 MiB store, ten seconds of shared post-init work, at most sixteen observed
owner/fetch replies and a three-second reference-close budget in finally. All
four PIDs and five endpoints, including the Driver owner, are checked even
after a work or close failure. Only the outer process-tree runner bounds
startup/shutdown; reference receipt timeouts do not cancel remote cleanup.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
import miniray.core as core_module
from miniray.api import _get_runtime
from miniray.ownership import ObjectState


pytestmark = pytest.mark.multiprocess_smoke

_GET_TIMEOUT_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 16
_STORED_BYTES = 64 * 1024


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("foreign stored ref exceeded its shared work deadline")
    return remaining


def _close(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


@ray.remote(num_cpus=1)
def foreign_stored_child(value: int) -> tuple[str, int, int, bytes]:
    return "foreign-stored", value + 1, os.getpid(), b"S" * _STORED_BYTES


@ray.remote(num_cpus=1)
def return_nested_stored_child_ref(value: int, deadline: float) -> object:
    child = None
    try:
        _remaining(deadline)
        child = foreign_stored_child.remote(value)
        # Keep this Worker slot occupied until the second Worker has published
        # the large result. Its handle must stay live through output discovery;
        # do not close a successfully returned child before publication.
        observed = ray.get(child, timeout=_remaining(deadline))
        return {
            "parent_pid": os.getpid(),
            "nested": [{"preview": observed[:2], "child": child}],
        }
    except BaseException:
        _close(child, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        raise


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_inline_outer_restores_foreign_ref_then_driver_fetches_stored_bytes_from_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = None
    report = None
    outer = None
    inner = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    expected_worker_pids: tuple[int, ...] = ()
    owner_replies: list[protocol.GetOwnedObjectReply] = []
    object_fetches: list[
        tuple[object, protocol.GetObject, protocol.GetObjectReply]
    ] = []
    observed_lock = threading.Lock()
    observation_overflow = threading.Event()
    close_errors = []
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=2,
            num_workers_per_node=2,
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _GET_TIMEOUT_SECONDS
        node = context.nodes[0]
        expected_worker_pids = node.worker_pids
        assert context.trace_address is None
        assert len(node.worker_ids) == len(node.worker_pids) == 2
        assert len(node.worker_addresses) == 2

        managed_pids.update(
            {context.gcs_pid, node.node_pid, *node.worker_pids}
        )
        managed_addresses.update(
            {context.gcs_address, node.node_address, *node.worker_addresses}
        )
        assert len(managed_pids) == 4
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_addresses) == 5
        assert os.getpid() not in managed_pids

        outer = return_nested_stored_child_ref.remote(41, deadline)
        container = ray.get(outer, timeout=_remaining(deadline))
        assert container["nested"][0]["preview"] == (
            "foreign-stored", 42
        )
        inner = container["nested"][0]["child"]
        assert isinstance(inner, ray.ObjectRef)
        assert inner.owner_worker_id in node.worker_ids
        assert inner.owner_address in node.worker_addresses
        assert inner.borrower_token is not None

        core = _get_runtime().core_worker
        outer_snapshot = core.owner_table.snapshot(outer.object_id)
        assert outer_snapshot.state is ObjectState.READY_INLINE
        assert outer_snapshot.inline_data is not None
        assert len(outer_snapshot.inline_data) <= 1024
        assert outer.object_id not in core._stored_descriptors
        original_transport_rpc = core_module.rpc_request

        def record_transport_rpc(
            address: object, handler: str, request: object, **kwargs: object
        ) -> object:
            reply = original_transport_rpc(
                address, handler, request, **kwargs
            )
            if handler in ("get_owned_object", "get_object"):
                with observed_lock:
                    if len(owner_replies) + len(object_fetches) >= _MAX_OBSERVATIONS:
                        observation_overflow.set()
                    elif handler == "get_owned_object":
                        owner_replies.append(reply)
                    else:
                        object_fetches.append((address, request, reply))
            # A passive observation cannot turn a real ACK into a failure.
            return reply

        monkeypatch.setattr(core_module, "rpc_request", record_transport_rpc)

        # Releasing the INLINE container must not invalidate the independently
        # acknowledged borrower token held by ``inner``.
        _close(outer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert outer.closed
        value = ray.get(inner, timeout=_remaining(deadline))
        assert value[:2] == ("foreign-stored", 42)
        child_pid = value[2]
        assert value[3] == b"S" * _STORED_BYTES
        assert {container["parent_pid"], child_pid} == set(node.worker_pids)

        with observed_lock:
            owners, fetches = tuple(owner_replies), tuple(object_fetches)
        assert not observation_overflow.is_set()
        assert owners and all(type(reply) is protocol.GetOwnedObjectReply for reply in owners)
        stored_reply = owners[-1]
        assert stored_reply.accepted
        assert stored_reply.state is protocol.OwnedObjectState.READY_STORED
        assert stored_reply.data is None and stored_reply.error is None
        descriptor = stored_reply.descriptor
        assert descriptor is not None
        assert descriptor.object_id == inner.object_id
        assert descriptor.owner_worker_id == inner.owner_worker_id
        assert descriptor.producer_attempt_id.task_id == inner.object_id.task_id
        assert descriptor.producer_attempt_id.attempt_number == 0
        assert descriptor.node_id == node.node_id

        assert len(fetches) == 1
        address, request, fetch_reply = fetches[0]
        assert type(request) is protocol.GetObject and type(fetch_reply) is protocol.GetObjectReply
        assert address == node.node_address
        assert request == protocol.GetObject(
            object_id=descriptor.object_id,
            requester_node_id=node.node_id,
            expected_attempt_id=descriptor.producer_attempt_id,
            expected_owner_worker_id=descriptor.owner_worker_id,
            expected_size_bytes=descriptor.size_bytes,
            expected_checksum=descriptor.checksum,
        )
        assert fetch_reply.object_id == descriptor.object_id
        assert fetch_reply.node_id == descriptor.node_id
        assert fetch_reply.producer_attempt_id == descriptor.producer_attempt_id
        assert fetch_reply.owner_worker_id == descriptor.owner_worker_id
        assert fetch_reply.size_bytes == descriptor.size_bytes
        assert fetch_reply.checksum == descriptor.checksum
        assert fetch_reply.data is not None
        assert len(fetch_reply.data) == descriptor.size_bytes
        assert hashlib.sha256(fetch_reply.data).hexdigest() == descriptor.checksum

        _close(inner, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert inner.closed
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for reference in (inner, outer):
                try:
                    _close(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                surviving_pids = tuple(pid for pid in managed_pids if _pid_exists(pid))
                surviving_children = tuple(child.pid for child in mp.active_children() if child.pid in managed_pids)
                open_addresses = []
                for address in managed_addresses:
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            open_addresses.append(address)
                    except OSError:
                        pass
                assert not surviving_pids, surviving_pids
                assert not surviving_children, surviving_children
                assert not open_addresses, open_addresses
                assert not close_errors, close_errors
                assert not observation_overflow.is_set()

    assert context is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert tuple(report.worker_pids) == expected_worker_pids
    assert len(report.worker_exitcodes) == len(report.worker_cleans) == 2
    assert len(report.worker_forced) == 2
    assert not any(report.worker_forced)
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
