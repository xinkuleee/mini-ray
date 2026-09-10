"""Bounded proof that a surviving replica wins before lineage replay.

The producer is forced onto the remote source Node and returns one store-backed
value.  A consumer on the Driver-local target Node forces the normal source-pin,
chunk transfer, target-seal, and owner-location-ACK path.  Only after both replica
locations are authoritative does the test crash the source Node.  The same
ObjectRef must remain READY on the target replica with attempt zero and no retry or
reconstruction.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Static bounds
are one GCS, two NodeManagers, one Worker per Node, two logical tasks, two 1 MiB
object stores, one exact managed Node crash, no tracing or test-owned listener, and
the runner's 30-second process-tree deadline.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
import signal
import socket
import threading
from dataclasses import replace

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.node import GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER
from miniray.ownership import ObjectState
from miniray.recovery import TaskState


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_RESOURCE = "secondary_replica_source"
_TARGET_RESOURCE = "secondary_replica_target"
_PAYLOAD = b"R" * (64 * 1024)
_BOUND_SECONDS = 10.0


@ray.remote(
    num_cpus=1, resources={_SOURCE_RESOURCE: 1}, max_retries=1
)
def _produce_replica_source() -> tuple[int, bytes]:
    return os.getpid(), _PAYLOAD


@ray.remote(num_cpus=1, resources={_TARGET_RESOURCE: 1})
def _consume_on_replica_target(value: tuple[int, bytes]) -> tuple[object, ...]:
    producer_pid, payload = value
    return (
        os.getpid(), producer_pid, len(payload),
        hashlib.sha256(payload).hexdigest(), payload[:4], payload[-4:],
    )


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_dead_primary_promotes_surviving_replica_without_lineage_replay(
) -> None:
    context = runtime = core = None
    source = target = None
    producer_ref = consumer_ref = None
    death = report = None
    original_rpc = None
    calls: list[tuple[tuple[str, int], str, object, object]] = []
    calls_lock = threading.Lock()
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _TARGET_RESOURCE: 1},
                {"CPU": 1, _SOURCE_RESOURCE: 1},
            ),
            num_workers_per_node=1,
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        target, source = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
        original_rpc = core._rpc
        assert runtime.owner_service is not None
        managed_pids.update({
            context.gcs_pid, target.node_pid, target.worker_pid,
            source.node_pid, source.worker_pid,
        })
        managed_addresses.update({
            context.gcs_address, target.node_address, target.worker_address,
            source.node_address, source.worker_address,
            runtime.owner_service.address,
        })
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 6
        assert context.trace_address is None
        assert os.getpid() not in managed_pids

        def inspect_rpc(address, handler, message):
            reply = original_rpc(address, handler, message)
            if handler in (REQUEST_LEASE_HANDLER, GET_OBJECT_HANDLER):
                with calls_lock:
                    calls.append((address, handler, message, reply))
            return reply

        core._rpc = inspect_rpc

        producer_ref = _produce_replica_source.remote()
        consumer_ref = _consume_on_replica_target.remote(producer_ref)
        checksum = hashlib.sha256(_PAYLOAD).hexdigest()
        assert ray.get(consumer_ref, timeout=_BOUND_SECONDS) == (
            target.worker_pid, source.worker_pid, len(_PAYLOAD), checksum,
            _PAYLOAD[:4], _PAYLOAD[-4:],
        )

        object_id = producer_ref.object_id
        before = core.owner_table.snapshot(object_id)
        before_route = core._stored_descriptors[object_id]
        before_recovery = core._recovery.task_record(object_id.task_id)
        assert before.state is ObjectState.READY_STORED
        assert before.current_attempt is not None
        assert before.current_attempt.attempt_number == 0
        assert before.locations == frozenset({source.node_id, target.node_id})
        assert before.canonical_stored_result == before_route
        assert before.canonical_stored_result is not None
        assert before.canonical_stored_result.node_id == source.node_id
        assert before_route.node_id == source.node_id
        assert before_route.owner_worker_id == core.worker_id
        assert before_route.size_bytes >= len(_PAYLOAD)
        assert before_recovery.state is TaskState.SUCCEEDED
        assert before_recovery.current_attempt == before.current_attempt
        assert before_recovery.retries_started == 0
        assert core._recovery.active_recovery(object_id.task_id) is None

        death = _test_crash_node(source.node_id, timeout=_BOUND_SECONDS)
        assert death.node_id == source.node_id
        assert death.node_pid == source.node_pid
        assert death.registration_epoch == runtime.nodes[1].registration_epoch
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert not _pid_exists(source.node_pid)
        assert not _pid_exists(source.worker_pid)

        after = core.owner_table.snapshot(object_id)
        promoted_route = core._stored_descriptors[object_id]
        after_recovery = core._recovery.task_record(object_id.task_id)
        assert after.state is ObjectState.READY_STORED
        assert after.current_attempt == before.current_attempt
        assert after.locations == frozenset({target.node_id})
        assert after.canonical_stored_result == before.canonical_stored_result
        assert promoted_route == replace(
            before_route, node_id=target.node_id
        )
        assert after_recovery.state is TaskState.SUCCEEDED
        assert after_recovery.current_attempt == before.current_attempt
        assert after_recovery.retries_started == 0
        assert core._recovery.active_recovery(object_id.task_id) is None
        with core._state_lock:
            assert core._dead_nodes[source.node_id] == death
            installed = core._installed_cluster_snapshot
        assert tuple(info.node_id for info in installed.nodes) == (target.node_id,)

        with calls_lock:
            producer_leases_before_get = tuple(
                (message, reply)
                for _address, handler, message, reply in calls
                if handler == REQUEST_LEASE_HANDLER
                and isinstance(message, protocol.RequestWorkerLease)
                and message.task_id == object_id.task_id
            )
        assert producer_leases_before_get
        assert {
            request.attempt_id.attempt_number
            for request, _reply in producer_leases_before_get
        } == {0}

        with calls_lock:
            get_call_start = len(calls)
        producer_pid, payload = ray.get(producer_ref, timeout=_BOUND_SECONDS)
        assert producer_pid == source.worker_pid
        assert payload == _PAYLOAD

        with calls_lock:
            observed = tuple(calls)
        fetches = tuple(
            (address, request, reply)
            for address, handler, request, reply in observed[get_call_start:]
            if handler == GET_OBJECT_HANDLER
            and isinstance(request, protocol.GetObject)
            and request.object_id == object_id
        )
        assert len(fetches) == 1
        fetch_address, fetch_request, fetch_reply = fetches[0]
        assert fetch_address == target.node_address
        assert fetch_request.requester_node_id == target.node_id
        assert fetch_request.expected_attempt_id == before.current_attempt
        assert fetch_request.expected_owner_worker_id == core.worker_id
        assert fetch_request.expected_size_bytes == before_route.size_bytes
        assert fetch_request.expected_checksum == before_route.checksum
        assert isinstance(fetch_reply, protocol.GetObjectReply)
        assert fetch_reply.node_id == target.node_id
        assert fetch_reply.producer_attempt_id == before.current_attempt
        assert fetch_reply.owner_worker_id == core.worker_id
        assert fetch_reply.size_bytes == before_route.size_bytes
        assert fetch_reply.checksum == before_route.checksum

        final = core.owner_table.snapshot(object_id)
        final_recovery = core._recovery.task_record(object_id.task_id)
        assert final.state is ObjectState.READY_STORED
        assert final.current_attempt == before.current_attempt
        assert final.locations == frozenset({target.node_id})
        assert final_recovery.state is TaskState.SUCCEEDED
        assert final_recovery.retries_started == 0
        assert core._recovery.active_recovery(object_id.task_id) is None
        producer_leases_after_get = tuple(
            message for _address, handler, message, _reply in observed
            if handler == REQUEST_LEASE_HANDLER
            and isinstance(message, protocol.RequestWorkerLease)
            and message.task_id == object_id.task_id
        )
        assert producer_leases_after_get == tuple(
            request for request, _reply in producer_leases_before_get
        )
    finally:
        for ref in (consumer_ref, producer_ref):
            if ref is not None:
                ref.close()
        if core is not None and original_rpc is not None:
            core._rpc = original_rpc
        report = ray.shutdown()

    assert context is not None and source is not None and target is not None
    assert death is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced
    assert report.node_pids == (target.node_pid, source.node_pid)
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == (True, False)
    assert report.node_forced == (False, False)
    assert report.node_finalized == (True, False)
    assert report.worker_pids == (target.worker_pid, source.worker_pid)
    assert report.worker_exitcodes == (0, None)
    assert report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False)
    assert report.node_deaths[1] == death
    assert not report.node_clean and not report.worker_clean
    assert not report.finalized and not report.shutdown_ack_clean
    assert not report.resources_clean
    assert all(not _pid_exists(pid) for pid in managed_pids)
    assert all(child.pid not in managed_pids for child in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            socket.create_connection(address, timeout=0.1)
