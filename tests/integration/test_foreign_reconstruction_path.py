"""Bounded foreign-owner lineage reconstruction smoke.

One ordinary Worker returns a still-pending child ObjectRef to the Driver.  A
second Worker publishes the child's stored result. An owner-local control task
then drops its sole replica through the same embedded Core. The Driver's
still-active borrower must request reconstruction from that Worker owner, keep
the same logical ObjectID and borrower token, and fetch attempt 1.

Run only this exact node ID through ``scripts/run_baseline.py --smoke EXACT``.  Static
bounds are one GCS, two NodeManagers, one ordinary Worker each, three logical
Tasks/four physical executions, two 1 MiB stores and one 64 KiB result per
producer attempt. Gets, state observations and the owner-local Node drop
share a fifteen-second work budget. Owner-reference handoffs retain their
own finite internal retries, so this is not a strict whole-call time bound.
Observations are passive and capped at 64 per list. Public closes
share three seconds in finally before unconditional shutdown and five-PID/
six-endpoint checks. No Actor, tracing, listener, test thread or sleep is added.
Startup/shutdown keep their existing contracts under the 30-second tree runner.
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
import miniray.core as core_module
from miniray.api import _get_runtime
from miniray.core import ObjectRef
from miniray.ids import AttemptID, ObjectID, WorkerID
from miniray.ownership import ObjectState
from miniray.runtime_binding import current_core_worker


pytestmark = pytest.mark.multiprocess_smoke

_OWNER = "foreign_reconstruction_owner"
_PRODUCER = "foreign_reconstruction_producer"
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 64
_PAYLOAD_BYTES = 64 * 1024
_PAYLOAD_BYTE = b"F"


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("foreign reconstruction exceeded its work deadline")
    return remaining


def _close_reference(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


@ray.remote(num_cpus=1, resources={_PRODUCER: 1}, max_retries=1)
def _foreign_reconstructible_child(value: int) -> tuple[object, ...]:
    return (
        "foreign-reconstructed",
        value + 1,
        os.getpid(),
        _PAYLOAD_BYTE * _PAYLOAD_BYTES,
    )


@ray.remote(num_cpus=0, resources={_OWNER: 1})
def _return_foreign_child_ref(value: int) -> object:
    return _foreign_reconstructible_child.remote(value)


@ray.remote(num_cpus=0, resources={_OWNER: 1})
def _drop_owned_child_replica(
    object_id: ObjectID, owner_worker_id: WorkerID, deadline: float,
) -> tuple[object, ...]:
    """Drop bytes only through the actual Worker owner's embedded Core."""

    core = current_core_worker()
    if core is None or core.worker_id != owner_worker_id:
        raise RuntimeError("control task did not run at the object owner")
    before = core.owner_table.snapshot(object_id)
    if before.state is not ObjectState.READY_STORED:
        raise RuntimeError("foreign child was not stored before the drop")
    if before.current_attempt is None:
        raise RuntimeError("foreign child has no producer attempt")
    local_ref = ObjectRef(object_id, owner_worker_id, core.owner_address)
    _remaining(deadline)
    rpc_deadline = min(deadline, time.monotonic() + core_module._RPC_TOTAL_TIMEOUT_SECONDS)
    parent_deadline = core_module._RPC_CALL_DEADLINE.get()
    if parent_deadline is not None:
        rpc_deadline = min(rpc_deadline, parent_deadline)
    token = core_module._RPC_CALL_DEADLINE.set(rpc_deadline)
    try:
        dropped = core.drop_object(local_ref)
    finally:
        core_module._RPC_CALL_DEADLINE.reset(token)
        # This temporary logical view has no registered local token; it does
        # not replace the existing contained/borrower lifetime of the child.
        local_ref.close(timeout=0)
    _remaining(deadline)
    after = core.owner_table.snapshot(object_id)
    return (
        os.getpid(),
        dropped,
        before.current_attempt,
        after.current_attempt,
        after.state.value,
        tuple(after.locations),
    )


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_driver_reconstructs_worker_owned_stored_object_through_owner() -> None:
    context = None
    report = None
    outer = foreign = drop_probe = None
    core = None
    original_transport_rpc = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    reconstruction_exchanges: list[tuple[object, object, object]] = []
    owner_reads: list[object] = []
    observation_lock = threading.Lock()
    observation_overflow = threading.Event()
    cleanup_errors = []
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _OWNER: 1},
                {"CPU": 1, _PRODUCER: 1},
            ),
            num_workers_per_node=1,
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        # Track every published handle before validating the expected shape,
        # so a structure assertion cannot hide a live child or endpoint.
        managed_pids.update(
            (context.gcs_pid, *context.node_pids, *context.worker_pids)
        )
        managed_addresses.update(
            (context.gcs_address, *context.node_addresses, *context.worker_addresses)
        )
        if context.trace_address is not None:
            managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        core = runtime.core_worker
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        owner_node, producer_node = context.nodes
        assert len(owner_node.worker_ids) == len(producer_node.worker_ids) == 1
        assert context.trace_address is None
        assert runtime.owner_service is not None
        assert len(managed_pids) == 5 and len(managed_addresses) == 6
        assert os.getpid() not in managed_pids

        outer = _return_foreign_child_ref.remote(41)
        foreign = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(foreign, ray.ObjectRef)
        assert foreign.owner_worker_id == owner_node.worker_id
        assert foreign.owner_address == owner_node.worker_address
        assert foreign.borrower_token is not None
        logical_id = foreign.object_id
        borrower_token = foreign.borrower_token

        first_value = ray.get(foreign, timeout=_remaining(deadline))
        assert first_value[:2] == ("foreign-reconstructed", 42)
        assert first_value[3] == _PAYLOAD_BYTE * _PAYLOAD_BYTES
        producer_pid = first_value[2]
        assert producer_pid == producer_node.worker_pid

        drop_probe = _drop_owned_child_replica.remote(
            logical_id, foreign.owner_worker_id, deadline
        )
        (
            owner_pid, dropped, before_attempt, after_attempt,
            after_state, locations,
        ) = ray.get(drop_probe, timeout=_remaining(deadline))
        assert owner_pid == owner_node.worker_pid
        assert owner_pid != producer_pid
        assert dropped
        assert before_attempt == after_attempt == AttemptID(
            logical_id.task_id, 0
        )
        assert after_state == ObjectState.LOST.value
        assert locations == ()

        original_transport_rpc = core_module.rpc_request

        def inspect_owner_rpc(
            address: object, handler: str, request: object, **kwargs: object
        ) -> object:
            reply = original_transport_rpc(
                address, handler, request, **kwargs
            )
            if handler == "get_owned_object":
                with observation_lock:
                    if len(owner_reads) < _MAX_OBSERVATIONS:
                        owner_reads.append(reply)
                    else:
                        observation_overflow.set()
            elif handler == "request_owned_object_reconstruction":
                with observation_lock:
                    if len(reconstruction_exchanges) < _MAX_OBSERVATIONS:
                        reconstruction_exchanges.append((address, request, reply))
                    else:
                        observation_overflow.set()
            return reply

        core_module.rpc_request = inspect_owner_rpc

        second_value = ray.get(foreign, timeout=_remaining(deadline))
        assert second_value[:2] == ("foreign-reconstructed", 42)
        assert second_value[2] == producer_pid
        assert second_value[3] == _PAYLOAD_BYTE * _PAYLOAD_BYTES
        assert foreign.object_id == logical_id
        assert foreign.borrower_token == borrower_token

        with observation_lock:
            observed_reads = tuple(owner_reads)
            observed_reconstructions = tuple(reconstruction_exchanges)
        assert not observation_overflow.is_set()
        assert all(isinstance(reply, protocol.GetOwnedObjectReply) for reply in observed_reads)
        lost_reads = [
            reply for reply in observed_reads
            if reply.state is protocol.OwnedObjectState.LOST
        ]
        assert len(lost_reads) == 1
        assert lost_reads[0].current_attempt == AttemptID(logical_id.task_id, 0)
        assert len(observed_reconstructions) == 1
        address, request, reply = observed_reconstructions[0]
        assert address == foreign.owner_address
        assert isinstance(request, protocol.RequestOwnedObjectReconstruction)
        assert isinstance(
            reply, protocol.RequestOwnedObjectReconstructionReply
        )
        assert request.object_id == reply.object_id == logical_id
        assert request.owner_worker_id == reply.owner_worker_id == (
            foreign.owner_worker_id
        )
        assert request.requester_worker_id == reply.requester_worker_id == (
            core.worker_id
        )
        assert request.borrower_token == reply.borrower_token == borrower_token
        assert request.source == reply.source
        assert request.expected_owner_attempt == (
            reply.expected_owner_attempt
        ) == AttemptID(logical_id.task_id, 0)
        assert reply.disposition is (
            protocol.OwnedObjectReconstructionDisposition.STARTED
        )
        assert reply.reconstruction_attempt == AttemptID(logical_id.task_id, 1)
        assert reply.failure is None and reply.detail is None
        ready_replies = [
            reply for reply in observed_reads
            if reply.state is protocol.OwnedObjectState.READY_STORED
        ]
        assert ready_replies
        reconstructed = ready_replies[-1]
        assert reconstructed.current_attempt == AttemptID(
            logical_id.task_id, 1
        )
        assert reconstructed.descriptor is not None
        assert reconstructed.descriptor.producer_attempt_id == AttemptID(
            logical_id.task_id, 1
        )

        close_deadline = min(deadline, time.monotonic() + _CLEANUP_SECONDS)
        _close_reference(foreign, close_deadline)
        _close_reference(outer, close_deadline)
        _close_reference(drop_probe, close_deadline)
        assert foreign.closed and outer.closed and drop_probe.closed
        _remaining(deadline)
    finally:
        close_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if original_transport_rpc is not None:
                core_module.rpc_request = original_transport_rpc
            for ref in (drop_probe, foreign, outer):
                try:
                    _close_reference(ref, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
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
                assert not cleanup_errors, cleanup_errors
                assert not observation_overflow.is_set()
                assert not ray.is_initialized()

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert tuple(report.worker_pids) == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
