"""Bounded Worker-death ownership cleanup across one task retry.

Attempt 0 imports a Driver-owned nested ObjectRef and exits before user code.
The Node reports that exact child exit to GCS, replaces the one Worker slot,
and the same logical task hold authorizes attempt 1 on the fresh Worker.  The
Driver then consumes GCS's ordered death journal to remove the dead attempt's
borrower without shortening the logical hold.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Static
bounds are one GCS, one Node, one live ordinary Worker slot, one dead Worker,
one task with at most two attempts, two tiny objects, one 1 MiB store and one
loopback gate. Three managed children at peak, four lifetime PIDs and at most
six distinct endpoints. Work shares fifteen seconds; all final gate/reference
cleanup shares three seconds before unconditional shutdown. Passive records
are capped at 64 RPCs/four acquisitions; cleanup polls are finite.

The exact test is registered as a reviewed migration for the ordinary Task
exception-boundary fix. Use only its explicit --case selector; it is not part
of the delivery smoke gate.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import struct
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.control import GET_WORKER_STATE_HANDLER
from miniray.core import _RPC_CALL_DEADLINE, _worker_death_reference_id
from miniray.ids import AttemptID
from miniray.node import (
    GET_WORKER_LEASE_OUTCOME_HANDLER,
    REQUEST_LEASE_HANDLER,
)
from miniray.ownership import ObjectCollectionState
from miniray.worker import (
    CRASH_AFTER_NESTED_IMPORT_EXIT_CODE,
    WorkerFailpointConfig,
    WorkerFailpointMode,
)


pytestmark = pytest.mark.multiprocess_smoke

_TIMEOUT_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 64
_MAX_ACQUISITIONS = 4
_PID_FORMAT = "!Q"
_PID_SIZE = struct.calcsize(_PID_FORMAT)
_RELEASE = b"G"


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Worker-death ownership exceeded its work deadline")
    return remaining


def _close_reference(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert size == _PID_SIZE
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(min(_TIMEOUT_SECONDS, _remaining(deadline)))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("replacement Worker closed its gate")
        payload.extend(chunk)
    return bytes(payload)


@ray.remote(num_cpus=1, max_retries=1)
def _consume_after_nested_import_crash(
    container: object, gate_address: tuple[str, int], deadline: float
) -> tuple[int, object]:
    # Attempt 0 must exit before reaching even this first user-code statement.
    worker_pid = os.getpid()
    with socket.create_connection(
        gate_address, timeout=min(_TIMEOUT_SECONDS, _remaining(deadline))
    ) as connection:
        connection.settimeout(min(_TIMEOUT_SECONDS, _remaining(deadline)))
        connection.sendall(struct.pack(_PID_FORMAT, worker_pid))
        connection.settimeout(min(_TIMEOUT_SECONDS, _remaining(deadline)))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the replacement Worker gate")
    nested = container["nested"]  # type: ignore[index]
    return worker_pid, ray.get(nested, timeout=_remaining(deadline))


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _await_source_terminal_cleanup(core: object, object_id: object, deadline: float) -> object:
    wake = threading.Event()
    for _ in range(1024):
        snapshot = core.owner_table.snapshot(object_id)
        if not snapshot.submitted_tokens and not snapshot.borrowed_tokens:
            return snapshot
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return snapshot
        wake.wait(min(0.01, remaining))
    return snapshot


def _await_source_collection(core: object, object_id: object, deadline: float) -> ObjectCollectionState:
    wake = threading.Event()
    for _ in range(1024):
        state = core.owner_table.collection_state(object_id)
        if state is ObjectCollectionState.COLLECTED:
            return state
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return state
        wake.wait(min(0.01, remaining))
    return state


def test_dead_attempt_borrower_is_swept_while_logical_hold_spans_retry() -> None:
    listener = None
    context = None
    source = result = None
    replacement_connection: socket.socket | None = None
    replacement_pid = None
    core = None
    original_acquire = None
    original_rpc = None
    report = None
    observations: list[tuple[str, object, object]] = []
    acquisitions: list[tuple[object, object, object]] = []
    observation_lock = threading.Lock()
    observation_overflow = threading.Event()
    observation_failed = threading.Event()
    cleanup_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
            _test_worker_failpoint=WorkerFailpointConfig(
                mode=WorkerFailpointMode.CRASH_AFTER_NESTED_IMPORT
            ),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        node = context.nodes[0]
        managed_pids.update(
            {context.gcs_pid, node.node_pid, node.worker_pid}
        )
        managed_addresses.update(
            {context.gcs_address, node.node_address, node.worker_address}
        )
        assert context.trace_address is None
        assert len(managed_pids) == 3

        core = _get_runtime().core_worker
        owner_service = _get_runtime().owner_service
        assert owner_service is not None
        managed_addresses.add(owner_service.address)
        assert len(managed_addresses) == 5 and os.getpid() not in managed_pids
        original_rpc = core._rpc

        def inspect_rpc(address: object, handler: str, message: object) -> object:
            reply = original_rpc(address, handler, message)
            if handler in (REQUEST_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER):
                with observation_lock:
                    if len(observations) < _MAX_OBSERVATIONS:
                        observations.append((handler, message, reply))
                    else:
                        observation_overflow.set()
            return reply

        core._rpc = inspect_rpc
        source = ray.put(("worker-death-ownership", 41))
        source_id = source.object_id
        original_acquire = core.owner_table.acquire_exported_reference

        def inspect_acquire(
            object_id: object, source_credential: object, borrower: object
        ) -> bool:
            acquired = original_acquire(
                object_id, source_credential, borrower
            )
            if object_id == source_id and isinstance(
                source_credential, protocol.TaskHoldSource
            ):
                try:
                    snapshot = core.owner_table.snapshot(source_id)
                    with observation_lock:
                        if len(acquisitions) < _MAX_ACQUISITIONS:
                            acquisitions.append((source_credential.hold, borrower, snapshot))
                        else:
                            observation_overflow.set()
                except Exception:
                    # Diagnostic failure must not convert a real Acquire ACK
                    # into an injected transport/ownership failure.
                    observation_failed.set()
            return acquired

        core.owner_table.acquire_exported_reference = inspect_acquire

        result = _consume_after_nested_import_crash.remote(
            {"nested": source}, gate_address, deadline
        )
        result_id = result.object_id
        listener.settimeout(min(_TIMEOUT_SECONDS, _remaining(deadline)))
        replacement_connection, _peer = listener.accept()
        (replacement_pid,) = struct.unpack(
            _PID_FORMAT, _recv_exact(replacement_connection, _PID_SIZE, deadline)
        )
        managed_pids.add(replacement_pid)

        with observation_lock:
            acquired = tuple(acquisitions)
            observed = tuple(observations)
        assert len(acquired) == 2
        first_hold, first_borrower, first_snapshot = acquired[0]
        second_hold, second_borrower, _second_snapshot = acquired[1]
        assert isinstance(first_hold, protocol.TaskReferenceHold)
        assert first_hold == second_hold
        assert first_hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert first_hold.submitting_worker_id == core.worker_id
        assert first_hold.task_id == result_id.task_id
        assert first_hold.origin_attempt_id == AttemptID(
            result_id.task_id, 0
        )
        assert first_hold in first_snapshot.submitted_tokens
        assert first_borrower in first_snapshot.borrowed_tokens

        old_worker_id = first_borrower[0]
        replacement_worker_id = second_borrower[0]
        assert old_worker_id == node.worker_id
        assert replacement_worker_id != old_worker_id
        assert replacement_pid != node.worker_pid
        assert not _pid_exists(node.worker_pid)

        grants = [
            reply
            for handler, _request, reply in observed
            if handler == REQUEST_LEASE_HANDLER
            and isinstance(reply, protocol.GrantWorkerLease)
        ]
        assert len(grants) == 2
        assert [grant.attempt_id.attempt_number for grant in grants] == [0, 1]
        assert grants[0].worker_id == old_worker_id
        assert grants[1].worker_id == replacement_worker_id
        assert grants[0].lease_id != grants[1].lease_id
        managed_addresses.add(grants[1].worker_address)

        outcomes = [
            reply
            for handler, _request, reply in observed
            if handler == GET_WORKER_LEASE_OUTCOME_HANDLER
            and isinstance(reply, protocol.GetWorkerLeaseOutcomeReply)
            and reply.executor_worker_id == old_worker_id
        ]
        assert outcomes
        assert not outcomes[-1].worker_alive
        assert outcomes[-1].state is protocol.LeaseExecutionState.WORKER_LOST

        # A fresh journal barrier installs the exact GCS-authoritative death
        # fact before ownership assertions.  An unreachable endpoint alone is
        # never accepted as proof.
        token = _RPC_CALL_DEADLINE.set(min(deadline, time.monotonic() + 1.0))
        try:
            assert core._sync_worker_deaths()
        finally:
            _RPC_CALL_DEADLINE.reset(token)
        death_tombstone = core.owner_table.dead_worker_record(old_worker_id)
        assert death_tombstone is not None
        token = _RPC_CALL_DEADLINE.set(min(deadline, time.monotonic() + 1.0))
        try:
            state_reply = original_rpc(
                context.gcs_address, GET_WORKER_STATE_HANDLER,
                protocol.GetWorkerState(old_worker_id),
            )
        finally:
            _RPC_CALL_DEADLINE.reset(token)
        assert isinstance(state_reply, protocol.GetWorkerStateReply)
        assert state_reply.found
        assert state_reply.state is protocol.WorkerMembershipState.DEAD
        assert state_reply.death is not None
        assert state_reply.death.reason is protocol.WorkerDeathReason.PROCESS_EXIT
        assert state_reply.death.exit_code == CRASH_AFTER_NESTED_IMPORT_EXIT_CODE
        assert state_reply.death.worker_pid == node.worker_pid
        assert state_reply.death.worker_id == old_worker_id
        assert death_tombstone.death_id == _worker_death_reference_id(
            state_reply.death
        )

        live_snapshot = core.owner_table.snapshot(source_id)
        assert first_hold in live_snapshot.submitted_tokens
        assert first_borrower not in live_snapshot.borrowed_tokens
        assert first_borrower in live_snapshot.released_borrowed_tokens
        assert second_borrower in live_snapshot.borrowed_tokens
        pending_result = core.owner_table.snapshot(result_id)
        assert pending_result.current_attempt is not None
        assert pending_result.current_attempt.attempt_number == 1

        # The source's local reference remains independently usable while the
        # replacement attempt owns the task-scoped borrower.
        assert ray.get(source, timeout=_remaining(deadline)) == ("worker-death-ownership", 41)
        replacement_connection.settimeout(min(_TIMEOUT_SECONDS, _remaining(deadline)))
        replacement_connection.sendall(_RELEASE)
        worker_pid, value = ray.get(result, timeout=_remaining(deadline))
        assert worker_pid == replacement_pid
        assert value == ("worker-death-ownership", 41)

        terminal_source = _await_source_terminal_cleanup(core, source_id, deadline)
        assert first_hold not in terminal_source.submitted_tokens
        assert terminal_source.borrowed_tokens == frozenset()
        assert first_borrower in terminal_source.released_borrowed_tokens
        assert second_borrower in terminal_source.released_borrowed_tokens
        assert ray.get(source, timeout=_remaining(deadline)) == ("worker-death-ownership", 41)

        _close_reference(source, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert source.closed
        # The consumer task's producer lineage is task-scoped and remains live
        # while its result handle is in scope.  Closing the source therefore
        # removes only the local token; it must not shorten reconstructibility.
        source_after_close = core.owner_table.snapshot(source_id)
        assert source_after_close.local_tokens == frozenset()
        assert source_after_close.lineage_tokens
        assert (
            core.owner_table.collection_state(source_id)
            is ObjectCollectionState.ACTIVE
        )

        _close_reference(result, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert result.closed
        assert (
            _await_source_collection(core, result_id, deadline)
            is ObjectCollectionState.COLLECTED
        )
        assert (
            _await_source_collection(core, source_id, deadline)
            is ObjectCollectionState.COLLECTED
        )
        assert not core.owner_table.contains(source_id)
        assert not core.owner_table.contains(result_id)
        assert len(managed_pids) == 4 and len(managed_addresses) <= 6
        assert not observation_overflow.is_set() and not observation_failed.is_set()
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if replacement_connection is not None:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if replacement_connection.fileno() != -1 and remaining > 0:
                        replacement_connection.settimeout(min(0.1, remaining))
                        replacement_connection.sendall(_RELEASE)
                except OSError:
                    pass
                finally:
                    replacement_connection.close()
        finally:
            try:
                if listener is not None:
                    listener.close()
            finally:
                try:
                    if core is not None and original_acquire is not None:
                        core.owner_table.acquire_exported_reference = original_acquire
                    if core is not None and original_rpc is not None:
                        core._rpc = original_rpc
                    for ref in (result, source):
                        try:
                            _close_reference(ref, cleanup_deadline)
                        except Exception as exc:
                            cleanup_errors.append(exc)
                finally:
                    try:
                        report = ray.shutdown()
                    finally:
                        with observation_lock:
                            final_observations = tuple(observations)
                        for handler, _request, reply in final_observations:
                            if handler == REQUEST_LEASE_HANDLER and isinstance(reply, protocol.GrantWorkerLease):
                                managed_addresses.add(reply.worker_address)
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
                        assert not observation_overflow.is_set() and not observation_failed.is_set()
                        assert listener is None or listener.fileno() == -1
                        assert replacement_connection is None or replacement_connection.fileno() == -1

    assert context is not None
    assert replacement_pid is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.worker_pids == (replacement_pid,)
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
