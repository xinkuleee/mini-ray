"""Bounded golden trace for one ordinary task across runtime processes.

Task completion is synchronized by ``ray.get``. Trace delivery is observational
and asynchronous, so polling stops at a semantic condition or monotonic deadline.
Each exact case starts three children and one 1 MiB store, submits one tiny
Task, and shares ten seconds for work plus three seconds for its real reference
finalizer before unconditional shutdown. Run alone via the 30-second runner.
"""

from __future__ import annotations

from collections import defaultdict
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.trace_contract import (
    APPLICATION_ERROR_TRACE_CONTRACT,
    SUCCESS_TRACE_CONTRACT,
    load_trace_contract,
)
from tests.integration.test_task_path import _close_reference, _remaining

pytestmark = pytest.mark.multiprocess_smoke

_TRACE_DELIVERY_TIMEOUT_SECONDS = 2.0
_TRACE_POLL_SECONDS = 0.01
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_TASK_EVENT_PREFIXES = ("task_", "lease_")
_SUCCESS_CONTRACT = load_trace_contract(SUCCESS_TRACE_CONTRACT)
_APPLICATION_ERROR_CONTRACT = load_trace_contract(
    APPLICATION_ERROR_TRACE_CONTRACT
)


@ray.remote
def traced_identity(value: int) -> int:
    return value


@ray.remote(max_retries=3)
def traced_application_failure() -> None:
    raise ValueError("intentional application trace failure")


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _assert_runtime_exited(
    managed_pids: set[int], managed_addresses: set[tuple[str, int]]
) -> None:
    """Check recorded process/endpoint hygiene even if the task assertion fails."""

    live_pids = tuple(sorted(pid for pid in managed_pids if _pid_exists(pid)))
    active_pids = tuple(
        sorted(child.pid for child in mp.active_children() if child.pid in managed_pids)
    )
    open_addresses = []
    for address in sorted(managed_addresses):
        try:
            with socket.create_connection(address, timeout=0.1):
                open_addresses.append(address)
        except OSError:
            pass
    initialized = ray.is_initialized()
    assert not live_pids, live_pids
    assert not active_pids, active_pids
    assert not open_addresses, open_addresses
    assert not initialized


def _wait_for_golden_trace(task_id: str, work_deadline: float) -> tuple[object, ...]:
    deadline = min(work_deadline, time.monotonic() + _TRACE_DELIVERY_TIMEOUT_SECONDS)
    wake = threading.Event()
    while True:
        records = tuple(ray.trace())
        if _SUCCESS_CONTRACT.match(records, task_id=task_id).ok:
            return records
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return records
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))


def _records_for_task(
    records: tuple[object, ...], task_id: str
) -> tuple[object, ...]:
    return tuple(
        record
        for record in records
        if dict(record.fields).get("task_id") == task_id
    )


def _application_error_semantics(
    records: tuple[object, ...], task_id: str
) -> bool:
    task_records = _records_for_task(records, task_id)
    core_failure = tuple(
        record
        for record in task_records
        if record.component == "core_worker"
        and record.event == "task_failed"
        and dict(record.fields).get("status") == "APPLICATION_ERROR"
        and dict(record.fields).get("failure_kind") == "APPLICATION"
    )
    return (
        len(core_failure) == 1
        and any(
            record.component == "worker"
            and record.event == "task_application_failed"
            for record in task_records
        )
        and _APPLICATION_ERROR_CONTRACT.match(
            records, task_id=task_id
        ).ok
    )


def _physical_attempt_ids(
    records: tuple[object, ...], task_id: str
) -> set[str]:
    task_records = _records_for_task(records, task_id)
    return {
        dict(record.fields)["attempt_id"]
        for record in task_records
        if "attempt_id" in dict(record.fields)
    }


def _wait_for_application_error_trace(task_id: str, work_deadline: float) -> tuple[object, ...]:
    deadline = min(work_deadline, time.monotonic() + _TRACE_DELIVERY_TIMEOUT_SECONDS)
    wake = threading.Event()
    while True:
        records = tuple(ray.trace())
        if _application_error_semantics(records, task_id):
            return records
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return records
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))


def test_one_task_emits_cross_process_golden_trace_and_cleans_up() -> None:
    context = None
    report = None
    ref = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           object_store_bytes=1024 * 1024, enable_tracing=True)
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        managed_pids.update({context.gcs_pid, context.node_pid, context.worker_pid})
        managed_addresses.update(
            {context.gcs_address, context.node_address, context.worker_address}
        )
        assert context.trace_address is not None and runtime.owner_service is not None
        managed_addresses.update((context.trace_address, runtime.owner_service.address))
        assert len(managed_pids) == 3 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 5

        ref = traced_identity.remote(42)
        assert ray.get(ref, timeout=_remaining(deadline)) == 42
        task_id = str(ref.object_id.task_id)
        records = _wait_for_golden_trace(task_id, deadline)
        golden = _SUCCESS_CONTRACT.match(records, task_id=task_id)
        assert golden.ok, golden.explain()
        publication_requests = tuple(
            golden.rpc_matches[key][0]
            for key in (
                "intent_rpc", "arm_rpc", "terminal_rpc", "adopted_rpc"
            )
        )
        assert all(record.event == "rpc_request_sent" for record in publication_requests)
        assert len({record.event_id for record in publication_requests}) == 4
        assert len({dict(record.fields)["rpc_id"] for record in publication_requests}) == 4
        task_records = _records_for_task(records, task_id)
        dependency_ready = tuple(
            record for record in task_records
            if record.component == "core_worker"
            and record.event == "dependency_ready"
        )
        object_ready = tuple(
            record for record in task_records
            if record.component == "core_worker"
            and record.event == "object_ready"
        )
        assert len(dependency_ready) == 1
        assert dict(dependency_ready[0].fields)["attempt_id"] == (
            "{}:0".format(task_id)
        )
        assert dict(dependency_ready[0].fields)["dependency_count"] == "0"
        assert len(object_ready) == 1
        ready_fields = dict(object_ready[0].fields)
        assert ready_fields["attempt_id"] == "{}:0".format(task_id)
        assert ready_fields["object_id"] == str(ref.object_id)
        assert ready_fields["return_index"] == "0"
        assert ready_fields["storage"] == "INLINE"
        finished = next(
            record for record in task_records
            if record.component == "core_worker"
            and record.event == "task_finished"
        )
        assert finished.process_sequence < object_ready[0].process_sequence
        # The owner's first READY/wake is earlier than adoption and payload
        # retirement, not the later object_ready completion-tail observation.
        owner_publication_tail = (
            golden.event_matches["owner_ready"][0],
            golden.rpc_matches["adopted_rpc"][0],
            golden.event_matches["core_adopted_ack"][0],
            golden.rpc_matches["retire_rpc"][0],
            golden.event_matches["payload_retired"][0],
        )
        assert all(
            record.process_id == finished.process_id
            for record in owner_publication_tail
        )
        assert all(
            previous.process_sequence < current.process_sequence
            for previous, current in zip(
                owner_publication_tail, owner_publication_tail[1:]
            )
        )

        assert not any(
            record.component == "gcs"
            and record.event.startswith(_TASK_EVENT_PREFIXES)
            for record in records
        )
        sequences: dict[str, list[int]] = defaultdict(list)
        for record in records:
            sequences[record.process_id].append(record.process_sequence)
        assert sequences
        assert all(
            sequence == sorted(sequence)
            and len(sequence) == len(set(sequence))
            and all(
                current > previous
                for previous, current in zip(sequence, sequence[1:])
            )
            for sequence in sequences.values()
        )
    finally:
        try:
            _close_reference(ref, time.monotonic() + _CLEANUP_SECONDS)
        except Exception as exc:
            close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                _assert_runtime_exited(managed_pids, managed_addresses)

    assert not close_errors and context is not None
    assert report is not None
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes + report.worker_exitcodes)
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced


def test_application_error_trace_is_terminal_without_system_retry() -> None:
    """One user exception is one attempt, not a transport/system retry."""

    context = None
    report = None
    ref = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           object_store_bytes=1024 * 1024, enable_tracing=True)
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        managed_pids.update(
            {context.gcs_pid, context.node_pid, context.worker_pid}
        )
        managed_addresses.update(
            {context.gcs_address, context.node_address, context.worker_address}
        )
        assert context.trace_address is not None and runtime.owner_service is not None
        managed_addresses.update((context.trace_address, runtime.owner_service.address))
        assert len(managed_pids) == 3 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 5

        ref = traced_application_failure.remote()
        with pytest.raises(ray.TaskError) as caught:
            ray.get(ref, timeout=_remaining(deadline))
        assert caught.value.remote_type == "ValueError"
        assert (
            caught.value.remote_message
            == "intentional application trace failure"
        )

        core = _get_runtime().core_worker
        owner_snapshot = core.owner_table.snapshot(ref.object_id)
        recovery = core._recovery.task_record(ref.object_id.task_id)
        assert owner_snapshot.state is ObjectState.ERROR
        assert isinstance(owner_snapshot.error, ray.TaskError)
        assert owner_snapshot.current_attempt.attempt_number == 0
        assert recovery.state is TaskState.APPLICATION_FAILED
        assert recovery.retries_started == 0
        assert recovery.retries_remaining == 3

        task_id = str(ref.object_id.task_id)
        records = _wait_for_application_error_trace(task_id, deadline)
        task_records = _records_for_task(records, task_id)
        assert _application_error_semantics(records, task_id), [
            (record.process_id, record.component, record.event, record.fields)
            for record in records
        ]

        attempts = _physical_attempt_ids(records, task_id)
        assert attempts == {"{}:0".format(task_id)}
        assert sum(
            record.component == "core_worker"
            and record.event == "lease_requested"
            for record in task_records
        ) == 1
        assert sum(
            record.component == "core_worker"
            and record.event == "task_pushed"
            for record in task_records
        ) == 1
        assert not any(
            record.event in {"task_retried", "task_system_failed"}
            for record in task_records
        )

        # APPLICATION_ERROR is a successful transport reply carrying a
        # terminal task status, not a failed push_task RPC.
        push_handlers = tuple(
            record
            for record in records
            if record.component == "worker"
            and record.event == "rpc_handler_finished"
            and dict(record.fields).get("handler") == "push_task"
        )
        assert len(push_handlers) == 1
        by_id = {record.event_id: record for record in records}
        push_replies = tuple(
            record
            for record in records
            if record.component == "core_worker"
            and record.event == "rpc_reply_received"
            and dict(record.fields).get("handler") == "push_task"
        )
        assert len(push_replies) == 1
        server_send = by_id.get(push_replies[0].cause_event_id)
        assert server_send is not None
        assert server_send.event == "rpc_reply_sent"
        assert dict(server_send.fields).get("ok") == "true"
        assert (
            dict(server_send.fields).get("rpc_id")
            == dict(push_replies[0].fields).get("rpc_id")
        )
    finally:
        try:
            _close_reference(ref, time.monotonic() + _CLEANUP_SECONDS)
        except Exception as exc:
            close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                _assert_runtime_exited(managed_pids, managed_addresses)

    assert not close_errors and context is not None
    assert report is not None
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes + report.worker_exitcodes)
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
