"""Bounded trace proof of Actor control-plane creation and direct calls.

One Actor is created through Driver Core -> GCS -> NodeManager and executes one
method through Driver Core -> dedicated ActorWorker.  Transport sidecars prove
all three cross-PID request edges without timestamp ordering, while the absence
of ``actor_call`` at GCS proves that the steady-state call bypasses control.

Run only this exact node ID through ``scripts/run_bounded_test.py``.  Bounds are
one GCS, one Node, one ordinary Worker, one dedicated Actor Worker, one Actor,
one tiny call, one trace collector and a 1 MiB Node store. Work shares ten seconds
after init; trace delivery uses at most two seconds of that same budget and the
actual public reference close has a separate three-second deadline. Synchronous
Actor creation/debug calls and startup/shutdown retain their existing contracts
under the runner's 30-second outer bound, not deadline-triggered cancellation.
All observed PIDs/endpoints (four/six on success) are checked after failures;
create before route installation still relies on Node/outer-runner cleanup.
The test starts no helper thread/listener; trace polling uses bounded Event waits.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import debug as ray_debug
from miniray import protocol
from miniray.api import _get_runtime
from tests.integration.test_task_path import _close_reference, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_TRACE_TIMEOUT_SECONDS = 2.0
_TRACE_POLL_SECONDS = 0.01
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_MAX_TRACE_RECORDS = 4096
_HANDLERS = ("create_actor", "reserve_actor_worker", "actor_call")


@ray.remote(num_cpus=1)
class TracedActor:
    def identify(self) -> tuple[str, int]:
        return "direct-actor-call", os.getpid()


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _fields(record: object) -> dict[str, str]:
    return dict(record.fields)


def _edges(
    records: tuple[object, ...], handler: str
) -> tuple[tuple[object, object], ...]:
    by_id = {record.event_id: record for record in records}
    found = []
    for received in records:
        fields = _fields(received)
        if (
            received.event != "rpc_request_received"
            or fields.get("handler") != handler
        ):
            continue
        sent = by_id.get(received.cause_event_id)
        if sent is None:
            continue
        sent_fields = _fields(sent)
        if (
            sent.event == "rpc_request_sent"
            and sent_fields.get("handler") == handler
            and sent_fields.get("rpc_id") == fields.get("rpc_id")
            and sent.process_id != received.process_id
        ):
            found.append((sent, received))
    return tuple(found)


def _descends(records: tuple[object, ...], descendant: object, ancestor: object) -> bool:
    by_id = {record.event_id: record for record in records}
    current = descendant
    seen: set[str] = set()
    while current.cause_event_id is not None:
        cause = current.cause_event_id
        if cause == ancestor.event_id:
            return True
        if cause in seen or cause not in by_id:
            return False
        seen.add(cause)
        current = by_id[cause]
    return False


def _complete_trace(work_deadline: float) -> tuple[object, ...]:
    _remaining(work_deadline)
    deadline = min(work_deadline, time.monotonic() + _TRACE_TIMEOUT_SECONDS)
    wake = threading.Event()
    records = ()
    while True:
        if time.monotonic() >= deadline:
            return records
        records = tuple(ray.trace())
        assert len(records) <= _MAX_TRACE_RECORDS, "one-Actor trace exceeded its reviewed bound"
        if all(len(_edges(records, handler)) == 1 for handler in _HANDLERS):
            create = _edges(records, "create_actor")[0]
            reserve = _edges(records, "reserve_actor_worker")[0]
            if _descends(records, reserve[0], create[1]):
                return records
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return records
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))


def test_actor_creation_uses_control_plane_and_method_call_bypasses_gcs() -> None:
    context = None
    report = None
    actor_pid = None
    actor_address = None
    result_ref = None
    actor_handle = None
    runtime = None
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=1, num_cpus=1, num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=True,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update(
            {context.gcs_pid, context.node_pid, context.worker_pid}
        )
        managed_addresses.update(
            {
                context.gcs_address, context.node_address,
                context.worker_address,
            }
        )
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3 and len(managed_addresses) == 5
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        actor_handle = TracedActor.remote()
        _remaining(deadline)
        actor = ray_debug.snapshot(actor_handle)
        assert isinstance(actor, protocol.ActorSnapshot)
        if actor.worker_pid is not None:
            managed_pids.add(actor.worker_pid)
        if actor.worker_address is not None:
            managed_addresses.add(actor.worker_address)
        _remaining(deadline)
        assert actor.state is protocol.ActorState.ALIVE
        assert actor.node_id == context.node_id
        assert actor.worker_pid is not None and actor.worker_address is not None
        actor_pid = actor.worker_pid
        actor_address = actor.worker_address
        managed_pids.add(actor_pid)
        managed_addresses.add(actor_address)

        _remaining(deadline)
        result_ref = actor_handle.identify.remote()
        assert ray.get(result_ref, timeout=_remaining(deadline)) == (
            "direct-actor-call", actor_pid
        )
        records = _complete_trace(deadline)
        _remaining(deadline)
        edge_by_handler = {
            handler: _edges(records, handler) for handler in _HANDLERS
        }
        assert all(len(edges) == 1 for edges in edge_by_handler.values()), [
            (record.process_id, record.component, record.event, record.fields)
            for record in records
        ]
        create_send, create_receive = edge_by_handler["create_actor"][0]
        reserve_send, reserve_receive = edge_by_handler["reserve_actor_worker"][0]
        call_send, call_receive = edge_by_handler["actor_call"][0]

        assert (create_send.process_id, create_receive.process_id) == (
            str(os.getpid()), str(context.gcs_pid)
        )
        assert (reserve_send.process_id, reserve_receive.process_id) == (
            str(context.gcs_pid), str(context.node_pid)
        )
        assert (call_send.process_id, call_receive.process_id) == (
            str(os.getpid()), str(actor_pid)
        )
        assert create_receive.component == "gcs"
        assert reserve_receive.component == "node"
        assert call_receive.component == "actor_worker"
        assert _descends(records, reserve_send, create_receive)
        assert not any(
            record.component == "gcs"
            and _fields(record).get("handler") == "actor_call"
            for record in records
        )
        create_reply = next(
            record for record in records
            if record.process_id == str(os.getpid())
            and record.component == "core_worker"
            and record.event == "rpc_reply_received"
            and _fields(record).get("handler") == "create_actor"
            and _fields(record).get("rpc_id")
            == _fields(create_send).get("rpc_id")
        )
        assert create_reply.process_sequence < call_send.process_sequence
        assert actor_pid not in context.worker_pids
        assert len(managed_pids) == 4
        assert len(managed_addresses) == 6
        assert os.getpid() not in managed_pids
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if runtime is not None:
                try:
                    # Existing local routes also cover create/debug failure
                    # after installation but before a handle returned. Still-
                    # CREATING entries have no endpoint. Never query GCS here.
                    clients = runtime.core_worker._actor_clients
                    if not clients._lock.acquire(timeout=max(0.0, cleanup_deadline - time.monotonic())):
                        raise TimeoutError("Actor route inspection exceeded cleanup deadline")
                    try:
                        known_actors = tuple(entry.snapshot for entry in clients._entries.values())
                    finally:
                        clients._lock.release()
                    assert len(known_actors) <= 1
                    for known_actor in known_actors:
                        if known_actor.worker_pid is not None:
                            managed_pids.add(known_actor.worker_pid)
                        if known_actor.worker_address is not None:
                            managed_addresses.add(known_actor.worker_address)
                except Exception as exc:
                    close_errors.append(exc)
            try:
                _close_reference(result_ref, cleanup_deadline)
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
    assert actor_pid is not None and actor_address is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
