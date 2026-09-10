"""Bounded cross-node foreign STORED task-dependency acceptance test.

One source Worker returns a still-pending Worker-owned ObjectRef.  The Driver
retains it for a consumer pinned to the second node, then closes both the
borrower handle and outer container.  After the producer returns a 64 KiB value,
only byte-free descriptors may cross the Driver: the target Node pins and pulls
the source in chunks, seals it, and returns a target-local grant.  The Driver
must report that replica to the real foreign owner before direct PushTask.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Bounds are
one GCS, two Nodes, one ordinary Worker per Node, three tasks, one test-owned
gate, 1 MiB per ObjectStore, no Actor/trace, and a 30-second runner deadline.
All gate/get work shares fifteen seconds after init; public reference receipts
share three seconds in finally. Observations retain at most 64 records without
changing replies, and failure cleanup checks all five PIDs and seven endpoints
(including the Driver owner and gate). The foreign close receipt is not itself
proof of an owner Release ACK.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
import hashlib
import multiprocessing as mp
import os
import socket
import struct
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
import miniray.core as core_module
from miniray.api import _get_runtime
from miniray.node import REQUEST_LEASE_HANDLER
from miniray.worker import PUSH_TASK_HANDLER


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_PARENT = "foreign_stored_parent"
_SOURCE_CHILD = "foreign_stored_child"
_TARGET = "foreign_stored_target"
_PAYLOAD_BYTES = 64 * 1024
_PAYLOAD_BYTE = b"R"
_GATE_TIMEOUT_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 64
_ARRIVAL_FORMAT = "!Q"
_ARRIVAL_SIZE = struct.calcsize(_ARRIVAL_FORMAT)
_RELEASE = b"G"


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("foreign stored dependency exceeded its work deadline")
    return remaining


def _close_reference(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    assert size == _ARRIVAL_SIZE
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(min(_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed before completing gate message")
        payload.extend(chunk)
    return bytes(payload)


@ray.remote(num_cpus=1, resources={_SOURCE_CHILD: 1})
def _stored_child(gate_address: tuple[str, int], deadline: float) -> tuple[int, bytes]:
    worker_pid = os.getpid()
    with socket.create_connection(
        gate_address, timeout=min(_GATE_TIMEOUT_SECONDS, _remaining(deadline))
    ) as connection:
        connection.settimeout(min(_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        connection.sendall(struct.pack(_ARRIVAL_FORMAT, worker_pid))
        connection.settimeout(min(_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        if connection.recv(1) != _RELEASE:
            raise RuntimeError("Driver closed the stored-child gate")
    return worker_pid, _PAYLOAD_BYTE * _PAYLOAD_BYTES


@ray.remote(num_cpus=0, resources={_SOURCE_PARENT: 1})
def _return_stored_child_ref(gate_address: tuple[str, int], deadline: float) -> object:
    _remaining(deadline)
    return _stored_child.remote(gate_address, deadline)


@ray.remote(num_cpus=1, resources={_TARGET: 1})
def _consume_stored(value: tuple[int, bytes]) -> tuple[object, ...]:
    producer_pid, payload = value
    return (
        os.getpid(), producer_pid, len(payload), hashlib.sha256(payload).hexdigest(),
        payload[:4], payload[-4:],
    )


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _byte_field_sizes(value: object) -> tuple[int, ...]:
    if isinstance(value, bytes):
        return (len(value),)
    if is_dataclass(value) and not isinstance(value, type):
        return tuple(
            size
            for field in fields(value)
            for size in _byte_field_sizes(getattr(value, field.name))
        )
    if isinstance(value, dict):
        return tuple(
            size for item in value.items() for part in item
            for size in _byte_field_sizes(part)
        )
    if isinstance(value, (tuple, list, set, frozenset)):
        return tuple(size for item in value for size in _byte_field_sizes(item))
    return ()


def test_foreign_stored_dependency_pulls_node_to_node_before_push() -> None:
    listener = None
    context = None
    outer = foreign = consumer = None
    child_connection: socket.socket | None = None
    core = None
    original_rpc = original_borrow_rpc = original_push_rpc = None
    original_transport_rpc = None
    observed: list[tuple[str, object, object]] = []
    observed_lock = threading.Lock()
    observation_overflow = threading.Event()
    driver_fetch_observed = threading.Event()
    close_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    report = None

    def remember(name, message, reply):
        with observed_lock:
            if len(observed) < _MAX_OBSERVATIONS:
                observed.append((name, message, reply))
            else:
                observation_overflow.set()

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _SOURCE_PARENT: 1, _SOURCE_CHILD: 1},
                {"CPU": 1, _TARGET: 1},
            ),
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        source, target = context.nodes
        managed_pids.update(
            {context.gcs_pid, source.node_pid, source.worker_pid,
             target.node_pid, target.worker_pid}
        )
        managed_addresses.update(
            {context.gcs_address, source.node_address, source.worker_address,
             target.node_address, target.worker_address}
        )
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5 and len(managed_addresses) == 7
        assert os.getpid() not in managed_pids
        assert context.trace_address is None

        outer = _return_stored_child_ref.remote(gate_address, deadline)
        foreign = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(foreign, ray.ObjectRef)
        assert foreign.owner_worker_id == source.worker_id
        assert foreign.owner_address == source.worker_address
        assert foreign.borrower_token is not None

        listener.settimeout(min(_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        child_connection, _peer = listener.accept()
        (producer_pid,) = struct.unpack(
            _ARRIVAL_FORMAT, _recv_exact(child_connection, _ARRIVAL_SIZE, deadline)
        )
        assert producer_pid == source.worker_pid

        core = _get_runtime().core_worker
        original_rpc = core._rpc
        original_borrow_rpc = core._borrow_rpc
        original_push_rpc = core._push_task_rpc
        original_transport_rpc = core_module.rpc_request

        def observe_driver_transport(address, handler, message, **kwargs):
            if handler == "get_object":
                driver_fetch_observed.set()
            return original_transport_rpc(address, handler, message, **kwargs)

        def inspect_rpc(address, handler, message):
            if handler == REQUEST_LEASE_HANDLER:
                remember(handler + "_send", message, None)
            reply = original_rpc(address, handler, message)
            if handler == REQUEST_LEASE_HANDLER:
                remember(handler + "_reply", message, reply)
            return reply

        def inspect_borrow(address, handler, message):
            if handler in {
                "get_retained_owned_object",
                "report_retained_object_location",
                "release_owned_object_for_task",
            }:
                remember(handler + "_send", message, None)
            reply = original_borrow_rpc(address, handler, message)
            if handler in {
                "get_retained_owned_object",
                "report_retained_object_location",
                "release_owned_object_for_task",
            }:
                remember(handler + "_reply", message, reply)
            return reply

        def inspect_push(address, handler, message):
            remember(handler + "_send", message, None)
            reply = original_push_rpc(address, handler, message)
            assert handler == PUSH_TASK_HANDLER
            remember(handler + "_reply", message, reply)
            return reply

        core._rpc = inspect_rpc
        core._borrow_rpc = inspect_borrow
        core._push_task_rpc = inspect_push
        core_module.rpc_request = observe_driver_transport

        _remaining(deadline)
        consumer = _consume_stored.remote(foreign)
        close_deadline = min(deadline, time.monotonic() + _CLEANUP_SECONDS)
        _close_reference(foreign, close_deadline)
        _close_reference(outer, close_deadline)
        assert foreign.closed and outer.closed
        ready, remaining = ray.wait((consumer,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [consumer]

        child_connection.settimeout(_remaining(deadline))
        child_connection.sendall(_RELEASE)
        result = ray.get(consumer, timeout=_remaining(deadline))
        assert result == (
            target.worker_pid, source.worker_pid, _PAYLOAD_BYTES,
            hashlib.sha256(_PAYLOAD_BYTE * _PAYLOAD_BYTES).hexdigest(),
            _PAYLOAD_BYTE * 4, _PAYLOAD_BYTE * 4,
        )

        with observed_lock:
            calls = tuple(observed)
        task_id = consumer.object_id.task_id
        relevant = tuple(
            call for call in calls
            if (
                call[0].startswith((
                    "get_retained_owned_object_",
                    "report_retained_object_location_",
                    "release_owned_object_for_task_",
                ))
                or (isinstance(call[1], protocol.RequestWorkerLease)
                    and call[1].task_id == task_id)
                or (isinstance(call[1], protocol.PushTask)
                    and call[1].spec.task_id == task_id)
            )
        )
        names = [item[0] for item in relevant]
        report_send_index = names.index("report_retained_object_location_send")
        report_reply_index = names.index("report_retained_object_location_reply")
        push_send_index = names.index(PUSH_TASK_HANDLER + "_send")

        retained_replies = [
            reply for name, _message, reply in relevant
            if name == "get_retained_owned_object_reply"
            and isinstance(reply, protocol.GetRetainedOwnedObjectReply)
            and reply.state is protocol.OwnedObjectState.READY_STORED
        ]
        assert retained_replies
        source_descriptor = retained_replies[-1].descriptor
        assert source_descriptor is not None
        assert source_descriptor.node_id == source.node_id
        assert retained_replies[-1].data is None
        stored_reply_index = max(
            index for index, (name, _message, reply) in enumerate(relevant)
            if name == "get_retained_owned_object_reply"
            and reply is retained_replies[-1]
        )

        location_message = relevant[report_send_index][1]
        location_reply = relevant[report_reply_index][2]
        assert isinstance(location_message, protocol.ReportRetainedObjectLocation)
        assert isinstance(location_reply, protocol.ReportRetainedObjectLocationReply)
        assert location_reply.accepted
        assert location_reply.status is protocol.RetainedLocationReportStatus.ADDED
        assert location_reply.descriptor == location_message.descriptor
        assert location_message.descriptor.node_id == target.node_id
        assert location_message.descriptor.object_id == foreign.object_id

        lease_calls = [
            (message, reply) for name, message, reply in relevant
            if name == REQUEST_LEASE_HANDLER + "_reply"
        ]
        assert len(lease_calls) == 2
        assert isinstance(lease_calls[0][1], protocol.SpillbackWorkerLease)
        target_grant = lease_calls[1][1]
        assert isinstance(target_grant, protocol.GrantWorkerLease)
        assert target_grant.node_id == target.node_id
        assert target_grant.dependencies == (location_message.descriptor,)
        target_lease_reply_index = max(
            index for index, (name, _message, reply) in enumerate(relevant)
            if name == REQUEST_LEASE_HANDLER + "_reply" and reply is target_grant
        )
        assert (
            stored_reply_index < target_lease_reply_index < report_send_index
            < report_reply_index < push_send_index
        )

        push = relevant[push_send_index][1]
        assert isinstance(push, protocol.PushTask)
        assert push.worker_id == target.worker_id
        assert push.spec.args == (
            protocol.RefArg(foreign.object_id, foreign.owner_worker_id),
        )
        assert push.dependencies == target_grant.dependencies
        for _name, message, _reply in relevant:
            assert sum(_byte_field_sizes(message)) < _PAYLOAD_BYTES
        assert not observation_overflow.is_set() and not driver_fetch_observed.is_set()
        _close_reference(consumer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if child_connection is not None:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if remaining > 0:
                        child_connection.settimeout(min(0.1, remaining))
                        child_connection.sendall(_RELEASE)
                except OSError:
                    pass
                finally:
                    child_connection.close()
        finally:
            try:
                if listener is not None:
                    listener.close()
            finally:
                try:
                    if core is not None and original_rpc is not None:
                        core._rpc = original_rpc
                    if core is not None and original_borrow_rpc is not None:
                        core._borrow_rpc = original_borrow_rpc
                    if core is not None and original_push_rpc is not None:
                        core._push_task_rpc = original_push_rpc
                    if original_transport_rpc is not None:
                        core_module.rpc_request = original_transport_rpc
                    for ref in (consumer, foreign, outer):
                        try:
                            _close_reference(ref, cleanup_deadline)
                        except Exception as exc:
                            close_errors.append(exc)
                finally:
                    try:
                        report = ray.shutdown()
                    finally:
                        surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
                        surviving_children = tuple(child.pid for child in mp.active_children() if child.pid in managed_pids)
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
                        assert listener is None or listener.fileno() == -1
                        assert child_connection is None or child_connection.fileno() == -1
                        assert not close_errors, close_errors
                        assert not observation_overflow.is_set()
                        assert not driver_fetch_observed.is_set()

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
