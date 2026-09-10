"""Bounded two-replica physical-GC acceptance test.

A source Worker returns a still-pending Worker-owned ObjectRef.  A consumer on
the second node retains that object, pulls its 64 KiB payload, and therefore
creates one sealed replica on each node.  All public handles are closed before
an owner-local, zero-CPU probe verifies that the source Worker's embedded Core
has completed ``ACTIVE -> COLLECTING -> COLLECTED`` and forgotten metadata, the
canonical descriptor, waiter, durable obligation, and producer lineage.

Run only this exact node ID through ``scripts/run_baseline.py --smoke EXACT``.  Bounds are
one GCS, two Nodes, one ordinary Worker per Node, four tasks, one test-owned
gate, 1 MiB per ObjectStore, one trace collector, and a 30-second runner
deadline. Five child PIDs and eight endpoints include the Driver owner.
All gets/gate/probe/trace/direct RPC observations share fifteen seconds after
init. The owner probe and trace delivery keep their shorter five-/two-second
budgets; observation lists and polls are finite. Final gate/reference cleanup
shares three seconds and always reaches shutdown and PID/endpoint checks.
"""

from __future__ import annotations

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
from miniray.api import _get_runtime
from miniray.ids import ObjectID
from miniray.node import DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER
from miniray.ownership import ObjectCollectionState
from miniray.runtime_binding import current_core_worker
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_PARENT = "stored_gc_parent"
_SOURCE_CHILD = "stored_gc_child"
_SOURCE_PROBE = "stored_gc_probe"
_TARGET_CONSUMER = "stored_gc_consumer"
_PAYLOAD_BYTES = 64 * 1024
_PAYLOAD_BYTE = b"G"
_OBJECT_STORE_BYTES = 1024 * 1024
_GATE_TIMEOUT_SECONDS = 10.0
_PROBE_TIMEOUT_SECONDS = 5.0
_TRACE_DELIVERY_TIMEOUT_SECONDS = 2.0
_TRACE_POLL_SECONDS = 0.01
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 128
_MAX_PROBE_POLLS = 512
_MAX_TRACE_POLLS = 201
_ARRIVAL_FORMAT = "!Q"
_ARRIVAL_SIZE = struct.calcsize(_ARRIVAL_FORMAT)
_RELEASE = b"G"
_COLLECTION_EVENTS = frozenset(
    {
        "object_collection_started",
        "object_replica_collection_acknowledged",
        "object_collection_completed",
    }
)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("stored physical GC exceeded its work deadline")
    return remaining


def _close(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.25, remaining),
        request_timeout=min(1.0, remaining), deadline=deadline,
    )


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
    return _stored_child.remote(gate_address, deadline)


@ray.remote(num_cpus=1, resources={_TARGET_CONSUMER: 1})
def _consume_stored(value: tuple[int, bytes]) -> tuple[object, ...]:
    producer_pid, payload = value
    return (
        os.getpid(),
        producer_pid,
        len(payload),
        hashlib.sha256(payload).hexdigest(),
        payload[:4],
        payload[-4:],
    )


@ray.remote(num_cpus=0, resources={_SOURCE_PROBE: 1})
def _probe_source_owner_collection(object_id: ObjectID, work_deadline: float) -> tuple[object, ...]:
    """Inspect only the Core already embedded in the source Worker."""

    core = current_core_worker()
    if core is None:
        raise RuntimeError("source probe has no Worker-side Core binding")
    deadline = min(work_deadline, time.monotonic() + _PROBE_TIMEOUT_SECONDS)
    wake = threading.Event()
    for _ in range(_MAX_PROBE_POLLS):
        phase = core.owner_table.collection_state(object_id)
        if phase is ObjectCollectionState.COLLECTED:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))

    phase = core.owner_table.collection_state(object_id)
    return (
        os.getpid(),
        phase.value,
        not core.owner_table.contains(object_id),
        object_id not in core._stored_descriptors,
        object_id not in core._objects,
        object_id not in core._object_gc_obligations,
        core._recovery.lineage_for_object(object_id) is None,
    )


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _logical_replica_identity(
    descriptor: protocol.ObjectStoreDescriptor,
) -> tuple[object, ...]:
    """Return fields which remain stable when a pull creates a replica."""

    return (
        descriptor.object_id,
        descriptor.owner_worker_id,
        descriptor.producer_attempt_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )


def _wait_for_collection_trace(
    object_id: ObjectID, source_worker_pid: int, work_deadline: float,
) -> tuple[object, ...]:
    deadline = min(work_deadline, time.monotonic() + _TRACE_DELIVERY_TIMEOUT_SECONDS)
    wake = threading.Event()
    for _ in range(_MAX_TRACE_POLLS):
        records = tuple(
            record
            for record in ray.trace()
            if record.process_id == str(source_worker_pid)
            and record.component == "worker_core"
            and record.event in _COLLECTION_EVENTS
            and dict(record.fields).get("object_id") == str(object_id)
        )
        counts = {
            event: sum(record.event == event for record in records)
            for event in _COLLECTION_EVENTS
        }
        if (
            counts["object_collection_started"] == 1
            and counts["object_replica_collection_acknowledged"] == 2
            and counts["object_collection_completed"] == 1
        ):
            return records
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return records
        wake.wait(min(_TRACE_POLL_SECONDS, remaining))
    return records


def _assert_replica_absent(
    *,
    node: object,
    requester_node_id: object,
    descriptor: protocol.ObjectStoreDescriptor,
    deadline: float,
) -> None:
    reply = _query(
        node.node_address,
        GET_OBJECT_HANDLER,
        protocol.GetObject(
            object_id=descriptor.object_id,
            requester_node_id=requester_node_id,
            expected_attempt_id=descriptor.producer_attempt_id,
            expected_owner_worker_id=descriptor.owner_worker_id,
            expected_size_bytes=descriptor.size_bytes,
            expected_checksum=descriptor.checksum,
        ),
        deadline,
    )
    assert isinstance(reply, protocol.GetObjectReply)
    assert reply.object_id == descriptor.object_id
    assert reply.node_id == node.node_id
    assert not reply.found and not reply.sealed
    assert reply.data is None and reply.checksum is None
    assert reply.producer_attempt_id is None
    assert reply.owner_worker_id is None and reply.size_bytes is None
    assert reply.error == "object is not present in this node's object store"


def _assert_exact_drop_replay(
    *, node: object, descriptor: protocol.ObjectStoreDescriptor, deadline: float,
) -> None:
    request = protocol.DropObjectReplica(
        object_id=descriptor.object_id,
        producer_attempt_id=descriptor.producer_attempt_id,
        owner_worker_id=descriptor.owner_worker_id,
        node_id=node.node_id,
        checksum=descriptor.checksum,
    )
    reply = _query(
        node.node_address, DROP_OBJECT_REPLICA_HANDLER, request, deadline
    )
    assert isinstance(reply, protocol.DropObjectReplicaReply)
    assert (
        reply.object_id,
        reply.producer_attempt_id,
        reply.owner_worker_id,
        reply.node_id,
        reply.checksum,
    ) == (
        request.object_id,
        request.producer_attempt_id,
        request.owner_worker_id,
        request.node_id,
        request.checksum,
    )
    assert reply.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert reply.error is None


def test_foreign_stored_dependency_collects_source_and_target_replicas() -> None:
    listener = None
    context = None
    outer = foreign = consumer = probe = None
    child_connection: socket.socket | None = None
    core = None
    original_borrow_rpc = None
    observed: list[tuple[str, object, object]] = []
    observed_lock = threading.Lock()
    observed_overflow = threading.Event()
    cleanup_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    report = None
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        listener.listen(1)
        context = ray.init(
            num_nodes=2,
            num_workers_per_node=1,
            node_resources=(
                {
                    "CPU": 1,
                    _SOURCE_PARENT: 1,
                    _SOURCE_CHILD: 1,
                    _SOURCE_PROBE: 1,
                },
                {"CPU": 1, _TARGET_CONSUMER: 1},
            ),
            inline_threshold=1024,
            object_store_bytes=_OBJECT_STORE_BYTES,
            enable_tracing=True,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        source, target = context.nodes
        managed_pids.update(
            {
                context.gcs_pid,
                source.node_pid,
                source.worker_pid,
                target.node_pid,
                target.worker_pid,
            }
        )
        managed_addresses.update(
            {
                context.gcs_address,
                source.node_address,
                source.worker_address,
                target.node_address,
                target.worker_address,
            }
        )
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 8
        assert os.getpid() not in managed_pids

        outer = _return_stored_child_ref.remote(gate_address, deadline)
        foreign = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(foreign, ray.ObjectRef)
        object_id = foreign.object_id
        assert foreign.owner_worker_id == source.worker_id
        assert foreign.owner_address == source.worker_address
        assert foreign.borrower_token is not None

        listener.settimeout(min(_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        child_connection, _peer = listener.accept()
        (producer_pid,) = struct.unpack(
            _ARRIVAL_FORMAT, _recv_exact(child_connection, _ARRIVAL_SIZE, deadline)
        )
        assert producer_pid == source.worker_pid

        original_borrow_rpc = core._borrow_rpc

        def inspect_borrow(address, handler, message):
            reply = original_borrow_rpc(address, handler, message)
            if handler in {
                "get_retained_owned_object",
                "report_retained_object_location",
                "release_owned_object_for_task",
            }:
                with observed_lock:
                    if len(observed) < _MAX_OBSERVATIONS:
                        observed.append((handler, message, reply))
                    else:
                        observed_overflow.set()
            return reply

        core._borrow_rpc = inspect_borrow

        consumer = _consume_stored.remote(foreign)
        early_close_deadline = min(deadline, time.monotonic() + _CLEANUP_SECONDS)
        _close(foreign, early_close_deadline)
        _close(outer, early_close_deadline)
        assert foreign.closed and outer.closed
        ready, remaining = ray.wait((consumer,), num_returns=1, timeout=0)
        assert ready == [] and remaining == [consumer]

        child_connection.settimeout(min(_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        child_connection.sendall(_RELEASE)
        result = ray.get(consumer, timeout=_remaining(deadline))
        assert result == (
            target.worker_pid,
            source.worker_pid,
            _PAYLOAD_BYTES,
            hashlib.sha256(_PAYLOAD_BYTE * _PAYLOAD_BYTES).hexdigest(),
            _PAYLOAD_BYTE * 4,
            _PAYLOAD_BYTE * 4,
        )
        _close(consumer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert consumer.closed

        probe = _probe_source_owner_collection.remote(object_id, deadline)
        assert ray.get(probe, timeout=_remaining(deadline)) == (
            source.worker_pid,
            ObjectCollectionState.COLLECTED.value,
            True,
            True,
            True,
            True,
            True,
        )
        _close(probe, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert probe.closed

        with observed_lock:
            calls = tuple(observed)
        retained_replies = [
            reply
            for handler, _message, reply in calls
            if handler == "get_retained_owned_object"
            and isinstance(reply, protocol.GetRetainedOwnedObjectReply)
            and reply.state is protocol.OwnedObjectState.READY_STORED
        ]
        assert retained_replies
        source_descriptor = retained_replies[-1].descriptor
        assert source_descriptor is not None
        location_reports = [
            (message, reply)
            for handler, message, reply in calls
            if handler == "report_retained_object_location"
            and isinstance(message, protocol.ReportRetainedObjectLocation)
            and isinstance(reply, protocol.ReportRetainedObjectLocationReply)
            and reply.accepted
        ]
        assert location_reports
        location_message, location_reply = location_reports[-1]
        target_descriptor = location_message.descriptor
        assert location_reply.descriptor == target_descriptor
        assert source_descriptor.node_id == source.node_id
        assert target_descriptor.node_id == target.node_id
        assert _logical_replica_identity(
            source_descriptor
        ) == _logical_replica_identity(target_descriptor)
        assert source_descriptor.object_id == object_id

        releases = [
            reply
            for handler, _message, reply in calls
            if handler == "release_owned_object_for_task"
            and isinstance(reply, protocol.ReleaseOwnedObjectForTaskReply)
        ]
        assert releases and releases[-1].accepted and releases[-1].released

        trace = _wait_for_collection_trace(object_id, source.worker_pid, deadline)
        starts = [
            record for record in trace
            if record.event == "object_collection_started"
        ]
        acknowledgements = [
            record for record in trace
            if record.event == "object_replica_collection_acknowledged"
        ]
        completions = [
            record for record in trace
            if record.event == "object_collection_completed"
        ]
        assert len(starts) == len(completions) == 1
        assert len(acknowledgements) == 2
        collection_id = dict(starts[0].fields)["collection_id"]
        assert {dict(record.fields)["collection_id"] for record in trace} == {
            collection_id
        }
        assert {
            dict(record.fields)["node_id"] for record in acknowledgements
        } == {str(source.node_id), str(target.node_id)}
        assert {
            dict(record.fields)["attempt_id"]
            for record in acknowledgements
        } == {str(source_descriptor.producer_attempt_id)}
        assert {
            dict(record.fields)["status"] for record in acknowledgements
        } == {protocol.DropObjectReplicaStatus.DROPPED.value}
        assert (
            starts[0].process_sequence
            < min(record.process_sequence for record in acknowledgements)
            <= max(record.process_sequence for record in acknowledgements)
            < completions[0].process_sequence
        )

        _assert_replica_absent(
            node=source,
            requester_node_id=source.node_id,
            descriptor=source_descriptor,
            deadline=deadline,
        )
        _assert_replica_absent(
            node=target,
            requester_node_id=source.node_id,
            descriptor=target_descriptor,
            deadline=deadline,
        )
        _assert_exact_drop_replay(node=source, descriptor=source_descriptor, deadline=deadline)
        _assert_exact_drop_replay(node=target, descriptor=target_descriptor, deadline=deadline)
        assert not observed_overflow.is_set()
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if child_connection is not None:
                try:
                    remaining = cleanup_deadline - time.monotonic()
                    if child_connection.fileno() != -1 and remaining > 0:
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
                    if core is not None and original_borrow_rpc is not None:
                        core._borrow_rpc = original_borrow_rpc
                    for ref in (probe, consumer, foreign, outer):
                        try:
                            _close(ref, cleanup_deadline)
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
                        assert not observed_overflow.is_set()
                        assert listener is None or listener.fileno() == -1
                        assert child_connection is None or child_connection.fileno() == -1

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
