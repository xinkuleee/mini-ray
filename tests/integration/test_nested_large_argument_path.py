"""Bounded public StoredArg lift, nested-handle import and physical GC.

Two resource-pinned tasks occupy different one-Worker Nodes.  The producer
stays PENDING behind a test-owned socket gate.  Its consumer receives a 64 KiB
by-value container holding that pending ObjectRef, after the Driver has closed
the original handle and before the producer is released.  Only the lifted
container is a readiness/pull dependency; the nested ref remains a handle
until user code explicitly calls get().

Static bounds: spawn; one GCS, two Nodes, two Workers; exactly two tasks; no
Actor, PG, fault, retry, reconstruction or trace collector; 1 MiB ObjectStore
per Node; one <68 KiB lifted object with two replicas and two <1 KiB results.
Synchronization uses one test-owned loopback listener, fixed-size messages
and semantic Events; no polling sleep or additional test thread.  Run only
the exact allowlisted node ID through the 30-second bounded runner.

The original five-second main steps, shared ten-second work budget and
ten-second Worker gates remain.  Early source.close shares work with a
three-second cap.  Final gate/handle cleanup, actual three-object collection
and the original two typed replica reads share min(work deadline, now + 3s);
finally reuses that same deadline.  Runtime shutdown keeps its own bounds.
Observation retains at most 32 relevant records; callbacks cannot turn their
own assertions into runtime retry/error behavior.  Their flags are checked by
the main thread and after failure cleanup.  These are retained observations,
not a bound on production RPC attempts or a claim of deadline cancellation.
Metadata inspection has a 2,048-node/32-level budget; actual collection waits
inspect local conditions at most 128 times.
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
from miniray.api import _get_runtime
from miniray.ids import (
    ActorID, AttemptID, JobID, LeaseID, NodeID, ObjectID, PlacementGroupID,
    TaskID, WorkerID,
)
from miniray.node import GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import request as rpc_request
from miniray.worker import PUSH_TASK_HANDLER


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_RESOURCE = "nested_lift_source"
_TARGET_RESOURCE = "nested_lift_target"
_PAYLOAD_BYTES = 64 * 1024
_PAYLOAD_BYTE = b"N"
_INLINE_THRESHOLD = 1024
_OBJECT_STORE_BYTES = 1024 * 1024
_STEP_TIMEOUT_SECONDS = 5.0
_GATE_TIMEOUT_SECONDS = 10.0
_WORK_TIMEOUT_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 32
_MAX_POLLS = 128
_MAX_METADATA_NODES = 2048
_MAX_METADATA_DEPTH = 32
_ARRIVAL_FORMAT = "!cQ"
_ARRIVAL_SIZE = struct.calcsize(_ARRIVAL_FORMAT)
_PRODUCE = b"P"
_CONSUME = b"C"
_RELEASE_PRODUCER = b"G"
_READ_NESTED = b"R"


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        part = connection.recv(size - len(data))
        if not part:
            raise RuntimeError("Worker closed before the complete gate message")
        data.extend(part)
    return bytes(data)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("nested-lift smoke exhausted its work deadline")
    return min(_STEP_TIMEOUT_SECONDS, remaining)


@ray.remote(num_cpus=1, resources={_SOURCE_RESOURCE: 1}, max_retries=0)
def _pending_source(gate_address: tuple[str, int]) -> tuple[int, int]:
    with socket.create_connection(
        gate_address, timeout=_GATE_TIMEOUT_SECONDS
    ) as gate:
        gate.settimeout(_GATE_TIMEOUT_SECONDS)
        gate.sendall(struct.pack(_ARRIVAL_FORMAT, _PRODUCE, os.getpid()))
        if gate.recv(1) != _RELEASE_PRODUCER:
            raise RuntimeError("producer gate closed before release")
    return os.getpid(), 41


@ray.remote(num_cpus=1, resources={_TARGET_RESOURCE: 1}, max_retries=0)
def _consume_lifted(
    gate_address: tuple[str, int], *, container: object,
) -> tuple[object, ...]:
    nested = container["nested"][0]  # type: ignore[index]
    repeated = container["nested"][1]["again"]  # type: ignore[index]
    padding = container["padding"]  # type: ignore[index]
    with socket.create_connection(
        gate_address, timeout=_GATE_TIMEOUT_SECONDS
    ) as gate:
        gate.settimeout(_GATE_TIMEOUT_SECONDS)
        # Reaching this user-code point proves both argument decoding and
        # Acquire completed while the nested producer is still PENDING.
        gate.sendall(struct.pack(_ARRIVAL_FORMAT, _CONSUME, os.getpid()))
        if gate.recv(1) != _READ_NESTED:
            raise RuntimeError("consumer gate closed before explicit get")
        value = ray.get(nested, timeout=_STEP_TIMEOUT_SECONDS)
    return (
        os.getpid(), isinstance(nested, ray.ObjectRef), nested is repeated,
        len(padding), hashlib.sha256(padding).hexdigest(), value,
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
    """Inspect typed control messages without materializing an object."""

    if isinstance(value, bytes):
        return (len(value),)
    if is_dataclass(value) and not isinstance(value, type):
        return tuple(
            size for field in fields(value)
            for size in _byte_field_sizes(getattr(value, field.name))
        )
    if isinstance(value, (tuple, list)):
        return tuple(size for item in value for size in _byte_field_sizes(item))
    return ()


def _assert_descriptor(
    descriptor: protocol.ObjectStoreDescriptor, *, object_id: ObjectID,
) -> None:
    assert isinstance(descriptor, protocol.ObjectStoreDescriptor)
    assert descriptor.object_id == object_id
    assert _PAYLOAD_BYTES < descriptor.size_bytes < _PAYLOAD_BYTES + 4096
    assert len(descriptor.checksum) == 64
    assert not hasattr(descriptor, "data")
    assert not hasattr(descriptor, "inline_data")
    assert not hasattr(descriptor, "payload")


def _assert_replica_absent(
    node: object, descriptor: protocol.ObjectStoreDescriptor, *, deadline: float,
) -> None:
    reply = rpc_request(
        node.node_address, GET_OBJECT_HANDLER,  # type: ignore[attr-defined]
        protocol.GetObject(
            descriptor.object_id, descriptor.node_id,
            expected_attempt_id=descriptor.producer_attempt_id,
            expected_owner_worker_id=descriptor.owner_worker_id,
            expected_size_bytes=descriptor.size_bytes,
            expected_checksum=descriptor.checksum,
        ),
        connect_timeout=min(0.5, _remaining(deadline)),
        request_timeout=min(1.0, _remaining(deadline)),
    )
    assert isinstance(reply, protocol.GetObjectReply)
    assert reply.object_id == descriptor.object_id
    assert reply.node_id == descriptor.node_id
    assert not reply.found and not reply.sealed
    assert reply.data is None and reply.checksum is None
    assert reply.producer_attempt_id is None
    assert reply.owner_worker_id is None and reply.size_bytes is None


def _recv_exact_current(connection, size, deadline):
    assert size == _ARRIVAL_SIZE
    data = bytearray()
    # A positive recv adds at least one byte, so a fixed frame needs at most
    # nine observations.  Recompute the shared deadline for every fragment.
    for _ in range(_ARRIVAL_SIZE):
        if len(data) == size:
            break
        connection.settimeout(_remaining(deadline))
        part = connection.recv(size - len(data))
        if not part:
            raise RuntimeError("Worker closed before the complete gate message")
        data.extend(part)
    assert len(data) == size
    return bytes(data)


def _close_current(reference, deadline):
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    # This is the local receipt, not a metadata/physical collection proof.
    assert reference.closed and done.is_set()


def _wait_current(core, predicate, deadline):
    with core._completion:
        for index in range(_MAX_POLLS):
            if predicate():
                return
            if index + 1 == _MAX_POLLS:
                break
            core._completion.wait(min(0.05, _remaining(deadline)))
    raise TimeoutError("nested-lift cleanup did not converge")


def _byte_field_sizes_current(value):
    # Opaque IDs are identities, not payload.  A node/depth budget also stops
    # cycles while permitting ordinary repeated immutable metadata aliases.
    budget = _MAX_METADATA_NODES
    opaque_ids = (ActorID, AttemptID, JobID, LeaseID, NodeID, ObjectID,
                  PlacementGroupID, TaskID, WorkerID)

    def visit(item, depth):
        nonlocal budget
        budget -= 1
        assert budget >= 0 and depth <= _MAX_METADATA_DEPTH
        if isinstance(item, opaque_ids):
            return ()
        if isinstance(item, (bytes, bytearray, memoryview)):
            return (len(item),)
        if is_dataclass(item) and not isinstance(item, type):
            return tuple(size for field in fields(item)
                         for size in visit(getattr(item, field.name), depth + 1))
        if isinstance(item, dict):
            return tuple(size for pair in item.items() for part in pair
                         for size in visit(part, depth + 1))
        if isinstance(item, (tuple, list, set, frozenset)):
            return tuple(size for part in item for size in visit(part, depth + 1))
        return ()

    return visit(value, 0)


def _assert_replica_absent_current(node, descriptor, *, deadline):
    remaining = _remaining(deadline)
    reply = rpc_request(
        node.node_address, GET_OBJECT_HANDLER,
        protocol.GetObject(
            descriptor.object_id, descriptor.node_id,
            expected_attempt_id=descriptor.producer_attempt_id,
            expected_owner_worker_id=descriptor.owner_worker_id,
            expected_size_bytes=descriptor.size_bytes,
            expected_checksum=descriptor.checksum,
        ),
        connect_timeout=min(0.5, remaining),
        request_timeout=min(1.0, remaining), deadline=deadline,
    )
    assert isinstance(reply, protocol.GetObjectReply)
    assert reply.object_id == descriptor.object_id
    assert reply.node_id == descriptor.node_id == node.node_id
    assert not reply.found and not reply.sealed
    assert reply.data is None and reply.checksum is None
    assert reply.producer_attempt_id is None
    assert reply.owner_worker_id is None and reply.size_bytes is None


def test_nested_large_argument_pulls_without_gating_on_pending_handle_and_collects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = None
    source = consumer = None
    context = runtime = core = report = None
    work_deadline = cleanup_deadline = None
    cleanup_errors = []
    # An independent ledger restores only these four case-owned patches.
    case_patch = pytest.MonkeyPatch()
    connections: list[tuple[socket.socket, bytes]] = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    observer_lock = threading.Lock()
    before_consumer_push = threading.Event()
    allow_consumer_push = threading.Event()
    nested_read_started = threading.Event()
    all_collected = threading.Event()
    lease_calls: list[tuple[object, object, object]] = []
    pushes: list[tuple[object, protocol.PushTask]] = []
    sealed_objects: list[tuple[ObjectID, int]] = []
    expected_collection_ids: set[str] = set()
    collected_ids: set[str] = set()
    replica_acks: set[tuple[str, str]] = set()
    first_reads: list[tuple[object, ObjectState]] = []
    source_id = hidden_id = consumer_id = None
    observation_count = 0
    observation_failed = observation_overflow = False
    driver_fetched = invalid_push = push_gate_expired = False

    def reserve_observation_locked():
        nonlocal observation_count, observation_overflow
        if observation_count >= _MAX_OBSERVATIONS:
            observation_overflow = True
            return False
        observation_count += 1
        return True

    def observer_ok():
        with observer_lock:
            flags = (observation_failed, observation_overflow, driver_fetched,
                     invalid_push, push_gate_expired)
        assert not any(flags), flags

    def release_gates(deadline):
        # Every accepted socket is closed around its release attempt.  The
        # ledger has only the two original accepts; listener close is separate.
        nonlocal listener
        while connections:
            connection, release = connections.pop()
            try:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    connection.settimeout(min(0.1, remaining))
                    connection.sendall(release)
            except OSError:
                pass
            finally:
                try:
                    connection.close()
                except Exception as exc:
                    cleanup_errors.append(("gate connection close", repr(exc)))
        if listener is not None:
            try:
                listener.close()
            except Exception as exc:
                cleanup_errors.append(("listener close", repr(exc)))
            listener = None

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        listener.listen(2)
        listener.settimeout(_STEP_TIMEOUT_SECONDS)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=(
                {"CPU": 1, _SOURCE_RESOURCE: 1},
                {"CPU": 1, _TARGET_RESOURCE: 1},
            ),
            inline_threshold=_INLINE_THRESHOLD,
            object_store_bytes=_OBJECT_STORE_BYTES, enable_tracing=False,
        )
        # One shared budget prevents multiple independently bounded waits
        # accumulating and leaves runner time for the real cluster teardown.
        work_deadline = time.monotonic() + _WORK_TIMEOUT_SECONDS
        managed_pids.update({
            context.gcs_pid, *context.node_pids, *context.worker_pids,
        })
        managed_addresses.update({
            context.gcs_address, *context.node_addresses,
            *context.worker_addresses,
        })
        runtime = _get_runtime()
        core = runtime.core_worker
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        assert runtime.owner_service is not None
        source_node, target_node = context.nodes
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7
        assert all(len(node.worker_pids) == 1 for node in context.nodes)
        assert context.trace_address is None
        assert os.getpid() not in managed_pids

        original_rpc = core._rpc
        original_push = core._push_task_rpc
        original_emit = core._emit
        original_has_borrowed = core.owner_table.has_borrowed_reference

        def inspect_rpc(address: object, handler: str, message: object) -> object:
            nonlocal observation_failed, driver_fetched
            # Worker fetches the pulled bytes locally.  The Driver must not
            # fetch the hidden stream to decode or forward the container.
            try:
                if handler == GET_OBJECT_HANDLER:
                    driver_fetched = True
            except Exception:
                observation_failed = True
            reply = original_rpc(address, handler, message)
            try:
                with observer_lock:
                    if handler == REQUEST_LEASE_HANDLER:
                        if reserve_observation_locked():
                            lease_calls.append((address, message, reply))
                    elif isinstance(message, protocol.SealObject):
                        if reserve_observation_locked():
                            sealed_objects.append((message.object_id, len(message.data)))
            except Exception:
                observation_failed = True
            return reply

        def inspect_push(address: object, handler: str, message: object) -> object:
            nonlocal observation_failed, invalid_push, push_gate_expired
            try:
                if handler != PUSH_TASK_HANDLER or not isinstance(message, protocol.PushTask):
                    invalid_push = True
                elif message.spec.resources.get(_TARGET_RESOURCE, 0):
                    with observer_lock:
                        if reserve_observation_locked():
                            pushes.append((address, message))
                    before_consumer_push.set()
                    remaining = work_deadline - time.monotonic()
                    if remaining <= 0 or not allow_consumer_push.wait(
                        min(_STEP_TIMEOUT_SECONDS, remaining)
                    ):
                        push_gate_expired = True
            except Exception:
                observation_failed = True
            # A failed observation is asserted by the main/finally path, not
            # raised here for the runtime to swallow/retry as a task failure.
            return original_push(address, handler, message)

        def inspect_borrowed(object_id: ObjectID, token: object) -> bool:
            nonlocal observation_failed
            active = original_has_borrowed(object_id, token)
            try:
                if object_id == source_id and active and not nested_read_started.is_set():
                    # Called by real GetOwnedObject, not Acquire.  Read owner
                    # state before the observation lock: the caller holds the
                    # Core condition, so never add an observer->owner lock edge.
                    state = core.owner_table.snapshot(object_id).state
                    with observer_lock:
                        if not nested_read_started.is_set():
                            if reserve_observation_locked():
                                first_reads.append((token, state))
                            nested_read_started.set()
            except Exception:
                observation_failed = True
            return active

        def inspect_emit(name: str, **attributes: object) -> None:
            nonlocal observation_failed
            original_emit(name, **attributes)
            try:
                object_id = attributes.get("object_id")
                with observer_lock:
                    if name == "object_replica_collection_acknowledged":
                        if reserve_observation_locked():
                            replica_acks.add((object_id, attributes.get("node_id")))
                    if name == "object_collection_completed":
                        if reserve_observation_locked():
                            collected_ids.add(object_id)
                        if expected_collection_ids and expected_collection_ids <= collected_ids:
                            all_collected.set()
            except Exception:
                observation_failed = True

        case_patch.setattr(core, "_rpc", inspect_rpc)
        case_patch.setattr(core, "_push_task_rpc", inspect_push)
        case_patch.setattr(core, "_emit", inspect_emit)
        case_patch.setattr(core.owner_table, "has_borrowed_reference", inspect_borrowed)

        _remaining(work_deadline)
        source = _pending_source.remote(gate_address)
        assert isinstance(source, ray.ObjectRef)
        source_id = source.object_id
        listener.settimeout(_remaining(work_deadline))
        producer_gate, _peer = listener.accept()
        connections.append((producer_gate, _RELEASE_PRODUCER))
        producer_gate.settimeout(_remaining(work_deadline))
        assert struct.unpack(
            _ARRIVAL_FORMAT, _recv_exact_current(producer_gate, _ARRIVAL_SIZE, work_deadline)
        ) == (
            _PRODUCE, source_node.worker_pid,
        )
        assert core.owner_table.snapshot(source_id).state is ObjectState.PENDING

        _remaining(work_deadline)
        consumer = _consume_lifted.remote(
            gate_address, container={
                "nested": [source, {"again": source}],
                "padding": _PAYLOAD_BYTE * _PAYLOAD_BYTES,
            },
        )
        assert isinstance(consumer, ray.ObjectRef)
        consumer_id = consumer.object_id
        spec = core.owner_table.snapshot(consumer_id).producer_task_spec
        assert isinstance(spec, protocol.TaskSpec)
        stored = dict(spec.kwargs)["container"]
        assert isinstance(stored, protocol.StoredArg)
        assert stored.serializer == "cloudpickle"
        hidden_id = stored.object_id
        assert hidden_id not in {source_id, consumer_id}
        assert len(stored.nested_refs) == 1
        transfer = stored.nested_refs[0]
        assert transfer.object_id == source_id
        assert transfer.owner_worker_id == core.worker_id
        assert transfer.owner_address == runtime.owner_service.address
        assert transfer.hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert transfer.hold.task_id == consumer_id.task_id
        assert transfer.hold.origin_attempt_id == spec.attempt_id
        with observer_lock:
            expected_collection_ids.update(map(str, (source_id, hidden_id, consumer_id)))

        assert before_consumer_push.wait(_remaining(work_deadline))
        observer_ok()
        _remaining(work_deadline)
        _close_current(source, min(work_deadline, time.monotonic() + _CLEANUP_SECONDS))
        held_source = core.owner_table.snapshot(source_id)
        assert held_source.state is ObjectState.PENDING
        assert held_source.local_tokens == frozenset()
        assert held_source.borrowed_tokens == frozenset()
        assert held_source.submitted_tokens == frozenset({transfer.hold})
        assert held_source.lineage_tokens
        hidden = core.owner_table.snapshot(hidden_id)
        assert hidden.state is ObjectState.READY_STORED
        assert hidden.local_tokens == frozenset()
        assert hidden.submitted_tokens == frozenset({transfer.hold})
        assert hidden.lineage_tokens
        assert hidden.locations == frozenset({source_node.node_id, target_node.node_id})

        with observer_lock:
            observed_leases = tuple(
                item for item in lease_calls
                if getattr(item[1], "task_id", None) == consumer_id.task_id
            )
            observed_pushes = tuple(pushes)
            observed_seals = tuple(sealed_objects)
        observer_ok()
        # The lifted bytes initially live only on source.  Locality chooses
        # that first hop without considering the target-only resource; Hybrid
        # scheduling must still return the original source -> target spillback.
        assert tuple(address for address, _request, _reply in observed_leases) == (
            source_node.node_address, target_node.node_address,
        )
        assert isinstance(observed_leases[0][2], protocol.SpillbackWorkerLease)
        spillback = observed_leases[0][2]
        grant = observed_leases[1][2]
        assert isinstance(grant, protocol.GrantWorkerLease)
        assert spillback.target_node_id == grant.node_id == target_node.node_id
        assert spillback.target_address == target_node.node_address
        assert grant.node_id == target_node.node_id
        assert grant.worker_id == target_node.worker_id
        assert len(observed_pushes) == 1
        address, push = observed_pushes[0]
        assert address == target_node.worker_address
        assert push.spec == spec
        assert push.worker_id == target_node.worker_id
        assert spec.max_retries == 0 and spec.attempt_id.attempt_number == 0
        assert push.lease_id == spillback.lease_id == grant.lease_id
        assert spillback.task_id == grant.task_id == spec.task_id
        assert spillback.attempt_id == grant.attempt_id == spec.attempt_id
        assert push.dependencies == grant.dependencies
        assert len(push.dependencies) == 1
        source_descriptor = observed_leases[0][1].dependencies[0]
        target_descriptor = push.dependencies[0]
        assert source_descriptor.node_id == source_node.node_id
        assert target_descriptor.node_id == target_node.node_id
        for descriptor in (source_descriptor, target_descriptor):
            _assert_descriptor(descriptor, object_id=hidden_id)
            assert descriptor.owner_worker_id == core.worker_id
        assert target_descriptor.producer_attempt_id == source_descriptor.producer_attempt_id
        assert target_descriptor.checksum == source_descriptor.checksum
        assert observed_seals == ((hidden_id, source_descriptor.size_bytes),)
        assert observed_leases[0][1].target_node_id is None
        assert observed_leases[1][1].target_node_id == target_node.node_id
        for _address, request, reply in observed_leases:
            assert isinstance(request, protocol.RequestWorkerLease)
            assert request.lease_id == grant.lease_id
            assert request.task_id == spec.task_id and request.attempt_id == spec.attempt_id
            assert request.requester_node_id == source_node.node_id
            assert request.requester_worker_id == core.worker_id
            assert request.preferred_node_id == source_node.node_id
            assert request.dependencies == (source_descriptor,)
            assert sum(_byte_field_sizes_current(request)) < _PAYLOAD_BYTES
            assert sum(_byte_field_sizes_current(reply)) < _PAYLOAD_BYTES
        assert sum(_byte_field_sizes_current(spec)) < _PAYLOAD_BYTES
        assert sum(_byte_field_sizes_current(push)) < _PAYLOAD_BYTES
        assert sum(
            len(argument.data)
            for argument in spec.args + tuple(value for _name, value in spec.kwargs)
            if isinstance(argument, protocol.InlineArg)
        ) <= _INLINE_THRESHOLD
        assert source_id not in {item.object_id for item in push.dependencies}

        _remaining(work_deadline)
        allow_consumer_push.set()
        listener.settimeout(_remaining(work_deadline))
        consumer_gate, _peer = listener.accept()
        connections.append((consumer_gate, _READ_NESTED))
        consumer_gate.settimeout(_remaining(work_deadline))
        assert struct.unpack(
            _ARRIVAL_FORMAT, _recv_exact_current(consumer_gate, _ARRIVAL_SIZE, work_deadline)
        ) == (
            _CONSUME, target_node.worker_pid,
        )
        before_get = core.owner_table.snapshot(source_id)
        assert before_get.state is ObjectState.PENDING
        assert len(before_get.borrowed_tokens) == 1
        assert before_get.local_tokens == frozenset()
        assert transfer.hold in before_get.submitted_tokens
        token, = before_get.borrowed_tokens
        assert token[0] == target_node.worker_id
        assert before_get.borrowed_sources == frozenset({
            (token, protocol.TaskHoldSource(transfer.hold)),
        })
        assert not nested_read_started.is_set()
        observer_ok()

        consumer_gate.settimeout(_remaining(work_deadline))
        consumer_gate.sendall(_READ_NESTED)
        assert nested_read_started.wait(_remaining(work_deadline))
        with observer_lock:
            observed_reads = tuple(first_reads)
        observer_ok()
        assert observed_reads == ((token, ObjectState.PENDING),)
        producer_gate.settimeout(_remaining(work_deadline))
        producer_gate.sendall(_RELEASE_PRODUCER)
        assert ray.get(consumer, timeout=_remaining(work_deadline)) == (
            target_node.worker_pid, True, True, _PAYLOAD_BYTES,
            hashlib.sha256(_PAYLOAD_BYTE * _PAYLOAD_BYTES).hexdigest(),
            (source_node.worker_pid, 41),
        )

        cleanup_deadline = min(work_deadline, time.monotonic() + _CLEANUP_SECONDS)
        release_gates(cleanup_deadline)
        _close_current(consumer, cleanup_deadline)
        _close_current(source, cleanup_deadline)
        assert all_collected.wait(_remaining(cleanup_deadline))
        observer_ok()
        with core._completion:
            for object_id in (consumer_id, source_id, hidden_id):
                assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
                assert not core.owner_table.contains(object_id)
                assert object_id not in core._objects
                assert object_id not in core._stored_descriptors
                assert object_id not in core._object_gc_obligations
                assert core._recovery.lineage_for_object(object_id) is None
            assert not core._recovery.reconstruction_snapshot(hidden_id).is_put
        with observer_lock:
            assert {(str(hidden_id), str(node.node_id)) for node in context.nodes} <= replica_acks
        _assert_replica_absent_current(source_node, source_descriptor, deadline=cleanup_deadline)
        _assert_replica_absent_current(target_node, target_descriptor, deadline=cleanup_deadline)
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
            if work_deadline is not None:
                cleanup_deadline = min(work_deadline, cleanup_deadline)
        # Release the pre-Push gate and undo only this case's observations
        # before failure cleanup or shutdown can call them again.
        allow_consumer_push.set()
        try:
            try:
                case_patch.undo()
            except Exception as exc:
                cleanup_errors.append(("observer restore", repr(exc)))
            release_gates(cleanup_deadline)
            for label, reference in (("consumer", consumer), ("source", source)):
                if not isinstance(reference, ray.ObjectRef):
                    continue
                try:
                    _close_current(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append((label + " close", repr(exc)))
            collection_ids = tuple(object_id for object_id in (consumer_id, source_id, hidden_id)
                                   if object_id is not None)
            if core is not None and collection_ids:
                try:
                    _wait_current(
                        core,
                        lambda: all(core.owner_table.collection_state(object_id)
                                    is ObjectCollectionState.COLLECTED
                                    for object_id in collection_ids),
                        cleanup_deadline,
                    )
                except Exception as exc:
                    cleanup_errors.append(("owner collection convergence", repr(exc)))
        finally:
            try:
                report = ray.shutdown()
            except Exception as exc:
                cleanup_errors.append(("shutdown", repr(exc)))

        # Gather every shutdown, child, and endpoint observation even after a
        # main-path assertion fails.  Assertions follow all hygiene probes.
        if report is not None:
            managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
        surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
        active_pids = tuple(child.pid for child in mp.active_children()
                            if child.pid in managed_pids)
        open_addresses = []
        for address in sorted(managed_addresses):
            try:
                with socket.create_connection(address, timeout=0.1):
                    open_addresses.append(address)
            except OSError:
                pass
        with observer_lock:
            observer_flags = (observation_failed, observation_overflow, driver_fetched,
                              invalid_push, push_gate_expired)
        checks = [
            (not ray.is_initialized(), "runtime remains initialized"),
            (not surviving_pids, ("surviving PIDs", surviving_pids)),
            (not active_pids, ("active children", active_pids)),
            (not open_addresses, ("open endpoints", open_addresses)),
            (not any(observer_flags), ("observer failure flags", observer_flags)),
        ]
        if context is not None:
            checks.append((report is not None, "missing shutdown report"))
        if report is not None and context is not None:
            checks.extend((
                (report.core_stopped, "Core did not stop"),
                (report.gcs_pid == context.gcs_pid, "GCS identity changed"),
                (tuple(report.node_pids) == context.node_pids, "Node identities changed"),
                (tuple(report.worker_pids) == context.worker_pids, "Worker identities changed"),
                (report.gcs_clean and report.gcs_exitcode == 0, "unclean GCS exit"),
                (report.node_clean and report.worker_clean, "unclean Node/Worker exit"),
                (report.resources_clean and report.finalized, "resources not finalized"),
                (report.shutdown_ack_clean and not report.forced, "forced or unacknowledged exit"),
                (tuple(report.node_exitcodes) == (0, 0), "unexpected Node exitcodes"),
                (tuple(report.worker_exitcodes) == (0, 0), "unexpected Worker exitcodes"),
                (tuple(report.worker_cleans) == (True, True), "unexpected Worker clean flags"),
                (tuple(report.worker_forced) == (False, False), "unexpected Worker forced flags"),
            ))
        cleanup_errors.extend(message for passed, message in checks if not passed)
        assert not cleanup_errors, cleanup_errors

    assert context is not None and report is not None
