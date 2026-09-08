"""Bounded acceptance test for a cross-node ObjectRef dependency pull.

The producer is pinned to node 0 and returns a store-backed value.  Its
consumer is submitted *before* that value is ready and is pinned to node 1.
The test inspects only the Driver's outgoing immutable messages: object bytes
must move NodeManager-to-NodeManager, while lease and direct-task messages
carry descriptors and ``RefArg`` handles only.

Run this exact node ID only through ``scripts/run_bounded_test.py``.  The hard
bounds are one GCS, two NodeManagers, one Worker per node, two tasks, a 1 MiB
store per node, and the runner's 30 second process-tree deadline. Init and gate
setup are covered by finally; API/gate work shares ten seconds, public close
shares three seconds, and every actual PID/owner/trace/gate endpoint is checked.
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
from dataclasses import fields, is_dataclass
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.node import REQUEST_LEASE_HANDLER
from miniray.worker import PUSH_TASK_HANDLER
from tests.integration.test_task_path import _close_reference, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_SOURCE_RESOURCE = "source_only"
_TARGET_RESOURCE = "target_only"
_PAYLOAD_BYTES = 64 * 1024
_PAYLOAD_BYTE = b"D"
_MAX_INLINE_ARGUMENT_BYTES = 1024
_PRODUCER_GATE_TIMEOUT_SECONDS = 5.0
_PRODUCER_RELEASE = b"G"


def _produce_after_gate(gate_address: tuple[str, int], deadline: float) -> tuple[int, bytes]:
    """Keep the producer pending until the Driver has submitted its consumer."""

    with socket.create_connection(
        gate_address, timeout=min(_PRODUCER_GATE_TIMEOUT_SECONDS, _remaining(deadline))
    ) as gate:
        gate.settimeout(min(_PRODUCER_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        if gate.recv(1) != _PRODUCER_RELEASE:
            raise RuntimeError("Driver closed the producer gate without releasing it")
    return os.getpid(), _PAYLOAD_BYTE * _PAYLOAD_BYTES


def _consume_and_summarize(value: tuple[int, bytes]) -> tuple[object, ...]:
    producer_pid, payload = value
    return (
        os.getpid(),
        producer_pid,
        len(payload),
        hashlib.sha256(payload).hexdigest(),
        payload[:4],
        payload[-4:],
    )


source_task = ray.remote(
    num_cpus=1, resources={_SOURCE_RESOURCE: 1}
)(_produce_after_gate)
target_task = ray.remote(
    num_cpus=1, resources={_TARGET_RESOURCE: 1}
)(_consume_and_summarize)


def _pid_exists(pid: int) -> bool:
    """Return whether a POSIX process still exists, including a zombie."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _task_inline_bytes(spec: protocol.TaskSpec) -> int:
    positional = sum(
        len(argument.data)
        for argument in spec.args
        if isinstance(argument, protocol.InlineArg)
    )
    keyword = sum(
        len(argument.data)
        for _name, argument in spec.kwargs
        if isinstance(argument, protocol.InlineArg)
    )
    return positional + keyword


def _assert_byte_free_descriptor(descriptor: object) -> None:
    """A transfer descriptor may describe bytes but must never contain them."""

    assert isinstance(descriptor, protocol.ObjectStoreDescriptor)
    assert descriptor.size_bytes >= _PAYLOAD_BYTES
    assert descriptor.checksum == descriptor.checksum.lower()
    assert len(descriptor.checksum) == 64
    assert not hasattr(descriptor, "data")
    assert not hasattr(descriptor, "inline_data")
    assert not hasattr(descriptor, "payload")


def _byte_field_sizes(value: object) -> tuple[int, ...]:
    """Inspect typed messages without serializing them or traversing runtimes."""

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
            size
            for item in value.items()
            for part in item
            for size in _byte_field_sizes(part)
        )
    if isinstance(value, (tuple, list, set, frozenset)):
        return tuple(
            size for item in value for size in _byte_field_sizes(item)
        )
    return ()


def test_store_backed_dependency_pulls_to_consumer_node_before_direct_push() -> None:
    """Move one 64 KiB value across nodes without routing bytes via Driver."""

    gate_listener = gate_address = None
    gate_connection = None
    context = None
    core = None
    original_rpc = None
    original_push_task_rpc = None
    report = None
    producer_ref = consumer_ref = None
    cleanup_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    calls: list[tuple[tuple[str, int], str, object, object]] = []
    calls_lock = threading.Lock()
    observation_overflow = threading.Event()

    try:
        gate_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gate_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        gate_listener.bind(("127.0.0.1", 0))
        gate_listener.listen(1)
        gate_address = gate_listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _SOURCE_RESOURCE: 1},
                {"CPU": 1, _TARGET_RESOURCE: 1},
            ),
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
        )
        deadline = time.monotonic() + 10.0
        runtime = _get_runtime()
        source_node, target_node = context.nodes
        managed_pids.update(
            {
                context.gcs_pid,
                source_node.node_pid,
                source_node.worker_pid,
                target_node.node_pid,
                target_node.worker_pid,
            }
        )
        managed_addresses.update(
            {
                context.gcs_address,
                source_node.node_address,
                source_node.worker_address,
                target_node.node_address,
                target_node.worker_address,
            }
        )
        assert runtime.owner_service is not None and context.trace_address is not None
        managed_addresses.update((runtime.owner_service.address, context.trace_address))
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 8
        assert os.getpid() not in managed_pids

        core = _get_runtime().core_worker
        original_rpc = core._rpc
        original_push_task_rpc = core._push_task_rpc

        def inspect_driver_rpc(
            address: tuple[str, int], handler: str, message: object
        ) -> object:
            assert original_rpc is not None
            reply = original_rpc(address, handler, message)
            if handler == REQUEST_LEASE_HANDLER:
                with calls_lock:
                    if len(calls) < 32:
                        calls.append((address, handler, message, reply))
                    else:
                        observation_overflow.set()
            return reply

        def inspect_push_task_rpc(
            address: tuple[str, int], handler: str, message: object
        ) -> object:
            assert original_push_task_rpc is not None
            reply = original_push_task_rpc(address, handler, message)
            assert handler == PUSH_TASK_HANDLER
            with calls_lock:
                if len(calls) < 32:
                    calls.append((address, handler, message, reply))
                else:
                    observation_overflow.set()
            return reply

        core._rpc = inspect_driver_rpc
        core._push_task_rpc = inspect_push_task_rpc

        producer_ref = source_task.remote(gate_address, deadline)
        consumer_ref = target_task.remote(producer_ref)

        # The producer cannot return until the Driver accepts its loopback gate
        # connection and sends the ACK below.  Because both remote() calls have
        # already returned, this proves submission accepts an unready RefArg
        # without an implicit Driver-side get().  No timing delay is involved.
        ready, remaining = ray.wait(
            [producer_ref, consumer_ref], num_returns=1, timeout=0
        )
        assert ready == []
        assert remaining == [producer_ref, consumer_ref]
        gate_listener.settimeout(min(_PRODUCER_GATE_TIMEOUT_SECONDS, _remaining(deadline)))
        gate_connection, _peer = gate_listener.accept()
        gate_connection.settimeout(_remaining(deadline))
        gate_connection.sendall(_PRODUCER_RELEASE)
        gate_connection.close()
        gate_connection = None

        result = ray.get(consumer_ref, timeout=_remaining(deadline))
        assert result == (
            target_node.worker_pid,
            source_node.worker_pid,
            _PAYLOAD_BYTES,
            hashlib.sha256(_PAYLOAD_BYTE * _PAYLOAD_BYTES).hexdigest(),
            _PAYLOAD_BYTE * 4,
            _PAYLOAD_BYTE * 4,
        )

        producer_descriptor = core._stored_descriptors[producer_ref.object_id]
        assert producer_descriptor.storage is protocol.ResultStorage.OBJECT_STORE
        assert producer_descriptor.node_id == source_node.node_id
        assert producer_descriptor.inline_data is None
        assert producer_descriptor.size_bytes >= _PAYLOAD_BYTES

        with calls_lock:
            observed_calls = tuple(calls)
        consumer_task_id = consumer_ref.object_id.task_id
        lease_calls = tuple(
            (address, message, reply)
            for address, handler, message, reply in observed_calls
            if handler == REQUEST_LEASE_HANDLER
            and isinstance(message, protocol.RequestWorkerLease)
            and message.task_id == consumer_task_id
        )
        assert tuple(address for address, _message, _reply in lease_calls) == (
            source_node.node_address,
            target_node.node_address,
        )
        assert len(lease_calls) == 2
        for _address, request, _reply in lease_calls:
            assert sum(_byte_field_sizes(request)) < _PAYLOAD_BYTES
            assert len(request.dependencies) == 1
            descriptor = request.dependencies[0]
            _assert_byte_free_descriptor(descriptor)
            assert descriptor.object_id == producer_ref.object_id
            assert descriptor.owner_worker_id == producer_ref.owner_worker_id
            assert descriptor.node_id == source_node.node_id
        assert isinstance(lease_calls[0][2], protocol.SpillbackWorkerLease)
        target_grant = lease_calls[1][2]
        assert isinstance(target_grant, protocol.GrantWorkerLease)
        assert target_grant.node_id == target_node.node_id
        assert len(target_grant.dependencies) == 1
        _assert_byte_free_descriptor(target_grant.dependencies[0])
        assert target_grant.dependencies[0].node_id == target_node.node_id

        consumer_pushes = tuple(
            (address, message)
            for address, handler, message, _reply in observed_calls
            if handler == PUSH_TASK_HANDLER
            and isinstance(message, protocol.PushTask)
            and message.spec.task_id == consumer_task_id
        )
        assert len(consumer_pushes) == 1
        push_address, push = consumer_pushes[0]
        assert push_address == target_node.worker_address
        assert push.worker_id == target_node.worker_id
        assert push.spec.args == (
            protocol.RefArg(producer_ref.object_id, producer_ref.owner_worker_id),
        )
        assert push.spec.kwargs == ()
        assert _task_inline_bytes(push.spec) < _MAX_INLINE_ARGUMENT_BYTES
        assert sum(_byte_field_sizes(push)) < _PAYLOAD_BYTES
        assert len(push.dependencies) == 1
        assert push.dependencies == target_grant.dependencies
        local_descriptor = push.dependencies[0]
        _assert_byte_free_descriptor(local_descriptor)
        assert local_descriptor.object_id == producer_ref.object_id
        assert local_descriptor.owner_worker_id == producer_ref.owner_worker_id
        assert local_descriptor.node_id == target_node.node_id
        assert local_descriptor.size_bytes == producer_descriptor.size_bytes
        assert local_descriptor.checksum == producer_descriptor.checksum
        assert not observation_overflow.is_set()
    finally:
        # Closing either side of the test-owned gate releases a blocked recv
        # with EOF if an assertion above fails before the normal ACK.
        cleanup_deadline = time.monotonic() + 3.0
        try:
            try:
                if gate_connection is not None:
                    gate_connection.close()
            finally:
                if gate_listener is not None:
                    gate_listener.close()
        finally:
            try:
                if core is not None and original_rpc is not None:
                    core._rpc = original_rpc
                if core is not None and original_push_task_rpc is not None:
                    core._push_task_rpc = original_push_task_rpc
                for reference in (consumer_ref, producer_ref):
                    try:
                        _close_reference(reference, cleanup_deadline)
                    except Exception as exc:
                        cleanup_errors.append(exc)
            finally:
                report = ray.shutdown()

    assert context is not None
    assert report is not None
    assert not cleanup_errors and not observation_overflow.is_set()
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.resources_clean
    assert report.finalized
    assert report.shutdown_ack_clean
    assert not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
    assert report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid and report.gcs_clean
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert all(not _pid_exists(pid) for pid in managed_pids)
    assert all(
        child.pid not in managed_pids for child in mp.active_children()
    )
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
