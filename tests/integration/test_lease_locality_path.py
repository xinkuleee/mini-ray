"""Bounded ordinary-task locality and resource-override acceptance.

The Driver lives on A. A producer pinned to B returns one 32 KiB value.
After its real finish barrier, an unconstrained consumer must request its
first, untargeted lease from B and execute there without creating an A replica.
A third task requires A's custom resource: its locality-first request still
goes to B, whose real Hybrid policy spills it back to A. A must pull and seal
the dependency, transfer custody to its unchanged Driver owner, and only then
receive the direct Worker Push. All three tasks use the public remote API.

Static bounds: one GCS, two Nodes, one Worker each, five managed children, six
runtime/owner endpoints, two 1 MiB stores, three task executions, one stored
application value below 64 KiB, no faults/Actor/trace/test thread/listener.
Actual RPC observations share a 32-record cap and never substitute replies or
scheduling decisions. Work shares fifteen seconds; all public closes and the
normal owner GC proof share three seconds, including failure cleanup. Startup
and shutdown retain their own contracts: run only this exact ID through the
external 30-second process-tree runner plus its bounded termination grace.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
import hashlib
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.ids import AttemptID
from miniray.node import (
    DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER,
)
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.worker import PUSH_TASK_HANDLER
from tests.integration.test_task_path import _close_reference, _remaining


pytestmark = pytest.mark.multiprocess_smoke

_HOME_RESOURCE = "locality_home_only"
_DATA_RESOURCE = "locality_data_only"
_PAYLOAD_BYTES = 32 * 1024
_PAYLOAD_BYTE = b"L"
_MAX_STORED_BYTES = 64 * 1024
_WORK_SECONDS = 15.0
_CLOSE_SECONDS = 3.0
_MAX_OBSERVATIONS = 32


@ray.remote(num_cpus=1, resources={_DATA_RESOURCE: 1}, max_retries=0)
def _produce_data() -> tuple[int, bytes]:
    return os.getpid(), _PAYLOAD_BYTE * _PAYLOAD_BYTES


def _summarize_data(value: tuple[int, bytes]) -> tuple[object, ...]:
    producer_pid, payload = value
    return (
        os.getpid(), producer_pid, len(payload),
        hashlib.sha256(payload).hexdigest(), payload[:4], payload[-4:],
    )


_plain_consumer = ray.remote(num_cpus=1, max_retries=0)(_summarize_data)
_home_consumer = ray.remote(
    num_cpus=1, resources={_HOME_RESOURCE: 1}, max_retries=0
)(_summarize_data)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_finished(core, reference, deadline: float) -> None:
    # READY precedes the final adoption ACK. Wait for the real accounting
    # barrier, not a delay or another task used as an implicit readiness probe.
    with core._completion:
        while reference.object_id in core._task_finish_barriers:
            core._completion.wait(_remaining(deadline))
    _remaining(deadline)


def _wait_collected(core, references, deadline: float) -> None:
    with core._completion:
        while any(
            core.owner_table.collection_state(reference.object_id)
            is not ObjectCollectionState.COLLECTED
            for reference in references
        ):
            core._completion.wait(_remaining(deadline))
    _remaining(deadline)


def _byte_field_sizes(value: object) -> tuple[int, ...]:
    """Inspect only immutable messages, never pickle or runtime objects."""
    if isinstance(value, bytes):
        return (len(value),)
    if is_dataclass(value) and not isinstance(value, type):
        return tuple(
            size for field in fields(value)
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


def _lease_replies(observed, task_id):
    return tuple(
        (address, request, reply)
        for stage, address, handler, request, reply in observed
        if stage == "rpc_reply" and handler == REQUEST_LEASE_HANDLER
        and type(request) is protocol.RequestWorkerLease
        and request.task_id == task_id
    )


def _assert_push(observed, reference, node, request, grant):
    task_id = reference.object_id.task_id
    sends = tuple(
        (index, address, message)
        for index, (stage, address, handler, message, _reply) in enumerate(observed)
        if stage == "push_send" and handler == PUSH_TASK_HANDLER
        and type(message) is protocol.PushTask and message.spec.task_id == task_id
    )
    (send_index, address, push), = sends
    assert address == node.worker_address == grant.worker_address
    assert push.worker_id == grant.worker_id == node.worker_id
    assert push.lease_id == grant.lease_id == request.lease_id
    assert push.spec.task_id == grant.task_id == request.task_id == task_id
    assert push.spec.attempt_id == grant.attempt_id == request.attempt_id == AttemptID(task_id, 0)
    assert push.spec.owner_worker_id == reference.owner_worker_id == request.requester_worker_id
    assert push.spec.resources == request.resources
    assert push.spec.return_ids() == request.return_ids == (reference.object_id,)
    assert push.dependencies == grant.dependencies
    assert push.spec.scheduling_key is grant.scheduling_key is request.scheduling_key is None
    assert sum(_byte_field_sizes(push)) < _PAYLOAD_BYTES
    replies = tuple(
        (index, reply_address, message, reply)
        for index, (stage, reply_address, handler, message, reply) in enumerate(observed)
        if stage == "push_reply" and handler == PUSH_TASK_HANDLER
        and type(message) is protocol.PushTask and message.spec.task_id == task_id
    )
    (reply_index, reply_address, message, reply), = replies
    assert reply_index > send_index and reply_address == address and message == push
    assert type(reply) is protocol.TaskReply and reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert (reply.task_id, reply.attempt_id, reply.worker_id) == (task_id, request.attempt_id, node.worker_id)
    assert reply.error is None
    return send_index, push


def _assert_consumer_custody(
    observed, reference, node, request, grant, source, source_snapshot, owner_address,
):
    send_index, push = _assert_push(observed, reference, node, request, grant)
    assert push.spec.args == (protocol.RefArg(source.object_id, source.owner_worker_id),)
    assert push.spec.kwargs == ()
    (source_descriptor,) = request.dependencies
    assert type(source_descriptor) is protocol.ObjectStoreDescriptor
    canonical = source_snapshot.canonical_stored_result
    assert source_descriptor == protocol.ObjectStoreDescriptor(
        source.object_id, source.owner_worker_id, source_snapshot.current_attempt,
        canonical.node_id, canonical.size_bytes, canonical.checksum,
    )
    assert grant.dependencies == (replace(source_descriptor, node_id=node.node_id),)
    (route,) = request.dependency_owner_routes
    assert route.object_id == source.object_id and route.owner_worker_id == source.owner_worker_id
    assert route.owner_address == owner_address
    assert route.hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
    assert route.hold.submitting_worker_id == source.owner_worker_id
    assert route.hold.task_id == reference.object_id.task_id
    assert route.hold.origin_attempt_id == request.attempt_id
    acknowledgements = tuple(
        (index, address, message, reply)
        for index, (stage, address, handler, message, reply) in enumerate(observed)
        if stage == "rpc_reply" and handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        and type(message) is protocol.AckLeaseDependencyCustody
        and message.inventory.lease_request.task_id == reference.object_id.task_id
    )
    (ack_index, address, acknowledgement, reply), = acknowledgements
    assert address == node.node_address
    assert acknowledgement.requester_worker_id == source.owner_worker_id
    assert acknowledgement.inventory == protocol.LeaseDependencyInventory(request, node.node_id, grant.dependencies)
    assert type(reply) is protocol.AckLeaseDependencyCustodyReply
    assert reply.request == acknowledgement and reply.accepted and reply.error is None
    before_ack = tuple(
        (index, snapshots)
        for index, (stage, _address, _handler, message, snapshots) in enumerate(observed)
        if stage == "owner_before_custody" and message == acknowledgement
    )
    (owner_index, snapshots), = before_ack
    (owner_before,) = snapshots
    assert owner_index < ack_index < send_index
    assert owner_before.state is ObjectState.READY_STORED
    assert owner_before.current_attempt == source_snapshot.current_attempt
    assert owner_before.canonical_stored_result == canonical
    assert node.node_id in owner_before.locations
    assert route.hold in owner_before.submitted_tokens


def test_stored_dependency_selects_data_first_hop_and_resources_can_spill_back_home():
    context = core = report = None
    source = plain = constrained = None
    original_rpc = original_push = None
    close_deadline = None
    refs, cleanup_errors = [], []
    managed_pids, managed_addresses = set(), set()
    observed = []
    observation_lock = threading.Lock()
    observation_overflow = threading.Event()

    def remember(stage, address, handler, request, reply):
        with observation_lock:
            if len(observed) < _MAX_OBSERVATIONS:
                observed.append((stage, address, handler, request, reply))
            else:
                # Observation must not turn a real ACK into a transport error.
                observation_overflow.set()

    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=(
                {"CPU": 1, _HOME_RESOURCE: 1},
                {"CPU": 1, _DATA_RESOURCE: 1},
            ),
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        home, data_node = context.nodes
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        owner_address = runtime.owner_service.address
        managed_addresses.add(owner_address)
        assert len(managed_pids) == 5 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 6 and context.trace_address is None
        assert all(len(node.worker_pids) == 1 for node in context.nodes)
        assert core.node_id == context.node_id == home.node_id
        original_rpc, original_push = core._rpc, core._push_task_rpc

        def observe_rpc(address, handler, request):
            if handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER:
                try:
                    with core._state_lock:
                        snapshots = tuple(
                            core.owner_table.snapshot(item.object_id)
                            for item in request.inventory.descriptors
                        )
                except Exception as exc:
                    # A failed diagnostic is recorded, not substituted for the
                    # real RPC. The main test rejects this observation later.
                    snapshots = exc
                remember(
                    "owner_before_custody", address, handler, request, snapshots,
                )
            reply = original_rpc(address, handler, request)
            if handler in (
                REQUEST_LEASE_HANDLER, GET_OBJECT_HANDLER, DROP_OBJECT_REPLICA_HANDLER,
                protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER,
            ):
                remember("rpc_reply", address, handler, request, reply)
            return reply

        def observe_push(address, handler, request):
            remember("push_send", address, handler, request, None)
            reply = original_push(address, handler, request)
            remember("push_reply", address, handler, request, reply)
            return reply

        core._rpc, core._push_task_rpc = observe_rpc, observe_push
        source = _produce_data.remote()
        refs.append(source)
        ready, pending = ray.wait([source], num_returns=1, timeout=_remaining(deadline))
        assert ready == [source] and pending == []
        _wait_finished(core, source, deadline)
        with core._state_lock:
            source_snapshot = core.owner_table.snapshot(source.object_id)
            canonical = core._stored_descriptors[source.object_id]
        assert source.owner_worker_id == core.worker_id and source.borrower_token is None
        assert source_snapshot.state is ObjectState.READY_STORED
        assert source_snapshot.current_attempt == AttemptID(source.object_id.task_id, 0)
        assert source_snapshot.locations == frozenset({data_node.node_id})
        assert source_snapshot.canonical_stored_result == canonical
        assert canonical.node_id == data_node.node_id and canonical.owner_worker_id == core.worker_id
        assert canonical.storage is protocol.ResultStorage.OBJECT_STORE and canonical.inline_data is None
        assert _PAYLOAD_BYTES <= canonical.size_bytes <= _MAX_STORED_BYTES

        # Do not get() the stored producer or create a home replica before the
        # unconstrained task. Only metadata makes this locality decision.
        expected_tail = (
            data_node.worker_pid, _PAYLOAD_BYTES,
            hashlib.sha256(_PAYLOAD_BYTE * _PAYLOAD_BYTES).hexdigest(),
            _PAYLOAD_BYTE * 4, _PAYLOAD_BYTE * 4,
        )
        plain = _plain_consumer.remote(source)
        refs.append(plain)
        assert ray.get(plain, timeout=_remaining(deadline)) == (data_node.worker_pid, *expected_tail)
        _wait_finished(core, plain, deadline)
        with core._state_lock:
            local_only = core.owner_table.snapshot(source.object_id)
            assert local_only.locations == frozenset({data_node.node_id})
            assert local_only.current_attempt == source_snapshot.current_attempt
            assert local_only.canonical_stored_result == canonical
            assert core._stored_descriptors[source.object_id] == canonical

        constrained = _home_consumer.remote(source)
        refs.append(constrained)
        assert ray.get(constrained, timeout=_remaining(deadline)) == (home.worker_pid, *expected_tail)
        _wait_finished(core, constrained, deadline)
        with core._state_lock:
            replicated = core.owner_table.snapshot(source.object_id)
            assert replicated.locations == frozenset({home.node_id, data_node.node_id})
            assert replicated.current_attempt == source_snapshot.current_attempt
            assert replicated.canonical_stored_result == canonical
            assert core._stored_descriptors[source.object_id] == canonical
            assert not replicated.submitted_tokens and len(replicated.lineage_tokens) == 2
            assert core._accepted_task_count == 0 and not core._protocol_unresolved
            for reference in refs:
                record = core._recovery.task_record(reference.object_id.task_id)
                assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
                assert record.current_attempt == AttemptID(reference.object_id.task_id, 0)
                assert core._recovery.active_recovery(reference.object_id.task_id) is None
            assert all(
                core.owner_table.snapshot(reference.object_id).state is ObjectState.READY_INLINE
                for reference in (plain, constrained)
            )

        with observation_lock:
            calls = tuple(observed)
        source_leases = _lease_replies(calls, source.object_id.task_id)
        plain_leases = _lease_replies(calls, plain.object_id.task_id)
        home_leases = _lease_replies(calls, constrained.object_id.task_id)
        assert tuple(address for address, _, _ in source_leases) == (home.node_address, data_node.node_address)
        assert tuple(address for address, _, _ in plain_leases) == (data_node.node_address,)
        assert tuple(address for address, _, _ in home_leases) == (data_node.node_address, home.node_address)
        for sequence, first_node, final_node in (
            (source_leases, home, data_node), (home_leases, data_node, home),
        ):
            (_, first, spillback), (_, targeted, grant) = sequence
            assert first.target_node_id is None and first.preferred_node_id == first_node.node_id
            assert type(spillback) is protocol.SpillbackWorkerLease
            assert (spillback.lease_id, spillback.task_id, spillback.attempt_id) == (first.lease_id, first.task_id, first.attempt_id)
            assert spillback.target_node_id == final_node.node_id
            assert spillback.target_address == final_node.node_address
            assert targeted == replace(first, target_node_id=final_node.node_id)
            assert type(grant) is protocol.GrantWorkerLease and grant.node_id == final_node.node_id
        (_, plain_request, plain_grant), = plain_leases
        assert plain_request.target_node_id is None and plain_request.preferred_node_id == data_node.node_id
        assert type(plain_grant) is protocol.GrantWorkerLease and plain_grant.node_id == data_node.node_id
        for _address, request, _reply in source_leases + plain_leases + home_leases:
            assert request.requester_node_id == home.node_id
            assert request.requester_worker_id == core.worker_id
            assert request.scheduling_key is None
            assert sum(_byte_field_sizes(request)) < _PAYLOAD_BYTES
        assert not source_leases[0][1].dependencies
        assert not plain_request.resources.get(_HOME_RESOURCE, 0)
        assert not plain_request.resources.get(_DATA_RESOURCE, 0)
        assert home_leases[0][1].resources.get(_HOME_RESOURCE, 0) == 1
        _assert_push(calls, source, data_node, source_leases[-1][1], source_leases[-1][2])
        _assert_consumer_custody(
            calls, plain, data_node, plain_request, plain_grant, source, source_snapshot, owner_address,
        )
        _assert_consumer_custody(
            calls, constrained, home, home_leases[-1][1], home_leases[-1][2],
            source, source_snapshot, owner_address,
        )
        assert not any(handler == GET_OBJECT_HANDLER for _, _, handler, _, _ in calls)
        assert not observation_overflow.is_set()
        _remaining(deadline)

        # Actual consumer collection releases their lineage roots. Public
        # close alone is only a local release receipt, not proof of physical GC.
        close_deadline = time.monotonic() + _CLOSE_SECONDS
        for reference in (plain, constrained):
            _close_reference(reference, close_deadline)
        _wait_collected(core, (plain, constrained), close_deadline)
        with core._state_lock:
            rooted = core.owner_table.snapshot(source.object_id)
            assert rooted.local_tokens == source_snapshot.local_tokens
            assert not rooted.submitted_tokens and not rooted.lineage_tokens
            assert rooted.locations == replicated.locations
            assert not source.closed
        _close_reference(source, close_deadline)
        _wait_collected(core, refs, close_deadline)
        with core._state_lock:
            for reference in refs:
                object_id = reference.object_id
                assert not core.owner_table.contains(object_id)
                assert object_id not in core._objects
                assert object_id not in core._stored_descriptors
                assert object_id not in core._object_gc_obligations
                assert core._recovery.lineage_for_object(object_id) is None
            assert not core._protocol_unresolved and core._accepted_task_count == 0
        with observation_lock:
            final_calls = tuple(observed)
        drops = tuple(
            (address, request, reply)
            for stage, address, handler, request, reply in final_calls
            if stage == "rpc_reply" and handler == DROP_OBJECT_REPLICA_HANDLER
            and type(request) is protocol.DropObjectReplica and request.object_id == source.object_id
        )
        acknowledged_nodes = set()
        addresses_by_node = {node.node_id: node.node_address for node in context.nodes}
        for address, request, reply in drops:
            assert address == addresses_by_node[request.node_id]
            assert request.owner_worker_id == source.owner_worker_id
            assert request.producer_attempt_id == source_snapshot.current_attempt
            assert request.checksum == canonical.checksum
            assert type(reply) is protocol.DropObjectReplicaReply
            assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
                request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
            )
            if reply.status in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED):
                acknowledged_nodes.add(reply.node_id)
        assert acknowledged_nodes == {home.node_id, data_node.node_id}
        assert not observation_overflow.is_set()
    finally:
        if close_deadline is None:
            close_deadline = time.monotonic() + _CLOSE_SECONDS
        try:
            for reference in reversed(refs):
                try:
                    _close_reference(reference, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if core is not None and original_rpc is not None:
                    core._rpc = original_rpc
                if core is not None and original_push is not None:
                    core._push_task_rpc = original_push
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
                assert not cleanup_errors, cleanup_errors
                assert not observation_overflow.is_set()
                assert not ray.is_initialized()
                if context is not None:
                    assert report is not None
                    assert report.gcs_pid == context.gcs_pid
                    assert report.node_pids == context.node_pids
                    assert report.worker_pids == context.worker_pids
                    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
                    assert report.node_clean and report.worker_clean
                    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
                    assert report.finalized and report.shutdown_ack_clean
                    assert report.resources_clean and not report.forced
