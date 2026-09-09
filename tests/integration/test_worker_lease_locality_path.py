"""Bounded cold-locality lookup from a real Worker-embedded Core.

The Driver owns a 32 KiB result on B. A zero-CPU parent on A receives its
ObjectRef inside a small list, imports a real borrower, and submits two
sequential unconstrained children. Both first leases and direct PushTask RPCs
must go to B. Only the first locality-scoring scope resolves B through GCS;
publication adoption has its own address lookups and is not a cache miss.

Children remain parent-Worker-owned and their retained foreign lineage is
released after their actual collection, before the parent returns. The Driver
then collects the parent and source while both Workers are still alive. No
owner death, injected reply, manual cleanup drive, snapshot installation, or
test-selected lease route can make this pass.

Static bounds: one GCS, two Nodes, one Worker each (five managed children, six
runtime/owner endpoints), four tiny user executions, two 1 MiB stores, one
stored result below 64 KiB, no fault/Actor/PG/trace/test listener or thread.
Parent and Driver observers retain at most 64 and 24 filtered records. Work
shares fifteen seconds after init; parent reference cleanup uses a shared
three-second subdeadline within work. Driver closes and normal GC share three
seconds, including finally. Startup/shutdown have their existing contracts.
Run this exact ID only through the external 30-second process-tree runner.
"""

from __future__ import annotations

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
from miniray.control import GET_NODE_ADDRESS_HANDLER
from miniray.core import CoreWorker
from miniray.foreign_lineage import ForeignLineageRole
from miniray.ids import AttemptID
from miniray.node import DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER
from miniray.owner_service import (
    GET_RETAINED_OWNED_OBJECT_HANDLER, RELEASE_BORROWED_OBJECT_HANDLER,
    RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER, REPORT_RETAINED_OBJECT_LOCATION_HANDLER,
    RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER,
)
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from miniray.runtime_binding import current_core_worker, current_execution_context
from miniray.transport import request as rpc_request
from miniray.worker import PUSH_TASK_HANDLER


pytestmark = pytest.mark.multiprocess_smoke

_PARENT_RESOURCE = "worker_locality_parent"
_SOURCE_RESOURCE = "worker_locality_source"
_PAYLOAD_BYTES = 32 * 1024
_PAYLOAD_BYTE = b"W"
_INLINE_THRESHOLD = 4096
_WORK_SECONDS = 15.0
_CLOSE_SECONDS = 3.0
_BORROW_HANDLERS = frozenset({
    RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER, GET_RETAINED_OWNED_OBJECT_HANDLER,
    REPORT_RETAINED_OBJECT_LOCATION_HANDLER, RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER,
    RELEASE_BORROWED_OBJECT_HANDLER,
})


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Worker-locality smoke exceeded its shared deadline")
    return remaining


def _close(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _wait_finished(core, reference, deadline: float) -> None:
    with core._completion:
        while reference.object_id in core._task_finish_barriers:
            core._completion.wait(_remaining(deadline))
    _remaining(deadline)


def _wait_collected(core, references, deadline: float) -> None:
    # COLLECTED can precede the foreign owner Release ACK. Wait for the
    # activated/prepared receipts and registry too, never drive them here.
    object_ids = tuple(reference.object_id for reference in references)
    with core._completion:
        while any(
            core.owner_table.collection_state(object_id) is not ObjectCollectionState.COLLECTED
            or object_id in core._foreign_lineage_collection_receipts
            or object_id in core._foreign_lineage_prepared_collection_receipts
            or core._foreign_lineage_registry.snapshot(object_id.task_id) is not None
            for object_id in object_ids
        ):
            core._completion.wait(_remaining(deadline))
        for object_id in object_ids:
            assert not core.owner_table.contains(object_id)
            assert object_id not in core._objects
            assert object_id not in core._stored_descriptors
            assert object_id not in core._object_gc_obligations
            assert core._recovery.lineage_for_object(object_id) is None
    _remaining(deadline)


class _Observations:
    """A capped passive ledger; overflow never changes a real RPC reply."""

    def __init__(self, maximum: int) -> None:
        self.maximum = maximum
        self.records = []
        self.overflow = False
        self.lock = threading.Lock()

    def add(self, stage, scope, address, handler, request, reply) -> None:
        with self.lock:
            if len(self.records) < self.maximum:
                self.records.append((stage, scope, address, handler, request, reply))
            else:
                self.overflow = True

    def snapshot(self):
        with self.lock:
            assert not self.overflow, "filtered observation limit exceeded"
            return tuple(self.records)


@ray.remote(num_cpus=1, resources={_SOURCE_RESOURCE: 1}, max_retries=0)
def _source_data():
    return os.getpid(), _PAYLOAD_BYTE * _PAYLOAD_BYTES


@ray.remote(num_cpus=1, max_retries=0)
def _plain_child(value):
    producer_pid, payload = value
    return os.getpid(), producer_pid, len(payload), hashlib.sha256(payload).hexdigest()


def _child_story(calls, index, reference, nested, descriptor, parent, data_node, parent_task_id):
    task_id = reference.object_id.task_id
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, parent.worker_id, task_id, AttemptID(task_id, 0),
    )
    leases = tuple(
        (position, address, request, reply)
        for position, (stage, _scope, address, handler, request, reply) in enumerate(calls)
        if stage == "rpc" and handler == REQUEST_LEASE_HANDLER and request.task_id == task_id
    )
    (lease_position, address, lease, grant), = leases
    assert address == data_node.node_address
    assert type(lease) is protocol.RequestWorkerLease and type(grant) is protocol.GrantWorkerLease
    assert lease.preferred_node_id == data_node.node_id and lease.target_node_id is None
    assert lease.requester_node_id == parent.node_id and lease.requester_worker_id == parent.worker_id
    assert dict(lease.resources) == {"CPU": 1}
    assert lease.dependencies == grant.dependencies == (descriptor,)
    assert grant.node_id == data_node.node_id and grant.worker_id == data_node.worker_id
    assert grant.worker_address == data_node.worker_address
    assert lease.scheduling_key is grant.scheduling_key is None
    assert lease.attempt_id.task_id == grant.attempt_id.task_id == task_id
    assert lease.return_ids == (reference.object_id,)
    assert (grant.task_id, grant.attempt_id, grant.lease_id) == (task_id, AttemptID(task_id, 0), lease.lease_id)
    assert lease.attempt_id == grant.attempt_id
    (owner_route,) = lease.dependency_owner_routes
    assert owner_route == protocol.DependencyOwnerRoute(
        nested.object_id, nested.owner_worker_id, nested.owner_address, hold,
    )
    routes = tuple(
        (position, address, request, reply)
        for position, (stage, scope, address, _handler, request, reply) in enumerate(calls)
        if stage == "route" and scope == index
    )
    (route_position, address, inputs, route), = routes
    assert address == parent.node_address and inputs == (descriptor,)
    assert route == (data_node.node_id, data_node.node_address)
    assert route_position < lease_position
    cold = tuple(
        position for position, (stage, scope, _address, handler, _request, _reply) in enumerate(calls)
        if stage == "rpc" and handler == GET_NODE_ADDRESS_HANDLER and scope == index
    )
    assert len(cold) == (1 if index == 0 else 0)
    assert all(position < route_position for position in cold)

    def borrow_calls(handler):
        return tuple(
            (position, request, reply)
            for position, (stage, _scope, address, name, request, reply) in enumerate(calls)
            if stage == "borrow" and name == handler and request.hold == hold
            and address == nested.owner_address
        )

    (retain_position, retain, retained), = borrow_calls(RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER)
    assert retain == protocol.RetainOwnedObjectForTask(
        nested.object_id, nested.owner_worker_id, parent.worker_id, nested.borrower_token, hold,
    )
    assert type(retained) is protocol.RetainOwnedObjectForTaskReply
    assert (retained.object_id, retained.owner_worker_id, retained.borrower_worker_id,
            retained.borrower_token, retained.hold) == (
        retain.object_id, retain.owner_worker_id, retain.borrower_worker_id, retain.borrower_token, hold,
    )
    assert retained.accepted and retained.retained and retained.error is None
    reads = borrow_calls(GET_RETAINED_OWNED_OBJECT_HANDLER)
    assert reads
    for position, request, reply in reads:
        assert type(reply) is protocol.GetRetainedOwnedObjectReply
        assert request == protocol.GetRetainedOwnedObject(
            nested.object_id, nested.owner_worker_id, parent.worker_id, hold,
        )
        assert reply.accepted and reply.state is protocol.OwnedObjectState.READY_STORED
        assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id, reply.hold) == (
            request.object_id, request.owner_worker_id, request.borrower_worker_id, hold,
        )
        assert reply.data is None and reply.descriptor == descriptor and reply.current_attempt == descriptor.producer_attempt_id
        assert retain_position < position < route_position
    (report_position, request, reply), = borrow_calls(REPORT_RETAINED_OBJECT_LOCATION_HANDLER)
    assert request == protocol.ReportRetainedObjectLocation(
        nested.object_id, nested.owner_worker_id, parent.worker_id, hold, descriptor,
    )
    assert type(reply) is protocol.ReportRetainedObjectLocationReply
    assert reply.accepted and reply.status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
    assert reply.descriptor == descriptor and reply.hold == hold
    assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id) == (
        request.object_id, request.owner_worker_id, request.borrower_worker_id,
    )
    acknowledgements = tuple(
        (position, address, request, reply)
        for position, (stage, _scope, address, handler, request, reply) in enumerate(calls)
        if stage == "rpc" and handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        and request.inventory.lease_request.task_id == task_id
    )
    (ack_position, address, request, reply), = acknowledgements
    assert address == data_node.node_address
    assert request == protocol.AckLeaseDependencyCustody(
        parent.worker_id, protocol.LeaseDependencyInventory(lease, data_node.node_id, grant.dependencies),
    )
    assert type(reply) is protocol.AckLeaseDependencyCustodyReply and reply.request == request and reply.accepted
    pushes = tuple(
        (position, stage, address, request, reply)
        for position, (stage, _scope, address, handler, request, reply) in enumerate(calls)
        if stage in ("push_send", "push_reply") and handler == PUSH_TASK_HANDLER and request.spec.task_id == task_id
    )
    (send_position, stage, address, push, _), (reply_position, final_stage, final_address, final_push, reply) = pushes
    assert stage == "push_send" and final_stage == "push_reply"
    assert address == final_address == data_node.worker_address and final_push == push
    assert push.worker_id == data_node.worker_id and push.lease_id == grant.lease_id
    assert push.spec.owner_worker_id == reference.owner_worker_id == parent.worker_id
    assert push.spec.parent_task_id == parent_task_id and push.spec.attempt_id == AttemptID(task_id, 0)
    assert push.spec.scheduling_key is None and push.spec.max_retries == 0
    assert push.spec.args == (protocol.RefArg(nested.object_id, nested.owner_worker_id),) and not push.spec.kwargs
    assert push.spec.resources == lease.resources and push.spec.return_ids() == lease.return_ids
    assert push.dependencies == grant.dependencies
    assert push.spec.return_ids() == (reference.object_id,)
    assert type(reply) is protocol.TaskReply and reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert (reply.task_id, reply.attempt_id, reply.worker_id) == (task_id, lease.attempt_id, data_node.worker_id)
    assert reply.output_publication.manifest.header.owner_worker_id == parent.worker_id
    assert reply.results[0].storage is protocol.ResultStorage.INLINE
    (release_position, request, reply), = borrow_calls(RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER)
    assert request == protocol.ReleaseOwnedObjectForTask(nested.object_id, nested.owner_worker_id, parent.worker_id, hold)
    assert type(reply) is protocol.ReleaseOwnedObjectForTaskReply
    assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id, reply.hold) == (
        request.object_id, request.owner_worker_id, request.borrower_worker_id, hold,
    )
    assert reply.accepted and reply.released and reply.error is None
    assert lease_position < report_position < ack_position < send_position < reply_position < release_position
    return len(cold), release_position


@ray.remote(num_cpus=0, resources={_PARENT_RESOURCE: 1}, max_retries=0)
def _parent_with_foreign_handle(container, descriptor, parent, data_node, deadline):
    core = current_core_worker()
    context = current_execution_context()
    nested = container[0]
    children, values = [], []
    close_deadline = None
    original_rpc = original_borrow = original_push = original_route = None
    observations = _Observations(64)
    scoring = threading.local()
    route_count = 0
    try:
        assert isinstance(core, CoreWorker) and context is not None
        assert os.getpid() == parent.worker_pid and core.worker_id == parent.worker_id
        assert core.node_id == parent.node_id and core.owner_address == parent.worker_address
        assert isinstance(nested, ray.ObjectRef) and nested.borrower_token is not None
        assert nested.object_id == descriptor.object_id and nested.owner_worker_id == descriptor.owner_worker_id
        assert nested.owner_worker_id != core.worker_id and nested.owner_address is not None
        assert isinstance(nested.borrow_source, protocol.TaskHoldSource)
        assert nested.borrow_source.hold.task_id == context.parent_task_id
        assert nested.borrow_source.hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert nested.borrow_source.hold.submitting_worker_id == nested.owner_worker_id
        with core._state_lock:
            assert core._installed_cluster_snapshot is None and not core._lease_locality_addresses
            assert not core.owner_table.contains(nested.object_id)
            assert not core._dead_nodes and core.owner_table.dead_worker_record(nested.owner_worker_id) is None
            assert tuple(core._attempt_borrow_releases) == ((nested.owner_worker_id, nested.object_id, context.parent_attempt_id),)
        original_rpc, original_borrow = core._rpc, core._borrow_rpc
        original_push, original_route = core._push_task_rpc, core._first_lease_route

        def observe_route(dependencies, *, home_route):
            nonlocal route_count
            index = route_count
            # Saturate the diagnostic counter; a regression still cannot grow
            # the ledger or replace a real route with a test-selected one.
            route_count = min(route_count + 1, 3)
            scoring.index = index
            try:
                route = original_route(dependencies, home_route=home_route)
                observations.add("route", index, home_route.address, "", dependencies, route)
                return route
            finally:
                del scoring.index

        def observe_rpc(address, handler, request):
            scope = getattr(scoring, "index", None)
            reply = original_rpc(address, handler, request)
            if handler in (GET_NODE_ADDRESS_HANDLER, REQUEST_LEASE_HANDLER, GET_OBJECT_HANDLER,
                           protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER):
                observations.add("rpc", scope, address, handler, request, reply)
            return reply

        def observe_borrow(address, handler, request):
            reply = original_borrow(address, handler, request)
            if handler in _BORROW_HANDLERS:
                observations.add("borrow", None, address, handler, request, reply)
            return reply

        def observe_push(address, handler, request):
            observations.add("push_send", None, address, handler, request, None)
            reply = original_push(address, handler, request)
            observations.add("push_reply", None, address, handler, request, reply)
            return reply

        core._rpc, core._borrow_rpc = observe_rpc, observe_borrow
        core._push_task_rpc, core._first_lease_route = observe_push, observe_route
        for index in range(2):
            _remaining(deadline)
            with core._state_lock:
                assert core._installed_cluster_snapshot is None
                assert core._lease_locality_addresses == ({} if index == 0 else {data_node.node_id: data_node.node_address})
            child = _plain_child.remote(nested)
            children.append(child)
            values.append(ray.get(child, timeout=_remaining(deadline)))
            _wait_finished(core, child, deadline)
            with core._state_lock:
                assert child.owner_worker_id == core.worker_id and child.owner_address == parent.worker_address
                assert child.borrower_token is None
                assert core.owner_table.snapshot(child.object_id).state is ObjectState.READY_INLINE
                record = core._recovery.task_record(child.object_id.task_id)
                assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
                assert record.current_attempt == AttemptID(child.object_id.task_id, 0)
                lineage = core._foreign_lineage_registry.snapshot(child.object_id.task_id)
                assert lineage is not None and lineage.output_ids == (child.object_id,)
                (edge,) = lineage.edges
                assert edge.roles == ForeignLineageRole.TOP_LEVEL
                assert (edge.dependency_object_id, edge.owner_worker_id, edge.owner_address) == (
                    nested.object_id, nested.owner_worker_id, nested.owner_address,
                )

        close_deadline = min(deadline, time.monotonic() + _CLOSE_SECONDS)
        for child in children:
            _close(child, close_deadline)
        _wait_collected(core, children, close_deadline)
        _close(nested, close_deadline)
        with core._completion:
            while core._attempt_borrow_releases:
                core._completion.wait(_remaining(close_deadline))
            assert not core._borrowed_release_obligations
            assert not core._foreign_lineage_collection_receipts and not core._foreign_lineage_prepared_collection_receipts
            assert not core._foreign_lineage_registry.has_pending_claims()
            assert not core._protocol_unresolved and core._accepted_task_count == 0
            assert core._installed_cluster_snapshot is None
            assert core._lease_locality_addresses == {data_node.node_id: data_node.node_address}
            assert not core.owner_table.contains(nested.object_id) and nested.object_id not in core._stored_descriptors
            assert not core._dead_nodes and core.owner_table.dead_worker_record(nested.owner_worker_id) is None
        calls = observations.snapshot()
        assert route_count == 2
        cold_counts, releases = [], []
        for index, child in enumerate(children):
            count, position = _child_story(calls, index, child, nested, descriptor, parent, data_node, context.parent_task_id)
            cold_counts.append(count)
            releases.append(position)
        lookups = tuple(
            (scope, address, request, reply)
            for stage, scope, address, handler, request, reply in calls
            if stage == "rpc" and handler == GET_NODE_ADDRESS_HANDLER
        )
        for _scope, address, request, reply in lookups:
            assert address == core.gcs_address and request == protocol.GetNodeAddress(data_node.node_id)
            assert type(reply) is protocol.GetNodeAddressReply and reply.found
            assert reply.node_id == data_node.node_id and reply.address == data_node.node_address
        # Per-child adoption still resolves B; those non-scoring queries must
        # not be mislabeled as locality-cache misses.
        other_lookups = sum(scope is None for scope, *_rest in lookups)
        assert other_lookups >= 2 and not any(handler == GET_OBJECT_HANDLER for _, _, _, handler, _, _ in calls)
        borrowed_releases = tuple(
            (position, address, request, reply)
            for position, (stage, _scope, address, handler, request, reply) in enumerate(calls)
            if stage == "borrow" and handler == RELEASE_BORROWED_OBJECT_HANDLER
        )
        (position, address, request, reply), = borrowed_releases
        assert all(release < position for release in releases)
        assert address == nested.owner_address
        assert request == protocol.ReleaseBorrowedObject(nested.object_id, nested.owner_worker_id, core.worker_id, nested.borrower_token)
        assert type(reply) is protocol.ReleaseBorrowedObjectReply and reply.accepted and reply.released
        assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id, reply.borrower_token) == (
            request.object_id, request.owner_worker_id, request.borrower_worker_id, request.borrower_token,
        )
        _remaining(deadline)
        # Only compact scalar/identity metadata crosses back to the Driver.
        # No Core, ObjectRef, TaskSpec, full trace or control request escapes.
        return {
            "parent": (os.getpid(), core.worker_id, core.node_id),
            "source": (nested.object_id, nested.owner_worker_id, nested.owner_address),
            "borrower_token": nested.borrower_token,
            "children": tuple(child.object_id for child in children),
            "child_owner": (core.worker_id, core.owner_address),
            "values": tuple(values),
            "cold_locality_queries": tuple(cold_counts),
            "other_address_queries": other_lookups,
            "collected_children": len(children),
            "acknowledged_releases": (len(releases), len(borrowed_releases)),
        }
    finally:
        if close_deadline is None:
            close_deadline = min(deadline, time.monotonic() + _CLOSE_SECONDS)
        try:
            close_errors = []
            for reference in (*reversed(children), nested):
                try:
                    _close(reference, close_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            if original_rpc is not None:
                core._rpc, core._borrow_rpc = original_rpc, original_borrow
                core._push_task_rpc, core._first_lease_route = original_push, original_route
        assert not close_errors, close_errors


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_worker_without_snapshot_caches_cold_locality_for_foreign_stored_dependency():
    context = core = report = None
    source = parent_ref = None
    references, cleanup_errors = [], []
    managed_pids, managed_addresses = set(), set()
    close_deadline = None
    original_rpc = original_push = None
    observations = _Observations(24)
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _PARENT_RESOURCE: 1}, {"CPU": 1, _SOURCE_RESOURCE: 1}),
            inline_threshold=_INLINE_THRESHOLD, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        parent_node, data_node = context.nodes
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
        original_rpc, original_push = core._rpc, core._push_task_rpc

        def observe_rpc(address, handler, request):
            reply = original_rpc(address, handler, request)
            if handler in (REQUEST_LEASE_HANDLER, DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER):
                observations.add("rpc", None, address, handler, request, reply)
            return reply

        def observe_push(address, handler, request):
            observations.add("push_send", None, address, handler, request, None)
            reply = original_push(address, handler, request)
            observations.add("push_reply", None, address, handler, request, reply)
            return reply

        core._rpc, core._push_task_rpc = observe_rpc, observe_push
        source = _source_data.remote()
        references.append(source)
        ready, pending = ray.wait((source,), num_returns=1, timeout=_remaining(deadline))
        assert ready == [source] and pending == []
        _wait_finished(core, source, deadline)
        with core._state_lock:
            initial = core.owner_table.snapshot(source.object_id)
            canonical = core._stored_descriptors[source.object_id]
        assert source.owner_worker_id == core.worker_id and source.borrower_token is None
        assert source.owner_address == owner_address and initial.state is ObjectState.READY_STORED
        assert initial.locations == frozenset({data_node.node_id})
        assert initial.canonical_stored_result == canonical and canonical.node_id == data_node.node_id
        assert canonical.storage is protocol.ResultStorage.OBJECT_STORE and canonical.inline_data is None
        assert _PAYLOAD_BYTES <= canonical.size_bytes < 64 * 1024
        descriptor = protocol.ObjectStoreDescriptor(
            source.object_id, core.worker_id, initial.current_attempt, data_node.node_id, canonical.size_bytes, canonical.checksum,
        )
        # Neither Driver nor parent get() this source. The nested list is an
        # argument-lifetime edge, not an instruction to materialize bytes on A.
        parent_ref = _parent_with_foreign_handle.remote([source], descriptor, parent_node, data_node, deadline)
        references.append(parent_ref)
        metadata = ray.get(parent_ref, timeout=_remaining(deadline))
        _wait_finished(core, parent_ref, deadline)
        assert metadata["parent"] == (parent_node.worker_pid, parent_node.worker_id, parent_node.node_id)
        assert metadata["source"] == (source.object_id, source.owner_worker_id, owner_address)
        assert metadata["child_owner"] == (parent_node.worker_id, parent_node.worker_address)
        assert metadata["cold_locality_queries"] == (1, 0) and metadata["other_address_queries"] >= 2
        assert metadata["collected_children"] == 2 and metadata["acknowledged_releases"] == (2, 1)
        expected_value = (data_node.worker_pid, data_node.worker_pid, _PAYLOAD_BYTES, hashlib.sha256(_PAYLOAD_BYTE * _PAYLOAD_BYTES).hexdigest())
        assert metadata["values"] == (expected_value, expected_value)
        child_ids = metadata["children"]
        assert len(child_ids) == len(set(child_ids)) == 2
        expected_holds = frozenset(
            protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.RETAINED, parent_node.worker_id, object_id.task_id, AttemptID(object_id.task_id, 0))
            for object_id in child_ids
        )
        with core._state_lock:
            parent_snapshot = core.owner_table.snapshot(parent_ref.object_id)
            assert parent_snapshot.state is ObjectState.READY_INLINE
            assert parent_snapshot.inline_data is not None and len(parent_snapshot.inline_data) <= _INLINE_THRESHOLD
            spec = parent_snapshot.producer_task_spec
            assert all(isinstance(argument, protocol.InlineArg) for argument in spec.args)
            (transfer,) = spec.args[0].nested_refs
            assert (transfer.object_id, transfer.owner_worker_id, transfer.owner_address) == (source.object_id, core.worker_id, owner_address)
            assert transfer.hold == protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id, parent_ref.object_id.task_id, spec.attempt_id)
            after_parent = core.owner_table.snapshot(source.object_id)
            assert after_parent.state is ObjectState.READY_STORED and after_parent.canonical_stored_result == canonical
            assert after_parent.current_attempt == initial.current_attempt and after_parent.locations == initial.locations
            assert after_parent.local_tokens == initial.local_tokens
            assert not after_parent.submitted_tokens and not after_parent.borrowed_tokens and not after_parent.retained_tokens
            assert not after_parent.contained_holds and not after_parent.borrowed_sources
            assert after_parent.released_borrowed_tokens == frozenset({(parent_node.worker_id, metadata["borrower_token"])})
            assert after_parent.released_retained_tokens == expected_holds
            (lineage_edge,) = parent_snapshot.outgoing_lineage_edges
            assert lineage_edge.producer_object_id == parent_ref.object_id and lineage_edge.dependency_object_id == source.object_id
            assert after_parent.lineage_tokens == frozenset({lineage_edge.token})
            assert not core._protocol_unresolved and core._accepted_task_count == 0
            assert not core._dead_nodes and core.owner_table.dead_worker_record(parent_node.worker_id) is None
            for reference in references:
                record = core._recovery.task_record(reference.object_id.task_id)
                assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
                assert record.current_attempt == AttemptID(reference.object_id.task_id, 0)
        calls = observations.snapshot()
        driver_pushes = tuple(
            request.spec.task_id for stage, _, _, handler, request, _ in calls
            if stage == "push_send" and handler == PUSH_TASK_HANDLER
        )
        assert driver_pushes == (source.object_id.task_id, parent_ref.object_id.task_id)
        parent_leases = tuple(
            (address, request, reply) for stage, _, address, handler, request, reply in calls
            if stage == "rpc" and handler == REQUEST_LEASE_HANDLER and request.task_id == parent_ref.object_id.task_id
        )
        (address, request, grant), = parent_leases
        assert address == parent_node.node_address and not request.dependencies
        assert request.target_node_id is None and request.preferred_node_id == parent_node.node_id
        assert request.requester_worker_id == core.worker_id
        assert request.resources.get("CPU", 0) == 0 and request.resources.get(_PARENT_RESOURCE) == 1
        assert type(grant) is protocol.GrantWorkerLease and grant.worker_id == parent_node.worker_id
        parent_pushes = tuple(
            (address, push) for stage, _, address, handler, push, _ in calls
            if stage == "push_send" and handler == PUSH_TASK_HANDLER and push.spec.task_id == parent_ref.object_id.task_id
        )
        (address, push), = parent_pushes
        assert address == parent_node.worker_address and push.spec == spec
        assert push.worker_id == grant.worker_id and push.lease_id == grant.lease_id
        assert not push.dependencies and push.spec.owner_worker_id == core.worker_id
        assert not any(handler == GET_OBJECT_HANDLER for _, _, _, handler, _, _ in calls)
        assert all(_pid_exists(pid) for pid in context.worker_pids)

        close_deadline = time.monotonic() + _CLOSE_SECONDS
        _close(parent_ref, close_deadline)
        _wait_collected(core, (parent_ref,), close_deadline)
        with core._state_lock:
            rooted = core.owner_table.snapshot(source.object_id)
            assert rooted.local_tokens == initial.local_tokens and not source.closed
            assert not rooted.submitted_tokens and not rooted.borrowed_tokens and not rooted.retained_tokens and not rooted.lineage_tokens
            assert rooted.locations == initial.locations
        _close(source, close_deadline)
        _wait_collected(core, references, close_deadline)
        drops = tuple(
            (address, request, reply) for stage, _, address, handler, request, reply in observations.snapshot()
            if stage == "rpc" and handler == DROP_OBJECT_REPLICA_HANDLER and request.object_id == source.object_id
        )
        assert drops
        acknowledged_nodes = set()
        for address, request, reply in drops:
            assert address == data_node.node_address and request.node_id == data_node.node_id
            assert request.owner_worker_id == core.worker_id and request.producer_attempt_id == initial.current_attempt and request.checksum == canonical.checksum
            assert type(reply) is protocol.DropObjectReplicaReply
            assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
                request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
            )
            if reply.status in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED):
                acknowledged_nodes.add(reply.node_id)
        assert acknowledged_nodes == {data_node.node_id}
        remaining = _remaining(close_deadline)
        absent = rpc_request(
            data_node.node_address, GET_OBJECT_HANDLER,
            protocol.GetObject(source.object_id, parent_node.node_id, initial.current_attempt, core.worker_id, canonical.size_bytes, canonical.checksum),
            connect_timeout=min(0.2, remaining / 2), request_timeout=remaining / 2, deadline=close_deadline,
        )
        assert type(absent) is protocol.GetObjectReply and absent.object_id == source.object_id and absent.node_id == data_node.node_id
        assert not absent.found and not absent.sealed and absent.data is None
        assert absent.checksum is None and absent.producer_attempt_id is None
        assert absent.owner_worker_id is None and absent.size_bytes is None
        assert all(_pid_exists(pid) for pid in context.worker_pids)
        with core._state_lock:
            assert not core._dead_nodes and core.owner_table.dead_worker_record(parent_node.worker_id) is None
            assert not core._protocol_unresolved and core._accepted_task_count == 0
    finally:
        if close_deadline is None:
            close_deadline = time.monotonic() + _CLOSE_SECONDS
        try:
            for reference in reversed(references):
                try:
                    _close(reference, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if original_rpc is not None:
                    core._rpc, core._push_task_rpc = original_rpc, original_push
                alive = tuple(pid for pid in managed_pids if _pid_exists(pid))
                children = tuple(child.pid for child in mp.active_children() if child.pid in managed_pids)
                open_addresses = []
                for address in managed_addresses:
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            open_addresses.append(address)
                    except OSError:
                        pass
                assert not alive and not children and not open_addresses, (alive, children, open_addresses)
                assert not cleanup_errors, cleanup_errors
                observations.snapshot()
                assert not ray.is_initialized()
                if context is not None:
                    assert report is not None and report.gcs_pid == context.gcs_pid
                    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
                    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
                    assert report.node_clean and report.worker_clean
                    assert all(code == 0 for code in report.node_exitcodes + report.worker_exitcodes)
                    assert report.finalized and report.shutdown_ack_clean and report.resources_clean and not report.forced
