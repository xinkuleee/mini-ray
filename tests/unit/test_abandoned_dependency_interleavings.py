"""Pure bounded interleavings of Node-owned abandoned input custody.

Two 1 KiB passive Node stores and at most two threadless owner Cores. One
actual lease, two tiny inputs at most, and three explicit supervisor drives
exercise one-replica progress, a delayed normal ACK, and a contended request
lock. No threads, waits, processes, sockets, GCS service or user code runs.
"""

from copy import deepcopy
from dataclasses import replace
import threading

import pytest

from miniray import node as node_module, protocol
from miniray.core import _ObjectWaiter
from miniray.ids import AttemptID, LeaseID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_abandoned_dependency_node import _Fixture
from tests.unit.test_cancelled_grant_inventory import _NodeFixture
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


def test_failed_first_owner_and_gcs_lookup_do_not_starve_a_later_owner(monkeypatch):
    nodes = _NodeFixture(monkeypatch, commit=False)
    owners = (make_pure_core(), make_pure_core())
    submitter = WorkerID(b"d" * 16)
    gcs = ("typed-death-input.invalid", 2401)
    addresses = (("first-owner.invalid", 2402), ("second-owner.invalid", 2403))
    death = protocol.WorkerDeathRecord(
        "two-owner-submitter-exit", protocol.WorkerIncarnation(nodes.source.node_id, 2501, 1, submitter, 2601),
        1, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    hold = protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.RETAINED, submitter, nodes.request.task_id, nodes.request.attempt_id)
    routes, reports, queries, drops = [], [], [], []
    first_failed = False
    request = grant = None

    def owner_rpc(address, handler, message):
        if address == gcs:
            assert handler == "get_worker_deaths" and message.after_epoch in (0, 1)
            return protocol.GetWorkerDeathsReply(message.after_epoch, 1, (death,) if message.after_epoch == 0 else ())
        node = nodes.source if address == nodes.source_address else nodes.target
        assert handler == node_module.DROP_OBJECT_REPLICA_HANDLER
        assert message.node_id == node.node_id
        reply = node._handle_drop_object_replica(message)
        assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
        assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
            message.object_id, message.producer_attempt_id, message.owner_worker_id, message.node_id, message.checksum,
        )
        drops.append(message)
        assert len(drops) <= 4
        return reply

    try:
        for index, (owner, descriptor) in enumerate(zip(owners, nodes.descriptors)):
            owner.worker_id = descriptor.owner_worker_id
            owner.owner_address = addresses[index]
            owner.gcs_address = gcs
            owner._rpc = owner_rpc
            owner._resolve_node_address = lambda node_id, *, home_route=None: nodes.source_address if node_id == nodes.source.node_id else ("target.invalid", 2404)
            owner.owner_table.register(descriptor.object_id, current_attempt=descriptor.producer_attempt_id, local_token="owner-live-ref")
            result = protocol.ResultDescriptor(descriptor.object_id, protocol.ResultStorage.OBJECT_STORE, descriptor.size_bytes,
                                               descriptor.owner_worker_id, descriptor.node_id, descriptor.checksum)
            assert owner.owner_table.publish_stored(descriptor.object_id, descriptor.producer_attempt_id, descriptor.node_id, descriptor=result)
            owner._stored_descriptors[descriptor.object_id] = result
            owner._objects[descriptor.object_id] = _ObjectWaiter(threading.Event())
            owner._objects[descriptor.object_id].event.set()
            owner._recovery.register_put(descriptor.object_id)
            borrower = (submitter, "borrow-{}".format(index))
            assert owner.owner_table.add_borrowed_reference(descriptor.object_id, borrower)
            assert owner.owner_table.retain_borrowed_reference_for_task(descriptor.object_id, borrower, hold)
            assert owner.owner_table.release_borrowed_reference(descriptor.object_id, borrower)
            routes.append(protocol.DependencyOwnerRoute(descriptor.object_id, owner.worker_id, addresses[index], hold))
        request = replace(nodes.request, requester_worker_id=submitter, dependency_owner_routes=tuple(routes))
        grant = nodes.target._handle_request_lease(request)
        assert type(grant) is protocol.GrantWorkerLease and len(nodes.transfers) == 6
        registry = nodes.target._dependency_custody_registry_locked()
        inventory = registry.snapshot(request.lease_id)

        def background(address, handler, message, **kwargs):
            nonlocal first_failed
            assert not nodes.target._state_lock._is_owned()
            if address == gcs:
                assert handler == node_module.GCS_GET_WORKER_STATE_HANDLER
                queries.append(message.worker_id)
                assert len(queries) <= 2
                if message.worker_id == submitter:
                    return protocol.GetWorkerStateReply(submitter, True, 1, protocol.WorkerMembershipState.DEAD, death.incarnation, death)
                assert message.worker_id == owners[0].worker_id and first_failed
                raise TransportTimeout("first owner's GCS lookup also failed")
            assert handler == protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER
            assert kwargs["connect_timeout"] == 0.25 and kwargs["request_timeout"] == 0.5
            assert type(kwargs["deadline"]) is float
            index = addresses.index(address)
            assert message.inventory == inventory and message.descriptor == inventory.descriptors[index]
            reports.append(index)
            assert len(reports) <= 3
            if index == 0 and not first_failed:
                first_failed = True
                raise TransportTimeout("first owner report unavailable")
            return owners[index].report_abandoned_dependency_replica(message)

        nodes.target._gcs_address = gcs
        nodes.target._registered_with_gcs = False
        nodes.target._background_rpc = background
        assert not nodes.target._drive_abandoned_dependency_custody(force=True)
        assert reports == [0] and queries == [submitter, owners[0].worker_id]
        assert registry.receipt(request.lease_id, inventory.descriptors[0].object_id) is None
        assert registry.receipt(request.lease_id, inventory.descriptors[1].object_id) is None
        assert not nodes.target._drive_abandoned_dependency_custody(force=True)
        assert reports == [0, 1] and len(queries) == 2
        second_receipt = registry.receipt(request.lease_id, inventory.descriptors[1].object_id)
        assert second_receipt.custody_transferred
        assert registry.has_pending() and registry.receipt(request.lease_id, inventory.descriptors[0].object_id) is None
        assert nodes.target._drive_abandoned_dependency_custody(force=True)
        assert reports == [0, 1, 0] and len(queries) == 2
        entry = registry._entries[request.lease_id]
        assert entry.abandoned_complete and entry.acknowledged is None and not registry.has_pending()
        assert registry.receipt(request.lease_id, inventory.descriptors[1].object_id) == second_receipt
        assert nodes.target._leases[request.lease_id].state is protocol.LeaseExecutionState.ABANDONED
        for owner, descriptor in zip(owners, nodes.descriptors):
            assert owner.owner_table.snapshot(descriptor.object_id).locations == frozenset((nodes.source.node_id, nodes.target.node_id))
            assert owner.owner_table.release_local_reference(descriptor.object_id, "owner-live-ref")
            owner._reference_released(descriptor.object_id)
            assert owner._reference_mailbox.pending.qsize() <= 4
            owner._reference_mailbox.drain()
            assert owner.owner_table.collection_state(descriptor.object_id) is ObjectCollectionState.COLLECTED
        assert len(drops) == 4 and nodes.target.object_store.used_bytes == nodes.source.object_store.used_bytes == 0
    finally:
        if request is not None and grant is not None and nodes.target._leases[request.lease_id].state is protocol.LeaseExecutionState.GRANTED:
            nodes.target._handle_cancel_worker_lease(protocol.CancelWorkerLease(
                request.lease_id, request.task_id, request.attempt_id, request.requester_node_id, request.requester_worker_id, lease_request=request,
            ))
        for owner in owners:
            for object_id in tuple(owner._objects):
                owner.owner_table.release_retained_reference_for_task(object_id, hold)
                for token in owner.owner_table.snapshot(object_id).local_tokens:
                    owner.owner_table.release_local_reference(object_id, token)
            close_pure_core(owner)


def test_normal_custody_ack_interleaves_with_node_owner_report_without_losing_its_receipt(monkeypatch):
    f = _Fixture(monkeypatch)
    try:
        original = f.background_rpc
        normal = protocol.AckLeaseDependencyCustody(f.submitter, f.inventory)
        observed = []

        def report_then_normal_ack(address, handler, request, **kwargs):
            result = original(address, handler, request, **kwargs)
            if handler == protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER:
                assert result.custody_transferred and not observed
                registry = f.registry()
                entry = registry._entries[f.request.lease_id]
                assert entry.acknowledged is None and not entry.abandoned_complete
                assert not entry.owner_receipts
                # Delayed normal ACK was sent by the submitter before death;
                # all exact owner custody is already real when it arrives.
                ack = f.target._handle_ack_lease_dependency_custody(normal)
                assert ack.accepted and ack.request == normal
                assert entry.acknowledged == f.inventory and not entry.abandoned_complete
                assert not registry.has_pending()
                with f.target._state_lock:
                    assert not f.target._cleanup_plane_quiescent_locked(), "live owner-RPC ticket was ignored by drain"
                observed.append((deepcopy(result), ack))
            return result

        f.target._background_rpc = report_then_normal_ack
        assert f.drive()
        entry = f.registry()._entries[f.request.lease_id]
        assert entry.abandoned_complete and entry.acknowledged == f.inventory
        assert entry.submitter_death == f.death and len(entry.owner_receipts) == 1
        assert f.registry().receipt(f.request.lease_id, f.sources[0].object_id) == observed[0][0]
        assert not f.registry().has_pending() and not f.target._dependency_handoff_drivers
        assert f.target._handle_ack_lease_dependency_custody(normal) == observed[0][1]
        assert not f.drive() and len(f.reports) == 1
        f.collect()
    finally:
        f.close()


def test_death_cached_while_request_lock_busy_fences_start_and_new_request(monkeypatch):
    f = _Fixture(monkeypatch)
    lock = f.target._lease_request_locks[f.request.lease_id]
    held = False
    try:
        assert lock.acquire(blocking=False)
        held = True
        assert not f.drive()
        assert f.target._dead_dependency_submitters[f.submitter] == f.death
        assert f.registry()._entries[f.request.lease_id].submitter_death is None
        assert not f.reports and not f.target._dependency_handoff_drivers
        assert f.target._leases[f.request.lease_id].state is protocol.LeaseExecutionState.GRANTED
        start = f.target._handle_start_worker_lease(protocol.StartWorkerLease(
            f.reply.lease_id, f.reply.task_id, f.reply.attempt_id, f.reply.worker_id,
        ))
        assert not start.accepted and start.state is protocol.LeaseExecutionState.ABANDONED
        task = TaskID(b"n" * 16)
        hold = replace(f.hold, task_id=task, origin_attempt_id=AttemptID(task, 0))
        new = replace(f.request, lease_id=LeaseID(b"n" * 16), task_id=task, attempt_id=AttemptID(task, 0),
                      return_ids=(ObjectID.for_task(task),), dependency_owner_routes=tuple(
                          replace(route, hold=hold) for route in f.request.dependency_owner_routes))
        rejected = f.target._handle_request_lease(new)
        assert type(rejected) is protocol.RejectWorkerLease and rejected.reason is protocol.LeaseRejectReason.STALE_ATTEMPT
        assert f.registry().request(new.lease_id) is None and len(f.nodes.transfers) == 3
        assert not f.reports and f.registry().has_pending()
        lock.release()
        held = False
        assert f.drive()
        f.assert_custody_completed()
        assert f.worker_queries == [f.submitter] and len(f.reports) == 1
        assert f.target.resource_ledger.available == f.target.resource_ledger.total
        f.collect()
    finally:
        if held:
            lock.release()
        f.close()
