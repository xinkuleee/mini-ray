"""Pure Node pull contracts plus one explicitly bounded concurrent smoke."""

from __future__ import annotations

import hashlib
import math
import threading
import time

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import (
    GET_OBJECT_CHUNK_HANDLER,
    OBJECT_TRANSFER_CHUNK_BYTES,
    PIN_OBJECT_HANDLER,
    RELEASE_OBJECT_PIN_HANDLER,
    NodeServer,
    _WorkerSlot,
)
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.resources import HybridPolicy, ResourceLedger, ResourceVector
from miniray.transport import TransportError


# Pure reducers are marked individually; real threads/sockets are bounded
# loopback smoke tests and must not inherit the default unit marker.


def _id(id_type: type, byte: int):
    return id_type(bytes([byte]) * 16)


def _worker_id_for_node(node_id: NodeID) -> WorkerID:
    """Map the full NodeID bijectively to a deterministic test WorkerID."""

    return WorkerID(bytes(value ^ 0xA5 for value in bytes(node_id)))


def _object_descriptor(
    source_node_id: NodeID, payload: bytes
) -> protocol.ObjectStoreDescriptor:
    job_id = _id(JobID, 10)
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return protocol.ObjectStoreDescriptor(
        object_id=ObjectID.for_task(task_id, 0),
        owner_worker_id=_id(WorkerID, 11),
        producer_attempt_id=AttemptID(task_id, 0),
        node_id=source_node_id,
        size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _bare_node(node_id: NodeID, total: ResourceVector) -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = node_id
    worker_id = _worker_id_for_node(node_id)
    node._worker_order = (worker_id,)
    node._workers = {worker_id: _WorkerSlot(worker_id)}
    node.num_workers_per_node = 1
    node._state_lock = threading.RLock()
    node._scheduling_lock = threading.Lock()
    node._ledger = ResourceLedger(total)
    node._object_store = ObjectStore(512 * 1024)
    node._object_manager = ObjectManager(node_id, node._object_store)
    node._sealed_metadata = {}
    node._pinned_transfers = {}
    node._object_localization_locks = {}
    node._leases = {}
    node._lease_outcomes = {}
    node._lease_request_locks = {}
    node._inflight_lease_requests = 0
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._gcs_address = None
    node._scheduling_policy = HybridPolicy(seed=0)
    node._cluster_nodes = ()
    node._cluster_addresses = {}
    return node


@pytest.mark.unit
def test_source_pin_chunk_and_release_validate_one_session() -> None:
    source_id = _id(NodeID, 1)
    requester_id = _id(NodeID, 2)
    payload = b"p" * (OBJECT_TRANSFER_CHUNK_BYTES + 7)
    descriptor = _object_descriptor(source_id, payload)
    source = _bare_node(source_id, ResourceVector({"CPU": 1}))
    source._object_store.put(descriptor.object_id, payload)
    source._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )

    pin = protocol.PinObjectForTransfer("transfer-1", descriptor, requester_id)
    first = source._handle_pin_object_for_transfer(pin)
    assert first.pinned
    assert source._handle_pin_object_for_transfer(pin) == first
    assert source.object_store.snapshot(descriptor.object_id).pin_count == 1

    oversized = source._handle_get_object_chunk(
        protocol.GetObjectChunk(
            "transfer-1",
            descriptor.object_id,
            requester_id,
            0,
            OBJECT_TRANSFER_CHUNK_BYTES + 1,
        )
    )
    assert not oversized.ok and oversized.data == b""
    chunk = source._handle_get_object_chunk(
        protocol.GetObjectChunk(
            "transfer-1", descriptor.object_id, requester_id, 0, 8
        )
    )
    assert chunk.ok and chunk.data == payload[:8]

    release = protocol.ReleaseObjectPin(
        "transfer-1", descriptor.object_id, requester_id
    )
    first_release = source._handle_release_object_pin(release)
    replay = source._handle_release_object_pin(release)
    assert first_release.accepted and first_release.released
    assert replay.accepted and not replay.released
    assert source.object_store.snapshot(descriptor.object_id).pin_count == 0


class _AliveWorker:
    def is_alive(self) -> bool:
        return True


def _assert_pull_rpc_deadline(handler: str, options: dict[str, object]) -> None:
    """Validate the bounded source-pin close seam without doing transport."""

    if handler != RELEASE_OBJECT_PIN_HANDLER:
        assert options == {}
        return
    assert set(options) == {"connect_timeout", "request_timeout", "deadline"}
    assert all(
        type(value) in (int, float) and math.isfinite(value) and value > 0
        for value in options.values()
    )
    assert options["connect_timeout"] == 0.25
    assert options["request_timeout"] == 0.5
    # This must be a future absolute monotonic deadline, not another relative
    # socket timeout or an omitted bound accepted by a permissive fake.
    assert 0 < options["deadline"] - time.monotonic() <= 0.75


@pytest.mark.unit
def test_target_seals_dependency_before_allocating_and_granting(monkeypatch) -> None:
    source_id = _id(NodeID, 3)
    target_id = _id(NodeID, 4)
    payload = b"d" * (OBJECT_TRANSFER_CHUNK_BYTES * 2 + 9)
    descriptor = _object_descriptor(source_id, payload)
    source = _bare_node(source_id, ResourceVector({"CPU": 1}))
    source._object_store.put(descriptor.object_id, payload)
    source._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )
    target = _bare_node(target_id, ResourceVector({"CPU": 1, "target": 1}))
    target._workers[target.worker_id].process = _AliveWorker()
    target._workers[target.worker_id].address = ("127.0.0.1", 13004)
    source_address = ("127.0.0.1", 12003)
    target._cluster_addresses = {source_id: source_address}

    calls: list[str] = []

    def fake_rpc(address, handler, message, **options):
        _assert_pull_rpc_deadline(handler, options)
        assert address == source_address
        # Pulling precedes resource allocation, even across all chunk RPCs.
        assert target.resource_ledger.available == target.resource_ledger.total
        calls.append(handler)
        if handler == PIN_OBJECT_HANDLER:
            return source._handle_pin_object_for_transfer(message)
        if handler == GET_OBJECT_CHUNK_HANDLER:
            return source._handle_get_object_chunk(message)
        if handler == RELEASE_OBJECT_PIN_HANDLER:
            return source._handle_release_object_pin(message)
        raise AssertionError(handler)

    monkeypatch.setattr("miniray.node.rpc_request", fake_rpc)
    consumer_job = _id(JobID, 20)
    consumer_task = TaskID.derive(
        consumer_job, TaskID.for_driver(consumer_job), 0
    )
    request = protocol.RequestWorkerLease(
        lease_id=_id(LeaseID, 21),
        task_id=consumer_task,
        attempt_id=AttemptID(consumer_task, 0),
        resources=ResourceVector({"CPU": 1, "target": 1}),
        requester_node_id=source_id,
        requester_worker_id=_id(WorkerID, 22),
        target_node_id=target_id,
        dependencies=(descriptor,),
    )

    grant = target._handle_request_lease(request)

    assert isinstance(grant, protocol.GrantWorkerLease)
    assert grant.node_id == target_id
    assert grant.dependencies == (
        protocol.ObjectStoreDescriptor(
            descriptor.object_id,
            descriptor.owner_worker_id,
            descriptor.producer_attempt_id,
            target_id,
            descriptor.size_bytes,
            descriptor.checksum,
        ),
    )
    assert target.object_store.get(descriptor.object_id) == payload
    assert target.object_store.snapshot(descriptor.object_id).pin_count == 1
    assert target.resource_ledger.available.is_zero()
    assert calls[0] == PIN_OBJECT_HANDLER
    assert calls[-1] == RELEASE_OBJECT_PIN_HANDLER
    assert calls.count(GET_OBJECT_CHUNK_HANDLER) == 3
    assert source.object_store.snapshot(descriptor.object_id).pin_count == 0

    # The outcome cache prevents a duplicate lease request from pulling again.
    assert target._handle_request_lease(request) == grant
    assert calls.count(PIN_OBJECT_HANDLER) == 1

    release = target._handle_release_lease(
        protocol.ReleaseWorkerLease(
            grant.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert release.released
    assert target.object_store.snapshot(descriptor.object_id).pin_count == 0


@pytest.mark.loopback_smoke
def test_concurrent_localizers_pull_once_and_second_uses_local_replica(monkeypatch) -> None:
    source_id = _id(NodeID, 5)
    target_id = _id(NodeID, 6)
    payload = b"c" * (OBJECT_TRANSFER_CHUNK_BYTES + 3)
    descriptor = _object_descriptor(source_id, payload)
    source = _bare_node(source_id, ResourceVector({"CPU": 1}))
    source._object_store.put(descriptor.object_id, payload)
    source._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )
    target = _bare_node(target_id, ResourceVector({"CPU": 1}))
    source_address = ("127.0.0.1", 12005)
    target._cluster_addresses = {source_id: source_address}
    calls: list[str] = []
    first_pin = threading.Event()
    second_contending = threading.Event()
    allow_pull = threading.Event()
    real_object_lock = threading.Lock()
    lock_entries = 0
    entry_guard = threading.Lock()

    class _ObservedObjectLock:
        """Observe contention while preserving a real serialization lock."""

        def __enter__(self):
            nonlocal lock_entries
            with entry_guard:
                lock_entries += 1
                entry = lock_entries
            if entry == 2:
                assert first_pin.is_set()
                second_contending.set()
            if not real_object_lock.acquire(timeout=2.0):
                raise TimeoutError("localization serialization lock did not release")
            return self

        def __exit__(self, *_args):
            real_object_lock.release()

    target._object_localization_locks[descriptor.object_id] = _ObservedObjectLock()

    def fake_rpc(address, handler, message, **options):
        _assert_pull_rpc_deadline(handler, options)
        assert address == source_address
        calls.append(handler)
        if handler == PIN_OBJECT_HANDLER:
            first_pin.set()
            assert allow_pull.wait(2.0)
            return source._handle_pin_object_for_transfer(message)
        if handler == GET_OBJECT_CHUNK_HANDLER:
            return source._handle_get_object_chunk(message)
        assert handler == RELEASE_OBJECT_PIN_HANDLER
        return source._handle_release_object_pin(message)

    monkeypatch.setattr("miniray.node.rpc_request", fake_rpc)
    results: list[protocol.ObjectStoreDescriptor] = []
    failures: list[BaseException] = []

    def localize():
        try:
            results.extend(target._localize_dependencies((descriptor,)))
        except BaseException as exc:
            failures.append(exc)

    threads = [
        threading.Thread(target=localize, daemon=True)
        for _ in range(2)
    ]
    started = []
    try:
        deadline = time.monotonic() + 3.0
        threads[0].start()
        started.append(threads[0])
        assert first_pin.wait(1.0)
        threads[1].start()
        started.append(threads[1])
        assert second_contending.wait(1.0)
        assert len(results) == 0
        # Both real threads have entered localization, but only one owns the
        # per-object lock.  Releasing this ACK lets one pull finish and the
        # contender prove it reuses the already-sealed replica.
        allow_pull.set()
        for thread in started:
            thread.join(max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in started)
        assert failures == []
        assert len(results) == 2 and results[0] == results[1]
        assert lock_entries == 2
        assert calls.count(PIN_OBJECT_HANDLER) == 1
        assert calls.count(GET_OBJECT_CHUNK_HANDLER) == 2
        assert source.object_store.snapshot(descriptor.object_id).pin_count == 0
        assert target.object_store.snapshot(descriptor.object_id).pin_count == 0
    finally:
        allow_pull.set()
        cleanup_deadline = time.monotonic() + 1.0
        for thread in threads:
            if thread.ident is not None:
                thread.join(max(0.0, cleanup_deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in threads)
        source._release_all_transfer_pins()
        for node in (source, target):
            node.object_store.delete(descriptor.object_id)
            assert not node.object_store.contains(descriptor.object_id, sealed_only=False)
            assert node.resource_ledger.available == node.resource_ledger.total


@pytest.mark.unit
def test_pin_release_retries_transport_errors_and_requires_valid_ack(monkeypatch) -> None:
    source_id = _id(NodeID, 7)
    target_id = _id(NodeID, 8)
    payload = b"r" * 8
    descriptor = _object_descriptor(source_id, payload)
    source = _bare_node(source_id, ResourceVector({"CPU": 1}))
    source._object_store.put(descriptor.object_id, payload)
    source._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )
    target = _bare_node(target_id, ResourceVector({"CPU": 1}))
    source_address = ("127.0.0.1", 12007)
    target._cluster_addresses = {source_id: source_address}
    releases = 0

    def fake_rpc(address, handler, message, **options):
        nonlocal releases
        _assert_pull_rpc_deadline(handler, options)
        assert address == source_address
        if handler == PIN_OBJECT_HANDLER:
            return source._handle_pin_object_for_transfer(message)
        if handler == GET_OBJECT_CHUNK_HANDLER:
            return source._handle_get_object_chunk(message)
        releases += 1
        if releases < 3:
            raise TransportError("ambiguous release")
        return source._handle_release_object_pin(message)

    monkeypatch.setattr("miniray.node.rpc_request", fake_rpc)
    assert target._localize_dependencies((descriptor,))[0].node_id == target_id
    assert releases == 3
    assert source.object_store.snapshot(descriptor.object_id).pin_count == 0


@pytest.mark.unit
def test_source_shutdown_cleanup_releases_every_active_pin() -> None:
    source_id = _id(NodeID, 9)
    payload = b"s" * 4
    descriptor = _object_descriptor(source_id, payload)
    source = _bare_node(source_id, ResourceVector({"CPU": 1}))
    source._object_store.put(descriptor.object_id, payload)
    source._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )
    pin = protocol.PinObjectForTransfer(
        "shutdown-pin", descriptor, _id(NodeID, 10)
    )
    assert source._handle_pin_object_for_transfer(pin).pinned

    assert source._release_all_transfer_pins() == 1
    assert source._release_all_transfer_pins() == 0
    assert source.object_store.snapshot(descriptor.object_id).pin_count == 0


@pytest.mark.unit
def test_get_object_fences_expectations_and_returns_producer_metadata() -> None:
    node_id = _id(NodeID, 11)
    payload = b"metadata-fenced"
    descriptor = _object_descriptor(node_id, payload)
    node = _bare_node(node_id, ResourceVector({"CPU": 1}))
    node._object_store.put(descriptor.object_id, payload)
    node._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )
    exact = protocol.GetObject(
        descriptor.object_id,
        node_id,
        descriptor.producer_attempt_id,
        descriptor.owner_worker_id,
        descriptor.size_bytes,
        descriptor.checksum,
    )
    reply = node._handle_get_object(exact)
    assert reply.found and reply.sealed and reply.data == payload
    assert reply.producer_attempt_id == descriptor.producer_attempt_id
    assert reply.owner_worker_id == descriptor.owner_worker_id
    assert reply.size_bytes == descriptor.size_bytes

    mismatched = protocol.GetObject(
        descriptor.object_id,
        node_id,
        descriptor.producer_attempt_id,
        _id(WorkerID, 12),
        descriptor.size_bytes,
        descriptor.checksum,
    )
    rejected = node._handle_get_object(mismatched)
    assert not rejected.found and not rejected.sealed
    assert rejected.data is None

    # Defensive validation prevents corrupted physical bytes from being exposed
    # even if the logical metadata table still looks valid.
    node._object_store._entries[descriptor.object_id].sealed_data = b"corrupt"
    corrupt = node._handle_get_object(
        protocol.GetObject(descriptor.object_id, node_id)
    )
    assert not corrupt.found and not corrupt.sealed


@pytest.mark.unit
def test_local_same_bytes_from_another_epoch_never_satisfy_old_descriptor() -> None:
    source_id = _id(NodeID, 13)
    target_id = _id(NodeID, 14)
    payload = b"same-physical-bytes"
    old = _object_descriptor(source_id, payload)
    target = _bare_node(target_id, ResourceVector({"CPU": 1}))
    target._object_store.put(old.object_id, payload)
    new_attempt = old.producer_attempt_id.next()
    target._sealed_metadata[old.object_id] = (
        new_attempt, old.owner_worker_id, len(payload), old.checksum,
    )
    target._cluster_addresses = {source_id: ("127.0.0.1", 12013)}

    with pytest.raises(RuntimeError, match="conflicts with producer metadata"):
        target._localize_one_dependency(old)

    # The same rule applies to an owner mismatch even when attempt, size, and
    # checksum are otherwise byte-for-byte identical.
    target._sealed_metadata[old.object_id] = (
        old.producer_attempt_id, _id(WorkerID, 15), len(payload), old.checksum,
    )
    with pytest.raises(RuntimeError, match="conflicts with producer metadata"):
        target._localize_one_dependency(old)


@pytest.mark.unit
def test_grant_constructor_failure_rolls_back_allocation_pin_and_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_id = _id(NodeID, 16)
    payload = b"transactional-grant"
    descriptor = _object_descriptor(node_id, payload)
    node = _bare_node(node_id, ResourceVector({"CPU": 1}))
    node._workers[node.worker_id].process = _AliveWorker()
    node._workers[node.worker_id].address = ("127.0.0.1", 13016)
    node._object_store.put(descriptor.object_id, payload)
    node._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id, descriptor.owner_worker_id,
        descriptor.size_bytes, descriptor.checksum,
    )
    consumer_job = _id(JobID, 17)
    consumer_task = TaskID.derive(
        consumer_job, TaskID.for_driver(consumer_job), 0
    )
    request = protocol.RequestWorkerLease(
        _id(LeaseID, 18), consumer_task, AttemptID(consumer_task, 0),
        ResourceVector({"CPU": 1}), node_id, _id(WorkerID, 19),
        target_node_id=node_id, dependencies=(descriptor,),
    )
    real_grant = protocol.GrantWorkerLease
    monkeypatch.setattr(
        "miniray.node.protocol.GrantWorkerLease",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("grant construction failed")
        ),
    )

    reply = node._handle_request_lease_serialized(request)

    assert isinstance(reply, protocol.RejectWorkerLease)
    assert reply.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._leases == {}
    assert node._workers[node.worker_id].active_lease_id is None
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 0
    assert node._lease_outcomes == {}
    monkeypatch.setattr("miniray.node.protocol.GrantWorkerLease", real_grant)


@pytest.mark.unit
def test_epoch_change_after_localization_is_revalidated_before_target_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_id = _id(NodeID, 20)
    payload = b"epoch-race-same-bytes"
    descriptor = _object_descriptor(node_id, payload)
    node = _bare_node(node_id, ResourceVector({"CPU": 1}))
    node._workers[node.worker_id].process = _AliveWorker()
    node._workers[node.worker_id].address = ("127.0.0.1", 13020)
    node._object_store.put(descriptor.object_id, payload)
    old_metadata = (
        descriptor.producer_attempt_id, descriptor.owner_worker_id,
        descriptor.size_bytes, descriptor.checksum,
    )
    node._sealed_metadata[descriptor.object_id] = old_metadata
    consumer_job = _id(JobID, 21)
    consumer_task = TaskID.derive(
        consumer_job, TaskID.for_driver(consumer_job), 0
    )
    request = protocol.RequestWorkerLease(
        _id(LeaseID, 22), consumer_task, AttemptID(consumer_task, 0),
        ResourceVector({"CPU": 1}), node_id, _id(WorkerID, 23),
        target_node_id=node_id, dependencies=(descriptor,),
    )
    real_localize = node._localize_dependencies

    def localize_then_advance(dependencies):
        localized = real_localize(dependencies)
        node._sealed_metadata[descriptor.object_id] = (
            descriptor.producer_attempt_id.next(), descriptor.owner_worker_id,
            descriptor.size_bytes, descriptor.checksum,
        )
        return localized

    monkeypatch.setattr(node, "_localize_dependencies", localize_then_advance)

    reply = node._handle_request_lease_serialized(request)

    assert isinstance(reply, protocol.RejectWorkerLease)
    assert reply.reason is protocol.LeaseRejectReason.DEPENDENCY_UNAVAILABLE
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 0
    assert node._leases == {} and node._workers[node.worker_id].active_lease_id is None


def _grant_with_target_pin() -> tuple[
    NodeServer, protocol.RequestWorkerLease, protocol.GrantWorkerLease,
    protocol.ObjectStoreDescriptor,
]:
    node_id = _id(NodeID, 24)
    payload = b"target-pin-cleanup"
    descriptor = _object_descriptor(node_id, payload)
    node = _bare_node(node_id, ResourceVector({"CPU": 1}))
    node._workers[node.worker_id].process = _AliveWorker()
    node._workers[node.worker_id].address = ("127.0.0.1", 13100)
    node._object_store.put(descriptor.object_id, payload)
    node._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id, descriptor.owner_worker_id,
        descriptor.size_bytes, descriptor.checksum,
    )
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    request = protocol.RequestWorkerLease(
        LeaseID.random(), task_id, AttemptID(task_id, 0),
        ResourceVector({"CPU": 1}), node_id, WorkerID.random(),
        target_node_id=node_id, dependencies=(descriptor,),
    )
    grant = node._handle_request_lease(request)
    assert isinstance(grant, protocol.GrantWorkerLease)
    return node, request, grant, descriptor


@pytest.mark.unit
def test_grant_rollback_unpin_failure_becomes_retryable_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry the exact pin release without discarding pre-grant custody.

    Final owner handoff and collection are covered by
    ``test_pregrant_dependency_custody.py::
    test_busy_worker_capacity_budget_cancels_and_acknowledges_localized_inventory``.
    This Node-only case must stay unclean until that independent handoff occurs.
    """

    node_id = NodeID.random()
    payload = b"rollback-unpin"
    descriptor = _object_descriptor(node_id, payload)
    node = _bare_node(node_id, ResourceVector({"CPU": 1}))
    node._workers[node.worker_id].process = _AliveWorker()
    node._workers[node.worker_id].address = ("127.0.0.1", 13101)
    node._object_store.put(descriptor.object_id, payload)
    node._sealed_metadata[descriptor.object_id] = (
        descriptor.producer_attempt_id, descriptor.owner_worker_id,
        descriptor.size_bytes, descriptor.checksum,
    )
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    request = protocol.RequestWorkerLease(
        LeaseID.random(), task_id, AttemptID(task_id, 0),
        ResourceVector({"CPU": 1}), node_id, WorkerID.random(),
        target_node_id=node_id, dependencies=(descriptor,),
    )
    real_unpin = node._object_store.unpin
    unpins = 0

    def fail_once(object_id, token):
        nonlocal unpins
        unpins += 1
        if unpins == 1:
            raise RuntimeError("transient unpin")
        return real_unpin(object_id, token)

    monkeypatch.setattr(node._object_store, "unpin", fail_once)
    monkeypatch.setattr(
        "miniray.node.protocol.GrantWorkerLease",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("grant construction failed")
        ),
    )
    reply = node._handle_request_lease_serialized(request)
    assert isinstance(reply, protocol.RejectWorkerLease)
    assert node.resource_ledger.available == node.resource_ledger.total
    assert len(node._dependency_pin_cleanups) == 1
    assert not node._resources_clean_locked()
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 1
    assert unpins == 1

    node._retry_dependency_pin_cleanups(force=True)
    assert not node._dependency_pin_cleanups
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 0
    assert unpins == 2
    assert node.resource_ledger.available == node.resource_ledger.total
    assert node._leases == {} and node._workers[node.worker_id].active_lease_id is None
    # Unpin completion is not custody handoff. LOCAL_READY witnessed this
    # existing replica before Grant construction failed; rollback must neither
    # delete shared bytes nor invent the owner's acknowledgement. The pure
    # test_pregrant_dependency_custody composition separately drives actual
    # owner receipts -> exact Node inventory ACK -> quiescence and physical GC.
    with node._state_lock:
        registry = node._dependency_custody_registry_locked()
        assert registry.candidates(request.lease_id) == ()
        assert registry.snapshot(request.lease_id) == protocol.LeaseDependencyInventory(
            request, node.node_id, (descriptor,),
        )
        assert registry.has_pending()
        assert not node._resources_clean_locked()
    assert node.object_store.get(descriptor.object_id) == payload


@pytest.mark.unit
def test_terminal_unpin_failure_retries_and_does_not_double_release_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, descriptor = _grant_with_target_pin()
    real_unpin = node._object_store.unpin
    unpins = 0

    def fail_once(object_id, token):
        nonlocal unpins
        unpins += 1
        if unpins == 1:
            raise RuntimeError("terminal unpin failed")
        return real_unpin(object_id, token)

    monkeypatch.setattr(node._object_store, "unpin", fail_once)
    released = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            request.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert released.released
    assert node.resource_ledger.available == node.resource_ledger.total
    assert len(node._dependency_pin_cleanups) == 1
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 1

    replay = node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            request.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert not replay.released
    assert not node._dependency_pin_cleanups
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 0
    assert node.resource_ledger.available == node.resource_ledger.total


@pytest.mark.unit
def test_permanent_unpin_failure_keeps_shutdown_resources_unclean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node, request, grant, descriptor = _grant_with_target_pin()
    monkeypatch.setattr(
        node._object_store, "unpin",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("permanent unpin")),
    )
    node._handle_release_lease(
        protocol.ReleaseWorkerLease(
            request.lease_id, grant.worker_id, grant.allocation_token
        )
    )
    assert node.resource_ledger.available == node.resource_ledger.total
    assert len(node._dependency_pin_cleanups) == 1
    node._retry_dependency_pin_cleanups(force=True)
    assert len(node._dependency_pin_cleanups) == 1
    assert not node._resources_clean_locked()
    assert node.object_store.snapshot(descriptor.object_id).pin_count == 1
