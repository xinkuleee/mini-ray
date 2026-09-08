"""Threadless object-store/fetch contracts, using at most 128 KiB of storage.

All RPC boundaries are synchronous stand-ins or rejected before I/O. Worker
and Core success use the real selected-output journal/adapter and physical
store; fetched bytes never bypass the Node's producer-identity checks.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import replace

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import CoreWorker, ObjectRef, _ObjectWaiter, _PendingTask
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.resources import ResourceVector
from miniray.transport import TransportConnectionError


pytestmark = pytest.mark.unit


def _identity_spec(owner: WorkerID) -> protocol.TaskSpec:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    attempt_id = AttemptID(task_id, 0)
    key = protocol.FunctionKey(job_id, __name__, "identity", "v1")
    return protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=attempt_id,
        function=key,
        args=(),
        num_returns=1,
        resources=ResourceVector(),
        owner_worker_id=owner,
    )


def test_node_seals_and_reads_physical_bytes_idempotently() -> None:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._object_store = ObjectStore(128 * 1024)
    node._sealed_metadata = {}
    node._state_lock = threading.RLock()

    spec = _identity_spec(WorkerID.random())
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"x" * (64 * 1024))
    request = protocol.SealObject.from_data(
        object_id, spec.attempt_id, spec.owner_worker_id, payload
    )

    first = node._handle_seal_object(request)
    repeated = node._handle_seal_object(request)
    fetched = node._handle_get_object(protocol.GetObject(object_id, node.node_id))

    assert first.sealed and repeated == first
    assert fetched.found and fetched.sealed
    assert fetched.data == payload
    assert fetched.checksum == hashlib.sha256(payload).hexdigest()
    assert node.object_store.used_bytes == len(payload)


def test_worker_large_result_reply_contains_descriptor_not_bytes(monkeypatch) -> None:
    """Pure: one 64 KiB value, one 128 KiB Node store, no runtime threads."""
    from miniray import output_protocol as wire
    from miniray.worker import START_WORKER_LEASE_HANDLER, COMPLETE_WORKER_LEASE_HANDLER
    from tests.unit.test_worker_unified_output import (
        _ActualNodePublication, _Fixture, _install_no_runtime,
    )

    _install_no_runtime(monkeypatch)
    value = b"z" * (64 * 1024)
    fixture = _Fixture(monkeypatch, lambda: value, threshold=1024)
    backend = _ActualNodePublication(fixture)
    reply = fixture.worker._handle_push_task(fixture.push)
    (result,) = reply.results
    assert result.storage is protocol.ResultStorage.OBJECT_STORE
    assert result.inline_data is None
    assert result.size_bytes == len(cloudpickle.dumps(value))
    assert reply.output_publication == fixture.complete_envelope
    assert fixture.handlers == [
        START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
        COMPLETE_WORKER_LEASE_HANDLER,
    ]
    (prepare,) = fixture.prepares
    assert prepare.slot_payloads == (cloudpickle.dumps(value),)
    assert backend.store.get(result.object_id) == prepare.slot_payloads[0]
    assert backend.completions == [reply.output_publication.complete]
    assert backend.ledger.available == ResourceVector({"CPU": 1})
    assert fixture.worker._handle_push_task(fixture.push) is reply
    assert len(fixture.calls) == 3 and fixture.executions == [True]


def test_core_publishes_stored_location_then_fetches_from_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from miniray import output_protocol as wire
    from miniray.ownership import ObjectCollectionState, OutputOwnerPublicationPlan
    from tests.unit._pure_core import close_pure_core, make_pure_core
    from tests.unit._pure_output_runtime import _metadata
    from tests.unit.test_worker_unified_output import (
        _ActualNodePublication, _Fixture, _install_no_runtime,
    )

    _install_no_runtime(monkeypatch)
    value = b"q" * (64 * 1024)
    fixture = _Fixture(monkeypatch, lambda: value, threshold=1024)
    backend = _ActualNodePublication(fixture)
    spec = fixture.push.spec
    owner = spec.owner_worker_id
    node_id = fixture.worker.node_id
    object_id = spec.return_ids()[0]
    pending = _PendingTask(object_id, spec)
    payload = cloudpickle.dumps(value)
    checksum = hashlib.sha256(payload).hexdigest()
    descriptor = protocol.ResultDescriptor(
        object_id,
        protocol.ResultStorage.OBJECT_STORE,
        len(payload),
        owner,
        node_id,
        checksum,
    )
    reply = fixture.worker._handle_push_task(fixture.push)
    envelope = reply.output_publication
    assert reply.results == (descriptor,)
    assert envelope == fixture.complete_envelope
    assert envelope.manifest.execution == pending.execution
    assert backend.completions == [envelope.complete]
    assert backend.store.get(object_id) == payload
    assert backend.store.used_bytes == len(payload)

    core = make_pure_core()
    core.job_id = spec.job_id
    core.worker_id = owner
    core.driver_task_id = TaskID.for_driver(spec.job_id)
    core.node_id = node_id
    core.node_address = fixture.worker.node_address
    core.gcs_address = ("large-output-gcs.invalid", 1)
    core.owner_table.register(
        object_id, current_attempt=spec.attempt_id, producer_task_spec=spec,
    )
    core._recovery.register_task(spec)
    core._objects = {object_id: _ObjectWaiter(threading.Event())}
    calls = []
    control_calls = []
    owner_plan = OutputOwnerPublicationPlan(pending.execution, envelope)

    def fetch(address, handler, message):
        _metadata(message)
        if handler == "get_object":
            assert address == core.node_address
            calls.append((address, handler, message))
            return backend.node._handle_get_object(message)
        if handler == "drop_object_replica":
            assert address == core.node_address
            return backend.node._handle_drop_object_replica(message)
        control_calls.append((handler, message))
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == core.gcs_address
            if type(message) is wire.ReportOutputPublicationTerminal:
                assert message.witness == envelope.complete
                ack = backend.recovery.report_terminal(message.witness)
            elif type(message) is wire.ReportOutputPublicationAdopted:
                assert message.proof.complete == envelope.complete
                assert core.owner_table.output_owner_publication_receipt(
                    owner_plan,
                ).committed
                ack = backend.recovery.report_adopted(message.proof)
            else:
                assert type(message) is wire.ReportOutputPublicationSlotCollected
                assert message.proof.complete == envelope.complete
                assert core.owner_table.collection_state(object_id) is (
                    ObjectCollectionState.COLLECTING
                )
                assert not backend.store.contains(object_id, sealed_only=False)
                ack = backend.recovery.report_slot_collected(message.proof)
            result = wire.OutputRecoveryReply(message, ack)
        else:
            assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
            assert address == core.node_address
            assert type(message) is wire.AckOutputPublicationAdopted
            assert message.proof.complete == envelope.complete
            assert backend.recovery.snapshot(envelope.publication_id).adopted == (
                message.proof
            )
            assert core.owner_table.output_owner_publication_receipt(
                owner_plan,
            ).committed
            backend.journal.retire_completed(message.proof)
            result = wire.AckOutputPublicationAdoptedReply(message, True)
        _metadata(result)
        return result

    core._rpc = fetch

    try:
        assert core._publish_reply(
            pending, reply, expected_node_id=node_id,
            expected_lease_id=fixture.push.lease_id,
        )
        snapshot = core.owner_table.snapshot(object_id)
        assert snapshot.state is ObjectState.READY_STORED
        assert snapshot.inline_data is None
        assert snapshot.locations == frozenset({node_id})
        assert core._fetch_stored_object(object_id, snapshot) == payload
        assert calls == [(
            core.node_address, "get_object",
            protocol.GetObject(
                object_id, node_id, spec.attempt_id, owner, len(payload), checksum
            ),
        )]
        assert [type(message) for _handler, message in control_calls] == [
            wire.ReportOutputPublicationTerminal,
            wire.ReportOutputPublicationAdopted,
            wire.AckOutputPublicationAdopted,
        ]
        assert not backend.journal.snapshot(envelope.publication_id).retained_result_slots
        assert backend.store.get(object_id) == payload

        # Adoption retires Node reply custody, not the physical replica. The
        # actual owner GC later drops it and reports the exact slot receipt.
        assert core._finish_pending_task(pending)
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        assert backend.store.used_bytes == 0
        assert not backend.node._sealed_metadata
        assert backend.recovery.snapshot(envelope.publication_id).slot_collections
    finally:
        close_pure_core(core)


def test_owner_fetch_retries_only_after_authoritative_same_attempt_route_change(
) -> None:
    owner = WorkerID.random()
    source = NodeID.random()
    target = NodeID.random()
    source_address = ("127.0.0.1", 12347)
    target_address = ("127.0.0.1", 12348)
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"r" * (64 * 1024))
    checksum = hashlib.sha256(payload).hexdigest()
    canonical = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        source, checksum,
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = target
    core.node_address = target_address
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(
        object_id, spec.attempt_id, source, descriptor=canonical
    )
    core._owner_table.add_location(object_id, spec.attempt_id, target)
    core._stored_descriptors = {object_id: canonical}
    core._state_lock = threading.RLock()
    core._dead_nodes = {}
    core._resolve_node_address = lambda node, **_kwargs: {
        source: source_address, target: target_address,
    }[node]
    calls = []

    def fetch(address, handler, request):
        calls.append((address, handler, request))
        if address == source_address:
            with core._state_lock:
                removal = core._owner_table.remove_node_locations(source)
                assert removal.surviving == (object_id,)
                core._stored_descriptors[object_id] = replace(
                    canonical, node_id=target
                )
                core._dead_nodes[source] = object()
            raise TransportConnectionError("source exited")
        assert address == target_address
        return protocol.GetObjectReply(
            object_id, target, True, True, payload, checksum,
            producer_attempt_id=spec.attempt_id, owner_worker_id=owner,
            size_bytes=len(payload),
        )

    core._rpc = fetch

    assert core._fetch_stored_object(
        object_id, core.owner_table.snapshot(object_id)
    ) == payload
    assert [address for address, _handler, _request in calls] == [
        source_address, target_address,
    ]
    assert all(
        request.expected_attempt_id == spec.attempt_id
        and request.expected_owner_worker_id == owner
        and request.expected_size_bytes == len(payload)
        and request.expected_checksum == checksum
        for _address, _handler, request in calls
    )


def test_owner_fetch_reads_owner_snapshot_and_route_as_one_atomic_pair() -> None:
    owner = WorkerID.random()
    source = NodeID.random()
    target = NodeID.random()
    target_address = ("127.0.0.1", 12358)
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"atomic-route")
    checksum = hashlib.sha256(payload).hexdigest()
    canonical = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        source, checksum,
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = target
    core.node_address = target_address
    core.event_sink = None
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(
        object_id, spec.attempt_id, source, descriptor=canonical
    )
    core._stored_descriptors = {object_id: canonical}
    core._state_lock = threading.RLock()
    core._dead_nodes = {}

    # The caller hands fetch an older snapshot.  Before fetch takes its first
    # Core lock, death handling has already installed target as the sole route.
    core._owner_table.add_location(
        object_id, spec.attempt_id, target,
        descriptor=replace(canonical, node_id=target),
    )
    core._owner_table.remove_node_locations(source)
    core._stored_descriptors[object_id] = replace(canonical, node_id=target)
    core._dead_nodes[source] = object()
    core._resolve_node_address_with_timeout_at_route = (
        lambda node, _timeout, _route: target_address
    )
    calls = []

    def fetch(address, handler, request):
        calls.append((address, handler, request))
        return protocol.GetObjectReply(
            object_id, target, True, True, payload, checksum,
            producer_attempt_id=spec.attempt_id, owner_worker_id=owner,
            size_bytes=len(payload),
        )

    core._rpc = fetch
    stale = replace(
        core.owner_table.snapshot(object_id),
        locations=frozenset({source}),
    )

    assert core._fetch_stored_object(object_id, stale) == payload
    assert len(calls) == 1
    assert calls[0][2].expected_attempt_id == spec.attempt_id


def test_owner_fetch_discards_reply_after_attempt_advances_in_flight() -> None:
    owner = WorkerID.random()
    node_id = NodeID.random()
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"old-attempt")
    checksum = hashlib.sha256(payload).hexdigest()
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        node_id, checksum,
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = node_id
    core.node_address = ("127.0.0.1", 12361)
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(
        object_id, spec.attempt_id, node_id, descriptor=descriptor
    )
    core._stored_descriptors = {object_id: descriptor}
    core._state_lock = threading.RLock()
    core._dead_nodes = {}

    def fetch(*_args):
        core._owner_table.mark_lost(object_id, spec.attempt_id)
        assert core._owner_table.advance_attempt(
            object_id,
            expected_attempt=spec.attempt_id,
            next_attempt=spec.attempt_id.next(),
        )
        core._stored_descriptors.pop(object_id)
        return protocol.GetObjectReply(
            object_id, node_id, True, True, payload, checksum,
            producer_attempt_id=spec.attempt_id, owner_worker_id=owner,
            size_bytes=len(payload),
        )

    core._rpc = fetch
    with pytest.raises(Exception) as raised:
        core._fetch_stored_object(
            object_id, core.owner_table.snapshot(object_id)
        )
    assert type(raised.value).__name__ == "_StoredFetchStateChanged"


def test_owner_stored_get_zero_timeout_never_starts_network_io() -> None:
    owner = WorkerID.random()
    node_id = NodeID.random()
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"deadline")
    checksum = hashlib.sha256(payload).hexdigest()
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        node_id, checksum,
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = node_id
    core.node_address = ("127.0.0.1", 12359)
    core.event_sink = None
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(
        object_id, spec.attempt_id, node_id, descriptor=descriptor
    )
    core._objects = {object_id: _ObjectWaiter(threading.Event())}
    core._stored_descriptors = {object_id: descriptor}
    core._state_lock = threading.RLock()
    core._dead_nodes = {}
    core._loads_owned_value = cloudpickle.loads
    core._resolve_node_address_with_timeout_at_route = (
        lambda *_args: pytest.fail("zero timeout must not resolve a Node")
    )
    core._rpc = lambda *_args: pytest.fail("zero timeout must not issue RPC")
    ref = ObjectRef(object_id, owner)

    started = time.monotonic()
    with pytest.raises(TimeoutError, match="before timeout"):
        core.get(ref, timeout=0)
    assert time.monotonic() - started < 0.1


def test_owner_fetch_does_not_retry_transport_error_without_route_change() -> None:
    owner = WorkerID.random()
    node_id = NodeID.random()
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"u" * (64 * 1024))
    checksum = hashlib.sha256(payload).hexdigest()
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        node_id, checksum,
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = node_id
    core.node_address = ("127.0.0.1", 12349)
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(
        object_id, spec.attempt_id, node_id, descriptor=descriptor
    )
    core._stored_descriptors = {object_id: descriptor}
    core._state_lock = threading.RLock()
    core._dead_nodes = {}
    calls = []

    def unavailable(*args):
        calls.append(args)
        raise TransportConnectionError("not a death proof")

    core._rpc = unavailable

    with pytest.raises(TransportConnectionError, match="death proof"):
        core._fetch_stored_object(
            object_id, core.owner_table.snapshot(object_id)
        )
    assert len(calls) == 1


def test_owner_fetch_does_not_spin_when_unchanged_route_resolution_fails() -> None:
    owner = WorkerID.random()
    node_id = NodeID.random()
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"route-unavailable")
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        node_id, hashlib.sha256(payload).hexdigest(),
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 12360)
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(
        object_id, spec.attempt_id, node_id, descriptor=descriptor
    )
    core._stored_descriptors = {object_id: descriptor}
    core._state_lock = threading.RLock()
    core._dead_nodes = {}
    calls = []

    def unavailable(*args):
        calls.append(args)
        raise TransportConnectionError("route unavailable without death proof")

    core._resolve_node_address_with_timeout_at_route = unavailable
    with pytest.raises(TransportConnectionError, match="without death proof"):
        core._fetch_stored_object(
            object_id, core.owner_table.snapshot(object_id)
        )
    assert len(calls) == 1


@pytest.mark.parametrize(
    "mutation",
    (
        "object", "node", "attempt", "owner", "size",
        "checksum", "bytes", "missing_metadata", "missing_replica",
        "type_confusion",
    ),
)
def test_owner_local_fetch_rejects_malformed_or_drifted_reply(
    mutation: str,
) -> None:
    owner = WorkerID.random()
    node_id = NodeID.random()
    spec = _identity_spec(owner)
    object_id = spec.return_ids()[0]
    payload = cloudpickle.dumps(b"m" * (64 * 1024))
    checksum = hashlib.sha256(payload).hexdigest()
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, len(payload), owner,
        node_id, checksum,
    )
    core = object.__new__(CoreWorker)
    core.worker_id = owner
    core.node_id = node_id
    core.node_address = ("127.0.0.1", 12346)
    core._owner_table = ObjectOwnerTable()
    core._owner_table.register(object_id, current_attempt=spec.attempt_id)
    core._owner_table.publish_stored(object_id, spec.attempt_id, node_id)
    core._owner_table._entries[object_id].canonical_stored_result = descriptor
    core._stored_descriptors = {object_id: descriptor}
    core._state_lock = threading.RLock()
    base = protocol.GetObjectReply(
        object_id, node_id, True, True, payload, checksum,
        producer_attempt_id=spec.attempt_id, owner_worker_id=owner,
        size_bytes=len(payload),
    )
    values = {
        field: getattr(base, field)
        for field in (
            "object_id", "node_id", "found", "sealed", "data",
            "checksum", "error", "producer_attempt_id",
            "owner_worker_id", "size_bytes",
        )
    }
    if mutation == "object":
        other_task = TaskID.derive(spec.job_id, spec.task_id, 9)
        values["object_id"] = ObjectID.for_task(other_task)
        values["producer_attempt_id"] = AttemptID(other_task, 0)
    elif mutation == "node":
        values["node_id"] = NodeID.random()
    elif mutation == "attempt":
        values["producer_attempt_id"] = spec.attempt_id.next()
    elif mutation == "owner":
        values["owner_worker_id"] = WorkerID.random()
    elif mutation == "size":
        values["size_bytes"] = len(payload) + 1
    elif mutation == "checksum":
        other = b"x" * len(payload)
        values["data"] = other
        values["checksum"] = hashlib.sha256(other).hexdigest()
    elif mutation == "bytes":
        # Bypass construction to model a wire value whose bytes no longer match
        # its otherwise correct checksum.
        corrupt = object.__new__(protocol.GetObjectReply)
        for name, value in values.items():
            object.__setattr__(corrupt, name, value)
        object.__setattr__(corrupt, "data", b"x" * len(payload))
        core._rpc = lambda *_args: corrupt
        with pytest.raises(SystemTaskError, match="malformed object reply"):
            core._fetch_stored_object(
                object_id, core.owner_table.snapshot(object_id)
            )
        return
    elif mutation == "missing_metadata":
        values["producer_attempt_id"] = None
        values["owner_worker_id"] = None
        values["size_bytes"] = None
    elif mutation == "missing_replica":
        values.update({
            "found": False, "sealed": False, "data": None,
            "checksum": None, "error": "replica missing",
            "producer_attempt_id": None, "owner_worker_id": None,
            "size_bytes": None,
        })
    elif mutation == "type_confusion":
        malformed = object.__new__(protocol.GetObjectReply)
        for name, value in values.items():
            object.__setattr__(malformed, name, value)
        object.__setattr__(malformed, "found", 1)
        object.__setattr__(malformed, "sealed", 1)
        core._rpc = lambda *_args: malformed
        with pytest.raises(SystemTaskError, match="malformed object reply"):
            core._fetch_stored_object(
                object_id, core.owner_table.snapshot(object_id)
            )
        return

    # Some mutations are invalid at construction and therefore already prove
    # the wire contract; build unchecked instances so Core's decode fence is
    # exercised independently.
    candidate = object.__new__(protocol.GetObjectReply)
    for name, value in values.items():
        object.__setattr__(candidate, name, value)
    core._rpc = lambda *_args: candidate
    expected_error = (
        "malformed object reply"
        if mutation == "size"
        else "stored object checksum does not match"
        if mutation == "checksum"
        else "replica missing"
        if mutation == "missing_replica"
        else "stored object is unavailable"
    )
    with pytest.raises(SystemTaskError, match=expected_error):
        core._fetch_stored_object(
            object_id, core.owner_table.snapshot(object_id)
        )
