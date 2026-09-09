"""Bounded, threadless put/reference composition with actual Node bytes.

Two owner Cores share one 16 KiB ObjectStore. All owner and Node calls execute
their real handlers synchronously; only transport and reference-event delivery
are replaced. Cases use at most five put objects and inject at most one
child-stage fault, or one Seal ACK loss plus one cleanup Drop ACK loss. The
store-full case submits two 8 KiB byte values to the real 16 KiB store.
Seal validation cases send at most two six-byte requests through the real Node
handler and use pickle round trips to check the received-value boundary.
"""

from __future__ import annotations

import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, node as node_module, protocol, transport
from miniray import enhanced_publication as enhanced
from miniray.control import NodeRegistry
from miniray.core import CoreWorker, _ReleaseBorrowedReference
from miniray.ids import AttemptID, ObjectID, TaskID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from tests.unit._pure_core import (
    SynchronousReferenceMailbox, close_pure_core, make_pure_core,
)


pytestmark = pytest.mark.unit


class _Mailbox(SynchronousReferenceMailbox):
    def enqueue_internal(self, event):
        if isinstance(event, _ReleaseBorrowedReference):
            core = self.core_reference()
            assert core._drive_borrowed_reference_release(
                event.key, scheduled_round=event.scheduled_round
            )
            if event.done is not None:
                event.done.set()
            return True
        return super().enqueue_internal(event)


class _Runtime:
    def __init__(self):
        self.owner, self.borrower = make_pure_core(), make_pure_core()
        self.cores = (self.owner, self.borrower)
        self.borrower.job_id = self.owner.job_id
        self.borrower.node_id = self.owner.node_id
        self.owner.owner_address = ("owner-a.invalid", 1001)
        self.borrower.owner_address = ("owner-b.invalid", 1002)
        self.node = node = object.__new__(NodeServer)
        node.node_id = self.owner.node_id
        node._state_lock = threading.RLock()
        node._object_store = self.store = ObjectStore(16 * 1024)
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata = {}
        node._dropped_metadata = {}
        node._object_localization_locks = {}
        self.registry = NodeRegistry()
        self.registry.register_message(protocol.RegisterNode(
            node.node_id, 1001, self.owner.node_address, ResourceVector({"CPU": 1})))
        self.authority = enhanced.PublicationAuthority()
        self.gcs_calls = []
        self.references, self.calls = [], []
        self.before = self.after = None
        for core in self.cores:
            core._reference_mailbox = _Mailbox(core)
            core.gcs_address = ("gcs.invalid", 1003)
            core._rpc = core._borrow_rpc = self.rpc
            core._borrow_rpc_with_deadline = self.owner_rpc

    def track(self, reference):
        self.references.append(reference)
        return reference

    def owner_rpc(self, address, handler, request, timeout):
        assert timeout is None
        return self.rpc(address, handler, request)

    def rpc(self, address, handler, request):
        if address == self.owner.gcs_address:
            assert len(self.gcs_calls) < 160, "put graph composition exceeded finite metadata budget"
            self.gcs_calls.append((handler, request))
            if handler == enhanced.PUBLICATION_HANDLER:
                return self.authority.apply(request)
            assert handler == "get_node_state"
            return self.registry.get_state_reply(request)
        assert len(self.calls) < 128, "put composition exceeded its finite RPC budget"
        self.calls.append((handler, request))
        if self.before is not None:
            self.before(handler, request)
        if address == self.owner.node_address:
            assert handler in ("seal_object", "get_object", "drop_object_replica")
            reply = getattr(self.node, "_handle_" + handler)(request)
        else:
            destination, = [core for core in self.cores if core.owner_address == address]
            methods = {
                "prepare_stored_contained_pin": "prepare_stored_contained_pin",
                "promote_stored_contained_pin": "promote_stored_contained_pin",
                "release_contained_reference": "release_contained_reference",
                "acquire_borrowed_object": "acquire_exported_reference",
                "release_borrowed_object": "release_borrowed_reference",
                "get_owned_object": "get_owned_object",
            }
            assert handler in methods
            reply = getattr(destination, methods[handler])(request)
        if self.after is not None:
            self.after(handler, request, reply)
        return reply

    def drain(self):
        # A child release can enqueue collection on the other owner. Revisit
        # both explicit mailboxes, without creating a background consumer.
        for _ in range(4):
            for core in self.cores:
                core._reference_mailbox.drain()
        assert all(core._reference_mailbox.pending.empty() for core in self.cores)

    def close(self, reference):
        reference.close(timeout=0)
        assert reference.closed and reference._release_done.is_set()
        self.drain()

    def source(self, foreign):
        leaf = self.track(self.owner.put("leaf-value"))
        child = self.track(self.owner.put(("inner", leaf)))
        self.close(leaf)
        if not foreign:
            return self.owner, child, leaf.object_id
        seed = self.track(self.owner.put((child,)))
        source, = self.borrower._loads_owned_value(
            self.owner.owner_table.snapshot(seed.object_id).inline_data
        )
        self.track(source)
        assert source.borrower_token is not None
        self.close(seed)
        self.close(child)
        return self.borrower, source, leaf.object_id

    def assert_no_tasks(self):
        for core in self.cores:
            assert core._accepted_task_count == core._inflight_puts == 0
            assert core._inflight_submissions == 0
            # Ready notifications share this queue with accepted Tasks. Put
            # legitimately wakes existing dependants, but must enqueue no Task.
            with core._submissions.mutex:
                assert all(item is core_module._WAKE_COORDINATOR
                           for item in core._submissions.queue)
            assert not core._recovery._tasks
            assert not core._task_finish_barriers and not core._protocol_unresolved
            for object_id in core._objects:
                assert core._recovery.lineage_for_object(object_id) is None
                assert core.owner_table.snapshot(object_id).producer_task_spec is None
        assert not any(isinstance(request, (protocol.RequestWorkerLease, protocol.PushTask))
                       for _, request in self.calls)

    def finish(self):
        self.before = self.after = None
        for reference in reversed(self.references):
            if not reference.closed:
                self.close(reference)
        for core in self.cores:
            for object_id in tuple(getattr(core, "_put_handoffs", {})):
                core._reference_released(object_id)
        self.drain()
        for core in self.cores:
            assert not getattr(core, "_put_handoffs", {})
            assert not getattr(core, "_borrowed_release_obligations", {})
            assert not core._object_gc_obligations
            close_pure_core(core)
        assert self.store.used_bytes == 0
        assert all(snapshot.receipt(enhanced.PublicationStage.RETIRED) is not None
                   for snapshot in self.authority.snapshots())


@pytest.fixture
def runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("put contract attempted process/thread/socket/blocking work")

    def already_set(event, _timeout=None):
        assert event.is_set(), "pure put contract attempted to wait for progress"
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Timer, "__init__"), (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(node_module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    composition = _Runtime()
    try:
        yield composition
    finally:
        composition.finish()


@pytest.mark.parametrize("foreign", [False, True], ids=["owned", "borrowed"])
@pytest.mark.parametrize("stored", [False, True], ids=["inline", "stored"])
def test_put_complete_tuple_preserves_aliases_and_independent_child_lifetime(
    runtime, foreign, stored
):
    core, source, leaf_id = runtime.source(foreign)
    # A borrowed source itself contains another ref. get must install the
    # caller's importer scope instead of producing a detached inner handle.
    tag, imported_leaf = core.get(source)
    runtime.track(imported_leaf)
    assert tag == "inner" and core.get(imported_leaf) == "leaf-value"
    if foreign:
        assert imported_leaf.borrower_token is not None
    else:
        assert imported_leaf._local_token is not None
    runtime.close(imported_leaf)
    core.inline_threshold = 1 if stored else 8192
    outer = runtime.track(core.put(("whole", {"aliases": [source, source]}, 7)))
    snapshot = core.owner_table.snapshot(outer.object_id)
    assert snapshot.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
    assert len(snapshot.outgoing_contained_edges) == 1
    assert snapshot.output_publication is None
    assert runtime.store.contains(outer.object_id) is stored
    original_token = source.borrower_token
    runtime.close(source)
    assert runtime.owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.ACTIVE

    value = core.get(outer)
    assert type(value) is tuple and len(value) == 3 and value[0] == "whole" and value[2] == 7
    restored = runtime.track(value[1]["aliases"][0])
    assert value[1]["aliases"][1] is restored
    assert restored.object_id == source.object_id and restored is not source
    if foreign:
        assert restored.borrower_token is not None and restored.borrower_token != original_token
    else:
        assert restored._local_token is not None and restored._local_token != source._local_token
    runtime.close(outer)
    assert core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
    assert not runtime.store.contains(outer.object_id, sealed_only=False)
    tag, last_leaf = core.get(restored)
    runtime.track(last_leaf)
    assert tag == "inner" and core.get(last_leaf) == "leaf-value"
    runtime.close(restored)
    assert runtime.owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED
    assert runtime.owner.owner_table.collection_state(leaf_id) is ObjectCollectionState.ACTIVE
    runtime.close(last_leaf)
    assert runtime.owner.owner_table.collection_state(leaf_id) is ObjectCollectionState.COLLECTED
    assert not getattr(core, "_put_handoffs", {})
    runtime.assert_no_tasks()


@pytest.mark.parametrize("stage", ["prepare", "promote"])
def test_child_effect_unknown_retains_sources_and_both_exact_cleanup_holds(runtime, stage):
    core, source, _ = runtime.source(True)
    unknown = []
    cleanup_blocked = True

    def after(handler, request, reply):
        if handler == stage + "_stored_contained_pin" and not unknown:
            assert reply.accepted
            unknown.append(request)
            raise TimeoutError("lost child-stage ACK")

    def before(handler, request):
        if (cleanup_blocked and unknown and handler == "release_contained_reference"
                and request.hold.container_object_id == unknown[0].transfer.final_hold.container_object_id):
            raise TimeoutError("child cleanup unavailable")

    runtime.before, runtime.after = before, after
    with pytest.raises(TimeoutError, match="child-stage"):
        core.put((source, source))
    transfer = unknown[0].transfer
    failed_id = transfer.final_hold.container_object_id
    retained = core._put_handoffs[failed_id]
    assert retained["prepared"].sources == (source,)
    assert retained["prepared"].sources[0] is source
    assert retained["prepared"].manifest.transfers == (transfer,)
    assert retained["aborted"] and not retained["driver"]
    assert core.owner_table.snapshot(failed_id).state is ObjectState.ERROR
    runtime.close(source)
    core._reference_released(failed_id)
    assert core._put_handoffs[failed_id] is retained
    assert core.owner_table.collection_state(failed_id) is ObjectCollectionState.ACTIVE
    assert runtime.owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.ACTIVE
    assert not runtime.store.contains(failed_id, sealed_only=False)

    cleanup_blocked = False
    core._reference_released(failed_id)
    runtime.drain()
    assert failed_id not in core._put_handoffs
    assert core.owner_table.collection_state(failed_id) is ObjectCollectionState.COLLECTED
    assert runtime.owner.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED
    assert set(retained["releases"]) == {
        protocol.ReleaseContainedReference(source.object_id, source.owner_worker_id, hold)
        for hold in (transfer.final_hold, transfer.provisional_hold)
    }
    assert all(reply.accepted for reply in retained["releases"].values())
    runtime.assert_no_tasks()


def test_unknown_seal_and_lost_drop_ack_keep_cleanup_until_exact_drop_replay(runtime):
    core, source, _ = runtime.source(True)
    _assert_unknown_seal_cleanup(runtime, core, (source, source), sources=(source,))
    runtime.close(source)
    runtime.assert_no_tasks()


def test_plain_large_put_keeps_exact_cleanup_after_seal_and_drop_ack_loss(runtime):
    _assert_unknown_seal_cleanup(runtime, runtime.owner, b"plain" * 512, sources=())
    runtime.assert_no_tasks()


def _assert_unknown_seal_cleanup(runtime, core, value, *, sources):
    core.inline_threshold = 1
    seal_lost, drop_lost = [], []
    block_drop = True

    def before(handler, _request):
        if handler == "drop_object_replica" and block_drop:
            raise TimeoutError("Drop unavailable")

    def after(handler, request, reply):
        if handler == "seal_object" and not seal_lost:
            assert reply.sealed and runtime.store.get(request.object_id) == request.data
            seal_lost.append(request)
            raise TimeoutError("Seal ACK lost after bytes were stored")
        if handler == "drop_object_replica" and not drop_lost:
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert not runtime.store.contains(request.object_id, sealed_only=False)
            drop_lost.append(request)
            raise TimeoutError("Drop ACK lost after bytes were deleted")

    runtime.before, runtime.after = before, after
    with pytest.raises(TimeoutError, match="Seal ACK"):
        core.put(value)
    seal, = seal_lost
    retained = core._put_handoffs[seal.object_id]
    assert retained["prepared"].payload == seal.data
    assert retained["prepared"].sources == sources
    assert all(actual is source for actual, source in zip(retained["prepared"].sources, sources))
    assert len(retained["prepared"].manifest.transfers) == len(sources)
    assert runtime.store.get(seal.object_id) == seal.data
    assert core.owner_table.snapshot(seal.object_id).state is ObjectState.ERROR
    core._reference_released(seal.object_id)
    assert core.owner_table.collection_state(seal.object_id) is ObjectCollectionState.ACTIVE
    assert seal.object_id in core._put_handoffs

    block_drop = False
    core._reference_released(seal.object_id)
    assert len(drop_lost) == 1
    assert not runtime.store.contains(seal.object_id, sealed_only=False)
    assert core._put_handoffs[seal.object_id] is retained
    assert core.owner_table.collection_state(seal.object_id) is ObjectCollectionState.ACTIVE
    call_count = len(runtime.calls)
    core._reference_released(seal.object_id)
    assert runtime.calls[call_count:] == [("drop_object_replica", drop_lost[0])]
    runtime.drain()
    assert seal.object_id not in core._put_handoffs
    assert core.owner_table.collection_state(seal.object_id) is ObjectCollectionState.COLLECTED
    assert all(request == seal for handler, request in runtime.calls if handler == "seal_object")


def test_custom_reducer_hidden_ref_uses_public_put_discovery_once(runtime):
    core, source, _ = runtime.source(True)
    reductions = []

    class HiddenReference:
        def __reduce__(self):
            reductions.append("reduce")
            return dict, ((("ref", source), ("alias", source)),)

    hidden = HiddenReference()
    # The old built-in-container walk cannot see this reducer-produced ref.
    assert not core_module._contains_object_ref(hidden)
    outer = runtime.track(core.put(hidden))
    assert reductions == ["reduce"]
    snapshot = core.owner_table.snapshot(outer.object_id)
    assert snapshot.state is ObjectState.READY_INLINE
    edge, = snapshot.outgoing_contained_edges
    assert edge.contained_object_id == source.object_id
    assert edge.contained_owner_worker_id == source.owner_worker_id
    runtime.close(source)
    restored = core.get(outer)
    independent = runtime.track(restored["ref"])
    assert restored["alias"] is independent
    assert independent.borrower_token is not None
    assert independent.borrower_token != source.borrower_token
    runtime.close(outer)
    assert core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
    tag, leaf = core.get(independent)
    runtime.track(leaf)
    assert tag == "inner" and core.get(leaf) == "leaf-value"
    runtime.close(independent)
    runtime.close(leaf)
    assert reductions == ["reduce"]
    runtime.assert_no_tasks()


def test_real_store_full_fences_absence_and_rejects_late_seal_after_capacity_returns(runtime):
    core = runtime.owner
    core.inline_threshold = 1
    filler = runtime.track(core.put(b"F" * (8 * 1024)))
    before_bytes = runtime.store.used_bytes
    assert before_bytes > 8 * 1024
    rejected = []

    def after(handler, request, reply):
        if handler == "seal_object" and not reply.sealed:
            rejected.append((request, reply))

    runtime.after = after
    with pytest.raises(Exception, match="ObjectStoreFullError"):
        core.put(b"R" * (8 * 1024))
    assert len(rejected) == 1
    seal, reply = rejected[0]
    assert reply.absence_fenced and not reply.sealed
    assert runtime.store.used_bytes == before_bytes
    assert not runtime.store.contains(seal.object_id, sealed_only=False)
    assert seal.object_id not in runtime.node._sealed_metadata
    assert runtime.node._dropped_metadata[seal.object_id] == (
        seal.attempt_id, seal.owner_worker_id, seal.checksum
    )
    assert seal.object_id not in core._put_handoffs
    assert core.owner_table.snapshot(seal.object_id).state is ObjectState.ERROR
    assert core._inflight_puts == 0
    assert not any(handler == "drop_object_replica" and request.object_id == seal.object_id
                   for handler, request in runtime.calls)
    runtime.drain()
    assert core.owner_table.collection_state(seal.object_id) is ObjectCollectionState.COLLECTED
    runtime.close(filler)
    assert runtime.store.used_bytes == 0
    # Capacity now permits these exact bytes, but the real deletion receipt
    # fences the delayed request rather than letting it recreate an orphan.
    late = runtime.rpc(core.node_address, "seal_object", seal)
    assert not late.sealed and late.absence_fenced
    assert runtime.store.used_bytes == 0
    assert not runtime.store.contains(seal.object_id, sealed_only=False)
    drop = protocol.DropObjectReplica(
        seal.object_id, seal.attempt_id, seal.owner_worker_id, core.node_id, seal.checksum
    )
    receipt = runtime.node._handle_drop_object_replica(drop)
    assert receipt.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert receipt.object_id == seal.object_id and receipt.checksum == seal.checksum
    assert not core._put_handoffs
    runtime.assert_no_tasks()


@pytest.mark.parametrize("corruption", [
    "payload", "attempt-number", "object-task", "attempt-task", "owner",
])
def test_real_seal_revalidates_deserialized_identity_before_any_effect(runtime, corruption):
    task = TaskID.for_put(runtime.owner.job_id, runtime.owner.worker_id, 123)
    request = protocol.SealObject.from_data(
        ObjectID.for_task(task), AttemptID(task, 0), runtime.owner.worker_id, b"good",
    )
    if corruption == "payload":
        object.__setattr__(request, "data", b"evil")
    elif corruption == "attempt-number":
        object.__setattr__(request.attempt_id, "attempt_number", -1)
    elif corruption == "object-task":
        object.__setattr__(request.object_id.task_id, "value", b"short")
    elif corruption == "attempt-task":
        object.__setattr__(request.attempt_id, "task_id", TaskID.random())
    else:
        object.__setattr__(request, "owner_worker_id", "not-a-worker-id")
    # Real transport uses pickle.loads, which preserves this invalid state.
    incoming = pickle.loads(pickle.dumps(request))
    with pytest.raises((TypeError, ValueError, protocol.ProtocolError)):
        runtime.node._handle_seal_object(incoming)
    assert runtime.store.used_bytes == 0
    assert not runtime.node._sealed_metadata and not runtime.node._dropped_metadata
    assert not runtime.node._object_localization_locks


def test_real_seal_freezes_nested_identity_and_preserves_exact_replay(runtime):
    task = TaskID.for_put(runtime.owner.job_id, runtime.owner.worker_id, 124)
    request = protocol.SealObject.from_data(
        ObjectID.for_task(task), AttemptID(task, 0), runtime.owner.worker_id, b"stable",
    )
    expected = pickle.loads(pickle.dumps(request))
    incoming = pickle.loads(pickle.dumps(request))
    first = runtime.node._handle_seal_object(incoming)
    assert first.sealed and runtime.store.get(expected.object_id) == expected.data
    object.__setattr__(incoming.object_id.task_id, "value", b"short")
    object.__setattr__(incoming.attempt_id, "attempt_number", -1)
    object.__setattr__(incoming.owner_worker_id, "value", b"short")
    assert runtime.node._sealed_metadata[expected.object_id] == (
        expected.attempt_id, expected.owner_worker_id, len(expected.data), expected.checksum,
    )
    replay = runtime.node._handle_seal_object(expected)
    assert replay == first and runtime.store.used_bytes == len(expected.data)
    drop = protocol.DropObjectReplica(
        expected.object_id, expected.attempt_id, expected.owner_worker_id,
        runtime.node.node_id, expected.checksum,
    )
    assert runtime.node._handle_drop_object_replica(drop).dropped
    assert runtime.store.used_bytes == 0
