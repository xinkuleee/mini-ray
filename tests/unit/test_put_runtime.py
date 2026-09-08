"""Mixed put contracts: metadata failures versus real reference threads.

Successful put returns a runtime-bound ObjectRef and lazily starts Core's real
reference-event thread, even from the bare _core fixture. Those four original
cases stay heavy pending bounded lifecycle review. ID construction and the two
failure-only paths never expose a handle or start that consumer and stay unit.
Injected RPCs and a cleanup finalizer do not by themselves make success pure.
"""

from __future__ import annotations

import hashlib
import queue
import threading

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import CoreWorker, ObjectRef, _HomeRoute
from miniray.errors import NodeDiedError
from miniray.ids import JobID, NodeID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable, ObjectState
from tests.unit._core_test_utils import add_core_thread_finalizer


def _core(*, inline_threshold: int = 1024) -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID(bytes.fromhex("11" * 16))
    core.worker_id = WorkerID(bytes.fromhex("22" * 16))
    core.node_id = NodeID(bytes.fromhex("33" * 16))
    core.node_address = ("127.0.0.1", 12001)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.inline_threshold = inline_threshold
    core._put_index = 0
    core._owner_table = ObjectOwnerTable()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._accepting = True
    core._inflight_puts = 0
    return core


@pytest.mark.unit
def test_put_ids_use_stable_separate_domain() -> None:
    job = JobID(bytes.fromhex("01" * 16))
    owner = WorkerID(bytes.fromhex("02" * 16))
    first = TaskID.for_put(job, owner, 0)

    assert first == TaskID.for_put(job, owner, 0)
    assert first != TaskID.for_put(job, owner, 1)
    assert first != TaskID.derive(job, TaskID.for_driver(job), 0)


@pytest.mark.heavy
def test_inline_put_is_ready_and_has_no_producer_lineage() -> None:
    core = _core()

    ref = core.put({"value": 7})

    assert isinstance(ref, ObjectRef)
    snapshot = core.owner_table.snapshot(ref.object_id)
    assert snapshot.state is ObjectState.READY_INLINE
    assert snapshot.producer_task_spec is None
    assert snapshot.local_tokens == frozenset({ref._local_token})
    assert core.get(ref) == {"value": 7}
    assert core._inflight_puts == 0


@pytest.mark.heavy
def test_large_put_seals_on_home_node_and_publishes_descriptor(
    request: pytest.FixtureRequest,
) -> None:
    core = _core(inline_threshold=1)
    add_core_thread_finalizer(request, core)
    value = b"large-value"
    payload = cloudpickle.dumps(value)
    calls = []

    def rpc(address, handler, message):
        calls.append((address, handler, message))
        return protocol.SealObjectReply(
            message.object_id,
            True,
            core.node_id,
            len(message.data),
            message.checksum,
        )

    core._rpc = rpc
    ref = core.put(value)

    assert len(calls) == 1
    assert calls[0][0] == core.node_address
    assert calls[0][1] == "seal_object"
    assert calls[0][2].data == payload
    snapshot = core.owner_table.snapshot(ref.object_id)
    assert snapshot.state is ObjectState.READY_STORED
    assert snapshot.producer_task_spec is None
    assert snapshot.locations == frozenset({core.node_id})
    descriptor = core._stored_descriptors[ref.object_id]
    assert snapshot.canonical_stored_result == descriptor
    assert descriptor.storage is protocol.ResultStorage.OBJECT_STORE
    assert descriptor.inline_data is None
    assert descriptor.checksum == hashlib.sha256(payload).hexdigest()


@pytest.mark.unit
def test_failed_large_put_never_leaves_pending_object() -> None:
    core = _core(inline_threshold=1)
    core._rpc = lambda *_args, **_kwargs: protocol.SealObjectReply(
        # The IDs are intentionally irrelevant because the rejected reply is
        # converted into the owner-visible terminal error below.
        next(iter(core._objects)),
        False,
        core.node_id,
        0,
        "0" * 64,
        error="store full",
    )

    with pytest.raises(Exception, match="store full"):
        core.put(b"large-value")

    assert len(core._objects) == 1
    object_id = next(iter(core._objects))
    snapshot = core.owner_table.snapshot(object_id)
    assert snapshot.state is ObjectState.ERROR
    assert core._objects[object_id].event.is_set()
    assert core._inflight_puts == 0


@pytest.mark.heavy
def test_large_put_reseals_same_identity_after_captured_home_dies(
    request: pytest.FixtureRequest,
) -> None:
    core = _core(inline_threshold=1)
    add_core_thread_finalizer(request, core)
    old_route = _HomeRoute(core.node_id, core.node_address, 1)
    next_route = _HomeRoute(
        NodeID.random(), ("127.0.0.1", 12002), 2
    )
    core._home_route = old_route
    core._dead_nodes = {}
    calls = []

    def rpc(address, _handler, message):
        calls.append((address, message.object_id, message.attempt_id))
        reply = protocol.SealObjectReply(
            message.object_id, True,
            old_route.node_id if len(calls) == 1 else next_route.node_id,
            len(message.data), message.checksum,
        )
        if len(calls) == 1:
            with core._state_lock:
                core._dead_nodes[old_route.node_id] = object()
                core._home_route = next_route
        return reply

    core._rpc = rpc
    ref = core.put(b"migrate-me")

    assert len(calls) == 2
    assert calls[0][1:] == calls[1][1:]
    assert [call[0] for call in calls] == [old_route.address, next_route.address]
    snapshot = core.owner_table.snapshot(ref.object_id)
    assert snapshot.locations == frozenset({next_route.node_id})
    descriptor = core._stored_descriptors[ref.object_id]
    assert descriptor.node_id == next_route.node_id
    assert snapshot.canonical_stored_result == descriptor


@pytest.mark.unit
def test_large_put_without_survivor_is_typed_and_transport_error_does_not_failover() -> None:
    no_survivor = _core(inline_threshold=1)
    no_survivor._home_route = None
    with pytest.raises(NodeDiedError, match="live Node"):
        no_survivor.put(b"large")

    unavailable = _core(inline_threshold=1)
    calls = []

    def fail(*args):
        calls.append(args)
        raise TimeoutError("not a death proof")

    unavailable._rpc = fail
    with pytest.raises(TimeoutError, match="death proof"):
        unavailable.put(b"large")
    assert len(calls) == 1


@pytest.mark.heavy
def test_put_rejects_object_refs_and_shutdown_state() -> None:
    core = _core()
    ref = core.put(1)

    with pytest.raises(TypeError, match="containing ObjectRefs"):
        core.put({"nested": [ref]})

    core._accepting = False
    with pytest.raises(RuntimeError, match="shutting down"):
        core.put(2)
