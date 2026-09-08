"""Pure contracts for Ray-style by-value task argument lifting."""

from __future__ import annotations

import hashlib
import queue
import threading
from dataclasses import replace

import cloudpickle
import pytest

from miniray import protocol
import miniray.worker as worker_module
from miniray.core import (
    CoreWorker, ObjectRef, RemoteFunctionDefinition,
    _LocalReferenceRelease, _PendingTask, _RetryInlineGc,
)
from miniray.foreign_lineage import ForeignLineageRole
from miniray.errors import UnreconstructableObjectError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState
from miniray.resources import AllocationToken, ResourceVector
from miniray.trace import MemoryEventSink
from miniray.worker import WorkerServer
from tests.unit._core_test_utils import add_core_thread_finalizer


pytestmark = pytest.mark.unit


class _EncodeOnce:
    reductions = 0

    def __init__(self, value: bytes) -> None:
        self.value = value

    def __reduce__(self):
        type(self).reductions += 1
        return type(self), (self.value,)


class _Unserializable:
    def __reduce__(self):
        raise TypeError("second argument cannot serialize")


class _ManualReferenceMailbox:
    """Apply local handle release inline, advance GC only when requested.

    The real Core finalizer and owner/lineage collector still run unchanged.
    Only their scheduling is fake: no listener, thread, timer or real wait is
    needed to test submission's exact handoff and rollback boundaries.
    """

    def __init__(self, core: CoreWorker) -> None:
        self.core = core
        self.lock = threading.RLock()
        self.accepting = True
        self.pending: list[_RetryInlineGc] = []

    def enqueue(self, event: object) -> bool:
        assert isinstance(event, _LocalReferenceRelease)
        if self.core.owner_table.release_local_reference(
            event.object_id, event.token
        ):
            self.enqueue_internal(_RetryInlineGc(event.object_id))
        event.done.set()
        return True

    def enqueue_internal(self, event: object) -> bool:
        assert isinstance(event, _RetryInlineGc)
        self.pending.append(event)
        return True

    def drain(self) -> None:
        # Hard bound makes an accidental retry cycle fail instead of hanging.
        for _ in range(128):
            if not self.pending:
                return
            event = self.pending.pop(0)
            self.core._reference_released(event.object_id)
        raise AssertionError("manual reference collection did not converge")


def _core(*, threshold: int, seed: int = 0) -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID(bytes([0x11 + seed]) * 16)
    core.worker_id = WorkerID(bytes([0x22 + seed]) * 16)
    core.node_id = NodeID(bytes([0x33 + seed]) * 16)
    core.node_address = ("127.0.0.1", 12001 + seed * 2)
    core.owner_address = ("127.0.0.1", 12002 + seed * 2)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.inline_threshold = threshold
    core._submission_index = 0
    core._put_index = 0
    core._owner_table = ObjectOwnerTable()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._accepting = True
    core._owner_protocol_open = True
    core._owner_retain_admission_open = True
    core._inflight_borrow_ops = 0
    core._inflight_puts = 0
    core._inflight_submissions = 0
    core._accepted_task_count = 0
    core._submissions = queue.Queue()
    core._object_gc_obligations = {}
    core._inline_gc_obligations = core._object_gc_obligations
    core._gc_retry_timers = set()
    core._gc_retry_timers_open = False
    core._dead_nodes = {}
    core._reference_index = 0
    core._reference_mailbox = _ManualReferenceMailbox(core)
    core._foreign_lineage_prepared_collection_receipts = {}
    core._foreign_lineage_collection_receipts = {}
    core._ensure_foreign_lineage_runtime()
    return core


def _definition(core: CoreWorker) -> RemoteFunctionDefinition:
    return RemoteFunctionDefinition.from_callable(
        lambda *args, **kwargs: (args, kwargs), core.job_id
    )


def _seal_rpc(core: CoreWorker, calls: list[protocol.SealObject]):
    def rpc(_address: object, handler: str, message: object) -> object:
        assert handler == "seal_object"
        assert isinstance(message, protocol.SealObject)
        calls.append(message)
        return protocol.SealObjectReply(
            message.object_id, True, core.node_id, len(message.data),
            message.checksum,
        )

    return rpc


def _drop_reply(request: protocol.DropObjectReplica) -> protocol.DropObjectReplicaReply:
    return protocol.DropObjectReplicaReply(
        request.object_id, request.producer_attempt_id,
        request.owner_worker_id, request.node_id, request.checksum,
        protocol.DropObjectReplicaStatus.DROPPED,
    )


def _store_rpc(core: CoreWorker, seals: list[protocol.SealObject]):
    def rpc(_address: object, handler: str, message: object) -> object:
        if handler == "seal_object":
            return _seal_rpc(core, seals)(_address, handler, message)
        assert handler == "drop_object_replica"
        assert isinstance(message, protocol.DropObjectReplica)
        return _drop_reply(message)

    core._resolve_node_address = lambda _node_id: core.node_address
    return rpc


def _foreign_source(submitter: CoreWorker):
    owner = _core(threshold=1024, seed=1)
    producer, owner_ref = owner._register_submission(
        _definition(owner), (), {}, ResourceVector()
    )
    owner.owner_table.add_borrowed_reference(
        producer.object_id, (submitter.worker_id, "borrow")
    )
    reference = ObjectRef(
        producer.object_id, owner.worker_id, owner.owner_address
    )
    reference._borrower_token = "borrow"
    calls: list[tuple[str, object]] = []

    def rpc(_address: object, handler: str, request: object) -> object:
        assert _address == owner.owner_address
        calls.append((handler, request))
        operations = {
            "retain_owned_object_for_task": owner.retain_owned_object_for_task,
            "replace_retained_object_for_task": owner.replace_retained_object_for_task,
            "release_owned_object_for_task": owner.release_owned_object_for_task,
            "get_retained_owned_object": owner.get_retained_owned_object,
        }
        return operations[handler](request)

    submitter._borrow_rpc = rpc
    return owner, owner_ref, reference, calls


def _next_pending(core: CoreWorker) -> _PendingTask:
    for _ in range(32):
        item = core._submissions.get_nowait()
        if isinstance(item, _PendingTask):
            return item
    raise AssertionError("no pending task in bounded test queue")


def test_threshold_boundary_and_positional_keyword_order() -> None:
    first, second, keyword = b"first", b"second", b"keyword"
    first_size = len(cloudpickle.dumps(first))
    second_size = len(cloudpickle.dumps(second))
    keyword_size = len(cloudpickle.dumps(keyword))
    core = _core(threshold=first_size + second_size)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)

    pending, result = core._register_submission(
        _definition(core), (first, second), {"later": keyword},
        ResourceVector(),
    )
    try:
        assert tuple(type(arg) for arg in pending.spec.args) == (
            protocol.InlineArg, protocol.InlineArg,
        )
        assert isinstance(pending.spec.kwargs[0][1], protocol.StoredArg)
        assert len(seals) == 1
        assert seals[0].data == cloudpickle.dumps(keyword)
        # Equality stays inline; the next argument in stable TaskSpec order is
        # the first one whose inclusion would make the aggregate exceed it.
        assert first_size + second_size == core.inline_threshold
        assert first_size + second_size + keyword_size > core.inline_threshold
    finally:
        result.close()


def test_two_individually_small_arguments_cross_cumulative_budget() -> None:
    first, second = b"a", b"b"
    each = len(cloudpickle.dumps(first))
    core = _core(threshold=each)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)

    pending, result = core._register_submission(
        _definition(core), (first, second), {}, ResourceVector()
    )
    try:
        assert isinstance(pending.spec.args[0], protocol.InlineArg)
        assert isinstance(pending.spec.args[1], protocol.StoredArg)
        assert seals[0].data == cloudpickle.dumps(second)
    finally:
        result.close()


def test_lift_serializes_once_and_spec_carries_only_a_stored_descriptor() -> None:
    core = _core(threshold=0)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    _EncodeOnce.reductions = 0
    value = _EncodeOnce(b"payload")

    pending, result = core._register_submission(
        _definition(core), (value,), {}, ResourceVector()
    )
    lifted = pending.spec.args[0]
    try:
        assert _EncodeOnce.reductions == 1
        assert isinstance(lifted, protocol.StoredArg)
        assert not any(
            isinstance(arg, protocol.InlineArg) and arg.data == seals[0].data
            for arg in pending.spec.args
        )
        snapshot = core.owner_table.snapshot(lifted.object_id)
        descriptor = core._stored_descriptors[lifted.object_id]
        assert snapshot.state is ObjectState.READY_STORED
        assert descriptor.storage is protocol.ResultStorage.OBJECT_STORE
        assert descriptor.inline_data is None
        assert descriptor.size_bytes == len(seals[0].data)
        assert descriptor.checksum == hashlib.sha256(seals[0].data).hexdigest()
        assert snapshot.local_tokens == frozenset()
        assert pending.dependency_hold in snapshot.submitted_tokens
    finally:
        result.close()


def test_nested_object_ref_argument_lifts_with_manifest_outside_bytes() -> None:
    core = _core(threshold=0)
    core.owner_address = ("127.0.0.1", 12002)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    source = core._put_serialized(cloudpickle.dumps(7))

    pending, result = core._register_submission(
        _definition(core), ({"nested": source, "padding": b"x" * 64},),
        {}, ResourceVector(),
    )
    try:
        argument = pending.spec.args[0]
        assert isinstance(argument, protocol.StoredArg)
        assert argument.nested_refs
        # The TaskSpec carries only object identity plus the manifest.  The
        # serialized container is a second sealed object, never control-plane
        # bytes, and retains the exact stream produced during first encoding.
        assert len(seals) == 2
        assert argument.object_id == seals[1].object_id
    finally:
        source.close()
        result.close()


def test_local_nested_lift_keeps_storage_gating_and_handle_lifetime_separate() -> None:
    core = _core(threshold=0)
    core.owner_address = ("127.0.0.1", 12002)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    source = core._put_serialized(cloudpickle.dumps(7))

    pending, result = core._register_submission(
        _definition(core), ({"nested": [source, source]},), {},
        ResourceVector(), max_retries=1,
    )
    argument = pending.spec.args[0]
    try:
        assert isinstance(argument, protocol.StoredArg)
        assert argument.nested_refs[0].object_id == source.object_id
        assert pending.protected_dependencies == (argument.object_id,)
        assert pending.nested_local_holds == (source.object_id,)
        assert pending.dependency_hold in core.owner_table.snapshot(
            argument.object_id
        ).submitted_tokens
        assert pending.dependency_hold in core.owner_table.snapshot(
            source.object_id
        ).submitted_tokens

        prepared, dependencies, protected = core._prepare_task_dependencies(
            pending.spec
        )
        assert prepared.args == (argument,)
        assert protected == (argument.object_id,)
        assert tuple(item.object_id for item in dependencies) == (
            argument.object_id,
        )
        push = protocol.PushTask(
            LeaseID.random(), WorkerID.random(), prepared, dependencies
        )
        assert push.spec.args[0] == argument
        assert not hasattr(push.dependencies[0], "data")
    finally:
        source.close()
        result.close()


def test_nested_lift_serializes_user_state_once() -> None:
    core = _core(threshold=0)
    core.owner_address = ("127.0.0.1", 12002)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    source = core._put_serialized(cloudpickle.dumps(9))
    _EncodeOnce.reductions = 0

    pending, result = core._register_submission(
        _definition(core),
        ({"nested": source, "state": _EncodeOnce(b"one-pass")},),
        {}, ResourceVector(),
    )
    try:
        assert isinstance(pending.spec.args[0], protocol.StoredArg)
        assert _EncodeOnce.reductions == 1
        assert seals[-1].object_id == pending.spec.args[0].object_id
    finally:
        source.close()
        result.close()


def test_nested_lift_system_retry_preserves_logical_holds_and_exact_stream() -> None:
    core = _core(threshold=0)
    seals: list[protocol.SealObject] = []
    core._rpc = _store_rpc(core, seals)
    local_producer, local = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )
    owner, owner_ref, foreign, owner_calls = _foreign_source(core)
    pending, result = core._register_submission(
        _definition(core), ({"local": local, "foreign": foreign},),
        {}, ResourceVector(), max_retries=1,
    )
    stored = pending.spec.args[0]
    assert isinstance(stored, protocol.StoredArg)
    local.close()
    assert core._dependencies_ready(pending)
    assert core.owner_table.snapshot(local.object_id).state is ObjectState.PENDING
    assert owner.owner_table.snapshot(foreign.object_id).state is ObjectState.PENDING

    reply = protocol.TaskReply(
        pending.task_id, pending.spec.attempt_id, WorkerID.random(),
        protocol.TaskReplyStatus.SYSTEM_ERROR,
        error=protocol.RemoteErrorInfo("RuntimeError", "one retry"),
    )
    assert not core._retry_explicit_system_failure(pending, reply)
    retried = _next_pending(core)
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert retried.spec.args[0] == stored
    assert retried.dependency_hold == pending.dependency_hold
    assert retried.nested_foreign_guards == pending.nested_foreign_guards
    assert len(seals) == 1
    assert [handler for handler, _ in owner_calls] == [
        "retain_owned_object_for_task"
    ]
    for object_id in (stored.object_id, local.object_id):
        assert core.owner_table.snapshot(object_id).submitted_tokens == frozenset({
            pending.dependency_hold
        })
    assert core._publish_task_error(retried, RuntimeError("cleanup"))
    assert core._finish_pending_task(retried)
    assert core._finish_pending_task(retried)
    for object_id in (stored.object_id, local.object_id):
        assert not core.owner_table.snapshot(object_id).submitted_tokens
    result.close()
    core._reference_mailbox.drain()
    assert core.owner_table.collection_state(
        stored.object_id
    ) is ObjectCollectionState.COLLECTED
    assert not owner.owner_table.snapshot(foreign.object_id).retained_tokens
    core._publish_task_error(local_producer, RuntimeError("cleanup"))
    core._finish_pending_task(local_producer)
    owner_ref.close()


@pytest.mark.parametrize("num_returns", [1, 2])
def test_nested_lift_whole_and_targeted_reconstruction_rebind_holds_without_reseal(
    num_returns: int,
) -> None:
    core = _core(threshold=0)
    seals: list[protocol.SealObject] = []
    core._rpc = _store_rpc(core, seals)
    _producer, local = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )
    owner, owner_ref, foreign, owner_calls = _foreign_source(core)
    pending, output = core._register_submission(
        _definition(core), (),
        {"value": {"local": local, "foreign": foreign}},
        ResourceVector(), max_retries=1, num_returns=num_returns,
    )
    outputs = (output,) if isinstance(output, ObjectRef) else output
    stored = pending.spec.kwargs[0][1]
    assert isinstance(stored, protocol.StoredArg)
    canonical_stream = seals[0].data
    original_transfers = {
        transfer.owner_worker_id: transfer for transfer in stored.nested_refs
    }
    for object_id in pending.output_ids:
        payload = cloudpickle.dumps(("result", object_id.return_index))
        descriptor = protocol.ResultDescriptor(
            object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
            core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(),
        )
        assert core.owner_table.publish_stored(
            object_id, pending.spec.attempt_id, core.node_id,
            descriptor=descriptor,
        )
        core._stored_descriptors[object_id] = descriptor
    core._recovery.record_task_success(pending.task_id, pending.spec.attempt_id)
    assert core._finish_pending_task(pending)
    core._reference_mailbox.drain()
    requested = pending.output_ids[-1]
    assert core.owner_table.mark_lost(requested, pending.spec.attempt_id)
    core._start_or_join_reconstruction(requested, core._object_waiter(requested))
    if num_returns > 1:
        core._start_open_targeted_reconstruction(pending.task_id)
    retried = _next_pending(core)
    rewritten = retried.spec.kwargs[0][1]
    assert isinstance(rewritten, protocol.StoredArg)
    assert rewritten.object_id == stored.object_id
    assert rewritten.serializer == stored.serializer
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert retried.protected_dependencies == (stored.object_id,)
    assert retried.nested_local_holds == (local.object_id,)
    assert retried.foreign_dependency_guards == ()
    assert len(retried.nested_foreign_guards) == 1
    assert len(seals) == 1 and seals[0].data == canonical_stream
    assert core._dependencies_ready(retried)
    assert core.owner_table.snapshot(local.object_id).state is ObjectState.PENDING
    assert owner.owner_table.snapshot(foreign.object_id).state is ObjectState.PENDING
    for transfer in rewritten.nested_refs:
        assert transfer.hold != original_transfers[transfer.owner_worker_id].hold
        assert transfer.hold.origin_attempt_id == retried.spec.attempt_id
    assert rewritten.nested_refs[0].hold == retried.dependency_hold
    foreign_hold = retried.nested_foreign_guards[0].hold
    assert owner.owner_table.has_retained_reference_for_task(
        foreign.object_id, foreign_hold
    )
    assert not owner.owner_table.has_retained_reference_for_task(
        foreign.object_id, original_transfers[owner.worker_id].hold
    )
    record = core._foreign_lineage_registry.snapshot(pending.task_id)
    assert record is not None
    assert record.edges[0].roles == ForeignLineageRole.NESTED
    assert [handler for handler, _ in owner_calls] == [
        "retain_owned_object_for_task", "replace_retained_object_for_task"
    ]
    if num_returns > 1:
        assert retried.target_execution is not None
        assert retried.output_ids == (requested,)
        untouched = core.owner_table.snapshot(pending.output_ids[0])
        assert untouched.state is ObjectState.READY_STORED
        assert untouched.current_attempt == pending.spec.attempt_id
    assert core._publish_task_error(retried, RuntimeError("cleanup"))
    assert core._finish_pending_task(retried)
    for reference in outputs:
        reference.close()
    core._reference_mailbox.drain()
    local.close()
    owner_ref.close()


def test_nested_lift_later_encode_failure_rolls_back_local_foreign_and_storage() -> None:
    core = _core(threshold=0)
    seals: list[protocol.SealObject] = []
    core._rpc = _store_rpc(core, seals)
    _producer, local = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )
    owner, owner_ref, foreign, owner_calls = _foreign_source(core)
    with pytest.raises(Exception, match="_Unserializable is not serializable"):
        core._register_submission(
            _definition(core),
            ({"local": local, "foreign": foreign}, _Unserializable()),
            {}, ResourceVector(),
        )
    assert len(seals) == 1
    stored_id = seals[0].object_id
    core._reference_mailbox.drain()
    assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTED
    assert stored_id not in core._objects
    assert stored_id not in core._stored_descriptors
    assert not core.owner_table.snapshot(local.object_id).submitted_tokens
    assert not core.owner_table.snapshot(local.object_id).lineage_tokens
    assert not owner.owner_table.snapshot(foreign.object_id).retained_tokens
    assert [handler for handler, _ in owner_calls] == [
        "retain_owned_object_for_task", "release_owned_object_for_task"
    ]
    assert core._inflight_submissions == core._inflight_puts == 0
    assert core._accepted_task_count == 0
    assert core._submissions.empty()
    local.close()
    owner_ref.close()


def test_last_output_gc_collects_nested_argument_stream_and_child_lifetimes() -> None:
    core = _core(threshold=0)
    seals: list[protocol.SealObject] = []
    core._rpc = _store_rpc(core, seals)
    local = core._put_serialized(cloudpickle.dumps(5))
    owner, owner_ref, foreign, _owner_calls = _foreign_source(core)
    producer_spec = owner.owner_table.snapshot(foreign.object_id).producer_task_spec
    assert owner.owner_table.publish_inline(
        foreign.object_id, producer_spec.attempt_id, cloudpickle.dumps(7)
    )
    owner._recovery.record_task_success(
        producer_spec.task_id, producer_spec.attempt_id
    )
    pending, outputs = core._register_submission(
        _definition(core), ({"local": local, "foreign": foreign},),
        {}, ResourceVector(), num_returns=2, max_retries=1,
    )
    assert isinstance(outputs, tuple)
    stored = pending.spec.args[0]
    assert isinstance(stored, protocol.StoredArg)
    local.close()
    owner_ref.close()
    owner.owner_table.release_borrowed_reference(
        foreign.object_id, (core.worker_id, "borrow")
    )
    for object_id in pending.output_ids:
        assert core.owner_table.publish_inline(
            object_id, pending.spec.attempt_id, cloudpickle.dumps("result")
        )
    core._recovery.record_task_success(pending.task_id, pending.spec.attempt_id)
    assert core._finish_pending_task(pending)
    outputs[0].close()
    core._reference_mailbox.drain()
    owner._reference_mailbox.drain()
    for object_id in (stored.object_id, local.object_id):
        snapshot = core.owner_table.snapshot(object_id)
        assert snapshot.lineage_tokens
        assert not snapshot.local_tokens
        assert not snapshot.submitted_tokens
    assert owner.owner_table.snapshot(foreign.object_id).retained_tokens

    outputs[1].close()
    core._reference_mailbox.drain()
    owner._reference_mailbox.drain()
    for object_id in pending.output_ids + (stored.object_id, local.object_id):
        assert core.owner_table.collection_state(
            object_id
        ) is ObjectCollectionState.COLLECTED
        assert object_id not in core._objects
        assert object_id not in core._stored_descriptors
    assert owner.owner_table.collection_state(
        foreign.object_id
    ) is ObjectCollectionState.COLLECTED
    assert core._foreign_lineage_registry.snapshot(pending.task_id) is None
    assert not core._foreign_lineage_collection_receipts
    assert not core._object_gc_obligations
    assert not core._recovery.reconstruction_snapshot(stored.object_id).is_put


def test_nested_lift_seal_failure_rolls_back_source_hold_and_internal_put(
    request: pytest.FixtureRequest,
) -> None:
    core = _core(threshold=0)
    add_core_thread_finalizer(request, core)
    core.owner_address = ("127.0.0.1", 12002)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    source = core._put_serialized(cloudpickle.dumps(11))
    rejected: list[protocol.SealObject] = []

    def reject(_address: object, handler: str, message: object) -> object:
        assert handler == "seal_object"
        assert isinstance(message, protocol.SealObject)
        rejected.append(message)
        return protocol.SealObjectReply(
            message.object_id, False, core.node_id, 0, "0" * 64,
            error="store full",
        )

    core._rpc = reject
    with pytest.raises(Exception, match="store full"):
        core._register_submission(
            _definition(core), ({"nested": source},), {}, ResourceVector()
        )

    core._reference_mailbox.drain()
    assert len(rejected) == 1
    assert core.owner_table.snapshot(source.object_id).submitted_tokens == frozenset()
    failed_id = rejected[0].object_id
    assert core.owner_table.collection_state(
        failed_id
    ) is ObjectCollectionState.COLLECTED
    assert failed_id not in core._objects
    assert failed_id not in core._stored_descriptors
    source.close()


def test_lift_rollback_collects_partial_put_after_later_encode_failure(
    request: pytest.FixtureRequest,
) -> None:
    core = _core(threshold=0)
    add_core_thread_finalizer(request, core)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)

    with pytest.raises(Exception, match="_Unserializable is not serializable"):
        core._register_submission(
            _definition(core), (b"lift first", _Unserializable()), {},
            ResourceVector(),
        )

    core._reference_mailbox.drain()
    assert len(seals) == 1
    lifted_id = seals[0].object_id
    core._rpc = lambda _address, handler, message: (
        _drop_reply(message)
        if handler == "drop_object_replica"
        else pytest.fail("unexpected GC handler: {}".format(handler))
    )
    core._resolve_node_address = lambda _node: core.node_address
    core._reference_released(lifted_id)
    assert core.owner_table.collection_state(
        lifted_id
    ) is ObjectCollectionState.COLLECTED
    assert lifted_id not in core._objects
    assert lifted_id not in core._stored_descriptors
    assert core._recovery.reconstruction_snapshot(lifted_id).is_put is False


def test_lift_seal_failure_rolls_back_internal_put_metadata(
    request: pytest.FixtureRequest,
) -> None:
    core = _core(threshold=0)
    add_core_thread_finalizer(request, core)
    seen: list[protocol.SealObject] = []

    def reject(_address: object, handler: str, message: object) -> object:
        assert handler == "seal_object"
        assert isinstance(message, protocol.SealObject)
        seen.append(message)
        return protocol.SealObjectReply(
            message.object_id, False, core.node_id, 0, "0" * 64,
            error="store full",
        )

    core._rpc = reject
    with pytest.raises(Exception, match="store full"):
        core._register_submission(
            _definition(core), (b"must lift",), {}, ResourceVector()
        )

    core._reference_mailbox.drain()
    assert len(seen) == 1
    object_id = seen[0].object_id
    assert core.owner_table.collection_state(
        object_id
    ) is ObjectCollectionState.COLLECTED
    assert object_id not in core._objects
    assert core._recovery.reconstruction_snapshot(object_id).is_put is False


def test_ready_lift_handle_binding_failure_is_handed_to_gc(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
) -> None:
    core = _core(threshold=0)
    add_core_thread_finalizer(request, core)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    real_bind = ObjectRef._bind_local_reference

    def fail_bind(self: ObjectRef, owner: CoreWorker, token: object) -> None:
        del self, owner, token
        raise RuntimeError("bind allocation failed")

    monkeypatch.setattr(ObjectRef, "_bind_local_reference", fail_bind)
    with pytest.raises(RuntimeError, match="bind allocation failed"):
        core._register_submission(
            _definition(core), (b"published before bind",), {},
            ResourceVector(),
        )
    monkeypatch.setattr(ObjectRef, "_bind_local_reference", real_bind)

    assert len(seals) == 1
    object_id = seals[0].object_id
    core._rpc = lambda _address, handler, message: (
        _drop_reply(message)
        if handler == "drop_object_replica"
        else pytest.fail("unexpected GC handler: {}".format(handler))
    )
    core._resolve_node_address = lambda _node: core.node_address
    core._reference_mailbox.drain()
    assert core.owner_table.collection_state(
        object_id
    ) is ObjectCollectionState.COLLECTED
    assert object_id not in core._objects


def test_lift_lineage_outlives_execution_and_final_output_gc_collects_both(
    request: pytest.FixtureRequest,
) -> None:
    core = _core(threshold=0)
    add_core_thread_finalizer(request, core)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    pending, result = core._register_submission(
        _definition(core), (b"large by value",), {}, ResourceVector(),
        max_retries=1,
    )
    lifted = pending.spec.args[0]
    assert isinstance(lifted, protocol.StoredArg)
    lifted_id = lifted.object_id
    lineage_token = "lineage:{}:{}".format(
        pending.spec.task_id, lifted_id
    )
    output_payload = cloudpickle.dumps("done")
    try:
        assert pending.dependency_hold in core.owner_table.snapshot(
            lifted_id
        ).submitted_tokens
        assert lineage_token in core.owner_table.snapshot(
            lifted_id
        ).lineage_tokens
        assert core.owner_table.publish_inline(
            pending.object_id, pending.spec.attempt_id, output_payload
        )
        core._recovery.record_task_success(
            pending.spec.task_id, pending.spec.attempt_id
        )
        core._wake_object(pending.object_id)
        assert core._finish_pending_task(pending)
        lifted_snapshot = core.owner_table.snapshot(lifted_id)
        assert pending.dependency_hold not in lifted_snapshot.submitted_tokens
        assert lineage_token in lifted_snapshot.lineage_tokens

        def gc_rpc(_address: object, handler: str, message: object) -> object:
            assert handler == "drop_object_replica"
            assert isinstance(message, protocol.DropObjectReplica)
            return _drop_reply(message)

        core._rpc = gc_rpc
        core._resolve_node_address = lambda _node: core.node_address
        result.close()
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(
            pending.object_id
        ) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(
            lifted_id
        ) is ObjectCollectionState.COLLECTED
        assert core._recovery.reconstruction_snapshot(lifted_id).is_put is False
    finally:
        result.close()


def test_lost_lifted_argument_is_terminal_instead_of_staying_blocked(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
) -> None:
    core = _core(threshold=0)
    add_core_thread_finalizer(request, core)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    pending, result = core._register_submission(
        _definition(core), (b"lost before dispatch",), {},
        ResourceVector(),
    )
    lifted = pending.spec.args[0]
    assert isinstance(lifted, protocol.StoredArg)
    snapshot = core.owner_table.snapshot(lifted.object_id)
    assert core.owner_table.remove_location(
        lifted.object_id, snapshot.current_attempt, core.node_id
    )
    monkeypatch.setattr(core, "_finish_pending_task", lambda _pending: True)
    try:
        # Terminal loss is promoted into preparation, which publishes an error
        # for the consumer; it is not parked in _blocked_tasks indefinitely.
        assert core._dependencies_ready(pending)
        with pytest.raises(UnreconstructableObjectError, match="no replayable"):
            core._prepare_task_dependencies(pending.spec)
        core._fail_pending_task(
            pending, UnreconstructableObjectError(
                "dependency replica was lost and no replayable producer lineage exists"
            )
        )
        assert core.owner_table.snapshot(
            pending.object_id
        ).state is ObjectState.ERROR
    finally:
        result.close()


@pytest.mark.parametrize("with_nested_ref", [False, True])
def test_prepared_lease_and_push_keep_lifted_bytes_out_of_task_spec(
    with_nested_ref: bool,
) -> None:
    core = _core(threshold=0)
    seals: list[protocol.SealObject] = []
    core._rpc = _seal_rpc(core, seals)
    source = None
    value: object = b"descriptor only"
    if with_nested_ref:
        _producer, source = core._register_submission(
            _definition(core), (), {}, ResourceVector()
        )
        value = {"padding": value, "nested": source}
    pending, result = core._register_submission(
        _definition(core), (), {"value": value}, ResourceVector()
    )
    try:
        prepared, dependencies, _protected = core._prepare_task_dependencies(
            pending.spec
        )
        keyword = prepared.kwargs[0][1]
        assert isinstance(keyword, protocol.StoredArg)
        assert len(dependencies) == 1
        assert dependencies[0].object_id == keyword.object_id
        lease = protocol.RequestWorkerLease(
            LeaseID.random(), prepared.task_id, prepared.attempt_id,
            prepared.resources, core.node_id, core.worker_id,
            dependencies=dependencies, return_ids=prepared.return_ids(),
        )
        target_node = NodeID.random()
        target_worker = WorkerID.random()
        grant = protocol.GrantWorkerLease(
            lease.lease_id, prepared.task_id, prepared.attempt_id,
            target_node, target_worker, ("127.0.0.1", 12100),
            AllocationToken("stored-argument-lease"),
            tuple(replace(item, node_id=target_node) for item in dependencies),
        )
        core._validate_granted_dependencies(dependencies, grant)
        # Exact control-plane messages carry metadata only.  The Node's pull
        # changes the replica route, never the stored stream or nested holds.
        push = protocol.PushTask(
            grant.lease_id, target_worker, prepared, grant.dependencies,
        )
        assert push.spec.kwargs[0][1] == keyword
        for message in (prepared, lease, grant, push):
            assert seals[0].data not in cloudpickle.dumps(message)
    finally:
        if source is not None:
            source.close()
        result.close()


def test_worker_embedded_core_uses_the_same_argument_lift_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = object.__new__(WorkerServer)
    worker.worker_id = WorkerID.random()
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 12101)
    worker.gcs_address = ("127.0.0.1", 12102)
    worker.inline_threshold = 0
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core = None
    worker._embedded_core_job_id = None
    worker._embedded_core_stopped = False
    worker._worker_core_enabled = True
    worker._owner_retain_admission_open = True
    worker.event_sink = MemoryEventSink()

    def create_core(
        node_address: object, node_id: NodeID, **options: object
    ) -> CoreWorker:
        core = _core(threshold=options["inline_threshold"])
        core.job_id = options["job_id"]
        core.worker_id = options["worker_id"]
        core.node_id = node_id
        core.node_address = node_address
        core.owner_address = options["owner_address"]
        core.driver_task_id = TaskID.for_driver(core.job_id)
        return core

    # Exercise Worker embedding and the real Core submission implementation;
    # only construction/scheduling is replaced with the pure fixture above.
    monkeypatch.setattr(worker_module, "CoreWorker", create_core)
    core = worker._embedded_core_for(JobID.random())
    seals: list[protocol.SealObject] = []

    def rpc(_address: object, handler: str, message: object) -> object:
        if handler == "seal_object":
            assert isinstance(message, protocol.SealObject)
            seals.append(message)
            return protocol.SealObjectReply(
                message.object_id, True, core.node_id, len(message.data),
                message.checksum,
            )
        if handler == "drop_object_replica":
            assert isinstance(message, protocol.DropObjectReplica)
            return _drop_reply(message)
        raise AssertionError("unexpected worker-Core RPC: {}".format(handler))

    core._rpc = rpc
    core._resolve_node_address = lambda _node: core.node_address
    pending, result = core._register_submission(
        _definition(core), (b"worker child argument",), {},
        ResourceVector(),
    )
    try:
        assert len(seals) == 1
        assert isinstance(pending.spec.args[0], protocol.StoredArg)
        error = RuntimeError("test cleanup")
        assert core._publish_task_error(pending, error)
        assert core._finish_pending_task(pending)
        result.close()
        core._reference_mailbox.drain()
    finally:
        result.close()
