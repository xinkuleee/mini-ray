from __future__ import annotations

import hashlib

import pytest

pytestmark = pytest.mark.unit

from miniray.dependency import (
    ArgumentEncodingError,
    ContainedRef,
    contained_references,
    decode_inline_argument,
    encode_task_arguments,
    resolve_task_arguments,
    top_level_dependencies,
)
from miniray.object_manager import (
    ConflictingPullError,
    IncompletePullError,
    ObjectManager,
    PullAction,
    PullChecksumError,
    PullNotReadyError,
    PullState,
    choose_object_location,
    decide_pull,
)
from miniray.object_store import ObjectStore
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.protocol import (
    InlineArg, NestedReferenceTransfer, RefArg, TaskReferenceHold,
    TaskReferenceHoldKind,
)


def _runtime_ids(seed: int) -> tuple[ObjectID, AttemptID, NodeID, NodeID]:
    task_id = TaskID(bytes([seed]) * 16)
    return (
        ObjectID(task_id, 0),
        AttemptID(task_id, 0),
        NodeID(bytes([seed + 1]) * 16),
        NodeID(bytes([seed + 2]) * 16),
    )


def _logical_ref(seed: int) -> ContainedRef:
    task_id = TaskID(bytes([seed]) * 16)
    return ContainedRef(
        ObjectID(task_id, 0), WorkerID(bytes([seed + 1]) * 16)
    )


def _nested_transfer(
    reference: ContainedRef, *, task_seed: int = 40
) -> NestedReferenceTransfer:
    submitting_worker = WorkerID(bytes([task_seed + 1]) * 16)
    task_id = TaskID(bytes([task_seed]) * 16)
    return NestedReferenceTransfer(
        object_id=reference.object_id,
        owner_worker_id=reference.owner_worker_id,
        owner_address=("127.0.0.1", 20000 + task_seed),
        hold=TaskReferenceHold(
            TaskReferenceHoldKind.RETAINED,
            submitting_worker,
            task_id,
            AttemptID(task_id, 0),
        ),
    )


def test_top_level_ref_is_dependency_but_nested_ref_remains_handle() -> None:
    top_level = _logical_ref(1)
    nested = _logical_ref(3)
    transfer = _nested_transfer(nested)
    exported: list[object] = []

    def export(reference: object) -> NestedReferenceTransfer:
        exported.append(reference)
        return transfer

    arguments = encode_task_arguments(
        (top_level, {"items": [nested, 7, nested]}, "plain"),
        export_nested_ref=export,
    )

    assert arguments[0] == RefArg(top_level.object_id, top_level.owner_worker_id)
    assert isinstance(arguments[1], InlineArg)
    assert arguments[1].nested_refs == (transfer,)
    assert isinstance(arguments[2], InlineArg)
    assert top_level_dependencies(arguments) == (top_level.object_id,)
    assert contained_references(arguments) == (nested.object_id,)
    assert exported == [nested]

    imported_handle = object()
    decoded = decode_inline_argument(
        arguments[1], import_nested_ref=lambda _transfer: imported_handle
    )
    assert decoded["items"][0] is imported_handle
    assert decoded["items"][1] == 7
    assert decoded["items"][2] is imported_handle


def test_dependency_gate_decodes_nothing_until_every_top_level_ref_is_ready() -> None:
    top_a = _logical_ref(5)
    nested = _logical_ref(7)
    top_b = _logical_ref(9)
    transfer = _nested_transfer(nested, task_seed=42)
    arguments = encode_task_arguments(
        (
            top_a,
            {"handle": nested},
            top_b,
        ),
        export_nested_ref=lambda _reference: transfer,
    )
    materialized: list[ObjectID] = []

    def materialize(object_id: ObjectID, owner_worker_id: WorkerID) -> bytes:
        materialized.append(object_id)
        return (owner_worker_id, object_id)

    waiting = resolve_task_arguments(
        arguments,
        is_ready=lambda object_id: object_id == top_a.object_id,
        materialize_ref=materialize,
    )
    assert not waiting.ready
    assert waiting.missing == (top_b.object_id,)
    assert waiting.values is None
    assert materialized == []

    imported_handle = object()
    ready = resolve_task_arguments(
        arguments,
        is_ready=lambda _object_id: True,
        materialize_ref=materialize,
        import_nested_ref=lambda _transfer: imported_handle,
    )
    assert ready.ready
    assert ready.missing == ()
    assert ready.values[0] == (top_a.owner_worker_id, top_a.object_id)
    assert ready.values[1]["handle"] is imported_handle
    assert ready.values[2] == (top_b.owner_worker_id, top_b.object_id)
    # The contained handle was not materialized by the dependency resolver.
    assert materialized == [top_a.object_id, top_b.object_id]


def test_same_object_id_cannot_name_two_owners() -> None:
    conflicting = (
        RefArg("object-1", "owner-a"),
        RefArg("object-1", "owner-b"),
    )
    with pytest.raises(ArgumentEncodingError):
        top_level_dependencies(conflicting)


def test_location_selection_and_pull_decisions_are_pure_and_deterministic() -> None:
    locations = {"node-c", "node-a", "node-b"}

    assert choose_object_location(
        locations, requester_location="node-a"
    ) == "node-b"
    assert locations == {"node-c", "node-a", "node-b"}
    assert (
        choose_object_location(
            locations,
            requester_location="node-a",
            excluded_locations={"node-b"},
        )
        == "node-c"
    )

    assert decide_pull(
        "object-1", local_ready=True, locations=locations
    ).action is PullAction.LOCAL_READY
    start = decide_pull(
        "object-1",
        local_ready=False,
        locations=locations,
        requester_location="node-a",
    )
    assert start.action is PullAction.START_PULL
    assert start.source_location == "node-b"

    joined = decide_pull(
        "object-1",
        local_ready=False,
        locations=(),
        active_transfer_id="transfer-1",
        active_source_location="node-b",
    )
    assert joined.action is PullAction.JOIN_PULL
    assert joined.transfer_id == "transfer-1"
    assert decide_pull(
        "object-1", local_ready=False, locations=()
    ).action is PullAction.WAIT_FOR_LOCATION


def test_concurrent_pull_is_coalesced_and_ready_only_after_complete_seal() -> None:
    payload = b"abcdefgh"
    checksum = hashlib.sha256(payload).hexdigest()
    object_id, attempt_id, source_node, target_node = _runtime_ids(1)
    owner = ObjectOwnerTable()
    owner.register(object_id, current_attempt=attempt_id)
    assert owner.publish_stored(object_id, attempt_id, source_node)
    store = ObjectStore(capacity_bytes=32)
    manager = ObjectManager(target_node, store, owner)

    first = manager.request_pull(
        object_id,
        waiter_token="consumer-a",
        expected_size=len(payload),
        expected_checksum=checksum,
    )
    assert first.action is PullAction.START_PULL
    assert first.source_location == source_node
    assert first.transfer_id is not None

    second = manager.request_pull(
        object_id,
        waiter_token="consumer-b",
        expected_size=len(payload),
        expected_checksum=checksum,
    )
    assert second.action is PullAction.JOIN_PULL
    assert second.transfer_id == first.transfer_id
    assert manager.snapshot(object_id).waiter_tokens == frozenset(
        {"consumer-a", "consumer-b"}
    )

    assert manager.receive_chunk(
        object_id, b"efgh", offset=4, transfer_id=first.transfer_id
    ) == 4
    assert not manager.is_ready(object_id)
    assert not store.contains(object_id)
    with pytest.raises(PullNotReadyError):
        manager.get_local(object_id)
    with pytest.raises(IncompletePullError):
        manager.finish_pull(object_id, transfer_id=first.transfer_id)

    assert manager.receive_chunk(
        object_id, b"abcd", offset=0, transfer_id=first.transfer_id
    ) == len(payload)
    completion = manager.finish_pull(
        object_id, transfer_id=first.transfer_id
    )

    assert completion.waiter_tokens == frozenset({"consumer-a", "consumer-b"})
    assert manager.is_ready(object_id)
    assert manager.snapshot(object_id).state is PullState.READY
    assert manager.get_local(object_id) == payload
    owner_snapshot = owner.snapshot(object_id)
    assert owner_snapshot.state is ObjectState.READY_STORED
    assert owner_snapshot.locations == frozenset({source_node, target_node})


def test_duplicate_chunks_must_agree_and_checksum_failure_never_becomes_ready() -> None:
    payload = b"payload"
    advertised = hashlib.sha256(b"different").hexdigest()
    object_id, _attempt_id, source_node, target_node = _runtime_ids(4)
    store = ObjectStore(capacity_bytes=32)
    manager = ObjectManager(target_node, store)
    decision = manager.request_pull(
        object_id, locations={source_node}, waiter_token="consumer"
    )
    assert decision.action is PullAction.START_PULL
    manager.begin_transfer(
        object_id,
        size_bytes=len(payload),
        checksum=advertised,
        transfer_id=decision.transfer_id,
    )
    manager.receive_chunk(
        object_id, payload[:4], offset=0, transfer_id=decision.transfer_id
    )
    # Exact retransmission is idempotent.
    manager.receive_chunk(
        object_id, payload[:4], offset=0, transfer_id=decision.transfer_id
    )
    with pytest.raises(ConflictingPullError):
        manager.receive_chunk(
            object_id, b"FAIL", offset=0, transfer_id=decision.transfer_id
        )
    manager.receive_chunk(
        object_id, payload[4:], offset=4, transfer_id=decision.transfer_id
    )

    with pytest.raises(PullChecksumError):
        manager.finish_pull(object_id, transfer_id=decision.transfer_id)
    assert manager.snapshot(object_id).state is PullState.FAILED
    assert not manager.is_ready(object_id)
    assert not store.contains(object_id, sealed_only=False)


def test_waiting_pull_binds_descriptor_and_conflict_has_no_side_effects() -> None:
    checksum = hashlib.sha256(b"data").hexdigest()
    object_id, _attempt_id, source_node, target_node = _runtime_ids(7)
    manager = ObjectManager(target_node, ObjectStore(capacity_bytes=16))

    waiting = manager.request_pull(
        object_id,
        locations=(),
        waiter_token="consumer-a",
        expected_size=4,
        expected_checksum=checksum,
    )
    assert waiting.action is PullAction.WAIT_FOR_LOCATION
    before = manager.snapshot(object_id)
    assert before.expected_size == 4
    assert before.expected_checksum == checksum

    with pytest.raises(ConflictingPullError):
        manager.request_pull(
            object_id,
            locations={source_node},
            waiter_token="consumer-b",
            expected_size=5,
            expected_checksum=hashlib.sha256(b"other").hexdigest(),
        )
    assert manager.snapshot(object_id) == before

    start = manager.request_pull(
        object_id,
        locations={source_node},
        waiter_token="consumer-b",
        expected_size=4,
        expected_checksum=checksum,
    )
    assert start.action is PullAction.START_PULL
    assert manager.snapshot(object_id).waiter_tokens == frozenset(
        {"consumer-a", "consumer-b"}
    )


def test_failed_pull_can_reset_only_after_incomplete_replica_is_gone() -> None:
    payload = b"retry"
    object_id, _attempt_id, source_node, target_node = _runtime_ids(8)
    manager = ObjectManager(target_node, ObjectStore(capacity_bytes=32))
    decision = manager.request_pull(
        object_id,
        locations={source_node},
        expected_size=len(payload),
        expected_checksum=hashlib.sha256(payload).hexdigest(),
    )
    manager.fail_pull(object_id, "first source failed")

    assert manager.reset_failed_pull(object_id)
    assert not manager.reset_failed_pull(object_id)
    retry = manager.request_pull(
        object_id,
        locations={source_node},
        expected_size=len(payload),
        expected_checksum=hashlib.sha256(payload).hexdigest(),
    )
    assert retry.action is PullAction.START_PULL
    assert retry.transfer_id != decision.transfer_id
