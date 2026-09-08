from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.object_store import (
    IncompleteObjectError,
    InvalidWriteError,
    ObjectAlreadySealedError,
    ObjectNotSealedError,
    ObjectStore,
    ObjectStoreFullError,
)
from miniray.ownership import (
    ConflictingObjectResultError,
    InvalidObjectTransitionError,
    ObjectOwnerTable,
    ObjectState,
    OwnershipError,
    ReferenceKind,
)
from miniray.protocol import (
    ResultDescriptor,
    ResultStorage,
    TaskReferenceHold,
    TaskReferenceHoldKind,
)


def object_and_attempts(
    submission_index: int = 0,
) -> tuple[ObjectID, AttemptID, AttemptID, AttemptID]:
    job_id = JobID(bytes.fromhex("01" * 16))
    task_id = TaskID.derive(
        job_id, TaskID.for_driver(job_id), submission_index
    )
    object_id = ObjectID.for_task(task_id)
    return (
        object_id,
        AttemptID(task_id, 0),
        AttemptID(task_id, 1),
        AttemptID(task_id, 2),
    )


def node_id(byte: int) -> NodeID:
    return NodeID(bytes([byte]) * 16)


def stored_descriptor(
    object_id: ObjectID, location: NodeID, *, checksum_byte: int = 0
) -> ResultDescriptor:
    return ResultDescriptor(
        object_id=object_id,
        storage=ResultStorage.OBJECT_STORE,
        size_bytes=4,
        owner_worker_id=WorkerID(bytes.fromhex("02" * 16)),
        node_id=location,
        checksum=f"{checksum_byte:02x}" * 32,
    )


def test_store_reserves_capacity_and_exposes_only_sealed_bytes() -> None:
    store = ObjectStore(capacity_bytes=8)
    object_id = object_and_attempts()[0]
    too_large_id = object_and_attempts(1)[0]
    invalid_payload_id = object_and_attempts(2)[0]

    store.create(object_id, 5)
    assert store.used_bytes == 5
    assert store.available_bytes == 3
    assert not store.contains(object_id)
    assert store.contains(object_id, sealed_only=False)

    store.write(object_id, b"abc", offset=0)
    with pytest.raises(ObjectNotSealedError):
        store.get(object_id)
    with pytest.raises(IncompleteObjectError):
        store.seal(object_id)
    with pytest.raises(ObjectStoreFullError):
        store.create(too_large_id, 4)

    store.write(object_id, b"de", offset=3)
    store.seal(object_id)
    assert store.get(object_id) == b"abcde"
    assert store.contains(object_id)

    with pytest.raises(InvalidWriteError):
        store.put(invalid_payload_id, 3)  # type: ignore[arg-type]


def test_store_seals_exactly_once_and_pin_tokens_are_idempotent() -> None:
    store = ObjectStore(capacity_bytes=4)
    object_id = object_and_attempts()[0]
    store.put(object_id, b"data")

    with pytest.raises(ObjectAlreadySealedError):
        store.seal(object_id)
    with pytest.raises(ObjectAlreadySealedError):
        store.write(object_id, b"x")

    assert store.pin(object_id, token="transfer-1") == "transfer-1"
    assert store.pin(object_id, token="transfer-1") == "transfer-1"
    assert store.snapshot(object_id).pin_count == 1
    assert not store.delete(object_id)

    assert store.unpin(object_id, "transfer-1")
    assert not store.unpin(object_id, "transfer-1")
    assert store.delete(object_id)
    assert store.used_bytes == 0


def test_reference_tokens_keep_object_live_until_every_reason_is_released() -> None:
    table = ObjectOwnerTable()
    object_id, attempt_0, _, _ = object_and_attempts()
    table.register(
        object_id,
        current_attempt=attempt_0,
        producer_task_spec="task-spec-placeholder",
        local_token="driver-handle",
    )

    submitted_hold = TaskReferenceHold(
        TaskReferenceHoldKind.SUBMITTED,
        WorkerID.random(),
        attempt_0.task_id,
        attempt_0,
    )
    assert table.add_submitted_reference(object_id, submitted_hold)
    assert not table.add_submitted_reference(object_id, submitted_hold)
    assert table.add_borrowed_reference(object_id, "borrower:1")
    assert table.add_contained_reference(object_id, "container:outer")

    assert table.release_local_reference(object_id, "driver-handle")
    assert table.is_live(object_id)
    assert not table.collect_if_unused(object_id)

    assert table.release_submitted_reference(object_id, submitted_hold)
    assert not table.release_submitted_reference(object_id, submitted_hold)
    assert table.release_borrowed_reference(object_id, "borrower:1")
    assert table.is_live(object_id)  # The nested/contained edge remains.
    assert table.release_contained_reference(object_id, "container:outer")
    assert not table.is_live(object_id)

    snapshot = table.snapshot(object_id)
    assert snapshot.is_reconstructible
    assert table.collect_if_unused(object_id)
    assert not table.contains(object_id)


def test_attempt_fencing_rejects_stale_results_and_location_removals() -> None:
    table = ObjectOwnerTable()
    object_id, attempt_0, attempt_1, attempt_2 = object_and_attempts()
    table.register(
        object_id,
        current_attempt=attempt_0,
        producer_task_spec="task-spec-placeholder",
    )

    assert table.advance_attempt(
        object_id, expected_attempt=attempt_0, next_attempt=attempt_1
    )
    # Repeated retry messages are harmless, while messages based on an older
    # attempt cannot advance the object a second time.
    assert table.advance_attempt(
        object_id, expected_attempt=attempt_0, next_attempt=attempt_1
    )
    assert not table.advance_attempt(
        object_id, expected_attempt=attempt_0, next_attempt=attempt_2
    )
    first_node = node_id(1)
    assert not table.publish_stored(object_id, attempt_0, first_node)
    assert table.publish_stored(object_id, attempt_1, first_node)

    assert not table.remove_location(object_id, attempt_0, first_node)
    assert table.snapshot(object_id).locations == frozenset({first_node})
    assert table.remove_location(object_id, attempt_1, first_node)
    assert table.snapshot(object_id).state is ObjectState.LOST

    assert table.advance_attempt(
        object_id, expected_attempt=attempt_1, next_attempt=attempt_2
    )
    assert table.publish_stored(object_id, attempt_2, first_node)
    # A delayed remove for the old same-node replica must not erase attempt 2.
    assert not table.remove_location(object_id, attempt_1, first_node)
    assert table.snapshot(object_id).locations == frozenset({first_node})


def test_owner_states_do_not_mix_inline_stored_and_error_results() -> None:
    table = ObjectOwnerTable()
    inline_id, inline_attempt, _, _ = object_and_attempts(0)
    failed_id, failed_attempt, _, _ = object_and_attempts(1)
    lost_id, lost_attempt, _, _ = object_and_attempts(2)
    first_node = node_id(1)
    second_node = node_id(2)

    table.register(inline_id, current_attempt=inline_attempt)
    assert table.publish_inline(inline_id, inline_attempt, b"small")
    assert table.publish_inline(inline_id, inline_attempt, b"small")
    assert table.snapshot(inline_id).state is ObjectState.READY_INLINE
    with pytest.raises(InvalidObjectTransitionError):
        table.publish_stored(inline_id, inline_attempt, first_node)

    table.register(failed_id, current_attempt=failed_attempt)
    error = {"code": "USER_ERROR", "message": "boom"}
    assert table.publish_error(failed_id, failed_attempt, error)
    assert table.snapshot(failed_id).state is ObjectState.ERROR
    with pytest.raises(InvalidObjectTransitionError):
        table.publish_inline(failed_id, failed_attempt, b"late success")

    table.register(lost_id, current_attempt=lost_attempt)
    assert table.publish_stored(lost_id, lost_attempt, first_node)
    assert table.remove_location(lost_id, lost_attempt, first_node)
    with pytest.raises(InvalidObjectTransitionError):
        table.publish_error(lost_id, lost_attempt, {"code": "LATE_ERROR"})
    # A late location report may safely reveal that a current-attempt replica
    # still exists; it does not change the logical result.
    assert table.publish_stored(lost_id, lost_attempt, second_node)
    assert table.snapshot(lost_id).state is ObjectState.READY_STORED


def test_publish_stored_ready_location_epoch_conflict_has_no_mutation() -> None:
    table = ObjectOwnerTable()
    object_id, attempt, different_attempt, _ = object_and_attempts()
    location = node_id(1)
    descriptor = stored_descriptor(object_id, location)

    table.register(object_id, current_attempt=attempt)
    assert table.publish_stored(object_id, attempt, location)
    # Inject the defensive conflict this branch must reject: the logical object
    # is current at attempt 0, but the same physical route names attempt 1.
    table._entries[object_id].location_attempts[location] = different_attempt
    before = table.snapshot(object_id)

    with pytest.raises(
        ConflictingObjectResultError, match="different attempt epoch"
    ):
        table.publish_stored(
            object_id, attempt, location, descriptor=descriptor
        )

    assert table.snapshot(object_id) == before


def test_publish_stored_lost_descriptor_conflict_has_no_mutation() -> None:
    table = ObjectOwnerTable()
    object_id, attempt, _, _ = object_and_attempts()
    first_location = node_id(1)
    rediscovered_location = node_id(2)
    canonical = stored_descriptor(object_id, first_location)
    conflict = stored_descriptor(
        object_id, rediscovered_location, checksum_byte=1
    )

    table.register(object_id, current_attempt=attempt)
    assert table.publish_stored(
        object_id, attempt, first_location, descriptor=canonical
    )
    assert table.remove_location(object_id, attempt, first_location)
    before = table.snapshot(object_id)
    assert before.state is ObjectState.LOST

    with pytest.raises(
        ConflictingObjectResultError,
        match="rediscovered replica changed canonical descriptor",
    ):
        table.publish_stored(
            object_id,
            attempt,
            rediscovered_location,
            descriptor=conflict,
        )

    assert table.snapshot(object_id) == before


def test_generic_reference_api_keeps_token_namespaces_independent() -> None:
    table = ObjectOwnerTable()
    object_id = object_and_attempts()[0]
    table.register(object_id)

    generic_kinds = tuple(
        kind for kind in ReferenceKind
        if kind not in (ReferenceKind.SUBMITTED, ReferenceKind.RETAINED)
    )
    for kind in generic_kinds:
        assert table.add_reference(object_id, kind, "same-token")
    snapshot = table.snapshot(object_id)
    assert snapshot.local_tokens == frozenset({"same-token"})
    assert not snapshot.submitted_tokens
    assert snapshot.borrowed_tokens == frozenset({"same-token"})
    assert snapshot.contained_tokens == frozenset({"same-token"})

    for kind in generic_kinds:
        assert table.release_reference(object_id, kind, "same-token")
    assert not table.is_live(object_id)

    for task_kind in (ReferenceKind.SUBMITTED, ReferenceKind.RETAINED):
        with pytest.raises(OwnershipError, match="TaskReferenceHold"):
            table.add_reference(object_id, task_kind, "same-token")
        with pytest.raises(OwnershipError, match="TaskReferenceHold"):
            table.release_reference(object_id, task_kind, "same-token")
