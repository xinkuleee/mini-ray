"""Pure contracts for task-scoped, atomic multi-return owner metadata."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ConflictingObjectResultError,
    InvalidObjectTransitionError,
    ObjectAlreadyRegisteredError,
    ObjectOwnerTable,
    ObjectState,
)
from miniray.recovery import RecoveryManager, UnknownTaskError
from miniray.resources import ResourceVector
from miniray.task_outputs import (
    MAX_TASK_RETURNS,
    TaskExecutionKey,
    TaskOutputManifest,
    validate_num_returns,
)


pytestmark = pytest.mark.unit


def _spec(
    num_returns: int = 3, *, attempt_number: int = 0
) -> protocol.TaskSpec:
    job_id = JobID(bytes.fromhex("61" * 16))
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 7)
    return protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=AttemptID(task_id, attempt_number),
        function=protocol.FunctionKey(job_id, __name__, "producer", "v1"),
        args=(),
        num_returns=num_returns,
        resources=ResourceVector(),
        owner_worker_id=WorkerID(bytes.fromhex("62" * 16)),
        max_retries=2,
    )


def _inline(
    object_id: ObjectID, owner_id: WorkerID, node_id: NodeID, payload: bytes
) -> protocol.ResultDescriptor:
    return protocol.ResultDescriptor(
        object_id=object_id,
        storage=protocol.ResultStorage.INLINE,
        size_bytes=len(payload),
        owner_worker_id=owner_id,
        node_id=node_id,
        checksum=hashlib.sha256(payload).hexdigest(),
        inline_data=payload,
    )


def _stored(
    object_id: ObjectID, owner_id: WorkerID, node_id: NodeID, payload: bytes
) -> protocol.ResultDescriptor:
    return protocol.ResultDescriptor(
        object_id=object_id,
        storage=protocol.ResultStorage.OBJECT_STORE,
        size_bytes=len(payload),
        owner_worker_id=owner_id,
        node_id=node_id,
        checksum=hashlib.sha256(payload).hexdigest(),
    )


@pytest.mark.parametrize("value", [1, MAX_TASK_RETURNS])
def test_bounded_manifest_derives_ordered_stable_output_ids(value: int) -> None:
    spec = _spec(value)
    manifest = TaskOutputManifest.from_task_spec(spec)
    execution = TaskExecutionKey.from_task_spec(spec)

    assert validate_num_returns(value) == value
    assert manifest.output_ids == tuple(
        ObjectID.for_task(spec.task_id, index) for index in range(value)
    )
    assert execution.manifest == manifest
    assert execution.attempt_id == spec.attempt_id


@pytest.mark.parametrize("invalid", [True, 0, MAX_TASK_RETURNS + 1, 1.5, "2"])
def test_bounded_manifest_rejects_invalid_public_counts(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError), match="num_returns"):
        validate_num_returns(invalid)


def test_manifest_rejects_missing_reordered_or_foreign_slots() -> None:
    spec = _spec()
    outputs = spec.return_ids()
    other = TaskID.random()

    for invalid in (
        (),
        (outputs[1], outputs[0], outputs[2]),
        (outputs[0], outputs[1], ObjectID.for_task(other, 2)),
    ):
        with pytest.raises(ValueError, match="num_returns|ordered, contiguous"):
            TaskOutputManifest(spec.task_id, invalid)

    with pytest.raises(ValueError, match="belong"):
        TaskExecutionKey(
            TaskOutputManifest.for_task(spec.task_id, 1),
            AttemptID(TaskID.random(), 0),
        )


def test_batch_registration_validation_is_side_effect_free_and_commit_is_atomic() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    tokens = ("handle:0", "handle:1", "handle:2")

    plan = table.validate_register_task_outputs(spec, local_tokens=tokens)
    assert all(not table.contains(object_id) for object_id in spec.return_ids())

    table.commit_register_task_outputs(plan)
    for object_id, token in zip(spec.return_ids(), tokens):
        snapshot = table.snapshot(object_id)
        assert snapshot.current_attempt == spec.attempt_id
        assert snapshot.producer_task_spec == spec
        assert snapshot.local_tokens == frozenset({token})
        assert snapshot.state is ObjectState.PENDING

    # Exact replay is idempotent and still preflights the complete manifest.
    table.commit_register_task_outputs(plan)
    assert tuple(
        table.snapshot(object_id).local_tokens for object_id in spec.return_ids()
    ) == tuple(frozenset({token}) for token in tokens)


def test_partial_registration_is_rejected_without_filling_missing_siblings() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    first, second, third = spec.return_ids()
    table.register(
        first,
        current_attempt=spec.attempt_id,
        producer_task_spec=spec,
    )

    with pytest.raises(ObjectAlreadyRegisteredError, match="partially"):
        table.validate_register_task_outputs(spec)

    assert table.contains(first)
    assert not table.contains(second)
    assert not table.contains(third)


def test_success_manifest_validation_and_commit_cover_mixed_storage_atomically() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    node_id = NodeID(bytes.fromhex("63" * 16))
    results = (
        _inline(spec.return_ids()[0], spec.owner_worker_id, node_id, b"zero"),
        _stored(spec.return_ids()[1], spec.owner_worker_id, node_id, b"one"),
        _inline(spec.return_ids()[2], spec.owner_worker_id, node_id, b"two"),
    )

    plan = table.validate_publish_task_outputs(execution, results)
    assert plan is not None
    assert all(
        table.snapshot(object_id).state is ObjectState.PENDING
        for object_id in spec.return_ids()
    )

    assert table.commit_publish_task_outputs(plan)
    snapshots = tuple(table.snapshot(value) for value in spec.return_ids())
    assert tuple(value.state for value in snapshots) == (
        ObjectState.READY_INLINE,
        ObjectState.READY_STORED,
        ObjectState.READY_INLINE,
    )
    assert snapshots[0].inline_data == b"zero"
    assert snapshots[1].locations == frozenset({node_id})
    assert snapshots[2].inline_data == b"two"
    assert table.publish_task_outputs(execution, results)


def test_success_manifest_requires_exact_order_and_one_owner_before_mutation() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    node_id = NodeID.random()
    outputs = spec.return_ids()
    results = tuple(
        _inline(object_id, spec.owner_worker_id, node_id, bytes([index]))
        for index, object_id in enumerate(outputs)
    )

    with pytest.raises(ValueError, match="ordered output manifest"):
        table.validate_publish_task_outputs(execution, results[:-1])
    with pytest.raises(ValueError, match="ordered output manifest"):
        table.validate_publish_task_outputs(
            execution, (results[1], results[0], results[2])
        )
    with pytest.raises(ValueError, match="same logical owner"):
        table.validate_publish_task_outputs(
            execution,
            (
                results[0],
                _inline(outputs[1], WorkerID.random(), node_id, b"other"),
                results[2],
            ),
        )
    foreign_owner = WorkerID.random()
    foreign_results = tuple(
        _inline(object_id, foreign_owner, node_id, bytes([index]))
        for index, object_id in enumerate(outputs)
    )
    with pytest.raises(ValueError, match="registered task output owner"):
        table.validate_publish_task_outputs(execution, foreign_results)
    assert all(
        table.snapshot(object_id).state is ObjectState.PENDING
        for object_id in outputs
    )


@pytest.mark.parametrize(
    "field", ["object_id", "owner_worker_id", "node_id", "size_bytes", "checksum"]
)
def test_stored_replay_binds_complete_descriptor_without_partial_mutation(
    field: str,
) -> None:
    spec = _spec(2)
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    node = NodeID.random()
    payloads = (b"first", b"second")
    original = tuple(
        _stored(object_id, spec.owner_worker_id, node, payload)
        for object_id, payload in zip(spec.return_ids(), payloads)
    )
    assert table.publish_task_outputs(execution, original)
    before = tuple(table.snapshot(value) for value in spec.return_ids())

    changed = list(original)
    index = 1
    if field == "object_id":
        # Keep the ordered manifest valid while changing the descriptor bound
        # to slot one by swapping in a descriptor from another task.  The plan
        # constructor must reject before the owner table is consulted.
        changed[index] = replace(
            changed[index], object_id=ObjectID.for_task(TaskID.random(), 1)
        )
        expected = (ValueError, "ordered output manifest")
    elif field == "owner_worker_id":
        changed[index] = replace(changed[index], owner_worker_id=WorkerID.random())
        expected = (ValueError, "same logical owner")
    elif field == "node_id":
        changed[index] = replace(changed[index], node_id=NodeID.random())
        expected = (ConflictingObjectResultError, "identity or integrity")
    elif field == "size_bytes":
        changed[index] = replace(changed[index], size_bytes=99)
        expected = (ConflictingObjectResultError, "identity or integrity")
    else:
        changed[index] = replace(
            changed[index], checksum=hashlib.sha256(b"changed").hexdigest()
        )
        expected = (ConflictingObjectResultError, "identity or integrity")

    with pytest.raises(expected[0], match=expected[1]):
        table.publish_task_outputs(execution, tuple(changed))
    assert tuple(table.snapshot(value) for value in spec.return_ids()) == before


def test_stored_replay_attempt_drift_is_fenced_without_mutation() -> None:
    spec = _spec(2)
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    node = NodeID.random()
    results = tuple(
        _stored(object_id, spec.owner_worker_id, node, bytes([index]))
        for index, object_id in enumerate(spec.return_ids())
    )
    assert table.publish_task_outputs(execution, results)
    before = tuple(table.snapshot(value) for value in spec.return_ids())

    assert not table.publish_task_outputs(
        execution.for_attempt(spec.attempt_id.next()), results
    )
    assert tuple(table.snapshot(value) for value in spec.return_ids()) == before


def test_validated_success_plan_rechecks_and_refuses_partial_visibility() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    node_id = NodeID.random()
    results = tuple(
        _inline(object_id, spec.owner_worker_id, node_id, str(index).encode())
        for index, object_id in enumerate(spec.return_ids())
    )
    plan = table.validate_publish_task_outputs(execution, results)
    assert plan is not None

    assert table.publish_inline(
        spec.return_ids()[0], spec.attempt_id, results[0].inline_data
    )
    with pytest.raises(InvalidObjectTransitionError, match="partially published"):
        table.commit_publish_task_outputs(plan)

    assert tuple(
        table.snapshot(object_id).state for object_id in spec.return_ids()
    ) == (ObjectState.READY_INLINE, ObjectState.PENDING, ObjectState.PENDING)


def test_attempt_advance_validates_all_siblings_before_one_commit() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    next_attempt = spec.attempt_id.next()

    plan = table.validate_advance_task_outputs(execution, next_attempt)
    assert all(
        table.snapshot(value).current_attempt == spec.attempt_id
        for value in spec.return_ids()
    )
    assert table.commit_advance_task_outputs(plan)
    assert all(
        table.snapshot(value).current_attempt == next_attempt
        for value in spec.return_ids()
    )
    assert table.commit_advance_task_outputs(plan)

    stale_target = next_attempt.next()
    stale_expected = execution.for_attempt(spec.attempt_id)
    stale_plan = table.validate_advance_task_outputs(stale_expected, stale_target)
    assert not table.commit_advance_task_outputs(stale_plan)
    assert all(
        table.snapshot(value).current_attempt == next_attempt
        for value in spec.return_ids()
    )


def test_attempt_advance_rejects_one_unready_sibling_without_advancing_any() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    assert table.publish_inline(spec.return_ids()[1], spec.attempt_id, b"ready")

    with pytest.raises(InvalidObjectTransitionError, match="cannot advance"):
        table.validate_advance_task_outputs(
            TaskExecutionKey.from_task_spec(spec), spec.attempt_id.next()
        )
    assert all(
        table.snapshot(value).current_attempt == spec.attempt_id
        for value in spec.return_ids()
    )


def test_task_error_validation_commit_and_replay_are_manifest_atomic() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    error = RuntimeError("producer failed")

    plan = table.validate_publish_task_error(execution, error)
    assert plan is not None
    assert all(
        table.snapshot(value).state is ObjectState.PENDING
        for value in spec.return_ids()
    )
    assert table.commit_publish_task_error(plan)
    assert all(
        table.snapshot(value).state is ObjectState.ERROR
        and table.snapshot(value).error is error
        for value in spec.return_ids()
    )
    assert table.publish_task_error(execution, error)


def test_validated_error_plan_rechecks_and_never_partially_fails_siblings() -> None:
    spec = _spec()
    table = ObjectOwnerTable()
    table.register_task_outputs(spec)
    execution = TaskExecutionKey.from_task_spec(spec)
    error = RuntimeError("producer failed")
    plan = table.validate_publish_task_error(execution, error)
    assert plan is not None

    assert table.publish_error(spec.return_ids()[0], spec.attempt_id, error)
    with pytest.raises(InvalidObjectTransitionError, match="partially failed"):
        table.commit_publish_task_error(plan)
    assert tuple(
        table.snapshot(value).state for value in spec.return_ids()
    ) == (ObjectState.ERROR, ObjectState.PENDING, ObjectState.PENDING)


def test_recovery_lineage_survives_until_the_last_sibling_is_collected() -> None:
    spec = _spec()
    recovery = RecoveryManager()
    recovery.register_task(spec, max_retries=2)
    recovery.record_task_success(spec.task_id, spec.attempt_id)
    active = recovery.request_reconstruction(spec.return_ids()[0]).attempt_id
    assert active is not None

    for object_id in spec.return_ids()[:-1]:
        plan = recovery.validate_forget_collected_object(
            object_id,
            expected_task_spec=spec,
            expected_attempt=active,
        )
        assert not plan.remove_task
        assert recovery.commit_forget_collected_object(plan)
        remaining = next(
            sibling for sibling in spec.return_ids() if sibling != object_id
            and recovery.lineage_for_object(sibling) is not None
        )
        assert recovery.lineage_for_object(remaining) is not None
        assert recovery.task_record(spec.task_id).task_id == spec.task_id
        assert recovery.active_recovery(spec.task_id) == active
        joined = recovery.request_reconstruction(remaining)
        assert joined.action.name == "JOIN_RECONSTRUCTION"
        assert joined.attempt_id == active

    last = spec.return_ids()[-1]
    final = recovery.validate_forget_collected_object(
        last, expected_task_spec=spec, expected_attempt=active
    )
    assert final.remove_task
    assert recovery.commit_forget_collected_object(final)
    assert recovery.lineage_for_object(last) is None
    assert recovery.active_recovery(spec.task_id) is None
    with pytest.raises(UnknownTaskError):
        recovery.task_record(spec.task_id)
