"""Pure model tests for partial-loss multi-return reconstruction."""

from __future__ import annotations

import hashlib

import pytest

from miniray import protocol
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, JobID, NodeID, TaskID, WorkerID
from miniray.ownership import (
    ConflictingObjectResultError, InvalidObjectTransitionError,
    ObjectOwnerTable, ObjectState,
)
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import ResourceVector
from miniray.targeted_reconstruction import (
    TargetedReconstructionCoordinator, TargetedReconstructionError,
    TargetedRequestDisposition, TargetedSessionPhase,
)
from miniray.task_outputs import TargetExecutionKey, TaskExecutionKey


pytestmark = pytest.mark.unit


def _spec(*, retries: int = 4) -> protocol.TaskSpec:
    job_id = JobID(bytes.fromhex("d1" * 16))
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 19)
    return protocol.TaskSpec(
        job_id, task_id, AttemptID(task_id, 0),
        protocol.FunctionKey(job_id, __name__, "producer", "v1"),
        (), 3, ResourceVector(), WorkerID(bytes.fromhex("d2" * 16)),
        max_retries=retries,
    )


def _stored(
    object_id: object, owner_id: WorkerID, node_id: NodeID, payload: bytes
) -> protocol.ResultDescriptor:
    return protocol.ResultDescriptor(
        object_id=object_id,
        storage=protocol.ResultStorage.OBJECT_STORE,
        size_bytes=len(payload),
        owner_worker_id=owner_id,
        node_id=node_id,
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _fixture(*, retries: int = 4):
    spec = _spec(retries=retries)
    owner = ObjectOwnerTable()
    recovery = RecoveryManager()
    owner.register_task_outputs(spec)
    recovery.register_task(spec, max_retries=retries)
    outputs = spec.return_ids()
    nodes = tuple(NodeID(bytes([0xe1 + index]) * 16) for index in range(3))
    results = tuple(
        _stored(object_id, spec.owner_worker_id, node_id, payload)
        for object_id, node_id, payload in zip(
            outputs, nodes, (b"zero", b"one", b"two")
        )
    )
    assert owner.publish_task_outputs(
        TaskExecutionKey.from_task_spec(spec),
        results,
    )
    recovery.record_task_success(spec.task_id, spec.attempt_id)
    return (
        spec, outputs, nodes, results, owner, recovery,
        TargetedReconstructionCoordinator(recovery, owner),
    )


def test_target_identity_has_no_ambiguous_output_ids_projection() -> None:
    spec, outputs, *_ = _fixture()
    from miniray.task_outputs import TargetOutputManifest

    manifest = TargetOutputManifest.from_task_spec(
        spec, (outputs[0], outputs[2])
    )

    assert manifest.full_output_ids == outputs
    assert manifest.target_output_ids == (outputs[0], outputs[2])
    assert not hasattr(manifest, "output_ids")


def test_pre_start_losses_merge_and_consume_one_attempt_and_budget() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    assert owner.mark_lost(outputs[0], spec.attempt_id)

    first = coordinator.request(outputs[2], spec.attempt_id)
    second = coordinator.request(outputs[0], spec.attempt_id)
    assert first.disposition is TargetedRequestDisposition.OPENED
    assert second.disposition is TargetedRequestDisposition.MERGED
    assert second.session.target_output_ids == (outputs[0], outputs[2])
    assert recovery.task_record(spec.task_id).retries_started == 0

    started = coordinator.start(spec.task_id)

    assert started.phase is TargetedSessionPhase.STARTED
    assert isinstance(started.execution, TargetExecutionKey)
    assert started.execution.target_output_ids == (outputs[0], outputs[2])
    assert started.execution.full_output_ids == outputs
    assert started.execution.attempt_id == AttemptID(spec.task_id, 1)
    record = recovery.task_record(spec.task_id)
    assert record.retries_started == 1
    assert record.current_attempt == AttemptID(spec.task_id, 1)
    assert recovery.active_recovery(spec.task_id) == record.current_attempt


def test_healthy_sibling_keeps_epoch_descriptor_state_and_location() -> None:
    spec, outputs, _nodes, _results, owner, _recovery, coordinator = _fixture()
    healthy_before = owner.snapshot(outputs[1])
    assert healthy_before.canonical_stored_result is not None
    assert owner.mark_lost(outputs[0], spec.attempt_id)

    coordinator.request(outputs[0], spec.attempt_id)
    started = coordinator.start(spec.task_id)
    healthy_after = owner.snapshot(outputs[1])

    assert healthy_after == healthy_before
    assert healthy_after.current_attempt == spec.attempt_id
    assert healthy_after.state is ObjectState.READY_STORED
    assert healthy_after.locations == healthy_before.locations
    assert (
        healthy_after.canonical_stored_result
        == healthy_before.canonical_stored_result
    )
    target = owner.snapshot(outputs[0])
    assert target.state is ObjectState.PENDING
    assert target.current_attempt == started.execution.attempt_id
    assert target.canonical_stored_result is None


def test_old_target_publication_is_fenced_after_target_only_advance() -> None:
    spec, outputs, _nodes, _results, owner, _recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    coordinator.request(outputs[2], spec.attempt_id)
    started = coordinator.start(spec.task_id)

    assert not owner.publish_stored(
        outputs[2], spec.attempt_id, NodeID.random()
    )
    assert owner.snapshot(outputs[2]).state is ObjectState.PENDING
    assert owner.publish_stored(
        outputs[2], started.execution.attempt_id, NodeID.random()
    )


def test_start_preflight_failure_changes_neither_authority() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    before_owner = tuple(owner.snapshot(item) for item in outputs)
    before_record = recovery.task_record(spec.task_id)
    before_values = (
        before_record.current_attempt, before_record.retries_started,
        before_record.state, recovery.active_recovery(spec.task_id),
    )

    # The OPEN observation is now stale.  Owner CAS rejects before recovery
    # budget or active markers can be committed.
    assert owner.advance_attempt(
        outputs[0], expected_attempt=spec.attempt_id,
        next_attempt=AttemptID(spec.task_id, 1),
    )
    with pytest.raises(TargetedReconstructionError):
        coordinator.start(spec.task_id)

    record = recovery.task_record(spec.task_id)
    assert (
        record.current_attempt, record.retries_started, record.state,
        recovery.active_recovery(spec.task_id),
    ) == before_values
    assert owner.snapshot(outputs[1]) == before_owner[1]
    assert owner.snapshot(outputs[2]) == before_owner[2]


def test_owner_commit_rejection_changes_no_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    coordinator.request(outputs[2], spec.attempt_id)
    plan = coordinator.validate_start(spec.task_id)
    before_owner = tuple(owner.snapshot(item) for item in outputs)
    record = recovery.task_record(spec.task_id)
    before_recovery = (
        record.current_attempt, record.retries_started, record.state,
        recovery.active_recovery(spec.task_id),
    )
    monkeypatch.setattr(
        owner, "commit_advance_target_outputs", lambda _plan: False
    )

    with pytest.raises(TargetedReconstructionError, match="owner rejected"):
        coordinator.commit_start(plan)

    assert tuple(owner.snapshot(item) for item in outputs) == before_owner
    record = recovery.task_record(spec.task_id)
    assert (
        record.current_attempt, record.retries_started, record.state,
        recovery.active_recovery(spec.task_id),
    ) == before_recovery
    session = coordinator.current_session(spec.task_id)
    assert session is not None
    assert session.phase is TargetedSessionPhase.OPEN


def test_targeted_publication_is_atomic_and_preserves_healthy_descriptor() -> None:
    spec, outputs, _nodes, _results, owner, _recovery, coordinator = _fixture()
    healthy_before = owner.snapshot(outputs[1])
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    coordinator.request(outputs[2], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    started = coordinator.start(spec.task_id)
    attempt = started.execution.attempt_id
    target_results = (
        _stored(outputs[0], spec.owner_worker_id, NodeID.random(), b"new-0"),
        _stored(outputs[2], spec.owner_worker_id, NodeID.random(), b"new-2"),
    )

    plan = owner.validate_publish_target_outputs(
        started.execution, target_results
    )
    assert plan is not None
    assert owner.snapshot(outputs[0]).state is ObjectState.PENDING
    assert owner.snapshot(outputs[2]).state is ObjectState.PENDING
    assert owner.commit_publish_target_outputs(plan)

    assert owner.snapshot(outputs[1]) == healthy_before
    assert owner.snapshot(outputs[0]).current_attempt == attempt
    assert owner.snapshot(outputs[2]).current_attempt == attempt
    assert owner.snapshot(outputs[0]).canonical_stored_result == target_results[0]
    assert owner.snapshot(outputs[2]).canonical_stored_result == target_results[1]
    receipt = owner.targeted_publication_receipt(started.execution)
    assert receipt == target_results
    assert owner.publish_target_outputs(started.execution, target_results)
    assert owner.targeted_publication_receipt(started.execution) == target_results
    changed = (
        _stored(outputs[0], spec.owner_worker_id, NodeID.random(), b"changed"),
        target_results[1],
    )
    with pytest.raises(
        ConflictingObjectResultError, match="result manifest"
    ):
        owner.publish_target_outputs(started.execution, changed)
    assert owner.targeted_publication_receipt(started.execution) == target_results

    # ACK loss followed by physical loss still replays the immutable receipt;
    # it does not reinterpret current LOST state as permission to republish.
    for result in target_results:
        assert owner.mark_lost(result.object_id, started.execution.attempt_id)
    assert owner.publish_target_outputs(started.execution, target_results)
    assert all(
        owner.snapshot(item).state is ObjectState.LOST
        for item in started.execution.target_output_ids
    )


def test_success_coordinator_requires_and_publishes_all_targets_atomically() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    healthy_before = owner.snapshot(outputs[1])
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    coordinator.request(outputs[2], spec.attempt_id)
    session = coordinator.start(spec.task_id)
    results = (
        _stored(outputs[0], spec.owner_worker_id, NodeID.random(), b"fresh-0"),
        _stored(outputs[2], spec.owner_worker_id, NodeID.random(), b"fresh-2"),
    )

    with pytest.raises(ValueError, match="targeted output order"):
        coordinator.validate_success(
            spec.task_id, session.execution.attempt_id, results[:1]
        )
    assert all(
        owner.snapshot(item).state is ObjectState.PENDING
        for item in (outputs[0], outputs[2])
    )
    assert recovery.task_record(spec.task_id).state is TaskState.RETRY_PENDING

    assert coordinator.succeed(
        spec.task_id, session.execution.attempt_id, results
    ) is None
    assert owner.snapshot(outputs[1]) == healthy_before
    assert all(
        owner.snapshot(item).state is ObjectState.READY_STORED
        for item in (outputs[0], outputs[2])
    )
    assert recovery.task_record(spec.task_id).state is TaskState.SUCCEEDED
    assert recovery.active_recovery(spec.task_id) is None
    assert coordinator.current_session(spec.task_id) is None


def test_complete_refuses_partial_target_publication() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    coordinator.request(outputs[2], spec.attempt_id)
    session = coordinator.start(spec.task_id)
    attempt = session.execution.attempt_id

    assert owner.publish_stored(outputs[0], attempt, NodeID.random())
    recovery.record_task_success(spec.task_id, attempt)

    assert coordinator.complete(spec.task_id, attempt) is None
    assert coordinator.current_session(spec.task_id) == session
    assert owner.snapshot(outputs[2]).state is ObjectState.PENDING


def test_complete_accepts_all_target_receipt_even_after_immediate_loss() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    session = coordinator.start(spec.task_id)
    attempt = session.execution.attempt_id
    result = _stored(
        outputs[0], spec.owner_worker_id, NodeID.random(), b"ephemeral"
    )
    assert owner.publish_target_outputs(session.execution, (result,))
    assert owner.mark_lost(outputs[0], attempt)
    assert coordinator.request(
        outputs[0], attempt
    ).disposition is TargetedRequestDisposition.QUEUED_NEXT
    recovery.record_task_success(spec.task_id, attempt)

    promoted = coordinator.complete(spec.task_id, attempt)

    assert promoted is not None
    assert promoted.target_output_ids == (outputs[0],)
    assert promoted.expected_attempts == {outputs[0]: attempt}


def test_system_retry_advances_same_targets_and_preserves_late_loss_queue() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    healthy_before = owner.snapshot(outputs[2])
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    first = coordinator.start(spec.task_id)
    attempt_1 = first.execution.attempt_id

    assert owner.mark_lost(outputs[1], spec.attempt_id)
    coordinator.request(outputs[1], spec.attempt_id)
    retry = coordinator.retry_system_failure(
        spec.task_id, attempt_1, SystemTaskError("worker died")
    )

    assert retry.target_output_ids == (outputs[0],)
    assert retry.execution.attempt_id == AttemptID(spec.task_id, 2)
    assert owner.snapshot(outputs[0]).current_attempt == retry.execution.attempt_id
    assert owner.snapshot(outputs[0]).state is ObjectState.PENDING
    assert owner.snapshot(outputs[2]) == healthy_before
    assert coordinator.queued_losses(spec.task_id)[0].object_id == outputs[1]
    assert recovery.task_record(spec.task_id).retries_started == 2
    assert recovery.active_recovery(spec.task_id) == retry.execution.attempt_id


def test_system_retry_owner_rejection_consumes_no_second_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    first = coordinator.start(spec.task_id)
    before = owner.snapshot(outputs[0])
    record = recovery.task_record(spec.task_id)
    before_recovery = (
        record.current_attempt, record.retries_started, record.state,
        recovery.active_recovery(spec.task_id),
    )
    plan = coordinator.validate_system_retry(
        spec.task_id, first.execution.attempt_id,
        SystemTaskError("worker died"),
    )
    monkeypatch.setattr(
        owner, "commit_retry_target_outputs", lambda _plan: False
    )

    with pytest.raises(TargetedReconstructionError, match="retry CAS"):
        coordinator.commit_system_retry(plan)

    assert owner.snapshot(outputs[0]) == before
    record = recovery.task_record(spec.task_id)
    assert (
        record.current_attempt, record.retries_started, record.state,
        recovery.active_recovery(spec.task_id),
    ) == before_recovery
    assert coordinator.current_session(spec.task_id) == first


def test_terminal_target_error_is_atomic_and_does_not_touch_healthy_sibling() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    healthy_before = owner.snapshot(outputs[1])
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    coordinator.request(outputs[2], spec.attempt_id)
    session = coordinator.start(spec.task_id)
    error = SystemTaskError("terminal reconstruction failure")

    plan = coordinator.validate_terminal_failure(
        spec.task_id, session.execution.attempt_id, error
    )
    assert all(
        owner.snapshot(item).state is ObjectState.PENDING
        for item in (outputs[0], outputs[2])
    )
    coordinator.commit_terminal_failure(plan)

    assert owner.snapshot(outputs[1]) == healthy_before
    assert all(
        owner.snapshot(item).state is ObjectState.ERROR
        and owner.snapshot(item).error is error
        for item in (outputs[0], outputs[2])
    )
    record = recovery.task_record(spec.task_id)
    assert record.state is TaskState.SUCCEEDED
    assert record.retries_remaining == spec.max_retries - 1
    assert recovery.active_recovery(spec.task_id) is None


def test_terminal_failure_atomically_converges_queued_late_losses() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    healthy_before = owner.snapshot(outputs[2])
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    session = coordinator.start(spec.task_id)
    attempt = session.execution.attempt_id

    assert owner.mark_lost(outputs[1], spec.attempt_id)
    coordinator.request(outputs[1], spec.attempt_id)
    error = SystemTaskError("cannot retry")
    plan = coordinator.validate_terminal_failure(spec.task_id, attempt, error)

    # Validation is side-effect-free across both active and queued targets.
    assert owner.snapshot(outputs[0]).state is ObjectState.PENDING
    assert owner.snapshot(outputs[1]).state is ObjectState.LOST
    assert recovery.task_record(spec.task_id).state is TaskState.RETRY_PENDING
    coordinator.commit_terminal_failure(plan)

    assert owner.snapshot(outputs[2]) == healthy_before
    assert owner.snapshot(outputs[0]).state is ObjectState.ERROR
    assert owner.snapshot(outputs[1]).state is ObjectState.ERROR
    assert owner.snapshot(outputs[0]).error is error
    assert owner.snapshot(outputs[1]).error is error
    assert coordinator.current_session(spec.task_id) is None
    record = recovery.task_record(spec.task_id)
    assert record.state is TaskState.SUCCEEDED
    assert record.retries_remaining == spec.max_retries - 1


def test_target_execution_receipts_are_bounded_by_final_sibling_gc() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    session = coordinator.start(spec.task_id)
    result = _stored(
        outputs[0], spec.owner_worker_id, NodeID.random(), b"recreated"
    )
    assert owner.publish_target_outputs(session.execution, (result,))
    assert owner.targeted_publication_receipt(session.execution) == (result,)

    with pytest.raises(InvalidObjectTransitionError, match="final sibling"):
        owner.validate_forget_target_execution_state(spec.task_id)

    # This unit test uses direct inline metadata collection to isolate receipt
    # lifetime.  Recovery lineage cleanup remains a separate Core transaction.
    for object_id in outputs:
        snapshot = owner.snapshot(object_id)
        for token in snapshot.local_tokens:
            owner.release_local_reference(object_id, token)
        if snapshot.state is ObjectState.READY_STORED:
            for location in snapshot.locations:
                owner.remove_location(
                    object_id, snapshot.current_attempt, location
                )
        plan = owner.begin_collection(
            object_id, canonical_size_bytes=(
                snapshot.canonical_stored_result.size_bytes
                if snapshot.canonical_stored_result is not None else None
            ), canonical_checksum=(
                snapshot.canonical_stored_result.checksum
                if snapshot.canonical_stored_result is not None else None
            ),
        )
        assert plan is not None
        owner.complete_collection(plan)

    cleanup = owner.validate_forget_target_execution_state(spec.task_id)
    assert cleanup.advances
    assert cleanup.publications == (session.execution,)
    assert owner.commit_forget_target_execution_state(cleanup)
    assert not owner.commit_forget_target_execution_state(cleanup)


def test_post_start_loss_is_queued_for_next_session() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    first = coordinator.start(spec.task_id)
    attempt_1 = first.execution.attempt_id

    assert owner.mark_lost(outputs[1], spec.attempt_id)
    queued = coordinator.request(outputs[1], spec.attempt_id)
    assert queued.disposition is TargetedRequestDisposition.QUEUED_NEXT
    assert queued.session.target_output_ids == (outputs[0],)
    assert tuple(item.object_id for item in coordinator.queued_losses(spec.task_id)) == (
        outputs[1],
    )
    assert recovery.task_record(spec.task_id).retries_started == 1

    result = _stored(
        outputs[0], spec.owner_worker_id, NodeID.random(), b"reconstructed"
    )
    assert owner.publish_target_outputs(first.execution, (result,))
    promoted = coordinator.complete(spec.task_id, attempt_1)
    assert promoted is None
    recovery.record_task_success(spec.task_id, attempt_1)
    promoted = coordinator.complete(spec.task_id, attempt_1)

    assert promoted is not None
    assert promoted.phase is TargetedSessionPhase.OPEN
    assert promoted.target_output_ids == (outputs[1],)
    second = coordinator.start(spec.task_id)
    assert second.execution.attempt_id == AttemptID(spec.task_id, 2)
    assert recovery.task_record(spec.task_id).retries_started == 2


def test_next_session_targets_can_have_different_expected_attempts() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture()
    assert owner.mark_lost(outputs[0], spec.attempt_id)
    coordinator.request(outputs[0], spec.attempt_id)
    first = coordinator.start(spec.task_id)
    attempt_1 = first.execution.attempt_id

    result = _stored(
        outputs[0], spec.owner_worker_id, NodeID.random(), b"first-pass"
    )
    assert owner.publish_target_outputs(first.execution, (result,))
    assert owner.mark_lost(outputs[0], attempt_1)
    assert owner.mark_lost(outputs[2], spec.attempt_id)
    assert coordinator.request(
        outputs[0], attempt_1
    ).disposition is TargetedRequestDisposition.QUEUED_NEXT
    assert coordinator.request(
        outputs[2], spec.attempt_id
    ).disposition is TargetedRequestDisposition.QUEUED_NEXT

    # The execution itself has completed; loss notification for its freshly
    # published target arrived before its session marker was retired.
    recovery.record_task_success(spec.task_id, attempt_1)
    promoted = coordinator.complete(spec.task_id, attempt_1)
    assert promoted is not None
    assert promoted.expected_attempts == {
        outputs[0]: attempt_1, outputs[2]: spec.attempt_id,
    }

    second = coordinator.start(spec.task_id)
    assert second.execution.attempt_id == AttemptID(spec.task_id, 2)
    assert owner.snapshot(outputs[0]).current_attempt == second.execution.attempt_id
    assert owner.snapshot(outputs[2]).current_attempt == second.execution.attempt_id
    assert owner.snapshot(outputs[1]).current_attempt == spec.attempt_id


def test_budget_exhaustion_leaves_open_session_and_owner_unchanged() -> None:
    spec, outputs, _nodes, _results, owner, recovery, coordinator = _fixture(
        retries=0
    )
    assert owner.mark_lost(outputs[1], spec.attempt_id)
    coordinator.request(outputs[1], spec.attempt_id)
    before = tuple(owner.snapshot(item) for item in outputs)

    with pytest.raises(TargetedReconstructionError, match="did not admit"):
        coordinator.start(spec.task_id)

    assert tuple(owner.snapshot(item) for item in outputs) == before
    assert recovery.task_record(spec.task_id).retries_started == 0
    assert recovery.active_recovery(spec.task_id) is None
    assert coordinator.current_session(spec.task_id).phase is TargetedSessionPhase.OPEN
