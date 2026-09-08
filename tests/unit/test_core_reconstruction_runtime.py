"""Pure Core/owner/recovery composition; all FIFO progress is explicit.

The two real concurrent-request contracts live in
tests/integration/test_core_reconstruction_concurrency.py and are opt-in L1.
Here even an accidental Core constructor, socket, thread, timer, or blocking
wait is rejected before it can run.
"""

from __future__ import annotations

import hashlib
import socket
import threading
import time
from dataclasses import replace

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker, _DelayedTargetedReconstruction, _PendingTask,
    _PushRequestState, _StartTargetedReconstruction, _WAKE_COORDINATOR,
)
from miniray.foreign_lineage import ForeignLineageEdge, ForeignLineageRole
from miniray.ids import AttemptID, LeaseID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.errors import RuntimeShuttingDownError, SystemTaskError
from miniray.recovery import TaskState
from miniray.foreign_lineage_runtime import ForeignLineageRenewalDisposition
from miniray.resources import AllocationToken, ResourceVector
from tests.unit._pure_core import (
    SynchronousReferenceMailbox, close_pure_core, lost_reconstruction_core,
    make_pure_core,
)
from tests.unit._pure_reference_output_runtime import PureReferenceOutputRuntime




@pytest.fixture(autouse=True)
def _forbid_runtime_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure reconstruction contract attempted runtime work")

    def immediate_event_wait(event, timeout=None):
        assert event.is_set(), "pure contract attempted to block on an Event"
        return True

    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", immediate_event_wait)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _next_pending(core: CoreWorker) -> _PendingTask:
    for _ in range(32):
        item = core._submissions.get_nowait()
        if isinstance(item, _PendingTask):
            return item
        assert item is _WAKE_COORDINATOR
    pytest.fail("no pending reconstruction within 32 FIFO records")


def _stop(core: CoreWorker) -> None:
    close_pure_core(core)


def _lost_core():
    core, pending, refs, _descriptors = lost_reconstruction_core()
    return core, pending, refs[0]


def _lost_multi_core(*, partial: bool = False):
    return lost_reconstruction_core(num_returns=3, partial=partial)


@pytest.mark.unit
def test_pure_mailbox_preserves_real_close_and_explicit_collection() -> None:
    core = make_pure_core()
    pending, ref = core._register_submission(
        core.define_remote_function(lambda: 1), (), {}, ResourceVector(),
    )
    mailbox = core._reference_mailbox
    assert isinstance(mailbox, SynchronousReferenceMailbox)
    token = ref._local_token
    assert core.owner_table.snapshot(pending.object_id).local_tokens == frozenset({token})
    try:
        assert core._publish_task_error(pending, SystemTaskError("terminal"))
        ref.close()
        ref.close()
        assert ref.closed and ref._release_done.is_set()
        assert len(mailbox.releases) == 1
        assert not core.owner_table.snapshot(pending.object_id).local_tokens
        # A synchronous close releases the token but never pretends GC already
        # ran.  Explicit FIFO progress invokes the real owner/recovery collector.
        assert core.owner_table.collection_state(pending.object_id) is (
            ObjectCollectionState.ACTIVE
        )
        assert core._recovery.lineage_for_object(pending.object_id) is not None
        mailbox.drain()
        assert core.owner_table.collection_state(pending.object_id) is (
            ObjectCollectionState.COLLECTED
        )
        assert pending.object_id not in core._objects
        assert core._recovery.lineage_for_object(pending.object_id) is None
        assert mailbox.pending.empty()
    finally:
        ref.close()
        _stop(core)


@pytest.mark.unit
def test_local_lost_get_commits_one_reconstruction_plan() -> None:
    core, pending, ref = _lost_core()
    try:
        core._start_or_join_reconstruction(
            pending.object_id, core._objects[pending.object_id]
        )
        queued = core._submissions.get_nowait()
        assert isinstance(queued, _PendingTask)
        assert queued.spec.task_id == pending.spec.task_id
        assert queued.object_id == pending.object_id
        assert queued.spec.attempt_id == pending.spec.attempt_id.next()
        assert core._accepted_task_count == 1
        assert pending.object_id not in core._stored_descriptors
        assert not core._objects[pending.object_id].event.is_set()
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
    finally:
        core._publish_error(
            pending.object_id, pending.spec.attempt_id.next(), RuntimeError("cleanup")
        )
        ref.close()
        _stop(core)


@pytest.mark.unit
def test_shutdown_fence_prevents_reconstruction_state_mutation() -> None:
    core, pending, ref = _lost_core()
    before = core.owner_table.snapshot(pending.object_id)
    record = core._recovery.task_record(pending.spec.task_id)
    core._accepting = False
    try:
        with pytest.raises(RuntimeShuttingDownError, match="shutting down"):
            core._start_or_join_reconstruction(
                pending.object_id, core._objects[pending.object_id]
            )
        after = core.owner_table.snapshot(pending.object_id)
        assert after.state is ObjectState.LOST
        assert after.current_attempt == before.current_attempt
        assert record.retries_started == 0
        assert core._recovery.active_recovery(pending.spec.task_id) is None
        assert core._accepted_task_count == 0
    finally:
        ref.close()
        _stop(core)


@pytest.mark.unit
def test_reconstruction_terminal_error_converges_all_three_authorities() -> None:
    core, pending, ref = _lost_core()
    try:
        core._start_or_join_reconstruction(
            pending.object_id, core._objects[pending.object_id]
        )
        retried = _next_pending(core)
        attempt = retried.spec.attempt_id
        assert core._recovery.active_recovery(pending.spec.task_id) == attempt
        assert pending.spec.task_id in core._reconstruction._sessions

        error = SystemTaskError("terminal scheduling failure")
        assert core._publish_error(pending.object_id, attempt, error)

        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.ERROR
        record = core._recovery.task_record(pending.spec.task_id)
        assert record.current_attempt == attempt
        assert record.state is TaskState.SYSTEM_FAILED
        assert core._recovery.active_recovery(pending.spec.task_id) is None
        assert pending.spec.task_id not in core._reconstruction._sessions
    finally:
        ref.close()
        _stop(core)


@pytest.mark.unit
def test_recursive_chain_requeues_dependency_first_with_stable_ids() -> None:
    core = make_pure_core()
    leaf, leaf_ref = core._register_submission(
        core.define_remote_function(lambda: 1), (), {}, ResourceVector(),
        max_retries=2,
    )
    middle, middle_ref = core._register_submission(
        core.define_remote_function(lambda value: value + 1),
        (leaf_ref,), {}, ResourceVector(), max_retries=2,
    )
    root, root_ref = core._register_submission(
        core.define_remote_function(lambda value: value + 1),
        (middle_ref,), {}, ResourceVector(), max_retries=2,
    )
    pendings = (leaf, middle, root)
    refs = (leaf_ref, middle_ref, root_ref)
    try:
        for index, pending in enumerate(pendings):
            payload = cloudpickle.dumps(index + 1)
            descriptor = protocol.ResultDescriptor(
                pending.object_id, protocol.ResultStorage.OBJECT_STORE,
                len(payload), core.worker_id, core.node_id,
                hashlib.sha256(payload).hexdigest(),
            )
            assert core.owner_table.publish_stored(
                pending.object_id, pending.spec.attempt_id, core.node_id
            )
            core._stored_descriptors[pending.object_id] = descriptor
            core._recovery.record_task_success(
                pending.spec.task_id, pending.spec.attempt_id
            )
            assert core.owner_table.mark_lost(
                pending.object_id, pending.spec.attempt_id
            )

        core._start_or_join_reconstruction(
            root.object_id, core._objects[root.object_id]
        )
        queued = [core._submissions.get_nowait() for _ in range(3)]
        assert [item.spec.task_id for item in queued] == [
            leaf.spec.task_id, middle.spec.task_id, root.spec.task_id
        ]
        assert [item.object_id for item in queued] == [
            leaf.object_id, middle.object_id, root.object_id
        ]
        assert all(
            item.spec.attempt_id == original.spec.attempt_id.next()
            for item, original in zip(queued, pendings)
        )
        assert queued[0].protected_dependencies == ()
        assert queued[1].protected_dependencies == (leaf.object_id,)
        assert queued[2].protected_dependencies == (middle.object_id,)
    finally:
        for pending in pendings:
            snapshot = core.owner_table.snapshot(pending.object_id)
            if snapshot.state is ObjectState.PENDING:
                core._publish_error(
                    pending.object_id, snapshot.current_attempt,
                    RuntimeError("cleanup"),
                )
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_nested_local_reconstruction_installs_fresh_lifetime_hold() -> None:
    core = make_pure_core()
    nested, nested_ref = core._register_submission(
        core.define_remote_function(lambda: 1), (), {}, ResourceVector(),
    )
    consumer, consumer_ref = core._register_submission(
        core.define_remote_function(lambda value: value),
        ({"ref": nested_ref},), {}, ResourceVector(), max_retries=2,
    )
    try:
        original_hold = consumer.dependency_hold
        assert original_hold is not None
        original_argument = consumer.spec.args[0]
        assert isinstance(original_argument, protocol.InlineArg)
        assert original_argument.nested_refs[0].hold == original_hold

        payload = cloudpickle.dumps({"original": True})
        descriptor = protocol.ResultDescriptor(
            consumer.object_id, protocol.ResultStorage.OBJECT_STORE,
            len(payload), core.worker_id, core.node_id,
            hashlib.sha256(payload).hexdigest(),
        )
        assert core.owner_table.publish_stored(
            consumer.object_id, consumer.spec.attempt_id, core.node_id
        )
        core._stored_descriptors[consumer.object_id] = descriptor
        core._recovery.record_task_success(
            consumer.spec.task_id, consumer.spec.attempt_id
        )
        assert core._finish_pending_task(consumer)
        assert original_hold not in core.owner_table.snapshot(
            nested.object_id
        ).submitted_tokens
        assert core.owner_table.mark_lost(
            consumer.object_id, consumer.spec.attempt_id
        )

        core._start_or_join_reconstruction(
            consumer.object_id, core._objects[consumer.object_id]
        )
        retried = _next_pending(core)
        fresh_hold = retried.dependency_hold
        assert fresh_hold is not None
        assert fresh_hold != original_hold
        assert fresh_hold.origin_attempt_id == retried.spec.attempt_id
        assert retried.protected_dependencies == ()
        assert retried.nested_local_holds == (nested.object_id,)
        argument = retried.spec.args[0]
        assert isinstance(argument, protocol.InlineArg)
        assert argument.nested_refs[0].hold == fresh_hold
        assert core.owner_table.snapshot(
            nested.object_id
        ).submitted_tokens == frozenset({fresh_hold})

        error_reply = protocol.TaskReply(
            retried.spec.task_id, retried.spec.attempt_id, core.worker_id,
            protocol.TaskReplyStatus.SYSTEM_ERROR,
            error=protocol.RemoteErrorInfo("RuntimeError", "retry"),
        )
        assert not core._retry_explicit_system_failure(retried, error_reply)
        system_retry = _next_pending(core)
        assert system_retry.dependency_hold == fresh_hold
        assert system_retry.nested_local_holds == (nested.object_id,)
        system_argument = system_retry.spec.args[0]
        assert isinstance(system_argument, protocol.InlineArg)
        assert system_argument.nested_refs[0].hold == fresh_hold

        core._publish_error(
            system_retry.object_id, system_retry.spec.attempt_id,
            RuntimeError("done"),
        )
        assert core._finish_pending_task(system_retry)
        assert fresh_hold not in core.owner_table.snapshot(
            nested.object_id
        ).submitted_tokens
    finally:
        for pending in (consumer, nested):
            snapshot = core.owner_table.snapshot(pending.object_id)
            if snapshot.state is ObjectState.PENDING:
                core._publish_error(
                    pending.object_id, snapshot.current_attempt,
                    RuntimeError("cleanup"),
                )
                core._finish_pending_task(pending)
        consumer_ref.close()
        nested_ref.close()
        _stop(core)


@pytest.mark.unit
def test_multi_return_all_lost_requeues_full_manifest_from_nonzero_sibling() -> None:
    core, original, refs, _descriptors = _lost_multi_core()
    runtime = PureReferenceOutputRuntime(core)
    try:
        requested = original.output_ids[2]
        core._start_or_join_reconstruction(
            requested, core._objects[requested]
        )
        retried = _next_pending(core)

        assert retried.task_id == original.task_id
        assert retried.output_ids == original.output_ids
        assert retried.spec.attempt_id == original.spec.attempt_id.next()
        assert core._accepted_task_count == 1
        assert not any(
            output_id in core._stored_descriptors
            for output_id in original.output_ids
        )
        assert all(
            not core._objects[output_id].event.is_set()
            and core.owner_table.snapshot(output_id).state
            is ObjectState.PENDING
            and core.owner_table.snapshot(output_id).current_attempt
            == retried.spec.attempt_id
            for output_id in original.output_ids
        )

        payloads = tuple(cloudpickle.dumps(value) for value in (10, 20, 30))
        results = tuple(
            protocol.ResultDescriptor(
                output_id, protocol.ResultStorage.INLINE, len(payload),
                core.worker_id, core.node_id,
                hashlib.sha256(payload).hexdigest(), payload,
            )
            for output_id, payload in zip(retried.output_ids, payloads)
        )
        reply = runtime.complete(
            protocol.PushTask(LeaseID.random(), core.worker_id, retried.spec),
            (10, 20, 30),
        )
        assert reply.results == results
        assert reply.output_publication.manifest.execution == retried.execution
        assert core._publish_reply(retried, reply)
        assert all(
            core.owner_table.snapshot(output_id).state
            is ObjectState.READY_INLINE
            and core._objects[output_id].event.is_set()
            for output_id in retried.output_ids
        )
        assert core._recovery.active_recovery(retried.task_id) is None
        assert retried.task_id not in core._reconstruction._sessions
        # Execution cleanup is task-scoped, so a reconstructed multi-return
        # attempt releases its logical execution exactly once.
        assert core._finish_pending_task(retried)
        assert core._finish_pending_task(retried)
        assert core._accepted_task_count == 0
    finally:
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_multi_return_partial_loss_opens_then_starts_only_lost_target() -> None:
    core, original, refs, descriptors = _lost_multi_core(partial=True)
    lost = original.output_ids[1]
    before = tuple(
        core.owner_table.snapshot(output_id)
        for output_id in original.output_ids
    )
    before_descriptors = dict(core._stored_descriptors)
    healthy = (original.output_ids[0], original.output_ids[2])
    healthy_before = tuple(
        core.owner_table.snapshot(output_id) for output_id in healthy
    )
    try:
        core._start_or_join_reconstruction(lost, core._objects[lost])
        event = core._submissions.get_nowait()
        assert event.task_id == original.task_id
        # OPEN is merge-only and consumes neither owner epoch nor budget.
        assert tuple(
            core.owner_table.snapshot(output_id)
            for output_id in original.output_ids
        ) == before
        assert core._stored_descriptors == before_descriptors
        assert core._recovery.task_record(original.task_id).retries_started == 0

        core._start_open_targeted_reconstruction(event.task_id)
        retried = _next_pending(core)
        assert retried.output_ids == (lost,)
        assert retried.full_output_ids == original.output_ids
        assert retried.target_execution is not None
        assert retried.target_execution.target_output_ids == (lost,)
        assert retried.task_key == (
            original.task_id, retried.reconstruction_origin_attempt
        )
        assert tuple(
            core.owner_table.snapshot(output_id) for output_id in healthy
        ) == healthy_before
        assert tuple(
            core._stored_descriptors[output_id] for output_id in healthy
        ) == (descriptors[0], descriptors[2])
        assert lost not in core._stored_descriptors
        assert not core._objects[lost].event.is_set()
        assert tuple(
            core._objects[output_id].event.is_set() for output_id in healthy
        ) == (True, True)
        assert core._publish_task_error(
            retried, SystemTaskError("cleanup")
        )
        assert core._finish_pending_task(retried)
    finally:
        for ref in refs:
            ref.close()
        _stop(core)


def _start_partial(core: CoreWorker, original: _PendingTask) -> _PendingTask:
    lost = original.output_ids[1]
    core._start_or_join_reconstruction(lost, core._objects[lost])
    event = core._submissions.get_nowait()
    core._start_open_targeted_reconstruction(event.task_id)
    return _next_pending(core)


@pytest.mark.unit
def test_partial_target_success_publishes_only_target_and_preserves_healthy() -> None:
    core, original, refs, descriptors = _lost_multi_core(partial=True)
    runtime = PureReferenceOutputRuntime(core)
    healthy = (original.output_ids[0], original.output_ids[2])
    healthy_before = tuple(core.owner_table.snapshot(value) for value in healthy)
    try:
        retried = _start_partial(core, original)
        payload = cloudpickle.dumps(200)
        result = protocol.ResultDescriptor(
            retried.object_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
            core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(),
        )
        reply = runtime.complete(
            protocol.PushTask(
                LeaseID.random(), WorkerID.random(), retried.spec,
                target_execution=retried.target_execution,
            ),
            (200,), inline_threshold=0,
        )
        assert reply.results == (result,)
        assert runtime.store.get(retried.object_id) == payload
        assert reply.output_publication.manifest.execution == retried.execution

        assert core._publish_reply(
            retried, reply, expected_node_id=core.node_id
        )
        assert tuple(core.owner_table.snapshot(value) for value in healthy) == (
            healthy_before
        )
        assert tuple(core._stored_descriptors[value] for value in healthy) == (
            descriptors[0], descriptors[2]
        )
        target = core.owner_table.snapshot(retried.object_id)
        assert target.state is ObjectState.READY_STORED
        assert target.current_attempt == retried.spec.attempt_id
        assert core._stored_descriptors[retried.object_id] == result
        assert core._finish_pending_task(retried)
        refs[1].close()
        core._reference_released(retried.object_id)
        assert runtime.store.used_bytes == 0
        assert core.owner_table.collection_state(retried.object_id) is (
            ObjectCollectionState.COLLECTED
        )
        runtime.assert_collected()
    finally:
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_late_loss_starts_second_session_with_distinct_lifecycle_key() -> None:
    core, original, refs, _ = _lost_multi_core(partial=True)
    runtime = PureReferenceOutputRuntime(core)
    try:
        first = _start_partial(core, original)
        late = original.output_ids[2]
        assert core.owner_table.mark_lost(late, original.spec.attempt_id)
        core._start_or_join_reconstruction(late, core._objects[late])
        session = core._targeted_reconstruction.current_session(original.task_id)
        assert session is not None
        assert tuple(
            item.object_id
            for item in core._targeted_reconstruction.queued_losses(
                original.task_id
            )
        ) == (late,)
        payload = cloudpickle.dumps(20)
        result = protocol.ResultDescriptor(
            first.object_id, protocol.ResultStorage.INLINE, len(payload),
            core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(),
            payload,
        )
        reply = runtime.complete(
            protocol.PushTask(
                LeaseID.random(), WorkerID.random(), first.spec,
                target_execution=first.target_execution,
            ),
            (20,),
        )
        assert reply.results == (result,)
        assert reply.output_publication.manifest.execution == first.execution
        assert core._publish_reply(first, reply, expected_node_id=core.node_id)
        assert core._finish_pending_task(first)
        event = core._submissions.get_nowait()
        for _ in range(32):
            if isinstance(event, _StartTargetedReconstruction):
                break
            assert event is _WAKE_COORDINATOR
            event = core._submissions.get_nowait()
        else:
            pytest.fail("no targeted start within 32 FIFO records")
        core._start_open_targeted_reconstruction(event.task_id)
        second = _next_pending(core)

        assert first.task_key != second.task_key
        assert second.output_ids == (late,)
        assert first.task_key in core._finished_tasks
        assert second.task_key not in core._finished_tasks
        assert core._publish_task_error(second, SystemTaskError("cleanup"))
        assert core._finish_pending_task(second)
    finally:
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_target_start_renews_foreign_lineage_before_owner_attempt_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, original, refs, _ = _lost_multi_core(partial=True)
    lost = original.output_ids[1]
    calls: list[tuple[str, AttemptID]] = []
    observations = []
    healthy = (original.output_ids[0], original.output_ids[2])
    healthy_before = tuple(core.owner_table.snapshot(value) for value in healthy)

    def observe(stage):
        owner = core.owner_table.snapshot(lost)
        recovery = core._recovery.task_record(original.task_id)
        observations.append((stage, (
            owner.current_attempt, owner.state,
            recovery.current_attempt, recovery.retries_started,
        )))
        assert tuple(core.owner_table.snapshot(value) for value in healthy) == (
            healthy_before
        )

    def renewal_callback(stage, task_id, attempt):
        assert task_id == original.task_id
        calls.append((stage, attempt))
        observe(stage)

    retire = core._retire_lost_output_memberships

    def retire_observed(object_id):
        assert object_id == lost
        observe("retire_before")
        result = retire(object_id)
        observe("retire_after")
        return result

    commit_owner = core.owner_table.commit_advance_target_outputs

    def commit_owner_observed(plan):
        observe("owner_commit_before")
        result = commit_owner(plan)
        observe("owner_commit_after")
        return result

    monkeypatch.setattr(core, "_retire_lost_output_memberships", retire_observed)
    monkeypatch.setattr(
        core.owner_table, "commit_advance_target_outputs", commit_owner_observed
    )
    runtime = type(
        "Renewal", (), {
            "drive_renewal": lambda _self, task_id, attempt: (
                renewal_callback("drive", task_id, attempt)
                or type("Result", (), {
                    "disposition": ForeignLineageRenewalDisposition.READY,
                    "failure": None,
                })()
            ),
            "validate_renewal_ready": lambda _self, task_id, attempt: (
                renewal_callback("validate", task_id, attempt) or None
            ),
            "complete_renewal": lambda _self, task_id, attempt: (
                renewal_callback("complete", task_id, attempt) or True
            ),
        }
    )()
    core._foreign_lineage_runtime = runtime
    monkeypatch.setattr(
        core._foreign_lineage_registry, "snapshot",
        lambda task_id: type("Record", (), {"edges": ()})()
        if task_id == original.task_id else None,
    )
    try:
        core._start_or_join_reconstruction(lost, core._objects[lost])
        event = core._submissions.get_nowait()
        assert core.owner_table.snapshot(lost).current_attempt == (
            original.spec.attempt_id
        )

        core._start_open_targeted_reconstruction(event.task_id)
        retried = _next_pending(core)

        assert [name for name, _ in calls] == [
            "drive", "validate", "validate", "complete"
        ]
        assert all(attempt == retried.spec.attempt_id for _, attempt in calls)
        assert [stage for stage, _state in observations] == [
            "drive", "validate", "retire_before", "retire_after",
            "validate", "owner_commit_before", "owner_commit_after",
            "complete",
        ]
        old_state = (
            original.spec.attempt_id, ObjectState.LOST,
            original.spec.attempt_id, 0,
        )
        # Both validations bracket retirement without advancing owner state or
        # spending retry budget. Their real owner CAS occurs only afterwards.
        assert all(state == old_state for _stage, state in observations[:6])
        assert observations[6][1] == (
            retried.spec.attempt_id, ObjectState.PENDING,
            original.spec.attempt_id, 0,
        )
        # complete_renewal acknowledges the completed owner + recovery commit,
        # not just a READY proposal or the first half of the composition.
        assert observations[7][1] == (
            retried.spec.attempt_id, ObjectState.PENDING,
            retried.spec.attempt_id, 1,
        )
        assert core._publish_task_error(retried, SystemTaskError("cleanup"))
        assert core._finish_pending_task(retried)
    finally:
        for ref in refs:
            ref.close()
        # Restore the injected registry view; no distributed shutdown runs.
        monkeypatch.setattr(core._foreign_lineage_registry, "snapshot", lambda _: None)
        core._foreign_lineage_runtime = None
        _stop(core)


@pytest.mark.unit
def test_target_start_installs_renewed_foreign_guards_and_nested_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, original, refs, _ = _lost_multi_core(partial=True)
    foreign_owner = WorkerID.random()
    dependency = ObjectID.for_task(TaskID.random(), 0)
    old_hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
        original.task_id, original.spec.attempt_id,
    )
    transfer = protocol.NestedReferenceTransfer(
        dependency, foreign_owner, ("foreign-owner.invalid", 1), old_hold
    )
    spec = replace(
        original.spec,
        args=(protocol.RefArg(dependency, foreign_owner),
              protocol.InlineArg(b"nested", nested_refs=(transfer,))),
    )
    for output_id in original.output_ids:
        core.owner_table._entries[output_id].producer_task_spec = spec
    core._recovery._lineages[original.task_id] = replace(
        core._recovery._lineages[original.task_id], task_spec=spec
    )
    edge = ForeignLineageEdge(
        original.task_id, dependency, foreign_owner, ("foreign-owner.invalid", 1),
        core.worker_id, old_hold,
        ForeignLineageRole.TOP_LEVEL | ForeignLineageRole.NESTED,
    )
    core._foreign_lineage_registry.register(
        original.task_id, original.output_ids, (edge,)
    )

    def drive(task_id, attempt):
        renewed = replace(edge, hold=replace(old_hold, origin_attempt_id=attempt))
        core._foreign_lineage_registry.commit_edge_replacement(edge, renewed)
        return type("Result", (), {
            "disposition": ForeignLineageRenewalDisposition.READY,
            "failure": None,
        })()

    monkeypatch.setattr(core._foreign_lineage_runtime, "drive_renewal", drive)
    monkeypatch.setattr(
        core._foreign_lineage_runtime, "validate_renewal_ready", lambda *_: True
    )
    monkeypatch.setattr(
        core._foreign_lineage_runtime, "complete_renewal", lambda *_: True
    )
    try:
        retried = _start_partial(core, original)
        renewed = core._foreign_lineage_registry.snapshot(original.task_id).edges[0]
        assert retried.foreign_dependency_guards[0].hold == renewed.hold
        assert retried.nested_foreign_guards == ()
        assert retried.spec.args[1].nested_refs[0].hold == renewed.hold
        assert core._publish_task_error(retried, SystemTaskError("cleanup"))
        assert core._finish_pending_task(retried)
    finally:
        core._foreign_lineage_registry._tasks.clear()
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_target_open_waiting_backs_off_and_definitive_failure_wakes_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, original, refs, _ = _lost_multi_core(partial=True)
    lost = original.output_ids[1]
    monkeypatch.setattr(
        core._foreign_lineage_registry, "snapshot",
        lambda task_id: type("Record", (), {"edges": ()})()
        if task_id == original.task_id else None,
    )
    waiting = type("Result", (), {
        "disposition": ForeignLineageRenewalDisposition.WAITING,
        "failure": None,
    })()
    monkeypatch.setattr(
        core._foreign_lineage_runtime, "drive_renewal", lambda *_: waiting
    )
    try:
        core._start_or_join_reconstruction(lost, core._objects[lost])
        event = core._submissions.get_nowait()
        core._start_open_targeted_reconstruction(event.task_id)
        delayed = core._submissions.get_nowait()
        assert isinstance(delayed, _DelayedTargetedReconstruction)
        assert delayed.event == _StartTargetedReconstruction(original.task_id, 1)

        error = SystemTaskError("definitive renewal failure")
        core._fail_open_targeted_reconstruction(original.task_id, error)
        assert core._targeted_reconstruction.current_session(original.task_id) is None
        snapshot = core.owner_table.snapshot(lost)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is error
        assert core._objects[lost].event.is_set()
        # L0 proves the reducer leaves admission usable by actually starting
        # another reconstruction.  It must not fake a coordinator's liveness.
        assert core._accepting
        another = original.output_ids[2]
        assert core.owner_table.mark_lost(another, original.spec.attempt_id)
        core._start_or_join_reconstruction(another, core._objects[another])
        next_session = core._targeted_reconstruction.current_session(original.task_id)
        assert next_session is not None
        assert next_session.target_output_ids == (another,)
    finally:
        monkeypatch.setattr(core._foreign_lineage_registry, "snapshot", lambda _: None)
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_targeted_explicit_system_error_queries_node_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, original, refs, _ = _lost_multi_core(partial=True)
    try:
        pending = _start_partial(core, original)
        worker = WorkerID.random()
        grant = protocol.GrantWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id,
            core.node_id, worker, ("worker.invalid", 1),
            AllocationToken("target-system"),
            target_execution=pending.target_execution,
        )
        request = protocol.RequestWorkerLease(
            grant.lease_id, pending.task_id, pending.spec.attempt_id,
            pending.spec.resources, core.node_id, core.worker_id,
            target_node_id=core.node_id, return_ids=pending.output_ids,
            target_execution=pending.target_execution,
        )
        push = protocol.PushTask(
            grant.lease_id, worker, pending.spec,
            target_execution=pending.target_execution,
        )
        state = _PushRequestState(
            push, grant, core.node_address, grant.worker_address,
            lease_request=request,
        )
        reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, worker,
            protocol.TaskReplyStatus.SYSTEM_ERROR,
            error=protocol.RemoteErrorInfo("RuntimeError", "failed"),
            target_execution=pending.target_execution,
        )
        seen: list[protocol.GetWorkerLeaseOutcome] = []

        def rpc(_address, handler, message):
            assert _address == core.node_address
            assert handler == "get_worker_lease_outcome"
            assert isinstance(message, protocol.GetWorkerLeaseOutcome)
            seen.append(message)
            return protocol.GetWorkerLeaseOutcomeReply(
                grant.lease_id, pending.task_id, pending.spec.attempt_id,
                worker, core.worker_id, pending.output_ids, core.node_id,
                True, False,
                state=protocol.LeaseExecutionState.COMPLETED,
                completion_status=protocol.TaskReplyStatus.SYSTEM_ERROR,
                target_execution=pending.target_execution,
            )

        monkeypatch.setattr(core, "_rpc", rpc)
        core._mark_protocol_unresolved(
            pending, "push_replay_wait", target_node_id=core.node_id
        )

        def push_rpc(address, handler, message):
            assert address == grant.worker_address
            assert handler == "push_task"
            assert message is push
            return reply

        monkeypatch.setattr(core, "_push_task_rpc", push_rpc)

        assert not core._replay_push(pending, state)
        retried = _next_pending(core)
        assert len(seen) == 1
        assert seen[0].target_execution == pending.target_execution
        assert seen[0].object_ids == pending.output_ids
        assert retried.target_execution is not None
        assert retried.spec.attempt_id == pending.spec.attempt_id.next()
        assert core._publish_task_error(retried, SystemTaskError("cleanup"))
        assert core._finish_pending_task(retried)
    finally:
        for ref in refs:
            ref.close()
        _stop(core)


@pytest.mark.unit
def test_multi_return_reconstruction_error_wakes_every_sibling() -> None:
    core, original, refs, _ = _lost_multi_core()
    try:
        core._start_or_join_reconstruction(
            original.output_ids[1], core._objects[original.output_ids[1]]
        )
        retried = _next_pending(core)
        error = SystemTaskError("reconstruction failed")
        assert core._publish_task_error(retried, error)
        snapshots = tuple(
            core.owner_table.snapshot(output_id)
            for output_id in retried.output_ids
        )
        assert all(
            snapshot.state is ObjectState.ERROR and snapshot.error is error
            for snapshot in snapshots
        )
        assert all(
            core._objects[output_id].event.is_set()
            for output_id in retried.output_ids
        )
        assert core._recovery.active_recovery(retried.task_id) is None
        assert retried.task_id not in core._reconstruction._sessions
    finally:
        for ref in refs:
            ref.close()
        _stop(core)
