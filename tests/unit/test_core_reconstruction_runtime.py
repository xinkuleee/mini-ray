"""Pure Core/owner/recovery composition; all FIFO progress is explicit.

The real concurrent-request contract lives in
tests/integration/test_core_reconstruction_concurrency.py and is opt-in L1.
Here even an accidental Core constructor, socket, thread, timer, or blocking
wait is rejected before it can run.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker, _PendingTask,
    _WAKE_COORDINATOR,
)
from miniray.ids import LeaseID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.errors import RuntimeShuttingDownError, SystemTaskError
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import (
    SynchronousReferenceMailbox, close_pure_core,
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
        core._submissions.task_done()
        if isinstance(item, _PendingTask):
            return item
        assert item is _WAKE_COORDINATOR
    pytest.fail("no pending reconstruction within 32 FIFO records")


def _stop(core: CoreWorker) -> None:
    close_pure_core(core)


def _lost_core():
    core = make_pure_core()
    runtime = PureReferenceOutputRuntime(core)
    pending, ref = _accepted(core,lambda:{'original':True},max_retries=1)
    _publish_small_stored(runtime,pending,{'original':True})
    assert core.drop_object(ref)
    return core, pending, ref


def _accepted(core, function, args=(), *, max_retries=0):
    pending, ref = core._register_submission(core.define_remote_function(function), args, {},
        ResourceVector(), max_retries=max_retries, _enqueue=True)
    assert _next_pending(core) is pending
    return pending, ref


def _publish_small_stored(runtime, pending, value):
    prepared, dependencies, _ = runtime.core._prepare_task_dependencies(pending.spec)
    reply=runtime.complete(protocol.PushTask(LeaseID.random(),WorkerID.random(),prepared,dependencies),
        (value,),inline_threshold=0)
    assert runtime.core._publish_reply(pending,reply)
    assert runtime.core._finish_pending_task(pending)
    while not runtime.core._submissions.empty():
        assert runtime.core._submissions.get_nowait() is _WAKE_COORDINATOR
        runtime.core._submissions.task_done()
    return reply


@pytest.mark.unit
def test_pure_mailbox_preserves_real_close_and_explicit_collection() -> None:
    core = make_pure_core()
    pending, ref = _accepted(core,lambda:1)
    mailbox = core._reference_mailbox
    assert isinstance(mailbox, SynchronousReferenceMailbox)
    token = ref._local_token
    assert core.owner_table.snapshot(pending.object_id).local_tokens == frozenset({token})
    try:
        assert core._publish_task_error(pending, SystemTaskError("terminal"))
        assert core._finish_pending_task(pending)
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
        queued = _next_pending(core)
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
    runtime = PureReferenceOutputRuntime(core)
    leaf, leaf_ref = _accepted(core,lambda:1,max_retries=2)
    middle, middle_ref = _accepted(core,lambda value:value+1,(leaf_ref,),max_retries=2)
    root, root_ref = _accepted(core,lambda value:value+1,(middle_ref,),max_retries=2)
    pendings = (leaf, middle, root)
    refs = (leaf_ref, middle_ref, root_ref)
    try:
        for index, pending in enumerate(pendings):
            _publish_small_stored(runtime,pending,index+1)
        for ref in refs:
            assert core.drop_object(ref)

        core._start_or_join_reconstruction(
            root.object_id, core._objects[root.object_id]
        )
        queued = [_next_pending(core) for _ in range(3)]
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
    runtime = PureReferenceOutputRuntime(core)
    nested, nested_ref = _accepted(core,lambda:1)
    consumer, consumer_ref = _accepted(core,lambda value:value,({'ref':nested_ref},),max_retries=2)
    try:
        original_hold = consumer.dependency_hold
        assert original_hold is not None
        original_argument = consumer.spec.args[0]
        assert isinstance(original_argument, protocol.InlineArg)
        assert original_argument.nested_refs[0].hold == original_hold

        _publish_small_stored(runtime,consumer,{'original':True})
        assert original_hold not in core.owner_table.snapshot(nested.object_id).submitted_tokens
        assert core.drop_object(consumer_ref)

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
