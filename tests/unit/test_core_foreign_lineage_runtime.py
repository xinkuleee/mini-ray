"""Pure Core composition contracts for TaskID-scoped foreign lineage.

Real owner RPC handlers, renewal/recovery reducers, GC, and Core.shutdown run
synchronously.  No runtime constructor, worker, thread, socket, sleep, or live
queue consumer is needed; reference GC advances only through a manual mailbox.
"""

from __future__ import annotations

import socket
import threading
import time

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import CoreWorker, ObjectRef, _PendingTask, _WAKE_COORDINATOR
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, ObjectID, TaskID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core




@pytest.fixture(autouse=True)
def _forbid_runtime_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure foreign-lineage contract attempted runtime work")

    def immediate_event_wait(event, timeout=None):
        assert event.is_set(), "pure contract attempted to block on an Event"
        return True

    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Thread, "join", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", immediate_event_wait)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _pair(monkeypatch: pytest.MonkeyPatch):
    owner = make_pure_core()
    borrower = make_pure_core()
    dependency_task = TaskID.random()
    dependency_id = ObjectID.for_task(dependency_task)
    dependency_attempt = AttemptID(dependency_task, 0)
    owner.owner_table.register(
        dependency_id, current_attempt=dependency_attempt
    )
    assert owner.owner_table.publish_inline(
        dependency_id, dependency_attempt, cloudpickle.dumps(7)
    )
    owner.owner_table.add_borrowed_reference(
        dependency_id, (borrower.worker_id, "borrow")
    )
    ref = ObjectRef(dependency_id, owner.worker_id, owner.owner_address)
    ref._borrower_token = "borrow"
    calls: list[tuple[str, object]] = []

    def rpc(address, handler, request):
        operations = {
            "retain_owned_object_for_task": (
                protocol.RetainOwnedObjectForTask, owner.retain_owned_object_for_task
            ),
            "get_retained_owned_object": (
                protocol.GetRetainedOwnedObject, owner.get_retained_owned_object
            ),
            "release_owned_object_for_task": (
                protocol.ReleaseOwnedObjectForTask, owner.release_owned_object_for_task
            ),
            "replace_retained_object_for_task": (
                protocol.ReplaceRetainedObjectForTask, owner.replace_retained_object_for_task
            ),
            "request_owned_object_reconstruction": (
                protocol.RequestOwnedObjectReconstruction,
                owner.request_owned_object_reconstruction,
            ),
        }
        if address != owner.owner_address or handler not in operations:
            pytest.fail("unmodelled foreign-owner RPC route: {!r}".format((address, handler)))
        request_type, operation = operations[handler]
        if not isinstance(request, request_type):
            pytest.fail("foreign-owner RPC carried the wrong request type")
        if (
            request.owner_worker_id != owner.worker_id
            or request.object_id != dependency_id
            or getattr(request, "borrower_worker_id", borrower.worker_id)
            != borrower.worker_id
        ):
            pytest.fail("foreign-owner RPC changed its owner/object identity")
        calls.append((handler, request))
        return operation(request)

    monkeypatch.setattr(borrower, "_borrow_rpc", rpc)
    return owner, borrower, ref, dependency_id, calls


def _stop(core: CoreWorker) -> None:
    close_pure_core(core)


def _next_pending(core: CoreWorker) -> _PendingTask:
    for _ in range(32):
        item = core._submissions.get_nowait()
        if isinstance(item, _PendingTask):
            return item
        assert item is _WAKE_COORDINATOR
    pytest.fail("no reconstruction within 32 in-memory FIFO records")


def _mark_output_lost(core: CoreWorker, pending: _PendingTask) -> None:
    assert core.owner_table.publish_stored(
        pending.object_id, pending.spec.attempt_id, core.node_id
    )
    core._recovery.record_task_success(pending.task_id, pending.spec.attempt_id)
    assert core._finish_pending_task(pending)
    assert core.owner_table.mark_lost(pending.object_id, pending.spec.attempt_id)
    # The test owns this FIFO; the original finish has exactly one wake, not
    # a racing coordinator that might consume the reconstruction to come.
    assert core._submissions.get_nowait() is _WAKE_COORDINATOR
    assert core._submissions.empty()


@pytest.mark.unit
def test_submission_registers_lineage_and_terminal_finish_keeps_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, borrower, ref, dependency_id, calls = _pair(monkeypatch)
    pending, output = borrower._register_submission(
        borrower.define_remote_function(lambda value: value),
        (ref,), {}, ResourceVector(), max_retries=1,
    )
    try:
        record = borrower._foreign_lineage_registry.snapshot(pending.task_id)
        assert record is not None and record.output_ids == pending.output_ids
        assert len(record.edges) == 1
        edge = record.edges[0]
        assert edge.dependency_object_id == dependency_id
        assert owner.owner_table.has_retained_reference_for_task(
            dependency_id, edge.hold
        )

        assert borrower._publish_error(
            pending.object_id, pending.spec.attempt_id, RuntimeError("done")
        )
        assert borrower._finish_pending_task(pending)
        assert owner.owner_table.has_retained_reference_for_task(
            dependency_id, edge.hold
        )
        assert [handler for handler, _request in calls] == [
            "retain_owned_object_for_task"
        ]
    finally:
        output.close()
        _stop(borrower)
        _stop(owner)


@pytest.mark.unit
def test_reconstruction_replaces_hold_before_local_owner_attempt_advances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, borrower, ref, dependency_id, calls = _pair(monkeypatch)
    pending, output = borrower._register_submission(
        borrower.define_remote_function(lambda value: value),
        (ref,), {}, ResourceVector(), max_retries=1,
    )
    old_hold = borrower._foreign_lineage_registry.snapshot(
        pending.task_id
    ).edges[0].hold
    _mark_output_lost(borrower, pending)
    replace_at_owner = owner.replace_retained_object_for_task
    observed: list[protocol.ReplaceRetainedObjectForTask] = []

    def replace_before_local_commit(request):
        result = replace_at_owner(request)
        # This is the real owner ACK boundary, before the borrower receives it.
        assert borrower.owner_table.snapshot(pending.object_id).current_attempt == (
            pending.spec.attempt_id
        )
        assert borrower._recovery.task_record(pending.task_id).retries_started == 0
        assert owner.owner_table.has_retained_reference_for_task(
            dependency_id, request.replacement_hold
        )
        observed.append(request)
        return result

    monkeypatch.setattr(owner, "replace_retained_object_for_task", replace_before_local_commit)
    retried = None
    try:
        borrower._start_or_join_reconstruction(
            pending.object_id, borrower._object_waiter(pending.object_id)
        )
        retried = _next_pending(borrower)
        new_hold = retried.foreign_dependency_guards[0].hold
        assert new_hold.origin_attempt_id == retried.spec.attempt_id
        assert new_hold != old_hold
        assert not owner.owner_table.has_retained_reference_for_task(
            dependency_id, old_hold
        )
        assert owner.owner_table.has_retained_reference_for_task(
            dependency_id, new_hold
        )
        assert borrower.owner_table.snapshot(
            pending.object_id
        ).current_attempt == retried.spec.attempt_id
        assert len(observed) == 1 and observed[0].replacement_hold == new_hold
        assert [handler for handler, _request in calls] == [
            "retain_owned_object_for_task",
            "replace_retained_object_for_task",
            "get_retained_owned_object",
        ]
        assert borrower._submissions.empty()
    finally:
        snapshot = borrower.owner_table.snapshot(pending.object_id)
        if snapshot.state is ObjectState.PENDING:
            assert borrower._publish_error(
                pending.object_id, snapshot.current_attempt, RuntimeError("cleanup")
            )
        if retried is not None:
            assert borrower._finish_pending_task(retried)
        output.close()
        _stop(borrower)
        _stop(owner)


@pytest.mark.unit
def test_ambiguous_replacement_never_advances_local_authorities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, borrower, ref, dependency_id, _calls = _pair(monkeypatch)
    pending, output = borrower._register_submission(
        borrower.define_remote_function(lambda value: value),
        (ref,), {}, ResourceVector(), max_retries=1,
    )
    _mark_output_lost(borrower, pending)
    runtime = borrower._foreign_lineage_runtime
    old_hold = borrower._foreign_lineage_registry.snapshot(pending.task_id).edges[0].hold
    original_replace = runtime._replace
    attempts: list[protocol.ReplaceRetainedObjectForTask] = []

    def lose_first_ack(address, request):
        result = original_replace(address, request)
        attempts.append(request)
        if len(attempts) == 1:
            assert result.disposition is protocol.ReplaceRetainedObjectDisposition.REPLACED
            raise TimeoutError("lost ACK after owner commit")
        assert result.disposition is protocol.ReplaceRetainedObjectDisposition.ALREADY_REPLACED
        return result

    monkeypatch.setattr(
        runtime, "_replace", lose_first_ack,
    )
    try:
        assert borrower._start_or_join_reconstruction(
            pending.object_id, borrower._object_waiter(pending.object_id)
        ) is None
        snapshot = borrower.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.LOST
        assert snapshot.current_attempt == pending.spec.attempt_id
        record = borrower._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id
        assert record.retries_started == 0
        assert borrower._foreign_lineage_runtime.has_pending_obligations()
        assert borrower._submissions.empty()
        assert borrower._foreign_lineage_registry.snapshot(pending.task_id).edges[0].hold == old_hold
        assert not owner.owner_table.has_retained_reference_for_task(dependency_id, old_hold)
        assert owner.owner_table.has_retained_reference_for_task(
            dependency_id, attempts[0].replacement_hold
        )

        # Exact replay reconciles an already-committed owner transition, then
        # and only then spends one local retry and enqueues one reconstruction.
        borrower._start_or_join_reconstruction(
            pending.object_id, borrower._object_waiter(pending.object_id)
        )
        retried = _next_pending(borrower)
        assert len(attempts) == 2 and attempts[1] is attempts[0]
        assert retried.spec.attempt_id == pending.spec.attempt_id.next()
        assert borrower._recovery.task_record(pending.task_id).retries_started == 1
        assert borrower._submissions.empty()
        assert retried.foreign_dependency_guards[0].hold == attempts[0].replacement_hold
        assert borrower._publish_task_error(retried, SystemTaskError("done"))
        assert borrower._finish_pending_task(retried)
    finally:
        output.close()
        _stop(borrower)
        _stop(owner)


@pytest.mark.unit
def test_final_output_collection_releases_foreign_lineage_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, borrower, ref, dependency_id, calls = _pair(monkeypatch)
    pending, output = borrower._register_submission(
        borrower.define_remote_function(lambda value: value),
        (ref,), {}, ResourceVector(), max_retries=1,
    )
    edge = borrower._foreign_lineage_registry.snapshot(
        pending.task_id
    ).edges[0]
    assert borrower.owner_table.publish_inline(
        pending.object_id, pending.spec.attempt_id, cloudpickle.dumps(7)
    )
    borrower._recovery.record_task_success(
        pending.task_id, pending.spec.attempt_id
    )
    assert borrower._finish_pending_task(pending)
    try:
        output.close()
        assert borrower._foreign_lineage_registry.snapshot(pending.task_id) is not None
        assert owner.owner_table.has_retained_reference_for_task(dependency_id, edge.hold)
        borrower._reference_mailbox.drain()
        assert borrower._foreign_lineage_registry.snapshot(pending.task_id) is None
        assert not owner.owner_table.has_retained_reference_for_task(
            dependency_id, edge.hold
        )
        assert not borrower._foreign_lineage_runtime.has_pending_obligations()
        assert borrower.owner_table.collection_state(pending.object_id) is (
            ObjectCollectionState.COLLECTED
        )
        assert borrower._recovery.lineage_for_object(pending.object_id) is None
        assert [handler for handler, _request in calls] == [
            "retain_owned_object_for_task", "release_owned_object_for_task",
        ]
    finally:
        output.close()
        _stop(borrower)
        _stop(owner)


@pytest.mark.unit
def test_registry_only_lineage_is_a_shutdown_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, borrower, ref, dependency_id, _calls = _pair(monkeypatch)
    pending, output = borrower._register_submission(
        borrower.define_remote_function(lambda value: value),
        (ref,), {}, ResourceVector(), max_retries=1,
    )
    edge = borrower._foreign_lineage_registry.snapshot(
        pending.task_id
    ).edges[0]
    assert borrower._publish_error(
        pending.object_id, pending.spec.attempt_id, RuntimeError("done")
    )
    assert borrower._finish_pending_task(pending)
    try:
        # Drain notification-only work while the output handle is still live.
        # No worker/thread or pending GC callback can explain the False result:
        # the registry itself must keep the actual Core.shutdown barrier open.
        borrower._reference_mailbox.drain()
        runtime = borrower._foreign_lineage_runtime
        assert not runtime._renewals and not runtime._collections
        assert not runtime._collection_receipts
        assert borrower._reference_mailbox.events.unfinished_tasks == 0
        assert not borrower.shutdown(0.05)
        assert borrower._foreign_lineage_registry.snapshot(pending.task_id) is not None
        assert borrower._reference_mailbox.accepting
        assert borrower._owner_protocol_open
        assert owner.owner_table.has_retained_reference_for_task(dependency_id, edge.hold)
        output.close()
        borrower._reference_mailbox.drain()
        assert borrower.shutdown(1.0)
        assert not borrower._reference_mailbox.accepting
        assert not borrower._owner_protocol_open
        assert borrower._foreign_lineage_registry.snapshot(pending.task_id) is None
        assert not owner.owner_table.has_retained_reference_for_task(dependency_id, edge.hold)
        assert not runtime.has_pending_obligations()
    finally:
        output.close()
        _stop(borrower)
        _stop(owner)
