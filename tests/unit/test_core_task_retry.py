"""Pure retry contracts with explicit submission and reference progress.

The first three cases register at most two Tasks on a threadless Core, consume
their actual FIFO records and drive terminal finish, local close and owner GC.
The final three retain their direct LOST owner/recovery fixtures without live
handles or consumers. Successful stored replies use the real unified
publication path with a 1-KiB store; unit classification is independent of passing.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import output_protocol as output_wire, protocol
from miniray.core import (
    CoreWorker, RemoteFunctionDefinition, _ObjectWaiter, _PendingTask,
    _WAKE_COORDINATOR, _lineage_hold_token,
)
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState
from miniray.reconstruction_runtime import (
    ReconstructionCoordinator, ReconstructionDisposition,
)
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import ResourceVector
from miniray.trace import EventSink
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_reference_output_runtime import PureReferenceOutputRuntime


@pytest.fixture
def _no_submission_runtime(monkeypatch):
    """Guard only the three manually driven submission cases."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure retry submission attempted runtime infrastructure")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure close attempted a blocking receipt wait"
        return True

    for kind, name in ((CoreWorker, "__init__"), (threading.Thread, "start"),
                       (threading.Thread, "join"), (threading.Timer, "start"),
                       (threading.Condition, "wait"), (threading.Barrier, "wait"),
                       (multiprocessing.process.BaseProcess, "start"),
                       (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _take_submissions(core: CoreWorker) -> tuple[_PendingTask, ...]:
    """Consume real finite FIFO work, distinguishing wakes from attempts."""
    size = core._submissions.qsize()
    assert size <= 16
    attempts = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        try:
            if item is not _WAKE_COORDINATOR:
                assert isinstance(item, _PendingTask), "unexpected submission work"
                attempts.append(item)
        finally:
            core._submissions.task_done()
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    return tuple(attempts)


def _close_submissions(core: CoreWorker, *refs) -> None:
    """Terminate admitted fixtures through Core, then collect real metadata."""
    assert 1 <= len(refs) <= 2
    try:
        assert not core._protocol_unresolved
        for ref in refs:
            pending = core._task_finish_barriers.get(ref.object_id)
            if pending is not None:
                snapshot = core.owner_table.snapshot(ref.object_id)
                if snapshot.state is ObjectState.PENDING:
                    assert core._publish_error(
                        ref.object_id, pending.spec.attempt_id,
                        SystemTaskError("pure retry fixture cleanup"),
                    )
                assert core._recovery.task_record(pending.task_id).state is (
                    TaskState.SYSTEM_FAILED
                )
                assert core._finish_pending_task(pending)
        for ref in refs:
            ref.close(timeout=0)
        assert core._reference_mailbox.pending.qsize() <= 16
        core._reference_mailbox.drain()
        # Completion and GC may wake the absent coordinator, but neither may
        # create an unrequested physical retry or leave unconsumed work.
        assert _take_submissions(core) == ()
        assert core._reference_mailbox.pending.empty()
        assert core._reference_mailbox.pending.unfinished_tasks == 0
        for ref in refs:
            assert core.owner_table.collection_state(ref.object_id) is (
                ObjectCollectionState.COLLECTED
            )
            assert core._recovery.lineage_for_object(ref.object_id) is None
        assert core._accepted_task_count == 0
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert not core._active_task_finishes and not core._finishing_tasks
        assert not core._object_gc_obligations
        assert not core._objects and not core._stored_descriptors
    finally:
        for ref in refs:
            ref.close(timeout=0)
        close_pure_core(core)


def _core() -> CoreWorker:
    return make_pure_core()


def _lost_reconstructible_task(
    *, max_retries: int
) -> tuple[CoreWorker, _PendingTask]:
    """Build a LOST producer without starting runtime threads."""

    core = _core()
    core._accepting = True
    core._accepted_task_count = 0
    core._reconstruction = ReconstructionCoordinator(
        core._recovery, core._owner_table
    )
    task_id = TaskID.derive(core.job_id, core.driver_task_id, 0)
    attempt_id = AttemptID(task_id, 0)
    object_id = ObjectID.for_task(task_id)
    spec = protocol.TaskSpec(
        job_id=core.job_id,
        task_id=task_id,
        attempt_id=attempt_id,
        function=protocol.FunctionKey(core.job_id, "tests", "producer", "v1"),
        args=(protocol.InlineArg(b"argument"),),
        num_returns=1,
        resources=ResourceVector({"CPU": 1}),
        max_retries=max_retries,
        owner_worker_id=core.worker_id,
    )
    pending = _PendingTask(object_id, spec)
    core._owner_table.register(
        object_id, current_attempt=attempt_id, producer_task_spec=spec
    )
    core._recovery.register_task(spec, max_retries=max_retries)
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    core._owner_table.publish_stored(object_id, attempt_id, core.node_id)
    core._recovery.record_task_success(task_id, attempt_id)
    core._owner_table.mark_lost(object_id, attempt_id)
    return core, pending


def _next_pending(core: CoreWorker) -> _PendingTask:
    while True:
        item = core._submissions.get_nowait()
        if isinstance(item, _PendingTask):
            return item


def _stored_success_reply(
    runtime: PureReferenceOutputRuntime, pending: _PendingTask
) -> protocol.TaskReply:
    return runtime.complete(
        protocol.PushTask(LeaseID.random(), WorkerID.random(), pending.spec),
        (b"stored-result",), inline_threshold=0,
    )


def _system_reply(pending, worker_id: WorkerID) -> protocol.TaskReply:
    return protocol.TaskReply(
        task_id=pending.spec.task_id,
        attempt_id=pending.spec.attempt_id,
        worker_id=worker_id,
        status=protocol.TaskReplyStatus.SYSTEM_ERROR,
        error=protocol.RemoteErrorInfo("RuntimeError", "injected system failure"),
    )


@pytest.mark.unit
@pytest.mark.usefixtures("_no_submission_runtime")
def test_explicit_system_error_retries_with_stable_logical_ids() -> None:
    core = make_pure_core()
    definition = RemoteFunctionDefinition.from_callable(lambda: 1, core.job_id)
    pending, ref = core._register_submission(
        definition, (), {}, ResourceVector(), max_retries=1, _enqueue=True,
    )
    try:
        assert _take_submissions(core) == (pending,)
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {ref.object_id: pending}
        old_attempt = pending.spec.attempt_id

        assert not core._retry_explicit_system_failure(
            pending, _system_reply(pending, WorkerID.random())
        )
        (retried,) = _take_submissions(core)

        assert retried.spec.task_id == pending.spec.task_id
        assert retried.object_id == pending.object_id == ref.object_id
        assert retried.spec.attempt_id == old_attempt.next()
        assert core.owner_table.snapshot(ref.object_id).current_attempt == old_attempt.next()
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.PENDING
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == retried.spec.attempt_id
        assert record.state is TaskState.RETRY_PENDING
        assert record.retries_started == record.max_retries == 1
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {ref.object_id: retried}
        assert not core._protocol_unresolved

        # A completion from the superseded physical attempt cannot publish the
        # stable logical result or remove the successor's finish barrier.
        assert not core._publish_error(
            ref.object_id, old_attempt, SystemTaskError("late old attempt")
        )
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.PENDING
        assert not core._finish_pending_task(pending)
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {ref.object_id: retried}

        assert core._retry_explicit_system_failure(
            retried, _system_reply(retried, WorkerID.random())
        )
        assert core.owner_table.snapshot(ref.object_id).current_attempt == retried.spec.attempt_id
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.ERROR
        assert record.state is TaskState.SYSTEM_FAILED
        assert record.retries_started == 1
        assert core._finish_pending_task(retried)
        assert core._finish_pending_task(retried)
        assert core._accepted_task_count == 0
        assert _take_submissions(core) == ()
    finally:
        _close_submissions(core, ref)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_submission_runtime")
def test_retry_exhaustion_publishes_current_attempt_error() -> None:
    core = make_pure_core()
    definition = RemoteFunctionDefinition.from_callable(lambda: 1, core.job_id)
    pending, ref = core._register_submission(
        definition, (), {}, ResourceVector(), max_retries=0, _enqueue=True,
    )
    try:
        assert _take_submissions(core) == (pending,)
        assert core._accepted_task_count == 1
        assert core._retry_explicit_system_failure(
            pending, _system_reply(pending, WorkerID.random())
        )
        snapshot = core.owner_table.snapshot(ref.object_id)
        assert snapshot.state is ObjectState.ERROR
        assert isinstance(snapshot.error, SystemTaskError)
        assert snapshot.current_attempt == pending.spec.attempt_id
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id
        assert record.state is TaskState.SYSTEM_FAILED
        assert record.retries_started == record.max_retries == 0
        # Publication wakes dependency readiness. Consume that event and
        # preserve the original claim: exhaustion enqueues no new attempt.
        assert _take_submissions(core) == ()
        assert core._submissions.empty()
        assert not core._protocol_unresolved
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {ref.object_id: pending}
        assert core._finish_pending_task(pending)
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0
    finally:
        _close_submissions(core, ref)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_submission_runtime")
def test_dependency_hold_survives_intermediate_retry() -> None:
    core = make_pure_core()
    definition = RemoteFunctionDefinition.from_callable(lambda value: value, core.job_id)
    producer, dependency = core._register_submission(
        definition, (), {}, ResourceVector(), max_retries=0, _enqueue=True,
    )
    result = None
    try:
        consumer, result = core._register_submission(
            definition, (dependency,), {}, ResourceVector(), max_retries=1,
            _enqueue=True,
        )
        assert _take_submissions(core) == (producer, consumer)
        assert core._accepted_task_count == 2
        hold = consumer.dependency_hold
        assert hold == protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED,
            core.worker_id,
            consumer.spec.task_id,
            consumer.spec.attempt_id,
        )
        assert hold in core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens
        lineage_hold = _lineage_hold_token(consumer.task_id, producer.object_id)
        assert lineage_hold in core.owner_table.snapshot(producer.object_id).lineage_tokens

        assert not core._retry_explicit_system_failure(
            consumer, _system_reply(consumer, WorkerID.random())
        )
        (retried,) = _take_submissions(core)

        assert retried.dependency_hold == hold
        assert retried.dependency_hold.origin_attempt_id == (
            consumer.spec.attempt_id
        )
        assert retried.spec.attempt_id == consumer.spec.attempt_id.next()
        assert retried.protected_dependencies == (producer.object_id,)
        assert hold in core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens
        assert core._accepted_task_count == 2
        assert core._task_finish_barriers[result.object_id] is retried
        assert not core._protocol_unresolved
        record = core._recovery.task_record(consumer.task_id)
        assert record.current_attempt == retried.spec.attempt_id
        assert record.state is TaskState.RETRY_PENDING
        assert record.retries_started == 1
        assert not core._finish_pending_task(consumer)
        assert hold in core.owner_table.snapshot(producer.object_id).submitted_tokens

        assert core._retry_explicit_system_failure(
            retried, _system_reply(retried, WorkerID.random())
        )
        assert core.owner_table.snapshot(result.object_id).state is ObjectState.ERROR
        assert record.state is TaskState.SYSTEM_FAILED
        assert hold in core.owner_table.snapshot(producer.object_id).submitted_tokens
        assert core._finish_pending_task(retried)
        assert core._finish_pending_task(retried)
        assert core._accepted_task_count == 1
        assert hold not in core.owner_table.snapshot(producer.object_id).submitted_tokens
        # Finish releases the execution hold exactly once. Canonical lineage
        # continues retaining the dependency until result metadata is collected.
        assert lineage_hold in core.owner_table.snapshot(producer.object_id).lineage_tokens
        assert _take_submissions(core) == ()
    finally:
        refs = (dependency,) if result is None else (dependency, result)
        _close_submissions(core, *refs)


@pytest.mark.unit
def test_reconstruction_system_retry_success_and_next_loss_start_cleanly() -> None:
    core, original = _lost_reconstructible_task(max_retries=3)
    core.node_address = ("reference-node.invalid", 1)
    runtime = PureReferenceOutputRuntime(core)

    core._start_or_join_reconstruction(
        original.object_id, core._objects[original.object_id]
    )
    attempt_1 = _next_pending(core)
    assert attempt_1.spec.attempt_id == original.spec.attempt_id.next()
    first_reconstruction_hold = attempt_1.dependency_hold
    assert first_reconstruction_hold == protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.SUBMITTED,
        core.worker_id,
        original.spec.task_id,
        attempt_1.spec.attempt_id,
    )

    assert not core._retry_explicit_system_failure(
        attempt_1, _system_reply(attempt_1, WorkerID.random())
    )
    attempt_2 = _next_pending(core)
    assert attempt_2.object_id == original.object_id
    assert attempt_2.spec.task_id == original.spec.task_id
    assert attempt_2.spec.attempt_id == attempt_1.spec.attempt_id.next()
    # A SYSTEM retry belongs to the same admitted reconstruction and therefore
    # preserves its complete hold incarnation instead of minting attempt 2.
    assert attempt_2.dependency_hold == first_reconstruction_hold
    assert attempt_2.dependency_hold.origin_attempt_id == (
        attempt_1.spec.attempt_id
    )
    assert core._recovery.active_recovery(
        original.spec.task_id
    ) == attempt_2.spec.attempt_id

    # Do not invent a stale successful Node publication after owner advance.
    # The current owner must reject old registration before any child/Store
    # effect. Stale terminal state cannot clear the admitted successor either.
    assert not core._reconstruction.complete(original.task_id, attempt_1.spec.attempt_id)
    before_stale = core.owner_table.snapshot(original.object_id)
    old_id = OutputPublicationID(LeaseID.random(), attempt_1.execution)
    stale_discovery = OutputDiscoverySession(OutputPublicationHeader(
        old_id, core.job_id, WorkerID.random(), core.worker_id, runtime.incarnation), inline_threshold=0)
    stale = stale_discovery.discover((b'stored-result',))
    registration = core.register_output_handoff(output_wire.RegisterOutputHandoff(stale.manifest))
    assert not registration.accepted
    assert not runtime.store.used_bytes and not runtime.journal.publication_ids()
    stale_discovery.abort()
    assert not core._publish_error(original.object_id, attempt_1.spec.attempt_id,
                                   SystemTaskError('late old attempt'))
    assert core.owner_table.snapshot(original.object_id) == before_stale
    joined = core._reconstruction.request(original.object_id)
    assert joined.disposition is ReconstructionDisposition.JOIN
    assert joined.decision.attempt_id == attempt_2.spec.attempt_id

    current_reply = _stored_success_reply(runtime, attempt_2)
    assert core._publish_reply(attempt_2, current_reply)
    assert core.owner_table.snapshot(original.object_id).state is ObjectState.READY_STORED
    assert runtime.store.get(original.object_id) == cloudpickle.dumps(b"stored-result")
    assert core._recovery.active_recovery(original.spec.task_id) is None
    assert not core._reconstruction.complete(
        original.spec.task_id, attempt_2.spec.attempt_id
    )
    # The current runtime gates a new reconstruction until this execution's
    # logical finish closes. A successful publication alone is not that gate.
    assert core._finish_pending_task(attempt_2)

    assert core._owner_table.mark_lost(
        original.object_id, attempt_2.spec.attempt_id
    )
    core._start_or_join_reconstruction(
        original.object_id, core._objects[original.object_id]
    )
    attempt_3 = _next_pending(core)
    assert attempt_3.object_id == original.object_id
    assert attempt_3.spec.task_id == original.spec.task_id
    assert attempt_3.spec.attempt_id == attempt_2.spec.attempt_id.next()
    # A later loss starts a new reconstruction transaction, so its origin must
    # be distinct even though TaskID and ObjectID remain stable.
    assert attempt_3.dependency_hold == protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.SUBMITTED,
        core.worker_id,
        original.spec.task_id,
        attempt_3.spec.attempt_id,
    )
    assert attempt_3.dependency_hold != first_reconstruction_hold
    assert core._recovery.task_record(
        original.spec.task_id
    ).retries_started == 3
    assert runtime.store.used_bytes == 0
    assert not runtime.replicas
    old_handoff = runtime.handoff_snapshot(current_reply.output_publication.publication_id)
    assert old_handoff.adoption is not None
    assert core.owner_table.snapshot(original.object_id).output_publication is None
    assert runtime.journal.snapshot(current_reply.output_publication.publication_id).result_retained is False


@pytest.mark.unit
def test_reconstruction_system_retry_exhaustion_clears_both_markers() -> None:
    core, original = _lost_reconstructible_task(max_retries=2)

    core._start_or_join_reconstruction(
        original.object_id, core._objects[original.object_id]
    )
    attempt_1 = _next_pending(core)
    assert not core._retry_explicit_system_failure(
        attempt_1, _system_reply(attempt_1, WorkerID.random())
    )
    attempt_2 = _next_pending(core)

    assert core._retry_explicit_system_failure(
        attempt_2, _system_reply(attempt_2, WorkerID.random())
    )
    snapshot = core.owner_table.snapshot(original.object_id)
    assert snapshot.state is ObjectState.ERROR
    assert snapshot.current_attempt == attempt_2.spec.attempt_id
    assert core._recovery.active_recovery(original.spec.task_id) is None
    assert not core._reconstruction.complete(
        original.spec.task_id, attempt_2.spec.attempt_id
    )
    record = core._recovery.task_record(original.spec.task_id)
    assert record.retries_started == record.max_retries == 2
    assert not any(
        isinstance(item, _PendingTask)
        for item in tuple(core._submissions.queue)
    )


@pytest.mark.unit
def test_reconstruction_identity_disagreement_consumes_no_retry_budget() -> None:
    core, original = _lost_reconstructible_task(max_retries=3)
    core._start_or_join_reconstruction(
        original.object_id, core._objects[original.object_id]
    )
    attempt_1 = _next_pending(core)
    record = core._recovery.task_record(original.spec.task_id)
    assert record.retries_started == 1

    # Model an invariant breach: the coordinator plan vanished while the
    # RecoveryManager still owns the active reconstruction identity.
    core._reconstruction._sessions.pop(original.spec.task_id)
    with pytest.raises(SystemTaskError, match="without a coordinator plan"):
        core._retry_explicit_system_failure(
            attempt_1, _system_reply(attempt_1, WorkerID.random())
        )

    assert record.retries_started == 1
    assert record.current_attempt == attempt_1.spec.attempt_id
    snapshot = core.owner_table.snapshot(original.object_id)
    assert snapshot.state is ObjectState.PENDING
    assert snapshot.current_attempt == attempt_1.spec.attempt_id
    assert not any(
        isinstance(item, _PendingTask)
        for item in tuple(core._submissions.queue)
    )
