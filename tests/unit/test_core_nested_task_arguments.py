"""Nested Task-argument lifetime contracts with explicit runtime boundaries.

Four unit cases use threadless Cores, real owner handlers and explicitly drained
reference/FIFO work. At most two Cores, two Tasks and four small handles appear;
no user function, process, socket, timer or real wait runs in those cases.
The two attempt-borrow cases retain their original live Core/timing behavior
and remain heavy. Their factory and bodies are deliberately unchanged.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.core import CoreWorker, ObjectRef, _WAKE_COORDINATOR
from miniray.foreign_lineage import ForeignLineageRole
from miniray.ids import AttemptID, NodeID
from miniray.ownership import ObjectCollectionState
from miniray.resources import ResourceVector
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core, make_pure_core


@pytest.fixture
def _no_pure_runtime(monkeypatch):
    """Used explicitly by four unit cases, never inherited by heavy ones."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure nested-argument contract attempted runtime work")

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


def _drain_pure(core):
    """Run existing reference reducers, then consume only finite wake records."""
    assert core._reference_mailbox.pending.qsize() <= 16
    core._reference_mailbox.drain()
    size = core._submissions.qsize()
    assert size <= 16
    for _ in range(size):
        assert core._submissions.get_nowait() is _WAKE_COORDINATOR
        core._submissions.task_done()
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    assert core._reference_mailbox.pending.empty()
    assert core._reference_mailbox.pending.unfinished_tasks == 0


def _core() -> CoreWorker:
    return CoreWorker(
        ("127.0.0.1", 28001),
        NodeID.random(),
        owner_address=("127.0.0.1", 28002),
    )


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pure_runtime")
def test_nested_local_ref_is_held_but_never_gates_readiness() -> None:
    core = make_pure_core()
    producer, source = core._register_submission(
        core.define_remote_function(lambda: 7), (), {}, ResourceVector()
    )
    consumer, result = core._register_submission(
        core.define_remote_function(lambda value: value),
        ({"left": source, "right": [source]},),
        {},
        ResourceVector(),
    )
    try:
        argument = consumer.spec.args[0]
        assert isinstance(argument, protocol.InlineArg)
        assert len(argument.nested_refs) == 1
        assert argument.nested_refs[0].hold == consumer.dependency_hold
        assert consumer.dependency_hold == protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED,
            core.worker_id,
            consumer.spec.task_id,
            consumer.spec.attempt_id,
        )
        assert consumer.protected_dependencies == ()
        assert consumer.foreign_dependency_guards == ()
        assert consumer.nested_local_holds == (producer.object_id,)
        assert consumer.dependency_hold in core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens
        lineage_token = (
            f"lineage:{consumer.spec.task_id}:{producer.object_id}"
        )
        assert lineage_token in core.owner_table.snapshot(
            producer.object_id
        ).lineage_tokens
        assert any(
            edge.dependency_object_id == producer.object_id
            and edge.token == lineage_token
            for edge in core.owner_table.snapshot(
                consumer.object_id
            ).outgoing_lineage_edges
        )
        # The producer remains PENDING. A nested handle is data, so it must not
        # enter the dependency-ready gate.
        assert core._dependencies_ready(consumer)
        source.close()
        assert consumer.dependency_hold in core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens
        core._publish_error(
            consumer.object_id, consumer.spec.attempt_id, RuntimeError("done")
        )
        assert core._finish_pending_task(consumer)
        assert consumer.dependency_hold not in core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens
        # The execution incarnation is terminal, but canonical producer
        # lineage still owns the nested handle until the consumer output is
        # itself collected.
        assert lineage_token in core.owner_table.snapshot(
            producer.object_id
        ).lineage_tokens
    finally:
        source.close()
        result.close()
        core._publish_error(
            producer.object_id, producer.spec.attempt_id, RuntimeError("cleanup")
        )
        core._finish_pending_task(producer)
        _drain_pure(core)
        assert core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(producer.object_id) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(consumer.object_id) is None
        assert core._recovery.lineage_for_object(producer.object_id) is None
        assert core.shutdown(timeout=1.0)
        close_pure_core(core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pure_runtime")
def test_nested_hold_survives_system_retry_and_releases_once() -> None:
    core = make_pure_core()
    producer, source = core._register_submission(
        core.define_remote_function(lambda: 7), (), {}, ResourceVector()
    )
    consumer, result = core._register_submission(
        core.define_remote_function(lambda value: value),
        ({"ref": source},), {}, ResourceVector(), max_retries=1,
    )
    try:
        error_reply = protocol.TaskReply(
            consumer.spec.task_id, consumer.spec.attempt_id, core.worker_id,
            protocol.TaskReplyStatus.SYSTEM_ERROR,
            error=protocol.RemoteErrorInfo("RuntimeError", "retry"),
        )
        assert not core._retry_explicit_system_failure(consumer, error_reply)
        retried = core._submissions.get_nowait()
        core._submissions.task_done()
        assert retried.spec.attempt_id != consumer.spec.attempt_id
        assert retried.dependency_hold == consumer.dependency_hold
        assert consumer.dependency_hold == protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED,
            core.worker_id,
            consumer.spec.task_id,
            consumer.spec.attempt_id,
        )
        assert retried.nested_local_holds == consumer.nested_local_holds
        assert core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens == frozenset({consumer.dependency_hold})
        core._publish_error(
            retried.object_id, retried.spec.attempt_id, RuntimeError("done")
        )
        assert core._finish_pending_task(retried)
        assert core._finish_pending_task(retried)
        assert consumer.dependency_hold not in core.owner_table.snapshot(
            producer.object_id
        ).submitted_tokens
    finally:
        source.close()
        result.close()
        core._publish_error(
            producer.object_id, producer.spec.attempt_id, RuntimeError("cleanup")
        )
        core._finish_pending_task(producer)
        _drain_pure(core)
        assert core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(producer.object_id) is ObjectCollectionState.COLLECTED
        assert core.shutdown(timeout=1.0)
        close_pure_core(core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pure_runtime")
def test_ambiguous_foreign_nested_retain_is_recorded_before_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = make_pure_core()
    borrower = make_pure_core()
    produced, owner_ref = owner._register_submission(
        owner.define_remote_function(lambda: 9), (), {}, ResourceVector()
    )
    owner.owner_table.add_borrowed_reference(
        produced.object_id, (borrower.worker_id, "borrow")
    )
    foreign = ObjectRef(
        produced.object_id, owner.worker_id, owner.owner_address
    )
    foreign._borrower_token = "borrow"
    releases = 0
    retains, release_requests = [], []

    def rpc(_address: object, handler: str, request: object) -> object:
        nonlocal releases
        assert _address == owner.owner_address
        assert not borrower._state_lock._is_owned()
        if handler == "retain_owned_object_for_task":
            reply = owner.retain_owned_object_for_task(request)
            assert reply.accepted
            assert request.hold in owner.owner_table.snapshot(produced.object_id).retained_tokens
            retains.append(request)
            raise TransportTimeout("all retain ACKs lost")
        if handler == "release_owned_object_for_task":
            releases += 1
            release_requests.append(request)
            return owner.release_owned_object_for_task(request)
        raise AssertionError(handler)

    monkeypatch.setattr(borrower, "_borrow_rpc", rpc)
    try:
        with pytest.raises(TransportTimeout, match="retain ACKs lost"):
            borrower._register_submission(
                borrower.define_remote_function(lambda value: value),
                ({"ref": foreign},), {}, ResourceVector(),
            )
        assert releases == 1
        assert not owner.owner_table.snapshot(produced.object_id).retained_tokens
        assert len(retains) == len(release_requests) == 1
        assert release_requests[0] == protocol.ReleaseOwnedObjectForTask(
            retains[0].object_id, retains[0].owner_worker_id,
            retains[0].borrower_worker_id, retains[0].hold,
        )
        assert borrower._inflight_submissions == 0 and not borrower._objects
        assert not getattr(borrower, "_orphan_foreign_guard_releases", {})
        assert borrower._foreign_lineage_registry.snapshot(retains[0].hold.task_id) is None
    finally:
        foreign.close()
        owner_ref.close()
        owner.owner_table.release_borrowed_reference(
            produced.object_id, (borrower.worker_id, "borrow")
        )
        owner._publish_error(
            produced.object_id, produced.spec.attempt_id, RuntimeError("cleanup")
        )
        owner._finish_pending_task(produced)
        _drain_pure(borrower)
        _drain_pure(owner)
        assert owner.owner_table.collection_state(produced.object_id) is ObjectCollectionState.COLLECTED
        assert borrower.shutdown(timeout=1.0)
        assert owner.shutdown(timeout=1.0)
        close_pure_core(borrower)
        close_pure_core(owner)


@pytest.mark.heavy
def test_attempt_borrow_release_obligation_replays_exact_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = _core()
    worker = _core()
    produced, owner_ref = owner._register_submission(
        owner.define_remote_function(lambda: 5), (), {}, ResourceVector()
    )
    consumer_task = worker.driver_task_id
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.SUBMITTED,
        owner.worker_id,
        consumer_task,
        AttemptID(consumer_task, 0),
    )
    owner.owner_table.add_submitted_reference(produced.object_id, hold)
    transfer = protocol.NestedReferenceTransfer(
        produced.object_id, owner.worker_id, owner.owner_address,
        hold,
    )
    replies: list[object] = []
    release_calls: list[protocol.ReleaseBorrowedObject] = []

    def rpc(_address: object, handler: str, request: object) -> object:
        if handler == "acquire_borrowed_object":
            return owner.acquire_exported_reference(request)
        if handler == "release_borrowed_object":
            release_calls.append(request)
            if len(release_calls) == 1:
                raise TransportTimeout("release ACK lost")
            return owner.release_borrowed_reference(request)
        raise AssertionError(handler)

    monkeypatch.setattr(worker, "_borrow_rpc", rpc)
    attempt = AttemptID(consumer_task, 0)
    ref = worker._restore_task_argument_reference(transfer, attempt)
    key = (owner.worker_id, produced.object_id, attempt)
    assert key in worker._attempt_borrow_releases
    ref.close()
    deadline = time.monotonic() + 1.0
    while key in worker._attempt_borrow_releases:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    assert len(release_calls) >= 2
    assert all(call == release_calls[0] for call in release_calls)
    assert key not in worker._attempt_borrow_releases
    assert not owner.owner_table.snapshot(produced.object_id).borrowed_tokens

    owner.owner_table.release_submitted_reference(produced.object_id, hold)
    owner_ref.close()
    owner._publish_error(
        produced.object_id, produced.spec.attempt_id, RuntimeError("cleanup")
    )
    owner._finish_pending_task(produced)
    assert worker.shutdown(timeout=1.0)
    assert owner.shutdown(timeout=1.0)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_pure_runtime")
def test_top_level_and_nested_foreign_handles_share_one_logical_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = make_pure_core()
    submitter = make_pure_core()
    produced, owner_ref = owner._register_submission(
        owner.define_remote_function(lambda: 3), (), {}, ResourceVector()
    )
    # Two Python handles for the same foreign object may have distinct parent
    # borrower tokens. The logical consumer still needs exactly one Task hold.
    owner.owner_table.add_borrowed_reference(
        produced.object_id, (submitter.worker_id, "borrow-a")
    )
    owner.owner_table.add_borrowed_reference(
        produced.object_id, (submitter.worker_id, "borrow-b")
    )
    first = ObjectRef(produced.object_id, owner.worker_id, owner.owner_address)
    second = ObjectRef(produced.object_id, owner.worker_id, owner.owner_address)
    first._borrower_token = "borrow-a"
    second._borrower_token = "borrow-b"
    retains: list[protocol.RetainOwnedObjectForTask] = []
    releases: list[protocol.ReleaseOwnedObjectForTask] = []

    def rpc(_address: object, handler: str, request: object) -> object:
        assert _address == owner.owner_address
        assert not submitter._state_lock._is_owned()
        if handler == "retain_owned_object_for_task":
            retains.append(request)
            return owner.retain_owned_object_for_task(request)
        if handler == "release_owned_object_for_task":
            releases.append(request)
            return owner.release_owned_object_for_task(request)
        raise AssertionError(handler)

    monkeypatch.setattr(submitter, "_borrow_rpc", rpc)
    consumer, result = submitter._register_submission(
        submitter.define_remote_function(lambda value, nested: value),
        (first, {"same": second}), {}, ResourceVector(),
    )
    try:
        assert len(retains) == 1
        assert len(consumer.foreign_dependency_guards) == 1
        assert consumer.nested_foreign_guards == ()
        assert len(owner.owner_table.snapshot(produced.object_id).retained_tokens) == 1
        lineage = submitter._foreign_lineage_registry.snapshot(consumer.task_id)
        assert lineage is not None and len(lineage.edges) == 1
        assert lineage.edges[0].hold == retains[0].hold
        assert lineage.edges[0].roles == ForeignLineageRole.TOP_LEVEL | ForeignLineageRole.NESTED
        submitter._publish_error(
            consumer.object_id, consumer.spec.attempt_id, RuntimeError("done")
        )
        assert submitter._finish_pending_task(consumer)
        # Execution completion releases only attempt-local holds.  The shared
        # foreign edge belongs to producer lineage and survives for possible
        # reconstruction until the result ObjectID is finally collected.
        assert releases == []
        assert len(
            owner.owner_table.snapshot(produced.object_id).retained_tokens
        ) == 1
    finally:
        result.close()
        _drain_pure(submitter)
        assert submitter._foreign_lineage_registry.snapshot(consumer.task_id) is None
        assert len(releases) == 1
        assert releases[0] == protocol.ReleaseOwnedObjectForTask(
            retains[0].object_id, retains[0].owner_worker_id,
            retains[0].borrower_worker_id, retains[0].hold,
        )
        assert not owner.owner_table.snapshot(
            produced.object_id
        ).retained_tokens
        owner.owner_table.release_borrowed_reference(
            produced.object_id, (submitter.worker_id, "borrow-a")
        )
        owner.owner_table.release_borrowed_reference(
            produced.object_id, (submitter.worker_id, "borrow-b")
        )
        owner_ref.close()
        owner._publish_error(
            produced.object_id, produced.spec.attempt_id, RuntimeError("cleanup")
        )
        owner._finish_pending_task(produced)
        _drain_pure(owner)
        assert submitter.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED
        assert owner.owner_table.collection_state(produced.object_id) is ObjectCollectionState.COLLECTED
        assert not submitter._foreign_lineage_runtime.has_pending_obligations()
        assert submitter.shutdown(timeout=1.0)
        assert owner.shutdown(timeout=1.0)
        close_pure_core(submitter)
        close_pure_core(owner)


@pytest.mark.heavy
def test_unreachable_owner_does_not_block_attempt_but_keeps_core_unclean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = _core()
    worker = _core()
    produced, owner_ref = owner._register_submission(
        owner.define_remote_function(lambda: 4), (), {}, ResourceVector()
    )
    consumer_task = worker.driver_task_id
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.SUBMITTED,
        owner.worker_id,
        consumer_task,
        AttemptID(consumer_task, 0),
    )
    owner.owner_table.add_submitted_reference(produced.object_id, hold)
    transfer = protocol.NestedReferenceTransfer(
        produced.object_id, owner.worker_id, owner.owner_address,
        hold,
    )
    release_available = False
    release_calls: list[protocol.ReleaseBorrowedObject] = []

    def rpc(_address: object, handler: str, request: object) -> object:
        if handler == "acquire_borrowed_object":
            return owner.acquire_exported_reference(request)
        if handler == "release_borrowed_object":
            release_calls.append(request)
            if not release_available:
                raise TransportTimeout("owner unavailable")
            return owner.release_borrowed_reference(request)
        raise AssertionError(handler)

    monkeypatch.setattr(worker, "_borrow_rpc", rpc)
    attempt = AttemptID(consumer_task, 0)
    ref = worker._restore_task_argument_reference(transfer, attempt)
    started = time.monotonic()
    ref.close()
    assert time.monotonic() - started < 0.5
    key = (owner.worker_id, produced.object_id, attempt)
    assert key in worker._attempt_borrow_releases
    assert not worker.can_finalize_shutdown(require_distributed_clean=False)

    release_available = True
    assert worker._drive_attempt_borrow_release(key)
    assert key not in worker._attempt_borrow_releases
    assert all(call == release_calls[0] for call in release_calls)
    assert not owner.owner_table.snapshot(produced.object_id).borrowed_tokens

    owner.owner_table.release_submitted_reference(produced.object_id, hold)
    owner_ref.close()
    owner._publish_error(
        produced.object_id, produced.spec.attempt_id, RuntimeError("cleanup")
    )
    owner._finish_pending_task(produced)
    assert worker.shutdown(timeout=1.0)
    assert owner.shutdown(timeout=1.0)
