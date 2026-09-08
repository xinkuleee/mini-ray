"""Pure fault-injection contracts for multi-return submission admission.

One threadless Core registers at most one inert producer and one three-output
consumer; no callable executes or result bytes are published. Real owner and
recovery preflights/commits, reference binding and compensating rollback remain
in use. Exact saved finalizers prove that partial binding is detached, not
merely hidden by object GC. The manual mailbox drains at most three local GC
notices. No thread, process, socket, wait, timer or store allocation runs.

The dependency case deliberately retains its unexecuted PENDING producer
metadata after local-handle release; release is not terminal task completion.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module
from miniray.core import CoreWorker, ObjectRef, RemoteFunctionDefinition, _RetryInlineGc
from miniray.ids import ObjectID, TaskID
from miniray.object_store import ObjectStore
from miniray.ownership import ObjectState
from miniray.recovery import UnknownTaskError
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.unit


class _FailingQueue(queue.Queue):
    def put(self, item: object, block: bool = True, timeout=None) -> None:
        raise RuntimeError("injected enqueue failure")


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure submission rollback attempted runtime work")

    for kind, method in (
        (CoreWorker, "__init__"), (ObjectStore, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)


def _close_local(ref):
    if ref._finalizer is not None and ref._finalizer.alive:
        assert ref.borrower_token is None
        done = ref._release_done
        ref._closed = True
        ref._finalizer()
        assert done is not None and done.is_set()


@pytest.fixture
def submission_core(monkeypatch):
    core = make_pure_core()
    created, bindings = [], []
    initialize = ObjectRef.__init__
    bind = ObjectRef._bind_local_reference

    def observe_initialize(ref, *args, **kwargs):
        assert len(created) < 4
        initialize(ref, *args, **kwargs)
        created.append(ref)

    def observe_bind(ref, owner, token):
        assert owner is core and len(bindings) < 3
        bind(ref, owner, token)
        assert ref._finalizer.alive and not ref._release_done.is_set()
        bindings.append((ref, ref._finalizer, ref._release_done))

    monkeypatch.setattr(ObjectRef, "__init__", observe_initialize)
    monkeypatch.setattr(ObjectRef, "_bind_local_reference", observe_bind)
    try:
        yield core, created, bindings
    finally:
        for ref in created:
            _close_local(ref)
        close_pure_core(core)


def _definition(core: CoreWorker) -> RemoteFunctionDefinition:
    return RemoteFunctionDefinition.from_callable(lambda value=None: value, core.job_id)


def _next_output_ids(core: CoreWorker) -> tuple:
    task_id = TaskID.derive(core.job_id, core.driver_task_id, core._submission_index)
    return tuple(ObjectID.for_task(task_id, index) for index in range(3))


def _assert_clean(core: CoreWorker, output_ids: tuple) -> None:
    assert all(not core.owner_table.contains(value) for value in output_ids)
    assert not any(
        core._recovery.lineage_for_object(value) is not None
        for value in output_ids
    )
    assert not set(output_ids).intersection(core._objects)
    assert not core.owner_table.task_lineage_edges(output_ids[0].task_id)
    with pytest.raises(UnknownTaskError):
        core._recovery.task_record(output_ids[0].task_id)
    assert core._accepted_task_count == 0
    assert core._inflight_submissions == 0
    assert not core._task_finish_barriers and not core._protocol_unresolved
    assert core._submissions.empty()
    assert core._submissions.unfinished_tasks == 0
    assert core._reference_mailbox.events.unfinished_tasks == 0


def _assert_aborted_handles(core, created, bindings, output_ids, *, initialized=3, bound=0):
    refs = tuple(ref for ref in created if ref.object_id in output_ids)
    assert tuple(ref.object_id for ref in refs) == output_ids[:initialized]
    assert len(refs) == initialized
    for ref in refs:
        assert ref._finalizer is None and ref._local_token is None
        assert ref._release_done is None and ref.borrower_token is None
    saved = tuple((ref, finalizer, done) for ref, finalizer, done in bindings
                  if ref.object_id in output_ids)
    assert len(saved) == bound
    for _ref, finalizer, done in saved:
        assert not finalizer.alive and finalizer.peek() is None
        assert not done.is_set()  # detach must not issue a local release
        assert finalizer() is None
    assert not core._reference_mailbox.releases


def _drain_gc_notices(core, object_id, expected):
    mailbox = core._reference_mailbox
    queued = tuple(mailbox.pending.queue)
    assert len(queued) == expected and expected <= 2
    assert all(type(event) is _RetryInlineGc and event.object_id == object_id for event in queued)
    mailbox.drain()
    assert mailbox.pending.empty() and mailbox.events.unfinished_tasks == 0


def test_failure_immediately_after_owner_manifest_registration_aborts_all(
    monkeypatch: pytest.MonkeyPatch, submission_core,
) -> None:
    core, created, bindings = submission_core
    output_ids = _next_output_ids(core)
    original = core.owner_table.commit_register_task_outputs

    def commit_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        assert all(core.owner_table.contains(value) for value in output_ids)
        raise RuntimeError("after owner registration")

    monkeypatch.setattr(
        core.owner_table, "commit_register_task_outputs", commit_then_fail
    )
    with pytest.raises(RuntimeError, match="after owner registration"):
        core._register_submission(
            _definition(core), (), {}, ResourceVector(), num_returns=3
        )
    _assert_clean(core, output_ids)
    _assert_aborted_handles(core, created, bindings, output_ids)


@pytest.mark.parametrize("fail_after", [1, 2])
def test_failure_during_inert_ref_creation_precedes_all_authority(
    monkeypatch: pytest.MonkeyPatch, fail_after: int, submission_core,
) -> None:
    core, created, bindings = submission_core
    output_ids = _next_output_ids(core)
    original = ObjectRef.__init__
    calls = 0

    def initialize_then_fail(ref, *args, **kwargs):
        nonlocal calls
        original(ref, *args, **kwargs)
        calls += 1
        if calls == fail_after:
            assert all(not core.owner_table.contains(value) for value in output_ids)
            assert not core._objects and not bindings
            raise RuntimeError("inert ref creation")

    monkeypatch.setattr(ObjectRef, "__init__", initialize_then_fail)
    with pytest.raises(RuntimeError, match="inert ref creation"):
        core._register_submission(
            _definition(core), (), {}, ResourceVector(), num_returns=3
        )
    _assert_clean(core, output_ids)
    assert calls == fail_after
    _assert_aborted_handles(core, created, bindings, output_ids, initialized=fail_after)


def test_failure_after_recovery_registration_aborts_both_authorities(
    monkeypatch: pytest.MonkeyPatch, submission_core,
) -> None:
    core, created, bindings = submission_core
    output_ids = _next_output_ids(core)
    original = core._recovery.register_task

    def register_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        assert all(core._recovery.lineage_for_object(value) is not None for value in output_ids)
        raise RuntimeError("after recovery registration")

    monkeypatch.setattr(core._recovery, "register_task", register_then_fail)
    with pytest.raises(RuntimeError, match="after recovery registration"):
        core._register_submission(
            _definition(core), (), {}, ResourceVector(), num_returns=3
        )
    _assert_clean(core, output_ids)
    _assert_aborted_handles(core, created, bindings, output_ids)


@pytest.mark.parametrize("fail_after", [1, 2])
def test_failure_after_partial_ref_binding_leaves_no_token_or_finalizer(
    monkeypatch: pytest.MonkeyPatch, fail_after: int, submission_core,
) -> None:
    core, created, bindings = submission_core
    output_ids = _next_output_ids(core)
    original = ObjectRef._bind_local_reference
    calls = 0

    def bind_then_maybe_fail(ref, owner, token):
        nonlocal calls
        original(ref, owner, token)
        calls += 1
        if calls == fail_after:
            raise RuntimeError("partial ref binding")

    monkeypatch.setattr(
        ObjectRef, "_bind_local_reference", bind_then_maybe_fail
    )
    with pytest.raises(RuntimeError, match="partial ref binding"):
        core._register_submission(
            _definition(core), (), {}, ResourceVector(), num_returns=3
        )
    _assert_clean(core, output_ids)
    assert calls == fail_after
    _assert_aborted_handles(core, created, bindings, output_ids, bound=fail_after)


def test_enqueue_failure_rolls_back_count_refs_waiters_and_authorities(submission_core) -> None:
    core, created, bindings = submission_core
    output_ids = _next_output_ids(core)
    core._submissions = _FailingQueue()

    with pytest.raises(RuntimeError, match="enqueue failure"):
        core._register_submission(
            _definition(core), (), {}, ResourceVector(),
            num_returns=3, _enqueue=True,
        )
    _assert_clean(core, output_ids)
    _assert_aborted_handles(core, created, bindings, output_ids, bound=3)


def test_dependency_holds_and_lineage_are_exactly_rolled_back(
    monkeypatch: pytest.MonkeyPatch, submission_core,
) -> None:
    core, created, bindings = submission_core
    producer, dependency = core._register_submission(
        _definition(core), (), {}, ResourceVector()
    )
    before = core.owner_table.snapshot(producer.object_id)
    output_ids = _next_output_ids(core)
    original = core._recovery.register_task
    attempted = []

    def fail_consumer(task_spec, **kwargs):
        if task_spec.task_id != producer.task_id:
            original(task_spec, **kwargs)
            assert task_spec.return_ids() == output_ids and not attempted
            during = core.owner_table.snapshot(producer.object_id)
            assert len(during.submitted_tokens - before.submitted_tokens) == 1
            assert len(during.lineage_tokens - before.lineage_tokens) == 1
            assert core.owner_table.task_lineage_edges(task_spec.task_id)
            attempted.append(task_spec)
            raise RuntimeError("consumer registration failure")
        return original(task_spec, **kwargs)

    monkeypatch.setattr(core._recovery, "register_task", fail_consumer)
    with pytest.raises(RuntimeError, match="consumer registration failure"):
        core._register_submission(
            _definition(core), (dependency,), {}, ResourceVector(),
            num_returns=3,
        )

    after = core.owner_table.snapshot(producer.object_id)
    assert after.submitted_tokens == before.submitted_tokens
    assert after.lineage_tokens == before.lineage_tokens
    assert set(core._objects) == {producer.object_id}
    assert core._recovery.lineage_for_object(producer.object_id) is not None
    assert len(attempted) == 1
    assert after.state is ObjectState.PENDING and after.local_tokens == before.local_tokens
    assert after.current_attempt == before.current_attempt
    assert len(after.released_lineage_tokens - before.released_lineage_tokens) == 1
    _assert_aborted_handles(core, created, bindings, output_ids)
    _drain_gc_notices(core, producer.object_id, 2)
    _assert_clean(core, output_ids)
    assert core.owner_table.snapshot(producer.object_id) == after

    # Invoke the same local finalizer as close, without Event.wait. An
    # unexecuted producer remains PENDING and is not metadata-collectable.
    _close_local(dependency)
    assert dependency.closed and not dependency._finalizer.alive
    assert len(core._reference_mailbox.releases) == 1
    _drain_gc_notices(core, producer.object_id, 1)
    current = core.owner_table.snapshot(producer.object_id)
    assert current.state is ObjectState.PENDING and not current.local_tokens
    assert not current.submitted_tokens and not current.lineage_tokens
    assert core._recovery.lineage_for_object(producer.object_id) is not None
    assert not core._object_gc_obligations
