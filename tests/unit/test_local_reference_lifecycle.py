"""Real reference-consumer lifecycle contracts, not pure owner reducers.

Four exact loopback cases run one real reference thread, one PENDING object
and at most two handles; they create no socket, process or user Task. Finite
public close and FIFO stop/joins are protected by an unconditional fixture
finalizer. Run one exact ID at a time through the 30-second bounded runner.
The two cases that explicitly run process-wide gc.collect remain heavy.
"""

from __future__ import annotations

import gc
import pickle
import threading
import time
import weakref

import pytest

from miniray.core import CoreWorker, ObjectRef, _ObjectWaiter
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable, ObjectState




def _core_with_object(*, start: bool = True) -> tuple[CoreWorker, ObjectID]:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core._owner_table = ObjectOwnerTable()
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    task_id = TaskID.derive(core.job_id, TaskID.for_driver(core.job_id), 0)
    attempt_id = AttemptID(task_id, 0)
    object_id = ObjectID.for_task(task_id, 0)
    core._owner_table.register(object_id, current_attempt=attempt_id)
    core._objects = {object_id: _ObjectWaiter(threading.Event())}
    if start:
        core._initialize_reference_events()
    return core, object_id


def _stop_reference_events(core: CoreWorker) -> None:
    assert core._stop_reference_events(time.monotonic() + 1.0)


@pytest.fixture
def _bounded_reference_core():
    core, object_id = _core_with_object(start=False)
    try:
        core._initialize_reference_events()
        assert core._reference_thread.is_alive()
        yield core, object_id
    finally:
        # Register this finally before Thread.start, including partial startup.
        # Stopping the actual mailbox admits no new handles and processes all
        # already-admitted releases before its FIFO stop marker.
        if hasattr(core, "_reference_thread"):
            thread = core._reference_thread
            if thread.ident is not None:
                _stop_reference_events(core)
                assert not thread.is_alive()
                assert core._reference_mailbox.stopped.is_set()
            else:
                # A Thread.start failure has no consumer to join or drain.
                # No handle was exposed before the fixture yielded.
                core._reference_mailbox.close_admission()
                core._reference_runtime_finalizer.detach()
                assert not core._reference_mailbox.stop_enqueued
            assert core._reference_mailbox.events.unfinished_tasks == 0
            assert not core._gc_retry_timers


def _close_handles(*references):
    deadline = time.monotonic() + 1.0
    failures = []
    for ref in references:
        if ref is not None:
            try:
                ref.close(timeout=max(0.0, deadline - time.monotonic()))
            except Exception as exc:
                failures.append(exc)
    assert not failures, failures


@pytest.mark.loopback_smoke
def test_each_python_handle_has_a_distinct_local_token(_bounded_reference_core) -> None:
    core, object_id = _bounded_reference_core
    first = second = None
    try:
        first = core._new_object_ref(object_id)
        second = core._new_object_ref(object_id)

        snapshot = core.owner_table.snapshot(object_id)
        assert snapshot.local_tokens == frozenset(
            {first._local_token, second._local_token}
        )
        assert first._local_token != second._local_token
        assert first == second
        assert hash(first) == hash(second)
    finally:
        _close_handles(first, second)


@pytest.mark.loopback_smoke
def test_close_releases_exactly_once_and_invalidates_only_that_handle(_bounded_reference_core) -> None:
    core, object_id = _bounded_reference_core
    ref = core._new_object_ref(object_id)
    token = ref._local_token
    try:
        ref.close(timeout=1.0)
        ref.close(timeout=1.0)

        assert ref.closed
        assert token not in core.owner_table.snapshot(object_id).local_tokens
        with pytest.raises(ValueError, match="closed"):
            core._validate_ref(ref)
        with pytest.raises(RuntimeError, match="closed"):
            pickle.dumps(ref)
    finally:
        _close_handles(ref)


@pytest.mark.heavy
def test_gc_finalizer_enqueues_release_but_does_not_collect_owner_metadata() -> None:
    core, object_id = _core_with_object()
    try:
        ref = core._new_object_ref(object_id)
        token = ref._local_token
        released = ref._release_done
        ref_weak = weakref.ref(ref)

        del ref
        gc.collect()

        assert released is not None and released.wait(1.0)
        assert ref_weak() is None
        snapshot = core.owner_table.snapshot(object_id)
        assert token not in snapshot.local_tokens
        assert snapshot.state is ObjectState.PENDING
        assert core.owner_table.contains(object_id)
    finally:
        _stop_reference_events(core)


@pytest.mark.loopback_smoke
def test_shutdown_drains_accepted_releases_and_late_close_is_a_noop(_bounded_reference_core) -> None:
    core, object_id = _bounded_reference_core
    accepted = core._new_object_ref(object_id)
    accepted_token = accepted._local_token
    accepted._finalizer()

    late = core._new_object_ref(object_id)
    late_token = late._local_token
    _stop_reference_events(core)

    assert accepted_token not in core.owner_table.snapshot(object_id).local_tokens
    late.close(timeout=1.0)
    assert late.closed
    assert late_token in core.owner_table.snapshot(object_id).local_tokens


@pytest.mark.heavy
def test_object_ref_finalizer_does_not_keep_core_worker_alive() -> None:
    core, object_id = _core_with_object()
    ref = core._new_object_ref(object_id)
    _stop_reference_events(core)
    core_weak = weakref.ref(core)

    del core
    gc.collect()

    assert core_weak() is None
    ref.close()


@pytest.mark.loopback_smoke
def test_unpickled_logical_handle_is_detached_until_borrower_protocol_exists(_bounded_reference_core) -> None:
    core, object_id = _bounded_reference_core
    ref = core._new_object_ref(object_id)
    try:
        restored = pickle.loads(pickle.dumps(ref))
        assert restored == ref
        assert restored._local_token is None
        restored.close(timeout=1.0)
        assert not restored.closed
    finally:
        _close_handles(ref)
