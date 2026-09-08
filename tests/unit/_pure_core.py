"""Threadless Core composition for reconstruction contract tests.

Only infrastructure is fake: real Core methods own submission, reconstruction,
publication, finalization, and the owner/recovery state transitions.  There is
no Core constructor, coordinator, dispatch lane, listener, or timer.  The test
owns FIFO consumption and explicitly chooses when queued reference GC runs.
"""

from __future__ import annotations

import hashlib
import queue
import threading
import weakref

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import CoreWorker, _LocalReferenceRelease, _RetryInlineGc, _RetryReplicaCleanup
from miniray.ids import JobID, NodeID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector
from miniray.trace import MemoryEventSink


def _unexpected_effect(*_args, **_kwargs):
    # pytest.fail is not an Exception, so best-effort production RPC/finalizer
    # handlers cannot silently turn an unexpected effect into a passing test.
    pytest.fail("pure Core fixture attempted unmodelled RPC/thread/timer work")


class SynchronousReferenceMailbox:
    """Apply the real local-token release before ObjectRef.close returns.

    GC notifications stay in an explicit FIFO.  Draining calls the actual Core
    collector; it never substitutes an always-successful collection algorithm.
    RLock/Condition/Event objects do not start threads.  Keeping real locks also
    permits the separately marked two-request L1 tests to exercise composition.
    """

    def __init__(self, core: CoreWorker) -> None:
        self.core_reference = weakref.ref(core)
        self.lock = threading.RLock()
        self.accepting = True
        self.pending: queue.Queue[object] = queue.Queue()
        # Core.shutdown observes the real FIFO's unfinished-work counter.
        # Tests drain it explicitly; no fake zero hides outstanding GC work.
        self.events = self.pending
        self.releases: list[_LocalReferenceRelease] = []

    def enqueue(self, event: object) -> bool:
        if not isinstance(event, _LocalReferenceRelease):
            _unexpected_effect(event)
        with self.lock:
            if not self.accepting:
                return False
            core = self.core_reference()
            assert core is not None
            try:
                released = core.owner_table.release_local_reference(
                    event.object_id, event.token
                )
                if released:
                    core._enqueue_inline_gc_check(event.object_id)
                self.releases.append(event)
            finally:
                event.done.set()
            return True

    def enqueue_internal(self, event: object) -> bool:
        if not isinstance(event, (_RetryInlineGc, _RetryReplicaCleanup)):
            _unexpected_effect(event)
        self.pending.put_nowait(event)
        return True

    def close_admission(self) -> None:
        with self.lock:
            self.accepting = False

    def drain(self) -> None:
        core = self.core_reference()
        assert core is not None
        for _ in range(128):
            try:
                event = self.pending.get_nowait()
            except queue.Empty:
                return
            try:
                if isinstance(event, _RetryReplicaCleanup):
                    core._drive_late_replica_cleanup(from_event=True)
                else:
                    core._reference_released(event.object_id)
            finally:
                self.pending.task_done()
        if self.pending.empty():
            return
        pytest.fail("pure Core reference collection exceeded 128 events")


def make_pure_core() -> CoreWorker:
    """Construct only the authorities touched by reconstruction contracts.

    Addresses are inert protocol metadata, not listeners.  Every transport
    boundary rejects calls unless a test installs an exact typed fake reply.
    Missing state fails normally instead of inheriting new constructor effects.
    """

    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.node_address = ("node.invalid", 1)
    core.owner_address = ("owner.invalid", 1)
    core.gcs_address = None
    core.inline_threshold = 100 * 1024
    core._submission_index = 0
    core._put_index = 0
    core._reference_index = 0
    core._owner_table = ObjectOwnerTable()
    core._recovery = RecoveryManager()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._accepting = True
    core._owner_protocol_open = True
    core._owner_retain_admission_open = True
    core._inflight_submissions = 0
    core._inflight_borrow_ops = 0
    core._inflight_puts = 0
    core._accepted_task_count = 0
    core._submissions = queue.Queue()
    core._blocked_tasks = {}
    core._protocol_unresolved = {}
    core._task_finish_barriers = {}
    core._finished_tasks = set()
    core._finishing_tasks = set()
    core._active_task_finishes = set()
    core._object_gc_obligations = {}
    core._inline_gc_obligations = core._object_gc_obligations
    core._dead_nodes = {}
    core._gc_retry_timers = set()
    core._gc_retry_timers_open = False
    core._foreign_lineage_collection_receipts = {}
    core._foreign_lineage_prepared_collection_receipts = {}
    core._actor_call_threads = set()
    core._dispatchers = ()
    core._dispatcher = None
    core._sink_closed = False
    core.event_sink = MemoryEventSink()
    core._reference_mailbox = SynchronousReferenceMailbox(core)
    core._rpc = _unexpected_effect
    core._borrow_rpc = _unexpected_effect
    core._borrow_rpc_with_deadline = _unexpected_effect
    core._push_task_rpc = _unexpected_effect
    core._actor_call_rpc = _unexpected_effect
    core._initialize_reference_events = _unexpected_effect
    core._schedule_reference_event = _unexpected_effect
    core._ensure_foreign_lineage_runtime()
    core._reconstruction_coordinator()
    core._targeted_reconstruction_coordinator()
    return core


def close_pure_core(core: CoreWorker) -> None:
    """Fence the fixture without pretending to do distributed shutdown.

    Tests may intentionally leave admitted tasks or unprocessed GC notices.
    Neither can progress by itself.  Do not reset counts or erase authorities
    to make teardown pass; check the real handle releases and absence of runtime
    infrastructure instead.
    """

    mailbox = core._reference_mailbox
    assert isinstance(mailbox, SynchronousReferenceMailbox)
    mailbox.close_admission()
    assert all(event.done.is_set() for event in mailbox.releases)
    assert all(
        not core.owner_table.snapshot(object_id).local_tokens
        for object_id in core._objects
    ), "pure Core fixture retained an unclosed local ObjectRef"
    assert not core._gc_retry_timers
    assert core._dispatchers == () and core._dispatcher is None
    assert not any(
        hasattr(core, name)
        for name in ("_coordinator", "_reference_thread")
    )


def lost_reconstruction_core(*, num_returns: int = 1, partial: bool = False):
    """Publish then lose a real owner/recovery task without executing it.

    The complete immutable output manifest and original success are installed
    through the same authorities as the old full-runtime test fixtures.
    """

    assert num_returns in (1, 3)
    assert not partial or num_returns == 3
    core = make_pure_core()
    definition = core.define_remote_function(
        (lambda: {"reconstructed": True})
        if num_returns == 1 else (lambda: (10, 20, 30))
    )
    pending, output = core._register_submission(
        definition, (), {}, ResourceVector({"CPU": 1}),
        num_returns=num_returns, max_retries=1 if num_returns == 1 else 2,
    )
    assert isinstance(output, tuple) is (num_returns == 3)
    refs = output if isinstance(output, tuple) else (output,)
    assert len(refs) == num_returns
    descriptors = []
    for index, output_id in enumerate(pending.output_ids):
        payload = cloudpickle.dumps({"original": True} if num_returns == 1 else index)
        descriptor = protocol.ResultDescriptor(
            output_id, protocol.ResultStorage.OBJECT_STORE, len(payload),
            core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(),
        )
        assert core.owner_table.publish_stored(
            output_id, pending.spec.attempt_id, core.node_id
        )
        core._stored_descriptors[output_id] = descriptor
        descriptors.append(descriptor)
    core._recovery.record_task_success(pending.task_id, pending.spec.attempt_id)
    lost_ids = (pending.output_ids[1],) if partial else pending.output_ids
    for output_id in lost_ids:
        assert core.owner_table.mark_lost(output_id, pending.spec.attempt_id)
    for output_id in pending.output_ids:
        core._objects[output_id].event.set()
    return core, pending, refs, tuple(descriptors)
