"""Retained public value/lifetime contracts after multi-return retirement.

One Task returns one Python value, including tuples; wait may still request
several independent references. Existing pure publication fixtures provide
real owner/Node handoffs, stored bytes and GC. Per case: at most two Tasks,
three handles, one 4 KiB store, one retry and one explicit replay. No runtime
thread/process/socket, blocking wait or producer execution is started.
"""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

import miniray as ray
from miniray import api, protocol
from miniray.core import CoreWorker, ObjectRef, _DelayedReadyTask, _PendingTask, _WAKE_COORDINATOR
from miniray.errors import ProtocolError, SystemTaskError, TaskError
from miniray.ids import LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectCollectionState, ObjectState, OutputOwnerPublicationPlan
from miniray.recovery import FailureKind, TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime
from tests.unit.test_core_output_publication import _fixture as _publication, _close, _take_adoption
from tests.unit.test_task_finish_barrier import _OutputBackend


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("public single-output contract attempted runtime work")
    for kind, method in (
        (CoreWorker, "__init__"), (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"), (threading.Barrier, "wait"),
        (queue.Queue, "join"), (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _never_execute(*_args):
    pytest.fail("pure public fixture executed user code")


def _take(core):
    size = core._submissions.qsize()
    assert size <= 8
    items = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            assert isinstance(item, (_PendingTask, _DelayedReadyTask))
            items.append(item)
    assert core._submissions.unfinished_tasks == 0
    return tuple(items)


def _register(core, *args):
    pending, ref = core._register_submission(
        core.define_remote_function(_never_execute), args, {}, ResourceVector({"CPU": 1}),
        num_returns=1, max_retries=1, _enqueue=True,
    )
    assert _take(core) == (pending,)
    return pending, ref


def _release(ref):
    if ref.closed:
        return
    done, finalizer = ref._release_done, ref._finalizer
    assert done is not None and finalizer is not None
    ref._closed = True
    finalizer()
    assert done.is_set() and not finalizer.alive


@contextmanager
def _core_case():
    core, refs = make_pure_core(), []
    try:
        yield core, refs
    finally:
        for ref in refs:
            _release(ref)
        close_pure_core(core)


def _facts(core, pending):
    return (
        core.owner_table.snapshot(pending.object_id), replace(core._recovery.task_record(pending.task_id)),
        tuple(core._submissions.queue), core._submissions.unfinished_tasks,
        core._accepted_task_count, dict(core._task_finish_barriers), dict(core._protocol_unresolved),
        dict(core._stored_descriptors), tuple(core._reference_mailbox.pending.queue),
        core._objects[pending.object_id].event.is_set(),
    )


def _finish_and_collect(core, pending, ref):
    assert core._finish_pending_task(pending)
    assert _take(core) == ()
    _release(ref)
    core._reference_mailbox.drain()
    assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
    assert core._recovery.lineage_for_object(ref.object_id) is None


def _finish_unleased(core, pending, ref):
    assert core._publish_task_error(pending, SystemTaskError("unleased fixture terminal"))
    _finish_and_collect(core, pending, ref)


def _inline_reply(core, runtime, pending, value):
    push = protocol.PushTask(LeaseID.random(), WorkerID.random(), pending.spec)
    return runtime.complete(push, (value,))


def _bind_backend(core):
    def forbidden(*_args):
        pytest.fail("ref-free publication attempted unrelated transport")
    backend = _OutputBackend(core, forbidden)
    core._rpc, core._resolve_node_address = backend.rpc, backend.address
    return backend


def test_public_option_is_single_output_function_only_and_copied():
    fn = ray.remote(num_returns=1, max_retries=2)(_never_execute)
    overridden = fn.options(num_returns=1, max_retries=1)
    assert fn is not overridden and fn._num_returns == overridden._num_returns == 1
    assert fn._max_retries == 2 and overridden._max_retries == 1
    for invalid in (True, 0, 2, 3, -1, 1.5, "2"):
        with pytest.raises((TypeError, ValueError), match="num_returns"):
            ray.remote(num_returns=invalid)(_never_execute)
        with pytest.raises((TypeError, ValueError), match="num_returns"):
            fn.options(num_returns=invalid)
    class Actor:
        pass
    with pytest.raises(TypeError, match="remote functions.*Actor"):
        ray.remote(num_returns=1)(Actor)
    with pytest.raises(TypeError, match="remote functions.*Actor"):
        ray.remote(Actor).options(num_returns=1)


def test_public_scalar_and_tuple_each_return_one_ref_and_wait_accepts_multiple_refs(monkeypatch):
    with _core_case() as (core, refs):
        monkeypatch.setattr(api, "_active_core_worker", lambda: core)
        scalar = ray.remote(_never_execute).remote()
        sequence = ray.remote(num_returns=1)(_never_execute).remote()
        refs.extend((scalar, sequence))
        assert all(type(ref) is ObjectRef and ref.object_id.return_index == 0 for ref in refs)
        first, second = _take(core)
        assert first.output_ids == (scalar.object_id,) and second.output_ids == (sequence.object_id,)
        assert (first.task_id, second.task_id) == tuple(
            TaskID.derive(core.job_id, core.driver_task_id, index) for index in (0, 1)
        )
        assert core._accepted_task_count == 2 and core._submission_index == 2
        assert ray.wait(refs, num_returns=2, timeout=0) == ([], refs)
        runtime = PureOutputRuntime(core)
        core._rpc = runtime.rpc
        for pending, value in ((first, 7), (second, (11, 22, 33))):
            reply = _inline_reply(core, runtime, pending, value)
            assert len(reply.results) == 1
            assert core._publish_reply(pending, reply, expected_node_id=core.node_id)
            assert core._finish_pending_task(pending)
            assert _take(core) == ()
            if pending is first:
                assert ray.wait(refs, num_returns=2, timeout=0) == ([scalar], [sequence])
        assert ray.get(scalar) == 7 and ray.get(sequence) == (11, 22, 33)
        assert ray.wait(refs, num_returns=2, timeout=0) == (refs, [])
        assert ray.wait((sequence, scalar), num_returns=1, timeout=0) == ([sequence], [scalar])
        for ref in refs:
            _release(ref)
        core._reference_mailbox.drain()
        runtime.assert_collected()
        assert not core._objects and not core._task_finish_barriers and core._accepted_task_count == 0


def test_submission_registers_one_entry_waiter_ref_and_task_lineage():
    with _core_case() as (core, refs):
        pending, ref = _register(core)
        refs.append(ref)
        assert pending.output_ids == pending.spec.return_ids() == (ref.object_id,)
        assert pending.task_key == pending.task_id
        assert set(core._objects) == {ref.object_id}
        owner = core.owner_table.snapshot(ref.object_id)
        assert owner.state is ObjectState.PENDING and owner.producer_task_spec == pending.spec
        assert owner.current_attempt == pending.spec.attempt_id
        assert owner.local_tokens == frozenset({ref._local_token})
        assert ref._finalizer.alive and not ref._release_done.is_set()
        assert not core._objects[ref.object_id].event.is_set()
        assert core._recovery.lineage_for_object(ref.object_id).output_ids == pending.output_ids
        assert core._accepted_task_count == 1 and core._inflight_submissions == 0
        assert core._task_finish_barriers == {ref.object_id: pending}
        assert not core._protocol_unresolved and not core._reference_mailbox.releases
        _finish_unleased(core, pending, ref)


@pytest.mark.parametrize("stored", (False, True))
def test_single_value_success_commits_owner_and_route_before_wake(monkeypatch, stored):
    with _core_case() as (core, refs):
        backend = _bind_backend(core)
        pending, ref = _register(core)
        refs.append(ref)
        wakes = []
        original = core._wake_object
        def wake(object_id):
            owner = core.owner_table.snapshot(object_id)
            assert owner.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
            assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
            if stored:
                assert core._stored_descriptors[object_id] == owner.canonical_stored_result
            assert not core._objects[object_id].event.is_set()
            wakes.append(object_id)
            original(object_id)
        monkeypatch.setattr(core, "_wake_object", wake)
        results = backend.succeed(pending, stored=stored, value=(11, 22, 33))
        payload = backend.store.get(ref.object_id) if stored else results[0].inline_data
        assert cloudpickle.loads(payload) == (11, 22, 33) and wakes == [ref.object_id]
        _finish_and_collect(core, pending, ref)
        assert backend.store.used_bytes == 0 and not core._stored_descriptors


@pytest.mark.parametrize("field", ("object_id", "owner_worker_id", "node_id", "size_bytes", "checksum"))
def test_core_stored_replay_drift_cannot_overwrite_canonical_descriptor(monkeypatch, field):
    fixture, node, core, pending, reply, calls, _rpc = _publication(refs=False)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id)
        result, = reply.results
        before, physical = _facts(core, pending), (fixture.store.get(pending.object_id), fixture.journal.snapshot(fixture.id))
        changed = replace(result, **{field: (
            ObjectID.for_task(TaskID.random()) if field == "object_id" else WorkerID.random() if field == "owner_worker_id"
            else NodeID.random() if field == "node_id" else 99 if field == "size_bytes"
            else hashlib.sha256(b"changed").hexdigest()
        )})
        with pytest.raises(ProtocolError):
            replace(reply, results=(changed,))
        damaged = deepcopy(reply)
        object.__setattr__(damaged, "results", (changed,))
        preflights = []
        actual = core._preflight_stored_result_replays
        def preflight(selected, descriptors):
            assert selected is pending and descriptors == (changed,)
            preflights.append(descriptors)
            return actual(selected, descriptors)
        with monkeypatch.context() as patch:
            patch.setattr(core, "_preflight_stored_result_replays", preflight)
            with pytest.raises((SystemTaskError, ProtocolError)):
                core._decode_reply(pending, damaged)
        assert len(preflights) == int(field in ("node_id", "size_bytes", "checksum"))
        before_calls = tuple(calls)
        with pytest.raises(ProtocolError):
            core._publish_reply(pending, damaged, expected_node_id=node.node_id)
        assert _facts(core, pending) == before and tuple(calls) == before_calls
        assert (fixture.store.get(pending.object_id), fixture.journal.snapshot(fixture.id)) == physical
        assert core._stored_descriptors == {pending.object_id: result}
        assert core._finish_pending_task(pending)
        assert core.owner_table.release_local_reference(pending.object_id, "outer0")
        core._reference_released(pending.object_id)
        fixture.assert_no_pins_or_bytes()
    finally:
        _close(core)


@pytest.mark.parametrize("mode", ("missing", "duplicate", "wrong-object"))
def test_invalid_success_manifest_cannot_publish_the_output(mode):
    fixture, node, core, pending, reply, calls, _rpc = _publication(refs=False)
    try:
        invalid = () if mode == "missing" else reply.results * 2 if mode == "duplicate" else (
            replace(reply.results[0], object_id=ObjectID.for_task(TaskID.random())),
        )
        before, physical = _facts(core, pending), (fixture.journal.snapshot(fixture.id), fixture.store.get(pending.object_id))
        with pytest.raises(ProtocolError):
            replace(reply, results=invalid)
        damaged = deepcopy(reply)
        object.__setattr__(damaged, "results", invalid)
        with pytest.raises(ProtocolError):
            core._publish_reply(pending, damaged, expected_node_id=node.node_id)
        with pytest.raises(SystemTaskError, match="manifest must exactly match"):
            core._decode_reply(pending, damaged, expected_node_id=node.node_id)
        assert _facts(core, pending) == before and not calls
        assert (fixture.journal.snapshot(fixture.id), fixture.store.get(pending.object_id)) == physical
        assert core.owner_table.output_owner_publication_receipt(OutputOwnerPublicationPlan(pending.execution, reply.output_publication)) is None
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id)
        assert core._finish_pending_task(pending)
    finally:
        _close(core)


def test_application_error_publishes_one_terminal_error_and_wakes():
    with _core_case() as (core, refs):
        pending, ref = _register(core)
        refs.append(ref)
        error = TaskError("user application failed")
        assert core._publish_task_error(pending, error, failure_kind=FailureKind.APPLICATION)
        owner = core.owner_table.snapshot(ref.object_id)
        assert owner.state is ObjectState.ERROR and owner.error is error
        assert core._objects[ref.object_id].event.is_set()
        assert core._recovery.task_record(pending.task_id).state is TaskState.APPLICATION_FAILED
        _finish_and_collect(core, pending, ref)


def test_system_retry_advances_one_output_once_and_preserves_task_key():
    with _core_case() as (core, refs):
        pending, ref = _register(core)
        refs.append(ref)
        token = core.owner_table.snapshot(ref.object_id).local_tokens
        assert not core._retry_system_failure(pending, RuntimeError("retry"))
        retried, = _take(core)
        assert retried.task_key == pending.task_key == pending.task_id
        assert retried.output_ids == pending.output_ids == (ref.object_id,)
        assert retried.spec.attempt_id == pending.spec.attempt_id.next()
        assert retried.dependency_hold == pending.dependency_hold
        owner = core.owner_table.snapshot(ref.object_id)
        assert owner.current_attempt == retried.spec.attempt_id and owner.state is ObjectState.PENDING
        assert owner.local_tokens == token
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.RETRY_PENDING and record.retries_started == 1
        assert str(record.last_error) == "retry" and core._recovery.active_recovery(pending.task_id) is None
        assert core._accepted_task_count == 1 and core._task_finish_barriers == {ref.object_id: retried}
        before = _facts(core, retried)
        assert not core._finish_pending_task(pending)
        assert core._retry_system_failure(pending, RuntimeError("retry"))
        assert _facts(core, retried) == before
        _finish_unleased(core, retried, ref)


def test_retry_owner_preflight_failure_consumes_no_recovery_budget(monkeypatch):
    with _core_case() as (core, refs):
        pending, ref = _register(core)
        refs.append(ref)
        before, calls = _facts(core, pending), []
        actual = core.owner_table.validate_advance_task_outputs
        def fail(execution, attempt):
            assert execution == pending.execution and attempt == pending.spec.attempt_id.next()
            calls.append(actual(execution, attempt))
            assert _facts(core, pending) == before
            raise RuntimeError("owner preflight")
        with monkeypatch.context() as patch:
            patch.setattr(core.owner_table, "validate_advance_task_outputs", fail)
            with pytest.raises(RuntimeError, match="owner preflight"):
                core._retry_system_failure(pending, SystemTaskError("retry"))
        assert len(calls) == 1 and _facts(core, pending) == before
        assert ref._finalizer.alive and not ref._release_done.is_set()
        _finish_unleased(core, pending, ref)


@pytest.mark.parametrize("status", ("success", "application", "terminal"))
def test_owner_terminal_preflight_failure_does_not_mutate_owner_or_recovery(monkeypatch, status):
    if status == "success":
        fixture, node, core, pending, reply, calls, _rpc = _publication(refs=False)
        try:
            before = (core.owner_table.snapshot(pending.object_id), replace(core._recovery.task_record(pending.task_id)), dict(core._stored_descriptors))
            physical = (fixture.journal.snapshot(fixture.id), fixture.store.get(pending.object_id))
            actual, preflights = core.owner_table.validate_output_publication, []
            def fail(plan):
                preflights.append(actual(plan))
                raise RuntimeError("owner preflight")
            with monkeypatch.context() as patch:
                patch.setattr(core.owner_table, "validate_output_publication", fail)
                assert not core._publish_reply(pending, reply, expected_node_id=node.node_id)
            assert len(preflights) == 1 and not calls
            assert (core.owner_table.snapshot(pending.object_id), replace(core._recovery.task_record(pending.task_id)), dict(core._stored_descriptors)) == before
            assert (fixture.journal.snapshot(fixture.id), fixture.store.get(pending.object_id)) == physical
            assert not core._objects[pending.object_id].event.is_set()
            assert not core._finish_pending_task(pending)
            obligation = _take_adoption(core)
            assert obligation.envelope == reply.output_publication and obligation.round == 1
            assert core._execute(pending, pending.spec, output_adoption=obligation)
            assert core._finish_pending_task(pending)
        finally:
            _close(core)
        return
    with _core_case() as (core, refs):
        pending, ref = _register(core)
        refs.append(ref)
        before = _facts(core, pending)
        error = TaskError("user") if status == "application" else SystemTaskError("terminal")
        kind = FailureKind.APPLICATION if status == "application" else None
        actual, preflights = core.owner_table.validate_publish_task_error, []
        def fail(execution, candidate):
            assert execution == pending.execution and candidate is error
            preflights.append(actual(execution, candidate))
            assert _facts(core, pending) == before
            raise RuntimeError("owner preflight")
        with monkeypatch.context() as patch:
            patch.setattr(core.owner_table, "validate_publish_task_error", fail)
            assert not core._publish_task_error(pending, error, failure_kind=kind)
        assert len(preflights) == 1 and _facts(core, pending) == before
        assert core._publish_task_error(pending, error, failure_kind=kind)
        assert core.owner_table.snapshot(ref.object_id).error is error
        assert core._recovery.task_record(pending.task_id).state is (TaskState.APPLICATION_FAILED if kind else TaskState.SYSTEM_FAILED)
        _finish_and_collect(core, pending, ref)


def test_task_lifecycle_tables_use_task_id_and_exact_output_barrier():
    with _core_case() as (core, refs):
        pending, ref = _register(core)
        refs.append(ref)
        identity = protocol.PushTask(LeaseID.random(), WorkerID.random(), pending.spec)
        runtime = PureOutputRuntime(core)
        core._rpc = runtime.rpc
        reply = runtime.complete(identity, (7,))
        core._mark_protocol_unresolved(pending, "test", output_candidate=reply.output_publication.publication_id)
        assert set(core._protocol_unresolved) == {pending.task_id} and ref.object_id not in core._protocol_unresolved
        assert core._clear_protocol_unresolved(pending)
        assert core._publish_reply(pending, reply, expected_node_id=core.node_id)
        _finish_and_collect(core, pending, ref)
        assert pending.task_id in core._finished_tasks and ref.object_id not in core._finished_tasks
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        runtime.assert_collected()


@pytest.mark.parametrize("producer_closed_first", (False, True))
def test_dependency_lineage_releases_only_after_final_output_handle(producer_closed_first):
    with _core_case() as (core, refs):
        backend = _bind_backend(core)
        producer, dependency = _register(core)
        consumer, result = _register(core, dependency)
        second_handle = core._new_object_ref(result.object_id)
        refs.extend((dependency, result, second_handle))
        edges = core.owner_table.task_lineage_edges(consumer.task_id)
        assert len(edges) == 1
        edge, = edges
        assert edge.dependency_object_id == producer.object_id
        backend.succeed(producer, stored=False, value=1)
        backend.succeed(consumer, stored=False, value=2)
        assert core._finish_pending_task(producer) and core._finish_pending_task(consumer)
        assert _take(core) == ()
        if producer_closed_first:
            _release(dependency)
        _release(result)
        core._reference_mailbox.drain()
        assert core.owner_table.contains(consumer.object_id)
        assert core.owner_table.task_lineage_edges(consumer.task_id) == edges
        assert edge.token in core.owner_table.snapshot(producer.object_id).lineage_tokens
        _release(second_handle)
        _release(second_handle)
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.task_lineage_edges(consumer.task_id)
        if not producer_closed_first:
            assert not core.owner_table.snapshot(producer.object_id).lineage_tokens
            _release(dependency)
            core._reference_mailbox.drain()
        assert core.owner_table.collection_state(producer.object_id) is ObjectCollectionState.COLLECTED
        assert not core._objects and not core._object_gc_obligations


def test_single_output_contained_refs_require_current_publication_and_actual_gc():
    fixture, node, core, pending, reply, _calls, _rpc = _publication(refs=True)
    try:
        before = core.owner_table.snapshot(pending.object_id)
        with pytest.raises(TypeError, match="contained_edges"):
            protocol.TaskReply(
                reply.task_id, reply.attempt_id, reply.worker_id, reply.status, reply.results,
                contained_edges=tuple(fixture.manifest.slots[0].edges),
            )
        with pytest.raises(SystemTaskError, match="single-output"):
            core._publish_reply(pending, replace(reply, output_publication=None), expected_node_id=node.node_id)
        assert core.owner_table.snapshot(pending.object_id) == before
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id)
        assert len(core.owner_table.snapshot(pending.object_id).outgoing_contained_edges) == 2
        assert core._finish_pending_task(pending)
        assert core.owner_table.release_local_reference(pending.object_id, "outer0")
        core._reference_released(pending.object_id)
        fixture.assert_no_pins_or_bytes()
        assert not core.owner_table.contains(pending.object_id)
    finally:
        _close(core)


def test_pinned_stored_output_keeps_task_lineage_until_exact_drop_replay(monkeypatch):
    with _core_case() as (core, refs):
        backend = _bind_backend(core)
        producer, dependency = _register(core)
        consumer, result = _register(core, dependency)
        refs.extend((dependency, result))
        edges = core.owner_table.task_lineage_edges(consumer.task_id)
        backend.succeed(producer, stored=False, value=1)
        backend.succeed(consumer, stored=True, value=2)
        assert core._finish_pending_task(producer) and core._finish_pending_task(consumer)
        assert _take(core) == ()
        # Deliver publication/finish notices while both actual handles are
        # live. They cannot claim collection; the close below is a new event.
        core._reference_mailbox.drain()
        assert core._reference_mailbox.pending.unfinished_tasks == 0
        assert not core._object_gc_obligations
        scheduled, drops = [], []
        rpc = core._rpc
        def observe(address, handler, request):
            reply = rpc(address, handler, request)
            if handler == "drop_object_replica":
                drops.append((request, reply))
            return reply
        def schedule(mailbox, event, delay):
            assert mailbox is core._reference_mailbox and 0 < delay <= 0.25
            obligation = core._object_gc_obligations[event.object_id]
            owner = core.owner_table.snapshot(event.object_id)
            assert obligation.retry_scheduled
            assert obligation.plan.producer_attempt_id == owner.current_attempt
            scheduled.append((mailbox, event, owner.current_attempt, obligation, obligation.plan, obligation.retry_round))
        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core, "_schedule_reference_event", schedule)
        pin = backend.store.pin(result.object_id, ("physical-reader", result.object_id))
        _release(result)
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(result.object_id) is ObjectCollectionState.COLLECTING
        assert drops[-1][1].status is protocol.DropObjectReplicaStatus.PINNED
        obligation = core._object_gc_obligations[result.object_id]
        plan = obligation.plan
        assert set(plan.lineage_releases) == set(edges)
        assert core.owner_table.task_lineage_edges(consumer.task_id) == edges
        assert all(edge.token in core.owner_table.snapshot(producer.object_id).lineage_tokens for edge in edges)
        tickets = [ticket for ticket in scheduled if ticket[1].object_id == result.object_id]
        assert len(tickets) == 1 and len(scheduled) == len(tickets)
        assert tickets[0][2:] == (consumer.spec.attempt_id, obligation, plan, 1)
        assert obligation.retry_scheduled and obligation.retry_round == 1
        assert backend.store.snapshot(result.object_id).pin_count == 1
        assert len(drops) == 1 and drops[0][0] in obligation.pending_drops.values()
        assert backend.store.unpin(result.object_id, pin)
        core._reference_released(result.object_id)
        assert len(drops) == 2 and drops[0][0] == drops[1][0]
        assert drops[1][1].status is protocol.DropObjectReplicaStatus.DROPPED
        assert core.owner_table.collection_state(result.object_id) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.task_lineage_edges(consumer.task_id)
        before = tuple(drops)
        for mailbox, event, epoch, old_obligation, old_plan, retry_round in scheduled:
            assert epoch == consumer.spec.attempt_id and old_obligation is obligation
            assert old_plan == plan and retry_round == 1
            assert mailbox.enqueue_internal(event)
        core._reference_mailbox.drain()
        assert tuple(drops) == before and not core._object_gc_obligations
        _release(dependency)
        core._reference_mailbox.drain()
        assert not core._objects and backend.store.used_bytes == 0
