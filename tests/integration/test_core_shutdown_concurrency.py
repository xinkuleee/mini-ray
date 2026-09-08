"""Opt-in L1 shutdown races with bounded threads and no transport.

Each exact case starts one real Core (one coordinator, one dispatcher, one
reference thread), at most one daemon request thread, and two tiny inline
objects.  No Node, Worker, process, listener, Actor, or real RPC is started.
Send/grant gates time out after 2 s; request joins share a 2 s deadline; finally
releases gates before bounded joins and structurally stops residual threads
without rewriting accepted counts or owner/recovery authority.
Run one reviewed exact node ID through scripts/run_bounded_test.py.
"""
from __future__ import annotations

import hashlib
import queue
import socket
import threading
import time
from contextlib import contextmanager

import cloudpickle
import pytest

from miniray import core as core_module, protocol
from miniray.core import CoreWorker, _LeaseRequestState, _PushRequestState
from miniray.ids import LeaseID, NodeID, WorkerID
from miniray.ownership import ObjectState
from miniray.resources import AllocationToken, ResourceVector
from miniray.trace import EventSink
from miniray.transport import RemoteCallError
from tests.integration.test_core_dispatch_concurrency import (
    _runtime_threads, _stop_owned_threads,
)

pytestmark = pytest.mark.loopback_smoke


@pytest.fixture(autouse=True)
def _no_real_transport(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("shutdown L1 fixture attempted real transport/timer")

    for name in ("_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline",
                 "_push_task_rpc", "_actor_call_rpc"):
        monkeypatch.setattr(CoreWorker, name, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _close_ref_until(ref, deadline):
    if ref is None or ref.closed:
        return
    assert ref.borrower_token is None and ref._finalizer is not None
    ref._closed = True
    ref._finalizer()
    done = ref._release_done
    assert done is not None
    assert done.wait(max(0.0, deadline - time.monotonic())), "reference release timed out"


@contextmanager
def _live_case(*, dependency=False):
    core = object.__new__(CoreWorker)
    initialized = False
    refs, gates, threads = [], [], []
    pendings = []
    try:
        # Retain the partial object before __init__ can start any owned thread.
        CoreWorker.__init__(core, ("node.invalid", 1), NodeID.random(),
                            event_sink=EventSink(), dispatch_lanes=1)
        initialized = True
        assert len(_runtime_threads(core)) == 3
        dependency_ref = None
        if dependency:
            producer, dependency_ref = core._register_submission(
                core.define_remote_function(lambda: 1), (), {}, ResourceVector()
            )
            refs.append(dependency_ref)
            pendings.append(producer)
            assert core.owner_table.publish_inline(
                producer.object_id, producer.spec.attempt_id, cloudpickle.dumps(1)
            )
            core._recovery.record_task_success(producer.task_id, producer.spec.attempt_id)
            core._objects[producer.object_id].event.set()
        pending, ref = core._register_submission(
            core.define_remote_function((lambda value: value) if dependency else (lambda: 7)),
            (dependency_ref,) if dependency else (), {}, ResourceVector(),
        )
        refs.append(ref)
        pendings.append(pending)
        with core._state_lock:
            core._accepted_task_count = 1
            core._install_task_finish_barrier_locked(pending)
        prepared, descriptors, _ = core._prepare_task_dependencies(pending.spec)
        assert descriptors == ()
        grant = protocol.GrantWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, core.node_id,
            WorkerID.random(), ("worker.invalid", 1), AllocationToken("shutdown-l1"),
        )
        state = _PushRequestState(protocol.PushTask(grant.lease_id, grant.worker_id, prepared),
                                  grant, core.node_address, grant.worker_address, 1, True)
        yield core, dependency_ref, pending, ref, state, gates, threads
    finally:
        for gate in gates:
            gate.set()
        deadline = time.monotonic() + 2.0
        for thread in threads:
            if thread.ident is not None:
                thread.join(max(0.0, deadline - time.monotonic()))
        live = tuple(thread.name for thread in threads if thread.is_alive())
        if not live:
            try:
                if initialized:
                    for pending in reversed(pendings):
                        core._clear_protocol_unresolved(pending)
                        if core.owner_table.contains(pending.object_id):
                            snapshot = core.owner_table.snapshot(pending.object_id)
                            if snapshot.state is ObjectState.PENDING:
                                core._publish_task_error(pending, RuntimeError("L1 fixture cleanup"))
                    if pendings:
                        core._finish_pending_task(pendings[-1])
                    deadline = time.monotonic() + 2.0
                    for ref in reversed(refs):
                        _close_ref_until(ref, deadline)
                    core.shutdown(timeout=1.0)
                else:
                    core._abort_unpublished_startup(timeout=1.0)
            finally:
                # Failure-only signals/joins never reset accepted-task counts
                # or erase an unresolved owner/recovery state to fake success.
                _stop_owned_threads(core, time.monotonic() + 1.0)
                assert all(not thread.is_alive() for thread in _runtime_threads(core))
        else:
            # Avoid taking a potentially stuck Core lock in failed teardown.
            # The outer bounded runner owns final process termination.
            for ref in refs:
                if ref._finalizer is not None:
                    ref._finalizer.detach()
        assert not live, "bounded request threads did not stop: {!r}".format(live)


def _success(core, pending, worker_id):
    payload = cloudpickle.dumps(7)
    result = protocol.ResultDescriptor(pending.object_id, protocol.ResultStorage.INLINE,
        len(payload), core.worker_id, core.node_id, hashlib.sha256(payload).hexdigest(), payload)
    return protocol.TaskReply(pending.task_id, pending.spec.attempt_id, worker_id,
                              protocol.TaskReplyStatus.SUCCEEDED, (result,))


def _run_request(threads, operation):
    outcomes = queue.Queue()

    def invoke():
        try:
            outcomes.put_nowait(operation())
        except BaseException as exc:
            outcomes.put_nowait(exc)

    thread = threading.Thread(target=invoke, daemon=True, name="miniray-test-shutdown-request")
    threads.append(thread)  # attempted start is already in the cleanup ledger
    thread.start()
    return thread, outcomes


def _assert_request_finished(thread, outcomes):
    thread.join(timeout=2.0)
    assert not thread.is_alive(), "request exceeded two-second join"
    result = outcomes.get_nowait()
    assert result is True, result
    assert outcomes.empty()


def _assert_live_after_short_shutdown(core, pending):
    assert not core.shutdown(timeout=0.01)
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
    assert pending.task_key in core._protocol_unresolved
    assert core.dispatcher_alive and core._reference_thread.is_alive()
    assert not core._sink_closed


def _close_and_shutdown(core, pending, *refs):
    assert pending.task_key not in core._protocol_unresolved
    assert core._finish_pending_task(pending)
    deadline = time.monotonic() + 2.0
    for ref in refs:
        _close_ref_until(ref, deadline)
    assert core.shutdown(timeout=1.0)
    assert all(not thread.is_alive() for thread in _runtime_threads(core))


def test_short_shutdown_keeps_runtime_alive_until_ambiguous_push_resolves(monkeypatch):
    with _live_case(dependency=True) as (core, dependency, pending, ref, state, _gates, _threads):
        outcomes = [RemoteCallError("push_task", "RuntimeError", "late reply", ""),
                    _success(core, pending, state.push.worker_id)]
        queries, sends, delayed = [], [], []
        real_enqueue = core._submissions.put

        def controlled_enqueue(item, *args, **kwargs):
            # Retain the one real scheduler output, but let this test choose
            # its replay point instead of racing an autonomous delayed retry.
            if isinstance(item, core_module._DelayedReadyTask):
                assert not delayed, "ambiguous retry scheduled more than once"
                delayed.append(item)
                return
            return real_enqueue(item, *args, **kwargs)

        def push(address, handler, message):
            assert (address, handler) == (state.worker_address, "push_task")
            assert message is state.push and outcomes
            sends.append(message)
            value = outcomes.pop(0)
            if isinstance(value, BaseException):
                raise value
            return value

        def query(address, handler, message):
            assert (address, handler) == (core.node_address, "get_worker_lease_outcome")
            assert type(message) is protocol.GetWorkerLeaseOutcome and not queries
            assert message == protocol.GetWorkerLeaseOutcome(
                state.grant.lease_id, pending.task_id, pending.spec.attempt_id,
                state.grant.worker_id, core.worker_id, pending.output_ids,
            )
            queries.append(message)
            return protocol.GetWorkerLeaseOutcomeReply(
                message.lease_id, message.task_id, message.attempt_id, message.executor_worker_id,
                message.owner_worker_id, message.object_ids, core.node_id, True, True,
                protocol.LeaseExecutionState.RUNNING,
            )

        monkeypatch.setattr(core._submissions, "put", controlled_enqueue)
        monkeypatch.setattr(core, "_push_task_rpc", push)
        monkeypatch.setattr(core, "_rpc", query)
        assert not core._replay_push(pending, state)
        assert len(delayed) == 1 and delayed[0].ready.push_state.push is state.push
        _assert_live_after_short_shutdown(core, pending)
        assert pending.dependency_hold in core.owner_table.snapshot(dependency.object_id).submitted_tokens
        assert core._replay_push(pending, state)
        assert core.get(ref, timeout=0) == 7
        _close_and_shutdown(core, pending, ref, dependency)
        assert len(sends) == 2 and len(queries) == 1 and not outcomes


def test_push_is_marked_unresolved_before_send_can_race_shutdown(monkeypatch):
    with _live_case() as (core, _dependency, pending, ref, state, gates, threads):
        entered, release = threading.Event(), threading.Event()
        gates.append(release)
        request = protocol.RequestWorkerLease(
            state.grant.lease_id, pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, preferred_node_id=core.node_id, return_ids=pending.output_ids,
        )
        lease_state = _LeaseRequestState(request, core.node_address, core.node_id, True)
        leases, sends = [], []

        def lease(actual):
            assert actual == lease_state and not leases
            leases.append(actual)
            return state.grant

        def push(address, handler, message):
            assert (address, handler) == (state.worker_address, "push_task")
            assert type(message) is protocol.PushTask and not sends
            assert message.lease_id == state.grant.lease_id
            assert pending.task_key in core._protocol_unresolved
            sends.append(message)
            entered.set()
            assert release.wait(2.0), "send gate timed out"
            return _success(core, pending, message.worker_id)

        monkeypatch.setattr(core, "_request_lease_hop", lease)
        monkeypatch.setattr(core, "_push_task_rpc", push)
        thread, results = _run_request(threads, lambda: core._execute(
            pending, pending.spec, lease_state=lease_state
        ))
        assert entered.wait(2.0), "send did not reach its gate"
        _assert_live_after_short_shutdown(core, pending)
        release.set()
        _assert_request_finished(thread, results)
        _close_and_shutdown(core, pending, ref)
        assert len(leases) == len(sends) == 1


def test_lease_is_marked_unresolved_before_grant_reply_races_shutdown(monkeypatch):
    with _live_case() as (core, _dependency, pending, ref, state, gates, threads):
        entered, release = threading.Event(), threading.Event()
        gates.append(release)
        request = protocol.RequestWorkerLease(
            state.grant.lease_id, pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, preferred_node_id=core.node_id, return_ids=pending.output_ids,
        )
        lease_state = _LeaseRequestState(request, core.node_address, core.node_id, True)
        leases, sends = [], []

        def lease(actual):
            assert actual == lease_state and not leases
            assert pending.task_key in core._protocol_unresolved
            leases.append(actual)
            entered.set()
            assert release.wait(2.0), "grant reply gate timed out"
            return state.grant

        def push(address, handler, message):
            assert (address, handler) == (state.worker_address, "push_task")
            assert type(message) is protocol.PushTask and not sends
            assert message.lease_id == request.lease_id
            sends.append(message)
            return _success(core, pending, message.worker_id)

        monkeypatch.setattr(core, "_request_lease_hop", lease)
        monkeypatch.setattr(core, "_push_task_rpc", push)
        thread, results = _run_request(threads, lambda: core._execute(
            pending, pending.spec, lease_state=lease_state
        ))
        assert entered.wait(2.0), "grant did not reach its gate"
        _assert_live_after_short_shutdown(core, pending)
        assert not sends
        release.set()
        _assert_request_finished(thread, results)
        _close_and_shutdown(core, pending, ref)
        assert len(leases) == len(sends) == 1
