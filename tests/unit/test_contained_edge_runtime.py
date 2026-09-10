"""Current owner-led contained result lifetimes and one bounded L1 re-entry.

Each pure case uses one accepted Task, one child put, real Node/owner/Store
authority and a manual reference FIFO. No graph or slot-collected RPC exists.
The L1 case keeps one real reference thread and bounded joins, never disguises
that execution as unit. A close receipt does not prove remote or physical GC.
"""
from copy import deepcopy
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, node as node_module, protocol
from miniray.core import CoreWorker, _PendingTask, _RetryInlineGc, _WAKE_COORDINATOR
from miniray.node import NodeServer
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState
from tests.support._contained_output import ContainedOutput
from tests.unit._pure_core import close_pure_core


@pytest.fixture
def _no_edge_retry_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('pure contained lifetime attempted runtime')
    for kind, name in ((CoreWorker,'__init__'),(NodeServer,'__init__'),(threading.Thread,'start'),
                       (threading.Thread,'join'),(threading.Timer,'start'),(threading.Condition,'wait'),
                       (multiprocessing.process.BaseProcess,'start')):
        monkeypatch.setattr(kind,name,forbidden)
    for name in ('socket','socketpair','create_connection'):
        monkeypatch.setattr(socket,name,forbidden)
    monkeypatch.setattr(subprocess,'Popen',forbidden)
    monkeypatch.setattr(time,'sleep',forbidden)
    monkeypatch.setattr(core_module,'rpc_request',forbidden)
    monkeypatch.setattr(node_module,'rpc_request',forbidden)
    def receipt(event, timeout=None):
        assert event.is_set(), 'pure reference wait was not already acknowledged'
        return True
    monkeypatch.setattr(threading.Event,'wait',receipt)


def _edge_release_local(ref):
    if ref is not None:
        done=ref._release_done
        ref.close(timeout=0)
        assert ref.closed and done.is_set()


def _edge_take_submissions(core):
    work=[]
    assert core._submissions.qsize()<=16
    for _ in range(core._submissions.qsize()):
        item=core._submissions.get_nowait();core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            assert isinstance(item,_PendingTask)
            work.append(item)
    return tuple(work)


def _edge_drain_notices(core, object_id=None):
    assert core._reference_mailbox.pending.qsize()<=16
    core._reference_mailbox.drain()


class _EdgeRetryFixture(ContainedOutput):
    def __init__(self, monkeypatch, *, fail_first_release=True):
        super().__init__()
        # Multiple already-queued GC notices may run in one manual drain.
        # Availability is an explicit test phase, never a one-shot exception
        # which silently lets the next notice acknowledge the same hold.
        self.allow_release=not fail_first_release
        self.delayed=[]
        def schedule(mailbox,event,delay):
            assert mailbox is self.core._reference_mailbox
            assert type(event) is _RetryInlineGc and event.object_id==self.ref.object_id
            assert len(self.delayed)<4 and 0<delay<=0.25
            self.delayed.append((mailbox,event,delay))
        monkeypatch.setattr(self.core,'_schedule_reference_event',schedule)
    def release_child(self,address,handler,request):
        assert address==self.child_owner.owner_address and handler=='release_contained_reference'
        assert request.object_id==self.child_id and request.hold==self.transfer.final_hold
        assert request.owner_worker_id==self.child_owner.worker_id
        self.release_calls.append(request)
        assert len(self.release_calls)<=4
        if not self.allow_release:
            assert request.hold in self.child_owner.owner_table.snapshot(self.child_id).contained_holds
            raise RuntimeError('child release unavailable until explicit test phase')
        reply=self.child_owner.release_contained_reference(request)
        assert reply.accepted and reply.object_id==request.object_id and reply.hold==request.hold
        self.released.append(reply)
        return reply
    def register(self):
        super().register()
        assert _edge_take_submissions(self.core)==(self.pending,)
        assert _edge_take_submissions(self.child_owner)==()
    def complete(self):
        reply=super().complete()
        _edge_release_local(self.child_ref)
        _edge_drain_notices(self.child_owner)
        return reply
    def close(self):
        _edge_release_local(self.ref);_edge_release_local(self.child_ref)
        close_pure_core(self.core);close_pure_core(self.child_owner)
    def adopt_finish(self):
        reply=self.complete()
        assert self.core._publish_reply(self.pending,reply,expected_node_id=self.node.node_id,expected_lease_id=self.grant.lease_id)
        assert self.core._finish_pending_task(self.pending)
        _edge_take_submissions(self.core)
        # Publication/finish may enqueue multiple advisory GC checks. Consume
        # them while the outer handle is still a live root: they cannot issue
        # child Release. The later explicit close owns the tested GC round.
        assert not self.ref.closed
        _edge_drain_notices(self.core)
        assert self.core._reference_mailbox.pending.empty()
        assert not self.release_calls and not self.delayed
        return reply
    def assert_collected(self):
        for core,ref in ((self.core,self.ref),(self.child_owner,self.child_ref)):
            assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
            assert not core._object_gc_obligations and not core.owner_table.contains(ref.object_id)
            assert core._recovery.lineage_for_object(ref.object_id) is None
        assert len(self.released)==1 and self.released[0].hold==self.transfer.final_hold
        assert self.node.object_store.used_bytes==0


@pytest.mark.unit
@pytest.mark.usefixtures('_no_edge_retry_runtime')
def test_pending_outer_close_collects_only_after_reply_installs_edge(monkeypatch):
    f=_EdgeRetryFixture(monkeypatch,fail_first_release=False)
    try:
        f.register();p=f.pending;c=f.core
        _edge_release_local(f.ref);_edge_drain_notices(c)
        before=c.owner_table.snapshot(p.object_id)
        assert before.state is ObjectState.PENDING and not before.local_tokens and not before.outgoing_contained_edges
        assert c._task_finish_barriers[p.object_id] is p and not c._object_gc_obligations
        reply=f.complete();observed=[];wake=c._wake_object
        def see_edge(object_id):
            state=c.owner_table.snapshot(object_id)
            assert state.state is ObjectState.READY_INLINE and state.outgoing_contained_edges==frozenset({f.edge})
            assert f.child_owner.owner_table.snapshot(f.child_id).contained_holds==frozenset({f.transfer.final_hold})
            assert not c._objects[object_id].event.is_set()
            observed.append(object_id);wake(object_id)
        monkeypatch.setattr(c,'_wake_object',see_edge)
        assert c._publish_reply(p,reply,expected_node_id=f.node.node_id,expected_lease_id=f.grant.lease_id)
        assert observed==[p.object_id]
        _edge_drain_notices(c)
        assert c.owner_table.contains(p.object_id) and not f.release_calls
        assert c._finish_pending_task(p) and c._finish_pending_task(p)
        _edge_drain_notices(c);f.assert_collected()
        assert c._accepted_task_count==0 and not c._task_finish_barriers
    finally:f.close()


@pytest.mark.unit
@pytest.mark.usefixtures('_no_edge_retry_runtime')
def test_failed_edge_release_freezes_outer_metadata_until_retry_ack(monkeypatch):
    f=_EdgeRetryFixture(monkeypatch)
    try:
        f.register();f.adopt_finish();c=f.core
        _edge_release_local(f.ref);_edge_drain_notices(c)
        obligation=c._object_gc_obligations[f.ref.object_id]
        plan=obligation.plan
        assert obligation.pending_edges=={f.edge} and obligation.retry_scheduled
        assert not obligation.pending_drops and f.node.object_store.used_bytes==0
        assert c.owner_table.collection_state(f.ref.object_id) is ObjectCollectionState.COLLECTING
        assert f.child_owner.owner_table.snapshot(f.child_id).contained_holds==frozenset({f.transfer.final_hold})
        blocked=tuple(f.release_calls)
        scheduled=tuple(f.delayed)
        assert 1<=len(blocked)<=2 and not f.released and scheduled
        assert all(request==blocked[0] for request in blocked)
        assert c._object_gc_obligations[f.ref.object_id] is obligation
        assert c.owner_table.snapshot(f.ref.object_id).collection_plan==plan
        f.allow_release=True
        for mailbox,event,_ in scheduled:
            assert mailbox.enqueue_internal(event)
        _edge_drain_notices(c)
        f.assert_collected()
        assert len(f.release_calls)==len(blocked)+1
        assert all(request==blocked[0] for request in f.release_calls)
        assert tuple(f.delayed)==scheduled
    finally:f.close()


@pytest.mark.unit
@pytest.mark.usefixtures('_no_edge_retry_runtime')
def test_stale_reply_cannot_release_committed_publication_edges(monkeypatch):
    f=_EdgeRetryFixture(monkeypatch,fail_first_release=False)
    try:
        f.register();reply=f.adopt_finish();c=f.core
        _edge_drain_notices(c)
        outer=c.owner_table.snapshot(f.ref.object_id);child=f.child_owner.owner_table.snapshot(f.child_id)
        history=f.journal.snapshot(reply.output_publication.publication_id)
        for duplicate in (reply,deepcopy(reply)):
            assert not c._publish_reply(f.pending,duplicate,expected_node_id=f.node.node_id,expected_lease_id=f.grant.lease_id)
            assert c.owner_table.snapshot(f.ref.object_id)==outer
            assert f.child_owner.owner_table.snapshot(f.child_id)==child
            assert f.journal.snapshot(reply.output_publication.publication_id)==history
            assert not f.release_calls and not c._protocol_unresolved
        _edge_release_local(f.ref);_edge_drain_notices(c);f.assert_collected()
        assert not c._publish_reply(f.pending,reply,expected_node_id=f.node.node_id,expected_lease_id=f.grant.lease_id)
        assert len(f.release_calls)==1
    finally:f.close()


class _OwnerCollectionFixture(ContainedOutput):
    def __init__(self, *, contained):
        super().__init__(same_owner=True,contained=contained,stored=not contained)
        self.lock=self.core._state_lock
        self.releases=[]
        def observe(request,reply):
            assert not self.lock._is_owned()
            self.releases.append((threading.current_thread(),request,reply))
        self.release_observer=observe
    def register(self):
        super().register()
        assert _edge_take_submissions(self.core)==(self.pending,)
    def close_pure(self):
        _edge_release_local(self.ref);_edge_release_local(self.child_ref);close_pure_core(self.core)
    def assert_collected(self):
        assert self.core.owner_table.collection_state(self.ref.object_id) is ObjectCollectionState.COLLECTED
        assert self.core._recovery.lineage_for_object(self.ref.object_id) is None
        if self.child_ref is not None:
            assert self.core.owner_table.collection_state(self.child_ref.object_id) is ObjectCollectionState.COLLECTED
        assert not self.core._object_gc_obligations and not self.core._task_finish_barriers
        assert not self.node.object_store.used_bytes


@pytest.mark.unit
@pytest.mark.usefixtures('_no_edge_retry_runtime')
def test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected():
    f=_OwnerCollectionFixture(contained=False)
    try:
        f.register();c=f.core;p=f.pending
        _edge_release_local(f.ref);_edge_drain_notices(c)
        reply=f.complete()
        assert f.node.object_store.used_bytes>0
        assert c._publish_reply(p,reply,expected_node_id=f.node.node_id,expected_lease_id=f.grant.lease_id)
        _edge_drain_notices(c)
        assert c.owner_table.snapshot(p.object_id).state is ObjectState.READY_STORED
        assert c._task_finish_barriers[p.object_id] is p and not f.drops
        assert c._finish_pending_task(p)
        _edge_drain_notices(c);f.assert_collected()
        assert len(f.drops)==1 and f.drops[0][1].status is protocol.DropObjectReplicaStatus.DROPPED
    finally:f.close_pure()


@pytest.mark.unit
@pytest.mark.usefixtures('_no_edge_retry_runtime')
def test_task_reply_edge_validation_and_worker_commit_names_outer(monkeypatch):
    from dataclasses import replace
    f=_EdgeRetryFixture(monkeypatch,fail_first_release=False)
    try:
        f.register();reply=f.complete()
        assert f.edge.container_object_id==f.pending.object_id and f.edge.contained_object_id==f.child_id
        assert f.transfer.final_hold.container_owner_worker_id==f.core.worker_id
        with pytest.raises(TypeError):replace(reply,contained_edges=(f.edge,))
        with pytest.raises(ValueError):
            replace((reply.output_publication.manifest.value),transfers=(replace(f.transfer,
                final_hold=replace(f.transfer.final_hold,container_object_id=f.child_id)),))
        assert f.core._publish_reply(f.pending,reply,expected_node_id=f.node.node_id,expected_lease_id=f.grant.lease_id)
        assert f.core._finish_pending_task(f.pending)
        _edge_release_local(f.ref);_edge_drain_notices(f.core);f.assert_collected()
    finally:f.close()


@pytest.mark.unit
@pytest.mark.usefixtures('_no_edge_retry_runtime')
def test_shutdown_retries_retained_edge_obligation_before_clean(monkeypatch):
    f=_EdgeRetryFixture(monkeypatch)
    try:
        f.register();f.adopt_finish();c=f.core
        _edge_release_local(f.ref);_edge_drain_notices(c)
        obligation=c._object_gc_obligations[f.ref.object_id]
        plan=obligation.plan
        assert 1<=len(f.release_calls)<=2 and not f.released
        assert obligation.pending_edges=={f.edge} and not obligation.pending_drops
        assert obligation.retry_scheduled and f.node.object_store.used_bytes==0
        assert not c._retry_gc_obligations_for_shutdown()
        assert c._object_gc_obligations[f.ref.object_id] is obligation
        assert obligation.plan==plan and obligation.pending_edges=={f.edge}
        assert c.owner_table.collection_state(f.ref.object_id) is ObjectCollectionState.COLLECTING
        assert f.child_owner.owner_table.snapshot(f.child_id).contained_holds==frozenset({f.transfer.final_hold})
        assert not f.released
        blocked=tuple(f.release_calls)
        scheduled=tuple(f.delayed)
        assert all(request==blocked[0] for request in blocked)
        f.allow_release=True
        assert c._retry_gc_obligations_for_shutdown()
        f.assert_collected()
        assert len(f.release_calls)==len(blocked)+1
        assert all(request==blocked[0] for request in f.release_calls)
        for mailbox,event,_ in scheduled:
            assert mailbox.enqueue_internal(event)
        _edge_drain_notices(c)
        assert c._retry_gc_obligations_for_shutdown() and len(f.release_calls)==len(blocked)+1
        assert tuple(f.delayed)==scheduled
    finally:f.close()


class _SameOwnerReferenceProbe:
    """One real reference consumer, exact same-owner callbacks, bounded joins.

    Core initialization and dispatch are not under test. Only the original
    CoreWorker reference runtime is explicitly installed on the single Core,
    and no synchronous fake mailbox is allowed to perform the tested release.
    The main thread publishes/finishes; the real consumer collects both objects.
    """

    def __init__(self, monkeypatch):
        import math
        from miniray import transport, worker

        self.fixture = None
        self.starting = False
        self.started, self.created, self.joins, self.errors, self.violations, self.gc_calls = [], [], [], [], [], []
        self.observation_lock = threading.Lock()
        self.collected = threading.Event()
        self.baseline = self.runtime_threads()
        real_thread = threading.Thread
        probe = self

        class ReferenceThread(real_thread):
            def __init__(thread, *args, **kwargs):
                super().__init__(*args, **kwargs)
                if not probe.starting or thread.name != "miniray-core-reference-events" or probe.created:
                    probe.forbidden("unexpected reference-thread construction")
                probe.created.append(thread)

            def start(thread):
                if thread not in probe.created or probe.started:
                    probe.forbidden("unexpected reference-thread start")
                super().start()
                probe.started.append(thread)

            def join(thread, timeout=None):
                if (thread not in probe.created or type(timeout) not in (int, float)
                        or not math.isfinite(timeout) or not 0 <= timeout <= 1.0):
                    probe.forbidden("unowned or unbounded reference-thread join")
                probe.joins.append((thread, timeout))
                return super().join(timeout)

            def run(thread):
                try:
                    return super().run()
                except BaseException as exc:
                    probe.errors.append(exc)

        monkeypatch.setattr(threading, "Thread", ReferenceThread)
        for kind in (CoreWorker, NodeServer, worker.WorkerServer, transport.TCPServer):
            monkeypatch.setattr(kind, "__init__", self.forbidden)
        for name in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, name, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", self.forbidden)
        monkeypatch.setattr(threading.Timer, "__init__", self.forbidden)
        monkeypatch.setattr(queue.Queue, "join", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        for module in (core_module, node_module, worker):
            monkeypatch.setattr(module, "rpc_request", self.forbidden)
        monkeypatch.setattr(transport, "request", self.forbidden)

    @staticmethod
    def runtime_threads():
        return {thread for thread in threading.enumerate() if thread.name.startswith("miniray-core-")}

    def forbidden(self, *args, **kwargs):
        with self.observation_lock:
            self.violations.append((threading.current_thread(), args, kwargs))
        raise AssertionError("same-owner reference L1 attempted an unmodelled effect")

    def start(self, monkeypatch):
        self.fixture = f = _OwnerCollectionFixture(contained=True)
        core = f.core
        original_collector = core._reference_released

        def observe_collection(object_id):
            try:
                result = original_collector(object_id)
                if len(self.gc_calls) >= 12:
                    self.forbidden("same-owner collector observation limit exceeded")
                self.gc_calls.append((threading.current_thread(), object_id))
                # Recursive child checks can precede outer commit. Signal only
                # after both real owner commits, never just a release ACK.
                if (f.pending is not None and f.child_ref is not None
                        and core.owner_table.collection_state(f.pending.object_id) is ObjectCollectionState.COLLECTED
                        and core.owner_table.collection_state(f.child_ref.object_id) is ObjectCollectionState.COLLECTED):
                    self.collected.set()
                return result
            except BaseException as exc:
                # The production event loop intentionally continues after GC
                # callbacks. Preserve a test failure before it can be swallowed.
                with self.observation_lock:
                    if len(self.errors) < 12:
                        self.errors.append(exc)
                raise

        monkeypatch.setattr(core, "_reference_released", observe_collection)
        monkeypatch.setattr(core, "_execute", self.forbidden)
        monkeypatch.setattr(core, "_push_task_rpc", self.forbidden)
        monkeypatch.setattr(core, "_schedule_reference_event", self.forbidden)
        self.starting = True
        try:
            # Unbound actual initializer, not make_pure_core's tripwire. It
            # replaces only this Core's unused synchronous reference mailbox.
            CoreWorker._initialize_reference_events(core)
        finally:
            self.starting = False
        assert self.started == self.created == [core._reference_thread]
        assert core._reference_thread.is_alive()
        assert core._state_lock is f.lock and core._completion._lock is f.lock
        f.register()
        return f

    def stop_verified(self):
        core = self.fixture.core
        assert self.collected.wait(1.0), "same-owner collection did not finish within the L1 window"
        assert core._stop_reference_events(time.monotonic() + 1.0)
        assert not core._reference_thread.is_alive()
        assert not core._reference_runtime_finalizer.alive
        assert core._reference_mailbox.stopped.is_set()
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        assert not core._gc_retry_timers and not core._gc_retry_timers_open
        assert len(self.gc_calls) <= 12
        assert self.runtime_threads() == self.baseline
        assert not self.errors and not self.violations

    def cleanup(self):
        # If GC is stuck inside the Core lock, do not re-enter it from here
        # through ref.close or _stop_reference_events. The mailbox has a
        # separate short lock; signal its FIFO stop, then join only our thread.
        f = self.fixture
        if f is not None:
            mailbox = getattr(f.core, "_reference_mailbox", None)
            if mailbox is not None and hasattr(mailbox, "stop"):
                mailbox.close_admission()
                mailbox.stop()
        deadline = time.monotonic() + 1.0
        for thread in self.created:
            if thread.ident is not None:
                thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in self.created)
        if f is not None:
            finalizer = getattr(f.core, "_reference_runtime_finalizer", None)
            if finalizer is not None:
                finalizer.detach()
            # Mailbox admission is now closed; local finalizers cannot issue
            # RPCs or wait for a failed GC thread. No owner counts are cleared.
            for ref in (f.ref, f.child_ref):
                if ref is not None and not ref.closed:
                    ref._closed = True
                    ref._finalizer()
        assert self.runtime_threads() == self.baseline
        assert not self.errors and not self.violations

@pytest.mark.loopback_smoke
def test_publish_installs_edge_before_wake_and_same_owner_release_does_not_deadlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real reference-consumer re-entry on one Core, not arbitrary deadlock freedom."""
    probe = _SameOwnerReferenceProbe(monkeypatch)
    try:
        f = probe.start(monkeypatch)
        core, pending, ref = f.core, f.pending, f.ref
        reply = f.complete()
        child_id = f.child_ref.object_id
        f.child_ref.close(timeout=0.5)
        assert not core.owner_table.snapshot(child_id).local_tokens
        observed = []
        original_wake = core._wake_object

        def wake(object_id):
            assert object_id == pending.object_id and not observed
            assert threading.current_thread() is threading.main_thread()
            assert core.owner_table.snapshot(object_id).outgoing_contained_edges == frozenset({f.edge})
            assert core.owner_table.snapshot(child_id).contained_holds == frozenset({f.transfer.final_hold})
            assert not core._objects[object_id].event.is_set()
            observed.append(frozenset({f.edge}))
            original_wake(object_id)

        monkeypatch.setattr(core, "_wake_object", wake)
        assert core._publish_reply(pending, reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
        assert observed == [frozenset({f.edge})]
        assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        assert core._finish_pending_task(pending)
        assert core.owner_table.snapshot(child_id).lineage_tokens == f.lineage
        assert not f.releases  # outer local handle still owns the result
        ref.close(timeout=0.5)
        probe.stop_verified()
        assert len(f.releases) == 1 and f.releases[0][0] is core._reference_thread
        assert f.releases[0][1].owner_worker_id == core.worker_id
        assert any(thread is core._reference_thread and object_id == child_id for thread, object_id in probe.gc_calls)
        assert not core.owner_table.contains(pending.object_id)
        assert not core.owner_table.contains(child_id)
        f.assert_collected()
    finally:
        probe.cleanup()
