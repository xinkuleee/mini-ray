"""Foreign INLINE dependency gates and retained lifetime through real reducers.

Pure cases use <=3 threadless Cores, <=2 source Tasks and1 consumer, <=2 tiny
container puts, actual owner Acquire/Retain/Get/Release and1KiB source stores.
Every accepted submission and mailbox event is advanced explicitly. No fake
borrower token, hand-incremented accepted count or successful release reply.
Two L1 cases use one bounded test thread around actual retain or local hold
release; they preserve the original race and are never counted as pure.
Pending gates are tested at actual admission/ready-queue boundaries. Real
dispatch evidence additionally lives in test_two_worker_pool_path and the
pending explicit-put cross-Node smoke; this file does not claim live lanes.
"""
from dataclasses import replace
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import core as core_module, protocol
from miniray.core import CoreWorker, _ReleaseBorrowedReference, _PendingTask, _TaskFinishing, _WAKE_COORDINATOR
from miniray.errors import BorrowedObjectUnavailableError, RuntimeShuttingDownError, SystemTaskError, TaskError, ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, ObjectID, TaskID, WorkerID
from miniray.ownership import ConflictingRetainedTokenError, ObjectCollectionState, ObjectState, ReleasedRetainedTokenError
from miniray.resources import ResourceVector
from tests.unit._pure_core import SynchronousReferenceMailbox, close_pure_core, make_pure_core
from tests.unit._pure_reference_output_runtime import PureReferenceOutputRuntime


def _object():
    job=JobID.random();task=TaskID.derive(job,TaskID.for_driver(job),0)
    return ObjectID.for_task(task),AttemptID(task,0)


def _retained_hold(worker,origin_attempt=None):
    origin_attempt=origin_attempt or _object()[1]
    return protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.RETAINED,worker,
        origin_attempt.task_id,origin_attempt)


@pytest.fixture(autouse=True)
def no_runtime(request,monkeypatch):
    def forbidden(*args,**kwargs):pytest.fail('foreign dependency contract attempted unreviewed runtime')
    for kind,name in ((CoreWorker,'__init__'),(threading.Timer,'start'),
                      (multiprocessing.process.BaseProcess,'start')):
        monkeypatch.setattr(kind,name,forbidden)
    for name in ('socket','socketpair','create_connection'):monkeypatch.setattr(socket,name,forbidden)
    monkeypatch.setattr(subprocess,'Popen',forbidden);monkeypatch.setattr(time,'sleep',forbidden)
    monkeypatch.setattr(core_module,'rpc_request',forbidden)
    if request.node.get_closest_marker('loopback_smoke') is None:
        monkeypatch.setattr(threading.Thread,'start',forbidden)
        monkeypatch.setattr(threading.Thread,'join',forbidden)
        monkeypatch.setattr(threading.Condition,'wait',forbidden)
        def receipt(event,timeout=None):
            assert event.is_set(),'pure wait not already acknowledged'
            return True
        monkeypatch.setattr(threading.Event,'wait',receipt)


def _take(core):
    tasks=[];assert core._submissions.qsize()<=16
    for _ in range(core._submissions.qsize()):
        item=core._submissions.get_nowait();core._submissions.task_done()
        if item is not _WAKE_COORDINATOR:
            assert isinstance(item,_PendingTask);tasks.append(item)
    return tuple(tasks)


class _Mailbox(SynchronousReferenceMailbox):
    def enqueue_internal(self,event):
        if type(event) is not _ReleaseBorrowedReference:return super().enqueue_internal(event)
        core=self.core_reference()
        try:assert core._drive_borrowed_reference_release(event.key)
        finally:event.done.set()
        return True


class _Case:
    def __init__(self,count=1):
        assert count in (1,2)
        self.core=make_pure_core();self.core._ready_tasks=queue.Queue()
        self.core._reference_mailbox=_Mailbox(self.core)
        self.owners=[];self.sources=[];self.containers=[];self.foreign=[];self.outputs=[]
        self.calls=[];self.scheduled=[];self.hook=None
        self.core._borrow_rpc=self.rpc
        self.core._schedule_reference_event=lambda mailbox,event,delay:self.scheduled.append((mailbox,event,delay))
        for index in range(count):
            owner=make_pure_core();owner.owner_address=('foreign-owner.invalid',100+index)
            def release_local_container(address,handler,message,owner=owner):
                assert address==owner.owner_address and handler=='release_contained_reference'
                return owner.release_contained_reference(message)
            owner._borrow_rpc=release_local_container
            backend=PureReferenceOutputRuntime(owner)
            source,local=owner._register_submission(owner.define_remote_function(lambda:7),(),{},
                ResourceVector(),_enqueue=True)
            assert _take(owner)==(source,)
            container=owner.put([local]);_take(owner)
            transfer,=owner.owner_table.snapshot(container.object_id).outgoing_contained_edges
            self.owners.append((owner,backend));self.sources.append((source,local));self.containers.append(container)
            foreign=self.core._restore_borrowed_reference(local.object_id,owner.worker_id,
                owner.owner_address,transfer.incoming_hold(owner.worker_id))
            self.foreign.append(foreign)
    def rpc(self,address,handler,message):
        assert len(self.calls)<96
        owner=next(owner for owner,_ in self.owners if owner.owner_address==address)
        methods={'acquire_borrowed_object':owner.acquire_exported_reference,
                 'release_borrowed_object':owner.release_borrowed_reference,
                 'retain_owned_object_for_task':owner.retain_owned_object_for_task,
                 'get_retained_owned_object':owner.get_retained_owned_object,
                 'release_owned_object_for_task':owner.release_owned_object_for_task}
        self.calls.append((handler,message))
        invoke=lambda:methods[handler](message)
        return self.hook(owner,handler,message,invoke) if self.hook else invoke()
    def publish_source(self,index=0,value=7,*,stored=False,error=False):
        owner,backend=self.owners[index];pending,_=self.sources[index]
        if error:assert owner._publish_task_error(pending,RuntimeError('producer'))
        else:
            push=protocol.PushTask(LeaseID.random(),WorkerID.random(),pending.spec)
            reply=backend.complete(push,(value,),inline_threshold=0 if stored else 1024)
            assert owner._publish_reply(pending,reply)
        assert owner._finish_pending_task(pending);_take(owner)
    def submit(self,*,independent=False):
        args=() if independent else tuple(self.foreign)
        pending,ref=self.core._register_submission(self.core.define_remote_function(lambda *args:None),
            args,{},ResourceVector(),max_retries=1,_enqueue=True)
        assert _take(self.core)==(pending,);self.outputs.append(ref)
        return pending,ref
    def terminal(self,pending):
        assert self.core._publish_task_error(pending,SystemTaskError('test terminal'))
        assert self.core._finish_pending_task(pending);_take(self.core)
        # Drain publication/finish checks while the consumer handle remains
        # live, so a later close tests one explicit foreign-lineage GC round.
        self.core._reference_mailbox.drain()
    def close(self):
        self.hook=None
        for ref in self.outputs:
            pending=self.core._task_finish_barriers.get(ref.object_id)
            if pending is not None:
                self.core._clear_protocol_unresolved(pending)
                if self.core.owner_table.snapshot(ref.object_id).state is ObjectState.PENDING:
                    self.core._publish_task_error(pending,SystemTaskError('test cleanup'))
                self.core._finish_pending_task(pending)
            ref.close(timeout=0)
        self.core._reference_mailbox.drain()
        for receipt in tuple(self.core._foreign_lineage_collection_receipts.values()):
            self.core._drive_foreign_lineage_collection(receipt,from_retry=True)
        for ref in self.foreign:ref.close(timeout=0)
        for (owner,_),(pending,local),container in zip(self.owners,self.sources,self.containers):
            if owner.owner_table.snapshot(local.object_id).state is ObjectState.PENDING:
                owner._publish_task_error(pending,SystemTaskError('source cleanup'));owner._finish_pending_task(pending)
            local.close(timeout=0);container.close(timeout=0);owner._reference_mailbox.drain();_take(owner)
            close_pure_core(owner)
        _take(self.core);close_pure_core(self.core)


@pytest.mark.loopback_smoke
def test_registration_retain_is_outside_state_lock_and_shutdown_wins():
    """One real caller thread;1s events/joins, no background Core runtime."""
    f=_Case();entered=threading.Event();allow=threading.Event();errors=[]
    thread=None
    try:
        def hook(owner,kind,message,invoke):
            result=invoke()
            if kind=='retain_owned_object_for_task':
                entered.set();assert allow.wait(1.0)
            return result
        f.hook=hook
        def register():
            try:f.submit()
            except BaseException as exc:errors.append(exc)
        thread=threading.Thread(target=register,name='foreign-retain-race')
        thread.start();assert entered.wait(1.0)
        assert f.core._state_lock.acquire(timeout=0.1)
        try:f.core._accepting=False
        finally:f.core._state_lock.release()
        allow.set();thread.join(1.0);assert not thread.is_alive()
        assert len(errors)==1 and isinstance(errors[0],RuntimeShuttingDownError)
        releases=[r for k,r in f.calls if k=='release_owned_object_for_task']
        assert len(releases)==1 and not f.core._objects
        owner,_=f.owners[0]
        assert not owner.owner_table.snapshot(f.sources[0][0].object_id).retained_tokens
    finally:
        allow.set()
        if thread is not None:thread.join(1.0);assert not thread.is_alive()
        f.close()


@pytest.mark.loopback_smoke
def test_finish_claim_and_protocol_send_are_mutually_fenced(monkeypatch):
    """Actual finish thread pauses at local submitted-hold release, not fake ACK."""
    c=make_pure_core();entered=threading.Event();allow=threading.Event();results=[];errors=[];thread=None
    source=c.put(7);_take(c)
    p,ref=c._register_submission(c.define_remote_function(lambda value:value),(source,),{},
        ResourceVector(),_enqueue=True)
    assert _take(c)==(p,)
    assert c._publish_task_error(p,SystemTaskError('done'))
    actual=c.owner_table.release_submitted_reference
    def release(object_id,hold):
        assert object_id==source.object_id and hold==p.dependency_hold
        entered.set();assert allow.wait(1.0)
        return actual(object_id,hold)
    monkeypatch.setattr(c.owner_table,'release_submitted_reference',release)
    def finish():
        try:results.append(c._finish_pending_task(p))
        except BaseException as exc:errors.append(exc)
    try:
        thread=threading.Thread(target=finish,name='foreign-finish-race');thread.start()
        assert entered.wait(1.0)
        # The finish holds Core's composition lock here. A separate sender
        # must not acquire it until local release/finish are committed.
        assert not c._state_lock.acquire(timeout=0.01)
        allow.set();thread.join(1.0);assert not thread.is_alive() and not errors and results==[True]
        with pytest.raises(_TaskFinishing):c._mark_protocol_unresolved(p,'late-push-send')
        assert p.dependency_hold not in c.owner_table.snapshot(source.object_id).submitted_tokens
        assert c._accepted_task_count==0 and not c._protocol_unresolved
    finally:
        allow.set()
        if thread is not None:thread.join(1.0);assert not thread.is_alive()
        ref.close(timeout=0);source.close(timeout=0);c._reference_mailbox.drain();_take(c);close_pure_core(c)


@pytest.mark.unit
def test_execute_cannot_send_after_finish_claim(dependency,monkeypatch):
    f=dependency;p,_=f.submit(independent=True);c=f.core
    # Exercise the actual completed finish tombstone rather than hand-writing
    # accepted counts/finishing sets. A stale execute must produce no lease RPC.
    f.terminal(p)
    sent=[]
    monkeypatch.setattr(c,'_request_lease_hop',lambda *a,**kw:sent.append((a,kw)))
    assert c._execute(p,p.spec)
    assert sent==[] and c._accepted_task_count==0


@pytest.fixture
def dependency():
    case=_Case()
    try:yield case
    finally:case.close()


@pytest.mark.unit
def test_retained_hold_is_bound_idempotent_and_survives_parent_release(dependency):
    f=dependency;pending,_=f.submit();owner,_=f.owners[0];source,_=f.sources[0]
    guard=pending.foreign_dependency_guards[0]
    retain=next(message for kind,message in f.calls if kind=='retain_owned_object_for_task')
    replay=owner.retain_owned_object_for_task(retain)
    assert replay.accepted and not replay.retained
    with pytest.raises(ConflictingRetainedTokenError):
        owner.owner_table.retain_borrowed_reference_for_task(
            source.object_id,(f.core.worker_id,'wrong-parent-borrower'),guard.hold)
    f.foreign[0].close(timeout=0);f.publish_source()
    query=protocol.GetRetainedOwnedObject(source.object_id,owner.worker_id,f.core.worker_id,guard.hold)
    reply=owner.get_retained_owned_object(query)
    assert reply.accepted and cloudpickle.loads(reply.data)==7
    assert guard.hold in owner.owner_table.snapshot(source.object_id).retained_tokens


@pytest.mark.unit
def test_release_before_retain_tombstones_reordered_delivery(dependency):
    f=dependency;owner,_=f.owners[0];source,_=f.sources[0];foreign=f.foreign[0]
    hold=_retained_hold(f.core.worker_id)
    release=protocol.ReleaseOwnedObjectForTask(source.object_id,owner.worker_id,f.core.worker_id,hold)
    assert owner.release_owned_object_for_task(release).accepted
    retain=protocol.RetainOwnedObjectForTask(source.object_id,owner.worker_id,f.core.worker_id,foreign.borrower_token,hold)
    reply=owner.retain_owned_object_for_task(retain)
    assert not reply.accepted and not reply.retained
    assert owner.owner_table.retained_release_was_seen(source.object_id,hold)


@pytest.mark.unit
def test_ambiguous_retain_rolls_back_exact_token_and_retains_obligation(dependency):
    f=dependency;releases=[];allow=False
    def hook(owner,kind,message,invoke):
        if kind=='retain_owned_object_for_task':invoke();raise RuntimeError('all retain ACKs lost')
        if kind=='release_owned_object_for_task':
            releases.append(message)
            if not allow:raise RuntimeError('release unavailable')
        return invoke()
    f.hook=hook
    with pytest.raises(RuntimeError,match='retain ACKs lost'):f.submit()
    assert len(f.core._orphan_foreign_guard_releases)==1
    first=releases[0];owner,_=f.owners[0]
    assert first.hold in owner.owner_table.snapshot(first.object_id).retained_tokens
    allow=True;f.core._retry_orphan_foreign_guard_releases()
    assert releases==[first,first] and not f.core._orphan_foreign_guard_releases
    assert first.hold not in owner.owner_table.snapshot(first.object_id).retained_tokens


@pytest.mark.unit
def test_foreign_pending_stays_off_lane_then_inline_rewrites_and_executes(dependency):
    f=dependency;p,_=f.submit();c=f.core
    c._admit_or_block(p)
    assert c._blocked_tasks[p.task_key] is p and c._ready_tasks.empty()
    f.publish_source();c._promote_unblocked_tasks()
    ready=c._ready_tasks.get_nowait();c._ready_tasks.task_done()
    assert ready.pending is p and ready.dependencies==()
    assert isinstance(ready.spec.args[0],protocol.InlineArg)
    assert cloudpickle.loads(ready.spec.args[0].data)==7
    assert not c._blocked_tasks


@pytest.mark.unit
def test_pending_foreign_dependency_does_not_block_independent_ready_task(dependency):
    f=dependency;blocked,_=f.submit();independent,_=f.submit(independent=True);c=f.core
    c._admit_or_block(blocked);c._admit_or_block(independent)
    ready=c._ready_tasks.get_nowait();c._ready_tasks.task_done()
    assert ready.pending is independent and c._ready_tasks.empty()
    assert c._blocked_tasks[blocked.task_key] is blocked
    assert c.owner_table.snapshot(blocked.object_id).state is ObjectState.PENDING


@pytest.mark.parametrize('state,error_type',[(protocol.OwnedObjectState.ERROR,TaskError),(protocol.OwnedObjectState.LOST,SystemTaskError)])
@pytest.mark.unit
def test_foreign_error_or_lost_never_executes_user_path(dependency,state,error_type):
    f=dependency;p,_=f.submit();owner,_=f.owners[0]
    f.publish_source(stored=state is protocol.OwnedObjectState.LOST,error=state is protocol.OwnedObjectState.ERROR)
    if state is protocol.OwnedObjectState.LOST:assert owner.drop_object(f.sources[0][1])
    with pytest.raises(error_type):f.core._dependencies_ready(p)
    assert f.core._ready_tasks.empty()


@pytest.mark.unit
def test_retry_keeps_same_foreign_guard_and_input_handle_may_close(dependency):
    f=dependency;f.publish_source(value=9);p,_=f.submit();c=f.core;guard=p.foreign_dependency_guards[0]
    f.foreign[0].close(timeout=0)
    reply=protocol.TaskReply(p.task_id,p.spec.attempt_id,WorkerID.random(),protocol.TaskReplyStatus.SYSTEM_ERROR,
        error=protocol.RemoteErrorInfo('RuntimeError','retry'))
    assert not c._retry_explicit_system_failure(p,reply)
    retried,=_take(c)
    assert retried.foreign_dependency_guards==(guard,) and retried.spec.attempt_id==p.spec.attempt_id.next()
    prepared,descriptors,_=c._prepare_task_dependencies(retried.spec,retried.foreign_dependency_guards)
    assert not descriptors and cloudpickle.loads(prepared.args[0].data)==9
    assert sum(kind=='retain_owned_object_for_task' for kind,_ in f.calls)==1


@pytest.mark.unit
def test_finish_is_once_and_partial_multi_guard_retry_skips_released():
    f=_Case(2)
    try:
        for index in range(2):f.publish_source(index)
        p,ref=f.submit();f.terminal(p)
        assert f.core._finish_pending_task(p)
        guards=p.foreign_dependency_guards
        by_owner={owner.worker_id:owner for owner,_ in f.owners}
        assert all(guard.hold in by_owner[guard.owner_worker_id].owner_table.snapshot(guard.object_id).retained_tokens
                   for guard in guards)
        releases=[];allow=False
        edges=f.core._foreign_lineage_registry.snapshot(p.task_id).edges
        first,second=(edge.dependency_object_id for edge in edges)
        def hook(owner,kind,message,invoke):
            if kind=='release_owned_object_for_task':
                releases.append(message)
                if message.object_id==second and not allow:raise RuntimeError('second release unavailable')
            return invoke()
        f.hook=hook;ref.close(timeout=0);f.core._reference_mailbox.drain()
        receipt=f.core._foreign_lineage_collection_receipts[ref.object_id]
        assert [r.object_id for r in releases]==[first,second]
        allow=True;assert f.core._drive_foreign_lineage_collection(receipt,from_retry=True)
        assert [r.object_id for r in releases]==[first,second,second]
        assert releases[1]==releases[2] and not f.core._foreign_lineage_collection_receipts
    finally:f.close()


@pytest.mark.unit
def test_existing_protocol_fence_prevents_finish_release(dependency):
    f=dependency;p,_=f.submit();c=f.core
    c._mark_protocol_unresolved(p,'push_send')
    assert not c._finish_pending_task(p) and c._accepted_task_count==1
    assert not any(kind=='release_owned_object_for_task' for kind,_ in f.calls)
    c._clear_protocol_unresolved(p);f.terminal(p)
    assert not any(kind=='release_owned_object_for_task' for kind,_ in f.calls)


@pytest.mark.unit
def test_owner_shutdown_waits_for_active_retained_hold_then_closes(dependency):
    f=dependency;f.publish_source();p,ref=f.submit();owner,_=f.owners[0];guard=p.foreign_dependency_guards[0]
    f.terminal(p)
    owner._accepting=False
    assert not owner.can_finalize_shutdown(require_distributed_clean=False) and owner._owner_protocol_open
    query=protocol.GetRetainedOwnedObject(guard.object_id,owner.worker_id,f.core.worker_id,guard.hold)
    assert owner.get_retained_owned_object(query).accepted
    # Actual consumer GC, not a fabricated release, retires the lineage hold.
    ref.close(timeout=0);f.core._reference_mailbox.drain()
    assert guard.hold not in owner.owner_table.snapshot(guard.object_id).retained_tokens
    assert owner.can_finalize_shutdown(require_distributed_clean=False)
    owner._accepting=True


@pytest.mark.unit
def test_foreign_dependency_protocol_validates_full_identity_and_payload() -> None:
    object_id, _ = _object()
    owner = WorkerID.random()
    borrower = WorkerID.random()
    hold = _retained_hold(borrower)
    retain = protocol.RetainOwnedObjectForTask(
        object_id, owner, borrower, "borrow", hold
    )
    assert retain.hold == hold
    assert protocol.RetainOwnedObjectForTaskReply(
        object_id, owner, borrower, "borrow", hold, True, True
    ).hold == hold
    assert protocol.GetRetainedOwnedObjectReply(
        object_id, owner, borrower, hold, True,
        state=protocol.OwnedObjectState.PENDING,
    ).hold == hold
    assert protocol.ReleaseOwnedObjectForTaskReply(
        object_id, owner, borrower, hold, True, False
    ).hold == hold

    with pytest.raises(ProtocolError, match="borrower_token"):
        protocol.RetainOwnedObjectForTask(
            object_id, owner, borrower, "", hold
        )
    with pytest.raises(ProtocolError, match="cannot expose"):
        protocol.GetRetainedOwnedObjectReply(
            object_id, owner, borrower, hold, False,
            state=protocol.OwnedObjectState.PENDING, detail="rejected",
        )
    with pytest.raises(ProtocolError, match="rejected.*cannot remove"):
        protocol.ReleaseOwnedObjectForTaskReply(
            object_id, owner, borrower, hold, False, True, "rejected"
        )
    with pytest.raises(ProtocolError, match="RETAINED kind"):
        protocol.GetRetainedOwnedObject(
            object_id, owner, borrower,
            replace(hold, kind=protocol.TaskReferenceHoldKind.SUBMITTED),
        )
    with pytest.raises(ProtocolError, match="submitting worker"):
        protocol.ReleaseOwnedObjectForTask(
            object_id, owner, borrower,
            _retained_hold(WorkerID.random(), hold.origin_attempt_id),
        )
