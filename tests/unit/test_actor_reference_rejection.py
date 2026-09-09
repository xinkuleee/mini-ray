"""Current Actor constructor/method Ref input rejection before side effects.

One threadless Core and one pure ActorClientTable with an explicit ALIVE
snapshot fixture. A detached ObjectRef is only invalid API input; no borrowed
capability is fabricated. No Actor class/user function, process, socket, wait,
timer, Core constructor or RPC runs. The result case exercises the actual
Actor result serializer and must reject before any Seal or descriptor escapes.
"""

import multiprocessing.process
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import actor_worker as actor_worker_module, core as core_module, protocol
from miniray.actor_client import ActorClientTable
from miniray.actor_worker import ActorWorkerServer
from miniray.core import CoreWorker, ObjectRef
from miniray.ids import ActorGeneration, ActorID, AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.ref_transfer import current_exporter
from tests.unit._pure_core import make_pure_core, close_pure_core

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def no_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Actor Ref rejection attempted side effect")
    for kind, name in ((CoreWorker, "__init__"), (threading.Thread, "start"),
                       (threading.Thread, "join"), (threading.Event, "wait"),
                       (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(actor_worker_module, "rpc_request", forbidden)


@pytest.mark.parametrize('method', (False, True))
def test_nested_actor_ref_input_is_rejected_without_submission_or_owner_mutation(method):
    core = make_pure_core()
    actor_id = ActorID(b'a' * 16)
    core._actor_clients = ActorClientTable()
    snapshot = protocol.ActorSnapshot(actor_id, ActorGeneration(actor_id), protocol.ActorState.ALIVE,
        1, 0, 0, node_id=core.node_id, worker_id=WorkerID(b'w' * 16),
        worker_address=('actor.invalid', 1), worker_pid=101)
    core._actor_clients.register(snapshot, ('use',))
    ref = ObjectRef(ObjectID.for_task(TaskID(b'r' * 16)), core.worker_id, core.owner_address)
    args = ({'nested': [ref]},)
    before = (core._submission_index, core._accepted_task_count, core._inflight_submissions,
              core._submissions.qsize(), core._reference_mailbox.pending.qsize())
    try:
        with pytest.raises(TypeError, match='do not accept ObjectRef arguments'):
            if method:
                core.submit_actor_call(actor_id, 'use', args, {})
            else:
                # Ref rejection precedes class-definition/endpoint handling.
                core.create_actor(None, args, {}, ResourceVector())
        assert (core._submission_index, core._accepted_task_count, core._inflight_submissions,
                core._submissions.qsize(), core._reference_mailbox.pending.qsize()) == before
        assert core._actor_clients.snapshot(actor_id) == snapshot
        assert not core._objects and not core._task_finish_barriers
        assert not core.owner_table.contains(ref.object_id)
        assert ref._finalizer is None and ref.borrower_token is None
    finally:
        ref.close(timeout=0)
        close_pure_core(core)


@pytest.mark.parametrize('stored', (False, True))
def test_actor_result_nested_ref_is_rejected_before_inline_or_store_publication(stored):
    actor_id, owner, executor = ActorID(b'a' * 16), WorkerID(b'o' * 16), WorkerID(b'w' * 16)
    task = TaskID(b't' * 16)
    request = protocol.ActorCallRequest(actor_id, ActorGeneration(actor_id), owner, 0,
        'return_reference', task, AttemptID(task, 0), owner, cloudpickle.dumps(((), {})), executor, 1)
    worker = object.__new__(ActorWorkerServer)
    worker.inline_threshold = 0 if stored else 1024 * 1024
    worker.node_id = NodeID(b'n' * 16)
    worker.node_address = ('node.invalid', 1)
    child = ObjectRef(ObjectID.for_task(TaskID(b'r' * 16)), owner, ('owner.invalid', 1))
    value = {'nested': [child], 'padding': b'x' * 64}
    before = dict(vars(worker))
    exporter_before = current_exporter()
    try:
        with pytest.raises(TypeError, match='Actor results do not support ObjectRef'):
            worker._encode_result(request, value)
        assert vars(worker) == before
        assert current_exporter() is exporter_before
        assert not child.closed and child._finalizer is None and child.borrower_token is None
    finally:
        child.close(timeout=0)


def test_actor_result_custom_reducer_cannot_hide_reference_from_rejection():
    class Hidden:
        def __init__(self, ref):
            self.ref = ref
            self.calls = 0
        def __reduce__(self):
            self.calls += 1
            return list, ((self.ref,),)
    actor_id, owner = ActorID(b'a' * 16), WorkerID(b'o' * 16)
    task = TaskID(b't' * 16)
    request = protocol.ActorCallRequest(actor_id, ActorGeneration(actor_id), owner, 0,
        'hidden_reference', task, AttemptID(task, 0), owner, cloudpickle.dumps(((), {})))
    worker = object.__new__(ActorWorkerServer)
    worker.inline_threshold = 1024 * 1024
    worker.node_id = NodeID(b'n' * 16)
    worker.node_address = ('node.invalid', 1)
    child = ObjectRef(ObjectID.for_task(TaskID(b'r' * 16)), owner, ('owner.invalid', 1))
    value = Hidden(child)
    assert not core_module._contains_object_ref(value)
    exporter_before = current_exporter()
    try:
        with pytest.raises(TypeError, match='Actor results do not support ObjectRef'):
            worker._encode_result(request, value)
        assert value.calls == 1
        assert current_exporter() is exporter_before
        assert not child.closed and child._finalizer is None
    finally:
        child.close(timeout=0)
