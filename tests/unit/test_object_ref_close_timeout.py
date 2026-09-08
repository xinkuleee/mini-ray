"""Pure receipt-wait contracts for ObjectRef.close(timeout=...).

One tiny owner table and a real weakref.finalize per bound handle. The Core
stand-in only records enqueue identities; explicit test steps apply receipts.
Event waits and monotonic time are deterministic records, never real waits.
No Core constructor, runtime, thread, socket, process or physical GC runs.
"""

from types import SimpleNamespace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time
import weakref

import pytest

from miniray import core as core_module, protocol
from miniray.core import ObjectRef
from miniray.ids import AttemptID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable


pytestmark = pytest.mark.unit


class _Receipt:
    def __init__(self):
        self.signaled = False
        self.waits = []
        self.on_wait = None

    def set(self):
        self.signaled = True

    def is_set(self):
        return self.signaled

    def wait(self, timeout=None):
        self.waits.append(timeout)
        assert len(self.waits) <= 6, "close exceeded this test's finite wait script"
        if self.on_wait is not None:
            self.on_wait(timeout)
        return self.signaled


@pytest.fixture(autouse=True)
def _pure_environment(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("ObjectRef close contract attempted runtime work")

    for target, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (subprocess, "Popen"), (socket, "socket"),
        (socket, "create_connection"), (time, "sleep"),
    ):
        monkeypatch.setattr(target, method, forbidden)
    clock, receipts = [100.0], []

    def event():
        receipt = _Receipt()
        receipts.append(receipt)
        return receipt

    monkeypatch.setattr(core_module.threading, "Event", event)
    monkeypatch.setattr(core_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(core_module.CoreWorker, "__init__", forbidden)
    return SimpleNamespace(clock=clock, receipts=receipts)


class _TinyCore:
    def __init__(self, worker_id, table):
        self.worker_id = worker_id
        self.table = table
        self.enqueued = []
        self.on_enqueue = None

    def _record(self, kind, identity, done):
        self.enqueued.append((kind, identity, done))
        if self.on_enqueue is not None:
            self.on_enqueue()
        return True

    def _enqueue_local_reference_release(self, object_id, token, done):
        return self._record("local", (object_id, token), done)

    def _enqueue_borrowed_reference_release(self, object_id, owner, address, token, done):
        return self._record("foreign", (object_id, owner, address, token), done)

    def _request_attempt_borrow_release(self, key, done):
        return self._record("attempt", key, done)


def _bound(kind):
    task = TaskID(bytes((71,)) * 16)
    object_id = ObjectID.for_task(task)
    owner = WorkerID(bytes((72,)) * 16)
    borrower = WorkerID(bytes((73,)) * 16)
    attempt = AttemptID(task, 0)
    address, token = ("127.0.0.1", 39001), "one-handle-token"
    table = ObjectOwnerTable()
    table.register(object_id, current_attempt=attempt, local_token=token if kind == "local" else None)
    core = _TinyCore(owner if kind == "local" else borrower, table)
    ref = ObjectRef(object_id, owner, address)
    source = protocol.TaskHoldSource(protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, borrower, task, attempt,
    ))
    if kind == "local":
        ref._bind_local_reference(core, token)
        identity = (object_id, token)
    else:
        assert table.add_borrowed_reference(object_id, (borrower, token))
        if kind == "foreign":
            ref._bind_borrowed_reference(core, token, source)
            identity = (object_id, owner, address, token)
        else:
            assert kind == "attempt"
            identity = (owner, object_id, attempt)
            ref._bind_attempt_borrow_reference(core, identity, token, source)
    assert isinstance(ref._finalizer, weakref.finalize)
    assert ref._finalizer.alive and not ref._finalizer.atexit
    return SimpleNamespace(ref=ref, core=core, done=ref._release_done,
                           identity=identity, table=table, token=token, borrower=borrower)


@pytest.mark.parametrize("kind", ("local", "foreign", "attempt"))
@pytest.mark.parametrize("timeout", (0, 0.0, 1, 0.25))
def test_timeout_keeps_closed_and_repeated_close_waits_on_exact_single_release(kind, timeout):
    case = _bound(kind)
    ref, done = case.ref, case.done
    before = case.table.snapshot(ref.object_id)
    equivalent = ObjectRef(ref.object_id, ref.owner_worker_id)
    identity_hash = hash(ref)
    with pytest.raises(TimeoutError):
        ref.close(timeout=timeout)
    assert ref.closed and not ref._finalizer.alive and not done.is_set()
    assert case.core.enqueued == [(kind, case.identity, done)]
    assert case.table.snapshot(ref.object_id) == before
    assert done.waits == [float(timeout)]
    assert ref == equivalent and hash(ref) == identity_hash == hash(equivalent)
    with pytest.raises(RuntimeError, match="closed"):
        pickle.dumps(ref)
    with pytest.raises(TimeoutError):
        ref.close(timeout=0)
    assert case.core.enqueued == [(kind, case.identity, done)]
    assert done.waits == [float(timeout), 0.0]
    # A real receipt arriving later completes close; it is not synthesized by
    # the timeout, and the finalizer cannot enqueue its release again.
    done.set()
    assert ref.close(timeout=0) is None
    assert ref.close() is None
    assert case.core.enqueued == [(kind, case.identity, done)]
    assert done.waits == [float(timeout), 0.0, 0.0, None]


@pytest.mark.parametrize("explicit_none", (False, True))
def test_default_close_waits_for_original_receipt_not_physical_collection(explicit_none):
    case = _bound("local")

    def apply_local_release(timeout):
        assert timeout is None
        assert case.core.enqueued == [("local", case.identity, case.done)]
        assert case.table.release_local_reference(case.ref.object_id, case.token)
        case.done.set()

    case.done.on_wait = apply_local_release
    assert case.ref.close(**({"timeout": None} if explicit_none else {})) is None
    assert case.ref.closed and case.done.waits == [None]
    assert case.table.contains(case.ref.object_id)
    state = case.table.snapshot(case.ref.object_id)
    assert not state.local_tokens and not state.collection_pending
    assert case.ref._release_done is case.done


@pytest.mark.parametrize("kind", ("local", "foreign", "attempt", "detached"))
def test_invalid_timeouts_reject_before_mutating_even_detached_handles(kind):
    case = _bound("local" if kind == "detached" else kind)
    ref = ObjectRef(case.ref.object_id, case.ref.owner_worker_id) if kind == "detached" else case.ref
    try:
        before = case.table.snapshot(ref.object_id)
        for timeout in (True, False, "0", object(), [], 1j):
            with pytest.raises(TypeError):
                ref.close(timeout=timeout)
        for timeout in (-1, -0.5, float("inf"), float("-inf"), float("nan"), 10 ** 400):
            with pytest.raises(ValueError):
                ref.close(timeout=timeout)
        assert not ref.closed and not case.core.enqueued and not case.done.waits
        assert case.ref._finalizer.alive
        assert case.table.snapshot(ref.object_id) == before
        if kind == "detached":
            assert ref._finalizer is ref._release_done is ref._local_token is None
    finally:
        case.ref._finalizer.detach()


@pytest.mark.parametrize("timeout", (None, 0, 0.5))
def test_detached_close_is_noop_without_receipt_or_new_lifetime(_pure_environment, timeout):
    object_id = ObjectID.for_task(TaskID(bytes((74,)) * 16))
    ref = ObjectRef(object_id, WorkerID(bytes((75,)) * 16))
    assert ref.close(timeout=timeout) is None
    assert not ref.closed and ref._finalizer is ref._release_done is ref._local_token is None
    restored = pickle.loads(pickle.dumps(ref))
    assert restored == ref and not restored.closed
    assert restored.close(timeout=timeout) is None
    assert not _pure_environment.receipts


def test_close_is_keyword_only_and_invalid_retry_cannot_reenqueue():
    case = _bound("local")
    with pytest.raises(TypeError):
        case.ref.close(0)
    assert not case.ref.closed and case.ref._finalizer.alive
    with pytest.raises(TimeoutError):
        case.ref.close(timeout=0)
    with pytest.raises(ValueError):
        case.ref.close(timeout=-1)
    assert case.core.enqueued == [("local", case.identity, case.done)]
    assert case.done.waits == [0.0] and case.ref.closed


@pytest.mark.parametrize("timeout", (None, 0.5))
def test_close_captures_receipt_before_finalizer_can_clear_binding(timeout):
    case = _bound("local")
    case.core.on_enqueue = lambda: setattr(case.ref, "_release_done", None)
    if timeout is None:
        case.done.on_wait = lambda _timeout: case.done.set()
        assert case.ref.close() is None
    else:
        with pytest.raises(TimeoutError):
            case.ref.close(timeout=timeout)
    assert case.ref.closed and case.ref._release_done is None
    assert case.done.waits == [timeout]
    assert case.core.enqueued == [("local", case.identity, case.done)]


def test_finalizer_time_consumes_same_deadline_without_losing_pending_release(_pure_environment):
    case = _bound("local")
    case.core.on_enqueue = lambda: _pure_environment.clock.__setitem__(0, 100.25)
    with pytest.raises(TimeoutError):
        case.ref.close(timeout=1)
    assert case.done.waits == [0.75] and case.ref.closed
    assert case.core.enqueued == [("local", case.identity, case.done)]


def test_large_finite_timeout_slices_platform_wait_limit_without_new_release(_pure_environment, monkeypatch):
    case = _bound("local")
    monkeypatch.setattr(core_module.threading, "TIMEOUT_MAX", 2.0)

    def advance(timeout):
        assert timeout in (2.0, 1.0)
        _pure_environment.clock[0] += timeout
        if len(case.done.waits) == 3:
            case.done.set()

    case.done.on_wait = advance
    assert case.ref.close(timeout=5) is None
    assert case.done.waits == [2.0, 2.0, 1.0]
    assert case.core.enqueued == [("local", case.identity, case.done)]
    assert case.ref.closed
