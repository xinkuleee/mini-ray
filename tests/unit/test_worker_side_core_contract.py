"""Explicitly classified contracts for Worker-side Core behavior.

Nine original synchronous contracts use only values or bare Workers with fake
Cores. Driver-only API fencing and two canonical child registrations are now
pure: no Core constructor or distributed shutdown is claimed. Thread-local
binding isolation is L1 with one real thread, two one-second gates, a two-second
normal join and a one-second failure-cleanup join. Three further original
contracts use real threadless Cores: wrapper serialization, owner-retain fencing,
and existing-owner stored pins. Their shutdown calls run actual quiescent Core
reducers, not runtime thread teardown; the retain case models only the Worker
Condition deadline boundary, without claiming real wait_for timing. Three more
L1 contracts retain actual Condition waits: one task/shutdown race owns exactly
two threads, a two-second task gate, a one-second shutdown wait, and shared
two-second normal/failure joins; two timeout cases each use the calling thread
and a real one-millisecond wait_for deadline. They keep the original fake Core
drain seam, not task execution/publication or Core thread teardown. Markers stay
per function so pure selection cannot silently include these four L1 cases.
"""

from __future__ import annotations

from types import SimpleNamespace
import threading

import cloudpickle
import pytest

import miniray.api as api
import miniray.worker as worker_module
from miniray import protocol
from miniray.api import RemoteFunction
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import CoreWorker
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.resources import ResourceVector
from miniray.runtime_binding import (
    ExecutionContext,
    bind_runtime,
    current_binding,
    current_core_worker,
    current_execution_context,
)
from miniray.publication_sources import (
    OwnedContainedSource, PreparedContainedTransfer,
)
from miniray.trace import EventSink, MemoryEventSink, NonOwningEventSink
from miniray.worker import WorkerServer


def _plus_one(value: int) -> int:
    return value + 1


def _core(job_id: JobID) -> CoreWorker:
    """Construct a live CoreWorker; its background threads make this heavy."""

    return CoreWorker(
        ("127.0.0.1", 19001),
        NodeID.random(),
        job_id=job_id,
        worker_id=WorkerID.random(),
        event_sink=EventSink(),
        dispatch_lanes=1,
    )


def _push_for_job(worker_id: WorkerID, job_id: JobID) -> protocol.PushTask:
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    payload = cloudpickle.dumps(_plus_one)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job_id, __name__, "_plus_one", "worker-core-v1"),
        payload,
    )
    spec = protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=AttemptID(task_id, 0),
        function=definition.key,
        args=(protocol.InlineArg(cloudpickle.dumps(1), serializer="cloudpickle"),),
        num_returns=1,
        resources=ResourceVector(),
        owner_worker_id=WorkerID.random(),
        parent_task_id=TaskID.for_driver(job_id),
        function_definition=definition,
    )
    return protocol.PushTask(LeaseID.random(), worker_id, spec)


def _bare_embedded_worker() -> WorkerServer:
    worker = object.__new__(WorkerServer)
    worker.worker_id = WorkerID.random()
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 19002)
    worker.gcs_address = ("127.0.0.1", 19003)
    worker.inline_threshold = 1024
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core_drain_lock = threading.Lock()
    worker._embedded_core = None
    worker._embedded_core_job_id = None
    worker._embedded_core_stopped = False
    worker._worker_core_enabled = True
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._accepting_tasks = True
    worker._active_tasks = 0
    worker._owner_retain_admission_open = True
    worker.event_sink = MemoryEventSink(
        clock_ns=lambda: 1, process_id=lambda: 7001
    )
    return worker


class _CloseTrackingSink(MemoryEventSink):
    def __init__(self) -> None:
        super().__init__(clock_ns=lambda: 1, process_id=lambda: 7001)
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


@pytest.mark.unit
def test_non_owning_trace_view_forwards_identity_and_component_without_close() -> None:
    """A nested runtime shares one source but advertises its own role."""

    sink = _CloseTrackingSink()
    view = NonOwningEventSink(sink, component="worker_core")

    ready = sink.emit("process_ready", component="worker")
    submitted = view.emit(
        "task_submitted", component="core_worker", task_id="child-1"
    )
    view.close()
    stopping = sink.emit("process_stopping", component="worker")

    assert ready is not None and submitted is not None and stopping is not None
    assert view.sink is sink
    assert view.component == "worker_core"
    assert [event.component for event in sink.events] == [
        "worker", "worker_core", "worker",
    ]
    assert [event.process_seq for event in sink.events] == [1, 2, 3]
    assert submitted.attributes == {"task_id": "child-1"}
    assert sink.close_calls == 0

    sink.close()
    assert sink.close_calls == 1


def _pure_existing_core_guards(monkeypatch):
    """Case-local infrastructure fences with a sticky failure observation.

    These cases use real Core/owner methods but never construct or lazy-start a
    runtime. A swallowed best-effort exception must not hide a transport attempt.
    The single flag has constant size even if a broken reducer repeats a call.
    """

    import multiprocessing.process
    import queue
    import socket
    import subprocess
    import time

    from miniray import control, core as core_module, node, owner_service, transport
    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    failed = [False]

    def forbidden(*_args, **_kwargs):
        failed[0] = True
        pytest.fail("pure existing-Core contract attempted runtime work")

    for kind, method in (
        (CoreWorker, "__init__"), (WorkerServer, "__init__"),
        (WorkerServer, "_embedded_core_for"),
        (CoreWorker, "_node_death_view_rpc"),
        (node.NodeServer, "__init__"), (control.GCSLite, "__init__"),
        (owner_service.OwnerService, "__init__"),
        (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    for module in (api, core_module, node, worker_module, control):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    return failed, forbidden


def _fence_pure_core_callbacks(core, forbidden):
    """Give the already-threadless fixture the same sticky effect guard."""

    for name in (
        "_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline",
        "_push_task_rpc", "_actor_call_rpc",
        "_initialize_reference_events", "_schedule_reference_event",
    ):
        setattr(core, name, forbidden)


def _attach_existing_pure_owner(worker, core, job_id, failed):
    """Install one existing logical owner, not the lazy Core startup path.

    Only the Worker-death journal boundary is a typed in-memory reply. There is
    no certified Node-death bootstrap, GCS constructor, owner listener, task
    execution, or unbounded callback log. The real Core shutdown consumes three
    fresh empty suffixes: two drain observations and one final owner fence.
    """

    assert worker._embedded_core is None
    assert not core._objects and core._accepted_task_count == 0
    core.job_id = job_id
    core.driver_task_id = TaskID.for_driver(job_id)
    core.worker_id = worker.worker_id
    core.node_id = worker.node_id
    core.node_address = worker.node_address
    core.owner_address = worker._owner_address()
    core.gcs_address = worker.gcs_address
    barriers = []

    def journal_suffix(address, handler, request):
        try:
            assert len(barriers) < 16
            assert address == worker.gcs_address
            assert handler == "get_worker_deaths"
            assert type(request) is protocol.GetWorkerDeaths
            assert request.after_epoch == 0
            barriers.append(request)
            return protocol.GetWorkerDeathsReply(0, 0, ())
        except BaseException:
            # _sync_worker_deaths intentionally catches BaseException. Retain
            # this observation even if a later shutdown attempt could succeed.
            failed[0] = True
            raise

    core._rpc = journal_suffix
    worker._embedded_core = core
    worker._embedded_core_job_id = job_id
    assert worker._borrow_owner_core() is core
    return barriers


@pytest.mark.unit
def test_remote_function_cloudpickle_rebuilds_runtime_cache_and_lock() -> None:
    """A transported remote wrapper must not retain its old CoreWorker."""

    from tests.unit._pure_core import close_pure_core, make_pure_core

    with pytest.MonkeyPatch.context() as monkeypatch:
        failed, forbidden = _pure_existing_core_guards(monkeypatch)
        cores = []
        try:
            original = RemoteFunction(
                _plus_one,
                num_cpus=0,
                resources={"teaching": 2},
                max_retries=3,
            )
            for _ in range(2):
                core = make_pure_core()
                cores.append(core)
                _fence_pure_core_callbacks(core, forbidden)
            first_core, second_core = cores
            first_definition = original._definition_for(first_core)
            assert original._definition_for(first_core) is first_definition
            payload = cloudpickle.dumps(original)
            assert 0 < len(payload) <= 8192
            restored = cloudpickle.loads(payload)

            assert restored is not original
            assert restored._function(4) == 5
            assert restored._num_cpus == 0
            assert restored._custom_resources == ResourceVector({"teaching": 2})
            assert restored._resources == ResourceVector({"teaching": 2})
            assert restored._max_retries == 3

            # Real serialization excludes process-local caches and locks; both
            # definition builds still call the actual Core implementation. No
            # second process or concurrent definition call is claimed here.
            assert restored._definition_owner is None
            assert restored._definition is None
            assert restored._definition_lock is not original._definition_lock
            assert original._definition_lock.acquire(blocking=False)
            try:
                assert restored._definition_lock.acquire(blocking=False)
                restored._definition_lock.release()
            finally:
                original._definition_lock.release()
            restored_definition = restored._definition_for(second_core)
            assert restored._definition_for(second_core) is restored_definition
            assert restored._definition_owner is second_core
            assert restored_definition.key.job_id == second_core.job_id
            assert first_definition.key.job_id == first_core.job_id
            assert original._definition_owner is first_core
            assert original._definition is first_definition
            assert first_core.job_id != second_core.job_id
            assert not original._definition_lock.locked()
            assert not restored._definition_lock.locked()
            assert all(not core._objects and core._submissions.empty()
                       and core._accepted_task_count == 0 for core in cores)
        finally:
            # Definition-only Cores own no admitted Tasks or ObjectRefs. This
            # closes the pure fixture, not actual runtime shutdown threads.
            for core in cores:
                close_pure_core(core)
            assert failed == [False]


@pytest.mark.unit
def test_parent_execution_context_derives_child_ids_from_parent_and_index() -> None:
    """Two child submissions use the current task, not the driver, as parent."""

    context_type = getattr(api, "ExecutionContext", None)
    if context_type is None:
        context_type = getattr(__import__("miniray.core", fromlist=["ExecutionContext"]), "ExecutionContext", None)
    if context_type is None:
        pytest.fail(
            "Worker-side Core requires an ExecutionContext-like public seam"
        )

    job_id = JobID.random()
    parent_task_id = TaskID.derive(
        job_id, TaskID.for_driver(job_id), 7
    )
    parent_attempt_0 = AttemptID(parent_task_id, 0)
    context = context_type(
        job_id=job_id,
        parent_task_id=parent_task_id,
        parent_attempt_id=parent_attempt_0,
    )

    # The helper name is deliberately discovered from a tiny semantic set so
    # the test does not force ExecutionContext into one particular module or
    # dictate incidental spelling.
    derive = next(
        (
            getattr(context, name)
            for name in (
                "next_task_id",
                "next_child_task_id",
                "derive_child_task_id",
            )
            if callable(getattr(context, name, None))
        ),
        None,
    )
    if derive is None:
        pytest.fail(
            "ExecutionContext must expose one monotonic child-TaskID operation"
        )

    first = derive()
    second = derive()

    attempt_0_seed = TaskID.derive(job_id, parent_task_id, 0)
    assert first == TaskID.derive(job_id, attempt_0_seed, 0)
    assert second == TaskID.derive(job_id, attempt_0_seed, 1)

    # Recreating the same execution context replays the same deterministic
    # child IDs, while a later physical attempt of the logical parent gets a
    # disjoint child namespace.  TaskSpec still records parent_task_id as the
    # logical parent; the attempt is only an identity-derivation seed.
    replay = context_type(
        job_id=job_id,
        parent_task_id=parent_task_id,
        parent_attempt_id=parent_attempt_0,
    )
    replay_derive = next(
        getattr(replay, name)
        for name in (
            "next_task_id",
            "next_child_task_id",
            "derive_child_task_id",
        )
        if callable(getattr(replay, name, None))
    )
    assert replay_derive() == first
    assert replay_derive() == second

    later_attempt = context_type(
        job_id=job_id,
        parent_task_id=parent_task_id,
        parent_attempt_id=parent_attempt_0.next(),
    )
    later_derive = next(
        getattr(later_attempt, name)
        for name in (
            "next_task_id",
            "next_child_task_id",
            "derive_child_task_id",
        )
        if callable(getattr(later_attempt, name, None))
    )
    attempt_1_seed = TaskID.derive(job_id, parent_task_id, 1)
    later_first = later_derive()
    assert later_first == TaskID.derive(job_id, attempt_1_seed, 0)
    assert later_first not in {first, second}


@pytest.mark.loopback_smoke
def test_runtime_binding_isolates_worker_thread_and_restores_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L1: one real handler thread; Core identities need no runtime."""

    import multiprocessing.process
    import queue
    import socket
    import subprocess
    import time

    from miniray import control, core as core_module, node, transport

    def forbidden(*_args, **_kwargs):
        pytest.fail("binding-only L1 attempted runtime/process/transport work")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (WorkerServer, "__init__"), (node.NodeServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Timer, "start"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (api, core_module, node, worker_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)

    # _active_core_worker checks the bound object's real type, but this
    # contract never calls a Core method or requires owner/dispatch state.
    driver_core = object.__new__(CoreWorker)
    worker_core = object.__new__(CoreWorker)
    driver_runtime = SimpleNamespace(core_worker=driver_core)
    monkeypatch.setattr(api, "_runtime", driver_runtime)
    previous_binding = current_binding()
    assert previous_binding is None
    job_id = JobID.random()
    parent = TaskID.derive(job_id, TaskID.for_driver(job_id), 2)
    worker_context = ExecutionContext(job_id, parent, AttemptID(parent, 0))
    driver_context = ExecutionContext(job_id, parent, AttemptID(parent, 1))
    entered = threading.Event()
    release = threading.Event()
    observations: list[object] = []
    errors: list[BaseException] = []

    def worker_handler() -> None:
        try:
            assert current_core_worker() is None
            assert current_execution_context() is None
            with bind_runtime(worker_core, worker_context):
                observations.append(api._active_core_worker())
                observations.append(current_core_worker())
                assert current_execution_context() is worker_context
                entered.set()
                assert release.wait(1.0)
                assert current_execution_context() is worker_context
            observations.append(current_core_worker())
            assert current_execution_context() is None
        except BaseException as exc:
            # One invocation, one terminal exception: bounded at one entry
            # and observed by the main thread after the owned thread joins.
            errors.append(exc)
        finally:
            entered.set()

    thread = threading.Thread(
        target=worker_handler, name="miniray-runtime-binding-test", daemon=True,
    )
    try:
        thread.start()
        assert entered.wait(1.0)
        assert not errors
        assert current_core_worker() is None
        assert current_execution_context() is None
        assert api._active_core_worker() is driver_core
        with bind_runtime(driver_core, driver_context):
            assert api._active_core_worker() is driver_core
            assert current_execution_context() is driver_context
        assert current_binding() is previous_binding
        assert api._runtime is driver_runtime
        release.set()
        thread.join(2.0)
        assert not thread.is_alive()
        assert not errors
        assert observations == [worker_core, worker_core, None]
    finally:
        release.set()
        if thread.ident is not None and thread.is_alive():
            thread.join(1.0)
        assert not thread.is_alive()
        assert len(errors) <= 1 and not errors
        assert current_binding() is previous_binding
        assert current_execution_context() is None
        assert api._runtime is driver_runtime
        assert not vars(driver_core) and not vars(worker_core)


@pytest.mark.unit
def test_execution_context_cleanup_and_core_child_specs(monkeypatch) -> None:
    """Context exit/exception restores state and child specs name the parent."""

    import queue

    from miniray.core import _RetryInlineGc, _WAKE_COORDINATOR
    from miniray.errors import SystemTaskError
    from miniray.ownership import ObjectCollectionState, ObjectState
    from miniray.recovery import TaskState
    from tests.unit._pure_core import close_pure_core, make_pure_core
    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    core = make_pure_core()
    job_id = core.job_id
    parent_task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 4)
    parent_attempt = AttemptID(parent_task_id, 1)
    context = ExecutionContext(job_id, parent_task_id, parent_attempt)
    definition = core.define_remote_function(_plus_one)
    refs = []
    pendings = []
    previous_binding = current_binding()

    def release_handles():
        # Invoke the real local finalizer, then check its exact receipt and
        # token removal. This is not public close or a remote protocol ACK.
        assert len(refs) <= 2
        for ref in refs:
            finalizer, done, token = ref._finalizer, ref._release_done, ref._local_token
            assert finalizer is not None and done is not None and token is not None
            finalizer()
            assert done.is_set() and not finalizer.alive
            if core.owner_table.contains(ref.object_id):
                assert token not in core.owner_table.snapshot(ref.object_id).local_tokens

    try:
        with bind_runtime(core, context):
            first, first_ref = core._register_submission(
                definition, (1,), {}, ResourceVector()
            )
            pendings.append(first)
            refs.append(first_ref)
            second, second_ref = core._register_submission(
                definition, (2,), {}, ResourceVector()
            )
            pendings.append(second)
            refs.append(second_ref)
            assert first.spec.parent_task_id == parent_task_id
            assert second.spec.parent_task_id == parent_task_id
            assert first.spec.owner_worker_id == core.worker_id
            assert second.spec.owner_worker_id == core.worker_id
            assert first.spec.task_id != second.spec.task_id
            assert current_execution_context() is context
            attempt_seed = TaskID.derive(job_id, parent_task_id, parent_attempt.attempt_number)
            assert first.task_id == TaskID.derive(job_id, attempt_seed, 0)
            assert second.task_id == TaskID.derive(job_id, attempt_seed, 1)
            assert context._submission_index == 2 and core._submission_index == 0
            assert tuple(pending.spec.attempt_id for pending in pendings) == tuple(
                AttemptID(pending.task_id, 0) for pending in pendings
            )
            assert all(core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING for pending in pendings)
            # Preserve the original registration-only scope: no queue
            # admission, lease, accepted-count increment or user execution.
            assert core._submissions.empty() and core._accepted_task_count == 0

        assert current_core_worker() is None
        assert current_execution_context() is None

        with pytest.raises(RuntimeError, match="leave binding"):
            with bind_runtime(core, ExecutionContext(job_id, parent_task_id, parent_attempt)):
                raise RuntimeError("leave binding")
        assert current_core_worker() is None
        assert current_execution_context() is None
        assert current_binding() is previous_binding

        # End these two unleased canonical Tasks through the real local error
        # reducer only after their original assertions, then finish and GC.
        # This is fixture disposal, not a successful Worker or cluster drain.
        for pending in pendings:
            error = SystemTaskError("pure child-spec fixture terminal; user function was not run")
            assert core._publish_task_error(pending, error)
            snapshot = core.owner_table.snapshot(pending.object_id)
            assert snapshot.state is ObjectState.ERROR and snapshot.error is error
            assert core._recovery.task_record(pending.task_id).state is TaskState.SYSTEM_FAILED
            assert core._finish_pending_task(pending)
        assert core._finished_tasks == {pending.task_id for pending in pendings}
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        # Each terminal publication wakes the coordinator once, and its
        # subsequent logical finish wakes it once more: two Tasks, four wakes.
        assert core._submissions.qsize() == 4
        for _ in range(4):
            assert core._submissions.get_nowait() is _WAKE_COORDINATOR
            core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        release_handles()
        assert [(event.object_id, event.token) for event in core._reference_mailbox.releases] == [
            (ref.object_id, ref._local_token) for ref in refs
        ]
        fifo = core._reference_mailbox.pending
        assert fifo.qsize() <= 16
        for _ in range(16):
            try:
                event = fifo.get_nowait()
            except queue.Empty:
                break
            try:
                assert type(event) is _RetryInlineGc and event.object_id in (first.object_id, second.object_id)
                core._reference_released(event.object_id)
            finally:
                fifo.task_done()
        assert fifo.empty() and fifo.unfinished_tasks == 0
        assert all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED for ref in refs)
        assert all(core._recovery.lineage_for_object(ref.object_id) is None for ref in refs)
        assert not core._objects and not core._object_gc_obligations and not core._stored_descriptors
        assert not core._protocol_unresolved
    finally:
        release_handles()
        close_pure_core(core)
        assert current_binding() is previous_binding


@pytest.mark.unit
def test_worker_lazily_constructs_one_embedded_core_and_fences_other_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _bare_embedded_worker()
    created: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class FakeCore:
        def __init__(self, *args: object, **kwargs: object) -> None:
            created.append((args, kwargs))
            self.shutdown_calls: list[float] = []

        def _sync_node_deaths(self) -> bool:
            return True

        def shutdown(self, timeout: float) -> bool:
            self.shutdown_calls.append(timeout)
            return True

    monkeypatch.setattr(worker_module, "CoreWorker", FakeCore)
    job_id = JobID.random()

    assert worker._embedded_core is None
    first = worker._embedded_core_for(job_id)
    second = worker._embedded_core_for(job_id)

    assert first is second
    assert len(created) == 1
    args, kwargs = created[0]
    assert args == (worker.node_address, worker.node_id)
    assert kwargs["job_id"] == job_id
    assert kwargs["worker_id"] == worker.worker_id
    assert kwargs["gcs_address"] == worker.gcs_address
    assert kwargs["poll_node_deaths"] is True
    core_sink = kwargs["event_sink"]
    assert isinstance(core_sink, NonOwningEventSink)
    assert core_sink.sink is worker.event_sink
    assert core_sink.component == "worker_core"

    with pytest.raises(RuntimeError, match="one active job"):
        worker._embedded_core_for(JobID.random())
    assert len(created) == 1

    assert worker._shutdown_embedded_core(0.25)
    assert first.shutdown_calls == [0.25]
    # FakeCore.shutdown models Core ownership only; the non-owning trace view
    # must leave the Worker's physical sink available to worker_main.
    core_sink.close()
    assert worker.event_sink.emit(
        "after_embedded_core_shutdown", component="worker"
    ) is not None
    assert worker._shutdown_embedded_core(0.5)
    assert first.shutdown_calls == [0.25]


@pytest.mark.unit
def test_worker_shutdown_before_first_push_never_constructs_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _bare_embedded_worker()
    constructed = False

    class UnexpectedCore:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            nonlocal constructed
            constructed = True

    monkeypatch.setattr(worker_module, "CoreWorker", UnexpectedCore)

    assert worker._shutdown_embedded_core(0.1)
    assert not constructed
    with pytest.raises(RuntimeError, match="shut down"):
        worker._embedded_core_for(JobID.random())
    assert not constructed


@pytest.mark.unit
def test_embedded_core_prepublication_failure_uses_local_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _bare_embedded_worker()
    worker._owner_retain_admission_open = False
    events: list[str] = []

    class FakeCore:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def _sync_node_deaths(self) -> bool:
            return True

        def close_owner_retain_admission(self) -> None:
            raise RuntimeError("retain fence failed")

        def _abort_unpublished_startup(self) -> bool:
            events.append("abort")
            return True

        def shutdown(self, *_args: object, **_kwargs: object) -> bool:
            pytest.fail("unpublished Core must not use semantic shutdown")

    monkeypatch.setattr(worker_module, "CoreWorker", FakeCore)

    with pytest.raises(RuntimeError, match="retain fence failed"):
        worker._embedded_core_for(JobID.random())

    assert events == ["abort"]
    assert worker._embedded_core is None


@pytest.mark.unit
def test_foreign_job_is_fenced_before_start_lease_or_user_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reused Worker cannot start a lease it cannot bind to its Core."""

    worker = _bare_embedded_worker()
    worker._execution_lock = threading.Lock()
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepting_tasks = True
    worker._active_tasks = 0
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._failpoint = None
    worker._failpoint_triggers = 0
    first_job = JobID.random()
    worker._embedded_core = object()
    worker._embedded_core_job_id = first_job
    foreign = _push_for_job(worker.worker_id, JobID.random())
    starts: list[object] = []
    decodes: list[bytes] = []

    def unexpected_start(request: protocol.PushTask) -> None:
        starts.append(request)

    real_loads = cloudpickle.loads

    def observed_loads(payload: bytes) -> object:
        decodes.append(payload)
        return real_loads(payload)

    monkeypatch.setattr(worker, "_start_worker_lease", unexpected_start)
    monkeypatch.setattr(worker_module.cloudpickle, "loads", observed_loads)

    with pytest.raises(RuntimeError, match="one active job"):
        worker._handle_push_task(foreign)

    assert starts == []
    assert decodes == []
    assert worker._lease_bindings == {}
    assert worker._active_tasks == 0


class _DrainCore:
    def __init__(self, events: list[str], result: bool = True) -> None:
        self.events = events
        self.result = result
        self.calls: list[float] = []
        self.retain_admission_closes = 0

    def close_owner_retain_admission(self) -> None:
        self.retain_admission_closes += 1

    def shutdown(self, timeout: float) -> bool:
        self.events.append("core_shutdown")
        self.calls.append(timeout)
        return self.result


def _draining_worker(core: _DrainCore) -> WorkerServer:
    worker = object.__new__(WorkerServer)
    worker.worker_id = WorkerID.random()
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepting_tasks = True
    worker._active_tasks = 0
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._stop_event = threading.Event()
    worker._request_timeout = 1.0
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core_drain_lock = threading.Lock()
    worker._embedded_core = core
    worker._embedded_core_job_id = JobID.random()
    worker._embedded_core_stopped = False
    worker._owner_retain_admission_open = True
    return worker


def _worker_shutdown_l1_guards(monkeypatch, owned_threads):
    """Allow only the named case threads, never runtime or transport work.

    Condition/Event waiting is deliberately real in these L1 cases. Thread
    starts and joins are fenced by object identity; even swallowed unexpected
    infrastructure calls leave a constant-size sticky failure observation.
    """

    import multiprocessing.process
    import queue
    import socket
    import subprocess
    import time

    from miniray import control, core as core_module, node, transport

    failed = [False]
    started = []
    real_start, real_join = threading.Thread.start, threading.Thread.join

    def forbidden(*_args, **_kwargs):
        failed[0] = True
        pytest.fail("Worker shutdown L1 attempted unowned runtime work")

    def start(thread):
        if (thread not in owned_threads or thread in started
                or len(owned_threads) > 2 or len(started) >= 2):
            forbidden()
        started.append(thread)
        return real_start(thread)

    def join(thread, timeout=None):
        if thread not in owned_threads or timeout is None or not 0 <= timeout <= 2.0:
            forbidden()
        return real_join(thread, timeout)

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (WorkerServer, "__init__"), (WorkerServer, "_embedded_core_for"),
        (node.NodeServer, "__init__"), (control.GCSLite, "__init__"),
        (transport.TCPServer, "__init__"),
        (threading.Timer, "start"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(threading.Thread, "join", join)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    for module in (api, core_module, node, worker_module, control):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    return failed


def _observe_real_shutdown_wait(monkeypatch, worker, failed, waiting=None):
    """Observe, but never replace, this Condition's real timed wait.

    The untouched stdlib wait_for owns its original absolute deadline, including
    any spurious wakeups. Only this instance's wait method is wrapped; neither
    clock nor predicate/result is simulated. Observations are capped at 16.
    """

    real_wait = worker._lifecycle.wait
    request_timeout = worker._request_timeout
    observations = []

    def observed_wait(timeout=None):
        try:
            assert len(observations) < 16
            assert timeout is not None and 0 < timeout <= request_timeout
            assert request_timeout in (0.001, 1.0)
            assert worker._lifecycle._is_owned()
            assert worker._active_tasks == 1 and not worker._accepting_tasks
            assert not worker._owner_retain_admission_open
            assert not worker._stop_event.is_set()
            index = len(observations)
            observations.append((timeout, None))
            if waiting is not None:
                waiting.set()
            result = real_wait(timeout)
            observations[index] = (timeout, result)
            return result
        except BaseException:
            failed[0] = True
            raise

    monkeypatch.setattr(worker._lifecycle, "wait", observed_wait)
    return observations


@pytest.mark.loopback_smoke
def test_worker_shutdown_drains_task_before_embedded_core_and_clean_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two real handlers race on Worker lifecycle; Core drain is the seam."""

    import time

    threads = []
    failed = _worker_shutdown_l1_guards(monkeypatch, threads)
    events: list[str] = []
    core = _DrainCore(events)
    worker = _draining_worker(core)
    entered = threading.Event()
    release = threading.Event()
    waiting = threading.Event()
    observations = _observe_real_shutdown_wait(monkeypatch, worker, failed, waiting)
    task_result: list[object] = []
    shutdown_result: list[protocol.ShutdownAck] = []
    errors: list[BaseException] = []
    error_lock = threading.Lock()

    def record_error(exc):
        with error_lock:
            failed[0] = True
            if len(errors) < 16:
                errors.append(exc)

    def admitted(request: protocol.PushTask) -> str:
        assert request is push
        assert not events and worker._active_tasks == 1
        events.append("task_entered")
        entered.set()
        # Separate from shutdown's one-second deadline: do not let an equal
        # task-gate timeout masquerade as orderly parent completion.
        assert release.wait(2.0)
        assert events == ["task_entered"] and core.calls == []
        events.append("task_completed")
        return "reply"

    def run_task():
        try:
            reply = worker._handle_push_task(push)
            assert not task_result
            task_result.append(reply)
        except BaseException as exc:
            record_error(exc)
        finally:
            entered.set()

    def run_shutdown():
        try:
            reply = worker._handle_shutdown(shutdown_request)
            assert type(reply) is protocol.ShutdownAck and not shutdown_result
            shutdown_result.append(reply)
        except BaseException as exc:
            record_error(exc)
        finally:
            waiting.set()

    # The try owns every thread start and observation, including failure before
    # the first entry signal. No count is manually decremented during cleanup.
    try:
        push = _push_for_job(worker.worker_id, worker._embedded_core_job_id)
        shutdown_request = protocol.Shutdown.create("unit drain")
        monkeypatch.setattr(worker, "_handle_admitted_push_task", admitted)
        task = threading.Thread(target=run_task, name="worker-drain-task", daemon=False)
        threads.append(task)
        # A non-daemon shutdown handler takes the real inline finalize-exit
        # branch; the daemon TCP-handler branch would start a third thread.
        shutdown = threading.Thread(
            target=run_shutdown, name="worker-drain-shutdown", daemon=False
        )
        threads.append(shutdown)
        task.start()
        assert entered.wait(1.0)
        assert not errors and failed == [False]
        shutdown.start()
        assert waiting.wait(1.0)
        assert not errors and failed == [False]
        with worker._lifecycle:
            # Acquiring the same real lock after the probe signal proves the
            # shutdown wait released it. The actual wait_for still owns drain.
            assert observations and len(observations) <= 16
            assert worker._active_tasks == 1 and not worker._accepting_tasks
            assert shutdown.is_alive() and not shutdown_result
            assert not worker._stop_event.is_set()
            assert core.calls == [] and core.retain_admission_closes == 1
            with pytest.raises(RuntimeError, match="shutting down"):
                worker._handle_push_task(
                    _push_for_job(worker.worker_id, worker._embedded_core_job_id)
                )
            assert worker._active_tasks == 1
        release.set()
        join_deadline = time.monotonic() + 2.0
        for thread in threads:
            thread.join(max(0.0, join_deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in threads)
        assert not errors and failed == [False]
        assert task_result == ["reply"]
        assert len(shutdown_result) == 1 and shutdown_result[0].clean
        assert shutdown_result[0].request_id == shutdown_request.request_id
        assert worker._stop_event.is_set() and worker._embedded_core_stopped
        assert worker._active_tasks == 0 and core.calls == [1.0]
        assert core.retain_admission_closes == 1
        assert events == ["task_entered", "task_completed", "core_shutdown"]
        assert all(result is not None for _timeout, result in observations)
        assert not hasattr(worker, "_replies")  # original lifecycle-only seam
    finally:
        release.set()
        cleanup_deadline = time.monotonic() + 2.0
        for thread in threads:
            if thread.ident is not None and thread.is_alive():
                thread.join(max(0.0, cleanup_deadline - time.monotonic()))
        # Never re-enter owner/lifecycle locks while a surviving handler may
        # hold them. The exact-case runner owns escalation if joins fail.
        assert all(not thread.is_alive() for thread in threads)
        assert worker._active_tasks == 0
        if not worker._embedded_core_stopped:
            # Only the real _end_task can have drained the admitted handler. A
            # cleanup retry is a genuine Worker shutdown, not a fabricated ACK.
            cleanup = worker._handle_shutdown(protocol.Shutdown.create("bounded drain cleanup"))
            assert cleanup.clean
        assert len(errors) <= 16 and not errors
        assert len(events) <= 4 and len(core.calls) <= 2
        assert failed == [False]


@pytest.mark.loopback_smoke
def test_worker_shutdown_timeout_keeps_core_open_for_inflight_parent() -> None:
    """L1: caller alone performs a real one-millisecond Condition timeout."""

    with pytest.MonkeyPatch.context() as monkeypatch:
        failed = _worker_shutdown_l1_guards(monkeypatch, ())
        try:
            events: list[str] = []
            core = _DrainCore(events)
            worker = _draining_worker(core)
            worker._request_timeout = 0.001
            worker._active_tasks = 1
            observations = _observe_real_shutdown_wait(monkeypatch, worker, failed)

            reply = worker._handle_shutdown(protocol.Shutdown.create("unit timeout"))

            assert not reply.clean
            assert not worker._stop_event.is_set()
            assert core.calls == []
            assert core.retain_admission_closes == 1
            assert not worker._embedded_core_stopped
            assert 1 <= len(observations) <= 16 and observations[-1][1] is False
            assert worker._active_tasks == 1 and not worker._accepting_tasks
            assert worker._borrow_owner_core() is core and not events
            # This fixture has no running parent or background infrastructure.
            # Preserve the original pending count; do not fabricate clean drain.
        finally:
            assert failed == [False]


@pytest.mark.loopback_smoke
def test_worker_shutdown_fences_only_new_owner_retains() -> None:
    """L1: an actual timed-out wait keeps the original Core route open."""

    with pytest.MonkeyPatch.context() as monkeypatch:
        failed = _worker_shutdown_l1_guards(monkeypatch, ())
        try:
            events: list[str] = []
            core = _DrainCore(events)
            worker = _draining_worker(core)
            worker._request_timeout = 0.001
            worker._active_tasks = 1
            observations = _observe_real_shutdown_wait(monkeypatch, worker, failed)

            reply = worker._handle_shutdown(protocol.Shutdown.create("retain fence"))

            assert not reply.clean
            assert core.retain_admission_closes == 1
            assert core.calls == []
            assert worker._owner_retain_admission_open is False
            # The embedded Core is deliberately still live: owner query/release
            # and exact retain replay continue routing to this same object.
            assert worker._borrow_owner_core() is core
            assert 1 <= len(observations) <= 16 and observations[-1][1] is False
            assert worker._active_tasks == 1 and not worker._accepting_tasks
            assert not worker._stop_event.is_set() and not worker._embedded_core_stopped
            assert not events
            # Actual owner APIs belong to the separate real-owner contract; this
            # original fake-Core test proves Worker routing/fencing after wait.
        finally:
            assert failed == [False]


@pytest.mark.unit
def test_real_owner_core_retain_fence_preserves_replay_query_and_release() -> None:
    """Pure owner protocol; only the Worker's wait boundary is a model."""

    from miniray.core import _STOP
    from miniray.ownership import ObjectCollectionState, ObjectState
    from tests.unit._pure_core import close_pure_core, make_pure_core

    with pytest.MonkeyPatch.context() as monkeypatch:
        failed, forbidden = _pure_existing_core_guards(monkeypatch)
        core = None
        try:
            worker = _bare_embedded_worker()
            worker._lifecycle = threading.Condition(threading.RLock())
            worker._accepting_tasks = True
            worker._active_tasks = 1
            worker._stop_event = threading.Event()
            worker._request_timeout = 0.001
            worker._owner_retain_admission_open = True
            job_id = JobID.random()
            core = make_pure_core()
            _fence_pure_core_callbacks(core, forbidden)
            barriers = _attach_existing_pure_owner(worker, core, job_id, failed)

            # This instance-only boundary evaluates the real drain predicate
            # at a frozen logical deadline. It does not run Condition.wait_for,
            # wait on a lock, patch global time, or prove scheduler timing. The
            # original pending-parent and later completed-parent states remain
            # the inputs to Worker._handle_shutdown, whose reducer is unchanged.
            logical_now = [17.0]
            waits = []
            predicates = []

            def model_wait_for(predicate, timeout=None):
                try:
                    assert len(waits) < 2
                    assert timeout == 0.001
                    assert worker._lifecycle._is_owned()
                    start = logical_now[0]
                    deadline = start + timeout
                    assert len(predicates) < 3
                    result = bool(predicate())
                    predicates.append(result)
                    if not result:
                        assert not waits and worker._active_tasks == 1
                        logical_now[0] = deadline
                        assert len(predicates) < 3
                        result = bool(predicate())
                        predicates.append(result)
                        assert not result and logical_now[0] == deadline
                    else:
                        assert len(waits) == 1 and worker._active_tasks == 0
                        assert logical_now[0] == start
                    waits.append((start, deadline, logical_now[0], result))
                    return result
                except BaseException:
                    failed[0] = True
                    raise

            monkeypatch.setattr(worker._lifecycle, "wait_for", model_wait_for)
            shutdown_calls = []
            real_shutdown = core.shutdown

            def observed_shutdown(timeout, *, preserve_owner_protocol=False):
                try:
                    assert not shutdown_calls
                    assert timeout == 0.001 and not preserve_owner_protocol
                    shutdown_calls.append((timeout, preserve_owner_protocol))
                    return real_shutdown(
                        timeout=timeout, preserve_owner_protocol=preserve_owner_protocol
                    )
                except BaseException:
                    failed[0] = True
                    raise

            monkeypatch.setattr(core, "shutdown", observed_shutdown)
            task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 90)
            object_id = ObjectID.for_task(task_id)
            attempt_id = AttemptID(task_id, 0)
            borrower = WorkerID.random()
            # Preserve the original owner-only PENDING metadata, not a put or
            # successful producer substitution. No Core Task is submitted.
            core.owner_table.register(object_id, current_attempt=attempt_id)
            assert core.owner_table.add_borrowed_reference(
                object_id, (borrower, "borrow")
            )
            hold = protocol.TaskReferenceHold(
                protocol.TaskReferenceHoldKind.RETAINED,
                borrower,
                task_id,
                attempt_id,
            )
            first = protocol.RetainOwnedObjectForTask(
                object_id, worker.worker_id, borrower, "borrow", hold
            )
            retained = worker._handle_retain_owned_object_for_task(first)
            assert retained.accepted and retained.retained
            before = core.owner_table.snapshot(object_id)
            assert before.state is ObjectState.PENDING
            assert before.retained_tokens == frozenset({hold})
            assert before.borrowed_tokens == frozenset({(borrower, "borrow")})
            assert before.retained_borrower_tokens == frozenset({(hold, (borrower, "borrow"))})

            shutdown = worker._handle_shutdown(protocol.Shutdown.create("retain fence"))
            assert not shutdown.clean
            assert worker._active_tasks == 1 and not worker._stop_event.is_set()
            assert not worker._accepting_tasks
            assert not worker._owner_retain_admission_open
            assert not core._owner_retain_admission_open
            assert core._owner_protocol_open and core._accepting
            assert worker._borrow_owner_core() is core
            assert not worker._embedded_core_stopped
            assert shutdown_calls == [] and barriers == []
            assert predicates == [False, False]
            assert core.owner_table.snapshot(object_id) == before
            successor = protocol.TaskReferenceHold(
                protocol.TaskReferenceHoldKind.RETAINED,
                borrower,
                task_id,
                attempt_id.next(),
            )
            rejected = worker._handle_retain_owned_object_for_task(
                protocol.RetainOwnedObjectForTask(
                    object_id, worker.worker_id, borrower, "borrow", successor
                )
            )
            assert not rejected.accepted and not rejected.retained
            assert rejected.hold == successor
            assert core.owner_table.snapshot(object_id) == before
            replay = worker._handle_retain_owned_object_for_task(first)
            assert replay.accepted and not replay.retained and replay.hold == hold
            assert core.owner_table.snapshot(object_id) == before
            query = worker._handle_get_retained_owned_object(
                protocol.GetRetainedOwnedObject(
                    object_id, worker.worker_id, borrower, hold
                )
            )
            assert query.accepted and query.state is protocol.OwnedObjectState.PENDING
            assert query.hold == hold and query.current_attempt == attempt_id
            released = worker._handle_release_owned_object_for_task(
                protocol.ReleaseOwnedObjectForTask(
                    object_id, worker.worker_id, borrower, hold
                )
            )
            assert released.accepted and released.released
            after_hold = core.owner_table.snapshot(object_id)
            assert not after_hold.retained_tokens
            assert after_hold.borrowed_tokens == before.borrowed_tokens
            assert core.owner_table.retained_release_was_seen(object_id, hold)
            borrowed = worker._handle_release_borrowed_object(
                protocol.ReleaseBorrowedObject(
                    object_id, worker.worker_id, borrower, "borrow"
                )
            )
            assert borrowed.accepted and borrowed.released
            after_release = core.owner_table.snapshot(object_id)
            assert not after_release.borrowed_tokens and not after_release.retained_tokens
            assert after_release.released_borrowed_tokens == frozenset({(borrower, "borrow")})
            assert after_release.released_retained_tokens == frozenset({hold})
            assert after_release.state is ObjectState.PENDING
            assert not core.owner_table.has_active_distributed_references()
            assert core._inflight_borrow_ops == 0
            assert not core._objects and core._accepted_task_count == 0
            assert not core._protocol_unresolved and not core._object_gc_obligations
            assert core._reference_mailbox.pending.empty()
            assert core._reference_mailbox.pending.unfinished_tasks == 0

            worker._active_tasks = 0
            clean = worker._handle_shutdown(protocol.Shutdown.create("retry shutdown"))
            assert clean.clean
            assert predicates == [False, False, True] and len(waits) == 2
            assert waits[0][2] == waits[0][1] == waits[1][0]
            assert waits[1][2] == waits[1][0]
            assert shutdown_calls == [(0.001, False)] and len(barriers) == 3
            assert worker._embedded_core_stopped and worker._stop_event.is_set()
            assert worker._borrow_owner_core() is core
            assert core.owner_protocol_closed and core._sink_closed
            assert not core._reference_mailbox.accepting
            # Core finalizes its protocol, not arbitrary detached PENDING
            # metadata. No owner entries are erased to obtain a clean result;
            # this case proves neither physical GC nor actual thread teardown.
            assert core.owner_table.snapshot(object_id) == after_release
            assert core.owner_table.collection_state(object_id) is ObjectCollectionState.ACTIVE
            assert core._submissions.qsize() == 1
            assert core._submissions.get_nowait() is _STOP
            core._submissions.task_done()
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        finally:
            if core is not None:
                # On assertion failure preserve all owner state; fencing this
                # threadless fixture is never counted as successful shutdown.
                close_pure_core(core)
            assert failed == [False]


@pytest.mark.unit
def test_owner_proxy_rejections_echo_full_borrow_source_and_task_hold() -> None:
    worker = _bare_embedded_worker()
    worker._embedded_core_stopped = True
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 91)
    object_id = ObjectID.for_task(task_id)
    borrower = WorkerID.random()
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED,
        borrower,
        task_id,
        AttemptID(task_id, 0),
    )
    source = protocol.TaskHoldSource(hold)

    acquire = worker._handle_acquire_borrowed_object(
        protocol.AcquireBorrowedObject(
            object_id, worker.worker_id, borrower, source, "attempt-borrow"
        )
    )
    retain = worker._handle_retain_owned_object_for_task(
        protocol.RetainOwnedObjectForTask(
            object_id, worker.worker_id, borrower, "parent-borrow", hold
        )
    )
    query = worker._handle_get_retained_owned_object(
        protocol.GetRetainedOwnedObject(
            object_id, worker.worker_id, borrower, hold
        )
    )
    release = worker._handle_release_owned_object_for_task(
        protocol.ReleaseOwnedObjectForTask(
            object_id, worker.worker_id, borrower, hold
        )
    )
    successor = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, borrower, task_id,
        AttemptID(task_id, 1),
    )
    replacement = worker._handle_replace_retained_object_for_task(
        protocol.ReplaceRetainedObjectForTask(
            object_id, worker.worker_id, borrower, hold, successor
        )
    )

    assert not acquire.accepted and acquire.source == source
    assert not retain.accepted and retain.hold == hold
    assert not query.accepted and query.hold == hold
    assert not release.accepted and release.hold == hold
    assert (
        replacement.disposition
        is protocol.ReplaceRetainedObjectDisposition.FAILED
    )
    assert replacement.failure is protocol.ReplaceRetainedObjectFailure.OWNER_STOPPED
    assert replacement.expected_hold == hold
    assert replacement.replacement_hold == successor


@pytest.mark.unit
def test_worker_stored_pin_proxy_uses_only_an_existing_owner_core() -> None:
    """Actual pin authority, without lazy startup or a physical object store."""

    from miniray.core import _STOP
    from miniray.ownership import (
        ObjectCollectionState, ObjectState, StoredContainedReferenceDisposition,
    )
    from tests.unit._pure_core import close_pure_core, make_pure_core

    with pytest.MonkeyPatch.context() as monkeypatch:
        failed, forbidden = _pure_existing_core_guards(monkeypatch)
        core = None
        try:
            worker = _bare_embedded_worker()
            job_id = JobID.random()
            task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 92)
            outer_task = TaskID.derive(job_id, TaskID.for_driver(job_id), 93)
            child = ObjectID.for_task(task_id)
            outer = ObjectID.for_task(outer_task)
            transfer = PreparedContainedTransfer(
                child, worker.worker_id, ("127.0.0.1", 19004),
                OwnedContainedSource(worker.worker_id),
                ContainedReferenceHold(outer, WorkerID.random(), "stored:pin"),
                ContainedReferenceHold(outer, WorkerID.random(), "stored:pin"),
            )
            prepare = protocol.PrepareStoredContainedPin(transfer, worker.worker_id)

            missing = worker._handle_prepare_stored_contained_pin(prepare)
            assert not missing.accepted
            assert missing.error_kind is protocol.StoredPublicationRPCErrorKind.UNAVAILABLE
            assert missing.request == prepare
            assert worker._embedded_core is None

            # The proxy is allowed to use an existing owner, not create one.
            # Install the same real threadless Core used by owner contracts;
            # _embedded_core_for and every runtime constructor are tripwired.
            core = make_pure_core()
            _fence_pure_core_callbacks(core, forbidden)
            barriers = _attach_existing_pure_owner(worker, core, job_id, failed)
            core.owner_table.register(child, current_attempt=AttemptID(task_id, 0))
            assert core.owner_table.snapshot(child).state is ObjectState.PENDING
            prepared = worker._handle_prepare_stored_contained_pin(prepare)
            assert prepared.accepted and prepared.request == prepare
            assert prepared.disposition is StoredContainedReferenceDisposition.PREPARED
            assert core._inflight_borrow_ops == 0
            assert core.owner_table.snapshot(child).contained_holds == frozenset(
                {transfer.provisional_hold}
            )
            promote = protocol.PromoteStoredContainedPin(transfer, worker.worker_id)
            promoted = worker._handle_promote_stored_contained_pin(promote)

            assert prepared.accepted
            assert promoted.accepted and promoted.request == promote
            assert promoted.disposition is StoredContainedReferenceDisposition.PROMOTED
            assert core._inflight_borrow_ops == 0
            assert core.owner_table.snapshot(child).contained_holds == frozenset(
                {transfer.final_hold}
            )
            assert core.owner_table.contained_release_was_seen(child, transfer.provisional_hold)
            assert core.owner_table.release_contained_reference(child, transfer.final_hold)
            assert core.owner_table.contained_release_was_seen(child, transfer.final_hold)
            released = core.owner_table.snapshot(child)
            assert released.state is ObjectState.PENDING and not released.contained_holds
            assert not core.owner_table.has_active_distributed_references()
            # PENDING metadata cannot be collected. Run the real reducer once
            # to make that boundary explicit; do not publish a substitute value
            # or erase the owner table merely to allow fixture shutdown.
            core._reference_released(child)
            assert core.owner_table.snapshot(child) == released
            assert core.owner_table.collection_state(child) is ObjectCollectionState.ACTIVE
            assert not core._objects and core._accepted_task_count == 0
            assert not core._protocol_unresolved and not core._object_gc_obligations
            assert core._reference_mailbox.pending.empty()
            assert core._reference_mailbox.pending.unfinished_tasks == 0
            assert barriers == [] and core._submissions.empty()
            assert core.shutdown(timeout=1.0)
            assert len(barriers) == 3
            assert core.owner_protocol_closed and core._sink_closed
            assert not core._reference_mailbox.accepting
            assert worker._borrow_owner_core() is core
            # Direct Core shutdown is not Worker shutdown or lazy-startup
            # evidence. Its owner-only PENDING metadata and bounded replay
            # receipts remain, without any admitted Task or live reference.
            assert not worker._embedded_core_stopped
            assert core.owner_table.snapshot(child) == released
            assert core.owner_table._stored_contained_preparations == {
                (child, transfer.provisional_hold): transfer,
            }
            assert core.owner_table._stored_contained_promotions == {
                (child, transfer.final_hold): transfer,
            }
            assert core._submissions.qsize() == 1
            assert core._submissions.get_nowait() is _STOP
            core._submissions.task_done()
            assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        finally:
            if core is not None:
                close_pure_core(core)
            assert failed == [False]


@pytest.mark.unit
def test_worker_shutdown_is_unclean_when_embedded_core_cannot_drain() -> None:
    events: list[str] = []
    core = _DrainCore(events, result=False)
    worker = _draining_worker(core)

    reply = worker._handle_shutdown(
        protocol.Shutdown.create("embedded core cannot drain")
    )

    assert not reply.clean
    assert not worker._stop_event.is_set()
    assert events == ["core_shutdown"]
    assert len(core.calls) == 1
    assert not worker._embedded_core_stopped


@pytest.mark.unit
def test_closed_admission_still_serves_exact_completed_push_replay() -> None:
    """Shutdown rejects new work, not idempotent recovery of old replies."""

    worker = object.__new__(WorkerServer)
    worker.worker_id = WorkerID.random()
    worker._execution_lock = threading.Lock()
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepting_tasks = False
    worker._active_tasks = 0
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    worker._worker_core_enabled = False
    push = _push_for_job(worker.worker_id, JobID.random())
    key = (push.spec.attempt_id, push.lease_id)
    payload = cloudpickle.dumps(2)
    result = protocol.ResultDescriptor(
        push.spec.return_ids()[0],
        protocol.ResultStorage.INLINE,
        len(payload),
        push.spec.owner_worker_id,
        NodeID.random(),
        __import__("hashlib").sha256(payload).hexdigest(),
        payload,
    )
    cached = protocol.TaskReply(
        push.spec.task_id,
        push.spec.attempt_id,
        worker.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED,
        (result,),
    )
    worker._replies = {key: cached}
    worker._cached_pushes = {key: push}
    worker._completion_acked = {key}
    worker._accepted_pushes = {key: push}
    worker._push_obligations = set()

    assert worker._handle_push_task(push) is cached
    assert worker._active_tasks == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("operation", "message"),
    (
        (lambda: api.init(enable_tracing=False), "init.*Driver-only"),
        (api.shutdown, "shutdown.*Driver-only"),
        (api.trace, "trace.*Driver-only"),
    ),
    ids=("init", "shutdown", "trace"),
)
def test_worker_binding_rejects_driver_lifecycle_operations_without_mutation(
    monkeypatch: pytest.MonkeyPatch, operation: object, message: str,
) -> None:
    """A task may use Core APIs, but cannot create or destroy its cluster."""

    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    driver_runtime = SimpleNamespace(
        core_worker=object(), trace_collector=object(), sentinel=object()
    )
    before = vars(driver_runtime).copy()
    # Each API rejects a thread binding before touching a Core or cluster.
    # Preserve a real CoreWorker type without constructing its runtime.
    worker_core = object.__new__(CoreWorker)
    monkeypatch.setattr(api, "_runtime", driver_runtime)
    previous_binding = current_binding()
    try:
        with bind_runtime(worker_core) as binding:
            assert current_binding() is binding
            assert api.is_initialized()
            with pytest.raises(RuntimeError, match=message):
                operation()  # type: ignore[operator]
            assert api._runtime is driver_runtime
            assert api.is_initialized()
            assert vars(driver_runtime) == before and not vars(worker_core)
        assert api._runtime is driver_runtime
    finally:
        assert current_binding() is previous_binding
        assert api._runtime is driver_runtime and vars(driver_runtime) == before
        assert not vars(worker_core)
