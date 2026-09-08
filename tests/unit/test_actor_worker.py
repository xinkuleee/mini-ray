"""Actor worker contracts with explicit synchronous and threaded scopes.

Four unit cases call the unstarted Worker's handlers synchronously with tiny
INLINE results. Five cases create real threads, wait for mailbox/event changes,
or start shutdown joiners; they remain heavy pending exact bounded review.
Actor result descriptors use the separate Actor path, not ordinary publication.
"""

from __future__ import annotations

import threading

import cloudpickle
import pytest

from miniray import protocol
from miniray.actor_state import ActorSubmitStatus
from miniray.actor_worker import ActorWorkerServer
from miniray.ids import (
    ActorGeneration,
    ActorID,
    AttemptID,
    JobID,
    NodeID,
    TaskID,
    WorkerID,
)


class Recorder:
    def __init__(self) -> None:
        self.values: list[int] = []
        self.failures = 0
        self.block_entered = threading.Event()
        self.block_release = threading.Event()

    def record(self, value: int) -> tuple[int, ...]:
        self.values.append(value)
        return tuple(self.values)

    def fail(self) -> None:
        self.failures += 1
        raise ValueError("expected application failure")

    def alive(self) -> tuple[int, int]:
        return self.failures, len(self.values)

    def block(self, value: int) -> int:
        self.block_entered.set()
        if not self.block_release.wait(2.0):
            raise RuntimeError("test did not release blocked Actor call")
        self.values.append(value)
        return value


def _definition(method_names: tuple[str, ...]) -> protocol.ActorClassDefinition:
    job_id = JobID.random()
    payload = cloudpickle.dumps(Recorder)
    return protocol.ActorClassDefinition(
        key=protocol.FunctionKey(job_id, __name__, "Recorder", "v1"),
        payload=payload,
        sha256=__import__("hashlib").sha256(payload).hexdigest(),
        method_names=method_names,
    )


def _worker() -> tuple[ActorWorkerServer, Recorder, WorkerID, WorkerID]:
    actor_id = ActorID.random()
    worker_id = WorkerID.random()
    caller_id = WorkerID.random()
    instance = Recorder()
    worker = object.__new__(ActorWorkerServer)
    worker._initialize_actor_state(
        actor_id=actor_id,
        generation=ActorGeneration(actor_id, 0),
        worker_id=worker_id,
        instance=instance,
        method_names=("record", "fail", "alive", "block"),
        node_id=NodeID.random(),
        node_address=("127.0.0.1", 19000),
        inline_threshold=1024,
    )
    return worker, instance, worker_id, caller_id


def _request(
    worker: ActorWorkerServer,
    caller_id: WorkerID,
    sequence: int,
    method_name: str,
    *arguments: object,
    generation: ActorGeneration | None = None,
) -> protocol.ActorCallRequest:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), sequence)
    return protocol.ActorCallRequest(
        actor_id=worker.actor_id,
        generation=generation or worker.generation,
        caller_worker_id=caller_id,
        sequence=sequence,
        method_name=method_name,
        task_id=task_id,
        attempt_id=AttemptID(task_id, 0),
        owner_worker_id=caller_id,
        arguments=cloudpickle.dumps((tuple(arguments), {})),
    )


def _result(reply: protocol.ActorCallReply) -> object:
    assert reply.task_reply.status is protocol.TaskReplyStatus.SUCCEEDED
    descriptor = reply.task_reply.results[0]
    assert descriptor.storage is protocol.ResultStorage.INLINE
    assert descriptor.inline_data is not None
    return cloudpickle.loads(descriptor.inline_data)


@pytest.mark.heavy
def test_gap_waiter_does_not_block_missing_sequence_and_fifo_execution() -> None:
    worker, instance, _, caller = _worker()
    sequence_one = _request(worker, caller, 1, "record", 1)
    sequence_zero = _request(worker, caller, 0, "record", 0)
    one_buffered = threading.Event()
    original_submit = worker._mailbox.submit_call

    def observed_submit(call):
        submission = original_submit(call)
        if call.sequence == 1 and submission.status is ActorSubmitStatus.BUFFERED:
            one_buffered.set()
        return submission

    worker._mailbox.submit_call = observed_submit  # type: ignore[method-assign]
    reply_one: list[protocol.ActorCallReply] = []
    thread = threading.Thread(
        target=lambda: reply_one.append(worker._handle_actor_call(sequence_one))
    )
    thread.start()
    assert one_buffered.wait(1.0), "sequence 1 was not buffered"

    reply_zero = worker._handle_actor_call(sequence_zero)
    thread.join(1.0)

    assert not thread.is_alive(), "gap waiter deadlocked the missing call"
    assert instance.values == [0, 1]
    assert _result(reply_zero) == (0,)
    assert _result(reply_one[0]) == (0, 1)


@pytest.mark.unit
def test_duplicate_completed_call_returns_cached_reply_without_reexecution() -> None:
    worker, instance, _, caller = _worker()
    request = _request(worker, caller, 0, "record", 7)

    first = worker._handle_actor_call(request)
    duplicate = worker._handle_actor_call(request)

    assert duplicate == first
    assert _result(duplicate) == (7,)
    assert instance.values == [7]


@pytest.mark.unit
def test_stale_generation_is_fenced_before_method_execution() -> None:
    worker, instance, _, caller = _worker()
    stale = _request(
        worker,
        caller,
        0,
        "record",
        9,
        generation=ActorGeneration(worker.actor_id, 1),
    )

    reply = worker._handle_actor_call(stale)

    assert reply.task_reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert reply.task_reply.error is not None
    assert reply.task_reply.error.type_name == "StaleGenerationError"
    assert instance.values == []


@pytest.mark.unit
def test_application_error_is_cached_and_actor_survives_for_next_call() -> None:
    worker, instance, _, caller = _worker()
    failing = _request(worker, caller, 0, "fail")

    first_error = worker._handle_actor_call(failing)
    duplicate_error = worker._handle_actor_call(failing)
    alive = worker._handle_actor_call(_request(worker, caller, 1, "alive"))

    assert first_error.task_reply.status is protocol.TaskReplyStatus.APPLICATION_ERROR
    assert first_error.task_reply.error is not None
    assert first_error.task_reply.error.type_name == "ValueError"
    assert duplicate_error == first_error
    assert instance.failures == 1
    assert _result(alive) == (1, 0)


@pytest.mark.heavy
def test_shutdown_drains_inflight_call_but_abandons_gap_waiter() -> None:
    worker, instance, _, caller = _worker()
    inflight = _request(worker, caller, 0, "block", 11)
    gap_caller = WorkerID.random()
    gap = _request(worker, gap_caller, 1, "record", 99)
    replies: dict[str, protocol.ActorCallReply] = {}
    gap_buffered = threading.Event()
    original_submit = worker._mailbox.submit_call

    def observed_submit(call):
        submission = original_submit(call)
        if call.caller_id == gap_caller and submission.status is ActorSubmitStatus.BUFFERED:
            gap_buffered.set()
        return submission

    worker._mailbox.submit_call = observed_submit  # type: ignore[method-assign]
    inflight_thread = threading.Thread(
        target=lambda: replies.__setitem__(
            "inflight", worker._handle_actor_call(inflight)
        )
    )
    gap_thread = threading.Thread(
        target=lambda: replies.__setitem__("gap", worker._handle_actor_call(gap))
    )
    inflight_thread.start()
    assert instance.block_entered.wait(1.0)
    gap_thread.start()
    assert gap_buffered.wait(1.0)

    shutdown_reply: list[protocol.ShutdownAck] = []
    shutdown_thread = threading.Thread(
        target=lambda: shutdown_reply.append(
            worker._handle_shutdown(protocol.Shutdown.create("test drain"))
        )
    )
    shutdown_thread.start()
    with worker._condition:
        assert worker._condition.wait_for(lambda: not worker._accepting, timeout=1.0)

    gap_thread.join(1.0)
    assert not gap_thread.is_alive(), "gap-buffered handler was not abandoned"
    assert replies["gap"].task_reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert replies["gap"].task_reply.error is not None
    assert replies["gap"].task_reply.error.type_name == "RuntimeShuttingDownError"
    assert shutdown_thread.is_alive(), "shutdown acknowledged before inflight work drained"
    assert not worker._stop_event.is_set()

    instance.block_release.set()
    inflight_thread.join(1.0)
    shutdown_thread.join(1.0)

    assert not inflight_thread.is_alive()
    assert not shutdown_thread.is_alive()
    assert _result(replies["inflight"]) == 11
    assert shutdown_reply[0].clean
    assert worker._stop_event.is_set()


@pytest.mark.heavy
def test_shutdown_timeout_is_not_reported_as_clean_or_stopped() -> None:
    worker, instance, _, caller = _worker()
    worker._request_timeout = 0.02
    request = _request(worker, caller, 0, "block", 5)
    call_thread = threading.Thread(target=worker._handle_actor_call, args=(request,))
    call_thread.start()
    assert instance.block_entered.wait(1.0)

    reply = worker._handle_shutdown(protocol.Shutdown.create("test timeout"))

    assert not reply.clean
    assert "timed out" in reply.detail
    assert not worker._stop_event.is_set()
    instance.block_release.set()
    call_thread.join(1.0)
    assert not call_thread.is_alive()


@pytest.mark.heavy
def test_clean_shutdown_defers_stop_until_daemon_handler_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _, _, _ = _worker()
    request = protocol.Shutdown.create("daemon handler handoff")
    joiner_started = threading.Event()
    handler_joined = threading.Event()
    allow_stop = threading.Event()
    handler_returned = threading.Event()
    replies: list[protocol.ShutdownAck] = []
    original = worker._release_wait_after_shutdown_handler

    def gated_handoff(handler_thread: threading.Thread) -> None:
        joiner_started.set()
        handler_thread.join()
        handler_joined.set()
        assert allow_stop.wait(1.0)
        original(handler_thread)

    monkeypatch.setattr(
        worker, "_release_wait_after_shutdown_handler", gated_handoff
    )

    def invoke() -> None:
        replies.append(worker._handle_shutdown(request))
        handler_returned.set()

    handler = threading.Thread(target=invoke, daemon=True)
    handler.start()
    assert joiner_started.wait(1.0)
    assert handler_returned.wait(1.0)
    handler.join(1.0)
    assert handler_joined.wait(1.0)

    assert replies == [
        protocol.ShutdownAck(
            request.request_id,
            "actor-worker:{}".format(worker.worker_id),
            True,
            detail="actor mailbox drained; actor worker stopping",
        )
    ]
    assert not worker._stop_event.is_set()

    allow_stop.set()
    assert worker._stop_event.wait(1.0)


@pytest.mark.heavy
def test_replayed_clean_shutdown_schedules_one_daemon_exit_joiner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _, _, _ = _worker()
    request = protocol.Shutdown.create("idempotent daemon handoff")
    joiner_calls = 0
    joiner_started = threading.Event()
    allow_stop = threading.Event()
    replies: list[protocol.ShutdownAck] = []
    original = worker._release_wait_after_shutdown_handler

    def gated_handoff(handler_thread: threading.Thread) -> None:
        nonlocal joiner_calls
        joiner_calls += 1
        joiner_started.set()
        handler_thread.join()
        assert allow_stop.wait(1.0)
        original(handler_thread)

    monkeypatch.setattr(
        worker, "_release_wait_after_shutdown_handler", gated_handoff
    )

    first = threading.Thread(
        target=lambda: replies.append(worker._handle_shutdown(request)),
        daemon=True,
    )
    first.start()
    assert joiner_started.wait(1.0)
    first.join(1.0)
    second = threading.Thread(
        target=lambda: replies.append(worker._handle_shutdown(request)),
        daemon=True,
    )
    second.start()
    second.join(1.0)

    assert not first.is_alive() and not second.is_alive()
    assert len(replies) == 2 and replies[0] == replies[1]
    assert replies[0].clean
    assert joiner_calls == 1
    assert not worker._stop_event.is_set()

    allow_stop.set()
    assert worker._stop_event.wait(1.0)


@pytest.mark.unit
def test_clean_shutdown_direct_call_stops_immediately() -> None:
    worker, _, _, _ = _worker()

    reply = worker._handle_shutdown(protocol.Shutdown.create("direct call"))

    assert reply.clean
    assert worker._shutdown_exit_scheduled
    assert worker._stop_event.is_set()
