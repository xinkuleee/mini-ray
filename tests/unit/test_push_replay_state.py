"""Pure exact-Push replay through one explicit in-memory Node outcome.

The fixture traps every RPC, thread and wait. A small unified output peer
supplies real journal/Complete metadata for success; replay still uses the
same Push and lease. No runtime constructor or background loop is started.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from miniray import protocol
from miniray.core import _DelayedReadyTask, _PendingTask, _PushRequestState
from miniray.ids import AttemptID, LeaseID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectState
from miniray.resources import AllocationToken, ResourceVector
from miniray.transport import RemoteCallError, TransportConnectionError
from tests.unit._pure_core import make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Push replay attempted runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Event, "wait"), (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _fixture():
    core = make_pure_core()
    core._registered_functions = set()
    task = TaskID.derive(core.job_id, core.driver_task_id, 0)
    attempt = AttemptID(task, 0)
    object_id = ObjectID.for_task(task, 0)
    key = protocol.FunctionKey(core.job_id, "test", "f", "v1")
    definition = protocol.FunctionDefinition.from_payload(key, b"function")
    spec = protocol.TaskSpec(
        core.job_id, task, attempt, key, (), 1, ResourceVector({"CPU": 1}),
        core.worker_id, function_definition=definition
    )
    pending = _PendingTask(object_id, spec)
    core._recovery.register_task(spec, output_ids=(object_id,))
    core._owner_table.register(object_id, current_attempt=attempt, producer_task_spec=spec)
    core._objects[object_id] = type("Waiter", (), {"event": threading.Event()})()
    worker = WorkerID.random()
    grant = protocol.GrantWorkerLease(
        LeaseID.random(), task, attempt, core.node_id, worker,
        ("127.0.0.1", 21002), AllocationToken("allocation")
    )
    push = protocol.PushTask(grant.lease_id, worker, spec)
    state = _PushRequestState(push, grant, core.node_address, grant.worker_address, 1, True)
    outputs = PureOutputRuntime(core)
    core.gcs_address = outputs.gcs_address
    core._test_outputs = outputs
    core._test_outcome_queries = []

    def rpc(address, handler, request):
        if outputs.handles(handler):
            return outputs.rpc(address, handler, request)
        assert address == core.node_address and handler == "get_worker_lease_outcome"
        assert request == protocol.GetWorkerLeaseOutcome(
            grant.lease_id, task, attempt, worker, core.worker_id, (object_id,),
        )
        core._test_outcome_queries.append(request)
        assert len(core._test_outcome_queries) <= 1
        return protocol.GetWorkerLeaseOutcomeReply(
            request.lease_id, task, attempt, worker, core.worker_id, (object_id,),
            core.node_id, True, True, state=protocol.LeaseExecutionState.RUNNING,
        )

    core._rpc = rpc
    return core, pending, state


@pytest.mark.unit
def test_remote_call_error_then_exact_replay_success(monkeypatch: pytest.MonkeyPatch) -> None:
    core, pending, state = _fixture()
    reply = core._test_outputs.complete(state.push, (b"value",))
    seen = []
    outcomes = [RemoteCallError("push_task", "RuntimeError", "late complete failed", ""), reply]

    def push(address, handler, message):
        seen.append((address, handler, message))
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(core, "_push_task_rpc", push)
    assert not core._replay_push(pending, state)
    delayed = core._submissions.get_nowait()
    assert isinstance(delayed, _DelayedReadyTask)
    assert delayed.ready.push_state is not None
    assert delayed.ready.push_state.push is state.push
    assert core._replay_push(pending, delayed.ready.push_state)
    assert seen[0] == seen[1]
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.READY_INLINE
    assert len(core._test_outcome_queries) == core._test_outputs.discoveries == 1
    assert core._test_outputs.recovery.snapshot(reply.output_publication.publication_id).adopted is not None
    assert not core._test_outputs.journal.snapshot(reply.output_publication.publication_id).result_retained


@pytest.mark.unit
def test_connection_error_after_ambiguity_preserves_exact_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, pending, state = _fixture()
    monkeypatch.setattr(
        core, "_push_task_rpc",
        lambda *_args: (_ for _ in ()).throw(TransportConnectionError("refused")),
    )
    assert not core._replay_push(pending, state)
    delayed = core._submissions.get_nowait()
    replay = delayed.ready.push_state
    assert replay is not None and replay.ambiguous
    assert replay.round == state.round + 1
    assert replay.push is state.push
    assert replay.grant is state.grant
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
    assert len(core._test_outcome_queries) == 1 and core._test_outputs.discoveries == 0


@pytest.mark.unit
def test_large_replay_round_keeps_bounded_delay_and_exact_push() -> None:
    core, pending, state = _fixture()
    state = _PushRequestState(
        state.push, state.grant, state.granting_node_address,
        state.worker_address, round=10_000, ambiguous=True,
    )
    started = __import__("time").monotonic()

    assert not core._schedule_ambiguous_push(
        pending, state.push.spec, state.push.dependencies, state
    )
    delayed = core._submissions.get_nowait()

    assert isinstance(delayed, _DelayedReadyTask)
    assert delayed.ready.push_state is state
    assert delayed.ready.push_state.push is state.push
    assert delayed.due_at - started <= 0.30
    assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
