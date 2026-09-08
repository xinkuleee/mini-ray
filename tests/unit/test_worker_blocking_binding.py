"""Pure contract that a PushTask binds its exact lease to public ``get``."""

from __future__ import annotations

import threading

import cloudpickle
import pytest

from miniray import protocol
from miniray.blocking import BlockingNotifier
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.runtime_binding import (
    current_core_worker, current_execution_context,
)
from miniray.worker import WorkerServer


pytestmark = pytest.mark.unit


def _push(worker_id: WorkerID) -> protocol.PushTask:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 5)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(job_id, __name__, "parent", "v1"),
        cloudpickle.dumps(lambda: None),
    )
    spec = protocol.TaskSpec(
        job_id=job_id,
        task_id=task_id,
        attempt_id=AttemptID(task_id, 2),
        function=definition.key,
        args=(),
        num_returns=1,
        resources=ResourceVector({"CPU": 1}),
        owner_worker_id=WorkerID.random(),
        parent_task_id=TaskID.for_driver(job_id),
        function_definition=definition,
    )
    return protocol.PushTask(LeaseID.random(), worker_id, spec)


def test_execution_binding_carries_exact_push_and_node_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = object.__new__(WorkerServer)
    worker.worker_id = WorkerID.random()
    worker.node_id = NodeID.random()
    worker.node_address = ("127.0.0.1", 19002)
    worker._worker_core_enabled = True
    worker._stop_event = threading.Event()
    embedded_core = object()
    monkeypatch.setattr(
        worker, "_embedded_core_for", lambda _job_id: embedded_core
    )
    push = _push(worker.worker_id)

    assert current_core_worker() is None
    assert current_execution_context() is None
    with worker._execution_binding(push):
        context = current_execution_context()
        assert context is not None
        assert current_core_worker() is embedded_core
        assert context.job_id == push.spec.job_id
        assert context.parent_task_id == push.spec.task_id
        assert context.parent_attempt_id == push.spec.attempt_id
        assert isinstance(context.blocking_notifier, BlockingNotifier)
        assert context.blocking_notifier.identity == (
            context.blocking_notifier.identity.__class__(
                push.lease_id, push.spec.task_id, push.spec.attempt_id,
                worker.worker_id, worker.node_address,
            )
        )

    assert current_core_worker() is None
    assert current_execution_context() is None
