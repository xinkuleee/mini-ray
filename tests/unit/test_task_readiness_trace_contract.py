"""Pure schema/order contract for required ordinary-task readiness events."""

from __future__ import annotations

import hashlib
import queue

import pytest

from miniray import protocol
from miniray.core import CoreWorker, _PendingTask, _ReadyTask
from miniray.ids import AttemptID, JobID, NodeID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.trace import MemoryEventSink


pytestmark = pytest.mark.unit


def _record(event: str, sequence: int, **fields: object) -> protocol.TraceRecord:
    return protocol.TraceRecord(
        event_id="driver:{}".format(sequence),
        timestamp_ns=1,
        process_id="driver",
        process_sequence=sequence,
        component="core_worker",
        event=event,
        entity_kind="event",
        entity_id="driver:{}".format(sequence),
        fields=tuple((name, str(value)) for name, value in fields.items()),
    )


def test_required_readiness_events_bind_task_attempt_and_output_slot() -> None:
    records = (
        _record(
            "dependency_ready", 1, task_id="task", attempt_id="task:0",
            dependency_count=1, dependency_ids=("input",),
        ),
        _record("lease_requested", 2, task_id="task", attempt_id="task:0"),
        _record("task_finished", 3, task_id="task", attempt_id="task:0"),
        _record(
            "object_ready", 4, task_id="task", attempt_id="task:0",
            object_id="task:0", return_index=0, storage="INLINE",
        ),
    )

    dependency, lease, finished, ready = records
    assert dependency.process_sequence < lease.process_sequence
    assert finished.process_sequence < ready.process_sequence
    dependency_fields = dict(dependency.fields)
    ready_fields = dict(ready.fields)
    assert dependency_fields["task_id"] == ready_fields["task_id"]
    assert dependency_fields["attempt_id"] == ready_fields["attempt_id"]
    assert dependency_fields["dependency_count"] == "1"
    assert ready_fields == {
        "task_id": "task",
        "attempt_id": "task:0",
        "object_id": "task:0",
        "return_index": "0",
        "storage": "INLINE",
    }


def _pending(*, reconstruction: bool = False) -> tuple[CoreWorker, _PendingTask]:
    core = object.__new__(CoreWorker)
    core.event_sink = MemoryEventSink(process_id=lambda: 17)
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    attempt = AttemptID(task, 2 if reconstruction else 0)
    spec = protocol.TaskSpec(
        job, task, attempt,
        protocol.FunctionKey(job, __name__, "producer", "v1"),
        (), 1, ResourceVector({"CPU": 1}), WorkerID.random(),
    )
    return core, _PendingTask(
        spec.return_ids()[0], spec,
    )


def _result(
    pending: _PendingTask, object_id: object, storage: protocol.ResultStorage
) -> protocol.ResultDescriptor:
    payload = b"value"
    return protocol.ResultDescriptor(
        object_id, storage, len(payload), pending.spec.owner_worker_id,
        NodeID.random(), hashlib.sha256(payload).hexdigest(),
        payload if storage is protocol.ResultStorage.INLINE else None,
    )


@pytest.mark.parametrize("reconstruction", (False, True))
def test_object_ready_uses_only_the_validated_reply_manifest(
    reconstruction: bool,
) -> None:
    core, pending = _pending(reconstruction=reconstruction)
    results = tuple(
        _result(
            pending, object_id,
            protocol.ResultStorage.INLINE
            if position == 0 else protocol.ResultStorage.OBJECT_STORE,
        )
        for position, object_id in enumerate(pending.output_ids)
    )
    reply = protocol.TaskReply(
        pending.task_id, pending.spec.attempt_id, WorkerID.random(),
        protocol.TaskReplyStatus.SUCCEEDED, results,
    )

    core._emit_published_task_reply(pending, reply)

    events = core.event_sink.events
    assert [event.name for event in events] == [
        "task_finished", *(["object_ready"] * len(results))
    ]
    assert tuple(
        event.attributes["object_id"] for event in events[1:]
    ) == tuple(str(object_id) for object_id in pending.output_ids)
    assert tuple(
        event.attributes["return_index"] for event in events[1:]
    ) == tuple(object_id.return_index for object_id in pending.output_ids)
    assert all(
        event.process_seq > events[0].process_seq for event in events[1:]
    )


def test_dependency_ready_is_emitted_once_after_successful_preparation() -> None:
    core, pending = _pending()
    core._ready_tasks = queue.Queue()
    core._is_current_task_pending = lambda _pending: True
    core._raise_if_placement_group_lost = lambda _pending: None
    core._dependencies_ready = lambda _pending: True
    core._prepare_task_dependencies = (
        lambda spec, _guards: (spec, (), ())
    )

    core._admit_or_block(pending)

    ready = core._ready_tasks.get_nowait()
    assert isinstance(ready, _ReadyTask)
    events = core.event_sink.events
    assert len(events) == 1 and events[0].name == "dependency_ready"
    assert events[0].attributes == {
        "task_id": str(pending.task_id),
        "attempt_id": str(pending.spec.attempt_id),
        "dependency_ids": (),
        "dependency_count": 0,
    }
