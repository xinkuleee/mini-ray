"""Pure contracts mirrored by the bounded application-error trace smoke."""

from miniray import protocol
from miniray.recovery import RecoveryAction, TaskRecord, TaskState

import pytest


pytestmark = pytest.mark.unit


def _attempt():
    from miniray.ids import AttemptID, JobID, TaskID

    job_id = JobID(bytes([7]) * 16)
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return AttemptID(task_id, 0)


def _trace_record(
    event_id, process_id, component, event, *, cause=None, **fields
):
    return protocol.TraceRecord(
        event_id=event_id,
        timestamp_ns=1,
        process_id=process_id,
        process_sequence=1,
        component=component,
        event=event,
        entity_kind="event",
        entity_id=event_id,
        cause_event_id=cause,
        fields=tuple((key, str(value)) for key, value in fields.items()),
    )


def test_application_error_is_terminal_without_consuming_retry_budget():
    attempt = _attempt()
    record = TaskRecord(attempt.task_id, attempt, max_retries=3)

    decision = record.record_failure(
        attempt, protocol.TaskReplyStatus.APPLICATION_ERROR
    )

    assert decision.action is RecoveryAction.FAIL_APPLICATION
    assert record.current_attempt == attempt
    assert record.state is TaskState.APPLICATION_FAILED
    assert record.retries_started == 0
    assert record.retries_remaining == 3


def test_application_error_transport_edge_is_a_successful_rpc_reply():
    server_send = _trace_record(
        "send", "worker-pid", "worker", "rpc_reply_sent",
        handler="push_task", rpc_id="physical-rpc", ok="true",
    )
    client_receive = _trace_record(
        "receive", "driver-pid", "core_worker",
        "rpc_reply_received", cause=server_send.event_id,
        handler="push_task", rpc_id="physical-rpc", ok="true",
    )
    task_failure = _trace_record(
        "failure", "driver-pid", "core_worker", "task_failed",
        task_id="task-1", attempt_id="task-1:0",
        object_id="task-1:0", status="APPLICATION_ERROR",
        failure_kind="APPLICATION",
    )

    assert client_receive.cause_event_id == server_send.event_id
    assert dict(server_send.fields)["ok"] == "true"
    assert dict(client_receive.fields)["rpc_id"] == "physical-rpc"
    failure_fields = dict(task_failure.fields)
    assert failure_fields["status"] == "APPLICATION_ERROR"
    assert failure_fields["failure_kind"] == "APPLICATION"
