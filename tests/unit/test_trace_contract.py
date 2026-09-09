"""Pure synthetic matcher tests, not evidence of runtime publication success.

Each fixture describes fewer than 200 observations. No runtime, thread, socket
or timer is started; the only I/O is loading the packaged contract JSON. The
successful shape includes nested handler scopes and exact semantic ACK causes
so the matcher cannot substitute another publication's transport round trip.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from miniray import protocol
from miniray.trace_contract import (
    APPLICATION_ERROR_TRACE_CONTRACT,
    SUCCESS_TRACE_CONTRACT,
    TRACE_CONTRACT_SCHEMA,
    TraceContract,
    TraceContractError,
    TraceContractMismatch,
    load_trace_contract,
    render_trace_sequence,
)


pytestmark = pytest.mark.unit


def _record(
    event_id: str,
    pid: str,
    sequence: int,
    component: str,
    event: str,
    *,
    cause: str | None = None,
    timestamp: int = 1,
    **fields: object,
) -> protocol.TraceRecord:
    return protocol.TraceRecord(
        event_id=event_id,
        timestamp_ns=timestamp,
        process_id=pid,
        process_sequence=sequence,
        component=component,
        event=event,
        entity_kind="event",
        entity_id=event_id,
        cause_event_id=cause,
        fields=tuple((key, str(value)) for key, value in fields.items()),
    )


def _rpc(
    prefix: str,
    client: str,
    server: str,
    handler: str,
    sequences: tuple[int, int, int, int],
    *,
    cause: str | None = None,
    handler_span: bool = False,
    finished_cause: str | None = None,
) -> tuple[protocol.TraceRecord, ...]:
    sent_sequence, received_sequence, reply_sequence, done_sequence = sequences
    rpc_id = "volatile-rpc-{}".format(prefix)
    client_component = (
        "transport" if client == "worker-process" else _component(client)
    )
    sent = _record(
        "{}-sent".format(prefix), client, sent_sequence,
        client_component, "rpc_request_sent", cause=cause,
        handler=handler, rpc_id=rpc_id, delivery="unknown",
    )
    received = _record(
        "{}-received".format(prefix), server, received_sequence,
        _component(server), "rpc_request_received", cause=sent.event_id,
        handler=handler, rpc_id=rpc_id,
    )
    span = ()
    if handler_span:
        started = _record(
            "{}-started".format(prefix), server, received_sequence + 1,
            _component(server), "rpc_handler_started", cause=received.event_id,
            handler=handler, rpc_id=rpc_id,
        )
        finished = _record(
            "{}-finished".format(prefix), server, reply_sequence - 1,
            _component(server), "rpc_handler_finished",
            cause=finished_cause or started.event_id,
            handler=handler, rpc_id=rpc_id,
        )
        span = (started, finished)
    reply = _record(
        "{}-reply".format(prefix), server, reply_sequence,
        _component(server), "rpc_reply_sent",
        cause=span[-1].event_id if span else received.event_id,
        handler=handler, rpc_id=rpc_id, ok="true", delivery="unknown",
    )
    done = _record(
        "{}-done".format(prefix), client, done_sequence,
        client_component, "rpc_reply_received", cause=reply.event_id,
        handler=handler, rpc_id=rpc_id, ok="true",
    )
    return sent, received, *span, reply, done


def _component(process: str) -> str:
    return {
        "gcs-process": "gcs",
        "driver-process": "core_worker",
        "node-process": "node",
        "worker-process": "worker",
        "owner-process": "owner_service",
    }[process]


def _bounded_records(records):
    records = tuple(records)
    assert len(records) < 200
    assert len({record.event_id for record in records}) == len(records)
    assert len({(record.process_id, record.process_sequence) for record in records}) == len(records)
    return records


def _success_records():
    """Synthetic success with real-shaped nesting, not simulated reducers."""
    task, attempt, obj = "random-task-id", "random-task-id:0", "random-task-id:0"
    driver, node, worker, gcs = (
        "driver-process", "node-process", "worker-process", "gcs-process",
    )
    identity = dict(
        task_id=task, attempt_id=attempt, lease_id="random-lease-id",
        manifest_digest="random-manifest-digest",
    )

    def fact(event_id, process, sequence, event, *, cause=None, **fields):
        return _record(
            event_id, process, sequence, _component(process), event,
            cause=cause, **identity, **fields,
        )

    records = [
        _record("gcs-registered", gcs, 100, "gcs", "node_registered"),
        _record("node-ready", node, 100, "node", "process_ready"),
        _record("worker-ready", worker, 100, "worker", "process_ready"),
        _record(
            "submitted", driver, 100, "core_worker", "task_submitted",
            task_id=task, attempt_id=attempt, object_id=obj,
        ),
        _record(
            "ready", driver, 200, "core_worker", "dependency_ready",
            task_id=task, attempt_id=attempt, dependency_count=0,
        ),
        _record(
            "lease-requested", driver, 300, "core_worker", "lease_requested",
            task_id=task, attempt_id=attempt, lease_id="random-lease-id",
        ),
        *_rpc(
            "lease", driver, node, "request_worker_lease", (400, 200, 300, 500),
            handler_span=True,
        ),
        _record(
            "lease-granted", driver, 600, "core_worker", "lease_granted",
            task_id=task, lease_id="random-lease-id",
            node_id="random-node-id", worker_id="random-worker-id",
        ),
        _record(
            "pushed", driver, 700, "core_worker", "task_pushed",
            task_id=task, worker_id="random-worker-id",
        ),
        *_rpc(
            "push", driver, worker, "push_task", (800, 200, 1100, 1200),
            handler_span=True, finished_cause="worker-terminal",
        ),
        _record(
            "worker-started", worker, 300, "worker", "task_started",
            cause="push-started", task_id=task, attempt_id=attempt,
        ),
        *_rpc(
            "start", worker, node, "start_worker_lease", (400, 400, 500, 500),
            cause="worker-started", handler_span=True,
        ),
        *_rpc(
            "prepare", worker, node, "prepare_output_publication",
            (600, 600, 1100, 700), cause="start-done",
            handler_span=True, finished_cause="handoff-done",
        ),
        *_rpc(
            "handoff", node, "owner-process", "register_output_handoff",
            (700, 1000, 1100, 800), cause="prepare-started", handler_span=True,
        ),
        *_rpc(
            "complete", worker, node, "complete_worker_lease",
            (800, 1200, 1300, 900), cause="prepare-done",
            handler_span=True, finished_cause="node-complete",
        ),
        fact(
            "node-complete", node, 1250, "output_lease_completed",
            cause="complete-started", status="SUCCEEDED",
            state="COMPLETED", released="true",
        ),
        _record(
            "worker-terminal", worker, 1000, "worker", "task_succeeded",
            cause="complete-done", task_id=task, attempt_id=attempt,
        ),
        # This is the base branch's actual semantic order: owner CAS/wake
        # after direct Push; Node metadata retirement follows owner readiness.
        fact("owner-ready", driver, 1600, "output_owner_ready", return_count=1),
        *_rpc(
            "retire", driver, node, "ack_output_publication_adopted",
            (1900, 1400, 1500, 2000), handler_span=True,
        ),
        fact(
            "payload-retired", driver, 2010, "output_payload_retired",
            cause="retire-done",
        ),
        _record(
            "owner-terminal", driver, 2100, "core_worker", "task_finished",
            task_id=task, attempt_id=attempt, object_id=obj, status="SUCCEEDED",
        ),
        _record(
            "object-ready", driver, 2200, "core_worker", "object_ready",
            task_id=task, attempt_id=attempt, object_id=obj, return_index=0,
            storage="INLINE",
        ),
    ]
    assert len(records) == 57
    return _bounded_records(records), task


def _ordinary_records(*, application_error: bool = False):
    if not application_error:
        return _success_records()
    task = "random-task-id"
    attempt = "random-task-id:0"
    obj = "random-task-id:0"
    driver = "driver-process"
    node = "node-process"
    worker = "worker-process"
    lease = _rpc(
        "lease", driver, node, "request_worker_lease", (4, 3, 4, 5)
    )
    push = _rpc("push", driver, worker, "push_task", (8, 2, 13, 12))
    start = _rpc(
        "start", worker, node, "start_worker_lease", (5, 7, 8, 6)
    )
    complete = _rpc(
        "complete", worker, node, "complete_worker_lease",
        (8, 10, 11, 9),
    )
    records = [
        _record("gcs-registered", "gcs-process", 1, "gcs", "node_registered"),
        _record("node-ready", node, 1, "node", "process_ready"),
        _record("worker-ready", worker, 1, "worker", "process_ready"),
        _record(
            "submitted", driver, 1, "core_worker", "task_submitted",
            task_id=task, attempt_id=attempt, object_id=obj,
        ),
        _record(
            "ready", driver, 2, "core_worker", "dependency_ready",
            task_id=task, attempt_id=attempt, dependency_count=0,
        ),
        _record(
            "lease-requested", driver, 3, "core_worker",
            "lease_requested", task_id=task, attempt_id=attempt,
            lease_id="random-lease-id",
        ),
        *lease,
        _record(
            "lease-granted", driver, 6, "core_worker", "lease_granted",
            task_id=task, lease_id="random-lease-id",
            node_id="random-node-id", worker_id="random-worker-id",
        ),
        _record(
            "pushed", driver, 7, "core_worker", "task_pushed",
            task_id=task, worker_id="random-worker-id",
        ),
        *push,
        _record(
            "worker-started", worker, 3, "worker", "task_started",
            task_id=task, attempt_id=attempt,
        ),
        *start,
        *complete,
    ]
    records.extend((
        _record(
            "worker-terminal", worker, 10, "worker",
            "task_application_failed", task_id=task, attempt_id=attempt,
        ),
        _record(
            "owner-terminal", driver, 13, "core_worker", "task_failed",
            task_id=task, attempt_id=attempt, object_id=obj,
            status="APPLICATION_ERROR", failure_kind="APPLICATION",
        ),
    ))
    return _bounded_records(records), task


@pytest.mark.parametrize(
    ("name", "application_error"),
    (
        (SUCCESS_TRACE_CONTRACT, False),
        (APPLICATION_ERROR_TRACE_CONTRACT, True),
    ),
)
def test_packaged_contracts_match_semantics_without_comparing_runtime_ids_or_time(
    name: str, application_error: bool
) -> None:
    records, task_id = _ordinary_records(application_error=application_error)
    contract = load_trace_contract(name)

    match = contract.match(reversed(records), task_id=task_id).require()

    assert match.ok
    assert match.bindings["attempt_id"] == "random-task-id:0"
    assert match.bindings["worker_id"] == "random-worker-id"
    # Deliberately arbitrary and non-monotonic timestamps do not participate.
    changed_time = tuple(
        protocol.TraceRecord(
            record.event_id, 10**18 - index, record.process_id,
            record.process_sequence, record.component, record.event,
            record.entity_kind, record.entity_id, record.cause_event_id,
            record.fields,
        )
        for index, record in enumerate(records)
    )
    assert contract.match(changed_time, task_id=task_id).ok


def test_renderer_is_a_stable_teaching_sequence_not_a_raw_trace_dump() -> None:
    records, task_id = _ordinary_records()
    rendered = render_trace_sequence(
        records, load_trace_contract(SUCCESS_TRACE_CONTRACT), task_id=task_id
    )

    assert "Owner CoreWorker -> NodeServer : request_worker_lease" in rendered
    assert "Execution Worker -> NodeServer : complete_worker_lease" in rendered
    assert "task_finished [status=SUCCEEDED]" in rendered
    assert "object_ready [storage=INLINE]" in rendered
    lines = rendered.splitlines()[1:]

    def position(fragment):
        matches = [index for index, line in enumerate(lines) if fragment in line]
        assert len(matches) == 1, fragment
        return matches[0]

    # B Prepare encloses Node -> owner registration. GCS only participates
    # in membership; no ordinary result INTENT/ARM transaction is normalized in.
    ordered = (
        "Owner CoreWorker -> Execution Worker : push_task [request_sent]",
        "Execution Worker : task_started",
        "Execution Worker -> NodeServer : prepare_output_publication [request_sent]",
        "NodeServer -> OwnerService (Driver owner) : register_output_handoff [request_sent]",
        "OwnerService (Driver owner) -> NodeServer : register_output_handoff [reply_received, transport_ok=true]",
        "NodeServer -> Execution Worker : prepare_output_publication [reply_received, transport_ok=true]",
        "Execution Worker -> NodeServer : complete_worker_lease [request_sent]",
        "NodeServer : output_lease_completed [status=SUCCEEDED, released=true]",
        "NodeServer -> Execution Worker : complete_worker_lease [reply_received, transport_ok=true]",
        "Execution Worker : task_succeeded",
        "Execution Worker -> Owner CoreWorker : push_task [reply_received, transport_ok=true]",
        "Owner CoreWorker : output_owner_ready [return_count=1]",
        "NodeServer -> Owner CoreWorker : ack_output_publication_adopted [reply_received, transport_ok=true]",
        "Owner CoreWorker : output_payload_retired",
        "Owner CoreWorker : task_finished [status=SUCCEEDED]",
        "Owner CoreWorker : object_ready [storage=INLINE]",
    )
    positions = tuple(position(fragment) for fragment in ordered)
    assert positions == tuple(sorted(positions))
    assert "report_output_publication" not in rendered
    assert "round trip" not in rendered
    for volatile in (
        "random-task-id",
        "random-worker-id",
        "random-lease-id",
        "random-manifest-digest",
        "volatile-rpc",
        "driver-process",
        "1000000000000000000",
    ):
        assert volatile not in rendered


def test_application_error_contract_rejects_retry_or_system_failure() -> None:
    records, task_id = _ordinary_records(application_error=True)
    forbidden = _record(
        "retry", "driver-process", 14, "core_worker", "task_retried",
        task_id=task_id,
    )
    match = load_trace_contract(APPLICATION_ERROR_TRACE_CONTRACT).match(
        records + (forbidden,), task_id=task_id
    )

    assert not match.ok
    with pytest.raises(TraceContractMismatch, match="forbidden event"):
        match.require()
    rendered = render_trace_sequence(
        records, load_trace_contract(APPLICATION_ERROR_TRACE_CONTRACT), task_id=task_id,
    )
    assert "request; round trip verified" in rendered
    assert "publication" not in rendered
    assert "stage=" not in rendered


def test_contract_rejects_rpc_name_match_without_concrete_cross_process_edge() -> None:
    records, task_id = _ordinary_records()
    broken = tuple(
        protocol.TraceRecord(
            record.event_id, record.timestamp_ns, record.process_id,
            record.process_sequence, record.component, record.event,
            record.entity_kind, record.entity_id,
            ("not-the-send-event" if record.event_id == "push-received"
             else record.cause_event_id),
            record.fields,
        )
        for record in records
    )

    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(
        broken, task_id=task_id
    )
    assert not match.ok
    assert any("push_task" in violation for violation in match.violations)


_STAGE_RPCS = ("register_handoff_rpc", "complete_rpc", "retire_rpc")
_ACK_IDS = ("node-complete", "owner-ready", "payload-retired")


def _change_record(records, event_id, *, fields=None, **changes):
    assert sum(record.event_id == event_id for record in records) == 1
    changed = []
    for record in records:
        if record.event_id == event_id:
            if fields is not None:
                values = dict(record.fields)
                values.update(fields)
                changes = dict(changes, fields=tuple(values.items()))
            record = replace(record, **changes)
        changed.append(record)
    return _bounded_records(changed)


def _terminal_echo(records, prefix, *, fields=None):
    """Independent synthetic retirement round trip and its identity fact."""
    original = next(record for record in records if record.event_id == "payload-retired")
    values = dict(original.fields)
    values.update(fields or {})
    echo = _rpc(prefix, "driver-process", "node-process", "ack_output_publication_adopted",
                (1700, 1320, 1350, 1800), handler_span=True)
    ack = _record(prefix + "-ack", "driver-process", 1810, "core_worker",
                  "output_payload_retired", cause=prefix + "-done", **values)
    return (*echo, ack)


@pytest.mark.parametrize("event_id", _ACK_IDS)
def test_success_contract_requires_each_publication_fact(event_id) -> None:
    records, task_id = _ordinary_records()
    missing = tuple(record for record in records if record.event_id != event_id)
    assert not load_trace_contract(SUCCESS_TRACE_CONTRACT).match(missing, task_id=task_id).ok


@pytest.mark.parametrize(("event_id", "fields"), (
    ("node-complete", {"released": "false"}),
    ("node-complete", {"state": "STARTED"}),
    ("owner-ready", {"return_count": "0"}),
))
def test_transport_ok_does_not_replace_accepted_publication_facts(event_id, fields) -> None:
    records, task_id = _ordinary_records()
    changed = _change_record(records, event_id, fields=fields)
    assert all(dict(record.fields)["ok"] == "true" for record in changed if record.event == "rpc_reply_received")
    assert not load_trace_contract(SUCCESS_TRACE_CONTRACT).match(changed, task_id=task_id).ok


@pytest.mark.parametrize("field", ("task_id", "attempt_id", "lease_id", "manifest_digest"))
@pytest.mark.parametrize("keep_original", (False, True))
def test_other_publication_ack_cannot_substitute_or_hide_the_selected_report(field, keep_original) -> None:
    records, task_id = _ordinary_records()
    echo = _terminal_echo(records, "foreign-retire", fields={field: "foreign-" + field})
    if not keep_original:
        records = tuple(record for record in records if not record.event_id.startswith("retire-") and record.event_id != "payload-retired")
    mixed = _bounded_records((*records, *echo))
    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(reversed(mixed), task_id=task_id)
    assert match.ok is keep_original
    if keep_original:
        assert match.rpc_matches["retire_rpc"][0].event_id == "retire-sent"


@pytest.mark.parametrize(("event_id", "cause"), (
    ("handoff-received", "not-the-send-event"),
    ("handoff-done", "complete-reply"),
    ("retire-done", "handoff-reply"),
    ("payload-retired", "handoff-done"),
    ("complete-started", "start-received"),
    ("node-complete", "start-started"),
    ("complete-finished", "start-started"),
))
def test_publication_rpc_requires_its_exact_ack_and_server_cause_chain(event_id, cause) -> None:
    records, task_id = _ordinary_records()
    broken = _change_record(records, event_id, cause_event_id=cause)
    assert not load_trace_contract(SUCCESS_TRACE_CONTRACT).match(broken, task_id=task_id).ok


@pytest.mark.parametrize(("event_id", "other_rpc"), (("handoff-received", "prepare"),
    ("complete-reply", "handoff"), ("retire-done", "complete")))
def test_publication_round_trip_cannot_mix_different_rpc_ids(event_id, other_rpc) -> None:
    records, task_id = _ordinary_records()
    broken = _change_record(records, event_id, fields={"rpc_id": "volatile-rpc-" + other_rpc})
    assert not load_trace_contract(SUCCESS_TRACE_CONTRACT).match(broken, task_id=task_id).ok


@pytest.mark.parametrize(("first", "second"), (("owner-ready", "retire-sent"), ("payload-retired", "owner-terminal")))
def test_publication_stages_cannot_be_reversed_inside_valid_round_trips(first, second) -> None:
    records, task_id = _ordinary_records()
    sequences = {record.event_id: record.process_sequence for record in records}
    changed = _change_record(records, first, process_sequence=sequences[second] + 1)
    assert not load_trace_contract(SUCCESS_TRACE_CONTRACT).match(changed, task_id=task_id).ok


def test_publication_stages_select_distinct_current_base_request_edges() -> None:
    records, task_id = _ordinary_records()
    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(records, task_id=task_id).require()
    assert tuple(match.rpc_matches[key][0].event_id for key in _STAGE_RPCS) == (
        "handoff-sent", "complete-sent", "retire-sent",
    )
    complete = match.rpc_matches["complete_rpc"]
    node_complete = next(record for record in records if record.event_id == "node-complete")
    assert complete[1].event_id == "complete-received"
    assert node_complete.cause_event_id == "complete-started"
    assert complete[2].cause_event_id == "complete-finished"


def test_valid_duplicate_report_keeps_its_own_reply_ack_and_does_not_supply_other_stages() -> None:
    records, task_id = _ordinary_records()
    mixed = _bounded_records((*records, *_terminal_echo(records, "duplicate-retire")))
    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(mixed, task_id=task_id).require()
    reports = tuple(match.rpc_matches[key] for key in _STAGE_RPCS)
    assert len({report[0].event_id for report in reports}) == len(_STAGE_RPCS)
    retired = match.rpc_matches["retire_rpc"]
    assert retired[0].event_id in ("retire-sent", "duplicate-retire-sent")
    assert any(record.cause_event_id == retired[3].event_id for record in match.event_matches["payload_retired"])


def test_one_request_edge_cannot_satisfy_two_rpc_rules() -> None:
    records, task_id = _ordinary_records()
    contract = load_trace_contract(SUCCESS_TRACE_CONTRACT)
    register = next(rule for rule in contract.rpcs if rule.key == "register_handoff_rpc")
    contract = replace(contract, rpcs=(*contract.rpcs, replace(register, key="second_handoff_rpc")))
    assert not contract.match(records, task_id=task_id).ok


def _semantic_rpc_schema():
    return {
        "schema": TRACE_CONTRACT_SCHEMA,
        "name": "one_report",
        "title": "One observed report",
        "participants": {"core_worker": "Owner", "gcs": "GCS"},
        "input_bindings": ["task_id"],
        "events": [{
            "key": "ack", "component": "core_worker",
            "event": "output_publication_ack",
            "fields": {"task_id": "$task_id", "stage": "TERMINAL", "accepted": "true"},
        }],
        "rpcs": [{
            "key": "report", "client": "core_worker", "server": "gcs",
            "handler": "report_output_publication", "round_trip": True,
        }],
    }


@pytest.mark.parametrize("field", ("reply_event", "server_event"))
@pytest.mark.parametrize(
    ("reference", "round_trip"), (("missing", True), ("report", True), ("ack", False)),
)
def test_semantic_rpc_schema_rejects_non_event_anchor_or_non_round_trip(
    field, reference, round_trip,
) -> None:
    value = _semantic_rpc_schema()
    value["rpcs"][0].update({field: reference, "round_trip": round_trip})

    with pytest.raises(TraceContractError):
        TraceContract.from_dict(value)


def test_render_schema_accepts_explicit_request_and_reply_endpoints() -> None:
    value = _semantic_rpc_schema()
    value["rpcs"][0]["reply_event"] = "ack"
    value["render_order"] = ["report.request_sent", "report.reply_received"]

    contract = TraceContract.from_dict(value)
    assert contract.render_order == ("report.request_sent", "report.reply_received")


@pytest.mark.parametrize(
    ("points", "round_trip"),
    ((["missing.request_sent"], True),
     (["report.request_sent", "report.request_sent"], True),
     (["report.reply_received"], False)),
)
def test_render_schema_rejects_unknown_duplicate_or_unavailable_endpoints(points, round_trip) -> None:
    value = _semantic_rpc_schema()
    value["rpcs"][0]["round_trip"] = round_trip
    value["render_order"] = points

    with pytest.raises(TraceContractError):
        TraceContract.from_dict(value)


@pytest.mark.parametrize(("event_id", "cause", "rpc_key"), (
    ("complete-started", "start-received", "complete_rpc"),
    ("complete-finished", "complete-started", "complete_rpc"),
    ("push-started", "worker-ready", "push_rpc"),
    ("push-finished", "push-started", "push_rpc"),
))
def test_nested_prepare_and_push_require_their_business_fact_in_the_explicit_span(event_id, cause, rpc_key) -> None:
    records, task_id = _ordinary_records()
    broken = _change_record(records, event_id, cause_event_id=cause)
    assert tuple((r.process_id, r.process_sequence) for r in broken) == tuple((r.process_id, r.process_sequence) for r in records)
    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(broken, task_id=task_id)
    assert not match.ok and not match.rpc_matches[rpc_key]


def test_prepare_and_nested_handoff_reject_an_earlier_independent_echo() -> None:
    # Shared semantic anchor is a generic matcher capability. Base's packaged
    # registration RPC has no emitted semantic ACK, so test this with an
    # explicitly synthetic local schema rather than inventing a base event.
    schema = {
        "schema": TRACE_CONTRACT_SCHEMA, "name": "synthetic_nested_ack",
        "title": "Synthetic shared ACK anchor",
        "participants": {"worker": "Worker", "node": "Node", "owner_service": "Owner"},
        "input_bindings": ["task_id"],
        "events": [{"key": "ack", "component": "node", "event": "toy_ack",
                    "fields": {"task_id": "$task_id", "accepted": "true"}}],
        "rpcs": [
            {"key": "outer", "client": "worker", "server": "node",
             "client_component": "transport", "handler": "toy_prepare", "round_trip": True, "server_event": "ack"},
            {"key": "nested", "client": "node", "server": "owner_service",
             "handler": "toy_register", "round_trip": True, "reply_event": "ack"},
        ],
    }
    outer = _rpc("outer", "worker-process", "node-process", "toy_prepare",
                 (10, 100, 300, 20), handler_span=True, finished_cause="nested-ack")
    nested = _rpc("nested", "node-process", "owner-process", "toy_register",
                  (200, 100, 200, 220), cause="outer-started", handler_span=True)
    ack = _record("nested-ack", "node-process", 230, "node", "toy_ack",
                  cause="nested-done", task_id="toy-task", accepted="true")
    echo = _rpc("independent", "node-process", "owner-process", "toy_register",
                (110, 10, 20, 120), handler_span=True)
    echo_ack = _record("independent-ack", "node-process", 130, "node", "toy_ack",
                       cause="independent-done", task_id="toy-task", accepted="true")
    records = _bounded_records((*outer, *nested, ack, *echo, echo_ack))
    match = TraceContract.from_dict(schema).match(reversed(records), task_id="toy-task").require()
    assert match.rpc_matches["outer"][0].event_id == "outer-sent"
    assert match.rpc_matches["nested"][0].event_id == "nested-sent"
    assert match.event_matches["ack"][0] == ack
    assert echo_ack in match.event_matches["ack"] and echo[0].cause_event_id is None


def test_synthetic_semantic_ack_rejects_false_acceptance_despite_transport_success():
    # Generic matcher schema only. This local toy is not the packaged base
    # runtime contract and makes no claim that B emits GCS publication stages.
    value = _semantic_rpc_schema()
    value["rpcs"][0]["reply_event"] = "ack"
    contract = TraceContract.from_dict(value)
    rpc = _rpc("toy", "driver-process", "gcs-process", "report_output_publication", (1, 1, 2, 2))
    ack = _record("toy-ack", "driver-process", 3, "core_worker", "output_publication_ack",
                  cause="toy-done", task_id="toy-task", stage="TERMINAL", accepted="true")
    assert contract.match((*rpc, ack), task_id="toy-task").ok
    false_ack = replace(ack, fields=tuple((key, "false" if key == "accepted" else item) for key, item in ack.fields))
    assert not contract.match((*rpc, false_ack), task_id="toy-task").ok


def test_ordinary_success_contract_rejects_retry_of_the_selected_task() -> None:
    records, task_id = _ordinary_records()
    retry = _record(
        "selected-task-retry", "driver-process", 1300, "core_worker",
        "task_retried", task_id=task_id,
    )
    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(
        _bounded_records((*records, retry)), task_id=task_id,
    )

    assert not match.ok
    assert any("forbidden event" in violation and "task_retried" in violation for violation in match.violations)


def test_contract_rejects_duplicate_event_ids_before_they_can_alias_a_causal_edge() -> None:
    records, task_id = _ordinary_records()
    sent = next(record for record in records if record.event_id == "handoff-sent")
    # Keep component/event/count rules harmless while deliberately aliasing
    # the event ID used by the real cross-process request edge.
    duplicate = replace(sent, process_sequence=750)
    assert len(records) + 1 < 200
    match = load_trace_contract(SUCCESS_TRACE_CONTRACT).match(
        (*records, duplicate), task_id=task_id,
    )

    assert not match.ok
    assert any("event IDs must be unique" in violation for violation in match.violations)
