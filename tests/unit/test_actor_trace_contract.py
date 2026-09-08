"""Pure graph contract mirrored by the bounded Actor trace smoke."""

from __future__ import annotations

import pytest

from miniray import protocol


pytestmark = pytest.mark.unit


def _record(
    event_id: str,
    process_id: str,
    sequence: int,
    component: str,
    event: str,
    *,
    cause: str | None = None,
    handler: str = "",
    rpc_id: str = "",
) -> protocol.TraceRecord:
    return protocol.TraceRecord(
        event_id=event_id,
        timestamp_ns=1,
        process_id=process_id,
        process_sequence=sequence,
        component=component,
        event=event,
        entity_kind="event",
        entity_id=event_id,
        cause_event_id=cause,
        fields=(("handler", handler), ("rpc_id", rpc_id)),
    )


def _edge(records: tuple[protocol.TraceRecord, ...], handler: str):
    by_id = {record.event_id: record for record in records}
    matches = []
    for received in records:
        fields = dict(received.fields)
        if (
            received.event != "rpc_request_received"
            or fields.get("handler") != handler
        ):
            continue
        sent = by_id.get(received.cause_event_id)
        if sent is None:
            continue
        sent_fields = dict(sent.fields)
        if (
            sent.event == "rpc_request_sent"
            and sent_fields.get("handler") == handler
            and sent_fields.get("rpc_id") == fields.get("rpc_id")
            and sent.process_id != received.process_id
        ):
            matches.append((sent, received))
    return tuple(matches)


def _descends(
    records: tuple[protocol.TraceRecord, ...],
    descendant: protocol.TraceRecord,
    ancestor: protocol.TraceRecord,
) -> bool:
    by_id = {record.event_id: record for record in records}
    current = descendant
    seen: set[str] = set()
    while current.cause_event_id is not None:
        cause = current.cause_event_id
        if cause == ancestor.event_id:
            return True
        if cause in seen or cause not in by_id:
            return False
        seen.add(cause)
        current = by_id[cause]
    return False


def test_actor_control_chain_and_direct_call_are_distinct_cross_pid_edges() -> None:
    records = (
        _record("create-send", "driver", 1, "core_worker",
                "rpc_request_sent", handler="create_actor", rpc_id="c"),
        _record("create-recv", "gcs", 1, "gcs",
                "rpc_request_received", cause="create-send",
                handler="create_actor", rpc_id="c"),
        _record("reserve-send", "gcs", 2, "transport",
                "rpc_request_sent", cause="create-recv",
                handler="reserve_actor_worker", rpc_id="r"),
        _record("reserve-recv", "node", 1, "node",
                "rpc_request_received", cause="reserve-send",
                handler="reserve_actor_worker", rpc_id="r"),
        _record("call-send", "driver", 2, "core_worker",
                "rpc_request_sent", handler="actor_call", rpc_id="a"),
        _record("call-recv", "actor", 1, "actor_worker",
                "rpc_request_received", cause="call-send",
                handler="actor_call", rpc_id="a"),
    )

    create = _edge(records, "create_actor")
    reserve = _edge(records, "reserve_actor_worker")
    direct = _edge(records, "actor_call")

    assert len(create) == len(reserve) == len(direct) == 1
    assert _descends(records, reserve[0][0], create[0][1])
    assert (create[0][0].process_id, create[0][1].process_id) == (
        "driver", "gcs"
    )
    assert (reserve[0][0].process_id, reserve[0][1].process_id) == (
        "gcs", "node"
    )
    assert (direct[0][0].process_id, direct[0][1].process_id) == (
        "driver", "actor"
    )
    assert direct[0][1].process_id != create[0][1].process_id

