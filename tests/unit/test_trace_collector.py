"""Trace contracts with explicit in-memory and background-thread modes.

The three unit cases use only records and the in-memory store. The original
two asynchronous sink cases start real threads and remain heavy until a
separate bounded-runtime review. No runtime assertion has been replaced.
"""

from __future__ import annotations

import threading
import time

import pytest

from miniray import protocol
from miniray.trace import AsyncRemoteEventSink, TraceEvent
from miniray.trace_collector import TraceRecordStore


def _record(event_id: str, sequence: int) -> protocol.TraceRecord:
    return protocol.TraceRecord(
        event_id=event_id, timestamp_ns=sequence, process_id="123",
        process_sequence=sequence, component="worker", event="task_started",
        entity_kind="task", entity_id="task-1", fields=(("task_id", "task-1"),)
    )


@pytest.mark.unit
def test_trace_event_converts_to_existing_wire_record() -> None:
    event = TraceEvent(
        "event-1", 10, 123, 4, "core_worker", "lease_requested",
        cause_id="cause-1", attributes={"task_id": "task-1", "attempt": 2}
    )
    record = event.to_record()
    assert record.event_id == event.event_id
    assert record.process_id == "123"
    assert record.process_sequence == 4
    assert record.cause_event_id == "cause-1"
    assert dict(record.fields) == {"attempt": "2", "task_id": "task-1"}


@pytest.mark.unit
def test_store_deduplicates_identical_event_ids() -> None:
    store = TraceRecordStore()
    record = _record("event-1", 1)
    first = store.add_batch(protocol.TraceBatch("worker-1", (record,)))
    replay = store.add_batch(protocol.TraceBatch("worker-1", (record,)))
    assert (first.accepted, first.deduplicated) == (1, 0)
    assert (replay.accepted, replay.deduplicated) == (0, 1)
    assert store.records == (record,)


@pytest.mark.unit
def test_store_rejects_event_id_content_collision() -> None:
    store = TraceRecordStore()
    store.add_batch(protocol.TraceBatch("worker-1", (_record("same", 1),)))
    with pytest.raises(ValueError, match="different contents"):
        store.add_batch(protocol.TraceBatch("worker-1", (_record("same", 2),)))


@pytest.mark.heavy
def test_async_sink_batches_on_background_thread() -> None:
    delivered = []
    ready = threading.Event()

    def sender(batch: protocol.TraceBatch) -> protocol.TraceBatchAck:
        delivered.append((threading.current_thread(), batch))
        ready.set()
        return protocol.TraceBatchAck(batch.source_id, len(batch.records))

    sink = AsyncRemoteEventSink(
        "worker-1", sender, capacity=8, batch_size=8, flush_interval=0.005,
        clock_ns=lambda: 1, process_id=lambda: 123
    )
    caller = threading.current_thread()
    emitted = sink.emit("task_started", component="worker", task_id="task-1")
    assert emitted is not None
    assert emitted.entity_kind == "task"
    assert emitted.entity_id == "task-1"
    assert ready.wait(0.5)
    sink.close()
    assert delivered[0][0] is not caller
    assert delivered[0][1].records[0].event_id == emitted.event_id
    assert delivered[0][1].records[0].entity_kind == "task"
    assert delivered[0][1].records[0].entity_id == "task-1"


@pytest.mark.heavy
def test_async_sink_failure_and_queue_pressure_only_drop_trace() -> None:
    entered = threading.Event()
    release = threading.Event()
    second_batch = threading.Event()
    sends = 0

    def blocked_failure(_batch: protocol.TraceBatch) -> object:
        nonlocal sends
        sends += 1
        entered.set()
        release.wait(0.5)
        if sends >= 2:
            second_batch.set()
        raise OSError("collector unavailable")

    sink = AsyncRemoteEventSink(
        "worker-1", blocked_failure, capacity=1, batch_size=1,
        flush_interval=0.005, clock_ns=time.time_ns, process_id=lambda: 123
    )
    assert sink.emit("one", component="worker") is not None
    assert entered.wait(0.5)
    # Sender is blocked and capacity is one: at least one later event is dropped,
    # but every emit remains a normal, non-raising runtime operation.
    for index in range(8):
        assert sink.emit("queued", component="worker", index=index) is not None
    assert sink.dropped_events > 0
    release.set()
    # The second callback proves that the sender dequeued the one queued
    # record.  close() can now enqueue its stop marker deterministically.
    assert second_batch.wait(0.5)
    sink.close(0.5)
    assert not sink._thread.is_alive()
    assert sink.dropped_events > 0
