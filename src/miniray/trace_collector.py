"""Driver-owned, best-effort collection for cross-process teaching traces."""

from __future__ import annotations

import threading
from typing import Dict, Tuple

from . import protocol
from .trace import AsyncRemoteEventSink, EventSink, TraceSinkConfig
from .transport import Address, LOOPBACK_HOST, TCPServer, request


TRACE_BATCH_HANDLER = "trace_batch"


class TraceRecordStore:
    """Thread-safe arrival-ordered storage with event-ID deduplication."""

    def __init__(self) -> None:
        self._records = []  # type: list[protocol.TraceRecord]
        self._by_id: Dict[str, protocol.TraceRecord] = {}
        self._lock = threading.Lock()

    def add_batch(self, batch: protocol.TraceBatch) -> protocol.TraceBatchAck:
        if not isinstance(batch, protocol.TraceBatch):
            raise TypeError("trace_batch expects TraceBatch")
        accepted = 0
        deduplicated = 0
        with self._lock:
            for record in batch.records:
                previous = self._by_id.get(record.event_id)
                if previous is not None:
                    if previous != record:
                        raise ValueError("trace event ID was reused with different contents")
                    deduplicated += 1
                    continue
                self._by_id[record.event_id] = record
                self._records.append(record)
                accepted += 1
        return protocol.TraceBatchAck(batch.source_id, accepted, deduplicated)

    def snapshot(self) -> Tuple[protocol.TraceRecord, ...]:
        with self._lock:
            return tuple(self._records)

    @property
    def records(self) -> Tuple[protocol.TraceRecord, ...]:
        return self.snapshot()


class TraceCollector:
    """Small loopback server owned by the Driver process."""

    def __init__(self, *, host: str = LOOPBACK_HOST, port: int = 0) -> None:
        self.store = TraceRecordStore()
        self._server = TCPServer(
            {TRACE_BATCH_HANDLER: self.store.add_batch}, host=host, port=port
        )

    @property
    def address(self) -> Address:
        return self._server.address

    @property
    def records(self) -> Tuple[protocol.TraceRecord, ...]:
        return self.store.snapshot()

    def start(self) -> Address:
        return self._server.start()

    def stop(self) -> None:
        self._server.stop()

    def __enter__(self) -> "TraceCollector":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()


def remote_event_sink(
    address: Address, source_id: str, **kwargs: object
) -> AsyncRemoteEventSink:
    """Build an async sink whose network I/O stays on its sender thread."""

    def send(batch: protocol.TraceBatch) -> object:
        return request(address, TRACE_BATCH_HANDLER, batch)

    return AsyncRemoteEventSink(source_id, send, **kwargs)


def sink_from_config(config: TraceSinkConfig | None) -> EventSink:
    if config is None:
        return EventSink()
    return remote_event_sink(
        config.collector_address,
        config.source_role,
        capacity=config.capacity,
        batch_size=config.batch_size,
        flush_interval=config.flush_interval,
    )


__all__ = [
    "TRACE_BATCH_HANDLER",
    "TraceCollector",
    "TraceRecordStore",
    "remote_event_sink",
    "sink_from_config",
]
