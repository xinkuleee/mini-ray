"""Best-effort structured tracing for mini-Ray teaching examples.

Tracing is deliberately observational: ``emit`` never raises because a trace
could not be recorded.  Runtime state machines must therefore never inspect a
sink, wait for it, or use its return value to make correctness decisions.
"""

from __future__ import annotations

import json
import os
import queue
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Callable, Dict, Iterable, Iterator, Mapping, Optional, TextIO, Tuple,
)

from . import protocol


# A plain thread-local would keep independent socket-server threads apart, but
# it would lose causality when a handler uses asyncio or another context-aware
# execution mechanism.  ContextVar gives each logical execution context its
# own cursor.  The private sentinel distinguishes "there is no active causal
# scope" from an active scope whose first event has no known predecessor.
_INACTIVE_CAUSAL_CURSOR = object()
_causal_cursor = ContextVar(
    "miniray_trace_causal_cursor", default=_INACTIVE_CAUSAL_CURSOR
)  # type: ContextVar[object]
_causal_sink = ContextVar(
    "miniray_trace_causal_sink", default=_INACTIVE_CAUSAL_CURSOR
)  # type: ContextVar[object]


def current_cause_id() -> Optional[str]:
    """Return the active scope's latest event ID, if one is known.

    This is primarily a transport integration point.  Runtime components only
    need to call :meth:`EventSink.emit`; inside a causal scope, each emitted
    event automatically follows the event before it.
    """

    value = _causal_cursor.get()
    if isinstance(value, str) and value:
        return value
    return None


def current_event_sink() -> Optional["EventSink"]:
    """Return the sink inherited from the active RPC handler scope."""

    value = _causal_sink.get()
    return value if isinstance(value, EventSink) else None


def _advance_causal_cursor(event_id: Optional[str]) -> None:
    """Advance an active scope to an event produced by another sink API."""

    if not isinstance(event_id, str) or not event_id:
        return
    if _causal_cursor.get() is not _INACTIVE_CAUSAL_CURSOR:
        _causal_cursor.set(event_id)


@contextmanager
def causal_scope(
    cause_id: Optional[str] = None, *, event_sink: Optional["EventSink"] = None
) -> Iterator[None]:
    """Isolate and advance one logical chain of trace events.

    A nested scope starts at ``cause_id``.  Its final event is propagated back
    to an already-active parent scope, so a nested RPC becomes the predecessor
    of work that resumes after that RPC.  Leaving an outermost scope restores
    the previous context and cannot leak a request's cursor into a later
    request handled by the same thread.
    """

    if cause_id is not None and (
        not isinstance(cause_id, str) or not cause_id
    ):
        raise ValueError("cause_id must be a non-empty string or None")
    if event_sink is not None and not isinstance(event_sink, EventSink):
        raise TypeError("event_sink must be an EventSink or None")
    parent = _causal_cursor.get()
    token = _causal_cursor.set(cause_id)
    sink_token = (
        _causal_sink.set(event_sink) if event_sink is not None else None
    )
    try:
        yield
    finally:
        final = _causal_cursor.get()
        _causal_cursor.reset(token)
        if (
            parent is not _INACTIVE_CAUSAL_CURSOR
            and isinstance(final, str)
            and final
        ):
            _causal_cursor.set(final)
        if sink_token is not None:
            _causal_sink.reset(sink_token)


# ``causal_context`` reads naturally at integration call sites; both names
# intentionally denote the same scoped cursor abstraction.
causal_context = causal_scope


@dataclass(frozen=True)
class TraceSinkConfig:
    """Small spawn-safe description used to construct a child-process sink."""

    collector_address: Tuple[str, int]
    source_role: str
    capacity: int = 1024
    batch_size: int = 32
    flush_interval: float = 0.01

    def __post_init__(self) -> None:
        host, port = self.collector_address
        if not isinstance(host, str) or not host or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("collector_address must be a bound (host, port) tuple")
        if not isinstance(self.source_role, str) or not self.source_role:
            raise ValueError("source_role must be non-empty")

    def for_role(self, role: str) -> "TraceSinkConfig":
        return TraceSinkConfig(self.collector_address, role, self.capacity, self.batch_size, self.flush_interval)


@dataclass(frozen=True)
class TraceEvent:
    """One structured observation and its causal predecessor, if known."""

    event_id: str
    timestamp_ns: int
    process_id: int
    process_seq: int
    component: str
    name: str
    cause_id: Optional[str] = None
    attributes: Mapping[str, object] = field(default_factory=dict)
    entity_kind: str = "event"
    entity_id: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "event_id": self.event_id,
            "timestamp_ns": self.timestamp_ns,
            "process_id": self.process_id,
            "process_seq": self.process_seq,
            "component": self.component,
            "name": self.name,
            "cause_id": self.cause_id,
            "entity_kind": self.entity_kind,
            "entity_id": self.entity_id or self.event_id,
            "attributes": dict(self.attributes),
        }

    def to_record(self) -> protocol.TraceRecord:
        """Convert to the transport-neutral wire representation."""

        fields = tuple(
            (str(key), _trace_field(value))
            for key, value in sorted(self.attributes.items())
        )
        return protocol.TraceRecord(
            event_id=self.event_id,
            timestamp_ns=self.timestamp_ns,
            process_id=str(self.process_id),
            process_sequence=self.process_seq,
            component=self.component,
            event=self.name,
            entity_kind=self.entity_kind,
            entity_id=self.entity_id or self.event_id,
            cause_event_id=self.cause_id,
            fields=fields,
        )


def _trace_field(value: object) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True, default=_json_fallback)
    except Exception:
        return _json_fallback(value)


TRACE_JSONL_SCHEMA_VERSION = 1


def _trace_record_dict(record: protocol.TraceRecord) -> Dict[str, object]:
    """Return the stable JSON representation of one wire trace record.

    Identity is copied from protocol fields rather than from ``repr(record)``
    or dataclass implementation details that may change independently of the
    exported schema.
    """

    if not isinstance(record, protocol.TraceRecord):
        raise TypeError("trace records must be protocol.TraceRecord values")
    fields: Dict[str, str] = {}
    for name, value in record.fields:
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("TraceRecord fields must contain string pairs")
        if name in fields:
            raise ValueError("TraceRecord field names must be unique")
        fields[name] = value
    return {
        "schema_version": TRACE_JSONL_SCHEMA_VERSION,
        "event_id": record.event_id,
        "timestamp_ns": record.timestamp_ns,
        "process_id": record.process_id,
        "process_sequence": record.process_sequence,
        "component": record.component,
        "event": record.event,
        "entity_kind": record.entity_kind,
        "entity_id": record.entity_id,
        "cause_event_id": record.cause_event_id,
        "fields": fields,
    }


def write_trace_records_jsonl(
    path: object, records: Iterable[protocol.TraceRecord]
) -> int:
    """Atomically replace ``path`` with deterministic TraceRecord JSONL.

    Cross-process wall-clock order is not meaningful, so the export uses the
    same total order as the teaching trace renderer: process identity, then
    that process's sequence, then event identity as a final tie-breaker.
    """

    try:
        raw_path = os.fspath(path)
    except TypeError:
        raise TypeError("path must be a string or os.PathLike") from None
    if not isinstance(raw_path, str):
        raise TypeError("path must resolve to a text filesystem path")
    if not raw_path:
        raise ValueError("path must be non-empty")
    target = Path(raw_path)
    snapshot = tuple(records)
    if any(not isinstance(record, protocol.TraceRecord) for record in snapshot):
        raise TypeError("trace records must be protocol.TraceRecord values")
    ordered = tuple(
        sorted(
            snapshot,
            key=lambda record: (
                record.process_id, record.process_sequence, record.event_id
            ),
        )
    )

    descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=".{}.trace-".format(target.name or "miniray"),
        suffix=".tmp",
    )
    descriptor_open = True
    try:
        with os.fdopen(
            descriptor, "w", encoding="utf-8", newline="\n"
        ) as stream:
            descriptor_open = False
            for record in ordered:
                json.dump(
                    _trace_record_dict(record),
                    stream,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, target)
    except BaseException:
        if descriptor_open:
            os.close(descriptor)
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
    return len(ordered)


class EventSink:
    """Assign event identity and deliver observations on a best-effort basis.

    Subclasses implement ``_write``.  The base class is also a useful null sink:
    it assigns events and silently discards them.  Sequence numbers are scoped
    to the current OS process and start at one.
    """

    def __init__(
        self,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        process_id: Callable[[], int] = os.getpid,
    ) -> None:
        self._clock_ns = clock_ns
        self._process_id = process_id
        self._source_id = uuid.uuid4().hex
        self._sequence_by_pid: Dict[int, int] = {}
        # Serialize emission within one process so process_seq also describes
        # the order events reach a concrete sink.  Cross-process ordering is
        # intentionally represented only by cause_id.
        self._emit_lock = threading.Lock()
        self._failure_lock = threading.Lock()
        self._dropped_events = 0

    @property
    def dropped_events(self) -> int:
        with self._failure_lock:
            return self._dropped_events

    def emit(
        self,
        name: str,
        *,
        component: str,
        cause_id: Optional[str] = None,
        attributes: Optional[Mapping[str, object]] = None,
        entity_kind: Optional[str] = None,
        entity_id: Optional[object] = None,
        **extra_attributes: object,
    ) -> Optional[TraceEvent]:
        """Create and record an event, returning ``None`` only if creation fails.

        Sink I/O failures increment ``dropped_events`` but the created event is
        still returned.  Callers may use that return solely to attach a later
        observation's ``cause_id``; it must not gate runtime progress.
        """

        try:
            if not isinstance(name, str) or not name:
                raise ValueError("event name must be a non-empty string")
            if not isinstance(component, str) or not component:
                raise ValueError("component must be a non-empty string")
            values = dict(attributes or {})
            values.update(extra_attributes)
            resolved_kind, resolved_id = _semantic_entity(
                values, entity_kind=entity_kind, entity_id=entity_id
            )
            pid = int(self._process_id())
            timestamp_ns = int(self._clock_ns())
            cursor = _causal_cursor.get()
            effective_cause_id = cause_id
            if (
                effective_cause_id is None
                and isinstance(cursor, str)
                and cursor
            ):
                effective_cause_id = cursor
            with self._emit_lock:
                sequence = self._sequence_by_pid.get(pid, 0) + 1
                self._sequence_by_pid[pid] = sequence
                event = TraceEvent(
                    event_id="{}:{}:{}".format(pid, self._source_id, sequence),
                    timestamp_ns=timestamp_ns,
                    process_id=pid,
                    process_seq=sequence,
                    component=component,
                    name=name,
                    cause_id=effective_cause_id,
                    attributes=values,
                    entity_kind=resolved_kind,
                    entity_id=resolved_id,
                )
                try:
                    self._write(event)
                except Exception:
                    self._mark_dropped()
            if cursor is not _INACTIVE_CAUSAL_CURSOR:
                _causal_cursor.set(event.event_id)
        except Exception:
            self._mark_dropped()
            return None
        return event

    def _mark_dropped(self) -> None:
        try:
            with self._failure_lock:
                self._dropped_events += 1
        except Exception:
            # Even accounting for failed observations is observational.
            pass

    def _write(self, event: TraceEvent) -> None:
        del event

    def close(self) -> None:
        """Release sink resources without affecting caller correctness."""

    def __enter__(self) -> "EventSink":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


class NonOwningEventSink(EventSink):
    """A component-labelled, non-owning view of another event sink.

    Runtime components sometimes have nested lifetimes while sharing one
    physical trace source.  In particular, an execution Worker owns the
    asynchronous sender thread, while its embedded CoreWorker is shut down
    first.  Giving that CoreWorker the physical sink directly would let its
    normal ``close()`` stop trace delivery before the outer Worker records its
    own shutdown.

    This view forwards every emission to ``sink`` so event IDs, per-process
    sequence numbers, queue bounds, and delivery all remain owned by one
    source.  ``close()`` is deliberately a no-op.  An optional fixed component
    label makes the nested role explicit without changing CoreWorker call
    sites.
    """

    def __init__(
        self, sink: EventSink, *, component: Optional[str] = None
    ) -> None:
        if not isinstance(sink, EventSink):
            raise TypeError("sink must be an EventSink")
        if component is not None and (
            not isinstance(component, str) or not component
        ):
            raise ValueError("component must be a non-empty string or None")
        # EventSink.__init__ is intentionally not called: this view must not
        # allocate a second event-identity or sequence-number authority.  All
        # public EventSink operations are overridden or delegate below.
        self._sink = sink
        self._component = component

    @property
    def sink(self) -> EventSink:
        """Return the physical sink whose lifecycle the caller still owns."""

        return self._sink

    @property
    def component(self) -> Optional[str]:
        return self._component

    @property
    def dropped_events(self) -> int:
        return self._sink.dropped_events

    def emit(
        self,
        name: str,
        *,
        component: str,
        cause_id: Optional[str] = None,
        attributes: Optional[Mapping[str, object]] = None,
        entity_kind: Optional[str] = None,
        entity_id: Optional[object] = None,
        **extra_attributes: object,
    ) -> Optional[TraceEvent]:
        return self._sink.emit(
            name,
            component=self._component or component,
            cause_id=cause_id,
            attributes=attributes,
            entity_kind=entity_kind,
            entity_id=entity_id,
            **extra_attributes,
        )

    def close(self) -> None:
        """Release no resources; only the physical sink owner may close."""


class MemoryEventSink(EventSink):
    """Thread-safe in-memory event collection for tests and small demos."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._events = []  # type: list[TraceEvent]
        self._events_lock = threading.Lock()

    def _write(self, event: TraceEvent) -> None:
        with self._events_lock:
            self._events.append(event)

    def snapshot(self) -> Tuple[TraceEvent, ...]:
        with self._events_lock:
            return tuple(self._events)

    @property
    def events(self) -> Tuple[TraceEvent, ...]:
        return self.snapshot()


class AsyncRemoteEventSink(EventSink):
    """Non-blocking, bounded trace sender for runtime child processes.

    ``sender`` performs the actual batch delivery and is called only by the
    background thread.  Runtime threads merely enqueue with ``put_nowait``; a
    full queue or failed send marks records dropped and never affects runtime
    progress.  The default network sender lives in :mod:`trace_collector`,
    while tests inject a pure callable.
    """

    _STOP = object()

    def __init__(
        self,
        source_id: str,
        sender: Callable[[protocol.TraceBatch], object],
        *,
        capacity: int = 1024,
        batch_size: int = 32,
        flush_interval: float = 0.01,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        if not isinstance(source_id, str) or not source_id:
            raise ValueError("source_id must be a non-empty string")
        if not callable(sender):
            raise TypeError("sender must be callable")
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if flush_interval <= 0:
            raise ValueError("flush_interval must be positive")
        self.source_id = source_id
        self._sender = sender
        self._batch_size = batch_size
        self._flush_interval = float(flush_interval)
        self._queue = queue.Queue(maxsize=capacity)  # type: queue.Queue[object]
        self._close_lock = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name="miniray-trace-sender-{}".format(source_id),
            daemon=True,
        )
        self._thread.start()

    def _write(self, event: TraceEvent) -> None:
        if self._closed:
            raise RuntimeError("trace sink is closed")
        try:
            self._queue.put_nowait(event.to_record())
        except queue.Full:
            raise RuntimeError("trace queue is full") from None

    def _run(self) -> None:
        stop = False
        while not stop:
            try:
                first = self._queue.get(timeout=self._flush_interval)
            except queue.Empty:
                continue
            if first is self._STOP:
                break
            records = [first]
            deadline = time.monotonic() + self._flush_interval
            while len(records) < self._batch_size:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    break
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    break
                if item is self._STOP:
                    stop = True
                    break
                records.append(item)
            try:
                batch = protocol.TraceBatch(self.source_id, tuple(records))  # type: ignore[arg-type]
                reply = self._sender(batch)
                if isinstance(reply, protocol.TraceBatchAck):
                    if reply.source_id != self.source_id:
                        raise RuntimeError("trace ACK names another source")
            except Exception:
                for _ in records:
                    self._mark_dropped()

    def close(self, timeout: float = 0.25) -> None:
        """Best-effort bounded drain; failure remains observational."""

        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._queue.put_nowait(self._STOP)
            except queue.Full:
                self._mark_dropped()
            thread = self._thread
        if thread is not threading.current_thread():
            thread.join(max(0.0, float(timeout)))


def _json_fallback(value: object) -> str:
    try:
        return repr(value)
    except Exception:
        return "<unrepresentable {}>".format(type(value).__name__)


_ENTITY_FIELDS = (
    ("object_id", "object"),
    ("task_id", "task"),
    ("lease_id", "lease"),
    ("actor_id", "actor"),
    ("placement_group_id", "placement_group"),
    ("worker_id", "worker"),
    ("node_id", "node"),
    ("request_id", "request"),
)


def _semantic_entity(
    attributes: Mapping[str, object], *,
    entity_kind: Optional[str], entity_id: Optional[object],
) -> tuple[str, Optional[str]]:
    """Select one stable runtime entity without changing event fields."""

    if entity_kind is not None:
        if not isinstance(entity_kind, str) or not entity_kind:
            raise ValueError("entity_kind must be a non-empty string or None")
        if entity_id is None:
            raise ValueError("an explicit entity_kind requires entity_id")
        value = str(entity_id)
        if not value:
            raise ValueError("entity_id must be non-empty")
        return entity_kind, value
    if entity_id is not None:
        raise ValueError("entity_id requires an explicit entity_kind")
    for field, kind in _ENTITY_FIELDS:
        value = attributes.get(field)
        if value is not None and str(value):
            return kind, str(value)
    return "event", None


class JsonlEventSink(EventSink):
    """Append events to a UTF-8 JSON Lines file.

    The file is opened lazily so an unavailable trace destination cannot make
    runtime initialization fail.  Writes are flushed per event for useful
    crash-time teaching traces; this sink is intentionally not a fast logger.
    """

    def __init__(self, path: object, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.path = Path(path)
        self._file: Optional[TextIO] = None
        self._file_lock = threading.Lock()
        self._closed = False

    def _write(self, event: TraceEvent) -> None:
        line = json.dumps(
            event.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            default=_json_fallback,
        )
        with self._file_lock:
            if self._closed:
                raise RuntimeError("JSONL event sink is closed")
            if self._file is None:
                self._file = self.path.open("a", encoding="utf-8")
            self._file.write(line + "\n")
            self._file.flush()

    def close(self) -> None:
        try:
            with self._file_lock:
                self._closed = True
                if self._file is not None:
                    self._file.close()
                    self._file = None
        except Exception:
            self._mark_dropped()


# US spelling is a convenient compatibility alias for callers.
JSONLEventSink = JsonlEventSink


__all__ = [
    "EventSink",
    "AsyncRemoteEventSink",
    "JSONLEventSink",
    "JsonlEventSink",
    "MemoryEventSink",
    "NonOwningEventSink",
    "TRACE_JSONL_SCHEMA_VERSION",
    "TraceEvent",
    "TraceSinkConfig",
    "causal_scope",
    "causal_context",
    "current_cause_id",
    "current_event_sink",
    "write_trace_records_jsonl",
]
