"""Thin, read-only debugging views over a mini-Ray runtime.

Debugging must remain observational.  These helpers call existing ``snapshot``
APIs and read an event sink; they never participate in scheduling, object
ownership, retries, or any other correctness decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Tuple

from .trace import EventSink, TraceEvent


class DebugTargetError(TypeError):
    """Raised when an object does not expose the requested debug surface."""


@dataclass(frozen=True)
class DebugAPI:
    """A runtime-bound facade matching ``runtime.debug`` style usage."""

    runtime: object

    def snapshot(self) -> object:
        return snapshot(self.runtime)

    def trace(
        self,
        ref: object = None,
        *,
        component: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Tuple[TraceEvent, ...]:
        return trace(self.runtime, ref, component=component, name=name)


def bind(runtime: object) -> DebugAPI:
    """Return a small debug facade bound to ``runtime``."""

    return DebugAPI(runtime)


def snapshot(target: object) -> object:
    """Return ``target``'s own immutable/read-only diagnostic snapshot.

    Runtime implementations may expose their state directly through
    ``snapshot()`` or through a ``control``/``gcs`` component.  No state is
    synthesized here, which keeps subsystem ownership visible to students.
    """

    for method_name in ("debug_snapshot", "snapshot"):
        method = getattr(target, method_name, None)
        if callable(method):
            return method()

    for attribute in ("control", "gcs", "_control", "_gcs"):
        component = getattr(target, attribute, None)
        method = getattr(component, "snapshot", None)
        if callable(method):
            return method()

    raise DebugTargetError(
        "debug snapshot requires a snapshot() method or a control/GCS component"
    )


def trace(
    source: object,
    ref: object = None,
    *,
    component: Optional[str] = None,
    name: Optional[str] = None,
) -> Tuple[TraceEvent, ...]:
    """Read and optionally filter structured events from a runtime or sink.

    ``ref`` may be an ID itself or an ObjectRef-like value exposing
    ``object_id``, ``task_id``, or ``actor_id``.  Matching checks event
    attributes only, so tracing cannot affect runtime progress.
    """

    events = _events_from(_event_sink_from(source))
    identifier = _identifier_from(ref) if ref is not None else None
    return tuple(
        event
        for event in events
        if (component is None or event.component == component)
        and (name is None or event.name == name)
        and (identifier is None or _event_mentions(event, identifier))
    )


def _event_sink_from(source: object) -> object:
    if isinstance(source, EventSink) or hasattr(source, "events"):
        return source

    for attribute in (
        "event_sink",
        "trace_sink",
        "_event_sink",
        "_trace_sink",
    ):
        sink = getattr(source, attribute, None)
        if sink is not None:
            return sink

    raise DebugTargetError(
        "debug trace requires an event sink, or a runtime exposing event_sink"
    )


def _events_from(sink: object) -> Tuple[TraceEvent, ...]:
    # Event sinks prefer snapshot() because it can take the sink's lock.  Some
    # minimal protocol implementations expose only an ``events`` property.
    if hasattr(sink, "events"):
        raw_events = getattr(sink, "events")
    else:
        method = getattr(sink, "snapshot", None)
        if callable(method):
            raw_events = method()
        elif isinstance(sink, EventSink):
            return ()  # the base/null sink deliberately retains no events
        else:
            raise DebugTargetError("event sink has no events or snapshot view")
    try:
        events = tuple(raw_events)
    except TypeError as exc:
        raise DebugTargetError("event sink did not return an iterable event view") from exc
    if not all(isinstance(event, TraceEvent) for event in events):
        raise DebugTargetError("event sink contains values other than TraceEvent")
    return events


trace_events = trace


def _identifier_from(ref: object) -> object:
    for attribute in ("object_id", "task_id", "actor_id", "attempt_id"):
        if hasattr(ref, attribute):
            return getattr(ref, attribute)
    return ref


def _event_mentions(event: TraceEvent, identifier: object) -> bool:
    attributes: Mapping[str, object] = event.attributes
    for key, value in attributes.items():
        if key.endswith("_id") or key.endswith("_ids"):
            if _matches(value, identifier):
                return True
    return False


def _matches(value: object, identifier: object) -> bool:
    try:
        if value == identifier:
            return True
    except Exception:
        pass
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            try:
                if item == identifier:
                    return True
            except Exception:
                continue
    return False


__all__ = [
    "DebugAPI",
    "DebugTargetError",
    "bind",
    "snapshot",
    "trace",
    "trace_events",
]
