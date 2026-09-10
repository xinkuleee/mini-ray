"""Inert audit collector; no project imports, test imports, CLI, or workloads.

A post-freeze driver supplies real callbacks, serializers, and semantics.
This module is not an experiment and produces no performance claims.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
import json
from pathlib import Path
import sys
from typing import Callable


CONTENT_TYPES = frozenset({
    "TaskPublication", "PutPublication", "OutputPublicationManifest",
    "TaskPreparedReceipt", "PutPreparedReceipt",
    "OutputPublicationCompleteWitness", "OutputPublicationAdoptionProof",
    "ClosedContainedHolds", "PublicationSnapshot",
})
PROFILE_FUNCTIONS = frozenset({
    "_copy", "__post_init__", "_validate_snapshot",
    "_validate_prepared", "_validate_closed",
})


def content_occurrences(value: object) -> dict:
    """Audit-only shape walk, never serialization or runtime validation.

    Paths and unique identities are both reported; aliases are not silently
    deduplicated into an invented serialized byte estimate.
    """
    found: list[dict] = []
    unique: dict[str, set[int]] = {}

    def visit(item: object, path: str, ancestors: frozenset[int]) -> None:
        identity = id(item)
        kind = type(item).__name__
        if kind in CONTENT_TYPES:
            found.append({"type": kind, "path": path})
            unique.setdefault(kind, set()).add(identity)
        if identity in ancestors:
            raise ValueError("cycle in captured metadata graph")
        if is_dataclass(item) and not isinstance(item, type):
            next_ancestors = ancestors | {identity}
            for field in fields(item):
                visit(getattr(item, field.name), path + "." + field.name, next_ancestors)
        elif type(item) is tuple:
            next_ancestors = ancestors | {identity}
            for index, child in enumerate(item):
                visit(child, path + "[" + str(index) + "]", next_ancestors)

    visit(value, "$", frozenset())
    return {
        "paths": found,
        "path_counts": dict(Counter(row["type"] for row in found)),
        "unique_identity_counts": {kind: len(ids) for kind, ids in unique.items()},
    }


class CostCapture:
    """Finite in-memory recorder, explicitly driven by a real lifecycle.

    Profile scope is the current thread only. Runtime work in additional
    threads/processes must be reported separately, never assumed covered.
    """

    def __init__(self, *, slice_name: str, repetition: int, max_events: int = 2048):
        self.slice_name = slice_name
        self.repetition = repetition
        self.max_events = max_events
        self.events: list[dict] = []
        self.copy_counts: Counter[tuple[str, str, str, str]] = Counter()
        self._boundary = "unlabelled"
        self._suspended = 0

    def _add(self, event: dict) -> None:
        if len(self.events) >= self.max_events:
            raise RuntimeError("finite capture event budget exceeded")
        self.events.append({
            "slice": self.slice_name, "repetition": self.repetition,
            "event": len(self.events) + 1, "boundary": self._boundary, **event,
        })

    @contextmanager
    def boundary(self, name: str):
        previous = self._boundary
        self._boundary = name
        try:
            yield
        finally:
            self._boundary = previous

    @contextmanager
    def suspended(self):
        self._suspended += 1
        try:
            yield
        finally:
            self._suspended -= 1

    def _profile(self, frame, event, arg):
        if self._suspended or event != "call":
            return
        module = frame.f_globals.get("__name__", "")
        function = frame.f_code.co_name
        if not module.startswith("miniray.") or function not in PROFILE_FUNCTIONS:
            return
        value = frame.f_locals.get("self", frame.f_locals.get("value"))
        if value is None:
            value = frame.f_locals.get("snapshot", frame.f_locals.get("prepared"))
        if value is None:
            value = frame.f_locals.get("closed")
        kind = type(value).__name__ if value is not None else ""
        self.copy_counts[(self._boundary, module, function, kind)] += 1

    @contextmanager
    def profile_current_thread(self):
        if sys.getprofile() is not None:
            raise RuntimeError("refusing to replace an existing profiler")
        sys.setprofile(self._profile)
        try:
            yield
        finally:
            sys.setprofile(None)

    def message(
        self, *, direction: str, handler: str, value: object,
        serializer: Callable[[object], bytes],
        envelope: Callable[[object], object],
        delivery: str, query_role: str,
    ) -> None:
        """Observe one already-real request/reply, without dispatching it.

        The caller passes the frozen runtime serializer and exact business
        envelope factory. Loss delivery status is explicit. Trace is separate.
        """
        if direction not in ("request", "reply"):
            raise ValueError("direction must be request or reply")
        if delivery not in ("generated", "delivered", "discarded", "not_sent"):
            raise ValueError("unknown delivery status")
        if query_role not in ("business", "controller_internal", "test_observation", "none"):
            raise ValueError("unknown query role")
        with self.suspended():
            dto_bytes = len(serializer(value))
            business_bytes = len(serializer(envelope(value)))
            shape = content_occurrences(value)
        self._add({
            "kind": "message", "direction": direction, "handler": handler,
            "value_type": type(value).__name__, "delivery": delivery,
            "query_role": query_role, "dto_bytes": dto_bytes,
            "business_envelope_bytes": business_bytes,
            "untraced_business_frame_bytes": business_bytes + 4,
            "trace_sidecar_included": False, "content": shape,
        })

    def checkpoint(self, name: str, semantic_scalars: dict) -> None:
        """Caller supplies already-asserted JSON semantic facts.

        No automatic success projection or discarded-history normalization.
        """
        detached = json.loads(json.dumps(semantic_scalars, sort_keys=True))
        self._add({"kind": "checkpoint", "name": name, "facts": detached})

    def payload(self) -> dict:
        return {
            "status": "CAPTURE_ONLY_NOT_A_VERDICT",
            "slice": self.slice_name, "repetition": self.repetition,
            "profile_scope": "current_thread_business_boundaries_only",
            "events": self.events,
            "copy_validation_calls": [
                {"boundary": key[0], "module": key[1], "function": key[2],
                 "value_type": key[3], "calls": count}
                for key, count in sorted(self.copy_counts.items())
            ],
        }

    def write(self, path: Path) -> None:
        """Explicit output only; never called automatically or at import."""
        path.write_text(json.dumps(self.payload(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
