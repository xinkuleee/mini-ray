"""Trace export contracts with explicit pure/file-I/O classification.

The two rejection-path functions (four expanded cases) must fail before any
runtime or file-writing work. The four original filesystem contracts remain
heavy until individually bounded execution is reviewed: small tmp_path data
does not bound actual fsync latency. Their real I/O is not replaced by fakes.
Do not add a module-level unit marker; pytest markers are additive.
"""

from __future__ import annotations

import builtins
from contextlib import contextmanager
import importlib
import io
import json
import multiprocessing.process
from pathlib import Path
import socket
import subprocess
import threading
from types import SimpleNamespace

import pytest

import miniray
from miniray import api, protocol
from miniray import trace as public_trace


trace_module = importlib.import_module("miniray.trace")


class _Collector:
    def __init__(self, records: tuple[protocol.TraceRecord, ...]) -> None:
        self.records = records


def _record(
    event_id: str,
    process_id: str,
    sequence: int,
    *,
    event: str = "task_started",
    entity_kind: str = "task",
    entity_id: str = "task-1",
    cause: str | None = None,
    fields: tuple[tuple[str, str], ...] = (("task_id", "task-1"),),
) -> protocol.TraceRecord:
    return protocol.TraceRecord(
        event_id=event_id,
        timestamp_ns=100 + sequence,
        process_id=process_id,
        process_sequence=sequence,
        component="worker",
        event=event,
        entity_kind=entity_kind,
        entity_id=entity_id,
        cause_event_id=cause,
        fields=fields,
    )


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch, collector: _Collector | None
) -> None:
    monkeypatch.setattr(
        api, "_runtime", SimpleNamespace(trace_collector=collector)
    )
    monkeypatch.setattr(api, "current_core_worker", lambda: None)


def _json_lines(path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@contextmanager
def _pure_rejection_guard(monkeypatch, *, forbid_writer=False):
    """Tripwires apply only inside the two pure rejection tests below."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure trace-export rejection reached runtime or file I/O")

    # A local context restores even open()/wait() before pytest reports a
    # failure. Heavy tests and their original filesystem behavior are untouched.
    with monkeypatch.context() as patch:
        for target, name in (
            (api.CoreWorker, "__init__"), (api.OwnerService, "__init__"),
            (api.TraceCollector, "__init__"),
            (multiprocessing.process.BaseProcess, "start"),
            (subprocess, "Popen"), (socket, "socket"), (socket, "create_connection"),
            (threading.Thread, "start"), (threading.Thread, "join"),
            (threading.Event, "wait"), (threading.Condition, "wait"),
            (trace_module.time, "sleep"), (trace_module.tempfile, "mkstemp"),
            (builtins, "open"), (io, "open"), (trace_module.os, "open"),
            (trace_module.os, "fdopen"), (trace_module.os, "fsync"),
            (trace_module.os, "replace"), (trace_module.os, "unlink"),
        ):
            patch.setattr(target, name, forbidden)
        if forbid_writer:
            patch.setattr(api, "write_trace_records_jsonl", forbidden)
            patch.setattr(trace_module, "write_trace_records_jsonl", forbidden)
        yield patch


@pytest.mark.heavy
def test_public_export_is_deterministic_and_preserves_semantic_schema(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cause = _record(
        "driver-1", "driver", 1, event="task_submitted", cause=None
    )
    child = _record(
        "worker-2",
        "worker",
        2,
        entity_kind="object",
        entity_id="object-7",
        cause=cause.event_id,
        fields=(("storage", "INLINE"), ("object_id", "object-7")),
    )
    earlier_worker_event = _record("worker-1", "worker", 1)
    collector = _Collector((child, earlier_worker_event, cause))
    _install_runtime(monkeypatch, collector)
    destination = tmp_path / "trace.jsonl"

    assert miniray.export_trace(destination) is None
    first_bytes = destination.read_bytes()
    rows = _json_lines(destination)
    collector.records = tuple(reversed(collector.records))
    miniray.export_trace(destination)

    assert destination.read_bytes() == first_bytes
    assert [row["event_id"] for row in rows] == [
        "driver-1", "worker-1", "worker-2"
    ]
    assert rows[2] == {
        "schema_version": 1,
        "event_id": "worker-2",
        "timestamp_ns": 102,
        "process_id": "worker",
        "process_sequence": 2,
        "component": "worker",
        "event": "task_started",
        "entity_kind": "object",
        "entity_id": "object-7",
        "cause_event_id": "driver-1",
        "fields": {"object_id": "object-7", "storage": "INLINE"},
    }
    assert miniray.export_trace is api.export_trace
    assert public_trace is api.trace


@pytest.mark.heavy
def test_export_overwrites_old_tail_and_empty_snapshot_creates_empty_file(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    collector = _Collector((_record("one", "worker", 1),))
    _install_runtime(monkeypatch, collector)
    destination = tmp_path / "trace.jsonl"
    destination.write_text("stale first line\nstale trailing bytes", encoding="utf-8")

    miniray.export_trace(destination)
    assert [row["event_id"] for row in _json_lines(destination)] == ["one"]
    assert "stale" not in destination.read_text(encoding="utf-8")

    collector.records = ()
    miniray.export_trace(destination)
    assert destination.read_bytes() == b""

    _install_runtime(monkeypatch, None)
    destination.write_text("old", encoding="utf-8")
    miniray.export_trace(destination)
    assert destination.read_bytes() == b""


@pytest.mark.heavy
def test_export_failure_leaves_the_previous_file_and_removes_temporary_file(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    collector = _Collector((_record("one", "worker", 1),))
    _install_runtime(monkeypatch, collector)
    destination = tmp_path / "trace.jsonl"
    destination.write_text("previous complete export\n", encoding="utf-8")

    def fail_dump(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected serialization failure")

    monkeypatch.setattr(trace_module.json, "dump", fail_dump)
    with pytest.raises(OSError, match="injected serialization failure"):
        miniray.export_trace(destination)

    assert destination.read_text(encoding="utf-8") == "previous complete export\n"
    assert tuple(tmp_path.glob(".trace.jsonl.trace-*.tmp")) == ()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("path", "error", "message"),
    (
        (object(), TypeError, "string or os.PathLike"),
        (b"trace.jsonl", TypeError, "text filesystem path"),
        ("", ValueError, "non-empty"),
    ),
)
def test_export_rejects_invalid_path_values(
    path: object,
    error: type[BaseException],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Keep the real writer's argument validation; its first physical I/O
    # operation, not the validator itself, is forbidden by this guard.
    with _pure_rejection_guard(monkeypatch) as patch:
        _install_runtime(patch, None)
        with pytest.raises(error, match=message):
            miniray.export_trace(path)


@pytest.mark.unit
def test_export_requires_an_initialized_driver_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = Path("trace-must-not-be-written.jsonl")
    # No tmp_path allocation or file existence probe: neither rejection may
    # even invoke the writer, which is stronger than inspecting its output.
    with _pure_rejection_guard(monkeypatch, forbid_writer=True) as patch:
        patch.setattr(api, "current_core_worker", lambda: None)
        patch.setattr(api, "_runtime", None)
        with pytest.raises(RuntimeError, match=r"init\(\).*before"):
            miniray.export_trace(destination)

        patch.setattr(api, "current_core_worker", lambda: object())
        with pytest.raises(RuntimeError, match="export_trace.*Driver-only"):
            miniray.export_trace(destination)


@pytest.mark.heavy
def test_export_rejects_missing_parent_and_directory_targets(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_runtime(monkeypatch, None)

    with pytest.raises(FileNotFoundError):
        miniray.export_trace(tmp_path / "missing" / "trace.jsonl")
    with pytest.raises(IsADirectoryError):
        miniray.export_trace(tmp_path)
    assert tuple(tmp_path.glob(".*.trace-*.tmp")) == ()
