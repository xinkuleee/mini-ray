"""Transport and tracing contracts with explicit execution modes.

Unit cases use fake sockets, early frame validation, or in-memory sinks.
The original two TCP-server cases and JSONL file-writing case remain heavy
until independently reviewed for a bounded runtime; their bodies are intact.
"""

import json
import socket
from pathlib import Path

import pytest

from miniray.trace import EventSink, JsonlEventSink, MemoryEventSink
from miniray.transport import (
    FrameTooLarge,
    RemoteCallError,
    TCPClient,
    TCPServer,
    TransportTimeout,
    request,
)


@pytest.mark.heavy
def test_tcp_request_reply_uses_explicit_handler_map() -> None:
    server = TCPServer(
        {
            "echo": lambda value: value,
            "add": lambda pair: pair[0] + pair[1],
        }
    )

    with server:
        assert server.is_running
        assert request(server.address, "echo", {"nested": [1, 2, 3]}) == {
            "nested": [1, 2, 3]
        }
        client = TCPClient(*server.address)
        assert client.request("add", (20, 22)) == 42
        with pytest.raises(RemoteCallError, match="unknown handler"):
            client.request("not-registered", None)

    assert not server.is_running


@pytest.mark.heavy
def test_remote_handler_error_is_returned_as_text() -> None:
    def fail(_value: object) -> object:
        raise ValueError("teaching failure")

    with TCPServer({"fail": fail}) as server:
        with pytest.raises(RemoteCallError) as caught:
            request(server.address, "fail", None)

    assert caught.value.remote_type == "ValueError"
    assert caught.value.remote_message == "teaching failure"
    assert "in fail" in caught.value.remote_traceback
    assert "ValueError: teaching failure" in caught.value.remote_traceback


@pytest.mark.unit
def test_client_receive_timeout_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    class TimeoutSocket:
        def __enter__(self) -> "TimeoutSocket":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def settimeout(self, timeout: float) -> None:
            assert timeout == 0.25

        def sendall(self, payload: bytes) -> None:
            assert payload

        def recv(self, _size: int) -> bytes:
            raise socket.timeout()

    monkeypatch.setattr(
        socket, "create_connection", lambda *args, **kwargs: TimeoutSocket()
    )
    client = TCPClient("127.0.0.1", 12345, request_timeout=0.25)

    with pytest.raises(TransportTimeout, match="receiving"):
        client.request("echo", 1)


@pytest.mark.unit
def test_frame_limit_is_checked_before_connect() -> None:
    client = TCPClient("127.0.0.1", 12345, max_frame_bytes=32)
    with pytest.raises(FrameTooLarge):
        client.request("echo", b"x" * 1024)


@pytest.mark.unit
def test_memory_trace_has_process_sequence_and_cause_id() -> None:
    timestamps = iter([100, 200])
    sink = MemoryEventSink(
        clock_ns=lambda: next(timestamps), process_id=lambda: 7001
    )

    submitted = sink.emit(
        "task_submitted", component="core_worker", task_id="task-1"
    )
    assert submitted is not None
    started = sink.emit(
        "task_started",
        component="worker",
        cause_id=submitted.event_id,
        attributes={"task_id": "task-1"},
    )

    assert started is not None
    assert [event.process_seq for event in sink.events] == [1, 2]
    assert [event.timestamp_ns for event in sink.events] == [100, 200]
    assert started.cause_id == submitted.event_id
    assert started.attributes == {"task_id": "task-1"}
    assert submitted.entity_kind == started.entity_kind == "task"
    assert submitted.entity_id == started.entity_id == "task-1"


@pytest.mark.heavy
def test_jsonl_trace_writes_one_structured_event_per_line(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    with JsonlEventSink(
        path, clock_ns=lambda: 123, process_id=lambda: 99
    ) as sink:
        event = sink.emit(
            "object_ready",
            component="object_store",
            object_id="object-1",
        )

    assert event is not None
    assert event.entity_kind == "object"
    assert event.entity_id == "object-1"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows == [event.to_dict()]


@pytest.mark.unit
def test_trace_failure_never_escapes_to_runtime() -> None:
    class BrokenSink(EventSink):
        def _write(self, event: object) -> None:
            del event
            raise OSError("trace destination unavailable")

    sink = BrokenSink(clock_ns=lambda: 1, process_id=lambda: 2)

    event = sink.emit("still_observational", component="runtime")

    assert event is not None
    assert sink.dropped_events == 1
