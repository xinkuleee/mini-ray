"""Transport causal tracing contracts with explicit execution modes.

The thirteen unit cases use only synchronous fake transport and memory sinks.
The original Thread and ThreadPoolExecutor cases remain heavy pending
separate bounded-runtime review. No listener is bound by these test bodies.
"""

import pickle
import socket
import struct
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import miniray.transport as transport
from miniray.trace import EventSink, MemoryEventSink, causal_scope


class _FakeConnection:
    def __init__(self, reply_factory):
        self.reply_factory = reply_factory
        self.sent = []
        self.timeout_calls = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def settimeout(self, value):
        self.timeout_calls.append(value)

    def sendall(self, frame):
        self.sent.append(frame)


def _patch_transport_for_current_thread(
    monkeypatch, connection, receiver
):
    """Expose one fake transport only to the test thread that owns it.

    ``socket`` is a process-wide module object and ``transport._receive`` is a
    process-wide function.  A plain monkeypatch therefore lets an unrelated
    background RPC borrow this test's connection and replay its first frame.
    Delegate calls from every other thread to the functions that were active
    before this local patch instead.
    """

    owner_thread = threading.current_thread()
    original_create_connection = transport.socket.create_connection
    original_receive = transport._receive

    def create_connection(*args, **kwargs):
        if threading.current_thread() is owner_thread:
            return connection
        return original_create_connection(*args, **kwargs)

    def receive(sock, max_frame_bytes):
        if threading.current_thread() is owner_thread:
            return receiver(sock, max_frame_bytes)
        return original_receive(sock, max_frame_bytes)

    monkeypatch.setattr(
        transport.socket, "create_connection", create_connection
    )
    monkeypatch.setattr(transport, "_receive", receive)


def _drive_client_through_server(monkeypatch, client_sink, server_sink, handler):
    connection = _FakeConnection(None)

    def fake_receive(sock, max_frame_bytes):
        assert sock is connection
        request_value = _decode_frame(connection.sent[0], max_frame_bytes)
        replies = []
        transport._serve_wire_request(
            request_value,
            {"work": handler},
            replies.append,
            sink=server_sink,
            component="server",
            peer="client",
        )
        assert len(replies) == 1
        return replies[0]

    _patch_transport_for_current_thread(
        monkeypatch, connection, fake_receive
    )
    client = transport.TCPClient(
        transport.LOOPBACK_HOST,
        12345,
        event_sink=client_sink,
        trace_component="client",
    )
    return client.request("work", 21), connection


def _decode_frame(frame, max_frame_bytes=transport.DEFAULT_MAX_FRAME_BYTES):
    offset = 0
    size = struct.unpack("!I", frame[offset : offset + 4])[0]
    offset += 4
    if size == 0:
        sidecar_size = struct.unpack(
            "!I", frame[offset : offset + 4]
        )[0]
        offset += 4
        sidecar = pickle.loads(frame[offset : offset + sidecar_size])
        offset += sidecar_size
        business_size = struct.unpack(
            "!I", frame[offset : offset + 4]
        )[0]
        offset += 4
        assert business_size <= max_frame_bytes
        business = pickle.loads(frame[offset : offset + business_size])
        return transport._TracedWireValue(business, sidecar)
    assert size <= max_frame_bytes
    return pickle.loads(frame[offset : offset + size])


def _assert_linear_causes(events, initial=None):
    cause = initial
    for event in events:
        assert event.cause_id == cause
        cause = event.event_id


@pytest.mark.unit
def test_successful_rpc_has_one_cross_boundary_causal_chain(monkeypatch):
    client_sink = MemoryEventSink(process_id=lambda: 101)
    server_sink = MemoryEventSink(process_id=lambda: 202)
    parent = client_sink.emit("caller", component="business")
    assert parent is not None

    with causal_scope(parent.event_id):
        result, connection = _drive_client_through_server(
            monkeypatch,
            client_sink,
            server_sink,
            lambda value: value * 2,
        )

    assert result == 42
    assert connection.closed
    client_events = client_sink.events[1:]
    server_events = server_sink.events
    assert [event.name for event in client_events] == [
        "rpc_request_sent",
        "rpc_reply_received",
    ]
    assert [event.name for event in server_events] == [
        "rpc_request_received",
        "rpc_handler_started",
        "rpc_handler_finished",
        "rpc_reply_sent",
    ]
    assert client_events[0].cause_id == parent.event_id
    _assert_linear_causes(server_events, client_events[0].event_id)
    assert client_events[1].cause_id == server_events[-1].event_id
    rpc_ids = {
        event.attributes["rpc_id"]
        for event in client_events + server_events
    }
    assert len(rpc_ids) == 1


@pytest.mark.unit
def test_handler_nested_rpc_advances_outer_handler_scope(monkeypatch):
    server_sink = MemoryEventSink(process_id=lambda: 212)
    nested_client_sink = MemoryEventSink(process_id=lambda: 213)

    def outer_handler(value):
        connection = _FakeConnection(None)

        def nested_receive(sock, max_frame_bytes):
            assert sock is connection
            request_value = _decode_frame(connection.sent[0], max_frame_bytes)
            replies = []
            transport._serve_wire_request(
                request_value,
                {"nested": lambda nested: nested + 1},
                replies.append,
                sink=None,
                component="nested-server",
                peer="nested-client",
            )
            return replies[0]

        _patch_transport_for_current_thread(
            monkeypatch, connection, nested_receive
        )
        return transport.TCPClient(
            transport.LOOPBACK_HOST,
            12346,
            event_sink=nested_client_sink,
            trace_component="nested-client",
        ).request("nested", value)

    replies = []
    request = transport._TracedWireValue(
        transport._WireRequest("outer", 10),
        transport._WireTraceSidecar("outer-rpc", "caller-event"),
    )
    transport._serve_wire_request(
        request,
        {"outer": outer_handler},
        replies.append,
        sink=server_sink,
        component="outer-server",
        peer="outer-client",
    )

    assert replies[0].value.value == 11
    nested_received = nested_client_sink.events[-1]
    finished = next(
        event
        for event in server_sink.events
        if event.name == "rpc_handler_finished"
    )
    assert finished.cause_id == nested_received.event_id


@pytest.mark.unit
def test_handler_nested_rpc_inherits_server_sink_without_explicit_client_sink(
    monkeypatch,
):
    sink = MemoryEventSink(process_id=lambda: 214)

    def outer_handler(value):
        connection = _FakeConnection(None)

        def nested_receive(sock, max_frame_bytes):
            request_value = _decode_frame(connection.sent[0], max_frame_bytes)
            replies = []
            transport._serve_wire_request(
                request_value, {"nested": lambda nested: nested + 1},
                replies.append, sink=None, component="nested-server",
                peer="nested-client",
            )
            return replies[0]

        _patch_transport_for_current_thread(
            monkeypatch, connection, nested_receive
        )
        return transport.TCPClient(
            transport.LOOPBACK_HOST, 12347
        ).request("nested", value)

    replies = []
    request = transport._TracedWireValue(
        transport._WireRequest("outer", 10),
        transport._WireTraceSidecar("outer-rpc-inherited", "caller-event"),
    )
    transport._serve_wire_request(
        request, {"outer": outer_handler}, replies.append, sink=sink,
        component="outer-server", peer="outer-client",
    )

    assert replies[0].value.value == 11
    names = [event.name for event in sink.events]
    assert "rpc_request_sent" in names
    assert "rpc_reply_received" in names
    finished = next(
        event for event in sink.events
        if event.name == "rpc_handler_finished"
        and event.attributes["handler"] == "outer"
    )
    nested_received = next(
        event for event in reversed(sink.events)
        if event.name == "rpc_reply_received"
        and event.attributes["handler"] == "nested"
    )
    assert finished.cause_id == nested_received.event_id


@pytest.mark.unit
def test_handler_business_event_and_failure_stay_in_scope(monkeypatch):
    client_sink = MemoryEventSink(process_id=lambda: 101)
    server_sink = MemoryEventSink(process_id=lambda: 202)

    def fail(_value):
        server_sink.emit("business_event", component="worker")
        raise ValueError("teaching failure")

    with pytest.raises(transport.RemoteCallError) as caught:
        _drive_client_through_server(
            monkeypatch, client_sink, server_sink, fail
        )

    assert caught.value.remote_type == "ValueError"
    assert [event.name for event in server_sink.events] == [
        "rpc_request_received",
        "rpc_handler_started",
        "business_event",
        "rpc_handler_failed",
        "rpc_reply_sent",
    ]
    _assert_linear_causes(server_sink.events, client_sink.events[0].event_id)
    assert [event.name for event in client_sink.events] == [
        "rpc_request_sent",
        "rpc_reply_received",
    ]
    assert client_sink.events[-1].attributes["ok"] is False


@pytest.mark.heavy
def test_fake_transport_does_not_capture_a_background_rpc(monkeypatch):
    client_sink = MemoryEventSink(process_id=lambda: 101)
    server_sink = MemoryEventSink(process_id=lambda: 202)
    background_connection = _FakeConnection(None)
    background_connector_calls = []
    background_receiver_calls = []
    background_outcomes = []

    # The helper must delegate a non-owner thread to the connector that was
    # active before it installed its local fake.  This sentinel keeps the
    # regression test pure while making that branch deterministic.
    def background_connector(*args, **kwargs):
        background_connector_calls.append((args, kwargs))
        return background_connection

    def background_receiver(sock, max_frame_bytes):
        background_receiver_calls.append((sock, max_frame_bytes))
        assert sock is background_connection
        request = _decode_frame(
            background_connection.sent[0], max_frame_bytes
        )
        assert request == transport._WireRequest("unrelated", None)
        return transport._WireReply(ok=True, value="background reply")

    monkeypatch.setattr(
        transport.socket, "create_connection", background_connector
    )
    monkeypatch.setattr(transport, "_receive", background_receiver)

    def handler(value):
        def call_from_background():
            background_outcomes.append(
                transport.TCPClient(
                    transport.LOOPBACK_HOST, 12345
                ).request("unrelated", None)
            )

        thread = threading.Thread(target=call_from_background)
        thread.start()
        thread.join(1.0)
        assert not thread.is_alive()
        return value * 2

    result, _connection = _drive_client_through_server(
        monkeypatch, client_sink, server_sink, handler
    )

    assert result == 42
    assert len(background_connector_calls) == 1
    assert len(background_receiver_calls) == 1
    assert background_outcomes == ["background reply"]
    assert background_connection.closed
    assert [event.name for event in server_sink.events] == [
        "rpc_request_received",
        "rpc_handler_started",
        "rpc_handler_finished",
        "rpc_reply_sent",
    ]


@pytest.mark.unit
def test_unknown_handler_is_a_dispatch_failure_with_error_reply():
    sink = MemoryEventSink(process_id=lambda: 214)
    replies = []
    request = transport._TracedWireValue(
        transport._WireRequest("missing", None),
        transport._WireTraceSidecar("rpc-missing", "caller-event"),
    )

    transport._serve_wire_request(
        request,
        {},
        replies.append,
        sink=sink,
        component="server",
        peer="client",
    )

    assert [event.name for event in sink.events] == [
        "rpc_request_received",
        "rpc_handler_failed",
        "rpc_reply_sent",
    ]
    assert sink.events[1].attributes["stage"] == "dispatch"
    assert replies[0].value.ok is False
    assert replies[0].value.error_type == "KeyError"


@pytest.mark.unit
def test_each_physical_client_attempt_gets_a_fresh_rpc_id(monkeypatch):
    sink = MemoryEventSink(process_id=lambda: 303)

    def refuse(*args, **kwargs):
        raise OSError("refused")

    monkeypatch.setattr(transport.socket, "create_connection", refuse)
    client = transport.TCPClient(
        transport.LOOPBACK_HOST, 12345, event_sink=sink
    )
    for _ in range(2):
        with pytest.raises(transport.TransportConnectionError):
            client.request("same-business-request", None)

    sent = [event for event in sink.events if event.name == "rpc_request_sent"]
    assert len(sent) == 2
    assert sent[0].event_id != sent[1].event_id
    assert sent[0].attributes["rpc_id"] != sent[1].attributes["rpc_id"]


@pytest.mark.unit
def test_server_reply_send_failure_is_not_retried_after_unknown_delivery():
    sink = MemoryEventSink(process_id=lambda: 404)
    calls = []

    def sender(value):
        calls.append(value)
        raise transport.TransportTimeout("unknown delivery")

    transport._send_server_reply(
        transport._WireReply(ok=True, value=42),
        sender,
        sink=sink,
        component="server",
        handler="work",
        peer="client",
        rpc_id="rpc-1",
    )

    assert len(calls) == 1
    assert [event.name for event in sink.events] == [
        "rpc_reply_sent",
        "rpc_reply_failed",
    ]
    assert sink.events[-1].attributes["stage"] == "reply_send"


@pytest.mark.unit
def test_server_reply_encode_failure_uses_one_fresh_fallback_attempt():
    sink = MemoryEventSink(process_id=lambda: 405)
    calls = []

    def sender(value):
        calls.append(value)
        if len(calls) == 1:
            raise transport._PreSendTransportError(
                transport.FrameTooLarge("too large")
            )

    transport._send_server_reply(
        transport._WireReply(ok=True, value=object()),
        sender,
        sink=sink,
        component="server",
        handler="work",
        peer="client",
        rpc_id="rpc-1",
    )

    assert len(calls) == 2
    assert [event.name for event in sink.events] == [
        "rpc_reply_sent",
        "rpc_reply_failed",
        "rpc_reply_sent",
    ]
    assert sink.events[0].event_id != sink.events[2].event_id
    assert sink.events[1].attributes["stage"] == "reply_encode"
    assert calls[1].value.ok is False


@pytest.mark.unit
def test_sink_override_failure_never_changes_client_outcome(monkeypatch):
    class HostileSink(EventSink):
        def emit(self, *args, **kwargs):
            raise KeyboardInterrupt("observational only")

    result, _connection = _drive_client_through_server(
        monkeypatch, HostileSink(), HostileSink(), lambda value: value * 2
    )
    assert result == 42


@pytest.mark.unit
def test_parent_cause_crosses_client_without_a_client_sink(monkeypatch):
    server_sink = MemoryEventSink(process_id=lambda: 505)
    parent_sink = MemoryEventSink(process_id=lambda: 504)
    parent = parent_sink.emit("caller", component="business")
    assert parent is not None

    with causal_scope(parent.event_id):
        result, _connection = _drive_client_through_server(
            monkeypatch, None, server_sink, lambda value: value
        )

    assert result == 21
    assert server_sink.events[0].cause_id == parent.event_id


@pytest.mark.unit
def test_trace_sidecar_does_not_consume_business_frame_budget():
    request = transport._WireRequest("work", b"x" * 100)
    business_size = len(transport._serialize(request))
    traced = transport._TracedWireValue(
        request, transport._WireTraceSidecar("rpc-1", "parent-1")
    )

    plain_frame = transport._encode(request, business_size)
    traced_frame = transport._encode(traced, business_size)

    assert isinstance(_decode_frame(plain_frame, business_size), transport._WireRequest)
    decoded = _decode_frame(traced_frame, business_size)
    assert isinstance(decoded, transport._TracedWireValue)
    assert decoded.value == request


@pytest.mark.unit
def test_no_sink_and_no_scope_preserve_exact_legacy_request_envelope(monkeypatch):
    connection = _FakeConnection(None)
    observed = {}

    def fake_receive(sock, max_frame_bytes):
        assert sock is connection
        observed["request"] = _decode_frame(
            connection.sent[0], max_frame_bytes
        )
        return transport._WireReply(ok=True, value="done")

    _patch_transport_for_current_thread(
        monkeypatch, connection, fake_receive
    )

    client = transport.TCPClient(transport.LOOPBACK_HOST, 12345)
    assert client.request("work", 7) == "done"
    assert observed["request"] == transport._WireRequest("work", 7)
    assert not isinstance(observed["request"], transport._TracedWireValue)


@pytest.mark.heavy
def test_causal_scopes_do_not_leak_between_threads_or_later_work():
    sink = MemoryEventSink(process_id=lambda: 606)

    def emit_chain(parent):
        with causal_scope(parent):
            first = sink.emit("first", component="test")
            second = sink.emit("second", component="test")
        return first, second

    with ThreadPoolExecutor(max_workers=2) as pool:
        chains = tuple(pool.map(emit_chain, ("parent-a", "parent-b")))

    for parent, (first, second) in zip(("parent-a", "parent-b"), chains):
        assert first.cause_id == parent
        assert second.cause_id == first.event_id
    outside = sink.emit("outside", component="test")
    assert outside is not None
    assert outside.cause_id is None


@pytest.mark.unit
def test_connection_failure_preserves_existing_exception_contract(monkeypatch):
    cause = socket.timeout()
    sink = MemoryEventSink(process_id=lambda: 707)

    def timeout(*args, **kwargs):
        raise cause

    monkeypatch.setattr(transport.socket, "create_connection", timeout)
    client = transport.TCPClient(
        transport.LOOPBACK_HOST, 12345, event_sink=sink
    )

    with pytest.raises(transport.TransportConnectionTimeout) as caught:
        client.request("work", None)

    assert caught.value.__cause__ is cause
    assert [event.name for event in sink.events] == [
        "rpc_request_sent",
        "rpc_request_failed",
    ]
    assert sink.events[-1].attributes["stage"] == "connect"
    assert sink.events[-1].attributes["delivery"] == "not_sent"
