import math
import socket

import pytest

import miniray.transport as transport
from miniray.transport import TCPClient, TCPServer, request

pytestmark = pytest.mark.unit


class _FakeConnection:
    def __init__(self) -> None:
        self.timeout_calls = []
        self.sent = []
        self.closed = False

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def settimeout(self, timeout: object) -> None:
        self.timeout_calls.append(timeout)

    def sendall(self, payload: bytes) -> None:
        self.sent.append(payload)

    def recv(self, size: int) -> bytes:
        del size
        return b"x"


def test_none_request_timeout_restores_blocking_mode_after_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    connect_calls = []

    def fake_create_connection(address: object, *, timeout: float) -> object:
        connect_calls.append((address, timeout))
        return connection

    def fake_receive(sock: object, max_frame_bytes: int) -> object:
        assert sock is connection
        assert connection.timeout_calls == [None]
        assert max_frame_bytes == transport.DEFAULT_MAX_FRAME_BYTES
        return transport._WireReply(ok=True, value="done")

    monkeypatch.setattr(
        transport.socket, "create_connection", fake_create_connection
    )
    monkeypatch.setattr(transport, "_receive", fake_receive)

    client = TCPClient(
        transport.LOOPBACK_HOST,
        12345,
        connect_timeout=0.75,
        request_timeout=None,
    )

    assert client.request("work", 42) == "done"
    assert connect_calls == [((transport.LOOPBACK_HOST, 12345), 0.75)]
    assert len(connection.sent) == 1
    assert connection.closed


def test_request_wrapper_accepts_none_request_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    class FakeClient:
        def __init__(self, host: str, port: int, **options: object) -> None:
            captured["init"] = (host, port, options)

        def request(self, handler: str, payload: object) -> object:
            captured["request"] = (handler, payload)
            return "reply"

    monkeypatch.setattr(transport, "TCPClient", FakeClient)

    assert (
        request(
            (transport.LOOPBACK_HOST, 12345),
            "echo",
            7,
            request_timeout=None,
        )
        == "reply"
    )
    assert captured["init"][2]["request_timeout"] is None
    assert captured["request"] == ("echo", 7)


def test_receive_recomputes_timeout_from_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    ticks = iter((10.0, 10.2, 10.7))
    monkeypatch.setattr(transport.time, "monotonic", lambda: next(ticks))

    assert transport._recv_exact(
        connection, 3, deadline=11.0, request_timeout=5.0
    ) == b"xxx"
    assert connection.timeout_calls == pytest.approx([1.0, 0.8, 0.3])


def test_receive_stops_when_absolute_deadline_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    ticks = iter((10.0, 10.6))
    monkeypatch.setattr(transport.time, "monotonic", lambda: next(ticks))

    with pytest.raises(transport.TransportTimeout, match="deadline expired"):
        transport._recv_exact(
            connection, 2, deadline=10.5, request_timeout=5.0
        )
    assert connection.timeout_calls == pytest.approx([0.5])


def test_client_threads_one_deadline_through_connect_send_and_receive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    observed = {}
    ticks = iter((10.0, 10.1, 10.3))
    monkeypatch.setattr(transport.time, "monotonic", lambda: next(ticks))

    def connect(address: object, *, timeout: float) -> object:
        observed["connect"] = (address, timeout)
        return connection

    def receive(
        sock: object, max_frame_bytes: int, *, deadline: float,
        request_timeout: float | None,
    ) -> object:
        observed["receive"] = (
            sock, max_frame_bytes, deadline, request_timeout
        )
        return transport._WireReply(ok=True, value="done")

    monkeypatch.setattr(transport.socket, "create_connection", connect)
    monkeypatch.setattr(transport, "_receive", receive)
    client = TCPClient(
        transport.LOOPBACK_HOST, 12345, connect_timeout=5.0,
        request_timeout=5.0, deadline=11.0,
    )

    assert client.request("work", 1) == "done"
    assert observed["connect"][1] == pytest.approx(1.0)
    assert connection.timeout_calls == pytest.approx([0.9])
    assert observed["receive"][2:] == (11.0, 5.0)


def test_expired_client_deadline_emits_terminal_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from miniray.trace import MemoryEventSink

    sink = MemoryEventSink(process_id=lambda: 999)
    monkeypatch.setattr(transport.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(
        transport.socket, "create_connection",
        lambda *_args, **_kwargs: pytest.fail("expired deadline must not connect"),
    )
    client = TCPClient(
        transport.LOOPBACK_HOST, 12345, event_sink=sink, deadline=9.0
    )

    with pytest.raises(transport.TransportTimeout, match="deadline expired"):
        client.request("work", None)
    assert [event.name for event in sink.events] == [
        "rpc_request_sent", "rpc_request_failed",
    ]
    assert sink.events[-1].attributes["stage"] == "connect"
    assert sink.events[-1].attributes["delivery"] == "not_sent"


@pytest.mark.parametrize(
    ("cause", "expected_type"),
    [
        (OSError("refused"), transport.TransportConnectionError),
        (socket.timeout(), transport.TransportConnectionTimeout),
    ],
)
def test_connection_failures_have_a_pre_send_type(
    monkeypatch: pytest.MonkeyPatch,
    cause: OSError,
    expected_type: type,
) -> None:
    def fail_to_connect(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise cause

    monkeypatch.setattr(transport.socket, "create_connection", fail_to_connect)

    client = TCPClient(transport.LOOPBACK_HOST, 12345)
    with pytest.raises(transport.TransportConnectionError) as caught:
        client.request("work", None)

    assert type(caught.value) is expected_type
    assert caught.value.__cause__ is cause
    if isinstance(cause, socket.timeout):
        assert isinstance(caught.value, transport.TransportTimeout)


@pytest.mark.parametrize("value", [0.0, -1.0, math.inf, -math.inf, math.nan])
def test_client_rejects_non_positive_or_non_finite_request_timeout(
    value: float,
) -> None:
    with pytest.raises(ValueError, match="positive finite"):
        TCPClient(transport.LOOPBACK_HOST, 12345, request_timeout=value)


@pytest.mark.parametrize("value", [None, 0.0, math.inf, math.nan])
def test_none_is_not_permitted_for_other_timeout_roles(value: object) -> None:
    with pytest.raises(ValueError, match="positive finite"):
        TCPClient(transport.LOOPBACK_HOST, 12345, connect_timeout=value)

    with pytest.raises(ValueError, match="positive finite"):
        TCPServer({}, request_timeout=value)


def test_server_thread_start_failure_remains_synchronously_stoppable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = object.__new__(TCPServer)
    backend = type(
        "Backend",
        (),
        {
            "serve_forever": lambda self, **_kwargs: None,
            "shutdown": lambda self: pytest.fail(
                "a never-started serve loop must not be shut down"
            ),
            "server_close": lambda self: setattr(self, "closed", True),
            "closed": False,
        },
    )()
    lock = transport.threading.Lock()
    server._server = backend
    server._address = (transport.LOOPBACK_HOST, 12345)
    server._state_lock = lock
    server._thread = None
    server._closed = False

    class _StartFailureThread:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def start(self) -> None:
            raise RuntimeError("thread start failed")

    monkeypatch.setattr(transport.threading, "Thread", _StartFailureThread)

    with pytest.raises(RuntimeError, match="thread start failed"):
        server.start()

    assert server._thread is None
    assert not server.is_running
    server.stop()
    assert backend.closed
